#!/usr/bin/env python3
"""
FORWARDER + DUODETECTIVE BOT - OPTIMIZED QUEUE VERSION
Combined bot that handles:
1. Message forwarding with filters - direct sending with queued retries
2. Duplicate message detection with notifications and auto-reply
3. Full navigation with Cancel/Back buttons
"""

import os
import sys
import asyncio
import logging
import hashlib
import time
import gc
import json
import threading
import functools
import re
import signal
import traceback
from types import SimpleNamespace
import pytz
import uuid
import atexit
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Set, Callable, Any, DefaultDict
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram.error import BadRequest

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.helpers import escape_markdown

import psycopg
from psycopg.rows import dict_row
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()

logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("flask").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('combined_bot_optimized.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger("forwarder_duodetect_opt")

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.error("DATABASE_URL must be set for PostgreSQL!")
    raise ValueError("DATABASE_URL environment variable is required")

required_vars = ["BOT_TOKEN", "API_ID", "API_HASH", "DATABASE_URL"]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

logger.info("Using PostgreSQL database")

# Optional access allow-list. Comma/space separated Telegram user IDs.
# Unset  -> the bot is open to every Telegram user.
# Set    -> only the listed IDs can use the bot (fails closed on a malformed value).
ALLOWED_USER_IDS: Set[int] = set()
_allowed_env = os.getenv("ALLOWED_USER_IDS", "").strip()
if _allowed_env:
    for _tok in re.split(r"[,\s]+", _allowed_env):
        if not _tok:
            continue
        if not _tok.lstrip("-").isdigit():
            raise ValueError(f"ALLOWED_USER_IDS contains an invalid entry: {_tok!r}")
        ALLOWED_USER_IDS.add(int(_tok))
    if not ALLOWED_USER_IDS:
        raise ValueError("ALLOWED_USER_IDS is set but contains no valid user IDs")


def is_user_allowed(user_id: int) -> bool:
    """True when no allow-list is configured, or the user is on it."""
    return not ALLOWED_USER_IDS or user_id in ALLOWED_USER_IDS


SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "150"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "8000"))
MONITOR_WORKER_COUNT = int(os.getenv("MONITOR_WORKER_COUNT", "100"))
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "1"))
MAX_CONCURRENT_USERS = max(80, int(os.getenv("MAX_CONCURRENT_USERS", "80")))
SEND_CONCURRENCY_PER_USER = int(os.getenv("SEND_CONCURRENCY_PER_USER", "60"))
SEND_RATE_PER_USER = float(os.getenv("SEND_RATE_PER_USER", "60.0"))
TARGET_ENTITY_CACHE_SIZE = int(os.getenv("TARGET_ENTITY_CACHE_SIZE", "2000"))

IMMEDIATE_WORKERS = int(os.getenv("IMMEDIATE_WORKERS", "220"))
RETRY_WORKERS = int(os.getenv("RETRY_WORKERS", "90"))
RETRY_SLEEP = int(os.getenv("RETRY_SLEEP", "1"))
FLOOD_WAIT_WORKERS = int(os.getenv("FLOOD_WAIT_WORKERS", "50"))


DUPLICATE_CHECK_WINDOW = int(os.getenv("DUPLICATE_CHECK_WINDOW", "900"))
MESSAGE_HASH_LIMIT = int(os.getenv("MESSAGE_HASH_LIMIT", "10000"))
CONN_HEALTH_CHECK_INTERVAL = int(os.getenv("CONN_HEALTH_CHECK_INTERVAL", "60"))
MAX_REPLIES_PER_DUPLICATE = 3

SESSION_CHECK_INTERVAL = int(os.getenv("SESSION_CHECK_INTERVAL", "1800"))

WEB_SERVER_PORT = int(os.getenv("WEB_SERVER_PORT", "5000"))
DEFAULT_CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "6144"))

GC_INTERVAL = int(os.getenv("GC_INTERVAL", "1800"))

_gc_pause_start = {}

def _gc_pause_callback(phase, info):
    if phase == "start":
        _gc_pause_start["t"] = time.time()
    elif phase == "stop":
        t0 = _gc_pause_start.get("t")
        if t0 is not None:
            dur_ms = (time.time() - t0) * 1000
            if dur_ms > 50:
                logger.info(f"[GC-PAUSE] gen={info.get('generation')} {dur_ms:.0f}ms | "
                            f"collected={info.get('collected')} uncollectable={info.get('uncollectable')}")

gc.callbacks.append(_gc_pause_callback)


EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+", flags=re.UNICODE
)

WORD_PATTERN = re.compile(r'\S+')
NUMERIC_PATTERN = re.compile(r'^\d+$')
ALPHABETIC_PATTERN = re.compile(r'^[A-Za-z]+$')


def parse_chat_id(id_val) -> Tuple[int, Optional[int]]:
    """
    Parse a stored ID value into (chat_id, topic_id).
    Accepts int, or strings like "-100123456789" or "-100123456789:36615".
    Returns (chat_id, None) when no topic is specified.
    """
    s = str(id_val)
    if ':' in s:
        parts = s.split(':', 1)
        return int(parts[0]), int(parts[1])
    return int(s), None


def _is_valid_id_token(token: str) -> bool:
    """Return True if *token* is a valid plain ID or 'chat_id:topic_id' token."""
    token = token.strip()
    if ':' in token:
        parts = token.split(':', 1)
        return (parts[0].strip().lstrip('-').isdigit() and
                parts[1].strip().isdigit())
    return token.lstrip('-').isdigit()


def _parse_id_token(token: str) -> Optional[str]:
    """
    Convert a user-typed ID token to its canonical string form.
    Examples: "-100123" → "-100123",  "-100123:36615" → "-100123:36615".
    Returns None for invalid tokens.
    """
    token = token.strip()
    if ':' in token:
        parts = token.split(':', 1)
        chat_part = parts[0].strip()
        topic_part = parts[1].strip()
        if chat_part.lstrip('-').isdigit() and topic_part.isdigit():
            return f"{int(chat_part)}:{int(topic_part)}"
        return None
    elif token.lstrip('-').isdigit():
        return token
    return None


def _get_message_topic_id(message) -> Optional[int]:
    """
    Extract the forum-topic ID from a Telethon message object.
    Returns None for regular (non-topic) messages.
    """
    reply_to = getattr(message, 'reply_to', None)
    if reply_to is None:
        return None
    return (getattr(reply_to, 'reply_to_top_id', None) or
            getattr(reply_to, 'reply_to_msg_id', None))


def _source_id_matches(event_chat_id: int,
                        event_topic_id: Optional[int],
                        src_val) -> bool:
    """
    Return True if a stored source-ID value matches (event_chat_id, event_topic_id).
    When the stored value has no topic part every message in that chat matches.
    When it has a topic part only messages in that specific topic match.
    """
    src_chat_id, src_topic_id = parse_chat_id(src_val)
    if src_chat_id != event_chat_id:
        return False
    if src_topic_id is not None:
        return event_topic_id == src_topic_id
    return True


class Database:

    def __init__(self):
        self.postgres_url = DATABASE_URL

        self._conn_init_lock = threading.Lock()
        self._thread_local = threading.local()
        self._thread_pool = ThreadPoolExecutor(max_workers=50, thread_name_prefix="db_worker")

        self._user_cache: Dict[int, Dict] = {}
        self._forwarding_tasks_cache: Dict[int, List[Dict]] = defaultdict(list)
        self._monitoring_tasks_cache: Dict[int, List[Dict]] = defaultdict(list)

        try:
            self.init_db()
            self._load_caches()
            logger.info("PostgreSQL database initialized")
        except Exception as e:
            logger.exception(f"Failed initializing DB: {e}")
            raise

        atexit.register(self.close_connection)

    def _create_postgres_connection(self) -> psycopg.Connection:
        if not self.postgres_url:
            raise ValueError("DATABASE_URL not set for PostgreSQL")

        parsed = urlparse(self.postgres_url)

        dbname = parsed.path[1:]
        user = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port or 5432

        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        if parsed.query:
            params = dict(pair.split('=') for pair in parsed.query.split('&') if '=' in pair)
            sslmode = params.get('sslmode', 'require')
            conn_str += f"?sslmode={sslmode}"

        conn = psycopg.connect(
            conn_str,
            autocommit=True,
            row_factory=dict_row
        )
        return conn

    def _load_caches(self):
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at
                    FROM users WHERE is_logged_in = TRUE
                """)
                rows = cur.fetchall()
                for row in rows:
                    uid = row["user_id"]
                    entry = {
                        'user_id': uid,
                        'phone': row["phone"],
                        'name': row["name"],
                        'session_data': row["session_data"],
                        'is_logged_in': row["is_logged_in"],
                        'created_at': row["created_at"].isoformat() if row["created_at"] else None,
                        'updated_at': row["updated_at"].isoformat() if row["updated_at"] else None
                    }
                    self._user_cache[uid] = entry

            logger.info(f"Loaded caches: {len(self._user_cache)} logged-in users")

        except Exception as e:
            logger.exception("Error loading caches: %s", e)

    def get_connection(self):
        conn = getattr(self._thread_local, "conn", None)

        if conn:
            last_checked = getattr(self._thread_local, "last_checked", 0)
            now = time.time()

            if now - last_checked < CONN_HEALTH_CHECK_INTERVAL:
                return conn

            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                self._thread_local.last_checked = now
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                self._thread_local.conn = None

        try:
            new_conn = self._create_postgres_connection()
            with new_conn.cursor() as cur:
                cur.execute("SELECT 1")
            self._thread_local.conn = new_conn
            self._thread_local.last_checked = time.time()
            return self._thread_local.conn
        except Exception as e:
            logger.exception("Failed to create DB connection: %s", e)
            raise

    def close_connection(self):
        conn = getattr(self._thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                logger.exception("Failed to close DB connection")
            self._thread_local.conn = None

    def init_db(self):
        with self._conn_init_lock:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        phone VARCHAR(255),
                        name TEXT,
                        session_data TEXT,
                        is_logged_in BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS forwarding_tasks (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        label VARCHAR(255),
                        source_ids JSONB,
                        target_ids JSONB,
                        filters JSONB,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users (user_id),
                        UNIQUE(user_id, label)
                    )
                """)
                cur.execute("ALTER TABLE forwarding_tasks ADD COLUMN IF NOT EXISTS active_source_ids JSONB DEFAULT NULL")
                cur.execute("ALTER TABLE forwarding_tasks ADD COLUMN IF NOT EXISTS active_target_ids JSONB DEFAULT NULL")
                cur.execute("UPDATE forwarding_tasks SET active_source_ids = source_ids WHERE active_source_ids IS NULL")
                cur.execute("UPDATE forwarding_tasks SET active_target_ids = target_ids WHERE active_target_ids IS NULL")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS monitoring_tasks (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        label VARCHAR(255),
                        chat_ids JSONB,
                        settings JSONB,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users (user_id),
                        UNIQUE(user_id, label)
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_users_logged_in ON users(is_logged_in)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_forwarding_tasks_user_active ON forwarding_tasks(user_id, is_active)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_monitoring_tasks_user_active ON monitoring_tasks(user_id, is_active)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS notification_state (
                        msg_id BIGINT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        data JSONB NOT NULL,
                        created_at DOUBLE PRECISION NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS duplicate_counts_state (
                        duplicate_key TEXT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        count INTEGER DEFAULT 0,
                        updated_at DOUBLE PRECISION NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS message_hash_history (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        chat_id BIGINT NOT NULL,
                        msg_hash TEXT NOT NULL,
                        seen_at DOUBLE PRECISION NOT NULL,
                        msg_text TEXT
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mhh_user_chat
                    ON message_hash_history(user_id, chat_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mhh_seen_at
                    ON message_hash_history(seen_at)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS flood_wait_state (
                        user_id BIGINT PRIMARY KEY,
                        wait_until DOUBLE PRECISION NOT NULL
                    )
                """)

            conn.commit()

            logger.info("Database initialized successfully")


    def get_user(self, user_id: int) -> Optional[Dict]:
        if user_id in self._user_cache:
            return self._user_cache[user_id].copy()

        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at
                    FROM users WHERE user_id = %s
                """, (user_id,))
                row = cur.fetchone()

                if row:
                    user_data = {
                        'user_id': row["user_id"],
                        'phone': row["phone"],
                        'name': row["name"],
                        'session_data': row["session_data"],
                        'is_logged_in': row["is_logged_in"],
                        'created_at': row["created_at"].isoformat() if row["created_at"] else None,
                        'updated_at': row["updated_at"].isoformat() if row["updated_at"] else None
                    }
                    self._user_cache[user_id] = user_data
                    return user_data.copy()

            return None
        except Exception as e:
            logger.exception("Error in get_user for %s: %s", user_id, e)
            return None

    def save_user(self, user_id: int, phone: Optional[str] = None, name: Optional[str] = None,
                  session_data: Optional[str] = None, is_logged_in: bool = False):
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        phone = EXCLUDED.phone,
                        name = EXCLUDED.name,
                        session_data = EXCLUDED.session_data,
                        is_logged_in = EXCLUDED.is_logged_in,
                        updated_at = CURRENT_TIMESTAMP
                """, (user_id, phone, name, session_data, is_logged_in))

            conn.commit()

            if user_id in self._user_cache:
                user_data = self._user_cache[user_id]
                if phone is not None:
                    user_data['phone'] = phone
                if name is not None:
                    user_data['name'] = name
                if session_data is not None:
                    user_data['session_data'] = session_data
                user_data['is_logged_in'] = is_logged_in
                user_data['updated_at'] = datetime.now().isoformat()
            else:
                if is_logged_in:
                    self._user_cache[user_id] = {
                        'user_id': user_id,
                        'phone': phone,
                        'name': name,
                        'session_data': session_data,
                        'is_logged_in': is_logged_in,
                        'updated_at': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.exception("Error in save_user for %s: %s", user_id, e)
            raise

    def purge_user_data(self, user_id: int) -> bool:
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("DELETE FROM forwarding_tasks WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM monitoring_tasks WHERE user_id = %s", (user_id,))
                cur.execute("""
                    UPDATE users
                    SET phone = NULL,
                        name = NULL,
                        session_data = NULL,
                        is_logged_in = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                """, (user_id,))
                conn.commit()

            self._user_cache.pop(user_id, None)
            self._forwarding_tasks_cache.pop(user_id, None)
            self._monitoring_tasks_cache.pop(user_id, None)

            logger.info(f"Purged all data for user {user_id} on logout")
            return True

        except Exception as e:
            logger.exception(f"Error purging user data for {user_id}: {e}")
            return False

    def add_forwarding_task(self, user_id: int, label: str, source_ids: List[int],
                           target_ids: List[int], filters: Optional[Dict[str, Any]] = None) -> bool:
        try:
            conn = self.get_connection()

            if filters is None:
                filters = {
                    "filters": {
                        "raw_text": True,
                        "numbers_only": False,
                        "alphabets_only": False,
                        "removed_alphabetic": False,
                        "removed_numeric": False,
                        "prefix": "",
                        "suffix": ""
                    },
                    "outgoing": True,
                    "forward_tag": False,
                    "control": True
                }

            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO forwarding_tasks (user_id, label, source_ids, target_ids, filters,
                                                       active_source_ids, active_target_ids)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, label) DO NOTHING
                        RETURNING id
                    """,
                        (user_id, label, json.dumps(source_ids), json.dumps(target_ids), json.dumps(filters),
                         json.dumps(source_ids), json.dumps(target_ids)),
                    )
                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        task_id = row["id"]
                        task = {
                            "id": task_id,
                            "label": label,
                            "source_ids": source_ids,
                            "target_ids": target_ids,
                            "active_source_ids": list(source_ids),
                            "active_target_ids": list(target_ids),
                            "filters": filters,
                            "is_active": 1
                        }
                        self._forwarding_tasks_cache[user_id].append(task)
                        return True
                    return False
                except psycopg.errors.UniqueViolation:
                    return False

        except Exception as e:
            logger.exception("Error in add_forwarding_task for %s: %s", user_id, e)
            raise

    def update_task_filters(self, user_id: int, label: str, filters: Dict[str, Any]) -> bool:
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE forwarding_tasks
                    SET filters = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND label = %s
                    """,
                    (json.dumps(filters), user_id, label),
                )
                updated = cur.rowcount > 0
                conn.commit()

            if updated and user_id in self._forwarding_tasks_cache:
                for task in self._forwarding_tasks_cache[user_id]:
                    if task['label'] == label:
                        task['filters'] = filters
                        break

            return updated
        except Exception as e:
            logger.exception("Error in update_task_filters for %s, task %s: %s", user_id, label, e)
            raise

    def remove_forwarding_task(self, user_id: int, label: str) -> bool:
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("DELETE FROM forwarding_tasks WHERE user_id = %s AND label = %s", (user_id, label))
                deleted = cur.rowcount > 0
                conn.commit()

            if deleted and user_id in self._forwarding_tasks_cache:
                self._forwarding_tasks_cache[user_id] = [t for t in self._forwarding_tasks_cache[user_id] if t.get('label') != label]

            return deleted
        except Exception as e:
            logger.exception("Error in remove_forwarding_task for %s: %s", user_id, e)
            raise

    def update_forwarding_task(self, user_id: int, label: str,
                               new_label: Optional[str] = None,
                               source_ids: Optional[List] = None,
                               target_ids: Optional[List] = None) -> bool:
        try:
            conn = self.get_connection()
            sets, vals = [], []
            if new_label is not None:
                sets.append("label = %s"); vals.append(new_label)
            if source_ids is not None:
                sets.append("source_ids = %s"); vals.append(json.dumps(source_ids))
                sets.append("active_source_ids = %s"); vals.append(json.dumps(source_ids))
            if target_ids is not None:
                sets.append("target_ids = %s"); vals.append(json.dumps(target_ids))
                sets.append("active_target_ids = %s"); vals.append(json.dumps(target_ids))
            if not sets:
                return False
            sets.append("updated_at = CURRENT_TIMESTAMP")
            vals += [user_id, label]
            with conn.cursor() as cur:
                cur.execute(f"UPDATE forwarding_tasks SET {', '.join(sets)} WHERE user_id = %s AND label = %s", tuple(vals))
                updated = cur.rowcount > 0
                conn.commit()
            if updated and user_id in self._forwarding_tasks_cache:
                for task in self._forwarding_tasks_cache[user_id]:
                    if task['label'] == label:
                        if new_label is not None: task['label'] = new_label
                        if source_ids is not None:
                            task['source_ids'] = source_ids
                            task['active_source_ids'] = list(source_ids)
                        if target_ids is not None:
                            task['target_ids'] = target_ids
                            task['active_target_ids'] = list(target_ids)
                        break
                if new_label is not None:
                    pass
            return updated
        except Exception as e:
            logger.exception("Error in update_forwarding_task for %s: %s", user_id, e)
            return False

    def update_forwarding_task_active_ids(self, user_id: int, label: str,
                                          active_source_ids: Optional[List] = None,
                                          active_target_ids: Optional[List] = None) -> bool:
        """
        Persist the CONFIRMED subset of source_ids/target_ids actually used for
        forwarding, without touching the master lists. Used by the active-source
        /active-target toggle picker. Each provided list is clamped to only IDs
        that exist in the corresponding master list (defensive — the picker UI
        already guarantees this, this is a second line of defense).
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT source_ids, target_ids FROM forwarding_tasks WHERE user_id=%s AND label=%s",
                            (user_id, label))
                row = cur.fetchone()
                if not row:
                    return False
                master_src = set(row["source_ids"] or [])
                master_tgt = set(row["target_ids"] or [])
                sets, vals = [], []
                if active_source_ids is not None:
                    clamped_src = [i for i in active_source_ids if i in master_src]
                    if not clamped_src:
                        return False
                    sets.append("active_source_ids = %s"); vals.append(json.dumps(clamped_src))
                if active_target_ids is not None:
                    clamped_tgt = [i for i in active_target_ids if i in master_tgt]
                    if not clamped_tgt:
                        return False
                    sets.append("active_target_ids = %s"); vals.append(json.dumps(clamped_tgt))
                if not sets:
                    return False
                sets.append("updated_at = CURRENT_TIMESTAMP")
                vals += [user_id, label]
                cur.execute(f"UPDATE forwarding_tasks SET {', '.join(sets)} WHERE user_id = %s AND label = %s", tuple(vals))
                updated = cur.rowcount > 0
                conn.commit()
            if updated and user_id in self._forwarding_tasks_cache:
                for task in self._forwarding_tasks_cache[user_id]:
                    if task['label'] == label:
                        if active_source_ids is not None:
                            task['active_source_ids'] = [i for i in active_source_ids if i in master_src]
                        if active_target_ids is not None:
                            task['active_target_ids'] = [i for i in active_target_ids if i in master_tgt]
                        break
            return updated
        except Exception as e:
            logger.exception("Error in update_forwarding_task_active_ids for %s: %s", user_id, e)
            return False

    def update_monitoring_task(self, user_id: int, label: str,
                               new_label: Optional[str] = None,
                               chat_ids: Optional[List] = None) -> bool:
        try:
            conn = self.get_connection()
            sets, vals = [], []
            if new_label is not None:
                sets.append("label = %s"); vals.append(new_label)
            if chat_ids is not None:
                sets.append("chat_ids = %s"); vals.append(json.dumps(chat_ids))
            if not sets:
                return False
            sets.append("updated_at = CURRENT_TIMESTAMP")
            vals += [user_id, label]
            with conn.cursor() as cur:
                cur.execute(f"UPDATE monitoring_tasks SET {', '.join(sets)} WHERE user_id = %s AND label = %s", tuple(vals))
                updated = cur.rowcount > 0
                conn.commit()
            if updated and user_id in self._monitoring_tasks_cache:
                for task in self._monitoring_tasks_cache[user_id]:
                    if task['label'] == label:
                        if new_label is not None: task['label'] = new_label
                        if chat_ids is not None: task['chat_ids'] = chat_ids
                        break
            return updated
        except Exception as e:
            logger.exception("Error in update_monitoring_task for %s: %s", user_id, e)
            return False

    def get_user_forwarding_tasks(self, user_id: int) -> List[Dict]:
        if user_id in self._forwarding_tasks_cache and self._forwarding_tasks_cache[user_id]:
            return [t.copy() for t in self._forwarding_tasks_cache[user_id]]

        try:
            conn = self.get_connection()
            tasks = []

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, label, source_ids, target_ids, active_source_ids, active_target_ids,
                           filters, is_active, created_at
                    FROM forwarding_tasks
                    WHERE user_id = %s AND is_active = TRUE
                    ORDER BY created_at DESC
                """,
                    (user_id,),
                )

                for row in cur.fetchall():
                    src = row["source_ids"] if row["source_ids"] else []
                    tgt = row["target_ids"] if row["target_ids"] else []
                    tasks.append(
                        {
                            "id": row["id"],
                            "label": row["label"],
                            "source_ids": src,
                            "target_ids": tgt,
                            "active_source_ids": row["active_source_ids"] if row["active_source_ids"] else src,
                            "active_target_ids": row["active_target_ids"] if row["active_target_ids"] else tgt,
                            "filters": row["filters"] if row["filters"] else {},
                            "is_active": row["is_active"],
                            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                        }
                    )

            if tasks:
                self._forwarding_tasks_cache[user_id] = tasks

            return [t.copy() for t in tasks]
        except Exception as e:
            logger.exception("Error in get_user_forwarding_tasks for %s: %s", user_id, e)
            return []

    def get_all_active_forwarding_tasks(self) -> List[Dict]:
        try:
            conn = self.get_connection()
            tasks = []

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, id, label, source_ids, target_ids, active_source_ids, active_target_ids, filters
                    FROM forwarding_tasks
                    WHERE is_active = TRUE
                """
                )
                for row in cur.fetchall():
                    src = row["source_ids"] if row["source_ids"] else []
                    tgt = row["target_ids"] if row["target_ids"] else []
                    task = {
                        "user_id": row["user_id"],
                        "id": row["id"],
                        "label": row["label"],
                        "source_ids": src,
                        "target_ids": tgt,
                        "active_source_ids": row["active_source_ids"] if row["active_source_ids"] else src,
                        "active_target_ids": row["active_target_ids"] if row["active_target_ids"] else tgt,
                        "filters": row["filters"] if row["filters"] else {},
                    }
                    tasks.append(task)

                    uid = task["user_id"]
                    if uid not in self._forwarding_tasks_cache or not any(t['id'] == task['id'] for t in self._forwarding_tasks_cache.get(uid, [])):
                        self._forwarding_tasks_cache[uid].append({
                            "id": task["id"],
                            "label": task["label"],
                            "source_ids": task["source_ids"],
                            "target_ids": task["target_ids"],
                            "active_source_ids": task["active_source_ids"],
                            "active_target_ids": task["active_target_ids"],
                            "filters": task["filters"],
                            "is_active": 1
                        })
            return tasks
        except Exception as e:
            logger.exception("Error in get_all_active_forwarding_tasks: %s", e)
            raise

    def add_monitoring_task(self, user_id: int, label: str, chat_ids: List[int],
                           settings: Optional[Dict[str, Any]] = None) -> bool:
        try:
            conn = self.get_connection()

            if settings is None:
                settings = {
                    "check_duplicate_and_notify": True,
                    "manual_reply_system": True,
                    "auto_reply_system": False,
                    "auto_reply_message": "",
                    "outgoing_message_monitoring": True
                }

            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO monitoring_tasks (user_id, label, chat_ids, settings)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, label) DO NOTHING
                        RETURNING id
                    """, (user_id, label, json.dumps(chat_ids), json.dumps(settings)))

                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        task_id = row["id"]
                        task = {
                            'id': task_id,
                            'label': label,
                            'chat_ids': chat_ids,
                            'settings': settings,
                            'is_active': 1
                        }
                        self._monitoring_tasks_cache[user_id].append(task)
                        return True
                    return False
                except psycopg.errors.UniqueViolation:
                    return False

        except Exception as e:
            logger.exception("Error in add_monitoring_task for %s: %s", user_id, e)
            return False

    def update_task_settings(self, user_id: int, label: str, settings: Dict[str, Any]) -> bool:
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE monitoring_tasks
                    SET settings = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND label = %s
                """, (json.dumps(settings), user_id, label))
                updated = cur.rowcount > 0
                conn.commit()

            if updated and user_id in self._monitoring_tasks_cache:
                for task in self._monitoring_tasks_cache[user_id]:
                    if task['label'] == label:
                        task['settings'] = settings
                        break

            return updated
        except Exception as e:
            logger.exception("Error in update_task_settings for %s, task %s: %s", user_id, label, e)
            return False

    def remove_monitoring_task(self, user_id: int, label: str) -> bool:
        try:
            conn = self.get_connection()

            with conn.cursor() as cur:
                cur.execute("DELETE FROM monitoring_tasks WHERE user_id = %s AND label = %s", (user_id, label))
                deleted = cur.rowcount > 0
                conn.commit()

            if deleted and user_id in self._monitoring_tasks_cache:
                self._monitoring_tasks_cache[user_id] = [t for t in self._monitoring_tasks_cache[user_id] if t.get('label') != label]

            return deleted
        except Exception as e:
            logger.exception("Error in remove_monitoring_task for %s: %s", user_id, e)
            return False

    def get_user_monitoring_tasks(self, user_id: int) -> List[Dict]:
        if user_id in self._monitoring_tasks_cache and self._monitoring_tasks_cache[user_id]:
            return [t.copy() for t in self._monitoring_tasks_cache[user_id]]

        try:
            conn = self.get_connection()
            tasks = []

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, label, chat_ids, settings, is_active
                    FROM monitoring_tasks
                    WHERE user_id = %s AND is_active = TRUE
                    ORDER BY created_at ASC
                """, (user_id,))

                for row in cur.fetchall():
                    task = {
                        'id': row["id"],
                        'label': row["label"],
                        'chat_ids': row["chat_ids"] if row["chat_ids"] else [],
                        'settings': row["settings"] if row["settings"] else {},
                        'is_active': row["is_active"]
                    }
                    tasks.append(task)

            if tasks:
                self._monitoring_tasks_cache[user_id] = tasks

            return [t.copy() for t in tasks]
        except Exception as e:
            logger.exception("Error in get_user_monitoring_tasks for %s: %s", user_id, e)
            return []

    def get_all_active_monitoring_tasks(self) -> List[Dict]:
        try:
            conn = self.get_connection()
            tasks = []

            with conn.cursor() as cur:
                cur.execute("SELECT user_id, id, label, chat_ids, settings FROM monitoring_tasks WHERE is_active = TRUE")

                for row in cur.fetchall():
                    uid = row["user_id"]
                    task = {
                        'user_id': uid,
                        'id': row["id"],
                        'label': row["label"],
                        'chat_ids': row["chat_ids"] if row["chat_ids"] else [],
                        'settings': row["settings"] if row["settings"] else {}
                    }
                    tasks.append(task)

                    if uid not in self._monitoring_tasks_cache or not any(t['id'] == task['id'] for t in self._monitoring_tasks_cache.get(uid, [])):
                        self._monitoring_tasks_cache[uid].append({
                            'id': task['id'],
                            'label': task['label'],
                            'chat_ids': task['chat_ids'],
                            'settings': task['settings'],
                            'is_active': 1
                        })

            return tasks
        except Exception as e:
            logger.exception("Error in get_all_active_monitoring_tasks: %s", e)
            return []






















    def get_logged_in_users(self, limit: Optional[int] = None) -> List[Dict]:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                if limit and int(limit) > 0:
                    cur.execute(
                        "SELECT user_id, session_data FROM users WHERE is_logged_in = TRUE ORDER BY updated_at DESC LIMIT %s",
                        (int(limit),),
                    )
                else:
                    cur.execute(
                        "SELECT user_id, session_data FROM users WHERE is_logged_in = TRUE ORDER BY updated_at DESC"
                    )
                rows = cur.fetchall()
                result = []
                for r in rows:
                    result.append({"user_id": r["user_id"], "session_data": r["session_data"]})
                return result
        except Exception as e:
            logger.exception("Error fetching logged-in users: %s", e)
            raise



    def __del__(self):
        try:
            self.close_connection()
            self._thread_pool.shutdown(wait=False)
        except Exception:
            pass

db = Database()

class ClientWrapper:
    """
    Transparent proxy around TelegramClient.
    All attribute/method access is forwarded to the inner client via __getattr__.
    When a new client is needed after a storm, call wrapper.update(new_client) —
    every reference to the wrapper (forward handler, monitor handlers etc.)
    automatically uses the new inner client with zero restarts.
    """
    def __init__(self, client: TelegramClient):
        object.__setattr__(self, '_client', client)

    def update(self, new_client: TelegramClient):
        object.__setattr__(self, '_client', new_client)

    def get_inner(self) -> TelegramClient:
        return object.__getattribute__(self, '_client')

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, '_client'), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, '_client'), name, value)

    def __repr__(self):
        return f"ClientWrapper({object.__getattribute__(self, '_client')!r})"


user_clients: Dict[int, ClientWrapper] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
phone_verification_states: Dict[int, Dict] = {}
task_creation_states: Dict[int, Dict[str, Any]] = {}

forwarding_tasks_cache: Dict[int, List[Dict]] = {}
target_entity_cache: Dict[int, OrderedDict] = {}
forward_handler_registered: Dict[int, Callable] = {}
user_send_semaphores: Dict[int, asyncio.Semaphore] = {}
user_rate_limiters: Dict[int, Tuple[float, float, float]] = {}

monitoring_tasks_cache: Dict[int, List[Dict]] = {}
chat_entity_cache: Dict[int, Dict[int, Any]] = {}
monitor_handler_registered: Dict[int, List[Callable]] = {}
notification_messages: Dict[int, Dict] = {}
message_history: Dict[Tuple[int, int], deque] = {}
reply_states: Dict[int, Dict] = {}
auto_reply_states: Dict[int, Dict] = {}
duplicate_reply_counts: Dict[str, int] = {}
notification_creation_time: Dict[int, float] = {}


immediate_queue: Optional[asyncio.Queue] = None
retry_queue: Optional[asyncio.Queue] = None
flood_wait_queue: Optional[asyncio.Queue] = None
notification_queue: Optional[asyncio.Queue] = None

immediate_worker_tasks: List[asyncio.Task] = []
retry_worker_tasks: List[asyncio.Task] = []
flood_wait_worker_tasks: List[asyncio.Task] = []
monitor_worker_tasks: List[asyncio.Task] = []

_flood_wait_send_locks: Dict[int, asyncio.Lock] = {}
_flood_wait_last_send: Dict[int, float] = {}

_workers_started = False

MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None
_application_ref: Optional[Any] = None
_last_gc_run = 0


class FloodWaitManager:

    def __init__(self):
        self.user_flood_wait_until = {}
        self.start_notifications_sent = set()
        self.end_notifications_pending = set()
        self.lock = threading.Lock()

    def set_flood_wait(self, user_id: int, wait_seconds: int):
        with self.lock:
            wait_until = time.time() + wait_seconds + 2
            self.user_flood_wait_until[user_id] = wait_until

            should_notify_start = False

            if wait_seconds > 30:
                flood_wait_key = f"{user_id}_start_{wait_seconds}"
                if flood_wait_key not in self.start_notifications_sent:
                    self.start_notifications_sent.add(flood_wait_key)
                    self.end_notifications_pending.add(user_id)
                    should_notify_start = True

            return should_notify_start, wait_seconds

    def is_in_flood_wait(self, user_id: int):
        with self.lock:
            if user_id not in self.user_flood_wait_until:
                should_notify_end = user_id in self.end_notifications_pending
                if should_notify_end:
                    self.end_notifications_pending.discard(user_id)
                    self._cleanup_old_notifications(user_id)
                return False, 0, should_notify_end

            wait_until = self.user_flood_wait_until[user_id]
            current_time = time.time()

            if current_time >= wait_until:
                del self.user_flood_wait_until[user_id]
                should_notify_end = user_id in self.end_notifications_pending
                if should_notify_end:
                    self.end_notifications_pending.discard(user_id)
                    self._cleanup_old_notifications(user_id)
                return False, 0, should_notify_end

            return True, wait_until - current_time, False

    def _cleanup_old_notifications(self, user_id: int):
        current_time = time.time()
        keys_to_remove = []

        for key in self.start_notifications_sent:
            if key.startswith(f"{user_id}_"):
                keys_to_remove.append(key)

        for key in keys_to_remove:
            self.start_notifications_sent.discard(key)

    def clear_flood_wait(self, user_id: int):
        with self.lock:
            self.user_flood_wait_until.pop(user_id, None)
            self._cleanup_old_notifications(user_id)
            self.end_notifications_pending.discard(user_id)

    def restore_flood_waits(self, waits: dict):
        """Repopulate flood wait timestamps from DB on startup.
        Only entries still in the future are kept."""
        with self.lock:
            current_time = time.time()
            for uid, wait_until in waits.items():
                if wait_until > current_time:
                    self.user_flood_wait_until[int(uid)] = float(wait_until)

flood_wait_manager = FloodWaitManager()

def _clean_phone_number(text: str) -> str:
    return '+' + ''.join(c for c in text if c.isdigit())


_db_pool_lock = threading.Lock()
_db_pool_active = 0
_db_pool_peak = 0
_db_pool_calls = 0
_db_pool_wait_total = 0.0
_db_pool_wait_max = 0.0

def _db_pool_run(func, args, kwargs, submit_time):
    global _db_pool_active, _db_pool_peak, _db_pool_calls, _db_pool_wait_total, _db_pool_wait_max
    wait = time.time() - submit_time
    with _db_pool_lock:
        _db_pool_active += 1
        _db_pool_peak = max(_db_pool_peak, _db_pool_active)
        _db_pool_calls += 1
        _db_pool_wait_total += wait
        _db_pool_wait_max = max(_db_pool_wait_max, wait)
    try:
        return func(*args, **kwargs)
    finally:
        with _db_pool_lock:
            _db_pool_active -= 1

async def db_call(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    submit_time = time.time()
    return await loop.run_in_executor(db._thread_pool, functools.partial(_db_pool_run, func, args, kwargs, submit_time))

async def _db_pool_reporter():
    global _db_pool_peak, _db_pool_calls, _db_pool_wait_total, _db_pool_wait_max
    while True:
        await asyncio.sleep(120)
        with _db_pool_lock:
            calls, peak = _db_pool_calls, _db_pool_peak
            wait_total, wait_max = _db_pool_wait_total, _db_pool_wait_max
            _db_pool_calls = 0; _db_pool_peak = 0
            _db_pool_wait_total = 0.0; _db_pool_wait_max = 0.0
        if calls:
            avg_wait_ms = (wait_total / calls) * 1000
            logger.info(f"[DB-POOL] {calls} calls | peak_concurrent={peak}/50 | "
                        f"avg_wait={avg_wait_ms:.0f}ms | max_wait={wait_max*1000:.0f}ms")

async def optimized_gc():
    global _last_gc_run
    current_time = time.time()
    if current_time - _last_gc_run > GC_INTERVAL:
        try:
            if gc.get_count()[0] > gc.get_threshold()[0]:
                collected = gc.collect(2)
                if collected > 1000:
                    logger.debug(f"GC collected {collected} objects")
        except Exception:
            try:
                gc.collect()
            except Exception:
                pass
        _last_gc_run = current_time


def format_datetime_utc1(dt: datetime = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    utc_plus_one = pytz.timezone('Etc/GMT-1')
    dt_utc1 = dt.astimezone(utc_plus_one)

    month = dt_utc1.strftime('%b')
    day = str(dt_utc1.day)
    year = dt_utc1.strftime('%Y')
    hour = dt_utc1.strftime('%I').lstrip('0') or '12'
    minute = dt_utc1.strftime('%M')
    second = dt_utc1.strftime('%S')
    am_pm = dt_utc1.strftime('%p')

    return f"{month} {day}, {year} {hour}:{minute}:{second} {am_pm}"


async def _preload_user_dialogs(user_id: int, client: TelegramClient, user_name: str):
    try:
        logger.info(f"🔄 Pre-caching dialogs for user {user_id} ({user_name})...")
        count = 0
        async for _ in client.iter_dialogs():
            count += 1
        logger.info(f"✅ Pre-cached {count} dialogs for user {user_id}")
    except Exception as e:
        logger.warning(f"Failed to pre-cache dialogs for user {user_id}: {e}")

def _ensure_user_target_cache(user_id: int):
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = OrderedDict()

def _get_cached_target(user_id: int, target_id: int):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    if target_id in od:
        od.move_to_end(target_id)
        return od[target_id]
    return None

def _set_cached_target(user_id: int, target_id: int, entity: object):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    od[target_id] = entity
    od.move_to_end(target_id)
    while len(od) > TARGET_ENTITY_CACHE_SIZE:
        od.popitem(last=False)

def _ensure_user_send_semaphore(user_id: int):
    if user_id not in user_send_semaphores:
        user_send_semaphores[user_id] = asyncio.Semaphore(SEND_CONCURRENCY_PER_USER)

def _ensure_user_rate_limiter(user_id: int):
    if user_id not in user_rate_limiters:
        user_rate_limiters[user_id] = (SEND_RATE_PER_USER, time.time(), SEND_RATE_PER_USER * 5)

async def _consume_token(user_id: int, amount: float = 1.0):
    _ensure_user_rate_limiter(user_id)

    while True:
        tokens, last_refill, burst = user_rate_limiters[user_id]
        now = time.time()
        elapsed = max(0.0, now - last_refill)

        refill = elapsed * SEND_RATE_PER_USER
        tokens = min(tokens + refill, burst)

        if tokens >= amount:
            tokens -= amount
            user_rate_limiters[user_id] = (tokens, now, burst)
            return

        user_rate_limiters[user_id] = (tokens, now, burst)

        needed = amount - tokens
        wait_time = needed / SEND_RATE_PER_USER

        await asyncio.sleep(min(wait_time, 0.1))

def extract_message_text_only(message) -> str:
    if not message:
        return ""
    text = (
        getattr(message, "raw_text", None)
        or getattr(message, "message", None)
        or getattr(message, "text", None)
        or ""
    )
    return text if isinstance(text, str) else ""


def extract_words(text: str) -> List[str]:
    return WORD_PATTERN.findall(text)

def is_numeric_word(word: str) -> bool:
    return bool(NUMERIC_PATTERN.match(word))

def is_alphabetic_word(word: str) -> bool:
    return bool(ALPHABETIC_PATTERN.match(word))

def contains_numeric(word: str) -> bool:
    return any(c.isdigit() for c in word)

def contains_alphabetic(word: str) -> bool:
    return any(c.isalpha() for c in word)

def contains_special_characters(word: str) -> bool:
    for char in word:
        if not char.isalnum() and not EMOJI_PATTERN.search(char):
            return True
    return False

def apply_filters(message_text: str, task_filters: Dict) -> List[str]:
    if not message_text:
        return []

    filters_enabled = task_filters.get('filters', {})

    if filters_enabled.get('raw_text', False):
        processed = message_text
        if prefix := filters_enabled.get('prefix'):
            processed = prefix + processed
        if suffix := filters_enabled.get('suffix'):
            processed = processed + suffix
        return [processed]

    messages_to_send = []

    if filters_enabled.get('numbers_only', False):
        stripped = message_text.replace(' ', '')
        if is_numeric_word(stripped) or (contains_numeric(stripped) and not contains_alphabetic(stripped)):
            processed = message_text
            if prefix := filters_enabled.get('prefix'):
                processed = prefix + processed
            if suffix := filters_enabled.get('suffix'):
                processed = processed + suffix
            messages_to_send.append(processed)

    elif filters_enabled.get('alphabets_only', False):
        stripped = message_text.replace(' ', '')
        if is_alphabetic_word(stripped) or (contains_alphabetic(stripped) and not contains_numeric(stripped)):
            processed = message_text
            if prefix := filters_enabled.get('prefix'):
                processed = prefix + processed
            if suffix := filters_enabled.get('suffix'):
                processed = processed + suffix
            messages_to_send.append(processed)

    else:
        words = extract_words(message_text)
        for word in words:
            if not word:
                continue

            skip_word = False
            if filters_enabled.get('removed_alphabetic', False):
                if contains_numeric(word) or EMOJI_PATTERN.search(word):
                    skip_word = True

            elif filters_enabled.get('removed_numeric', False):
                if contains_alphabetic(word) or EMOJI_PATTERN.search(word):
                    skip_word = True

            if not skip_word:
                processed = word
                if prefix := filters_enabled.get('prefix'):
                    processed = prefix + processed
                if suffix := filters_enabled.get('suffix'):
                    processed = processed + suffix
                messages_to_send.append(processed)

    return messages_to_send

def create_message_hash(message_text: str, sender_id: Optional[int] = None) -> str:
    if sender_id:
        content = f"{sender_id}:{message_text.strip().lower()}"
    else:
        content = message_text.strip().lower()
    return hashlib.md5(content.encode()).hexdigest()[:12]

def is_duplicate_message(user_id: int, chat_id: int, message_hash: str) -> bool:
    key = (user_id, chat_id)
    if key not in message_history:
        return False

    current_time = time.time()
    dq = message_history[key]

    while dq and current_time - dq[0][1] > DUPLICATE_CHECK_WINDOW:
        dq.popleft()

    return any(stored_hash == message_hash for stored_hash, _, _ in dq)

def store_message_hash(user_id: int, chat_id: int, message_hash: str, message_text: str):
    key = (user_id, chat_id)
    if key not in message_history:
        message_history[key] = deque(maxlen=MESSAGE_HASH_LIMIT)

    message_history[key].append((message_hash, time.time(), message_text[:80]))

async def cleanup_old_notifications():
    while True:
        try:
            await asyncio.sleep(600)

            current_time = time.time()
            keys_to_remove = []

            for msg_id, creation_time in list(notification_creation_time.items()):
                if current_time - creation_time > 3600:
                    keys_to_remove.append(msg_id)

            for msg_id in keys_to_remove:
                notification_messages.pop(msg_id, None)
                notification_creation_time.pop(msg_id, None)
                asyncio.create_task(db_call(db.delete_notif_state, msg_id))

            if keys_to_remove:
                logger.debug(f"Cleaned up {len(keys_to_remove)} old notification messages")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Error in cleanup_old_notifications: {e}")
            await asyncio.sleep(60)

async def cleanup_old_messages_task():
    """Cleanup messages older than 1 hour 15 minutes (4500 seconds) every 10 minutes (600 seconds)."""
    while True:
        try:
            await asyncio.sleep(600)
            current_time = time.time()
            cutoff_time = current_time - 4500

            keys_to_delete = []
            for key, messages_deque in list(message_history.items()):
                while messages_deque and messages_deque[0][1] < cutoff_time:
                    messages_deque.popleft()

                if not messages_deque:
                    keys_to_delete.append(key)

            for key in keys_to_delete:
                if key in message_history:
                    del message_history[key]

            logger.debug(f"Cleaned up old messages, {len(keys_to_delete)} empty keys removed.")
            asyncio.create_task(db_call(db.cleanup_old_msg_hashes, current_time - DUPLICATE_CHECK_WINDOW))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Error in cleanup_old_messages_task: {e}")

async def cleanup_old_reply_counts():
    while True:
        try:
            await asyncio.sleep(3600)

            keys_to_remove = []
            active_duplicate_keys = set()

            for msg_data in notification_messages.values():
                if "duplicate_key" in msg_data:
                    active_duplicate_keys.add(msg_data["duplicate_key"])

            for key in list(duplicate_reply_counts.keys()):
                if key not in active_duplicate_keys:
                    still_active = False
                    for msg_data in notification_messages.values():
                        if msg_data.get("duplicate_key") == key:
                            still_active = True
                            break

                    if not still_active:
                        keys_to_remove.append(key)

            for key in keys_to_remove:
                duplicate_reply_counts.pop(key, None)
                asyncio.create_task(db_call(db.delete_dup_count, key))

            if keys_to_remove:
                logger.debug(f"Cleaned up {len(keys_to_remove)} old reply count entries")

        except Exception as e:
            logger.exception(f"Error in cleanup_old_reply_counts: {e}")


async def check_phone_number_required(user_id: int) -> bool:
    user = await db_call(db.get_user, user_id)
    return bool(user and user.get("is_logged_in") and not user.get("phone"))

async def ask_for_phone_number(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    phone_verification_states[user_id] = {
        "step": "waiting_phone",
        "chat_id": chat_id
    }

    message = """📱 **Phone Number Verification Required**

Your account was restored from a saved session, but we need your phone number for security.

⚠️ **Important:**
• This is the phone number associated with your Telegram account
• It will only be used for logout confirmation
• Your phone number is stored securely

**Please enter your phone number (with country code):**

**Examples:**
• `+1234567890`
• `+447911123456`
• `+4915112345678`

**Type your phone number now:**"""

    try:
        await context.bot.send_message(
            chat_id,
            message,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_phone_verification")]]),
            parse_mode="Markdown"
        )
    except Exception:
        logger.exception("Failed to send phone verification message")

async def handle_phone_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id not in phone_verification_states:
        return

    state = phone_verification_states[user_id]
    text = update.message.text.strip()

    if state["step"] == "waiting_phone":
        if not text.startswith('+'):
            await update.message.reply_text(
                "❌ **Invalid format!**\n\nPhone number must start with `+`\nExample: `+1234567890`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_phone_verification")]])
            )
            return

        clean_phone = _clean_phone_number(text)

        if len(clean_phone) < 8:
            await update.message.reply_text(
                "❌ **Invalid phone number!**\n\nPhone number seems too short.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_phone_verification")]])
            )
            return

        client = user_clients.get(user_id)
        if client:
            try:
                me = await client.get_me()
                await db_call(db.save_user, user_id, clean_phone, me.first_name,
                             None, True)

                del phone_verification_states[user_id]

                await update.message.reply_text(
                    f"✅ **Phone number verified!**\n\n📱 **Phone:** `{clean_phone}`\n👤 **Name:** {me.first_name or 'User'}\n\nYour account is now fully restored! 🎉",
                    parse_mode="Markdown",
                )

                await show_main_menu(update, context, user_id)

            except Exception:
                logger.exception("Error verifying phone")
                await update.message.reply_text("❌ Error verifying phone number!")
        else:
            await update.message.reply_text("❌ Session not found!")
            del phone_verification_states[user_id]

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    user = await db_call(db.get_user, user_id)

    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"
    current_time = format_datetime_utc1()

    message_text = f"""╔════════════════════════════════════╗
║   📨🔍 FORWARDER + DUODETECTIVE BOT   ║
║   Telegram Message Management System   ║
╚════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}
🕐 **Time:** {current_time}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  /login - Connect your Telegram account
  /logout - Disconnect your account

📨 **Forwarding System:**
  /forwadd - Create a new forwarding task
  /fortasks - List all your forwarding tasks

🔍 **Monitoring System:**
  /monitoradd - Create a new monitoring task
  /monitortasks - List all your monitoring tasks

🆔 **Utilities:**
  /getallid - Get all your chat IDs"""

    message_text += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⚙️ **Features:**\n1. Forward messages between chats with filters\n2. Detect duplicate messages in monitored chats\n3. Get notifications and reply to duplicates\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📨 Forwarding Tasks", callback_data="show_forward_tasks")])
        keyboard.append([InlineKeyboardButton("🔍 Monitoring Tasks", callback_data="show_monitor_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    if update.callback_query:
        await update.callback_query.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )

async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Allow-list gate. Open to everyone unless ALLOWED_USER_IDS is configured."""
    user = update.effective_user
    if user is None:
        return False
    if is_user_allowed(user.id):
        return True

    msg = f"🚫 **Access Denied!**\n\nThis bot is private.\n\nYour ID: `{user.id}`"
    try:
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(msg, parse_mode="Markdown")
        elif update.message:
            await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception:
        pass
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    await show_main_menu(update, context, user_id)

async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\nToo many users are currently connected. Please try again later.",
            parse_mode="Markdown",
        )
        return

    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in") and user.get("session_data"):
        if user_id in user_clients:
            await message.reply_text(
                "✅ **You are already logged in!**\n\n"
                f"📱 Phone: `{user['phone'] or 'Not set'}`\n"
                f"👤 Name: `{user['name'] or 'User'}`\n\n"
                "Use /logout if you want to disconnect.",
                parse_mode="Markdown",
            )
            return
        else:
            logger.warning(f"User {user_id} has session in DB but not in memory, cleaning up")
            try:
                await db_call(db.purge_user_data, user_id)
            except Exception:
                pass

    client = TelegramClient(StringSession(), API_ID, API_HASH)

    try:
        await client.connect()
    except Exception as e:
        logger.error(f"Telethon connection failed: {e}")
        await message.reply_text(
            f"❌ **Connection failed:** {str(e)}\n\nPlease try again in a few minutes.",
            parse_mode="Markdown",
        )
        return

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Login Process**\n\n1️⃣ **Enter your phone number** (with country code):\n\n**Examples:**\n• `+1234567890`\n• `+447911123456`\n• `+4915112345678`\n\n⚠️ **Important:**\n• Include the `+` sign\n• Use international format\n• No spaces or dashes\n\n**Type your phone number now:**",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
        parse_mode="Markdown",
    )

async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return

    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if any(key.startswith("waiting_auto_reply_") for key in context.user_data.keys()):
        await handle_auto_reply_message(update, context)
        return

    if update.message.reply_to_message:
        await handle_notification_reply(update, context)
        return

    if user_id not in login_states:
        return

    state = login_states[user_id]
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            if not text.startswith('+'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPhone number must start with `+`\nExample: `+1234567890`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            clean_phone = _clean_phone_number(text)

            if len(clean_phone) < 8:
                await update.message.reply_text(
                    "❌ **Invalid phone number!**\n\nPhone number seems too short.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            processing_msg = await update.message.reply_text(
                "⏳ **Sending verification code...**\n\nThis may take a few seconds.",
                parse_mode="Markdown",
            )

            try:
                result = await client.send_code_request(clean_phone)

                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"

                await processing_msg.edit_text(
                    f"✅ **Verification code sent!**\n\n📱 **Code sent to:** `{clean_phone}`\n\n2️⃣ **Enter the verification code:**\n\n**Format:** `verify12345`\n• Type `verify` followed by your 5-digit code\n• No spaces, no brackets\n\n**Example:** If your code is `54321`, type:\n`verify54321`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error sending code for user {user_id}: {error_msg}")

                if "PHONE_NUMBER_INVALID" in error_msg:
                    error_text = "❌ **Invalid phone number!**"
                elif "PHONE_NUMBER_BANNED" in error_msg:
                    error_text = "❌ **Phone number banned!**"
                elif "FLOOD" in error_msg or "Too many" in error_msg:
                    error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease start over."
                else:
                    error_text = f"❌ **Error:** {error_msg}"

                await processing_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

                try:
                    await client.disconnect()
                except:
                    pass

                if user_id in login_states:
                    del login_states[user_id]
                return

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `verify12345`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            code = text[6:]

            if not code or not code.isdigit() or len(code) != 5:
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\nCode must be 5 digits.\n**Example:** `verify12345`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying code...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = ClientWrapper(client)
                forwarding_tasks_cache.setdefault(user_id, [])
                monitoring_tasks_cache.setdefault(user_id, [])

                asyncio.create_task(_preload_user_dialogs(user_id, client, me.first_name or "User"))

                _ensure_user_target_cache(user_id)
                chat_entity_cache.setdefault(user_id, {})
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)

                await start_forwarding_for_user(user_id)
                await start_monitoring_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\n**Now you can:**\n• Create forwarding tasks with /forwadd\n• Create monitoring tasks with /monitoradd\n• View tasks with /fortasks and /monitortasks\n• Get chat IDs with /getallid",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2-Step Verification Required**\n\n3️⃣ **Enter your 2FA password:**\n\n**Format:** `passwordYourPassword123`\n• Type `password` followed by your 2FA password\n• No spaces, no brackets\n\n**Example:** If your password is `mypass123`, type:\n`passwordmypass123`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]]),
                    parse_mode="Markdown",
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying code for user {user_id}: {error_msg}")

                if "PHONE_CODE_INVALID" in error_msg:
                    error_text = "❌ **Invalid code!**"
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**"
                else:
                    error_text = f"❌ **Verification failed:** {error_msg}"

                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `passwordYourPassword123`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_login")]])
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA password...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(password=password)

                me = await client.get_me()
                session_string = client.session.save()

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = ClientWrapper(client)
                forwarding_tasks_cache.setdefault(user_id, [])
                monitoring_tasks_cache.setdefault(user_id, [])

                asyncio.create_task(_preload_user_dialogs(user_id, client, me.first_name or "User"))

                _ensure_user_target_cache(user_id)
                chat_entity_cache.setdefault(user_id, {})
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)

                await start_forwarding_for_user(user_id)
                await start_monitoring_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected with 2FA!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\nYour account is now securely connected! 🔐",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")

                if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                    error_text = "❌ **Invalid 2FA password!**"
                else:
                    error_text = f"❌ **2FA verification failed:** {error_msg}"

                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

    except Exception as e:
        logger.exception("Unexpected error during login")
        await update.message.reply_text(
            f"❌ **Unexpected error:** {str(e)[:100]}\n\nPlease try /login again.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                pass
            del login_states[user_id]

async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\nUse /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        f"⚠️ **Confirm Logout**\n\n📱 **Enter your phone number to confirm disconnection:**\n\nYour connected phone: `{user['phone']}`\n\nType your phone number exactly to confirm logout.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_logout")]]),
        parse_mode="Markdown",
    )

async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            f"❌ **Phone number doesn't match!**\n\nExpected: `{stored_phone}`\nYou entered: `{text}`",
            parse_mode="Markdown",
        )
        return True

    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            if user_id in forward_handler_registered:
                handler = forward_handler_registered.get(user_id)
                if handler:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                forward_handler_registered.pop(user_id, None)

            if user_id in monitor_handler_registered:
                for handler in monitor_handler_registered[user_id]:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                monitor_handler_registered.pop(user_id, None)

            await client.disconnect()
        except Exception:
            pass
        finally:
            user_clients.pop(user_id, None)

    try:
        success = await db_call(db.purge_user_data, user_id)
        if not success:
            logger.error(f"Failed to purge user data for {user_id}")
    except Exception as e:
        logger.exception(f"Error purging user data for {user_id}: {e}")

    phone_verification_states.pop(user_id, None)
    forwarding_tasks_cache.pop(user_id, None)
    monitoring_tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    chat_entity_cache.pop(user_id, None)
    user_send_semaphores.pop(user_id, None)
    _flood_wait_send_locks.pop(user_id, None)
    _flood_wait_last_send.pop(user_id, None)
    user_rate_limiters.pop(user_id, None)
    logout_states.pop(user_id, None)
    reply_states.pop(user_id, None)
    auto_reply_states.pop(user_id, None)


    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n✅ All your sessions, tasks, and data have been cleared.\n🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True

async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect.",
            parse_mode="Markdown"
        )
        return

    task_creation_states[user_id] = {
        "step": "waiting_name",
        "type": "forwarding",
        "name": "",
        "source_ids": [],
        "target_ids": []
    }

    await update.message.reply_text(
        "🎯 **Let's create a new forwarding task!**\n\n📝 **Step 1 of 3:** Please enter a name for your task.\n\n💡 *Example: My Forwarding Task*",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]]),
        parse_mode="Markdown"
    )

async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message
    tasks = forwarding_tasks_cache.get(user_id, [])

    if not tasks:
        await message.reply_text(
            "📋 **No Active Forwarding Tasks**\n\nYou don't have any forwarding tasks yet.\n\nCreate one with:\n/forwadd",
            parse_mode="Markdown"
        )
        return

    page = context.user_data.get('forward_page', 0)
    items_per_page = 10
    total_pages = max(1, (len(tasks) + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))

    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(tasks))
    page_tasks = tasks[start_idx:end_idx]

    task_list = f"📋 **Your Forwarding Tasks**"
    if total_pages > 1:
        task_list += f" (Page {page + 1}/{total_pages})"
    task_list += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard = []

    for i, task in enumerate(page_tasks, start_idx + 1):
        task_list += f"{i}. **{task['label']}**\n   📥 Sources: {', '.join(map(str, task['source_ids']))}\n   📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"forward_task_{task['label']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**\n\n💡 **Tap any task below to manage it!**"

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data="forward_page_prev"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="forward_page_next"))
    if nav_row:
        keyboard.append(nav_row)

    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_forward_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("forward_task_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)

    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    filters = task.get("filters", {})

    outgoing_emoji = "✅" if filters.get("outgoing", True) else "❌"
    forward_tag_emoji = "✅" if filters.get("forward_tag", False) else "❌"
    control_emoji = "✅" if filters.get("control", True) else "❌"

    active_src = task.get("active_source_ids") or task["source_ids"]
    active_tgt = task.get("active_target_ids") or task["target_ids"]
    message_text = f"🔧 **Forward Task Management: {task_label}**\n\n📥 **Sources ({len(active_src)}/{len(task['source_ids'])} active):** {', '.join(map(str, task['source_ids']))}\n📤 **Targets ({len(active_tgt)}/{len(task['target_ids'])} active):** {', '.join(map(str, task['target_ids']))}\n\n⚙️ **Settings:**\n{outgoing_emoji} Outgoing - Controls if outgoing messages are forwarded\n{forward_tag_emoji} Forward Tag - Shows/hides 'Forwarded from' tag\n{control_emoji} Control - Pauses/runs forwarding\n\n💡 **Tap any option below to change it!**"

    keyboard = [
        [InlineKeyboardButton("🔍 Filters", callback_data=f"forward_filter_{task_label}")],
        [
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_forward_{task_label}_outgoing"),
            InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"toggle_forward_{task_label}_forward_tag")
        ],
        [
            InlineKeyboardButton(f"{control_emoji} Control", callback_data=f"toggle_forward_{task_label}_control"),
            InlineKeyboardButton("✏️ Edit", callback_data=f"forward_edit_{task_label}")
        ],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_forward_{task_label}")],
        [InlineKeyboardButton("🔙 Back to Forward Tasks", callback_data="show_forward_tasks")]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_forward_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("forward_edit_", "")
    for k in [f"fw_edit_name_{task_label}", f"fw_edit_src_{task_label}", f"fw_edit_tgt_{task_label}"]:
        context.user_data.pop(k, None)

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    keyboard = [
        [InlineKeyboardButton("✏️ Edit Name",    callback_data=f"fw_editname_{task_label}")],
        [InlineKeyboardButton("📥 Edit Sources", callback_data=f"fw_editsrc_{task_label}"),
         InlineKeyboardButton("📤 Edit Targets", callback_data=f"fw_edittgt_{task_label}")],
        [InlineKeyboardButton("🎯 Active Sources", callback_data=f"fw_actsrc_{task_label}"),
         InlineKeyboardButton("🎯 Active Targets", callback_data=f"fw_acttgt_{task_label}")],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"forward_task_{task_label}")]
    ]
    active_src = task.get("active_source_ids") or task["source_ids"]
    active_tgt = task.get("active_target_ids") or task["target_ids"]
    await query.edit_message_text(
        f"✏️ *Edit Task: {task_label}*\n\n"
        f"📥 Sources ({len(active_src)}/{len(task['source_ids'])} active): {', '.join(map(str, task['source_ids']))}\n"
        f"📤 Targets ({len(active_tgt)}/{len(task['target_ids'])} active): {', '.join(map(str, task['target_ids']))}\n\n"
        "Choose what to edit:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def _fw_render_active_picker(query, context, user_id: int, task_label: str, field: str):
    """
    Render the instant-toggle picker for which sources/targets are actually
    active on a forwarding task. `field` is "source" or "target". Each tap
    persists immediately (🟢 active / 🔴 inactive) — no separate confirm step.
    Toggling off the last remaining active one is rejected in the handler.
    """
    user_tasks = forwarding_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    master_list = task["source_ids"] if field == "source" else task["target_ids"]
    active_key  = "active_source_ids" if field == "source" else "active_target_ids"
    active_now  = set(task.get(active_key) or master_list)

    toggle_prefix = "fw_asrctog_" if field == "source" else "fw_atgttog_"
    label_word    = "Source" if field == "source" else "Target"
    emoji         = "📥" if field == "source" else "📤"

    kb = [[InlineKeyboardButton(f"{'🟢' if cid in active_now else '🔴'} {cid}",
                                 callback_data=f"{toggle_prefix}{idx}_{task_label}")]
          for idx, cid in enumerate(master_list)]
    kb.append([InlineKeyboardButton("« Back", callback_data=f"forward_edit_{task_label}")])

    try:
        await query.edit_message_text(
            f"{emoji} *Active {label_word}s — {escape_markdown(task_label, version=2)}*\n\n"
            f"Tap to turn a {field} on 🟢 or off 🔴 for forwarding — changes save instantly "
            f"\\({len(active_now)}/{len(master_list)} active\\)\\. At least one must stay active\\.",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise

async def handle_forward_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("forward_filter_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)

    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})

    raw_text_emoji = "✅" if filter_settings.get("raw_text", False) else "❌"
    numbers_only_emoji = "✅" if filter_settings.get("numbers_only", False) else "❌"
    alphabets_only_emoji = "✅" if filter_settings.get("alphabets_only", False) else "❌"
    removed_alphabetic_emoji = "✅" if filter_settings.get("removed_alphabetic", False) else "❌"
    removed_numeric_emoji = "✅" if filter_settings.get("removed_numeric", False) else "❌"

    prefix = filter_settings.get("prefix", "")
    suffix = filter_settings.get("suffix", "")
    prefix_text = f"'{prefix}'" if prefix else "Not set"
    suffix_text = f"'{suffix}'" if suffix else "Not set"

    message_text = f"🔍 **Filters for: {task_label}**\n\nApply filters to messages before forwarding:\n\n📋 **Available Filters:**\n{raw_text_emoji} Raw text - Forward any text\n{numbers_only_emoji} Numbers only - Forward only numbers\n{alphabets_only_emoji} Alphabets only - Forward only letters\n{removed_alphabetic_emoji} Removed Alphabetic - Keep letters & special chars, remove numbers & emojis\n{removed_numeric_emoji} Removed Numeric - Keep numbers & special chars, remove letters & emojis\n📝 **Prefix:** {prefix_text}\n📝 **Suffix:** {suffix_text}\n\n💡 **Multiple filters can be active at once!**"

    keyboard = [
        [
            InlineKeyboardButton(f"{raw_text_emoji} Raw text", callback_data=f"toggle_forward_{task_label}_raw_text"),
            InlineKeyboardButton(f"{numbers_only_emoji} Numbers only", callback_data=f"toggle_forward_{task_label}_numbers_only")
        ],
        [
            InlineKeyboardButton(f"{alphabets_only_emoji} Alphabets only", callback_data=f"toggle_forward_{task_label}_alphabets_only"),
            InlineKeyboardButton(f"{removed_alphabetic_emoji} Removed Alphabetic", callback_data=f"toggle_forward_{task_label}_removed_alphabetic")
        ],
        [
            InlineKeyboardButton(f"{removed_numeric_emoji} Removed Numeric", callback_data=f"toggle_forward_{task_label}_removed_numeric"),
            InlineKeyboardButton("📝 Prefix/Suffix", callback_data=f"prefix_suffix_{task_label}")
        ],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"forward_task_{task_label}")]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_toggle_forward_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    remainder = query.data.replace("toggle_forward_", "")

    _FORWARD_TOGGLE_SUFFIXES = [
        "removed_alphabetic", "removed_numeric", "numbers_only",
        "alphabets_only", "raw_text", "forward_tag", "outgoing", "control",
        "clear_prefix_suffix",
    ]
    task_label = None
    toggle_type = None
    for suffix in _FORWARD_TOGGLE_SUFFIXES:
        if remainder.endswith("_" + suffix):
            toggle_type = suffix
            task_label = remainder[: -(len(suffix) + 1)]
            break

    if task_label is None or toggle_type is None:
        await query.answer("Invalid action!", show_alert=True)
        return

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    task_index = next((i for i, t in enumerate(user_tasks) if t["label"] == task_label), -1)

    if task_index == -1:
        await query.answer("Task not found!", show_alert=True)
        return

    task = user_tasks[task_index]
    filters = task.get("filters", {})
    new_state = None

    if toggle_type in ["outgoing", "forward_tag", "control"]:
        if toggle_type == "outgoing":
            new_state = not filters.get("outgoing", True)
            filters["outgoing"] = new_state
        elif toggle_type == "forward_tag":
            new_state = not filters.get("forward_tag", False)
            filters["forward_tag"] = new_state
        elif toggle_type == "control":
            new_state = not filters.get("control", True)
            filters["control"] = new_state

    elif toggle_type in ["raw_text", "numbers_only", "alphabets_only", "removed_alphabetic", "removed_numeric"]:
        filter_settings = filters.get("filters", {})
        new_state = not filter_settings.get(toggle_type, False)
        filter_settings[toggle_type] = new_state
        filters["filters"] = filter_settings

    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return

    if new_state is not None:
        task["filters"] = filters
        forwarding_tasks_cache[user_id][task_index] = task

    asyncio.create_task(db_call(db.update_task_filters, user_id, task_label, filters))

    if toggle_type in ["outgoing", "forward_tag", "control"]:
        user_tasks = forwarding_tasks_cache.get(user_id, [])
        task = next((t for t in user_tasks if t["label"] == task_label), None)

        if task:
            filters = task.get("filters", {})
            outgoing_emoji = "✅" if filters.get("outgoing", True) else "❌"
            forward_tag_emoji = "✅" if filters.get("forward_tag", False) else "❌"
            control_emoji = "✅" if filters.get("control", True) else "❌"

            active_src = task.get("active_source_ids") or task["source_ids"]
            active_tgt = task.get("active_target_ids") or task["target_ids"]
            message_text = f"🔧 **Forward Task Management: {task_label}**\n\n📥 **Sources ({len(active_src)}/{len(task['source_ids'])} active):** {', '.join(map(str, task['source_ids']))}\n📤 **Targets ({len(active_tgt)}/{len(task['target_ids'])} active):** {', '.join(map(str, task['target_ids']))}\n\n⚙️ **Settings:**\n{outgoing_emoji} Outgoing - Controls if outgoing messages are forwarded\n{forward_tag_emoji} Forward Tag - Shows/hides 'Forwarded from' tag\n{control_emoji} Control - Pauses/runs forwarding\n\n💡 **Tap any option below to change it!**"

            keyboard = [
                [InlineKeyboardButton("🔍 Filters", callback_data=f"forward_filter_{task_label}")],
                [
                    InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_forward_{task_label}_outgoing"),
                    InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"toggle_forward_{task_label}_forward_tag")
                ],
                [
                    InlineKeyboardButton(f"{control_emoji} Control", callback_data=f"toggle_forward_{task_label}_control"),
                    InlineKeyboardButton("✏️ Edit", callback_data=f"forward_edit_{task_label}")
                ],
                [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_forward_{task_label}")],
                [InlineKeyboardButton("🔙 Back to Forward Tasks", callback_data="show_forward_tasks")]
            ]

            try:
                await query.edit_message_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to update message: {e}")
    else:
        user_tasks = forwarding_tasks_cache.get(user_id, [])
        task = next((t for t in user_tasks if t["label"] == task_label), None)

        if task:
            filters = task.get("filters", {})
            filter_settings = filters.get("filters", {})

            raw_text_emoji = "✅" if filter_settings.get("raw_text", False) else "❌"
            numbers_only_emoji = "✅" if filter_settings.get("numbers_only", False) else "❌"
            alphabets_only_emoji = "✅" if filter_settings.get("alphabets_only", False) else "❌"
            removed_alphabetic_emoji = "✅" if filter_settings.get("removed_alphabetic", False) else "❌"
            removed_numeric_emoji = "✅" if filter_settings.get("removed_numeric", False) else "❌"

            prefix = filter_settings.get("prefix", "")
            suffix = filter_settings.get("suffix", "")
            prefix_text = f"'{prefix}'" if prefix else "Not set"
            suffix_text = f"'{suffix}'" if suffix else "Not set"

            message_text = f"🔍 **Filters for: {task_label}**\n\nApply filters to messages before forwarding:\n\n📋 **Available Filters:**\n{raw_text_emoji} Raw text - Forward any text\n{numbers_only_emoji} Numbers only - Forward only numbers\n{alphabets_only_emoji} Alphabets only - Forward only letters\n{removed_alphabetic_emoji} Removed Alphabetic - Keep letters & special chars, remove numbers & emojis\n{removed_numeric_emoji} Removed Numeric - Keep numbers & special chars, remove letters & emojis\n📝 **Prefix:** {prefix_text}\n📝 **Suffix:** {suffix_text}\n\n💡 **Multiple filters can be active at once!**"

            keyboard = [
                [
                    InlineKeyboardButton(f"{raw_text_emoji} Raw text", callback_data=f"toggle_forward_{task_label}_raw_text"),
                    InlineKeyboardButton(f"{numbers_only_emoji} Numbers only", callback_data=f"toggle_forward_{task_label}_numbers_only")
                ],
                [
                    InlineKeyboardButton(f"{alphabets_only_emoji} Alphabets only", callback_data=f"toggle_forward_{task_label}_alphabets_only"),
                    InlineKeyboardButton(f"{removed_alphabetic_emoji} Removed Alphabetic", callback_data=f"toggle_forward_{task_label}_removed_alphabetic")
                ],
                [
                    InlineKeyboardButton(f"{removed_numeric_emoji} Removed Numeric", callback_data=f"toggle_forward_{task_label}_removed_numeric"),
                    InlineKeyboardButton("📝 Prefix/Suffix", callback_data=f"prefix_suffix_{task_label}")
                ],
                [InlineKeyboardButton("🔙 Back to Task", callback_data=f"forward_task_{task_label}")]
            ]

            try:
                await query.edit_message_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to update message: {e}")

async def handle_delete_forward_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_forward_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    message_text = f"🗑️ **Delete Forwarding Task: {task_label}**\n\n⚠️ **Are you sure you want to delete this task?**\n\nThis action cannot be undone!\nAll forwarding will stop immediately."

    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_forward_{task_label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"forward_task_{task_label}")
        ]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_confirm_delete_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_forward_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    deleted = await db_call(db.remove_forwarding_task, user_id, task_label)

    if deleted:
        if user_id in forwarding_tasks_cache:
            forwarding_tasks_cache[user_id] = [t for t in forwarding_tasks_cache[user_id] if t.get("label") != task_label]

        if user_id in user_clients:
            await update_forwarding_for_user(user_id)

        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\nAll forwarding for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )

def ensure_forward_handler_registered_for_user(user_id: int, client: TelegramClient):
    if forward_handler_registered.get(user_id):
        return

    async def _forward_message_handler(event):
        try:
            await optimized_gc()

            is_edit = isinstance(event, events.MessageEdited)
            message = getattr(event, "message", None)
            if not message:
                return

            message_text = extract_message_text_only(message)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return

            user_tasks = forwarding_tasks_cache.get(user_id)
            if not user_tasks:
                return

            message_outgoing = getattr(message, "out", False)

            message_topic_id = _get_message_topic_id(message)

            for task in user_tasks:
                if not task.get("filters", {}).get("control", True):
                    continue

                if message_outgoing and not task.get("filters", {}).get("outgoing", True):
                    continue

                if any(_source_id_matches(chat_id, message_topic_id, sid)
                       for sid in (task.get("active_source_ids") or task.get("source_ids", []))):
                    forward_tag = task.get("filters", {}).get("forward_tag", False)
                    filtered_messages = apply_filters(message_text, task.get("filters", {}))

                    for filtered_msg in filtered_messages:
                        for target_id in (task.get("active_target_ids") or task.get("target_ids", [])):
                            queued = False
                            if immediate_queue is not None:
                                try:
                                    immediate_queue.put_nowait((
                                        user_id, target_id, filtered_msg,
                                        task.get("filters", {}), forward_tag,
                                        chat_id if forward_tag else None,
                                        message.id if forward_tag else None
                                    ))
                                    queued = True
                                except asyncio.QueueFull:
                                    pass

                            if not queued:
                                await forward_message_direct(
                                    user_id, target_id, filtered_msg,
                                    task.get("filters", {}), forward_tag,
                                    chat_id if forward_tag else None,
                                    message.id if forward_tag else None
                                )

        except Exception as e:
            logger.exception(f"Error in forward message handler for user {user_id}: {e}")

    try:
        client.add_event_handler(_forward_message_handler, events.NewMessage())
        client.add_event_handler(_forward_message_handler, events.MessageEdited())
        forward_handler_registered[user_id] = _forward_message_handler
        logger.info(f"✅ Registered forward handler for user {user_id}")
    except Exception as e:
        logger.exception(f"Failed to add forward event handler for user {user_id}: {e}")

async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id):
    cache_key = str(target_id)
    ent = _get_cached_target(user_id, cache_key)
    if ent:
        return ent

    chat_id_only, _ = parse_chat_id(target_id)
    try:
        entity = await client.get_input_entity(chat_id_only)
        _set_cached_target(user_id, cache_key, entity)
        return entity
    except Exception as e:
        logger.debug(f"Failed to resolve entity {target_id} for user {user_id}: {e}")
        return None

async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids:
        for attempt in range(2):
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)

async def start_forwarding_for_user(user_id: int):
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    forwarding_tasks_cache.setdefault(user_id, [])
    _ensure_user_target_cache(user_id)
    _ensure_user_send_semaphore(user_id)
    _ensure_user_rate_limiter(user_id)

    ensure_forward_handler_registered_for_user(user_id, client)

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    if user_tasks:
        all_targets = []
        for task in user_tasks:
            all_targets.extend(task.get("active_target_ids") or task.get("target_ids", []))

        if all_targets:
            unique_targets = list(set(all_targets))
            asyncio.create_task(resolve_targets_for_user(user_id, unique_targets))

async def update_forwarding_for_user(user_id: int):
    if user_id not in user_clients:
        logger.warning(f"Cannot update forwarding for user {user_id}: no client")
        return

    client = user_clients[user_id]

    if user_id in forward_handler_registered:
        handler = forward_handler_registered.get(user_id)
        if handler:
            try:
                client.remove_event_handler(handler)
            except Exception:
                pass

    forward_handler_registered.pop(user_id, None)
    ensure_forward_handler_registered_for_user(user_id, client)

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    if user_tasks:
        all_targets = []
        for task in user_tasks:
            all_targets.extend(task.get("active_target_ids") or task.get("target_ids", []))

        if all_targets:
            unique_targets = list(set(all_targets))
            asyncio.create_task(resolve_targets_for_user(user_id, unique_targets))

    logger.info(f"✅ Updated forwarding for user {user_id}")

async def reinitialize_forwarding_for_user(user_id: int):
    if user_id not in user_clients:
        logger.warning(f"Cannot reinitialize forwarding for user {user_id}: no client")
        return

    client = user_clients[user_id]

    if user_id in forward_handler_registered:
        handler = forward_handler_registered.get(user_id)
        if handler:
            try:
                client.remove_event_handler(handler)
            except Exception:
                pass
        forward_handler_registered.pop(user_id, None)

    _ensure_user_target_cache(user_id)
    _ensure_user_send_semaphore(user_id)
    _ensure_user_rate_limiter(user_id)

    ensure_forward_handler_registered_for_user(user_id, client)

    user_tasks = forwarding_tasks_cache.get(user_id, [])
    if user_tasks:
        all_targets = []
        for task in user_tasks:
            all_targets.extend(task.get("active_target_ids") or task.get("target_ids", []))

        if all_targets:
            unique_targets = list(set(all_targets))
            logger.info(f"Resolving {len(unique_targets)} targets for user {user_id}")
            asyncio.create_task(resolve_targets_for_user(user_id, unique_targets))

    logger.info(f"✅ Reinitialized forwarding for user {user_id} with {len(user_tasks)} tasks")

async def monitoradd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user.get("is_logged_in"):
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect your Telegram account.",
            parse_mode="Markdown"
        )
        return

    task_creation_states[user_id] = {
        "step": "waiting_name",
        "type": "monitoring",
        "name": "",
        "chat_ids": []
    }

    await update.message.reply_text(
        "🎯 **Let's create a new monitoring task!**\n\n"
        "📝 **Step 1 of 2:** Please enter a name for your monitoring task.\n\n"
        "💡 *Example: Group Duplicate Checker*",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]]),
        parse_mode="Markdown"
    )

async def monitortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        user_id = update.effective_user.id
        message = update.message
    else:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    tasks = monitoring_tasks_cache.get(user_id, [])

    if not tasks:
        await message.reply_text(
            "📋 **No Active Monitoring Tasks**\n\n"
            "You don't have any monitoring tasks yet.\n\n"
            "Create one with:\n"
            "/monitoradd",
            parse_mode="Markdown"
        )
        return

    page = context.user_data.get('monitor_page', 0)
    items_per_page = 10
    total_pages = max(1, (len(tasks) + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))

    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(tasks))
    page_tasks = tasks[start_idx:end_idx]

    task_list = f"📋 **Your Monitoring Tasks**"
    if total_pages > 1:
        task_list += f" (Page {page + 1}/{total_pages})"
    task_list += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard = []

    for i, task in enumerate(page_tasks, start_idx + 1):
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Monitoring: {', '.join(map(str, task['chat_ids']))}\n\n"
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"monitor_task_{task['label']}")])

    task_list += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: **{len(tasks)} task(s)**\n\n💡 **Tap any task below to manage it!**"

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data="monitor_page_prev"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data="monitor_page_next"))
    if nav_row:
        keyboard.append(nav_row)

    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_monitor_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("monitor_task_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)

    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    settings = task.get("settings", {})

    check_duplicate_emoji = "✅" if settings.get("check_duplicate_and_notify", True) else "❌"
    manual_reply_emoji = "✅" if settings.get("manual_reply_system", True) else "❌"
    auto_reply_emoji = "✅" if settings.get("auto_reply_system", False) else "❌"
    outgoing_emoji = "✅" if settings.get("outgoing_message_monitoring", True) else "❌"

    auto_reply_message = settings.get("auto_reply_message", "")
    auto_reply_display = f"'{auto_reply_message[:30]}...'" if auto_reply_message else "Off"

    message_text = f"🔧 **Monitor Task Management: {task_label}**\n\n"
    message_text += f"📥 **Monitoring Chats:** {', '.join(map(str, task['chat_ids']))}\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{check_duplicate_emoji} Check Duplicate - Detects duplicates and sends alerts\n"
    message_text += f"{manual_reply_emoji} Manual reply system - Allows manual replies to duplicates\n"
    message_text += f"{auto_reply_emoji} Auto Reply = {auto_reply_display}\n"
    message_text += f"{outgoing_emoji} Monitor Outgoing - Monitors your outgoing messages\n\n"
    message_text += "💡 **Tap any option below to change it!**"

    keyboard = [
        [
            InlineKeyboardButton(f"{check_duplicate_emoji} Check Duplicate", callback_data=f"toggle_monitor_{task_label}_check_duplicate_and_notify"),
            InlineKeyboardButton(f"{manual_reply_emoji} Manual Reply", callback_data=f"toggle_monitor_{task_label}_manual_reply_system")
        ],
        [
            InlineKeyboardButton(f"{auto_reply_emoji} Auto Reply", callback_data=f"toggle_monitor_{task_label}_auto_reply_system"),
            InlineKeyboardButton(f"{outgoing_emoji} Monitor Outgoing", callback_data=f"toggle_monitor_{task_label}_outgoing_message_monitoring")
        ],
        [InlineKeyboardButton("✏️ Edit", callback_data=f"monitor_edit_{task_label}")],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_monitor_{task_label}")],
        [InlineKeyboardButton("🔙 Back to Monitor Tasks", callback_data="show_monitor_tasks")]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_monitor_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("monitor_edit_", "")
    for k in [f"mon_edit_name_{task_label}", f"mon_edit_chats_{task_label}"]:
        context.user_data.pop(k, None)

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    keyboard = [
        [InlineKeyboardButton("✏️ Edit Name",     callback_data=f"mon_editname_{task_label}")],
        [InlineKeyboardButton("💬 Edit Chat IDs", callback_data=f"mon_editchats_{task_label}")],
        [InlineKeyboardButton("🔙 Back to Task",  callback_data=f"monitor_task_{task_label}")]
    ]
    await query.edit_message_text(
        f"✏️ *Edit Monitor Task: {task_label}*\n\n"
        f"📥 Monitoring Chats: {', '.join(map(str, task['chat_ids']))}\n\n"
        "Choose what to edit:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_toggle_monitor_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    remainder = query.data.replace("toggle_monitor_", "")

    _MONITOR_TOGGLE_SUFFIXES = [
        "check_duplicate_and_notify", "manual_reply_system", "auto_reply_system",
        "outgoing_message_monitoring",
    ]
    task_label = None
    toggle_type = None
    for suffix in _MONITOR_TOGGLE_SUFFIXES:
        if remainder.endswith("_" + suffix):
            toggle_type = suffix
            task_label = remainder[: -(len(suffix) + 1)]
            break

    if task_label is None or toggle_type is None:
        await query.answer("Invalid action!", show_alert=True)
        return

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    task_index = next((i for i, t in enumerate(user_tasks) if t["label"] == task_label), -1)

    if task_index == -1:
        await query.answer("Task not found!", show_alert=True)
        return

    task = user_tasks[task_index]
    settings = task.get("settings", {})
    new_state = None

    if toggle_type == "check_duplicate_and_notify":
        new_state = not settings.get("check_duplicate_and_notify", True)
        settings["check_duplicate_and_notify"] = new_state

    elif toggle_type == "manual_reply_system":
        new_state = not settings.get("manual_reply_system", True)
        settings["manual_reply_system"] = new_state

    elif toggle_type == "auto_reply_system":
        current_state = settings.get("auto_reply_system", False)

        if not current_state:
            context.user_data[f"waiting_auto_reply_{task_label}"] = True
            await query.edit_message_text(
                f"🤖 **Auto Reply Setup for: {task_label}**\n\n"
                "Please enter the message you want to use for auto reply.\n\n"
                "⚠️ **Important:** This message will be sent automatically whenever a duplicate is detected.\n"
                "It will appear as coming from your account.\n\n"
                "💡 **Example messages:**\n"
                "• 'Please avoid sending duplicate messages.'\n"
                "• 'This message was already sent.'\n"
                "• 'Duplicate detected.'\n\n"
                "**Type your auto reply message now:**",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_auto_reply_{task_label}")]]),
                parse_mode="Markdown"
            )
            return
        else:
            new_state = False
            settings["auto_reply_system"] = new_state
            settings["auto_reply_message"] = ""

    elif toggle_type == "outgoing_message_monitoring":
        new_state = not settings.get("outgoing_message_monitoring", True)
        settings["outgoing_message_monitoring"] = new_state

    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return

    if new_state is not None:
        task["settings"] = settings
        monitoring_tasks_cache[user_id][task_index] = task
        asyncio.create_task(db_call(db.update_task_settings, user_id, task_label, settings))

    if new_state is not None:

        check_duplicate_emoji = "✅" if settings.get("check_duplicate_and_notify", True) else "❌"
        manual_reply_emoji = "✅" if settings.get("manual_reply_system", True) else "❌"
        auto_reply_emoji = "✅" if settings.get("auto_reply_system", False) else "❌"
        outgoing_emoji = "✅" if settings.get("outgoing_message_monitoring", True) else "❌"

        auto_reply_message = settings.get("auto_reply_message", "")
        auto_reply_display = f"'{auto_reply_message[:30]}...'" if auto_reply_message else "Off"

        message_text = f"🔧 **Monitor Task Management: {task_label}**\n\n"
        message_text += f"📥 **Monitoring Chats:** {', '.join(map(str, task['chat_ids']))}\n\n"
        message_text += "⚙️ **Settings:**\n"
        message_text += f"{check_duplicate_emoji} Check Duplicate - Detects duplicates and sends alerts\n"
        message_text += f"{manual_reply_emoji} Manual reply system - Allows manual replies to duplicates\n"
        message_text += f"{auto_reply_emoji} Auto Reply = {auto_reply_display}\n"
        message_text += f"{outgoing_emoji} Monitor Outgoing - Monitors your outgoing messages\n\n"
        message_text += "💡 **Tap any option below to change it!**"

        keyboard = [
            [
                InlineKeyboardButton(f"{check_duplicate_emoji} Check Duplicate", callback_data=f"toggle_monitor_{task_label}_check_duplicate_and_notify"),
                InlineKeyboardButton(f"{manual_reply_emoji} Manual Reply", callback_data=f"toggle_monitor_{task_label}_manual_reply_system")
            ],
            [
                InlineKeyboardButton(f"{auto_reply_emoji} Auto Reply", callback_data=f"toggle_monitor_{task_label}_auto_reply_system"),
                InlineKeyboardButton(f"{outgoing_emoji} Monitor Outgoing", callback_data=f"toggle_monitor_{task_label}_outgoing_message_monitoring")
            ],
            [InlineKeyboardButton("✏️ Edit", callback_data=f"monitor_edit_{task_label}")],
            [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_monitor_{task_label}")],
            [InlineKeyboardButton("🔙 Back to Monitor Tasks", callback_data="show_monitor_tasks")]
        ]

        try:
            await query.edit_message_text(
                message_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to update monitor task menu: {e}")

async def handle_delete_monitor_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_monitor_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    message_text = f"🗑️ **Delete Monitoring Task: {task_label}**\n\n"
    message_text += "⚠️ **Are you sure you want to delete this task?**\n\n"
    message_text += "This action cannot be undone!\n"
    message_text += "All monitoring will stop immediately."

    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_monitor_{task_label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"monitor_task_{task_label}")
        ]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_confirm_delete_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_monitor_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    deleted = await db_call(db.remove_monitoring_task, user_id, task_label)

    if deleted:
        if user_id in monitoring_tasks_cache:
            monitoring_tasks_cache[user_id] = [t for t in monitoring_tasks_cache[user_id] if t.get('label') != task_label]

        if user_id in user_clients:
            await update_monitoring_for_user(user_id)

        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\n"
            "All monitoring for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )

def _clear_monitor_handlers(user_id: int, client: TelegramClient):
    """
    Remove all registered monitor event handlers for user_id from client.
    Uses the tracked list in monitor_handler_registered only — no closure sweep,
    as that would risk removing unrelated handlers (e.g. the forward handler).
    """
    for handler in monitor_handler_registered.pop(user_id, []):
        try:
            client.remove_event_handler(handler)
        except Exception:
            pass


async def update_monitoring_for_user(user_id: int):
    if user_id not in user_clients:
        logger.warning(f"User {user_id} not in user_clients, cannot update monitoring")
        return

    client = user_clients[user_id]

    _clear_monitor_handlers(user_id, client)

    monitored_chat_ids_raw: set = set()
    user_tasks = monitoring_tasks_cache.get(user_id, [])
    for task in user_tasks:
        monitored_chat_ids_raw.update(task.get("chat_ids", []))

    monitored_chat_ids = set(parse_chat_id(cid)[0] for cid in monitored_chat_ids_raw)

    if not monitored_chat_ids:
        logger.info(f"No monitored chats for user {user_id}")
        return

    registered_count = 0
    for chat_id in monitored_chat_ids:
        try:
            await register_monitor_handler_for_chat(user_id, chat_id, client)
            registered_count += 1
        except Exception as e:
            logger.error(f"Failed to register monitor handler for chat {chat_id}: {e}")

    logger.info(f"✅ Updated monitoring for user {user_id}: {registered_count}/{len(monitored_chat_ids)} chat(s) registered")

async def register_monitor_handler_for_chat(user_id: int, chat_id: int, client: TelegramClient):

    async def _monitor_chat_handler(event):
        try:
            await optimized_gc()

            message = event.message
            if not message:
                return

            if hasattr(message, 'reactions') and message.reactions:
                return

            message_text = extract_message_text_only(message)
            if not message_text:
                return

            sender_id = message.sender_id
            message_id = message.id
            message_outgoing = getattr(message, "out", False)

            msg_topic_id = _get_message_topic_id(message)

            user_tasks_local = monitoring_tasks_cache.get(user_id, [])
            for task in user_tasks_local:
                if not any(_source_id_matches(chat_id, msg_topic_id, cid)
                           for cid in task.get("chat_ids", [])):
                    continue

                settings = task.get("settings", {})
                task_label = task.get("label", "Unknown")

                if message_outgoing and not settings.get("outgoing_message_monitoring", True):
                    continue

                if settings.get("check_duplicate_and_notify", True):
                    message_hash = create_message_hash(message_text, sender_id)

                    if is_duplicate_message(user_id, chat_id, message_hash):
                        logger.info(f"DUPLICATE DETECTED: User {user_id}, Task {task_label}, Chat {chat_id}")

                        occurrence_id = str(uuid.uuid4())[:8]
                        duplicate_key = f"{user_id}:{chat_id}:{message_hash}:{occurrence_id}"

                        if duplicate_key not in duplicate_reply_counts:
                            duplicate_reply_counts[duplicate_key] = 0

                        current_reply_count = duplicate_reply_counts[duplicate_key]

                        if settings.get("auto_reply_system", False) and settings.get("auto_reply_message"):
                            if current_reply_count < MAX_REPLIES_PER_DUPLICATE:
                                asyncio.create_task(handle_auto_reply_background(
                                    user_id, client, chat_id, message_id,
                                    settings.get("auto_reply_message"), duplicate_key,
                                    current_reply_count, message_text
                                ))

                        if settings.get("manual_reply_system", True):
                            try:
                                if notification_queue:
                                    await notification_queue.put((
                                        user_id, task, chat_id, message_id,
                                        message_text, message_hash, duplicate_key
                                    ))
                                else:
                                    logger.error("Notification queue not initialized!")
                            except asyncio.QueueFull:
                                logger.warning("Notification queue full")
                            except Exception as e:
                                logger.exception(f"Error queuing notification: {e}")
                        continue

                    store_message_hash(user_id, chat_id, message_hash, message_text)
                    asyncio.create_task(db_call(db.append_msg_hash,
                        user_id, chat_id, message_hash, time.time(), message_text[:80]))

        except Exception as e:
            logger.exception(f"Error in monitor message handler for user {user_id}, chat {chat_id}: {e}")

    try:
        client.add_event_handler(_monitor_chat_handler, events.NewMessage(chats=chat_id))
        client.add_event_handler(_monitor_chat_handler, events.MessageEdited(chats=chat_id))

        monitor_handler_registered.setdefault(user_id, []).append(_monitor_chat_handler)
        logger.debug(f"Registered monitor handler for user {user_id}, chat {chat_id}")
    except Exception as e:
        logger.exception(f"Failed to register monitor handler for user {user_id}, chat {chat_id}: {e}")

async def start_monitoring_for_user(user_id: int):
    if user_id not in user_clients:
        logger.warning(f"User {user_id} not in user_clients")
        return

    client = user_clients[user_id]
    monitoring_tasks_cache.setdefault(user_id, [])
    chat_entity_cache.setdefault(user_id, {})

    await update_monitoring_for_user(user_id)

async def reinitialize_monitoring_for_user(user_id: int):
    if user_id not in user_clients:
        logger.warning(f"Cannot reinitialize monitoring for user {user_id}: no client")
        return

    client = user_clients[user_id]

    _clear_monitor_handlers(user_id, client)

    chat_entity_cache.setdefault(user_id, {})

    await update_monitoring_for_user(user_id)

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    logger.info(f"✅ Reinitialized monitoring for user {user_id} with {len(user_tasks)} tasks")


async def handle_auto_reply_background(user_id, client, chat_id, message_id,
                                      auto_reply_message, duplicate_key,
                                      current_reply_count, message_text):
    try:
        await asyncio.sleep(2)

        chat_entity = await client.get_input_entity(chat_id)
        await client.send_message(chat_entity, auto_reply_message, reply_to=message_id)

        new_reply_count = current_reply_count + 1
        duplicate_reply_counts[duplicate_key] = new_reply_count
        asyncio.create_task(db_call(db.save_dup_count, duplicate_key,
            user_id, new_reply_count))

        remaining = MAX_REPLIES_PER_DUPLICATE - new_reply_count

        try:
            _bot = get_bot()
            preview_text = message_text[:50] + "..." if len(message_text) > 50 else message_text
            escaped_auto_reply = escape_markdown(auto_reply_message, version=2)
            remaining_text = f"{remaining} remaining" if remaining > 0 else "No more replies allowed"

            timestamp = format_datetime_utc1()

            await _bot.send_message(
                user_id,
                f"🤖 **Auto Reply sent successfully!**\n\n"
                f"🕐 **Time:** {timestamp}\n"
                f"📝 **Your auto reply:** {escaped_auto_reply}\n"
                f"**Reply No.:** {new_reply_count}/{MAX_REPLIES_PER_DUPLICATE} ({remaining_text})\n"
                f"🔗 **Replying to:** `{preview_text}`\n\n"
                f"The duplicate sender has been notified with your reply.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.debug(f"Failed to send auto reply notification: {e}")

    except Exception as e:
        logger.exception(f"Error in background auto reply: {e}")

async def handle_notification_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    if not update.message.reply_to_message:
        return

    replied_message_id = update.message.reply_to_message.message_id

    if replied_message_id not in notification_messages:
        return

    notification_data = notification_messages[replied_message_id]

    if notification_data["user_id"] != user_id:
        return

    task_label = notification_data["task_label"]
    chat_id = notification_data["chat_id"]
    original_message_id = notification_data["original_message_id"]
    message_preview = notification_data.get("message_preview", "Unknown message")
    duplicate_key = notification_data.get("duplicate_key")

    current_count = duplicate_reply_counts.get(duplicate_key, 0)
    if current_count >= MAX_REPLIES_PER_DUPLICATE:
        await update.message.reply_text(
            f"⚠️ **Max replies ({MAX_REPLIES_PER_DUPLICATE}) reached for this message!**",
            parse_mode="Markdown"
        )
        return

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    task = next((t for t in user_tasks if t["label"] == task_label), None)

    if not task:
        await update.message.reply_text("❌ Task not found!")
        return

    if user_id not in user_clients:
        await update.message.reply_text("❌ You need to be logged in to send replies!")
        return

    client = user_clients[user_id]

    if not await _ensure_client_healthy(user_id, client):
        await update.message.reply_text("❌ Your Telegram session is unresponsive. Reconnecting — please try again in a moment.")
        return

    try:
        chat_entity = await client.get_input_entity(chat_id)
        await client.send_message(chat_entity, text, reply_to=original_message_id)

        new_count = current_count + 1
        duplicate_reply_counts[duplicate_key] = new_count
        asyncio.create_task(db_call(db.save_dup_count, duplicate_key,
            notification_data.get("user_id", 0), new_count))

        escaped_text = escape_markdown(text, version=2)

        remaining = MAX_REPLIES_PER_DUPLICATE - new_count
        remaining_text = f"{remaining} remaining" if remaining > 0 else "No more replies allowed"

        await update.message.reply_text(
            f"✍️ **Manual Reply sent successfully!**\n\n"
            f"📝 **Your manual reply:** {escaped_text}\n"
            f"**Reply No.:** {new_count}/{MAX_REPLIES_PER_DUPLICATE} ({remaining_text})\n"
            f"🔗 **Replying to:** `{message_preview}`\n\n"
            f"The duplicate sender has been notified with your reply.",
            parse_mode="Markdown"
        )

        if new_count >= MAX_REPLIES_PER_DUPLICATE:
            notification_messages.pop(replied_message_id, None)
            notification_creation_time.pop(replied_message_id, None)
            asyncio.create_task(db_call(db.delete_notif_state, replied_message_id))

    except Exception as e:
        logger.exception(f"Error sending manual reply: {e}")
        await update.message.reply_text(f"❌ **Failed:** {str(e)}")

async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    if user_id in task_creation_states:
        state = task_creation_states[user_id]
        task_type = state.get("type", "forwarding")

        try:
            if state["step"] == "waiting_name":
                if not text:
                    await update.message.reply_text(
                        "❌ **Please enter a valid task name!**",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                    )
                    return

                state["name"] = text
                state["step"] = "waiting_ids"

                if task_type == "forwarding":
                    await update.message.reply_text(
                        f"✅ **Task name saved:** {text}\n\n📥 **Step 2 of 3:** Please enter the source chat ID(s).\n\nYou can enter multiple IDs separated by spaces.\n💡 *Use /getallid to find your chat IDs*\n💡 Use plain IDs or topic IDs (`-100123456789:36615`)\n\n**Example:** `123456789 987654321` or `-100123:36615`",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Back", callback_data="back_task_name")],
                            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]
                        ]),
                        parse_mode="Markdown"
                    )
                else:
                    await update.message.reply_text(
                        f"✅ **Task name saved:** {text}\n\n"
                        "📥 **Step 2 of 2:** Please enter the chat ID(s) to monitor.\n\n"
                        "You can enter multiple IDs separated by spaces.\n"
                        "💡 *Use /getallid to find your chat IDs*\n"
                        "💡 Use plain IDs or topic IDs (`-100123456789:36615`)\n\n"
                        "**Example:** `-1001234567890` or `-100123456789:36615`",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Back", callback_data="back_task_name")],
                            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]
                        ]),
                        parse_mode="Markdown"
                    )

            elif state["step"] == "waiting_ids":
                if not text:
                    await update.message.reply_text(
                        "❌ **Please enter at least one ID!**",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                    )
                    return

                try:
                    ids = [_parse_id_token(t) for t in text.split() if _is_valid_id_token(t)]
                    ids = [x for x in ids if x is not None]
                    if not ids:
                        await update.message.reply_text(
                            "❌ **Please enter valid IDs!**\n\nUse plain IDs (`-100123456789`) or topic IDs (`-100123456789:36615`).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                        )
                        return

                    if task_type == "forwarding":
                        state["source_ids"] = ids
                        state["step"] = "waiting_targets"

                        await update.message.reply_text(
                            f"✅ **Source IDs saved:** {', '.join(ids)}\n\n📤 **Step 3 of 3:** Please enter the target chat ID(s).\n\nYou can enter multiple IDs separated by spaces.\n💡 *Use /getallid to find your chat IDs*\n💡 Use plain IDs or topic IDs (`-100123:36615`)\n\n**Example:** `111222333` or `-100123456789:36615`",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Back", callback_data="back_task_sources")],
                                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]
                            ]),
                            parse_mode="Markdown"
                        )
                    else:
                        state["chat_ids"] = ids

                        task_settings = {
                            "check_duplicate_and_notify": True,
                            "manual_reply_system": True,
                            "auto_reply_system": False,
                            "auto_reply_message": "",
                            "outgoing_message_monitoring": True
                        }

                        added = await db_call(db.add_monitoring_task,
                                             user_id,
                                             state["name"],
                                             state["chat_ids"],
                                             task_settings)

                        if added:
                            monitoring_tasks_cache.setdefault(user_id, []).append({
                                "id": None,
                                "label": state["name"],
                                "chat_ids": state["chat_ids"],
                                "is_active": 1,
                                "settings": task_settings
                            })

                            await update.message.reply_text(
                                f"🎉 **Monitoring task created successfully!**\n\n"
                                f"📋 **Name:** {state['name']}\n"
                                f"📥 **Monitoring Chats:** {', '.join(map(str, state['chat_ids']))}\n\n"
                                "✅ Default settings applied:\n"
                                "• Check Duplicate: ✅ Active\n"
                                "• Manual reply system: ✅ Enabled\n"
                                "• Auto Reply system: ❌ Disabled\n"
                                "• Monitor Outgoing: ✅ Enabled\n\n"
                                "Use /monitortasks to manage your task!",
                                parse_mode="Markdown"
                            )

                            logger.info(f"Monitoring task created for user {user_id}: {state['name']}")

                            if user_id in user_clients:
                                await update_monitoring_for_user(user_id)

                            del task_creation_states[user_id]

                        else:
                            await update.message.reply_text(
                                f"❌ **Task '{state['name']}' already exists!**\n\n"
                                "Please choose a different name.",
                                parse_mode="Markdown"
                            )

                except ValueError:
                    await update.message.reply_text(
                        "❌ **Please enter valid IDs!**\n\nUse plain IDs or topic IDs (`-100123456789:36615`).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                    )

            elif state["step"] == "waiting_targets" and task_type == "forwarding":
                if not text:
                    await update.message.reply_text(
                        "❌ **Please enter at least one target ID!**",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                    )
                    return

                try:
                    target_ids = [_parse_id_token(t) for t in text.split() if _is_valid_id_token(t)]
                    target_ids = [x for x in target_ids if x is not None]
                    if not target_ids:
                        await update.message.reply_text(
                            "❌ **Please enter valid IDs!**\n\nUse plain IDs or topic IDs (`-100123456789:36615`).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                        )
                        return

                    state["target_ids"] = target_ids

                    task_filters = {
                        "filters": {
                            "raw_text": True,
                            "numbers_only": False,
                            "alphabets_only": False,
                            "removed_alphabetic": False,
                            "removed_numeric": False,
                            "prefix": "",
                            "suffix": ""
                        },
                        "outgoing": True,
                        "forward_tag": False,
                        "control": True
                    }

                    added = await db_call(db.add_forwarding_task,
                                         user_id,
                                         state["name"],
                                         state["source_ids"],
                                         state["target_ids"],
                                         task_filters)

                    if added:
                        forwarding_tasks_cache.setdefault(user_id, []).append({
                            "id": None,
                            "label": state["name"],
                            "source_ids": state["source_ids"],
                            "target_ids": state["target_ids"],
                            "is_active": 1,
                            "filters": task_filters
                        })

                        if user_id in user_clients:
                            await update_forwarding_for_user(user_id)
                        else:
                            try:
                                asyncio.create_task(resolve_targets_for_user(user_id, target_ids))
                            except Exception:
                                logger.exception("Failed to schedule resolve_targets_for_user")

                        await update.message.reply_text(
                            f"🎉 **Forwarding task created successfully!**\n\n📋 **Name:** {state['name']}\n📥 **Sources:** {', '.join(map(str, state['source_ids']))}\n📤 **Targets:** {', '.join(map(str, state['target_ids']))}\n\n✅ All filters are set to default:\n• Outgoing: ✅ On\n• Forward Tag: ❌ Off\n• Control: ✅ On\n\nUse /fortasks to manage your task!",
                            parse_mode="Markdown"
                        )

                        del task_creation_states[user_id]

                    else:
                        await update.message.reply_text(
                            f"❌ **Task '{state['name']}' already exists!**\n\nPlease choose a different name.",
                            parse_mode="Markdown"
                        )

                except ValueError:
                    await update.message.reply_text(
                        "❌ **Please enter valid IDs!**\n\nUse plain IDs or topic IDs (`-100123456789:36615`).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Operation", callback_data="cancel_task_creation")]])
                    )

        except Exception as e:
            logger.exception("Error in task creation")
            await update.message.reply_text(
                f"❌ **Error creating task:** {str(e)[:100]}",
                parse_mode="Markdown"
            )
            if user_id in task_creation_states:
                del task_creation_states[user_id]
        return

async def show_prefix_suffix_menu(query, task_label: str, task_type: str):
    user_id = query.from_user.id

    if task_type == "forward":
        user_tasks = forwarding_tasks_cache.get(user_id, [])
        task_key = "filters"
        filter_key = "filters"
    else:
        user_tasks = monitoring_tasks_cache.get(user_id, [])
        task_key = "settings"
        filter_key = None

    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break

    if not task:
        await query.answer("Task not found!", show_alert=True)
        return

    if task_type == "forward":
        filters = task.get("filters", {})
        filter_settings = filters.get("filters", {})
        prefix = filter_settings.get("prefix", "")
        suffix = filter_settings.get("suffix", "")
    else:
        await query.answer("Prefix/suffix only available for forwarding tasks!")
        return

    message_text = f"🔤 **Prefix/Suffix Setup for: {task_label}**\n\nAdd custom text to messages:\n\n📝 **Current Prefix:** '{prefix}'\n📝 **Current Suffix:** '{suffix}'\n\n💡 **Examples:**\n• Prefix '🔔 ' adds a bell before each message\n• Suffix ' ✅' adds a checkmark after\n• Use any characters: emojis, signs, numbers, letters\n\n**Tap an option below to set it!**"

    keyboard = [
        [InlineKeyboardButton("➕ Set Prefix", callback_data=f"prefix_{task_label}_{task_type}")],
        [InlineKeyboardButton("➕ Set Suffix", callback_data=f"suffix_{task_label}_{task_type}")],
        [InlineKeyboardButton("🗑️ Clear Prefix/Suffix", callback_data=f"toggle_{task_type}_{task_label}_clear_prefix_suffix")],
        [InlineKeyboardButton("🔙 Back to Filters", callback_data=f"{task_type}_filter_{task_label}")]
    ]

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_prefix_suffix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    raw = query.data
    first_sep = raw.index("_")
    action_type = raw[:first_sep]
    remainder = raw[first_sep + 1:]

    if not remainder.endswith("_forward"):
        await query.answer("Invalid action!", show_alert=True)
        return

    task_label = remainder[: -len("_forward")]
    task_type = "forward"

    if not action_type or not task_label:
        await query.answer("Invalid action!", show_alert=True)
        return

    context.user_data[f"waiting_{action_type}"] = {"task_label": task_label, "task_type": task_type}
    await query.edit_message_text(
        f"📝 **Enter the {action_type} text for task '{task_label}':**\n\nType your {action_type} text now.\n💡 *You can use any characters: emojis 🔔, signs ⚠️, numbers 123, letters ABC*\n\n**Example:** If you want the {action_type} '🔔 ', type: 🔔 ",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_prefix_suffix_{task_label}")]]),
        parse_mode="Markdown"
    )

async def handle_prefix_suffix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    waiting_data = context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix")
    if not waiting_data:
        return

    action_type = "prefix" if "waiting_prefix" in context.user_data else "suffix"
    task_label = waiting_data["task_label"]
    task_type = waiting_data["task_type"]

    if "waiting_prefix" in context.user_data:
        del context.user_data["waiting_prefix"]
    if "waiting_suffix" in context.user_data:
        del context.user_data["waiting_suffix"]

    if task_type == "forward":
        user_tasks = forwarding_tasks_cache.get(user_id, [])
        task_index = -1
        for i, t in enumerate(user_tasks):
            if t["label"] == task_label:
                task_index = i
                break

        if task_index == -1:
            await update.message.reply_text("❌ Task not found!")
            return

        task = user_tasks[task_index]
        filters = task.get("filters", {})
        filter_settings = filters.get("filters", {})

        if action_type == "prefix":
            filter_settings["prefix"] = text
            confirmation = f"✅ **Prefix set to:** '{text}'"
        else:
            filter_settings["suffix"] = text
            confirmation = f"✅ **Suffix set to:** '{text}'"

        filters["filters"] = filter_settings
        task["filters"] = filters
        forwarding_tasks_cache[user_id][task_index] = task

        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )

        await update.message.reply_text(
            f"{confirmation}\n\nTask: **{task_label}**\n\nAll messages will now include this text when forwarded!",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Prefix/suffix only available for forwarding tasks!")

async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\nUse /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)

async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = """🗂️ **Chat ID Categories**

📋 Choose which type of chat IDs you want to see:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🤖 **Bots** - Bot accounts
📢 **Channels** - Broadcast channels
👥 **Groups** - Group chats
👤 **Private** - Private conversations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 Select a category below:"""

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_load_bots"), InlineKeyboardButton("📢 Channels", callback_data="chatids_load_channels")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_load_groups"), InlineKeyboardButton("👤 Private", callback_data="chatids_load_private")],
    ]

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE, reload: bool = False):
    from telethon.tl.types import User, Channel, Chat

    if user_id not in user_clients:
        return

    if "chatids_cache" not in context.user_data:
        context.user_data["chatids_cache"] = {}

    if reload or category not in context.user_data["chatids_cache"]:
        client = user_clients[user_id]
        if not await _ensure_client_healthy(user_id, client):
            return
        cached = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if category == "bots":
                if isinstance(entity, User) and entity.bot:
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "channels":
                if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "groups":
                if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                    cached.append({"name": dialog.name, "id": dialog.id})
            elif category == "private":
                if isinstance(entity, User) and not entity.bot:
                    cached.append({"name": dialog.name, "id": dialog.id})

        context.user_data["chatids_cache"][category] = cached

    categorized_dialogs = context.user_data["chatids_cache"][category]

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}

    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")

    if not categorized_dialogs:
        chat_list = f"{emoji} **{name}**\n\n📭 **No {name.lower()} found!**\n\nTry another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = (dialog["name"] or "Unknown")[:30]
            escaped_chat_name = escape_markdown(chat_name, version=2)
            chat_list += f"{i}. **{escaped_chat_name}**\n   🆔 `{dialog['id']}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

    keyboard = []

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_nav_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_nav_{category}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])

    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def handle_auto_reply_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    waiting_for_auto_reply = False
    task_label = None

    for key in list(context.user_data.keys()):
        if key.startswith("waiting_auto_reply_"):
            waiting_for_auto_reply = True
            task_label = key.replace("waiting_auto_reply_", "")
            del context.user_data[key]
            break

    if not waiting_for_auto_reply or not task_label:
        return

    user_tasks = monitoring_tasks_cache.get(user_id, [])
    task_index = next((i for i, t in enumerate(user_tasks) if t["label"] == task_label), -1)

    if task_index == -1:
        await update.message.reply_text("❌ Task not found!")
        return

    task = user_tasks[task_index]
    settings = task.get("settings", {})

    settings["auto_reply_system"] = True
    settings["auto_reply_message"] = text

    task["settings"] = settings
    monitoring_tasks_cache[user_id][task_index] = task

    try:
        await db_call(db.update_task_settings, user_id, task_label, settings)
    except Exception as e:
        logger.exception("Error updating task settings in DB: %s", e)
        await update.message.reply_text("❌ Error saving auto reply message!")
        return

    escaped_text = escape_markdown(text, version=2)

    await update.message.reply_text(
        f"✅ **Auto Reply Message Added Successfully!**\n\n"
        f"Task: **{task_label}**\n"
        f"Auto Reply Message: '{text}'\n\n"
        f"This message will be sent automatically whenever a duplicate is detected.\n"
        f"⚠️ **Remember:** It will appear as coming from your account.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"monitor_task_{task_label}")]]),
        parse_mode="Markdown"
    )

    logger.info(f"Auto reply message set for task {task_label} by user {user_id}")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    await query.answer()

    action = query.data

    if action == "login":
        await query.message.delete()
        await login_command(update, context)
    elif action == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif action == "show_forward_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
    elif action == "show_monitor_tasks":
        await query.message.delete()
        await monitortasks_command(update, context)
    elif action == "back_to_main":
        await show_main_menu(update, context, user_id)

    elif action == "forward_page_prev":
        context.user_data['forward_page'] = context.user_data.get('forward_page', 0) - 1
        await query.message.delete()
        await fortasks_command(update, context)
    elif action == "forward_page_next":
        context.user_data['forward_page'] = context.user_data.get('forward_page', 0) + 1
        await query.message.delete()
        await fortasks_command(update, context)

    elif action == "monitor_page_prev":
        context.user_data['monitor_page'] = context.user_data.get('monitor_page', 0) - 1
        await query.message.delete()
        await monitortasks_command(update, context)
    elif action == "monitor_page_next":
        context.user_data['monitor_page'] = context.user_data.get('monitor_page', 0) + 1
        await query.message.delete()
        await monitortasks_command(update, context)

    elif action.startswith("forward_edit_"):
        await handle_forward_edit_menu(update, context)
    elif action.startswith("fw_editname_"):
        task_label = action[len("fw_editname_"):]
        context.user_data[f"fw_edit_name_{task_label}"] = True
        await query.edit_message_text(
            f"✏️ *Edit Task Name*\n\nCurrent name: *{task_label}*\n\nSend the new name:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
    elif action.startswith("fw_editsrc_"):
        task_label = action[len("fw_editsrc_"):]
        context.user_data[f"fw_edit_src_{task_label}"] = True
        await query.edit_message_text(
            "📥 *Edit Source IDs*\n\nSend the new source chat ID(s) separated by spaces.\n\n💡 Use /getallid to find IDs.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
    elif action.startswith("fw_edittgt_"):
        task_label = action[len("fw_edittgt_"):]
        context.user_data[f"fw_edit_tgt_{task_label}"] = True
        await query.edit_message_text(
            "📤 *Edit Target IDs*\n\nSend the new target chat ID(s) separated by spaces.\n\n💡 Use /getallid to find IDs.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
    elif action.startswith("fw_actsrc_"):
        task_label = action[len("fw_actsrc_"):]
        await _fw_render_active_picker(query, context, user_id, task_label, "source")
    elif action.startswith("fw_acttgt_"):
        task_label = action[len("fw_acttgt_"):]
        await _fw_render_active_picker(query, context, user_id, task_label, "target")
    elif action.startswith("fw_asrctog_") or action.startswith("fw_atgttog_"):
        field = "source" if action.startswith("fw_asrctog_") else "target"
        prefix = "fw_asrctog_" if field == "source" else "fw_atgttog_"
        idx_str, task_label = action[len(prefix):].split("_", 1)
        user_tasks = forwarding_tasks_cache.get(user_id, [])
        task = next((t for t in user_tasks if t["label"] == task_label), None)
        if not task:
            await query.message.reply_text("❌ Task not found — it may have been deleted.")
        else:
            master_list = task["source_ids"] if field == "source" else task["target_ids"]
            active_key  = "active_source_ids" if field == "source" else "active_target_ids"
            idx = int(idx_str)
            changed = False
            if 0 <= idx < len(master_list):
                cid = master_list[idx]
                active_now = set(task.get(active_key) or master_list)
                if cid in active_now:
                    if len(active_now) == 1:
                        await query.message.reply_text(f"❌ At least one {field} must stay active — can't turn this off.")
                    else:
                        active_now.discard(cid)
                        kwargs = {active_key: list(active_now)}
                        ok = await db_call(db.update_forwarding_task_active_ids, user_id, task_label, **kwargs)
                        if not ok:
                            await query.message.reply_text(f"❌ At least one {field} must stay active — can't turn this off.")
                        else:
                            task[active_key] = list(active_now)
                            changed = True
                else:
                    active_now.add(cid)
                    kwargs = {active_key: list(active_now)}
                    ok = await db_call(db.update_forwarding_task_active_ids, user_id, task_label, **kwargs)
                    if ok:
                        task[active_key] = list(active_now)
                        changed = True
                    else:
                        await query.message.reply_text("❌ Save failed — try again.")
            if changed:
                await _fw_render_active_picker(query, context, user_id, task_label, field)
    elif action.startswith("monitor_edit_"):
        await handle_monitor_edit_menu(update, context)
    elif action.startswith("mon_editname_"):
        task_label = action[len("mon_editname_"):]
        context.user_data[f"mon_edit_name_{task_label}"] = True
        await query.edit_message_text(
            f"✏️ *Edit Task Name*\n\nCurrent name: *{task_label}*\n\nSend the new name:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"monitor_edit_{task_label}")]]))
    elif action.startswith("mon_editchats_"):
        task_label = action[len("mon_editchats_"):]
        context.user_data[f"mon_edit_chats_{task_label}"] = True
        await query.edit_message_text(
            "💬 *Edit Chat IDs*\n\nSend the new monitoring chat ID(s) separated by spaces.\n\n💡 Use /getallid to find IDs.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"monitor_edit_{task_label}")]]))
    elif action.startswith("forward_task_"):
        await handle_forward_task_menu(update, context)
    elif action.startswith("forward_filter_"):
        await handle_forward_filter_menu(update, context)
    elif action.startswith("toggle_forward_"):
        await handle_toggle_forward_action(update, context)
    elif action.startswith("delete_forward_"):
        await handle_delete_forward_action(update, context)
    elif action.startswith("confirm_delete_forward_"):
        await handle_confirm_delete_forward(update, context)
    elif action.startswith("prefix_suffix_"):
        task_label = action.replace("prefix_suffix_", "")
        await show_prefix_suffix_menu(query, task_label, "forward")

    elif action.startswith("monitor_task_"):
        await handle_monitor_task_menu(update, context)
    elif action.startswith("toggle_monitor_"):
        await handle_toggle_monitor_action(update, context)
    elif action.startswith("delete_monitor_"):
        await handle_delete_monitor_action(update, context)
    elif action.startswith("confirm_delete_monitor_"):
        await handle_confirm_delete_monitor(update, context)

    elif action.startswith("chatids_"):
        if action == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        elif action.startswith("chatids_load_"):
            category = action[len("chatids_load_"):]
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, 0, context, reload=True)
        elif action.startswith("chatids_nav_"):
            parts = action.split("_")
            category = parts[2]
            page = int(parts[3])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context, reload=False)


    elif action == "cancel_login":
        if user_id in login_states:
            try:
                client = login_states[user_id].get("client")
                if client:
                    await client.disconnect()
            except:
                pass
            del login_states[user_id]
        await query.edit_message_text(
            "❌ **Login cancelled.** You can start again with /login anytime.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )

    elif action == "cancel_logout":
        if user_id in logout_states:
            del logout_states[user_id]
        await query.edit_message_text(
            "❌ **Logout cancelled.** Your session remains active.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )

    elif action == "cancel_task_creation":
        task_creation_states.pop(user_id, None)
        await query.edit_message_text(
            "❌ **Task creation cancelled.**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )

    elif action == "back_task_name":
        if user_id in task_creation_states:
            task_creation_states[user_id]["step"] = "waiting_name"
            task_type = task_creation_states[user_id].get("type", "forwarding")

            if task_type == "forwarding":
                await query.edit_message_text(
                    "📝 **Step 1 of 3:** Please enter a name for your task.\n\n💡 *Example: My Forwarding Task*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]]),
                    parse_mode="Markdown"
                )
            else:
                await query.edit_message_text(
                    "📝 **Step 1 of 2:** Please enter a name for your monitoring task.\n\n💡 *Example: Group Duplicate Checker*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]]),
                    parse_mode="Markdown"
                )
        else:
            await query.edit_message_text("❌ Task creation session expired.")

    elif action == "back_task_sources":
        if user_id in task_creation_states:
            task_creation_states[user_id]["step"] = "waiting_ids"
            await query.edit_message_text(
                "📥 **Step 2 of 3:** Please enter the source chat ID(s).\n\nYou can enter multiple IDs separated by spaces.\n💡 *Use /getallid to find your chat IDs*\n\n**Example:** `123456789 987654321`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="back_task_name")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("❌ Task creation session expired.")

    elif action.startswith("cancel_prefix_suffix_"):
        task_label = action.replace("cancel_prefix_suffix_", "")
        context.user_data.pop("waiting_prefix", None)
        context.user_data.pop("waiting_suffix", None)
        await query.edit_message_text(
            f"❌ **Prefix/Suffix setting cancelled.** No changes made to task '{task_label}'.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Task", callback_data=f"forward_task_{task_label}")]]),
            parse_mode="Markdown"
        )

    elif action.startswith("cancel_auto_reply_"):
        task_label = action.replace("cancel_auto_reply_", "")
        for key in list(context.user_data.keys()):
            if key.startswith("waiting_auto_reply_"):
                del context.user_data[key]
        await query.edit_message_text(
            f"❌ **Auto reply setup cancelled.** Auto reply remains disabled for task '{task_label}'.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Task", callback_data=f"monitor_task_{task_label}")]]),
            parse_mode="Markdown"
        )

    elif action == "cancel_phone_verification":
        phone_verification_states.pop(user_id, None)
        await query.edit_message_text(
            "❌ **Phone verification cancelled.** Your session will remain limited until you verify.\n\nUse /start to try again.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )

async def handle_all_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_authorization(update, context):
        return

    user_id = update.effective_user.id
    text = update.message.text.strip()

    for key in list(context.user_data.keys()):
        if key.startswith("fw_edit_name_"):
            task_label = key[len("fw_edit_name_"):]
            context.user_data.pop(key, None)
            if len(text) > 60:
                await update.message.reply_text(
                    "⚠️ Task name must be 60 characters or fewer.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
                return
            ok = await db_call(db.update_forwarding_task, user_id, task_label, new_label=text)
            if ok:
                if user_id in forwarding_tasks_cache:
                    for t in forwarding_tasks_cache[user_id]:
                        if t['label'] == task_label: t['label'] = text; break
                await update.message.reply_text(
                    f"✅ *Task Renamed*\n\nNew name: *{text}*", parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Task", callback_data=f"forward_task_{text}")]]))
            else:
                await update.message.reply_text("❌ Rename failed. Task not found.")
            return
        if key.startswith("fw_edit_src_"):
            task_label = key[len("fw_edit_src_"):]
            context.user_data.pop(key, None)
            parts = text.split(); raw_ids = []
            for p in parts:
                p = p.strip().rstrip(',')
                if _is_valid_id_token(p): raw_ids.append(p)
            if not raw_ids:
                await update.message.reply_text(
                    "❌ No valid IDs found. Send numeric chat IDs separated by spaces.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
                return
            ids = list(dict.fromkeys(raw_ids))
            dupe_note = f"\n\n_{len(raw_ids) - len(ids)} repeated ID\\(s\\) were merged \\(rearranged, not duplicated\\)\\._" if len(ids) != len(raw_ids) else ""
            await db_call(db.update_forwarding_task, user_id, task_label, source_ids=ids)
            if user_id in forwarding_tasks_cache:
                for t in forwarding_tasks_cache[user_id]:
                    if t['label'] == task_label:
                        t['source_ids'] = ids; t['active_source_ids'] = list(ids); break
            await update.message.reply_text(
                f"✅ *Sources Updated*\n\nNew sources: `{', '.join(map(str, ids))}`{dupe_note}", parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Task", callback_data=f"forward_task_{task_label}")]]))
            return
        if key.startswith("fw_edit_tgt_"):
            task_label = key[len("fw_edit_tgt_"):]
            context.user_data.pop(key, None)
            parts = text.split(); raw_ids = []
            for p in parts:
                p = p.strip().rstrip(',')
                if _is_valid_id_token(p): raw_ids.append(p)
            if not raw_ids:
                await update.message.reply_text(
                    "❌ No valid IDs found. Send numeric chat IDs separated by spaces.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"forward_edit_{task_label}")]]))
                return
            ids = list(dict.fromkeys(raw_ids))
            dupe_note = f"\n\n_{len(raw_ids) - len(ids)} repeated ID\\(s\\) were merged \\(rearranged, not duplicated\\)\\._" if len(ids) != len(raw_ids) else ""
            await db_call(db.update_forwarding_task, user_id, task_label, target_ids=ids)
            if user_id in forwarding_tasks_cache:
                for t in forwarding_tasks_cache[user_id]:
                    if t['label'] == task_label:
                        t['target_ids'] = ids; t['active_target_ids'] = list(ids); break
            await update.message.reply_text(
                f"✅ *Targets Updated*\n\nNew targets: `{', '.join(map(str, ids))}`{dupe_note}", parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Task", callback_data=f"forward_task_{task_label}")]]))
            return


    for key in list(context.user_data.keys()):
        if key.startswith("mon_edit_name_"):
            task_label = key[len("mon_edit_name_"):]
            context.user_data.pop(key, None)
            if len(text) > 60:
                await update.message.reply_text(
                    "⚠️ Task name must be 60 characters or fewer.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"monitor_edit_{task_label}")]]))
                return
            ok = await db_call(db.update_monitoring_task, user_id, task_label, new_label=text)
            if ok:
                if user_id in monitoring_tasks_cache:
                    for t in monitoring_tasks_cache[user_id]:
                        if t['label'] == task_label: t['label'] = text; break
                await update.message.reply_text(
                    f"✅ *Task Renamed*\n\nNew name: *{text}*", parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Task", callback_data=f"monitor_task_{text}")]]))
            else:
                await update.message.reply_text("❌ Rename failed. Task not found.")
            return
        if key.startswith("mon_edit_chats_"):
            task_label = key[len("mon_edit_chats_"):]
            context.user_data.pop(key, None)
            parts = text.split(); ids = []
            for p in parts:
                p = p.strip().rstrip(',')
                if _is_valid_id_token(p): ids.append(int(p) if ':' not in p else p)
            if not ids:
                await update.message.reply_text(
                    "❌ No valid IDs found. Send numeric chat IDs separated by spaces.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"monitor_edit_{task_label}")]]))
                return
            await db_call(db.update_monitoring_task, user_id, task_label, chat_ids=ids)
            if user_id in monitoring_tasks_cache:
                for t in monitoring_tasks_cache[user_id]:
                    if t['label'] == task_label: t['chat_ids'] = ids; break
            if user_id in user_clients:
                await reinitialize_monitoring_for_user(user_id)
            await update.message.reply_text(
                f"✅ *Chat IDs Updated*\n\nNew chats: `{', '.join(map(str, ids))}`", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Task", callback_data=f"monitor_task_{task_label}")]]))
            return

    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    if user_id in login_states:
        await handle_login_process(update, context)
        return

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return

    for key in list(context.user_data.keys()):
        if key.startswith("waiting_auto_reply_"):
            task_label = key.replace("waiting_auto_reply_", "")
            await handle_auto_reply_message(update, context)
            return

    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return

    if update.message.reply_to_message:
        await handle_notification_reply(update, context)
        return

    await update.message.reply_text(
        "🤔 **I didn't understand that command.**\n\nUse /start to see available commands.",
        parse_mode="Markdown"
    )


_health_reconnect_in_progress: set = set()
_health_reconnect_cooldown: Dict[int, float] = {}
_HEALTH_RECONNECT_COOLDOWN = 60.0
_CONNECT_TIMEOUT = 15.0


async def _do_connect(user_id: int, wrapper: ClientWrapper) -> bool:
    """
    Part 1 — fast inline connect: creates a fresh TelegramClient from the stored
    session and updates the wrapper so all existing references (forward handler,
    monitor handlers etc.) transparently use the new clean client.
    Wrapped in a timeout so it never hangs the event loop.
    """
    try:
        async def _connect():
            try:
                await wrapper.get_inner().disconnect()
            except Exception:
                pass
            user_record = await db_call(db.get_user, user_id)
            session_data = user_record.get("session_data") if user_record else None
            if not session_data:
                logger.warning(f"[HealthCheck] No session data found for user {user_id}")
                return False
            new_client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
            await new_client.connect()
            if new_client.is_connected():
                wrapper.update(new_client)
                return True
            return False
        return await asyncio.wait_for(_connect(), timeout=_CONNECT_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(f"[HealthCheck] Connect timed out for user {user_id}")
        return False
    except Exception as e:
        logger.warning(f"[HealthCheck] Connect failed for user {user_id}: {e}")
        return False


async def _do_recover(user_id: int, wrapper: ClientWrapper):
    """
    Part 2 — background recovery: all heavy work after connect succeeds.
    Fired as asyncio.create_task so it never blocks the caller.
    Uses wrapper.get_inner() to get the fresh client after _do_connect updated it.
    """
    client = wrapper.get_inner()
    try:
        try:
            if forwarding_tasks_cache.get(user_id):
                forward_handler_registered.pop(user_id, None)
                ensure_forward_handler_registered_for_user(user_id, client)
            all_targets = []
            for task in forwarding_tasks_cache.get(user_id, []):
                all_targets.extend(task.get("active_target_ids") or task.get("target_ids", []))
            if all_targets:
                asyncio.create_task(resolve_targets_for_user(user_id, list(set(all_targets))))
        except Exception as e:
            logger.warning(f"[HealthCheck] Forward handler restore failed for {user_id}: {e}")
        try:
            if monitoring_tasks_cache.get(user_id):
                await reinitialize_monitoring_for_user(user_id)
        except Exception as e:
            logger.warning(f"[HealthCheck] Monitor handler restore failed for {user_id}: {e}")
        asyncio.create_task(_preload_user_dialogs(user_id, client, "reconnect"))
    except Exception as e:
        logger.warning(f"[HealthCheck] Recovery exception for user {user_id}: {e}")
    finally:
        _health_reconnect_in_progress.discard(user_id)


async def _ensure_client_healthy(user_id: int, client) -> bool:
    """
    Health check that guarantees the client is connected before returning.
    Accepts either a ClientWrapper or raw TelegramClient.
    - If disconnected: awaits _do_connect (creates fresh client, updates wrapper)
      then fires _do_recover as background task for heavy work.
    - If connected: returns True immediately — no network calls.
    Returns False only if connect timed out or failed — session validator
    will handle it at the next 30-minute mark.
    """
    if not client.is_connected():
        if time.time() - _health_reconnect_cooldown.get(user_id, 0) < _HEALTH_RECONNECT_COOLDOWN:
            return True
        if user_id not in _health_reconnect_in_progress:
            _health_reconnect_in_progress.add(user_id)
            wrapper = user_clients.get(user_id)
            if not isinstance(wrapper, ClientWrapper):
                _health_reconnect_in_progress.discard(user_id)
                return False
            connected = await _do_connect(user_id, wrapper)
            if connected:
                logger.info(f"[HealthCheck] Reconnect successful for user {user_id} — fresh client created")
                _health_reconnect_cooldown[user_id] = time.time()
                asyncio.create_task(_do_recover(user_id, wrapper))
            else:
                _health_reconnect_in_progress.discard(user_id)
                return False
    return True


async def forward_message_direct(user_id: int, target_id, message_text: str,
                                task_filters: dict, forward_tag: bool,
                                source_chat_id: int = None, message_id: int = None):
    in_flood_wait, wait_left, should_notify_end = flood_wait_manager.is_in_flood_wait(user_id)

    if should_notify_end:
        asyncio.create_task(notify_user_flood_wait_ended(user_id))

    if in_flood_wait:
        try:
            await flood_wait_queue.put((user_id, target_id, message_text, task_filters,
                                      forward_tag, source_chat_id, message_id,
                                      time.time() + wait_left))
            return False, True
        except asyncio.QueueFull:
            return False, False

    client = user_clients.get(user_id)
    if not client:
        return False, False

    if not await _ensure_client_healthy(user_id, client):
        return False, False

    try:
        await _consume_token(user_id, 1.0)

        entity = _get_cached_target(user_id, str(target_id))
        if not entity:
            entity = await resolve_target_entity_once(user_id, client, target_id)

        if not entity:
            try:
                await retry_queue.put((user_id, target_id, message_text, task_filters,
                                     forward_tag, source_chat_id, message_id))
                return False, True
            except asyncio.QueueFull:
                return False, False

        _, target_topic_id = parse_chat_id(target_id)

        try:
            if forward_tag and source_chat_id and message_id:
                try:
                    source_entity = _get_cached_target(user_id, str(source_chat_id))
                    if not source_entity:
                        source_entity = await client.get_input_entity(int(source_chat_id))
                        if source_entity:
                            _set_cached_target(user_id, str(source_chat_id), source_entity)
                    fwd_kwargs = {}
                    if target_topic_id is not None:
                        fwd_kwargs['reply_to'] = target_topic_id
                    await client.forward_messages(entity, message_id, source_entity, **fwd_kwargs)
                except Exception:
                    send_kwargs = {}
                    if target_topic_id is not None:
                        send_kwargs['reply_to'] = target_topic_id
                    await client.send_message(entity, message_text, **send_kwargs)
            else:
                send_kwargs = {}
                if target_topic_id is not None:
                    send_kwargs['reply_to'] = target_topic_id
                await client.send_message(entity, message_text, **send_kwargs)

            in_fw, _, should_notify_end = flood_wait_manager.is_in_flood_wait(user_id)
            flood_wait_manager.clear_flood_wait(user_id)
            asyncio.create_task(db_call(db.delete_flood_wait_state, user_id))
            if should_notify_end:
                asyncio.create_task(notify_user_flood_wait_ended(user_id))
            return True, False

        except FloodWaitError as fwe:
            wait = int(getattr(fwe, "seconds", 10))
            should_notify_start, wait_time = flood_wait_manager.set_flood_wait(user_id, wait)
            asyncio.create_task(db_call(db.save_flood_wait_state,
                user_id, time.time() + wait))

            try:
                await flood_wait_queue.put((user_id, target_id, message_text, task_filters,
                                          forward_tag, source_chat_id, message_id,
                                          time.time() + wait))

                if should_notify_start and wait_time > 30:
                    asyncio.create_task(notify_user_flood_wait(user_id, wait_time))

                return False, True
            except asyncio.QueueFull:
                return False, False

        except Exception:
            try:
                await retry_queue.put((user_id, target_id, message_text, task_filters,
                                     forward_tag, source_chat_id, message_id))
                return False, True
            except asyncio.QueueFull:
                return False, False

    except Exception:
        return False, False

def get_bot():
    """Return application.bot — the single, properly configured PTB bot instance."""
    if _application_ref is None:
        raise RuntimeError("get_bot() called before application was initialised in post_init")
    return _application_ref.bot

async def notify_user_flood_wait(user_id: int, wait_seconds: int):
    try:
        bot = get_bot()

        wait_minutes = wait_seconds // 60
        if wait_seconds % 60 > 0:
            wait_minutes += 1

        resume_time = format_datetime_utc1(datetime.fromtimestamp(time.time() + wait_seconds, tz=timezone.utc))

        message = f"""⏰ **Flood Wait Alert**

Your account is temporarily limited by Telegram.

📋 **Details:**
• Wait time: {wait_minutes} minutes
• Resumes at: {resume_time}

⚠️ **Please note:**
• This is a Telegram restriction, not the bot
• Bot will automatically resume when the wait ends
• No action is needed on your part

🔄 **Status:** ⏳ Waiting for Telegram to lift restriction..."""

        await bot.send_message(user_id, message, parse_mode="Markdown")

    except Exception as e:
        logger.debug(f"Failed to send flood wait notification to user {user_id}: {e}")

async def notify_user_flood_wait_ended(user_id: int):
    try:
        bot = get_bot()

        timestamp = format_datetime_utc1()

        message = f"""✅ **Flood Wait Ended**

Your account restriction has been lifted!

🕐 **Time:** {timestamp}

📋 **Status:**
• Forwarding has resumed automatically
• All queued messages are being sent
• You can now send messages normally

🔄 **Status:** ✅ Active and forwarding..."""

        await bot.send_message(user_id, message, parse_mode="Markdown")

    except Exception as e:
        logger.debug(f"Failed to send flood wait ended notification to user {user_id}: {e}")

async def immediate_worker_loop(worker_id: int):
    while True:
        try:
            user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = await immediate_queue.get()
            await forward_message_direct(user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id)
            immediate_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Immediate worker {worker_id} error: {e}")
            try:
                immediate_queue.task_done()
            except:
                pass

async def retry_worker_loop(worker_id: int):
    while True:
        try:
            user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = await retry_queue.get()
            await asyncio.sleep(RETRY_SLEEP)
            await forward_message_direct(user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id)
            retry_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Retry worker {worker_id} error: {e}")
            try:
                retry_queue.task_done()
            except:
                pass

async def flood_wait_worker_loop(worker_id: int):
    while True:
        try:
            user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id, ready_time = await flood_wait_queue.get()

            wait_left = ready_time - time.time()
            if wait_left > 0:
                await asyncio.sleep(wait_left)

            if user_id not in _flood_wait_send_locks:
                _flood_wait_send_locks[user_id] = asyncio.Lock()

            async with _flood_wait_send_locks[user_id]:
                gap = time.time() - _flood_wait_last_send.get(user_id, 0)
                if gap < 0.5:
                    await asyncio.sleep(0.5 - gap)
                await forward_message_direct(
                    user_id, target_id, message_text,
                    task_filters, forward_tag, source_chat_id, message_id
                )
                _flood_wait_last_send[user_id] = time.time()

            flood_wait_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Flood wait worker {worker_id} error: {e}")
            try:
                flood_wait_queue.task_done()
            except:
                pass

async def notification_worker_loop(worker_id: int):
    if notification_queue is None:
        return

    while True:
        try:
            user_id, task, chat_id, message_id, message_text, message_hash, duplicate_key = await notification_queue.get()

            settings = task.get("settings", {})
            if not settings.get("manual_reply_system", True):
                notification_queue.task_done()
                continue

            task_label = task.get("label", "Unknown")
            preview_text = message_text[:50] + "..." if len(message_text) > 50 else message_text

            bot_instance = get_bot()

            current_time = format_datetime_utc1()

            if duplicate_key not in duplicate_reply_counts:
                duplicate_reply_counts[duplicate_key] = 0
                asyncio.create_task(db_call(db.save_dup_count, duplicate_key, user_id, 0))

            current_count = duplicate_reply_counts[duplicate_key]

            escaped_task_label = escape_markdown(task_label, version=2)

            notification_msg = (
                f"🚨 **DUPLICATE MESSAGE DETECTED!**\n\n"
                f"**Task:** {escaped_task_label}\n"
                f"**Time:** {current_time}\n\n"
                f"📝 **Message Preview:**\n`{preview_text}`\n\n"
                f"💬 **To send a manual reply:** Swipe left on this message\n"
                f"⚠️ **Note:** Only {MAX_REPLIES_PER_DUPLICATE} replies allowed per duplicate message."
            )

            try:
                sent_message = await bot_instance.send_message(
                    chat_id=user_id,
                    text=notification_msg,
                    parse_mode="Markdown"
                )

                notification_messages[sent_message.message_id] = {
                    "user_id": user_id,
                    "task_label": task_label,
                    "chat_id": chat_id,
                    "original_message_id": message_id,
                    "duplicate_hash": message_hash,
                    "message_preview": preview_text,
                    "duplicate_key": duplicate_key
                }

                notification_creation_time[sent_message.message_id] = time.time()
                asyncio.create_task(db_call(db.save_notif_state,
                    sent_message.message_id, user_id,
                    notification_messages[sent_message.message_id],
                    notification_creation_time[sent_message.message_id]))

            except BadRequest as e:
                logger.error(f"Markdown error in notification: {e}")
                try:
                    sent_message = await bot_instance.send_message(
                        chat_id=user_id,
                        text=f"🚨 DUPLICATE DETECTED!\n\nTask: {task_label}\nTime: {current_time}\n\nPreview: {preview_text}\n\nReply to this message to send manual reply."
                    )

                    notification_messages[sent_message.message_id] = {
                        "user_id": user_id,
                        "task_label": task_label,
                        "chat_id": chat_id,
                        "original_message_id": message_id,
                        "duplicate_hash": message_hash,
                        "message_preview": preview_text,
                        "duplicate_key": duplicate_key
                    }

                    notification_creation_time[sent_message.message_id] = time.time()
                    asyncio.create_task(db_call(db.save_notif_state,
                        sent_message.message_id, user_id,
                        notification_messages[sent_message.message_id],
                        notification_creation_time[sent_message.message_id]))

                except Exception as e2:
                    logger.error(f"Failed to send notification even without markdown: {e2}")

            except Exception as e:
                logger.error(f"Failed to send notification: {e}")

            notification_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Notification worker {worker_id} error: {e}")
            try:
                notification_queue.task_done()
            except:
                pass


async def start_optimized_workers():
    global _workers_started, immediate_queue, retry_queue, flood_wait_queue, notification_queue
    global immediate_worker_tasks, retry_worker_tasks, flood_wait_worker_tasks, monitor_worker_tasks

    if _workers_started:
        return

    immediate_queue            = asyncio.Queue(maxsize=40000)
    retry_queue                = asyncio.Queue(maxsize=20000)
    flood_wait_queue           = asyncio.Queue(maxsize=10000)
    notification_queue         = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    for i in range(IMMEDIATE_WORKERS):
        task = asyncio.create_task(immediate_worker_loop(i + 1))
        immediate_worker_tasks.append(task)

    for i in range(RETRY_WORKERS):
        task = asyncio.create_task(retry_worker_loop(i + 1))
        retry_worker_tasks.append(task)

    for i in range(FLOOD_WAIT_WORKERS):
        task = asyncio.create_task(flood_wait_worker_loop(i + 1))
        flood_wait_worker_tasks.append(task)

    for i in range(MONITOR_WORKER_COUNT):
        task = asyncio.create_task(notification_worker_loop(i + 1))
        monitor_worker_tasks.append(task)

    _workers_started = True
    logger.info(f"✅ Started optimized workers:")
    logger.info(f"   • Immediate: {IMMEDIATE_WORKERS}, Retry: {RETRY_WORKERS}, Flood Wait: {FLOOD_WAIT_WORKERS}")
    logger.info(f"   • Notification: {MONITOR_WORKER_COUNT}")

async def monitor_queue_health():
    global immediate_queue, retry_queue, flood_wait_queue, notification_queue

    while True:
        try:
            current_time = time.time()
            if int(current_time) % 30 == 0:
                immediate_qsize = immediate_queue.qsize() if immediate_queue else 0
                retry_qsize = retry_queue.qsize() if retry_queue else 0
                flood_qsize = flood_wait_queue.qsize() if flood_wait_queue else 0
                notify_qsize = notification_queue.qsize() if notification_queue else 0

                logger.info(f"📊 Queue Status: Immediate={immediate_qsize}, Retry={retry_qsize}, "
                           f"Flood={flood_qsize}, Notify={notify_qsize}")

                if immediate_qsize > 8000:
                    logger.warning(f"Immediate queue nearly full: {immediate_qsize}/10000")
                if retry_qsize > 4000:
                    logger.warning(f"Retry queue nearly full: {retry_qsize}/5000")
                if flood_qsize > 2000:
                    logger.warning(f"Flood wait queue nearly full: {flood_qsize}/2500")
                if notify_qsize > 900:
                    logger.warning(f"Notification queue nearly full: {notify_qsize}/{SEND_QUEUE_MAXSIZE}")

            await asyncio.sleep(5)

        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(5)

async def performance_logger():
    while True:
        try:
            immediate_qsize = immediate_queue.qsize() if immediate_queue else 0
            retry_qsize = retry_queue.qsize() if retry_queue else 0
            flood_qsize = flood_wait_queue.qsize() if flood_wait_queue else 0
            notify_qsize = notification_queue.qsize() if notification_queue else 0

            active_users = len(user_clients)
            forward_tasks = sum(len(tasks) for tasks in forwarding_tasks_cache.values())
            monitor_tasks = sum(len(tasks) for tasks in monitoring_tasks_cache.values())

            logger.info(f"📈 Performance: Users={active_users}, Forward={forward_tasks}, "
                       f"Monitor={monitor_tasks}")
            logger.info(f"📦 Queues: Immediate={immediate_qsize}, Retry={retry_qsize}, "
                       f"Flood={flood_qsize}, Notify={notify_qsize}")

            await asyncio.sleep(60)

        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(60)


async def _wait_until_next_half_hour():
    """Sleep until the next :00 or :30 mark (UTC), giving a check every 30 minutes."""
    now = datetime.now(timezone.utc)
    if now.minute < 30:
        nxt = now.replace(minute=30, second=0, microsecond=0)
    else:
        nxt = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    wait_secs = max(1.0, (nxt - now).total_seconds())
    logger.info(f"[SessionValidator] Next run at {nxt.strftime('%H:%M UTC')} — sleeping {wait_secs:.0f}s")
    await asyncio.sleep(wait_secs)


async def periodic_session_validator():
    """Runs at every :00 and :30 UTC: validates sessions and restores any missing ones."""
    await _wait_until_next_half_hour()

    while True:
        try:
            logger.info("🔍 [SessionValidator] Starting session validation...")

            stale_users: List[int] = []
            for uid, client in list(user_clients.items()):
                try:
                    if not client.is_connected():
                        await client.connect()

                    if not await client.is_user_authorized():
                        stale_users.append(uid)
                        logger.warning(f"[SessionValidator] Session invalid for user {uid} — marking offline")
                    else:
                        logger.debug(f"[SessionValidator] Session OK for user {uid}")
                except Exception as e:
                    logger.warning(f"[SessionValidator] Could not verify user {uid}: {e} — treating as stale")
                    stale_users.append(uid)

            for uid in stale_users:
                try:
                    client = user_clients.pop(uid, None)
                    if client:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass

                    if uid in monitor_handler_registered:
                        del monitor_handler_registered[uid]

                    await db_call(db.save_user, uid, None, None, None, False)

                    try:
                        bot = get_bot()
                        await bot.send_message(
                            uid,
                            "⚠️ **Session Expired**\n\n"
                            "Your Telegram session has expired or been revoked.\n\n"
                            "Please use /login to reconnect your account.",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass

                    logger.info(f"[SessionValidator] Cleaned up stale session for user {uid}")
                except Exception as e:
                    logger.error(f"[SessionValidator] Error cleaning up user {uid}: {e}")

            try:
                db_users = await db_call(db.get_logged_in_users, MAX_CONCURRENT_USERS * 2)
            except Exception as e:
                logger.error(f"[SessionValidator] Could not fetch DB users: {e}")
                db_users = []

            new_sessions: List[Tuple[int, str]] = []
            for row in db_users:
                try:
                    uid = row.get("user_id") if isinstance(row, dict) else row[0]
                    sess = row.get("session_data") if isinstance(row, dict) else row[1]
                except Exception:
                    continue
                if uid not in user_clients and sess and is_user_allowed(uid):
                    new_sessions.append((uid, sess))

            if new_sessions:
                logger.info(f"[SessionValidator] Restoring {len(new_sessions)} new DB session(s)")
                batch_size = 10
                for i in range(0, len(new_sessions), batch_size):
                    batch = new_sessions[i:i + batch_size]
                    results = await asyncio.gather(
                        *[restore_single_session(uid, sess) for uid, sess in batch],
                        return_exceptions=True
                    )
                    for (uid, _), result in zip(batch, results):
                        if isinstance(result, Exception):
                            logger.error(f"[SessionValidator] Failed to restore user {uid}: {result}")
                        elif result:
                            try:
                                target_entity_cache.setdefault(uid, OrderedDict())
                                chat_entity_cache.setdefault(uid, {})
                                _ensure_user_send_semaphore(uid)
                                _ensure_user_rate_limiter(uid)
                                if forwarding_tasks_cache.get(uid):
                                    await reinitialize_forwarding_for_user(uid)
                                if monitoring_tasks_cache.get(uid):
                                    await reinitialize_monitoring_for_user(uid)
                            except Exception as e:
                                logger.error(f"[SessionValidator] Task init error for user {uid}: {e}")
                    await asyncio.sleep(0.5)

            removed = len(stale_users)
            added = sum(1 for (uid, _) in new_sessions if uid in user_clients)
            logger.info(
                f"✅ [SessionValidator] Done. Removed={removed} stale, "
                f"Added={added} new, Active={len(user_clients)}"
            )

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"[SessionValidator] Unexpected error: {e}")

        await _wait_until_next_half_hour()

async def restore_sessions():
    logger.info("🔄 Restoring sessions...")

    forwarding_tasks_cache.clear()
    monitoring_tasks_cache.clear()

    try:
        all_forward_active = await db_call(db.get_all_active_forwarding_tasks)
        logger.info(f"Loaded {len(all_forward_active)} active forwarding tasks")
    except Exception as e:
        logger.error(f"Error loading forwarding tasks: {e}")
        all_forward_active = []

    try:
        all_monitor_active = await db_call(db.get_all_active_monitoring_tasks)
        logger.info(f"Loaded {len(all_monitor_active)} active monitoring tasks")
    except Exception as e:
        logger.error(f"Error loading monitoring tasks: {e}")
        all_monitor_active = []

    for t in all_forward_active:
        uid = t["user_id"]
        forwarding_tasks_cache.setdefault(uid, []).append({
            "id": t["id"],
            "label": t["label"],
            "source_ids": t["source_ids"],
            "target_ids": t["target_ids"],
            "active_source_ids": t.get("active_source_ids") or t["source_ids"],
            "active_target_ids": t.get("active_target_ids") or t["target_ids"],
            "is_active": 1,
            "filters": t.get("filters", {})
        })

    for t in all_monitor_active:
        uid = t["user_id"]
        monitoring_tasks_cache.setdefault(uid, []).append({
            "id": t["id"],
            "label": t["label"],
            "chat_ids": t["chat_ids"] if t["chat_ids"] else [],
            "is_active": 1,
            "settings": t.get("settings", {})
        })

    try:
        users = await db_call(db.get_logged_in_users, MAX_CONCURRENT_USERS * 2)
        logger.info(f"Found {len(users)} logged-in users in database")
    except Exception as e:
        logger.error(f"Error fetching logged-in users: {e}")
        users = []

    batch_size = 10
    restore_tasks = []
    db_restored = []

    for row in users:
        try:
            user_id = row.get("user_id") if isinstance(row, dict) else row[0]
            session_data = row.get("session_data") if isinstance(row, dict) else row[1]
        except Exception as e:
            logger.error(f"Error parsing user row: {e}")
            continue

        if user_id in user_clients:
            continue

        if not is_user_allowed(user_id):
            logger.info(f"restore_sessions: skipping DB user {user_id} — not in ALLOWED_USER_IDS")
            continue

        if session_data:
            logger.info(f"Queueing session restore for user {user_id}")
            restore_tasks.append((user_id, session_data))

        if len(restore_tasks) >= batch_size:
            results = await asyncio.gather(
                *[restore_single_session(uid, sess) for uid, sess in restore_tasks],
                return_exceptions=True
            )

            for (uid, sess), result in zip(restore_tasks, results):
                if isinstance(result, Exception):
                    logger.error(f"Error restoring session for user {uid}: {result}")
                elif result:
                    db_restored.append(uid)

            restore_tasks = []
            await asyncio.sleep(0.5)

    if restore_tasks:
        results = await asyncio.gather(
            *[restore_single_session(uid, sess) for uid, sess in restore_tasks],
            return_exceptions=True
        )

        for (uid, sess), result in zip(restore_tasks, results):
            if isinstance(result, Exception):
                logger.error(f"Error restoring session for user {uid}: {result}")
            elif result:
                db_restored.append(uid)

    logger.info("🔍 Validating and initializing restored tasks...")
    all_restored_users = set(db_restored)

    for user_id in all_restored_users:
        if user_id in user_clients:
            target_entity_cache.setdefault(user_id, OrderedDict())
            chat_entity_cache.setdefault(user_id, {})
            _ensure_user_send_semaphore(user_id)
            _ensure_user_rate_limiter(user_id)

            if user_id in forwarding_tasks_cache and forwarding_tasks_cache[user_id]:
                logger.info(f"Initializing {len(forwarding_tasks_cache[user_id])} forwarding tasks for user {user_id}")
                try:
                    await reinitialize_forwarding_for_user(user_id)
                except Exception as e:
                    logger.error(f"Failed to initialize forwarding for user {user_id}: {e}")

            if user_id in monitoring_tasks_cache and monitoring_tasks_cache[user_id]:
                logger.info(f"Initializing {len(monitoring_tasks_cache[user_id])} monitoring tasks for user {user_id}")
                try:
                    await reinitialize_monitoring_for_user(user_id)
                except Exception as e:
                    logger.error(f"Failed to initialize monitoring for user {user_id}: {e}")

    logger.info(f"✅ Session restoration complete. Active users: {len(user_clients)}, "
                f"Restored from DB: {len(db_restored)}")

    try:
        _notif_rows = await db_call(db.load_notif_states)
        for _row in _notif_rows:
            _mid  = _row["msg_id"]
            _data = _row["data"] if isinstance(_row["data"], dict) else json.loads(_row["data"])
            notification_messages[_mid]      = _data
            notification_creation_time[_mid] = float(_row["created_at"])
        logger.info(f"[Restore] Loaded {len(_notif_rows)} notification state entries")
    except Exception as _re:
        logger.warning(f"[Restore] Could not restore notification state: {_re}")

    try:
        _dc_rows = await db_call(db.load_dup_counts)
        for _row in _dc_rows:
            duplicate_reply_counts[_row["duplicate_key"]] = int(_row["count"])
        logger.info(f"[Restore] Loaded {len(_dc_rows)} duplicate count entries")
    except Exception as _re:
        logger.warning(f"[Restore] Could not restore duplicate counts: {_re}")

    try:
        _mh_rows = await db_call(db.load_msg_hashes, DUPLICATE_CHECK_WINDOW)
        for _row in _mh_rows:
            _key = (int(_row["user_id"]), int(_row["chat_id"]))
            if _key not in message_history:
                message_history[_key] = deque(maxlen=MESSAGE_HASH_LIMIT)
            message_history[_key].append((_row["msg_hash"], float(_row["seen_at"]), _row["msg_text"] or ""))
        logger.info(f"[Restore] Loaded {len(_mh_rows)} message hash entries across {len(message_history)} chat keys")
    except Exception as _re:
        logger.warning(f"[Restore] Could not restore message history: {_re}")

    try:
        _fw_rows = await db_call(db.load_flood_wait_states)
        _fw_dict = {int(r["user_id"]): float(r["wait_until"]) for r in _fw_rows}
        flood_wait_manager.restore_flood_waits(_fw_dict)
        logger.info(f"[Restore] Restored {len(_fw_dict)} active flood wait entries")
    except Exception as _re:
        logger.warning(f"[Restore] Could not restore flood wait state: {_re}")

async def restore_single_session(user_id: int, session_data: str) -> bool:
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            await db_call(db.save_user, user_id, None, None, None, False)
            await client.disconnect()
            return False

        if len(user_clients) >= MAX_CONCURRENT_USERS:
            try:
                await client.disconnect()
            except Exception:
                pass
            await db_call(db.save_user, user_id, None, None, None, True)
            logger.warning(f"Max users reached, skipping restoration for user {user_id}")
            return False

        user_clients[user_id] = ClientWrapper(client)

        try:
            me = await client.get_me()
            user_name = me.first_name or "User"

            user = await db_call(db.get_user, user_id)
            has_phone = user and user.get("phone")

            await db_call(db.save_user, user_id,
                        user["phone"] if user else None,
                        user_name,
                        session_data,
                        True)

            logger.info(f"✅ Restored session for user {user_id} ({user_name}) from database")

            asyncio.create_task(_preload_user_dialogs(user_id, client, user_name))

            return True

        except Exception as e:
            logger.exception(f"Error in restore_single_session for user {user_id}: {e}")
            return False

    except Exception as e:
        logger.exception(f"Failed to restore session for user {user_id}: {e}")
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            pass
        return False

async def safe_bot_send_fordet(bot, chat_id: int, text: str, **kwargs) -> bool:
    """Send with up to 3 retries, silently ignore permanent failures."""
    from telegram.error import Forbidden, BadRequest
    MAX_TRIES = 3
    for attempt in range(1, MAX_TRIES + 1):
        try:
            await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            return True
        except Forbidden:
            return False
        except BadRequest as e:
            logger.warning("safe_bot_send_fordet BadRequest for %s: %s | text[:120]=%r", chat_id, e, text[:120])
            return False
        except Exception as e:
            if attempt < MAX_TRIES:
                await asyncio.sleep(0.5 * attempt)
            else:
                logger.warning("safe_bot_send_fordet failed for %s: %s", chat_id, e)
                return False
    return False


class WebServer:

    def __init__(self, port: int = 5000):
        self.port = port
        self.app = Flask(__name__)
        self.start_time = time.time()
        self._monitor_callback = None
        self._cached_container_limit_mb = None
        self.setup_routes()

    def register_monitoring(self, callback):
        self._monitor_callback = callback
        logger.info("Monitoring callback registered")

    def _mb_from_bytes(self, n_bytes: int) -> float:
        return round(n_bytes / (1024 * 1024), 2)

    def _read_cgroup_memory_limit_bytes(self) -> int:
        candidates = [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]

        for path in candidates:
            try:
                if not os.path.exists(path):
                    continue
                with open(path, "r") as fh:
                    raw = fh.read().strip()
                if raw == "max":
                    return 0
                val = int(raw)
                if val <= 0:
                    return 0
                if val > (1 << 50):
                    return 0
                return val
            except Exception:
                continue

        try:
            with open("/proc/self/cgroup", "r") as fh:
                lines = fh.read().splitlines()
            for ln in lines:
                parts = ln.split(":")
                if len(parts) >= 3:
                    controllers = parts[1]
                    cpath = parts[2]
                    if "memory" in controllers.split(","):
                        possible = f"/sys/fs/cgroup/memory{cpath}/memory.limit_in_bytes"
                        if os.path.exists(possible):
                            with open(possible, "r") as fh:
                                raw = fh.read().strip()
                            val = int(raw)
                            if val > 0 and val < (1 << 50):
                                return val
                        possible2 = f"/sys/fs/cgroup{cpath}/memory.max"
                        if os.path.exists(possible2):
                            with open(possible2, "r") as fh:
                                raw = fh.read().strip()
                            if raw != "max":
                                val = int(raw)
                                if val > 0 and val < (1 << 50):
                                    return val
        except Exception:
            pass

        return 0

    def get_container_memory_limit_mb(self) -> float:
        if self._cached_container_limit_mb is not None:
            return self._cached_container_limit_mb

        bytes_limit = self._read_cgroup_memory_limit_bytes()
        if bytes_limit and bytes_limit > 0:
            self._cached_container_limit_mb = self._mb_from_bytes(bytes_limit)
        else:
            self._cached_container_limit_mb = float(os.getenv("CONTAINER_MAX_RAM_MB", str(DEFAULT_CONTAINER_MAX_RAM_MB)))
        return self._cached_container_limit_mb

    def setup_routes(self):

        @self.app.route("/", methods=["GET"])
        def home():
            container_limit = self.get_container_memory_limit_mb()
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Forwarder + DuoDetective Bot Status</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        text-align: center;
                        padding: 50px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                    }}
                    .status {{
                        background: rgba(255,255,255,0.1);
                        padding: 30px;
                        border-radius: 15px;
                        max-width: 600px;
                        margin: 0 auto;
                        text-align: left;
                    }}
                    h1 {{ font-size: 2.2em; margin: 0; text-align: center; }}
                    p {{ font-size: 1.0em; }}
                    .emoji {{ font-size: 2.5em; text-align: center; }}
                    .stats {{ font-family: monospace; margin-top: 12px; }}
                </style>
            </head>
            <body>
                <div class="status">
                    <div class="emoji">🤖🔍</div>
                    <h1>Forwarder + DuoDetective Bot</h1>
                    <p>Combined bot is running. Use the monitoring endpoints:</p>
                    <ul>
                      <li>/health — basic uptime</li>
                      <li>/metrics — system metrics</li>
                      <li>/health/detailed — detailed system status</li>
                    </ul>
                    <div class="stats">
                      <strong>Container memory limit (detected):</strong> {container_limit} MB
                    </div>
                </div>
            </body>
            </html>
            """
            return html

        @self.app.route("/health", methods=["GET"])
        def health():
            uptime = int(time.time() - self.start_time)
            return jsonify({"status": "healthy", "uptime_seconds": uptime}), 200

        @self.app.route("/health/detailed", methods=["GET"])
        def health_detailed():
            return jsonify({
                "status": "healthy",
                "uptime_seconds": int(time.time() - self.start_time),
                "users": len(user_clients),
                "forwarding_tasks": sum(len(t) for t in forwarding_tasks_cache.values()),
                "monitoring_tasks": sum(len(t) for t in monitoring_tasks_cache.values()),
                "queues": {
                    "immediate": immediate_queue.qsize() if immediate_queue else 0,
                    "retry": retry_queue.qsize() if retry_queue else 0,
                    "flood_wait": flood_wait_queue.qsize() if flood_wait_queue else 0,
                    "notification": notification_queue.qsize() if notification_queue else 0
                }
            })

        @self.app.route("/metrics", methods=["GET"])
        def metrics():
            if self._monitor_callback is None:
                return jsonify({"status": "unavailable", "reason": "no monitor registered"}), 200

            try:
                data = self._monitor_callback()
                return jsonify({"status": "ok", "metrics": data}), 200
            except Exception as e:
                logger.exception("Monitoring callback failed")
                return jsonify({"status": "error", "error": str(e)}), 500

    def run_server(self):
        self.app.run(host="0.0.0.0", port=self.port, debug=False, use_reloader=False, threaded=True)

    def start(self):
        server_thread = threading.Thread(target=self.run_server, daemon=True)
        server_thread.start()
        logger.info(f"🌐 Web server started on port {self.port}")

web_server = WebServer(port=WEB_SERVER_PORT)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")

    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = ''.join(tb_list)
    logger.error(f"Traceback:\n{tb_string}")

    if update and update.effective_chat:
        try:
            await update.effective_chat.send_message(
                "❌ An error occurred while processing your request. Please try again later.",
                parse_mode="Markdown"
            )
        except Exception:
            pass

async def shutdown_cleanup():
    logger.info("Shutdown cleanup...")

    for task_list in [immediate_worker_tasks, retry_worker_tasks,
                      flood_wait_worker_tasks, monitor_worker_tasks]:
        for t in list(task_list):
            try:
                t.cancel()
            except Exception:
                pass

    all_tasks = (immediate_worker_tasks + retry_worker_tasks +
                 flood_wait_worker_tasks + monitor_worker_tasks)
    if all_tasks:
        try:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        except Exception:
            pass

    user_ids = list(user_clients.keys())
    batch_size = 10
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        disconnect_tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if not client:
                continue

            if uid in forward_handler_registered:
                handler = forward_handler_registered.get(uid)
                if handler:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                forward_handler_registered.pop(uid, None)

            if uid in monitor_handler_registered:
                for handler in monitor_handler_registered[uid]:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                monitor_handler_registered.pop(uid, None)

            try:
                disconnect_tasks.append(client.disconnect())
            except Exception:
                try:
                    sess = getattr(client, "session", None)
                    if sess is not None:
                        try:
                            sess.close()
                        except Exception:
                            pass
                except Exception:
                    pass

        if disconnect_tasks:
            try:
                await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            except Exception:
                pass

    user_clients.clear()
    forwarding_tasks_cache.clear()
    monitoring_tasks_cache.clear()
    phone_verification_states.clear()
    target_entity_cache.clear()
    chat_entity_cache.clear()
    user_send_semaphores.clear()
    _flood_wait_send_locks.clear()
    _flood_wait_last_send.clear()
    user_rate_limiters.clear()
    message_history.clear()
    notification_messages.clear()
    notification_creation_time.clear()
    reply_states.clear()
    auto_reply_states.clear()
    duplicate_reply_counts.clear()

    try:
        db.close_connection()
    except Exception:
        pass

    logger.info("Shutdown cleanup complete.")


async def post_init(application: Application):
    global MAIN_LOOP, _application_ref
    MAIN_LOOP = asyncio.get_running_loop()
    _application_ref = application

    logger.info("🔧 Initializing...")

    await application.bot.delete_webhook(drop_pending_updates=True)
    logger.info("🧹 Cleared webhooks")

    asyncio.create_task(_db_pool_reporter())
    logger.info("📊 DB pool reporter started (logs every 20s)")

    def _signal_handler(sig_num, frame):
        logger.info(f"Signal {sig_num} received")
        try:
            if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                future = asyncio.run_coroutine_threadsafe(_graceful_shutdown(application), MAIN_LOOP)
                try:
                    future.result(timeout=30)
                except Exception:
                    pass
        except Exception:
            pass

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass

    await start_optimized_workers()

    await restore_sessions()

    asyncio.create_task(monitor_queue_health())
    asyncio.create_task(performance_logger())
    asyncio.create_task(periodic_session_validator())
    asyncio.create_task(cleanup_old_reply_counts())
    asyncio.create_task(cleanup_old_notifications())
    asyncio.create_task(cleanup_old_messages_task())

    async def _collect_metrics():
        try:
            return {
                "queues": {
                    "immediate": immediate_queue.qsize() if immediate_queue else 0,
                    "retry": retry_queue.qsize() if retry_queue else 0,
                    "flood_wait": flood_wait_queue.qsize() if flood_wait_queue else 0,
                    "notification": notification_queue.qsize() if notification_queue else 0
                },
                "system": {
                    "active_users": len(user_clients),
                    "forwarding_tasks": sum(len(tasks) for tasks in forwarding_tasks_cache.values()),
                    "monitoring_tasks": sum(len(tasks) for tasks in monitoring_tasks_cache.values()),
                    "max_users": MAX_CONCURRENT_USERS
                },
                "workers": {
                    "immediate": IMMEDIATE_WORKERS,
                    "retry": RETRY_WORKERS,
                    "flood_wait": FLOOD_WAIT_WORKERS,
                }
            }
        except Exception as e:
            return {"error": f"failed to collect metrics: {e}"}

    def _forward_metrics():
        if MAIN_LOOP is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
                return future.result(timeout=1.0)
            except Exception:
                return {"error": "failed to collect metrics"}
        else:
            return {"error": "bot main loop not available"}

    try:
        web_server.register_monitoring(_forward_metrics)
    except Exception:
        pass

    web_server.start()

    gc.freeze()
    logger.info(f"🧊 GC freeze applied — {gc.get_freeze_count()} objects excluded from future collections")

    logger.info("✅ Bot initialized!")

async def _graceful_shutdown(application: Application):
    try:
        await shutdown_cleanup()
    except Exception:
        pass
    try:
        await application.stop()
    except Exception:
        pass


def _save_notif_state(self, msg_id, user_id, data, created_at):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("""
            INSERT INTO notification_state (msg_id, user_id, data, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (msg_id) DO UPDATE
            SET data=EXCLUDED.data, created_at=EXCLUDED.created_at
        """, (msg_id, user_id, json.dumps(data), created_at))
    conn.commit()

def _delete_notif_state(self, msg_id):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("DELETE FROM notification_state WHERE msg_id=%s", (msg_id,))
    conn.commit()

def _load_notif_states(self):
    conn = self.get_connection()
    cutoff = time.time() - 3600
    with conn.cursor() as c:
        c.execute("""
            SELECT msg_id, user_id, data, created_at
            FROM notification_state WHERE created_at > %s
        """, (cutoff,))
        return c.fetchall()


def _save_dup_count(self, key, user_id, count):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("""
            INSERT INTO duplicate_counts_state (duplicate_key, user_id, count, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (duplicate_key) DO UPDATE
            SET count=EXCLUDED.count, updated_at=EXCLUDED.updated_at
        """, (key, user_id, count, time.time()))
    conn.commit()

def _delete_dup_count(self, key):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("DELETE FROM duplicate_counts_state WHERE duplicate_key=%s", (key,))
    conn.commit()

def _load_dup_counts(self):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("SELECT duplicate_key, user_id, count FROM duplicate_counts_state")
        return c.fetchall()


def _append_msg_hash(self, user_id, chat_id, msg_hash, seen_at, msg_text):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("""
            INSERT INTO message_hash_history (user_id, chat_id, msg_hash, seen_at, msg_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, chat_id, msg_hash, seen_at, msg_text))
    conn.commit()

def _load_msg_hashes(self, window_secs):
    conn = self.get_connection()
    cutoff = time.time() - window_secs
    with conn.cursor() as c:
        c.execute("""
            SELECT user_id, chat_id, msg_hash, seen_at, msg_text
            FROM message_hash_history WHERE seen_at > %s
            ORDER BY user_id, chat_id, seen_at ASC
        """, (cutoff,))
        return c.fetchall()

def _cleanup_old_msg_hashes(self, cutoff):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("DELETE FROM message_hash_history WHERE seen_at < %s", (cutoff,))
        deleted = c.rowcount
    conn.commit()
    return deleted


def _save_flood_wait_state(self, user_id, wait_until):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("""
            INSERT INTO flood_wait_state (user_id, wait_until)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET wait_until=EXCLUDED.wait_until
        """, (user_id, wait_until))
    conn.commit()

def _delete_flood_wait_state(self, user_id):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("DELETE FROM flood_wait_state WHERE user_id=%s", (user_id,))
    conn.commit()

def _load_flood_wait_states(self):
    conn = self.get_connection()
    with conn.cursor() as c:
        c.execute("SELECT user_id, wait_until FROM flood_wait_state WHERE wait_until > %s",
                  (time.time(),))
        return c.fetchall()


Database.save_notif_state            = _save_notif_state
Database.delete_notif_state          = _delete_notif_state
Database.load_notif_states           = _load_notif_states
Database.save_dup_count              = _save_dup_count
Database.delete_dup_count            = _delete_dup_count
Database.load_dup_counts             = _load_dup_counts
Database.append_msg_hash             = _append_msg_hash
Database.load_msg_hashes             = _load_msg_hashes
Database.cleanup_old_msg_hashes      = _cleanup_old_msg_hashes
Database.save_flood_wait_state       = _save_flood_wait_state
Database.delete_flood_wait_state     = _delete_flood_wait_state
Database.load_flood_wait_states      = _load_flood_wait_states


def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting Forwarder + DuoDetective Bot...")
    logger.info(f"📊 Worker Configuration:")
    logger.info(f"   • Forwarding: {IMMEDIATE_WORKERS} immediate, {RETRY_WORKERS} retry, {FLOOD_WAIT_WORKERS} flood wait")
    if ALLOWED_USER_IDS:
        logger.info(f"🔒 Access restricted to {len(ALLOWED_USER_IDS)} allowed user ID(s)")
    else:
        logger.info("🔓 ALLOWED_USER_IDS not set — bot is open to every Telegram user")

    from telegram.request import HTTPXRequest

    _request = HTTPXRequest(
        connection_pool_size=400,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=15,
        pool_timeout=30,
    )
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(_request)
        .get_updates_request(HTTPXRequest(connection_pool_size=24, read_timeout=60))
        .post_init(post_init)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("monitoradd", monitoradd_command))
    application.add_handler(CommandHandler("monitortasks", monitortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_all_text_messages))

    application.add_error_handler(error_handler)

    logger.info("✅ Bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        loop_to_use = None
        try:
            if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                loop_to_use = MAIN_LOOP
            else:
                try:
                    running_loop = asyncio.get_running_loop()
                    if getattr(running_loop, "is_running", lambda: False)():
                        loop_to_use = running_loop
                except RuntimeError:
                    loop_to_use = None
        except Exception:
            loop_to_use = None

        if loop_to_use:
            try:
                future = asyncio.run_coroutine_threadsafe(shutdown_cleanup(), loop_to_use)
                future.result(timeout=30)
            except Exception:
                pass
        else:
            tmp_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(tmp_loop)
                tmp_loop.run_until_complete(shutdown_cleanup())
            finally:
                try:
                    tmp_loop.close()
                except Exception:
                    pass

if __name__ == "__main__":
    main()

