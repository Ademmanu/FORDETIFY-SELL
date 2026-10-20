# 📨🔍 Forwarder + DuoDetective Bot

A self-hosted **Telegram bot** that works through your own Telegram account to:

- **Forward** messages automatically from chosen chats to other chats, with filters.
- **Monitor** chats for **duplicate messages**, alert you instantly, and reply to the duplicate with one swipe or automatically.

Everything is controlled from inside Telegram with buttons and simple commands. No coding is needed once it is running. Multiple people can use one bot, and each person gets their own private tasks.

---

## ✨ Features

- Multi-user: every user connects their own Telegram account and manages their own tasks
- Forwarding tasks with many sources and many targets
- Message filters: raw text, numbers only, letters only, strip letters, strip numbers
- Optional prefix and suffix added to every forwarded message
- Supports forum topics (`-100123456789:36615`)
- Real "Forwarded from" tag on or off, pause and resume at any time
- Duplicate detection per chat, with instant alerts
- Manual reply (swipe on the alert) or automatic reply to duplicates
- Fast queued sending with retries, rate limiting and automatic flood-wait handling
- Sessions and tasks stored in PostgreSQL and restored automatically after a restart
- Health endpoints at `/health`, `/health/detailed` and `/metrics`

---

## 🚀 Quick start

**You need:** a Telegram bot token from [@BotFather](https://t.me/BotFather), an `API_ID` and `API_HASH` from [my.telegram.org](https://my.telegram.org), and a PostgreSQL database.

1. Copy `.env.example` to `.env` and fill in `BOT_TOKEN`, `API_ID`, `API_HASH` and `DATABASE_URL`.
2. Start the bot with either:
   - **Docker:** `docker build -t fordet .` then `docker run --env-file .env -p 5000:5000 fordet`
   - **Python 3.11+:** `pip install -r requirements.txt` then `python fordet.py`
3. Open your bot in Telegram and send `/start`.

Tables are created automatically on first run.

---

## 📖 Using the bot

| Command | What it does |
|---|---|
| `/start` | Main menu |
| `/login` / `/logout` | Connect or disconnect your Telegram account |
| `/forwadd` / `/fortasks` | Create or manage forwarding tasks |
| `/monitoradd` / `/monitortasks` | Create or manage duplicate-monitoring tasks |
| `/getallid` | Find the ID of any of your chats |

### 1. Logging in

Send `/login` and follow the three steps:

1. **Phone number:** send it with the country code, for example `+1234567890`.
2. **Verification code:** Telegram sends you a code. Reply with the word `verify` followed by the 5-digit code, with no spaces. If the code is `54321`, send `verify54321`.
3. **2-step verification (only if enabled on your account):** reply with `password` followed by your password, with no spaces. If it is `mypass123`, send `passwordmypass123`.

When it succeeds, the bot confirms your name and phone, and you can start creating tasks. Use the **Cancel** button at any step to stop. Your login is saved, so you stay connected after the bot restarts.

### 2. Logging out

Send `/logout`. To confirm, type your connected phone number exactly as shown. This disconnects your account and **deletes your saved session, forwarding tasks and monitoring tasks**. Use `/login` to connect again later.

### 3. Forwarding messages

**Find your chat IDs first:** send `/getallid`, choose Bots, Channels, Groups or Private, and copy the IDs you need.

**Create a task:** send `/forwadd` and answer three prompts:

1. **Name** for the task (it must be unique).
2. **Source chat ID(s):** the chats to watch. Separate several IDs with spaces.
3. **Target chat ID(s):** where messages should be sent. Separate several IDs with spaces.

Forum topics work too. Use `chatID:topicID`, for example `-100123456789:36615`.

**Manage tasks:** send `/fortasks`, then tap a task to change it:

- **🔍 Filters:** choose what gets forwarded. You can combine them.
  - *Raw text:* forward text as it is (on by default)
  - *Numbers only:* forward only the numbers
  - *Alphabets only:* forward only the letters
  - *Removed Alphabetic:* keep letters and symbols, remove numbers and emojis
  - *Removed Numeric:* keep numbers and symbols, remove letters and emojis
  - *📝 Prefix / Suffix:* add your own text before or after every message
- **Outgoing:** include or skip messages that you send yourself
- **Forward Tag:** show or hide the "Forwarded from" label
- **Control:** pause or resume the task without deleting it
- **✏️ Edit:** rename the task (up to 60 characters), change sources or targets, or switch individual sources and targets on or off
- **🗑️ Delete:** remove the task

### 4. Monitoring for duplicates

**Create a task:** send `/monitoradd` and answer two prompts:

1. **Name** for the task.
2. **Chat ID(s)** to monitor, separated by spaces.

A message counts as a **duplicate** when the same person sends the same text (ignoring capital letters) in the same chat within 15 minutes.

**When a duplicate appears,** the bot sends you a **🚨 DUPLICATE MESSAGE DETECTED!** alert with the task name, the time, and a preview of the message.

- **Reply manually:** swipe left on the alert and type your reply. It is posted in the monitored chat as a reply to the duplicate message, from your account.
- **Reply automatically:** turn on **Auto Reply** and enter your message once. The bot then replies for you and tells you each time.
- Each duplicate message allows up to **3 replies** in total.

**Manage tasks:** send `/monitortasks` and tap a task to switch these on or off:

- **Check Duplicate:** detect duplicates and send alerts (on by default)
- **Manual Reply:** allow swipe replies on alerts (on by default)
- **Auto Reply:** reply automatically with your saved message (off by default)
- **Monitor Outgoing:** also check messages that you send (on by default)
- **✏️ Edit** to rename the task or change its chats, and **🗑️ Delete** to remove it

**Optional allow-list:** set `ALLOWED_USER_IDS=123456789,987654321` in your `.env` to restrict the bot to those Telegram user IDs, or leave it unset to let anyone use it.

---

## ⚙️ Settings

All settings are environment variables. `.env.example` lists the required ones and the optional performance settings, such as worker counts and the duplicate check window.

## 🔒 Notes

- The bot acts through the connected user's real Telegram account. Use it responsibly and follow [Telegram's terms](https://telegram.org/tos).
- Connected sessions are stored in your PostgreSQL database, so keep that database private and secured.
- Timestamps shown in the bot use UTC+1.
