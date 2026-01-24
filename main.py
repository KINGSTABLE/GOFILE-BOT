#!/usr/bin/env python3

import os
import aiohttp
import asyncio
import time
import requests
import mimetypes
import logging
import uvloop
from datetime import datetime
import sqlite3
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrofork import Client, filters, idle
from pyrofork.types import InlineKeyboardMarkup, InlineKeyboardButton
from asyncio import Queue, Lock
from aiohttp import web

# ================== SPEED OPTIMIZATION ==================
# Install uvloop to make asyncio 2-4x faster
uvloop.install()

# ================== CONFIGURATION ==================

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN")

# Helper to fix Channel IDs
def sanitize_channel_id(value):
    try:
        val = int(value)
        if val > 0 and str(val).startswith("100") and len(str(val)) >= 13:
            return -val
        return val
    except (ValueError, TypeError):
        return None

BACKUP_CHANNEL_ID = sanitize_channel_id(os.environ.get("BACKUP_CHANNEL_ID"))
LOG_CHANNEL_ID = sanitize_channel_id(os.environ.get("LOG_CHANNEL_ID"))
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit()]

# LIMITS: Effectively unlimited for Render (Disk dependent)
MAX_FILE_SIZE = 50 * 1024 * 1024 * 1024  

# OPTIMIZED CHUNK SIZE: 4MB (Fast I/O, Low RAM)
CHUNK_SIZE = 4 * 1024 * 1024  

PRIORITIZED_SERVERS = [
    "upload-na-phx", "upload-ap-sgp", "upload-ap-hkg", 
    "upload-ap-tyo", "upload-sa-sao", "upload-eu-fra"
]

HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Database
DB_FILE = "bot_database.db"

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== BOT INSTANCE ==================

# Increased workers to handle background tasks faster
app = Client(
    "ultimate_gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=10
)

download_queue = Queue()
processing_lock = Lock()
scheduler = AsyncIOScheduler()

# ================== DATABASE SETUP ==================

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, join_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS broadcasts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, schedule_type TEXT, times INTEGER, interval INTEGER)''')
    conn.commit()
    conn.close()

init_db()

def add_user(user):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    join_date = datetime.now().isoformat()
    c.execute('''INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, join_date)
                 VALUES (?, ?, ?, ?, ?)''', (user.id, user.username, user.first_name, user.last_name or "N/A", join_date))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    users = [row[0] for row in c.fetchall()]
    conn.close()
    return users

def get_setting(key, default=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    result = c.fetchone()
    conn.close()
    return json.loads(result[0]) if result else default

def set_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)''', (key, json.dumps(value)))
    conn.commit()
    conn.close()

# Force subscribe channels
FORCE_CHANNELS_KEY = "force_channels"
def get_force_channels():
    return get_setting(FORCE_CHANNELS_KEY, [])

def add_force_channel(channel_id):
    channels = get_force_channels()
    if channel_id not in channels:
        channels.append(channel_id)
        set_setting(FORCE_CHANNELS_KEY, channels)

def remove_force_channel(channel_id):
    channels = get_force_channels()
    if channel_id in channels:
        channels.remove(channel_id)
        set_setting(FORCE_CHANNELS_KEY, channels)

# ================== HELPER FUNCTIONS ==================

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def check_force_subscribe(client, user_id):
    if user_id in ADMIN_IDS:
        return True
    channels = get_force_channels()
    for channel in channels:
        try:
            member = await client.get_chat_member(channel, user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except Exception:
            return False
    return True

async def immediate_backup(client, message, is_url=False, url_text=None):
    """
    Step 1: Immediately forward content to backup channel before processing.
    """
    if not BACKUP_CHANNEL_ID:
        return

    try:
        user_info = (
            f"#INCOMING_REQUEST\n"
            f"👤 User: {message.from_user.first_name} (ID: `{message.from_user.id}`)\n"
            f"🕒 Time: {get_current_time()}\n"
        )

        if is_url:
            await client.send_message(
                BACKUP_CHANNEL_ID,
                f"{user_info}🔗 **URL Source:**\n`{url_text}`"
            )
        else:
            await client.copy_message(
                chat_id=BACKUP_CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.id,
                caption=f"{user_info}\n⬇️ **Original File Backup**"
            )
    except Exception as e:
        logger.error(f"Immediate Backup Failed: {e}")
        await notify_admin(client, f"Immediate Backup Failed: {e}")

async def notify_admin(client, error_msg):
    for admin in ADMIN_IDS:
        try:
            await client.send_message(admin, f"🚨 Bot Error: {error_msg}")
        except:
            pass

# ================== COMMANDS ==================

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    user = message.from_user
    add_user(user)
    if not await check_force_subscribe(client, user.id):
        channels = get_force_channels()
        buttons = [[InlineKeyboardButton(f"Join Channel {i+1}", url=await client.export_chat_invite_link(ch)) for i, ch in enumerate(channels)]]
        await message.reply_text("Please join all required channels to use the bot.", reply_markup=InlineKeyboardMarkup(buttons))
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Help", callback_data="help")],
        [InlineKeyboardButton("Powered by @TOOLS_BOTS_KING", url="https://t.me/TOOLS_BOTS_KING")]
    ])
    await message.reply_text(
        "⚡ **High-Performance Uploader Online**\n\n"
        "📂 **Send:** Files or URLs\n\n"
        "I will backup your data and process it at maximum speed.",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex("help"))
async def help_callback(client, query):
    await query.answer()
    await query.message.edit_text(
        "🛠 **Commands:**\n"
        "/start - Start the bot\n"
        "/help - Show this help\n"
        "Admin Commands:\n"
        "/broadcast - Broadcast a message (reply to message)\n"
        "/ads <period> <times> - Schedule ads (e.g., /ads 1d 3), then reply with message\n"
        "/addchannel <channel_id> - Add force subscribe channel\n"
        "/removechannel <channel_id> - Remove force subscribe channel\n"
        "/listchannels - List force subscribe channels"
    )

@app.on_message(filters.command("help") & filters.private)
async def help_command(client, message):
    if not await check_force_subscribe(client, message.from_user.id):
        return
    await message.reply_text(
        "🛠 **Commands:**\n"
        "/start - Start the bot\n"
        "/help - Show this help\n"
        "Admin Commands:\n"
        "/broadcast - Broadcast a message (reply to message)\n"
        "/ads <period> <times> - Schedule ads (e.g., /ads 1d 3), then reply with message\n"
        "/addchannel <channel_id> - Add force subscribe channel\n"
        "/removechannel <channel_id> - Remove force subscribe channel\n"
        "/listchannels - List force subscribe channels"
    )

@app.on_message(filters.command("addchannel") & filters.private)
async def add_channel(client, message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if len(message.command) < 2:
        return await message.reply("Usage: /addchannel <channel_id>")
    channel_id = sanitize_channel_id(message.command[1])
    if channel_id:
        add_force_channel(channel_id)
        await message.reply("Channel added.")
    else:
        await message.reply("Invalid channel ID.")

@app.on_message(filters.command("removechannel") & filters.private)
async def remove_channel(client, message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if len(message.command) < 2:
        return await message.reply("Usage: /removechannel <channel_id>")
    channel_id = sanitize_channel_id(message.command[1])
    if channel_id:
        remove_force_channel(channel_id)
        await message.reply("Channel removed.")
    else:
        await message.reply("Invalid channel ID.")

@app.on_message(filters.command("listchannels") & filters.private)
async def list_channels(client, message):
    if message.from_user.id not in ADMIN_IDS:
        return
    channels = get_force_channels()
    await message.reply(f"Force Channels: {channels}")

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast(client, message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if not message.reply_to_message:
        return await message.reply("Reply to a message to broadcast.")
    users = get_all_users()
    for user_id in users:
        try:
            await message.reply_to_message.copy(user_id)
        except Exception as e:
            logger.error(f"Broadcast failed for {user_id}: {e}")
    await message.reply("Broadcast sent.")

@app.on_message(filters.command("ads") & filters.private)
async def ads_schedule(client, message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if len(message.command) < 3:
        return await message.reply("Usage: /ads <period> <times> (period: 1d/1w/1m)")
    period = message.command[1]
    times = int(message.command[2])
    if not message.reply_to_message:
        return await message.reply("Reply to a message for ads.")

    # Calculate interval
    if period == "1d":
        interval = 24 * 60 * 60 / times
    elif period == "1w":
        interval = 7 * 24 * 60 * 60 / times
    elif period == "1m":
        interval = 30 * 24 * 60 * 60 / times
    else:
        return await message.reply("Invalid period.")

    async def send_ads():
        users = get_all_users()
        for user_id in users:
            try:
                await message.reply_to_message.copy(user_id)
            except:
                pass

    for i in range(times):
        scheduler.add_job(send_ads, 'interval', seconds=interval * i)

    await message.reply(f"Ads scheduled: {times} times over {period}.")

# ================== URL DETECTION & HANDLING ==================

@app.on_message(filters.text & filters.private)
async def url_handler(client, message):
    if not await check_force_subscribe(client, message.from_user.id):
        return
    text = message.text.strip()
    
    if not (text.startswith("http://") or text.startswith("https://")):
        return    
    
    # 1. IMMEDIATE BACKUP
    await immediate_backup(client, message, is_url=True, url_text=text)

    msg = await message.reply_text("🔗 **URL Detected!**\n🚀 Queued for High-Speed Process...")
    await download_queue.put(("url", text, message, msg))
    
    asyncio.create_task(process_queue(client))

# ================== FILE HANDLING ==================

@app.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_handler(client, message):
    if message.chat.id == BACKUP_CHANNEL_ID:
        return
    if not await check_force_subscribe(client, message.from_user.id):
        return

    # 1. IMMEDIATE BACKUP
    await immediate_backup(client, message, is_url=False)

    media = message.document or message.video or message.audio or message.photo
    
    msg = await message.reply_text("📁 **File Detected!**\n🚀 Queued for High-Speed Process...")
    await download_queue.put(("file", media, message, msg))
    
    asyncio.create_task(process_queue(client))

# ================== QUEUE PROCESSOR ==================

async def process_queue(client):
    async with processing_lock:
        while not download_queue.empty():
            task = await download_queue.get()
            type_ = task[0]
            
            try:
                if type_ == "file":
                    await process_tg_file(client, *task[1:])
                elif type_ == "url":
                    await process_url_file(client, *task[1:])
            except Exception as e:
                logger.error(f"Queue Error: {e}")
                await notify_admin(client, f"Queue Error: {e}")
                try:
                    await task[3].edit_text(f"❌ **Error:**\n`{str(e)}`")
                except:
                    pass

# ================== FAST DOWNLOAD LOGIC ==================

async def process_tg_file(client, media, message, status_msg):
    file_name = getattr(media, "file_name", f"file_{message.id}_{int(time.time())}")
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    await status_msg.edit_text(
        f"⬇️ **Downloading...**\n"
        f"📦 Size: `{human_readable_size(media.file_size)}`\n"
        f"⚡ Mode: Native Stream"
    )

    # Pyrogram download with default optimized chunking
    await client.download_media(message, file_path)

    await upload_handler(
        client, message, status_msg,
        file_path, media.file_size,
        file_name, "Telegram File", media.mime_type or "unknown"
    )

async def process_url_file(client, url, message, status_msg):
    try:
        file_name = url.split("/")[-1].split("?")[0]
    except:
        file_name = "download.bin"

    if not file_name or len(file_name) > 100:
        file_name = f"url_file_{int(time.time())}.bin"
       
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    await status_msg.edit_text("⬇️ **Fast Downloading...**\n⏳ Mode: Optimized HTTP Stream")

    # Use TCPConnector to speed up connection handshake
    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, timeout=None) as response:
            if response.status != 200:
                return await status_msg.edit_text(f"❌ URL Error: {response.status}")
            
            with open(file_path, "wb") as f:
                # Optimized Chunk Size (4MB) reduces Disk I/O syscalls
                async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)

    final_size = os.path.getsize(file_path)
    
    await upload_handler(
        client, message, status_msg,
        file_path, final_size,
        file_name, "HTTP URL", "unknown"
    )

# ================== UPLOAD & FINAL LOGGING ==================

async def upload_handler(client, message, status_msg, file_path, file_size, file_name, source, file_type):
    try:
        await status_msg.edit_text("⬆️ **Fast Uploading to GoFile...**\n🚀 Optimized Buffer Active")
        
        link = await upload_to_gofile(file_path)

        if not link:
            return await status_msg.edit_text("❌ **Upload Failed.**\nGoFile servers might be busy.")

        # ================== 1. USER RESPONSE ==================
        user_text = (
            f"✅ **Process Completed!**\n\n"
            f"📂 **File:** `{file_name}`\n"
            f"📦 **Size:** `{human_readable_size(file_size)}`\n"
            f"🔗 **Download Link:**\n{link}"
        )
        await status_msg.edit_text(user_text, disable_web_page_preview=True)

        # ================== 2. BACKUP CHANNEL FINAL LOG ==================
        if BACKUP_CHANNEL_ID:
            user = message.from_user
            log_text = (
                f"#UPLOAD_COMPLETE\n"
                f"Date: {datetime.now().isoformat()}\n"
                f"User ID: {user.id}\n"
                f"First Name: {user.first_name}\n"
                f"Last Name: {user.last_name or 'N/A'}\n"
                f"Username: @{user.username if user.username else 'N/A'}\n"
                f"Chat ID: {message.chat.id}\n"
                f"File Type: {file_type}\n"
                f"File Size: {human_readable_size(file_size)}\n"
                f"Download Link: {link}\n"
                f"📥 **Source:** {source}\n"
                f"📂 **File:** `{file_name}`\n"
            )
            
            try:
                await client.send_message(
                    BACKUP_CHANNEL_ID,
                    log_text,
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Failed to send final log to backup: {e}")
                await notify_admin(client, f"Failed to send final log: {e}")

    except Exception as e:
        logger.error(f"Upload Handler Error: {e}")
        await notify_admin(client, f"Upload Handler Error: {e}")
        await status_msg.edit_text(f"❌ Critical Error: {e}")
    finally:
        # CLEANUP
        if os.path.exists(file_path):
            os.remove(file_path)

# ================== GOFILE UPLOADER (OPTIMIZED) ==================

async def upload_to_gofile(path):
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type is None:
        mime_type = "application/octet-stream"

    # Reuse connector for speed
    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)

    for server in PRIORITIZED_SERVERS:
        try:
            url = f"https://{server}.gofile.io/uploadfile"
            
            async with aiohttp.ClientSession(connector=connector) as session:
                with open(path, "rb") as f:
                    data = aiohttp.FormData()
                    # GoFile requires the file field
                    data.add_field('file', f, filename=os.path.basename(path), content_type=mime_type)
                    data.add_field('token', GOFILE_API_TOKEN)
                    
                    folder_id = os.environ.get("GOFILE_FOLDER_ID")
                    if folder_id:
                        data.add_field('folderId', folder_id)

                    async with session.post(url, data=data) as response:
                        if response.status == 200:
                            result = await response.json()
                            if result.get("status") == "ok":
                                return result["data"]["downloadPage"]
        except Exception as e:
            logger.error(f"Server {server} failed: {e}")
            await notify_admin(app, f"GoFile Server {server} failed: {e}")
            continue
            
    return None

# ================== WEB SERVER (RENDER KEEP-ALIVE) ==================

async def web_handler(request):
    return web.Response(text="Bot is Running | High Speed Mode Active")

async def start_web():
    appw = web.Application()
    appw.router.add_get("/", web_handler)
    runner = web.AppRunner(appw)
    await runner.setup()
    await web.TCPSite(
        runner, "0.0.0.0",
        int(os.environ.get("PORT", 8080))
    ).start()

# ================== MAIN EXECUTION ==================

async def main():
    print("🤖 Bot Starting with uvloop optimization...")
    await app.start()
    print("✅ Bot Connected to Telegram")
    print("🌍 Starting Web Server...")
    await start_web()
    scheduler.start()
    print("🚀 High Speed Pipeline Ready. Waiting for requests.")
    await idle()
    await app.stop()

if __name__ == "__main__":
    # uvloop is installed at the top level, so standard run works
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
