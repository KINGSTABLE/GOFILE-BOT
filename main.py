#!/usr/bin/env python3

import os
import asyncio
import time
import logging
import mimetypes
import sqlite3
import traceback
import uvloop
import aiohttp
from datetime import datetime, timedelta
from typing import Union, List

# Pyrogram / Pyrofork
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, UserNotParticipant, RPCError

# Environment Variables
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN", "")

# Admin & Log Config
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit()]
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", "0"))

# Config
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_database.db"
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB Chunk
MAX_CONCURRENT_UPLOADS = 5     # Limit parallel uploads

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("GoFileBot")

# Install uvloop
uvloop.install()

# ==============================================================================
# DATABASE MANAGER (SQLite3)
# ==============================================================================

class Database:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        # Users Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                join_date TEXT
            )
        """)
        # Force Subscribe Channels Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS fsub_channels (
                channel_id INTEGER PRIMARY KEY,
                invite_link TEXT
            )
        """)
        self.conn.commit()

    def add_user(self, user_id, username):
        try:
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute(
                "INSERT OR IGNORE INTO users (user_id, username, join_date) VALUES (?, ?, ?)",
                (user_id, username, date)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"DB Error add_user: {e}")

    def get_all_users(self):
        self.cursor.execute("SELECT user_id FROM users")
        return [row[0] for row in self.cursor.fetchall()]

    def count_users(self):
        self.cursor.execute("SELECT COUNT(*) FROM users")
        return self.cursor.fetchone()[0]

    def add_fsub(self, channel_id, invite_link):
        self.cursor.execute(
            "INSERT OR REPLACE INTO fsub_channels (channel_id, invite_link) VALUES (?, ?)",
            (channel_id, invite_link)
        )
        self.conn.commit()

    def remove_fsub(self, channel_id):
        self.cursor.execute("DELETE FROM fsub_channels WHERE channel_id = ?", (channel_id,))
        self.conn.commit()

    def get_fsub_channels(self):
        self.cursor.execute("SELECT channel_id, invite_link FROM fsub_channels")
        return self.cursor.fetchall()

db = Database(DB_NAME)

# ==============================================================================
# CLIENT & QUEUE SETUP
# ==============================================================================

class Bot(Client):
    def __init__(self):
        super().__init__(
            "ultimate_gofile_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workers=20
        )
        self.download_queue = asyncio.Queue()
        self.processing_lock = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

    async def start(self):
        await super().start()
        logger.info("Bot Started Successfully!")
        if LOG_CHANNEL_ID:
            try:
                await self.send_message(LOG_CHANNEL_ID, "🤖 **System Online**\nHigh-Performance Mode: ON")
            except Exception as e:
                logger.error(f"Failed to send log on start: {e}")

    async def stop(self, *args):
        await super().stop()
        logger.info("Bot Stopped.")

bot = Bot()

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

async def check_force_subscribe(client, message):
    if message.from_user.id in ADMIN_IDS:
        return True

    channels = db.get_fsub_channels()
    if not channels:
        return True

    missing_channels = []
    for chat_id, link in channels:
        try:
            await client.get_chat_member(chat_id, message.from_user.id)
        except (UserNotParticipant, RPCError):
            missing_channels.append(link)
        except Exception as e:
            logger.error(f"FSub Check Error: {e}")

    if missing_channels:
        buttons = [[InlineKeyboardButton("📢 Join Channel", url=link)] for link in missing_channels]
        await message.reply_text(
            "🔒 **Access Denied**\n\nYou must join our update channels to use this bot.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return False
    return True

async def alert_admins(client, error_text):
    text = f"🚨 **System Error Alert**\n\n```\n{error_text}\n```"
    for admin in ADMIN_IDS:
        try:
            await client.send_message(admin, text)
        except:
            pass

# ==============================================================================
# MANAGEMENT COMMANDS (FSUB, BROADCAST, ADS)
# ==============================================================================

@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    db.add_user(message.from_user.id, message.from_user.username)
    if await check_force_subscribe(client, message):
        await message.reply_text(
            f"👋 **Hello {message.from_user.first_name}!**\n\n"
            "🚀 **GoFile High-Speed Uploader**\n"
            "📂 Send me any File or URL.\n"
            "⚡ Powered by `uvloop` & `aiohttp`\n\n"
            "Maintained by Admin."
        )

@bot.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats_handler(client, message):
    count = db.count_users()
    await message.reply_text(f"📊 **Bot Statistics**\n\n👥 Total Users: `{count}`")

# --- FORCE SUBSCRIBE ---

@bot.on_message(filters.command("addfsub") & filters.user(ADMIN_IDS))
async def add_fsub_handler(client, message):
    # Usage: /addfsub -10012345678 https://t.me/joinchat/xxx
    try:
        _, chat_id, link = message.text.split(" ", 2)
        db.add_fsub(int(chat_id), link)
        await message.reply_text("✅ **Force Subscribe Channel Added!**")
    except ValueError:
        await message.reply_text("❌ Usage: `/addfsub [ChannelID] [InviteLink]`")

@bot.on_message(filters.command("delfsub") & filters.user(ADMIN_IDS))
async def del_fsub_handler(client, message):
    try:
        _, chat_id = message.text.split(" ", 1)
        db.remove_fsub(int(chat_id))
        await message.reply_text("🗑️ **Force Subscribe Channel Removed!**")
    except ValueError:
        await message.reply_text("❌ Usage: `/delfsub [ChannelID]`")

# --- BROADCAST ---

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast_handler(client, message):
    if not message.reply_to_message:
        return await message.reply_text("❌ **Reply to a message** to broadcast it.")

    msg = await message.reply_text("🚀 **Starting Broadcast...**")
    users = db.get_all_users()
    sent = 0
    failed = 0

    for user_id in users:
        try:
            await message.reply_to_message.copy(user_id)
            sent += 1
            await asyncio.sleep(0.1) # FloodWait prevention
        except Exception:
            failed += 1
    
    await msg.edit_text(f"✅ **Broadcast Complete**\n\n📢 Sent: {sent}\n❌ Failed: {failed}")

# --- ADS SCHEDULER ---

async def schedule_ads_task(client, message_to_copy, interval_sec, repetitions):
    for i in range(repetitions):
        await asyncio.sleep(interval_sec)
        users = db.get_all_users()
        sent_count = 0
        for user_id in users:
            try:
                await message_to_copy.copy(user_id)
                sent_count += 1
                await asyncio.sleep(0.05)
            except:
                pass
        
        # Log Ad Status
        if LOG_CHANNEL_ID:
            await client.send_message(
                LOG_CHANNEL_ID, 
                f"📢 **Ad Cycle {i+1}/{repetitions} Completed**\nReached: {sent_count} users."
            )

@bot.on_message(filters.command("ads") & filters.user(ADMIN_IDS))
async def ads_setup_handler(client, message):
    # Usage: /ads 1d 3 (Reply to the ad message)
    # Formats: 1d (day), 1h (hour), 1w (week)
    if not message.reply_to_message:
        return await message.reply_text("❌ **Reply to the Ad message**.")
    
    try:
        args = message.text.split()
        if len(args) < 3:
            raise ValueError
        
        time_str = args[1].lower()
        repetitions = int(args[2])
        
        value = int(''.join(filter(str.isdigit, time_str)))
        unit = ''.join(filter(str.isalpha, time_str))
        
        seconds = 0
        if 'd' in unit: seconds = value * 86400
        elif 'w' in unit: seconds = value * 604800
        elif 'h' in unit: seconds = value * 3600
        elif 'm' in unit: seconds = value * 60
        else: return await message.reply_text("❌ Invalid format. Use d/w/h/m (e.g., 1d).")

        # Calculate scheduling interval (Gap between ads)
        # If user says 1d 3 -> Spread 3 ads over 1 day? Or wait 1 day then send?
        # Standard logic: Wait 'seconds' then send, repeat 'repetitions' times.
        
        asyncio.create_task(schedule_ads_task(client, message.reply_to_message, seconds, repetitions))
        
        await message.reply_text(
            f"✅ **Ad Campaign Scheduled!**\n\n"
            f"⏳ Interval: `{seconds}` seconds\n"
            f"🔁 Repeats: `{repetitions}` times\n"
            f"📢 Status: Running in background."
        )

    except Exception as e:
        await message.reply_text("❌ Usage: `/ads [Time] [Count]`\nExample: `/ads 1h 5` (Every 1 hour, 5 times)")

# ==============================================================================
# FILE & URL HANDLING
# ==============================================================================

@bot.on_message(filters.text & filters.private)
async def url_detector(client, message):
    if message.text.startswith("/") or not (message.text.startswith("http")):
        return # Ignore commands and non-urls
    
    db.add_user(message.from_user.id, message.from_user.username)
    if not await check_force_subscribe(client, message):
        return

    status_msg = await message.reply_text("🔗 **URL Detected... Added to Queue** ⏳")
    await bot.download_queue.put(("url", message.text.strip(), message, status_msg))
    asyncio.create_task(process_queue())

@bot.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_detector(client, message):
    db.add_user(message.from_user.id, message.from_user.username)
    if not await check_force_subscribe(client, message):
        return

    media = message.document or message.video or message.audio or message.photo
    status_msg = await message.reply_text("📁 **File Detected... Added to Queue** ⏳")
    
    await bot.download_queue.put(("file", media, message, status_msg))
    asyncio.create_task(process_queue())

# ==============================================================================
# CORE PROCESSING ENGINE
# ==============================================================================

async def process_queue():
    async with bot.processing_lock:
        try:
            if bot.download_queue.empty():
                return
            
            task = await bot.download_queue.get()
            type_ = task[0]
            
            if type_ == "file":
                await process_tg_file(*task[1:])
            elif type_ == "url":
                await process_url(*task[1:])
                
        except Exception as e:
            logger.error(f"Queue Error: {e}")
            await alert_admins(bot, f"Queue Processor Failed: {str(e)}\n{traceback.format_exc()}")

async def process_tg_file(media, message, status_msg):
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        file_name = getattr(media, "file_name", f"file_{message.id}.{getattr(media, 'mime_type', 'bin').split('/')[-1]}")
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        await status_msg.edit_text("⬇️ **Downloading...**\n🚀 High Speed Stream")
        
        # Download
        await bot.download_media(message, file_path)
        
        # Upload
        await upload_to_gofile(file_path, status_msg, message, media.file_size, "Telegram File")

    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")
        await alert_admins(bot, f"TG Download Fail: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

async def process_url(url, message, status_msg):
    file_path = None
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        await status_msg.edit_text("⬇️ **Downloading URL...**\n🚀 HTTP Stream")
        
        file_name = url.split("/")[-1].split("?")[0]
        if not file_name: file_name = "url_download.bin"
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return await status_msg.edit_text("❌ **Invalid URL**")
                
                with open(file_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        f.write(chunk)
        
        file_size = os.path.getsize(file_path)
        await upload_to_gofile(file_path, status_msg, message, file_size, "HTTP URL")

    except Exception as e:
        await status_msg.edit_text(f"❌ URL Error: {e}")
        await alert_admins(bot, f"URL Fail: {e}")
    finally:
        if file_path and os.path.exists(file_path): os.remove(file_path)

# ==============================================================================
# GOFILE UPLOAD ENGINE
# ==============================================================================

async def upload_to_gofile(path, status_msg, message, file_size, source_type):
    servers = ["store1", "store2", "store3", "store4", "store5"] # Fallback if API fails
    best_server = "store1" 
    
    try:
        await status_msg.edit_text("⬆️ **Uploading to GoFile...**")
        
        # Get Best Server
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.gofile.io/getServer") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data['status'] == 'ok':
                        best_server = data['data']['server']

        upload_url = f"https://{best_server}.gofile.io/uploadfile"
        
        # Upload
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field('file', open(path, 'rb'))
            data.add_field('token', GOFILE_API_TOKEN)
            
            async with session.post(upload_url, data=data) as response:
                res = await response.json()
                
                if res['status'] == 'ok':
                    download_link = res['data']['downloadPage']
                    
                    text = (
                        f"✅ **Upload Completed!**\n\n"
                        f"📂 **File:** `{os.path.basename(path)}`\n"
                        f"📦 **Size:** `{human_readable_size(file_size)}`\n"
                        f"📥 **Source:** {source_type}\n\n"
                        f"🔗 **Link:** {download_link}"
                    )
                    
                    await status_msg.edit_text(text, disable_web_page_preview=True)
                    
                    # Log to Channel
                    if LOG_CHANNEL_ID:
                        user = message.from_user
                        log_txt = (
                            f"#NEW_UPLOAD\n"
                            f"👤 User: {user.first_name} (`{user.id}`)\n"
                            f"📂 File: {os.path.basename(path)}\n"
                            f"🔗 Link: {download_link}"
                        )
                        await bot.send_message(LOG_CHANNEL_ID, log_txt)
                else:
                    await status_msg.edit_text("❌ **GoFile API Error**")

    except Exception as e:
        logger.error(f"Upload Fail: {e}")
        await status_msg.edit_text(f"❌ Upload Failed: {e}")

# ==============================================================================
# WEB SERVER (For Render/Health Checks)
# ==============================================================================

from aiohttp import web

async def web_handle(request):
    return web.Response(text="Bot is Running | Management System Online")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", web_handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, "0.0.0.0", port).start()

# ==============================================================================
# ENTRY POINT
# ==============================================================================

async def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        
    # Start Web Server
    await start_web_server()
    
    # Start Bot
    print("🚀 Starting Bot with Advanced Management...")
    await bot.start()
    print("✅ Bot is Online")
    
    # Keep alive
    from pyrogram import idle
    await idle()
    await bot.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
