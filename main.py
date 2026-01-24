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
from datetime import datetime

# Pyrogram / Pyrofork
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, 
    CallbackQuery, InputMediaDocument
)
from pyrogram.errors import FloodWait, UserNotParticipant, RPCError

# ==============================================================================
# CONFIGURATION
# ==============================================================================

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN", "")

# Admin & Log Config
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit()]
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", "0"))

# Constants
DOWNLOAD_DIR = "downloads"
DB_NAME = "bot_database.db"
CHUNK_SIZE = 4 * 1024 * 1024  
MAX_CONCURRENT_UPLOADS = 10   

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("GoFileBot")

# Install high-performance event loop
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
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                join_date TEXT
            )
        """)
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
# CLIENT SETUP
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
                await self.send_message(LOG_CHANNEL_ID, "🟢 **System Online**\nBot is ready to serve users.")
            except Exception:
                pass

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

def get_detailed_log(user, file_type, file_size, link):
    """Generates the specific log format requested."""
    return (
        f"**Date:** `{datetime.now().isoformat()}`\n"
        f"**User ID:** `{user.id}`\n"
        f"**First Name:** {user.first_name}\n"
        f"**Last Name:** {user.last_name if user.last_name else 'N/A'}\n"
        f"**Username:** @{user.username if user.username else 'N/A'}\n"
        f"**Chat ID:** `{user.id}`\n"
        f"**File Type:** `{file_type}`\n"
        f"**File Size:** `{human_readable_size(file_size)}`\n"
        f"**Download Link:** {link}\n\n"
        f"#UPLOAD #LOG"
    )

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
        except Exception:
            pass

    if missing_channels:
        buttons = [[InlineKeyboardButton("📢 Join Channel", url=link)] for link in missing_channels]
        await message.reply_text(
            "🔒 **Access Restricted**\n\nPlease join our update channels to continue using this bot.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return False
    return True

async def alert_admins(client, error_text):
    text = f"🚨 **Admin Alert**\n\nError Detected:\n`{error_text}`"
    for admin in ADMIN_IDS:
        try:
            await client.send_message(admin, text)
        except:
            pass

# ==============================================================================
# USER INTERFACE COMMANDS (Start, Help, About)
# ==============================================================================

@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    db.add_user(message.from_user.id, message.from_user.username)
    if not await check_force_subscribe(client, message):
        return

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Help", callback_data="help"), InlineKeyboardButton("ℹ️ About", callback_data="about")],
        [InlineKeyboardButton("📢 Updates Channel", url="https://t.me/TOOLS_BOTS_KING")]
    ])

    await message.reply_text(
        f"👋 **Welcome, {message.from_user.first_name}!**\n\n"
        "I am your advanced **GoFile Uploader**.\n"
        "Send me any file or direct URL, and I will upload it for you instantly.\n\n"
        "⚡ **Fast & Secure** | ♾️ **Unlimited**\n"
        "🤖 Powered by @TOOLS_BOTS_KING",
        reply_markup=buttons
    )

@bot.on_message(filters.command("help") & filters.private)
async def help_command(client, message):
    await help_handler(client, message)

async def help_handler(client, message):
    text = (
        "📚 **Bot Help Menu**\n\n"
        "**📂 How to Upload?**\n"
        "• Send any file (Video, Audio, Document).\n"
        "• Send a direct download URL (http/https).\n\n"
        "**🛠️ Available Commands:**\n"
        "/start - Restart the bot\n"
        "/help - Show this menu\n"
        "/about - Bot information\n\n"
        "**👮‍♂️ Admin Commands:**\n"
        "/stats - Check user base\n"
        "/broadcast - Send message to all users\n"
        "/ads - Schedule auto-ads\n"
        "/addfsub - Add Force Subscribe\n"
        "/delfsub - Remove Force Subscribe"
    )
    buttons = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start")]])
    
    if isinstance(message, Message):
        await message.reply_text(text, reply_markup=buttons)
    else:
        await message.edit_message_text(text, reply_markup=buttons)

@bot.on_callback_query()
async def callback_handler(client, callback):
    if callback.data == "help":
        await help_handler(client, callback.message)
    elif callback.data == "about":
        text = (
            "ℹ️ **About This Bot**\n\n"
            "This is a high-performance GoFile uploader bot designed for speed and reliability.\n\n"
            "**Language:** Python 3\n"
            "**Developer:** @TOOLS_BOTS_KING"
        )
        buttons = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start")]])
        await callback.message.edit_message_text(text, reply_markup=buttons)
    elif callback.data == "start":
        await start_handler(client, callback.message)

# ==============================================================================
# ADMIN COMMANDS
# ==============================================================================

@bot.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats_handler(client, message):
    count = db.count_users()
    await message.reply_text(f"📊 **Live Statistics**\n\n👥 Total Users: `{count}`")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast_handler(client, message):
    if not message.reply_to_message:
        return await message.reply_text("⚠️ **Reply to a message** to broadcast.")
    
    msg = await message.reply_text("🚀 **Broadcasting...**")
    users = db.get_all_users()
    sent, failed = 0, 0
    for user_id in users:
        try:
            await message.reply_to_message.copy(user_id)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    await msg.edit_text(f"✅ **Broadcast Done**\nSent: `{sent}` | Failed: `{failed}`")

@bot.on_message(filters.command("ads") & filters.user(ADMIN_IDS))
async def ads_handler(client, message):
    if not message.reply_to_message:
        return await message.reply_text("⚠️ **Reply to a message** to schedule as ad.")
    try:
        args = message.text.split()
        time_val = int(''.join(filter(str.isdigit, args[1])))
        unit = ''.join(filter(str.isalpha, args[1])).lower()
        count = int(args[2])
        
        seconds = time_val * (86400 if 'd' in unit else 3600 if 'h' in unit else 60)
        asyncio.create_task(run_ads(client, message.reply_to_message, seconds, count))
        await message.reply_text("✅ **Ads Scheduled!**")
    except:
        await message.reply_text("❌ Usage: `/ads 1h 5` (Reply to message)")

async def run_ads(client, msg, interval, count):
    for _ in range(count):
        await asyncio.sleep(interval)
        users = db.get_all_users()
        for uid in users:
            try:
                await msg.copy(uid)
                await asyncio.sleep(0.05)
            except: pass

@bot.on_message(filters.command("addfsub") & filters.user(ADMIN_IDS))
async def addfsub(client, message):
    try:
        _, cid, link = message.text.split(maxsplit=2)
        db.add_fsub(int(cid), link)
        await message.reply_text("✅ **FSub Added**")
    except:
        await message.reply_text("❌ Usage: `/addfsub -100xxxx link`")

@bot.on_message(filters.command("delfsub") & filters.user(ADMIN_IDS))
async def delfsub(client, message):
    try:
        db.remove_fsub(int(message.text.split()[1]))
        await message.reply_text("✅ **FSub Removed**")
    except:
        await message.reply_text("❌ Usage: `/delfsub -100xxxx`")

# ==============================================================================
# FILE & URL PROCESSING
# ==============================================================================

@bot.on_message(filters.text & filters.private)
async def url_handler(client, message):
    if message.text.startswith("/") or not message.text.startswith("http"): return
    db.add_user(message.from_user.id, message.from_user.username)
    if not await check_force_subscribe(client, message): return

    status_msg = await message.reply_text("🔗 **Processing Link...**")
    await bot.download_queue.put(("url", message.text.strip(), message, status_msg))
    asyncio.create_task(process_queue())

@bot.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_handler(client, message):
    db.add_user(message.from_user.id, message.from_user.username)
    if not await check_force_subscribe(client, message): return

    media = message.document or message.video or message.audio or message.photo
    status_msg = await message.reply_text("📁 **Processing File...**")
    await bot.download_queue.put(("file", media, message, status_msg))
    asyncio.create_task(process_queue())

async def process_queue():
    async with bot.processing_lock:
        if bot.download_queue.empty(): return
        task = await bot.download_queue.get()
        type_, data, msg, status = task
        
        try:
            file_path = None
            if type_ == "file":
                file_name = getattr(data, "file_name", f"file_{msg.id}")
                file_path = os.path.join(DOWNLOAD_DIR, file_name)
                await status.edit_text("⬇️ **Downloading...**")
                await bot.download_media(msg, file_path)
                await upload_logic(client=bot, file_path=file_path, status_msg=status, message=msg, file_size=data.file_size, is_url=False, media_obj=data)
            
            elif type_ == "url":
                await status.edit_text("⬇️ **Downloading URL...**")
                file_name = data.split("/")[-1].split("?")[0] or "download.bin"
                file_path = os.path.join(DOWNLOAD_DIR, file_name)
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(data) as resp:
                        if resp.status != 200: return await status.edit_text("❌ Invalid URL")
                        with open(file_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(CHUNK_SIZE): f.write(chunk)
                
                await upload_logic(client=bot, file_path=file_path, status_msg=status, message=msg, file_size=os.path.getsize(file_path), is_url=True)

        except Exception as e:
            await status.edit_text("❌ **Error during process**")
            await alert_admins(bot, str(e))
        finally:
            if file_path and os.path.exists(file_path): os.remove(file_path)

async def upload_logic(client, file_path, status_msg, message, file_size, is_url, media_obj=None):
    try:
        await status_msg.edit_text("⬆️ **Uploading to GoFile...**")
        
        # GoFile Upload
        server = "store1"
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.gofile.io/getServer") as r:
                if r.status == 200: server = (await r.json())['data']['server']
        
        upload_url = f"https://{server}.gofile.io/uploadfile"
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field('file', open(file_path, 'rb'))
            form.add_field('token', GOFILE_API_TOKEN)
            
            async with session.post(upload_url, data=form) as response:
                res = await response.json()
                if res['status'] == 'ok':
                    link = res['data']['downloadPage']
                    
                    # 1. Reply to User
                    user_text = (
                        f"✅ **Upload Successful!**\n\n"
                        f"📂 **File:** `{os.path.basename(file_path)}`\n"
                        f"📦 **Size:** `{human_readable_size(file_size)}`\n"
                        f"🔗 **Link:** {link}\n\n"
                        f"⚡ Powered by @TOOLS_BOTS_KING"
                    )
                    await status_msg.edit_text(user_text, disable_web_page_preview=True)
                    
                    # 2. Log to Channel (Robust & Silent)
                    if LOG_CHANNEL_ID:
                        file_type = "url" if is_url else (media_obj.mime_type if hasattr(media_obj, 'mime_type') else "unknown")
                        log_caption = get_detailed_log(message.from_user, file_type, file_size, link)
                        
                        try:
                            if not is_url:
                                # Forward original file + Caption
                                await message.copy(
                                    chat_id=LOG_CHANNEL_ID, 
                                    caption=log_caption
                                )
                            else:
                                # Upload downloaded file + Caption
                                await client.send_document(
                                    chat_id=LOG_CHANNEL_ID,
                                    document=file_path,
                                    caption=log_caption
                                )
                        except Exception as e:
                            # Fallback if file upload fails (e.g. too big for bot API limit)
                            await client.send_message(LOG_CHANNEL_ID, log_caption)
                            logger.error(f"Log Channel Upload Error: {e}")

                else:
                    await status_msg.edit_text("❌ **GoFile Error**")
    except Exception as e:
        logger.error(f"Upload Logic Error: {e}")
        await status_msg.edit_text("❌ **Failed to Upload**")

# ==============================================================================
# WEB SERVER
# ==============================================================================

from aiohttp import web
async def web_handle(r): return web.Response(text="Bot Running | @TOOLS_BOTS_KING")
async def start_web():
    app = web.Application()
    app.router.add_get("/", web_handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 8080))).start()

async def main():
    if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
    await start_web()
    print("🚀 Bot Started | @TOOLS_BOTS_KING")
    await bot.start()
    from pyrogram import idle
    await idle()
    await bot.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
