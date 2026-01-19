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
from pyrogram import Client, filters, idle
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

# ================== HELPER FUNCTIONS ==================

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

# ================== COMMANDS ==================

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text(
        "⚡ **High-Performance Uploader Online**\n\n"
        "🚀 **System:** uvloop enabled (Fast Mode)\n"
        "📂 **Send:** Files or URLs\n\n"
        "I will backup your data and process it at maximum speed."
    )

# ================== URL DETECTION & HANDLING ==================

@app.on_message(filters.text & filters.private)
async def url_handler(client, message):
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
        file_name, "Telegram File"
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
        file_name, "HTTP URL"
    )

# ================== UPLOAD & FINAL LOGGING ==================

async def upload_handler(client, message, status_msg, file_path, file_size, file_name, source):
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
                f"👤 **User:** {user.first_name} (`{user.id}`)\n"
                f"📛 **Username:** @{user.username if user.username else 'None'}\n"
                f"📅 **Date:** {get_current_time()}\n"
                f"📥 **Source:** {source}\n"
                f"📂 **File:** `{file_name}`\n"
                f"📦 **Size:** `{human_readable_size(file_size)}`\n"
                f"🔗 **GoFile Link:** {link}\n"
            )
            
            try:
                await client.send_message(
                    BACKUP_CHANNEL_ID,
                    log_text,
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Failed to send final log to backup: {e}")

    except Exception as e:
        logger.error(f"Upload Handler Error: {e}")
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
    print("🚀 High Speed Pipeline Ready. Waiting for requests.")
    await idle()
    await app.stop()

if __name__ == "__main__":
    # uvloop is installed at the top level, so standard run works
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
