#!/usr/bin/env python3
import os
import aiohttp
import asyncio
import time
import requests
import mimetypes
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from asyncio import Queue, Lock
from aiohttp import web
import re
from urllib.parse import urlparse
import hashlib

# ================== CONFIG ==================

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN")

# Helper to fix Channel IDs that might be missing the -100 prefix
def sanitize_channel_id(value):
    try:
        val = int(value)
        # If ID is positive and starts with 100 (common copy-paste error), make it negative
        if val > 0 and str(val).startswith("100") and len(str(val)) >= 13:
            return -val
        return val
    except (ValueError, TypeError):
        return None

# GOFILE UPLOADER BOT backup
BACKUP_CHANNEL_ID = sanitize_channel_id(os.environ.get("BACKUP_CHANNEL_ID", -1003648024683))
LOG_CHANNEL_ID = sanitize_channel_id(os.environ.get("LOG_CHANNEL_ID", -1003648024683))

ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "5978396634").split()]

# Removed file size restrictions for pipeline approach
MAX_FILE_SIZE = None  # No limit for pipeline streaming
MAX_URL_UPLOAD_SIZE = None  # No limit for pipeline streaming

PRIORITIZED_SERVERS = [
    "upload-na-phx",
    "upload-ap-sgp",
    "upload-ap-hkg",
    "upload-ap-tyo",
    "upload-sa-sao",
]

HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# URL detection pattern
URL_PATTERN = re.compile(
    r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    r'(?::\d+)?(?:/[-\w%+.~!$&\'()*,;=:@]*)*'
    r'(?:\?[-\w%+.~!$&\'()*,;=:@/?]*)?'
    r'(?:#[-\w%+.~!$&\'()*,;=:@/?]*)?'
)

# ================== BOT ==================

app = Client(
    "ultimate_gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

download_queue = Queue()
processing_lock = Lock()

# ================== HELPERS ==================

def human_readable_size(size):
    if size is None:
        return "Unknown size"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def is_forwarded(message):
    return bool(
        message.forward_date
        or message.forward_from
        or message.forward_from_chat
        or message.forward_sender_name
    )

def backup_via_requests(file_path, caption):
    try:
        with open(file_path, "rb") as f:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
            data = {"chat_id": BACKUP_CHANNEL_ID, "caption": caption}
            files = {"document": f}
            r = requests.post(url, data=data, files=files, timeout=60)
            return r.status_code == 200
    except Exception as e:
        print("REQUEST BACKUP ERROR:", e)
        return False

def extract_urls(text):
    """Extract URLs from text message"""
    return URL_PATTERN.findall(text)

def generate_file_hash(file_path):
    """Generate MD5 hash for file identification"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# ================== COMMANDS ==================

# Added filters.private to prevent bot from replying to itself in channels (Infinite Loop Fix)
@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text(
        "👋 **Welcome to GoFile Uploader Bot**\n\n"
        "📤 Send me a file OR\n"
        "🔗 Send me a URL or use `/upload <url>`\n"
        "🔗 I'll auto-detect URLs in your messages\n\n"
        "⚠️ Forwarded files are detected.\n"
        "🚀 Now supports streaming uploads for large files!"
    )

@app.on_message(filters.command("upload") & filters.private)
async def url_upload(client, message):
    try:
        url = message.text.split(maxsplit=1)[1]
    except IndexError:
        return await message.reply_text("❌ Usage: `/upload <url>`")

    msg = await message.reply_text("📥 Added to queue")
    await download_queue.put(("url", url, message, msg))
    asyncio.create_task(process_queue(client))

@app.on_message(filters.text & filters.private & ~filters.command("start") & ~filters.command("upload"))
async def handle_text_message(client, message):
    """Auto-detect URLs in text messages"""
    urls = extract_urls(message.text)
    
    if urls:
        # If multiple URLs found, process the first one
        url = urls[0]
        msg = await message.reply_text(f"🔗 URL detected! Adding to queue...")
        await download_queue.put(("url", url, message, msg))
        asyncio.create_task(process_queue(client))
        
        # If more than one URL, notify user
        if len(urls) > 1:
            await message.reply_text(f"📝 Note: Found {len(urls)} URLs. Processing first one only. Use /upload command for specific URLs.")
    else:
        # If no URLs and not a command, show help
        await message.reply_text(
            "📝 I didn't find any URLs in your message.\n\n"
            "Send me:\n"
            "📁 A file to upload\n"
            "🔗 A URL to download and upload\n"
            "🔗 Or use `/upload <url>` command"
        )

# ================== FILE HANDLING ==================

@app.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def handle_file(client, message):
    # CHECK: If file is forwarded FROM the backup channel, ignore it.
    if message.forward_from_chat and message.forward_from_chat.id == BACKUP_CHANNEL_ID:
        return await message.reply_text("❌ This file is already in the backup drive.")

    media = message.document or message.video or message.audio or message.photo
    
    # Get file size (could be None for photos)
    file_size = getattr(media, 'file_size', None)
    
    msg = await message.reply_text("📥 Added to queue")
    await download_queue.put(("file", media, message, msg))
    asyncio.create_task(process_queue(client))

async def process_queue(client):
    async with processing_lock:
        while not download_queue.empty():
            task = await download_queue.get()
            if task[0] == "file":
                await process_tg_file(client, *task[1:])
            else:
                await process_url_file(client, *task[1:])

async def process_tg_file(client, media, message, status_msg):
    file_name = getattr(media, "file_name", f"file_{message.id}_{int(time.time())}")
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    file_size = getattr(media, 'file_size', None)
    human_size = human_readable_size(file_size) if file_size else "Unknown size"

    await status_msg.edit_text(f"⬇️ Downloading... ({human_size})")
    
    # Stream download with progress
    try:
        download_task = client.download_media(
            message, 
            file_path,
            progress=lambda current, total: asyncio.create_task(
                update_download_progress(status_msg, current, total, "Downloading")
            )
        )
        
        if asyncio.iscoroutine(download_task):
            await download_task
        else:
            # For synchronous download methods
            download_task
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Download failed: {str(e)}")
        return

    # Verify file was downloaded
    if not os.path.exists(file_path):
        await status_msg.edit_text("❌ Download failed - file not found")
        return

    actual_size = os.path.getsize(file_path)
    
    await upload_handler(
        client, message, status_msg,
        file_path, actual_size,
        file_name, "telegram"
    )

async def process_url_file(client, url, message, status_msg):
    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path) or f"url_file_{int(time.time())}"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    await status_msg.edit_text("⬇️ Downloading from URL...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3600) as response:
                if response.status != 200:
                    await status_msg.edit_text(f"❌ URL download failed with status {response.status}")
                    return
                
                # Try to get content length
                content_length = response.headers.get('Content-Length')
                total_size = int(content_length) if content_length else None
                
                # Stream download with progress
                downloaded = 0
                with open(file_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Update progress if we know total size
                        if total_size:
                            progress = (downloaded / total_size) * 100
                            if int(progress) % 5 == 0:  # Update every 5% to avoid spam
                                await status_msg.edit_text(
                                    f"⬇️ Downloading from URL...\n"
                                    f"📊 {progress:.1f}% ({human_readable_size(downloaded)}/{human_readable_size(total_size)})"
                                )
                
    except Exception as e:
        await status_msg.edit_text(f"❌ URL download failed: {str(e)}")
        if os.path.exists(file_path):
            os.remove(file_path)
        return

    if not os.path.exists(file_path):
        await status_msg.edit_text("❌ URL download failed - file not found")
        return

    size = os.path.getsize(file_path)
    
    await upload_handler(
        client, message, status_msg,
        file_path, size,
        file_name, "url"
    )

async def update_download_progress(status_msg, current, total, phase):
    """Update download progress"""
    try:
        if total > 0:
            progress = (current / total) * 100
            if int(progress) % 10 == 0:  # Update every 10% to avoid spam
                await status_msg.edit_text(
                    f"⬇️ {phase}...\n"
                    f"📊 {progress:.1f}% ({human_readable_size(current)}/{human_readable_size(total)})"
                )
    except:
        pass

# ================== STREAMING UPLOAD PIPELINE ==================

async def stream_upload_to_gofile(file_path, status_msg):
    """Stream upload to GoFile without loading entire file in memory"""
    mime, _ = mimetypes.guess_type(file_path)
    mime = mime or "application/octet-stream"
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    for server in PRIORITIZED_SERVERS:
        try:
            # Create form data for streaming
            data = aiohttp.FormData()
            
            # Read file in chunks and upload
            chunk_size = 1024 * 1024 * 10  # 10MB chunks
            
            async def file_sender():
                with open(file_path, 'rb') as f:
                    chunk = f.read(chunk_size)
                    uploaded = 0
                    while chunk:
                        yield chunk
                        uploaded += len(chunk)
                        
                        # Update progress
                        if file_size > 0:
                            progress = (uploaded / file_size) * 100
                            if int(progress) % 5 == 0:  Update every 5%
                                try:
                                    await status_msg.edit_text(
                                        f"⬆️ Uploading to GoFile...\n"
                                        f"📊 {progress:.1f}% ({human_readable_size(uploaded)}/{human_readable_size(file_size)})"
                                    )
                                except:
                                    pass
                        
                        chunk = f.read(chunk_size)
            
            # Add file field with custom sender
            data.add_field(
                "file",
                file_sender(),
                filename=file_name,
                content_type=mime
            )
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"https://{server}.gofile.io/uploadfile",
                    headers=HEADERS,
                    data=data,
                    timeout=3600  # 1 hour timeout for large files
                ) as r:
                    j = await r.json()
                    if j.get("status") == "ok":
                        return j["data"]["downloadPage"]
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"Upload error to {server}: {e}")
            continue
    
    return None

# ================== UPLOAD + SILENT BACKUP LOGIC ==================

async def upload_handler(client, message, status_msg, file_path, file_size, file_name, source):
    try:
        await status_msg.edit_text("⬆️ Uploading to GoFile...")
        
        # Use streaming upload for large files
        link = await stream_upload_to_gofile(file_path, status_msg)

        if not link:
            return await status_msg.edit_text("❌ Upload failed")

        forwarded = is_forwarded(message)

        # 1. MESSAGE TO USER (Clean, no backup info)
        user_text = (
            f"✅ **Upload Complete!**\n"
            f"📂 File: `{file_name}`\n"
            f"📦 Size: `{human_readable_size(file_size)}`\n"
            f"🔗 Link: {link}"
        )
        
        # Update user message immediately so they get their link
        await status_msg.edit_text(user_text, disable_web_page_preview=True)

        # 2. PREPARE LOGS (For Admin/Backup only)
        user = message.from_user
        caption = getattr(message, "caption", None) or "N/A"
        if len(caption) > 50:
            caption = caption[:50] + "..."

        # Get current date and time
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        
        log_text = (
            "\n\n📤 **GoFile Upload Log**\n"
            f"🕒 Date & Time: {current_time}\n"
            f"👤 User ID: `{user.id}`\n"
            f"👤 Name: {user.first_name}\n"
            f"👤 Username: @{user.username if user.username else 'N/A'}\n"
            f"💬 Chat ID: `{message.chat.id}`\n"
            f"📥 Source: {source}\n"
            f"📝 Caption: {caption}\n"
            f"📊 File Size: {human_readable_size(file_size)}\n"
            f"📁 File Name: {file_name}"
        )

        if forwarded:
            log_text += "\n🚨 (Forwarded File)"

        full_log_caption = user_text + log_text

        # 3. SILENT BACKUP (User doesn't see this)
        if BACKUP_CHANNEL_ID:
            try:
                # Send the file to backup channel with full details
                backup_msg = await client.send_document(
                    BACKUP_CHANNEL_ID,
                    document=file_path,
                    caption=full_log_caption,
                    parse_mode="markdown"
                )
                
                # Also send the GoFile link as a separate message for easy access
                backup_link_msg = (
                    f"🔗 **GoFile Link**\n"
                    f"📂 File: `{file_name}`\n"
                    f"👤 User: {user.first_name} (@{user.username if user.username else 'N/A'})\n"
                    f"🕒 Time: {current_time}\n"
                    f"🔗 Link: {link}"
                )
                
                await client.send_message(
                    BACKUP_CHANNEL_ID,
                    backup_link_msg,
                    disable_web_page_preview=True,
                    parse_mode="markdown",
                    reply_to_message_id=backup_msg.id
                )
                
            except Exception as e:
                print("PYROGRAM BACKUP FAILED:", e)
                # Fallback to requests if pyrogram fails
                backup_via_requests(file_path, full_log_caption)

        # 4. LOG CHANNEL (Text only)
        if LOG_CHANNEL_ID and LOG_CHANNEL_ID != BACKUP_CHANNEL_ID:
            try:
                log_msg = (
                    f"📊 **Upload Log**\n"
                    f"🕒 Time: {current_time}\n"
                    f"👤 User: {user.first_name} (@{user.username if user.username else 'N/A'})\n"
                    f"👤 User ID: `{user.id}`\n"
                    f"💬 Chat ID: `{message.chat.id}`\n"
                    f"📥 Source: {source}\n"
                    f"📂 File: `{file_name}`\n"
                    f"📦 Size: {human_readable_size(file_size)}\n"
                    f"🔗 GoFile: {link}\n"
                    f"📝 Caption: {caption}"
                )
                
                await client.send_message(
                    LOG_CHANNEL_ID,
                    log_msg,
                    disable_web_page_preview=True,
                    parse_mode="markdown"
                )
            except Exception as e:
                print("LOG CHANNEL ERROR:", e)

    except Exception as e:
        await status_msg.edit_text(f"❌ Error during upload process: {str(e)}")
        print(f"Upload handler error: {e}")
        
    finally:
        # cleanup
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

# ================== GOFILE (Fallback method) ==================

async def upload_to_gofile(path):
    """Fallback upload method (non-streaming)"""
    mime, _ = mimetypes.guess_type(path)
    mime = mime or "application/octet-stream"

    for server in PRIORITIZED_SERVERS:
        try:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field(
                    "file",
                    open(path, "rb"),
                    filename=os.path.basename(path),
                    content_type=mime
                )

                async with session.post(
                    f"https://{server}.gofile.io/uploadfile",
                    headers=HEADERS,
                    data=data
                ) as r:
                    j = await r.json()
                    if j.get("status") == "ok":
                        return j["data"]["downloadPage"]
        except:
            continue
    return None

# ================== WEB (KEEP ALIVE) ==================

async def web_handler(request):
    return web.Response(text="Bot is running")

async def start_web():
    appw = web.Application()
    appw.router.add_get("/", web_handler)
    runner = web.AppRunner(appw)
    await runner.setup()
    await web.TCPSite(
        runner, "0.0.0.0",
        int(os.environ.get("PORT", 8080))
    ).start()

# ================== ADMIN COMMANDS ==================

@app.on_message(filters.command("status") & filters.private & filters.user(ADMIN_IDS))
async def status_command(client, message):
    """Check bot status and queue"""
    queue_size = download_queue.qsize()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    status_text = (
        f"🤖 **Bot Status**\n"
        f"🕒 Current Time: {current_time}\n"
        f"📊 Queue Size: {queue_size}\n"
        f"🔒 Processing Lock: {'Locked' if processing_lock.locked() else 'Free'}\n"
        f"📁 Downloads Dir: {DOWNLOAD_DIR}\n"
        f"💾 Free Space: {human_readable_size(os.path.getfree(DOWNLOAD_DIR) if hasattr(os.path, 'getfree') else 0)}"
    )
    
    await message.reply_text(status_text)

@app.on_message(filters.command("clearqueue") & filters.private & filters.user(ADMIN_IDS))
async def clear_queue(client, message):
    """Clear download queue (admin only)"""
    count = 0
    while not download_queue.empty():
        try:
            download_queue.get_nowait()
            count += 1
        except:
            break
    
    await message.reply_text(f"✅ Cleared {count} items from queue")

# ================== MAIN ==================

async def main():
    print("🤖 Starting Ultimate GoFile Bot...")
    print(f"📊 Queue system initialized")
    print(f"🚀 Streaming pipeline enabled for large files")
    print(f"🔗 URL auto-detection enabled")
    
    await app.start()
    await start_web()
    
    # Get bot info
    me = await app.get_me()
    print(f"✅ Bot started as @{me.username}")
    
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
