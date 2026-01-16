import os
import aiohttp
import asyncio
import time
import json
import requests
import mimetypes
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from asyncio import Queue, Lock
from aiohttp import web

# ================== CONFIG ==================

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN")

BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID"))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID"))

ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "5978396634").split()]

MAX_FILE_SIZE = 500 * 1024 * 1024
MAX_URL_UPLOAD_SIZE = 500 * 1024 * 1024

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
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

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

# ================== COMMANDS ==================

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text(
        "👋 **Welcome!**\n\n"
        "📤 Send me a file OR\n"
        "🔗 Use `/upload <url>`"
    )

@app.on_message(filters.command("upload"))
async def url_upload(client, message):
    try:
        url = message.text.split(maxsplit=1)[1]
    except IndexError:
        return await message.reply_text("❌ Usage: `/upload <url>`")

    msg = await message.reply_text("📥 Added to queue")
    await download_queue.put(("url", url, message, msg))
    asyncio.create_task(process_queue(client))

# ================== FILE HANDLING ==================

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_file(client, message):
    media = message.document or message.video or message.audio or message.photo

    if media.file_size > MAX_FILE_SIZE:
        return await message.reply_text("❌ File too large (500MB max)")

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
    file_name = getattr(media, "file_name", f"file_{message.id}")
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    await status_msg.edit_text("⬇️ Downloading...")
    await client.download_media(message, file_path)

    await upload_handler(
        client, message, status_msg,
        file_path, media.file_size,
        file_name, "telegram"
    )

async def process_url_file(client, url, message, status_msg):
    file_name = url.split("/")[-1] or f"file_{int(time.time())}"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    await status_msg.edit_text("⬇️ Downloading from URL...")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            with open(file_path, "wb") as f:
                while True:
                    chunk = await r.content.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)

    size = os.path.getsize(file_path)

    await upload_handler(
        client, message, status_msg,
        file_path, size,
        file_name, "url"
    )

# ================== UPLOAD + FORWARD ==================

async def upload_handler(client, message, status_msg, file_path, file_size, file_name, source):
    try:
        await status_msg.edit_text("⬆️ Uploading to Gofile...")
        link = await upload_to_gofile(file_path)

        if not link:
            return await status_msg.edit_text("❌ Upload failed")

        # ===== USER MESSAGE =====
        await status_msg.edit_text(
            f"✅ **Upload Complete!**\n"
            f"📂 File: `{file_name}`\n"
            f"📦 Size: `{human_readable_size(file_size)}`\n"
            f"🔗 Link: {link}",
            disable_web_page_preview=True
        )

        user = message.from_user
        caption = getattr(message, "caption", None) or "N/A"
        if len(caption) > 50:
            caption = caption[:50] + "..."

        meta = (
            "File Uploaded Successfully\n"
            f"User ID: {user.id}\n"
            f"First Name: {user.first_name}\n"
            f"Username: @{user.username if user.username else 'N/A'}\n"
            f"File Source: {source}\n"
            f"File Size: {human_readable_size(file_size)}\n"
            f"Original Caption: {caption}\n"
            f"Download Link: {link}"
        )

        # ===== BACKUP CHANNEL =====
        try:
            await client.send_document(
                BACKUP_CHANNEL_ID,
                document=file_path,
                caption=meta,
                parse_mode=None
            )
        except Exception as e:
            print("PYROGRAM BACKUP FAIL:", e)
            backup_via_requests(file_path, meta)

        # ===== LOG CHANNEL =====
        if LOG_CHANNEL_ID != BACKUP_CHANNEL_ID:
            await client.send_message(
                LOG_CHANNEL_ID,
                meta,
                parse_mode=None,
                disable_web_page_preview=True
            )

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# ================== GOFILE ==================

async def upload_to_gofile(path):
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

# ================== WEB (RENDER KEEP ALIVE) ==================

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

# ================== MAIN ==================

async def main():
    await app.start()
    await start_web()
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
