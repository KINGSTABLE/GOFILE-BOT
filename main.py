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

# ================== CONFIG ==================

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN")

BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", -1003648024683))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", -1003648024683))

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

PORT = int(os.environ.get("PORT", 8080))

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

async def backup_via_pyrogram(client, file_path, caption):
    try:
        await client.send_document(BACKUP_CHANNEL_ID, file_path, caption=caption)
        return True
    except Exception as e:
        print("BACKUP ERROR:", e)
        return False

async def get_server():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.gofile.io/getServer", headers=HEADERS) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data['data']['server']
            else:
                raise Exception("Failed to get server")

async def upload_file(file_path):
    server = await get_server()
    url = f"https://{server}.gofile.io/uploadFile"
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field('file', open(file_path, 'rb'))
        async with session.post(url, headers=HEADERS, data=form) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data['data']['downloadPage']
            else:
                raise Exception("Upload failed")

async def download_url(url, path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(path, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
            else:
                raise Exception("Download failed")

# ================== WORKER ==================

async def process_worker():
    while True:
        message = await download_queue.get()
        async with processing_lock:
            try:
                if is_forwarded(message):
                    continue

                user_id = message.from_user.id
                if user_id not in ADMIN_IDS:
                    await message.reply("You are not authorized to use this bot.")
                    continue

                if message.text and message.command and message.command[0] == 'upload':
                    if len(message.command) < 2:
                        await message.reply("Usage: /upload <url>")
                        continue
                    url = message.command[1]
                    file_name = os.path.basename(url) or f"file_{time.time()}"
                    file_path = os.path.join(DOWNLOAD_DIR, file_name)
                    await message.reply("Downloading from URL...")
                    await download_url(url, file_path)
                    file_size = os.path.getsize(file_path)
                    if file_size > MAX_URL_UPLOAD_SIZE:
                        await message.reply("File too large.")
                        os.remove(file_path)
                        continue
                else:
                    media = message.document or message.video or message.audio or message.photo
                    if not media:
                        continue
                    if media.file_size > MAX_FILE_SIZE:
                        await message.reply("File too large.")
                        continue
                    file_name = media.file_name or f"file_{time.time()}"
                    file_path = os.path.join(DOWNLOAD_DIR, file_name)
                    await message.reply("Downloading file...")
                    await message.download(file_path)

                await message.reply("Uploading to Gofile...")
                download_page = await upload_file(file_path)
                await message.reply(f"Upload successful!\nDownload Page: {download_page}")

                caption = f"Uploaded by {message.from_user.mention}\nFile: {file_name}\nSize: {human_readable_size(os.path.getsize(file_path))}"
                await app.send_message(LOG_CHANNEL_ID, caption + f"\nLink: {download_page}")
                await backup_via_pyrogram(app, file_path, caption)

                os.remove(file_path)
            except Exception as e:
                await message.reply(f"Error: {str(e)}")
            finally:
                download_queue.task_done()

# ================== HANDLERS ==================

@app.on_message(filters.private)
async def forward_to_backup(client, message):
    try:
        await message.forward(BACKUP_CHANNEL_ID)
    except:
        pass  # Ignore if cannot forward (e.g., empty message)

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text(
        "Welcome to GoFile Uploader Bot\n\n"
        "Send me a file to upload to Gofile.io\n"
        "Or use /upload <url> to upload from a direct link.\n"
        "Use /help for more information."
    )

@app.on_message(filters.command("help"))
async def help_cmd(client, message):
    await message.reply_text(
        "Commands:\n"
        "/start - Start the bot and show welcome message\n"
        "/help - Show this help message\n"
        "/upload <url> - Upload a file from the provided URL to Gofile.io\n\n"
        "You can also send files directly (documents, videos, etc.) to upload them.\n"
        "Note: Only admins can use upload features. Max file size: 500 MB."
    )

@app.on_message(filters.command("upload") | filters.media & filters.private & ~filters.bot)
async def handle_upload(client, message):
    await download_queue.put(message)

# ================== WEB SERVER FOR RENDER ==================

routes = web.RouteTableDef()

@routes.get("/")
async def root_route_handler(request):
    return web.json_response({"status": "running"})

async def web_server():
    web_app = web.Application()
    web_app.add_routes(routes)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Web server started on port {PORT}")

# ================== MAIN ==================

async def main():
    await app.start()
    asyncio.create_task(process_worker())
    asyncio.create_task(web_server())
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
