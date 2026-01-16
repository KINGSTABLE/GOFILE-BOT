import os
import aiohttp
import asyncio
import time
import json
import shutil
import requests
import mimetypes
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, InputUserDeactivated
from asyncio import Queue, Lock
from aiohttp import web

# ==============================================================================
# ⚙️ CONFIGURATION
# ==============================================================================

# ⚠️ SECURITY: Set these in Render Environment Variables!
API_ID = os.environ.get("API_ID") 
API_HASH = os.environ.get("API_HASH") 
BOT_TOKEN = os.environ.get("BOT_TOKEN") 
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN") 

# Admin & Channels
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", "-1002889648510"))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", "-1002889648510"))
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "5978396634").split()]

# Feature 1: Force Subscribe Config
FORCE_SUB_CHANNEL_ID = int(os.environ.get("FORCE_SUB_CHANNEL_ID", "-1002642665601"))
FORCE_SUB_INVITE_LINK = os.environ.get("FORCE_SUB_INVITE_LINK", "https://t.me/TOOLS_BOTS_KING")

# Limits
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB (Gofile limit)
MAX_URL_UPLOAD_SIZE = 500 * 1024 * 1024 # 500 MB

# Server Config (Updated from Backup Code)
PRIORITIZED_SERVERS = [
    "upload-na-phx", "upload-ap-sgp", "upload-ap-hkg",
    "upload-ap-tyo", "upload-sa-sao",
]
HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DB_FILE = "users_db.json"

# Initialize Client
app = Client("ultimate_gofile_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Global State
download_queue = Queue()
processing_lock = Lock()
user_rename_preferences = {} 
maintenance_mode = False

# Ensure Directories
if not os.path.exists("downloads"): os.makedirs("downloads")
if not os.path.exists(DB_FILE): 
    with open(DB_FILE, "w") as f: json.dump({"users": [], "banned": []}, f)

# ==============================================================================
# 🛠️ HELPER FUNCTIONS
# ==============================================================================

def get_db():
    try:
        with open(DB_FILE, "r") as f: return json.load(f)
    except: return {"users": [], "banned": []}

def save_db(data):
    with open(DB_FILE, "w") as f: json.dump(data, f)

def add_user(user_id):
    data = get_db()
    if user_id not in data["users"]:
        data["users"].append(user_id)
        save_db(data)

def is_banned(user_id):
    data = get_db()
    return user_id in data.get("banned", [])

def ban_user_db(user_id):
    data = get_db()
    if user_id not in data["banned"]:
        data["banned"].append(user_id)
        save_db(data)

def unban_user_db(user_id):
    data = get_db()
    if user_id in data["banned"]:
        data["banned"].remove(user_id)
        save_db(data)

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0: return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

# --- 🚀 ROBUST BACKUP FALLBACK ---
def backup_via_requests(file_path, caption):
    """
    Uses standard HTTP requests to force the file into the channel.
    This bypasses Pyrogram specific issues.
    """
    try:
        with open(file_path, "rb") as f:
            # sendDocument works for ALL file types
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
            data = {"chat_id": BACKUP_CHANNEL_ID, "caption": caption}
            files = {"document": (os.path.basename(file_path), f)}
            
            response = requests.post(url, data=data, files=files, timeout=60)
            
            if response.status_code == 200:
                print(f"✅ Backup successful via requests fallback.")
                return True
            else:
                print(f"❌ Requests fallback failed: {response.text}")
                return False
    except Exception as e:
        print(f"❌ Backup Error (Requests): {e}")
        return False

# ==============================================================================
# 🔐 SECURITY CHECKS
# ==============================================================================

async def check_permissions(client, message):
    user_id = message.from_user.id
    if maintenance_mode and user_id not in ADMIN_IDS:
        await message.reply_text("🚧 **Bot is in Maintenance Mode.**\nPlease try again later.")
        return False
    if is_banned(user_id):
        return False 
    if FORCE_SUB_CHANNEL_ID:
        try:
            user = await client.get_chat_member(FORCE_SUB_CHANNEL_ID, user_id)
            if user.status in ["kicked", "left"]:
                raise UserNotParticipant
        except UserNotParticipant:
            buttons = [[InlineKeyboardButton("📢 Join Update Channel", url=FORCE_SUB_INVITE_LINK)]]
            if "start" in getattr(message, "text", ""):
                 buttons.append([InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{app.me.username}?start=start")])
            await message.reply_text("🛑 **Access Denied!**\n\nYou must join our channel.", reply_markup=InlineKeyboardMarkup(buttons))
            return False
        except Exception:
            pass 
    return True

# ==============================================================================
# 🎮 COMMANDS
# ==============================================================================

@app.on_message(filters.command("start"))
async def start(client, message):
    if not await check_permissions(client, message): return
    add_user(message.from_user.id)
    await message.reply_text(
        f"👋 **Hello {message.from_user.first_name}!**\n\n"
        "I am the **Ultimate Gofile Uploader**.\n"
        "🚀 **I will upload your files to Gofile and Backup them to your channel!**"
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    if not await check_permissions(client, message): return
    await message.reply_text("🔹 `/upload <url>`\n🔹 `/rename <name>`\n🔹 `/start`")

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(client, message):
    db = get_db()
    total, used, free = shutil.disk_usage(".")
    await message.reply_text(f"👥 Users: {len(db['users'])}\n💾 Free: {human_readable_size(free)}")

@app.on_message(filters.command("rename"))
async def set_rename(client, message):
    if not await check_permissions(client, message): return
    try:
        new_name = message.text.split(maxsplit=1)[1]
        user_rename_preferences[message.from_user.id] = new_name
        await message.reply_text(f"✍️ **Rename Set:** `{new_name}`")
    except IndexError:
        await message.reply_text("❌ Usage: `/rename NewName.mp4`")

@app.on_message(filters.command("maintenance") & filters.user(ADMIN_IDS))
async def toggle_maintenance(client, message):
    global maintenance_mode
    maintenance_mode = not maintenance_mode
    await message.reply_text(f"🚧 **Maintenance:** {'ON' if maintenance_mode else 'OFF'}")

@app.on_message(filters.command("upload"))
async def url_upload(client, message):
    if not await check_permissions(client, message): return
    try:
        url = message.text.split(maxsplit=1)[1]
    except IndexError:
        await message.reply_text("❌ Usage: `/upload http://example.com/video.mp4`")
        return
    if download_queue.qsize() > 5:
        await message.reply_text("⚠️ Queue is full.")
        return
    msg = await message.reply_text("🔗 **Processing URL...**")
    await download_queue.put(("url", url, message, msg))
    asyncio.create_task(process_queue(client))

# ==============================================================================
# 📂 FILE HANDLING CORE
# ==============================================================================

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_file(client, message):
    if not await check_permissions(client, message): return
    add_user(message.from_user.id)
    
    media = message.document or message.video or message.audio or message.photo
    if message.photo: media.file_name = f"photo_{message.id}.jpg" 
    
    if media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ **File too large!** Limit: 500MB")
        return

    custom_name = user_rename_preferences.pop(message.from_user.id, None)
    msg = await message.reply_text(f"✅ **Added to Queue** ({download_queue.qsize() + 1})")
    await download_queue.put(("file", media, message, msg, custom_name))
    asyncio.create_task(process_queue(client))

async def process_queue(client):
    async with processing_lock:
        while not download_queue.empty():
            task = await download_queue.get()
            if task[0] == "file":
                await process_tg_file(client, *task[1:])
            elif task[0] == "url":
                await process_url_file(client, *task[1:])

async def process_tg_file(client, media, message, status_msg, custom_name):
    try:
        file_name = custom_name or getattr(media, 'file_name', f"file_{message.id}")
        file_path = os.path.join("downloads", file_name)
        await status_msg.edit_text("📥 **Downloading...**")
        await client.download_media(message, file_name=file_path)
        await upload_handler(client, message, status_msg, file_path, media.file_size, file_name, "Telegram File")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")

async def process_url_file(client, url, message, status_msg):
    try:
        file_name = url.split("/")[-1] or f"leech_{int(time.time())}.dat"
        file_path = os.path.join("downloads", file_name)
        await status_msg.edit_text("📥 **Downloading from URL...**")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    await status_msg.edit_text("❌ Server Error.")
                    return
                if int(resp.headers.get('Content-Length', 0)) > MAX_URL_UPLOAD_SIZE:
                    await status_msg.edit_text(f"❌ File too large.")
                    return
                with open(file_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024*1024)
                        if not chunk: break
                        f.write(chunk)
        
        file_size = os.path.getsize(file_path)
        await upload_handler(client, message, status_msg, file_path, file_size, file_name, "URL Upload")
    except Exception as e:
        await status_msg.edit_text(f"❌ URL Error: {e}")

# --- 🚀 UPDATED UPLOAD LOGIC & METADATA FORMAT ---
async def upload_handler(client, message, status_msg, file_path, file_size, file_name, type_tag):
    try:
        await status_msg.edit_text("⬆️ **Uploading to Gofile...**")
        
        link = await upload_to_gofile(file_path)
        
        if link:
            # User Success Message
            await status_msg.edit_text(
                f"✅ **Upload Complete!**\n\n"
                f"📂 **File:** `{file_name}`\n"
                f"📦 **Size:** `{human_readable_size(file_size)}`\n"
                f"🔗 **Link:** {link}",
                disable_web_page_preview=True
            )
            
            # --- 📝 METADATA FORMATTING (LIKE SCREENSHOT) ---
            if BACKUP_CHANNEL_ID:
                
                # Gather User Info
                user = message.from_user
                first_name = user.first_name or "N/A"
                username = f"@{user.username}" if user.username else "N/A"
                user_id = user.id
                
                # Gather File Info
                file_type = type_tag.split(" ")[0].lower() # "telegram" or "url"
                if "URL" in type_tag: file_type = "url_upload"
                else: file_type = "document" # Default for telegram
                
                # Get Original Caption (if exists, else N/A)
                original_caption = getattr(message, "caption", "N/A") or "N/A"
                if len(original_caption) > 50: original_caption = original_caption[:50] + "..." # Truncate long captions

                # 📸 THE EXACT FORMAT
                meta_caption = (
                    f"**File Uploaded Successfully ✅**\n\n"
                    f"👤 **User ID:** `{user_id}`\n"
                    f"📛 **First Name:** {first_name}\n"
                    f"🌐 **Username:** {username}\n\n"
                    f"📦 **File Type:** {file_type}\n"
                    f"💾 **File Size:** {human_readable_size(file_size)}\n"
                    f"📝 **Original Caption:** {original_caption}\n\n"
                    f"🔗 **Download Link:**\n{link}"
                )
                
                # --- 🛡️ BACKUP SENDING LOGIC ---
                backup_success = False
                
                # Attempt 1: Pyrogram
                try:
                    await client.send_document(
                        chat_id=BACKUP_CHANNEL_ID,
                        document=file_path,
                        caption=meta_caption
                    )
                    backup_success = True
                    print(f"✅ Backup sent via Pyrogram: {file_name}")
                except Exception as e:
                    print(f"⚠️ Pyrogram Backup Failed: {e}. Trying Fallback...")
                
                # Attempt 2: Requests Fallback (For URL uploads & failed Pyrogram attempts)
                if not backup_success:
                    backup_via_requests(file_path, meta_caption)

            # --- ADMIN LOG ---
            if LOG_CHANNEL_ID and LOG_CHANNEL_ID != BACKUP_CHANNEL_ID:
                try:
                    await client.send_message(
                        LOG_CHANNEL_ID,
                        f"**#NEW_UPLOAD** ({type_tag})\n"
                        f"👤 {message.from_user.mention}\n"
                        f"📂 `{file_name}`\n"
                        f"🔗 {link}",
                        disable_web_page_preview=True
                    )
                except: pass
        else:
            await status_msg.edit_text("❌ Upload Failed (Gofile Error).")
            
    except Exception as e:
        print(f"Upload Error: {e}")
    finally:
        # 🧹 CLEANUP
        if os.path.exists(file_path): 
            os.remove(file_path)

async def upload_to_gofile(path):
    # Logic extracted from Backup Code
    mime_type, _ = mimetypes.guess_type(path)
    mime_type = mime_type or "application/octet-stream"

    for server in PRIORITIZED_SERVERS:
        try:
            async with aiohttp.ClientSession() as session:
                form_data = aiohttp.FormData()
                form_data.add_field("file", open(path, "rb"), filename=os.path.basename(path), content_type=mime_type)
                
                async with session.post(f"https://{server}.gofile.io/uploadfile", headers=HEADERS, data=form_data) as response:
                    response.raise_for_status()
                    result = await response.json()
                    if result.get("status") == "ok":
                        return result["data"]["downloadPage"]
        except Exception:
            continue
    return None

# ==============================================================================
# 🌐 WEB SERVER
# ==============================================================================
async def web_handler(request): return web.Response(text="Ultimate Bot Running")
async def start_web():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application(); app.router.add_get("/", web_handler)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()

async def main():
    print("--- Ultimate Bot Starting ---")
    await app.start()
    await start_web()
    await idle()
    await app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
