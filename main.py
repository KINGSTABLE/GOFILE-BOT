import os
import aiohttp
import asyncio
import time
import json
import math
import shutil
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
# Note: BACKUP_CHANNEL_ID is for file archives. LOG_CHANNEL_ID is for admin logs.
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", "0"))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", "0"))
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "5978396634").split()]

# Feature 1: Force Subscribe Config
FORCE_SUB_CHANNEL_ID = int(os.environ.get("FORCE_SUB_CHANNEL_ID", "-1002642665601"))
FORCE_SUB_INVITE_LINK = os.environ.get("FORCE_SUB_INVITE_LINK", "https://t.me/TOOLS_BOTS_KING")

# Limits
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB (Gofile limit)
MAX_URL_UPLOAD_SIZE = 500 * 1024 * 1024 # 250 MB (Limit for URL uploads to save server RAM)

# Server Config
PRIORITIZED_SERVERS = ["upload-na-phx", "upload-ap-sgp", "upload-ap-hkg", "upload-eu-ams"]
HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DB_FILE = "users_db.json"

# Initialize Client
app = Client("ultimate_gofile_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Global State
download_queue = Queue()
processing_lock = Lock()
user_rename_preferences = {} # Stores custom filenames: {user_id: "filename.ext"}
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

# ==============================================================================
# 🔐 SECURITY CHECKS (Decorators)
# ==============================================================================

async def check_permissions(client, message):
    user_id = message.from_user.id
    
    # 1. Maintenance Check
    if maintenance_mode and user_id not in ADMIN_IDS:
        await message.reply_text("🚧 **Bot is in Maintenance Mode.**\nPlease try again later.")
        return False

    # 2. Ban Check
    if is_banned(user_id):
        return False # Silently ignore banned users

    # 3. Force Subscribe Check
    if FORCE_SUB_CHANNEL_ID:
        try:
            user = await client.get_chat_member(FORCE_SUB_CHANNEL_ID, user_id)
            if user.status in ["kicked", "left"]:
                raise UserNotParticipant
        except UserNotParticipant:
            buttons = [[InlineKeyboardButton("📢 Join Update Channel", url=FORCE_SUB_INVITE_LINK)]]
            if "start" in getattr(message, "text", ""):
                 buttons.append([InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{app.me.username}?start=start")])
            
            await message.reply_text(
                "🛑 **Access Denied!**\n\nYou must join our channel to use this bot.",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return False
        except Exception:
            pass # If bot isn't admin in channel, skip check to avoid errors

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
        "I am the **Ultimate Gofile Uploader**.\n\n"
        "🔹 **Upload:** Send any file (Max 500MB)\n"
        "🔹 **URL Upload:** `/upload http://link.com/file.mp4`\n"
        "🔹 **Rename:** `/rename newname.mp4` (Set name before sending file)\n\n"
        "🚀 _Powered by Render_"
    )

# --- Feature 17: Stats ---
@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(client, message):
    db = get_db()
    total, used, free = shutil.disk_usage(".")
    await message.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Total Users:** {len(db['users'])}\n"
        f"🚫 **Banned Users:** {len(db['banned'])}\n"
        f"💾 **Disk Free:** {human_readable_size(free)}\n"
        f"💿 **Disk Used:** {human_readable_size(used)}"
    )

# --- Feature 11: Rename ---
@app.on_message(filters.command("rename"))
async def set_rename(client, message):
    if not await check_permissions(client, message): return
    try:
        new_name = message.text.split(maxsplit=1)[1]
        user_rename_preferences[message.from_user.id] = new_name
        await message.reply_text(f"✍️ **Rename Set:** `{new_name}`\n\nNow send me the file/video!")
    except IndexError:
        await message.reply_text("❌ Usage: `/rename NewName.mp4`")

# --- Feature 4: Maintenance ---
@app.on_message(filters.command("maintenance") & filters.user(ADMIN_IDS))
async def toggle_maintenance(client, message):
    global maintenance_mode
    maintenance_mode = not maintenance_mode
    status = "ON 🔴" if maintenance_mode else "OFF 🟢"
    await message.reply_text(f"🚧 **Maintenance Mode is now {status}**")

# --- Feature 3: Ban/Unban ---
@app.on_message(filters.command("ban") & filters.user(ADMIN_IDS))
async def ban_command(client, message):
    try:
        user_id = int(message.text.split()[1])
        ban_user_db(user_id)
        await message.reply_text(f"🚫 User `{user_id}` has been BANNED.")
    except: await message.reply_text("❌ Usage: `/ban 12345678`")

@app.on_message(filters.command("unban") & filters.user(ADMIN_IDS))
async def unban_command(client, message):
    try:
        user_id = int(message.text.split()[1])
        unban_user_db(user_id)
        await message.reply_text(f"✅ User `{user_id}` UNBANNED.")
    except: await message.reply_text("❌ Usage: `/unban 12345678`")

# --- Feature 2: Broadcast ---
@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast(client, message):
    if not message.reply_to_message:
        await message.reply_text("❌ Reply to a message to broadcast it.")
        return
    
    msg = await message.reply_text("📢 **Starting Broadcast...**")
    users = get_db()["users"]
    sent, failed = 0, 0
    
    for user_id in users:
        try:
            await message.reply_to_message.copy(chat_id=user_id)
            sent += 1
            await asyncio.sleep(0.1) # Prevent floodwait
        except (UserIsBlocked, InputUserDeactivated):
            failed += 1
        except Exception:
            failed += 1
            
    await msg.edit_text(f"✅ **Broadcast Complete**\n\nSent: {sent}\nFailed: {failed}")

# --- Feature 20: URL Leech ---
@app.on_message(filters.command("upload"))
async def url_upload(client, message):
    if not await check_permissions(client, message): return
    try:
        url = message.text.split(maxsplit=1)[1]
    except IndexError:
        await message.reply_text("❌ Usage: `/upload http://example.com/video.mp4`")
        return

    # Check queue limit
    if download_queue.qsize() > 5:
        await message.reply_text("⚠️ Queue is full. Please wait.")
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
    
    # Identify media
    media = message.document or message.video or message.audio or message.photo
    if message.photo: media.file_name = f"photo_{message.id}.jpg" # Photos have no name
    
    if media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ **File too large!**\nLimit: 500MB\nYour File: {human_readable_size(media.file_size)}")
        return

    # Check for rename
    custom_name = user_rename_preferences.pop(message.from_user.id, None)
    
    msg = await message.reply_text(f"✅ **Added to Queue**\nPosition: {download_queue.qsize() + 1}")
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

# --- Telegram File Processor ---
async def process_tg_file(client, media, message, status_msg, custom_name):
    try:
        file_name = custom_name or getattr(media, 'file_name', f"file_{message.id}")
        file_path = os.path.join("downloads", file_name)
        
        await status_msg.edit_text("📥 **Downloading from Telegram...**")
        start_time = time.time()
        
        await client.download_media(message, file_name=file_path)
        
        await upload_handler(client, message, status_msg, file_path, media.file_size, file_name, "Telegram File")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")

# --- URL File Processor ---
async def process_url_file(client, url, message, status_msg):
    try:
        file_name = url.split("/")[-1] or f"leech_{int(time.time())}.dat"
        file_path = os.path.join("downloads", file_name)
        
        await status_msg.edit_text("📥 **Downloading from URL...**")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    await status_msg.edit_text("❌ Invalid URL or Server Error.")
                    return
                
                # Size Check
                total_size = int(resp.headers.get('Content-Length', 0))
                if total_size > MAX_URL_UPLOAD_SIZE:
                    await status_msg.edit_text(f"❌ File too large for URL Upload.\nLimit: 500MB")
                    return

                with open(file_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024*1024) # 1MB chunks
                        if not chunk: break
                        f.write(chunk)
        
        file_size = os.path.getsize(file_path)
        await upload_handler(client, message, status_msg, file_path, file_size, file_name, "URL Upload")
    except Exception as e:
        await status_msg.edit_text(f"❌ URL Error: {e}")

# --- Common Upload Logic ---
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
            
            # Feature 18: Admin Log
            if LOG_CHANNEL_ID:
                await client.send_message(
                    LOG_CHANNEL_ID,
                    f"**#NEW_UPLOAD** ({type_tag})\n"
                    f"👤 {message.from_user.mention} (`{message.from_user.id}`)\n"
                    f"📂 `{file_name}`\n"
                    f"🔗 {link}",
                    disable_web_page_preview=True
                )
        else:
            await status_msg.edit_text("❌ Upload Failed (Gofile Error).")
            
    except Exception as e:
        print(f"Upload Error: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

async def upload_to_gofile(path):
    for server in PRIORITIZED_SERVERS:
        try:
            url = f"https://{server}.gofile.io/uploadfile"
            async with aiohttp.ClientSession() as session:
                with open(path, "rb") as f:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=os.path.basename(path))
                    data.add_field('token', GOFILE_API_TOKEN)
                    async with session.post(url, data=data) as response:
                        if response.status == 200:
                            res = await response.json()
                            if res['status'] == 'ok': return res['data']['downloadPage']
        except: continue
    return None

# ==============================================================================
# 🌐 WEB SERVER (Keep Alive)
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
