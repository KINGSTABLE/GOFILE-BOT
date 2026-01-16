import os
import aiohttp
import asyncio
import time
import json
import math
import shutil
import logging
from datetime import datetime, timedelta
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, InputUserDeactivated
from asyncio import Queue, Lock
from aiohttp import web

# Optional: OpenCV for screenshots (Graceful fallback if not installed)
try:
    import cv2
    HAS_OPENCV = True
except ImportError:
    HAS_OPENCV = False

# ==============================================================================
# ⚙️ CONFIGURATION
# ==============================================================================

# ⚠️ SECURITY: Set these in Render Environment Variables!
API_ID = os.environ.get("API_ID") 
API_HASH = os.environ.get("API_HASH") 
BOT_TOKEN = os.environ.get("BOT_TOKEN") 
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN") 

# Admin & Channels
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", "0"))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", "0"))
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "5978396634").split()]

# Feature 1: Force Subscribe Config
FORCE_SUB_CHANNEL_ID = int(os.environ.get("FORCE_SUB_CHANNEL_ID", "-1002642665601"))
FORCE_SUB_INVITE_LINK = os.environ.get("FORCE_SUB_INVITE_LINK", "https://t.me/TOOLS_BOTS_KING")

# Limits
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB (Gofile limit)
MAX_URL_UPLOAD_SIZE = 500 * 1024 * 1024 # 500 MB

# Server Config
PRIORITIZED_SERVERS = ["upload-na-phx", "upload-ap-sgp", "upload-ap-hkg", "upload-eu-ams"]
HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DB_FILE = "users_db.json"
START_TIME = time.time()

# Initialize Client
app = Client("ultimate_gofile_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Global State
download_queue = Queue()
processing_lock = Lock()
user_rename_preferences = {} 
maintenance_mode = False

# ==============================================================================
# 📊 ANALYTICS & DASHBOARD STATE
# ==============================================================================
# Lifetime stats (persisted in DB mostly, but tracked live here)
global_stats = {
    "total_uploads": 0,
    "total_data_moved": 0,
    "failed_uploads": 0,
    "active_session_users": set()
}

# Daily stats (Reset every 24h)
daily_stats = {
    "date": datetime.utcnow().date(),
    "uploads": 0,
    "failed": 0,
    "file_types": {"Video": 0, "Photo": 0, "Doc": 0, "Audio": 0, "Other": 0},
    "peak_hours": {} 
}

# Ensure Directories
if not os.path.exists("downloads"): os.makedirs("downloads")
if not os.path.exists(DB_FILE): 
    with open(DB_FILE, "w") as f: json.dump({"users": [], "banned": []}, f)

# ==============================================================================
# 🛠️ HELPER FUNCTIONS & CLASSES
# ==============================================================================

class ProgressReader:
    """Custom wrapper to track Upload Progress for aiohttp"""
    def __init__(self, filename, callback):
        self._file = open(filename, 'rb')
        self._total = os.path.getsize(filename)
        self._read = 0
        self._callback = callback

    def read(self, size=-1):
        data = self._file.read(size)
        if not data: return b''
        self._read += len(data)
        try: asyncio.create_task(self._callback(self._read, self._total))
        except: pass
        return data

    def close(self):
        self._file.close()

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
    global_stats["active_session_users"].add(user_id)

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

def generate_thumbnail(video_path):
    if not HAS_OPENCV: return None
    thumb_path = f"{video_path}.jpg"
    try:
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        # Get frame from 10% into the video or middle
        cap.set(cv2.CAP_PROP_POS_FRAMES, min(total_frames // 2, 100)) 
        success, image = cap.read()
        if success:
            cv2.imwrite(thumb_path, image)
            cap.release()
            return thumb_path
        cap.release()
    except Exception as e:
        print(f"Thumb Error: {e}")
    return None

def update_daily_analytics(file_type_tag):
    # Reset check
    now = datetime.utcnow()
    if daily_stats["date"] != now.date():
        daily_stats["date"] = now.date()
        daily_stats["uploads"] = 0
        daily_stats["failed"] = 0
        daily_stats["file_types"] = {k: 0 for k in daily_stats["file_types"]}
        daily_stats["peak_hours"] = {}

    daily_stats["uploads"] += 1
    daily_stats["file_types"][file_type_tag] = daily_stats["file_types"].get(file_type_tag, 0) + 1
    
    hour = now.strftime("%H")
    daily_stats["peak_hours"][hour] = daily_stats["peak_hours"].get(hour, 0) + 1

async def progress_bar(current, total, status_msg, start_time, mode="Downloading"):
    now = time.time()
    # Update every 3 seconds to avoid FloodWait
    if not hasattr(progress_bar, "last_update"): progress_bar.last_update = 0
    if now - progress_bar.last_update < 3 and current != total: return

    progress_bar.last_update = now
    percentage = current * 100 / total
    speed = current / (now - start_time) if now - start_time > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    
    filled_len = int(percentage // 10)
    bar = '█' * filled_len + '▒' * (10 - filled_len)
    
    try:
        await status_msg.edit_text(
            f"**{mode}...**\n\n"
            f"**Progress:** [{bar}] {percentage:.1f}%\n"
            f"**Speed:** {human_readable_size(speed)}/s\n"
            f"**Data:** {human_readable_size(current)} / {human_readable_size(total)}\n"
            f"**ETA:** {int(eta)}s"
        )
    except Exception: pass

# ==============================================================================
# 🔐 SECURITY CHECKS
# ==============================================================================

async def check_permissions(client, message):
    user_id = message.from_user.id
    
    if maintenance_mode and user_id not in ADMIN_IDS:
        await message.reply_text("🚧 **Bot is in Maintenance Mode.**")
        return False

    if is_banned(user_id):
        return False 

    if FORCE_SUB_CHANNEL_ID:
        try:
            user = await client.get_chat_member(FORCE_SUB_CHANNEL_ID, user_id)
            if user.status in ["kicked", "left"]: raise UserNotParticipant
        except UserNotParticipant:
            buttons = [[InlineKeyboardButton("📢 Join Update Channel", url=FORCE_SUB_INVITE_LINK)]]
            if "start" in getattr(message, "text", ""):
                 buttons.append([InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{app.me.username}?start=start")])
            await message.reply_text(
                "🛑 **Access Denied!**\n\nYou must join our channel to use this bot.",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return False
        except Exception: pass 
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
        "I am the **Ultimate Gofile Uploader** with **UX Pro Features**.\n\n"
        "📊 **Live Progress Bars**\n"
        "📸 **Auto-Screenshots**\n"
        "🚀 **Direct URL Uploads**\n\n"
        "👇 **Click /help to see all commands!**"
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    if not await check_permissions(client, message): return
    
    help_text = (
        "📚 **User Commands:**\n\n"
        "🔹 `/start` - Wake up the bot\n"
        "🔹 `/upload <url>` - Upload from direct link\n"
        "🔹 `/rename <name.ext>` - Set custom filename\n\n"
        "📂 **Simply send any file to upload!**"
    )

    if message.from_user.id in ADMIN_IDS:
        help_text += (
            "\n\n👮‍♂️ **Admin Commands:**\n"
            "🔸 `/stats` - View Dashboard Link & Disk Info\n"
            "🔸 `/broadcast` - Broadcast message\n"
            "🔸 `/ban <id>` - Ban user\n"
            "🔸 `/unban <id>` - Unban user\n"
            "🔸 `/maintenance` - Toggle Maintenance"
        )
    await message.reply_text(help_text)

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def stats(client, message):
    db = get_db()
    total, used, free = shutil.disk_usage(".")
    
    # Get the URL (assuming standard Render format or configured domain)
    app_url = os.environ.get("RENDER_EXTERNAL_URL", "https://your-app-name.onrender.com")
    
    await message.reply_text(
        f"📊 **System Statistics**\n\n"
        f"👥 **Total Users (DB):** {len(db['users'])}\n"
        f"⚡ **Active Session:** {len(global_stats['active_session_users'])}\n"
        f"🚫 **Banned:** {len(db['banned'])}\n"
        f"💾 **Disk Free:** {human_readable_size(free)}\n\n"
        f"🔗 **Admin Dashboard:**\n{app_url}"
    )

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
    status = "ON 🔴" if maintenance_mode else "OFF 🟢"
    await message.reply_text(f"🚧 **Maintenance Mode is now {status}**")

@app.on_message(filters.command("ban") & filters.user(ADMIN_IDS))
async def ban_command(client, message):
    try:
        user_id = int(message.text.split()[1])
        ban_user_db(user_id)
        await message.reply_text(f"🚫 User `{user_id}` BANNED.")
    except: await message.reply_text("❌ Usage: `/ban 12345678`")

@app.on_message(filters.command("unban") & filters.user(ADMIN_IDS))
async def unban_command(client, message):
    try:
        user_id = int(message.text.split()[1])
        unban_user_db(user_id)
        await message.reply_text(f"✅ User `{user_id}` UNBANNED.")
    except: await message.reply_text("❌ Usage: `/unban 12345678`")

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast(client, message):
    if not message.reply_to_message:
        await message.reply_text("❌ Reply to a message to broadcast.")
        return
    msg = await message.reply_text("📢 **Broadcasting...**")
    users = get_db()["users"]
    sent, failed = 0, 0
    for user_id in users:
        try:
            await message.reply_to_message.copy(chat_id=user_id)
            sent += 1
            await asyncio.sleep(0.1) 
        except: failed += 1
    await msg.edit_text(f"✅ **Broadcast Done**\nSent: {sent}\nFailed: {failed}")

@app.on_message(filters.command("upload"))
async def url_upload(client, message):
    if not await check_permissions(client, message): return
    try: url = message.text.split(maxsplit=1)[1]
    except IndexError: 
        await message.reply_text("❌ Usage: `/upload http://link.com/video.mp4`")
        return

    if download_queue.qsize() > 5:
        await message.reply_text("⚠️ Queue full.")
        return

    msg = await message.reply_text("🔗 **Processing URL...**")
    await download_queue.put(("url", url, message, msg, None)) # None = no custom name predefined
    asyncio.create_task(process_queue(client))

# ==============================================================================
# 📂 ADVANCED FILE HANDLING CORE
# ==============================================================================

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_file(client, message):
    if not await check_permissions(client, message): return
    add_user(message.from_user.id)
    
    media = message.document or message.video or message.audio or message.photo
    if message.photo: media.file_name = f"photo_{message.id}.jpg" 
    
    if media.file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ **File too large!**\nLimit: 500MB")
        return

    custom_name = user_rename_preferences.pop(message.from_user.id, None)
    
    msg = await message.reply_text(f"✅ **Added to Queue**\nPosition: {download_queue.qsize() + 1}")
    await download_queue.put(("file", media, message, msg, custom_name))
    asyncio.create_task(process_queue(client))

async def process_queue(client):
    async with processing_lock:
        while not download_queue.empty():
            task = await download_queue.get()
            # task = (type, media/url, message, msg, custom_name)
            if task[0] == "file":
                await process_upload_task(client, task[1], task[2], task[3], task[4], "Telegram")
            elif task[0] == "url":
                await process_url_task(client, task[1], task[2], task[3])

# --- URL Task Wrapper ---
async def process_url_task(client, url, message, status_msg):
    try:
        file_name = url.split("/")[-1] or f"leech_{int(time.time())}.dat"
        file_path = os.path.join("downloads", file_name)
        
        await status_msg.edit_text("📥 **Downloading from URL...**")
        start_dl = time.time()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    await status_msg.edit_text("❌ Invalid URL.")
                    return
                
                total_size = int(resp.headers.get('Content-Length', 0))
                if total_size > MAX_URL_UPLOAD_SIZE:
                    await status_msg.edit_text(f"❌ File too large for URL Upload.")
                    return

                with open(file_path, 'wb') as f:
                    downloaded = 0
                    while True:
                        chunk = await resp.content.read(1024*1024) 
                        if not chunk: break
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Minimal progress update for URL dl
                        if time.time() - start_dl > 4:
                            start_dl = time.time()
                            try: await status_msg.edit_text(f"📥 **Downloading URL...**\n{human_readable_size(downloaded)}")
                            except: pass
        
        file_size = os.path.getsize(file_path)
        # Pass to main upload handler
        await process_upload_logic(client, message, status_msg, file_path, file_size, file_name, "URL Upload", is_video=False)

    except Exception as e:
        await status_msg.edit_text(f"❌ URL Error: {e}")
    finally:
         if os.path.exists(file_path): os.remove(file_path) # Fallback cleanup

# --- Telegram Task Wrapper ---
async def process_upload_task(client, media, message, status_msg, custom_name, origin):
    try:
        file_name = custom_name or getattr(media, 'file_name', f"file_{message.id}")
        file_path = os.path.join("downloads", file_name)
        is_video = bool(message.video)

        start_time = time.time()
        await client.download_media(
            message, 
            file_name=file_path,
            progress=progress_bar,
            progress_args=(status_msg, start_time, "📥 Downloading")
        )
        
        await process_upload_logic(client, message, status_msg, file_path, media.file_size, file_name, origin, is_video)

    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")
        # Clean up if download failed in middle
        if os.path.exists(file_path): os.remove(file_path)

# --- Core Upload Logic (Shared) ---
async def process_upload_logic(client, message, status_msg, file_path, file_size, file_name, origin, is_video):
    thumb_path = None
    try:
        # 1. Screenshot Generation
        if is_video and HAS_OPENCV:
            await status_msg.edit_text("📸 **Generating Thumbnail...**")
            thumb_path = generate_thumbnail(file_path)

        # 2. Upload with Progress
        await status_msg.edit_text("⬆️ **Connecting to Gofile...**")
        start_up = time.time()

        # Define callback locally to capture scope
        async def upload_callback(current, total):
            await progress_bar(current, total, status_msg, start_up, "☁️ Uploading")

        link = await upload_to_gofile(file_path, upload_callback)

        if link:
            # 3. Analytics & Logging
            ftype = "Video" if is_video else "File"
            if file_name.lower().endswith(('.jpg', '.png', '.jpeg')): ftype = "Photo"
            
            global_stats["total_uploads"] += 1
            global_stats["total_data_moved"] += file_size
            update_daily_analytics(ftype)

            # 4. User Success Message
            await status_msg.edit_text(
                f"✅ **Upload Complete!**\n\n"
                f"📂 `{file_name}`\n"
                f"📦 `{human_readable_size(file_size)}`\n"
                f"🔗 **Link:** {link}",
                disable_web_page_preview=True
            )

            # 5. Backup & Admin Log
            log_caption = (
                f"**#NEW_UPLOAD** ({origin})\n"
                f"👤 {message.from_user.mention} (`{message.from_user.id}`)\n"
                f"📂 `{file_name}`\n"
                f"🔗 {link}"
            )
            
            if LOG_CHANNEL_ID:
                try: await client.send_message(LOG_CHANNEL_ID, log_caption, disable_web_page_preview=True)
                except: pass

            if BACKUP_CHANNEL_ID:
                try:
                    # Send with thumb if available
                    if thumb_path:
                        await client.send_document(BACKUP_CHANNEL_ID, file_path, thumb=thumb_path, caption=log_caption)
                    else:
                        await client.send_document(BACKUP_CHANNEL_ID, file_path, caption=log_caption)
                except Exception as e:
                    print(f"Backup failed: {e}")

        else:
            global_stats["failed_uploads"] += 1
            daily_stats["failed"] += 1
            await status_msg.edit_text("❌ Upload Failed (Gofile Rejected).")

    except Exception as e:
        print(f"Logic Error: {e}")
        await status_msg.edit_text(f"❌ Process Error: {e}")
    finally:
        # 🧹 CLEANUP
        if os.path.exists(file_path): os.remove(file_path)
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

async def upload_to_gofile(path, progress_callback):
    # Use ProgressReader wrapper
    file_reader = ProgressReader(path, progress_callback)
    for server in PRIORITIZED_SERVERS:
        try:
            url = f"https://{server}.gofile.io/uploadfile"
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field('file', file_reader, filename=os.path.basename(path))
                data.add_field('token', GOFILE_API_TOKEN)
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        res = await response.json()
                        if res['status'] == 'ok': return res['data']['downloadPage']
        except: continue
    return None

# ==============================================================================
# 📅 DAILY REPORT SCHEDULER
# ==============================================================================

async def daily_report_scheduler():
    while True:
        try:
            now = datetime.utcnow()
            # Wait for next check (every 60s)
            await asyncio.sleep(60)
            
            # Condition: It is 00:00 UTC (or close to it) and we haven't reported yet?
            # Simplified: Use daily_stats["date"] mismatch to trigger summary of "Yesterday"
            # But simpler approach for this bot: Just report status every 24h from start?
            # Better approach: Check if hour is 00 and minute is 00
            if now.hour == 0 and now.minute == 0:
                if LOG_CHANNEL_ID:
                    # Find top file type
                    top_type = max(daily_stats["file_types"], key=daily_stats["file_types"].get)
                    success_rate = 100
                    total = daily_stats["uploads"] + daily_stats["failed"]
                    if total > 0:
                        success_rate = (daily_stats["uploads"] / total) * 100
                    
                    # Find Peak Hour
                    peak_h = "N/A"
                    if daily_stats["peak_hours"]:
                        peak_h = max(daily_stats["peak_hours"], key=daily_stats["peak_hours"].get)

                    report = (
                        f"📈 **Daily Report**\n\n"
                        f"• **Total Uploads:** {daily_stats['uploads']}\n"
                        f"• **Success Rate:** {success_rate:.1f}%\n"
                        f"• **Peak Hour (UTC):** {peak_h}:00\n"
                        f"• **Top File Type:** {top_type} ({daily_stats['file_types'][top_type]})\n\n"
                        f"🤖 _Auto-Generated by Gofile Bot_"
                    )
                    await app.send_message(LOG_CHANNEL_ID, report)
                    
                # Sleep a bit to avoid double sending
                await asyncio.sleep(120) 
                
        except Exception as e:
            print(f"Scheduler Error: {e}")
            await asyncio.sleep(60)

# ==============================================================================
# 🌐 ADMIN DASHBOARD (Web UI)
# ==============================================================================

async def dashboard_handler(request):
    total, used, free = shutil.disk_usage(".")
    uptime = str(timedelta(seconds=int(time.time() - START_TIME)))
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Gofile Bot Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{ background-color: #121212; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 20px; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            .card {{ background: #1e1e1e; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }}
            h2 {{ color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
            .stat-box {{ background: #2c2c2c; padding: 15px; border-radius: 5px; text-align: center; }}
            .stat-val {{ font-size: 24px; font-weight: bold; color: #03dac6; }}
            .stat-label {{ font-size: 12px; color: #aaa; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Bot Admin Panel</h1>
            
            <div class="card">
                <h2>📈 Live Status</h2>
                <div class="grid">
                    <div class="stat-box">
                        <div class="stat-val">{download_queue.qsize()}</div>
                        <div class="stat-label">Queue Size</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-val">{len(global_stats['active_session_users'])}</div>
                        <div class="stat-label">Active Users (Session)</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-val">{uptime}</div>
                        <div class="stat-label">Uptime</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <h2>💾 Performance</h2>
                <div class="grid">
                    <div class="stat-box">
                        <div class="stat-val">{global_stats['total_uploads']}</div>
                        <div class="stat-label">Total Uploads</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-val">{human_readable_size(global_stats['total_data_moved'])}</div>
                        <div class="stat-label">Data Moved</div>
                    </div>
                     <div class="stat-box">
                        <div class="stat-val">{daily_stats['failed']}</div>
                        <div class="stat-label">Failed (Today)</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <h2>📀 Server Disk</h2>
                <p>Free: <b>{human_readable_size(free)}</b> / Used: <b>{human_readable_size(used)}</b></p>
                <div style="background:#333; height:10px; border-radius:5px; overflow:hidden;">
                    <div style="width:{(used/total)*100}%; background:#cf6679; height:100%;"></div>
                </div>
            </div>
            
            <p style="text-align:center; font-size:12px; color:#555;">Render Instance Running</p>
        </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def start_web():
    port = int(os.environ.get("PORT", 8080))
    app_web = web.Application()
    app_web.router.add_get("/", dashboard_handler)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"✅ Dashboard active on port {port}")

# ==============================================================================
# 🔥 MAIN ENTRY POINT
# ==============================================================================

async def main():
    print("--- Ultimate Bot Starting ---")
    await app.start()
    print("--- Telegram Client Connected ---")
    
    # Start Web Server
    await start_web()
    
    # Start Background Tasks (Daily Report)
    asyncio.create_task(daily_report_scheduler())
    
    await idle()
    await app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
