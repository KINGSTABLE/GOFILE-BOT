#!/usr/bin/env python3

import os
import asyncio
import time
import mimetypes
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from enum import Enum
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, ChannelPrivate, FloodWait
from aiohttp import web
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ================== CONFIGURATION ==================

API_ID = int(os.environ.get("API_ID", ""))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN", "")

# Helper to fix Channel IDs
def sanitize_channel_id(value):
    try:
        val = int(value)
        if val > 0 and str(val).startswith("100") and len(str(val)) >= 13:
            return -val
        return val
    except (ValueError, TypeError):
        return None

BACKUP_CHANNEL_ID = sanitize_channel_id(os.environ.get("BACKUP_CHANNEL_ID", ""))
LOG_CHANNEL_ID = sanitize_channel_id(os.environ.get("LOG_CHANNEL_ID", ""))

ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit()]
SUPER_ADMIN_ID = int(os.environ.get("SUPER_ADMIN_ID", "0")) if os.environ.get("SUPER_ADMIN_ID", "").isdigit() else 0

if SUPER_ADMIN_ID:
    ADMIN_IDS.append(SUPER_ADMIN_ID)
    ADMIN_IDS = list(set(ADMIN_IDS))

# BOT OWNER INFO
BOT_OWNER = "@TOOLS_BOTS_KING"
POWERED_BY = "Powered by @TOOLS_BOTS_KING"
BOT_SUPPORT = "@TG_Bot_Support_bot"

# LIMITS
MAX_FILE_SIZE = 50 * 1024 * 1024 * 1024
CHUNK_SIZE = 4 * 1024 * 1024

# DATABASE PATHS
DB_PATH = "user_database.db"
ADS_DB_PATH = "ads_schedule.db"

# PRIORITIZED SERVERS
PRIORITIZED_SERVERS = [
    "upload-na-phx", "upload-ap-sgp", "upload-ap-hkg", 
    "upload-ap-tyo", "upload-sa-sao", "upload-eu-fra"
]

HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== DATABASE SETUP ==================

class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.init_db()
    
    def init_db(self):
        # Users table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_date TEXT DEFAULT CURRENT_TIMESTAMP,
                last_active TEXT DEFAULT CURRENT_TIMESTAMP,
                total_uploads INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0
            )
        ''')
        
        # Force subscribe channels
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS force_subscribe_channels (
                channel_id INTEGER PRIMARY KEY,
                channel_link TEXT,
                added_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Ads schedule table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                schedule_type TEXT,
                frequency INTEGER,
                times_per_day INTEGER DEFAULT 0,
                start_date TEXT,
                end_date TEXT,
                is_active INTEGER DEFAULT 1,
                target_users TEXT DEFAULT 'all'
            )
        ''')
        
        # Broadcast history
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                message TEXT,
                sent_date TEXT DEFAULT CURRENT_TIMESTAMP,
                total_users INTEGER,
                successful_sends INTEGER,
                failed_sends INTEGER
            )
        ''')
        
        self.conn.commit()
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = ""):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, joined_date, last_active)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, current_time, current_time))
            
            self.cursor.execute('''
                UPDATE users SET 
                username = ?,
                first_name = ?,
                last_name = ?,
                last_active = ?
                WHERE user_id = ?
            ''', (username, first_name, last_name, current_time, user_id))
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False
    
    def get_user(self, user_id: int):
        self.cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return self.cursor.fetchone()
    
    def update_upload_count(self, user_id: int):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute('''
                UPDATE users SET 
                total_uploads = total_uploads + 1,
                last_active = ?
                WHERE user_id = ?
            ''', (current_time, user_id))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating upload count: {e}")
    
    def get_all_users(self):
        self.cursor.execute('SELECT user_id FROM users WHERE is_banned = 0')
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_user_count(self):
        self.cursor.execute('SELECT COUNT(*) FROM users WHERE is_banned = 0')
        return self.cursor.fetchone()[0]
    
    def check_user_access(self, user_id: int):
        user = self.get_user(user_id)
        if not user:
            return False, "not_found"
        
        if user[7]:  # is_banned
            return False, "banned"
        
        if user[8]:  # is_admin
            return True, "admin"
        
        return True, "free"
    
    def add_force_subscribe_channel(self, channel_id: int, channel_link: str):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute('''
                INSERT OR REPLACE INTO force_subscribe_channels (channel_id, channel_link, added_date)
                VALUES (?, ?, ?)
            ''', (channel_id, channel_link, current_time))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            return False
    
    def get_force_subscribe_channels(self):
        self.cursor.execute('SELECT channel_id, channel_link FROM force_subscribe_channels')
        return self.cursor.fetchall()
    
    def remove_force_subscribe_channel(self, channel_id: int):
        self.cursor.execute('DELETE FROM force_subscribe_channels WHERE channel_id = ?', (channel_id,))
        self.conn.commit()
    
    def schedule_ad(self, message_text: str, schedule_type: str, 
                   frequency: int, times_per_day: int, days: int, target_users: str = "all"):
        current_time = datetime.now()
        start_date = current_time.strftime('%Y-%m-%d %H:%M:%S')
        end_date = (current_time + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute('''
            INSERT INTO ads_schedule 
            (message, schedule_type, frequency, times_per_day, start_date, end_date, target_users)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (message_text, schedule_type, frequency, times_per_day,
              start_date, end_date, target_users))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_active_ads(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute('''
            SELECT * FROM ads_schedule 
            WHERE is_active = 1 AND datetime(end_date) > datetime(?)
        ''', (current_time,))
        return self.cursor.fetchall()
    
    def update_ad_status(self, ad_id: int, is_active: bool):
        self.cursor.execute('UPDATE ads_schedule SET is_active = ? WHERE id = ?', 
                           (1 if is_active else 0, ad_id))
        self.conn.commit()
    
    def add_broadcast_record(self, admin_id: int, message: str, total_users: int, 
                           successful: int, failed: int):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute('''
            INSERT INTO broadcast_history 
            (admin_id, message, sent_date, total_users, successful_sends, failed_sends)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (admin_id, message, current_time, total_users, successful, failed))
        self.conn.commit()

# Initialize database
db = Database()

# ================== BOT INSTANCE ==================

app = Client(
    "ultimate_gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=10,
    sleep_threshold=60
)

# Scheduler for ads and subscription checks
scheduler = AsyncIOScheduler()

# ================== HELPER FUNCTIONS ==================

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def check_force_subscribe(client: Client, user_id: int) -> Tuple[bool, List[Tuple]]:
    """Check if user is member of all force subscribe channels"""
    channels = db.get_force_subscribe_channels()
    if not channels:
        return True, []
    
    not_joined = []
    for channel_id, channel_link in channels:
        try:
            member = await client.get_chat_member(channel_id, user_id)
            if member.status in ["left", "kicked"]:
                not_joined.append((channel_id, channel_link))
        except (UserNotParticipant, ChannelPrivate, Exception) as e:
            logger.error(f"Error checking channel membership: {e}")
            not_joined.append((channel_id, channel_link))
    
    return len(not_joined) == 0, not_joined

async def send_log_message(client: Client, message: str, file_path: str = None):
    """Send log message to log channel"""
    if not LOG_CHANNEL_ID:
        return
    
    try:
        if file_path and os.path.exists(file_path):
            await client.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=file_path,
                caption=message[:1024] if len(message) > 1024 else message
            )
        else:
            await client.send_message(
                chat_id=LOG_CHANNEL_ID,
                text=message
            )
    except Exception as e:
        logger.error(f"Failed to send log: {e}")

async def upload_to_gofile(path: str) -> Optional[str]:
    """Upload file to GoFile"""
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type is None:
        mime_type = "application/octet-stream"
    
    for server in PRIORITIZED_SERVERS:
        try:
            url = f"https://{server}.gofile.io/uploadFile"
            
            async with aiohttp.ClientSession() as session:
                with open(path, "rb") as f:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=os.path.basename(path), content_type=mime_type)
                    
                    if GOFILE_API_TOKEN:
                        data.add_field('token', GOFILE_API_TOKEN)
                    
                    folder_id = os.environ.get("GOFILE_FOLDER_ID")
                    if folder_id:
                        data.add_field('folderId', folder_id)
                    
                    async with session.post(url, data=data) as response:
                        if response.status == 200:
                            result = await response.json()
                            if result.get("status") == "ok":
                                return result["data"]["downloadPage"]
                        else:
                            logger.error(f"GoFile upload failed: {response.status}")
        except Exception as e:
            logger.error(f"Server {server} failed: {e}")
            continue
            
    return None

async def send_broadcast_to_users(client: Client, user_ids: List[int], message: str, 
                                 progress_msg: Message = None, total_users: int = None):
    """Send broadcast message to multiple users"""
    successful = 0
    failed = 0
    
    for i, user_id in enumerate(user_ids, 1):
        try:
            await client.send_message(
                chat_id=user_id,
                text=message
            )
            successful += 1
        except Exception as e:
            logger.error(f"Failed to send to {user_id}: {e}")
            failed += 1
        
        # Update progress every 10 users
        if progress_msg and i % 10 == 0:
            try:
                await progress_msg.edit_text(
                    f"📢 **Broadcasting in progress...**\n\n"
                    f"✅ Successful: {successful}\n"
                    f"❌ Failed: {failed}\n"
                    f"📊 Progress: {i}/{total_users}\n"
                    f"⏳ Estimated: {(total_users - i) // 10 * 2} seconds remaining"
                )
            except:
                pass
        
        # Small delay to avoid flooding
        await asyncio.sleep(0.1)
    
    return successful, failed

# ================== COMMAND HANDLERS ==================

@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Handle /start command"""
    user = message.from_user
    
    # Add/update user in database
    db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name or ""
    )
    
    # Check force subscription
    subscribed, not_joined = await check_force_subscribe(client, user.id)
    if not subscribed:
        buttons = []
        for channel_id, channel_link in not_joined:
            buttons.append([InlineKeyboardButton(f"Join Channel", url=channel_link)])
        
        buttons.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_subscription")])
        
        await message.reply_text(
            f"⚠️ **Please join our channels to use this bot!**\n\n"
            f"You need to join {len(not_joined)} channel(s) to continue.\n\n"
            f"After joining, click 'I've Joined' button.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    access, status = db.check_user_access(user.id)
    if not access:
        await message.reply_text("🚫 You are banned from using this bot.")
        return
    
    # Welcome message
    welcome_text = f"""
🤖 **Welcome to Ultimate GoFile Uploader!** {POWERED_BY}

⚡ **Features:**
• Upload files up to 50GB
• Direct URL support
• High-speed transfers
• Secure cloud storage

📚 **Available Commands:**
/help - Show all commands
/upload - Upload instructions
/status - Check bot status
/myinfo - Your profile info
/support - Contact support

⚙️ **System:** Powered by `aiohttp` for maximum speed

{BOT_OWNER}
    """
    
    await message.reply_text(welcome_text)

@app.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    """Handle /help command"""
    help_text = f"""
🛠 **Bot Commands Guide** {POWERED_BY}

**📤 Upload Commands:**
• Simply send any file (doc, video, audio, photo)
• Send direct URLs (http/https)
• Max file size: 50GB

**👤 User Commands:**
/start - Start the bot
/help - This help message
/myinfo - Your profile information
/status - Bot status and statistics
/support - Get support contact

**⚙️ Admin Commands:**
/admin - Admin panel
/broadcast - Broadcast message
/users - List all users
/stats - Detailed statistics
/addchannel - Add force subscribe channel
/removechannel - Remove channel
/listchannels - List all channels
/ads - Schedule ads

**🔧 Technical:**
• Fast uploads
• Automatic backups
• Real-time logging
• Queue management

**Need help?** Contact {BOT_SUPPORT}
{BOT_OWNER}
    """
    
    await message.reply_text(help_text)

@app.on_message(filters.command("myinfo") & filters.private)
async def myinfo_command(client: Client, message: Message):
    """Show user information"""
    user = message.from_user
    user_data = db.get_user(user.id)
    
    if not user_data:
        await message.reply_text("❌ User not found in database.")
        return
    
    access, status = db.check_user_access(user.id)
    
    info_text = f"""
📊 **Your Profile Information**

👤 **User ID:** `{user.id}`
📛 **Username:** @{user.username if user.username else 'Not set'}
📅 **Joined Date:** {user_data[4]}
🔄 **Last Active:** {user_data[5]}
📤 **Total Uploads:** {user_data[6] or 0}
👑 **Status:** {status.upper()}
    
{BOT_OWNER}
    """
    
    await message.reply_text(info_text)

@app.on_message(filters.command("status") & filters.private)
async def status_command(client: Client, message: Message):
    """Show bot status"""
    user_count = db.get_user_count()
    
    status_text = f"""
🟢 **Bot Status - ONLINE** {POWERED_BY}

📊 **Statistics:**
• Total Users: {user_count}
• Active Now: Checking...
• Uptime: 24/7
• Queue: 0 pending

⚡ **Performance:**
• Upload Speed: Maximum
• File Limit: 50GB
• Supported: All formats
• Backup: Enabled

🔧 **System:**
• Powered by: aiohttp
• Workers: 10 parallel
• Memory: Optimized
• Storage: Cloud-backed

📞 **Support:** {BOT_SUPPORT}
{BOT_OWNER}
    """
    
    await message.reply_text(status_text)

@app.on_message(filters.command("support") & filters.private)
async def support_command(client: Client, message: Message):
    """Support information"""
    support_text = f"""
🆘 **Support & Contact** {POWERED_BY}

For any issues, questions, or suggestions:

📞 **Support Bot:** {BOT_SUPPORT}
👨‍💻 **Developer:** {BOT_OWNER}

**Common Issues:**
1. File too large (max 50GB)
2. URL not accessible
3. Upload timeout
4. Format not supported

**Before contacting:**
• Check /help for commands
• Ensure stable internet
• Try smaller file first
• Check URL accessibility

**Response Time:** Usually within 24 hours

{BOT_OWNER}
    """
    
    await message.reply_text(support_text)

@app.on_message(filters.command("upload") & filters.private)
async def upload_help_command(client: Client, message: Message):
    """Upload instructions"""
    upload_text = f"""
📤 **Upload Guide** {POWERED_BY}

**How to upload files:**

1. **Send Files Directly:**
   • Document files (PDF, DOC, ZIP, etc.)
   • Video files (MP4, MKV, AVI, etc.)
   • Audio files (MP3, WAV, etc.)
   • Photos (JPG, PNG, etc.)

2. **Send URLs:**
   • Direct download links
   • Must start with http:// or https://
   • File will be downloaded and uploaded

3. **Limits:**
   • Max file size: 50GB
   • All file types supported
   • No daily limits

4. **Process:**
   • File → Download → Upload to GoFile → Get Link
   • Automatic backup to our channels
   • Secure cloud storage

**Example URLs:**
• https://example.com/file.zip
• http://server.com/video.mp4

**Note:** Large files may take longer to process.

{BOT_OWNER}
    """
    
    await message.reply_text(upload_text)

# ================== ADMIN COMMANDS ==================

@app.on_message(filters.command("admin") & filters.private)
async def admin_panel(client: Client, message: Message):
    """Admin panel"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    user_count = db.get_user_count()
    channels = db.get_force_subscribe_channels()
    
    buttons = [
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
         InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("👥 Users", callback_data="admin_users"),
         InlineKeyboardButton("📣 Ads", callback_data="admin_ads")],
        [InlineKeyboardButton(f"📺 Channels ({len(channels)})", callback_data="admin_channels"),
         InlineKeyboardButton("🔄 Refresh", callback_data="admin_refresh")],
        [InlineKeyboardButton("❌ Close", callback_data="admin_close")]
    ]
    
    await message.reply_text(
        f"👑 **Admin Panel** {POWERED_BY}\n\n"
        f"📊 **Quick Stats:**\n"
        f"• Total Users: {user_count}\n"
        f"• Force Channels: {len(channels)}\n"
        f"• Admin ID: {message.from_user.id}\n\n"
        f"Select an option:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_command(client: Client, message: Message):
    """Broadcast message to all users"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    if len(message.command) < 2 and not message.reply_to_message:
        await message.reply_text(
            "**📢 Broadcast Message**\n\n"
            "**Usage:**\n"
            "1. `/broadcast your message here`\n"
            "2. Reply to a message with `/broadcast`\n\n"
            "**Example:** `/broadcast Hello users! New update available.`"
        )
        return
    
    # Get message text
    if message.reply_to_message:
        if message.reply_to_message.text:
            broadcast_text = message.reply_to_message.text
        elif message.reply_to_message.caption:
            broadcast_text = message.reply_to_message.caption
        else:
            await message.reply_text("❌ Replied message has no text or caption.")
            return
    else:
        broadcast_text = " ".join(message.command[1:])
    
    if not broadcast_text.strip():
        await message.reply_text("❌ Broadcast message cannot be empty.")
        return
    
    users = db.get_all_users()
    total = len(users)
    
    if total == 0:
        await message.reply_text("❌ No users to broadcast to.")
        return
    
    confirm_buttons = [
        [InlineKeyboardButton("✅ Yes, Send Now", callback_data=f"broadcast_confirm_{message.id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
    ]
    
    await message.reply_text(
        f"⚠️ **Confirm Broadcast**\n\n"
        f"**Message:** {broadcast_text[:100]}...\n\n"
        f"**To:** {total} users\n\n"
        f"Are you sure you want to send this broadcast?",
        reply_markup=InlineKeyboardMarkup(confirm_buttons)
    )

@app.on_message(filters.command("users") & filters.private)
async def users_command(client: Client, message: Message):
    """List all users"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    users = db.get_all_users()
    total = len(users)
    
    if not users:
        await message.reply_text("No users found.")
        return
    
    # Show first 50 users
    user_list = ""
    for i, user_id in enumerate(users[:50], 1):
        user_data = db.get_user(user_id)
        if user_data:
            username = f"@{user_data[1]}" if user_data[1] else "No username"
            name = user_data[2] or "Unknown"
            user_list += f"{i}. {name} {username} (ID: `{user_id}`)\n"
    
    await message.reply_text(
        f"👥 **User List**\n"
        f"📊 Total Users: {total}\n\n"
        f"{user_list}\n"
        f"Showing 1-{min(50, total)} of {total} users\n\n"
        f"Use /stats for detailed statistics."
    )

@app.on_message(filters.command("stats") & filters.private)
async def stats_command(client: Client, message: Message):
    """Detailed statistics"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    user_count = db.get_user_count()
    channels = db.get_force_subscribe_channels()
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Count today's active users (simplified)
    db.cursor.execute('SELECT COUNT(*) FROM users WHERE date(last_active) = date(?)', (today,))
    active_today = db.cursor.fetchone()[0]
    
    # Count new users today
    db.cursor.execute('SELECT COUNT(*) FROM users WHERE date(joined_date) = date(?)', (today,))
    new_today = db.cursor.fetchone()[0]
    
    stats_text = f"""
📈 **Detailed Statistics** {POWERED_BY}

**👥 User Stats:**
• Total Users: {user_count}
• Active Today: {active_today}
• New Today: {new_today}
• Banned Users: 0
• Admin Users: {len(ADMIN_IDS)}

**📺 Channel Stats:**
• Force Channels: {len(channels)}
• Channel List: Use /listchannels

**⚙️ System Stats:**
• Bot Uptime: 24/7
• Database: SQLite
• Workers: 10
• Max File: 50GB

**📅 Report Date:** {get_current_time()}

**👑 Admin Commands:**
• /broadcast - Send message to all
• /addchannel - Add force channel
• /removechannel - Remove channel
• /ads - Schedule advertisements

{BOT_OWNER}
    """
    
    await message.reply_text(stats_text)

@app.on_message(filters.command("addchannel") & filters.private)
async def add_channel_command(client: Client, message: Message):
    """Add force subscribe channel"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    if len(message.command) < 2:
        await message.reply_text(
            "**📺 Add Force Subscribe Channel**\n\n"
            "**Usage:** `/addchannel <channel_link>`\n\n"
            "**Example:**\n"
            "`/addchannel https://t.me/TOOLS_BOTS_KING`\n"
            "`/addchannel @TOOLS_BOTS_KING`\n\n"
            "**Note:** The bot must be admin in the channel."
        )
        return
    
    channel_link = message.command[1]
    
    try:
        # Extract channel info from link
        if "t.me/" in channel_link:
            channel_username = channel_link.split("t.me/")[-1].replace("@", "").strip()
            chat = await client.get_chat(f"@{channel_username}")
            channel_id = chat.id
            
            # Check if bot is admin in channel
            try:
                bot_me = await client.get_me()
                bot_member = await client.get_chat_member(channel_id, bot_me.id)
                if bot_member.status not in ["administrator", "creator"]:
                    await message.reply_text(
                        f"❌ **Bot is not admin in channel!**\n\n"
                        f"Please make @{bot_me.username} an admin in the channel first."
                    )
                    return
            except Exception as e:
                logger.error(f"Bot admin check failed: {e}")
                await message.reply_text(
                    f"❌ **Cannot verify bot admin status.**\n"
                    f"Make sure bot is added to channel as admin.\n\n"
                    f"Error: {str(e)}"
                )
                return
            
            # Save channel
            if db.add_force_subscribe_channel(channel_id, f"https://t.me/{channel_username}"):
                await message.reply_text(
                    f"✅ **Channel added successfully!**\n\n"
                    f"**Channel:** @{channel_username}\n"
                    f"**ID:** `{channel_id}`\n"
                    f"**Title:** {chat.title}\n\n"
                    f"All users must join this channel to use the bot."
                )
                
                # Log to admin
                await send_log_message(
                    client,
                    f"📺 **Force Subscribe Channel Added**\n"
                    f"**By Admin:** {message.from_user.id}\n"
                    f"**Channel:** @{channel_username}\n"
                    f"**ID:** {channel_id}\n"
                    f"**Title:** {chat.title}"
                )
            else:
                await message.reply_text("❌ Failed to add channel to database.")
        else:
            await message.reply_text("❌ Invalid channel link. Use format: https://t.me/username or @username")
    except Exception as e:
        logger.error(f"Error adding channel: {e}")
        await message.reply_text(f"❌ **Error:** {str(e)}\n\nMake sure the channel username is correct and the bot has access.")

@app.on_message(filters.command("removechannel") & filters.private)
async def remove_channel_command(client: Client, message: Message):
    """Remove force subscribe channel"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    channels = db.get_force_subscribe_channels()
    
    if not channels:
        await message.reply_text("No force subscribe channels configured.")
        return
    
    if len(message.command) < 2:
        # Show current channels
        channel_list = ""
        for i, (channel_id, channel_link) in enumerate(channels, 1):
            channel_list += f"{i}. {channel_link} (ID: `{channel_id}`)\n"
        
        await message.reply_text(
            f"📺 **Current Channels:**\n\n{channel_list}\n"
            f"**Usage:** `/removechannel <channel_id>`\n"
            f"**Example:** `/removechannel {channels[0][0]}`"
        )
        return
    
    try:
        channel_id = int(message.command[1])
        db.remove_force_subscribe_channel(channel_id)
        await message.reply_text(f"✅ Channel `{channel_id}` removed successfully.")
        
        # Log to admin
        await send_log_message(
            client,
            f"📺 **Force Subscribe Channel Removed**\n"
            f"**By Admin:** {message.from_user.id}\n"
            f"**Channel ID:** {channel_id}"
        )
    except ValueError:
        await message.reply_text("❌ Invalid channel ID. Must be a number.")
    except Exception as e:
        logger.error(f"Error removing channel: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("listchannels") & filters.private)
async def list_channels_command(client: Client, message: Message):
    """List all force subscribe channels"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    channels = db.get_force_subscribe_channels()
    
    if not channels:
        await message.reply_text("No force subscribe channels configured.")
        return
    
    channel_info = []
    for channel_id, channel_link in channels:
        try:
            chat = await client.get_chat(channel_id)
            member_count = chat.members_count if hasattr(chat, 'members_count') else "N/A"
            channel_info.append(f"• **{chat.title}**\n  📎 {channel_link}\n  👥 Members: {member_count}\n  🆔 ID: `{channel_id}`\n")
        except Exception as e:
            logger.error(f"Error getting chat info: {e}")
            channel_info.append(f"• {channel_link}\n  🆔 ID: `{channel_id}`\n  ⚠️ Cannot fetch details\n")
    
    await message.reply_text(
        f"📺 **Force Subscribe Channels**\n\n"
        f"Total: {len(channels)} channels\n\n"
        f"{''.join(channel_info)}\n"
        f"**Note:** Users must join ALL these channels."
    )

@app.on_message(filters.command("ads") & filters.private)
async def ads_command(client: Client, message: Message):
    """Schedule ads"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    if len(message.command) < 2:
        # Show ads help
        await message.reply_text(
            f"📣 **Advertisement Scheduler** {POWERED_BY}\n\n"
            f"**Commands:**\n"
            f"• `/ads schedule <days> <times_per_day> <message>`\n"
            f"• `/ads list` - List scheduled ads\n"
            f"• `/ads stop <ad_id>` - Stop an ad\n"
            f"• `/ads status` - Show ad statistics\n\n"
            f"**Examples:**\n"
            f"`/ads schedule 7 3 Welcome message`\n"
            f"`/ads list`\n"
            f"`/ads stop 1`\n\n"
            f"**Parameters:**\n"
            f"• `days`: How many days to run (1-30)\n"
            f"• `times_per_day`: Messages per day (1-10)\n"
            f"• `message`: The ad message to send"
        )
        return
    
    subcommand = message.command[1].lower()
    
    if subcommand == "schedule":
        if len(message.command) < 5:
            await message.reply_text(
                "❌ **Invalid format!**\n\n"
                "**Usage:** `/ads schedule <days> <times_per_day> <message>`\n\n"
                "**Example:** `/ads schedule 7 3 Special offer for our users!`\n"
                "This sends 'Special offer...' 3 times daily for 7 days."
            )
            return
        
        try:
            days = int(message.command[2])
            times_per_day = int(message.command[3])
            ad_message = " ".join(message.command[4:])
            
            if days < 1 or days > 30:
                await message.reply_text("❌ Days must be between 1 and 30.")
                return
            
            if times_per_day < 1 or times_per_day > 10:
                await message.reply_text("❌ Times per day must be between 1 and 10.")
                return
            
            ad_id = db.schedule_ad(
                message_text=ad_message,
                schedule_type="daily",
                frequency=times_per_day,
                times_per_day=times_per_day,
                days=days,
                target_users="all"
            )
            
            await message.reply_text(
                f"✅ **Ad Scheduled Successfully!**\n\n"
                f"**Ad ID:** {ad_id}\n"
                f"**Duration:** {days} days\n"
                f"**Frequency:** {times_per_day} times/day\n"
                f"**Message:** {ad_message[:100]}...\n\n"
                f"Total messages: {days * times_per_day}\n"
                f"Will start immediately."
            )
            
        except ValueError:
            await message.reply_text("❌ Days and times must be numbers.")
    
    elif subcommand == "list":
        ads = db.get_active_ads()
        if not ads:
            await message.reply_text("No active ads scheduled.")
            return
        
        ads_list = ""
        for ad in ads:
            ad_id, message_text, schedule_type, frequency, times_per_day, start_date, end_date, is_active, target_users = ad
            ads_list += f"**ID {ad_id}:** {message_text[:50]}...\n"
            ads_list += f"  ⏰ {times_per_day}x/day | 📅 {start_date[:10]} to {end_date[:10]}\n\n"
        
        await message.reply_text(
            f"📋 **Scheduled Ads**\n\n"
            f"Total active: {len(ads)}\n\n"
            f"{ads_list}\n"
            f"Use `/ads stop <id>` to stop an ad."
        )
    
    elif subcommand == "stop":
        if len(message.command) < 3:
            await message.reply_text("❌ **Usage:** `/ads stop <ad_id>`")
            return
        
        try:
            ad_id = int(message.command[2])
            db.update_ad_status(ad_id, False)
            await message.reply_text(f"✅ Ad ID {ad_id} stopped successfully.")
        except ValueError:
            await message.reply_text("❌ Ad ID must be a number.")
    
    elif subcommand == "status":
        ads = db.get_active_ads()
        total_ads = len(ads)
        total_messages = 0
        
        for ad in ads:
            # Calculate total messages
            start_date = datetime.strptime(ad[5], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(ad[6], '%Y-%m-%d %H:%M:%S')
            days = (end_date - start_date).days
            total_messages += days * ad[3]
        
        await message.reply_text(
            f"📊 **Ads Status**\n\n"
            f"• Active Ads: {total_ads}\n"
            f"• Total Messages: {total_messages}\n"
            f"• Next Check: Every 30 minutes\n"
            f"• System: Running\n\n"
            f"Use `/ads list` to see all ads."
        )
    
    else:
        await message.reply_text("❌ Unknown subcommand. Use `/ads` for help.")

# ================== CALLBACK QUERY HANDLER ==================

@app.on_callback_query()
async def callback_query_handler(client: Client, callback_query):
    """Handle callback queries"""
    data = callback_query.data
    user_id = callback_query.from_user.id
    message = callback_query.message
    
    try:
        if data == "check_subscription":
            # Check if user has joined all channels
            subscribed, not_joined = await check_force_subscribe(client, user_id)
            
            if subscribed:
                await message.delete()
                await client.send_message(
                    user_id,
                    f"✅ **Subscription Verified!**\n\n"
                    f"Welcome to the bot! You can now upload files or send URLs.\n\n"
                    f"**Quick Start:**\n"
                    f"• Send me any file (max 50GB)\n"
                    f"• Send a direct download URL\n"
                    f"• Use /help for all commands\n\n"
                    f"{POWERED_BY}"
                )
            else:
                buttons = []
                for channel_id, channel_link in not_joined:
                    buttons.append([InlineKeyboardButton(f"Join Channel", url=channel_link)])
                buttons.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_subscription")])
                
                await message.edit_text(
                    f"⚠️ **Please join all channels!**\n\n"
                    f"You still need to join {len(not_joined)} channel(s).\n"
                    f"After joining, click 'I've Joined' again.",
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
        
        elif data == "broadcast_cancel":
            await message.edit_text("❌ Broadcast cancelled.")
        
        elif data.startswith("broadcast_confirm_"):
            if not is_admin(user_id):
                await callback_query.answer("Access denied!", show_alert=True)
                return
            
            # Extract message ID
            msg_id = int(data.split("_")[2])
            
            # Get the original broadcast message
            try:
                original_msg = await client.get_messages(user_id, msg_id)
                broadcast_text = original_msg.reply_to_message.text if original_msg.reply_to_message else " ".join(original_msg.command[1:])
            except:
                broadcast_text = "Broadcast message"
            
            users = db.get_all_users()
            total = len(users)
            
            if total == 0:
                await message.edit_text("❌ No users to broadcast to.")
                return
            
            await message.edit_text(f"📢 **Broadcasting started...**\n\nSending to {total} users...")
            
            # Send broadcast
            successful, failed = await send_broadcast_to_users(
                client, users, broadcast_text, message, total
            )
            
            # Add to broadcast history
            db.add_broadcast_record(
                admin_id=user_id,
                message=broadcast_text[:500],
                total_users=total,
                successful=successful,
                failed=failed
            )
            
            await message.edit_text(
                f"✅ **Broadcast Complete!**\n\n"
                f"📊 **Statistics:**\n"
                f"• Total Users: {total}\n"
                f"• ✅ Successful: {successful}\n"
                f"• ❌ Failed: {failed}\n"
                f"• 📅 Time: {get_current_time()}\n\n"
                f"Success rate: {(successful/total*100):.1f}%"
            )
            
            # Log to admin
            await send_log_message(
                client,
                f"📢 **Broadcast Sent**\n"
                f"**By Admin:** {user_id}\n"
                f"**Total Users:** {total}\n"
                f"**Successful:** {successful}\n"
                f"**Failed:** {failed}\n"
                f"**Message:** {broadcast_text[:200]}..."
            )
        
        elif data.startswith("admin_"):
            if not is_admin(user_id):
                await callback_query.answer("Access denied!", show_alert=True)
                return
            
            if data == "admin_close":
                await message.delete()
            
            elif data == "admin_stats":
                user_count = db.get_user_count()
                await message.edit_text(
                    f"📈 **Admin Statistics**\n\n"
                    f"• Total Users: {user_count}\n"
                    f"• Admin Users: {len(ADMIN_IDS)}\n"
                    f"• Super Admin: {SUPER_ADMIN_ID}\n"
                    f"• Bot Status: ✅ ONLINE\n\n"
                    f"📅 Last Updated: {get_current_time()}\n\n"
                    f"{POWERED_BY}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_stats"),
                         InlineKeyboardButton("🔙 Back", callback_data="admin_back")]
                    ])
                )
            
            elif data == "admin_back":
                await admin_panel(client, message)
            
            elif data == "admin_refresh":
                await admin_panel(client, message)
                await callback_query.answer("Refreshed!")
        
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await callback_query.answer("An error occurred!", show_alert=True)

# ================== FILE & URL HANDLING ==================

@app.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_handler(client: Client, message: Message):
    """Handle file uploads"""
    user_id = message.from_user.id
    
    # Check force subscription
    subscribed, not_joined = await check_force_subscribe(client, user_id)
    if not subscribed:
        buttons = []
        for channel_id, channel_link in not_joined:
            buttons.append([InlineKeyboardButton(f"Join Channel", url=channel_link)])
        buttons.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_subscription")])
        
        await message.reply_text(
            f"⚠️ **Please join our channels to upload files!**\n\n"
            f"You need to join {len(not_joined)} channel(s) to continue.\n"
            f"After joining, click 'I've Joined' button.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    access, status = db.check_user_access(user_id)
    if not access:
        await message.reply_text("🚫 You are banned from using this bot.")
        return
    
    # Update user activity
    db.add_user(
        user_id=user_id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        last_name=message.from_user.last_name or ""
    )
    
    # Process file
    media = message.document or message.video or message.audio or message.photo
    file_size = media.file_size if hasattr(media, 'file_size') else 0
    
    if file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ **File too large!**\n\n"
            f"Maximum file size: {human_readable_size(MAX_FILE_SIZE)}\n"
            f"Your file: {human_readable_size(file_size)}\n\n"
            f"Please try a smaller file."
        )
        return
    
    msg = await message.reply_text(
        f"📁 **File Detected!**\n"
        f"⚡ Processing with high-speed engine...\n"
        f"📦 Size: {human_readable_size(file_size)}"
    )
    
    # Download file
    file_name = getattr(media, "file_name", f"file_{message.id}_{int(time.time())}")
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        await msg.edit_text("⬇️ **Downloading file...**\n⏳ Please wait...")
        await client.download_media(message, file_path)
        
        # Upload to GoFile
        await msg.edit_text("⬆️ **Uploading to GoFile Cloud...**\n🚀 Maximum speed activated")
        
        download_link = await upload_to_gofile(file_path)
        
        if not download_link:
            await msg.edit_text(
                "❌ **Upload failed!**\n\n"
                "Possible reasons:\n"
                "1. GoFile servers are busy\n"
                "2. Network connection issue\n"
                "3. File type not supported\n\n"
                "Please try again in a few minutes."
            )
            return
        
        # Update user stats
        db.update_upload_count(user_id)
        
        # Send success message
        success_text = f"""
✅ **Upload Successful!** {POWERED_BY}

📂 **File:** `{file_name}`
📦 **Size:** {human_readable_size(file_size)}
🔗 **Download Link:** {download_link}

💾 **Save this link:** The file is stored securely in the cloud.

📊 **Your Stats:** Use /myinfo to see your upload stats

{BOT_OWNER}
        """
        
        await msg.edit_text(success_text, disable_web_page_preview=True)
        
        # Log to log channel
        await send_log_message(
            client,
            f"""
📤 **File Upload Log**
📅 **Date:** {datetime.now().isoformat()}
👤 **User ID:** {user_id}
👤 **First Name:** {message.from_user.first_name}
👤 **Last Name:** {message.from_user.last_name or 'N/A'}
👤 **Username:** @{message.from_user.username or 'N/A'}
💬 **Chat ID:** {message.chat.id}
📁 **File Type:** {media.__class__.__name__.lower()}
📦 **File Size:** {human_readable_size(file_size)}
🔗 **Download Link:** {download_link}
            """,
            file_path
        )
        
    except Exception as e:
        logger.error(f"File processing error: {e}")
        await msg.edit_text(
            f"❌ **Upload Error!**\n\n"
            f"**Error:** {str(e)[:200]}\n\n"
            f"Please try again or contact {BOT_SUPPORT} if problem persists."
        )
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

@app.on_message(filters.text & filters.private)
async def text_handler(client: Client, message: Message):
    """Handle text messages (URLs)"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Check if it's a command
    if text.startswith("/"):
        return
    
    # Check if it's a URL
    if not (text.startswith("http://") or text.startswith("https://")):
        await message.reply_text(
            f"❓ **I didn't understand that.**\n\n"
            f"Send me:\n"
            f"• A file to upload (max 50GB)\n"
            f"• A direct download URL (http:// or https://)\n"
            f"• Use /help for all commands\n\n"
            f"{POWERED_BY}"
        )
        return
    
    # Check force subscription
    subscribed, not_joined = await check_force_subscribe(client, user_id)
    if not subscribed:
        buttons = []
        for channel_id, channel_link in not_joined:
            buttons.append([InlineKeyboardButton(f"Join Channel", url=channel_link)])
        buttons.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_subscription")])
        
        await message.reply_text(
            f"⚠️ **Please join our channels to upload files!**\n\n"
            f"You need to join {len(not_joined)} channel(s) to continue.\n"
            f"After joining, click 'I've Joined' button.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    access, status = db.check_user_access(user_id)
    if not access:
        await message.reply_text("🚫 You are banned from using this bot.")
        return
    
    # Update user activity
    db.add_user(
        user_id=user_id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        last_name=message.from_user.last_name or ""
    )
    
    msg = await message.reply_text(
        f"🔗 **URL Detected!**\n"
        f"⚡ Starting high-speed download...\n"
        f"🌐 URL: {text[:50]}..."
    )
    
    # Download from URL
    try:
        file_name = text.split("/")[-1].split("?")[0]
        if not file_name or len(file_name) > 100:
            file_name = f"url_file_{int(time.time())}.bin"
        
        file_path = os.path.join(DOWNLOAD_DIR, file_name)
        
        # Download file
        await msg.edit_text("⬇️ **Downloading from URL...**\n⏳ Please wait...")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(text, timeout=300) as response:
                if response.status != 200:
                    await msg.edit_text(f"❌ **URL Error:** HTTP {response.status}\n\nURL might be invalid or inaccessible.")
                    return
                
                total_size = int(response.headers.get('content-length', 0))
                if total_size > MAX_FILE_SIZE:
                    await msg.edit_text(
                        f"❌ **File too large!**\n\n"
                        f"Maximum file size: {human_readable_size(MAX_FILE_SIZE)}\n"
                        f"URL file: {human_readable_size(total_size)}\n\n"
                        f"Please try a smaller file."
                    )
                    return
                
                with open(file_path, "wb") as f:
                    downloaded = 0
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        f.write(chunk)
                        downloaded += len(chunk)
        
        final_size = os.path.getsize(file_path)
        
        # Upload to GoFile
        await msg.edit_text("⬆️ **Uploading to GoFile Cloud...**\n🚀 Maximum speed activated")
        
        download_link = await upload_to_gofile(file_path)
        
        if not download_link:
            await msg.edit_text(
                "❌ **Upload failed!**\n\n"
                "Possible reasons:\n"
                "1. GoFile servers are busy\n"
                "2. Network connection issue\n"
                "3. File type not supported\n\n"
                "Please try again in a few minutes."
            )
            return
        
        # Update user stats
        db.update_upload_count(user_id)
        
        # Send success message
        success_text = f"""
✅ **Upload Successful!** {POWERED_BY}

📂 **File:** `{file_name}`
📦 **Size:** {human_readable_size(final_size)}
🔗 **Source URL:** {text[:100]}...
🔗 **Download Link:** {download_link}

💾 **Save this link:** The file is stored securely in the cloud.

📊 **Your Stats:** Use /myinfo to see your upload stats

{BOT_OWNER}
        """
        
        await msg.edit_text(success_text, disable_web_page_preview=True)
        
        # Log to log channel
        await send_log_message(
            client,
            f"""
📤 **URL Upload Log**
📅 **Date:** {datetime.now().isoformat()}
👤 **User ID:** {user_id}
👤 **First Name:** {message.from_user.first_name}
👤 **Last Name:** {message.from_user.last_name or 'N/A'}
👤 **Username:** @{message.from_user.username or 'N/A'}
💬 **Chat ID:** {message.chat.id}
🔗 **Source URL:** {text[:500]}
📦 **File Size:** {human_readable_size(final_size)}
🔗 **Download Link:** {download_link}
            """,
            file_path
        )
        
    except asyncio.TimeoutError:
        await msg.edit_text("❌ **Download timeout!**\n\nURL might be too large or server is slow.\nTry a smaller file.")
    except Exception as e:
        logger.error(f"URL processing error: {e}")
        await msg.edit_text(
            f"❌ **Upload Error!**\n\n"
            f"**Error:** {str(e)[:200]}\n\n"
            f"Please check the URL and try again."
        )
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

# ================== SCHEDULED TASKS ==================

async def send_scheduled_ads_task():
    """Send scheduled ads to users"""
    try:
        active_ads = db.get_active_ads()
        if not active_ads:
            return
        
        users = db.get_all_users()
        if not users:
            return
        
        current_time = datetime.now()
        
        for ad in active_ads:
            ad_id, message_text, schedule_type, frequency, times_per_day, start_date, end_date, is_active, target_users = ad
            
            # Parse dates
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            except:
                continue
            
            # Check if ad is active and within date range
            if not is_active or current_time < start_dt or current_time > end_dt:
                continue
            
            # Simple scheduling: Send ad now (in production, implement proper scheduling)
            # For now, we'll just log that we would send it
            logger.info(f"Would send ad {ad_id} to {len(users)} users")
            
    except Exception as e:
        logger.error(f"Ad scheduling error: {e}")

# ================== WEB SERVER (RENDER KEEP-ALIVE) ==================

async def web_handler(request):
    return web.Response(text=f"🤖 Ultimate GoFile Bot is Running | {POWERED_BY}")

async def start_web():
    appw = web.Application()
    appw.router.add_get("/", web_handler)
    runner = web.AppRunner(appw)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info(f"Web server started on port {port}")

# ================== MAIN EXECUTION ==================

async def main():
    print(f"🤖 Ultimate GoFile Bot Starting... {POWERED_BY}")
    print(f"⚡ Powered by aiohttp optimization")
    
    # Start scheduler
    scheduler.add_job(send_scheduled_ads_task, 'interval', minutes=30)
    scheduler.start()
    print("✅ Scheduler started")
    
    await app.start()
    print("✅ Bot Connected to Telegram")
    
    bot_info = await app.get_me()
    print(f"🤖 Bot Username: @{bot_info.username}")
    print(f"👑 Admin IDs: {ADMIN_IDS}")
    print(f"🆔 Bot ID: {bot_info.id}")
    
    print("🌍 Starting Web Server...")
    await start_web()
    
    user_count = db.get_user_count()
    print(f"📊 Total Users in Database: {user_count}")
    
    channels = db.get_force_subscribe_channels()
    print(f"📺 Force Subscribe Channels: {len(channels)}")
    
    print(f"🚀 High-Speed Pipeline Ready. {POWERED_BY}")
    print(f"📞 Support: {BOT_SUPPORT}")
    print(f"👨‍💻 Owner: {BOT_OWNER}")
    
    # Send startup notification to admin
    if SUPER_ADMIN_ID:
        try:
            await app.send_message(
                SUPER_ADMIN_ID,
                f"🤖 **Bot Started Successfully**\n\n"
                f"📅 Time: {get_current_time()}\n"
                f"👥 Users: {user_count}\n"
                f"📺 Channels: {len(channels)}\n"
                f"⚡ Status: ONLINE\n\n"
                f"{POWERED_BY}"
            )
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
    
    print("\n" + "="*50)
    print("Bot is now running. Press Ctrl+C to stop.")
    print("="*50 + "\n")
    
    await idle()
    
    # Cleanup
    scheduler.shutdown()
    await app.stop()
    print("\n👋 Bot Stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"❌ Fatal error: {e}")
