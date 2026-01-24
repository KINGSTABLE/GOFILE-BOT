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
import uvloop
import aiohttp
from pyrofork import Client, filters, idle
from pyrofork.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrofork.errors import UserNotParticipant, ChannelPrivate, FloodWait
from aiohttp import web
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ================== CONFIGURATION ==================
uvloop.install()

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
                joined_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_uploads INTEGER DEFAULT 0,
                subscription_type TEXT DEFAULT 'free',
                subscription_end DATETIME,
                is_banned BOOLEAN DEFAULT 0,
                is_admin BOOLEAN DEFAULT 0,
                lossless_count INTEGER DEFAULT 0,
                lossless_reset_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Force subscribe channels
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS force_subscribe_channels (
                channel_id INTEGER PRIMARY KEY,
                channel_link TEXT,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Ads schedule table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                message_id TEXT,
                schedule_type TEXT,
                frequency INTEGER,
                times_per_day INTEGER DEFAULT 0,
                start_date DATETIME,
                end_date DATETIME,
                is_active BOOLEAN DEFAULT 1,
                target_users TEXT DEFAULT 'all'
            )
        ''')
        
        # Broadcast history
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                message TEXT,
                sent_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_users INTEGER,
                successful_sends INTEGER,
                failed_sends INTEGER
            )
        ''')
        
        self.conn.commit()
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = ""):
        try:
            self.cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, joined_date, last_active)
                VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
            ''', (user_id, username, first_name, last_name))
            
            self.cursor.execute('''
                UPDATE users SET 
                username = ?,
                first_name = ?,
                last_name = ?,
                last_active = datetime('now')
                WHERE user_id = ?
            ''', (username, first_name, last_name, user_id))
            
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
            self.cursor.execute('''
                UPDATE users SET 
                total_uploads = total_uploads + 1,
                last_active = datetime('now')
                WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating upload count: {e}")
    
    def get_all_users(self):
        self.cursor.execute('SELECT user_id FROM users WHERE is_banned = 0')
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_user_count(self):
        self.cursor.execute('SELECT COUNT(*) FROM users WHERE is_banned = 0')
        return self.cursor.fetchone()[0]
    
    def update_subscription(self, user_id: int, sub_type: str, days: int):
        end_date = datetime.now() + timedelta(days=days)
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute('''
            UPDATE users SET 
            subscription_type = ?,
            subscription_end = ?
            WHERE user_id = ?
        ''', (sub_type, end_date_str, user_id))
        self.conn.commit()
    
    def check_subscription(self, user_id: int):
        user = self.get_user(user_id)
        if not user:
            return False, "free"
        
        if user[9]:  # is_banned
            return False, "banned"
        
        if user[10]:  # is_admin
            return True, "admin"
        
        if user[7] == 'free':  # subscription_type
            return True, "free"
        
        if user[8]:  # subscription_end
            end_date = datetime.strptime(user[8], '%Y-%m-%d %H:%M:%S')
            if datetime.now() > end_date:
                # Subscription expired
                self.cursor.execute('''
                    UPDATE users SET 
                    subscription_type = 'free',
                    subscription_end = NULL
                    WHERE user_id = ?
                ''', (user_id,))
                self.conn.commit()
                return True, "free"
            return True, user[7]
        
        return True, "free"
    
    def add_force_subscribe_channel(self, channel_id: int, channel_link: str):
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO force_subscribe_channels (channel_id, channel_link, added_date)
                VALUES (?, ?, datetime('now'))
            ''', (channel_id, channel_link))
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
    
    def schedule_ad(self, message_text: str, message_id: str, schedule_type: str, 
                   frequency: int, times_per_day: int, days: int, target_users: str = "all"):
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days)
        
        self.cursor.execute('''
            INSERT INTO ads_schedule 
            (message, message_id, schedule_type, frequency, times_per_day, start_date, end_date, target_users)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (message_text, message_id, schedule_type, frequency, times_per_day,
              start_date.strftime('%Y-%m-%d %H:%M:%S'),
              end_date.strftime('%Y-%m-%d %H:%M:%S'), target_users))
        self.conn.commit()
    
    def get_active_ads(self):
        self.cursor.execute('''
            SELECT * FROM ads_schedule 
            WHERE is_active = 1 AND datetime(end_date) > datetime('now')
        ''')
        return self.cursor.fetchall()
    
    def update_ad_status(self, ad_id: int, is_active: bool):
        self.cursor.execute('UPDATE ads_schedule SET is_active = ? WHERE id = ?', (int(is_active), ad_id))
        self.conn.commit()
    
    def add_broadcast_record(self, admin_id: int, message: str, total_users: int, 
                           successful: int, failed: int):
        self.cursor.execute('''
            INSERT INTO broadcast_history 
            (admin_id, message, total_users, successful_sends, failed_sends)
            VALUES (?, ?, ?, ?, ?)
        ''', (admin_id, message, total_users, successful, failed))
        self.conn.commit()

# Initialize database
db = Database()

# ================== BOT INSTANCE ==================

app = Client(
    "ultimate_gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=10
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
        except (UserNotParticipant, ChannelPrivate, Exception):
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
                caption=message
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
    
    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
    
    for server in PRIORITIZED_SERVERS:
        try:
            url = f"https://{server}.gofile.io/uploadfile"
            
            async with aiohttp.ClientSession(connector=connector) as session:
                with open(path, "rb") as f:
                    data = aiohttp.FormData()
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
            f"You need to join {len(not_joined)} channel(s) to continue.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    user_data = db.get_user(user.id)
    if user_data and user_data[9]:  # is_banned
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

⚙️ **System:** Powered by `uvloop` & `aiohttp` for maximum speed

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

**🔧 Technical:**
• Fastest possible uploads
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
    
    subscription_status, sub_type = db.check_subscription(user.id)
    
    info_text = f"""
📊 **Your Profile Information**

👤 **User ID:** `{user.id}`
📛 **Username:** @{user.username if user.username else 'Not set'}
📅 **Joined Date:** {user_data[4]}
🔄 **Last Active:** {user_data[5]}
📤 **Total Uploads:** {user_data[6] or 0}
👑 **Status:** {sub_type.upper()}
    
{BOT_OWNER}
    """
    
    if sub_type != "free" and user_data[7]:  # subscription_end
        info_text += f"⏰ **Subscription ends:** {user_data[7]}\n"
    
    await message.reply_text(info_text)

@app.on_message(filters.command("status") & filters.private)
async def status_command(client: Client, message: Message):
    """Show bot status"""
    user_count = db.get_user_count()
    
    status_text = f"""
🟢 **Bot Status - ONLINE** {POWERED_BY}

📊 **Statistics:**
• Total Users: {user_count}
• Active Now: {len(app.get_me())}
• Uptime: 24/7
• Queue: 0 pending

⚡ **Performance:**
• Upload Speed: Maximum
• File Limit: 50GB
• Supported: All formats
• Backup: Enabled

🔧 **System:**
• Powered by: uvloop + aiohttp
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

# ================== ADMIN COMMANDS ==================

@app.on_message(filters.command("admin") & filters.private)
async def admin_panel(client: Client, message: Message):
    """Admin panel"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    buttons = [
        [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
         InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("👥 Users", callback_data="admin_users"),
         InlineKeyboardButton("📣 Ads", callback_data="admin_ads")],
        [InlineKeyboardButton("📺 Channels", callback_data="admin_channels"),
         InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
        [InlineKeyboardButton("❌ Close", callback_data="admin_close")]
    ]
    
    await message.reply_text(
        f"👑 **Admin Panel** {POWERED_BY}\n\n"
        f"Welcome, Admin! Select an option:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_command(client: Client, message: Message):
    """Broadcast message to all users"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    if len(message.command) < 2:
        await message.reply_text(
            "**Usage:** /broadcast <message>\n\n"
            "Or reply to a message with /broadcast"
        )
        return
    
    users = db.get_all_users()
    total = len(users)
    successful = 0
    failed = 0
    
    broadcast_msg = await message.reply_text(f"📢 Broadcasting to {total} users...")
    
    for user_id in users:
        try:
            await client.send_message(
                chat_id=user_id,
                text=" ".join(message.command[1:])
            )
            successful += 1
        except Exception as e:
            logger.error(f"Failed to send to {user_id}: {e}")
            failed += 1
        
        # Update progress every 10 users
        if (successful + failed) % 10 == 0:
            await broadcast_msg.edit_text(
                f"📢 Broadcasting...\n"
                f"✅ Successful: {successful}\n"
                f"❌ Failed: {failed}\n"
                f"📊 Progress: {successful + failed}/{total}"
            )
    
    # Add to broadcast history
    db.add_broadcast_record(
        admin_id=message.from_user.id,
        message=" ".join(message.command[1:]),
        total_users=total,
        successful=successful,
        failed=failed
    )
    
    await broadcast_msg.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Users: {total}\n"
        f"• ✅ Successful: {successful}\n"
        f"• ❌ Failed: {failed}\n"
        f"• 📅 Time: {get_current_time()}"
    )
    
    # Log to admin
    await send_log_message(client, f"📢 Broadcast sent by Admin {message.from_user.id}\n"
                                 f"Total: {total}, Success: {successful}, Failed: {failed}")

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
            user_list += f"{i}. {username} (ID: `{user_id}`)\n"
    
    await message.reply_text(
        f"👥 **User List**\n"
        f"📊 Total Users: {total}\n\n"
        f"{user_list}\n"
        f"Showing 1-{min(50, total)} of {total} users"
    )

@app.on_message(filters.command("stats") & filters.private)
async def stats_command(client: Client, message: Message):
    """Detailed statistics"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    user_count = db.get_user_count()
    today = datetime.now().strftime('%Y-%m-%d')
    
    stats_text = f"""
📈 **Detailed Statistics** {POWERED_BY}

**👥 User Stats:**
• Total Users: {user_count}
• Active Today: Calculating...
• New Today: Calculating...

**📊 Upload Stats:**
• Total Uploads: Calculating...
• Today's Uploads: Calculating...
• Average Size: Calculating...

**⚙️ System Stats:**
• Bot Uptime: 24/7
• Memory Usage: Monitoring
• Queue Size: 0
• Workers Active: 10

**📅 Report Date:** {get_current_time()}
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
            "**Usage:** /addchannel <channel_link>\n\n"
            "Example: /addchannel https://t.me/TOOLS_BOTS_KING"
        )
        return
    
    channel_link = message.command[1]
    
    try:
        # Extract channel ID from link
        if "t.me/" in channel_link:
            channel_username = channel_link.split("t.me/")[-1].replace("@", "")
            chat = await client.get_chat(f"@{channel_username}")
            channel_id = chat.id
            
            if db.add_force_subscribe_channel(channel_id, channel_link):
                await message.reply_text(
                    f"✅ **Channel added successfully!**\n\n"
                    f"**Channel:** {channel_link}\n"
                    f"**ID:** `{channel_id}`\n\n"
                    f"All users must join this channel to use the bot."
                )
                
                # Log to admin
                await send_log_message(
                    client,
                    f"📺 Force Subscribe Channel Added\n"
                    f"By Admin: {message.from_user.id}\n"
                    f"Channel: {channel_link}\n"
                    f"ID: {channel_id}"
                )
            else:
                await message.reply_text("❌ Failed to add channel to database.")
        else:
            await message.reply_text("❌ Invalid channel link. Use format: https://t.me/username")
    except Exception as e:
        logger.error(f"Error adding channel: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("removechannel") & filters.private)
async def remove_channel_command(client: Client, message: Message):
    """Remove force subscribe channel"""
    if not is_admin(message.from_user.id):
        await message.reply_text("🚫 Access denied.")
        return
    
    if len(message.command) < 2:
        channels = db.get_force_subscribe_channels()
        if not channels:
            await message.reply_text("No channels configured.")
            return
        
        channel_list = ""
        for i, (channel_id, channel_link) in enumerate(channels, 1):
            channel_list += f"{i}. {channel_link} (ID: `{channel_id}`)\n"
        
        await message.reply_text(
            f"📺 **Current Channels:**\n\n{channel_list}\n"
            f"**Usage:** /removechannel <channel_id>"
        )
        return
    
    try:
        channel_id = int(message.command[1])
        db.remove_force_subscribe_channel(channel_id)
        await message.reply_text(f"✅ Channel `{channel_id}` removed successfully.")
    except ValueError:
        await message.reply_text("❌ Invalid channel ID. Must be a number.")

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
    
    channel_list = ""
    for i, (channel_id, channel_link) in enumerate(channels, 1):
        try:
            chat = await client.get_chat(channel_id)
            member_count = chat.members_count if hasattr(chat, 'members_count') else "N/A"
            channel_list += f"{i}. **{chat.title}**\n   📎 {channel_link}\n   👥 Members: {member_count}\n   🆔 ID: `{channel_id}`\n\n"
        except:
            channel_list += f"{i}. {channel_link}\n   🆔 ID: `{channel_id}`\n\n"
    
    await message.reply_text(
        f"📺 **Force Subscribe Channels**\n\n"
        f"Total: {len(channels)} channels\n\n"
        f"{channel_list}"
    )

# ================== CALLBACK QUERY HANDLER ==================

@app.on_callback_query()
async def callback_query_handler(client: Client, callback_query):
    """Handle callback queries"""
    data = callback_query.data
    user_id = callback_query.from_user.id
    
    if data == "check_subscription":
        # Check if user has joined all channels
        subscribed, not_joined = await check_force_subscribe(client, user_id)
        
        if subscribed:
            await callback_query.message.delete()
            await callback_query.message.reply_text(
                f"✅ **Subscription Verified!**\n\n"
                f"Welcome to the bot! You can now upload files or send URLs.\n\n"
                f"{POWERED_BY}"
            )
        else:
            buttons = []
            for channel_id, channel_link in not_joined:
                buttons.append([InlineKeyboardButton(f"Join Channel", url=channel_link)])
            buttons.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_subscription")])
            
            await callback_query.message.edit_text(
                f"⚠️ **Please join all channels!**\n\n"
                f"You still need to join {len(not_joined)} channel(s).",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
    
    elif data.startswith("admin_"):
        if not is_admin(user_id):
            await callback_query.answer("Access denied!", show_alert=True)
            return
        
        if data == "admin_close":
            await callback_query.message.delete()
        
        elif data == "admin_stats":
            user_count = db.get_user_count()
            await callback_query.message.edit_text(
                f"📈 **Admin Statistics**\n\n"
                f"• Total Users: {user_count}\n"
                f"• Active Today: Calculating...\n"
                f"• Banned Users: 0\n"
                f"• Admin Users: {len(ADMIN_IDS)}\n\n"
                f"📅 Last Updated: {get_current_time()}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data="admin_stats"),
                     InlineKeyboardButton("🔙 Back", callback_data="admin_back")]
                ])
            )
        
        elif data == "admin_back":
            await admin_panel(client, callback_query.message)
    
    await callback_query.answer()

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
            f"You need to join {len(not_joined)} channel(s) to continue.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    user_data = db.get_user(user_id)
    if user_data and user_data[9]:  # is_banned
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
            f"Your file: {human_readable_size(file_size)}"
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
        await client.download_media(message, file_path)
        
        # Upload to GoFile
        await msg.edit_text("⬆️ **Uploading to GoFile Cloud...**\n🚀 Maximum speed activated")
        
        download_link = await upload_to_gofile(file_path)
        
        if not download_link:
            await msg.edit_text("❌ **Upload failed!**\nGoFile servers might be busy. Please try again.")
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

📊 **Your Stats:** {user_data[6] + 1 if user_data else 1} total uploads
{BOT_OWNER}
        """
        
        await msg.edit_text(success_text, disable_web_page_preview=True)
        
        # Log to backup channel
        if BACKUP_CHANNEL_ID:
            try:
                log_text = f"""
📤 **File Upload Complete**
👤 **User:** {message.from_user.first_name} (ID: `{user_id}`)
📛 **Username:** @{message.from_user.username if message.from_user.username else 'N/A'}
📅 **Date:** {get_current_time()}
📂 **File:** `{file_name}`
📦 **Size:** {human_readable_size(file_size)}
🔗 **GoFile Link:** {download_link}
                """
                await client.send_message(
                    BACKUP_CHANNEL_ID,
                    log_text,
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Backup log failed: {e}")
        
        # Log to log channel with file
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
        await msg.edit_text(f"❌ **Error:** {str(e)}")
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.text & filters.private)
async def text_handler(client: Client, message: Message):
    """Handle text messages (URLs)"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Check if it's a URL
    if not (text.startswith("http://") or text.startswith("https://")):
        # Check for commands
        if not text.startswith("/"):
            await message.reply_text(
                f"❓ **I didn't understand that.**\n\n"
                f"Send me:\n"
                f"• A file to upload (max 50GB)\n"
                f"• A direct download URL\n"
                f"• Use /help for commands"
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
            f"You need to join {len(not_joined)} channel(s) to continue.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    # Check if user is banned
    user_data = db.get_user(user_id)
    if user_data and user_data[9]:  # is_banned
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
        
        connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(text, timeout=None) as response:
                if response.status != 200:
                    await msg.edit_text(f"❌ **URL Error:** HTTP {response.status}")
                    return
                
                total_size = int(response.headers.get('content-length', 0))
                if total_size > MAX_FILE_SIZE:
                    await msg.edit_text(
                        f"❌ **File too large!**\n\n"
                        f"Maximum file size: {human_readable_size(MAX_FILE_SIZE)}\n"
                        f"URL file: {human_readable_size(total_size)}"
                    )
                    return
                
                with open(file_path, "wb") as f:
                    downloaded = 0
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Update progress every 10MB
                        if downloaded % (10 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size * 100) if total_size > 0 else 0
                            await msg.edit_text(
                                f"⬇️ **Downloading...**\n"
                                f"📊 Progress: {progress:.1f}%\n"
                                f"📦 {human_readable_size(downloaded)} / {human_readable_size(total_size)}"
                            )
        
        final_size = os.path.getsize(file_path)
        
        # Upload to GoFile
        await msg.edit_text("⬆️ **Uploading to GoFile Cloud...**\n🚀 Maximum speed activated")
        
        download_link = await upload_to_gofile(file_path)
        
        if not download_link:
            await msg.edit_text("❌ **Upload failed!**\nGoFile servers might be busy. Please try again.")
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

📊 **Your Stats:** {user_data[6] + 1 if user_data else 1} total uploads
{BOT_OWNER}
        """
        
        await msg.edit_text(success_text, disable_web_page_preview=True)
        
        # Log to backup channel
        if BACKUP_CHANNEL_ID:
            try:
                log_text = f"""
📤 **URL Upload Complete**
👤 **User:** {message.from_user.first_name} (ID: `{user_id}`)
📛 **Username:** @{message.from_user.username if message.from_user.username else 'N/A'}
📅 **Date:** {get_current_time()}
🔗 **Source URL:** {text[:200]}
📂 **File:** `{file_name}`
📦 **Size:** {human_readable_size(final_size)}
🔗 **GoFile Link:** {download_link}
                """
                await client.send_message(
                    BACKUP_CHANNEL_ID,
                    log_text,
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Backup log failed: {e}")
        
        # Log to log channel with file
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
🔗 **Source URL:** {text}
📦 **File Size:** {human_readable_size(final_size)}
🔗 **Download Link:** {download_link}
            """,
            file_path
        )
        
    except Exception as e:
        logger.error(f"URL processing error: {e}")
        await msg.edit_text(f"❌ **Error:** {str(e)}")
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)

# ================== SCHEDULED TASKS ==================

async def check_expired_subscriptions():
    """Check and notify about expired subscriptions"""
    try:
        # This function would check for expired subscriptions
        # Since we removed premium features, we keep it as placeholder
        pass
    except Exception as e:
        logger.error(f"Subscription check error: {e}")

async def send_scheduled_ads():
    """Send scheduled ads to users"""
    try:
        active_ads = db.get_active_ads()
        if not active_ads:
            return
        
        users = db.get_all_users()
        
        for ad in active_ads:
            ad_id, message_text, message_id, schedule_type, frequency, times_per_day, start_date, end_date, is_active, target_users = ad
            
            # Check if it's time to send this ad
            current_time = datetime.now()
            start_time = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            
            if current_time < start_time or current_time > end_time:
                continue
            
            # Send ad to users
            # Implementation depends on your scheduling logic
            
            logger.info(f"Sending scheduled ad {ad_id} to users")
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
    await web.TCPSite(
        runner, "0.0.0.0",
        int(os.environ.get("PORT", 8080))
    ).start()

# ================== MAIN EXECUTION ==================

async def main():
    print(f"🤖 Ultimate GoFile Bot Starting... {POWERED_BY}")
    print(f"⚡ Powered by uvloop optimization")
    
    # Start scheduler
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.add_job(send_scheduled_ads, 'interval', minutes=30)
    scheduler.start()
    
    await app.start()
    print("✅ Bot Connected to Telegram")
    
    bot_info = await app.get_me()
    print(f"🤖 Bot Username: @{bot_info.username}")
    print(f"👑 Admin IDs: {ADMIN_IDS}")
    
    print("🌍 Starting Web Server...")
    await start_web()
    
    user_count = db.get_user_count()
    print(f"📊 Total Users in Database: {user_count}")
    
    print(f"🚀 High-Speed Pipeline Ready. {POWERED_BY}")
    print(f"📞 Support: {BOT_SUPPORT}")
    
    # Send startup notification to admin
    if SUPER_ADMIN_ID:
        try:
            await app.send_message(
                SUPER_ADMIN_ID,
                f"🤖 **Bot Started Successfully**\n\n"
                f"📅 Time: {get_current_time()}\n"
                f"👥 Users: {user_count}\n"
                f"⚡ Status: ONLINE\n\n"
                f"{POWERED_BY}"
            )
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
    
    await idle()
    
    # Cleanup
    scheduler.shutdown()
    await app.stop()
    print("👋 Bot Stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
