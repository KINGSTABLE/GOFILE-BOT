#!/usr/bin/env python3
import os
import aiohttp
import asyncio
import time
import mimetypes
import logging
import uvloop
import random
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery,
    Message
)
from pyrogram.errors import FloodWait, UserNotParticipant
from asyncio import Queue, Lock
from aiohttp import web

# ================== SPEED OPTIMIZATION ==================
uvloop.install()

# ================== IMPORTS ==================
from config import *
from database import db
from helpers import check_force_sub, get_invite_links, broadcast_message
from helpers.force_sub import (
    get_fsub_keyboard, 
    get_fsub_message,
    get_random_bypass_message,
    get_random_left_message
)
from helpers.decorators import admin_only, owner_only, not_banned

# ================== SETUP ==================
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== BOT INSTANCE ==================
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

async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS or user_id == OWNER_ID

# ================== FORCE SUBSCRIBE MIDDLEWARE ==================

async def force_sub_check(client: Client, message: Message) -> bool:
    """
    Check force subscribe status
    Returns True if user can proceed, False otherwise
    """
    user_id = message.from_user.id
    
    # Skip check for admins
    if await is_admin(user_id):
        return True
    
    # Check if banned
    if await db.is_banned(user_id):
        await message.reply_text(
            "🚫 **You are BANNED from using this bot!**\n\n"
            "Contact support if you think this is a mistake."
        )
        return False
    
    # Check maintenance mode
    if await db.is_maintenance() and not await is_admin(user_id):
        await message.reply_text(
            "🔧 **Bot Under Maintenance!**\n\n"
            "Please try again later. We're improving things!"
        )
        return False
    
    # Check force subscribe
    is_subscribed, missing_channels = await check_force_sub(client, user_id)
    
    if not is_subscribed:
        invite_links = await get_invite_links(client, missing_channels)
        keyboard = get_fsub_keyboard(missing_channels, invite_links)
        fsub_msg = get_fsub_message(len(missing_channels))
        
        await message.reply_text(
            fsub_msg,
            reply_markup=keyboard
        )
        return False
    
    return True

# ================== CALLBACK HANDLER FOR FSUB ==================

@app.on_callback_query(filters.regex("^check_fsub$"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    """Handle force subscribe verification"""
    user_id = callback.from_user.id
    
    is_subscribed, missing_channels = await check_force_sub(client, user_id)
    
    if is_subscribed:
        await callback.message.edit_text(
            "✅ **Verification Successful!**\n\n"
            "🎉 You can now use all bot features!\n"
            "Send /start to begin."
        )
        await callback.answer("✅ Verified! You can use the bot now!", show_alert=True)
    else:
        # User trying to bypass
        invite_links = await get_invite_links(client, missing_channels)
        keyboard = get_fsub_keyboard(missing_channels, invite_links)
        
        bypass_msg = get_random_bypass_message()
        
        await callback.answer(bypass_msg, show_alert=True)
        await callback.message.edit_text(
            f"{bypass_msg}\n\n"
            f"⚠️ You still need to join **{len(missing_channels)}** channel(s)!\n\n"
            f"👇 Join all channels and try again:",
            reply_markup=keyboard
        )

# ================== START COMMAND ==================

@app.on_message(filters.command("start") & filters.private)
async def start(client: Client, message: Message):
    user = message.from_user
    
    # Add user to database
    await db.add_user(user.id, {
        "first_name": user.first_name,
        "username": user.username
    })
    
    # Check force subscribe
    if not await force_sub_check(client, message):
        return
    
    # Get custom welcome message or use default
    custom_welcome = await db.get_welcome_message()
    
    # Get ads
    ads = await db.get_ads()
    
    # Default welcome message
    welcome_text = custom_welcome if custom_welcome else (
        f"👋 **Welcome, {user.first_name}!**\n\n"
        f"⚡ **High-Performance GoFile Uploader**\n\n"
        f"🚀 **Features:**\n"
        f"├ 📁 Upload Files (up to 50GB)\n"
        f"├ 🔗 Upload from URLs\n"
        f"├ ⚡ Ultra-fast processing\n"
        f"└ 📊 Track your uploads\n\n"
        f"📤 **Send me a file or URL to get started!**"
    )
    
    # Build keyboard
    buttons = []
    
    if SUPPORT_CHAT:
        buttons.append([
            InlineKeyboardButton("💬 Support", url=f"https://t.me/{SUPPORT_CHAT}"),
            InlineKeyboardButton("📢 Updates", url=f"https://t.me/{UPDATE_CHANNEL}" if UPDATE_CHANNEL else f"https://t.me/{SUPPORT_CHAT}")
        ])
    
    buttons.append([
        InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
        InlineKeyboardButton("ℹ️ Help", callback_data="help_menu")
    ])
    
    if await is_admin(user.id):
        buttons.append([
            InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")
        ])
    
    keyboard = InlineKeyboardMarkup(buttons)
    
    # Add ads if enabled
    if ads["enabled"] and ads["message"]:
        welcome_text += f"\n\n📢 **Sponsored:**\n{ads['message']}"
        if ads["button_text"] and ads["button_url"]:
            buttons.insert(0, [
                InlineKeyboardButton(ads["button_text"], url=ads["button_url"])
            ])
            keyboard = InlineKeyboardMarkup(buttons)
    
    if START_IMG:
        await message.reply_photo(
            START_IMG,
            caption=welcome_text,
            reply_markup=keyboard
        )
    else:
        await message.reply_text(
            welcome_text,
            reply_markup=keyboard
        )

# ================== HELP COMMAND ==================

@app.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    if not await force_sub_check(client, message):
        return
    
    help_text = (
        "📖 **Help & Commands**\n\n"
        "**User Commands:**\n"
        "├ /start - Start the bot\n"
        "├ /help - Show this help\n"
        "├ /stats - Your upload statistics\n"
        "├ /ping - Check bot latency\n"
        "└ /about - About the bot\n\n"
        "**How to Upload:**\n"
        "1️⃣ Send any file (document/video/audio/photo)\n"
        "2️⃣ Or send a direct download URL\n"
        "3️⃣ Wait for processing\n"
        "4️⃣ Get your GoFile link!\n\n"
        "**Supported:**\n"
        "📁 Files up to 50GB\n"
        "🔗 Direct HTTP/HTTPS URLs"
    )
    
    buttons = [
        [InlineKeyboardButton("🔙 Back to Start", callback_data="go_start")]
    ]
    
    await message.reply_text(help_text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^help_menu$"))
async def help_menu_callback(client: Client, callback: CallbackQuery):
    help_text = (
        "📖 **Help & Commands**\n\n"
        "**User Commands:**\n"
        "├ /start - Start the bot\n"
        "├ /help - Show this help\n"
        "├ /stats - Your upload statistics\n"
        "├ /ping - Check bot latency\n"
        "└ /about - About the bot\n\n"
        "**How to Upload:**\n"
        "1️⃣ Send any file (document/video/audio/photo)\n"
        "2️⃣ Or send a direct download URL\n"
        "3️⃣ Wait for processing\n"
        "4️⃣ Get your GoFile link!\n\n"
        "**Supported:**\n"
        "📁 Files up to 50GB\n"
        "🔗 Direct HTTP/HTTPS URLs"
    )
    
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="go_start")]]
    
    await callback.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^go_start$"))
async def go_start_callback(client: Client, callback: CallbackQuery):
    user = callback.from_user
    
    welcome_text = (
        f"👋 **Welcome, {user.first_name}!**\n\n"
        f"⚡ **High-Performance GoFile Uploader**\n\n"
        f"🚀 **Features:**\n"
        f"├ 📁 Upload Files (up to 50GB)\n"
        f"├ 🔗 Upload from URLs\n"
        f"├ ⚡ Ultra-fast processing\n"
        f"└ 📊 Track your uploads\n\n"
        f"📤 **Send me a file or URL to get started!**"
    )
    
    buttons = []
    
    if SUPPORT_CHAT:
        buttons.append([
            InlineKeyboardButton("💬 Support", url=f"https://t.me/{SUPPORT_CHAT}"),
            InlineKeyboardButton("📢 Updates", url=f"https://t.me/{UPDATE_CHANNEL}" if UPDATE_CHANNEL else f"https://t.me/{SUPPORT_CHAT}")
        ])
    
    buttons.append([
        InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
        InlineKeyboardButton("ℹ️ Help", callback_data="help_menu")
    ])
    
    if await is_admin(user.id):
        buttons.append([
            InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")
        ])
    
    await callback.message.edit_text(welcome_text, reply_markup=InlineKeyboardMarkup(buttons))

# ================== USER STATS ==================

@app.on_message(filters.command("stats") & filters.private)
async def user_stats_command(client: Client, message: Message):
    if not await force_sub_check(client, message):
        return
    
    user_id = message.from_user.id
    user_data = await db.get_user(user_id)
    
    if not user_data:
        await message.reply_text("❌ No stats found! Upload some files first.")
        return
    
    stats_text = (
        f"📊 **Your Statistics**\n\n"
        f"👤 **User:** {message.from_user.first_name}\n"
        f"🆔 **ID:** `{user_id}`\n"
        f"📅 **Joined:** {user_data.get('joined_date', 'Unknown')[:10]}\n"
        f"📤 **Uploads:** {user_data.get('uploads_count', 0)}\n"
        f"💾 **Total Size:** {human_readable_size(user_data.get('total_size', 0))}\n"
        f"🕐 **Last Active:** {user_data.get('last_active', 'Unknown')[:10]}"
    )
    
    await message.reply_text(stats_text)

@app.on_callback_query(filters.regex("^my_stats$"))
async def my_stats_callback(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    user_data = await db.get_user(user_id)
    
    if not user_data:
        await callback.answer("No stats yet! Upload some files first.", show_alert=True)
        return
    
    stats_text = (
        f"📊 **Your Statistics**\n\n"
        f"👤 **User:** {callback.from_user.first_name}\n"
        f"🆔 **ID:** `{user_id}`\n"
        f"📅 **Joined:** {user_data.get('joined_date', 'Unknown')[:10]}\n"
        f"📤 **Uploads:** {user_data.get('uploads_count', 0)}\n"
        f"💾 **Total Size:** {human_readable_size(user_data.get('total_size', 0))}\n"
        f"🕐 **Last Active:** {user_data.get('last_active', 'Unknown')[:10]}"
    )
    
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="go_start")]]
    
    await callback.message.edit_text(stats_text, reply_markup=InlineKeyboardMarkup(buttons))

# ================== PING COMMAND ==================

@app.on_message(filters.command("ping") & filters.private)
async def ping_command(client: Client, message: Message):
    start_time = time.time()
    msg = await message.reply_text("🏓 Pinging...")
    latency = (time.time() - start_time) * 1000
    await msg.edit_text(f"🏓 **Pong!**\n⚡ Latency: `{latency:.2f}ms`")

# ================== ABOUT COMMAND ==================

@app.on_message(filters.command("about") & filters.private)
async def about_command(client: Client, message: Message):
    if not await force_sub_check(client, message):
        return
    
    bot_stats = await db.get_bot_stats()
    
    about_text = (
        "ℹ️ **About This Bot**\n\n"
        f"🤖 **Bot Name:** GoFile Uploader\n"
        f"⚡ **Engine:** uvloop (High Performance)\n"
        f"👥 **Total Users:** {bot_stats['total_users']}\n"
        f"📤 **Total Uploads:** {bot_stats['total_uploads']}\n"
        f"💾 **Data Processed:** {human_readable_size(bot_stats['total_size'])}\n\n"
        "🔧 **Developer:** @YourUsername\n"
        "📅 **Version:** 2.0.0"
    )
    
    await message.reply_text(about_text)

# ================== ADMIN PANEL ==================

@app.on_callback_query(filters.regex("^admin_panel$"))
@admin_only
async def admin_panel_callback(client: Client, callback: CallbackQuery):
    bot_stats = await db.get_bot_stats()
    
    admin_text = (
        "👑 **Admin Control Panel**\n\n"
        f"👥 **Total Users:** {bot_stats['total_users']}\n"
        f"🚫 **Banned Users:** {bot_stats['banned_users']}\n"
        f"📢 **FSub Channels:** {bot_stats['fsub_channels']}\n"
        f"📤 **Total Uploads:** {bot_stats['total_uploads']}\n"
        f"💾 **Data Processed:** {human_readable_size(bot_stats['total_size'])}"
    )
    
    buttons = [
        [
            InlineKeyboardButton("👥 Users", callback_data="admin_users"),
            InlineKeyboardButton("📢 FSub", callback_data="admin_fsub")
        ],
        [
            InlineKeyboardButton("📡 Broadcast", callback_data="admin_broadcast"),
            InlineKeyboardButton("📣 Ads", callback_data="admin_ads")
        ],
        [
            InlineKeyboardButton("🔧 Settings", callback_data="admin_settings"),
            InlineKeyboardButton("📊 Stats", callback_data="admin_stats_detail")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="go_start")]
    ]
    
    await callback.message.edit_text(admin_text, reply_markup=InlineKeyboardMarkup(buttons))

# ================== ADMIN COMMANDS ==================

# ----- BROADCAST -----
@app.on_message(filters.command("broadcast") & filters.private)
@admin_only
async def broadcast_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text(
            "📡 **Broadcast Usage:**\n\n"
            "Reply to a message with:\n"
            "• `/broadcast` - Copy message\n"
            "• `/broadcast -f` - Forward message\n"
            "• `/broadcast -p` - Copy & Pin message"
        )
        return
    
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    forward = "-f" in args
    pin = "-p" in args
    
    status_msg = await message.reply_text("📡 **Preparing broadcast...**")
    
    await broadcast_message(
        client,
        message.reply_to_message,
        status_msg,
        forward=forward,
        pin=pin
    )

@app.on_callback_query(filters.regex("^admin_broadcast$"))
@admin_only
async def admin_broadcast_callback(client: Client, callback: CallbackQuery):
    text = (
        "📡 **Broadcast System**\n\n"
        "**Commands:**\n"
        "• `/broadcast` - Reply to message to broadcast\n"
        "• `/broadcast -f` - Forward instead of copy\n"
        "• `/broadcast -p` - Copy & pin message\n\n"
        "⚠️ Broadcasts may take time based on user count."
    )
    
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ----- USERS MANAGEMENT -----
@app.on_message(filters.command("users") & filters.private)
@admin_only
async def users_command(client: Client, message: Message):
    stats = await db.get_bot_stats()
    users = await db.get_all_users()
    
    text = (
        f"👥 **User Statistics**\n\n"
        f"📊 **Total Users:** {stats['total_users']}\n"
        f"🚫 **Banned Users:** {stats['banned_users']}\n\n"
        f"**Commands:**\n"
        f"• `/ban <user_id>` - Ban user\n"
        f"• `/unban <user_id>` - Unban user\n"
        f"• `/user <user_id>` - User info\n"
        f"• `/export` - Export user list"
    )
    
    await message.reply_text(text)

@app.on_callback_query(filters.regex("^admin_users$"))
@admin_only
async def admin_users_callback(client: Client, callback: CallbackQuery):
    stats = await db.get_bot_stats()
    
    text = (
        f"👥 **User Management**\n\n"
        f"📊 **Total Users:** {stats['total_users']}\n"
        f"🚫 **Banned Users:** {stats['banned_users']}\n\n"
        f"**Commands:**\n"
        f"• `/ban <user_id>` - Ban user\n"
        f"• `/unban <user_id>` - Unban user\n"
        f"• `/user <user_id>` - User info\n"
        f"• `/banned` - List banned users\n"
        f"• `/export` - Export user list"
    )
    
    buttons = [
        [
            InlineKeyboardButton("📋 Export Users", callback_data="export_users"),
            InlineKeyboardButton("🚫 Banned List", callback_data="banned_list")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_message(filters.command("ban") & filters.private)
@admin_only
async def ban_command(client: Client, message: Message):
    if len(message.text.split()) < 2:
        await message.reply_text("❌ Usage: `/ban <user_id>`")
        return
    
    try:
        user_id = int(message.text.split()[1])
    except ValueError:
        await message.reply_text("❌ Invalid user ID!")
        return
    
    if user_id in ADMIN_IDS or user_id == OWNER_ID:
        await message.reply_text("❌ Cannot ban admins!")
        return
    
    await db.ban_user(user_id)
    await message.reply_text(f"✅ User `{user_id}` has been **banned**!")

@app.on_message(filters.command("unban") & filters.private)
@admin_only
async def unban_command(client: Client, message: Message):
    if len(message.text.split()) < 2:
        await message.reply_text("❌ Usage: `/unban <user_id>`")
        return
    
    try:
        user_id = int(message.text.split()[1])
    except ValueError:
        await message.reply_text("❌ Invalid user ID!")
        return
    
    await db.unban_user(user_id)
    await message.reply_text(f"✅ User `{user_id}` has been **unbanned**!")

@app.on_message(filters.command("banned") & filters.private)
@admin_only
async def banned_list_command(client: Client, message: Message):
    banned = await db.get_banned_users()
    
    if not banned:
        await message.reply_text("✅ No banned users!")
        return
    
    text = "🚫 **Banned Users:**\n\n"
    for user_id in banned[:50]:  # Limit to 50
        text += f"• `{user_id}`\n"
    
    if len(banned) > 50:
        text += f"\n_...and {len(banned) - 50} more_"
    
    await message.reply_text(text)

@app.on_message(filters.command("user") & filters.private)
@admin_only
async def user_info_command(client: Client, message: Message):
    if len(message.text.split()) < 2:
        await message.reply_text("❌ Usage: `/user <user_id>`")
        return
    
    try:
        user_id = int(message.text.split()[1])
    except ValueError:
        await message.reply_text("❌ Invalid user ID!")
        return
    
    user_data = await db.get_user(user_id)
    
    if not user_data:
        await message.reply_text("❌ User not found in database!")
        return
    
    is_banned = await db.is_banned(user_id)
    
    text = (
        f"👤 **User Info**\n\n"
        f"🆔 **ID:** `{user_id}`\n"
        f"📛 **Name:** {user_data.get('first_name', 'Unknown')}\n"
        f"👤 **Username:** @{user_data.get('username', 'None')}\n"
        f"📅 **Joined:** {user_data.get('joined_date', 'Unknown')[:10]}\n"
        f"📤 **Uploads:** {user_data.get('uploads_count', 0)}\n"
        f"💾 **Total Size:** {human_readable_size(user_data.get('total_size', 0))}\n"
        f"🚫 **Banned:** {'Yes ❌' if is_banned else 'No ✅'}"
    )
    
    await message.reply_text(text)

# ----- FSUB MANAGEMENT -----
@app.on_message(filters.command("addfsub") & filters.private)
@admin_only
async def add_fsub_command(client: Client, message: Message):
    """
    Usage: /addfsub <channel_id> [channel_link]
    Example: /addfsub -1001234567890 https://t.me/channel
    """
    args = message.text.split()[1:]
    
    if len(args) < 1:
        await message.reply_text(
            "📢 **Add Force Subscribe Channel**\n\n"
            "**Usage:** `/addfsub <channel_id> [invite_link]`\n\n"
            "**Examples:**\n"
            "• `/addfsub -1001234567890`\n"
            "• `/addfsub -1001234567890 https://t.me/channel`\n\n"
            "⚠️ Bot must be admin in the channel!"
        )
        return
    
    try:
        channel_id = int(args[0])
    except ValueError:
        await message.reply_text("❌ Invalid channel ID!")
        return
    
    channel_link = args[1] if len(args) > 1 else ""
    
    # Try to get channel info
    try:
        chat = await client.get_chat(channel_id)
        channel_name = chat.title
    except Exception as e:
        await message.reply_text(f"⚠️ Could not fetch channel info: {e}\nAdding anyway...")
        channel_name = f"Channel {channel_id}"
    
    success = await db.add_fsub_channel(channel_id, channel_name, channel_link)
    
    if success:
        await message.reply_text(
            f"✅ **Channel Added!**\n\n"
            f"📢 **Name:** {channel_name}\n"
            f"🆔 **ID:** `{channel_id}`\n"
            f"🔗 **Link:** {channel_link or 'Auto-generated'}"
        )
    else:
        await message.reply_text("❌ Channel already exists!")

@app.on_message(filters.command("remfsub") & filters.private)
@admin_only
async def remove_fsub_command(client: Client, message: Message):
    if len(message.text.split()) < 2:
        await message.reply_text("❌ Usage: `/remfsub <channel_id>`")
        return
    
    try:
        channel_id = int(message.text.split()[1])
    except ValueError:
        await message.reply_text("❌ Invalid channel ID!")
        return
    
    success = await db.remove_fsub_channel(channel_id)
    
    if success:
        await message.reply_text(f"✅ Channel `{channel_id}` removed from FSub!")
    else:
        await message.reply_text("❌ Channel not found in FSub list!")

@app.on_message(filters.command("fsub") & filters.private)
@admin_only
async def fsub_list_command(client: Client, message: Message):
    channels = await db.get_fsub_channels()
    is_enabled = await db.is_fsub_enabled()
    
    if not channels:
        await message.reply_text(
            "📢 **Force Subscribe Channels**\n\n"
            "❌ No channels configured!\n\n"
            "**Add channels using:**\n"
            "`/addfsub <channel_id> [link]`"
        )
        return
    
    text = f"📢 **Force Subscribe Channels**\n\n"
    text += f"**Status:** {'🟢 Enabled' if is_enabled else '🔴 Disabled'}\n\n"
    
    for i, ch in enumerate(channels, 1):
        text += f"{i}. **{ch.get('name', 'Unknown')}**\n"
        text += f"   🆔 `{ch['id']}`\n"
        if ch.get('link'):
            text += f"   🔗 {ch['link']}\n"
        text += "\n"
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable" if is_enabled else "🟢 Enable",
                callback_data="toggle_fsub"
            )
        ]
    ]
    
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^admin_fsub$"))
@admin_only
async def admin_fsub_callback(client: Client, callback: CallbackQuery):
    channels = await db.get_fsub_channels()
    is_enabled = await db.is_fsub_enabled()
    
    text = f"📢 **Force Subscribe Management**\n\n"
    text += f"**Status:** {'🟢 Enabled' if is_enabled else '🔴 Disabled'}\n"
    text += f"**Channels:** {len(channels)}\n\n"
    
    if channels:
        for i, ch in enumerate(channels, 1):
            text += f"{i}. {ch.get('name', 'Unknown')} (`{ch['id']}`)\n"
    else:
        text += "_No channels configured_\n"
    
    text += "\n**Commands:**\n"
    text += "• `/addfsub <id> [link]` - Add channel\n"
    text += "• `/remfsub <id>` - Remove channel\n"
    text += "• `/fsub` - List channels"
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable FSub" if is_enabled else "🟢 Enable FSub",
                callback_data="toggle_fsub"
            )
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^toggle_fsub$"))
@admin_only
async def toggle_fsub_callback(client: Client, callback: CallbackQuery):
    current = await db.is_fsub_enabled()
    await db.toggle_fsub(not current)
    
    status = "🟢 Enabled" if not current else "🔴 Disabled"
    await callback.answer(f"FSub {status}!", show_alert=True)
    
    # Refresh the panel
    await admin_fsub_callback(client, callback)

# ----- ADS MANAGEMENT -----
@app.on_message(filters.command("setad") & filters.private)
@admin_only
async def set_ad_command(client: Client, message: Message):
    """
    Usage: /setad <message>
    Or reply to a message with /setad
    """
    if message.reply_to_message:
        ad_message = message.reply_to_message.text or message.reply_to_message.caption or ""
    elif len(message.text.split(None, 1)) > 1:
        ad_message = message.text.split(None, 1)[1]
    else:
        await message.reply_text(
            "📣 **Set Advertisement**\n\n"
            "**Usage:**\n"
            "• `/setad <your ad message>`\n"
            "• Reply to a message with `/setad`\n\n"
            "**With Button:**\n"
            "`/setad <message> | <button_text> | <button_url>`"
        )
        return
    
    # Parse button if provided
    parts = ad_message.split(" | ")
    ad_text = parts[0]
    button_text = parts[1] if len(parts) > 1 else ""
    button_url = parts[2] if len(parts) > 2 else ""
    
    await db.set_ads(True, ad_text, button_text, button_url)
    
    await message.reply_text(
        f"✅ **Advertisement Set!**\n\n"
        f"📝 **Message:** {ad_text}\n"
        f"🔘 **Button:** {button_text or 'None'}\n"
        f"🔗 **URL:** {button_url or 'None'}"
    )

@app.on_message(filters.command("delad") & filters.private)
@admin_only
async def delete_ad_command(client: Client, message: Message):
    await db.set_ads(False, "", "", "")
    await message.reply_text("✅ Advertisement deleted!")

@app.on_message(filters.command("togglead") & filters.private)
@admin_only
async def toggle_ad_command(client: Client, message: Message):
    ads = await db.get_ads()
    new_status = not ads["enabled"]
    await db.toggle_ads(new_status)
    status = "🟢 Enabled" if new_status else "🔴 Disabled"
    await message.reply_text(f"✅ Ads {status}")

@app.on_callback_query(filters.regex("^admin_ads$"))
@admin_only
async def admin_ads_callback(client: Client, callback: CallbackQuery):
    ads = await db.get_ads()
    
    text = (
        f"📣 **Advertisement Management**\n\n"
        f"**Status:** {'🟢 Enabled' if ads['enabled'] else '🔴 Disabled'}\n"
        f"**Message:** {ads['message'][:50] + '...' if len(ads['message']) > 50 else ads['message'] or 'Not set'}\n"
        f"**Button:** {ads['button_text'] or 'Not set'}\n\n"
        f"**Commands:**\n"
        f"• `/setad <message>` - Set ad\n"
        f"• `/setad <msg> | <btn> | <url>` - With button\n"
        f"• `/delad` - Delete ad\n"
        f"• `/togglead` - Toggle ads"
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable" if ads['enabled'] else "🟢 Enable",
                callback_data="toggle_ads_btn"
            )
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^toggle_ads_btn$"))
@admin_only
async def toggle_ads_btn_callback(client: Client, callback: CallbackQuery):
    ads = await db.get_ads()
    new_status = not ads["enabled"]
    await db.toggle_ads(new_status)
    await callback.answer(f"Ads {'Enabled' if new_status else 'Disabled'}!", show_alert=True)
    await admin_ads_callback(client, callback)

# ----- SETTINGS -----
@app.on_callback_query(filters.regex("^admin_settings$"))
@admin_only
async def admin_settings_callback(client: Client, callback: CallbackQuery):
    is_maintenance = await db.is_maintenance()
    
    text = (
        "🔧 **Bot Settings**\n\n"
        f"**Maintenance Mode:** {'🟢 ON' if is_maintenance else '🔴 OFF'}\n\n"
        "**Commands:**\n"
        "• `/maintenance on/off` - Toggle maintenance\n"
        "• `/setwelcome <message>` - Set welcome message\n"
        "• `/resetwelcome` - Reset to default"
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable Maintenance" if is_maintenance else "🟢 Enable Maintenance",
                callback_data="toggle_maintenance"
            )
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^toggle_maintenance$"))
@admin_only
async def toggle_maintenance_callback(client: Client, callback: CallbackQuery):
    current = await db.is_maintenance()
    await db.set_maintenance(not current)
    status = "🟢 Enabled" if not current else "🔴 Disabled"
    await callback.answer(f"Maintenance {status}!", show_alert=True)
    await admin_settings_callback(client, callback)

@app.on_message(filters.command("maintenance") & filters.private)
@admin_only
async def maintenance_command(client: Client, message: Message):
    args = message.text.split()
    
    if len(args) < 2:
        current = await db.is_maintenance()
        await message.reply_text(
            f"🔧 **Maintenance Mode:** {'ON 🟢' if current else 'OFF 🔴'}\n\n"
            f"Usage: `/maintenance on` or `/maintenance off`"
        )
        return
    
    action = args[1].lower()
    
    if action == "on":
        await db.set_maintenance(True)
        await message.reply_text("✅ Maintenance mode **enabled**!")
    elif action == "off":
        await db.set_maintenance(False)
        await message.reply_text("✅ Maintenance mode **disabled**!")
    else:
        await message.reply_text("❌ Use: `/maintenance on` or `/maintenance off`")

@app.on_message(filters.command("setwelcome") & filters.private)
@admin_only
async def set_welcome_command(client: Client, message: Message):
    if len(message.text.split(None, 1)) < 2:
        await message.reply_text(
            "📝 **Set Welcome Message**\n\n"
            "Usage: `/setwelcome <your message>`\n\n"
            "**Available placeholders:**\n"
            "• `{first_name}` - User's first name\n"
            "• `{user_id}` - User's ID\n"
            "• `{username}` - User's username"
        )
        return
    
    welcome_msg = message.text.split(None, 1)[1]
    await db.set_welcome_message(welcome_msg)
    await message.reply_text(f"✅ Welcome message set!\n\n**Preview:**\n{welcome_msg}")

@app.on_message(filters.command("resetwelcome") & filters.private)
@admin_only
async def reset_welcome_command(client: Client, message: Message):
    await db.set_welcome_message("")
    await message.reply_text("✅ Welcome message reset to default!")

# ----- STATS -----
@app.on_callback_query(filters.regex("^admin_stats_detail$"))
@admin_only
async def admin_stats_detail_callback(client: Client, callback: CallbackQuery):
    stats = await db.get_bot_stats()
    
    text = (
        "📊 **Detailed Statistics**\n\n"
        f"👥 **Total Users:** {stats['total_users']}\n"
        f"🚫 **Banned Users:** {stats['banned_users']}\n"
        f"📢 **FSub Channels:** {stats['fsub_channels']}\n"
        f"📤 **Total Uploads:** {stats['total_uploads']}\n"
        f"💾 **Total Data:** {human_readable_size(stats['total_size'])}\n"
        f"📅 **Bot Started:** {stats['start_time'][:10]}"
    )
    
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ================== IMMEDIATE BACKUP ==================

async def immediate_backup(client, message, is_url=False, url_text=None):
    """Step 1: Immediately forward content to backup channel before processing."""
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

# ================== URL HANDLING ==================

@app.on_message(filters.text & filters.private & ~filters.command(["start", "help", "stats", "ping", "about", "broadcast", "users", "ban", "unban", "banned", "user", "addfsub", "remfsub", "fsub", "setad", "delad", "togglead", "maintenance", "setwelcome", "resetwelcome", "export"]))
async def url_handler(client: Client, message: Message):
    text = message.text.strip()
    
    if not (text.startswith("http://") or text.startswith("https://")):
        return

    # Force subscribe check
    if not await force_sub_check(client, message):
        return

    # 1. IMMEDIATE BACKUP
    await immediate_backup(client, message, is_url=True, url_text=text)

    msg = await message.reply_text(
        "🔗 **URL Detected!**\n\n"
        "🚀 Queued for High-Speed Processing...\n"
        "⏳ Please wait..."
    )
    await download_queue.put(("url", text, message, msg))
    
    asyncio.create_task(process_queue(client))

# ================== FILE HANDLING ==================

@app.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_handler(client: Client, message: Message):
    if message.chat.id == BACKUP_CHANNEL_ID:
        return

    # Force subscribe check
    if not await force_sub_check(client, message):
        return

    # 1. IMMEDIATE BACKUP
    await immediate_backup(client, message, is_url=False)

    media = message.document or message.video or message.audio or message.photo
    
    file_size = getattr(media, 'file_size', 0)
    file_name = getattr(media, 'file_name', 'file')
    
    msg = await message.reply_text(
        f"📁 **File Detected!**\n\n"
        f"📄 **Name:** `{file_name}`\n"
        f"📦 **Size:** `{human_readable_size(file_size)}`\n\n"
        f"🚀 Queued for High-Speed Processing..."
    )
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
        f"⬇️ **Downloading...**\n\n"
        f"📄 **File:** `{file_name}`\n"
        f"📦 **Size:** `{human_readable_size(media.file_size)}`\n"
        f"⚡ **Mode:** Native Stream"
    )

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

    await status_msg.edit_text(
        "⬇️ **Fast Downloading...**\n\n"
        f"🔗 **URL:** `{url[:50]}...`\n"
        "⏳ **Mode:** Optimized HTTP Stream"
    )

    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, timeout=None) as response:
            if response.status != 200:
                return await status_msg.edit_text(f"❌ URL Error: {response.status}")
            
            with open(file_path, "wb") as f:
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
        await status_msg.edit_text(
            "⬆️ **Uploading to GoFile...**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** `{human_readable_size(file_size)}`\n"
            "🚀 **Optimized Buffer Active**"
        )
        
        link = await upload_to_gofile(file_path)

        if not link:
            return await status_msg.edit_text("❌ **Upload Failed.**\nGoFile servers might be busy.")

        # Update user stats
        await db.update_user_stats(message.from_user.id, file_size)

        # ================== 1. USER RESPONSE ==================
        user_text = (
            f"✅ **Upload Complete!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** `{human_readable_size(file_size)}`\n"
            f"📥 **Source:** {source}\n\n"
            f"🔗 **Download Link:**\n{link}"
        )
        
        buttons = [
            [InlineKeyboardButton("🔗 Open Link", url=link)],
            [InlineKeyboardButton("📤 Upload Another", callback_data="go_start")]
        ]
        
        await status_msg.edit_text(
            user_text, 
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(buttons)
        )

        # ================== 2. BACKUP CHANNEL FINAL LOG ==================
        if BACKUP_CHANNEL_ID:
            user = message.from_user
            log_text = (
                f"#UPLOAD_COMPLETE\n\n"
                f"👤 **User:** {user.first_name} (`{user.id}`)\n"
                f"📛 **Username:** @{user.username if user.username else 'None'}\n"
                f"📅 **Date:** {get_current_time()}\n"
                f"📥 **Source:** {source}\n"
                f"📄 **File:** `{file_name}`\n"
                f"📦 **Size:** `{human_readable_size(file_size)}`\n"
                f"🔗 **GoFile Link:** {link}"
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
        await status_msg.edit_text(f"❌ **Critical Error:** {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# ================== GOFILE UPLOADER ==================

async def upload_to_gofile(path):
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
                    
                    if GOFILE_FOLDER_ID:
                        data.add_field('folderId', GOFILE_FOLDER_ID)

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
    stats = await db.get_bot_stats()
    return web.Response(
        text=f"Bot Running | Users: {stats['total_users']} | Uploads: {stats['total_uploads']}",
        content_type="text/plain"
    )

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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())