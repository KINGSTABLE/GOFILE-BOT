#!/usr/bin/env python3
import os
import aiohttp
import asyncio
import time
import mimetypes
import csv
import re
import logging
import uvloop
import random
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timedelta, timezone
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery,
    Message
)
from pyrogram.errors import FloodWait, UserNotParticipant, RPCError
from asyncio import Queue
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
MAX_CONCURRENT_QUEUE_WORKERS = 10
queue_worker_tasks = []
shutdown_in_progress = False
ADMIN_WIZARDS = {}
ACTION_UNDO = {}
LIST_PAGE_SIZE = 10
MAX_CHANNEL_NAME_DISPLAY_LENGTH = 45
ADMIN_TEXT_COMMANDS = [
    "start", "help", "stats", "ping", "about", "analytics", "usernamefile", "broadcast",
    "users", "ban", "unban", "banned", "user", "addfsub", "remfsub", "fsub", "setad",
    "delad", "togglead", "maintenance", "setwelcome", "resetwelcome", "export"
]

# ================== HELPER FUNCTIONS ==================

def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_bool_badge(value: bool) -> str:
    return "🟢 ON" if value else "🔴 OFF"

def get_admin_wizard_state(admin_id: int) -> dict:
    return ADMIN_WIZARDS.get(int(admin_id), {})

def set_admin_wizard_state(admin_id: int, flow: str, step: str, data: dict = None):
    ADMIN_WIZARDS[int(admin_id)] = {
        "flow": flow,
        "step": step,
        "data": data or {},
        "updated_at": int(time.time())
    }

def clear_admin_wizard_state(admin_id: int):
    ADMIN_WIZARDS.pop(int(admin_id), None)

def put_undo_action(admin_id: int, action_key: str, payload: dict, ttl_seconds: int = 120):
    ACTION_UNDO[f"{int(admin_id)}:{action_key}"] = {
        "payload": payload,
        "expires_at": int(time.time()) + max(30, int(ttl_seconds))
    }

def get_undo_action(admin_id: int, action_key: str):
    key = f"{int(admin_id)}:{action_key}"
    action = ACTION_UNDO.get(key)
    if not action:
        return None
    if int(time.time()) > int(action.get("expires_at", 0)):
        ACTION_UNDO.pop(key, None)
        return None
    return action.get("payload")

def consume_undo_action(admin_id: int, action_key: str):
    key = f"{int(admin_id)}:{action_key}"
    action = get_undo_action(admin_id, action_key)
    ACTION_UNDO.pop(key, None)
    return action

async def log_admin_action(user_id: int, action: str, metadata: dict = None):
    await db.log_user_event(
        user_id,
        "admin_action",
        chat_id=user_id,  # admin actions are logged from private-chat admin workflows
        metadata={"action": action, **(metadata or {})}
    )

def is_valid_http_url(url: str) -> bool:
    try:
        parsed = urlsplit((url or "").strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

def is_supported_fsub_chat_type(chat_type) -> bool:
    return str(chat_type).lower() in ("channel", "supergroup")

def normalize_channel_reference(raw: str):
    value = (raw or "").strip()
    if not value:
        raise ValueError("Channel reference is empty.")

    if re.fullmatch(r"-?\d+", value):
        return int(value), "chat_id"

    if re.search(r"^https?://t\.me/", value, re.IGNORECASE):
        match = re.search(r"t\.me/(.+)$", value, re.IGNORECASE)
        path = (match.group(1) if match else "").strip("/")
        if not path:
            raise ValueError("Invalid Telegram link.")
        first = path.split("/", 1)[0].strip()
        if first.startswith("+") or first == "joinchat":
            return value, "invite_link"
        return f"@{first.lstrip('@')}", "username"

    if value.startswith("@"):
        return value, "username"

    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", value):
        return f"@{value}", "username"

    raise ValueError("Unsupported channel reference. Use chat ID, @username, or t.me link.")

async def resolve_fsub_channel(client: Client, raw_reference: str) -> dict:
    ref, ref_type = normalize_channel_reference(raw_reference)
    chat = await client.get_chat(ref)
    if not is_supported_fsub_chat_type(chat.type):
        raise ValueError("Only channels/supergroups are supported for FSub.")

    me = await client.get_me()
    member = await client.get_chat_member(chat.id, me.id)
    member_status = getattr(member, "status", "")
    is_admin = member_status in ("administrator", "creator")
    return {
        "id": int(chat.id),
        "name": chat.title or f"Channel {chat.id}",
        "input_type": ref_type,
        "input_value": raw_reference,
        "is_admin": is_admin,
        "admin_error": "" if is_admin else "Bot must be admin in this channel to enforce FSub.",
        "chat_invite_link": getattr(chat, "invite_link", "") or ""
    }

async def create_fsub_invite_link(client: Client, channel_id: int, days: int = 0, member_limit: int = 0) -> str:
    expire_date = None
    if int(days) > 0:
        expire_date = datetime.now(timezone.utc) + timedelta(days=int(days))

    try:
        invite = await client.create_chat_invite_link(
            channel_id,
            expire_date=expire_date,
            member_limit=int(member_limit) if int(member_limit) > 0 else None
        )
        return getattr(invite, "invite_link", "") or ""
    except Exception:
        try:
            return await client.export_chat_invite_link(channel_id)
        except Exception:
            return ""

async def list_bot_admin_channels(client: Client, limit: int = 30) -> list:
    me = await client.get_me()
    channels = []
    seen = set()
    async for dialog in client.get_dialogs(limit=200):
        chat = getattr(dialog, "chat", None)
        if not chat:
            continue
        if not is_supported_fsub_chat_type(chat.type):
            continue
        chat_id = int(chat.id)
        if chat_id in seen:
            continue
        seen.add(chat_id)
        try:
            member = await client.get_chat_member(chat_id, me.id)
            if getattr(member, "status", "") in ("administrator", "creator"):
                channels.append({
                    "id": chat_id,
                    "name": chat.title or f"Channel {chat_id}"
                })
        except Exception:
            continue
        if len(channels) >= int(limit):
            break
    return channels

async def ensure_default_fsub_channel(client: Client):
    target = (DEFAULT_FSUB_CHANNEL or "").strip()
    if not target:
        return
    channels = await db.get_fsub_channels()
    try:
        resolved = await resolve_fsub_channel(client, target)
    except Exception as e:
        logger.warning(f"Could not resolve default FSUB channel {target}: {e}")
        return
    if any(int(ch.get("id", 0)) == int(resolved["id"]) for ch in channels):
        return
    await db.add_fsub_channel(resolved["id"], resolved["name"], "")

async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS or user_id == OWNER_ID

def get_user_payload(message: Message) -> dict:
    """Extract detailed Telegram user/chat payload."""
    user = message.from_user
    chat = message.chat
    return {
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "username": user.username or "",
        "language_code": getattr(user, "language_code", "") or "",
        "is_bot": bool(getattr(user, "is_bot", False)),
        "is_premium": bool(getattr(user, "is_premium", False)),
        "is_verified": bool(getattr(user, "is_verified", False)),
        "is_scam": bool(getattr(user, "is_scam", False)),
        "is_fake": bool(getattr(user, "is_fake", False)),
        "chat_id": chat.id if chat else user.id,
        "chat_type": chat.type if chat else "private",
    }

async def build_start_text_and_keyboard(user):
    custom_welcome = await db.get_welcome_message()
    ads = await db.get_ads()

    welcome_text = custom_welcome if custom_welcome else (
        f"👋 **Welcome, {user.first_name}!**\n\n"
        f"⚡ **High-Performance GoFile Uploader**\n\n"
        f"🚀 **Features:**\n"
        f"├ 📁 Upload Files (up to 4GB)\n"
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
            InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel"),
            InlineKeyboardButton("🧭 Admin Guide", callback_data="admin_guide")
        ])

    if ads["enabled"] and ads["message"]:
        welcome_text += f"\n\n📢 **Sponsored:**\n{ads['message']}"
        if ads["button_text"] and ads["button_url"]:
            buttons.insert(0, [InlineKeyboardButton(ads["button_text"], url=ads["button_url"])])

    return welcome_text, InlineKeyboardMarkup(buttons)

def strip_markdown_formatting(text: str) -> str:
    return re.sub(r"[*_`~>#+=|{}\[\]()]", "", text)

async def send_start_response(message: Message, welcome_text: str, keyboard: InlineKeyboardMarkup):
    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id if message.chat else "unknown"
    if START_IMG:
        try:
            await message.reply_photo(
                START_IMG,
                caption=welcome_text,
                reply_markup=keyboard
            )
            return
        except Exception as e:
            logger.error(f"Failed to send START_IMG welcome (user={user_id}, chat={chat_id}): {e}")

    try:
        await message.reply_text(
            welcome_text,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Failed to send markdown welcome text (user={user_id}, chat={chat_id}), falling back to plain text: {e}")
        try:
            await message.reply_text(
                strip_markdown_formatting(welcome_text),
                reply_markup=keyboard,
                parse_mode=None
            )
        except Exception as plain_send_error:
            logger.error(f"Failed to send plain-text welcome fallback (user={user_id}, chat={chat_id}): {plain_send_error}")

async def edit_start_response(callback: CallbackQuery, welcome_text: str, keyboard: InlineKeyboardMarkup):
    callback_message = callback.message if callback else None
    user_id = callback.from_user.id if callback and callback.from_user else "unknown"
    chat_id = callback_message.chat.id if callback_message and callback_message.chat else "unknown"
    try:
        await callback.message.edit_text(welcome_text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Failed to edit start text (user={user_id}, chat={chat_id}), falling back to plain text: {e}")
        plain_text = strip_markdown_formatting(welcome_text)
        try:
            await callback.message.edit_text(plain_text, reply_markup=keyboard, parse_mode=None)
        except Exception as plain_edit_error:
            logger.error(f"Failed to edit plain-text start message (user={user_id}, chat={chat_id}); sending reply instead: {plain_edit_error}")
            try:
                await callback.message.reply_text(plain_text, reply_markup=keyboard, parse_mode=None)
            except Exception as plain_reply_error:
                logger.error(f"Failed to send plain-text start reply fallback (user={user_id}, chat={chat_id}): {plain_reply_error}")
 
# ================== FORCE SUBSCRIBE MIDDLEWARE ==================

async def force_sub_check(client: Client, message: Message) -> bool:
    """
    Check force subscribe status
    Returns True if user can proceed, False otherwise
    """
    try:
        user_id = message.from_user.id
        user_payload = get_user_payload(message)
        chat_id = message.chat.id if message.chat else user_id

        # Skip check for admins
        if await is_admin(user_id):
            await db.add_user(user_id, user_payload, chat_id=chat_id, source="admin_interaction")
            await db.log_user_event(user_id, "admin_activity", chat_id=chat_id, metadata={"source": "force_sub_check"})
            return True

        await db.add_user(user_id, user_payload, chat_id=chat_id, source="user_interaction", persist=False)
        await db.log_user_event(user_id, "activity", chat_id=chat_id, metadata={"source": "force_sub_check"})

        # Check if banned
        if await db.is_banned(user_id):
            await message.reply_text(
                "🚫 **You are BANNED from using this bot!**\n\n"
                "Contact support if you think this is a mistake."
            )
            return False

        # Check maintenance mode
        if await db.is_maintenance():
            await message.reply_text(
                "🔧 **Bot Under Maintenance!**\n\n"
                "Please try again later. We're improving things!"
            )
            return False

        # Check force subscribe
        enforcement_mode = await db.get_enforcement_mode()
        is_subscribed, missing_channels = await check_force_sub(client, user_id)
        is_revoked = (not is_subscribed and enforcement_mode == "aggressive")
        await db.record_enforcement_check(
            passed=is_subscribed,
            revoked=is_revoked,
            user_id=user_id,
            persist=False
        )

        if not is_subscribed:
            if enforcement_mode == "aggressive":
                await db.log_user_event(
                    user_id,
                    "enforcement_revoked",
                    chat_id=chat_id,
                    metadata={
                        "reason": "missing_required_channels",
                        "missing_count": len(missing_channels)
                    },
                    persist=False
                )
            invite_links = await get_invite_links(client, missing_channels)
            keyboard = get_fsub_keyboard(missing_channels, invite_links)
            await message.reply_text(
                f"{get_random_left_message()}\n\n{get_fsub_message(len(missing_channels))}",
                reply_markup=keyboard
            )
            return False

        return True
    except Exception as e:
        logger.error(f"force_sub_check failed for user {message.from_user.id}: {e}")
        await message.reply_text("❌ Could not verify channel membership right now. Please try again in a moment.")
        return False

# ================== CALLBACK HANDLER FOR FSUB ==================

@app.on_callback_query(filters.regex("^check_fsub$"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    """Handle force subscribe verification"""
    user_id = callback.from_user.id

    if await is_admin(user_id):
        await callback.answer("✅ Admin bypass active.", show_alert=True)
        return
    
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

@app.on_message(filters.private & filters.regex(r"^/"), group=-1)
async def command_analytics_tracker(client: Client, message: Message):
    if message.from_user:
        try:
            user_payload = get_user_payload(message)
            user_id = message.from_user.id
            chat_id = message.chat.id if message.chat else user_id
            await db.add_user(
                user_id,
                user_payload,
                chat_id=chat_id,
                source="command",
                persist=False
            )
            await db.log_user_event(
                user_id,
                "command",
                chat_id=chat_id,
                metadata={
                    "command_name": ((getattr(message, "command", None) or [""])[0]),
                    "args_count": max(0, len(getattr(message, "command", None) or []) - 1)
                }
            )
        except Exception as e:
            logger.error(f"Command analytics tracking failed: {e}")

# ================== START COMMAND ==================

@app.on_message(filters.command("start") & filters.private)
async def start(client: Client, message: Message):
    user = message.from_user
    
    # Check force subscribe
    if not await force_sub_check(client, message):
        return
    
    welcome_text, keyboard = await build_start_text_and_keyboard(user)
    await send_start_response(message, welcome_text, keyboard)

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
        "📁 Files up to 4GB\n"
        "🔗 Direct HTTP/HTTPS URLs"
    )
    
    buttons = [
        [InlineKeyboardButton("🔙 Back to Start", callback_data="go_start")]
    ]
    
    await message.reply_text(help_text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^help_menu$"))
async def help_menu_callback(client: Client, callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        is_subscribed, missing_channels = await check_force_sub(client, callback.from_user.id)
        if not is_subscribed:
            invite_links = await get_invite_links(client, missing_channels)
            await callback.message.edit_text(
                get_fsub_message(len(missing_channels)),
                reply_markup=get_fsub_keyboard(missing_channels, invite_links)
            )
            await callback.answer("Join required channels first.", show_alert=True)
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
        "📁 Files up to 4GB\n"
        "🔗 Direct HTTP/HTTPS URLs"
    )
    
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="go_start")]]
    
    await callback.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^go_start$"))
async def go_start_callback(client: Client, callback: CallbackQuery):
    user = callback.from_user
    if not await is_admin(user.id):
        is_subscribed, missing_channels = await check_force_sub(client, user.id)
        if not is_subscribed:
            invite_links = await get_invite_links(client, missing_channels)
            await callback.message.edit_text(
                get_fsub_message(len(missing_channels)),
                reply_markup=get_fsub_keyboard(missing_channels, invite_links)
            )
            await callback.answer("Join required channels first.", show_alert=True)
            return

    welcome_text, keyboard = await build_start_text_and_keyboard(user)
    await edit_start_response(callback, welcome_text, keyboard)

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
    if not await is_admin(callback.from_user.id):
        is_subscribed, missing_channels = await check_force_sub(client, callback.from_user.id)
        if not is_subscribed:
            invite_links = await get_invite_links(client, missing_channels)
            await callback.message.edit_text(
                get_fsub_message(len(missing_channels)),
                reply_markup=get_fsub_keyboard(missing_channels, invite_links)
            )
            await callback.answer("Join required channels first.", show_alert=True)
            return

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
    if not await force_sub_check(client, message):
        return
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
        "🔧 **Developer:** @TG_Bot_Support_bot\n"
        "📅 **Version:** 2.0.0"
    )
    
    await message.reply_text(about_text)

# ================== ADMIN PANEL ==================

@app.on_callback_query(filters.regex("^admin_panel$"))
@admin_only
async def admin_panel_callback(client: Client, callback: CallbackQuery):
    bot_stats = await db.get_bot_stats()
    ads = await db.get_ads()
    maintenance = await db.is_maintenance()
    enforcement = await db.get_enforcement_stats()

    admin_text = (
        "👑 **Admin Control Center**\n\n"
        "**System Status**\n"
        f"• Maintenance: {format_bool_badge(maintenance)}\n"
        f"• Ads: {format_bool_badge(ads.get('enabled', False))}\n"
        f"• Enforcement: {'🛡 Aggressive' if enforcement['mode'] == 'aggressive' else '✅ Normal'}\n\n"
        "**Core Metrics**\n"
        f"• Users: `{bot_stats['total_users']}`\n"
        f"• Banned: `{bot_stats['banned_users']}`\n"
        f"• FSub Channels: `{bot_stats['fsub_channels']}`\n"
        f"• Uploads: `{bot_stats['total_uploads']}`\n"
        f"• Data: `{human_readable_size(bot_stats['total_size'])}`\n\n"
        "_Choose a section below._"
    )
    
    buttons = [
        [
            InlineKeyboardButton("👥 Users", callback_data="admin_users"),
            InlineKeyboardButton("🔐 FSub & Access", callback_data="admin_fsub:0")
        ],
        [
            InlineKeyboardButton("📡 Broadcast Wizard", callback_data="admin_broadcast"),
            InlineKeyboardButton("📣 Ads Wizard", callback_data="admin_ads_wizard")
        ],
        [
            InlineKeyboardButton("🔧 Settings Wizard", callback_data="admin_settings"),
            InlineKeyboardButton("📊 Stats", callback_data="admin_stats_detail")
        ],
        [
            InlineKeyboardButton("📈 Analytics", callback_data="admin_analytics"),
            InlineKeyboardButton("🛡 Safety Logs", callback_data="admin_safety_logs:0")
        ],
        [InlineKeyboardButton("🧭 Admin Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="go_start")]
    ]
    
    await callback.message.edit_text(admin_text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^admin_guide$"))
@admin_only
async def admin_guide_callback(client: Client, callback: CallbackQuery):
    text = (
        "🧭 **Admin Guidance**\n\n"
        "**What this panel does**\n"
        "• Gives guided admin workflows with confirmation steps.\n"
        "• Keeps risky actions protected by confirm/undo.\n"
        "• Shows health, analytics, and safety logs in one place.\n\n"
        "**Recommended usage**\n"
        "1) Configure **FSub & Access** first.\n"
        "2) Enable **Aggressive Enforcement** only after setup check passes.\n"
        "3) Use **Broadcast Wizard** for previews/dry-runs before send.\n"
        "4) Use **Safety Logs** daily for revocations/admin actions.\n\n"
        "**Fallback commands**\n"
        "• `/ban`, `/unban`, `/addfsub`, `/remfsub`, `/setad`, `/maintenance`\n"
        "• `/analytics`, `/usernamefile`, `/export`"
    )
    buttons = [
        [InlineKeyboardButton("👑 Admin Home", callback_data="admin_panel")],
        [InlineKeyboardButton("🔙 Back", callback_data="go_start")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

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
    clear_admin_wizard_state(callback.from_user.id)
    text = (
        "📡 **Broadcast Wizard**\n\n"
        "**What this does:**\n"
        "• Send one message to all users with guided confirmations.\n\n"
        "**Step 1/3:** Choose delivery mode."
    )

    buttons = [
        [
            InlineKeyboardButton("📄 Copy", callback_data="wiz_broadcast_mode:copy"),
            InlineKeyboardButton("↪️ Forward", callback_data="wiz_broadcast_mode:forward")
        ],
        [InlineKeyboardButton("📌 Copy + Pin", callback_data="wiz_broadcast_mode:pin")],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^wiz_broadcast_mode:(copy|forward|pin)$"))
@admin_only
async def wizard_broadcast_mode_callback(client: Client, callback: CallbackQuery):
    mode = callback.data.split(":")[1]
    mode_data = {
        "copy": {"forward": False, "pin": False, "label": "📄 Copy"},
        "forward": {"forward": True, "pin": False, "label": "↪️ Forward"},
        "pin": {"forward": False, "pin": True, "label": "📌 Copy + Pin"}
    }[mode]
    set_admin_wizard_state(callback.from_user.id, "broadcast", "await_content", mode_data)
    await callback.message.edit_text(
        "📡 **Broadcast Wizard**\n\n"
        f"**Mode:** {mode_data['label']}\n"
        "**Step 2/3:** Send the message (text/media) you want to broadcast.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_broadcast")]
        ])
    )

@app.on_callback_query(filters.regex("^wiz_broadcast_preview$"))
@admin_only
async def wizard_broadcast_preview_callback(client: Client, callback: CallbackQuery):
    state = get_admin_wizard_state(callback.from_user.id)
    data = state.get("data", {})
    source_chat = data.get("source_chat")
    source_message = data.get("source_message")
    if state.get("flow") != "broadcast" or state.get("step") != "preview" or not source_message:
        await callback.answer("No pending broadcast draft found.", show_alert=True)
        return
    try:
        msg = await client.get_messages(source_chat, source_message)
        if not msg:
            raise ValueError("message unavailable")
        if data.get("forward"):
            await msg.forward(callback.from_user.id)
        else:
            await msg.copy(callback.from_user.id)
        await callback.answer("Preview delivered to your chat.")
    except Exception as e:
        logger.error(f"Broadcast preview failed: {e}")
        await callback.answer("Preview failed. Send draft again.", show_alert=True)

@app.on_callback_query(filters.regex("^wiz_broadcast_confirm$"))
@admin_only
async def wizard_broadcast_confirm_callback(client: Client, callback: CallbackQuery):
    state = get_admin_wizard_state(callback.from_user.id)
    data = state.get("data", {})
    source_chat = data.get("source_chat")
    source_message = data.get("source_message")
    if state.get("flow") != "broadcast" or state.get("step") != "preview" or not source_message:
        await callback.answer("No pending broadcast found.", show_alert=True)
        return
    try:
        source_msg = await client.get_messages(source_chat, source_message)
        if not source_msg:
            raise ValueError("source message not found")
        status_msg = await callback.message.reply_text("📡 Starting broadcast to all users...")
        stats = await broadcast_message(
            client,
            source_msg,
            status_msg,
            forward=bool(data.get("forward")),
            pin=bool(data.get("pin"))
        )
        clear_admin_wizard_state(callback.from_user.id)
        await log_admin_action(
            callback.from_user.id,
            "broadcast_sent",
            {
                "success": stats.success,
                "failed": stats.failed,
                "blocked": stats.blocked,
                "deleted": stats.deleted,
                "total": stats.total
            }
        )
        await db.log_user_event(
            callback.from_user.id,
            "broadcast_report",
            chat_id=callback.from_user.id,
            metadata={
                "success": stats.success,
                "failed": stats.failed,
                "blocked": stats.blocked,
                "deleted": stats.deleted,
                "total": stats.total
            }
        )
        await callback.message.edit_text(
            "✅ **Broadcast Finished**\n\n"
            f"👥 Total: `{stats.total}`\n"
            f"✅ Success: `{stats.success}`\n"
            f"❌ Failed: `{stats.failed}`\n"
            f"🚫 Blocked: `{stats.blocked}`\n"
            f"👻 Deleted: `{stats.deleted}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📡 New Broadcast", callback_data="admin_broadcast")],
                [InlineKeyboardButton("🛡 Safety Logs", callback_data="admin_safety_logs:0")],
                [InlineKeyboardButton("🔙 Admin Home", callback_data="admin_panel")]
            ])
        )
    except Exception as e:
        logger.error(f"Broadcast execution failed: {e}")
        await callback.answer("Broadcast failed. Try again.", show_alert=True)

# ----- USERS MANAGEMENT -----
async def generate_users_export_file() -> tuple[str, int]:
    users = await db.get_all_users()
    if not users:
        return "", 0

    filename = f"users_export_{int(time.time())}.csv"
    export_path = os.path.join(DOWNLOAD_DIR, filename)

    with open(export_path, "w", encoding="utf-8") as f:
        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["user_id", "username", "first_name", "last_name", "joined_date", "last_active", "uploads", "total_size_bytes"])
        for user in users.values():
            row = [
                str(user.get("user_id", "")),
                str(user.get("username", "")),
                str(user.get("first_name", "")),
                str(user.get("last_name", "")),
                str(user.get("joined_date", "")),
                str(user.get("last_active", "")),
                str(user.get("uploads_count", 0)),
                str(user.get("total_size", 0)),
            ]
            writer.writerow(row)

    return export_path, len(users)

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

@app.on_message(filters.command("export") & filters.private)
@admin_only
async def export_users_command(client: Client, message: Message):
    export_path, total_users = await generate_users_export_file()
    if not export_path:
        await message.reply_text("❌ No users found to export.")
        return

    try:
        await message.reply_document(
            export_path,
            caption=f"📋 Users export generated.\n👥 Total users: `{total_users}`"
        )
    except RPCError as e:
        logger.error(f"Failed exporting users: {e}")
        await message.reply_text("❌ Failed to export users right now.")
    finally:
        if os.path.exists(export_path):
            try:
                os.remove(export_path)
            except OSError as cleanup_error:
                logger.warning(f"Failed to remove export file {export_path}: {cleanup_error}")

@app.on_callback_query(filters.regex("^admin_users$"))
@admin_only
async def admin_users_callback(client: Client, callback: CallbackQuery):
    stats = await db.get_bot_stats()
    
    text = (
        "👥 **User Management**\n\n"
        f"• Total Users: `{stats['total_users']}`\n"
        f"• Banned Users: `{stats['banned_users']}`\n\n"
        "**What this does:**\n"
        "• Moderate abuse and inspect user records.\n\n"
        "**Recommended usage:**\n"
        "• Use button actions first, commands as fallback."
    )
    
    buttons = [
        [
            InlineKeyboardButton("📋 Export Users", callback_data="export_users"),
            InlineKeyboardButton("🚫 Banned List", callback_data="banned_list:0")
        ],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^export_users$"))
@admin_only
async def export_users_callback(client: Client, callback: CallbackQuery):
    export_path, total_users = await generate_users_export_file()
    if not export_path:
        await callback.answer("No users found to export.", show_alert=True)
        return

    try:
        await callback.message.reply_document(
            export_path,
            caption=f"📋 Users export generated.\n👥 Total users: `{total_users}`"
        )
        await callback.answer("Users export sent.")
    except RPCError as e:
        logger.error(f"Failed exporting users via callback: {e}")
        await callback.answer("Failed to export users.", show_alert=True)
    finally:
        if os.path.exists(export_path):
            try:
                os.remove(export_path)
            except OSError as cleanup_error:
                logger.warning(f"Failed to remove export file {export_path}: {cleanup_error}")

@app.on_callback_query(filters.regex(r"^banned_list(?::\d+)?$"))
@admin_only
async def banned_list_callback(client: Client, callback: CallbackQuery):
    banned = await db.get_banned_users()
    page = 0
    if ":" in callback.data:
        try:
            page = max(0, int(callback.data.split(":")[1]))
        except ValueError:
            page = 0

    if not banned:
        text = "✅ No banned users!"
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
    else:
        total = len(banned)
        start = page * LIST_PAGE_SIZE
        end = start + LIST_PAGE_SIZE
        chunk = banned[start:end]
        total_pages = max(1, (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
        banned_lines = [f"• `{strip_markdown_formatting(str(user_id))}`" for user_id in chunk]
        text = (
            "🚫 **Banned Users**\n\n"
            f"{'\n'.join(banned_lines)}\n\n"
            f"_Page {page + 1}/{total_pages} • Total {total}_"
        )
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"banned_list:{page-1}"))
        if end < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"banned_list:{page+1}"))
        buttons = []
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="admin_users")])
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

    await message.reply_text(
        f"⚠️ **Confirm Ban**\n\nBan user `{user_id}`?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_ban:{user_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data="admin_users")
            ]
        ])
    )

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
    
    await message.reply_text(
        f"⚠️ **Confirm Unban**\n\nUnban user `{user_id}`?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_unban:{user_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data="admin_users")
            ]
        ])
    )

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

@app.on_callback_query(filters.regex(r"^confirm_ban:\-?\d+$"))
@admin_only
async def confirm_ban_callback(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    if user_id in ADMIN_IDS or user_id == OWNER_ID:
        await callback.answer("Cannot ban admins.", show_alert=True)
        return
    await db.ban_user(user_id)
    put_undo_action(callback.from_user.id, f"ban:{user_id}", {"user_id": user_id}, ttl_seconds=120)
    await log_admin_action(callback.from_user.id, "ban_user", {"target_user": user_id})
    await callback.message.edit_text(
        f"✅ User `{user_id}` banned.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ Undo (2m)", callback_data=f"undo_ban:{user_id}")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_users")]
        ])
    )

@app.on_callback_query(filters.regex(r"^undo_ban:\-?\d+$"))
@admin_only
async def undo_ban_callback(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    action = consume_undo_action(callback.from_user.id, f"ban:{user_id}")
    if not action:
        await callback.answer("Undo expired or unavailable.", show_alert=True)
        return
    await db.unban_user(user_id)
    await log_admin_action(callback.from_user.id, "undo_ban", {"target_user": user_id})
    await callback.message.edit_text(
        f"✅ Ban reverted for `{user_id}`.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]])
    )

@app.on_callback_query(filters.regex(r"^confirm_unban:\-?\d+$"))
@admin_only
async def confirm_unban_callback(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await db.unban_user(user_id)
    put_undo_action(callback.from_user.id, f"unban:{user_id}", {"user_id": user_id}, ttl_seconds=120)
    await log_admin_action(callback.from_user.id, "unban_user", {"target_user": user_id})
    await callback.message.edit_text(
        f"✅ User `{user_id}` unbanned.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ Undo (2m)", callback_data=f"undo_unban:{user_id}")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_users")]
        ])
    )

@app.on_callback_query(filters.regex(r"^undo_unban:\-?\d+$"))
@admin_only
async def undo_unban_callback(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    action = consume_undo_action(callback.from_user.id, f"unban:{user_id}")
    if not action:
        await callback.answer("Undo expired or unavailable.", show_alert=True)
        return
    await db.ban_user(user_id)
    await log_admin_action(callback.from_user.id, "undo_unban", {"target_user": user_id})
    await callback.message.edit_text(
        f"✅ Unban reverted for `{user_id}`.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]])
    )

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
    Usage: /addfsub <channel_id|@username|invite_link> [days] [member_limit]
    Example: /addfsub @TOOLS_BOTS_KING 7 100
    """
    args = message.text.split()[1:]
    
    if len(args) < 1:
        await message.reply_text(
            "📢 **Add Force Subscribe Channel**\n\n"
            "**Usage:** `/addfsub <channel_ref> [days] [member_limit]`\n\n"
            "Where `channel_ref` can be chat ID, @username, or invite link.\n\n"
            "**Examples:**\n"
            "• `/addfsub -1001234567890`\n"
            "• `/addfsub @TOOLS_BOTS_KING`\n"
            "• `/addfsub https://t.me/TOOLS_BOTS_KING 3 200`\n\n"
            "⚠️ Bot must be admin in the channel for Force Sub."
        )
        return

    channel_ref = args[0]
    days = 0
    member_limit = 0
    try:
        if len(args) > 1:
            days = max(0, int(args[1]))
        if len(args) > 2:
            member_limit = max(0, int(args[2]))
        resolved = await resolve_fsub_channel(client, channel_ref)
    except ValueError as e:
        await message.reply_text(f"❌ {e}")
        return
    except Exception as e:
        await message.reply_text(f"❌ Could not resolve channel: {e}")
        return

    if not resolved.get("is_admin"):
        await message.reply_text(
            "❌ Bot is not admin in that channel.\n"
            "Telegram requires admin rights to verify member status for Force Sub."
        )
        return

    channel_id = int(resolved["id"])
    channel_name = resolved["name"]
    channel_link = await create_fsub_invite_link(client, channel_id, days=days, member_limit=member_limit)
    if not channel_link:
        channel_link = resolved.get("chat_invite_link", "")

    success = await db.add_fsub_channel(channel_id, channel_name, channel_link)
    
    if success:
        await log_admin_action(message.from_user.id, "add_fsub_channel", {"channel_id": channel_id})
        await message.reply_text(
            f"✅ **Channel Added!**\n\n"
            f"📢 **Name:** {channel_name}\n"
            f"🆔 **ID:** `{channel_id}`\n"
            f"🔗 **Invite:** {channel_link or 'Auto (not available)'}\n"
            f"⏳ **Expiry Days:** `{days}`\n"
            f"👥 **Join Limit:** `{member_limit}`"
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

    await message.reply_text(
        f"⚠️ **Confirm Removal**\n\nRemove FSub channel `{channel_id}`?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_remfsub:{channel_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data="admin_fsub:0")
            ]
        ])
    )

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
            "`/addfsub <channel_ref> [days] [member_limit]`"
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
                "🔒 FSub Locked ON",
                callback_data="fsub_locked_info"
            )
        ]
    ]
    
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"^admin_fsub(?::\d+)?$"))
@admin_only
async def admin_fsub_callback(client: Client, callback: CallbackQuery):
    channels = await db.get_fsub_channels()
    is_enabled = await db.is_fsub_enabled()
    enforcement = await db.get_enforcement_stats()
    page = 0
    if ":" in callback.data:
        try:
            page = max(0, int(callback.data.split(":")[1]))
        except ValueError:
            page = 0
    total = len(channels)
    start = page * LIST_PAGE_SIZE
    end = start + LIST_PAGE_SIZE
    chunk = channels[start:end]
    total_pages = max(1, (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)

    text = (
        "🔐 **FSub & Access Management**\n\n"
        f"• Required Access: {format_bool_badge(is_enabled)}\n"
        f"• Enforcement Mode: {'🛡 Aggressive' if enforcement['mode'] == 'aggressive' else '✅ Normal'}\n"
        f"• Checks: `{enforcement['checks']}` | Fails: `{enforcement['failed_checks']}` | Revoked: `{enforcement['revoked_access']}`\n"
        f"• Channels: `{total}`\n\n"
        "**What this does:**\n"
        "• Keeps non-admin users in required channels before bot usage.\n"
        "• Bot must be admin in FSUB channels to verify members and generate invite links.\n\n"
    )
    if chunk:
        text += "**Configured channels:**\n"
        for i, ch in enumerate(chunk, start + 1):
            text += f"{i}. {ch.get('name', 'Unknown')} (`{ch['id']}`)\n"
        text += f"\n_Page {page + 1}/{total_pages}_"
    else:
        text += "_No channels configured yet._"

    buttons = [
        [
            InlineKeyboardButton("➕ Add Channel Wizard", callback_data="wiz_fsub_start"),
            InlineKeyboardButton("➖ Remove Channel", callback_data="wiz_fsub_remove_pick")
        ],
        [
            InlineKeyboardButton(
                "🛡 Set Normal" if enforcement["mode"] == "aggressive" else "🛡 Set Aggressive",
                callback_data="toggle_enforcement_mode"
            ),
            InlineKeyboardButton("🔄 Re-check Now", callback_data="fsub_recheck_now")
        ],
        [InlineKeyboardButton("📜 Revocation Logs", callback_data="admin_safety_logs:0")],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_fsub:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_fsub:{page+1}"))
    if nav:
        buttons.insert(1, nav)
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^fsub_locked_info$"))
@admin_only
async def toggle_fsub_callback(client: Client, callback: CallbackQuery):
    await db.toggle_fsub(True)
    await callback.answer("Channel subscription requirements cannot be disabled in this deployment.", show_alert=True)
    await admin_fsub_callback(client, callback)

@app.on_callback_query(filters.regex("^toggle_enforcement_mode$"))
@admin_only
async def toggle_enforcement_mode_callback(client: Client, callback: CallbackQuery):
    current = await db.get_enforcement_mode()
    new_mode = "normal" if current == "aggressive" else "aggressive"
    await db.set_enforcement_mode(new_mode)
    await log_admin_action(callback.from_user.id, "set_enforcement_mode", {"mode": new_mode})
    await callback.answer(f"Enforcement mode set to {new_mode.upper()}.", show_alert=True)
    await admin_fsub_callback(client, callback)

@app.on_callback_query(filters.regex("^fsub_recheck_now$"))
@admin_only
async def fsub_recheck_now_callback(client: Client, callback: CallbackQuery):
    await db.log_user_event(
        callback.from_user.id,
        "admin_action",
        chat_id=callback.from_user.id,
        metadata={"action": "manual_recheck_requested"}
    )
    await callback.answer("Manual re-check marker saved to safety logs.", show_alert=True)

@app.on_callback_query(filters.regex("^wiz_fsub_start$"))
@admin_only
async def wizard_fsub_start_callback(client: Client, callback: CallbackQuery):
    set_admin_wizard_state(callback.from_user.id, "fsub", "await_channel_ref", {})
    await callback.message.edit_text(
        "🔐 **FSub Setup Wizard**\n\n"
        "**Step 1/3:** Send channel reference now.\n"
        "Accepted: `-100...` chat ID, `@username`, or `https://t.me/...` link.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Pick from Bot Admin Channels", callback_data="wiz_fsub_pick_admin")],
            [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_fsub:0")]
        ])
    )

@app.on_callback_query(filters.regex("^wiz_fsub_pick_admin$"))
@admin_only
async def wizard_fsub_pick_admin_callback(client: Client, callback: CallbackQuery):
    channels = await list_bot_admin_channels(client, limit=20)
    if not channels:
        await callback.answer("No admin channels found for this bot.", show_alert=True)
        return
    buttons = []
    for ch in channels:
        buttons.append([
            InlineKeyboardButton(
                f"📢 {ch['name'][:MAX_CHANNEL_NAME_DISPLAY_LENGTH]}",
                callback_data=f"wiz_fsub_pick:{int(ch['id'])}"
            )
        ])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="wiz_fsub_start")])
    await callback.message.edit_text(
        "📋 **Pick a Channel**\n\nThese are channels where bot is currently admin:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^wiz_fsub_pick:\-?\d+$"))
@admin_only
async def wizard_fsub_pick_channel_callback(client: Client, callback: CallbackQuery):
    channel_id = int(callback.data.split(":")[1])
    try:
        chat = await client.get_chat(channel_id)
    except Exception as e:
        await callback.answer(f"Could not open channel: {e}", show_alert=True)
        return
    set_admin_wizard_state(
        callback.from_user.id,
        "fsub",
        "await_invite_settings",
        {
            "channel_id": int(chat.id),
            "channel_name": chat.title or f"Channel {chat.id}",
            "input_type": "picker"
        }
    )
    await callback.message.edit_text(
        "🔗 **Invite Link Options**\n\n"
        "Step 2/3: Send `days member_limit`.\n"
        "Examples:\n"
        "• `7 100` (expires in 7 days, max 100 joins)\n"
        "• `0 0` (no expiry/no limit)\n"
        "• `skip` (auto defaults)",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")],
            [InlineKeyboardButton("🔙 Back", callback_data="wiz_fsub_start")]
        ])
    )

@app.on_callback_query(filters.regex("^wiz_fsub_remove_pick$"))
@admin_only
async def wizard_fsub_remove_pick_callback(client: Client, callback: CallbackQuery):
    channels = await db.get_fsub_channels()
    if not channels:
        await callback.answer("No channels to remove.", show_alert=True)
        return
    buttons = []
    for ch in channels[:20]:
        buttons.append([
            InlineKeyboardButton(
                f"🗑 {ch.get('name', 'Channel')[:24]}",
                callback_data=f"confirm_remfsub:{int(ch['id'])}"
            )
        ])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="admin_fsub:0")])
    await callback.message.edit_text(
        "➖ **Remove FSub Channel**\n\nSelect channel to remove:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^confirm_remfsub:\-?\d+$"))
@admin_only
async def confirm_remove_fsub_callback(client: Client, callback: CallbackQuery):
    channel_id = int(callback.data.split(":")[1])
    success = await db.remove_fsub_channel(channel_id)
    if success:
        await log_admin_action(callback.from_user.id, "remove_fsub_channel", {"channel_id": channel_id})
        await callback.answer("FSub channel removed.", show_alert=True)
    else:
        await callback.answer("Channel not found.", show_alert=True)
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
    await log_admin_action(message.from_user.id, "set_ad_command", {"with_button": bool(button_text and button_url)})
    
    await message.reply_text(
        f"✅ **Advertisement Set!**\n\n"
        f"📝 **Message:** {ad_text}\n"
        f"🔘 **Button:** {button_text or 'None'}\n"
        f"🔗 **URL:** {button_url or 'None'}"
    )

@app.on_message(filters.command("delad") & filters.private)
@admin_only
async def delete_ad_command(client: Client, message: Message):
    await message.reply_text(
        "⚠️ **Confirm Delete Ad**\n\nThis removes ad message and button.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data="confirm_delad"),
                InlineKeyboardButton("❌ Cancel", callback_data="admin_ads_wizard")
            ]
        ])
    )

@app.on_message(filters.command("togglead") & filters.private)
@admin_only
async def toggle_ad_command(client: Client, message: Message):
    ads = await db.get_ads()
    new_status = not ads["enabled"]
    await db.toggle_ads(new_status)
    await log_admin_action(message.from_user.id, "toggle_ads", {"enabled": new_status})
    status = "🟢 Enabled" if new_status else "🔴 Disabled"
    await message.reply_text(f"✅ Ads {status}")

@app.on_callback_query(filters.regex("^(admin_ads|admin_ads_wizard)$"))
@admin_only
async def admin_ads_callback(client: Client, callback: CallbackQuery):
    ads = await db.get_ads()
    clear_admin_wizard_state(callback.from_user.id)
    
    text = (
        "📣 **Ads Wizard**\n\n"
        f"• Status: {format_bool_badge(ads['enabled'])}\n"
        f"• Message: {ads['message'][:60] + '...' if len(ads['message']) > 60 else ads['message'] or 'Not set'}\n"
        f"• Button: {ads['button_text'] or 'Not set'}\n\n"
        "**What this does:**\n"
        "• Configure promo card shown in start panel.\n"
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable" if ads['enabled'] else "🟢 Enable",
                callback_data="toggle_ads_btn"
            ),
            InlineKeyboardButton("✍️ Create/Update Ad", callback_data="wiz_ads_start")
        ],
        [InlineKeyboardButton("🗑 Delete Ad", callback_data="confirm_delad")],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^toggle_ads_btn$"))
@admin_only
async def toggle_ads_btn_callback(client: Client, callback: CallbackQuery):
    ads = await db.get_ads()
    new_status = not ads["enabled"]
    await db.toggle_ads(new_status)
    await log_admin_action(callback.from_user.id, "toggle_ads", {"enabled": new_status})
    await callback.answer(f"Ads {'Enabled' if new_status else 'Disabled'}!", show_alert=True)
    await admin_ads_callback(client, callback)

@app.on_callback_query(filters.regex("^confirm_delad$"))
@admin_only
async def confirm_delete_ad_callback(client: Client, callback: CallbackQuery):
    await db.set_ads(False, "", "", "")
    await log_admin_action(callback.from_user.id, "delete_ad")
    await callback.answer("Advertisement deleted.", show_alert=True)
    await admin_ads_callback(client, callback)

@app.on_callback_query(filters.regex("^wiz_ads_start$"))
@admin_only
async def wizard_ads_start_callback(client: Client, callback: CallbackQuery):
    set_admin_wizard_state(callback.from_user.id, "ads", "await_message", {})
    await callback.message.edit_text(
        "📣 **Ad Wizard**\n\n"
        "**Step 1/3:** Send ad message text now.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_ads_wizard")]
        ])
    )

# ----- SETTINGS -----
@app.on_callback_query(filters.regex("^admin_settings$"))
@admin_only
async def admin_settings_callback(client: Client, callback: CallbackQuery):
    is_maintenance = await db.is_maintenance()
    enforcement = await db.get_enforcement_mode()
    welcome = await db.get_welcome_message()
    
    text = (
        "🔧 **Settings Wizard**\n\n"
        f"• Maintenance: {format_bool_badge(is_maintenance)}\n"
        f"• Enforcement Mode: {'🛡 Aggressive' if enforcement == 'aggressive' else '✅ Normal'}\n"
        f"• Custom Welcome: {format_bool_badge(bool(welcome))}\n\n"
        "**What this does:**\n"
        "• Controls global behavior and admin safety defaults."
    )
    
    buttons = [
        [
            InlineKeyboardButton(
                "🔴 Disable Maintenance" if is_maintenance else "🟢 Enable Maintenance",
                callback_data="toggle_maintenance"
            ),
            InlineKeyboardButton("📝 Set Welcome", callback_data="wiz_setwelcome")
        ],
        [InlineKeyboardButton("♻️ Reset Welcome", callback_data="reset_welcome_btn")],
        [InlineKeyboardButton("🧪 Setup Checks", callback_data="admin_setup_checks")],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^toggle_maintenance$"))
@admin_only
async def toggle_maintenance_callback(client: Client, callback: CallbackQuery):
    current = await db.is_maintenance()
    await db.set_maintenance(not current)
    await log_admin_action(callback.from_user.id, "toggle_maintenance", {"enabled": (not current)})
    status = "🟢 Enabled" if not current else "🔴 Disabled"
    await callback.answer(f"Maintenance {status}!", show_alert=True)
    await admin_settings_callback(client, callback)

@app.on_callback_query(filters.regex("^reset_welcome_btn$"))
@admin_only
async def reset_welcome_btn_callback(client: Client, callback: CallbackQuery):
    await db.set_welcome_message("")
    await log_admin_action(callback.from_user.id, "reset_welcome")
    await callback.answer("Welcome reset to default.", show_alert=True)
    await admin_settings_callback(client, callback)

@app.on_callback_query(filters.regex("^admin_setup_checks$"))
@admin_only
async def admin_setup_checks_callback(client: Client, callback: CallbackQuery):
    checks = [
        ("WEB_BASE_URL", bool(WEB_BASE_URL), "Needed for web dashboard links."),
        ("ADMIN_DASHBOARD_TOKEN", bool(ADMIN_DASHBOARD_TOKEN), "Needed for secure dashboard access."),
        ("SUPPORT_CHAT", bool(SUPPORT_CHAT), "Recommended for user help routing."),
        ("DEFAULT_FSUB_CHANNEL", bool(DEFAULT_FSUB_CHANNEL), "Default channel is auto-seeded for FSub.")
    ]
    lines = []
    for name, ok, note in checks:
        lines.append(f"• `{name}`: {'✅ OK' if ok else '⚠️ Missing'} — {note}")
    text = (
        "🧪 **Admin Setup Checks**\n\n"
        "Use this to catch misconfiguration quickly.\n\n"
        + "\n".join(lines)
    )
    buttons = [
        [InlineKeyboardButton("🔐 Open FSub & Access", callback_data="admin_fsub:0")],
        [InlineKeyboardButton("📈 Open Analytics", callback_data="admin_analytics")],
        [InlineKeyboardButton("🔧 Back to Settings", callback_data="admin_settings")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

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
        await log_admin_action(message.from_user.id, "maintenance_command", {"enabled": True})
        await message.reply_text("✅ Maintenance mode **enabled**!")
    elif action == "off":
        await db.set_maintenance(False)
        await log_admin_action(message.from_user.id, "maintenance_command", {"enabled": False})
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
    await log_admin_action(message.from_user.id, "set_welcome")
    await message.reply_text(f"✅ Welcome message set!\n\n**Preview:**\n{welcome_msg}")

@app.on_message(filters.command("resetwelcome") & filters.private)
@admin_only
async def reset_welcome_command(client: Client, message: Message):
    await db.set_welcome_message("")
    await log_admin_action(message.from_user.id, "reset_welcome")
    await message.reply_text("✅ Welcome message reset to default!")

@app.on_callback_query(filters.regex("^wiz_setwelcome$"))
@admin_only
async def wizard_setwelcome_callback(client: Client, callback: CallbackQuery):
    set_admin_wizard_state(callback.from_user.id, "settings", "await_welcome_message", {})
    await callback.message.edit_text(
        "📝 **Welcome Wizard**\n\n"
        "Send the new welcome message now.\n"
        "Use `skip` to cancel.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_settings")]
        ])
    )

@app.on_callback_query(filters.regex("^wiz_cancel$"))
@admin_only
async def wizard_cancel_callback(client: Client, callback: CallbackQuery):
    clear_admin_wizard_state(callback.from_user.id)
    await callback.answer("Wizard cancelled.", show_alert=True)
    await admin_panel_callback(client, callback)

@app.on_callback_query(filters.regex("^wiz_ads_publish$"))
@admin_only
async def wizard_ads_publish_callback(client: Client, callback: CallbackQuery):
    state = get_admin_wizard_state(callback.from_user.id)
    data = state.get("data", {})
    if state.get("flow") != "ads" or state.get("step") != "preview":
        await callback.answer("No ad draft found.", show_alert=True)
        return
    await db.set_ads(True, data.get("message", ""), data.get("button_text", ""), data.get("button_url", ""))
    await log_admin_action(callback.from_user.id, "publish_ad", {"with_button": bool(data.get("button_text"))})
    clear_admin_wizard_state(callback.from_user.id)
    await callback.answer("Ad published.", show_alert=True)
    await admin_ads_callback(client, callback)

@app.on_callback_query(filters.regex("^wiz_fsub_save$"))
@admin_only
async def wizard_fsub_save_callback(client: Client, callback: CallbackQuery):
    state = get_admin_wizard_state(callback.from_user.id)
    data = state.get("data", {})
    if state.get("flow") != "fsub" or state.get("step") != "preview":
        await callback.answer("No FSub draft found.", show_alert=True)
        return
    channel_id = int(data["channel_id"])
    success = await db.add_fsub_channel(channel_id, data.get("channel_name", f"Channel {channel_id}"), data.get("channel_link", ""))
    clear_admin_wizard_state(callback.from_user.id)
    if success:
        await log_admin_action(callback.from_user.id, "add_fsub_channel", {"channel_id": channel_id})
        await callback.answer("FSub channel added.", show_alert=True)
    else:
        await callback.answer("Channel already exists.", show_alert=True)
    await admin_fsub_callback(client, callback)

@app.on_callback_query(filters.regex(r"^admin_safety_logs(?::\d+)?$"))
@admin_only
async def admin_safety_logs_callback(client: Client, callback: CallbackQuery):
    page = 0
    if ":" in callback.data:
        try:
            page = max(0, int(callback.data.split(":")[1]))
        except ValueError:
            page = 0
    events = await db.get_recent_user_events(limit=200, event_types=["admin_action", "enforcement_revoked", "broadcast_report"])
    total = len(events)
    start = page * LIST_PAGE_SIZE
    end = start + LIST_PAGE_SIZE
    chunk = events[start:end]
    total_pages = max(1, (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
    lines = []
    for event in chunk:
        ts = str(event.get("timestamp", ""))[:16].replace("T", " ")
        et = event.get("event_type", "event")
        uid = event.get("user_id", 0)
        meta = event.get("metadata", {})
        summary = meta.get("action") or meta.get("reason") or meta.get("mode") or "details"
        lines.append(f"• `{ts}` | **{et}** | u:`{uid}` | {str(summary)[:40]}")
    text = (
        "🛡 **Safety Logs**\n\n"
        + ("\n".join(lines) if lines else "_No safety events yet._")
        + f"\n\n_Page {page + 1}/{total_pages} • Total {total}_"
    )
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_safety_logs:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_safety_logs:{page+1}"))
    buttons = []
    if nav:
        buttons.append(nav)
    buttons.extend([
        [InlineKeyboardButton("🔄 Refresh", callback_data=f"admin_safety_logs:{page}")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_message(filters.private & ~filters.command(ADMIN_TEXT_COMMANDS))
async def admin_wizard_input_handler(client: Client, message: Message):
    if not message.from_user or not await is_admin(message.from_user.id):
        return
    state = get_admin_wizard_state(message.from_user.id)
    if not state:
        return
    flow = state.get("flow")
    step = state.get("step")
    data = state.get("data", {})

    if flow == "broadcast" and step == "await_content":
        set_admin_wizard_state(
            message.from_user.id,
            "broadcast",
            "preview",
            {
                **data,
                "source_chat": message.chat.id,
                "source_message": message.id
            }
        )
        await message.reply_text(
            "📡 **Broadcast Draft Ready**\n\n"
            "**Step 3/3:** Preview or send now.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👀 Dry-run Preview", callback_data="wiz_broadcast_preview")],
                [InlineKeyboardButton("✅ Confirm & Send", callback_data="wiz_broadcast_confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")]
            ])
        )
        return

    if flow == "ads":
        text = (message.text or message.caption or "").strip()
        if step == "await_message":
            if not text:
                await message.reply_text("❌ Send ad text first.")
                return
            set_admin_wizard_state(message.from_user.id, "ads", "await_button_text", {"message": text})
            await message.reply_text("Step 2/3: Send button text, or type `skip` for no button.")
            return
        if step == "await_button_text":
            if text.lower() == "skip":
                set_admin_wizard_state(message.from_user.id, "ads", "preview", {"message": data.get("message", ""), "button_text": "", "button_url": ""})
                await message.reply_text(
                    f"📣 **Ad Preview**\n\n📝 {data.get('message', '')}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Publish", callback_data="wiz_ads_publish")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")]
                    ])
                )
                return
            set_admin_wizard_state(message.from_user.id, "ads", "await_button_url", {**data, "button_text": text})
            await message.reply_text("Step 3/3: Send button URL (https://...), or type `skip`.")
            return
        if step == "await_button_url":
            button_url = "" if text.lower() == "skip" else text
            if button_url and not is_valid_http_url(button_url):
                await message.reply_text("❌ URL must start with http:// or https://. Send `skip` to continue without a button URL.")
                return
            preview_data = {**data, "button_url": button_url}
            set_admin_wizard_state(message.from_user.id, "ads", "preview", preview_data)
            preview_text = (
                "📣 **Ad Preview**\n\n"
                f"📝 {preview_data.get('message', '')}\n"
                f"🔘 {preview_data.get('button_text', 'No button')}\n"
                f"🔗 {preview_data.get('button_url', 'No URL')}"
            )
            await message.reply_text(
                preview_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Publish", callback_data="wiz_ads_publish")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")]
                ])
            )
            return

    if flow == "settings" and step == "await_welcome_message":
        text = (message.text or message.caption or "").strip()
        if text.lower() == "skip":
            clear_admin_wizard_state(message.from_user.id)
            await message.reply_text("Cancelled.")
            return
        if not text:
            await message.reply_text("❌ Welcome message cannot be empty.")
            return
        await db.set_welcome_message(text)
        await log_admin_action(message.from_user.id, "set_welcome")
        clear_admin_wizard_state(message.from_user.id)
        await message.reply_text(
            "✅ Welcome message updated.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔧 Open Settings", callback_data="admin_settings")]])
        )
        return

    if flow == "fsub":
        text = (message.text or message.caption or "").strip()
        if step == "await_channel_ref":
            try:
                resolved = await resolve_fsub_channel(client, text)
            except Exception as e:
                await message.reply_text(
                    f"❌ {e}\n\nSend a valid chat ID, @username, or t.me link."
                )
                return
            if not resolved.get("is_admin"):
                await message.reply_text(
                    "❌ Bot is not admin in this channel.\n"
                    "Force Sub requires bot admin rights to check membership."
                )
                return
            set_admin_wizard_state(
                message.from_user.id,
                "fsub",
                "await_invite_settings",
                {
                    "channel_id": int(resolved["id"]),
                    "channel_name": resolved["name"],
                    "input_type": resolved.get("input_type", "unknown"),
                    "input_value": resolved.get("input_value", text),
                    "chat_invite_link": resolved.get("chat_invite_link", "")
                }
            )
            await message.reply_text(
                "🔗 Step 2/3: Send invite settings as `days member_limit`.\n"
                "Examples: `7 100`, `0 0`, or `skip`."
            )
            return
        if step == "await_invite_settings":
            days = 0
            member_limit = 0
            if text.lower() != "skip":
                parts = text.split()
                if len(parts) > 2:
                    await message.reply_text("❌ Use format: `days member_limit` or `skip`.")
                    return
                try:
                    days = max(0, int(parts[0])) if len(parts) >= 1 else 0
                    member_limit = max(0, int(parts[1])) if len(parts) == 2 else 0
                except ValueError:
                    await message.reply_text("❌ Days/member_limit must be numeric.")
                    return
            channel_id = int(data["channel_id"])
            channel_link = await create_fsub_invite_link(client, channel_id, days=days, member_limit=member_limit)
            if not channel_link:
                channel_link = data.get("chat_invite_link", "")
            preview_data = {
                **data,
                "channel_link": channel_link,
                "invite_days": days,
                "invite_member_limit": member_limit,
                "verified": True,
                "verify_error": ""
            }
            set_admin_wizard_state(message.from_user.id, "fsub", "preview", preview_data)
            await message.reply_text(
                "🔐 **FSub Preview**\n\n"
                f"📢 {preview_data.get('channel_name')}\n"
                f"🆔 `{preview_data.get('channel_id')}`\n"
                f"🔎 Input: `{preview_data.get('input_type', 'manual')}`\n"
                f"🔗 Invite: {preview_data.get('channel_link') or 'Auto (not available)'}\n"
                f"⏳ Expiry Days: `{preview_data.get('invite_days', 0)}`\n"
                f"👥 Join Limit: `{preview_data.get('invite_member_limit', 0)}`\n"
                "✅ Verify: Passed",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Save", callback_data="wiz_fsub_save")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="wiz_cancel")]
                ])
            )
            return

# ----- STATS -----
@app.on_callback_query(filters.regex("^admin_stats_detail$"))
@admin_only
async def admin_stats_detail_callback(client: Client, callback: CallbackQuery):
    stats = await db.get_bot_stats()
    enforcement = stats.get("enforcement", {})
    
    text = (
        "📊 **Detailed Statistics**\n\n"
        f"👥 **Total Users:** {stats['total_users']}\n"
        f"🚫 **Banned Users:** {stats['banned_users']}\n"
        f"📢 **FSub Channels:** {stats['fsub_channels']}\n"
        f"🔐 **Default FSub Seed:** {DEFAULT_FSUB_CHANNEL or 'Not set'}\n"
        f"🛡 **Enforcement Mode:** {stats.get('enforcement_mode', 'normal').upper()}\n"
        f"🔎 **Checks / Fails / Revoked:** {enforcement.get('checks', 0)} / {enforcement.get('failed_checks', 0)} / {enforcement.get('revoked_access', 0)}\n"
        f"📤 **Total Uploads:** {stats['total_uploads']}\n"
        f"💾 **Total Data:** {human_readable_size(stats['total_size'])}\n"
        f"📅 **Bot Started:** {stats['start_time'][:10]}\n\n"
        "📈 Use **Analytics** panel for daily/weekly/monthly/yearly trends."
    )
    
    buttons = [
        [InlineKeyboardButton("📈 Analytics", callback_data="admin_analytics")],
        [InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

def format_analytics_block(title: str, data: dict) -> str:
    return (
        f"**{title}**\n"
        f"• Active Users: {data.get('active_users', 0)}\n"
        f"• New Users: {data.get('new_users', 0)}\n"
        f"• Uploads: {data.get('uploads', 0)}\n"
        f"• Data Uploaded: {human_readable_size(data.get('uploaded_size', 0))}\n"
        f"• Commands Used: {data.get('commands', 0)}\n"
    )

@app.on_message(filters.command("analytics") & filters.private)
@admin_only
async def analytics_command(client: Client, message: Message):
    analytics = await db.get_analytics_summary()
    dashboard_url = ""
    if WEB_BASE_URL and ADMIN_DASHBOARD_TOKEN:
        dashboard_url = f"{WEB_BASE_URL}/admin/dashboard?token={ADMIN_DASHBOARD_TOKEN}"
    text = (
        "📈 **Admin Analytics Panel**\n\n"
        f"{format_analytics_block('Today (DAU)', analytics['daily'])}\n"
        f"{format_analytics_block('Last 7 Days (WAU)', analytics['weekly'])}\n"
        f"{format_analytics_block('Last 30 Days (MAU)', analytics['monthly'])}\n"
        f"{format_analytics_block('Last 365 Days (YAU)', analytics['yearly'])}\n"
        f"{'🌐 Dashboard: ' + dashboard_url if dashboard_url else '⚠️ Set WEB_BASE_URL and ADMIN_DASHBOARD_TOKEN to enable web dashboard.'}"
    )
    buttons = []
    if dashboard_url:
        buttons.append([InlineKeyboardButton("🌐 Open Web Dashboard", url=dashboard_url)])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

@app.on_message(filters.command("usernamefile") & filters.private)
@admin_only
async def username_export_file_command(client: Client, message: Message):
    try:
        file_path = await db.get_username_export_file_path()
        if not file_path or not os.path.exists(file_path):
            await message.reply_text("❌ Username export file is not available yet.")
            return
        await message.reply_document(
            file_path,
            caption="📄 Latest username export snapshot"
        )
    except Exception as e:
        logger.error(f"Failed to send username export file: {e}")
        await message.reply_text("❌ Failed to fetch username export file right now.")

@app.on_callback_query(filters.regex("^admin_analytics$"))
@admin_only
async def admin_analytics_callback(client: Client, callback: CallbackQuery):
    analytics = await db.get_analytics_summary()
    dashboard_url = ""
    if WEB_BASE_URL and ADMIN_DASHBOARD_TOKEN:
        dashboard_url = f"{WEB_BASE_URL}/admin/dashboard?token={ADMIN_DASHBOARD_TOKEN}"
    text = (
        "📈 **Admin Analytics Panel**\n\n"
        f"{format_analytics_block('Today (DAU)', analytics['daily'])}\n"
        f"{format_analytics_block('Last 7 Days (WAU)', analytics['weekly'])}\n"
        f"{format_analytics_block('Last 30 Days (MAU)', analytics['monthly'])}\n"
        f"{format_analytics_block('Last 365 Days (YAU)', analytics['yearly'])}\n"
        "Use `/analytics` anytime for a fresh report.\n"
        f"{'🌐 Dashboard enabled.' if dashboard_url else '⚠️ WEB_BASE_URL + ADMIN_DASHBOARD_TOKEN not configured.'}"
    )
    buttons = []
    if dashboard_url:
        buttons.append([InlineKeyboardButton("🌐 Open Web Dashboard", url=dashboard_url)])
    buttons.append([InlineKeyboardButton("🛡 Safety Logs", callback_data="admin_safety_logs:0")])
    buttons.append([InlineKeyboardButton("🧭 Guide", callback_data="admin_guide")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
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

@app.on_message(filters.text & filters.private & ~filters.command(ADMIN_TEXT_COMMANDS))
async def url_handler(client: Client, message: Message):
    if message.from_user and await is_admin(message.from_user.id) and get_admin_wizard_state(message.from_user.id):
        return
    text = message.text.strip()
    
    if not (text.startswith("http://") or text.startswith("https://")):
        return

    # Force subscribe check
    if not await force_sub_check(client, message):
        return

    try:
        parsed_url = urlsplit(text)
        sanitized_url = urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
        await db.log_user_event(
            message.from_user.id,
            "url_request",
            chat_id=message.chat.id,
            metadata={"url": sanitized_url[:500]}
        )
    except Exception as e:
        logger.error(f"Failed to log URL request event: {e}")

    # 1. IMMEDIATE BACKUP
    await immediate_backup(client, message, is_url=True, url_text=text)

    msg = await message.reply_text(
        "🔗 **URL Detected!**\n\n"
        "🚀 Queued for High-Speed Processing...\n"
        "⏳ Please wait..."
    )
    if shutdown_in_progress:
        await msg.edit_text("⚠️ Bot is restarting. Please send your request again in a moment.")
        return
    await download_queue.put(("url", text, message, msg))

# ================== FILE HANDLING ==================

@app.on_message((filters.document | filters.video | filters.audio | filters.photo) & filters.private)
async def file_handler(client: Client, message: Message):
    if message.chat.id == BACKUP_CHANNEL_ID:
        return
    if message.from_user and await is_admin(message.from_user.id) and get_admin_wizard_state(message.from_user.id):
        return

    # Force subscribe check
    if not await force_sub_check(client, message):
        return

    try:
        media = message.document or message.video or message.audio or message.photo
        await db.log_user_event(
            message.from_user.id,
            "file_request",
            chat_id=message.chat.id,
            metadata={
                "file_name": getattr(media, "file_name", "file"),
                "file_size": getattr(media, "file_size", 0)
            }
        )
    except Exception as e:
        logger.error(f"Failed to log file request event: {e}")

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
    if shutdown_in_progress:
        await msg.edit_text("⚠️ Bot is restarting. Please send your file again in a moment.")
        return
    await download_queue.put(("file", media, message, msg))

# ================== QUEUE PROCESSOR ==================

async def queue_worker(client: Client, worker_number: int):
    while True:
        queued_task = await download_queue.get()
        if queued_task is None:
            download_queue.task_done()
            break

        type_ = queued_task[0]

        try:
            if type_ == "file":
                await process_tg_file(client, *queued_task[1:])
            elif type_ == "url":
                await process_url_file(client, *queued_task[1:])
        except Exception as e:
            logger.error(f"Queue Worker {worker_number} Error: {e}")
            try:
                await queued_task[3].edit_text(f"❌ **Error:**\n`{str(e)}`")
            except:
                pass
        finally:
            download_queue.task_done()

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
        try:
            await db.log_user_event(
                message.from_user.id,
                "upload_complete",
                chat_id=message.chat.id,
                metadata={
                    "file_name": file_name,
                    "file_size": file_size,
                    "source": source,
                    "link": link
                }
            )
        except Exception as e:
            logger.error(f"Failed to log upload completion event: {e}")

        # ================== 1. USER RESPONSE ==================
        user_text = (
            f"✅ **Upload Complete!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** `{human_readable_size(file_size)}`\n"
            f"📥 **Source:** {source}\n\n"
            f"🔗 **Download Link:**\n{link}\n\n"
            f"🔹**Powered By : @TOOLS_BOTS_KING **🔸"
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

def dashboard_access_granted(request) -> bool:
    token = request.query.get("token", "")
    cookie_token = request.cookies.get("admin_dash_token", "")
    if not ADMIN_DASHBOARD_TOKEN:
        return False
    return token == ADMIN_DASHBOARD_TOKEN or cookie_token == ADMIN_DASHBOARD_TOKEN

async def admin_dashboard_data_handler(request):
    if not dashboard_access_granted(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

    summary = await db.get_analytics_summary()
    daily_series = await db.get_recent_daily_analytics(days=30)
    storage_summary = await db.get_user_storage_summary()
    bot_stats = await db.get_bot_stats()

    return web.json_response({
        "ok": True,
        "summary": summary,
        "series_30d": daily_series,
        "storage": storage_summary,
        "bot_stats": bot_stats
    })

def build_dashboard_html() -> str:
    safe_data_url = "/admin/dashboard/data"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>GOFILE BOT - Admin Analytics</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b1020; color:#e9edf7; margin:0; }}
    .wrap {{ max-width:1100px; margin:0 auto; padding:20px; }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; }}
    .card {{ background:#151d35; border:1px solid #293252; border-radius:10px; padding:14px; }}
    h1,h2 {{ margin:8px 0 14px; }}
    table {{ width:100%; border-collapse:collapse; background:#151d35; border:1px solid #293252; border-radius:10px; overflow:hidden; }}
    th,td {{ padding:10px; border-bottom:1px solid #293252; text-align:left; font-size:14px; }}
    .bar {{ height:10px; background:#2c3a63; border-radius:8px; overflow:hidden; }}
    .fill {{ height:10px; background:#33c27f; }}
    .muted {{ color:#9fb0dd; font-size:13px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>📊 GOFILE BOT - Admin Dashboard</h1>
    <p class="muted">Production analytics and detailed user-storage overview.</p>
    <div id="cards" class="cards"></div>
    <h2>📈 Last 30 Days Activity</h2>
    <table>
      <thead>
        <tr><th>Date</th><th>Active</th><th>New</th><th>Uploads</th><th>Commands</th><th>Uploaded Data</th></tr>
      </thead>
      <tbody id="tableBody"></tbody>
    </table>
    <h2>📉 Activity Chart (Uploads)</h2>
    <div id="chart"></div>
  </div>
  <script>
    const dataUrl = "{safe_data_url}";
    const formatBytes = (bytes) => {{
      let n = Number(bytes || 0), units = ['B','KB','MB','GB','TB'], i = 0;
      while (n >= 1024 && i < units.length - 1) {{ n /= 1024; i++; }}
      return `${{n.toFixed(2)}} ${{units[i]}}`;
    }};
    fetch(dataUrl).then(r => r.json()).then(payload => {{
      if (!payload.ok) throw new Error(payload.error || 'Failed to load dashboard');
      const s = payload.summary || {{}};
      const storage = payload.storage || {{}};
      const cards = [
        ['DAU', s.daily?.active_users ?? 0],
        ['WAU', s.weekly?.active_users ?? 0],
        ['MAU', s.monthly?.active_users ?? 0],
        ['YAU', s.yearly?.active_users ?? 0],
        ['Users Stored', storage.total_users ?? 0],
        ['Event Logs', storage.global_event_log_size ?? 0],
        ['Username Export', storage.username_export_file || 'N/A'],
        ['Last Export', storage.last_username_export_at || 'N/A']
      ];
      document.getElementById('cards').innerHTML = cards.map(c =>
        `<div class="card"><div class="muted">${{c[0]}}</div><div style="font-size:22px;font-weight:700;margin-top:6px;">${{c[1]}}</div></div>`
      ).join('');

      const rows = payload.series_30d || [];
      const maxUploads = Math.max(1, ...rows.map(r => r.uploads || 0));
      document.getElementById('tableBody').innerHTML = rows.map(r => `
        <tr>
          <td>${{r.date}}</td>
          <td>${{r.active_users}}</td>
          <td>${{r.new_users}}</td>
          <td>${{r.uploads}}</td>
          <td>${{r.commands}}</td>
          <td>${{formatBytes(r.uploaded_size)}}</td>
        </tr>`).join('');
      document.getElementById('chart').innerHTML = rows.map(r => `
        <div style="margin:8px 0;">
          <div class="muted">${{r.date}} - uploads: ${{r.uploads}}</div>
          <div class="bar"><div class="fill" style="width:${{Math.max(2, (r.uploads / maxUploads) * 100)}}%"></div></div>
        </div>`).join('');
    }}).catch(err => {{
      document.body.innerHTML = '<pre style="padding:20px;color:#fff;background:#170b0b">Dashboard error: ' + err.message + '</pre>';
    }});
  </script>
</body>
</html>"""

async def admin_dashboard_handler(request):
    if not ADMIN_DASHBOARD_TOKEN:
        return web.Response(
            text="ADMIN_DASHBOARD_TOKEN is not configured. Set it in environment to enable dashboard.",
            status=503,
            content_type="text/plain"
        )
    if not dashboard_access_granted(request):
        return web.Response(text="Unauthorized", status=401, content_type="text/plain")

    response = web.Response(text=build_dashboard_html(), content_type="text/html")
    if request.query.get("token") == ADMIN_DASHBOARD_TOKEN:
        response.set_cookie(
            "admin_dash_token",
            ADMIN_DASHBOARD_TOKEN,
            httponly=True,
            secure=True,
            samesite="Strict",
            path="/admin",
            max_age=86400
        )
    return response

async def start_web():
    appw = web.Application()
    appw.router.add_get("/", web_handler)
    appw.router.add_get("/admin/dashboard", admin_dashboard_handler)
    appw.router.add_get("/admin/dashboard/data", admin_dashboard_data_handler)
    runner = web.AppRunner(appw)
    await runner.setup()
    await web.TCPSite(
        runner, "0.0.0.0",
        int(os.environ.get("PORT", 8080))
    ).start()

# ================== MAIN EXECUTION ==================

async def main():
    global shutdown_in_progress
    print("🤖 Bot Starting with uvloop optimization...")
    await db.get_username_export_file_path()
    await app.start()
    await ensure_default_fsub_channel(app)
    for i in range(MAX_CONCURRENT_QUEUE_WORKERS):
        queue_worker_tasks.append(asyncio.create_task(queue_worker(app, i)))
    print(f"⚙️ Started {MAX_CONCURRENT_QUEUE_WORKERS} concurrent queue workers.")
    print("✅ Bot Connected to Telegram")
    print("🌍 Starting Web Server...")
    await start_web()
    print("🚀 High Speed Pipeline Ready. Waiting for requests.")
    await idle()
    shutdown_in_progress = True
    await download_queue.join()
    for _ in queue_worker_tasks:
        await download_queue.put(None)
    await asyncio.gather(*queue_worker_tasks, return_exceptions=True)
    await app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.run_until_complete(main())

