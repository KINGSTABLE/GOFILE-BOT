#!/usr/bin/env python3
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, ChatAdminRequired, PeerIdInvalid
from database import db
from config import REQUIRED_FSUB_CHANNELS
import logging

logger = logging.getLogger(__name__)

def get_channel_candidates(channel_id: int) -> list:
    """Try useful channel-id variants for compatibility."""
    candidates = [channel_id]
    if channel_id > 0:
        candidates.append(int(f"-100{channel_id}"))
    if channel_id < -1000000000000:
        trimmed = str(abs(channel_id))[3:]
        if trimmed.isdigit():
            candidates.append(int(trimmed))
    return list(dict.fromkeys(candidates))

async def check_subscription(client: Client, user_id: int, channel_id: int) -> bool:
    """Check if user is subscribed to a channel"""
    for candidate in get_channel_candidates(channel_id):
        try:
            member = await client.get_chat_member(candidate, user_id)
            return member.status not in ["left", "kicked", "banned"]
        except UserNotParticipant:
            return False
        except ChatAdminRequired:
            logger.error(f"Bot is not admin in channel {candidate}; strict fsub is blocking access.")
            return False
        except PeerIdInvalid:
            logger.warning(f"Invalid channel ID variant: {candidate}")
            continue
        except Exception as e:
            logger.error(f"FSub check error for channel {candidate}: {e}")
            continue
    return False

async def check_force_sub(client: Client, user_id: int) -> tuple:
    """
    Check if user is subscribed to all required channels
    Returns: (is_subscribed: bool, missing_channels: list)
    """
    channels = await db.get_fsub_channels()
    mandatory = set(REQUIRED_FSUB_CHANNELS)
    for channel_id in mandatory:
        if not any(ch.get("id") == channel_id for ch in channels):
            channels.append({
                "id": channel_id,
                "name": f"Required Channel {channel_id}",
                "link": ""
            })
    missing_channels = []
    
    for channel in channels:
        channel_id = channel["id"]
        is_subscribed = await check_subscription(client, user_id, channel_id)
        
        if not is_subscribed:
            missing_channels.append(channel)
    
    return len(missing_channels) == 0, missing_channels

async def get_invite_links(client: Client, channels: list) -> list:
    """Get invite links for channels"""
    links = []
    
    for channel in channels:
        try:
            if channel.get("link"):
                links.append({
                    "name": channel.get("name", "Channel"),
                    "link": channel["link"]
                })
            else:
                # Try to get invite link
                try:
                    resolved = None
                    for candidate in get_channel_candidates(channel["id"]):
                        try:
                            chat = await client.get_chat(candidate)
                            if chat.invite_link:
                                resolved = {
                                    "name": chat.title or channel.get("name", "Channel"),
                                    "link": chat.invite_link
                                }
                                break
                            invite = await client.export_chat_invite_link(candidate)
                            resolved = {
                                "name": chat.title or channel.get("name", "Channel"),
                                "link": invite
                            }
                            break
                        except Exception:
                            continue

                    if resolved:
                        links.append(resolved)
                    else:
                        raise ValueError("Could not resolve invite link from candidates")
                except Exception as e:
                    logger.error(f"Could not get invite link for {channel['id']}: {e}")
                    links.append({
                        "name": channel.get("name", "Channel"),
                        "link": "https://t.me"
                    })
        except Exception as e:
            logger.error(f"Error getting invite link: {e}")
    
    return links

def get_fsub_keyboard(missing_channels: list, invite_links: list) -> InlineKeyboardMarkup:
    """Generate keyboard for force subscribe"""
    buttons = []
    
    for i, link in enumerate(invite_links):
        buttons.append([
            InlineKeyboardButton(
                f"🔔 Join {link['name']}",
                url=link["link"]
            )
        ])
    
    # Add verify button
    buttons.append([
        InlineKeyboardButton(
            "✅ I've Joined All Channels",
            callback_data="check_fsub"
        )
    ])
    
    return InlineKeyboardMarkup(buttons)

def get_fsub_message(missing_count: int) -> str:
    """Generate force subscribe message"""
    messages = [
        "🚫 **Access Blocked!**\n\n",
        f"⚠️ You must join **{missing_count}** required channel(s) before using this bot.\n\n",
        "🔐 **Why Join?**\n",
        "• Get latest updates & features\n",
        "• Support our community\n",
        "• Unlock full bot access\n\n",
        "👇 **Join first, then tap verify:**"
    ]
    
    return "".join(messages)

# Cheeky messages for users trying to bypass
BYPASS_MESSAGES = [
    "😏 **Nice try buddy!** But you still need to join the channels!",
    "🤨 **Smart, huh?** Join the channels first, then we'll talk!",
    "🧠 **Big brain moment!** But I'm smarter. Join the channels!",
    "😤 **You thought!** No shortcut here. Join the channels!",
    "🙄 **Really?** Just join the channels, it's free!",
    "🤔 **Trying to be clever?** Join first, use later!",
    "😒 **Not so fast!** Channels first, bot second!",
    "🎭 **The audacity!** Join the channels already!",
]

LEFT_CHANNEL_MESSAGES = [
    "😱 **Oops!** Looks like you left a channel! Rejoin to continue!",
    "🏃 **Running away?** Come back and join all channels!",
    "😢 **Why did you leave?** Join again to use the bot!",
    "🚪 **You left?** No worries, just join again!",
    "🔄 **Round 2!** Please rejoin all channels!",
]

import random

def get_random_bypass_message():
    return random.choice(BYPASS_MESSAGES)

def get_random_left_message():
    return random.choice(LEFT_CHANNEL_MESSAGES)
