#!/usr/bin/env python3
import asyncio
import logging
from pyrogram import Client
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
from database import db
from datetime import datetime

logger = logging.getLogger(__name__)

class BroadcastStats:
    def __init__(self):
        self.success = 0
        self.failed = 0
        self.blocked = 0
        self.deleted = 0
        self.total = 0
        self.start_time = None
        self.end_time = None

async def broadcast_message(
    client: Client,
    message,
    status_msg,
    forward: bool = False,
    pin: bool = False
):
    """
    Broadcast message to all users
    
    Args:
        client: Pyrogram client
        message: Message to broadcast
        status_msg: Status message to update progress
        forward: Whether to forward or copy message
        pin: Whether to pin message in user's chat
    """
    stats = BroadcastStats()
    stats.start_time = datetime.now()
    
    users = await db.get_all_users()
    stats.total = len(users)
    
    if stats.total == 0:
        await status_msg.edit_text("❌ **No users to broadcast to!**")
        return stats
    
    await status_msg.edit_text(
        f"📡 **Broadcasting Started...**\n\n"
        f"👥 Total Users: `{stats.total}`\n"
        f"⏳ Please wait..."
    )
    
    # Process users in batches to avoid rate limits
    batch_size = 25
    user_ids = list(users.keys())
    
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        
        for user_id in batch:
            try:
                user_id_int = int(user_id)
                
                if forward:
                    sent = await message.forward(user_id_int)
                else:
                    sent = await message.copy(user_id_int)
                
                if pin:
                    try:
                        await sent.pin(disable_notification=True)
                    except:
                        pass
                
                stats.success += 1
                
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    if forward:
                        await message.forward(int(user_id))
                    else:
                        await message.copy(int(user_id))
                    stats.success += 1
                except:
                    stats.failed += 1
                    
            except InputUserDeactivated:
                stats.deleted += 1
                stats.failed += 1
                
            except UserIsBlocked:
                stats.blocked += 1
                stats.failed += 1
                
            except PeerIdInvalid:
                stats.failed += 1
                
            except Exception as e:
                logger.error(f"Broadcast error for {user_id}: {e}")
                stats.failed += 1
        
        # Update progress every batch
        progress = (i + len(batch)) / stats.total * 100
        await status_msg.edit_text(
            f"📡 **Broadcasting...**\n\n"
            f"✅ Success: `{stats.success}`\n"
            f"❌ Failed: `{stats.failed}`\n"
            f"📊 Progress: `{progress:.1f}%`"
        )
        
        # Small delay between batches
        await asyncio.sleep(0.5)
    
    stats.end_time = datetime.now()
    duration = (stats.end_time - stats.start_time).seconds
    
    final_text = (
        f"📡 **Broadcast Completed!**\n\n"
        f"👥 Total Users: `{stats.total}`\n"
        f"✅ Success: `{stats.success}`\n"
        f"❌ Failed: `{stats.failed}`\n"
        f"🚫 Blocked: `{stats.blocked}`\n"
        f"👻 Deleted: `{stats.deleted}`\n"
        f"⏱ Duration: `{duration}s`"
    )
    
    await status_msg.edit_text(final_text)
    
    return stats