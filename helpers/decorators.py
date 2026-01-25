#!/usr/bin/env python3
from functools import wraps
from pyrogram import Client
from pyrogram.types import Message, CallbackQuery
from config import ADMIN_IDS, OWNER_ID
from database import db

def admin_only(func):
    """Decorator to restrict function to admins only"""
    @wraps(func)
    async def wrapper(client: Client, update, *args, **kwargs):
        if isinstance(update, CallbackQuery):
            user_id = update.from_user.id
            reply_func = update.answer
        else:
            user_id = update.from_user.id
            reply_func = update.reply_text
        
        if user_id not in ADMIN_IDS and user_id != OWNER_ID:
            await reply_func(
                "🚫 **Access Denied!**\n\n"
                "This command is for **Admins Only**!",
                show_alert=True if isinstance(update, CallbackQuery) else False
            )
            return
        
        return await func(client, update, *args, **kwargs)
    
    return wrapper

def owner_only(func):
    """Decorator to restrict function to owner only"""
    @wraps(func)
    async def wrapper(client: Client, update, *args, **kwargs):
        if isinstance(update, CallbackQuery):
            user_id = update.from_user.id
            reply_func = update.answer
        else:
            user_id = update.from_user.id
            reply_func = update.reply_text
        
        if user_id != OWNER_ID:
            await reply_func(
                "🚫 **Access Denied!**\n\n"
                "This command is for **Owner Only**!",
                show_alert=True if isinstance(update, CallbackQuery) else False
            )
            return
        
        return await func(client, update, *args, **kwargs)
    
    return wrapper

def not_banned(func):
    """Decorator to check if user is banned"""
    @wraps(func)
    async def wrapper(client: Client, update, *args, **kwargs):
        if isinstance(update, CallbackQuery):
            user_id = update.from_user.id
            reply_func = update.answer
        else:
            user_id = update.from_user.id
            reply_func = update.reply_text
        
        if await db.is_banned(user_id):
            await reply_func(
                "🚫 **You are BANNED!**\n\n"
                "Contact support if you think this is a mistake.",
                show_alert=True if isinstance(update, CallbackQuery) else False
            )
            return
        
        return await func(client, update, *args, **kwargs)
    
    return wrapper