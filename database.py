#!/usr/bin/env python3
import json
import os
import asyncio
from datetime import datetime
from config import DATABASE_FILE

class Database:
    def __init__(self):
        self.db_file = DATABASE_FILE
        self.lock = asyncio.Lock()
        self.data = self._load_db()
    
    def _load_db(self):
        """Load database from file"""
        default_data = {
            "users": {},
            "fsub_channels": [],
            "banned_users": [],
            "ads": {
                "enabled": False,
                "message": "",
                "button_text": "",
                "button_url": ""
            },
            "bot_stats": {
                "total_uploads": 0,
                "total_size_uploaded": 0,
                "start_time": datetime.now().isoformat()
            },
            "settings": {
                "fsub_enabled": True,
                "maintenance_mode": False,
                "welcome_message": ""
            }
        }
        
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r') as f:
                    loaded = json.load(f)
                    # Merge with defaults to handle missing keys
                    for key in default_data:
                        if key not in loaded:
                            loaded[key] = default_data[key]
                    return loaded
            except:
                return default_data
        return default_data
    
    async def _save_db(self):
        """Save database to file"""
        async with self.lock:
            with open(self.db_file, 'w') as f:
                json.dump(self.data, f, indent=2, default=str)
    
    # ================== USER MANAGEMENT ==================
    
    async def add_user(self, user_id: int, user_info: dict):
        """Add or update user"""
        user_id = str(user_id)
        if user_id not in self.data["users"]:
            self.data["users"][user_id] = {
                "user_id": int(user_id),
                "first_name": user_info.get("first_name", ""),
                "username": user_info.get("username", ""),
                "joined_date": datetime.now().isoformat(),
                "last_active": datetime.now().isoformat(),
                "uploads_count": 0,
                "total_size": 0
            }
        else:
            self.data["users"][user_id]["last_active"] = datetime.now().isoformat()
            self.data["users"][user_id]["first_name"] = user_info.get("first_name", "")
            self.data["users"][user_id]["username"] = user_info.get("username", "")
        
        await self._save_db()
    
    async def get_user(self, user_id: int):
        """Get user data"""
        return self.data["users"].get(str(user_id))
    
    async def get_all_users(self):
        """Get all users"""
        return self.data["users"]
    
    async def get_user_count(self):
        """Get total user count"""
        return len(self.data["users"])
    
    async def update_user_stats(self, user_id: int, file_size: int):
        """Update user upload stats"""
        user_id = str(user_id)
        if user_id in self.data["users"]:
            self.data["users"][user_id]["uploads_count"] += 1
            self.data["users"][user_id]["total_size"] += file_size
        
        self.data["bot_stats"]["total_uploads"] += 1
        self.data["bot_stats"]["total_size_uploaded"] += file_size
        await self._save_db()
    
    # ================== BAN MANAGEMENT ==================
    
    async def ban_user(self, user_id: int):
        """Ban a user"""
        if user_id not in self.data["banned_users"]:
            self.data["banned_users"].append(user_id)
            await self._save_db()
    
    async def unban_user(self, user_id: int):
        """Unban a user"""
        if user_id in self.data["banned_users"]:
            self.data["banned_users"].remove(user_id)
            await self._save_db()
    
    async def is_banned(self, user_id: int):
        """Check if user is banned"""
        return user_id in self.data["banned_users"]
    
    async def get_banned_users(self):
        """Get all banned users"""
        return self.data["banned_users"]
    
    # ================== FSUB CHANNELS ==================
    
    async def add_fsub_channel(self, channel_id: int, channel_name: str = "", channel_link: str = ""):
        """Add force subscribe channel"""
        channel_data = {
            "id": channel_id,
            "name": channel_name,
            "link": channel_link,
            "added_date": datetime.now().isoformat()
        }
        
        # Check if already exists
        for ch in self.data["fsub_channels"]:
            if ch["id"] == channel_id:
                return False
        
        self.data["fsub_channels"].append(channel_data)
        await self._save_db()
        return True
    
    async def remove_fsub_channel(self, channel_id: int):
        """Remove force subscribe channel"""
        initial_len = len(self.data["fsub_channels"])
        self.data["fsub_channels"] = [
            ch for ch in self.data["fsub_channels"] if ch["id"] != channel_id
        ]
        await self._save_db()
        return len(self.data["fsub_channels"]) < initial_len
    
    async def get_fsub_channels(self):
        """Get all force subscribe channels"""
        return self.data["fsub_channels"]
    
    async def is_fsub_enabled(self):
        """Check if force subscribe is enabled"""
        return self.data["settings"]["fsub_enabled"] and len(self.data["fsub_channels"]) > 0
    
    async def toggle_fsub(self, enabled: bool):
        """Enable/Disable force subscribe"""
        self.data["settings"]["fsub_enabled"] = enabled
        await self._save_db()
    
    # ================== ADS MANAGEMENT ==================
    
    async def set_ads(self, enabled: bool, message: str = "", button_text: str = "", button_url: str = ""):
        """Set advertisement"""
        self.data["ads"] = {
            "enabled": enabled,
            "message": message,
            "button_text": button_text,
            "button_url": button_url
        }
        await self._save_db()
    
    async def get_ads(self):
        """Get advertisement data"""
        return self.data["ads"]
    
    async def toggle_ads(self, enabled: bool):
        """Enable/Disable ads"""
        self.data["ads"]["enabled"] = enabled
        await self._save_db()
    
    # ================== SETTINGS ==================
    
    async def set_maintenance(self, enabled: bool):
        """Set maintenance mode"""
        self.data["settings"]["maintenance_mode"] = enabled
        await self._save_db()
    
    async def is_maintenance(self):
        """Check if maintenance mode"""
        return self.data["settings"]["maintenance_mode"]
    
    async def set_welcome_message(self, message: str):
        """Set custom welcome message"""
        self.data["settings"]["welcome_message"] = message
        await self._save_db()
    
    async def get_welcome_message(self):
        """Get custom welcome message"""
        return self.data["settings"].get("welcome_message", "")
    
    # ================== STATS ==================
    
    async def get_bot_stats(self):
        """Get bot statistics"""
        return {
            "total_users": len(self.data["users"]),
            "banned_users": len(self.data["banned_users"]),
            "fsub_channels": len(self.data["fsub_channels"]),
            "total_uploads": self.data["bot_stats"]["total_uploads"],
            "total_size": self.data["bot_stats"]["total_size_uploaded"],
            "start_time": self.data["bot_stats"]["start_time"]
        }

# Global database instance
db = Database()
