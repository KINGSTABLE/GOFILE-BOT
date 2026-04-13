#!/usr/bin/env python3
import json
import os
import asyncio
from datetime import datetime, timedelta
from config import DATABASE_FILE, REQUIRED_FSUB_CHANNELS
import logging

logger = logging.getLogger(__name__)

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
            },
            "analytics": {
                "daily": {}
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
                    if "daily" not in loaded.get("analytics", {}):
                        loaded["analytics"]["daily"] = {}
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
        now_iso = datetime.now().isoformat()
        is_new_user = user_id not in self.data["users"]

        if is_new_user:
            self.data["users"][user_id] = {
                "user_id": int(user_id),
                "first_name": user_info.get("first_name", ""),
                "username": user_info.get("username", ""),
                "joined_date": now_iso,
                "last_active": now_iso,
                "uploads_count": 0,
                "total_size": 0
            }
        else:
            self.data["users"][user_id]["last_active"] = now_iso
            self.data["users"][user_id]["first_name"] = user_info.get("first_name", "")
            self.data["users"][user_id]["username"] = user_info.get("username", "")

        await self.track_activity(int(user_id), event_type="activity", is_new_user=is_new_user, persist=False)
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
            self.data["users"][user_id]["last_active"] = datetime.now().isoformat()
        
        self.data["bot_stats"]["total_uploads"] += 1
        self.data["bot_stats"]["total_size_uploaded"] += file_size
        await self.track_activity(int(user_id), event_type="upload", upload_size=file_size, persist=False)
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

    async def ensure_required_fsub_channels(self):
        """Ensure required force-subscribe channels exist"""
        existing_ids = {ch.get("id") for ch in self.data["fsub_channels"]}
        changed = False

        for channel_id in REQUIRED_FSUB_CHANNELS:
            if channel_id not in existing_ids:
                self.data["fsub_channels"].append({
                    "id": channel_id,
                    "name": f"Required Channel {channel_id}",
                    "link": "",
                    "added_date": datetime.now().isoformat()
                })
                changed = True

        if changed:
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

    # ================== ANALYTICS ==================

    async def track_activity(self, user_id: int, event_type: str = "activity", upload_size: int = 0, is_new_user: bool = False, persist: bool = True):
        """Track daily usage analytics"""
        date_key = datetime.now().strftime("%Y-%m-%d")
        daily = self.data["analytics"].setdefault("daily", {})
        day_data = daily.setdefault(date_key, {
            "active_users": [],
            "new_users": 0,
            "uploads": 0,
            "uploaded_size": 0,
            "commands": 0
        })

        if user_id not in day_data["active_users"]:
            day_data["active_users"].append(user_id)

        if is_new_user:
            day_data["new_users"] += 1

        if event_type == "upload":
            day_data["uploads"] += 1
            try:
                parsed_size = upload_size if isinstance(upload_size, int) else int(upload_size)
            except (TypeError, ValueError):
                logger.warning(f"Invalid upload size received for analytics: {upload_size}")
                parsed_size = 0

            if parsed_size < 0:
                logger.warning(f"Negative upload size received for analytics: {parsed_size}")
                parsed_size = 0
            day_data["uploaded_size"] += parsed_size
        elif event_type == "command":
            day_data["commands"] += 1

        if persist:
            await self._save_db()

    def _sum_period(self, days: int):
        """Aggregate analytics data for the last `days` days."""
        today = datetime.now().date()
        daily = self.data.get("analytics", {}).get("daily", {})
        result = {
            "active_users": set(),
            "new_users": 0,
            "uploads": 0,
            "uploaded_size": 0,
            "commands": 0,
            "days_with_data": 0
        }

        for i in range(days):
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            data = daily.get(d)
            if not data:
                continue

            result["days_with_data"] += 1
            result["active_users"].update(data.get("active_users", []))
            result["new_users"] += data.get("new_users", 0)
            result["uploads"] += data.get("uploads", 0)
            result["uploaded_size"] += data.get("uploaded_size", 0)
            result["commands"] += data.get("commands", 0)

        result["active_users"] = len(result["active_users"])
        return result

    async def get_analytics_summary(self):
        """Get DAU/WAU/MAU/YAU style analytics summary"""
        return {
            "daily": self._sum_period(1),
            "weekly": self._sum_period(7),
            "monthly": self._sum_period(30),
            "yearly": self._sum_period(365)
        }

# Global database instance
db = Database()
