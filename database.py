#!/usr/bin/env python3
import json
import os
import asyncio
from datetime import datetime, timedelta
from config import DATABASE_FILE, REQUIRED_FSUB_CHANNELS
import logging

logger = logging.getLogger(__name__)
MAX_USER_EVENTS_PER_USER = 200
MAX_GLOBAL_USER_EVENTS = 20000

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
                "start_time": datetime.now().isoformat(),
                "username_export_file": "",
                "last_username_export_at": ""
            },
            "settings": {
                "fsub_enabled": True,
                "maintenance_mode": False,
                "welcome_message": "",
                "enforcement_mode": "normal"
            },
            "analytics": {
                "daily": {}
            },
            "enforcement": {
                "checks": 0,
                "failed_checks": 0,
                "revoked_access": 0,
                "last_revoked_at": "",
                "last_revoked_user": 0
            },
            "user_events": []
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
                    if "username_export_file" not in loaded.get("bot_stats", {}):
                        loaded["bot_stats"]["username_export_file"] = ""
                    if "last_username_export_at" not in loaded.get("bot_stats", {}):
                        loaded["bot_stats"]["last_username_export_at"] = ""
                    if "user_events" not in loaded:
                        loaded["user_events"] = []
                    settings = loaded.get("settings", {})
                    if "enforcement_mode" not in settings:
                        settings["enforcement_mode"] = "normal"
                    loaded["settings"] = settings
                    if "enforcement" not in loaded:
                        loaded["enforcement"] = default_data["enforcement"]
                    else:
                        for key, value in default_data["enforcement"].items():
                            if key not in loaded["enforcement"]:
                                loaded["enforcement"][key] = value
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
    
    async def add_user(self, user_id: int, user_info: dict, chat_id: int = None, source: str = "unknown", persist: bool = True):
        """Add or update user"""
        user_id = str(user_id)
        now_iso = datetime.now().isoformat()
        now_unix = int(datetime.now().timestamp())
        is_new_user = user_id not in self.data["users"]
        profile_changed = False

        if is_new_user:
            self.data["users"][user_id] = {
                "user_id": int(user_id),
                "first_name": user_info.get("first_name", ""),
                "last_name": user_info.get("last_name", ""),
                "username": user_info.get("username", ""),
                "language_code": user_info.get("language_code", ""),
                "is_bot": bool(user_info.get("is_bot", False)),
                "is_premium": bool(user_info.get("is_premium", False)),
                "is_verified": bool(user_info.get("is_verified", False)),
                "is_scam": bool(user_info.get("is_scam", False)),
                "is_fake": bool(user_info.get("is_fake", False)),
                "chat_id": chat_id if chat_id is not None else int(user_id),
                "chat_ids": [chat_id] if chat_id is not None else [int(user_id)],
                "usernames_history": [user_info.get("username")] if user_info.get("username") else [],
                "last_seen_source": source,
                "joined_date": now_iso,
                "last_active": now_iso,
                "created_unix": now_unix,
                "last_active_unix": now_unix,
                "uploads_count": 0,
                "total_size": 0,
                "events": [],
                "events_count": 0,
                "commands_count": 0,
                "url_requests_count": 0,
                "file_requests_count": 0
            }
            profile_changed = True
        else:
            user_row = self.data["users"][user_id]
            previous_username = user_row.get("username", "")
            current_username = user_info.get("username", "")

            user_row["last_active"] = now_iso
            user_row["last_active_unix"] = now_unix
            user_row["first_name"] = user_info.get("first_name", "")
            user_row["last_name"] = user_info.get("last_name", "")
            user_row["username"] = current_username
            user_row["language_code"] = user_info.get("language_code", "")
            user_row["is_bot"] = bool(user_info.get("is_bot", False))
            user_row["is_premium"] = bool(user_info.get("is_premium", False))
            user_row["is_verified"] = bool(user_info.get("is_verified", False))
            user_row["is_scam"] = bool(user_info.get("is_scam", False))
            user_row["is_fake"] = bool(user_info.get("is_fake", False))
            user_row["last_seen_source"] = source

            if chat_id is not None:
                user_row["chat_id"] = chat_id
                chat_ids = user_row.setdefault("chat_ids", [])
                if chat_id not in chat_ids:
                    chat_ids.append(chat_id)
                    profile_changed = True

            if current_username and current_username not in user_row.setdefault("usernames_history", []):
                user_row["usernames_history"].append(current_username)
                profile_changed = True
            if previous_username != current_username:
                profile_changed = True

        await self.track_activity(int(user_id), event_type="activity", is_new_user=is_new_user, persist=False)
        if is_new_user or profile_changed:
            self._write_username_snapshot()
        if persist:
            await self._save_db()
    
    def _write_username_snapshot(self):
        """Write Username_{totalusername}.txt snapshot"""
        users = self.data.get("users", {})
        total = len(users)
        db_dir = os.path.dirname(self.db_file) or "."
        filename = f"username_{total}.txt"
        path = os.path.join(db_dir, filename)

        lines = [
            f"username snapshot generated at {datetime.now().isoformat()}",
            f"total_users={total}",
            ""
        ]

        for _, user in sorted(users.items(), key=lambda item: item[0]):
            lines.append(
                "|".join([
                    str(user.get("username", "") or "None"),
                    str(user.get("user_id", "")),
                    str(user.get("chat_id", "")),
                    str(user.get("first_name", "") or ""),
                    str(user.get("last_name", "") or ""),
                    str(user.get("joined_date", "") or ""),
                    str(user.get("last_active", "") or "")
                ])
            )

        temp_path = f"{path}.tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"Failed writing username snapshot file {filename}: {e}")
            return

        old_file = self.data["bot_stats"].get("username_export_file")
        safe_old_file = os.path.basename(old_file) if old_file else ""
        old_file_is_safe = bool(old_file) and (safe_old_file == old_file)
        if old_file_is_safe and safe_old_file != filename:
            old_path = os.path.join(db_dir, safe_old_file)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                except Exception as e:
                    logger.warning(f"Could not remove old username export {safe_old_file}: {e}")

        self.data["bot_stats"]["username_export_file"] = filename
        self.data["bot_stats"]["last_username_export_at"] = datetime.now().isoformat()

    async def log_user_event(self, user_id: int, event_type: str, chat_id: int = None, metadata: dict = None, persist: bool = True):
        """Store detailed user events for audit and analytics."""
        metadata = metadata or {}
        user_key = str(user_id)
        now_iso = datetime.now().isoformat()

        event = {
            "event_type": event_type,
            "user_id": int(user_id),
            "chat_id": chat_id if chat_id is not None else int(user_id),
            "timestamp": now_iso,
            "metadata": metadata
        }

        user_data = self.data["users"].get(user_key)
        if user_data:
            user_events = user_data.setdefault("events", [])
            user_events.append(event)
            if len(user_events) > MAX_USER_EVENTS_PER_USER:
                user_events[:] = user_events[-MAX_USER_EVENTS_PER_USER:]
            user_data["events_count"] = int(user_data.get("events_count", 0)) + 1
            user_data["last_active"] = now_iso
            user_data["last_active_unix"] = int(datetime.now().timestamp())

            if event_type == "command":
                user_data["commands_count"] = int(user_data.get("commands_count", 0)) + 1
            elif event_type == "url_request":
                user_data["url_requests_count"] = int(user_data.get("url_requests_count", 0)) + 1
            elif event_type == "file_request":
                user_data["file_requests_count"] = int(user_data.get("file_requests_count", 0)) + 1

        global_events = self.data.setdefault("user_events", [])
        global_events.append(event)
        if len(global_events) > MAX_GLOBAL_USER_EVENTS:
            global_events[:] = global_events[-MAX_GLOBAL_USER_EVENTS:]

        if event_type == "command":
            await self.track_activity(int(user_id), event_type="command", persist=False)

        if persist:
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

    async def get_enforcement_mode(self):
        """Get current enforcement mode."""
        mode = self.data["settings"].get("enforcement_mode", "normal")
        if mode not in ("normal", "aggressive"):
            mode = "normal"
        return mode

    async def set_enforcement_mode(self, mode: str):
        """Set enforcement mode: normal/aggressive."""
        mode = (mode or "normal").lower().strip()
        if mode not in ("normal", "aggressive"):
            mode = "normal"
        self.data["settings"]["enforcement_mode"] = mode
        await self._save_db()

    async def record_enforcement_check(self, passed: bool, revoked: bool = False, user_id: int = 0, persist: bool = True):
        """Track force-subscription enforcement metrics."""
        enforcement = self.data.setdefault("enforcement", {
            "checks": 0,
            "failed_checks": 0,
            "revoked_access": 0,
            "last_revoked_at": "",
            "last_revoked_user": 0
        })
        enforcement["checks"] = int(enforcement.get("checks", 0)) + 1
        if not passed:
            enforcement["failed_checks"] = int(enforcement.get("failed_checks", 0)) + 1
        if revoked:
            enforcement["revoked_access"] = int(enforcement.get("revoked_access", 0)) + 1
            enforcement["last_revoked_at"] = datetime.now().isoformat()
            enforcement["last_revoked_user"] = int(user_id or 0)
        if persist:
            await self._save_db()

    async def get_enforcement_stats(self):
        """Get enforcement metrics summary."""
        enforcement = self.data.get("enforcement", {})
        return {
            "checks": int(enforcement.get("checks", 0)),
            "failed_checks": int(enforcement.get("failed_checks", 0)),
            "revoked_access": int(enforcement.get("revoked_access", 0)),
            "last_revoked_at": enforcement.get("last_revoked_at", ""),
            "last_revoked_user": int(enforcement.get("last_revoked_user", 0)),
            "mode": await self.get_enforcement_mode()
        }
    
    # ================== STATS ==================
    
    async def get_bot_stats(self):
        """Get bot statistics"""
        return {
            "total_users": len(self.data["users"]),
            "banned_users": len(self.data["banned_users"]),
            "fsub_channels": len(self.data["fsub_channels"]),
            "total_uploads": self.data["bot_stats"]["total_uploads"],
            "total_size": self.data["bot_stats"]["total_size_uploaded"],
            "start_time": self.data["bot_stats"]["start_time"],
            "enforcement_mode": await self.get_enforcement_mode(),
            "enforcement": await self.get_enforcement_stats()
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

    async def get_recent_daily_analytics(self, days: int = 30):
        """Get per-day analytics series for dashboard charts/tables."""
        days = max(1, min(365, int(days)))
        today = datetime.now().date()
        daily = self.data.get("analytics", {}).get("daily", {})
        series = []

        for i in range(days - 1, -1, -1):
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            day = daily.get(d, {})
            series.append({
                "date": d,
                "active_users": len(day.get("active_users", [])),
                "new_users": day.get("new_users", 0),
                "uploads": day.get("uploads", 0),
                "uploaded_size": day.get("uploaded_size", 0),
                "commands": day.get("commands", 0)
            })
        return series

    async def get_user_storage_summary(self):
        """Summarize detailed user storage health for admin dashboard."""
        users = self.data.get("users", {})
        with_username = 0
        with_language = 0
        premium_count = 0
        total_events = 0

        for user in users.values():
            if user.get("username"):
                with_username += 1
            if user.get("language_code"):
                with_language += 1
            if user.get("is_premium"):
                premium_count += 1
            total_events += int(user.get("events_count", 0))

        return {
            "total_users": len(users),
            "with_username": with_username,
            "with_language": with_language,
            "premium_users": premium_count,
            "stored_events": total_events,
            "global_event_log_size": len(self.data.get("user_events", [])),
            "username_export_file": self.data.get("bot_stats", {}).get("username_export_file", ""),
            "last_username_export_at": self.data.get("bot_stats", {}).get("last_username_export_at", "")
        }

    async def get_username_export_file_path(self):
        """Return absolute path to the latest username export file."""
        filename = self.data.get("bot_stats", {}).get("username_export_file", "")
        if not filename:
            self._write_username_snapshot()
            filename = self.data.get("bot_stats", {}).get("username_export_file", "")
        db_dir = os.path.dirname(self.db_file) or "."
        return os.path.abspath(os.path.join(db_dir, filename)) if filename else ""

    async def get_recent_user_events(self, limit: int = 20, event_types: list = None):
        """Get recent global events, optionally filtered by event type(s)."""
        limit = max(1, min(200, int(limit)))
        events = self.data.get("user_events", [])
        if event_types:
            allowed = {str(x) for x in event_types}
            filtered = [e for e in events if e.get("event_type") in allowed]
        else:
            filtered = list(events)
        return filtered[-limit:][::-1]

# Global database instance
db = Database()
