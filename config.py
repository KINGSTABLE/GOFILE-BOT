#!/usr/bin/env python3
import os

# ================== CONFIGURATION ==================

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GOFILE_API_TOKEN = os.environ.get("GOFILE_API_TOKEN", "")
GOFILE_FOLDER_ID = os.environ.get("GOFILE_FOLDER_ID", "")

# Helper to fix Channel IDs
def sanitize_channel_id(value):
    try:
        val = int(value)
        if val > 0 and str(val).startswith("100") and len(str(val)) >= 13:
            return -val
        return val
    except (ValueError, TypeError):
        return None

BACKUP_CHANNEL_ID = sanitize_channel_id(os.environ.get("BACKUP_CHANNEL_ID"))
LOG_CHANNEL_ID = sanitize_channel_id(os.environ.get("LOG_CHANNEL_ID"))

# Parse admin IDs
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit()]

# Owner ID (First admin or specific)
OWNER_ID = int(os.environ.get("OWNER_ID", ADMIN_IDS[0] if ADMIN_IDS else 0))

# LIMITS
MAX_FILE_SIZE = 50 * 1024 * 1024 * 1024  # 50GB
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB

# GoFile Servers
PRIORITIZED_SERVERS = [
    "upload-na-phx", "upload-ap-sgp", "upload-ap-hkg",
    "upload-ap-tyo", "upload-sa-sao", "upload-eu-fra"
]

HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}
DOWNLOAD_DIR = "downloads"
DATABASE_FILE = "database.json"

# Bot Info
BOT_USERNAME = os.environ.get("BOT_USERNAME", "YourBot")
SUPPORT_CHAT = os.environ.get("SUPPORT_CHAT", "")
UPDATE_CHANNEL = os.environ.get("UPDATE_CHANNEL", "")

# Messages
START_IMG = os.environ.get("START_IMG", "")  # Optional start image URL
