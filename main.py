import os
import aiohttp
import asyncio
import mimetypes
import json
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from queue import Queue
from pyrogram.errors import FloodWait

# Pyrogram and API Configuration
API_ID = "29714294"  # Get this from https://my.telegram.org
API_HASH = "bd44a7527bbb8ef23552c569ff3a0d93"  # Get this from https://my.telegram.org
BOT_TOKEN = "7926056695:AAE0Za6llNp4rkIrbYMNLVRd-sZzePayARo"
GOFILE_API_TOKEN = "avoA4ruw3nxglw11NOTR5GzH2bpB5QRe"
BACKUP_CHANNEL_ID = -1002889648510  # Your specified backup channel ID
ADMIN_IDS = [5978396634]  # Replace with actual admin user IDs

# Specify the prioritized servers
PRIORITIZED_SERVERS = [
    "upload-na-phx",  # North America (Phoenix)
    "upload-ap-sgp",  # Asia Pacific (Singapore)
    "upload-ap-hkg",  # Asia Pacific (Hong Kong)
    "upload-ap-tyo",  # Asia Pacific (Tokyo)
    "upload-sa-sao",  # South America (São Paulo)
]
HEADERS = {"Authorization": f"Bearer {GOFILE_API_TOKEN}"}

# File size limit (in bytes) for Gofile.io upload
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

# Initialize Pyrogram Client
app = Client("advanced_gofile_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Initialize queues
download_queue = Queue()
upload_queue = Queue()
processing_queue = asyncio.Lock()

# Ensure directories and files exist
if not os.path.exists("downloads"):
    os.makedirs("downloads")
if not os.path.exists("user.json"):
    with open("user.json", "w") as f:
        json.dump([], f)
if not os.path.exists("user.txt"):
    with open("user.txt", "w") as f:
        f.write("User Upload Log\n==============\n")

# Helper function to save user data
def save_user_data(message: Message, download_link: str, file_type: str, file_size: int):
    user_data = {
        "user_id": message.from_user.id,
        "first_name": message.from_user.first_name,
        "last_name": message.from_user.last_name or "N/A",
        "username": message.from_user.username or "N/A",
        "chat_id": message.chat.id,
        "file_type": file_type,
        "file_size_mb": f"{file_size / (1024 * 1024):.2f}",
        "download_link": download_link,
        "date": datetime.now().isoformat(),
        "caption": message.caption or "N/A"
    }
    with open("user.json", "r+") as f:
        data = json.load(f)
        data.append(user_data)
        f.seek(0)
        json.dump(data, f, indent=4)
    with open("user.txt", "a") as f:
        f.write(
            f"\nUser ID: {user_data['user_id']}\n"
            f"First Name: {user_data['first_name']}\n"
            f"Last Name: {user_data['last_name']}\n"
            f"Username: @{user_data['username']}\n"
            f"Chat ID: {user_data['chat_id']}\n"
            f"File Type: {user_data['file_type']}\n"
            f"File Size: {user_data['file_size_mb']} MB\n"
            f"Download Link: {user_data['download_link']}\n"
            f"Date: {user_data['date']}\n"
            f"Caption: {user_data['caption']}\n"
            f"{'='*20}\n"
        )

# Command handlers (/start, /help, /stats)
@app.on_message(filters.command("start"))
async def start(client: Client, message: Message):
    await message.reply_text(
        "🚀 **Welcome!** Send any file to back it up and get a Gofile.io link (if under 500 MB)."
    )

@app.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    await message.reply_text(
        "📋 **How to use:**\n"
        "1. Send any file (document, video, audio, or photo).\n"
        "2. All files are backed up to a private channel.\n"
        "3. If the file is under 500 MB, it will be uploaded to Gofile.io and you'll get a link.\n\n"
        "**Commands:**\n`/start`, `/help`\n`/stats` (Admin only)"
    )

@app.on_message(filters.command("stats"))
async def stats_command(client: Client, message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply_text("🚫 This command is for admins only.")
        return
    await message.reply_document("user.txt", caption="📊 User Upload Log (user.txt)")
    await message.reply_document("user.json", caption="📊 User Upload Data (user.json)")

# Main file handler
@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_media(client: Client, message: Message):
    file_size, file_type = 0, ""
    if message.document:
        file_size = message.document.file_size
        file_type = "document"
    elif message.video:
        file_size = message.video.file_size
        file_type = "video"
    elif message.audio:
        file_size = message.audio.file_size
        file_type = "audio"
    elif message.photo:
        file_size = message.photo.file_size
        file_type = "photo"

    # Handle files too large for Gofile
    if file_size > MAX_FILE_SIZE:
        try:
            backup_caption = (
                f"⚠️ **Large File Backup** ⚠️\n\n"
                f"This file exceeds the upload limit and has been archived.\n\n"
                f"**👤 User ID:** `{message.from_user.id}`\n"
                f"**🌐 Username:** `@{message.from_user.username or 'N/A'}`\n"
                f"**💾 File Size:** `{file_size / (1024 * 1024):.2f} MB`"
            )
            await message.copy(chat_id=BACKUP_CHANNEL_ID, caption=backup_caption)
        except Exception as e:
            print(f"Error forwarding large file: {e}")
        
        await message.reply_text(
            f"⚠️ It will not be uploaded to Gofile.io as it exceeds the 500 MB limit."
        )
        return

    # Queue valid files for upload
    download_queue.put((message, file_type, file_size))
    await message.reply_text("✅ Your file is in the queue to be uploaded. Please wait.")
    
    asyncio.create_task(process_queue(client))

async def process_queue(client: Client):
    async with processing_queue:
        while not download_queue.empty():
            message, file_type, file_size = download_queue.get()
            await process_file(client, message, file_type, file_size)

async def process_file(client: Client, message: Message, file_type: str, file_size: int):
    status_message = await message.reply_text("📥 Downloading...")
    
    file_name = getattr(message, file_type).file_name if hasattr(getattr(message, file_type), 'file_name') else f"{file_type}_{getattr(message, file_type).file_id}"
    download_path = f"./downloads/{file_name}"

    try:
        await client.download_media(message, file_name=download_path)
        await status_message.edit_text("⬆️ Uploading to Gofile.io...")
        download_link = await upload_file_to_gofile(download_path)

        if download_link:
            await status_message.edit_text(f"✅ **Upload Complete!**\n\nHere is your link:\n{download_link}")
            save_user_data(message, download_link, file_type, file_size)
            
            # --- CREATE AND SEND THE FINAL, STYLED BACKUP MESSAGE ---
            final_caption = (
                f"**File Uploaded Successfully** ✅\n\n"
                f"**👤 User ID:** `{message.from_user.id}`\n"
                f"**📛 First Name:** `{message.from_user.first_name}`\n"
                f"**🌐 Username:** `@{message.from_user.username or 'N/A'}`\n\n"
                f"**🗂️ File Type:** `{file_type}`\n"
                f"**💾 File Size:** `{file_size / (1024 * 1024):.2f} MB`\n"
                f"**📄 Original Caption:** `{message.caption or 'N/A'}`\n\n"
                f"**🔗 Download Link:** {download_link}"
            )

            # Send the correct media type with the final caption
            if file_type == "document":
                await client.send_document(BACKUP_CHANNEL_ID, message.document.file_id, caption=final_caption)
            elif file_type == "video":
                await client.send_video(BACKUP_CHANNEL_ID, message.video.file_id, caption=final_caption)
            elif file_type == "audio":
                await client.send_audio(BACKUP_CHANNEL_ID, message.audio.file_id, caption=final_caption)
            elif file_type == "photo":
                await client.send_photo(BACKUP_CHANNEL_ID, message.photo.file_id, caption=final_caption)
        else:
            await status_message.edit_text("❌ Upload failed. Please try again later.")
            
    except Exception as e:
        await status_message.edit_text(f"An error occurred: {str(e)}")
    finally:
        if os.path.exists(download_path):
            os.remove(download_path)

async def upload_file_to_gofile(file_path: str):
    for server in PRIORITIZED_SERVERS:
        try:
            async with aiohttp.ClientSession() as session:
                with open(file_path, "rb") as f:
                    form_data = aiohttp.FormData()
                    form_data.add_field("file", f, filename=os.path.basename(file_path))
                    async with session.post(f"https://{server}.gofile.io/uploadfile", headers=HEADERS, data=form_data) as response:
                        response.raise_for_status()
                        result = await response.json()
                        if result.get("status") == "ok":
                            return result["data"]["downloadPage"]
        except Exception as e:
            print(f"Failed on server {server}: {e}")
            continue
    return None

if __name__ == "__main__":
    print("Bot is running...")
    app.run()