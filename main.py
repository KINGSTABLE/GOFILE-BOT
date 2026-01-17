import os
import logging
import asyncio
import time
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

# --- CONFIGURATION ---
# (Using the credentials you provided)
API_ID = 29714294
API_HASH = "bd44a7527bbb8ef23552c569ff3a0d93"
BOT_TOKEN = "7926056695:AAF-S2VFyr84axsK9ZxdA0kpe-MC4aesHJQ"
GOFILE_TOKEN = "avoA4ruw3nxglw11NOTR5GzH2bpB5QRe"
BACKUP_CHANNEL_ID = -1003648024683
LOG_CHANNEL_ID = -1003648024683
ADMIN_IDS = [5978396634]  # List of admins
PORT = int(os.environ.get("PORT", 8080))

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- BOT CLIENT INITIALIZATION ---
app = Client(
    "gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- WEB SERVER (FOR RENDER KEEP-ALIVE) ---
routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response({"status": "running", "message": "GoFile Bot is Alive!"})

async def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(routes)
    return web_app

# --- HELPER FUNCTIONS ---

async def get_gofile_server():
    """Fetches the best available GoFile server for uploading."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.gofile.io/getServer") as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "ok":
                        return data["data"]["server"]
        except Exception as e:
            logger.error(f"Error getting GoFile server: {e}")
    return None

async def upload_to_gofile(file_path, token):
    """Uploads a file to GoFile."""
    server = await get_gofile_server()
    if not server:
        return None, "Could not connect to GoFile servers."

    url = f"https://{server}.gofile.io/uploadFile"
    
    async with aiohttp.ClientSession() as session:
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file', f)
                data.add_field('token', token)
                
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result["status"] == "ok":
                            return result["data"]["downloadPage"], None
                        else:
                            return None, f"GoFile Error: {result.get('status')}"
                    else:
                        return None, f"HTTP Error: {response.status}"
        except Exception as e:
            return None, f"Upload Exception: {str(e)}"

async def progress_bar(current, total, status_msg, start_time):
    """Updates the message with download/upload progress."""
    now = time.time()
    diff = now - start_time
    if round(diff % 5.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff)
        eta = round((total - current) / speed) if speed > 0 else 0
        
        # Helper to format size
        def sizeof_fmt(num):
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if abs(num) < 1024.0:
                    return "%3.1f%s" % (num, unit)
                num /= 1024.0
            return "%.1f%s" % (num, 'PB')

        try:
            await status_msg.edit(
                f"**Progress:** {percentage:.1f}%\n"
                f"**Status:** {status_msg.text.splitlines()[0]}\n"
                f"**Done:** {sizeof_fmt(current)} / {sizeof_fmt(total)}\n"
                f"**Speed:** {sizeof_fmt(speed)}/s\n"
                f"**ETA:** {eta}s"
            )
        except Exception:
            pass

# --- BOT COMMANDS ---

@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "👋 **Hello! I am your GoFile Uploader Bot.**\n\n"
        "Send me any file, and I will:\n"
        "1. Forward it to the backup channel.\n"
        "2. Upload it to GoFile.io and give you the link.\n\n"
        "Maintained by KingStable."
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    await message.reply_text(
        "**📚 Help Menu**\n\n"
        "/start - Check if I'm alive.\n"
        "**Just send a file** - I will auto-process it.\n\n"
        "**Note:** Ensure files are under 2GB for Telegram restrictions."
    )

# --- CORE FILE HANDLER ---

@app.on_message(filters.private & (filters.document | filters.video | filters.audio | filters.photo))
async def file_handler(client, message):
    user_id = message.from_user.id
    
    # 1. FORWARD TO BACKUP CHANNEL (The request fix)
    try:
        # We use copy_message to ensure it's a clean copy, 
        # but forward_messages is also valid. Copy is safer if user deletes original.
        forwarded = await message.forward(BACKUP_CHANNEL_ID)
        logger.info(f"Forwarded message {message.id} from {user_id} to backup channel.")
    except Exception as e:
        logger.error(f"Failed to forward to backup channel: {e}")
        await message.reply_text(f"⚠️ Warning: Could not forward to backup channel. Error: {e}")

    # 2. PROCESS UPLOAD TO GOFILE
    status_msg = await message.reply_text("📥 **Downloading from Telegram...**")
    start_time = time.time()
    
    file_path = f"downloads/{message.id}_{user_id}"
    
    try:
        # Download
        path = await client.download_media(
            message,
            file_name=file_path,
            progress=progress_bar,
            progress_args=(status_msg, start_time)
        )
        
        await status_msg.edit("wm **Uploading to GoFile...**")
        
        # Upload
        link, error = await upload_to_gofile(path, GOFILE_TOKEN)
        
        if link:
            # Success Message to User
            await status_msg.edit(
                f"✅ **Upload Complete!**\n\n"
                f"🔗 **Link:** {link}\n"
                f"👤 **User:** {message.from_user.mention}"
            )
            
            # Send Link to Backup/Log Channel as well
            await client.send_message(
                LOG_CHANNEL_ID,
                f"✅ **New File Processed**\n\n"
                f"📂 **File Name:** {os.path.basename(path)}\n"
                f"🔗 **GoFile Link:** {link}\n"
                f"👤 **From:** {message.from_user.mention} (`{user_id}`)"
            )
        else:
            await status_msg.edit(f"❌ **Upload Failed:** {error}")

    except Exception as e:
        logger.error(f"Processing error: {e}")
        await status_msg.edit(f"❌ **Error:** {str(e)}")
    
    finally:
        # Cleanup: Delete the local file to save space on Render
        if os.path.exists(path):
            os.remove(path)

# --- RUNNER ---

if __name__ == "__main__":
    # Create download directory if not exists
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    # Start loop
    loop = asyncio.get_event_loop()
    
    # Run Web Server and Bot together
    logger.info("Starting Web Server & Bot...")
    app.start()
    
    # Setup Aiohttp
    runner = web.AppRunner(loop.run_until_complete(web_server()))
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    loop.run_until_complete(site.start())
    
    logger.info(f"Bot started on port {PORT}. Idling...")
    
    # Keep the script running
    try:
        pyrogram.idle()
    except NameError:
        # Fallback if pyrogram.idle isn't imported directly
        loop.run_forever()
