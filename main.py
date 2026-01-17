import os
import logging
import asyncio
import time
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid, ChannelInvalid
from aiohttp import web

# --- CONFIGURATION ---
API_ID = 29714294
API_HASH = "bd44a7527bbb8ef23552c569ff3a0d93"
BOT_TOKEN = "7926056695:AAF-S2VFyr84axsK9ZxdA0kpe-MC4aesHJQ"
GOFILE_TOKEN = "avoA4ruw3nxglw11NOTR5GzH2bpB5QRe"
BACKUP_CHANNEL_ID = -1003648024683
LOG_CHANNEL_ID = -1003648024683
ADMIN_IDS = [5978396634]
PORT = int(os.environ.get("PORT", 8080))

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- BOT CLIENT INITIALIZATION ---
# in_memory=True keeps session in RAM (good for Render free tier ephemeral filesystem)
app = Client(
    "gofile_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
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

async def progress_bar(current, total, status_msg, start_time):
    """Updates the message with progress every few seconds."""
    now = time.time()
    diff = now - start_time
    
    # Update every 5 seconds or at completion to avoid rate limits
    if round(diff % 5.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff if diff > 0 else 0
        eta = round((total - current) / speed) if speed > 0 else 0
        
        def sizeof_fmt(num):
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if abs(num) < 1024.0:
                    return "%3.1f%s" % (num, unit)
                num /= 1024.0
            return "%.1f%s" % (num, 'PB')

        try:
            await status_msg.edit(
                f"**🚀 Stream Uploading...**\n\n"
                f"**Progress:** {percentage:.1f}%\n"
                f"**Done:** {sizeof_fmt(current)} / {sizeof_fmt(total)}\n"
                f"**Speed:** {sizeof_fmt(speed)}/s\n"
                f"**ETA:** {eta}s"
            )
        except Exception:
            pass

async def file_stream_generator(client, message, status_msg, start_time):
    """
    Yields file chunks from Telegram while updating progress.
    This acts as the 'file' object for aiohttp to read from,
    bridging the download from TG to the upload to GoFile.
    """
    total_size = message.document.file_size if message.document else (
        message.video.file_size if message.video else (
            message.audio.file_size if message.audio else message.photo.file_size
        )
    )
    
    current_size = 0
    # stream_media yields chunks directly from Telegram
    async for chunk in client.stream_media(message):
        yield chunk
        current_size += len(chunk)
        await progress_bar(current_size, total_size, status_msg, start_time)

async def upload_stream_to_gofile(client, message, status_msg):
    """Streams data directly from Telegram to GoFile."""
    server = await get_gofile_server()
    if not server:
        return None, "Could not connect to GoFile servers."

    url = f"https://{server}.gofile.io/uploadFile"
    start_time = time.time()
    
    # Determine filename
    if message.document:
        filename = message.document.file_name
    elif message.video:
        filename = message.video.file_name or "video.mp4"
    elif message.audio:
        filename = message.audio.file_name or "audio.mp3"
    else:
        filename = f"file_{message.id}.jpg"

    # Setup the stream generator
    stream = file_stream_generator(client, message, status_msg, start_time)
    
    # IMPORTANT: Set timeout to None for large files so Render doesn't kill the connection
    timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_connect=60, sock_read=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # We construct a FormData that pulls from the async generator
            data = aiohttp.FormData()
            data.add_field('token', GOFILE_TOKEN)
            data.add_field('file', stream, filename=filename)
            
            # Sending the request (this will run the generator loop)
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
            return None, f"Stream Exception: {str(e)}"

# --- BOT COMMANDS ---

@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "👋 **Hello! I am your GoFile Stream Uploader.**\n\n"
        "I support **Large Files** (up to 2GB+) on Render Free Tier!\n"
        "Send me a file, and I will stream it directly to GoFile.\n\n"
        f"**Backup Channel:** `{BACKUP_CHANNEL_ID}`"
    )

@app.on_message(filters.command("help"))
async def help_command(client, message):
    await message.reply_text(
        "**📚 Help Menu**\n\n"
        "/start - Check if I'm alive.\n"
        "**Just send a file** - I will auto-process it.\n\n"
        "**Features:**\n"
        "- Auto Forward to Backup Channel\n"
        "- Stream Upload (No storage needed)\n"
        "- Unlimited File Size Support (Network dependent)"
    )

# --- CORE FILE HANDLER ---

@app.on_message(filters.private & (filters.document | filters.video | filters.audio | filters.photo))
async def file_handler(client, message):
    user_id = message.from_user.id
    
    # 1. FORWARD TO BACKUP CHANNEL
    # We attempt this first. If it fails, we log it but don't stop the upload.
    try:
        await message.forward(BACKUP_CHANNEL_ID)
        logger.info(f"Forwarded message {message.id} from {user_id} to backup channel.")
    except (PeerIdInvalid, ChannelInvalid):
        logger.error(f"CRITICAL: Bot is NOT a member of channel {BACKUP_CHANNEL_ID}. Please add the bot as admin.")
        await message.reply_text(f"⚠️ **Warning:** I am not in the Backup Channel ({BACKUP_CHANNEL_ID}). Please add me as Admin so I can forward files.")
    except Exception as e:
        logger.error(f"Failed to forward: {e}")

    # 2. PROCESS STREAM UPLOAD
    status_msg = await message.reply_text("🔄 **Initializing Stream...**")
    
    try:
        # We pass the 'message' object itself to our stream helper
        link, error = await upload_stream_to_gofile(client, message, status_msg)
        
        if link:
            await status_msg.edit(
                f"✅ **Upload Complete!**\n\n"
                f"🔗 **Link:** {link}\n"
                f"👤 **User:** {message.from_user.mention}"
            )
            
            # Log success to channel if possible
            try:
                file_name = message.document.file_name if message.document else "Media File"
                await client.send_message(
                    LOG_CHANNEL_ID,
                    f"✅ **New File Streamed**\n\n"
                    f"📂 **File:** {file_name}\n"
                    f"🔗 **Link:** {link}\n"
                    f"👤 **From:** {message.from_user.mention} (`{user_id}`)"
                )
            except Exception:
                pass # Fail silently if log channel access is also broken
        else:
            await status_msg.edit(f"❌ **Upload Failed:** {error}")

    except Exception as e:
        logger.error(f"Processing error: {e}")
        await status_msg.edit(f"❌ **Critical Error:** {str(e)}")

async def check_channel_access():
    """Checks if the bot has access to the backup channel on startup."""
    try:
        logger.info(f"Checking access to Backup Channel: {BACKUP_CHANNEL_ID}...")
        chat = await app.get_chat(BACKUP_CHANNEL_ID)
        logger.info(f"✅ Backup Channel Verified: {chat.title}")
    except Exception as e:
        logger.error(f"❌ CANNOT ACCESS BACKUP CHANNEL: {e}")
        logger.error("👉 ACTION REQUIRED: Add the bot to the channel as an Administrator immediately.")

# --- RUNNER ---

if __name__ == "__main__":
    # Start loop
    loop = asyncio.get_event_loop()
    
    # Run Web Server and Bot together
    logger.info("Starting Web Server & Bot...")
    app.start()
    
    # Check channel access immediately after starting
    loop.run_until_complete(check_channel_access())
    
    # Setup Aiohttp Web Server
    runner = web.AppRunner(loop.run_until_complete(web_server()))
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    loop.run_until_complete(site.start())
    
    logger.info(f"Bot started on port {PORT}. Idling...")
    
    try:
        idle()
    except Exception:
        pass
