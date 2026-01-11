import os
import re
import asyncio
import logging
import aiohttp
import aiofiles
import json
from threading import Thread
from flask import Flask, request, abort
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatAction
import yt_dlp

def _get_env(key, default=None, required=False, cast=None):
    val = os.environ.get(key, None if default is None else str(default))
    if val is None and required:
        raise RuntimeError(f"Environment variable {key} is required")
    if val is None:
        return None
    if cast:
        try:
            return cast(val)
        except Exception as e:
            raise RuntimeError(f"Failed to cast env {key}: {e}")
    return val

class YTDLPLogger:
    def __init__(self):
        self._messages = []
    def debug(self, msg):
        self._messages.append(("DEBUG", str(msg)))
    def info(self, msg):
        self._messages.append(("INFO", str(msg)))
    def warning(self, msg):
        self._messages.append(("WARNING", str(msg)))
    def error(self, msg):
        self._messages.append(("ERROR", str(msg)))
    def text(self):
        return "\n".join(f"[{lvl}] {m}" for lvl, m in self._messages) if self._messages else ""

COOKIES_TXT_CONTENT = _get_env("COOKIES_TXT_CONTENT")
COOKIES_TXT_PATH = "cookies.txt"

def setup_cookies_file():
    if COOKIES_TXT_CONTENT:
        try:
            with open(COOKIES_TXT_PATH, "w") as f:
                f.write(COOKIES_TXT_CONTENT)
            logging.info(f"Successfully created {COOKIES_TXT_PATH} from COOKIES_TXT_CONTENT.")
        except Exception as e:
            logging.error(f"Failed to write COOKIES_TXT_CONTENT to file: {e}")
    elif not os.path.exists(COOKIES_TXT_PATH):
        open(COOKIES_TXT_PATH, "a").close()
        logging.warning(f"COOKIES_TXT_CONTENT not set. Created empty {COOKIES_TXT_PATH}.")

API_ID = _get_env("API_ID", required=True, cast=int)
API_HASH = _get_env("API_HASH", required=True)
BOT1_TOKEN = _get_env("BOT_TOKEN", required=True)
PORT = _get_env("PORT", 8080, cast=int)

DOWNLOAD_PATH = _get_env("DOWNLOAD_PATH", "downloads")
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

MAX_CONCURRENT_DOWNLOADS = _get_env("MAX_CONCURRENT_DOWNLOADS", 5, cast=int)
MAX_VIDEO_DURATION = _get_env("MAX_VIDEO_DURATION", 1800, cast=int)
MAX_VIDEO_DURATION_YOUTUBE = _get_env("MAX_VIDEO_DURATION_YOUTUBE", 600, cast=int)

BOT_USERNAME = None

YDL_OPTS_PIN = {
    "format": "bestvideo+bestaudio/best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

YDL_OPTS_YOUTUBE = {
    "format": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

YDL_OPTS_DEFAULT = {
    "format": "best",
    "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "cookiefile": COOKIES_TXT_PATH
}

SUPPORTED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "fb.watch", "pin.it",
    "x.com", "tiktok.com", "snapchat.com", "https://", "http://", "instagram.com"
]

pyro_client = Client("video_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT1_TOKEN)
flask_app = Flask(__name__)

active_downloads = 0
queue = None
lock = None

USER_SUCCESS_PATH = "user_success.json"
user_success = {}
counters_lock = None

async def ensure_primitives():
    global queue, lock, counters_lock
    if queue is None:
        queue = asyncio.Queue()
    if lock is None:
        lock = asyncio.Lock()
    if counters_lock is None:
        counters_lock = asyncio.Lock()

async def load_user_success():
    global user_success
    try:
        if os.path.exists(USER_SUCCESS_PATH):
            async with aiofiles.open(USER_SUCCESS_PATH, "r") as f:
                content = await f.read()
                if content:
                    raw = json.loads(content)
                    user_success = {int(k): int(v) for k, v in raw.items()}
                else:
                    user_success = {}
        else:
            user_success = {}
    except Exception:
        user_success = {}

async def save_user_success():
    try:
        async with counters_lock:
            serial = {str(k): v for k, v in user_success.items()}
            async with aiofiles.open(USER_SUCCESS_PATH, "w") as f:
                await f.write(json.dumps(serial))
    except Exception:
        pass

async def increment_user_success(uid: int):
    await ensure_primitives()
    async with counters_lock:
        count = user_success.get(uid, 0) + 1
        user_success[uid] = count
        try:
            serial = {str(k): v for k, v in user_success.items()}
            async with aiofiles.open(USER_SUCCESS_PATH, "w") as f:
                await f.write(json.dumps(serial))
        except Exception:
            pass
        return count

async def get_bot_username(client):
    global BOT_USERNAME
    if BOT_USERNAME is None:
        try:
            me = await client.get_me()
            BOT_USERNAME = me.username
        except Exception:
            return "Bot"
    return BOT_USERNAME

async def download_thumbnail(url, target_path):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(target_path, mode='wb')
                    await f.write(await resp.read())
                    await f.close()
                    if os.path.exists(target_path):
                        return target_path
    except:
        pass
    return None

def extract_metadata_from_info(info):
    width = info.get("width")
    height = info.get("height")
    duration = info.get("duration")
    if not width or not height:
        formats = info.get("formats") or []
        best = None
        for f in formats:
            if f.get("width") and f.get("height"):
                best = f
                break
        if best:
            if not width:
                width = best.get("width")
            if not height:
                height = best.get("height")
            if not duration:
                dms = best.get("duration_ms")
                duration = info.get("duration") or (dms / 1000 if dms else None)
    return width, height, duration

async def download_video(url: str, bot_username: str):
    loop = asyncio.get_running_loop()
    try:
        lowered = url.lower()
        is_pin = "pin.it" in lowered
        is_youtube = "youtube.com" in lowered or "youtu.be" in lowered
        if is_pin:
            ydl_opts = YDL_OPTS_PIN.copy()
        elif is_youtube:
            ydl_opts = YDL_OPTS_YOUTUBE.copy()
        else:
            ydl_opts = YDL_OPTS_DEFAULT.copy()
        logger = YTDLPLogger()
        def extract_info_sync():
            opts = ydl_opts.copy()
            opts["logger"] = logger
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        try:
            info = await loop.run_in_executor(None, extract_info_sync)
        except Exception as e:
            text = logger.text() or str(e)
            return ("YTDLP_ERROR", text)
        width, height, duration = extract_metadata_from_info(info)
        limit = MAX_VIDEO_DURATION_YOUTUBE if is_youtube else MAX_VIDEO_DURATION
        if duration and duration > limit:
            return ("TOO_LONG", limit)
        def download_sync():
            opts = ydl_opts.copy()
            opts["logger"] = logger
            with yt_dlp.YoutubeDL(opts) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        try:
            info, filename = await loop.run_in_executor(None, download_sync)
        except Exception as e:
            text = logger.text() or str(e)
            return ("YTDLP_ERROR", text)
        title = info.get("title") or ""
        desc = info.get("description") or ""
        bot_tag = f"@{bot_username}"
        is_youtube_flag = "youtube.com" in url.lower() or "youtu.be" in url.lower()
        if is_youtube_flag:
            caption = title or bot_tag
            if len(caption) > 1024:
                caption = caption[:1024]
        else:
            caption = desc.strip() or bot_tag
            if len(caption) > 1024:
                caption = caption[:1021] + "..."
        thumb = None
        thumb_url = info.get("thumbnail")
        if thumb_url:
            thumb_path = os.path.splitext(filename)[0] + ".jpg"
            thumb = await download_thumbnail(thumb_url, thumb_path)
        return caption, filename, width, height, duration, thumb
    except Exception as e:
        logging.exception(e)
        return "ERROR"

async def download_audio_only(url: str, bot_username: str):
    loop = asyncio.get_running_loop()
    lowered_url = url.lower()
    is_supported = any(domain in lowered_url for domain in ["youtube.com", "youtu.be"])
    if not is_supported:
        return None
    try:
        ydl_opts_info = {
            "skip_download": True,
            "quiet": True,
            "cookiefile": COOKIES_TXT_PATH
        }
        logger = YTDLPLogger()
        def extract_info_sync():
            opts = ydl_opts_info.copy()
            opts["logger"] = logger
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        try:
            info = await loop.run_in_executor(None, extract_info_sync)
        except Exception:
            return None
        duration = info.get("duration")
        if not duration or duration <= 120:
            return None
        ydl_opts_audio = {
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "outtmpl": os.path.join(DOWNLOAD_PATH, "%(title)s.m4a"),
            "noplaylist": True,
            "quiet": True,
            "cookiefile": COOKIES_TXT_PATH
        }
        def download_sync():
            opts = ydl_opts_audio.copy()
            opts["logger"] = logger
            with yt_dlp.YoutubeDL(opts) as ydl:
                info_dl = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info_dl)
                return info_dl, filename
        try:
            info_dl, filename = await loop.run_in_executor(None, download_sync)
        except Exception:
            return None
        title = info_dl.get("title") or "Audio"
        caption = ""
        return caption, filename
    except Exception as e:
        logging.exception(e)
        return None

async def _download_worker(client, message, url):
    bot_username = await get_bot_username(client)
    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    except:
        pass
    attempts = 0
    max_attempts = 2
    result = None
    while attempts < max_attempts:
        try:
            result = await download_video(url, bot_username)
        except Exception as e:
            logging.exception("download_video raised")
            result = "ERROR"
        if result == "ERROR" or (isinstance(result, tuple) and result and result[0] == "YTDLP_ERROR") or result is None:
            attempts += 1
            try:
                await asyncio.sleep(1)
            except:
                pass
            continue
        break
    if isinstance(result, tuple) and result[0] == "TOO_LONG":
        yt_limit = int(MAX_VIDEO_DURATION_YOUTUBE / 60)
        gen_limit = int(MAX_VIDEO_DURATION / 60)
        try:
            await message.reply(
                f"Cannot download videos longer than limits.\n\n"
                f"YouTube Limit: {yt_limit} minutes\n"
                f"Other Sites Limit: {gen_limit} minutes 👍"
            )
        except:
            pass
    elif result is None:
        try:
            await message.reply("Cannot download this video 👍")
        except:
            pass
    elif isinstance(result, tuple) and result[0] == "YTDLP_ERROR":
        error_text = result[1] or "Unknown yt-dlp error"
        try:
            if len(error_text) > 4000:
                error_text = error_text[:3990] + "\n... (truncated)"
            await message.reply(f"yt-dlp error log:\n\n{error_text}")
        except:
            try:
                await message.reply("yt-dlp produced an error but it could not be sent.")
            except:
                pass
    elif result == "ERROR":
        try:
            await message.reply("An unexpected error occurred.")
        except:
            pass
    else:
        caption, file_path, width, height, duration, thumb = result
        try:
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
        except:
            pass
        kwargs = {"video": file_path, "caption": caption, "supports_streaming": True}
        if width: kwargs["width"] = int(width)
        if height: kwargs["height"] = int(height)
        if duration: kwargs["duration"] = int(float(duration))
        if thumb and os.path.exists(thumb): kwargs["thumb"] = thumb
        sent_video = False
        try:
            await client.send_video(message.chat.id, **kwargs)
            sent_video = True
        except Exception:
            logging.exception("Sending video failed")
        audio_result = await download_audio_only(url, bot_username)
        if audio_result:
            audio_caption, audio_path = audio_result
            try:
                await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_AUDIO)
            except:
                pass
            try:
                await client.send_audio(
                    message.chat.id,
                    audio=audio_path,
                    caption=audio_caption,
                    title=os.path.splitext(os.path.basename(audio_path))[0],
                    performer=f"via @{bot_username}"
                )
            except Exception:
                logging.exception("Sending audio failed")
            if audio_path and os.path.exists(audio_path):
                try:
                    os.remove(audio_path)
                except:
                    pass
        for f in [file_path, thumb]:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
        if sent_video:
            try:
                uid = message.from_user.id
                await increment_user_success(uid)
            except Exception:
                pass

async def download_task_wrapper(client, message, url):
    global active_downloads
    await ensure_primitives()
    async with lock:
        active_downloads += 1
    try:
        await _download_worker(client, message, url)
    finally:
        async with lock:
            active_downloads -= 1
        await start_next_download()

async def start_next_download():
    await ensure_primitives()
    global active_downloads
    async with lock:
        while not queue.empty() and active_downloads < MAX_CONCURRENT_DOWNLOADS:
            client, message, url = await queue.get()
            asyncio.create_task(download_task_wrapper(client, message, url))

@pyro_client.on_message(filters.private & filters.command("start"))
async def start(client, message: Message):
    await message.reply("""Welcome 👋

This bot lets you download videos from
YouTube, TikTok, Instagram, and more.

👉 Just send the video link""")

@pyro_client.on_message(filters.private & filters.text)
async def handle_link(client, message: Message):
    url = message.text.strip()
    if not any(domain in url.lower() for domain in SUPPORTED_DOMAINS):
        await message.reply("Please send a valid video link 👍")
        return
    await ensure_primitives()
    async with lock:
        if active_downloads < MAX_CONCURRENT_DOWNLOADS:
            asyncio.create_task(download_task_wrapper(client, message, url))
        else:
            await queue.put((client, message, url))

@flask_app.route("/", methods=["GET"])
def keep_alive():
    html = (
        "<!doctype html>"
        "<html>"
        "<head>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<style>"
        "body{font-family:Arial,Helvetica,sans-serif;margin:0;padding:0;display:flex;align-items:center;justify-content:center;height:100vh;background:#f7f7f7}"
        ".card{background:#fff;padding:20px;border-radius:12px;box-shadow:0 6px 18px rgba(0,0,0,0.08);text-align:center;width:90%;max-width:420px}"
        ".status{font-size:18px;margin-top:8px;color:#333}"
        "</style>"
        "</head>"
        "<body>"
        "<div class='card'><div style='font-size:28px'>✅</div><div class='status'>Bot is running</div></div>"
        "</body>"
        "</html>"
    )
    return html, 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(PORT))

def run_bot():
    pyro_client.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    setup_cookies_file()
    asyncio.get_event_loop().run_until_complete(load_user_success())
    Thread(target=run_flask, daemon=True).start()
    run_bot()
