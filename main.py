import os
import html
import asyncio
import sqlite3
import hashlib
import aiohttp
import re
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional, Union
from contextlib import asynccontextmanager

# Библиотеки для сервера
from fastapi import FastAPI, Request
import uvicorn

import static_ffmpeg
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка FFmpeg
try:
    static_ffmpeg.add_paths()
except Exception:
    pass

load_dotenv() 

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message,
    CallbackQuery, Update
)
from aiogram.utils.chat_action import ChatActionSender

import yt_dlp

# --- [ КОНФИГУРАЦИЯ ] ---
ADMIN_ID = 391491090          
SUPPORT_USER = "твой_ник"   
CHANNEL_ID = "@Bns_888" 
CHANNEL_URL = "https://t.me/Bns_888" 
FREE_LIMIT = 3 
SUCCESS_STICKER = "CAACAgIAAxkBAAEL6_Zl9_2_" 

BASE_DIR = Path(__file__).parent
TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") or "https://my-bot-zxps.onrender.com"
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

FFMPEG_EXE = shutil.which("ffmpeg") or "ffmpeg"

class AdminStates(StatesGroup):
    waiting_for_broadcast_msg = State()

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
BOT_USERNAME: str = "Limiktikbot"

# --- [ БАЗА ДАННЫХ ] ---

def get_db():
    return sqlite3.connect(str(BASE_DIR / "database.db"), check_same_thread=False)

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                username TEXT, 
                joined TEXT, 
                downloads_count INTEGER DEFAULT 0,
                referred_by INTEGER
            )
        """)
        conn.execute("CREATE TABLE IF NOT EXISTS url_shorter (id TEXT PRIMARY KEY, url TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS media_cache (url_hash TEXT PRIMARY KEY, file_id TEXT, mode TEXT, service TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        for s in ['tiktok', 'instagram', 'vk', 'pinterest', 'youtube', 'other']:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{s}",))
        conn.commit()

def log_service_stat(url: str):
    service = "other"
    low_url = url.lower()
    if "tiktok.com" in low_url: service = "tiktok"
    elif "instagram.com" in low_url or "instagr.am" in low_url: service = "instagram"
    elif any(x in low_url for x in ["vk.com", "vkvideo.ru", "vk.ru"]): service = "vk"
    elif "pinterest.com" in low_url or "pin.it" in low_url: service = "pinterest"
    elif "youtube.com" in low_url or "youtu.be" in low_url: service = "youtube"
    try:
        with get_db() as conn:
            conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))
            conn.commit()
    except Exception as e:
        logger.error(f"Stat error: {e}")

def get_service_stats() -> str:
    services = ['tiktok', 'instagram', 'vk', 'pinterest', 'youtube', 'other']
    stats = []
    with get_db() as conn:
        for s in services:
            res = conn.execute("SELECT value FROM settings WHERE key = ?", (f"stat_{s}",)).fetchone()
            val = res[0] if res else "0"
            stats.append(f"🔹 {s.capitalize()}: <b>{val}</b>")
    return "\n".join(stats)

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except: return False

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_api_bypass(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Используем проверенный API Cobalt
    api = "https://api.cobalt.tools/api/json"
    headers = {
        "Accept": "application/json", 
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
    payload = {"url": url, "vCodec": "h264", "isAudioOnly": mode == "audio"}
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        try:
            async with session.post(api, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("url"), data.get("author"), data.get("filename")
                else:
                    logger.error(f"API Error: {resp.status}")
        except Exception as e:
            logger.error(f"API Request failed: {e}")
    return None, None, None

async def download_media(url: str, mode: str) -> Tuple[List[str], Dict[str, Any]]:
    # Для Pinterest и Instagram сначала пробуем быстрый API, так как yt-dlp часто банят
    if any(x in url.lower() for x in ["pin.it", "pinterest.com", "instagram.com", "instagr.am"]):
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author, "title": title}

    # Резервный метод через yt-dlp
    download_dir = BASE_DIR / "downloads"
    if download_dir.exists():
        try: shutil.rmtree(download_dir)
        except: pass
    download_dir.mkdir(exist_ok=True)
    
    ydl_params: Dict[str, Any] = {
        'quiet': True, 
        'noplaylist': True,
        'outtmpl': str(download_dir / "%(id)s.%(ext)s"),
        'ffmpeg_location': FFMPEG_EXE,
        'socket_timeout': 30,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    }

    if mode == "audio":
        ydl_params['format'] = 'bestaudio/best'
        ydl_params['postprocessors'] = [{'key': 'FFmpegExtractAudio','preferredcodec': 'mp3','preferredquality': '192'}]
    else:
        ydl_params['format'] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    
    try:
        def _ex():
            with yt_dlp.YoutubeDL(ydl_params) as ydl: return ydl.extract_info(url, download=True)
        info = await asyncio.to_thread(_ex)
        if not info: return [], {}
        files = [str(download_dir / f) for f in os.listdir(download_dir) if not f.endswith(".part")]
        return files, dict(info)
    except Exception as e:
        logger.warning(f"yt-dlp failed: {e}. Trying final API fallback.")
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author, "title": title}
    return [], {}

# --- [ ХЕНДЛЕРЫ ] ---

@dp.message(Command("start"))
async def start_cmd(message: Message, command: CommandObject):
    try:
        if not message.from_user: return
        user_id = message.from_user.id
        args = command.args
        referrer = int(args) if args and args.isdigit() and int(args) != user_id else None

        with get_db() as conn:
            conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined, referred_by) VALUES (?, ?, ?, ?)", 
                        (user_id, message.from_user.username or f"id_{user_id}", datetime.now().isoformat(), referrer))
            conn.commit()
        
        text = f"<b>✨ Привет! Я {BOT_USERNAME}</b>\n\nЗагружаю медиа из <b>Instagram, TikTok, Pinterest и VK</b>.\n━━━━━━━━━━━━━━━━━━━━\n🚀 <b>Просто пришли ссылку!</b>"
        kb = [[InlineKeyboardButton(text="👤 Мой профиль", callback_data="my_profile")],
              [InlineKeyboardButton(text="🆘 Поддержка", callback_data="get_support")]]
        if user_id == ADMIN_ID: kb.insert(0, [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_main")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logger.error(f"Start error: {e}")

@dp.message(F.text.startswith("http"))
async def handle_url(message: Message):
    try:
        if not message.from_user or not message.text: return
        user_id, url = message.from_user.id, message.text.strip()
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        with get_db() as conn:
            res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if (res[0] if res else 0) >= FREE_LIMIT and not await is_subscribed(user_id):
                return await message.answer("⚠️ <b>Лимит исчерпан!</b>\nПодпишись на канал:", 
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 ПОДПИСАТЬСЯ", url=CHANNEL_URL)],
                                                                       [InlineKeyboardButton(text="🔄 ПРОВЕРИТЬ", callback_data="check_sub")]]))
            cached = conn.execute("SELECT file_id, mode FROM media_cache WHERE url_hash = ?", (url_hash,)).fetchone()
        
        if cached:
            file_id, mode = cached
            await bot.send_sticker(user_id, SUCCESS_STICKER)
            return await (bot.send_video(user_id, video=file_id) if mode == "video" else bot.send_audio(user_id, audio=file_id))

        v_id = url_hash[:10]
        with get_db() as conn:
            conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, url))
            conn.commit()

        kb = [[InlineKeyboardButton(text="🎬 Видео", callback_data=f"dl_v_{v_id}"),
               InlineKeyboardButton(text="🎵 Аудио", callback_data=f"dl_a_{v_id}")]]
        await message.answer("🎬 <b>Выберите формат:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logger.error(f"URL error: {e}")

@dp.callback_query(F.data.startswith("dl_"))
async def process_download(callback: CallbackQuery):
    if not callback.message or not isinstance(callback.message, Message) or not callback.data: return
    
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    if len(parts) < 3: return
    _, mode_char, v_id = parts
    mode = "video" if mode_char == "v" else "audio"

    with get_db() as conn:
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
    if not row: return await callback.answer("Ошибка ссылки")
    
    url = row[0]
    url_hash = hashlib.md5(url.encode()).hexdigest()
    load_msg = await callback.message.edit_text("⏳ <b>Загрузка...</b>")

    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video" if mode == "video" else "upload_voice"):
            paths, info = await download_media(url, mode)
            if not paths:
                await load_msg.edit_text("❌ Извините, не удалось получить видео. Попробуйте другую ссылку.")
                return

            cap = f"<b>{info.get('title', 'Media')[:45]}</b>\n\n📥 @{BOT_USERNAME}"
            target = paths[0]
            
            if target.startswith("http"):
                sent = await (bot.send_video(user_id, video=target, caption=cap) if mode == "video" else bot.send_audio(user_id, audio=target, caption=cap))
            else:
                sent = await (bot.send_video(user_id, video=FSInputFile(target), caption=cap) if mode == "video" else bot.send_audio(user_id, audio=FSInputFile(target), caption=cap))
                if os.path.exists(target): 
                    try: os.remove(target)
                    except: pass

            if sent:
                f_id = sent.video.file_id if (mode == "video" and sent.video) else (sent.audio.file_id if sent.audio else None)
                if f_id:
                    with get_db() as conn:
                        conn.execute("INSERT OR IGNORE INTO media_cache (url_hash, file_id, mode) VALUES (?, ?, ?)", (url_hash, f_id, mode))
                        conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))
                        conn.commit()

            await bot.send_sticker(user_id, SUCCESS_STICKER)
            log_service_stat(url)
            await load_msg.delete()
    except Exception as e:
        logger.error(f"Final Send error: {e}")
        await load_msg.edit_text("❌ Ошибка при отправке. Файл может быть слишком большим.")

# --- [ СИСТЕМНЫЕ ФУНКЦИИ ] ---

@dp.callback_query(F.data == "my_profile")
async def profile_handler(callback: CallbackQuery):
    if not callback.message or not isinstance(callback.message, Message): return
    user_id = callback.from_user.id
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        ref_count = conn.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,)).fetchone()[0]
    sub = "✅ Активна" if await is_subscribed(user_id) else "❌ Не оформлена"
    text = (f"<b>👤 Ваш профиль</b>\n\n📊 Скачано: <b>{res[0] if res else 0}</b>\n👥 Рефералы: <b>{ref_count}</b>\n"
            f"💎 Подписка: <b>{sub}</b>\n\n🔗 Ссылка:\n<code>https://t.me/{BOT_USERNAME}?start={user_id}</code>")
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_start")]]))

@dp.callback_query(F.data == "back_start")
async def back_st(c: CallbackQuery):
    if not c.message or not isinstance(c.message, Message): return
    await start_cmd(c.message, CommandObject(command="start", args=None))
    await c.message.delete()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
    yield

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    data = await request.json()
    update = Update.model_validate(data, context={"bot": bot})
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def health(): return {"status": "ok"}

if __name__ == "__main__":
    while True:
        try:
            uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
        except Exception as e:
            logger.critical(f"Server crash: {e}")
            import time
            time.sleep(5)