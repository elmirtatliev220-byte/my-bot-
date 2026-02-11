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

# Настройка максимально подробного логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Попытка настроить FFmpeg
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
# ВАЖНО: Проверь, чтобы в Render в Environment Variables был RENDER_EXTERNAL_URL
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
    logger.info("База данных инициализирована")

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

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except: return False

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_from_api(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[str]]:
    """Попытка скачать через внешние API (Cobalt и альтернативы)"""
    api_endpoints = [
        "https://api.cobalt.tools/api/json",
        "https://cobalt.crst.it/api/json"
    ]
    
    headers = {
        "Accept": "application/json", 
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
    
    payload = {
        "url": url, 
        "vCodec": "h264", 
        "isAudioOnly": mode == "audio",
        "aFormat": "mp3"
    }

    async with aiohttp.ClientSession() as session:
        for api in api_endpoints:
            try:
                logger.info(f"Пробую API: {api}")
                async with session.post(api, json=payload, headers=headers, timeout=12) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Cobalt может вернуть url или picker (для каруселей)
                        res_url = data.get("url") or (data.get("picker")[0].get("url") if data.get("picker") else None)
                        if res_url:
                            return res_url, data.get("filename", "video")
            except Exception as e:
                logger.warning(f"Ошибка API {api}: {e}")
                continue
    return None, None

async def download_media(url: str, mode: str) -> Tuple[List[str], Dict[str, Any]]:
    """Основной метод: Сначала API, потом (если не вышло) yt-dlp"""
    
    # 1. Сначала пробуем API (для Instagram и Pinterest это единственный шанс на Render)
    api_link, filename = await fetch_from_api(url, mode)
    if api_link:
        logger.info(f"Успешно получена прямая ссылка через API: {api_link[:50]}...")
        return [api_link], {"title": filename}

    # 2. Резерв через yt-dlp
    logger.info("API не помогло, запускаю yt-dlp...")
    download_dir = BASE_DIR / "downloads"
    if not download_dir.exists(): download_dir.mkdir()
    
    unique_id = hashlib.md5(f"{url}{datetime.now()}".encode()).hexdigest()[:8]
    outtmpl = str(download_dir / f"{unique_id}.%(ext)s")
    
    ydl_params = {
        'quiet': True, 
        'noplaylist': True,
        'outtmpl': outtmpl,
        'ffmpeg_location': FFMPEG_EXE,
        'socket_timeout': 15,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    if mode == "audio":
        ydl_params['format'] = 'bestaudio/best'
        ydl_params['postprocessors'] = [{'key': 'FFmpegExtractAudio','preferredcodec': 'mp3','preferredquality': '192'}]
    else:
        # Для Pinterest/Instagram часто лучше работает 'best'
        if "pin.it" in url or "pinterest.com" in url:
            ydl_params['format'] = 'best'
        else:
            ydl_params['format'] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    
    try:
        def _ex():
            with yt_dlp.YoutubeDL(ydl_params) as ydl:
                return ydl.extract_info(url, download=True)
        info = await asyncio.to_thread(_ex)
        
        # Ищем файл
        downloaded_files = list(download_dir.glob(f"{unique_id}.*"))
        if downloaded_files:
            return [str(f) for f in downloaded_files], dict(info)
    except Exception as e:
        logger.error(f"yt-dlp совсем не справился: {e}")
        
    return [], {}

# --- [ ХЕНДЛЕРЫ ] ---

@dp.message(Command("start"))
async def start_cmd(message: Message, command: CommandObject):
    user_id = message.from_user.id
    logger.info(f"Команда /start от {user_id}")
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined) VALUES (?, ?, ?)", 
                    (user_id, message.from_user.username or f"id_{user_id}", datetime.now().isoformat()))
        conn.commit()
    
    text = f"<b>✨ Привет! Я {BOT_USERNAME}</b>\n\nЯ качаю видео из Instagram, TikTok, Pinterest и VK.\n\n🚀 <b>Просто пришли мне ссылку!</b>"
    kb = [[InlineKeyboardButton(text="👤 Мой профиль", callback_data="my_profile")]]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(F.text.startswith("http"))
async def handle_url(message: Message):
    user_id = message.from_user.id
    url = message.text.strip()
    logger.info(f"[RECEIVED] Ссылка от {user_id}: {url}")

    # Проверка лимита
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        count = res[0] if res else 0
        if count >= FREE_LIMIT and not await is_subscribed(user_id):
            return await message.answer("⚠️ <b>Лимит исчерпан!</b>\nПодпишись на канал, чтобы качать без ограничений:", 
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💎 ПОДПИСАТЬСЯ", url=CHANNEL_URL)],
                    [InlineKeyboardButton(text="🔄 ПРОВЕРИТЬ", callback_data="check_sub")]]))

    url_hash = hashlib.md5(url.encode()).hexdigest()
    v_id = url_hash[:10]
    
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, url))
        conn.commit()

    kb = [[InlineKeyboardButton(text="🎬 Видео", callback_data=f"dl_v_{v_id}"),
           InlineKeyboardButton(text="🎵 Аудио", callback_data=f"dl_a_{v_id}")]]
    
    await message.answer("🎯 <b>Формат найден! Что качаем?</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("dl_"))
async def process_download(callback: CallbackQuery):
    user_id = callback.from_user.id
    _, mode_char, v_id = callback.data.split("_")
    mode = "video" if mode_char == "v" else "audio"

    with get_db() as conn:
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
    if not row: return await callback.answer("Ошибка: ссылка потерялась.")
    
    url = row[0]
    status_msg = await callback.message.edit_text("⏳ <b>Обработка ссылки...</b>")

    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video" if mode == "video" else "upload_voice"):
            paths, info = await download_media(url, mode)
            
            if not paths:
                return await status_msg.edit_text("❌ <b>Не удалось скачать.</b>\nВозможно, профиль приватный или сервис временно недоступен.")

            await status_msg.edit_text("🚀 <b>Отправляю файл...</b>")
            target = paths[0]
            caption = f"<b>{info.get('title', 'Media')[:50]}</b>\n\n📥 @{BOT_USERNAME}"

            if target.startswith("http"):
                # Если это прямая ссылка
                sent = await (bot.send_video(user_id, video=target, caption=caption) if mode == "video" else bot.send_audio(user_id, audio=target, caption=caption))
            else:
                # Если это локальный файл
                sent = await (bot.send_video(user_id, video=FSInputFile(target), caption=caption) if mode == "video" else bot.send_audio(user_id, audio=FSInputFile(target), caption=caption))
                if os.path.exists(target): os.remove(target)

            if sent:
                with get_db() as conn:
                    conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))
                    conn.commit()
                await bot.send_sticker(user_id, SUCCESS_STICKER)
                log_service_stat(url)
            
            await status_msg.delete()

    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        await status_msg.edit_text("❌ Ошибка при отправке файла пользователю.")

@dp.callback_query(F.data == "my_profile")
async def profile_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
    count = res[0] if res else 0
    await callback.message.answer(f"<b>👤 Профиль</b>\n\n📊 Скачано: <b>{count}</b>\n💎 Лимит: <b>{'Безлимит' if await is_subscribed(user_id) else FREE_LIMIT}</b>")
    await callback.answer()

@dp.callback_query(F.data == "check_sub")
async def check_sub(callback: CallbackQuery):
    if await is_subscribed(callback.from_user.id):
        await callback.message.edit_text("✅ <b>Подписка подтверждена!</b> Теперь просто пришли ссылку.")
    else:
        await callback.answer("❌ Вы всё еще не подписаны на канал!", show_alert=True)

# --- [ SERVER ] ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Сброс вебхука для чистоты
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types())
    logger.info(f"Вебхук установлен: {WEBHOOK_URL}")
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Ошибка в вебхуке: {e}")
    return {"ok": True}

@app.get("/")
async def health(): return {"status": "ok", "bot": BOT_USERNAME}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))