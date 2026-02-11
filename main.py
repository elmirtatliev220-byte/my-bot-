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

# Для Webhook
from fastapi import FastAPI, Request
import uvicorn

import static_ffmpeg
from dotenv import load_dotenv

try:
    static_ffmpeg.add_paths()
except Exception:
    pass

load_dotenv() 

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
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

BASE_DIR = Path(__file__).parent
TOKEN = os.getenv("BOT_TOKEN", "").strip()
PROXY = os.getenv("PROXY_URL", None) 

RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") or "https://my-bot-zxps.onrender.com"
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

def get_ffmpeg_path():
    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg: return system_ffmpeg
    local_exe = BASE_DIR / "ffmpeg.exe"
    return str(local_exe) if local_exe.exists() else "ffmpeg"

FFMPEG_EXE = get_ffmpeg_path()

class AdminStates(StatesGroup):
    waiting_for_broadcast_msg = State()
    waiting_for_ad_text = State()
    waiting_for_ad_url = State()

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
                downloads_count INTEGER DEFAULT 0
            )
        """)
        conn.execute("CREATE TABLE IF NOT EXISTS url_shorter (id TEXT PRIMARY KEY, url TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS media_cache (url_hash TEXT PRIMARY KEY, file_id TEXT, mode TEXT, service TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        # ИСПРАВЛЕНО: Новый текст кнопки
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_text', '✅ Подписаться на канал')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_url', 'https://t.me/Bns_888')")
        for s in ['tiktok', 'instagram', 'youtube', 'vk', 'pinterest', 'other']:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{s}",))
        conn.commit()

def log_service_stat(url: str):
    service = "other"
    low_url = url.lower()
    # ИСПРАВЛЕНО: Улучшенное распознавание VK и YouTube
    if "tiktok.com" in low_url: service = "tiktok"
    elif "instagram.com" in low_url or "instagr.am" in low_url: service = "instagram"
    elif any(x in low_url for x in ["youtube.com", "youtu.be"]): service = "youtube"
    elif any(x in low_url for x in ["vk.com", "vkvideo.ru", "vk.ru"]): service = "vk"
    elif "pinterest.com" in low_url or "pin.it" in low_url: service = "pinterest"
    
    with get_db() as conn:
        conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))
        conn.commit()

def get_service_stats() -> str:
    services = ['tiktok', 'instagram', 'youtube', 'vk', 'pinterest', 'other']
    stats = []
    with get_db() as conn:
        for s in services:
            res = conn.execute("SELECT value FROM settings WHERE key = ?", (f"stat_{s}",)).fetchone()
            val = res[0] if res else "0"
            stats.append(f"🔹 {s.capitalize()}: <b>{val}</b>")
    return "\n".join(stats)

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_api_bypass(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Запасной метод через Cobalt API - лучше всего для Instagram Stories и YouTube"""
    api_url = "https://api.cobalt.tools/api/json"
    headers = {
        "Accept": "application/json", 
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    payload = {
        "url": url, 
        "vCodec": "h264", # Для совместимости с Telegram
        "isAudioOnly": mode == "audio",
        "isNoWatermark": True
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=payload, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "stream" or data.get("status") == "picker":
                        res_url = data.get("url") or data["picker"][0].get("url")
                        return res_url, data.get("author", "Social Media"), data.get("filename", "Media")
        except: pass
    return None, None, None

async def download_media(url: str, mode: str, user_id: int) -> Tuple[List[str], Dict[str, Any]]:
    # Для инстаграма и коротких ссылок YouTube сразу пробуем API (оно стабильнее для Stories)
    if "instagram.com" in url or "youtu.be" in url:
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author, "title": title}

    download_dir = BASE_DIR / "downloads"
    if download_dir.exists(): shutil.rmtree(download_dir)
    download_dir.mkdir(exist_ok=True)
    
    ydl_params = {
        'quiet': True, 'no_warnings': True, 'noplaylist': True,
        'proxy': PROXY,
        'outtmpl': str(download_dir / "%(id)s.%(ext)s"),
        'ffmpeg_location': FFMPEG_EXE,
        # ИСПРАВЛЕНО: Формат для лучшей совместимости с YouTube
        'format': "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }
    if mode == "audio":
        ydl_params['postprocessors'] = [{'key': 'FFmpegExtractAudio','preferredcodec': 'mp3','preferredquality': '192'}]

    try:
        def _ex():
            with yt_dlp.YoutubeDL(ydl_params) as ydl:
                return ydl.extract_info(url, download=True)
        info = await asyncio.to_thread(_ex)
        if not info: raise Exception("No info")
        
        found_files = [str(download_dir / f) for f in os.listdir(download_dir) if not f.endswith(".part")]
        if found_files: return found_files, info
    except:
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author, "title": title}
    return [], {}

# --- [ ХЕНДЛЕРЫ ] ---

@dp.message(Command("start"))
async def start_cmd(message: Message):
    if not message.from_user: return
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined) VALUES (?, ?, ?)", 
                    (message.from_user.id, message.from_user.username or f"id_{message.from_user.id}", datetime.now().isoformat()))
        conn.commit()
    
    text = (
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        f"Я помогу скачать видео/аудио без воды:\n"
        f"————————————————\n"
        f"✨ <b>TikTok</b> | 📸 <b>Instagram</b>\n"
        f"📌 <b>Pinterest</b> | 📺 <b>YouTube</b>\n"
        f"🔵 <b>VK Видео/Клипы</b>\n"
        f"————————————————\n"
        f"📍 <i>Просто пришли мне ссылку!</i>"
    )
    kb = [[InlineKeyboardButton(text="🆘 Поддержка", callback_data="get_support")]]
    if message.from_user.id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_main")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(F.text.startswith("http"))
async def handle_url(message: Message):
    if not message.from_user or not message.text: return
    user_id = message.from_user.id
    
    # Проверка подписки
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        count = res[0] if res else 0
        if count >= FREE_LIMIT:
            try:
                member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    kb = [[InlineKeyboardButton(text="✅ Подписаться", url=CHANNEL_URL)],
                          [InlineKeyboardButton(text="🔄 Проверить", callback_data="check_sub")]]
                    return await message.answer("⚠️ <b>Лимит исчерпан!</b>\nПодпишитесь для продолжения:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
            except: pass

    v_id = hashlib.md5(message.text.encode()).hexdigest()[:10]
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, message.text))
        conn.commit()
    
    kb = [[InlineKeyboardButton(text="🎬 Видео", callback_data=f"v_{v_id}"),
            InlineKeyboardButton(text="🎵 Аудио", callback_data=f"a_{v_id}")]]
    await message.answer("🎥 Выберите формат:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.regexp(r"^[va]_"))
async def process_download(callback: CallbackQuery):
    if not callback.from_user or not callback.message: return
    user_id = callback.from_user.id
    prefix, v_id = callback.data.split("_")
    mode = "video" if prefix == "v" else "audio"
    
    with get_db() as conn:
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
        ad_res = conn.execute("SELECT value FROM settings WHERE key='ad_text'").fetchone()
        url_res = conn.execute("SELECT value FROM settings WHERE key='ad_url'").fetchone()
    
    if not row: return
    url = row[0]
    
    # Кнопка подписки (Рекламная)
    ad_kb = [[InlineKeyboardButton(text=ad_res[0], url=url_res[0])]] if ad_res and url_res else []

    load_msg = await callback.message.edit_text("⏳ Готовлю файл...")
    
    try:
        action = "upload_video" if mode=="video" else "upload_document"
        async with ChatActionSender(bot=bot, chat_id=user_id, action=action):
            paths, info = await download_media(url, mode, user_id)
            if not paths:
                return await load_msg.edit_text("❌ Не удалось скачать. Возможно, профиль закрыт или ссылка неверна.")

            cap = f"<b>{info.get('title', 'Без названия')[:50]}...</b>\n\n📥 @{BOT_USERNAME}"
            target = paths[0]
            
            if target.startswith("http"):
                if mode == "video": await bot.send_video(user_id, video=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                else: await bot.send_audio(user_id, audio=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            else:
                if mode == "video": await bot.send_video(user_id, video=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                else: await bot.send_audio(user_id, audio=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                if os.path.exists(target): os.remove(target)

            log_service_stat(url)
            with get_db() as conn:
                conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))
                conn.commit()
            await load_msg.delete()
    except Exception as e:
        logging.error(f"Error: {e}")
        await load_msg.edit_text("❌ Ошибка при отправке файла.")

# --- [ ОСТАЛЬНОЕ (Админка, Webhook и т.д.) ] ---

@dp.callback_query(F.data == "admin_main")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    with get_db() as conn:
        u_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    text = f"🛠 <b>Админ-панель</b>\n\nЮзеров: <b>{u_count}</b>\n\nСтатистика:\n{get_service_stats()}"
    kb = [[InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
          [InlineKeyboardButton(text="📝 Рекламная кнопка", callback_data="edit_ad")],
          [InlineKeyboardButton(text="❌ Закрыть", callback_data="close_admin")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "edit_ad")
async def ad_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_ad_text)
    await c.message.answer("Введите: Текст_кнопки Ссылка")

@dp.message(AdminStates.waiting_for_ad_text)
async def ad_save(m: Message, state: FSMContext):
    try:
        parts = m.text.rsplit(" ", 1)
        if len(parts) < 2: raise Exception
        with get_db() as conn:
            conn.execute("UPDATE settings SET value = ? WHERE key = 'ad_text'", (parts[0],))
            conn.execute("UPDATE settings SET value = ? WHERE key = 'ad_url'", (parts[1],))
            conn.commit()
        await m.answer("✅ Кнопка обновлена!"); await state.clear()
    except: await m.answer("❌ Ошибка! Формат: Текст Ссылка (через пробел)")

@dp.callback_query(F.data == "check_sub")
async def ch_sb(c: CallbackQuery):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=c.from_user.id)
        if member.status in ["member", "administrator", "creator"]:
            await c.message.edit_text("✅ Подписка есть! Можете качать.")
        else: await c.answer("❌ Вы всё еще не подписаны!", show_alert=True)
    except: await c.answer("Ошибка проверки")

@dp.callback_query(F.data == "close_admin")
async def close_admin_handler(callback: CallbackQuery):
    await callback.message.delete()

@dp.callback_query(F.data == "get_support")
async def support_handler(callback: CallbackQuery):
    await callback.message.answer(f"🛠 Поддержка: @{SUPPORT_USER}")

# --- [ ЗАПУСК ] ---

async def stay_awake():
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(RENDER_URL) as resp: pass
        except: pass

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    init_db()
    await bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
    asyncio.create_task(stay_awake())

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    update = Update.model_validate(await request.json(), context={"bot": bot})
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def health_check():
    return {"status": "working"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)