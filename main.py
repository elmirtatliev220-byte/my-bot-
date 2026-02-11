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
        # Рекламные настройки для сообщения о лимите
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_text', '✅ ПОДПИСАТЬСЯ И КАЧАТЬ')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_url', 'https://t.me/Bns_888')")
        for s in ['tiktok', 'instagram', 'youtube', 'vk', 'pinterest', 'other']:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{s}",))
        conn.commit()

def log_service_stat(url: str):
    service = "other"
    low_url = url.lower()
    if "tiktok.com" in low_url: service = "tiktok"
    elif "instagram.com" in low_url or "instagr.am" in low_url: service = "instagram"
    elif any(x in low_url for x in ["youtube.com", "youtu.be", "/shorts/"]): service = "youtube"
    elif any(x in low_url for x in ["vk.com", "vkvideo.ru", "vk.ru"]): service = "vk"
    elif "pinterest.com" in low_url or "pin.it" in low_url: service = "pinterest"
    
    with get_db() as conn:
        conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))
        conn.commit()

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except: return False

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_api_bypass(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[str], Optional[str]]:
    api_url = "https://api.cobalt.tools/api/json"
    headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    payload = {"url": url, "vCodec": "h264", "isAudioOnly": mode == "audio", "isNoWatermark": True}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=payload, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res_url = data.get("url") or (data.get("picker")[0].get("url") if data.get("picker") else None)
                    return res_url, data.get("author", "Media"), data.get("filename", "Media")
        except: pass
    return None, None, None

async def download_media(url: str, mode: str, user_id: int) -> Tuple[List[str], Dict[str, Any]]:
    # 1. Instagram / Shorts - сразу через API для скорости и обхода блокировок
    if any(x in url.lower() for x in ["instagram.com", "/shorts/"]):
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author, "title": title}

    download_dir = BASE_DIR / "downloads"
    if download_dir.exists(): shutil.rmtree(download_dir)
    download_dir.mkdir(exist_ok=True)
    
    ydl_params = {
        'quiet': True, 'noplaylist': True, 'proxy': PROXY,
        'outtmpl': str(download_dir / "%(id)s.%(ext)s"),
        'ffmpeg_location': FFMPEG_EXE,
        'format': "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
    }
    if mode == "audio":
        ydl_params['postprocessors'] = [{'key': 'FFmpegExtractAudio','preferredcodec': 'mp3','preferredquality': '192'}]

    try:
        def _ex():
            with yt_dlp.YoutubeDL(ydl_params) as ydl: return ydl.extract_info(url, download=True)
        info = await asyncio.to_thread(_ex)
        files = [str(download_dir / f) for f in os.listdir(download_dir) if not f.endswith(".part")]
        if files: return files, info
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
    await message.answer(f"👋 Привет! Пришли ссылку из TikTok, Insta, YouTube или VK!")

@dp.message(F.text.startswith("http"))
async def handle_url(message: Message):
    user_id = message.from_user.id
    url = message.text.strip()
    url_hash = hashlib.md5(url.encode()).hexdigest()

    # --- КЭШИРОВАНИЕ (МГНОВЕННО) ---
    with get_db() as conn:
        cached = conn.execute("SELECT file_id, mode FROM media_cache WHERE url_hash = ?", (url_hash,)).fetchone()
        if cached:
            file_id, mode = cached
            if mode == "video": await message.answer_video(file_id, caption=f"📥 @{BOT_USERNAME}")
            else: await message.answer_audio(file_id, caption=f"📥 @{BOT_USERNAME}")
            return

        # ПРОВЕРКА ЛИМИТОВ
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        count = res[0] if res else 0
        if count >= FREE_LIMIT and not await is_subscribed(user_id):
            kb = [[InlineKeyboardButton(text="💎 ПОДПИСАТЬСЯ", url=CHANNEL_URL)],
                  [InlineKeyboardButton(text="🔄 ПРОВЕРИТЬ", callback_data="check_sub")]]
            return await message.answer("⚠️ Лимит 3 видео исчерпан. Подпишись, чтобы качать дальше!", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    v_id = hashlib.md5(url.encode()).hexdigest()[:10]
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, url))
        conn.commit()
    
    kb = [[InlineKeyboardButton(text="🎬 Видео", callback_data=f"v_{v_id}"),
            InlineKeyboardButton(text="🎵 Аудио", callback_data=f"a_{v_id}")]]
    await message.answer("🎥 Выберите формат:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.regexp(r"^[va]_"))
async def process_download(callback: CallbackQuery):
    user_id = callback.from_user.id
    prefix, v_id = callback.data.split("_")
    mode = "video" if prefix == "v" else "audio"
    
    with get_db() as conn:
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
    if not row: return
    url = row[0]
    url_hash = hashlib.md5(url.encode()).hexdigest()

    load_msg = await callback.message.edit_text("⏳ Загрузка...")
    
    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video"):
            paths, info = await download_media(url, mode, user_id)
            if not paths: return await load_msg.edit_text("❌ Ошибка загрузки.")

            cap = f"<b>{info.get('title', 'Media')[:45]}</b>\n\n📥 @{BOT_USERNAME}"
            target = paths[0]
            sent_msg = None

            if target.startswith("http"):
                if mode == "video": sent_msg = await bot.send_video(user_id, video=target, caption=cap)
                else: sent_msg = await bot.send_audio(user_id, audio=target, caption=cap)
            else:
                if mode == "video": sent_msg = await bot.send_video(user_id, video=FSInputFile(target), caption=cap)
                else: sent_msg = await bot.send_audio(user_id, audio=FSInputFile(target), caption=cap)
                if os.path.exists(target): os.remove(target)

            # Сохраняем в кэш для мгновенной отправки в след. раз
            if sent_msg:
                file_id = sent_msg.video.file_id if mode=="video" else sent_msg.audio.file_id
                with get_db() as conn:
                    conn.execute("INSERT OR IGNORE INTO media_cache VALUES (?, ?, ?, ?)", (url_hash, file_id, mode, "service"))
                    conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))
                    conn.commit()
            
            log_service_stat(url)
            await load_msg.delete()
    except Exception as e:
        await load_msg.edit_text("❌ Ошибка.")

# --- [ СТАНДАРТНЫЕ ФУНКЦИИ ] ---
@dp.callback_query(F.data == "admin_main")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    with get_db() as conn:
        u_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    await callback.message.edit_text(f"🛠 Юзеров: {u_count}\n\n{get_service_stats()}", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")]]))

@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_broadcast_msg)
    await c.message.answer("Текст рассылки:")

@dp.message(AdminStates.waiting_for_broadcast_msg)
async def broadcast_execute(m: Message, state: FSMContext):
    with get_db() as conn: users = conn.execute("SELECT user_id FROM users").fetchall()
    for u in users:
        try: await m.copy_to(u[0])
        except: continue
    await m.answer("✅ Готово"); await state.clear()

@dp.callback_query(F.data == "check_sub")
async def ch_sb(c: CallbackQuery):
    if await is_subscribed(c.from_user.id): await c.message.answer("✅ Подписан!")
    else: await c.answer("❌ Нет подписки", show_alert=True)

async def stay_awake():
    while True:
        await asyncio.sleep(600)
        async with aiohttp.ClientSession() as s:
            async with s.get(RENDER_URL): pass

app = FastAPI()
@app.on_event("startup")
async def on_startup():
    init_db()
    await bot.set_webhook(url=WEBHOOK_URL)
    asyncio.create_task(stay_awake())

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    update = Update.model_validate(await request.json(), context={"bot": bot})
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def health(): return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))