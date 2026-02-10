import os
import html
import asyncio
import sqlite3
import hashlib
import aiohttp
import re
import shutil
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional, Union

import static_ffmpeg
from dotenv import load_dotenv

# --- [ ТЕХНИЧЕСКИЙ ДОБАВОК ДЛЯ RENDER ] ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args): return

def run_health_check():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('0.0.0.0', port)
    try:
        httpd = HTTPServer(server_address, HealthCheckHandler)
        print(f"✅ Health-check server started on port {port}")
        httpd.serve_forever()
    except Exception as e:
        print(f"❌ Server error: {e}")

threading.Thread(target=run_health_check, daemon=True).start()

try:
    static_ffmpeg.add_paths()
except Exception:
    pass
# ------------------------------------------

load_dotenv() 

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message,
    CallbackQuery, InaccessibleMessage
)
from aiogram.utils.chat_action import ChatActionSender

import yt_dlp

# --- [ КОНФИРУРАЦИЯ ] ---
ADMIN_ID = 391491090        
SUPPORT_USER = "твой_ник"   
CHANNEL_ID = "@Bns_888" 
CHANNEL_URL = "https://t.me/Bns_888" 
FREE_LIMIT = 3 

BASE_DIR = Path(__file__).parent
RAW_TOKEN = os.getenv("BOT_TOKEN")
TOKEN = RAW_TOKEN.strip() if RAW_TOKEN else ""
PROXY = os.getenv("PROXY_URL", None) 

def get_ffmpeg_path():
    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg
    local_exe = BASE_DIR / "ffmpeg.exe"
    return str(local_exe) if local_exe.exists() else "ffmpeg"

FFMPEG_EXE = get_ffmpeg_path()

class AdminStates(StatesGroup):
    waiting_for_broadcast_msg = State()
    waiting_for_ad_text = State()
    waiting_for_ad_url = State()

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
ACTIVE_USERS: Dict[int, Message] = {}
BOT_USERNAME: str = "Limiktikbot"

# --- [ БАЗА ДАННЫХ ] ---

def get_db():
    return sqlite3.connect(str(BASE_DIR / "database.db"), check_same_thread=False)

def init_db():
    with get_db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, joined TEXT, downloads_count INTEGER DEFAULT 0)")
        conn.execute("CREATE TABLE IF NOT EXISTS url_shorter (id TEXT PRIMARY KEY, url TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS media_cache (url_hash TEXT PRIMARY KEY, file_id TEXT, mode TEXT, service TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_text', '💎 Заработать тут')")
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ad_url', 'https://t.me/Bns_888')")
        conn.commit()

def log_service_stat(url: str):
    service = "other"
    low_url = url.lower()
    if "tiktok.com" in low_url: service = "tiktok"
    elif "instagram.com" in low_url: service = "instagram"
    elif "youtube.com" in low_url or "youtu.be" in low_url: service = "youtube"
    elif "vk.com" in low_url: service = "vk"
    elif "pinterest.com" in low_url or "pin.it" in low_url: service = "pinterest"
    
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{service}",))
        conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))

def get_service_stats() -> str:
    services = ['tiktok', 'instagram', 'youtube', 'vk', 'pinterest', 'other']
    stats = []
    with get_db() as conn:
        for s in services:
            res = conn.execute("SELECT value FROM settings WHERE key = ?", (f"stat_{s}",)).fetchone()
            val = res[0] if res else "0"
            stats.append(f"🔹 {s.capitalize()}: <b>{val}</b>")
    return "\n".join(stats)

def get_cached_media(url: str, mode: str) -> Optional[Tuple[str]]:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    with get_db() as conn:
        return conn.execute("SELECT file_id FROM media_cache WHERE url_hash = ? AND mode = ?", (url_hash, mode)).fetchone()

def save_to_cache(url: str, file_id: str, mode: str):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO media_cache VALUES (?, ?, ?, ?)", (url_hash, file_id, mode, "detect"))

def increment_downloads(user_id: int):
    with get_db() as conn:
        conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except: return False

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_api_bypass(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[Optional[str]], Optional[str]]:
    api_url = "https://api.cobalt.tools/api/json"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "url": url, 
        "vCodec": "h264",
        "isAudioOnly": True if mode == "audio" else False,
        "isNoWatermark": True
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=payload, headers=headers, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "url" in data:
                        return data.get("url"), data.get("author", "Social Media"), data.get("filename", "Media")
                    elif "picker" in data and len(data["picker"]) > 0:
                        return data["picker"][0].get("url"), "Social Media", "Media"
        except: pass
    return None, None, None

async def download_media(url: str, mode: str, user_id: int) -> Tuple[List[str], Dict[str, Any]]:
    low_url = url.lower()
    
    download_dir = str(BASE_DIR / "downloads")
    if os.path.exists(download_dir): shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    
    ydl_params = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'proxy': PROXY,
        'outtmpl': f"{download_dir}/%(id)s.%(ext)s",
        'ffmpeg_location': FFMPEG_EXE,
        'format': "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best" if mode == "video" else "bestaudio/best",
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    # ДОБАВКА ДЛЯ PINTEREST (m3u8 и реферер)
    if "pinterest" in low_url or "pin.it" in low_url:
        ydl_params['referer'] = 'https://www.pinterest.com/'
        ydl_params['format'] = 'bestvideo+bestaudio/best'
    
    if mode == "audio":
        ydl_params['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]

    try:
        def _ex():
            with yt_dlp.YoutubeDL(ydl_params) as ydl:
                return ydl.extract_info(url, download=True)
        info = await asyncio.to_thread(_ex)
        if not info: raise Exception("No info")
        if 'entries' in info: info = info['entries'][0]
        
        ext = "mp3" if mode == "audio" else "mp4"
        # Проверяем все файлы, включая .webp и .mkv (которые бывают в Pinterest)
        for f in os.listdir(download_dir):
            file_path = os.path.join(download_dir, f)
            if f.endswith(ext) or f.endswith(".webp") or f.endswith(".mkv"):
                return [file_path], info
        return [], {}
    except Exception as e:
        logging.error(f"yt-dlp error: {e}")
        link, author, title = await fetch_api_bypass(url, mode)
        if link: return [link], {"uploader": author or "Unknown", "title": title or "Media"}
        return [], {}

# --- [ ХЕНДЛЕРЫ ] ---

@dp.message(Command("start"))
async def start_cmd(message: Message):
    if not message.from_user: return
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined) VALUES (?, ?, ?)", 
                    (message.from_user.id, message.from_user.username or f"id_{message.from_user.id}", datetime.now().isoformat()))
    
    text = (
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        f"Я помогу тебе скачать видео без\nводяных знаков:\n"
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
    
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        count = res[0] if res else 0
        if count >= FREE_LIMIT and not await is_subscribed(user_id):
            kb = [[InlineKeyboardButton(text="✅ Подписаться", url=CHANNEL_URL)],
                  [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")]]
            return await message.answer("⚠️ Лимит исчерпан! Подпишитесь на канал, чтобы продолжить:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    v_id = hashlib.md5(message.text.encode()).hexdigest()[:10]
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, message.text))
    
    kb = [[InlineKeyboardButton(text="🎬 Видео", callback_data=f"v_{v_id}"),
           InlineKeyboardButton(text="🎵 Аудио", callback_data=f"a_{v_id}")]]
    await message.answer("🎥 В каком формате скачать?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.regexp(r"^[va]_"))
async def process_download(callback: CallbackQuery):
    if not callback.from_user or not callback.message or not callback.data: return
    user_id = callback.from_user.id
    prefix, v_id = callback.data.split("_")
    mode = "video" if prefix == "v" else "audio"
    
    with get_db() as conn:
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
        ad_res = conn.execute("SELECT value FROM settings WHERE key='ad_text'").fetchone()
        url_res = conn.execute("SELECT value FROM settings WHERE key='ad_url'").fetchone()
    
    if not row: return
    url = row[0]
    
    cached = get_cached_media(url, mode)
    ad_kb = [[InlineKeyboardButton(text=ad_res[0], url=url_res[0])]] if ad_res and url_res else []

    if cached:
        try:
            if mode == "video":
                await bot.send_video(user_id, cached[0], reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            else:
                await bot.send_audio(user_id, cached[0], reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            increment_downloads(user_id)
            await callback.message.delete()
            return
        except: pass

    load_msg = await callback.message.edit_text("⏳ Начинаю загрузку, подождите...")
    
    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video" if mode=="video" else "upload_voice"):
            paths, info = await download_media(url, mode, user_id)
            if not paths:
                await load_msg.edit_text("❌ Ошибка загрузки. Ссылка не поддерживается или контент защищен.")
                return

            cap = f"📝 {info.get('title', 'Без названия')}\n👤 {info.get('uploader', 'Автор неизвестен')}\n\n📥 @{BOT_USERNAME}"
            target = paths[0]
            
            if target.startswith("http"):
                if mode == "video":
                    res = await bot.send_video(user_id, video=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                else:
                    res = await bot.send_audio(user_id, audio=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            else:
                if mode == "video":
                    res = await bot.send_video(user_id, video=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                else:
                    res = await bot.send_audio(user_id, audio=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
                if os.path.exists(target): os.remove(target)

            f_id = None
            if mode == "video" and getattr(res, "video", None): f_id = res.video.file_id
            elif mode == "audio" and getattr(res, "audio", None): f_id = res.audio.file_id
            
            if f_id: save_to_cache(url, f_id, mode)
            log_service_stat(url)
            increment_downloads(user_id)
            await load_msg.delete()
            
    except Exception as e:
        await load_msg.edit_text(f"❌ Ошибка: {str(e)}")

# --- [ АДМИН-ПАНЕЛЬ ] ---

@dp.callback_query(F.data == "admin_main")
async def admin_panel(callback: CallbackQuery):
    if not callback.from_user or callback.from_user.id != ADMIN_ID: return
    with get_db() as conn:
        u_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    
    text = f"🛠 <b>Админ-панель</b>\n\nВсего пользователей: <b>{u_count}</b>\n\nСтатистика по сервисам:\n{get_service_stats()}"
    kb = [
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📝 Настроить рекламу", callback_data="edit_ad")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="close_admin")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_broadcast_msg)
    await callback.message.answer("📩 Пришлите сообщение (текст, фото, видео), которое нужно разослать всем:")
    await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast_msg)
async def broadcast_execute(message: Message, state: FSMContext):
    with get_db() as conn:
        users = conn.execute("SELECT user_id FROM users").fetchall()
    
    count = 0
    for user in users:
        try:
            await message.copy_to(user[0])
            count += 1
            await asyncio.sleep(0.05)
        except: continue
    
    await message.answer(f"✅ Рассылка завершена!\nУспешно отправлено: <b>{count}</b> пользователям.")
    await state.clear()

@dp.callback_query(F.data == "edit_ad")
async def ad_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_ad_text)
    await c.message.answer("Введите текст кнопки и ссылку через пробел:")
    await c.answer()

@dp.message(AdminStates.waiting_for_ad_text)
async def ad_save(m: Message, state: FSMContext):
    if not m.text: return
    try:
        txt, link = m.text.rsplit(" ", 1)
        with get_db() as conn:
            conn.execute("UPDATE settings SET value = ? WHERE key = 'ad_text'", (txt,))
            conn.execute("UPDATE settings SET value = ? WHERE key = 'ad_url'", (link,))
        await m.answer("✅ Реклама обновлена!")
        await state.clear()
    except:
        await m.answer("❌ Ошибка. Формат: Текст Ссылка")

@dp.callback_query(F.data == "check_sub")
async def ch_sb(c: CallbackQuery):
    if await is_subscribed(c.from_user.id):
        await c.message.edit_text("✅ Подписка подтверждена!")
    else:
        await c.answer("❌ Сначала подпишись!", show_alert=True)

@dp.callback_query(F.data == "get_support")
async def support_handler(callback: CallbackQuery):
    await callback.message.answer(f"🛠 По всем вопросам пишите: @{SUPPORT_USER}")
    await callback.answer()

@dp.callback_query(F.data == "close_admin")
async def close_admin_handler(callback: CallbackQuery):
    if callback.message:
        await callback.message.delete()

async def main():
    init_db()
    print(f"🚀 Бот запущен! FFmpeg: {FFMPEG_EXE}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Выход...")