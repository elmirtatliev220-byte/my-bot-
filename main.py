import os
import html
import asyncio
import sqlite3
import hashlib
import aiohttp
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional

from dotenv import load_dotenv
load_dotenv() 

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message,
    CallbackQuery
)
from aiogram.utils.chat_action import ChatActionSender

import yt_dlp

# --- [ КОНФИГУРАЦИЯ ] ---
ADMIN_ID = 123456789        
SUPPORT_USER = "твой_ник"   
CHANNEL_ID = "@Bns_888" 
CHANNEL_URL = "https://t.me/Bns_888" 
FREE_LIMIT = 3 

BASE_DIR = Path(__file__).parent
RAW_TOKEN = os.getenv("BOT_TOKEN")
TOKEN = RAW_TOKEN.strip() if RAW_TOKEN else ""
PROXY = os.getenv("PROXY_URL", None) 
FFMPEG_EXE = str(BASE_DIR / "ffmpeg.exe")

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
    if "instagram.com" in low_url: service = "instagram"
    if "youtube.com" in low_url or "youtu.be" in low_url: service = "youtube"
    if "vk.com" in low_url: service = "vk"
    if "pinterest.com" in low_url or "pin.it" in low_url: service = "pinterest"
    
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
    except: 
        return False

# --- [ СИСТЕМА ЗАГРУЗКИ ] ---

async def fetch_api_bypass(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    api_url = "https://api.cobalt.tools/api/json"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    low_url = url.lower()
    payload = {
        "url": url,
        "vCodec": "h264",
        "isAudioOnly": False,
        "isNoTTWatermark": "tiktok.com" in low_url
    }
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(api_url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("url"), "Social Media", data.get("filename", "Video")
        except Exception as e:
            print(f"Cobalt error: {e}")
    return None, None, None

async def download_media(url: str, mode: str, user_id: int) -> Tuple[List[str], Dict[str, Any]]:
    download_dir = str(BASE_DIR / "downloads")
    os.makedirs(download_dir, exist_ok=True)

    paths: List[str] = []
    info: Dict[str, Any] = {}

    low_url = url.lower()

    # Попытка через cobalt для видео на TikTok/Instagram/Pinterest
    if mode == "video" and any(x in low_url for x in ["instagram.com", "pinterest.com", "pin.it", "tiktok.com"]):
        link, uploader, title = await fetch_api_bypass(url)
        if link:
            paths = [link]
            info = {"uploader": uploader or "Unknown", "title": title or "Video"}

    # Если cobalt не сработал — используем yt_dlp
    if not paths:
        ydl_params: Dict[str, Any] = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'proxy': PROXY,
            'outtmpl': f"{download_dir}/%(id)s.%(ext)s",
            'ffmpeg_location': FFMPEG_EXE,
            'format': "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best" if mode == "video" else "bestaudio/best",
        }
        if mode == "audio":
            ydl_params['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192'
            }]

        try:
            def _ex():
                with yt_dlp.YoutubeDL(ydl_params) as ydl:
                    return ydl.extract_info(url, download=True)
            info = await asyncio.to_thread(_ex)
            if not info:
                return [], {}

            if 'entries' in info:
                info = info['entries'][0]

            ext = "mp3" if mode == "audio" else "mp4"
            for f in os.listdir(download_dir):
                if info.get('id', 'none') in f and f.endswith(ext):
                    paths = [os.path.join(download_dir, f)]
                    break
        except Exception as e:
            print(f"yt_dlp error: {e}")
            return [], {}

    return paths, info

# --- [ ХЕНДЛЕРЫ ] ---

@dp.message(Command("start"))
async def start_cmd(message: Message):
    if not message.from_user: return
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined) VALUES (?, ?, ?)", 
                    (message.from_user.id, message.from_user.username or f"id_{message.from_user.id}", datetime.now().isoformat()))
    
    text = (
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "Я помогу тебе скачать видео без\nводяных знаков:\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✨ <b>TikTok</b> | 📸 <b>Instagram</b>\n"
        "📌 <b>Pinterest</b> | 📺 <b>YouTube</b>\n"
        "🔵 <b>VK Видео/Клипы</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📍 <b>Просто пришли мне ссылку!</b>"
    )
    kb = [[InlineKeyboardButton(text="🆘 Поддержка", callback_data="get_support")]]
    if message.from_user.id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_main")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(F.text.contains("https://") | F.text.contains("http://"))
async def handle_url(message: Message):
    if not message.from_user or not message.text: return
    user_id = message.from_user.id
    
    # Извлекаем первую валидную ссылку
    urls = re.findall(r'https?://[^\s]+', message.text)
    if not urls:
        return
    url = urls[0].rstrip('.,!?')

    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        downloads = res[0] if res else 0
        if downloads >= FREE_LIMIT and not await is_subscribed(user_id):
            kb = [[InlineKeyboardButton(text="✅ Подписаться", url=CHANNEL_URL)],
                  [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")]]
            return await message.answer("⚠️ <b>Лимит исчерпан!</b>\nПодпишись на канал для продолжения:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    v_id = hashlib.md5(url.encode()).hexdigest()[:10]
    with get_db() as conn: 
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, url))
    kb = [[InlineKeyboardButton(text="🎬 Видео (MP4)", callback_data=f"v_{v_id}"),
           InlineKeyboardButton(text="🎵 Музыка (MP3)", callback_data=f"a_{v_id}")]]
    await message.answer("🎥 <b>Выберите формат:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.regexp(r"^[va]_"))
async def process_download(callback: CallbackQuery):
    if not callback.from_user or not callback.message or not callback.data: return
    user_id = callback.from_user.id
    prefix, v_id = callback.data.split("_", 1)
    mode = "video" if prefix == "v" else "audio"
    
    with get_db() as conn: 
        row = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
        ad_res = conn.execute("SELECT value FROM settings WHERE key='ad_text'").fetchone()
        url_res = conn.execute("SELECT value FROM settings WHERE key='ad_url'").fetchone()
    
    if not row: 
        await callback.answer("Ссылка устарела.", show_alert=True)
        return
    url = row[0]
    ad_kb = [[InlineKeyboardButton(text=ad_res[0], url=url_res[0])]] if ad_res and url_res else []

    # Кэш
    cached = get_cached_media(url, mode)
    if cached:
        try:
            if mode == "video":
                await bot.send_video(user_id, cached[0], reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            else:
                await bot.send_audio(user_id, cached[0], reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb))
            increment_downloads(user_id)
            if callback.message:
                await callback.message.delete()
            return
        except:
            pass

    # Сообщение о загрузке
    load_msg = await callback.message.edit_text("⏳ <b>Загрузка... Пожалуйста, подождите</b>")

    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video"):
            paths, info = await download_media(url, mode, user_id)
            if not paths:
                await load_msg.edit_text("❌ <b>Ошибка!</b>\nКонтент недоступен или защищён.")
                return

            uploader = html.escape(str(info.get('uploader', 'Автор')))
            title = html.escape(str(info.get('title', 'Без названия')))
            cap = f"📝 <b>{title}</b>\n👤 <b>От:</b> {uploader}\n\n📥 @{BOT_USERNAME}"

            target = paths[0]
            if target.startswith("http"):
                res = await (bot.send_video(user_id, video=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb)) if mode == "video"
                            else bot.send_audio(user_id, audio=target, caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb)))
                f_id = res.video.file_id if mode == "video" and hasattr(res, "video") else (res.audio.file_id if hasattr(res, "audio") else None)
                if f_id:
                    save_to_cache(url, f_id, mode)
            else:
                res = await (bot.send_video(user_id, video=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb)) if mode == "video"
                            else bot.send_audio(user_id, audio=FSInputFile(target), caption=cap, reply_markup=InlineKeyboardMarkup(inline_keyboard=ad_kb)))
                f_id = res.video.file_id if mode == "video" and hasattr(res, "video") else (res.audio.file_id if hasattr(res, "audio") else None)
                if f_id:
                    save_to_cache(url, f_id, mode)
                if os.path.exists(target):
                    os.remove(target)
            
            log_service_stat(url)
            increment_downloads(user_id)
            await load_msg.delete()
    except Exception as e:
        print(f"Send error: {e}")
        await load_msg.edit_text("❌ Ошибка отправки файла.")
    finally:
        pass

# --- [ АДМИНКА ] --- (без изменений)

@dp.callback_query(F.data == "admin_main")
async def admin_panel(callback: CallbackQuery):
    if not callback.from_user or callback.from_user.id != ADMIN_ID: return
    with get_db() as conn: 
        u_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    text = f"🛠 <b>Админ-панель</b>\n\n👥 Юзеров: <b>{u_count}</b>\n\n📊 <b>Статистика:</b>\n{get_service_stats()}"
    kb = [
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🏆 ТОП Юзеров", callback_data="admin_top")],
        [InlineKeyboardButton(text="📝 Реклама", callback_data="edit_ad")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="close_admin")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "admin_top")
async def admin_top(c: CallbackQuery):
    with get_db() as conn: 
        top = conn.execute("SELECT username, downloads_count FROM users ORDER BY downloads_count DESC LIMIT 10").fetchall()
    msg = "🏆 <b>ТОП ПО ЗАГРУЗКАМ:</b>\n\n"
    for i, (u, cnt) in enumerate(top, 1):
        msg += f"{i}. @{u or 'unknown'} — {cnt}\n"
    await c.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_main")]]))

@dp.callback_query(F.data == "admin_broadcast")
async def br_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_broadcast_msg)
    await c.message.answer("📩 Пришли пост для рассылки:")
    await c.answer()

@dp.message(AdminStates.waiting_for_broadcast_msg)
async def br_exec(m: Message, state: FSMContext):
    with get_db() as conn: 
        users = conn.execute("SELECT user_id FROM users").fetchall()
    count = 0
    for u in users:
        try: 
            await m.copy_to(u[0])
            count += 1
            await asyncio.sleep(0.05)
        except: 
            continue
    await m.answer(f"✅ Готово! Получили: {count}")
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

@dp.callback_query(F.data == "close_admin")
async def cl_adm(c: CallbackQuery): 
    if c.message:
        await c.message.delete()

@dp.callback_query(F.data == "get_support")
async def sup(c: CallbackQuery): 
    await c.message.answer(f"🛠 Админ: @{SUPPORT_USER}")
    await c.answer()

async def main():
    init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    print("🚀 Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())