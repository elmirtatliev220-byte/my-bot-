import os
import asyncio
import sqlite3
import hashlib
import aiohttp
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional
from contextlib import asynccontextmanager

# Серверные библиотеки
from fastapi import FastAPI, Request
import uvicorn
import static_ffmpeg
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FFmpeg
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

# ================= [ КОНФИГУРАЦИЯ ] =================
ADMIN_ID = 391491090
CHANNEL_ID = "@Bns_888" 
CHANNEL_URL = "https://t.me/Bns_888" 
FREE_LIMIT = 3 
SUCCESS_STICKER = "CAACAgIAAxkBAAEL6_Zl9_2_" 

BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
if not RENDER_URL:
    RENDER_URL = "https://your-app-name.onrender.com" # Заглушка, если забыл указать

WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

FFMPEG_EXE = shutil.which("ffmpeg") or "ffmpeg"
BOT_USERNAME = "Limiktikbot"

# Список зеркал API (Ротация для стабильности)
COBALT_MIRRORS = [
    "https://api.cobalt.tools/api/json",
    "https://cobalt.crst.it/api/json",
    "https://api.wuk.sh/api/json", 
    "https://co.wuk.sh/api/json"
]

class AdminStates(StatesGroup):
    waiting_for_broadcast_msg = State()

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ================= [ БАЗА ДАННЫХ ] =================

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
        conn.execute("CREATE TABLE IF NOT EXISTS media_cache (url_hash TEXT PRIMARY KEY, file_id TEXT, mode TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        # Инициализация статистики
        for s in ['tiktok', 'instagram', 'vk', 'pinterest', 'youtube', 'other']:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{s}",))
        conn.commit()

def log_service_stat(url: str):
    service = "other"
    low = url.lower()
    if "tiktok" in low: service = "tiktok"
    elif "instagram" in low: service = "instagram"
    elif "vk.com" in low: service = "vk"
    elif "pin" in low: service = "pinterest"
    elif "youtu" in low: service = "youtube"
    try:
        with get_db() as conn:
            conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))
            conn.commit()
    except Exception as e:
        logger.error(f"Stat Error: {e}")

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return m.status in ["member", "administrator", "creator"]
    except:
        return False

# ================= [ СЛОЖНАЯ ЛОГИКА ЗАГРУЗКИ ] =================

async def resolve_redirects(url: str) -> str:
    """Расшифровывает короткие ссылки типа pin.it"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True, timeout=5) as resp:
                return str(resp.url)
    except:
        return url

async def fetch_cobalt_rotation(url: str, mode: str = "video") -> Tuple[Optional[str], Optional[str]]:
    """Пробует скачать через список API по очереди"""
    payload = {
        "url": url,
        "vCodec": "h264",
        "isAudioOnly": mode == "audio",
        "aFormat": "mp3"
    }
    headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

    async with aiohttp.ClientSession() as session:
        for api_url in COBALT_MIRRORS:
            try:
                # logger.info(f"Пробую зеркало: {api_url}")
                async with session.post(api_url, json=payload, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        link = data.get("url")
                        if not link and data.get("picker"):
                            link = data["picker"][0]["url"]
                        
                        if link:
                            return link, data.get("filename", "video.mp4")
            except:
                continue # Если зеркало не работает, идем к следующему
    return None, None

async def download_media_smart(url: str, mode: str) -> Tuple[List[str], str]:
    """Умная загрузка: API Rotation -> yt-dlp fallback"""
    
    # 1. Расшифровка коротких ссылок (важно для Pinterest)
    if "pin.it" in url or "t.co" in url:
        url = await resolve_redirects(url)

    # 2. Попытка через API (быстро, без бана IP)
    direct_link, filename = await fetch_cobalt_rotation(url, mode)
    if direct_link:
        return [direct_link], filename or "media"

    # 3. Резерв через yt-dlp (если API сдохли)
    # logger.info("API не справились, запускаю yt-dlp...")
    file_path = DOWNLOAD_DIR / f"dl_{int(datetime.now().timestamp())}_{hashlib.md5(url.encode()).hexdigest()[:5]}"
    
    opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': str(file_path),
        'quiet': True,
        'noplaylist': True,
        'socket_timeout': 20,
        'ffmpeg_location': FFMPEG_EXE,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    if "pinterest" in url:
        opts['format'] = 'best' # Pinterest часто лучше отдает так

    if mode == "audio":
        opts['format'] = 'bestaudio/best'
        opts['postprocessors'] = [{'key': 'FFmpegExtractAudio','preferredcodec': 'mp3'}]
        file_path = file_path.with_suffix(".mp3")
    else:
        file_path = file_path.with_suffix(".mp4")

    try:
        def _run():
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
        await asyncio.to_thread(_run)
        
        # Находим реальный файл
        base_name = file_path.stem
        found = list(DOWNLOAD_DIR.glob(f"{base_name}*"))
        if found:
            return [str(found[0])], "media"
    except Exception as e:
        logger.error(f"DL Error: {e}")

    return [], ""

# ================= [ ХЕНДЛЕРЫ: СТАРТ И ПРОФИЛЬ ] =================

@dp.message(Command("start"))
async def start_cmd(message: Message, command: CommandObject):
    user_id = message.from_user.id
    args = command.args
    referrer = None
    if args and args.isdigit() and int(args) != user_id:
        referrer = int(args)
    
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined, referred_by) VALUES (?, ?, ?, ?)", 
                    (user_id, message.from_user.username, datetime.now().isoformat(), referrer))
        conn.commit()

    kb_list = [
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/Bns_support")] # Заменил на кнопку-ссылку, так надежнее
    ]
    if user_id == ADMIN_ID:
        kb_list.insert(0, [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_main")])

    await message.answer(
        f"<b>✨ Привет! Я {BOT_USERNAME}</b>\n\n"
        "Я качаю видео и фото из <b>Instagram, Pinterest, TikTok, YouTube и VK</b>.\n"
        "Просто отправь мне ссылку!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list)
    )

@dp.callback_query(F.data == "my_profile")
async def profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    with get_db() as conn:
        u_data = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
        refs = conn.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,)).fetchone()[0]
    
    d_count = u_data[0] if u_data else 0
    is_sub = await is_subscribed(user_id)
    status = "✅ Активна" if is_sub else "❌ Не оформлена"
    
    text = (
        f"<b>👤 Ваш профиль</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📊 Скачано: <b>{d_count}</b>\n"
        f"👥 Рефералы: <b>{refs}</b>\n"
        f"💎 Подписка: <b>{status}</b>\n\n"
        f"🔗 Твоя реф. ссылка:\n<code>https://t.me/{BOT_USERNAME}?start={user_id}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_start")]])
    await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data == "back_start")
async def back_to_start(c: CallbackQuery):
    await c.message.delete()
    await start_cmd(c.message, CommandObject(command="start", args=None))

# ================= [ ХЕНДЛЕРЫ: АДМИНКА (ВЕРНУЛ) ] =================

@dp.callback_query(F.data == "admin_main")
async def admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    
    with get_db() as conn:
        users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        stats = conn.execute("SELECT key, value FROM settings").fetchall()
    
    stat_dict = {k: v for k, v in stats}
    msg = (
        f"<b>🛠 Админ-панель</b>\n\n"
        f"👥 Всего юзеров: <b>{users}</b>\n"
        f"📹 Instagram: {stat_dict.get('stat_instagram', 0)}\n"
        f"💃 TikTok: {stat_dict.get('stat_tiktok', 0)}\n"
        f"📌 Pinterest: {stat_dict.get('stat_pinterest', 0)}\n"
        f"📺 YouTube: {stat_dict.get('stat_youtube', 0)}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_start")]
    ])
    await callback.message.edit_text(msg, reply_markup=kb)

@dp.callback_query(F.data == "admin_broadcast")
async def admin_bc(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("📝 <b>Введите сообщение для рассылки:</b>\n(Или напишите 'отмена')")
    await state.set_state(AdminStates.waiting_for_broadcast_msg)

@dp.message(AdminStates.waiting_for_broadcast_msg)
async def process_broadcast(message: Message, state: FSMContext):
    if message.text.lower() == 'отмена':
        await state.clear()
        return await message.answer("Рассылка отменена.")
    
    await message.answer("🚀 Начинаю рассылку...")
    count = 0
    with get_db() as conn:
        users = conn.execute("SELECT user_id FROM users").fetchall()
    
    for u in users:
        try:
            await bot.copy_message(u[0], message.chat.id, message.message_id)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    
    await message.answer(f"✅ Рассылка завершена. Доставлено: {count}")
    await state.clear()

# ================= [ ХЕНДЛЕРЫ: ОБРАБОТКА ССЫЛОК ] =================

@dp.message(F.text.regexp(r"(http|www)"))
async def process_link(message: Message):
    user_id = message.from_user.id
    url = message.text.strip()

    # 1. Проверка подписки
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id = ?", (user_id,)).fetchone()
    
    d_count = res[0] if res else 0
    if d_count >= FREE_LIMIT and not await is_subscribed(user_id):
        return await message.answer(
            "⚠️ <b>Лимит исчерпан!</b>\nПодпишись на канал для безлимита:", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Подписаться", url=CHANNEL_URL)],
                [InlineKeyboardButton(text="🔄 Проверить", callback_data="check_sub")]
            ])
        )

    # 2. Кэш
    url_hash = hashlib.md5(url.encode()).hexdigest()
    with get_db() as conn:
        cached = conn.execute("SELECT file_id, mode FROM media_cache WHERE url_hash = ?", (url_hash,)).fetchone()
    
    if cached:
        await bot.send_sticker(user_id, SUCCESS_STICKER)
        if cached[1] == "video":
            return await message.answer_video(cached[0], caption=f"📥 @{BOT_USERNAME}")
        else:
            return await message.answer_audio(cached[0], caption=f"📥 @{BOT_USERNAME}")

    # 3. Меню выбора
    v_id = url_hash[:10]
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (v_id, url))
        conn.commit()

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎬 Видео", callback_data=f"dl_v_{v_id}"),
        InlineKeyboardButton(text="🎵 Аудио", callback_data=f"dl_a_{v_id}")
    ]])
    await message.answer("🔎 <b>Что скачиваем?</b>", reply_markup=kb)

@dp.callback_query(F.data.startswith("dl_"))
async def download_handler(c: CallbackQuery):
    _, mode_char, v_id = c.data.split("_")
    mode = "video" if mode_char == "v" else "audio"
    user_id = c.from_user.id

    with get_db() as conn:
        res = conn.execute("SELECT url FROM url_shorter WHERE id = ?", (v_id,)).fetchone()
    if not res: return await c.answer("Ссылка устарела")
    url = res[0]

    msg = await c.message.edit_text("⏳ <b>Загрузка...</b>\n<i>(Перебираю серверы...)</i>")

    try:
        async with ChatActionSender(bot=bot, chat_id=user_id, action="upload_video" if mode == "video" else "upload_voice"):
            paths, filename = await download_media_smart(url, mode)

            if not paths:
                return await msg.edit_text("❌ <b>Не удалось скачать.</b>\nПопробуйте позже или проверьте ссылку.")
            
            target = paths[0]
            cap = f"📥 @{BOT_USERNAME}"

            if target.startswith("http"):
                sent = await (bot.send_video(user_id, video=target, caption=cap) if mode == "video" else bot.send_audio(user_id, audio=target, caption=cap))
            else:
                sent = await (bot.send_video(user_id, video=FSInputFile(target), caption=cap) if mode == "video" else bot.send_audio(user_id, audio=FSInputFile(target), caption=cap))
                try: os.remove(target)
                except: pass

            if sent:
                f_id = sent.video.file_id if (mode=="video" and sent.video) else (sent.audio.file_id if sent.audio else None)
                if f_id:
                    with get_db() as conn:
                        conn.execute("INSERT OR IGNORE INTO media_cache (url_hash, file_id, mode) VALUES (?, ?, ?)", 
                                    (hashlib.md5(url.encode()).hexdigest(), f_id, mode))
                        conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id = ?", (user_id,))
                        conn.commit()
                    log_service_stat(url)
                
            await bot.send_sticker(user_id, SUCCESS_STICKER)
            await msg.delete()

    except Exception as e:
        logger.error(f"Send error: {e}")
        await msg.edit_text("❌ Ошибка отправки файла.")

@dp.callback_query(F.data == "check_sub")
async def check_sub_handler(c: CallbackQuery):
    if await is_subscribed(c.from_user.id):
        await c.message.edit_text("✅ <b>Подписка есть!</b> Жду ссылку.")
    else:
        await c.answer("❌ Нет подписки!", show_alert=True)

# ================= [ СЕРВЕР ] =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    try:
        update = Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot, update)
    except: pass
    return {"ok": True}

@app.get("/")
async def health(): return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))