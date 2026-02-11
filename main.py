import os
import asyncio
import sqlite3
import hashlib
import aiohttp
import logging
import random
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
import uvicorn
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message,
    CallbackQuery, Update, FSInputFile
)
from aiogram.utils.chat_action import ChatActionSender

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

# --- КОНФИГУРАЦИЯ ---
ADMIN_ID = 391491090
CHANNEL_ID = "@Bns_888"
CHANNEL_URL = "https://t.me/Bns_888"
FREE_LIMIT = 3
SUCCESS_STICKER = "CAACAgIAAxkBAAEL6_Zl9_2_"
BOT_USERNAME = "Limiktikbot"

TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}" if RENDER_URL else None

BASE_DIR = Path(__file__).parent
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- СИЛЬНЫЕ ЗЕРКАЛА API ---
COBALT_MIRRORS = [
    "https://api.cobalt.tools/api/json",
    "https://cobalt.crst.it/api/json",
    "https://api.wuk.sh/api/json",
    "https://co.wuk.sh/api/json",
    "https://cobalt.xy24.eu.org/api/json"
]

# Фейковые заголовки, чтобы нас не банили
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36"
]

class AdminStates(StatesGroup):
    waiting_for_broadcast = State()

# --- БАЗА ДАННЫХ ---
def get_db():
    return sqlite3.connect(str(BASE_DIR / "database.db"), check_same_thread=False)

def init_db():
    with get_db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, joined TEXT, downloads_count INTEGER DEFAULT 0, referred_by INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS url_shorter (id TEXT PRIMARY KEY, url TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        for s in ['tiktok', 'instagram', 'vk', 'pinterest', 'youtube']:
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (f"stat_{s}",))
        conn.commit()

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return m.status in ["member", "administrator", "creator"]
    except: return False

def update_stat(url: str):
    service = "other"
    if "tiktok" in url: service = "tiktok"
    elif "instagram" in url: service = "instagram"
    elif "pin" in url: service = "pinterest"
    elif "youtu" in url: service = "youtube"
    try:
        with get_db() as conn:
            conn.execute("UPDATE settings SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (f"stat_{service}",))
            conn.commit()
    except: pass

# --- ЛОГИКА ЗАГРУЗКИ ---

async def resolve_url(url: str) -> str:
    """Разворачивает ссылки pin.it, чтобы API их понял"""
    if "pin.it" not in url and "t.co" not in url:
        return url
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True, timeout=5) as resp:
                return str(resp.url)
    except:
        return url

async def get_media_link(url: str, mode: str) -> str | None:
    """Перебирает зеркала, пока не найдет рабочее"""
    clean_url = await resolve_url(url)
    
    payload = {
        "url": clean_url,
        "vCodec": "h264",
        "isAudioOnly": mode == "audio",
        "aFormat": "mp3"
    }
    
    async with aiohttp.ClientSession() as session:
        for api in COBALT_MIRRORS:
            try:
                # Каждый раз меняем User-Agent
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": random.choice(USER_AGENTS)
                }
                async with session.post(api, json=payload, headers=headers, timeout=8) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Разные форматы ответа Cobalt
                        if data.get("url"): return data["url"]
                        if data.get("picker"): return data["picker"][0]["url"]
                        if data.get("audio"): return data["audio"]
            except Exception as e:
                logger.warning(f"Зеркало {api} сбой: {e}")
                continue
    return None

# --- ХЕНДЛЕРЫ ---

@dp.message(Command("start"))
async def start(message: Message, command: CommandObject):
    user_id = message.from_user.id
    ref = command.args
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, username, joined, referred_by) VALUES (?, ?, ?, ?)", 
                    (user_id, message.from_user.username, datetime.now().isoformat(), ref if ref and ref.isdigit() else None))
        conn.commit()
    
    kb = [
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/Bns_support")]
    ]
    if user_id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")])

    await message.answer(f"👋 <b>Привет! Я {BOT_USERNAME}</b>\n\nПришли мне ссылку на Instagram, Pinterest, TikTok или YouTube.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(Command("ping"))
async def ping(message: Message):
    """Команда для проверки жизни бота"""
    await message.answer(f"🏓 <b>Pong!</b>\nServer: Render\nWebhook: {'Active' if WEBHOOK_URL else 'Not Set'}")

@dp.message(F.text.regexp(r"http"))
async def process_link(message: Message):
    user_id = message.from_user.id
    url = message.text.strip()
    
    # Лимит
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count FROM users WHERE user_id=?", (user_id,)).fetchone()
        count = res[0] if res else 0
    
    if count >= FREE_LIMIT and not await is_subscribed(user_id):
        return await message.answer("🔒 <b>Лимит исчерпан!</b> Подпишись на канал @Bns_888", 
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Подписаться", url=CHANNEL_URL)], [InlineKeyboardButton(text="Проверить", callback_data="check_sub")]]))

    # Сохраняем ссылку
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO url_shorter VALUES (?, ?)", (url_hash, url))
        conn.commit()
    
    kb = [[InlineKeyboardButton(text="📹 Видео", callback_data=f"d_v_{url_hash}"), 
           InlineKeyboardButton(text="🎵 Аудио", callback_data=f"d_a_{url_hash}")]]
    await message.answer("⬇️ <b>Что качаем?</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("d_"))
async def download(c: CallbackQuery):
    _, mode_char, uid = c.data.split("_")
    mode = "video" if mode_char == "v" else "audio"
    
    with get_db() as conn:
        res = conn.execute("SELECT url FROM url_shorter WHERE id=?", (uid,)).fetchone()
    if not res: return await c.answer("Ошибка ссылки")
    
    msg = await c.message.edit_text("⏳ <b>Ищу зеркало для загрузки...</b>")
    
    try:
        async with ChatActionSender(bot=bot, chat_id=c.from_user.id, action="upload_video" if mode == "video" else "upload_voice"):
            direct_link = await get_media_link(res[0], mode)
            
            if not direct_link:
                return await msg.edit_text("❌ <b>Ошибка загрузки.</b>\nСерверы перегружены или профиль закрыт.")
            
            cap = f"📥 @{BOT_USERNAME}"
            if mode == "video":
                await bot.send_video(c.from_user.id, video=direct_link, caption=cap)
            else:
                await bot.send_audio(c.from_user.id, audio=direct_link, caption=cap)
            
            # Успех
            with get_db() as conn:
                conn.execute("UPDATE users SET downloads_count = downloads_count + 1 WHERE user_id=?", (c.from_user.id,))
                conn.commit()
            update_stat(res[0])
            await bot.send_sticker(c.from_user.id, SUCCESS_STICKER)
            await msg.delete()
            
    except Exception as e:
        logger.error(f"Send Error: {e}")
        await msg.edit_text("❌ Не удалось отправить файл (возможно, он слишком большой).")

# --- ПРОФИЛЬ И АДМИНКА ---

@dp.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    user_id = c.from_user.id
    with get_db() as conn:
        res = conn.execute("SELECT downloads_count, referred_by FROM users WHERE user_id=?", (user_id,)).fetchone()
        refs = conn.execute("SELECT COUNT(*) FROM users WHERE referred_by=?", (user_id,)).fetchone()[0]
    
    sub = "✅ Есть" if await is_subscribed(user_id) else "❌ Нет"
    txt = f"👤 <b>Профиль</b>\n🆔: <code>{user_id}</code>\n📥 Скачано: {res[0]}\n👥 Рефералов: {refs}\n💎 Подписка: {sub}\n\n🔗 <code>https://t.me/{BOT_USERNAME}?start={user_id}</code>"
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back")]]))

@dp.callback_query(F.data == "back")
async def back(c: CallbackQuery):
    await c.message.delete()
    await start(c.message, CommandObject(command="start"))

@dp.callback_query(F.data == "check_sub")
async def check_s(c: CallbackQuery):
    if await is_subscribed(c.from_user.id): await c.message.edit_text("✅ Подписка активна! Жду ссылку.")
    else: await c.answer("❌ Нет подписки!", show_alert=True)

@dp.callback_query(F.data == "admin")
async def admin(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    with get_db() as conn:
        users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        stats = dict(conn.execute("SELECT key, value FROM settings").fetchall())
    
    txt = f"📊 <b>Админка</b>\n👥 Людей: {users}\n📷 Insta: {stats.get('stat_instagram',0)}\n💃 TikTok: {stats.get('stat_tiktok',0)}\n📌 Pin: {stats.get('stat_pinterest',0)}"
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Рассылка", callback_data="bc")], [InlineKeyboardButton(text="Назад", callback_data="back")]]))

@dp.callback_query(F.data == "bc")
async def bc(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("✍️ Пришли сообщение для рассылки:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@dp.message(AdminStates.waiting_for_broadcast)
async def do_bc(m: Message, state: FSMContext):
    await m.answer("🚀 Рассылка началась...")
    with get_db() as conn: users = conn.execute("SELECT user_id FROM users").fetchall()
    count = 0
    for u in users:
        try:
            await bot.copy_message(u[0], m.chat.id, m.message_id)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await m.answer(f"✅ Доставлено: {count}")
    await state.clear()

# --- ЗАПУСК ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    try:
        update = Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok", "webhook": WEBHOOK_URL}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))