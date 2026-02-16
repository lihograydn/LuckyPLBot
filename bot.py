import sqlite3
import re
import time
import asyncio
import logging
import uuid
import pytz
from aiohttp import web
import json
import os
import random
from aiogram import Bot, Dispatcher, types, executor
from datetime import datetime, timedelta
from aiogram.dispatcher import FSMContext
from collections import Counter
from aiogram.types import LabeledPrice, ContentType, PreCheckoutQuery, ChatMemberUpdated, Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from collections import deque
from aiogram.dispatcher.filters import Text
from aiogram.utils.callback_data import CallbackData
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import exceptions
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler


# ================== НАСТРОЙКИ ==================
MAIN_BOT_TOKEN = '8091475747:AAH6hmuh615lKKvFQmE1QTSMxpqHNNKPKuE'
ADMIN_IDS = [5826298831, 000]
BOT_USERNAME = 'LuckyPLBot'
BOT_ENABLED = True
logging.basicConfig(level=logging.INFO)
bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(bot)

DB_PATH = "users.db"
WEB_APP_URL = "http://localhost:8080"


# ================== ФУНКЦИИ ДЛЯ РАБОТЫ С БД ==================

def get_ban_info(user_id):
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()


def get_conn():
    """Создание нового подключения к базе"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализация всех таблиц"""
    with get_conn() as conn:
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            balance REAL DEFAULT 0,
            games_played INTEGER DEFAULT 0,
            lost REAL DEFAULT 0,
            reg_date TEXT,
            last_bonus INTEGER DEFAULT 0,
            ban_until TEXT,
            ban_reason TEXT,
            nickname TEXT,
            chat_id INTEGER,
            status TEXT DEFAULT NULL,
            last_command_time TEXT DEFAULT NULL,
            ref_code TEXT
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_promo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            promo_code TEXT,
            activation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (promo_code) REFERENCES promo_codes(code)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount REAL,
            activations INTEGER,
            one_time_per_user INTEGER DEFAULT 0,
            description TEXT DEFAULT ''
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS deposits (
            user_id INTEGER PRIMARY KEY,
            amount REAL DEFAULT 0,
            deposit_time TEXT,
            term_days INTEGER DEFAULT 7
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_statuses (
            user_id INTEGER,
            status_name TEXT,
            status_id INTEGER,
            PRIMARY KEY (user_id, status_id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_market (
            market_id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER NOT NULL,
            status_name TEXT NOT NULL,
            status_id INTEGER NOT NULL,
            price INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS invites (
            user_id INTEGER,
            invited_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(user_id, invited_id),
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(invited_id) REFERENCES users(user_id)
        )
        ''')

        conn.commit()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS message_earnings (
            user_id INTEGER,
            chat_id INTEGER,
            messages INTEGER DEFAULT 0,
            earned INTEGER DEFAULT 0,
            withdrawn INTEGER DEFAULT 0,
            last_message_time INTEGER DEFAULT 0,
            last_message_text TEXT DEFAULT '',
            repeat_count INTEGER DEFAULT 0,
            blocked_until INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, chat_id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS game_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            game TEXT,
            bet INTEGER,
            result TEXT,
            multiplier REAL DEFAULT 0,
            win INTEGER DEFAULT 0,
            created_at TEXT
        )
        ''')


# Запускаем инициализацию при старте
init_db()



# ================== ПРОСТАЯ ПРОВЕРКА НАГРУЗКИ ==================
MAX_ACTIVE_USERS = 3
active_users = set()

async def check_load(user_id):
    active_users.add(user_id)
    if len(active_users) > MAX_ACTIVE_USERS:
        return False
    return True

async def release_user(user_id):
    if user_id in active_users:
        active_users.remove(user_id)






def add_game_history(user_id, game, bet, result, multiplier=0, win=0):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO game_history
            (user_id, game, bet, result, multiplier, win, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                game,
                bet,
                result,
                multiplier,
                win,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        )
        conn.commit()


from aiogram.utils.exceptions import RetryAfter, MessageNotModified, InvalidQueryID

async def safe_edit(message, text, reply_markup=None):
    try:
        if message.text != text or message.reply_markup != reply_markup:
            await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except RetryAfter as e:
        await asyncio.sleep(e.timeout)
        await safe_edit(message, text, reply_markup)

async def safe_answer_callback(callback_query, text=None, show_alert=False):
    try:
        await callback_query.answer(text=text, show_alert=show_alert)
    except RetryAfter as e:
        await asyncio.sleep(e.timeout)
        await safe_answer_callback(callback_query, text=text, show_alert=show_alert)
    except InvalidQueryID:
        # Игнорируем старые коллбеки
        pass

last_click_time = {}

def can_click(user_id):
    now = time()
    if user_id in last_click_time and now - last_click_time[user_id] < 1.5:  # 1.5 сек между кликами
        return False
    last_click_time[user_id] = now
    return True

# ================== ПРИМЕР ФУНКЦИЙ ==================
def update_balance(user_id: int, delta: float):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
            (delta, user_id)
        )
        conn.commit()

def get_user(user_id: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()

last_command_time = {}
COMMAND_COOLDOWN = 2  # Задержка в секундах

async def is_command_allowed(user_id):
    """Проверяет, можно ли пользователю использовать команду."""
    now = datetime.now()
    if user_id in last_command_time:
        time_since_last_command = now - last_command_time[user_id]
        if time_since_last_command < timedelta(seconds=COMMAND_COOLDOWN):
            return False
    return True

async def update_last_command_time(user_id):
    """Обновляет время последнего использования команды для пользователя."""
    last_command_time[user_id] = datetime.now()


# Словарь для хранения ID пользователя, создавшего клавиатуру, по ID сообщения
keyboard_ownership = {}

def format_number(num):
    return f"{int(round(num)):,}".replace(',', "'")

# Исправленная функция format_stake принимает два аргумента: stake_str и balance
def format_stake(stake_str: str, balance: int) -> int:
    """
    Преобразует строку с сокращениями в целое число.
    Примеры:
    '1к' -> 1000
    '1.5к' -> 1500
    '2кк' -> 2_000_000
    '3.2ккк' -> 3_200_000_000
    'все' -> balance
    """
    stake_str = stake_str.lower().replace(' ', '')

    if stake_str == 'все':
        return int(round(balance))

    multipliers = {
        'кккккк': 10**18,
        'ккккк': 10**15,
        'кккк': 10**12,
        'ккк': 10**9,
        'кк': 10**6,
        'к': 10**3,
    }

    # Проверяем, есть ли в конце суффикс (больше длина проверяется первой)
    for suffix, multiplier in sorted(list(multipliers.items()), key=lambda x: -len(x[0])):
        if stake_str.endswith(suffix):
            number_part = stake_str[:-len(suffix)]
            try:
                value = float(number_part)
                return int(round(value * multiplier))
            except ValueError:
                return -1  # ошибка парсинга
    # Если без суффикса
    try:
        value = float(stake_str)
        if value.is_integer():
            return int(value)
        else:
            return int(round(value))
    except ValueError:
        return -1


async def rate_limit(user_id: int):
    """Декоратор для ограничения частоты запросов."""
    now = time.time()
    if user_id in ACTIVE_MINES_GAMES:
        last_click_time = ACTIVE_MINES_GAMES[user_id].get("last_click")
        if last_click_time and (now - last_click_time) < RATE_LIMIT:
            return False
    return True


async def apply_rate_limit(user_id: int):
    """Применяет ограничение по частоте запросов."""
    if user_id in ACTIVE_MINES_GAMES:
        ACTIVE_MINES_GAMES[user_id]["last_click"] = time.time()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def get_user_info(user_id_or_username: str) -> tuple[int | None, str | None]:
    """
    Поиск пользователя по ID или username. Возвращает (user_id, username) или (None, None)
    """
    try:
        user_id = int(user_id_or_username)
        cursor.execute('SELECT username FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            return user_id, result[0]
        return None, None
    except ValueError:
        cursor.execute('SELECT user_id, username FROM users WHERE username = ?', (user_id_or_username,))
        result = cursor.fetchone()
        if result:
            return result[0], result[1]
        return None, None

class BotOffMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: types.Message, data: dict):
        global BOT_ENABLED

        # Админы всегда могут пользоваться ботом
        if message.from_user.id in ADMIN_IDS:
            return

        # Если бот выключен — блокируем всех остальных
        if not BOT_ENABLED:
            raise CancelHandler()

dp.middleware.setup(BotOffMiddleware())

@dp.message_handler(commands=["off"])
async def off_cmd(message: types.Message):
    global BOT_ENABLED

    if message.from_user.id not in ADMIN_IDS:
        return

    BOT_ENABLED = False
    await message.reply("🔴 Бот ВЫКЛЮЧЕН. Работает только для администраторов.")

@dp.message_handler(commands=["on"])
async def on_cmd(message: types.Message):
    global BOT_ENABLED

    if message.from_user.id not in ADMIN_IDS:
        return

    BOT_ENABLED = True
    await message.reply("🟢 Бот ВКЛЮЧЕН. Работает для всех.")

user_last_start = {}

# --- Текст приветствия ---
def get_start_text(first_name):
    return (
        f"<b>🎉 Добро пожаловать {first_name}</b>\n\n"
        "Я — <b>игровой бот LuckyPL</b>, в нем ты можешь делать:\n"
        "🎮 <i>Играть</i> в различные и разнообразные игры\n"
        "🏆 <i>Коллекционировать</i> эксклюзивные статусы\n"
        "🤝 А также просто находить новых друзей в нашем чатике\n"
        "🎲 Во что будешь играть первым?\n\n"
        "❓ На случай если есть вопросы —> <code>/help</code>"
    )

# --- Кнопки стартового меню ---
keyboard_start = InlineKeyboardMarkup(row_width=3)
keyboard_start.add(
    InlineKeyboardButton("📢 Канал", url="https://t.me/LuckyPLchanel"),
    InlineKeyboardButton("💬 Чат", url="https://t.me/ChatLuckyPL"),
    InlineKeyboardButton("📜 Правила", callback_data="show_rules")
)

keyboard_rules = InlineKeyboardMarkup(row_width=1)
keyboard_rules.add(
    InlineKeyboardButton("🔙 Назад", callback_data="back_to_start"),
    InlineKeyboardButton("📄 Правила (В виде TelegramPH)", url="https://telegra.ph/Pravila-bota-LuckyPLBot-01-28")
)

# --- Текст правил ---
rules_text = (
    "<b>📜 Правила проекта @LuckyPLbot</b>\n\n"
    "1️⃣ <b>Реклама в топе</b>: запрещена — бан 30 дней (каналы, казино, сторонние сервисы)\n\n"
    "2️⃣ <b>Дюп (баг с дублированием)</b>:\n"
    "   • Расскажешь — получишь вознаграждение\n"
    "   • Не расскажешь — бан навсегда\n"
    "   • Отмазки типа «я случайно» или «я так больше не буду» — не работают\n\n"
    "3️⃣ <b>Обман с продажей статусов</b>: запрещён — бан на 30 дней\n\n"
    "4️⃣ <b>Незнание правил</b>: не освобождает от ответственности\n"
    "   • Нарушение наказывается блокировкой\n"
    "   • «Я не читал правила» — это не оправдание\n\n"
    "5️⃣ <b>Выдача себя за админов и ложь</b>: запрещено — бан от 7 дней до бессрочного\n"
    "   • Фразы «я просто прикалывался» — не спасут\n\n"
    "6️⃣ <b>Ответственность</b>:\n"
    "   • Все действия с твоего аккаунта — твоя ответственность\n"
    "   • «Это друг зашёл» — это не оправдание\n\n"
    "7️⃣ <b>Администрация</b>: вправе блокировать за любые нарушения без предупреждений"
)

# --- Обработчик команды /start ---
@dp.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    username = message.from_user.username
    chat_id = message.chat.id

    now = datetime.now()
    user_last_start[user_id] = now

    args = message.get_args()
    coupon_code = None
    if args and args.startswith("coupon_"):
        coupon_code = args.strip()

    # --- Работа с БД ---
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            # Регистрация нового пользователя
            reg_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                '''
                INSERT INTO users (
                    user_id, username, first_name, last_name,
                    balance, games_played, lost, reg_date, chat_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (user_id, username, first_name, last_name, 0, 0, 0, reg_date, chat_id)
            )
            conn.commit()

    # --- Отправка приветствия ---
    await message.reply(
        get_start_text(first_name),
        parse_mode="HTML",
        reply_markup=keyboard_start
    )


# --- Обработчики кнопок ---
@dp.callback_query_handler(lambda c: c.data == "show_rules")
async def rules_callback(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text(
        rules_text,
        parse_mode="HTML",
        reply_markup=keyboard_rules
    )
    await safe_answer_callback(callback_query)


@dp.callback_query_handler(lambda c: c.data == "back_to_start")
async def back_callback(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text(
        get_start_text(callback_query.from_user.first_name),
        parse_mode="HTML",
        reply_markup=keyboard_start
    )
    await safe_answer_callback(callback_query)    

GAME_COMMANDS = [
    ("/dice", "или кубик — начать игру в Кубик 🎲"),
    ("/cubes", "или кости — начать игру в Кости 🎲"),
    ("/hunt (ставка)", "или охота (ставка) — начать игру в Охоту 🔫"),
    ("/rul (ставка)", "или рул (ставка) — поставить на игру в Рулетку 🍒"),
    ("/log", "или лог — посмотреть лог Рулетки 📌"),
    ("/cancel", "или отмена — отменить ставку в Рулетке 📌"),
    ("/rates", "или ставки — посмотреть ставки на Рулетку 📌"),
    ("/go", "или го — начать игру в Рулетку 🍒"),
    ("/gold (ставка)", "или золото (ставка) — начать игру в Золото 🌕"),
    ("/crash (ставка) (множитель)", "— начать игру в Краш 🚀"),
    ("/21 (ставка)", "или бж (ставка) — начать игру в 21 ♥️"),
    ("/slots (ставка)", "или слоты (ставка) — начать игру в Слоты 🎰"),
    ("/chips (ставка) (тип)", "или фишки (ставка) (тип) — начать игру в Фишки 🔴🔵"),
    ("/mines (ставка)", "или мины (ставка) — начать игру в Мины 💣"),
    ("/chests (ставка)", "или честы (ставка) — начать игру в Сундуки удачи 🧰"),
    ("крестики (ставка)", "— начать игру в Крестики-нолики 3x3 ❌⭕ (2 игрока)"),
    ("/tower (ставка) (кол-во мин)", "или башня (ставка) (кол-во мин) — начать игру в Башню 🗼"),
    ("/hilo (ставка)", "или хило (ставка) — начать игру в HiLo 🎴"),
    ("/кнб", "— сыграть в Камень ✂️ Ножницы 📄 Бумага на PLcoins 🎮"),
    ("/vilin (ставка)", "или вилин (ставка) — начать игру в Вилин 🎮"),
    ("/plinko (ставка)", "или плинко (ставка) — начать игру в Плинко 🎯"),
    ("/duel (ставка)", "или дуэль (ставка) — начать дуэль кубов с другим игроком 🎲")  # ← добавлено
]


MAIN_COMMANDS = [
    ("/start", "— запустить бота"),
    ("/help", "или помощь — список команд"),
    ("/balance", "или баланс или Б — показать баланс"),
    ("/bonus", "или бонус — получить случайный бонус раз в час"),
    ("/nick", "или +ник — создать свой ник для топа 🏷"),
    ("/give", "или дать — передать PLcoins другому игроку"),
    ("/top", "или топ — показать топ 10 игроков по балансу 🏆"),
    ("/profile", "или профиль — показать профиль пользователя 📊"),
    ("/promo", "или /pr или промо — активировать промокод 🎟️ (пример: /promo bonus)"),
    ("/bank", "или банк — положить или снять PLcoins с депозита, получить +10% прибыли за 7 дней 🏦"),
    ("/рынок", "— открыть меню рынка статусов 🛒"),
    ("/sell или селл", "— выставить статус на продажу /sell &lt;ID_статуса&gt; &lt;цена&gt; 📤"),
    ("/unsell или ансел", "— снять статус с продажи /unsell &lt;ID_объявления&gt; ❌"),
    ("статусы или /status", "— показать ваши статусы 📋"),
    ("статус лист или /status_list", "— все возможные статусы"),
    ("/history", "или история — показать историю игр"),
    ("/donat_list", "или донат лист — цены на все статусы а так же PLcoins")
]



def create_game_help_text():
    lines = ["<b>Список доступных игр:</b>"]
    for cmd, desc in GAME_COMMANDS:
        # Можно выделить команды тегом <code> для моноширинного шрифта
        lines.append(f"<code>{cmd}</code> {desc}")
    return "\n".join(lines)

def create_main_help_text():
    lines = ["<b>Основные команды:</b>"]
    for cmd, desc in MAIN_COMMANDS:
        lines.append(f"<code>{cmd}</code> {desc}")
    return "\n".join(lines)


# Добавляем кнопку в главное меню
main_menu_keyboard = InlineKeyboardMarkup(row_width=2)
main_menu_keyboard.add(
    InlineKeyboardButton("Игры🎮", callback_data="help_games"),
    InlineKeyboardButton("Основные🪪", callback_data="help_main")
)
back_button = InlineKeyboardMarkup().add(
    InlineKeyboardButton("Назад◀️", callback_data="help_back")
)

# Обработчик кнопки «Правила»
@dp.callback_query_handler(lambda query: query.data == "help_rules")
async def help_rules_callback(query: types.CallbackQuery):
    user_id = query.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await query.answer(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                show_alert=True
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    message_id = query.message.message_id
    owner_id = keyboard_ownership.get(message_id)

    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну, это не твои кнопки.", show_alert=True)
        return

    text = create_rules_text()
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=back_button
    )
    await query.answer()

@dp.message_handler(lambda message: message.text and message.text.lower() in ['помощь'])
@dp.message_handler(commands=['help'])
async def help_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    msg = await message.reply(
    "Вы попали в пункт помощи — LuckyHelp💡!\n"
    "Здесь будут все команды, которые тебе могут понадобиться в боте❓\n"
    "Ниже выберите нужный вам пункт 👇",
    reply_markup=main_menu_keyboard
    )
    keyboard_ownership[msg.message_id] = user_id # сохраняем id пользователя и id сообщения

@dp.callback_query_handler(lambda query: query.data == "help_games")
async def help_games_callback(query: types.CallbackQuery):
    user_id = query.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await query.answer(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                show_alert=True
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    message_id = query.message.message_id # Получаем message_id из query
    owner_id = keyboard_ownership.get(message_id)

    # Проверяем, является ли пользователь владельцем клавиатуры
    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну , это не твои кнопки.", show_alert=True)
        return

    text = create_game_help_text()
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=back_button
    )
    await query.answer()

@dp.callback_query_handler(lambda query: query.data == "help_main")
async def help_main_callback(query: types.CallbackQuery):
    user_id = query.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await query.answer(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                show_alert=True
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    message_id = query.message.message_id # Получаем message_id из query
    owner_id = keyboard_ownership.get(message_id)

    # Проверяем, является ли пользователь владельцем клавиатуры
    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну , это не твои кнопки.", show_alert=True)
        return

    text = create_main_help_text()
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=back_button
    )
    await query.answer()

@dp.callback_query_handler(lambda query: query.data == "help_back")
async def help_back_callback(query: types.CallbackQuery):
    user_id = query.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await query.answer(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                show_alert=True
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    message_id = query.message.message_id # Получаем message_id из query
    owner_id = keyboard_ownership.get(message_id)

    # Проверяем, является ли пользователь владельцем клавиатуры
    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну , это не твои кнопки.", show_alert=True)
        return
    user_id = query.from_user.id
    msg = await query.message.edit_text(
    "Вы попали в пункт помощи — LuckyHelp💡!\n"
    "Здесь будут все команды, которые тебе могут понадобиться в боте❓\n"
    "Ниже выберите нужный вам пункт 👇",
    reply_markup=main_menu_keyboard
    )
    keyboard_ownership[msg.message_id] = user_id # Обновляем запись в ownership при возврате назад
    await query.answer()

#=======================
# Функция для проверки, является ли пользователь администратором
def is_admin(user_id):
    return user_id in ADMIN_IDS


@dp.message_handler(lambda message: message.text == '-стат')
async def reset_all_stats_handler(message: types.Message):
    user_id = message.from_user.id

    if not is_admin(user_id):
        await message.reply("У вас нет прав для выполнения этой команды.")
        return

    try:
        # Обнуляем статистику всех пользователей
        cursor.execute('UPDATE users SET balance = 0, games_played = 0, lost = 0')
        conn.commit()
        await message.reply("Статистика всех пользователей успешно сброшена!")
    except Exception as e:
        print(f"Ошибка при сбросе статистики: {e}")
        await message.reply("Произошла ошибка при сбросе статистики.")

@dp.message_handler(lambda message: message.text and message.text.lower() in ['б', 'баланс'])
@dp.message_handler(commands=['balance'])
async def balance_handler(message: types.Message):
    user_id = message.from_user.id

    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()

    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance, games_played, lost = user
    balance_str = format_number(balance)
    lost_str = format_number(lost)

    OWNER_ID = 5826298831
    TECH_ADMIN_ID = 7165256113
    MODER_IDS = [6]
    VERIFIED_PLAYER_ID = 8493326566

    # Для админов — полный блок с верификацией
    if user_id == OWNER_ID:
        text = (
            f"👑 Роль: Владелец проекта\n"
            f"💰 Баланс: {balance_str} PLcoins\n"
            f"🎮 Всего сыграно игр: {games_played}\n"
            f"💸♨️ Всего проиграно: {lost_str} PLcoins\n\n"
            f"<blockquote>✅ Аккаунт верифицирован\n"
            f"Этому пользователю можно доверять при проведении сделок и безопасных взаимодействиях."
            "</blockquote>"
        )
    elif user_id == TECH_ADMIN_ID:
        text = (
            f"👑 Роль: Тех.админ\n"
            f"💰 Баланс: {balance_str} PLcoins\n"
            f"🎮 Всего сыграно игр: {games_played}\n"
            f"💸♨️ Всего проиграно: {lost_str} PLcoins\n\n"
            f"<blockquote>✅ Аккаунт верифицирован\n"
            f"Этому пользователю можно доверять при проведении сделок и безопасных взаимодействиях."
            "</blockquote>"
        )
    elif user_id == VERIFIED_PLAYER_ID:
        text = (
            f"👑 Роль: Игрок\n"
            f"💰 Баланс: {balance_str} PLcoins\n"
            f"🎮 Всего сыграно игр: {games_played}\n"
            f"💸♨️ Всего проиграно: {lost_str} PLcoins\n\n"
            f"<blockquote>✅ Аккаунт верифицирован\n"
            f"Этому пользователю можно доверять при проведении сделок и безопасных взаимодействиях."
            "</blockquote>"
        )
    elif user_id in MODER_IDS:
        # Для модеров — отдельный блок без верификации
        text = (
            f"👑 Роль: Moder\n"
            f"💰 Баланс: {balance_str} PLcoins\n"
            f"🎮 Всего сыграно игр: {games_played}\n"
            f"💸♨️ Всего проиграно: {lost_str} PLcoins"
        )
    else:
        # Обычные игроки — компактно, статус сверху
        text = (
            f"👑 Роль: Игрок\n"
            f"💰 Баланс: {balance_str} PLcoins\n"
            f"🎮 Всего сыграно игр: {games_played}\n"
            f"💸♨️ Всего проиграно: {lost_str} PLcoins"
        )



    await message.reply(text, parse_mode="HTML")
    await apply_rate_limit(user_id)



def short_number(value: int) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}kkk"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.1f}kk"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}k"
    else:
        return str(value)

@dp.message_handler(lambda message: message.text and message.text.lower() == 'профиль')
@dp.message_handler(commands=['profile'])
async def profile_handler(message: types.Message):
    user_id = message.from_user.id

    # Проверка бана
    cursor.execute(
        'SELECT ban_until, ban_reason FROM users WHERE user_id = ?',
        (user_id,)
    )
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}\nПричина: {ban_reason}",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

    # Получаем данные пользователя
    cursor.execute(
        '''
        SELECT balance, games_played, lost, reg_date
        FROM users
        WHERE user_id = ?
        ''',
        (user_id,)
    )
    user = cursor.fetchone()

    if not user:
        await message.reply("❗ Вы не зарегистрированы. Используйте /start")
        return

    balance, games_played, lost, reg_date = user

    # Форматирование чисел
    balance_str = short_number(balance)
    lost_str = short_number(lost)

    # Формат даты регистрации
    if reg_date:
        dt = datetime.strptime(reg_date, '%Y-%m-%d %H:%M:%S')
        reg_date_str = dt.strftime('%d.%m.%Y %H:%M')
    else:
        reg_date_str = "неизвестно"

    # Текст профиля
    text = (
        f"<b>👤 Профиль игрока</b>\n\n"
        f"<blockquote>ID: {user_id}</blockquote>\n"
        f"<b>🎮 Всего игр:</b> {games_played}\n"
        f"<b>💰 Баланс:</b> {balance_str} PLcoins\n"
        f"<b>💸 Всего проиграно:</b> {lost_str} PLcoins\n"
        f"<b>📊 Регистрация:</b> {reg_date_str}"
    )

    await message.reply(text, parse_mode="HTML")


#=======================
# Создание промокода (только админы)
@dp.message_handler(commands=['new_promo'])
async def new_promo_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет прав для создания промокодов.", parse_mode="HTML")
        return

    args = message.get_args().split()
    if len(args) != 3:
        await message.reply("❗ Использование:\n/new_promo (название) (сумма) (активаций)", parse_mode="HTML")
        return

    code, amount_str, activations_str = args

    # Получаем баланс пользователя для передачи в format_stake
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    balance = user_data[0] if user_data else 0

    amount = format_stake(amount_str, balance)
    if amount <= 0:
        await message.reply("❗ Некорректная сумма промокода.", parse_mode="HTML")
        return

    try:
        activations = int(activations_str)
    except ValueError:
        await message.reply("❗ Количество активаций должно быть целым числом.", parse_mode="HTML")
        return

    cursor.execute('SELECT * FROM promo_codes WHERE code = ?', (code,))
    if cursor.fetchone():
        await message.reply("❗ Промокод с таким названием уже существует.", parse_mode="HTML")
        return

    cursor.execute('INSERT INTO promo_codes (code, amount, activations) VALUES (?, ?, ?)', (code, amount, activations))
    conn.commit()

    text = (
        "🎉 <b>Промокод создан!</b>\n"
        f"🔤 <b>Название:</b> <code>{code}</code>\n"
        f"💰 <b>Сумма:</b> <code>{amount}</code> PLcoins\n"
        f"🎟️ <b>Активаций:</b> <code>{activations}</code>\n"
        f"Использовать: <code>/promo {code}</code>"
    )
    await message.reply(text, parse_mode="HTML")

@dp.message_handler(lambda message: message.text and message.text.lower().startswith('дпромо'))
async def dpromo_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет прав для создания промокодов.", parse_mode="HTML")
        return

    parts = message.text.split()
    if len(parts) != 4:
        await message.reply("❗ Использование:\nдпромо (название) (сумма) (активаций)", parse_mode="HTML")
        return

    _, code, amount_str, activations_str = parts

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    balance = user_data[0] if user_data else 0

    amount = format_stake(amount_str, balance)
    if amount <= 0:
        await message.reply("❗ Некорректная сумма промокода.", parse_mode="HTML")
        return

    try:
        activations = int(activations_str)
    except ValueError:
        await message.reply("❗ Количество активаций должно быть целым числом.", parse_mode="HTML")
        return

    cursor.execute('SELECT * FROM promo_codes WHERE code = ?', (code,))
    if cursor.fetchone():
        await message.reply("❗ Промокод с таким названием уже существует.", parse_mode="HTML")
        return

    cursor.execute('INSERT INTO promo_codes (code, amount, activations) VALUES (?, ?, ?)', (code, amount, activations))
    conn.commit()

    text = (
        "🎉 <b>Промокод создан!</b>\n"
        f"🔤 <b>Название:</b> <code>{code}</code>\n"
        f"💰 <b>Сумма:</b> <code>{amount}</code> PLcoins\n"
        f"🎟️ <b>Активаций:</b> <code>{activations}</code>\n"
        f"Использовать: <code>/promo {code}</code>"
    )
    await message.reply(text, parse_mode="HTML")


#=======================
# Активация промокода
@dp.message_handler(lambda message: message.text and (
    message.text.lower().startswith('/promo ') or
    message.text.lower().startswith('/pr ') or
    message.text.lower().startswith('промо ')
))
async def activate_promo_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    text = message.text.strip()
    # Получаем код после команды
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("❗ Укажите название промокода. Пример: /promo bot")
        return

    code = parts[1].strip().lower()

    # Проверяем, активировал ли пользователь этот промокод ранее
    cursor.execute('SELECT 1 FROM user_promo_codes WHERE user_id = ? AND promo_code = ?', (user_id, code))
    if cursor.fetchone():
        await message.reply("Вы уже активировали этот промокод❗️")
        return


    cursor.execute('SELECT amount, activations FROM promo_codes WHERE code = ?', (code,))
    promo = cursor.fetchone()

    if not promo:
        await message.reply("❌ Промокод не найден или истек срок действия.")
        return

    amount, activations = promo
    if activations <= 0:
        await message.reply("❌ У этого промокода закончились активации.")
        return

    user_id = message.from_user.id
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    # Начинаем транзакцию
    try:
        new_balance = user[0] + amount
        new_activations = activations - 1

        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        cursor.execute('UPDATE promo_codes SET activations = ? WHERE code = ?', (new_activations, code))

        # Записываем в таблицу user_promo_codes, что пользователь активировал этот промокод
        cursor.execute('INSERT INTO user_promo_codes (user_id, promo_code) VALUES (?, ?)', (user_id, code))

        conn.commit()

        text = (
            "<b>✅ Промокод активирован!</b>\n"
            f"<b>💰 Ты получил:</b> +{format_number(amount)} PLcoins.\n"
            f"<b>Осталось активаций:</b> {new_activations}"
        )
        await message.reply(text, parse_mode="HTML")

    except Exception as e:
        conn.rollback()
        await message.reply("❌ Произошла ошибка при активации промокода.")
        print(f"Ошибка при активации промокода: {e}")


#=======================

@dp.message_handler(lambda message: (is_admin(message.from_user.id) or message.from_user.id == 7049811977) and message.text.lower().startswith('выдать'))
async def give_admin_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❗ Укажите сумму для выдачи. Пример: Выдать 100", parse_mode="HTML")
        return

    amount_str = parts[1].strip().lower()

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    balance = user_data[0] if user_data else 0

    amount = format_stake(amount_str, balance)
    if amount <= 0:
        await message.reply("❗ Укажите корректную положительную сумму для выдачи.", parse_mode="HTML")
        return

    if message.reply_to_message:
        recipient = message.reply_to_message.from_user
        recipient_id = recipient.id
        recipient_username = recipient.username if recipient.username else f"ID:{recipient_id}"
    elif len(parts) >= 3:
        recipient_id_or_username = parts[2].strip()
        recipient_id, recipient_username = await get_user_info(recipient_id_or_username)
        if not recipient_id:
            await message.reply("❗ Пользователь не найден.", parse_mode="HTML")
            return
    else:
        await message.reply("❗ Чтобы выдать монеты, ответьте на сообщение пользователя и напишите сумму, или укажите ID/Username.", parse_mode="HTML")
        return

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (recipient_id,))
    recipient_data = cursor.fetchone()
    if not recipient_data:
        await message.reply("❗ Пользователь не зарегистрирован.", parse_mode="HTML")
        return

    recipient_balance = recipient_data[0]
    new_recipient_balance = recipient_balance + amount

    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_recipient_balance, recipient_id))
    conn.commit()

    formatted_amount = format_number(amount)
    text = f"🪄 Пользователю <b>{recipient_username}</b> было выдано: +<b>{formatted_amount} PLCoins</b>"
    await message.reply(text, parse_mode="HTML")

@dp.message_handler(lambda message: (is_admin(message.from_user.id) or message.from_user.id == 7049811977) and message.text.lower().startswith('забрать'))
async def take_admin_handler(message: types.Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❗ Укажите сумму для изъятия. Пример: Забрать 100", parse_mode="HTML")
        return

    amount_str = parts[1].strip().lower()

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (message.from_user.id,))
    user_data = cursor.fetchone()
    balance = user_data[0] if user_data else 0

    amount = format_stake(amount_str, balance)
    if amount <= 0:
        await message.reply("❗ Укажите корректную положительную сумму для изъятия.", parse_mode="HTML")
        return

    if message.reply_to_message:
        recipient = message.reply_to_message.from_user
        recipient_id = recipient.id
        recipient_username = recipient.username if recipient.username else f"ID:{recipient_id}"
    elif len(parts) >= 3:
        recipient_id_or_username = parts[2].strip()
        recipient_id, recipient_username = await get_user_info(recipient_id_or_username)
        if not recipient_id:
            await message.reply("❗ Пользователь не найден.", parse_mode="HTML")
            return
    else:
        await message.reply("❗ Чтобы забрать монеты, ответьте на сообщение пользователя и напишите сумму, или укажите ID/Username.", parse_mode="HTML")
        return

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (recipient_id,))
    recipient_data = cursor.fetchone()
    if not recipient_data:
        await message.reply("❗ Пользователь не зарегистрирован.", parse_mode="HTML")
        return

    recipient_balance = recipient_data[0]
    new_recipient_balance = max(0, recipient_balance - amount)  # Не даем балансу уйти в минус

    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_recipient_balance, recipient_id))
    conn.commit()

    formatted_amount = format_number(amount)
    text = f"💢 Пользователь {recipient_username} был лишен: <b>{formatted_amount} PLCoins</b>"
    await message.reply(text, parse_mode="HTML")

#=======================
@dp.message_handler(lambda message: is_admin(message.from_user.id) and message.text.lower() == 'обнул')
async def reset_all_balances_handler(message: types.Message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    # ================= Проверка бана =================
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы заблокированы до: {ban_until}, причина: {ban_reason} 🚫",
                parse_mode="HTML"
            )
            conn.close()
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # ================= Обнуление данных =================
    try:
        # 1. Баланс
        cursor.execute('UPDATE users SET balance = 0')

        # 2. Статистика игр
        cursor.execute('UPDATE users SET lost = 0, games_played = 0')

        # 3. Банк / депозиты
        cursor.execute('DELETE FROM deposits')

        # 4. Промокоды
        cursor.execute('DELETE FROM user_promo_codes')  # история активаций
        cursor.execute('DELETE FROM promo_codes')       # сами коды

        # 5. Активные игры / чеки
        cursor.execute('DELETE FROM checks')
    except sqlite3.OperationalError as e:
        print(f"[WARN] Ошибка при обнулении таблиц: {e}")

    conn.commit()
    conn.close()

    # ================= Очистка активных игр в памяти =================
    # Если у тебя есть словари для хранения текущих игр, обнуляем их
    global mines_games, active_games
    try:
        mines_games.clear()
    except NameError:
        pass
    try:
        active_games.clear()
    except NameError:
        pass

    await message.reply("📛 Весь сервер бота был обнулен до 0!", parse_mode="HTML")



#=======================

@dp.message_handler(lambda message: is_admin(message.from_user.id) and message.text.lower().startswith('сетбал'))
async def set_balance_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❗ Укажите ID или Username пользователя.", parse_mode="HTML")
        return

    user_id_or_username = parts[1].strip()
    user_id, username = await get_user_info(user_id_or_username)

    if not user_id:
        await message.reply("❗ Пользователь не найден.", parse_mode="HTML")
        return

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❗ Пользователь не зарегистрирован.", parse_mode="HTML")
        return

    cursor.execute('UPDATE users SET balance = 0 WHERE user_id = ?', (user_id,))
    conn.commit()

    username_display = username if username else f"ID:{user_id}"
    text = f"🚷 Пользователь {username_display} был обнулен!"
    await message.reply(text, parse_mode="HTML")

#=======================


def parse_time_string(time_string: str) -> timedelta | None:
    """Парсит строку времени и возвращает timedelta."""
    time_string = time_string.lower()
    number = int(''.join(filter(str.isdigit, time_string)))
    if 'мин' in time_string or 'm' in time_string:
        return timedelta(minutes=number)
    elif 'ч' in time_string or 'h' in time_string:
        return timedelta(hours=number)
    elif 'д' in time_string or 'd' in time_string:
        return timedelta(days=number)
    elif 'мес' in time_string:
        return timedelta(days=number * 30)  # Приближение
    elif 'год' in time_string or 'y' in time_string:
        return timedelta(days=number * 365)  # Приближение
    else:
        return None


@dp.message_handler(commands=['ban'])
async def ban_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет прав для выполнения этой команды.", parse_mode="HTML")
        return

    parts = message.text.split(maxsplit=3)  # /ban id причина время
    if len(parts) < 4 and len(parts) >1: # Айди юзернейм
        await message.reply("❗ Используйте: /ban (ID или username) (причина) (время)", parse_mode="HTML")
        return
    if len(parts) < 2 :
        await message.reply("❗ Используйте: /ban (ID или username) (причина) (время)", parse_mode="HTML")
        return

    _, target_id_or_username, reason, time_str = parts
    target_id, target_username = await get_user_info(target_id_or_username)

    if not target_id:
        await message.reply("❗ Пользователь не найден.", parse_mode="HTML")
        return

    if target_id in ADMIN_IDS:
        await message.reply("❗ Нельзя забанить администратора.", parse_mode="HTML")
        return

    if time_str.lower() == 'навсегда':
        ban_until = None  # Бан навсегда (NULL)
        time_display = "навсегда"
    else:
        time_delta = parse_time_string(time_str)
        if not time_delta:
            await message.reply("❗ Некорректный формат времени. Используйте: 10мин, 2ч, 5д, 1мес, навсегда", parse_mode="HTML")
            return
        ban_until = (datetime.now() + time_delta).strftime('%Y-%m-%d %H:%M:%S')
        time_display = time_str

    cursor.execute('''
        UPDATE users
        SET ban_until = ?, ban_reason = ?
        WHERE user_id = ?
    ''', (ban_until, reason, target_id))
    conn.commit()

    username_display = target_username if target_username else f"ID:{target_id}"
    text = (
        f"🚫 Пользователь {username_display} был забанен в боте!\n"
        f"Причина: {reason}\n"
        f"На сколько: {time_display}🚫"
    )
    await message.reply(text, parse_mode="HTML")

#=======================

@dp.message_handler(commands=['unban'])
async def unban_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет прав для выполнения этой команды.", parse_mode="HTML")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("❗ Используйте: /unban (ID или username)", parse_mode="HTML")
        return

    _, target_id_or_username = parts
    target_id, target_username = await get_user_info(target_id_or_username)

    if not target_id:
        await message.reply("❗ Пользователь не найден.", parse_mode="HTML")
        return

    cursor.execute('''
        UPDATE users
        SET ban_until = NULL, ban_reason = NULL
        WHERE user_id = ?
    ''', (target_id,))
    conn.commit()

    username_display = target_username if target_username else f"ID:{target_id}"
    text = f"❇️ Пользователь {username_display} был разбанен!"
    await message.reply(text, parse_mode="HTML")

#=======================

@dp.message_handler(commands=['nick'])
async def set_nick_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    new_nick = message.get_args()
    if not new_nick:
        await message.reply("❗ Укажите никнейм. Пример: /nick НовыйНик")
        return

    new_nick = new_nick.strip()
    if len(new_nick) > 17:
        await message.reply("❗ Никнейм не должен превышать 17 символов.")
        return

    cursor.execute('UPDATE users SET nickname = ? WHERE user_id = ?', (new_nick, user_id))
    conn.commit()
    await message.reply(f"✅ Никнейм успешно изменен на: {new_nick}")

@dp.message_handler(lambda message: message.text and message.text.lower().startswith('+ник'))
async def set_nick_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    new_nick = message.text[4:].strip()  # Убираем "+ник " из текста сообщения
    if not new_nick:
        await message.reply("❗ Укажите никнейм. Пример: +ник НовыйНик")
        return

    if len(new_nick) > 17:
        await message.reply("❗ Никнейм не должен превышать 17 символов.")
        return

    cursor.execute('UPDATE users SET nickname = ? WHERE user_id = ?', (new_nick, user_id))
    conn.commit()
    await message.reply(f"✅ Никнейм успешно изменен на: {new_nick}")

def escape_md(text: str) -> str:
    escape_chars = r'*[]()~`>#+-=|{}.!'  # Убрал _ из списка экранируемых символов
    text = ''.join('\\' + c if c in escape_chars else c for c in text)
    text = re.sub(r'(?<!\\)_', r'\_', text) # Экранируем символ подчеркивания, только если перед ним нет обратного слеша

    return text


@dp.message_handler(lambda message: message.text and message.text.lower() in ['топ', 'Топ'])
@dp.message_handler(commands=['top'])
async def top_handler(message: types.Message):
    user_id = message.from_user.id

    # Проверка бана текущего пользователя
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Выбираем топ 10 только не забаненных игроков
    cursor.execute('''
        SELECT user_id, username, balance, nickname
        FROM users
        WHERE ban_until IS NULL OR ban_until <= ?
        ORDER BY balance DESC
        LIMIT 10
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    rows = cursor.fetchall()

    cursor.execute('SELECT COUNT(*) FROM users WHERE ban_until IS NULL OR ban_until <= ?', 
                   (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    total_users = cursor.fetchone()[0]

    if not rows:
        await message.reply("Топ пока пуст.")
        return

    # Эмодзи для позиций с 1 по 10
    position_emojis = ['1⃣', '2⃣', '3⃣', '4⃣', '5⃣', '6⃣', '7⃣', '8⃣', '9⃣', '🔟']

    text_lines = ["🏆 Топ 10 богатых игроков бота:"]
    for i, (user_id, username, balance, nickname) in enumerate(rows):
        bal_str = format_number(balance)
        name = nickname if nickname else username if username else f"ID:{user_id}"
        text_lines.append(f"{position_emojis[i]}. {escape_md(name)} — {bal_str} PLcoins")

    text = "\n".join(text_lines)
    await message.reply(text)


#=======================    

bonus_columns = {
    "last_bonus_normal": 0,
    "last_bonus_daily": 0,
    "last_bonus_wheel": 0
}

with get_conn() as conn:
    cursor = conn.cursor()
    for column_name, default_value in bonus_columns.items():
        try:
            cursor.execute(
                f"ALTER TABLE users ADD COLUMN {column_name} INTEGER DEFAULT {default_value}"
            )
        except sqlite3.OperationalError:
            pass  # колонка уже существует
    conn.commit()

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import random, asyncio, time

from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import random, time, asyncio
from datetime import datetime

CHANNEL_ID = -1002558236593
CHANNEL_USERNAME = "@LuckyPlChanel"  # юзернейм публичного канала
CHANNEL_LINK = "https://t.me/LuckyPlChanel"  # ссылка для кнопки


# Словарь для блокировки активных бонусов
active_bonuses = {}

# Функция для проверки кулдауна
def check_cooldown(last_time, cooldown):
    now = int(time.time())
    if last_time is None:
        return 0
    elapsed = now - last_time
    return max(0, cooldown - elapsed)

async def is_subscribed(user_id):
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        if member.status in ["creator", "administrator", "member", "restricted"]:
            return True
        return False
    except Exception as e:
        print(f"Ошибка при проверке подписки для {user_id}: {e}")
        return False


# Общая функция отправки клавиатуры бонусов
async def send_bonus_keyboard(message: types.Message):
    user_id = message.from_user.id

    # Проверка регистрации
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    sender_data = cursor.fetchone()
    if not sender_data:
        await message.reply("❗ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return
    sender_balance = sender_data[0]

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💰 Обычный бонус", callback_data="bonus_normal"),
        InlineKeyboardButton("🎁 Ежедневный бонус", callback_data="bonus_daily"),
        InlineKeyboardButton("🎡 Колесо удачи", callback_data="bonus_wheel")
    )
    await message.reply(
        "🎁 <b>Выберите, какой бонус хотите получить:</b>\n\n"
        "💰 <b>Обычный бонус</b>\n"
        "└ от <b>1,000</b> до <b>5,000</b> PLcoins\n\n"
        "🎁 <b>Ежедневный бонус</b>\n"
        "└ от <b>3,500</b> до <b>7,500</b> PLcoins\n\n"
        "🎡 <b>Колесо удачи</b>\n"
        "└ от <b>8,000</b> до <b>15,000</b> PLcoins",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    
# Хендлер для команды /bonus
@dp.message_handler(commands=['bonus'])
async def bonus_command_handler(message: types.Message):
    await send_bonus_keyboard(message)


# Хендлер для текста "бонус"
@dp.message_handler(lambda message: message.text and message.text.lower() == 'бонус')
async def bonus_text_handler(message: types.Message):
    await send_bonus_keyboard(message)


# Обработка выбора бонуса
@dp.callback_query_handler(lambda c: c.data in ["bonus_normal", "bonus_daily", "bonus_wheel"])
async def process_bonus_choice(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    choice = callback_query.data

    # Проверка подписки
    subscribed = await is_subscribed(user_id)
    if not subscribed:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("Подписаться на канал", url=CHANNEL_LINK))
        await callback_query.message.edit_text("❌ Чтобы получить бонус, подпишитесь на канал.", reply_markup=keyboard)
        return

    # Блокировка бонуса
    if active_bonuses.get((user_id, choice)):
        await callback_query.answer("⏳ Этот бонус уже в процессе!", show_alert=True)
        return
    active_bonuses[(user_id, choice)] = True

    # Получаем баланс и последние бонусы
    cursor.execute('SELECT balance, last_bonus_normal, last_bonus_daily, last_bonus_wheel FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    if not row:
        await callback_query.message.edit_text("Вы не зарегистрированы. Используйте /start.")
        active_bonuses.pop((user_id, choice), None)
        return

    balance, last_normal, last_daily, last_wheel = row

    if choice == "bonus_normal":
        cooldown = 3600  # 1 час
        remaining = check_cooldown(last_normal, cooldown)
        if remaining > 0:
            await callback_query.answer(f"⏳ Подождите {remaining} секунд до следующего обычного бонуса.", show_alert=True)
        else:
            bonus = random.randint(1000, 5000)  # <-- новый диапазон
            new_balance = balance + bonus
            cursor.execute('UPDATE users SET balance = ?, last_bonus_normal = ? WHERE user_id = ?', (new_balance, int(time.time()), user_id))
            conn.commit()
            await callback_query.message.edit_text(
                f"<b>🎉 Вы получили обычный бонус:</b> +{bonus} PLcoins!\n"
                f"<b>💰 Ваш текущий баланс:</b> {new_balance} PLcoins.",
                parse_mode="HTML"
            )

    elif choice == "bonus_daily":
        cooldown = 43200
        remaining = check_cooldown(last_daily, cooldown)

        if remaining > 0:
            await callback_query.answer(
                f"⏳ Подождите {remaining} секунд до следующего ежедневного бонуса.",
                show_alert=True
            )
            active_bonuses.pop((user_id, choice), None)
            return

        bonus = random.randint(3500, 7500)

        cursor.execute(
            'UPDATE users SET balance = balance + ?, last_bonus_daily = ? WHERE user_id = ?',
            (bonus, int(time.time()), user_id)
        )
        conn.commit()

        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        new_balance = cursor.fetchone()[0]

        await callback_query.message.edit_text(
            f"🎉 Ежедневный бонус: +{bonus} PLcoins!\n"
            f"💰 Новый баланс: {new_balance} PLcoins.",
            parse_mode="HTML"
        )


    elif choice == "bonus_wheel":
        cooldown = 86400  # 24 часа
        remaining = check_cooldown(last_wheel, cooldown)
        if remaining > 0:
            await callback_query.answer(f"⏳ Подождите {remaining} секунд до следующего колеса удачи.", show_alert=True)
        else:
            await callback_query.message.edit_text("🎡 Колесо крутится...")
            await asyncio.sleep(5)
            bonus = random.randint(8000, 15000)  # <-- новый диапазон для колеса удачи
            new_balance = balance + bonus
            cursor.execute('UPDATE users SET balance = ?, last_bonus_wheel = ? WHERE user_id = ?', (new_balance, int(time.time()), user_id))
            conn.commit()
            await callback_query.message.edit_text(
                f"🎉 Колесо остановилось!\n"
                f"💰 Ваш выигрыш: <b>{bonus}</b> PLcoins!\n"
                f"📊 Новый баланс: {new_balance} PLcoins.",
                parse_mode="HTML"
            )

    # Снимаем блокировку
    active_bonuses.pop((user_id, choice), None)


# Обработка выбора ящика (ежедневный бонус)
@dp.callback_query_handler(lambda c: c.data.startswith("daily_box_"))
async def process_daily_box(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    # Проверка подписки
    subscribed = await is_subscribed(user_id)
    if not subscribed:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("Подписаться на канал", url=CHANNEL_LINK))
        await callback_query.message.edit_text("❌ Чтобы получить бонус, подпишитесь на канал.", reply_markup=keyboard)
        return

    # Блокируем повторное открытие
    if active_bonuses.get((user_id, "bonus_daily")):
        await callback_query.answer("⏳ Ящик уже в процессе открытия!", show_alert=True)
        return
    active_bonuses[(user_id, "bonus_daily")] = True

    # Получаем баланс
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    balance = cursor.fetchone()[0]

    # Список возможных призов
    prizes = [0, 50000, 100000]
    prize = random.choice(prizes)

    new_balance = balance + prize if prize > 0 else balance
    cursor.execute('UPDATE users SET balance = ?, last_bonus_daily = ? WHERE user_id = ?', (new_balance, int(time.time()), user_id))
    conn.commit()

    if prize > 0:
        await callback_query.message.edit_text(
            f"📦 Вы открыли ящик и нашли <b>{prize}</b> PLcoins!\n"
            f"💰 Ваш новый баланс: {new_balance} PLcoins.",
            parse_mode="HTML"
        )
    else:
        await callback_query.message.edit_text(
            "📦 Вы открыли ящик... и он оказался пустым! 😢",
            parse_mode="HTML"
        )

    # Снимаем блокировку
    active_bonuses.pop((user_id, "bonus_daily"), None)
    
#=======================

@dp.message_handler(lambda message: message.text and message.text.lower().startswith('дать'))
@dp.message_handler(commands=['give'])
async def give_handler(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    if not message.reply_to_message:
        await message.reply("❗ Чтобы передать монеты, ответьте на сообщение пользователя и напишите сумму.")
        return

    sender_id = message.from_user.id
    recipient = message.reply_to_message.from_user
    recipient_id = recipient.id

    if sender_id == recipient_id:
        await message.reply("❗ Нельзя передавать деньги самому себе.")
        return

    if recipient_id == (await bot.get_me()).id:
        await message.reply("❗ Нельзя передавать деньги боту.")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❗ Укажите сумму для передачи. Пример: дать 100")
        return

    amount_str = args[1].strip().lower()

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (sender_id,))
    sender_data = cursor.fetchone()
    if not sender_data:
        await message.reply("❗ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return
    sender_balance = sender_data[0]

    if amount_str == 'все':
        amount = int(round(sender_balance))
        if amount <= 0:
            await message.reply("❗ У вас недостаточно средств для передачи.")
            return
    else:
        amount = format_stake(amount_str, sender_balance)
        if amount <= 0:
            await message.reply("❗ Укажите корректную положительную сумму для передачи.")
            return
        if amount > sender_balance:
            await message.reply("❗ У вас недостаточно средств для передачи.")
            return

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (recipient_id,))
    recipient_data = cursor.fetchone()
    if not recipient_data:
        cursor.execute('INSERT OR IGNORE INTO users (user_id, balance, reg_date) VALUES (?, ?, ?)', 
                       (recipient_id, 0, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        recipient_balance = 0
    else:
        recipient_balance = recipient_data[0]

    commission = round(amount * 0.10)
    received = amount - commission

    new_sender_balance = sender_balance - amount
    new_recipient_balance = recipient_balance + received

    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_sender_balance, sender_id))
    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_recipient_balance, recipient_id))
    conn.commit()

    def fmt(n): return f"{int(round(n))}"

    # --- Сообщение в чате ---
    await message.reply(
        f"<b>✅ Передача выполнена!</b>\n\n"
        f"💸 <b>Отправлено:</b> {format_number(amount)} PLCoins\n"
        f"📉 <b>Комиссия:</b> {format_number(commission)} PLCoins\n"
        f"📥 <b>Получено получателем:</b> +{format_number(received)} PLCoins",
        parse_mode="HTML"
    )

   # --- Подготовка данных отправителя ---
    sender_name = message.from_user.full_name
    sender_username = message.from_user.username
    sender_display = f"@{sender_username}" if sender_username else sender_name

    # --- Время по Москве ---
    moscow_tz = pytz.timezone("Europe/Moscow")
    now_moscow = datetime.now(moscow_tz)  # корректное МСК
    now_str = now_moscow.strftime('%d.%m.%Y %H:%M')

    # --- Текст уведомления ---
    notify_text = (
        f"💰 <b>Вам передали PLCoins</b>\n"
        f"- - - - - - - - - - - - - - - - - - - - - - -\n"
        f"💸 <b>Сумма:</b> {format_number(amount)}\n"
        f"📉 <b>Комиссия:</b> {format_number(commission)}\n"
        f"✅ <b>Получено:</b> {format_number(received)}\n\n"
        f"👤 <b>Отправитель:</b> {sender_display}\n"
        f"🆔 <b>ID:</b> <code>{sender_id}</code>\n"
        f"⌛ <b>Дата получения:</b> {now_str} МСК"
    )

    # --- Кнопка "Написать отправителю" ---
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            "✉️ Написать отправителю",
            url=f"tg://user?id={sender_id}"
        )
    )

    # --- Отправка ЛС ---
    try:
        await bot.send_message(
            chat_id=recipient_id,
            text=notify_text,
            parse_mode="HTML",
            reply_markup=kb
        )
    except Exception as e:
        print(f"Не удалось отправить ЛС получателю: {e}")


import random
import math

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/crash') or message.text.lower().startswith('краш')))
async def crash_handler(message: types.Message):
    user_id = message.from_user.id
    now = time.time()

    # Проверка кулдауна
    if user_id in last_command_time and (now - last_command_time[user_id]) < COMMAND_COOLDOWN:
        await message.reply(f"⏳ Подождите {COMMAND_COOLDOWN} секунд перед повторным использованием команды.", parse_mode="HTML")
        return
    last_command_time[user_id] = now

    # Проверка регистрации и бана
    cursor.execute('SELECT ban_until, ban_reason, balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start.", parse_mode="HTML")
        return

    ban_until, ban_reason, balance, games_played, lost = user_data
    if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
        await message.reply(f"🚫 Вы забанены до {ban_until}, причина: {ban_reason}", parse_mode="HTML")
        return

    # Парсим ставку и множитель
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("⚠️ Укажите ставку и множитель. Пример: /crash 100 2.0", parse_mode="HTML")
        return

    bet_str, multiplier_str = parts[1], parts[2].replace(',', '.')
    try:
        multiplier = float(multiplier_str)
        bet = balance if bet_str.lower() == 'все' else format_stake(bet_str, balance)
        if bet < 100:  # минимальная ставка
            await message.reply("❗ Минимальная ставка — 100 PLcoins.", parse_mode="HTML")
            return
        if bet <= 0 or bet > balance or multiplier <= 1.0 or multiplier > 10:
            raise ValueError()
    except ValueError:
        await message.reply("❗ Некорректные параметры. Пример: /crash 100 2.0", parse_mode="HTML")
        return

    # Списываем ставку
    new_balance = balance - bet
    cursor.execute('UPDATE users SET balance = ?, games_played = ? WHERE user_id = ?', (new_balance, games_played + 1, user_id))
    conn.commit()


    def generate_crash_point():
        r = random.random()
        
        if r < 0.15:  # 50% — очень низкие: 1.00–1.10 (x1 чаще падает)
            cp = random.uniform(1.00, 1.00)
        elif r < 0.65:  # 10% — низкие: 1.11–1.20
            cp = random.uniform(1.11, 1.20)
        elif r < 0.65:  # 25% — средние: 1.21–2.0
            cp = random.uniform(1.21, 2.0)
        elif r < 0.75:  # 10% — крупные: 2.01–5.0
            cp = random.uniform(2.01, 5.0)
        elif r < 0.82:  # 7% — высокий: 5.01–6.0
            cp = random.uniform(5.01, 6.0)
        elif r < 0.88:  # 6% — высокий: 6.01–7.0
            cp = random.uniform(6.01, 7.0)
        elif r < 0.93:  # 5% — высокий: 7.01–8.0
            cp = random.uniform(7.01, 8.0)
        elif r < 0.97:  # 4% — высокий: 8.01–9.0
            cp = random.uniform(8.01, 9.0)
        else:  # 5% — супер-выигрыш: 9.01–10.0 (x10)
            cp = random.uniform(9.01, 10.0)
            
        return math.floor(cp * 100) / 100


    crash = generate_crash_point()  # <- Важно: присвоить результат переменной

    # Отправляем ракету
    rocket_msg = await message.reply("🚀", parse_mode="HTML")
    await asyncio.sleep(2.7)

   # ===== РЕЗУЛЬТАТ =====
    if multiplier <= crash:
        win = int(bet * multiplier)
        new_balance += win

        cursor.execute(
            'UPDATE users SET balance = ? WHERE user_id = ?',
            (new_balance, user_id)
        )
        conn.commit()

        # ===== HISTORY (ВЫИГРЫШ) =====
        add_game_history(
            user_id=user_id,
            game="Краш",
            bet=bet,
            result="Выигрыш",
            multiplier=multiplier,
            win=win
        )

        await rocket_msg.edit_text(
            f"🚀 Ракета упала на x{crash}\n"
            f"🎉 Победа!\n"
            f"💰 Выигрыш: +{format_number(win)} PLcoins",
            parse_mode="HTML"
        )

    else:
        new_lost = lost + bet
        cursor.execute(
            'UPDATE users SET lost = ? WHERE user_id = ?',
            (new_lost, user_id)
        )
        conn.commit()

        # ===== HISTORY (ПРОИГРЫШ) =====
        add_game_history(
            user_id=user_id,
            game="Краш",
            bet=bet,
            result="Проигрыш"
        )

        await rocket_msg.edit_text(
            f"💥 Ракета упала на x{crash}\n"
            f"😞 Вы проиграли\n"
            f"❌ Проигрыш: {format_number(bet)} PLcoins",
            parse_mode="HTML"
        )

#=======================
@dp.message_handler(lambda message: message.text and 
                    (message.text.lower().startswith('кости') or message.text.lower().startswith('/cubes')))
async def cubes_handler(message: types.Message):
    user_id = message.from_user.id

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Разбор сообщения
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply(
            "⚠️ Использование: /cubes (ставка) (больше|меньше|равно)",
            parse_mode="HTML"
        )
        return

    bet_str = parts[1].strip().lower()
    choice = parts[2].lower()

    if choice not in ["больше", "меньше", "равно"]:
        await message.reply(
            "⚠️ Укажите один из вариантов: больше, меньше, равно",
            parse_mode="HTML"
        )
        return

    # Получаем баланс пользователя
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply(
            "❌ Вы не зарегистрированы. Нажмите /start для регистрации.",
            parse_mode="HTML"
        )
        return

    balance = user_data[0]

    # Обработка ставки
    if bet_str == 'все':
        bet = balance
        if bet <= 0:
            await message.reply("❌ Недостаточно средств.", parse_mode="HTML")
            return
    else:
        bet = format_stake(bet_str, balance)  # Передаём balance в format_stake
        if bet <= 0:
            await message.reply("⚠️ Некорректная ставка.", parse_mode="HTML")
            return

    # Проверка баланса и обновление игр
    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply(
            "❌ Вы не зарегистрированы. Нажмите /start для регистрации.",
            parse_mode="HTML"
        )
        return

    balance, games_played, lost = user_data
    if balance < bet:
        await message.reply("❌ Недостаточно средств.", parse_mode="HTML")
        return

    new_balance = balance - bet
    cursor.execute(
        'UPDATE users SET balance = ?, games_played = ? WHERE user_id = ?',
        (new_balance, games_played + 1, user_id)
    )

    # Выпадение числа
    outcomes = (
        [2, 3, 4, 5, 6, 8, 9, 10, 11, 12] * 2 +  # увеличиваем вероятность "не 7"
        [7] * 4  # "7" теперь появляется в 2 раза чаще, чем остальные числа
    )
    total = random.choice(outcomes)

    if total > 7:
        result = "больше"
        symbol = "🔼"
    elif total < 7:
        result = "меньше"
        symbol = "🔽"
    else:
        result = "равно"
        symbol = "🟰"

    # Определение выигрыша
    win = 0
    if result == choice:
        if choice == "равно":
            multiplier = 4.2
        else:
            multiplier = 2.25
        win = int(bet * multiplier)
        new_balance += win
        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))

        # --- HISTORY (Выигрыш) ---
        add_game_history(
            user_id=user_id,
            game="Кости",
            bet=bet,
            result="Выигрыш",
            multiplier=multiplier,
            win=win
        )

        title = "<b>Ты выиграл! ✅</b>"
        result_line = f"<b>📊 Выигрыш:</b> x{multiplier} / +{format_number(win)} PLcoins"
    else:
        new_lost = lost + bet
        cursor.execute('UPDATE users SET lost = ? WHERE user_id = ?', (new_lost, user_id))
        conn.commit()

        # --- HISTORY (Проигрыш) ---
        add_game_history(
            user_id=user_id,
            game="Кости",
            bet=bet,
            result="Проигрыш",
            multiplier=0,
            win=0
        )

        title = "<b>Ты проиграл! 😢</b>"
        result_line = ""

    bet_str = format_number(bet)

    # Формирование текста сообщения
    text = (
        f"{title}\n\n"
        f"<blockquote><b>💸 Ставка:</b> {bet_str} PLcoins</blockquote>\n"
        f"<b>🎲 Исход:</b> {choice} 7\n"
        f"{result_line}\n"
        f"<b>-----------------</b>\n"
        f"<b>⚡️Выпало:</b> {result} 7 {symbol}"
    )

    # Отправка результата
    await message.reply(text, parse_mode="HTML")

# ================== Константы ==================
GRID_ROWS = 12
BOOM_EMOJI = "💣"
GOLD_EMOJI = "💸"
QUESTION_EMOJI = "❓"
CHECK_EMOJI = "💰"
COMMAND_COOLDOWN = 2
DUPE_DETECTED_EMOJI = "⚰"

# ================== Глобальные структуры ==================
gold_games = {}                  # key: game_id, value: GoldGame
win_processed = {}               # key: game_id, value: True
last_game_end_time = {}
GOLD_GAMES_FAIRNESS = {}         # key: game_id, value: fairness_data
GOLD_GAMES_BACKUP = {}    # бэкап игры для кнопки "Назад"



# ================== Вспомогательные функции ==================
async def get_name(user_id):
    cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return 'Пользователь'

def format_number(number):
    return f"{int(int(number)):,}".replace(',', "'")

def format_stake(stake_str: str, balance: int) -> int:
    stake_str = stake_str.lower().replace(' ', '')
    if stake_str == 'все':
        return int(round(balance))
    multipliers = {'кккк': 10**12,'ккк':10**9,'кк':10**6,'к':10**3}
    for suffix, multiplier in sorted(list(multipliers.items()), key=lambda x: -len(x[0])):
        if stake_str.endswith(suffix):
            try:
                return int(round(float(stake_str[:-len(suffix)]) * multiplier))
            except:
                return -1
    try:
        value = float(stake_str)
        return int(round(value))
    except:
        return -1

async def is_user_banned(user_id: int) -> bool:
    cursor.execute('SELECT ban_until FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until = ban_info[0]
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            return True
    return False

# ================== FAIRNESS ==================
import secrets, hashlib

def generate_fairness(grid, stake):
    """
    Генерация честности:
    - grid: сетка игры [['💸','🧨'], ['🧨','💸'], ...]
    - stake: ставка игрока
    """

    # Строка для хэша (без форматирования!)
    raw_str = ''.join(
        '2' if cell == '🧨' else '0'
        for row in grid
        for cell in row
    )

    nonce = secrets.token_hex(16)
    hash_val = hashlib.sha3_256((raw_str + nonce).encode()).hexdigest()

    return {
        'raw': raw_str,      # для хэша
        'nonce': nonce,
        'hash': hash_val,
        'stake': stake
    }



# ================== Класс игры ==================
class GoldGame:
    def __init__(self, user_id, chat_id, stake):
        self.user_id = user_id
        self.chat_id = chat_id
        self.stake = stake
        self.grid = [random.choice([['💸','🧨'],['🧨','💸']]) for _ in range(GRID_ROWS)]
        self.player = [-1,-1]
        self.last_time = time.time()
        self.message_id = None
        self.current_multiplier = 1.0
        self.total_win = 0
        self.game_over = False
        self.claimed = False
        self.boom_position = None
        self.is_tapping = False
        self.is_stopping = False
        self.fair = None  # сохранение честности

    def get_pole(self, action: str) -> str:
        grid_display = [[QUESTION_EMOJI]*2 for _ in range(GRID_ROWS)]
        for i in range(GRID_ROWS):
            for j in range(2):
                if self.game_over:
                    if self.boom_position == (i,j):
                        grid_display[i][j] = BOOM_EMOJI
                    elif self.grid[i][j] == '🧨':
                        grid_display[i][j] = '🧨'
                    elif self.grid[i][j] == '💸':
                        grid_display[i][j] = GOLD_EMOJI
                elif i == self.player[0] and action != 'lose' and self.grid[i][j]=='💸':
                    grid_display[i][j] = CHECK_EMOJI
                elif self.grid[i][j]=='💸' and i<=self.player[0]:
                    grid_display[i][j] = GOLD_EMOJI
        pole_text = ""
        for i, row in reversed(list(enumerate(grid_display))):
            multiplier_text = f"({2**(i+1)}x)"
            pole_text += f"|{'|'.join(row)}| {multiplier_text}\n"
        return pole_text

    def make_move(self, y: int) -> str | None:
        self.player = [self.player[0]+1, y]
        pos = self.grid[self.player[0]][self.player[1]]
        if pos=='🧨':
            self.game_over = True
            self.boom_position = (self.player[0], self.player[1])
            return 'lose'
        if self.player[0] == GRID_ROWS-1:
            return 'win'
        self.current_multiplier *= 2.0
        self.total_win = int(self.stake*self.current_multiplier)
        return None

    async def stop_game(self, cancel=False, lost=False, dupe_attempt=False):
        cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (self.user_id,))
        data = cursor.fetchone()
        if not data: return
        balance, games_played, total_lost = data
        new_games_played = games_played + 1
        if dupe_attempt:
            new_balance = balance+self.stake
            cursor.execute('UPDATE users SET balance=?, games_played=? WHERE user_id=?',(new_balance,new_games_played,self.user_id))
        elif not cancel and not lost:
            new_balance = balance+self.total_win
            cursor.execute('UPDATE users SET balance=?, games_played=? WHERE user_id=?',(new_balance,new_games_played,self.user_id))
        elif cancel:
            new_balance = balance+self.stake
            cursor.execute('UPDATE users SET balance=?, games_played=? WHERE user_id=?',(new_balance,new_games_played,self.user_id))
        elif lost:
            cursor.execute('UPDATE users SET games_played=?, lost=? WHERE user_id=?',(new_games_played,total_lost+self.stake,self.user_id))
        conn.commit()

    def get_text(self, action: str) -> str:
        txt = ""
        if action == 'win':
            txt += f"🎉<b>{{}}</b>, ты забрал приз!🎉"
            self.game_over = True
        elif action == 'stop':
            txt += f"🛑<b>{{}}</b>, игра отменена!🛑"
            self.game_over = True
        elif action == 'lose':
            txt += f"{BOOM_EMOJI}<b>{{}}</b>, ты проиграл!\n{BOOM_EMOJI}"
            self.game_over = True
        elif action == 'dupe':
            txt += f"{DUPE_DETECTED_EMOJI}<b>{{}}</b>, попытка дюпа! Игра отклонена.{DUPE_DETECTED_EMOJI}"
            self.game_over = True
        else:
            txt += f"💰<b>{{}}</b>, игра GOLD началась!💰"

        pole = self.get_pole(action)
        txt += f"\n<code>·····················</code>\n💰 <b>Ставка:</b> {format_number(self.stake)} PLcoins"
        if action == 'game' and self.player[0] != -1:
            txt += f"\n📊 <b>Выигрыш:</b> x{self.current_multiplier:.1f} / +{format_number(self.total_win)} PLcoins"
        txt += "\n\n" + pole
        return txt

    def get_kb(self, game_id: str) -> InlineKeyboardMarkup:
        keyboard = InlineKeyboardMarkup(row_width=2)
        if not self.game_over:
            keyboard.add(
                InlineKeyboardButton(QUESTION_EMOJI, callback_data=f"gold-tap_0|{game_id}"),
                InlineKeyboardButton(QUESTION_EMOJI, callback_data=f"gold-tap_1|{game_id}")
            )
            if self.player[0] == -1:
                keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data=f"gold-stop|{game_id}"))
            else:
                keyboard.add(InlineKeyboardButton(f"Забрать {CHECK_EMOJI} {format_number(self.total_win)} PLcoins", callback_data=f"gold-stop|{game_id}"))
        else:
            keyboard.add(InlineKeyboardButton("🛡 Доказать честность", callback_data=f"gold-fair|{game_id}"))
        return keyboard


# ================== Запуск игры ==================
@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('золото') or message.text.lower().startswith('/gold')))
async def start_gold_game(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    now = time.time()
    name = await get_name(user_id)

    # Проверка на слишком быстрый старт
    if user_id in last_game_end_time and now-last_game_end_time[user_id]<COMMAND_COOLDOWN:
        await message.reply(f"Пожалуйста подождите {COMMAND_COOLDOWN} секунд.", parse_mode="HTML")
        return

    # Проверка на регистрацию и баланс
    cursor.execute('SELECT balance FROM users WHERE user_id=?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start.", parse_mode="HTML")
        return
    balance = user_data[0]

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id=?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S')>datetime.now():
            await message.reply(f"🚫 Вы забанены до <b>{ban_until}</b>, причина: <b>{ban_reason}</b>🚫", parse_mode="HTML")
            return
        else:
            cursor.execute('UPDATE users SET ban_until=NULL, ban_reason=NULL WHERE user_id=?', (user_id,))
            conn.commit()

    # Парсим ставку
    parts = message.text.lower().split()
    if len(parts)<2:
        await message.reply("Используйте: /gold (сумма) или /gold все", parse_mode="HTML")
        return
    # Парсим ставку
    stake = format_stake(parts[1], balance)
    if stake <= 0 or stake > balance:
        await message.reply("❗ Некорректная сумма ставки или недостаточно средств.", parse_mode="HTML")
        return

    # Списываем ставку
    cursor.execute('UPDATE users SET balance=balance-? WHERE user_id=?', (stake, user_id))
    conn.commit()

    # Создаем игру
    game_id = str(uuid.uuid4())
    game = GoldGame(user_id, chat_id, stake)
    gold_games[game_id] = game  # сохраняем в активные игры

    # FAIRNESS
    fair = generate_fairness(game.grid, stake)
    game.fair = fair  # сохраняем честность в объекте игры
    GOLD_GAMES_FAIRNESS[game_id] = fair

        # Сохраняем бэкап игры для "Назад"
    GOLD_GAMES_BACKUP[game_id] = {
        'user_id': game.user_id,
        'chat_id': game.chat_id,
        'stake': game.stake,
        'grid': game.grid,
        'player': game.player.copy(),
        'game_over': game.game_over,
        'current_multiplier': game.current_multiplier,
        'total_win': game.total_win,
        'boom_position': game.boom_position,
        'fair': game.fair
    }

    # Отправляем сообщение с игрой
    text = game.get_text('game').format(f"<a href='tg://user?id={user_id}'>{message.from_user.first_name}</a>")
    keyboard = game.get_kb(game_id)
    msg = await message.reply(text, reply_markup=keyboard, parse_mode="HTML")
    game.message_id = msg.message_id

# ================== Ходы ==================
@dp.callback_query_handler(Text(startswith="gold-tap_"))
async def game_kb(call: types.CallbackQuery):
    parts = call.data.split('|')
    y = int(parts[0].split('_')[1])
    game_id = parts[1]
    game = gold_games.get(game_id)
    if not game:
        await call.answer("Игра не найдена.⚡️", show_alert=True)
        return
    if call.from_user.id != game.user_id:
        await call.answer("❗ Это не ваши кнопки.", show_alert=True)
        return
    if game.is_tapping:
        await call.answer("Подождите...", show_alert=False)
        return

    game.is_tapping = True
    result = game.make_move(y)
    name = await get_name(game.user_id)

    if result in ['lose', 'win']:
        # Сохраняем текст прямо после окончания
        text_to_save = game.get_text(result).format(f"<a href='tg://user?id={game.user_id}'>{name}</a>")
        GOLD_GAMES_BACKUP[game_id]['original_text'] = text_to_save

        # Останавливаем игру и обновляем баланс
        await game.stop_game(lost=(result=='lose'))

        add_game_history(
            user_id=game.user_id,
            game="Золото",
            bet=game.stake,
            result="Проигрыш" if result=='lose' else "Выигрыш",
            multiplier=game.current_multiplier,
            win=0 if result=='lose' else game.total_win
        )

        # Кнопка честности
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🛡 Доказать честность", callback_data=f"gold-fair|{game_id}"))

        await call.message.edit_text(text_to_save, reply_markup=keyboard, parse_mode="HTML")
        gold_games.pop(game_id, None)
        last_game_end_time[game.user_id] = time.time()

    else:
        # Игра продолжается
        await call.message.edit_text(
            game.get_text('game').format(f"<a href='tg://user?id={game.user_id}'>{name}</a>"),
            reply_markup=game.get_kb(game_id),
            parse_mode="HTML"
        )

    game.is_tapping = False
    await call.answer()


# ================== Забрать или отменить ==================
@dp.callback_query_handler(Text(startswith="gold-stop"))
async def game_stop(call: types.CallbackQuery):
    game_id = call.data.split('|')[1]
    game = gold_games.get(game_id)
    if not game:
        await call.answer("Игра не найдена.⚡️")
        return
    if call.from_user.id != game.user_id:
        await call.answer("❗ Это не ваши кнопки.", show_alert=True)
        return

    cancel = game.player[0] == -1
    name = await get_name(game.user_id)

    if cancel:
        # Игра отменена до первого хода
        await game.stop_game(cancel=True)
        gold_games.pop(game_id, None)
        try:
            await bot.delete_message(game.chat_id, game.message_id)
        except:
            pass
        await call.answer("Игра отменена и сообщение удалено.")
        return

    if game.game_over and game.boom_position:
        await call.answer("Вы уже проиграли!", show_alert=True)
        return

    if game_id not in win_processed:
        win_processed[game_id] = True

        # Сохраняем текст выигрыша
        text_to_save = game.get_text('win').format(f"<a href='tg://user?id={game.user_id}'>{name}</a>")
        GOLD_GAMES_BACKUP[game_id]['original_text'] = text_to_save

        # Обновляем баланс
        await game.stop_game()

        add_game_history(
            user_id=game.user_id,
            game="Золото",
            bet=game.stake,
            result="Выигрыш",
            multiplier=game.current_multiplier,
            win=game.total_win
        )

        # Кнопка честности
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🛡 Доказать честность", callback_data=f"gold-fair|{game_id}"))

        await call.message.edit_text(text_to_save, reply_markup=keyboard, parse_mode="HTML")
        gold_games.pop(game_id, None)
        last_game_end_time[game.user_id] = time.time()
        await call.answer("Вы успешно забрали свой выигрыш!")
    else:
        await call.answer("Вы уже забрали свой выигрыш в этой игре.")


# ================== Хендлер "Доказать честность" ==================
@dp.callback_query_handler(Text(startswith="gold-fair"))
async def gold_fair(call):
    game_id = call.data.split("|")[1]

    game = GOLD_GAMES_BACKUP.get(game_id)
    if not game or 'fair' not in game or not game['fair']:
        return await call.answer("Данные честности недоступны", show_alert=True)

    fair = game['fair']
    raw = fair['raw']      # для хэша
    nonce = fair['nonce']
    hash_val = fair['hash']
    stake = fair['stake']

    # ---------- Форматирование поля для показа ----------
    pairs = [raw[i:i+2] for i in range(0, len(raw), 2)]
    formatted_pairs = ["-".join(pair) for pair in pairs]
    formatted_field = "; ".join(formatted_pairs)
    # -----------------------------------------------------

    # Ссылка для проверки хэша
    check_url = f"https://codebeautify.org/sha3-256-hash-generator?input={raw}{nonce}"

    # Текст доказательства честности с явной ссылкой
    text = (
        "🛡 Доказательство честности\n\n"
        f"💰 Ставка: {stake} PLcoins\n\n"
        f"📦 Поле мин:\n{formatted_field}\n\n"
        "2 — это мина\n"
        "0 — это пустая ячейка\n\n"
        f"🔐 Nonce: {nonce}\n"
        f"#️⃣ SHA3-256 Хэш: {hash_val}\n\n"
        f"Для проверки откройте SHA3-256 генератор и вставьте 'Поле мин+nonce':\n{check_url}\n\n"
        "Мы гарантируем 100% честность и никаких алгоритмов и скриптов 🍀"
    )

    # Кнопки
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("🔗 Проверить хэш", url=check_url)],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"gold-back|{game_id}")]
        ]
    )

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()



# ================== Хендлер "Назад" ==================
@dp.callback_query_handler(Text(startswith="gold-back"))
async def gold_back(call):
    game_id = call.data.split("|")[1]
    game_data = GOLD_GAMES_BACKUP.get(game_id)
    if not game_data:
        return await call.answer("Данные игры недоступны", show_alert=True)

    # Если есть сохранённый текст после конца игры — просто показываем его
    if 'original_text' in game_data:
        text = game_data['original_text']
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🛡 Доказать честность", callback_data=f"gold-fair|{game_id}"))
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return

    # Игра ещё не завершена — показываем последнее состояние
    name = await get_name(game_data['user_id'])
    temp_game = GoldGame(game_data['user_id'], game_data['chat_id'], game_data['stake'])
    temp_game.grid = game_data['grid']
    temp_game.player = game_data['player']
    temp_game.game_over = game_data['game_over']
    temp_game.current_multiplier = game_data['current_multiplier']
    temp_game.total_win = game_data['total_win']
    temp_game.boom_position = game_data.get('boom_position', None)
    temp_game.fair = game_data['fair']

    text = temp_game.get_text('game').format(f"<a href='tg://user?id={game_data['user_id']}'>{name}</a>")
    kb = temp_game.get_kb(game_id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()




# ================== Настройки ==================
import secrets
import hashlib


MINES_GRID_SIZE = 5
MIN_BET = 100

USER_COOLDOWN = 1.2  # минимальный интервал между кликами в секундах
_last_click = {}
ACTIVE_MINES_GAMES_FAIRNESS = {}  # game_id: fairness_data
ACTIVE_MINES_GAMES_BACKUP = {}  
ACTIVE_MINES_GAMES = {}         # game_id: game_data
mines_locks = {}   
BOMB_EMOJI = "💣"
EXPLOSION_EMOJI = "💥"
SAFE_EMOJI = "💰"
CLOSED_EMOJI = "❓"

def is_flood(user_id: int) -> bool:
    now = time.time()
    last = _last_click.get(user_id, 0)
    if now - last < USER_COOLDOWN:
        return True
    _last_click[user_id] = now
    return False


# ================== Ставки ==================
def parse_mines_bet(bet_str: str, balance: int) -> int:
    bet_str = bet_str.lower().replace(" ", "")
    if bet_str in ("все", "всё"):
        return balance
    units = {"ккк": 1_000_000_000, "кк": 1_000_000, "к": 1_000}
    for suf, mul in units.items():
        if bet_str.endswith(suf):
            try:
                return int(float(bet_str[:-len(suf)]) * mul)
            except ValueError:
                return -1
    try:
        return int(bet_str)
    except ValueError:
        return -1

# ================== Множители ==================
MULTIPLIERS = {
    1: [1.01,1.05,1.10,1.15,1.21,1.27,1.34,1.42,1.51,1.61,1.73,1.86,2.02,2.20,2.42,2.69,3.03,3.46,4.04,4.85,6.06,8.08,12.12,24.25],
    2: [1.05,1.15,1.26,1.38,1.53,1.70,1.90,2.14,2.42,2.77,3.19,3.73,4.40,5.29,6.46,8.08,10.39,13.85,19.40,29.10,48.50,97.00,291.00],
    3: [1.10,1.26,1.45,1.67,1.95,2.28,2.70,3.23,3.92,4.82,6.03,7.71,10.09,13.56,18.79,27.09,40.94,65.50,114.62,225.58,563.96,2255.83],
    4: [1.15,1.38,1.67,2.05,2.53,3.16,4.00,5.15,6.78,9.14,12.71,18.28,27.42,43.43,73.83,137.55,289.58,723.95,2292.52,11462.58,114625.83],
    5: [1.21,1.53,1.96,2.53,3.32,4.43,6.01,8.33,11.80,17.15,25.73,39.92,64.22,108.38,192.67,368.53,773.91,1934.78,6771.74,50788.04],
    6: [1.27,1.70,2.30,3.16,4.41,6.28,9.15,13.66,20.98,33.31,54.71,93.79,168.83,321.58,663.26,1525.50,4195.12,15382.11,169203.22],
    7: [1.34,1.90,2.73,4.00,5.98,9.16,14.42,23.42,39.26,68.21,123.63,238.16,494.34,1112.27,2780.68,8342.06,34758.58,451861.64],
    8: [1.42,2.14,3.28,5.15,8.33,13.89,23.95,42.75,79.53,154.21,312.43,676.93,1592.78,4247.42,13591.75,61162.88,1039769.00],
    9: [1.51,2.42,3.98,6.74,11.80,21.36,40.23,79.03,162.11,351.24,819.57,2091.13,5924.87,19552.07,83096.31,2077407.00],
    10:[1.61,2.77,4.95,9.29,18.28,37.74,82.11,188.85,464.87,1239.66,3636.33,12121.11,48484.44,266664.42,3199973.00],
    11:[1.73,3.19,6.19,12.71,27.58,63.66,158.45,431.14,1293.41,4526.96,18918.42,104051.34,832410.74,4330500.00],
    12:[1.86,3.73,7.96,18.28,45.71,125.72,384.47,1331.42,5325.71,26628.55,173085.60,1557770.40,5052250.00],
    13:[2.02,4.40,10.13,24.84,66.24,198.74,675.72,2533.95,11402.77,68416.65,615750.00,5052250.00],
    14:[2.20,5.29,13.60,38.25,119.53,418.37,1673.50,8032.81,50205.09,451845.83,4330500.00],
    15:[2.42,6.46,18.59,59.76,215.15,896.48,4333.00,25998.00,207984.00,3175700.00],
    16:[2.69,8.08,26.56,97.40,409.11,2045.55,12273.30,98186.44,1984812.00],
    17:[3.03,10.39,39.84,179.30,986.15,6574.34,55881.93,1047571.00],
    18:[3.46,13.85,63.74,366.52,2748.91,27489.13,465587.00],
    19:[4.04,19.40,111.55,818.04,8589.45,171789.00],
    20:[4.85,29.10,223.10,2454.10,51536.10],
    21:[6.06,48.50,557.75,12270.50],
    22:[8.08,97.00,2231.00],
    23:[12.12,291.00],
    24:[24.25]
}

def calculate_multiplier(opened, mines_count):
    table = MULTIPLIERS[mines_count]
    return table[opened-1] if opened-1 < len(table) else table[-1]

def calculate_take_info(game):
    opened = len(game["opened"])
    mult = 1.0 if opened == 0 else calculate_multiplier(opened, game["num_mines"])
    win = int(game["bet"] * mult)
    return mult, win

def build_game_message(game):
    mult, win = calculate_take_info(game)
    return (
        f"💣 Мин: {game['num_mines']}\n"
        f"💰 Ставка: {game['bet']} PLcoins\n"
        f"💸 Можно забрать: {game['bet']} | х{mult:.2f} | {win} PLcoins"
    )


# ================== Клавиатура ==================
def build_grid(opened, mines, game_id, exploded=None, clickable=True):
    kb = []
    for i in range(MINES_GRID_SIZE):
        row = []
        for j in range(MINES_GRID_SIZE):
            idx = i * MINES_GRID_SIZE + j
            if idx in opened:
                if idx in mines:
                    row.append(InlineKeyboardButton(EXPLOSION_EMOJI if idx == exploded else BOMB_EMOJI, callback_data="x"))
                else:
                    row.append(InlineKeyboardButton(SAFE_EMOJI, callback_data="x"))
            else:
                if clickable:
                    row.append(InlineKeyboardButton(CLOSED_EMOJI, callback_data=f"mines_cell_{idx}|{game_id}"))
                else:
                    row.append(InlineKeyboardButton(CLOSED_EMOJI, callback_data="x"))
        kb.append(row)
    return kb

def controls(game_id, opened_count=0):
    buttons = []
    if opened_count > 0:
        buttons.append([InlineKeyboardButton("💰 Забрать", callback_data=f"mines_take|{game_id}")])
    else:
        buttons.append([InlineKeyboardButton("❌ Отмена", callback_data=f"mines_cancel|{game_id}")])
    return buttons

# ================== Начало игры ==================
@dp.message_handler(Text(startswith=["мины", "/mines", "Мины"]))
async def start_mines(message: types.Message):
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("Пример: мины 1к 3")

    cursor.execute("SELECT balance FROM users WHERE user_id=?", (message.from_user.id,))
    row = cursor.fetchone()
    if not row:
        return await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
    balance = row[0]

    bet = parse_mines_bet(parts[1], balance)
    if bet < MIN_BET or bet > balance:
        return await message.reply("❌ Некорректная ставка")

    mines_count = int(parts[2]) if len(parts) >= 3 else 1
    if not 1 <= mines_count <= 24:
        return await message.reply("❌ Мин от 1 до 24")

    cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (balance-bet, message.from_user.id))
    conn.commit()

    game_id = str(uuid.uuid4())
    mines = set(random.sample(range(MINES_GRID_SIZE*MINES_GRID_SIZE), mines_count))

    # Сохраняем игру
    ACTIVE_MINES_GAMES[game_id] = {
        "user_id": message.from_user.id,
        "bet": bet,
        "num_mines": mines_count,
        "mines": mines,
        "opened": set(),
        "game_over": False
    }
    mines_locks[game_id] = asyncio.Lock()

    # Клавиатура при старте игры — только Отмена, нет честности
    kb = InlineKeyboardMarkup(
        inline_keyboard=build_grid(set(), mines, game_id) + controls(game_id)
    )

    await message.reply(
        f"💣 Мин: {mines_count}\n💰 Ставка: {bet} PLcoins\n💸 Можно забрать: {bet} | х1.00 | {bet} PLcoins",
        reply_markup=kb
    )


# ================== Callback ==================
@dp.callback_query_handler(Text(startswith="mines_"))
async def mines_callback(call: types.CallbackQuery):
    if is_flood(call.from_user.id):
        await call.answer("Не так быстро!", show_alert=False)
        return

    try:
        action, game_id = call.data.split("|")
    except ValueError:
        return await call.answer("Ошибка данных", show_alert=True)

    # ====== Кнопка честности ======
    if action == "mines_fair":
        game_fair = ACTIVE_MINES_GAMES_FAIRNESS.get(game_id)
        if not game_fair:
            return await call.answer("Данные честности недоступны", show_alert=True)

        field_nonce = f"{game_fair['field']}{game_fair['nonce']}"
        url = f"https://codebeautify.org/sha3-256-hash-generator?input={field_nonce}"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("🔗Проверить хэш", url=url)],
                [InlineKeyboardButton("🔙 Назад", callback_data=f"mines_back|{game_id}")]
            ]
        )

        text = (
            "🛡 Доказательство честности\n\n"
            f"💰 Ставка: {game_fair['bet']} PLcoins\n\n"
            f"📦 Поле мин:\n{game_fair['field']}\n\n"
            f"2 - это мина\n0 - это пустая ячейка\n\n"
            f"🔑 Nonce: {game_fair['nonce']}\n"
            f"#️⃣ SHA3-256 Хэш: {game_fair['hash']}\n\n"
            f"Для проверки откройте [SHA3-256 генератор]({url}) и вставьте 'Поле мин+nonce', хэш должен совпадать.\n\n"
            "Мы гарантируем 100% честность и никаких алгоритмов и скриптов 🍀"
        )
        await safe_edit(call.message, text, reply_markup=kb)
        return await call.answer()

    # ====== Кнопка Назад ======
    elif action == "mines_back":
        game = ACTIVE_MINES_GAMES_BACKUP.get(game_id)
        if not game:
            return await call.answer("Данные игры недоступны", show_alert=True)

        opened = set(game.get("opened", []))
        mines = set(game.get("mines", []))
        exploded_cell = game.get("exploded_cell")

        displayed = opened.union(mines) if game.get("game_over", False) and exploded_cell is None else opened

        kb = InlineKeyboardMarkup(
            inline_keyboard=[*build_grid(displayed, mines, game_id, exploded=exploded_cell),
                             [InlineKeyboardButton("🛡Доказать честность", callback_data=f"mines_fair|{game_id}")]]
        )

        if game.get("game_over", False):
            if exploded_cell is not None:
                previous_opened = len(opened) - len(mines)
                mult = 1.0 if previous_opened == 0 else calculate_multiplier(previous_opened, game["num_mines"])
                win = int(game["bet"] * mult)
                text = (
                    f"💥 Вы проиграли!\n"
                    f"└ 💰 Ставка: {game['bet']} PLcoins\n"
                    f"└ 💸 Можно было забрать: {game['bet']} | х{mult:.2f} | {win} PLcoins"
                )
            else:
                mult, win = calculate_take_info(game)
                text = (
                    f"🔥 Вы выиграли!\n"
                    f"💣 Мин: {game['num_mines']}\n"
                    f"💰 Ставка: {game['bet']} PLcoins\n"
                    f"🔥 Открыто ячеек: {len(opened)}\n"
                    f"💸 Выигрыш: {game['bet']} | х{mult:.2f} | {win} PLcoins"
                )
        else:
            mult, win = calculate_take_info(game)
            text = build_game_message(game)

        await safe_edit(call.message, text, reply_markup=kb)
        return await call.answer()

    # ----------- Основная игра -----------
    game = ACTIVE_MINES_GAMES.get(game_id)
    if not game:
        return await call.answer("Игра не найдена", show_alert=True)
    if call.from_user.id != game["user_id"]:
        return await call.answer("❌ Это не ваша игра!", show_alert=True)

    async with mines_locks[game_id]:
        if game.get("game_over", False):
            return await call.answer("Игра окончена")

        # --------- Открытие клетки ---------
        if action.startswith("mines_cell_"):
            cell = int(action[len("mines_cell_"):])
            if cell in game["opened"]:
                return await call.answer("Эта ячейка уже открыта", show_alert=True)

            game["opened"].add(cell)

            if cell in game["mines"]:
                previous_opened = len(game["opened"]) - 1
                mult = 1.0 if previous_opened == 0 else calculate_multiplier(previous_opened, game["num_mines"])
                win = int(game["bet"] * mult)

                game["opened"].update(game["mines"])
                game["game_over"] = True
                game["exploded_cell"] = cell

                # FAIRNESS SHA3-256
                field_str = "".join(["2" if i in game["mines"] else "0" for i in range(MINES_GRID_SIZE*MINES_GRID_SIZE)])
                nonce = secrets.token_hex(16)
                hash_val = hashlib.sha3_256((field_str + nonce).encode()).hexdigest()

                ACTIVE_MINES_GAMES_FAIRNESS[game_id] = {
                    "game_id": game_id,
                    "field": field_str,
                    "mines": list(game["mines"]),
                    "opened": list(game["opened"]),
                    "bet": game["bet"],
                    "nonce": nonce,
                    "hash": hash_val
                }

                ACTIVE_MINES_GAMES_BACKUP[game_id] = game.copy()

                kb = InlineKeyboardMarkup(
                    inline_keyboard=[*build_grid(game["opened"], game["mines"], game_id, exploded=cell),
                                     [InlineKeyboardButton("🛡Доказать честность", callback_data=f"mines_fair|{game_id}")]]
                )

                del ACTIVE_MINES_GAMES[game_id]
                del mines_locks[game_id]

                return await safe_edit(
                    call.message,
                    f"💥 Вы проиграли!\n"
                    f"└ 💰 Ставка: {game['bet']} PLcoins\n"
                    f"└ 💸 Можно было забрать: {game['bet']} | х{mult:.2f} | {win} PLcoins",
                    reply_markup=kb
                )

            # Успешное открытие
            mult, win = calculate_take_info(game)
            text = build_game_message(game)
            kb = InlineKeyboardMarkup(
                inline_keyboard=build_grid(game["opened"], game["mines"], game_id) +
                               controls(game_id, len(game["opened"]))
            )
            await safe_edit(call.message, text, reply_markup=kb)
            return await call.answer()

        # --------- Забрать выигрыш ---------
        elif action == "mines_take":
            mult, win = calculate_take_info(game)
            game["game_over"] = True

            cursor.execute("SELECT balance FROM users WHERE user_id=?", (game["user_id"],))
            balance = cursor.fetchone()[0]
            cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (balance + win, game["user_id"]))
            cursor.execute("UPDATE users SET games_played = games_played + 1 WHERE user_id = ?", (game["user_id"],))
            conn.commit()

            field_str = "".join(["2" if i in game["mines"] else "0" for i in range(MINES_GRID_SIZE*MINES_GRID_SIZE)])
            nonce = secrets.token_hex(16)
            hash_val = hashlib.sha3_256((field_str + nonce).encode()).hexdigest()

            ACTIVE_MINES_GAMES_FAIRNESS[game_id] = {
                "game_id": game_id,
                "field": field_str,
                "mines": list(game["mines"]),
                "opened": list(game["opened"]),
                "bet": game["bet"],
                "nonce": nonce,
                "hash": hash_val
            }

            ACTIVE_MINES_GAMES_BACKUP[game_id] = game.copy()

            kb = InlineKeyboardMarkup(
                inline_keyboard=[*build_grid(game["opened"].union(game["mines"]), game["mines"], game_id),
                                 [InlineKeyboardButton("🛡Доказать честность", callback_data=f"mines_fair|{game_id}")]]
            )

            del ACTIVE_MINES_GAMES[game_id]
            del mines_locks[game_id]

            text = (
                f"🔥 Вы выиграли!\n"
                f"💣 Мин: {game['num_mines']}\n"
                f"💰 Ставка: {game['bet']} PLcoins\n"
                f"🔥 Открыто ячеек: {len(game['opened'])}\n"
                f"💸 Выигрыш: {game['bet']} | х{mult:.2f} | {win} PLcoins"
            )
            return await safe_edit(call.message, text, reply_markup=kb)

        # --------- Отмена ---------
        elif action == "mines_cancel":
            game["game_over"] = True
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (game["user_id"],))
            balance = cursor.fetchone()[0]
            cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (balance + game["bet"], game["user_id"]))
            conn.commit()

            add_game_history(
                user_id=game["user_id"],
                game="Мины",
                bet=game["bet"],
                result="Отмена",
                multiplier=0,
                win=0
            )

            ACTIVE_MINES_GAMES_BACKUP[game_id] = game.copy()
            del ACTIVE_MINES_GAMES[game_id]
            del mines_locks[game_id]

            return await safe_edit(call.message, "❌ Игра отменена. Ставка возвращена",
                                   reply_markup=InlineKeyboardMarkup())



#============================
def get_combo_text(num: int) -> list:
    values = ["BAR", "🍇", "🍋", "7️⃣"]
    num = (num - 1) % 64
    return [values[(num // (4**i)) % 4] for i in range(3)]

def determine_multiplier(combo: list) -> float:
    combo_str = ''.join(combo)

    if combo_str == "7️⃣7️⃣7️⃣":
        return 5.0
    if combo_str == "🍋🍋🍋":
        return 2.0
    if combo_str == "BARBARBAR":
        return 1.5
    if combo_str == "🍇🍇🍇":
        return 1.5

    c = Counter(combo)
    if c["7️⃣"] == 2 and c["🍋"] == 1:
        return 1.5
    
    if c["BAR"] == 2 and c["7️⃣"] == 1:
        return 1.2

    if c["7️⃣"] == 2 and c["BAR"] == 1:
        return 1.2

    if c["7️⃣"] == 2 and c["🍇"] == 1:
        return 1.2

    if c["🍋"] == 2 and c["7️⃣"] == 1:
        return 1.2


    return 0.0

@dp.message_handler(lambda message: message.text and (
        message.text.lower().startswith('слоты') or message.text.lower().startswith('/slots')))
async def slots_handler(message: types.Message):
    user_id = message.from_user.id

    # ===== Проверка на бан =====
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

    # ===== Парсим ставку =====
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply(
            "⚠️ Укажите ставку. Пример: слоты 100 или /slots все",
            parse_mode="HTML"
        )
        return

    bet_str = parts[1].strip().lower()

    cursor.execute(
        'SELECT balance, username, games_played, lost FROM users WHERE user_id = ?',
        (user_id,)
    )
    user_data = cursor.fetchone()

    if not user_data:
        await message.reply(
            "❌ Вы не зарегистрированы. Нажмите /start для регистрации.",
            parse_mode="HTML"
        )
        return

    balance, username, games_played, lost = user_data

    bet = format_stake(bet_str, balance)

    if bet == -1 or bet <= 0:
        await message.reply("⚠️ Некорректная ставка.", parse_mode="HTML")
        return

    if bet < 100:
        await message.reply("⚠️ Минимальная ставка: 100 PLcoins.", parse_mode="HTML")
        return

    if bet > balance:
        await message.reply("❌ Недостаточно средств.", parse_mode="HTML")
        return

    # ===== Списываем ставку =====
    new_balance = balance - bet
    cursor.execute(
        'UPDATE users SET balance = ?, games_played = ? WHERE user_id = ?',
        (new_balance, games_played + 1, user_id)
    )
    conn.commit()

    # ===== Крутим слоты =====
    dice_msg = await message.answer_dice(emoji="🎰")
    await asyncio.sleep(2.5)

    value = dice_msg.dice.value
    combo = get_combo_text(value)
    multiplier = determine_multiplier(combo)

    winnings = int(bet * multiplier) if multiplier > 0 else 0

    # ===== Обработка результата =====
    if winnings > 0:
        new_balance += winnings
        cursor.execute(
            'UPDATE users SET balance = ? WHERE user_id = ?',
            (new_balance, user_id)
        )
        conn.commit()

        add_game_history(
            user_id=user_id,
            game="Слоты",
            bet=bet,
            result="Выигрыш",
            multiplier=multiplier,
            win=winnings
        )
    else:
        new_lost = lost + bet
        cursor.execute(
            'UPDATE users SET lost = ? WHERE user_id = ?',
            (new_lost, user_id)
        )
        conn.commit()

        add_game_history(
            user_id=user_id,
            game="Слоты",
            bet=bet,
            result="Проигрыш",
            multiplier=0,
            win=0
        )

    # ===== Текст результата =====
    combo_display = ' | '.join(combo)
    bet_fmt = format_number(bet)

    text = (
        f"<b>🎰 Слоты</b>\n"
        f"<b>Игрок:</b> {username}\n"
        f"<b>Ставка:</b> {bet_fmt} PLcoins\n"
        f"<b>Комбинация:</b> {combo_display}\n"
    )

    if winnings > 0:
        text += f"🎉 <b>Выигрыш:</b> +{format_number(winnings)} PLcoins!"
    else:
        text += f"❌ <b>Проигрыш:</b> {bet_fmt} PLcoins."

    await message.reply(text, parse_mode="HTML")


#===========================
active_chest_games = {}

MULTIPLIER_POOL = (
    [0] * 26 +      # x0 (проигрыш)
    [0.5] * 24 +    # x0.5
    [1] * 15 +      # x1
    [2] * 10 +      # x2
    [2.5] * 4 +       # x3
    [3] * 3 +       # x5
    [3.5] * 1        # x10 — редкий
)

def generate_chest_grid():
    return random.sample(MULTIPLIER_POOL, 9)

def build_chest_keyboard():
    buttons = []
    for row in range(3):
        row_buttons = []
        for col in range(3):
            index = row * 3 + col
            row_buttons.append(InlineKeyboardButton("🧰", callback_data=f"chest_{index}"))
        buttons.append(row_buttons)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def refund_unplayed_chest_game(user_id: int, bot: Bot):
    """Возврат ставки, если игрок не выбрал сундук."""
    if user_id in active_chest_games:
        game = active_chest_games.pop(user_id)
        bet = game["bet"]

        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()

        if not user_data:
            print(f"Ошибка: Пользователь с ID {user_id} не найден.")
            return

        balance = user_data[0]
        new_balance = balance + bet

        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        conn.commit()

        try:
            await bot.send_message(
                chat_id=user_id,
                text="⌛ <b>Ты не выбрал сундук вовремя. Ставка возвращена.</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.exception(f"Ошибка при отправке сообщения о возврате ставки: {e}")

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/chests') or message.text.lower().startswith('честы')))
async def chests_command(message: types.Message):
    """Команда запуска сундуков."""
    user_id = message.from_user.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()

    if not user_data:
        await message.reply("<b>Вы не зарегистрированы. Нажмите /start для регистрации.</b>", parse_mode="HTML")
        return

    balance, games_played, lost = user_data

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("⚠️ <b>Укажи ставку для игры в сундуки. Пример: /chests 1000</b>", parse_mode="HTML")
        return

    bet_str = parts[1].strip().lower()
    bet = format_stake(bet_str, balance)

    if bet == -1 or bet < 100:
        await message.reply("❌ <b>Минимальная ставка — 100 PLcoins.</b>", parse_mode="HTML")
        return

    if bet > balance:
        await message.reply("❌ <b>Недостаточно средств.</b>", parse_mode="HTML")
        return

    if user_id in active_chest_games:
        await message.reply("⚠️ <b>У тебя уже есть активная игра. Сначала выбери сундук.</b>", parse_mode="HTML")
        return

    new_balance = balance - bet

    grid = generate_chest_grid()
    active_chest_games[user_id] = {
        "bet": bet,
        "grid": grid,
        "games_played": games_played  # Сохраняем текущее значение games_played
    }

    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
    conn.commit()

    keyboard = build_chest_keyboard()
    await message.reply(
        f"<b>🎁 Игра в 'Сундуки удачи' началась!</b>\n"
        f"<b>💰Ставка:</b> {format_number(bet)} PLcoins\n"
        f"<b>Выбери один сундук:</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

    # Запуск возврата через 30 секунд
    asyncio.create_task(delayed_refund(user_id, bot))

async def delayed_refund(user_id: int, bot: Bot):
    await asyncio.sleep(30)
    await refund_unplayed_chest_game(user_id, bot)

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.dispatcher.filters import Text

@dp.callback_query_handler(Text(startswith="chest_"))
async def chests_button_handler(query: types.CallbackQuery):
    """Обработка выбора сундука."""
    user_id = query.from_user.id

    if user_id not in active_chest_games:
        await query.answer("⛔ У тебя нет активной игры.")
        return

    data = query.data
    idx = int(data.split("_")[1])
    game = active_chest_games.pop(user_id)  # Удаляем игру сразу

    original_grid = game["grid"]
    multiplier = original_grid[idx]

    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()

    if not user_data:
        await query.answer("❌ <b>Вы не зарегистрированы. Нажмите /start для регистрации.</b>", parse_mode="HTML")
        return

    balance, games_played, lost = user_data
    bet = game["bet"]
    reward = int(bet * multiplier)

    if reward > 0:
        # Игрок выиграл
        new_balance = balance + reward
        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        result_text = "Выигрыш"
    else:
        # Игрок проиграл
        new_lost = lost + bet
        cursor.execute('UPDATE users SET lost = ? WHERE user_id = ?', (new_lost, user_id))
        result_text = "Проигрыш"

    # Увеличиваем games_played только один раз за игру
    if game["games_played"] == games_played:  # Если значение не изменилось
        cursor.execute('UPDATE users SET games_played = ? WHERE user_id = ?', (games_played + 1, user_id))

    conn.commit()

    # ----------------- Добавляем историю игры -----------------
    add_game_history(
        user_id=user_id,
        game="Сундуки удачи",
        bet=bet,
        result=result_text,
        multiplier=multiplier,
        win=reward
    )

    # Формируем текст сообщения
    text = (
        f"🎉 <b>Ты нашёл:</b> x{multiplier}!\n💰 <b>Выигрыш:</b> {format_number(reward)} PLcoins"
        if reward > 0
        else "💥 <b>Ты нашёл</b> x0! <b>Сундук пустой. Попробуй ещё раз.</b>"
    )

    # Создаём клавиатуру с результатами
    result_keyboard = []
    for i in range(3):
        row = []
        for j in range(3):
            val = original_grid[i * 3 + j]
            emoji = {
                0: "💥",
                0.5: "🎉",
                1: "💎",
                2: "🍀",
                2.5: "🎁",
                3: "💍",
                3.5: "👑"
            }.get(val, "❓")
            row.append(InlineKeyboardButton(f"{emoji} x{val}", callback_data="none"))
        result_keyboard.append(row)

    await bot.edit_message_text(
        text=text,
        chat_id=query.message.chat.id,
        message_id=query.message.message_id,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=result_keyboard),
        parse_mode="HTML"
    )

#===================================

def sum_cards(cards):
    total = sum(cards)
    aces = cards.count(11)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total

blackjack_games = {}
active_blackjack_games = {}  # user_id -> game state

def deal_card():
    return random.choice([2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11])

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/21') or message.text.lower().startswith('бж')))
async def blackjack_command(message: types.Message):
    user_id = message.from_user.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("⚠️ Укажите ставку. Пример: /21 1к", parse_mode="HTML")
        return

    bet_str = parts[1].strip().lower()

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()

    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.", parse_mode="HTML")
        return

    balance = user_data[0]
    bet = format_stake(bet_str, balance)

    # Добавляем проверку на минимальную ставку
    MINIMAL_BET = 100  # Определяем минимальную ставку
    if bet < MINIMAL_BET:
        await message.reply(f"❌ Минимальная ставка — {MINIMAL_BET} PLcoins.", parse_mode="HTML")
        return
    # Конец проверки на минимальную ставку


    if bet <= 0:
        await message.reply("❌ Неверная ставка.", parse_mode="HTML")
        return

    if balance < bet:
        await message.reply("❌ Недостаточно средств.", parse_mode="HTML")
        return

    new_balance = balance - bet
    cursor.execute('UPDATE users SET balance = ?, games_played = games_played + 1 WHERE user_id = ?', (new_balance, user_id))
    conn.commit()

    # Раздача карт
    player = [deal_card(), deal_card()]
    dealer = [deal_card(), deal_card()]

    player_total = sum_cards(player)
    dealer_total = sum_cards(dealer)
    
    # Проверка на блэкджек сразу после раздачи
    if player_total == 21:
        win_amount = int(bet * 1.7)
        new_balance += win_amount
        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        conn.commit()

        active_blackjack_games[user_id] = {
            "bet": bet,
            "state": "ended",
            "player": player,
            "dealer": dealer
        }
        
        del active_blackjack_games[user_id]

        win_amount_str = format_number(win_amount)

        await message.reply(
            f"<b>🎉♥️Black-jack!</b>\n<b>Ты выиграл:</b> +{win_amount_str} PLcoins 🎉\n\n"
            f"<b>🤵‍♂️ Дилер:</b> {dealer} ({dealer_total})\n"
            f"-----------------\n"
            f"<b>🫵 Ты:</b> {player} ({player_total})",
            parse_mode="HTML"
        )
        return

    # Сохраняем игру
    active_blackjack_games[user_id] = {
        "bet": bet,
        "state": "playing",
        "player": player,
        "dealer": dealer
    }

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
         InlineKeyboardButton("🛑 Стоп", callback_data=f"bj_stop_{user_id}"),
         InlineKeyboardButton("➕ Ещё", callback_data=f"bj_more_{user_id}")
    )

    bet_str = format_number(bet)
    await message.reply(
        f"<b>Ты запустил игру в 21!</b>\n<b>💸 Ставка:</b> {bet_str} PLcoins\n\n"
        f"<b>🤵‍♂️ Дилер:</b> {dealer[0]},❓\n"  # Скрываем вторую карту дилера
        f"-----------------\n"
        f"<b>🫵 Ты:</b> {player} ({player_total})",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query_handler(Text(startswith="bj_"))
async def blackjack_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split('_')

    if len(parts) != 3 or str(user_id) != parts[2]:
        await call.answer("❌ Не ваша игра", show_alert=True)
        return

    game = active_blackjack_games.get(user_id)
    if not game:
        await call.answer("Игра не найдена.", show_alert=True)
        return

    bet = game["bet"]

    # ===== ЕЩЁ =====
    if parts[1] == "more":
        game["player"].append(deal_card())
        total = sum_cards(game["player"])

        if total > 21:
            cursor.execute('SELECT lost FROM users WHERE user_id = ?', (user_id,))
            lost = cursor.fetchone()[0]

            cursor.execute(
                'UPDATE users SET lost = ? WHERE user_id = ?',
                (lost + bet, user_id)
            )
            conn.commit()

            add_game_history(
                user_id=user_id,
                game="21",
                bet=bet,
                result="Проигрыш",
                multiplier=0,
                win=0
            )

            del active_blackjack_games[user_id]

            await call.message.edit_text(
                f"💥 <b>Перебор!</b>\n"
                f"🫵 Ты: {game['player']} ({total})",
                parse_mode="HTML"
            )
            return

        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("➕ Ещё", callback_data=f"bj_more_{user_id}"),
            InlineKeyboardButton("🛑 Стоп", callback_data=f"bj_stop_{user_id}")
        )

        await call.message.edit_text(
            f"🤵‍♂️ Дилер: {game['dealer'][0]}, ❓\n"
            f"🫵 Ты: {game['player']} ({total})",
            reply_markup=kb,
            parse_mode="HTML"
        )

    # ===== СТОП =====
    elif parts[1] == "stop":
        dealer = game["dealer"]
        while sum_cards(dealer) < 17:
            dealer.append(deal_card())

        player_total = sum_cards(game["player"])
        dealer_total = sum_cards(dealer)

        cursor.execute('SELECT balance, lost FROM users WHERE user_id = ?', (user_id,))
        balance, lost = cursor.fetchone()

        # ===== ПОБЕДА =====
        if dealer_total > 21 or player_total > dealer_total:
            win_amount = int(bet * 1.7)
            cursor.execute(
                'UPDATE users SET balance = ? WHERE user_id = ?',
                (balance + win_amount, user_id)
            )
            conn.commit()

            add_game_history(
                user_id=user_id,
                game="21",
                bet=bet,
                result="Выигрыш",
                multiplier=1.7,
                win=win_amount
            )

            result_text = f"🎉 Победа! +{format_number(win_amount)} PLcoins"

        # ===== НИЧЬЯ =====
        elif dealer_total == player_total:
            cursor.execute(
                'UPDATE users SET balance = ? WHERE user_id = ?',
                (balance + bet, user_id)
            )
            conn.commit()

            add_game_history(
                user_id=user_id,
                game="21",
                bet=bet,
                result="Ничья",
                multiplier=1.0,
                win=0
            )

            result_text = "🤝 Ничья! Ставка возвращена."

        # ===== ПРОИГРЫШ =====
        else:
            cursor.execute(
                'UPDATE users SET lost = ? WHERE user_id = ?',
                (lost + bet, user_id)
            )
            conn.commit()

            add_game_history(
                user_id=user_id,
                game="21",
                bet=bet,
                result="Проигрыш",
                multiplier=0,
                win=0
            )

            result_text = "😞 Проигрыш."

        del active_blackjack_games[user_id]

        await call.message.edit_text(
            f"🎲 <b>Игра окончена</b>\n\n"
            f"🤵‍♂️ Дилер: {dealer} ({dealer_total})\n"
            f"🫵 Ты: {game['player']} ({player_total})\n\n"
            f"{result_text}",
            parse_mode="HTML"
        )


active_kn_games = {}
KN_MIN_BET = 100
MOVE_TIMEOUT = 300  # 5 минут

# ================== ФУНКЦИИ ==================
def build_kn_board(board, game_id):
    buttons = []
    for i in range(0, 9, 3):
        row = []
        for j in range(3):
            cell = board[i + j]
            label = "❌" if cell == "X" else "⭕️" if cell == "O" else "⬜️"
            row.append(InlineKeyboardButton(label, callback_data=f"kn_move|{game_id}|{i + j}"))
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def check_kn_winner(board):
    wins = [
        [0,1,2],[3,4,5],[6,7,8],
        [0,3,6],[1,4,7],[2,5,8],
        [0,4,8],[2,4,6]
    ]
    for a,b,c in wins:
        if board[a]==board[b]==board[c]!=" ":
            return board[a]
    return None

async def get_user_display_name(user_id: int) -> str:
    cursor.execute('SELECT nickname, username, first_name FROM users WHERE user_id=?',(user_id,))
    result = cursor.fetchone()
    if result:
        nickname, username, first_name = result
        if nickname: return nickname
        if username: return username
        if first_name: return first_name
    return f"ID:{user_id}"

# ================== КОМАНДА НА СТАРТ ==================
@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/kn') or message.text.lower().startswith('крестики')))
async def cmd_kn(message: types.Message):
    user_id = message.from_user.id

    # Проверка на регистрацию и бан
    cursor.execute('SELECT balance, ban_until, ban_reason FROM users WHERE user_id=?', (user_id,))
    row = cursor.fetchone()
    if not row:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance, ban_until, ban_reason = row

    # Проверка на активный бан
    if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
        await message.reply(
            f"🚫 Вы забанены до {ban_until}. Причина: {ban_reason} 🚫",
            parse_mode="HTML"
        )
        return

    # Если бан истёк — снимаем
    if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') <= datetime.now():
        cursor.execute('UPDATE users SET ban_until=NULL, ban_reason=NULL WHERE user_id=?', (user_id,))
        conn.commit()

    # Разбор команды
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply("❌ Укажи ставку, например: крестики 100")
        return

    bet = int(parts[1])
    if bet < KN_MIN_BET:
        await message.reply(f"❌ Минимальная ставка: {KN_MIN_BET}")
        return

    if balance < bet:
        await message.reply("❌ Недостаточно средств.")
        return

    # Снимаем ставку
    cursor.execute('UPDATE users SET balance=balance-? WHERE user_id=?', (bet, user_id))
    conn.commit()

    game_id = str(uuid.uuid4())
    active_kn_games[game_id] = {
        "chat_id": message.chat.id,
        "player_x": user_id,
        "player_o": None,
        "turn": "X",
        "bet": bet,
        "board": [" "] * 9,
        "message_id": None,
        "last_move_time": datetime.now().timestamp()
    }

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton("✅ Принять", callback_data=f"kn_accept|{game_id}|{user_id}"),
            InlineKeyboardButton("❌ Отмена", callback_data=f"kn_cancel|{game_id}|{user_id}")
        ]
    ])

    display_name = await get_user_display_name(user_id)
    msg = await message.answer(
        f"🎮 Игра в крестики-нолики 3x3 на {bet} PLcoins\n\n"
        f"❌ {display_name} ждёт соперника.",
        reply_markup=keyboard
    )
    active_kn_games[game_id]["message_id"] = msg.message_id
    
# ================== CALLBACK ==================
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("kn_"))
async def callback_kn_handler(query: CallbackQuery):
    data = query.data.split("|")
    action = data[0]  # kn_accept / kn_cancel / kn_move
    game_id = data[1]

    if game_id not in active_kn_games:
        await query.answer("❌ Игра не найдена.", show_alert=True)
        return

    game = active_kn_games[game_id]
    user_id = query.from_user.id

    # ===== ПРОВЕРКА БАНА =====
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id=?', (user_id,))
    row = cursor.fetchone()
    if row and row[0] and datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S') > datetime.now():
        await query.answer(f"🚫 Вы забанены до {row[0]}. Причина: {row[1]}", show_alert=True)
        return

    # ===== ПРИНЯТИЕ ИГРЫ =====
    if action == "kn_accept":
        creator_id = int(data[2])
        if user_id == creator_id:
            await query.answer("❌ Ты не можешь принять свою игру.", show_alert=True)
            return
        if game["player_o"] is not None:
            await query.answer("❌ Игра уже началась.", show_alert=True)
            return
        await kn_accept(query, game, game_id, user_id)
        return

    # ===== ОТМЕНА =====
    if action == "kn_cancel":
        creator_id = int(data[2])
        if user_id != creator_id:
            await query.answer("❌ Только создатель игры может отменить её.", show_alert=True)
            return
        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?',(game["bet"],creator_id))
        conn.commit()
        await query.message.edit_text("🚫 Игра отменена создателем. Ставка возвращена.")
        del active_kn_games[game_id]
        return

    # ===== ХОД =====
    if action == "kn_move":
        idx = int(data[2])
        await handle_move(query, game, game_id, idx, user_id)


# ================== ФУНКЦИЯ ACCEPT ==================
async def kn_accept(query, game, game_id, user_id):
    cursor.execute('SELECT balance FROM users WHERE user_id=?',(user_id,))
    balance_o = cursor.fetchone()[0]
    if balance_o < game["bet"]:
        await query.answer("❌ Недостаточно средств.", show_alert=True)
        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?',(game["bet"],game["player_x"]))
        conn.commit()
        del active_kn_games[game_id]
        await query.message.edit_text("🚫 Игра отменена. Ставка возвращена первому игроку.")
        return

    cursor.execute('UPDATE users SET balance=balance-? WHERE user_id=?',(game["bet"],user_id))
    conn.commit()

    game["player_o"] = user_id
    game["last_move_time"] = datetime.now().timestamp()

    player_name_x = await get_user_display_name(game["player_x"])
    player_name_o = await get_user_display_name(game["player_o"])

    keyboard = build_kn_board(game["board"], game_id)
    await query.message.edit_text(
        f"❌⭕️ Крестики-нолики 3x3 на {game['bet']} PLcoins\n\n"
        f"❌ {player_name_x}\n"
        f"⭕️ {player_name_o}\n\n"
        f"Ходит ❌ {player_name_x}",
        reply_markup=keyboard
    )
    await query.answer("✅ Игра началась!")

# ================== ХОД ==================
# ================== ХОД ==================
async def handle_move(query, game, game_id, idx, user_id):
    if game["board"][idx] != " ":
        await query.answer("⛔️ Ячейка занята.", show_alert=True)
        return

    if (game["turn"] == "X" and user_id != game["player_x"]) or (game["turn"] == "O" and user_id != game["player_o"]):
        await query.answer("❌ Сейчас ходит другой игрок.", show_alert=True)
        return

    # Ставим символ
    game["board"][idx] = game["turn"]
    game["last_move_time"] = datetime.now().timestamp()

    player_name_x = await get_user_display_name(game["player_x"])
    player_name_o = await get_user_display_name(game["player_o"])

    # Проверка победителя
    winner = check_kn_winner(game["board"])
    if winner:
        winner_id = game["player_x"] if winner == "X" else game["player_o"]
        losing_id = game["player_o"] if winner_id == game["player_x"] else game["player_x"]
        winning_amount = game["bet"] * 2

        # Обновляем баланс победителя
        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (winning_amount, winner_id))
        conn.commit()

        # --- Добавляем в историю игры ---
        add_game_history(
            user_id=winner_id,
            game="Крестики-нолики",
            bet=game["bet"],
            result="Выигрыш",
            multiplier=2,
            win=winning_amount
        )
        add_game_history(
            user_id=losing_id,
            game="Крестики-нолики",
            bet=game["bet"],
            result="Проигрыш",
            multiplier=0,
            win=0
        )

        winner_name = await get_user_display_name(winner_id)
        loser_name = await get_user_display_name(losing_id)

        await query.message.edit_text(
            f"🏆 Победа!\n\n"
            f"✨ {winner_name} выиграл {winning_amount} PLcoins\n"
            f"😢 {loser_name} остался без ставки\n\n"
            f"Игра завершена, поле закрыто."
        )
        del active_kn_games[game_id]
        return

    # Проверка ничьи
    if " " not in game["board"]:
        # Возврат ставок
        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (game["bet"], game["player_x"]))
        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (game["bet"], game["player_o"]))
        conn.commit()

        # --- Добавляем в историю игры ---
        add_game_history(
            user_id=game["player_x"],
            game="Крестики-нолики",
            bet=game["bet"],
            result="Ничья",
            multiplier=1,
            win=game["bet"]
        )
        add_game_history(
            user_id=game["player_o"],
            game="Крестики-нолики",
            bet=game["bet"],
            result="Ничья",
            multiplier=1,
            win=game["bet"]
        )

        await query.message.edit_text(
            f"🤝 Ничья!\n\n"
            f"❌ {player_name_x}\n"
            f"⭕️ {player_name_o}\n\n"
            f"Ставки возвращены, поле закрыто."
        )
        del active_kn_games[game_id]
        return

    # Меняем ход
    game["turn"] = "O" if game["turn"] == "X" else "X"
    next_player = game["player_x"] if game["turn"] == "X" else game["player_o"]
    next_name = await get_user_display_name(next_player)

    await query.message.edit_text(
        f"❌⭕️ Крестики-нолики 3x3 на {game['bet']} PLcoins\n\n"
        f"❌ {player_name_x}\n"
        f"⭕️ {player_name_o}\n\n"
        f"Ходит {'❌' if game['turn']=='X' else '⭕️'} {next_name}",
        reply_markup=build_kn_board(game["board"], game_id)
    )


# ================== ПРОВЕРКА ТАЙМАУТА ==================
async def kn_timeout_checker():
    while True:
        now = datetime.now().timestamp()
        to_remove = []
        for game_id, game in list(active_kn_games.items()):
            if game.get("player_o") is None:
                continue
            if now - game["last_move_time"] > MOVE_TIMEOUT:
                loser_id = game["player_x"] if game["turn"] == "X" else game["player_o"]
                winner_id = game["player_o"] if loser_id == game["player_x"] else game["player_x"]
                winning_amount = game["bet"] * 2

                cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?',(winning_amount,winner_id))
                conn.commit()

                loser_name = await get_user_display_name(loser_id)
                winner_name = await get_user_display_name(winner_id)

                try:
                    await bot.edit_message_text(
                        chat_id=game["chat_id"],
                        message_id=game["message_id"],
                        text=(f"⏰ {loser_name} не сделал ход 5 минут!\n\n"
                              f"🏆 Победил {winner_name} (+{winning_amount} PLcoins)")
                    )
                except Exception:
                    pass

                to_remove.append(game_id)

        for gid in to_remove:
            active_kn_games.pop(gid, None)

        await asyncio.sleep(10)

# ================== СТАРТ ==================
async def on_startup(dp):
    asyncio.create_task(kn_timeout_checker())

#==========================================
# Словарь для отслеживания последнего использования команды
last_use = {}

async def try_edit_message(message, text=None, reply_markup=None, parse_mode='html', retries=20):
    """Функция для редактирования сообщения с обработкой ошибок."""
    for attempt in range(retries):
        try:
            if text is not None or reply_markup is not None:
                await message.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            return
        except exceptions.MessageNotModified:
            logging.warning("Сообщение не изменено.")
            return
        except Exception as e:
            logging.error(f"Не удалось обновить сообщение (попытка {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2)
                continue
            else:
                logging.error(f"Не удалось обновить сообщение после {retries} попыток: {e}")
                return

def create_tower_buttons(user_id, current_level, bombs, diamonds, game_field):
    cursor.execute("SELECT game_field, coefficient, bomb_count FROM tower WHERE user_id=?", (user_id,))
    result = cursor.fetchone()

    if not result:
        return InlineKeyboardMarkup(row_width=5)

    game_field_text, coefficient, bomb_count = result
    game_field = list(map(int, game_field_text.split(','))) if game_field_text else []

    buttons = []

    for i in range(1, 51):
        # Определяем уровень кнопки
        level = (i - 1) // 5 + 1

        # Если уровень меньше или равен текущему, отображаем кнопку
        if level <= current_level:
            if i in game_field:
                # Кнопка уже открыта
                if i in diamonds:
                    button_text = '💎'  # Алмаз
                elif i in bombs:
                    button_text = '💣'  # Бомба
                else:
                    button_text = '❌'  # Пустая клетка
            elif level == current_level:
                # Кнопка текущего уровня
                button_text = '❓'
            else:
                button_text = ' '  # Пустое место, чтобы не отображать кнопку

            buttons.append(InlineKeyboardButton(text=button_text, callback_data=f"tower_{i}_{user_id}"))
        else:
            # Если уровень больше текущего, пропускаем кнопку
            continue

    keyboard = InlineKeyboardMarkup(row_width=5)
    # Разделяем кнопки на ряды по 5 штук
    for i in range(0, len(buttons), 5):
        keyboard.row(*buttons[i:i + 5])

    keyboard.add(InlineKeyboardButton("🔄 Автовыбор", callback_data=f'tower_auto_{user_id}'))

    if game_field:
        keyboard.add(InlineKeyboardButton(f"✅ Забрать выигрыш x{coefficient:.2f}", callback_data=f'tower_claim_{user_id}'))
    else:
        keyboard.add(InlineKeyboardButton("❌ Отменить игру", callback_data=f'tower_cancel_{user_id}'))

    return keyboard

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/tower') or message.text.lower().startswith('башня')))
async def tower_handler(message: types.Message):
    user_id = message.from_user.id

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(f"🚫 Вы забанены до {ban_until}, причина: {ban_reason}", parse_mode="HTML")
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Кулдаун 3 секунды
    if last_use.get(user_id) and time.time() - last_use[user_id] < 3:
        await message.reply("❌ Попробуйте через 3 секунды!", parse_mode='HTML')
        return
    last_use[user_id] = time.time()

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("⚠️ Используйте: /tower (ставка) (кол-во бомб от 1 до 4). Пример: /tower 100 2", parse_mode="HTML")
        return

    bet_str = parts[1].lower()
    bomb_count = 1
    if len(parts) > 2:
        try:
            bomb_count = int(parts[2])
            if bomb_count < 1 or bomb_count > 4:
                await message.reply("⚠️ Количество бомб должно быть от 1 до 4.", parse_mode="HTML")
                return
        except ValueError:
            await message.reply("⚠️ Количество бомб должно быть числом от 1 до 4.", parse_mode="HTML")
            return

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()

    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.", parse_mode="HTML")
        return

    balance = user_data[0]
    stake = format_stake(bet_str, balance)
    if stake == -1:
        await message.reply("❗ Некорректная ставка.", parse_mode="HTML")
        return

    if stake < 100:
        await message.reply("❗ Минимальная ставка 100 PLcoins.", parse_mode="HTML")
        return

    if stake > balance:
        await message.reply("❗ Недостаточно средств на балансе.", parse_mode="HTML")
        return

    # Проверка активной игры
    cursor.execute("SELECT * FROM tower WHERE user_id = ?", (user_id,))
    if cursor.fetchone():
        await message.reply("❗ У вас уже есть активная игра в Башню. Продолжите её или отмените.", parse_mode="HTML")
        return

    # Списание ставки
    new_balance = balance - stake
    cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
    conn.commit()

    # Создание бомб и алмазов
    bombs = []
    diamonds = []
    for lvl in range(1, 11):
        level_cells = list(range((lvl - 1) * 5 + 1, lvl * 5 + 1))
        level_bombs = random.sample(level_cells, bomb_count)
        level_diamonds = [c for c in level_cells if c not in level_bombs]
        bombs.extend(level_bombs)
        diamonds.extend(level_diamonds)

    bomb_indexes = ','.join(str(i) for i in bombs)
    diamond_indexes = ','.join(str(i) for i in diamonds)

    cursor.execute(
        "INSERT INTO tower (user_id, bomb_indexes, diamond_indexes, coefficient, amount, game_field, current_level, bomb_count, game_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, bomb_indexes, diamond_indexes, 1.0, stake, '', 1, bomb_count, 1)
    )
    conn.commit()

    keyboard = create_tower_buttons(user_id, 1, bombs, diamonds, [])

    try:
        await message.reply(
            f"<b>Вы начали игру в Башню!</b>\n"
            f"<b>💣 Мин в башне:</b> {bomb_count}\n"
            f"<b>💰 Ставка:</b> {format_number(stake)} PLcoins\n"
            f"<b>Выберите одну из 5 закрытых ячеек для прохождения 1-го уровня👇</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except Exception as e:
        cursor.execute("DELETE FROM tower WHERE user_id = ?", (user_id,))
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (stake, user_id))
        conn.commit()
        await message.reply("⚠️ Возникла ошибка при запуске игры. Ставка возвращена. Попробуйте ещё раз.", parse_mode="HTML")
        return


ACTIVE_TOWER_GAMES = {}

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('tower_'))
async def tower_callback_handler(callback_query: types.CallbackQuery):
    data = callback_query.data.split('_')
    action = data[1]
    user_id = int(data[2])

    if callback_query.from_user.id != user_id:
        await callback_query.answer("❗ Это не ваши кнопки!", show_alert=True)
        return

    if action == 'auto':
        await tower_auto_select(callback_query)
    elif action == 'claim':
        await tower_claim(callback_query)
    elif action == 'cancel':
        await tower_cancel(callback_query)
    else:
        # Это индекс клетки
        try:
            cell_index = int(action)
            await tower_cell_select(callback_query, user_id, cell_index)
        except Exception as e:
            await callback_query.answer("Ошибка обработки выбора клетки.", show_alert=True)

async def tower_cell_select(callback_query: types.CallbackQuery, user_id: int, cell_index: int):
    cursor.execute("SELECT bomb_indexes, diamond_indexes, coefficient, game_field, current_level, amount, bomb_count FROM tower WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()

    if not res:
        await callback_query.answer("Игра не найдена.", show_alert=True)
        return

    bomb_indexes, diamond_indexes, coefficient, game_field_text, current_level, amount, bomb_count = res
    bombs = list(map(int, bomb_indexes.split(',')))
    diamonds = list(map(int, diamond_indexes.split(',')))
    game_field = list(map(int, game_field_text.split(','))) if game_field_text else []

    level = (cell_index - 1) // 5 + 1
    if level < current_level:
        await callback_query.answer("❗️Вы уже выбрали проход", show_alert=True)
        return

    if cell_index in game_field:
        await callback_query.answer("Эта ячейка уже открыта.", show_alert=True)
        return

    game_field.append(cell_index)

    if cell_index in bombs:
        # Проигрыш
        cursor.execute("DELETE FROM tower WHERE user_id = ?", (user_id,))
        ACTIVE_TOWER_GAMES.pop(user_id, None)
        cursor.execute(
            "UPDATE users SET lost = lost + ?, games_played = games_played + 1 WHERE user_id = ?",
            (amount, user_id)
        )
        conn.commit()

        # Добавляем историю игры — проигрыш
        add_game_history(
            user_id=user_id,
            game="Башня",
            bet=amount,
            result="Проигрыш",
            multiplier=coefficient,
            win=0
        )

        keyboard = create_tower_buttons(user_id, current_level, bombs, diamonds, game_field)
        await try_edit_message(
            callback_query.message,
            text=f"<b>💥 Вы наткнулись на бомбу и проиграли!</b>\n<b>💰 Ставка:</b> {format_number(amount)} PLcoins",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await safe_answer_callback(callback_query)
        return

    # Успешно прошли уровень
    current_level += 1
    if bomb_count == 1:
        coefficient *= 1.22
    elif bomb_count == 2:
        coefficient *= 1.63
    elif bomb_count == 3:
        coefficient *= 2.45
    elif bomb_count == 4:
        coefficient *= 4.9

    if current_level > 10:
        # Победа
        winnings = int(round(amount * coefficient))
        cursor.execute("UPDATE users SET balance = balance + ?, games_played = games_played + 1 WHERE user_id = ?", (winnings, user_id))
        cursor.execute("DELETE FROM tower WHERE user_id = ?", (user_id,))
        ACTIVE_TOWER_GAMES.pop(user_id, None)
        conn.commit()

        # Добавляем историю игры — победа
        add_game_history(
            user_id=user_id,
            game="Башня",
            bet=amount,
            result="Выигрыш",
            multiplier=coefficient,
            win=winnings
        )

        await try_edit_message(
            callback_query.message,
            text=f"<b>🎉 Поздравляем! Вы прошли Башню!</b>\n<b>💰 Выигрыш:</b> +{format_number(winnings)} PLcoins (x{coefficient:.2f})",
            parse_mode="HTML"
        )
        await safe_answer_callback(callback_query)
        return

    # Обновляем игру в БД
    game_field_text = ','.join(map(str, game_field))
    cursor.execute(
        "UPDATE tower SET coefficient = ?, game_field = ?, current_level = ? WHERE user_id = ?",
        (coefficient, game_field_text, current_level, user_id)
    )
    conn.commit()

    keyboard = create_tower_buttons(user_id, current_level, bombs, diamonds, game_field)
    await try_edit_message(
        callback_query.message,
        text=f"<b>📊Уровень: {current_level}</b>\n<b>💣Мин в башне:</b> {bomb_count}\n<b>💰 Ставка:</b> {format_number(amount)} PLcoins\n<b>Выберите одну из 5 закрытых ячеек👇</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await safe_answer_callback(callback_query)


async def tower_auto_select(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split('_')[2])

    cursor.execute("SELECT bomb_indexes, diamond_indexes, game_field, current_level FROM tower WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()

    if not res:
        await callback_query.answer("Игра не найдена.", show_alert=True)
        return

    bomb_indexes, diamond_indexes, game_field_text, current_level = res
    game_field = list(map(int, game_field_text.split(','))) if game_field_text else []

    closed_cells = [i for i in range(1 + (current_level - 1) * 5, 6 + (current_level - 1) * 5) if i not in game_field]
    if not closed_cells:
        await callback_query.answer("Нет доступных ячеек.", show_alert=True)
        return

    selected_cell = random.choice(closed_cells)
    callback_query.data = f'tower_{selected_cell}_{user_id}'
    await tower_cell_select(callback_query, user_id, selected_cell)

async def tower_claim(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split('_')[2])

    cursor.execute("SELECT coefficient, amount FROM tower WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()

    if not res:
        await callback_query.answer("Игра не найдена.", show_alert=True)
        return

    coefficient, amount = res
    winnings = int(round(amount * coefficient))

    cursor.execute("UPDATE users SET balance = balance + ?, games_played = games_played + 1 WHERE user_id = ?", (winnings, user_id))
    cursor.execute("DELETE FROM tower WHERE user_id = ?", (user_id,))
    ACTIVE_TOWER_GAMES.pop(user_id, None)
    conn.commit()

    # Добавляем историю игры — выигрыш при claim
    add_game_history(
        user_id=user_id,
        game="Башня",
        bet=amount,
        result="Выигрыш",
        multiplier=coefficient,
        win=winnings
    )

    await try_edit_message(
        callback_query.message,
        text=f"<b>✅ Вы забрали выигрыш!</b>\n<b>💰 Сумма:</b> +{format_number(winnings)} PLcoins (x{coefficient:.2f})",
        parse_mode="HTML"
    )
    await safe_answer_callback(callback_query)


async def tower_cancel(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split('_')[2])

    cursor.execute("SELECT amount FROM tower WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()

    if not res:
        await callback_query.answer("Игра не найдена.", show_alert=True)
        return

    amount = res[0]

    cursor.execute("UPDATE users SET balance = balance + ?, games_played = games_played + 1 WHERE user_id = ?", (amount, user_id))
    cursor.execute("DELETE FROM tower WHERE user_id = ?", (user_id,))
    ACTIVE_TOWER_GAMES.pop(user_id, None)
    conn.commit()

    # Добавляем историю игры — отмена
    add_game_history(
        user_id=user_id,
        game="Башня",
        bet=amount,
        result="Отмена",
        multiplier=0,
        win=0
    )

    await try_edit_message(
        callback_query.message,
        text=f"<b>ℹ️ Игра в Башню отменена.</b>\n<b>💰Ваша ставка:</b> {format_number(int(amount))} PLcoins возвращена.",
        parse_mode="HTML"
    )
    await safe_answer_callback(callback_query)
#=============================================
# -------------------- Рулетка --------------------

# Глобальные переменные для рулетки (для одной игры на чат)
current_bets = {}  # {chat_id: {user_id: [{bet}, {bet}]}}
result_log = deque(maxlen=10)  # История последних результатов
roulette_active = False  # Флаг, активна ли рулетка в данный момент
all_bets = {} # Словарь для хранения всех ставок в текущей игре
# Функция для проверки, является ли ставка на число корректной
def is_valid_number_bet(number):
    try:
        number = int(number)
        return 0 <= number <= 36
    except ValueError:
        return False

def roll_roulette():
    return random.randint(0, 36)

def get_color(number):
    if number == 0:
        return "🟢"
    elif number % 2 == 0:
        return "⚫"
    else:
        return "🔴"

def is_odd(number):
    return number % 2 != 0

def is_even(number):
    return number % 2 == 0

def check_win(bet_type, bet_value, result):
    if bet_type == 'к':
        return get_color(result) == '🔴' and bet_value == 'к'
    elif bet_type == 'ч':
        return get_color(result) == '⚫' and bet_value == 'ч'
    elif bet_type == 'одд':
        return is_odd(result) and bet_value == 'одд'
    elif bet_type == 'евен':
        return is_even(result) and bet_value == 'евен'
    elif bet_type == 'число':
        return int(bet_value) == result
    elif bet_type == 'числа':
        numbers = list(map(int, bet_value.split()))
        return result in numbers
    elif bet_type == 'диапазон1':
        return 1 <= result <= 12
    elif bet_type == 'диапазон2':
        return 13 <= result <= 24
    elif bet_type == 'диапазон3':
        return 25 <= result <= 36
    return False

def calculate_payout(bet_type, bet_value, stake):
    if bet_type in ('к', 'ч', 'одд', 'евен'):
        return stake * 2
    elif bet_type == 'число':
        return stake * 36
    elif bet_type == 'числа':
        return stake * 36
    elif bet_type in ('диапазон1', 'диапазон2', 'диапазон3'):
        return stake * 3
    return 0

def is_valid_range(range_str):
    try:
        start, end = list(map(int, range_str.split('-')))
        if start > end:
            return False
        if (start == 1 and end == 12) or (start == 13 and end == 24) or (start == 25 and end == 36):
            return True
        else:
            return False
    except ValueError:
        return False

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/rul') or message.text.lower().startswith('рул')))
async def roulette_handler(message: types.Message):
    global roulette_active, current_bets, all_bets

    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверяем, активна ли рулетка в данном чате
    if roulette_active:
        await message.reply("⏳ Дождитесь окончания текущей игры в рулетку.")
        return

    try:
        parts = message.text.lower().split()
        if len(parts) < 3:
            await message.reply("❌ Неверный формат команды. Пример: рул 100 к")
            return

        stake_str = parts[1].lower()
        username = message.from_user.username or message.from_user.first_name

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

        if stake_str == 'все':
            stake = balance
        else:
            stake = format_stake(stake_str, balance)

        if stake is None or stake == -1:
            await message.reply("❌ Неверный формат ставки. Используйте число или сокращение (1к, 1кк).")
            return

        if stake <= 0:
            await message.reply("❌ Ставка должна быть больше 0")
            return

        if balance < stake:
            await message.reply("❌ Недостаточно средств на балансе")
            return

        bet_parts = parts[2:]

        if len(bet_parts) > 1 and all(part.isdigit() for part in bet_parts):
            # Проверка на валидность каждой ставки на число
            if not all(is_valid_number_bet(number) for number in bet_parts):
                await message.reply("❌ Недопустимое значение числа. Ставка должна быть от 0 до 36.")
                return

            if len(bet_parts) > 50:
                await message.reply("❌ Максимальное количество отдельных чисел: 50")
                return

            total_stake = stake * len(bet_parts)
            if balance < total_stake:
                await message.reply("❌ Недостаточно средств на балансе")
                return

            # Снимаем ставку с баланса пользователя
            new_balance = balance - total_stake
            cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
            conn.commit()
            formatted_balance = format_number(new_balance)

            if chat_id not in current_bets:
                current_bets[chat_id] = {}
            if user_id not in current_bets[chat_id]:
                current_bets[chat_id][user_id] = []

            for number in bet_parts:
                bet_type = 'число'
                bet_value = number
                current_bets[chat_id][user_id].append({'stake': stake, 'bet_type': bet_type, 'bet_value': bet_value, 'username': username})
                if chat_id not in all_bets:
                    all_bets[chat_id] = []

                all_bets[chat_id].append({'user_id': user_id, 'stake': stake, 'bet_type': bet_type, 'bet_value': bet_value, 'username': username})

            formatted_stake = format_number(stake)
            numbers_str = ', '.join(bet_parts)
            await message.reply(
                f"<b>🍒 Ставки приняты:</b>\n"
                f"<blockquote>💰 Ставка: {formatted_stake} PLcoins</blockquote>\n"
                f"<b>На числа:</b> {numbers_str} 🍒\n\n"
                f"<b>💰 Новый баланс:</b> {formatted_balance} PLcoins",
                parse_mode="HTML"
            )


        else:
            bet_string = " ".join(bet_parts)

            if bet_string in ('к', 'ч', 'одд', 'евен'):
                bet_type = bet_string
                bet_value = bet_string
            elif bet_parts[0].isdigit():
                bet_type = 'число'
                bet_value = bet_parts[0]
                 # Проверка на валидность ставки на число
                if not is_valid_number_bet(bet_value):
                    await message.reply("❌ Недопустимое значение числа. Ставка должна быть от 0 до 36.")
                    return

            elif "-" in bet_parts[0]:
                range_str = bet_parts[0]
                if is_valid_range(range_str):
                    start, end = list(map(int, range_str.split('-')))
                    if start == 1:
                        bet_type = "диапазон1"
                    elif start == 13:
                        bet_type = "диапазон2"
                    else:
                        bet_type = "диапазон3"
                    bet_value = range_str
                else:
                    await message.reply("❌ Неправильно введен диапазон. Допустимые диапазоны: 1-12, 13-24")
                    return
            else:
                await message.reply("❌ Неверный тип ставки.")
                return

            if bet_type:
                if balance < stake:
                    await message.reply("❌ Недостаточно средств на балансе")
                    return

                # Снимаем ставку с баланса пользователя
                new_balance = balance - stake
                cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
                conn.commit()
                formatted_balance = format_number(new_balance)

                if chat_id not in current_bets:
                    current_bets[chat_id] = {}
                if user_id not in current_bets[chat_id]:
                    current_bets[chat_id][user_id] = []

                current_bets[chat_id][user_id].append({'stake': stake, 'bet_type': bet_type, 'bet_value': bet_value, 'username': username})
                if chat_id not in all_bets:
                    all_bets[chat_id] = []
                all_bets[chat_id].append({'user_id': user_id, 'stake': stake, 'bet_type': bet_type, 'bet_value': bet_value, 'username': username})

                formatted_stake = format_number(stake)
                await message.reply(
                    f"<b>🍒 Ставки приняты:</b>\n"
                    f"<blockquote>💰 Ставка: {formatted_stake} PLcoins на: {bet_value} 🍒</blockquote>\n\n"
                    f"<b>💰 Новый баланс:</b> {formatted_balance} Plcoins",
                    parse_mode="HTML"
                )



    except ValueError:
        await message.reply("Неверный формат ставки. Используйте целые числа для ставки.")

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/go') or message.text.lower().startswith('го')))
async def go_handler(message: types.Message):
    global roulette_active, current_bets, all_bets

    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверяем, есть ли ставки в чате
    if chat_id not in all_bets or not all_bets[chat_id]:
        await message.reply("❌ Нет ставок. Сначала сделайте ставки.")
        return

    if roulette_active:
        # Проверяем, делал ли пользователь ставку в текущей игре
        if chat_id not in current_bets or user_id not in current_bets[chat_id] or not current_bets[chat_id][user_id]:
            await message.reply("❌ Сначала сделайте ставку.")
            return

        await message.reply("⏳ Ожидаем, пока все игроки сделают свои ставки...")
        return

    roulette_active = True

    try:
        msg = await message.reply("🍒 Крупье крутит рулетку....")
        await asyncio.sleep(3)

        result = roll_roulette()
        result_color = get_color(result)

        result_log.append((result_color, result))

        results = []
        total_win = 0
        total_loss = 0

        if chat_id in all_bets:
            bets_for_chat = all_bets[chat_id]

            for bet in bets_for_chat:
                user_id = bet['user_id']
                bet_type = bet['bet_type']
                bet_value = bet['bet_value']
                stake = bet['stake']
                username = bet['username']

                formatted_stake = format_number(stake)

                if check_win(bet_type, bet_value, result):
                    payout = calculate_payout(bet_type, bet_value, stake)
                    formatted_payout = format_number(payout)

                    # Обновляем баланс
                    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                    balance = cursor.fetchone()[0]
                    new_balance = balance + payout
                    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
                    conn.commit()

                    # --- Добавляем историю выигрыша ---
                    add_game_history(
                        user_id=user_id,
                        game="Рулетка",
                        bet=stake,
                        result="Выигрыш",
                        multiplier=payout / stake,
                        win=payout
                    )

                    # Формируем текст для результата
                    bet_display = bet_value
                    if bet_type in ('к', 'ч'):
                        bet_display = f"цвет {bet_value}"
                    elif bet_type in ('одд', 'евен'):
                        bet_display = f"число {bet_value}"
                    elif bet_type.startswith('диапазон'):
                        bet_display = f"диапазон {bet_value}"

                    results.append(f"<b>✅💸 {username},</b> ставка <b>{formatted_stake}</b> Plcoins на <b>{bet_display}</b>, выиграл: <b>{formatted_payout}</b> Plcoins")
                    total_win += payout
                else:
                    # --- Добавляем историю проигрыша ---
                    add_game_history(
                        user_id=user_id,
                        game="Рулетка",
                        bet=stake,
                        result="Проигрыш",
                        multiplier=0,
                        win=0
                    )

                    # Считаем проигрыш
                    total_loss += stake
                    cursor.execute("UPDATE users SET lost = COALESCE(lost, 0) + ? WHERE user_id = ?", (stake, user_id))
                    conn.commit()

                    bet_display = bet_value
                    if bet_type in ('к', 'ч'):
                        bet_display = f"цвет {bet_value}"
                    elif bet_type in ('одд', 'евен'):
                        bet_display = f"число {bet_value}"
                    elif bet_type.startswith('диапазон'):
                        bet_display = f"диапазон {bet_value}"

                    results.append(f"<b>❌💸 {username},</b> ставка <b>{formatted_stake}</b> Plcoins на <b>{bet_display}</b>, проиграл: <b>{formatted_stake}</b> Plcoins")

        formatted_total_win = format_number(total_win)
        formatted_total_loss = format_number(total_loss)

        result_text = f"<b>🍒 Рулетка:</b> {result_color} {result}\n\n"
        result_text += "\n".join(results) + "\n\n"
        result_text += f"<b>🏆 Общий выйгрыш:</b> {formatted_total_win} Plcoins\n"
        result_text += f"<b>❌ Общий проигрыш:</b> {formatted_total_loss} Plcoins"

        await bot.edit_message_text(chat_id=message.chat.id, message_id=msg.message_id, text=result_text, parse_mode="HTML")

    finally:
        roulette_active = False
        current_bets[chat_id] = {}
        all_bets[chat_id] = []

@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/log') or message.text.lower().startswith('лог')))
async def log_handler(message: types.Message):
    """Показывает последние 10 результатов."""
    user_id = message.from_user.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка cooldown
    if not await is_command_allowed(user_id):
        return

    if not result_log:
        await message.reply("❌ Лог пуст.")
        return

    log_text = "<b>🍒 Последние 10 результатов рулетки:\n</b>\n"
    for color, number in result_log:
        log_text += f"{color} {number}\n\n"

    await message.reply(log_text, parse_mode="HTML")
    # await update_last_command_time(user_id)  # Обновляем время последней команды


@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/cancel') or message.text.lower().startswith('отмена')))
async def cancel_handler(message: types.Message):
    global roulette_active, current_bets, all_bets

    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason} 🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    if roulette_active:
        await message.reply("❌ Нельзя отменить ставку, когда рулетка уже запущена.")
        return

    if chat_id not in current_bets or user_id not in current_bets[chat_id] or not current_bets[chat_id][user_id]:
        await message.reply("❌ У вас нет активных ставок для отмены.")
        return

    total_refund = sum(bet['stake'] for bet in current_bets[chat_id][user_id])

    # Возврат денег на баланс
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return
    balance = user_data[0]
    new_balance = balance + total_refund
    cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
    conn.commit()

    # Удаляем ставки пользователя из current_bets и all_bets
    def remove_user_bets(data_dict):
        if chat_id in data_dict and user_id in data_dict[chat_id]:
            del data_dict[chat_id][user_id]
            if not data_dict[chat_id]:
                del data_dict[chat_id]

    remove_user_bets(current_bets)

    # Для all_bets — это список, удаляем все ставки пользователя
    if chat_id in all_bets:
        all_bets[chat_id] = [bet for bet in all_bets[chat_id] if bet['user_id'] != user_id]
        if not all_bets[chat_id]:
            del all_bets[chat_id]

    await message.reply(f"✅ Ваши ставки отменены. {total_refund} возвращено на баланс.")



@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/rates') or message.text.lower().startswith('ставки')))
async def show_bets_handler(message: types.Message):
    """Показывает ставки текущего раунда."""
    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка на бан
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка cooldown
    if not await is_command_allowed(user_id):
        return

    if chat_id not in current_bets or user_id not in current_bets[chat_id] or not current_bets[chat_id][user_id]:
        await message.reply("У вас нет активных ставок.")
        return

    response = "<b>🍒 Ваши текущие ставки:\n</b>\n"
    for index, bet in enumerate(current_bets[chat_id][user_id]):
        bet_type = bet['bet_type']
        bet_value = bet['bet_value']
        stake = bet['stake']
        formatted_stake = format_number(stake)
        bet_display = bet_value if bet_type not in ('числа', 'диапазон1', 'диапазон2', 'диапазон3') else bet_type
        response += f"{index + 1}. <b>Ставка:</b> {formatted_stake} Plcoins <b>на</b> {bet_display}\n\n"

    await message.reply(response, parse_mode="HTML")

@dp.message_handler(lambda message: message.text and (
    message.text.lower().startswith('/hunt') or
    message.text.lower().startswith('/hunt@') or
    message.text.lower().startswith('охота')))
async def hunt_command(message: types.Message):
    user_id = message.from_user.id

    # Проверка на бан
    if await is_user_banned(user_id):
        cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
        ban_info = cursor.fetchone()

        if ban_info:
            ban_until, ban_reason = ban_info
            if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
                await message.reply(
                    f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                    parse_mode="HTML"
                )
                return

    # Проверка cooldown
    if not await is_command_allowed(user_id):
        await message.reply("⏳ Подожди немного перед следующей охотой!")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply("⚠️ Используйте: /hunt (ставка)", parse_mode="HTML")
            return

        stake_str = parts[1].strip().lower()

        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()

        if not user_data:
             await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.", parse_mode="HTML")
             return

        balance = user_data[0]

        stake = format_stake(stake_str, int(round(balance)))

        if stake == -1:
            await message.reply("❗ Некорректная сумма ставки.", parse_mode="HTML")
            return

        if stake < 100:
            await message.reply("❗ Минимальная ставка для охоты 100 PLcoins", parse_mode="HTML")
            return

        if stake <= 0:
            await message.reply("❗ Ставка должна быть больше нуля.", parse_mode="HTML")
            return

        if balance < stake:
            await message.reply("❗ Недостаточно средств.", parse_mode="HTML")
            return

    except (IndexError, ValueError) as e:
        await message.reply("⚠️ Используйте: /hunt (ставка)", parse_mode="HTML")
        return
    except Exception as e:
        await message.reply("❌ Произошла ошибка, попробуйте позже.")
        return

    # Снимаем ставку с баланса и обновляем количество игр
    try:
        new_balance = balance - stake
        cursor.execute("UPDATE users SET balance = ?, games_played = games_played + 1 WHERE user_id = ?", (new_balance, user_id))
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка обновления баланса: {e}")
        await message.reply("❌ Ошибка при обновлении баланса, попробуйте позже.")
        return

    hunt_message = await message.reply("🔫💥Вы сделали выстрел....")

    await asyncio.sleep(3)

    winning_animals = {
        "Олень": {"multiplier": 1.2, "win_text": "Попали в оленя! Отличный выстрел! 🦌 Вы выиграли +{win_amount} PLcoins"},
        "Кабан": {"multiplier": 1.4, "win_text": "Кабан повержен! Хороший улов! 🐗 Вы выиграли +{win_amount} PLcoins"},
        "Лось": {"multiplier": 2, "win_text": "Огромный лось! Победа! 🦌 Вы выиграли +{win_amount} PLcoins"},
        "Медведь": {"multiplier": 2, "win_text": "Медведь повержен! Большая удача! 🐻 Вы выиграли +{win_amount} PLcoins"},
        "Волк": {"multiplier": 1.7, "win_text": "Волк убит! Неплохо! 🐺 Вы выиграли +{win_amount} PLcoins"},
        "Лиса": {"multiplier": 1.6, "win_text": "Лисица поймана! Хороший трофей! 🦊 Вы выиграли +{win_amount} PLcoins"},
        "Рысь": {"multiplier": 2.3, "win_text": "Рысь поймана! Отличный трофей! 😼 Вы выиграли +{win_amount} PLcoins"},
        "Бобр": {"multiplier": 2, "win_text": "Бобер пойман! Неплохой улов! 🦫 Вы выиграли +{win_amount} PLcoins"},
        "Обезъяна": {"multiplier": 2, "win_text": "Обезъяна схвачена за хвост! Ловкие руки! 🐒 Вы выиграли +{win_amount} PLcoins"},
        "Росомаха": {"multiplier": 2, "win_text": "Росомаха повержена! Редкая добыча! 🦡 Вы выиграли +{win_amount} PLcoins"},
    }

    losing_animals = {
        "Ворона": "Промах! Ворона улетела. 🐦",
        "Заяц": "Мимо! Заяц удрал ноги пока ты целился. 🐇",
        "Орел": "Не попал! Орел слишком быстрый чтоб подстрелить его. 🦅",
        "Белка": "Не попали! Белка скрылась. 🐿️",
        "Еж": "Промах! Еж свернулся клубком. 🦔",
        "Мышь": "Мимо! Мышь скрылась в траве. 🐭",
        "Лягушка": "Не попали! Лягушка ускользла. 🐸",
        "Змея": "Неудача! Змея впилась в вашу ногу отравив вас. 🐍",
        "Утка": "Мимо! Утка улетела в пруд. 🦆",
        "Сова": "Не видно! Сова улетела в лес и исчезла меж деревьев. 🦉"
    }

    try:
        if random.random() < 0.39:  # выигрыш
            animal = random.choice(list(winning_animals.keys()))
            multiplier = winning_animals[animal]["multiplier"]
            win_amount = int(round(stake * multiplier))
            new_balance += win_amount
            cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
            conn.commit()

            # --- HISTORY (Выигрыш) ---
            add_game_history(
                user_id=user_id,
                game="Охота",
                bet=stake,
                result="Выигрыш",
                multiplier=multiplier,
                win=win_amount
            )

            formatted_win_amount = format_number(win_amount)
            win_text = winning_animals[animal]['win_text'].format(win_amount=formatted_win_amount)
            await bot.edit_message_text(win_text, chat_id=message.chat.id, message_id=hunt_message.message_id, parse_mode="HTML")

        else:
            # --- Проигрыш ---
            cursor.execute("UPDATE users SET lost = COALESCE(lost,0)+? WHERE user_id=?", (stake, user_id))
            conn.commit()

            add_game_history(
                user_id=user_id,
                game="Охота",
                bet=stake,
                result="Проигрыш",
                multiplier=0,
                win=0
            )

            animal = random.choice(list(losing_animals.keys()))
            await bot.edit_message_text(losing_animals[animal], chat_id=message.chat.id, message_id=hunt_message.message_id)

    except Exception as e:
        logging.error(f"Ошибка при обработке результата охоты: {e}")
        await message.reply("❌ Ошибка при обработке охоты, попробуйте позже.")

    finally:
        try:
            await update_last_command_time(user_id)
        except Exception as e:
            logging.error(f"Ошибка обновления времени последней команды: {e}")

#===========================================
#===========================================
# -------------------- Проверка кулдауна --------------------
COOLDOWN_SECONDS = 5

MIN_STAKE_FS = 100

async def is_command_allowed(user_id):
    cursor.execute("SELECT last_command_time FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()

    if not result or not result[0]:
        return True  # если нет времени в БД → можно

    try:
        last_command_time = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return True  # если кривая дата → пропускаем

    now = datetime.now()
    cooldown = timedelta(seconds=COOLDOWN_SECONDS)

    return now - last_command_time >= cooldown


async def update_last_command_time(user_id):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("UPDATE users SET last_command_time = ? WHERE user_id = ?", (now, user_id))
    conn.commit()


# -------------------- Фишки (Красное/Синее) --------------------
@dp.message_handler(lambda message: message.text and (message.text.lower().startswith('/chips') or message.text.lower().startswith('фишки')))
async def chips_command(message: types.Message):
    user_id = message.from_user.id

    try:
        # Проверка бана
        if await is_user_banned(user_id):
            cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
            ban_info = cursor.fetchone()
            if ban_info:
                ban_until, ban_reason = ban_info
                if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
                    await message.reply(f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫", parse_mode="HTML")
                    return

        # Проверка кулдауна
        if not await is_command_allowed(user_id):
            await message.reply(f"⏳ Подождите {COOLDOWN_SECONDS} секунд перед следующей ставкой!", parse_mode="HTML")
            return

        # Разбор команды
        parts = message.text.split()
        if len(parts) < 3:
            await message.reply("⚠️ Используйте: /chips (ставка) (красный/синий/к/с)", parse_mode="HTML")
            return

        stake_str = parts[1]
        color_choice = parts[2].lower()

        # Баланс игрока
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.", parse_mode="HTML")
            return

        user_balance = user_data[0]
        stake = format_stake(stake_str, user_balance)
        if stake == -1:
            await message.reply("❗ Некорректная сумма ставки.", parse_mode="HTML")
            return

        if stake < MIN_STAKE_FS:
            await message.reply(f"❗ Минимальная ставка: {format_number(MIN_STAKE_FS)} PLcoins", parse_mode="HTML")
            return

        if user_balance < stake:
            await message.reply("❗ Недостаточно средств.", parse_mode="HTML")
            return

        # Выбор игрока
        if color_choice in ['красный', 'к']:
            user_choice = "красный"
            user_choice_emoji = "🔴"
        elif color_choice in ['синий', 'с']:
            user_choice = "синий"
            user_choice_emoji = "🔵"
        else:
            await message.reply("❗ Укажите один из вариантов: красный, синий, к, с", parse_mode="HTML")
            return

        # Рандомный результат
        winning_color = random.choice(["красный", "синий"])
        winning_color_emoji = "🔴" if winning_color == "красный" else "🔵"

        # Списание ставки
        new_balance = user_balance - stake
        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        conn.commit()

        # Статистика
        cursor.execute('SELECT games_played, lost FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        if not user:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return

        games_played, lost = user

        # ===== Выиграл =====
        if user_choice == winning_color:
            win_amount = stake * 2
            new_balance += win_amount
            cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
            conn.commit()

            formatted_win_amount = format_number(win_amount)
            cursor.execute('UPDATE users SET games_played = ? WHERE user_id = ?', (games_played + 1, user_id))
            conn.commit()

            # ===== История =====
            add_game_history(
                user_id=user_id,
                game="Фишки",
                bet=stake,
                result="Выигрыш",
                multiplier=2,  # красное/синее всегда x2
                win=win_amount
            )

            await message.reply(
                f"Вы загадали <b>{user_choice.capitalize()} {user_choice_emoji}</b>, "
                f"а выпал <b>{winning_color.capitalize()} {winning_color_emoji}</b>!\n"
                f"✅ Победа! +<b>{formatted_win_amount}</b> PLcoins",
                parse_mode="HTML"
            )

        # ===== Проиграл =====
        else:
            formatted_stake = format_number(stake)
            new_lost = lost + stake
            cursor.execute(
                'UPDATE users SET games_played = ?, lost = ? WHERE user_id = ?',
                (games_played + 1, new_lost, user_id)
            )
            conn.commit()

            # ===== История =====
            add_game_history(
                user_id=user_id,
                game="Фишки",
                bet=stake,
                result="Проигрыш",
                multiplier=0,
                win=0
            )

            await message.reply(
                f"Вы загадали <b>{user_choice.capitalize()} {user_choice_emoji}</b>, "
                f"а выпал <b>{winning_color.capitalize()} {winning_color_emoji}</b>!\n"
                f"❌ Проигрыш: -<b>{formatted_stake}</b> PLcoins",
                parse_mode="HTML"
            )


        # Записываем время команды
        await update_last_command_time(user_id)

    except Exception as e:
        logging.error(f"Ошибка в команде 'фишки': {e}")
        await message.reply("❗ Произошла ошибка, повторите попытку позже", parse_mode="HTML")

        
# Функция для разбора суммы с поддержкой "все" и "всё"

def format_stake(stake_str: str, balance: int) -> int:
    """
    Преобразует строку с сокращениями в целое число.
    Примеры:
    '1к' -> 1000
    '1.5к' -> 1500
    '2кк' -> 2_000_000
    '3.2ккк' -> 3_200_000_000
    'все' -> balance
    """
    stake_str = stake_str.lower().replace(' ', '')

    if stake_str == 'все':
        return int(round(balance))

    multipliers = {
        'кккк': 10**12,
        'ккк': 10**9,
        'кк': 10**6,
        'к': 10**3,
    }

    # Проверяем, есть ли в конце суффикс (больше длина проверяется первой)
    for suffix, multiplier in sorted(list(multipliers.items()), key=lambda x: -len(x[0])):
        if stake_str.endswith(suffix):
            number_part = stake_str[:-len(suffix)]
            try:
                value = float(number_part)
                return int(round(value * multiplier))
            except ValueError:
                return -1  # ошибка парсинга
    # Если без суффикса
    try:
        value = float(stake_str)
        if value.is_integer():
            return int(value)
        else:
            return int(round(value))
    except ValueError:
        return -1


# ---------- банк -----------------------------

def parse_amount(amount_str: str, max_value: float = float('inf')) -> int:
    amount_str = amount_str.lower().replace(" ", "")

    if amount_str in ["все", "всё"]:
        return int(max_value)

    try:
        if amount_str.endswith("ккк"):
            return int(float(amount_str[:-3]) * 1_000_000_000)
        elif amount_str.endswith("кк"):
            return int(float(amount_str[:-2]) * 1_000_000)
        elif amount_str.endswith("к"):
            return int(float(amount_str[:-1]) * 1_000)
        else:
            return int(float(amount_str))
    except ValueError:
        return -1

def format_number(num: float) -> str:
    if num >= 1_000_000_000_000_000_000:  # Квинтиллион
        return f"{round(num / 1_000_000_000_000_000_000, 2)}kkkkkk"
    elif num >= 1_000_000_000_000_000:  # Квадриллион
        return f"{round(num / 1_000_000_000_000_000, 2)}kkkkk"
    elif num >= 1_000_000_000_000:  # Триллион
        return f"{round(num / 1_000_000_000_000, 2)}kkkk"
    elif num >= 1_000_000_000:  # Миллиард
        return f"{round(num / 1_000_000_000, 2)}kkk"
    elif num >= 1_000_000:  # Миллион
        return f"{round(num / 1_000_000, 2)}kk"
    elif num >= 1_000:  # Тысяча
        return f"{round(num / 1_000, 2)}k"
    else:
        return str(int(num))

async def apply_deposit_bonus(user_id):
    cursor.execute("SELECT amount, deposit_time FROM deposits WHERE user_id = ?", (user_id,))
    dep = cursor.fetchone()
    if not dep:
        return

    amount, deposit_time = dep
    last_time = datetime.strptime(deposit_time, "%Y-%m-%d %H:%M:%S")
    now = datetime.now()

    # Вычисляем сколько полных 7-дневных периодов прошло
    days_passed = (now - last_time).days
    periods = days_passed // 7

    if periods > 0:
        # Начисляем бонус 10% за каждый полный 7-дневный период
        for _ in range(periods):
            bonus = round(amount * 0.10, 2)
            amount += bonus

        # Обновляем время последнего начисления — сдвигаем на количество периодов * 7 дней
        new_deposit_time = last_time + timedelta(days=periods * 7)

        cursor.execute(
            "UPDATE deposits SET amount = ?, deposit_time = ? WHERE user_id = ?",
            (amount, new_deposit_time.strftime("%Y-%m-%d %H:%M:%S"), user_id)
        )
        conn.commit()

async def check_ban_and_rate_limit(message: types.Message):
    user_id = message.from_user.id

    # Проверка ограничения по частоте
    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return False

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return False
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    return True


@dp.message_handler(commands=["bank"])
async def command_bank_with_args_handler(message: types.Message):
    user_id = message.from_user.id
    text = message.get_args().lower().strip()  # Получаем аргументы после /bank

    # Проверка бана и rate limit
    if not await check_ban_and_rate_limit(message):
        return

    # Проверка регистрации и баланса
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return
    balance = user[0]

    # Начисление бонуса, если есть депозит
    await apply_deposit_bonus(user_id)

    cursor.execute('SELECT amount, deposit_time FROM deposits WHERE user_id = ?', (user_id,))
    dep = cursor.fetchone()
    deposit = dep[0] if dep else 0

    if not text:
        # Просто /bank — показываем инфо
        deposit_msg = f"\n\n💰 На депозите: {format_number(deposit)} PLcoins." if deposit > 0 else ""
        await message.reply(
            f"🏦 <b>Банк</b>\n\n"
            f"Положи свои PLcoins под депозит и через 7 дней получи +10% прибыли!{deposit_msg}\n\n"
            f"📥 <b>Как положить PLcoins?</b>\n"
            f"Пример: /bank положить 100 или /bank положить все\n\n"
            f"📤 <b>Как снять PLcoins?</b>\n"
            f"Пример: /bank снять 100 или /bank снять все",
            parse_mode="HTML"
        )
        return

    # Обработка положить
    if text.startswith("положить"):
        amount_str = text.replace("положить", "").strip()
        amount = parse_amount(amount_str, balance)

        if amount <= 0 or amount > balance:
            await message.reply("❌ Укажи корректную сумму для депозита.")
            return

        new_balance = balance - amount
        deposit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if dep:
            cursor.execute('UPDATE deposits SET amount = amount + ?, deposit_time = ? WHERE user_id = ?', (amount, deposit_time, user_id))
        else:
            cursor.execute('INSERT INTO deposits (user_id, amount, deposit_time) VALUES (?, ?, ?)', (user_id, amount, deposit_time))
        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        conn.commit()

        await message.reply(f"✅ Ты положил {format_number(amount)} PLcoins под депозит!", parse_mode="HTML")
        return

    # Обработка снять
    if text.startswith("снять"):
        if deposit <= 0:
            await message.reply("❌ У тебя нет активного депозита.")
            return

        amount_str = text.replace("снять", "").strip()
        amount = parse_amount(amount_str, deposit)

        if amount <= 0 or amount > deposit:
            await message.reply("❌ Укажи корректную сумму для снятия.")
            return

        deposit_time = datetime.strptime(dep[1], '%Y-%m-%d %H:%M:%S')
        now = datetime.now()

        bonus = 0
        if now >= deposit_time + timedelta(days=7):
            bonus = int(amount * 0.10)

        total = amount + bonus
        new_deposit = deposit - amount

        cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (total, user_id))
        if new_deposit == 0:
            cursor.execute('DELETE FROM deposits WHERE user_id = ?', (user_id,))
        else:
            cursor.execute('UPDATE deposits SET amount = ?, deposit_time = ? WHERE user_id = ?', (new_deposit, dep[1], user_id))
        conn.commit()

        await message.reply(
            f"💸 Ты снял {format_number(amount)} PLcoins с депозита.",
            parse_mode="HTML"
        )
        return

    await message.reply("❗ Неизвестная команда. Напиши просто /bank для инструкции.", parse_mode="HTML")


async def command_bank_handler(message: types.Message):
    user_id = message.from_user.id

    # Ограничение по частоте
    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    # Бан-проверка
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации и загрузка баланса
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    # Всё ок — отправляем информацию о банке
    await send_bank_info(message)

    # Обработчик для команды /bank
@dp.message_handler(commands=["bank"])
async def command_bank_handler(message: types.Message):
    user_id = message.from_user.id

    # Ограничение по частоте
    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации и баланса
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    # Если всё ок — отправляем инфо о банке
    await send_bank_info(message)

async def send_bank_info(message: types.Message):
    user_id = message.from_user.id

    # Ограничение по частоте
    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance = user[0]

    cursor.execute('SELECT amount FROM deposits WHERE user_id = ?', (user_id,))
    dep = cursor.fetchone()
    deposit = dep[0] if dep else 0

    deposit_msg = f"\n\n💰 На депозите: {format_number(deposit)} PLcoins." if deposit > 0 else ""
    await message.reply(
        f"🏦 <b>Банк</b>\n\n"
        f"Положи свои PLcoins под депозит и через 7 дней получи +10% прибыли!{deposit_msg}\n\n"
        f"📥 <b>Как положить PLcoins?</b>\n"
        f"Пример: Банк положить 100 или банк положить все\n\n"
        f"📤 <b>Как снять PLcoins?</b>\n"
        f"Пример: Банк снять 100 или банк снять все",
        parse_mode="HTML"
    )


@dp.message_handler(lambda message: message.text.lower().startswith("банк"))
async def bank_handler(message: types.Message):
    user_id = message.from_user.id
    text = message.text.lower().strip()

    if not await rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance = user[0]

    await apply_deposit_bonus(user_id)

    cursor.execute('SELECT amount, deposit_time FROM deposits WHERE user_id = ?', (user_id,))
    dep = cursor.fetchone()
    deposit = dep[0] if dep else 0

    # Логика по тексту "банк"
    if text == "банк":
        deposit_msg = f"\n\n💰 На депозите: {format_number(deposit)} PLcoins." if deposit > 0 else ""
        await message.reply(
            f"🏦 <b>Банк</b>\n\n"
            f"Положи свои PLcoins под депозит и через 7 дней получи +10% прибыли!{deposit_msg}\n\n"
            f"📥 <b>Как положить PLcoins?</b>\n"
            f"Пример: Банк положить 100 или банк положить все\n\n"
            f"📤 <b>Как снять PLcoins?</b>\n"
            f"Пример: Банк снять 100 или банк снять все",
            parse_mode="HTML"
        )
        return


    # --- Положить
    if "положить" in text:
        amount_str = text.replace("банк положить", "").strip()
        amount = parse_amount(amount_str, balance)

        if amount <= 0 or amount > balance:
            await message.reply("❌ Укажи корректную сумму для депозита.")
            return

        new_balance = balance - amount
        deposit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        term_days = 7  # значение по умолчанию для срока депозита

        if dep:
            cursor.execute(
                'UPDATE deposits SET amount = amount + ?, deposit_time = ?, term_days = ? WHERE user_id = ?',
                (amount, deposit_time, term_days, user_id)
            )
        else:
            cursor.execute(
                'INSERT INTO deposits (user_id, amount, deposit_time, term_days) VALUES (?, ?, ?, ?)',
                (user_id, amount, deposit_time, term_days)
            )

        cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (new_balance, user_id))
        conn.commit()

        await message.reply(f"✅ Ты положил {format_number(amount)} PLcoins под депозит!", parse_mode="HTML")
        return

    # --- Снять
    if "снять" in text:
        if deposit <= 0:
            await message.reply("❌ У тебя нет активного депозита.")
            return

        amount_str = text.replace("банк снять", "").strip()
        amount = parse_amount(amount_str, deposit)

        if amount <= 0 or amount > deposit:
            await message.reply("❌ Укажи корректную сумму для снятия.")
            return

        deposit_time = datetime.strptime(dep[1], '%Y-%m-%d %H:%M:%S')
        now = datetime.now()

        bonus = 0
        if now >= deposit_time + timedelta(days=7):
            bonus = int(amount * 0.10)

        total = amount + bonus
        new_deposit = deposit - amount

        cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (total, user_id))
        if new_deposit == 0:
            cursor.execute('DELETE FROM deposits WHERE user_id = ?', (user_id,))
        else:
            cursor.execute(
                'UPDATE deposits SET amount = ?, deposit_time = ?, term_days = ? WHERE user_id = ?',
                (new_deposit, dep[1], 7, user_id)
            )
        conn.commit()

        await message.reply(
            f"💸 Ты снял {format_number(amount)} PLcoins с депозита.",
            parse_mode="HTML"
        )
        return
        
# ------------ КУБИК -------------- #

def parse_bet_amount(bet_str: str, balance: int) -> int:
    bet_str = bet_str.lower().replace(",", ".").replace("’", "").replace("'", "")

    multipliers = {
        'к': 1_000,
        'кк': 1_000_000,
        'ккк': 1_000_000_000
    }

    try:
        if bet_str == "все":
            return balance

        if bet_str.isdigit():
            return int(bet_str)

        for suffix, multiplier in list(multipliers.items()):
            if bet_str.endswith(suffix):
                number_part = bet_str[:-len(suffix)]
                return int(float(number_part) * multiplier)

        return -1
    except Exception:
        return -1


DICE_COOLDOWNS = {}
DICE_COOLDOWN = 5  # секунд

async def dice_rate_limit(user_id: int) -> bool:
    now = time.time()
    last_time = DICE_COOLDOWNS.get(user_id, 0)

    if now - last_time < DICE_COOLDOWN:
        return False  # ещё рано

    DICE_COOLDOWNS[user_id] = now
    return True


@dp.message_handler(lambda msg: msg.text and (msg.text.lower().startswith('/dice') or msg.text.lower().startswith('кубик')))
async def dice_handler(message: types.Message):
    user_id = message.from_user.id

    # Ограничение только для кубика
    if not await dice_rate_limit(user_id):
        await message.reply("⚠️ Пожалуйста, подождите немного перед следующим действием.")
        return

    # --- дальше весь твой код: проверка бана, регистрация, ставка, результат ---


    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫", parse_mode="HTML")
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return

    balance, games_played, lost = user_data
    parts = message.text.split()

    if len(parts) != 3:
        await message.reply("❗ Используй: /dice <ставка> <чёт/нечет/больше/меньше/1-6>")
        return

    bet_str, choice = parts[1], parts[2].lower()

    valid_choices = ["чет", "чёт", "нечет", "нечёт", "больше", "меньше", "б", "м"] + [str(i) for i in range(1, 7)]
    if choice not in valid_choices:
        await message.reply("❗ Некорректный выбор. Используй: чёт, нечет, больше(б), меньше(м) или число от 1 до 6.")
        return

    bet = parse_bet_amount(bet_str, balance)
    if bet == -1:
        await message.reply("⚠️ Некорректная сумма ставки. Пример: 1к, 2.5кк, все")
        return

    if bet < 100:
        await message.reply("⚠️ Минимальная ставка — 100 PLcoins.")
        return
    if bet > balance:
        await message.reply("❌ Недостаточно средств.")
        return

    # Списываем ставку
    cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (bet, user_id))
    conn.commit()

    # Кидаем кубик и получаем результат
    dice_msg = await message.answer_dice("🎲")
    await asyncio.sleep(3.5)
    result = dice_msg.dice.value  # от 1 до 6

    win = 0

    if choice in ["чет", "чёт"] and result % 2 == 0:
        win = int(bet * 1.9)
    elif choice in ["нечет", "нечёт"] and result % 2 != 0:
        win = int(bet * 1.9)
    elif choice in ["больше", "б"] and result > 3:
        win = int(bet * 1.9)
    elif choice in ["меньше", "м"] and result < 4:
        win = int(bet * 1.9)
    elif choice.isdigit():
        if int(choice) == result:
            win = int(bet * 5)

    if win > 0:
        cursor.execute("""
            UPDATE users
            SET balance = balance + ?, games_played = games_played + 1
            WHERE user_id = ?
        """, (win, user_id))
        conn.commit()

        # --- HISTORY (Выигрыш) ---
        add_game_history(
            user_id=user_id,
            game="Кубик",
            bet=bet,
            result="Выигрыш",
            multiplier=round(win / bet, 2),
            win=win
        )

        await message.reply(
            f"🎲 Выпало: <b>{result}</b>\n"
            f"✅ <b>Поздравляем!</b> Вы выиграли <b>{round(win)} PLcoins</b>!\n"
            f"💰 Ставка: {round(bet)} | 💸 Профит: {round(win - bet)}",
            parse_mode="HTML"
        )

    else:
        # Обновляем проигрыш игрока и количество сыгранных игр
        cursor.execute("""
            UPDATE users
            SET lost = COALESCE(lost, 0) + ?, games_played = games_played + 1
            WHERE user_id = ?
        """, (bet, user_id))
        conn.commit()

        # --- HISTORY (Проигрыш) ---
        add_game_history(
            user_id=user_id,
            game="Кубик",
            bet=bet,
            result="Проигрыш",
            win=0
        )

        await message.reply(
            f"🎲 Выпало: <b>{result}</b>\n"
            f"❌ <b>Увы, вы проиграли</b> <b>{round(bet)} PLcoins</b>.\n"
            f"Попробуйте снова – удача может быть рядом 🍀",
            parse_mode="HTML"
        )


status_hourly_income = {
    "💫Galaxy💫": 52000,
    "💎Diamond": 37600,
    "🏆Golden Panda": 32000,
    "🐼ZLOI_PANDA": 30000,
    "🌪STORM": 30000,
    "🪬GOD HANDS🪬": 29333,
    "🌟Limited": 28000,
    "❄️snowflake": 27000,
    "🐈Cat": 26700,
    "🚫AFK": 26000,
    "🎭MYSTERY": 25000,
    "🪽ANGEL": 24000,
    "💫COSMIC": 22000,
    "🦈SHARK": 20000,
    "🍉Watermelon": 20000,
    "👑Billionaire": 18000,
    "🌙Moonlight" : 17000,
    "💰VIP": 16000,
    "💲Premium": 14000,
    "🧊ice": 12000,
    "👻GHOST": 10000,
    "🔥LEGEND": 8000,
    "💸casino": 6000,
    "🐉DRAGON": 4000,
    "🌀ZERO": 2000,
    "💣miner": 1000,
}

all_statuses = [
    "💫Galaxy💫",
    "💎Diamond",
    "🏆Golden Panda",
    "🐼ZLOI_PANDA",
    "🌪STORM",
    "🪬GOD HANDS🪬",
    "🌟Limited",
    "❄️snowflake",
    "🐈Cat", 
    "🚫AFK",
    "🎭MYSTERY",
    "🪽ANGEL",
    "💫COSMIC",
    "🦈SHARK",
    "🍉Watermelon",
    "👑Billionaire",
    "🌙Moonlight",
    "💰VIP",
    "💲Premium",
    "🧊ice",
    "👻GHOST",
    "🔥LEGEND",
    "💸casino",
    "🐉DRAGON",
    "🌀ZERO",
    "💣miner"
]

status_prices = {
    "💫Galaxy💫": (8000000, 15000000),
    "💎Diamond": (4700000, 6000000),
    "🏆Golden Panda": (4000000, 5300000),
    "🐼ZLOI_PANDA": (3500000, 5000000),
    "🌪STORM": (3400000, 4500000),
    "🪬GOD HANDS🪬": (3333333, 4444444),
    "🌟Limited": (2000000, 3000000),
    "❄️snowflake": (1800000, 3000000),
    "🚫AFK": (1500000, 3000000),
    "🐈Cat": (1500000, 2900000),
    "🎭MYSTERY": (1450000, 2800000),
    "🪽ANGEL": (1400000, 1600000),
    "💫COSMIC": (1200000, 1800000),
    "🦈SHARK": (1100000, 1400000),
    "🍉Watermelon": (1100000, 1800000),
    "👑Billionaire": (1100000, 2300000),
    "🌙Moonlight": (1000000, 2000000),
    "💰VIP": (1000000, 2000000),
    "💲Premium": (1000000, 2000000),
    "🧊ice": (950000, 1500000),
    "👻GHOST": (900000, 1300000),
    "🔥LEGEND": (900000, 1400000),
    "💸casino": (500000, 1500000),
    "🐉DRAGON": (500000, 700000),
    "🌀ZERO": (500000, 700000),
    "💣miner": (250000, 500000)
}

# Комиссия за передачу статуса (PLcoins)
status_transfer_fee = {
    "💫Galaxy💫": 120000,       
    "💎Diamond": 90000,
    "🏆Golden Panda": 75000,
    "🐼ZLOI_PANDA": 70000,
    "🌪STORM": 70000,
    "🪬GOD HANDS🪬": 68000,
    "🌟Limited": 60000,
    "❄️snowflake": 55000,
    "🐈Cat": 54000,
    "🚫AFK": 50000,
    "🎭MYSTERY": 48000,
    "🪽ANGEL": 46000,
    "💫COSMIC": 43000,
    "🦈SHARK": 40000,
    "🍉Watermelon": 40000,
    "👑Billionaire": 38000,
    "🌙Moonlight": 36000,
    "💰VIP": 34000,
    "💲Premium": 32000,
    "🧊ice": 30000,
    "👻GHOST": 28000,
    "🔥LEGEND": 25000,
    "💸casino": 22000,
    "🐉DRAGON": 20000,
    "🌀ZERO": 18000,
    "💣miner": 15000
}


user_statuses = {}

@dp.message_handler(commands=['count_statuses'])
async def count_statuses(message: types.Message):
    # Проверка, что только аfдмин может использовать команду
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ У вас нет прав для использования этой команды.")
        return

    # Получаем количество статусов из базы
    cursor.execute("SELECT status_name, COUNT(*) FROM user_statuses GROUP BY status_name")
    results = dict(cursor.fetchall())

    text = "📊 <b>Количество статусов на сервере:</b>\n\n"
    total_count = 0

    for status in all_statuses:  # Берём порядок из списка all_statuses
        count = results.get(status, 0)
        total_count += count
        text += f"{status} — <b>{count}</b>\n"

    text += f"\n🔢 <b>Всего статусов:</b> {total_count}"

    await message.reply(text, parse_mode="HTML")


def normalize_status_name(name: str) -> str:
    return name.strip()

async def give_hourly_income():
    while True:
        now = datetime.now()

        # Забираем всё сразу, без N+1 запросов
        cursor.execute("""
            SELECT user_id, last_income_time, ban_until, balance
            FROM users
        """)
        users = cursor.fetchall()

        for user_id, last_time, ban_until, balance in users:

            # --- Проверка бана ---
            if ban_until:
                try:
                    if datetime.fromisoformat(ban_until) > now:
                        continue
                except ValueError:
                    pass

            # --- Первый запуск или битые данные ---
            if not last_time:
                cursor.execute(
                    "UPDATE users SET last_income_time = ? WHERE user_id = ?",
                    (now.isoformat(), user_id)
                )
                continue

            try:
                last_income = datetime.fromisoformat(last_time)
            except ValueError:
                cursor.execute(
                    "UPDATE users SET last_income_time = ? WHERE user_id = ?",
                    (now.isoformat(), user_id)
                )
                continue

            # --- Сколько часов прошло ---
            hours_passed = int((now - last_income).total_seconds() // 3600)
            if hours_passed <= 0:
                continue

            # --- Получаем статусы пользователя ---
            cursor.execute(
                "SELECT status_name FROM user_statuses WHERE user_id = ?",
                (user_id,)
            )
            statuses = [row[0] for row in cursor.fetchall()]

            # --- Считаем доход ---
            hourly_income = sum(
                status_hourly_income.get(normalize_status_name(status), 0)
                for status in statuses
            )

            total_income = hourly_income * hours_passed
            if total_income <= 0:
                continue

            # --- Обновляем баланс ---
            new_balance = balance + total_income
            cursor.execute(
                "UPDATE users SET balance = ? WHERE user_id = ?",
                (new_balance, user_id)
            )

            # --- Обновляем время последнего начисления ---
            new_last_income = last_income + timedelta(hours=hours_passed)
            cursor.execute(
                "UPDATE users SET last_income_time = ? WHERE user_id = ?",
                (new_last_income.isoformat(), user_id)
            )

        conn.commit()

        # Проверяем раз в минуту — достаточно
        await asyncio.sleep(60)



@dp.message_handler(lambda message: message.text and message.text.lower().startswith('передать'))
async def give_status_reply_handler(message: types.Message):
    sender_id = message.from_user.id

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (sender_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫", parse_mode="HTML")
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (sender_id,))
            conn.commit()

    # Проверка регистрации и баланса
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (sender_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return
    sender_balance = user_data[0]

    # Проверка на ответ на сообщение
    if not message.reply_to_message:
        await message.reply("❗ Чтобы передать статус, ответьте на сообщение пользователя и укажите ID статуса после команды.")
        return

    recipient = message.reply_to_message.from_user
    recipient_id = recipient.id

    if sender_id == recipient_id:
        await message.reply("❗ Нельзя передавать статус самому себе.")
        return
    if recipient_id == (await bot.get_me()).id:
        await message.reply("❗ Нельзя передавать статус боту.")
        return

    # Получаем ID статуса
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❗ Укажите ID статуса для передачи. Пример: передать 1234")
        return
    try:
        status_id = int(args[1].strip())
    except ValueError:
        await message.reply("❗ Неверный ID статуса. Укажите число.")
        return

    # Получаем статус отправителя
    cursor.execute("SELECT status_name, status_id FROM user_statuses WHERE user_id=?", (sender_id,))
    sender_statuses = cursor.fetchall()
    status_to_transfer = next(((name, sid) for name, sid in sender_statuses if sid == status_id), None)
    if not status_to_transfer:
        await message.reply("❌ У вас нет статуса с таким ID.")
        return

    status_name = status_to_transfer[0]
    fee = status_transfer_fee.get(status_name, 0)

    # Формируем клавиатуру подтверждения
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_transfer:{status_id}:{recipient_id}:{fee}"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel_transfer")
    )

    await message.reply(
        f"💳 Чтобы передать статус *{status_name} ({status_id})*, вы должны оплатить комиссию в размере {fee:,} PLcoins.\n"
        "Нажмите кнопку для согласия или отмены передачи 👇",
        reply_markup=kb,
        parse_mode="Markdown"
    )

# ================== Callback для подтверждения / отмены передачи ==================
@dp.callback_query_handler(lambda c: c.data and c.data.startswith(("confirm_transfer:", "cancel_transfer")))
async def transfer_callback(callback_query: CallbackQuery):
    data = callback_query.data
    user_id = callback_query.from_user.id

    if data == "cancel_transfer":
        # Просто редактируем текст сообщения, показываем отмену
        await callback_query.message.edit_text("❌ Передача статуса отменена")
        await callback_query.answer()
        return

    # Данные подтверждения
    _, status_id_str, recipient_id_str, fee_str = data.split(":")
    status_id = int(status_id_str)
    recipient_id = int(recipient_id_str)
    fee = int(fee_str)

    # Проверяем баланс
    cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
    balance = cursor.fetchone()[0]
    if balance < fee:
        await callback_query.answer("❌ Недостаточно средств для оплаты комиссии", show_alert=True)
        return

    # Получаем статус
    cursor.execute("SELECT status_name FROM user_statuses WHERE user_id=? AND status_id=?", (user_id, status_id))
    result = cursor.fetchone()
    if not result:
        await callback_query.answer("❌ Статус не найден", show_alert=True)
        return
    status_name = result[0]

    # Списываем комиссию и передаем статус
    cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (fee, user_id))
    cursor.execute("DELETE FROM user_statuses WHERE user_id=? AND status_id=?", (user_id, status_id))
    cursor.execute("SELECT 1 FROM user_statuses WHERE user_id=? AND status_id=?", (recipient_id, status_id))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO user_statuses (user_id, status_name, status_id) VALUES (?, ?, ?)", 
                       (recipient_id, status_name, status_id))
    conn.commit()

    await callback_query.message.edit_text(
        f"✅ Вы передали статус *{status_name} ({status_id})* пользователю {recipient_id}.\n"
        f"💰 Комиссия за передачу: {fee:,} PLcoins",
        parse_mode="Markdown"
    )
    await callback_query.answer()


status_page_size = 5  # количество статусов на странице

# ======= Функции =======
def format_price(number):
    return f"{number:,}".replace(",", "'")

def status_page(page: int):
    """Возвращает список статусов для заданной страницы"""
    start = page * status_page_size
    end = start + status_page_size
    return all_statuses[start:end]

def status_list_keyboard(page: int, user_id: int):
    """Клавиатура страницы со статусами"""
    kb = InlineKeyboardMarkup(row_width=1)
    page_statuses = status_page(page)

    # Статусы на текущей странице
    for status in page_statuses:
        kb.add(InlineKeyboardButton(
            text=status,
            callback_data=f"status_select:{status}:{page}:{user_id}"  # уникально для выбора статуса
        ))

    # Навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ Назад",
            callback_data=f"status_page_list:{page-1}:{user_id}"  # уникально для листания
        ))
    if (page + 1) * status_page_size < len(all_statuses):
        nav.append(InlineKeyboardButton(
            "➡️ Далее",
            callback_data=f"status_page_list:{page+1}:{user_id}"
        ))
    if nav:
        kb.row(*nav)

    return kb

# ======== Обработчики ========

@dp.message_handler(lambda msg: msg.text and msg.text.lower() in ['статус лист', '/status_list'])
async def status_list_handler(message: types.Message):
    user_id = message.from_user.id

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason} 🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    if not cursor.fetchone():
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return

    text = (
        "👋 <b>Добро пожаловать в статус лист</b>\n\n"
        "<blockquote>Здесь собраны все статусы бота, их прибыль в час и примерная стоимость 💸</blockquote>\n\n"
        "Выбери статус, который тебя интересует 👇"
    )

    await message.reply(
        text,
        parse_mode="HTML",
        reply_markup=status_list_keyboard(page=0, user_id=user_id)
    )

# --- Листание страниц ---
@dp.callback_query_handler(lambda c: c.data.startswith("status_page_list:"))
async def status_page_handler(query: types.CallbackQuery):
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("❌ Некорректная кнопка", show_alert=True)
        return

    page = int(parts[1])
    owner_id = int(parts[2])

    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну, это не твои кнопки.", show_alert=True)
        return

    text = (
        "👋 <b>Добро пожаловать в статус лист</b>\n\n"
        "<blockquote>Здесь собраны все статусы бота, их прибыль в час и примерная стоимость 💸</blockquote>\n\n"
        "Выбери статус, который тебя интересует 👇"
    )
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=status_list_keyboard(page=page, user_id=owner_id)
    )
    await query.answer()

# --- Выбор конкретного статуса ---
@dp.callback_query_handler(lambda c: c.data.startswith("status_select:"))
async def status_select_handler(query: types.CallbackQuery):
    parts = query.data.split(":")
    if len(parts) != 4:
        await query.answer("❌ Некорректная кнопка", show_alert=True)
        return

    status = parts[1]
    page = int(parts[2])
    owner_id = int(parts[3])

    if query.from_user.id != owner_id:
        await query.answer("❗️Ну-ну, это не твои кнопки.", show_alert=True)
        return

    income = status_hourly_income.get(status, 0)
    price_range = status_prices.get(status)

    if price_range:
        min_price, max_price = price_range
        price_text = f"{format_price(min_price)} - {format_price(max_price)} PLcoins"
    else:
        price_text = "неизвестно"

    text = (
        f"✨ <b>Статус: {status}</b>\n"
        f"<code>--------------------------</code>\n"
        f"💰 <b>Прибыль в час:</b> {format_price(income)}\n"
        f"💸 <b>Примерная стоимость:</b> {price_text}"
    )

    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "⬅️ Назад к списку", callback_data=f"status_page_list:{page}:{owner_id}"
        )
    )

    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=kb
    )
    await query.answer()



def generate_random_id():
    return random.randint(1, 9999)

@dp.message_handler(commands=['give_status'])
async def give_status_handler(message: types.Message):
    sender_id = message.from_user.id

    # Проверка бана
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (sender_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (sender_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (sender_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return

    if sender_id not in ADMIN_IDS:
        await message.reply("❌ У вас нет прав для этой команды.")
        return

    args = message.get_args().split()
    if len(args) < 2:
        await message.reply("⚠️ Использование:\n/give_status <user_id> <название_статуса>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ Неверный ID пользователя.")
        return

    status_name = " ".join(args[1:]).strip()

    if status_name not in all_statuses:
        await message.reply("❌ Статус не найден.")
        return

    status_id = generate_random_id()

    cursor.execute(
        "INSERT INTO user_statuses (user_id, status_name, status_id) VALUES (?, ?, ?)",
        (target_user_id, status_name, status_id)
    )
    conn.commit()

    await message.reply(f"✅ Статус *{status_name} ({status_id})* выдан пользователю {target_user_id}.", parse_mode="Markdown")


@dp.message_handler(commands=['unf'])
async def remove_status_handler(message: types.Message):
    sender_id = message.from_user.id

    # Проверка бана (копируем из give_status для консистентности)
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (sender_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (sender_id,))
            conn.commit()
    # Проверка регистрации (копируем из give_status для консистентности)
    cursor.execute('SELECT balance, games_played, lost FROM users WHERE user_id = ?', (sender_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return

    if sender_id not in ADMIN_IDS:
        await message.reply("❌ У вас нет прав для этой команды.")
        return

    args = message.get_args().split()
    if len(args) != 3:
        await message.reply("⚠️ Использование:\n/unf <user_id> <название_статуса> <status_id>")
        return

    try:
        target_user_id = int(args[0])
        status_name = args[1]
        status_id = int(args[2])
    except ValueError:
        await message.reply("⚠️ Неверный ID пользователя или ID статуса.")
        return


    cursor.execute(
        "DELETE FROM user_statuses WHERE user_id = ? AND status_name = ? AND status_id = ?",
        (target_user_id, status_name, status_id)
    )
    conn.commit()

    if cursor.rowcount > 0:
        await message.reply(f"✅ Статус *{status_name} ({status_id})* удален у пользователя {target_user_id}.", parse_mode="Markdown")
    else:
        await message.reply(f"❌ Статус *{status_name} ({status_id})* не найден у пользователя {target_user_id}.", parse_mode="Markdown")


STATUSES_PER_PAGE = 15

# ================== Клавиатура пагинации ==================
def build_status_keyboard(page: int, total_pages: int, user_id: int, sort_type: str, sort_order: str):
    kb = InlineKeyboardMarkup(row_width=2)

    # Кнопки назад/вперед
    if page > 0:
        kb.insert(InlineKeyboardButton(
            "⬅️ Назад",
            callback_data=f"status_page:{sort_type}:{sort_order}:{page-1}:{user_id}"
        ))
    if page < total_pages - 1:
        kb.insert(InlineKeyboardButton(
            "➡️ Вперед",
            callback_data=f"status_page:{sort_type}:{sort_order}:{page+1}:{user_id}"
        ))

    # Кнопки сортировки
    kb.add(
        InlineKeyboardButton(
            "🔢 Номер ↑↓",
            callback_data=f"status_sort:id:{'asc' if sort_order=='desc' else 'desc'}:0:{user_id}"
        ),
        InlineKeyboardButton(
            "📈 Доход ↑↓",
            callback_data=f"status_sort:income:{'asc' if sort_order=='desc' else 'desc'}:0:{user_id}"
        ),
        InlineKeyboardButton(
            "💰 Редкость ↑↓",
            callback_data=f"status_sort:price:{'asc' if sort_order=='desc' else 'desc'}:0:{user_id}"
        )
    )

    return kb

# ================== Сортировка статусов ==================
def sort_statuses(rows, sort_type: str, sort_order: str):
    reverse = sort_order == "desc"
    if sort_type == "price":
        return sorted(rows, key=lambda x: status_prices.get(x["status_name"], (0,0))[1], reverse=reverse)
    elif sort_type == "id":
        return sorted(rows, key=lambda x: x["status_id"], reverse=reverse)
    elif sort_type == "income":
        return sorted(rows, key=lambda x: status_hourly_income.get(x["status_name"], 0), reverse=reverse)
    return rows

# ================== Показ статусов ==================
# ================== Показ статусов ==================
async def show_statuses(callback_query_or_message, user_id: int, sort_type="price", sort_order="desc", page=0):
    is_callback = isinstance(callback_query_or_message, CallbackQuery)

    # Получаем все статусы пользователя
    cursor.execute("SELECT status_name, status_id FROM user_statuses WHERE user_id=?", (user_id,))
    rows = cursor.fetchall()
    if not rows:
        text = "😐 У вас пока нет ни одного статуса."
        if is_callback:
            await callback_query_or_message.answer(text, show_alert=True)
        else:
            await callback_query_or_message.reply(text)
        return

    # Сортируем статусы
    rows = sort_statuses(rows, sort_type, sort_order)

    # Общая прибыль
    total_income = sum(status_hourly_income.get(name, 0) for name, _ in rows)
    formatted_income = f"{total_income:,}".replace(",", "'")

    # Разбиваем на страницы
    pages = [rows[i:i + STATUSES_PER_PAGE] for i in range(0, len(rows), STATUSES_PER_PAGE)]
    page = max(0, min(page, len(pages)-1))
    page_rows = pages[page]

    # Формируем текст статусов — каждый на новой строке
    text_lines = [f"{name} ({sid})" for name, sid in page_rows]
    text = "📜 Ваши статусы:\n" + "\n".join(text_lines)
    text += "\n——————————————\n"

    # Категория сортировки с понятным указанием, что сверху, что снизу
    if sort_order == "asc":
        arrow = "↑"
        top_word = "меньшее сверху"
        bottom_word = "большее снизу"
    else:
        arrow = "↓"
        top_word = "большее сверху"
        bottom_word = "меньшее снизу"

    if sort_type == "id":
        text += f"✅Категория: Номер 🔢 {arrow} {top_word}\n"
    elif sort_type == "income":
        text += f"✅Категория: Доход 📈 {arrow} {top_word}\n"
    elif sort_type == "price":
        text += f"✅Категория: Редкость 💰 {arrow} {top_word}\n"

    text += f"💰Прибыль в час: {formatted_income} PLcoins"

    # Кнопки пагинации
    kb = build_status_keyboard(page, len(pages), user_id, sort_type, sort_order)

    if is_callback:
        await callback_query_or_message.message.edit_text(text, reply_markup=kb)
        await callback_query_or_message.answer()
    else:
        await callback_query_or_message.reply(text, reply_markup=kb)


# ================== Хэндлер команды /status ==================
@dp.message_handler(lambda msg: msg.text and msg.text.lower() in ('статусы', '/status'))
async def statuses_handler(message: types.Message):
    sender_id = message.from_user.id
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (sender_id,))
    if not cursor.fetchone():
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start")
        return

    await show_statuses(message, sender_id)

# ================== Callback для пагинации ==================
@dp.callback_query_handler(lambda c: c.data and c.data.startswith(("status_page:", "status_sort:")))
async def status_callback(callback_query: CallbackQuery):
    parts = callback_query.data.split(":")
    if len(parts) != 5:
        await callback_query.answer("❌ Некорректная кнопка", show_alert=True)
        return

    action, sort_type, sort_order, page, user_id = parts
    try:
        page = int(page)
        user_id = int(user_id)  # преобразуем сюда
    except ValueError:
        await callback_query.answer("❌ Ошибка данных", show_alert=True)
        return

    # Теперь сравнение корректное
    if callback_query.from_user.id != user_id:
        await callback_query.answer("❌ Это не ваши статусы", show_alert=True)
        return

    await show_statuses(callback_query, user_id, sort_type, sort_order, page)

# Команда /рынок

ITEMS_PER_PAGE = 3  # сколько объявлений показывать на одной странице

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("market_buy_list_"))
async def market_buy_list_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data  # например: market_buy_list_123_0

    parts = data.split('_')

    if len(parts) == 5:
        # Формат: market_buy_list_{user_id}_{page}
        try:
            allowed_user_id = int(parts[3])
            page = int(parts[4])
        except ValueError:
            await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
            return
    elif len(parts) == 4:
        # Возможно формат market_buy_list_{user_id} (без страницы)
        try:
            allowed_user_id = int(parts[3])
            page = 0
        except ValueError:
            await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
            return
    else:
        await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
        return

    if user_id != allowed_user_id:
        await callback_query.answer("❗ Это не ваша кнопка.", show_alert=True)
        return

    offset = page * ITEMS_PER_PAGE
    cursor.execute(
        "SELECT market_id, seller_id, status_name, status_id, price FROM status_market ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (ITEMS_PER_PAGE, offset)
    )
    listings = cursor.fetchall()

    if not listings:
        if page == 0:
            await callback_query.message.edit_text("📭 На рынке пока нет объявлений.")
        else:
            await callback_query.answer("🚫 Нет объявлений на этой странице.", show_alert=True)
        return

    text = f"📋 <b>Статусы на продаже (страница {page + 1}):</b>\n\n"
    buttons = InlineKeyboardMarkup(row_width=1)

    for market_id, seller_id, status_name, status_id, price in listings:
        text += f"ID: {market_id} | {status_name} ({status_id})\nПродавец: {seller_id}\nЦена: {price:,} PLcoins\n\n"
        buttons.add(InlineKeyboardButton(
            text=f"Подробнее: {status_name} ({status_id})",
            callback_data=f"market_info_{market_id}_{user_id}"
        ))

    # Кнопки пагинации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("⬅️ Назад", callback_data=f"market_buy_list_{user_id}_{page - 1}")
        )

    # Проверим есть ли следующая страница
    cursor.execute("SELECT COUNT(*) FROM status_market")
    total_count = cursor.fetchone()[0]
    max_page = (total_count - 1) // ITEMS_PER_PAGE

    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton("Вперёд ➡️", callback_data=f"market_buy_list_{user_id}_{page + 1}")
        )

    if nav_buttons:
        buttons.row(*nav_buttons)

    await safe_edit(callback_query.message, text, buttons)
    await safe_answer_callback(callback_query)


@dp.message_handler(lambda m: m.text and m.text.lower() in ['рынок', '/рынок'])
async def market_intro_handler(message: types.Message):
    user_id = message.from_user.id

    # Бан-проверка
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации и загрузка баланса
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(text="Купить", callback_data=f"market_buy_list_{user_id}"),
        InlineKeyboardButton(text="Продать", callback_data=f"market_sell_info_{user_id}")
    )
    text = (
        "🛒 <b>Рынок статусов</b>\n\n"
        "Здесь вы можете <b>продавать</b> свои статусы или <b>покупать</b> статусы других игроков.\n\n"
        "Для продажи статуса используйте команду:\n"
        "<code>/sell &lt;ID_статуса&gt; &lt;цена&gt;</code>\n\n"
        "Нажмите кнопку 'Купить', чтобы посмотреть все доступные статусы на рынке."
    )
    await message.reply(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("market_") or c.data.startswith("buy_")))
async def process_market_callback(callback_query: types.CallbackQuery):
    try:
        data = callback_query.data
        parts = data.split("_")
        user_id = callback_query.from_user.id

        if len(parts) < 2:
            await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
            return

        action = parts[0]

        # ==== Покупка конкретного статуса ====
        if action == "buy":
            # Формат: buy_{market_id}_{user_id}
            if len(parts) != 3:
                await callback_query.answer("❌ Неверный формат кнопки.", show_alert=True)
                return

            market_id = int(parts[1])
            allowed_user_id = int(parts[2])

            if user_id != allowed_user_id:
                await callback_query.answer("❗ Это не ваша кнопка.", show_alert=True)
                return

            await handle_market_buy(callback_query, market_id)
            return

        # ==== Действия с рынком ====
        elif action == "market":
            if len(parts) < 3:
                await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
                return

            sub_action = parts[1]
            allowed_user_id = int(parts[-1])

            if user_id != allowed_user_id:
                await callback_query.answer("❗ Это не ваша кнопка.", show_alert=True)
                return

            # ==== Список статусов на покупку ====
            if sub_action == "buy" and parts[2] == "list":
                page = int(parts[3]) if len(parts) > 3 else 0
                offset = page * ITEMS_PER_PAGE

                cursor.execute(
                    "SELECT market_id, seller_id, status_name, status_id, price FROM status_market ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (ITEMS_PER_PAGE, offset)
                )
                listings = cursor.fetchall()

                if not listings:
                    if page == 0:
                        await callback_query.message.edit_text("📭 На рынке пока нет объявлений.")
                    else:
                        await callback_query.answer("🚫 Нет объявлений на этой странице.", show_alert=True)
                    return

                text = f"📋 <b>Статусы на продаже (страница {page + 1}):</b>\n\n"
                buttons = InlineKeyboardMarkup(row_width=1)

                for market_id, seller_id, status_name, status_id, price in listings:
                    cursor.execute("SELECT username FROM users WHERE user_id = ?", (seller_id,))
                    seller_info = cursor.fetchone()
                    seller_display = f"@{seller_info[0]}" if seller_info and seller_info[0] else f"ID: {seller_id}"

                    text += f"ID: {market_id} | {status_name} ({status_id})\nПродавец: {seller_display}\nЦена: {price:,} PLcoins\n\n"
                    buttons.add(
                        InlineKeyboardButton(
                            text=f"Подробнее: {status_name} ({status_id})",
                            callback_data=f"market_info_{market_id}_{user_id}"
                        )
                    )

                # Кнопки навигации
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"market_buy_list_{user_id}_{page - 1}"))

                cursor.execute("SELECT COUNT(*) FROM status_market")
                total_count = cursor.fetchone()[0]
                max_page = (total_count - 1) // ITEMS_PER_PAGE

                if page < max_page:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"market_buy_list_{user_id}_{page + 1}"))

                if nav_buttons:
                    buttons.row(*nav_buttons)

                await safe_edit(callback_query.message, text, buttons)
                await safe_answer_callback(callback_query)
                return

            # ==== Информация о продаже статуса ====
            elif sub_action == "sell" and parts[2] == "info":
                text = (
                    "📤 <b>Продажа статуса</b>\n\n"
                    "Чтобы выставить статус на продажу, используйте команду:\n"
                    "<code>/sell &lt;ID_статуса&gt; &lt;цена&gt;</code>\n\n"
                    "Пример:\n<code>/sell 2 5000</code> — выставит статус с ID 2 за 5000 PLcoins.\n\n"
                    "Чтобы снять статус с продажи:\n"
                    "<code>/unsell &lt;ID_объявления&gt;</code>\n"
                    "└ Внимание: указывайте не ID статуса, а ID объявления ❗️"
                )
                await callback_query.message.edit_text(text, parse_mode="HTML")
                await safe_answer_callback(callback_query)
                return

            # ==== Информация о конкретном статусе ====
            elif sub_action == "info":
                if len(parts) != 4:
                    await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
                    return

                market_id = int(parts[2])
                cursor.execute("SELECT seller_id, status_name, status_id, price FROM status_market WHERE market_id=?", (market_id,))
                listing = cursor.fetchone()
                if not listing:
                    await callback_query.answer("❌ Объявление не найдено.", show_alert=True)
                    return

                seller_id, status_name, status_id, price = listing
                cursor.execute("SELECT username FROM users WHERE user_id = ?", (seller_id,))
                seller_info = cursor.fetchone()
                seller_display = f"@{seller_info[0]}" if seller_info and seller_info[0] else f"ID: {seller_id}"

                text = (
                    f"ℹ️ <b>Информация о статусе:</b>\n\n"
                    f"Статус: <b>{status_name}</b>\n"
                    f"ID статуса: <code>{status_id}</code>\n"
                    f"Продавец: <b>{seller_display}</b>\n"
                    f"Цена: <b>{price:,} PLcoins</b>"
                )

                keyboard = InlineKeyboardMarkup(row_width=1)
                keyboard.add(InlineKeyboardButton(text=f"Купить за {price:,} PLcoins", callback_data=f"buy_{market_id}_{user_id}"))
                keyboard.add(InlineKeyboardButton(text="Назад к списку", callback_data=f"market_buy_list_{user_id}"))

                await callback_query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
                await safe_answer_callback(callback_query)
                return

            else:
                await callback_query.answer("❌ Неизвестное действие.", show_alert=True)
                return

        else:
            await callback_query.answer("❌ Неизвестное действие.", show_alert=True)

    except Exception as e:
        print(f"Error in process_market_callback: {e}")
        await callback_query.answer("❌ Произошла ошибка при обработке кнопки.", show_alert=True)



@dp.message_handler(lambda m: m.text and (m.text.lower().startswith('/sell') or m.text.lower().startswith('селл')))
async def sell_status_handler(message: types.Message):
    user_id = message.from_user.id
    text = message.text

    # Бан-проверка
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    # Получаем аргументы после команды
    if text.lower().startswith('/sell'):
        args = message.get_args().split()
    else:
        args = text.split()[1:]

    # Проверяем, что количество аргументов четное (пары id+цена)
    if len(args) < 2 or len(args) % 2 != 0:
        await message.reply(
            "❗ Использование:\n/sell <ID_статуса> <цена>\n"
            "Например:\n/sell 123 1000 "
        )
        return

    MAX_PRICE = 50_000_000  # Максимальная цена продажи

    results = []
    for i in range(0, len(args), 2):
        try:
            status_id = int(args[i])
            price = int(args[i+1])
        except ValueError:
            results.append(f"❗ Пара {args[i]} {args[i+1]} некорректна (нужно числа).")
            continue

        cursor.execute("SELECT status_name FROM user_statuses WHERE user_id=? AND status_id=?", (user_id, status_id))
        res = cursor.fetchone()
        if not res:
            results.append(f"❌ У вас нет статуса с ID {status_id}.")
            continue

        status_name = res[0]

        if price < 1000:
            results.append(f"❗ Минимальная цена для статуса {status_name} ({status_id}) — 1000 PLcoins.")
            continue
        
        if price > MAX_PRICE:
            results.append(f"❗ Максимальная цена для статуса {status_name} ({status_id}) — {MAX_PRICE:,} PLcoins.")
            continue

        cursor.execute("SELECT 1 FROM status_market WHERE seller_id=? AND status_id=?", (user_id, status_id))
        if cursor.fetchone():
            results.append(f"❗ Статус {status_name} ({status_id}) уже выставлен на продажу.")
            continue

        cursor.execute(
            "INSERT INTO status_market (seller_id, status_name, status_id, price, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, status_name, status_id, price, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
        conn.commit()

        cursor.execute("DELETE FROM user_statuses WHERE user_id=? AND status_id=?", (user_id, status_id))
        conn.commit()

        results.append(f"✅ Вы выставили статус *{status_name} ({status_id})* на продажу за {price:,} PLcoins.")

    await message.reply("\n".join(results), parse_mode="Markdown")

# Команда /unsell
@dp.message_handler(lambda message: 
    (message.text and message.text.lower().startswith('ансел')) or
    (message.text and message.text.lower().startswith('/unsell'))
)
async def unsell_status_handler(message: types.Message):
    user_id = message.from_user.id
    
    # Получаем аргументы после команды или текста
    # Если команда, то через get_args(), если текст — через split
    if message.text.lower().startswith('/unsell'):
        args = message.get_args().split()
    else:
        # Для текста 'ансел 123' — отделяем слово 'ансел' и получаем остальное
        args = message.text.split()[1:]
    
    # Бан-проверка
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Проверка регистрации
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if not user:
        await message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    if len(args) < 1:
        await message.reply("❗ Использование:\n/unsell <ID_объявления_на_рынке>\nили\nансел <ID_объявления_на_рынке>")
        return

    try:
        market_id = int(args[0])
    except ValueError:
        await message.reply("❗ Некорректный ID объявления. Используйте число.")
        return

    cursor.execute("SELECT status_name, status_id FROM status_market WHERE market_id=? AND seller_id=?", (market_id, user_id))
    res = cursor.fetchone()
    if not res:
        await message.reply("❌ Объявление с таким ID не найдено у вас на продаже.")
        return

    status_name, status_id = res

    cursor.execute("DELETE FROM status_market WHERE market_id=?", (market_id,))
    conn.commit()

    cursor.execute("INSERT INTO user_statuses (user_id, status_name, status_id) VALUES (?, ?, ?)", (user_id, status_name, status_id))
    conn.commit()

    await message.reply(
        f"✅ Вы сняли с продажи статус *{status_name} ({status_id})* и он возвращён к вам.",
        parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data.startswith("buy_market:"))
async def buy_market_handler(callback_query: types.CallbackQuery):
    parts = callback_query.data.split(":")
    if len(parts) != 3:
        await callback_query.answer("❌ Неверный формат кнопки.", show_alert=True)
        return

    _, market_id_str, allowed_user_id_str = parts

    try:
        market_id = int(market_id_str)
        allowed_user_id = int(allowed_user_id_str)
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных кнопки.", show_alert=True)
        return

    if callback_query.from_user.id != allowed_user_id:
        await callback_query.answer("❗ Это не ваша кнопка.", show_alert=True)
        return

    # Тут твоя логика покупки:
    # Проверяем объявление, баланс, переводим деньги, даём статус, удаляем объявление и т.п.

    cursor.execute("SELECT seller_id, status_name, status_id, price FROM status_market WHERE market_id=?", (market_id,))
    listing = cursor.fetchone()
    if not listing:
        await callback_query.answer("❌ Объявление не найдено или уже продано.", show_alert=True)
        return

    seller_id, status_name, status_id, price = listing

    buyer_id = callback_query.from_user.id
    if buyer_id == seller_id:
        await callback_query.answer("❗ Нельзя купить свой статус.", show_alert=True)
        return

    cursor.execute("SELECT balance FROM users WHERE user_id=?", (buyer_id,))
    buyer_data = cursor.fetchone()
    if not buyer_data:
        await callback_query.answer("❗ Вы не зарегистрированы.", show_alert=True)
        return
    buyer_balance = buyer_data[0]

    if buyer_balance < price:
        await callback_query.answer("❗ Недостаточно средств.", show_alert=True)
        return

    cursor.execute("SELECT balance FROM users WHERE user_id=?", (seller_id,))
    seller_data = cursor.fetchone()
    seller_balance = seller_data[0] if seller_data else 0

    try:
        conn.execute('BEGIN')
        cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (buyer_balance - price, buyer_id))
        cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (seller_balance + price, seller_id))
        cursor.execute("INSERT INTO user_statuses (user_id, status_name, status_id) VALUES (?, ?, ?)", (buyer_id, status_name, status_id))
        cursor.execute("DELETE FROM status_market WHERE market_id=?", (market_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        await callback_query.answer("❌ Ошибка при покупке. Попробуйте позже.", show_alert=True)
        print(("Ошибка покупки:", e))
        return

    await callback_query.message.edit_text(
        f"✅ Вы успешно купили статус *{status_name} ({status_id})* за {price:,} PLcoins.",
        parse_mode="Markdown"
    )
    await safe_answer_callback(callback_query)

import html

async def handle_market_buy(callback_query: types.CallbackQuery, market_id: int):
    buyer_id = callback_query.from_user.id
    await callback_query.answer("Обработка покупки...")

    # Бан-проверка
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (buyer_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await callback_query.message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (buyer_id,))
            conn.commit()

    # Проверка регистрации и загрузка баланса
    cursor.execute('SELECT balance, username FROM users WHERE user_id = ?', (buyer_id,))
    user = cursor.fetchone()
    if not user:
        await callback_query.message.reply("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return
    buyer_balance, buyer_nick = user

    # Получаем информацию о статусе на продаже
    cursor.execute("SELECT seller_id, status_name, status_id, price FROM status_market WHERE market_id=?", (market_id,))
    listing = cursor.fetchone()
    if not listing:
        await callback_query.answer("❌ Объявление не найдено или уже продано.", show_alert=True)
        return

    seller_id, status_name, status_id, price = listing

    if buyer_id == seller_id:
        await callback_query.answer("❗ Нельзя купить свой статус.", show_alert=True)
        return

    if buyer_balance < price:
        await callback_query.answer("❗ Недостаточно средств.", show_alert=True)
        return

    # Получаем баланс продавца
    cursor.execute("SELECT balance, username FROM users WHERE user_id=?", (seller_id,))
    seller_data = cursor.fetchone()
    seller_balance, seller_nick = seller_data if seller_data else (0, "Продавец")

    try:
        conn.execute('BEGIN')
        cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (buyer_balance - price, buyer_id))
        cursor.execute("UPDATE users SET balance=? WHERE user_id=?", (seller_balance + price, seller_id))
        cursor.execute("INSERT INTO user_statuses (user_id, status_name, status_id) VALUES (?, ?, ?)", (buyer_id, status_name, status_id))
        cursor.execute("DELETE FROM status_market WHERE market_id=?", (market_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        await callback_query.answer("❌ Ошибка при покупке. Попробуйте позже.", show_alert=True)
        print(("Ошибка покупки:", e))
        return

    # Экранируем спецсимволы для безопасной HTML-разметки
    safe_status_name = html.escape(status_name)
    safe_buyer_nick = html.escape(buyer_nick if buyer_nick else f"ID:{buyer_id}")

    await callback_query.message.edit_text(
        f"✅ Вы успешно купили статус <b>{safe_status_name} ({status_id})</b> за {price:,} PLcoins.",
        parse_mode="HTML"
    )

    # Отправляем уведомление продавцу, только если статус действительно его
    if seller_id != buyer_id:
        try:
            safe_seller_nick = html.escape(seller_nick if seller_nick else f"ID:{seller_id}")
            await bot.send_message(
                chat_id=seller_id,
                text=f"📦 Ваш статус <b>{safe_status_name} ({status_id})</b> был куплен игроком {safe_buyer_nick} за {price:,} PLcoins.",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Не удалось отправить сообщение продавцу {seller_id}: {e}")

    await safe_answer_callback(callback_query)

# ================== НАСТРОЙКИ ==================
HILO_MIN_BET = 100
HILO_MAX_ROUNDS = 10
SUITS = ["♠️", "♥️", "♣️", "♦️"]
active_hilo_games = {}  # ключ: game_id, значение: данные игры

# ================== ФУНКЦИИ ==================
import random, uuid
from datetime import datetime
from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

def draw_card():
    return random.randint(1, 13), random.choice(SUITS)

def card_text(num, suit):
    names = {1: "Туз", 11: "Валет", 12: "Дама", 13: "Король"}
    return f"{names.get(num, str(num))}{suit}"

# ======= Фиксированные множители для карт =======
def calculate_multiplier_fixed(card_num):
    multipliers = {
        1: (1.00, 2.00),   # Туз
        2: (1.00, 1.79),
        3: (1.00, 1.79),
        4: (1.18, 1.32),
        5: (1.26, 1.23),
        6: (1.31, 1.29),
        7: (1.32, 1.32),
        8: (1.32, 1.29),
        9: (1.34, 1.30),
        10: (1.32, 1.25),
        11: (1.79, 1.10),  # Валет
        12: (1.75, 1.12),  # Дама
        13: (1.50, 1.00)   # Король
    }
    return multipliers.get(card_num, (1.3, 1.3))

def fancy_text(text, style="bold"):
    if style == "bold":
        normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        bold = "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝑏𝑐𝑑𝑒𝑓𝑔𝑕𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧0123456789"
        return text.translate(str.maketrans(normal, bold))
    elif style == "italic":
        normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        italic = "𝐴𝐵𝐶𝐷𝐸𝐹𝐺𝐻𝐼𝐽𝐾𝐿𝑀𝑁𝑂𝑃𝑄𝑅𝑆𝑇𝑈𝑉𝑊𝑋𝑌𝑍𝑎𝑏𝑐𝑑𝑒𝑓𝑔ℎ𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧"
        return text.translate(str.maketrans(normal, italic))
    elif style == "double":
        normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        double = "ⒶⒷⒸⒹⒺⒻⒼⒽⒾⒿⓀⓁⓂⓃⓄⓅⓆⓇⓈⓉⓊⓋⓌⓍⓨⓩⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩ0①②③④⑤⑥⑦⑧⑨"
        return text.translate(str.maketrans(normal, double))
    return text

def format_hilo_text(game, first_card, bet):
    first_card_text = card_text(*first_card)
    x_higher, x_lower = calculate_multiplier_fixed(first_card[0])
    lower = first_card[0] - 1
    higher = 13 - first_card[0]
    lower_perc = round(lower / (lower + higher) * 100, 2)
    higher_perc = round(higher / (lower + higher) * 100, 2)
    
    text = (
        "━━━━━━━━━━━━━━━━━━\n"
        f"🎮 Игра HiLo — Раунд {game['round']}/{HILO_MAX_ROUNDS}\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🃏 Карта: {first_card_text}\n"
        f"💰 Ставка: {bet} PLcoins\n\n"
        f"⬆️ Больше → {higher_perc}% (x{x_higher})  \n"
        f"⬇️ Меньше → {lower_perc}% (x{x_lower})\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "⚡️ Выберите ваш ход!\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    return text

# ================== КОМАНДА /hilo ==================
@dp.message_handler(lambda message: message.text and message.text.lower().startswith(('/hilo', 'хило')))
async def cmd_hilo(message: types.Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ Укажи ставку, например: /hilo 100, /hilo 1к или /hilo всё")
        return

    user_id = message.from_user.id
    cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.", parse_mode="HTML")
        return

    balance = user_data[0]
    raw_bet = parts[1].lower()
    if raw_bet in ["всё", "все"]:
        bet = balance
        if bet < HILO_MIN_BET:
            await message.reply(f"❌ Минимальная ставка: {HILO_MIN_BET}")
            return
    else:
        try:
            multiplier_map = {"кккк": 1_000_000_000, "ккк": 1_000_000, "кк": 1_000_000, "к": 1_000}
            for key, mult in multiplier_map.items():
                if key in raw_bet:
                    bet = int(float(raw_bet.replace(key,"")) * mult)
                    break
            else:
                bet = int(float(raw_bet))
        except:
            await message.reply("❌ Некорректная ставка")
            return
        if bet < HILO_MIN_BET:
            await message.reply(f"❌ Минимальная ставка: {HILO_MIN_BET}")
            return
        if balance < bet:
            await message.reply("❌ Недостаточно средств.")
            return

    game_id = str(uuid.uuid4())
    active_hilo_games[game_id] = {"user_id": user_id, "bet": bet, "round": 0, "claimed": False}

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[ 
        InlineKeyboardButton("✅ Начать", callback_data=f"hilo_start|{bet}|{game_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"hilo_cancel|{game_id}")
    ]])

    await message.reply(
        fancy_text('🎮 Игра HiLo', 'bold') + f" на {bet} PLcoins\n{fancy_text('Ты готов сыграть?', 'italic')}",
        reply_markup=keyboard
    )

# ================== CALLBACK ==================
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("hilo_"))
async def callback_hilo_handler(query: CallbackQuery):
    data = query.data.split("|")
    action = data[0]
    extra = data[1] if len(data) > 2 else None
    game_id = data[-1]

    if game_id not in active_hilo_games:
        await query.answer("❌ Игра не найдена или уже завершена", show_alert=True)
        return

    game = active_hilo_games[game_id]
    user_id = query.from_user.id
    if game["user_id"] != user_id:
        await query.answer("Нуну, это не твои кнопки 😡", show_alert=True)
        return

    # ----------------- Отмена игры -----------------
    if action == "hilo_cancel":
        await query.message.edit_text(fancy_text('🚫 Игра отменена!', 'bold'), reply_markup=None)

        # История — отмена
        add_game_history(
            user_id=user_id,
            game="HiLo",
            bet=game["bet"],
            result="Отмена",
            multiplier=0,
            win=0
        )

        del active_hilo_games[game_id]
        await query.answer("Игра отменена ✅")
        return

    # ----------------- Старт игры -----------------
    if action == "hilo_start":
        if game.get("started", False):
            await query.answer("Игра уже началась!", show_alert=True)
            return

        bet = int(float(extra))
        cursor.execute('SELECT balance FROM users WHERE user_id=?', (user_id,))
        balance = cursor.fetchone()[0]
        if balance < bet:
            await query.answer("❌ Недостаточно средств.", show_alert=True)
            return

        cursor.execute('UPDATE users SET balance=balance-? WHERE user_id=?', (bet, user_id))
        conn.commit()

        first_card = draw_card()
        game.update({
            "initial_bet": bet,        # исходная ставка
            "multiplier": 1.0,         # накопленный коэффициент
            "bet": bet,                # текущая ставка
            "round": 1,
            "first_card": first_card,
            "max_rounds": HILO_MAX_ROUNDS,
            "started": True,
            "claimed": False
        })

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[ 
            InlineKeyboardButton("⬆️ Больше", callback_data=f"hilo_guess|higher|{game_id}"),
            InlineKeyboardButton("⬇️ Меньше", callback_data=f"hilo_guess|lower|{game_id}"),
            InlineKeyboardButton("💵 Забрать выигрыш", callback_data=f"hilo_take|current|{game_id}")
        ]])

        first_card_text = card_text(*first_card)
        x_higher, x_lower = calculate_multiplier_fixed(first_card[0])
        lower_perc = round((first_card[0]-1)/12*100, 2)
        higher_perc = round((13-first_card[0])/12*100, 2)

        text = (
            "━━━━━━━━━━━━━━━━━━\n"
            f"🎮 Игра HiLo — Раунд {game['round']}/{HILO_MAX_ROUNDS}\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🃏 Карта: {first_card_text}\n"
            f"💰 Ставка: {bet} PLcoins\n\n"
            f"⬆️ Больше → {higher_perc}% (x{x_higher})  \n"
            f"⬇️ Меньше → {lower_perc}% (x{x_lower})\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "⚡️ Выберите ваш ход!\n"
            "━━━━━━━━━━━━━━━━━━"
        )
        await query.message.edit_text(text, reply_markup=keyboard)
        await query.answer("Игра началась! 🎴")
        return

    # ----------------- Забрать выигрыш -----------------
    if action == "hilo_take":
        if game.get("claimed", False):
            await query.answer("❌ Вы уже забрали выигрыш!", show_alert=True)
            return

        winnings = game["bet"]
        multiplier = game.get("multiplier", 1.0)
        initial_bet = game.get("initial_bet", winnings)

        cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (winnings, user_id))
        conn.commit()
        game["claimed"] = True

        # История
        add_game_history(
            user_id=user_id,
            game="HiLo",
            bet=initial_bet,
            result="Выигрыш",
            multiplier=multiplier,
            win=winnings
        )

        text = (
            "━━━━━━━━━━━━\n"
            f"{fancy_text('💰 Выигрыш забран!', 'bold')}\n"
            "━━━━━━━━━━━━\n\n"
            f"💵 Сумма: {winnings} PLcoins\n\n"
            "🎉 Отлично! Вы можете сыграть\n"
            "снова и попытать удачу.\n"
            "━━━━━━━━━━━━"
        )
        await query.message.edit_text(text, reply_markup=None)
        del active_hilo_games[game_id]
        await query.answer()
        return

    # ----------------- Угадать карту -----------------
    if action == "hilo_guess":
        first_num, first_suit = game["first_card"]
        second_num, second_suit = draw_card()
        while second_num == first_num:
            second_num, second_suit = draw_card()

        guess = extra
        x_higher, x_lower = calculate_multiplier_fixed(first_num)
        multiplier_round = x_higher if guess == "higher" else x_lower
        won = (guess == "higher" and second_num > first_num) or (guess == "lower" and second_num < first_num)

        if won:
            game["round"] += 1
            game["first_card"] = (second_num, second_suit)
            game["multiplier"] *= multiplier_round
            game["bet"] = int(game["initial_bet"] * game["multiplier"])

            if game["round"] > HILO_MAX_ROUNDS:
                winnings = game["bet"]
                cursor.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (winnings, user_id))
                conn.commit()

                # История — победа
                add_game_history(
                    user_id=user_id,
                    game="HiLo",
                    bet=game["initial_bet"],
                    result="Выигрыш",
                    multiplier=game["multiplier"],
                    win=winnings
                )

                text = (
                    "━━━━━━━━━━━━━━━━━━\n"
                    f"{fancy_text('🏆 Победа!', 'bold')}\n"
                    "━━━━━━━━━━━━━━━━━━\n\n"
                    "Ты угадал все карты!\n\n"
                    f"{fancy_text('Выигрыш:', 'bold')} {winnings} PLcoins\n\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "Можно сыграть ещё раз! 🎴\n"
                    "━━━━━━━━━━━━━━━━━━"
                )
                await query.message.edit_text(text, reply_markup=None)
                del active_hilo_games[game_id]
            else:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[ 
                    InlineKeyboardButton("⬆️ Больше", callback_data=f"hilo_guess|higher|{game_id}"),
                    InlineKeyboardButton("⬇️ Меньше", callback_data=f"hilo_guess|lower|{game_id}"),
                    InlineKeyboardButton("💵 Забрать выигрыш", callback_data=f"hilo_take|current|{game_id}")
                ]])
                second_card_text = card_text(second_num, second_suit)
                lower_perc = round((second_num-1)/12*100, 2)
                higher_perc = round((13-second_num)/12*100, 2)
                x_higher, x_lower = calculate_multiplier_fixed(second_num)

                text = (
                    "━━━━━━━━━━━━━━━━━━\n"
                    f"🎮 Игра HiLo — Раунд {game['round']}/{HILO_MAX_ROUNDS}\n"
                    "━━━━━━━━━━━━━━━━━━\n\n"
                    f"🃏 Предыдущая карта: {card_text(first_num, first_suit)}\n"
                    f"🃏 Новая карта: {second_card_text}\n"
                    f"💰 Ставка: {game['bet']} PLcoins\n\n"
                    f"⬆️ Больше → {higher_perc}% (x{x_higher})  \n"
                    f"⬇️ Меньше → {lower_perc}% (x{x_lower})\n\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "⚡️ Выберите ваш ход!\n"
                    "━━━━━━━━━━━━━━━━━━"
                )
                await query.message.edit_text(text, reply_markup=keyboard)
        else:
            first_card_text = card_text(first_num, first_suit)
            second_card_text = card_text(second_num, second_suit)
            text = (
                "━━━━━━━━━━━━\n"
                f"{fancy_text('💀 Проигрыш!', 'bold')}\n"
                "━━━━━━━━━━━━\n\n"
                f"🃏 Предыдущая карта: {first_card_text}\n"
                f"🃏 Следующая карта: {second_card_text}\n\n"
                "━━━━━━━━━━━━\n"
                "Попробуйте ещё раз! 🎴"
            )

            # История — проигрыш
            add_game_history(
                user_id=user_id,
                game="HiLo",
                bet=game["initial_bet"],
                result="Проигрыш",
                multiplier=0,
                win=0
            )

            await query.message.edit_text(text, reply_markup=None)
            del active_hilo_games[game_id]

        await query.answer()


from aiogram.types import ParseMode

@dp.message_handler(commands=["all"])
async def broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.reply("🚫 У вас нет прав на использование этой команды.")
        return

    text = message.get_args()
    if not text:
        await message.reply("⚠️ Укажите текст для рассылки. Пример: /all Привет всем!")
        return

    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()

    count_sent = 0
    count_failed = 0

    for u in users:
        uid = u[0]
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            count_sent += 1
            await asyncio.sleep(0.05)  # пауза между сообщениями
        except Exception as e:
            # если чат не найден или пользователь удалён — пропускаем
            count_failed += 1
            continue

    await message.reply(f"✅ Сообщение отправлено: {count_sent}\n❌ Не удалось: {count_failed}")



from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.callback_data import CallbackData
from decimal import Decimal, InvalidOperation
import uuid, random, asyncio
from datetime import datetime

# ================= CallbackData =================
confirm_cb = CallbackData("confirm_rps", "action", "uuid")
rps_cb = CallbackData("rps", "choice", "uuid")

active_rps_users = set()   # защита кнопок / антиспам
active_rps_games = {}      # uuid -> инфо об игре


# ================= Функция парсинга ставок =================
def parse_bet(bet_str: str, balance: int) -> int:
    bet_str = bet_str.lower().replace(",", ".").strip()

    # Ставка "всё"
    if bet_str in ["все", "всё"]:
        return balance

    multipliers = {
        "к": 1_000,
        "k": 1_000,
        "кк": 1_000_000,
        "kk": 1_000_000,
        "ккк": 1_000_000_000,
        "kkk": 1_000_000_000,
    }

    for suffix, mult in multipliers.items():
        if bet_str.endswith(suffix):
            try:
                return int(Decimal(bet_str[:-len(suffix)]) * mult)
            except InvalidOperation:
                raise ValueError("Неверный формат ставки")

    try:
        return int(Decimal(bet_str))
    except InvalidOperation:
        raise ValueError("Неверный формат ставки")


# ================= Команда /кнб =================
@dp.message_handler(lambda message: message.text and (
    message.text.lower().startswith('кнб') or
    message.text.lower().startswith('/knb'))
)
async def start_rps(message: types.Message):
    user_id = message.from_user.id

    # Проверка регистрации и бана
    cursor.execute('SELECT balance, ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance, ban_until, ban_reason = user_data
    if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
        await message.answer(
            f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason} 🚫",
            parse_mode="HTML"
        )
        return

    # Разбор ставки
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ Укажите ставку: пример `/кнб 100` или `/кнб 1к`")
        return

    try:
        bet = parse_bet(parts[1], balance)
    except:
        await message.reply("❌ Неверный формат ставки. Пример: `100`, `1к`, `2.5к`, `1кк`")
        return

    # Ограничения по ставкам
    MIN_BET = 10
    if bet < MIN_BET:
        await message.reply(f"❌ Минимальная ставка {MIN_BET} PLcoins.")
        return

    # Проверка баланса
    if balance < bet:
        await message.reply("❌ Недостаточно PLcoins на балансе.")
        return

    # Генерация UUID игры
    game_uuid = str(uuid.uuid4())

    # Списание ставки
    cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (bet, user_id))
    conn.commit()

    # Сохраняем игру
    active_rps_games[game_uuid] = {
        "user_id": user_id,
        "bet": bet,
        "confirmed": False,
        "finished": False
    }

    # Подтверждение
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да", callback_data=confirm_cb.new(action="yes", uuid=game_uuid)),
        InlineKeyboardButton("❌ Нет", callback_data=confirm_cb.new(action="no", uuid=game_uuid)),
    )

    await message.reply(
        f"🎮 Вы уверены, что хотите сыграть в <b>Камень ✂️ Ножницы 📄 Бумага</b>\n"
        f"<blockquote>💰 Ставка: <b>{bet:,} PLcoins</b></blockquote>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


# ================= Обработка подтверждения =================
@dp.callback_query_handler(confirm_cb.filter())
async def process_confirm(call: CallbackQuery, callback_data: dict):
    user_id = call.from_user.id
    action = callback_data["action"]
    game_uuid = callback_data["uuid"]

    # Проверка что игра существует
    if game_uuid not in active_rps_games:
        await call.answer("⚠️ Эта игра уже недействительна!", show_alert=True)
        return

    game = active_rps_games[game_uuid]
    bet = game["bet"]

    # Проверка на чужие кнопки
    if game["user_id"] != user_id:
        await call.answer("⚠️ Это не твои кнопки!", show_alert=True)
        return

    if action == "no":
        # Возврат денег
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (bet, user_id))
        conn.commit()
        active_rps_games.pop(game_uuid, None)
        await call.message.edit_text("❌ Игра отменена.")
        return

    if user_id in active_rps_users:
        await call.answer("⚠️ У вас уже есть активная игра!", show_alert=True)
        return
    active_rps_users.add(user_id)

    # Обновляем статус игры
    game["confirmed"] = True

    # Кнопки выбора
    keyboard = InlineKeyboardMarkup(row_width=3)
    keyboard.add(
        InlineKeyboardButton("🪨 Камень", callback_data=rps_cb.new(choice="rock", uuid=game_uuid)),
        InlineKeyboardButton("✂️ Ножницы", callback_data=rps_cb.new(choice="scissors", uuid=game_uuid)),
        InlineKeyboardButton("📄 Бумага", callback_data=rps_cb.new(choice="paper", uuid=game_uuid)),
    )

    await call.message.edit_text(
        f"🎮 Камень ✂️ Ножницы 📄 Бумага\n"
        f"<blockquote>💰 Ставка: <b>{bet:,} PLcoins</b></blockquote>\n"
        f"👉 Сделайте ваш выбор:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    
# ================= Обработка хода =================
@dp.callback_query_handler(rps_cb.filter())
async def process_rps(call: CallbackQuery, callback_data: dict):
    user_id = call.from_user.id
    game_uuid = callback_data["uuid"]

    if game_uuid not in active_rps_games:
        await call.answer("⚠️ Эта игра уже завершена!", show_alert=True)
        return

    game = active_rps_games[game_uuid]

    # Проверка на чужие кнопки
    if game["user_id"] != user_id:
        await call.answer("⚠️ Это не твои кнопки!", show_alert=True)
        return

    if game.get("finished"):
        await call.answer("⚠️ Игра уже завершена!", show_alert=True)
        return
    game["finished"] = True  # блокировка повторных нажатий

    user_choice = callback_data["choice"]
    bet = game["bet"]
    bot_choice = random.choice(["rock", "scissors", "paper"])
    mapping = {"rock": "🪨 Камень", "scissors": "✂️ Ножницы", "paper": "📄 Бумага"}

    await call.message.edit_text("🤖 Бот выбирает...")
    await asyncio.sleep(2)

    # ----------------- Определяем результат -----------------
    if user_choice == bot_choice:
        result_title = "🤝 Ничья!"
        balance_change = bet  # возвращаем ставку
        result_text = f"<blockquote>💰 Ставка: {bet:,} PLcoins — возвращено</blockquote>".replace(",", " ")
        game_result = "Ничья"
        win_amount = bet
        multiplier = 1
    elif (
        (user_choice == "rock" and bot_choice == "scissors") or
        (user_choice == "scissors" and bot_choice == "paper") or
        (user_choice == "paper" and bot_choice == "rock")
    ):
        win_amount = int(bet * 1.5)
        result_title = "🏆 Победа!"
        balance_change = win_amount
        result_text = f"<blockquote>💰 Ставка: {bet:,} PLcoins → Выигрыш: {win_amount:,} PLcoins</blockquote>".replace(",", " ")
        game_result = "Выигрыш"
        multiplier = 1.5
    else:
        result_title = "💀 Поражение!"
        balance_change = 0
        result_text = f"<blockquote>💰 Ставка: {bet:,} PLcoins — проиграно</blockquote>".replace(",", " ")
        game_result = "Проигрыш"
        win_amount = 0
        multiplier = 0

    # ----------------- Обновляем баланс -----------------
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (balance_change, user_id))
    conn.commit()

    # ----------------- Запись истории игры -----------------
    add_game_history(
        user_id=user_id,
        game="КНБ",
        bet=bet,
        result=game_result,
        multiplier=multiplier,
        win=win_amount
    )

    # ----------------- Редактируем сообщение -----------------
    await call.message.edit_text(
        f"{result_title}\n"
        f"Ваш выбор: {mapping[user_choice]}\n"
        f"Выбор бота: {mapping[bot_choice]}\n\n"
        f"{result_text}",
        parse_mode="HTML"
    )

    # ----------------- Очистка -----------------
    active_rps_users.discard(user_id)
    active_rps_games.pop(game_uuid, None)


active_games = {}

@dp.message_handler(commands=["vilin"])
@dp.message_handler(lambda m: m.text and m.text.lower() == "вилин")
async def vilin_start(message: types.Message):
    user_id = message.from_user.id

    # ================== ПРОВЕРКА НА БАН ==================
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason} 🚫",
                parse_mode="HTML"
            )
            return
        else:
            # снимаем бан, если он уже истёк
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

    # ================== ПРОВЕРКА НА РЕГИСТРАЦИЮ ==================
    user = get_user(user_id)
    if not user:
        await message.answer("Вы не зарегистрированы. Нажмите /start для регистрации.")
        return

    balance = user["balance"]
    if balance <= 0:
        await message.answer("У тебя нет средств для игры 😢")
        return

    # Проверка на активную игру
    if active_games.get(user_id):
        await message.answer("⚠️ У тебя уже есть активная игра. Дождись её завершения!")
        return

    # Фиксируем игру
    active_games[user_id] = {"stake": balance}

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🎲 Старт", callback_data="vilin_start"),
        InlineKeyboardButton("❌ Отмена", callback_data="vilin_cancel")
    )

    await bot.send_message(
        chat_id=message.chat.id,
        text=(
            f"🎰 Игра Вилин!\n\n"
            f"<blockquote>Ставка: {balance}</blockquote>\n"
            f"Правила: шанс 50/50.\n\n"
            f"Выиграешь → баланс удвоится 💰\n"
            f"Проиграешь → баланс сгорит 🔥"
        ),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
        reply_markup=kb
    )

# ================== ОБРАБОТКА КНОПОК ==================
@dp.callback_query_handler(lambda c: c.data in ["vilin_start", "vilin_cancel"])
async def vilin_play(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    # Проверка: это кнопка того, кто запустил игру
    if user_id not in active_games:
        await callback.answer("⚠️ Это не твоя игра!", show_alert=True)
        return

    user = get_user(user_id)
    if not user:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        active_games.pop(user_id, None)
        return

    stake = active_games[user_id]["stake"]
    balance = user["balance"]

    # ----------------- Отмена игры -----------------
    if callback.data == "vilin_cancel":
        active_games.pop(user_id, None)
        await callback.message.edit_text(
            f"❌ Игра отменена.\n\n<blockquote>Ставка {stake} возвращена</blockquote>",
            parse_mode="HTML"
        )

        # Добавляем историю игры — отмена
        add_game_history(
            user_id=user_id,
            game="Вилин",
            bet=stake,
            result="Отмена",
            multiplier=0,
            win=0
        )

        await callback.answer()
        return

    # ----------------- Старт игры -----------------
    if callback.data == "vilin_start":
        if balance < stake:
            await callback.answer("Недостаточно средств!", show_alert=True)
            active_games.pop(user_id, None)
            return

        # Списываем ставку
        update_balance(user_id, -stake)
        balance -= stake

        # 50/50 шанс выигрыша
        result = random.choice(["win", "lose"])

        if result == "win":
            win_amount = stake * 2
            update_balance(user_id, win_amount)
            new_balance = balance + win_amount
            text = (
                f"🎉 Победа!\n\n"
                f"<blockquote>💸 Выигрыш: {win_amount}</blockquote>\n"
                f"Баланс: {new_balance} 💰"
            )

            # Добавляем историю игры — выигрыш
            add_game_history(
                user_id=user_id,
                game="Вилин",
                bet=stake,
                result="Выигрыш",
                multiplier=2,
                win=win_amount
            )

        else:
            new_balance = balance
            text = (
                f"💀 Проигрыш!\n\n"
                f"<blockquote>💸 Ставка {stake} сгорела</blockquote>\n"
                f"Баланс: {new_balance}"
            )

            # Добавляем историю игры — проигрыш
            add_game_history(
                user_id=user_id,
                game="Вилин",
                bet=stake,
                result="Проигрыш",
                multiplier=0,
                win=0
            )

        # Завершаем игру
        active_games.pop(user_id, None)
        await callback.message.edit_text(
            text,
            parse_mode="HTML"
        )
        await callback.answer()
    
HISTORY_PER_PAGE = 5

def history_keyboard(user_id, page, total_pages):
    kb = InlineKeyboardMarkup(row_width=2)
    if page > 1:
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"history:{user_id}:{page-1}"))
    if page < total_pages:
        kb.add(InlineKeyboardButton("➡️ Далее", callback_data=f"history:{user_id}:{page+1}"))
    return kb

# --- Форматирование истории ---
def format_history(rows, page, total_pages):
    SEPARATOR = "\n- - - - - - - - - - - - - - - - - - - - - - -\n"
    text = f"📜 <b>История игр</b> ({page}/{total_pages})\n\n"

    blocks = []
    for r in rows:
        bet_short = format_short(r['bet'])
        win_short = format_short(r['win'])

        block = (
            f"🎮 <b>Игра</b> | {r['game']}\n"
            f"💸 <b>Ставка</b> | {bet_short} PLcoins\n"
            f"📊 <b>Результат</b> | {r['result']}"
        )

        if r['win'] > 0:
            block += f" | x{r['multiplier']} / {win_short} PLcoins"

        blocks.append(block)

    text += SEPARATOR.join(blocks)
    return text


# --- Универсальная функция получения истории ---
def get_history(user_id, page=1):
    offset = (page - 1) * HISTORY_PER_PAGE
    with get_conn() as conn:
        cursor = conn.cursor()

        # Общее количество игр
        cursor.execute("SELECT COUNT(*) FROM game_history WHERE user_id = ?", (user_id,))
        total = cursor.fetchone()[0]
        if total == 0:
            return None, 0

        total_pages = (total + HISTORY_PER_PAGE - 1) // HISTORY_PER_PAGE

        # Данные текущей страницы
        cursor.execute(
            "SELECT * FROM game_history WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (user_id, HISTORY_PER_PAGE, offset)
        )
        rows = cursor.fetchall()

    return format_history(rows, page, total_pages), total_pages

# --- Обработчик команды /history и текста "история" ---
@dp.message_handler(lambda message: message.text and message.text.lower() in ["/history", "история"])
async def history_command(message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

    text, total_pages = get_history(user_id, page=1)
    if text is None:
        await message.reply("📭 История игр пуста.")
        return

    await message.reply(
        text,
        parse_mode="HTML",
        reply_markup=history_keyboard(user_id, 1, total_pages)
    )


# --- Обработчик кнопок пагинации ---
@dp.callback_query_handler(lambda c: c.data.startswith("history:"))
async def history_callback(call: CallbackQuery):
    # callback_data теперь вида "history:user_id:page"
    _, owner_id, page = call.data.split(":")
    owner_id = int(owner_id)
    page = int(page)

    if call.from_user.id != owner_id:
        await call.answer("❌ Это не ваши кнопки!", show_alert=True)
        return

    text, total_pages = get_history(owner_id, page)
    if text is None:
        await call.answer("📭 История игр пуста.", show_alert=True)
        return

    await call.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=history_keyboard(owner_id, page, total_pages)
    )
    await call.answer()

    
# ================== ПЛИНКО ==================
def parse_bet(text: str) -> int:
    """
    Конвертирует текст ставки в число, поддерживает сокращения:
    1к = 1000, 1кк = 1_000_000, 1ккк = 1_000_000_000, 1кккк = 1_000_000_000_000
    """
    if not text:
        return None
    text = text.lower().replace(" ", "")
    multipliers = {"кккк": 1_000_000_000_000, "ккк": 1_000_000_000, "кк": 1_000_000, "к": 1_000}

    for key, value in multipliers.items():
        if text.endswith(key):
            try:
                return int(float(text.replace(key, "")) * value)
            except:
                return None
    try:
        return int(text)
    except:
        return None

def format_short(number: int) -> str:
    """
    Форматирует число с сокращениями:
    1_000 -> 1.0k
    1_500 -> 1.5k
    1_000_000 -> 1.0kk
    1_500_000 -> 1.5kk
    1_000_000_000 -> 1.0kkk
    """
    abs_number = abs(number)
    if abs_number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}kkk"
    elif abs_number >= 1_000_000:
        return f"{number / 1_000_000:.1f}kk"
    elif abs_number >= 1_000:
        return f"{number / 1_000:.1f}k"
    else:
        return str(number)

user_cooldowns = {}

COOLDOWN_SECONDS = 5  # Задержка между играми в секундах

async def check_cooldown_plinko(user_id: int) -> bool:
    """
    Проверяет, может ли пользователь сыграть.
    Возвращает True, если можно играть, False если еще в КД.
    """
    now = datetime.now()
    last_time = user_cooldowns.get(user_id)
    if last_time and (now - last_time).total_seconds() < COOLDOWN_SECONDS:
        return False
    user_cooldowns[user_id] = now
    return True

@dp.message_handler(commands=['plinko'])
async def plinko_command(message: types.Message):
    args = message.get_args()
    bet = parse_bet(args)
    if not bet or bet <= 0:
        await message.reply("⚠️ Используйте: /plinko <ставка> (например: 100, 1к, 1кк)")
        return
    await run_plinko(message, bet)


@dp.message_handler(lambda message: message.text and message.text.lower().startswith(("плинко", "/plinko")))
async def plinko_text(message: types.Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("⚠️ Используйте: плинко <ставка> (например: 100, 1к, 1кк)")
        return
    bet = parse_bet(parts[1])
    if not bet or bet <= 0:
        await message.reply("⚠️ Некорректная ставка!")
        return
    await run_plinko(message, bet)

async def run_plinko(message, bet: int):
    user_id = message.from_user.id

    # ===== Проверка кулдауна =====
    if not await check_cooldown_plinko(user_id):
        await message.reply(f"⏱ Пожалуйста, подождите {COOLDOWN_SECONDS} секунд перед следующей игрой.")
        return


    conn = get_conn()
    cursor = conn.cursor()

    # ===== Проверка регистрации =====
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        await message.reply(
            "❌ Вы не зарегистрированы. Нажмите /start для регистрации.",
            parse_mode="HTML"
        )
        return

    # ===== Проверка на бан =====
    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()
    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

    # ===== Проверка баланса =====
    balance = user_data['balance']
    games_played = user_data['games_played']
    lost = user_data['lost']

    if bet > balance:
        await message.reply(f"⚠️ Недостаточно средств для ставки {format_short(bet)} PLcoins.")
        return

    # ===== Списываем ставку =====
    cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (bet, user_id))
    conn.commit()

    # ===== Кидаем кубик 🎲 =====
    dice_msg = await message.answer_dice("🎲")
    await asyncio.sleep(3.5)
    result = dice_msg.dice.value

    # ===== Таблица коэффициентов =====
    multiplier_table = {1: 0, 2: 0.3, 3: 0.9, 4: 1.1, 5: 1.4, 6: 1.8}
    multiplier = multiplier_table[result]
    win = int(bet * multiplier)

    # ===== Начисляем выигрыш =====
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (win, user_id))

    # ===== Вычисляем проигрыш =====
    lost_amount = bet - win if bet - win > 0 else 0

    # ===== Обновляем статистику =====
    cursor.execute(
        "UPDATE users SET games_played = games_played + 1, lost = lost + ? WHERE user_id = ?",
        (lost_amount, user_id)
    )
    conn.commit()

    # ===== Логируем игру =====
    cursor.execute(
        "INSERT INTO game_history (user_id, game, bet, result, multiplier, win, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, "Plinko", bet, result, multiplier, win, datetime.now())
    )
    conn.commit()

    # ===== Эмодзи =====
    emoji_result = "💥" if multiplier < 1 else "🎉"

    # ===== Сообщение пользователю =====
    await message.reply(
        f"{emoji_result} Plinko 🎲\n"
        f"- - - - - - - - - - - -\n"
        f"✅Выпало: {result}\n"
        f"💸Ставка: {format_short(bet)} PLcoins\n"
        f"💰Выигрыш: {format_short(bet)} | х{multiplier} | {format_short(win)} PLcoins"
    )

DONAT_TEXT = """
💵Курс PLcoins💵
—————Рубли—————
1 руб = 40.000 PLcoins
————Доллар————— (через @send)
1 доллар = 3.300.000 PLcoins
————Звёзды—————
1 звезда = 80.000 PLcoins
—————————————
Покупка от 100 руб / 1 доллар / 15 звёзд
—————————————
Статусы:
💫Galaxy💫 - 100 звёзд
💎Diamond - 75 звёзд
🏆Golden Panda - 65 звёзд
🐼ZLOI_PANDA - 60 звёзд
🌪STORM - 60 звёзд
🪬GOD HANDS🪬 - 60 звёзд
🌟Limited - 55 звёзд
❄️snowflake - 55 звёзд
🐈Cat - 50 звёзд
🚫AFK - 50 звёзд
🎭MYSTERY - 45 звёзд

За покупкой —> @ZLOI_PANDIK ‼️
"""

cooldowns = {}  # user_id: last_time
COOLDOWN_TIME = 5  # секунд


def on_cooldown(user_id: int):
    now = time.time()
    last = cooldowns.get(user_id, 0)
    if now - last < COOLDOWN_TIME:
        return True, int(COOLDOWN_TIME - (now - last))
    cooldowns[user_id] = now
    return False, 0


@dp.message_handler(commands=["donat_list"])
async def cmd_donat_list(message: types.Message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

    cd, left = on_cooldown(message.from_user.id)
    if cd:
        await message.reply(f"Подожди {left} сек.")
        return

    await message.answer(DONAT_TEXT)


@dp.message_handler(Text(equals=["донат лист"], ignore_case=True))
async def text_donat_list(message: types.Message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

    cd, left = on_cooldown(message.from_user.id)
    if cd:
        await message.reply(f"Подожди {left} сек.")
        return

    await message.answer(DONAT_TEXT)

import asyncio

# ================== Дуэль ==================

def parse_bet(bet_str: str) -> int:
    """
    Преобразует строку ставки с сокращениями в число PLcoins.
    Примеры:
        1k -> 1000
        1kk -> 1_000_000
        2.5kkk -> 2_500_000_000
    """
    bet_str = bet_str.lower().replace(" ", "")
    match = re.fullmatch(r"(\d+(\.\d+)?)(к{0,3})", bet_str)
    if not match:
        return 0
    number = float(match.group(1))
    suffix = match.group(3)
    if suffix == "к":
        number *= 1_000
    elif suffix == "кк":
        number *= 1_000_000
    elif suffix == "ккк":
        number *= 1_000_000_000
    return int(number)

def format_plcoins(amount: int) -> str:
    """Форматирует число в краткую запись для PLcoins"""
    if amount >= 1_000_000_000:
        return f"{amount/1_000_000_000:.1f}kkk"
    elif amount >= 1_000_000:
        return f"{amount/1_000_000:.1f}kk"
    elif amount >= 1_000:
        return f"{amount/1_000:.1f}k"
    else:
        return str(amount)

# Хранение текущих дуэлей в памяти: duel_id -> данные
active_duels = {}  # {duel_id: {"creator": user_id, "bet": amount, "message_id": msg_id, "chat_id": chat_id}}

def create_duel_keyboard(creator_id: int, duel_id: str):
    """Создаем inline-клавиатуру для дуэли"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✅Принять дуэль", callback_data=f"accept:{duel_id}"),
        InlineKeyboardButton("❌Отменить дуэль", callback_data=f"cancel:{duel_id}:{creator_id}")
    )
    return keyboard

# Объединяем команды /duel и просто "дуэль <ставка>"
@dp.message_handler(lambda m: m.text.lower().startswith("дуэль") or m.text.lower().startswith("/duel"))
async def cmd_duel(message: types.Message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

    # ===== дальше идёт ТВОЙ существующий код дуэли =====

    args = message.get_args()
    if not args:
        parts = message.text.split(maxsplit=1)
        args = parts[1] if len(parts) > 1 else None

    if not args:
        await message.reply("❗️Укажи ставку! Пример: /duel 1k или дуэль 2.5kkk")
        return

    bet = parse_bet(args)
    if bet < 1_000:
        await message.reply("❌ Минимальная ставка для дуэли — 1k PLcoins")
        return

    for duel in active_duels.values():
        if duel["creator"] == user_id:
            await message.reply("❌ У тебя уже есть активная дуэль. Заверши её прежде чем создавать новую.")
            return

    if balance < bet:
        await message.reply("❗️У тебя недостаточно PLcoins для ставки.")
        return

    cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (bet, user_id))
    conn.commit()

    duel_id = str(uuid.uuid4())[:8]
    active_duels[duel_id] = {
        "creator": user_id,
        "bet": bet,
        "message_id": None,
        "chat_id": message.chat.id
    }

    duel_msg = await message.reply(
        f"🎲 Вы кинули дуэль кубов!\n💰 Ставка: {format_plcoins(bet)} PLcoins\nКто готов принять дуэль?👇",
        reply_markup=create_duel_keyboard(user_id, duel_id)
    )
    active_duels[duel_id]["message_id"] = duel_msg.message_id



@dp.callback_query_handler(Text(startswith="cancel:"))
async def cancel_duel(callback: types.CallbackQuery):
    _, duel_id, creator_id = callback.data.split(":")
    duel = active_duels.get(duel_id)
    if not duel:
        await callback.answer("Дуэль уже завершена или не найдена.", show_alert=True)
        return

    if str(callback.from_user.id) != creator_id:
        await callback.answer("Только создатель дуэли может её отменить.", show_alert=True)
        return

    # Возвращаем PLcoins
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (duel["bet"], duel["creator"]))
    conn.commit()

    await callback.message.edit_text("❌ Дуэль отменена. Ставка возвращена в PLcoins.")
    del active_duels[duel_id]

@dp.callback_query_handler(Text(startswith="accept:"))
async def accept_duel(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await callback.answer(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                show_alert=True
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute('UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await callback.answer(
                "❌ Вы не зарегистрированы. Нажмите /start для регистрации.",
                show_alert=True
            )
            return
        balance = user_data[0]

    # ===== дальше идёт ТВОЙ существующий код =====

    _, duel_id = callback.data.split(":")
    duel = active_duels.get(duel_id)
    if not duel:
        await callback.answer("Дуэль уже завершена или не найдена.", show_alert=True)
        return

    if callback.from_user.id == duel["creator"]:
        await callback.answer("Ты не можешь принять свою дуэль!", show_alert=True)
        return

    if balance < duel["bet"]:
        await callback.answer("У тебя недостаточно PLcoins для ставки.", show_alert=True)
        return

    cursor.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (duel["bet"], callback.from_user.id)
    )
    conn.commit()

    # дальше код дуэли без изменений


    creator_id = duel["creator"]
    chat_id = duel["chat_id"]
    player1 = await bot.get_chat_member(chat_id, creator_id)
    player2 = callback.from_user

    await callback.message.edit_text(f"🎲 Дуэль принята! Начинаем броски кубиков...")

    # Бросок кубика первого игрока — ответ на сообщение дуэли
    dice_msg1 = await bot.send_dice(chat_id, emoji="🎲", reply_to_message_id=duel["message_id"])
    await asyncio.sleep(4)
    dice1 = dice_msg1.dice.value

    # Бросок кубика второго игрока — ответ на сообщение первого кубика
    dice_msg2 = await bot.send_dice(chat_id, emoji="🎲", reply_to_message_id=dice_msg1.message_id)
    await asyncio.sleep(4)
    dice2 = dice_msg2.dice.value

    # Если ничья, перебрасываем — привязываем к последнему кубику
    while dice1 == dice2:
        tie_msg = await bot.send_message(chat_id, "Ничья! Перебрасываем кубики...", reply_to_message_id=dice_msg2.message_id)
        dice_msg1 = await bot.send_dice(chat_id, emoji="🎲", reply_to_message_id=tie_msg.message_id)
        await asyncio.sleep(4)
        dice1 = dice_msg1.dice.value

        dice_msg2 = await bot.send_dice(chat_id, emoji="🎲", reply_to_message_id=dice_msg1.message_id)
        await asyncio.sleep(4)
        dice2 = dice_msg2.dice.value

    # Определяем победителя
    if dice1 > dice2:
        winner = creator_id
        winner_name = player1.user.full_name
    else:
        winner = player2.id
        winner_name = player2.full_name

    total_prize = duel["bet"] * 2
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (total_prize, winner))
    conn.commit()

    # Результат — ответ на последний кубик
    await bot.send_message(
        chat_id,
        f"🏆 Победитель: {winner_name}\n"
        f"💰 Выигрыш: {format_plcoins(total_prize)} PLcoins\n"
        f"🎲 Результаты:\n"
        f"{player1.user.full_name}: {dice1}\n"
        f"{player2.full_name}: {dice2}",
        reply_to_message_id=dice_msg2.message_id
    )

    del active_duels[duel_id]

@dp.message_handler(commands=["game"])
@dp.message_handler(lambda m: m.text.lower() == "игры")
async def show_games(message: types.Message):
    user_id = message.from_user.id

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute('SELECT ban_until, ban_reason FROM users WHERE user_id = ?', (user_id,))
    ban_info = cursor.fetchone()

    if ban_info:
        ban_until, ban_reason = ban_info
        if ban_until and datetime.strptime(ban_until, '%Y-%m-%d %H:%M:%S') > datetime.now():
            await message.reply(
                f"🚫 Вы были забанены до: {ban_until}, причина: {ban_reason}🚫",
                parse_mode="HTML"
            )
            return
        else:
            # Снимаем бан, если время истекло
            cursor.execute(
                'UPDATE users SET ban_until = NULL, ban_reason = NULL WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()

        # Получаем баланс пользователя
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.reply("❌ Вы не зарегистрированы. Нажмите /start для регистрации.")
            return
        balance = user_data[0]

    text = "🎮 Доступные игры и команды:\n\n"
    for cmd, desc in GAME_COMMANDS:
        text += f"{cmd} — {desc}\n"
    await message.reply(text)

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InlineQueryResultArticle, InputTextMessageContent, CallbackQuery

# ---------------------- CallbackData ----------------------
# ================== КУПОНЫ ==================
coupon_cb = CallbackData("coupon", "action", "code", "amount", "activations", "creator_id")

def generate_coupon_code():
    return str(uuid.uuid4())[:8].upper()

# ---------------------- Инлайн-запрос ----------------------
@dp.inline_handler()
async def inline_coupon(inline_query: types.InlineQuery):
    query = inline_query.query.strip()
    user_id = inline_query.from_user.id
    user_name = inline_query.from_user.full_name

    if not query:
        text = (
            "Создать купон:\n"
            "Купон (сумма купона) (количество активаций)\n"
            "Пример:\n"
            "Купон 1000 1"
        )
        result = InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title="Создать купон",
            input_message_content=InputTextMessageContent(message_text=text)
        )
        await inline_query.answer([result], cache_time=0)
        return

    match = re.match(r"купон\s+(\d+)\s+(\d+)", query)
    if match:
        amount = int(match.group(1))
        activations = int(match.group(2))
        total_cost = amount * activations

        # Проверка лимитов
        if amount <= 0 or activations <= 0:
            text = "❌ Сумма и количество активаций должны быть больше 0"
            result = InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="Ошибка",
                input_message_content=InputTextMessageContent(message_text=text)
            )
            await inline_query.answer([result], cache_time=0)
            return

        if total_cost > 100_000_000:
            max_activations = 100_000_000 // amount
            if max_activations == 0:
                max_activations = 1
            text = (
                f"❌ Сумма {amount:,} PLcoins × {activations} активаций = {total_cost:,} PLcoins.\n"
                f"Максимальное количество активаций для этой суммы: {max_activations}"
            )
            result = InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="Превышен лимит",
                input_message_content=InputTextMessageContent(message_text=text)
            )
            await inline_query.answer([result], cache_time=0)
            return

        # Проверка баланса пользователя
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            user_balance = row["balance"] if row else 0
            if total_cost > user_balance:
                text = f"❌ У вас недостаточно PLcoins для создания купона ({total_cost:,} > {user_balance:,})"
                result = InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Недостаточно средств",
                    input_message_content=InputTextMessageContent(message_text=text)
                )
                await inline_query.answer([result], cache_time=0)
                return

        # Генерация купона
        code = generate_coupon_code()
        activation_text = "активация" if activations == 1 else "активации"
        text = (
            f"💰 Сумма купона: {amount:,} PLcoins\n"
            f"🔢 Количество активаций: {activations}\n"
            f"💸 Всего к списанию: {total_cost:,} PLcoins\n"
            f"👤 От кого: {user_name}\n"
            "Нажмите кнопку ниже, чтобы подтвердить создание купона 👇"
        )

        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton(
                text=f"Создать купон на {amount:,} PLcoins {activations} {activation_text}",
                callback_data=coupon_cb.new(
                    action="confirm_creation",
                    code=code,
                    amount=amount,
                    activations=activations,
                    creator_id=user_id
                )
            )
        )

        result = InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=f"Создать купон на {amount:,} PLcoins {activations} {activation_text}",
            input_message_content=InputTextMessageContent(message_text=text),
            reply_markup=kb
        )

        await inline_query.answer([result], cache_time=0)



# ---------------------- Подтверждение создания ----------------------
@dp.callback_query_handler(coupon_cb.filter(action="confirm_creation"))
async def confirm_creation(callback: CallbackQuery, callback_data: dict):
    code = callback_data["code"]
    amount = int(callback_data["amount"])
    activations = int(callback_data["activations"])
    creator_id = int(callback_data["creator_id"])  # получаем ID создателя
    user_id = callback.from_user.id
    user_name = callback.from_user.full_name

    # Проверяем, что нажал именно создатель
    if user_id != creator_id:
        await callback.answer("❌ Только создатель может создать этот купон", show_alert=True)
        return

    total_cost = amount * activations

    with get_conn() as conn:
        cursor = conn.cursor()
        # Проверка баланса
        cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        user_balance = row["balance"] if row else 0
        if user_balance < total_cost:
            await callback.answer("Недостаточно PLcoins для создания купона", show_alert=True)
            return

        # Списание баланса и создание купона
        cursor.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, ?)", (user_id, 0))
        cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (total_cost, user_id))
        cursor.execute(
            "INSERT INTO promo_codes (code, amount, activations, description) VALUES (?, ?, ?, ?)",
            (code, amount, activations, f"Создан от {user_name}")
        )
        conn.commit()

    text = (
        f"✅ Купон создан!\n\n"
        f"💰 Сумма: {amount} PLcoins\n"
        f"🔢 Количество активаций: {activations}\n"
        f"👤 От кого: {user_name}\n\n"
        "Нажмите кнопку ниже, чтобы забрать купон."
    )

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(
            text="🎁 Забрать купон",
            callback_data=coupon_cb.new(
                action="claim",
                code=code,
                amount=amount,
                activations=activations,
                creator_id=creator_id  # <-- обязательно
            )
        )
    )


    if callback.message:
        await callback.message.edit_text(text, reply_markup=kb)
    elif callback.inline_message_id:
        await bot.edit_message_text(text=text, inline_message_id=callback.inline_message_id, reply_markup=kb)
    await callback.answer("Купон подтвержден и баланс списан ✅", show_alert=True)


# ---------------------- Забрать купон ----------------------
@dp.callback_query_handler(coupon_cb.filter(action="claim"))
async def claim_coupon(callback: CallbackQuery, callback_data: dict):
    code = callback_data["code"]
    user_id = callback.from_user.id
    user_name = callback.from_user.username or callback.from_user.full_name
    creator_id = int(callback_data.get("creator_id", 0))  # ID того, кто создал купон

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT amount, activations FROM promo_codes WHERE code = ?", (code,))
        row = cursor.fetchone()
        if not row:
            await callback.answer("Купон недействителен", show_alert=True)
            return

        amount, activations = row["amount"], row["activations"]

        if activations <= 0:
            # убираем кнопку и показываем сообщение
            if callback.message:
                await callback.message.edit_text("❌ Все активации купона закончились")
            elif callback.inline_message_id:
                await bot.edit_message_text(
                    text="❌ Все активации купона закончились",
                    inline_message_id=callback.inline_message_id
                )
            await callback.answer("Все активации закончились", show_alert=True)
            return

        # Проверяем, забирал ли пользователь
        cursor.execute("SELECT 1 FROM user_promo_codes WHERE user_id = ? AND promo_code = ?", (user_id, code))
        if cursor.fetchone():
            await callback.answer("Вы уже забрали этот купон", show_alert=True)
            return

        # Начисление бонуса и уменьшение активаций
        cursor.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, ?)", (user_id, 0))
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        cursor.execute("INSERT INTO user_promo_codes (user_id, promo_code) VALUES (?, ?)", (user_id, code))
        cursor.execute("UPDATE promo_codes SET activations = activations - 1 WHERE code = ?", (code,))
        conn.commit()

        # Убираем кнопку, если активации закончились
        cursor.execute("SELECT activations FROM promo_codes WHERE code = ?", (code,))
        remaining = cursor.fetchone()[0]
        if remaining <= 0:
            if callback.message:
                await callback.message.edit_text("❌ Все активации купона закончились")
            elif callback.inline_message_id:
                await bot.edit_message_text(
                    text="❌ Все активации купона закончились",
                    inline_message_id=callback.inline_message_id
                )

    # ⚡ Отправка уведомления **только создателю купона** в ЛС
    if creator_id:  # ваш ID как создателя
        try:
            await bot.send_message(
                chat_id=creator_id,
                text=f"⚡ Игрок {user_name} активировал ваш купон на {amount:,} PLcoins!"
            )
        except Exception as e:
            print(f"Ошибка при отправке уведомления создателю: {e}")

    await callback.answer(f"Купон активирован! +{amount} PLcoins к балансу", show_alert=True)







# ==========================================================================================
# ==========================================================================================
# ==========================================================================================

TRACK_CHAT_ID = -1002533366959  # чат, который отслеживаем
CONTEST_HOUR = 22
CONTEST_MINUTE = 00
WIN_REWARD = 2_500_000

last_contest_date = None

# ================== ХЕЛПЕР ==================

def build_nickname(user):
    parts = [p for p in (user.first_name, user.last_name) if p]
    return " ".join(parts) if parts else None

# ================== ТРЕКЕР СООБЩЕНИЙ ==================

@dp.message_handler(lambda m: m.chat.id == TRACK_CHAT_ID, content_types=ContentType.TEXT)
async def track_messages(message: Message):
    user = message.from_user
    now = int(time.time())

    with get_conn() as conn:
        c = conn.cursor()

        c.execute("""
            INSERT OR IGNORE INTO users (user_id, username, nickname, balance)
            VALUES (?, ?, ?, 0)
        """, (
            user.id,
            user.username,
            build_nickname(user)
        ))

        c.execute("""
            UPDATE users
            SET username=?, nickname=?
            WHERE user_id=?
        """, (
            user.username,
            build_nickname(user),
            user.id
        ))

        c.execute("""
            SELECT 1 FROM message_earnings
            WHERE user_id=? AND chat_id=?
        """, (user.id, TRACK_CHAT_ID))

        if c.fetchone():
            c.execute("""
                UPDATE message_earnings
                SET messages = messages + 1,
                    last_message_time = ?
                WHERE user_id=? AND chat_id=?
            """, (now, user.id, TRACK_CHAT_ID))
        else:
            c.execute("""
                INSERT INTO message_earnings
                (user_id, chat_id, messages, last_message_time)
                VALUES (?, ?, 1, ?)
            """, (user.id, TRACK_CHAT_ID, now))

        conn.commit()

# ================== КОНКУРС ==================

async def daily_message_contest():
    global last_contest_date

    while True:
        now = datetime.now()
        today = now.date()

        if (
            now.hour == CONTEST_HOUR
            and now.minute == CONTEST_MINUTE
            and last_contest_date != today
        ):
            last_contest_date = today

            with get_conn() as conn:
                c = conn.cursor()

                c.execute("""
                    SELECT u.user_id, u.username, u.nickname, me.messages
                    FROM message_earnings me
                    JOIN users u ON u.user_id = me.user_id
                    WHERE me.chat_id=?
                    ORDER BY me.messages DESC
                    LIMIT 10
                """, (TRACK_CHAT_ID,))

                top_users = c.fetchall()

                text = "🏅Результаты ежедневного конкурса за сообщения:\n\n"
                text += "🎊Победители\n"

                rewarded_users = []

                for idx, row in enumerate(top_users, 1):
                    if row["messages"] == 0:
                        text += f"{idx}. None\n"
                    else:
                        if row["username"]:
                            name = f"@{row['username']}"
                        elif row["nickname"]:
                            name = row["nickname"]
                        else:
                            name = "None"

                        name = html.escape(name)
                        text += f"{idx}. {name} — {row['messages']} сообщений\n"

                    rewarded_users.append(row["user_id"])

                for i in range(len(top_users) + 1, 11):
                    text += f"{i}. None\n"

                text += f"\nКаждый получил по {WIN_REWARD:,} PLcoins себе на баланс💰"

                # 💰 НАЧИСЛЕНИЕ — ВСЕМ 10
                for user_id in rewarded_users:
                    c.execute("""
                        UPDATE users
                        SET balance = balance + ?
                        WHERE user_id=?
                    """, (WIN_REWARD, user_id))

                # 🔄 СБРОС СООБЩЕНИЙ
                c.execute("""
                    UPDATE message_earnings
                    SET messages = 0
                    WHERE chat_id=?
                """, (TRACK_CHAT_ID,))

                conn.commit()

                sent = await bot.send_message(TRACK_CHAT_ID, text)
                await bot.pin_chat_message(
                    TRACK_CHAT_ID,
                    sent.message_id,
                    disable_notification=True
                )

        await asyncio.sleep(20)
        

@dp.message_handler(commands=['web'])
async def cmd_web(message: types.Message):
    user_id = message.from_user.id
    
    # Создаем клавиатуру с WebApp кнопкой
    keyboard = types.InlineKeyboardMarkup()
    
    # Добавляем кнопку, которая откроет ваше приложение
    web_app_button = types.InlineKeyboardButton(
        text="🚀 Открыть LuckyPL App", 
        web_app=WebAppInfo(url=WEB_APP_URL)
    )
    keyboard.add(web_app_button)
    
    text = (
        f"👋 Привет, {message.from_user.first_name}!\n"
        f"Нажми кнопку ниже, чтобы открыть личный кабинет.\n"
        f"Там ты сможешь увидеть баланс и получить бонус."
    )
    
    await message.answer(text, reply_markup=keyboard)

# ================== WEB APP API ==================

# 1. API: Получить профиль
async def api_get_profile(user_id):
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT balance, status FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
            
            if user:
                status = user['status'] if user['status'] else "Новичок"
                return {'balance': int(user['balance']), 'status': status, 'exists': True}
            else:
                return {'balance': 0, 'status': 'Гость', 'exists': False}
    except Exception as e:
        logging.error(f"API Error get_profile: {e}")
        return {'balance': 0, 'status': 'Ошибка', 'exists': False}

# 2. API: Добавить бонус
async def api_add_bonus(user_id):
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT balance, last_bonus FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
            
            if not user:
                return {'success': False, 'message': 'Пользователь не найден'}

            # Проверка времени бонуса (раз в 24 часа)
            last_bonus_time = user['last_bonus'] or 0
            current_time = int(time.time())
            
            if current_time - last_bonus_time < 86400: # 86400 секунд = 24 часа
                remain = 86400 - (current_time - last_bonus_time)
                hours = remain // 3600
                minutes = (remain % 3600) // 60
                return {'success': False, 'message': f'Бонус можно получить через {hours}ч {minutes}мин'}

            # Начисление
            amount = 100
            new_balance = user['balance'] + amount
            
            # Логика статуса
            new_status = user['status']
            if new_balance >= 1000 and (not new_status or new_status == "Новичок"):
                new_status = "PRO Игрок"
            if new_balance >= 5000:
                new_status = "Легенда"

            cursor.execute('UPDATE users SET balance = ?, status = ?, last_bonus = ? WHERE user_id = ?', 
                           (new_balance, new_status, current_time, user_id))
            conn.commit()
            
            return {'success': True, 'new_balance': int(new_balance), 'new_status': new_status}
    except Exception as e:
        logging.error(f"API Error add_bonus: {e}")
        return {'success': False, 'message': 'Ошибка сервера'}

# 3. Обработчики HTTP запросов
async def handle_get_profile(request):
    user_id = request.rel_url.query.get('user_id')
    if not user_id: return web.json_response({'error': 'no_id'}, status=400)
    data = await api_get_profile(int(user_id))
    return web.json_response(data)

async def handle_add_bonus(request):
    try:
        data = await request.json()
        user_id = data.get('user_id')
        if not user_id: return web.json_response({'error': 'no_id'}, status=400)
        result = await api_add_bonus(int(user_id))
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def handle_index(request):
    # Отдает index.html из папки web
    try:
        path = os.path.join(os.path.dirname(__file__), 'web', 'index.html')
        with open(path, 'r', encoding='utf-8') as f:
            return web.Response(text=f.read(), content_type='text/html')
    except FileNotFoundError:
        return web.Response(text="Create folder 'web' and put index.html there", status=404)

# ================== ON_STARTUP ==================

async def on_startup(_):
    # 1. Запускаем ваши фоновые задачи (если они есть)
    # asyncio.create_task(give_hourly_income()) # Раскомментируйте, если функции есть
    # asyncio.create_task(kn_timeout_checker())
    # asyncio.create_task(daily_message_contest())
    
    # 2. Запускаем Web Server для WebApp
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index),
        web.get('/api/get_profile', handle_get_profile),
        web.post('/api/add_bonus', handle_add_bonus),
    ])
    
    runner = web.AppRunner(app)
    await runner.setup()
    # Запуск на порту 8080
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print("WebApp Server started on http://0.0.0.0:8080")

if __name__ == "__main__":
    print("BOT STARTED")
    # Передаем on_startup в executor
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)

if __name__ == "__main__":
    print("BOT STARTED")
    # Передаем on_startup в executor
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
