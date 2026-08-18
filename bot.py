import os
import json
import asyncio
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
from typing import Optional

import aiohttp
import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    MessageEntity,
    User,
)

from fastapi import FastAPI, Request, Header, HTTPException

# =========================
# ENV
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDENTIALS_RAW = os.getenv("GOOGLE_CREDENTIALS_JSON")
PROCESS_SECRET = os.getenv("PROCESS_SECRET")  # секретный ключ для защиты /process
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

if not GOOGLE_CREDENTIALS_RAW:
    raise ValueError("Не найден GOOGLE_CREDENTIALS_JSON в переменных окружения")

# =========================
# CONFIG
# =========================
TIMEZONE = "Asia/Almaty"
CACHE_TTL = 60  # секунд

# Таблица "Pacto Расходы и доходы"
SALES_BOT_SPREADSHEET_ID = "1d4PSPskoQhODRJUgeG0roCQ2_FzqbbGKRQUBLmz-RiU"

# ВАЖНО: chat_id группы с минусом
PLAN_ALERT_CHAT_ID = -1003065195919

# Структура листа месяца (одинакова для всех месяцев, см. "Шаблон месяца")
PROJECT_HEADER_ROWS = [11, 28, 45, 62, 79, 96, 113, 130, 147]  # строка с названием проекта
MANAGERS_PER_BLOCK = 10       # header+2 .. header+11 — строки менеджеров
ITOGO_OFFSET = 12             # header+12 — строка ИТОГО (сумма по проекту)

COL_NAME = 0    # A — Менеджер
COL_FACT = 3    # D — Факт тотал
COL_PLAN_PCT = 5  # F — % плана

RU_MONTHS = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

PLAN_ALERTS_FILE = "plan_alerts_state.json"

PACTOCOINS_URL = "https://pactocoins.vercel.app"

# =========================
# CACHE
# =========================
cache = {
    "month": {"time": 0, "data": None},
}


def is_cache_valid(key: str) -> bool:
    return time.time() - cache[key]["time"] < CACHE_TTL


def set_cache(key: str, data) -> None:
    cache[key]["time"] = time.time()
    cache[key]["data"] = data


def get_cache(key: str):
    return cache[key]["data"]


# =========================
# FILE HELPERS
# =========================
def load_json_file(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


plan_alerts_state = load_json_file(PLAN_ALERTS_FILE, {})


def get_current_month_key() -> str:
    return datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m")


def get_current_month_sheet_title() -> str:
    now = datetime.now(ZoneInfo(TIMEZONE))
    return f"{RU_MONTHS[now.month]} {now.year}"


def was_plan_alert_sent(name: str, level: str) -> bool:
    month_key = get_current_month_key()
    return plan_alerts_state.get(month_key, {}).get(name, {}).get(level, False)


def mark_plan_alert_sent(name: str, level: str):
    month_key = get_current_month_key()

    if month_key not in plan_alerts_state:
        plan_alerts_state[month_key] = {}

    if name not in plan_alerts_state[month_key]:
        plan_alerts_state[month_key][name] = {}

    plan_alerts_state[month_key][name][level] = True
    save_json_file(PLAN_ALERTS_FILE, plan_alerts_state)


# =========================
# GOOGLE
# =========================
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS_RAW),
    scopes=scope,
)
client = gspread.authorize(creds)

spreadsheet = client.open_by_key(SALES_BOT_SPREADSHEET_ID)

# =========================
# BOT
# =========================
bot = Bot(token=TOKEN)
dp = Dispatcher()

# =========================
# HELPERS
# =========================
def format_amount(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def parse_amount(x) -> int:
    try:
        s = str(x).strip()

        if not s:
            return 0

        s = s.replace("₸", "").replace("\xa0", "").replace(" ", "").strip()

        if "," in s and "." in s:
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            decimal_pos = max(last_comma, last_dot)
            s = s[:decimal_pos]

        elif "," in s:
            parts = s.split(",")
            if len(parts[-1]) <= 2:
                s = ",".join(parts[:-1]) or parts[0]
            s = s.replace(",", "")

        elif "." in s:
            parts = s.split(".")
            if len(parts[-1]) <= 2:
                s = ".".join(parts[:-1]) or parts[0]
            s = s.replace(".", "")

        cleaned = []
        for ch in s:
            if ch.isdigit() or ch == "-":
                cleaned.append(ch)

        s = "".join(cleaned)

        if s in ("", "-"):
            return 0

        return int(s)

    except Exception:
        return 0


def parse_percent(x) -> float:
    try:
        s = str(x).strip().replace("%", "").replace(",", ".")
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def safe_get(values, row_idx: int, col_idx: int) -> str:
    if row_idx < 0 or row_idx >= len(values):
        return ""
    row = values[row_idx]
    if col_idx >= len(row):
        return ""
    return str(row[col_idx]).strip()


# =========================
# DATA LOADER (текущий месяц, автоматически)
# =========================
def get_current_month_worksheet():
    title = get_current_month_sheet_title()
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return None


def load_month_data():
    """
    Возвращает {"managers": [...], "teams": [...]} за текущий месяц.
    Каждый manager: {"name", "amount", "team", "percent"}
    Каждый team: {"team", "total"}
    Лист месяца определяется автоматически по текущей дате — руками менять не нужно.
    """
    if is_cache_valid("month"):
        return get_cache("month")

    ws = get_current_month_worksheet()
    if ws is None:
        result = {"managers": [], "teams": []}
        set_cache("month", result)
        return result

    values = ws.get_all_values()

    managers = []
    teams = []

    for h in PROJECT_HEADER_ROWS:
        team_name = safe_get(values, h - 1, COL_NAME)

        for r in range(h + 2, h + 2 + MANAGERS_PER_BLOCK):
            row_idx = r - 1
            name = safe_get(values, row_idx, COL_NAME)

            if not name:
                continue

            amount = parse_amount(safe_get(values, row_idx, COL_FACT))
            percent = parse_percent(safe_get(values, row_idx, COL_PLAN_PCT))

            managers.append({
                "name": name,
                "amount": amount,
                "team": team_name,
                "percent": percent,
            })

        itogo_idx = h + ITOGO_OFFSET - 1
        team_total = parse_amount(safe_get(values, itogo_idx, COL_FACT))
        teams.append({"team": team_name, "total": team_total})

    result = {"managers": managers, "teams": teams}
    set_cache("month", result)
    return result


def load_plan_percent_data():
    data = load_month_data()
    return [(m["name"], m["percent"]) for m in data["managers"]]


# =========================
# PLAN ALERTS TEXT
# =========================
def build_80_text(name: str, percent: float) -> str:
    return (
        f"🔥 {name} выполнил план на {percent:.0f}%!\n"
        f"Осталось совсем чуть-чуть до 100%."
    )


def build_100_text(name: str, percent: float) -> str:
    return (
        f"🏆 {name} выполнил план на {percent:.0f}%!\n"
        f"План закрыт. Красавчик."
    )


# =========================
# PLAN ALERTS LOGIC
# =========================
async def check_plan_alerts(send_messages: bool = True):
    data = load_plan_percent_data()
    alerts_sent = []

    for name, percent in data:
        if percent >= 100:
            if not was_plan_alert_sent(name, "100"):
                text = build_100_text(name, percent)

                if send_messages:
                    await bot.send_message(chat_id=PLAN_ALERT_CHAT_ID, text=text)

                mark_plan_alert_sent(name, "100")
                alerts_sent.append(f"{name} -> 100%")

        elif percent >= 80:
            if not was_plan_alert_sent(name, "80"):
                text = build_80_text(name, percent)

                if send_messages:
                    await bot.send_message(chat_id=PLAN_ALERT_CHAT_ID, text=text)

                mark_plan_alert_sent(name, "80")
                alerts_sent.append(f"{name} -> 80%")

    return alerts_sent


async def plan_alerts_loop():
    while True:
        try:
            await check_plan_alerts(send_messages=True)
        except Exception as e:
            print(f"[PLAN ALERTS ERROR] {e}")

        await asyncio.sleep(3600)


# =========================
# COMMANDS
# =========================
@dp.message(Command("start"))
async def start(message: Message):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📲 Открыть PactoCoins",
                    web_app=WebAppInfo(url=PACTOCOINS_URL),
                )
            ]
        ]
    )

    await message.answer(
        "Привет! Это бот отдела продаж.\n\n"
        "💰 <b>PactoCoins</b> — копи coins за выручку и бонусы, следи за "
        "своим званием, трать в магазине наград или скидывайся в копилку "
        "с командой. Полная инструкция — прямо в приложении, раздел "
        "«Инструкция».\n\n"
        "📊 Команды со статистикой продаж:\n"
        "/top5\n"
        "/topall\n"
        "/topteam\n"
        "/chatid\n"
        "/checkplan",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.message(Command("chatid"))
async def chatid(message: Message):
    await message.answer(f"chat_id: {message.chat.id}")


@dp.message(Command("checkplan"))
async def checkplan(message: Message):
    alerts = await check_plan_alerts(send_messages=True)

    if not alerts:
        await message.answer("Новых уведомлений по плану нет.")
        return

    text = "Отправлены уведомления:\n\n" + "\n".join(alerts)
    await message.answer(text)


@dp.message(Command("top5"))
async def top5(message: Message):
    data = load_month_data()["managers"]
    data = [m for m in data if m["amount"] > 0]  # прячем нулевых
    data.sort(key=lambda m: m["amount"], reverse=True)

    if not data:
        await message.answer("Нет данных.")
        return

    text = f"Топ 5 ({get_current_month_sheet_title()}):\n\n"
    for i, m in enumerate(data[:5], 1):
        text += f"{i}. {m['name']} — {format_amount(m['amount'])}\n"

    await message.answer(text)


@dp.message(Command("topall"))
async def topall(message: Message):
    data = load_month_data()["managers"]
    data = [m for m in data if m["amount"] > 0]  # прячем нулевых
    data.sort(key=lambda m: m["amount"], reverse=True)

    if not data:
        await message.answer("Нет данных.")
        return

    text = f"Все ({get_current_month_sheet_title()}):\n\n"
    for i, m in enumerate(data, 1):
        text += f"{i}. {m['name']} — {format_amount(m['amount'])}\n"

    await message.answer(text)


@dp.message(Command("topteam"))
async def topteam(message: Message):
    teams = load_month_data()["teams"]
    teams = [t for t in teams if t["total"] > 0]  # прячем нулевые проекты
    teams.sort(key=lambda t: t["total"], reverse=True)

    if not teams:
        await message.answer("Нет данных.")
        return

    text = f"Команды ({get_current_month_sheet_title()}):\n\n"
    for i, t in enumerate(teams, 1):
        text += f"{i}. {t['team']} — {format_amount(t['total'])}\n"

    await message.answer(text)


async def fetch_registered_users():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return []

    url = (
        f"{SUPABASE_URL}/rest/v1/users"
        "?select=id,name,telegram_id,role"
        "&telegram_id=not.is.null"
    )
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    print(f"[ALL FETCH ERROR] status={resp.status} body={body}")
                    return []
                return json.loads(body)
    except Exception:
        print(f"[ALL FETCH ERROR]\n{traceback.format_exc()}")
        return []


@dp.message(Command("all"))
async def mention_all(message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда работает только в группе.")
        return

    users = await fetch_registered_users()
    sender = next(
        (u for u in users if u.get("telegram_id") == message.from_user.id), None
    )

    if not sender or sender.get("role") != "admin":
        await message.answer("Команда только для админа.")
        return

    targets = [u for u in users if u.get("telegram_id")]
    if not targets:
        await message.answer("Некого отмечать — никто ещё не заходил в приложение.")
        return

    text = "Все"
    entities = [
        MessageEntity(
            type="text_mention",
            offset=0,
            length=len(text),
            user=User(
                id=u["telegram_id"],
                is_bot=False,
                first_name=(u.get("name") or "Сотрудник")[:64],
            ),
        )
        for u in targets
    ]

    await bot.send_message(
        chat_id=message.chat.id,
        text=text,
        entities=entities,
        message_thread_id=message.message_thread_id,
    )


# =========================
# INBOX (обычные сообщения боту, не команды)
# =========================
async def save_inbox_message(telegram_id: int, telegram_name: str, text: str):
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return

    url = f"{SUPABASE_URL}/rest/v1/bot_inbox_messages"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "telegram_id": telegram_id,
        "telegram_name": telegram_name,
        "text": text,
    }

    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            )
    except Exception as e:
        print(f"[INBOX SAVE ERROR] {e}")


# Ловит всё, что не подошло ни под одну команду выше — должен быть
# зарегистрирован последним, иначе перехватит команды тоже.
@dp.message()
async def catch_all(message: Message):
    if not message.text:
        return

    name_parts = [message.from_user.first_name or ""]
    if message.from_user.last_name:
        name_parts.append(message.from_user.last_name)
    name = " ".join(p for p in name_parts if p).strip()
    if message.from_user.username:
        name = f"{name} (@{message.from_user.username})".strip()

    await save_inbox_message(message.from_user.id, name, message.text)


# =========================
# ЛИЧНЫЕ НАПОМИНАНИЯ (настраиваются в PactoCoins → Настройки)
# =========================
REMINDER_TEXT = (
    "⏰ Напоминание: не забудь сегодня отправить заявку на выручку "
    "или отметиться по бонусам, если ещё не успел."
)


def reminder_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📲 Открыть PactoCoins",
                    web_app=WebAppInfo(url=PACTOCOINS_URL),
                )
            ]
        ]
    )


async def fetch_due_reminder_users():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        print("[REMINDERS FETCH] SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY not set")
        return []

    url = (
        f"{SUPABASE_URL}/rest/v1/users"
        "?select=id,telegram_id,reminder_time,reminder_last_sent_date"
        "&reminder_enabled=eq.true"
        "&telegram_id=not.is.null"
    )
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    print(f"[REMINDERS FETCH ERROR] status={resp.status} body={body}")
                    return []
                return json.loads(body)
    except Exception:
        print(f"[REMINDERS FETCH ERROR]\n{traceback.format_exc()}")
        return []


async def mark_reminder_sent(user_id: str, date_str: str):
    url = f"{SUPABASE_URL}/rest/v1/users?id=eq.{user_id}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    try:
        async with aiohttp.ClientSession() as session:
            await session.patch(
                url,
                headers=headers,
                json={"reminder_last_sent_date": date_str},
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as e:
        print(f"[REMINDERS MARK ERROR] {e}")


async def check_reminders():
    now = datetime.now(ZoneInfo(TIMEZONE))
    today_str = now.strftime("%Y-%m-%d")
    now_hm = now.strftime("%H:%M")

    users = await fetch_due_reminder_users()
    print(f"[REMINDERS CHECK] now={now_hm} candidates={len(users)}")

    for u in users:
        reminder_time = (u.get("reminder_time") or "")[:5]  # "HH:MM:SS" -> "HH:MM"
        print(
            f"[REMINDERS CHECK] user={u.get('id')} reminder_time={reminder_time!r} "
            f"now={now_hm!r} last_sent={u.get('reminder_last_sent_date')!r}"
        )

        if reminder_time != now_hm:
            continue
        if u.get("reminder_last_sent_date") == today_str:
            continue

        print(f"[REMINDERS MATCH] sending to telegram_id={u.get('telegram_id')}")

        try:
            result = await bot.send_message(
                chat_id=u["telegram_id"],
                text=REMINDER_TEXT,
                reply_markup=reminder_keyboard(),
            )
            print(f"[REMINDERS SENT] user={u.get('id')} message_id={result.message_id}")
        except Exception:
            print(f"[REMINDER SEND ERROR] user {u.get('id')}:\n{traceback.format_exc()}")

        await mark_reminder_sent(u["id"], today_str)


async def reminders_loop():
    print("[REMINDERS LOOP] started")
    while True:
        try:
            await check_reminders()
        except Exception:
            print(f"[REMINDERS LOOP ERROR]\n{traceback.format_exc()}")

        await asyncio.sleep(60)


# =========================
# FASTAPI APP (вместо polling)
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(plan_alerts_loop())
    reminders_task = asyncio.create_task(reminders_loop())
    yield
    task.cancel()
    reminders_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "ok"}


@app.post("/process")
async def process_update(
    request: Request,
    x_internal_secret: Optional[str] = Header(default=None),
):
    if PROCESS_SECRET and x_internal_secret != PROCESS_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot=bot, update=update)

    return {"ok": True}
