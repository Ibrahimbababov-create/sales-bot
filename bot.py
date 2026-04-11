import os
import json
import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# =========================
# ENV
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDENTIALS_RAW = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

if not GOOGLE_CREDENTIALS_RAW:
    raise ValueError("Не найден GOOGLE_CREDENTIALS_JSON в переменных окружения")

# =========================
# CONFIG
# =========================
TIMEZONE = "Asia/Almaty"
CACHE_TTL = 60  # секунд

SALES_BOT_SPREADSHEET_ID = "19psuYsJk6s6Si-9vh7LaAvHp5LdmpiQvHFJVHiCwmnc"
SALES_BOT_SHEET_NAME = "Sales-Bot"

TODAY_SOURCES = [
    {
        "project": "Бухонин",
        "spreadsheet_id": "1vR5lHuASxlscEGscv6ka4QBRz73OE6Zbf5kvbgGCQPQ",
        "date_col_index": 1,   # B = дата
        "amount_col_index": 6, # G = сумма продажи
    },
    {
        "project": "Шолпан",
        "spreadsheet_id": "1FArSB2jUMY67MlTk7Z2-38Qr8BqGr5jdWjTo1CL73LM",
        "date_col_index": 1,   # B = дата
        "amount_col_index": 9, # J = сумма продажи
    },
    {
        "project": "Кайсар",
        "spreadsheet_id": "1YP7Xl6Sju7rAUVbqtQaryqpKbawr5_qJSEvbQTWoK84",
        "date_col_index": 1,   # B = дата
        "amount_col_index": 9, # J = сумма продажи
    },
]

SKIP_KEYWORDS = [
    "январ", "феврал", "март", "апрел", "май", "июн", "июл",
    "август", "сент", "октя", "ноябр", "декабр", "база", "base"
]

# =========================
# CACHE
# =========================
cache = {
    "top": {"time": 0, "data": None},
    "today": {"time": 0, "data": None},
}


def is_cache_valid(key: str) -> bool:
    return time.time() - cache[key]["time"] < CACHE_TTL


def set_cache(key: str, data) -> None:
    cache[key]["time"] = time.time()
    cache[key]["data"] = data


def get_cache(key: str):
    return cache[key]["data"]


# =========================
# GOOGLE
# =========================
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS_RAW),
    scopes=scope,
)
client = gspread.authorize(creds)

sheet = client.open_by_key(SALES_BOT_SPREADSHEET_ID).worksheet(SALES_BOT_SHEET_NAME)

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

        # убираем валюту и пробелы
        s = s.replace("₸", "").replace("\xa0", "").replace(" ", "").strip()

        # если есть и точка, и запятая
        # последний разделитель считаем десятичной частью
        if "," in s and "." in s:
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            decimal_pos = max(last_comma, last_dot)
            s = s[:decimal_pos]

        # если есть только запятая
        elif "," in s:
            parts = s.split(",")
            if len(parts[-1]) <= 2:
                s = ",".join(parts[:-1]) or parts[0]
            s = s.replace(",", "")

        # если есть только точка
        elif "." in s:
            parts = s.split(".")
            if len(parts[-1]) <= 2:
                s = ".".join(parts[:-1]) or parts[0]
            s = s.replace(".", "")

        # оставляем только цифры и минус
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


def parse_date(x):
    text = str(x).strip()
    if not text:
        return None

    formats = [
        "%d.%m.%Y",
        "%d.%m.%y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue

    return None


def skip_sheet(name: str) -> bool:
    name = name.lower().strip()
    return any(k in name for k in SKIP_KEYWORDS)


# =========================
# DATA LOADERS
# =========================
def load_top_data():
    if is_cache_valid("top"):
        return get_cache("top")

    data = sheet.get_all_values()[1:]
    managers = []

    for row in data:
        if len(row) < 4:
            continue

        name = row[0].strip()
        team = row[1].strip()
        amount = parse_amount(row[3])

        if not name:
            continue

        managers.append((name, amount, team))

    managers.sort(key=lambda x: x[1], reverse=True)

    set_cache("top", managers)
    return managers


def load_today_data():
    if is_cache_valid("today"):
        return get_cache("today")

    today = datetime.now(ZoneInfo(TIMEZONE)).date()
    result = []

    for src in TODAY_SOURCES:
        ss = client.open_by_key(src["spreadsheet_id"])

        for ws in ss.worksheets():
            if skip_sheet(ws.title):
                continue

            values = ws.get_all_values()

            # 1 строка: итоги
            # 2 строка: заголовки
            # с 3 строки: данные
            if len(values) < 3:
                continue

            rows = values[2:]

            for row in rows:
                if len(row) <= max(src["date_col_index"], src["amount_col_index"]):
                    continue

                sale_date = parse_date(row[src["date_col_index"]])
                if sale_date != today:
                    continue

                amount = parse_amount(row[src["amount_col_index"]])
                if amount <= 0:
                    continue

                result.append((ws.title.strip(), src["project"], amount))

    set_cache("today", result)
    return result


# =========================
# COMMANDS
# =========================
@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Команды:\n"
        "/top5\n"
        "/topall\n"
        "/topteam\n"
        "/today\n"
        "/todayteam\n"
        "/chatid"
    )


@dp.message(Command("chatid"))
async def chatid(message: Message):
    await message.answer(f"chat_id: {message.chat.id}")


@dp.message(Command("top5"))
async def top5(message: Message):
    data = load_top_data()

    if not data:
        await message.answer("Нет данных.")
        return

    text = "Топ 5:\n\n"
    for i, (name, amount, _) in enumerate(data[:5], 1):
        text += f"{i}. {name} — {format_amount(amount)}\n"

    await message.answer(text)


@dp.message(Command("topall"))
async def topall(message: Message):
    data = load_top_data()

    if not data:
        await message.answer("Нет данных.")
        return

    text = "Все:\n\n"
    for i, (name, amount, _) in enumerate(data, 1):
        text += f"{i}. {name} — {format_amount(amount)}\n"

    await message.answer(text)


@dp.message(Command("topteam"))
async def topteam(message: Message):
    data = load_top_data()

    if not data:
        await message.answer("Нет данных.")
        return

    teams = {}
    for _, amount, team in data:
        teams[team] = teams.get(team, 0) + amount

    sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

    text = "Команды:\n\n"
    for i, (team, amount) in enumerate(sorted_teams, 1):
        text += f"{i}. {team} — {format_amount(amount)}\n"

    await message.answer(text)


@dp.message(Command("today"))
async def today(message: Message):
    data = load_today_data()

    if not data:
        await message.answer("Сегодня оплат нет")
        return

    grouped = {}
    for name, project, amount in data:
        grouped[(name, project)] = grouped.get((name, project), 0) + amount

    sorted_data = sorted(grouped.items(), key=lambda x: x[1], reverse=True)

    text = "Сегодня:\n\n"
    total = 0

    for i, ((name, project), amount) in enumerate(sorted_data, 1):
        text += f"{i}. {name} [{project}] — {format_amount(amount)}\n"
        total += amount

    text += f"\nИтого: {format_amount(total)}"

    await message.answer(text)


@dp.message(Command("todayteam"))
async def todayteam(message: Message):
    data = load_today_data()

    if not data:
        await message.answer("Сегодня оплат нет")
        return

    teams = {}
    for _, project, amount in data:
        teams[project] = teams.get(project, 0) + amount

    sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

    text = "Сегодня по командам:\n\n"
    total = 0

    for i, (team, amount) in enumerate(sorted_teams, 1):
        text += f"{i}. {team} — {format_amount(amount)}\n"
        total += amount

    text += f"\nИтого: {format_amount(total)}"

    await message.answer(text)


# =========================
# RUN
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
