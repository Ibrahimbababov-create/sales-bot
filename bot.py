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
        "date_col_index": 1,
        "amount_col_index": 6,
    },
    {
        "project": "Шолпан",
        "spreadsheet_id": "1FArSB2jUMY67MlTk7Z2-38Qr8BqGr5jdWjTo1CL73LM",
        "date_col_index": 1,
        "amount_col_index": 9,
    },
    {
        "project": "Кайсар",
        "spreadsheet_id": "1YP7Xl6Sju7rAUVbqtQaryqpKbawr5_qJSEvbQTWoK84",
        "date_col_index": 1,
        "amount_col_index": 9,
    },
]

SKIP_KEYWORDS = ["январ", "феврал", "март", "апрел", "май", "июн", "июл", "август", "сент", "октя", "ноябр", "декабр", "база"]

# =========================
# CACHE
# =========================
cache = {
    "top": {"time": 0, "data": None},
    "today": {"time": 0, "data": None},
}

def is_cache_valid(key):
    return time.time() - cache[key]["time"] < CACHE_TTL

def set_cache(key, data):
    cache[key]["time"] = time.time()
    cache[key]["data"] = data

def get_cache(key):
    return cache[key]["data"]

# =========================
# GOOGLE
# =========================
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_RAW), scopes=scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(SALES_BOT_SPREADSHEET_ID).worksheet(SALES_BOT_SHEET_NAME)

bot = Bot(token=TOKEN)
dp = Dispatcher()

# =========================
# HELPERS
# =========================
def format_amount(n):
    return f"{n:,}".replace(",", " ")

def parse_amount(x):
    try:
        s = str(x).strip()

        if not s:
            return 0

        # убираем валюту и невидимые пробелы
        s = s.replace("₸", "").replace("\xa0", "").replace(" ", "").strip()

        # если есть и точка, и запятая
        # считаем, что последний символ-разделитель — это десятичная часть
        # а все до него — целая часть
        if "," in s and "." in s:
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            decimal_pos = max(last_comma, last_dot)
            s = s[:decimal_pos]

        # если есть только запятая
        elif "," in s:
            parts = s.split(",")
            # если после запятой 1-2 цифры, это копейки -> отрезаем
            if len(parts[-1]) <= 2:
                s = ",".join(parts[:-1]) or parts[0]
            s = s.replace(",", "")

        # если есть только точка
        elif "." in s:
            parts = s.split(".")
            # если после точки 1-2 цифры, это копейки -> отрезаем
            if len(parts[-1]) <= 2:
                s = ".".join(parts[:-1]) or parts[0]
            s = s.replace(".", "")

        # на всякий случай оставляем только цифры и минус
        cleaned = []
        for ch in s:
            if ch.isdigit() or ch == "-":
                cleaned.append(ch)

        s = "".join(cleaned)

        if s in ("", "-"):
            return 0

        return int(s)

    except:
        return 0
        return int(
            str(x)
            .replace(" ", "")
            .replace("\xa0", "")
            .replace(",", "")
            .replace("₸", "")
            .strip()
        )
    except:
        return 0

def parse_date(x):
    try:
        return datetime.strptime(x, "%d.%m.%Y").date()
    except:
        return None

def skip_sheet(name):
    name = name.lower()
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
        name = row[0]
        amount = parse_amount(row[3])
        managers.append((name, amount, row[1]))

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

            values = ws.get_all_values()[2:]

            for row in values:
                if len(row) <= src["amount_col_index"]:
                    continue

                d = parse_date(row[src["date_col_index"]])
                if d != today:
                    continue

                amount = parse_amount(row[src["amount_col_index"]])
                if amount > 0:
                    result.append((ws.title, src["project"], amount))

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
        "/todayteam"
    )


@dp.message(Command("top5"))
async def top5(message: Message):
    data = load_top_data()

    text = "Топ 5:\n\n"
    for i, (name, amount, _) in enumerate(data[:5], 1):
        text += f"{i}. {name} — {format_amount(amount)}\n"

    await message.answer(text)


@dp.message(Command("topall"))
async def topall(message: Message):
    data = load_top_data()

    text = "Все:\n\n"
    for i, (name, amount, _) in enumerate(data, 1):
        text += f"{i}. {name} — {format_amount(amount)}\n"

    await message.answer(text)


@dp.message(Command("topteam"))
async def topteam(message: Message):
    data = load_top_data()

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

    res = {}
    for name, project, amount in data:
        res[(name, project)] = res.get((name, project), 0) + amount

    sorted_data = sorted(res.items(), key=lambda x: x[1], reverse=True)

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
