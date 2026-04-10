import os
import json
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# =========================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDENTIALS_RAW = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

if not GOOGLE_CREDENTIALS_RAW:
    raise ValueError("Не найден GOOGLE_CREDENTIALS_JSON в переменных окружения")

# =========================
# ОСНОВНАЯ ТАБЛИЦА ДЛЯ СТАРЫХ КОМАНД
# =========================
SALES_BOT_SPREADSHEET_ID = "19psuYsJk6s6Si-9vh7LaAvHp5LdmpiQvHFJVHiCwmnc"
SALES_BOT_SHEET_NAME = "Sales-Bot"

# =========================
# ЧАСОВОЙ ПОЯС
# =========================
TIMEZONE = "Asia/Almaty"

# =========================
# ИСТОЧНИКИ ДЛЯ /today И /todayteam
# amount_col_index = индекс колонки "Сумма продажи" (0-based)
# У Бухонина сумма в G => индекс 6
# У Шолпан и Кайсар сумма в J => индекс 9
# date_col_index почти везде B => индекс 1
# =========================
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

# =========================
# КЛЮЧЕВЫЕ СЛОВА ДЛЯ ИГНОРА ЛИСТОВ
# =========================
SKIP_SHEET_KEYWORDS = [
    "январ",
    "феврал",
    "март",
    "апрел",
    "май",
    "мая",
    "июн",
    "июл",
    "август",
    "сент",
    "октя",
    "ноябр",
    "декабр",
    "база",
    "base",
]

# =========================
# GOOGLE + BOT
# =========================
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
google_creds = json.loads(GOOGLE_CREDENTIALS_RAW)

creds = Credentials.from_service_account_info(
    google_creds,
    scopes=scope
)

client = gspread.authorize(creds)

bot = Bot(token=TOKEN)
dp = Dispatcher()

sales_bot_sheet = client.open_by_key(SALES_BOT_SPREADSHEET_ID).worksheet(SALES_BOT_SHEET_NAME)


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================
def format_amount(amount: int) -> str:
    return f"{amount:,}".replace(",", " ")


def parse_amount(value) -> int:
    """
    Превращает сумму в число.
    Убирает пробелы, ₸, запятые, неразрывные пробелы.
    """
    if value is None:
        return 0

    text = str(value)
    text = text.replace("₸", "")
    text = text.replace(" ", "")
    text = text.replace("\xa0", "")
    text = text.replace(",", "")
    text = text.strip()

    if not text:
        return 0

    try:
        return int(float(text))
    except Exception:
        return 0


def parse_sheet_date(value):
    """
    Пытается распарсить дату из Google Sheets.
    Ожидаем в основном формат dd.mm.yyyy.
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    date_formats = [
        "%d.%m.%Y",
        "%d.%m.%y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue

    return None


def should_skip_sheet(sheet_name: str) -> bool:
    """
    Игнорируем месячные сводки и базы.
    Новые менеджеры подхватятся автоматически, если у них будет свой лист.
    """
    name = sheet_name.strip().lower()

    for keyword in SKIP_SHEET_KEYWORDS:
        if keyword in name:
            return True

    return False


def get_sales_bot_data():
    """
    Возвращает все строки из Sales-Bot, кроме заголовка.
    """
    data = sales_bot_sheet.get_all_values()

    if len(data) <= 1:
        return []

    return data[1:]


def get_today_sales_rows():
    """
    Читает все нужные таблицы и собирает продажи за сегодня.
    Возвращает список словарей:
    {
        "project": "...",
        "manager": "...",
        "amount": 123000
    }
    """
    today = datetime.now(ZoneInfo(TIMEZONE)).date()
    result = []

    for source in TODAY_SOURCES:
        spreadsheet = client.open_by_key(source["spreadsheet_id"])
        worksheets = spreadsheet.worksheets()

        for ws in worksheets:
            if should_skip_sheet(ws.title):
                continue

            values = ws.get_all_values()

            # В листах МОПов:
            # 1-я строка = итог сверху
            # 2-я строка = заголовки
            # 3-я и далее = данные
            if len(values) < 3:
                continue

            data_rows = values[2:]

            for row in data_rows:
                if len(row) <= max(source["date_col_index"], source["amount_col_index"]):
                    continue

                sale_date = parse_sheet_date(row[source["date_col_index"]])
                if sale_date != today:
                    continue

                amount = parse_amount(row[source["amount_col_index"]])
                if amount <= 0:
                    continue

                result.append(
                    {
                        "project": source["project"],
                        "manager": ws.title.strip(),
                        "amount": amount,
                    }
                )

    return result


# =========================
# СТАРЫЕ КОМАНДЫ
# =========================
@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "Бот работает.\n\n"
        "Команды:\n"
        "/top5 — топ 5 менеджеров\n"
        "/topall — весь рейтинг менеджеров\n"
        "/topteam — топ команд\n"
        "/today — кто сколько занес сегодня\n"
        "/todayteam — сколько занесли команды сегодня"
    )


@dp.message(Command("top5"))
async def top5_handler(message: Message):
    try:
        data = get_sales_bot_data()
        managers = []

        for row in data:
            if len(row) < 4:
                continue

            name = row[0].strip()
            amount = parse_amount(row[3])

            if not name:
                continue

            managers.append((name, amount))

        managers.sort(key=lambda x: x[1], reverse=True)

        if not managers:
            await message.answer("Нет данных по менеджерам.")
            return

        text = "Топ 5 менеджеров:\n\n"

        for i, (name, amount) in enumerate(managers[:5], start=1):
            text += f"{i}. {name} — {format_amount(amount)}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /top5: {str(e)}")


@dp.message(Command("topall"))
async def topall_handler(message: Message):
    try:
        data = get_sales_bot_data()
        managers = []

        for row in data:
            if len(row) < 4:
                continue

            name = row[0].strip()
            amount = parse_amount(row[3])

            if not name:
                continue

            managers.append((name, amount))

        managers.sort(key=lambda x: x[1], reverse=True)

        if not managers:
            await message.answer("Нет данных по менеджерам.")
            return

        text = "Общий рейтинг менеджеров:\n\n"

        for i, (name, amount) in enumerate(managers, start=1):
            text += f"{i}. {name} — {format_amount(amount)}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /topall: {str(e)}")


@dp.message(Command("topteam"))
async def topteam_handler(message: Message):
    try:
        data = get_sales_bot_data()
        teams = {}

        for row in data:
            if len(row) < 4:
                continue

            team = row[1].strip()
            amount = parse_amount(row[3])

            if not team:
                continue

            teams[team] = teams.get(team, 0) + amount

        sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

        if not sorted_teams:
            await message.answer("Нет данных по командам.")
            return

        text = "Топ команд:\n\n"

        for i, (team, amount) in enumerate(sorted_teams, start=1):
            text += f"{i}. {team} — {format_amount(amount)}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /topteam: {str(e)}")


# =========================
# НОВЫЕ КОМАНДЫ
# =========================
@dp.message(Command("today"))
async def today_handler(message: Message):
    try:
        sales = get_today_sales_rows()

        if not sales:
            today_str = datetime.now(ZoneInfo(TIMEZONE)).strftime("%d.%m.%Y")
            await message.answer(f"Сегодня ({today_str}) пока оплат нет.")
            return

        grouped = {}

        for item in sales:
            key = (item["manager"], item["project"])
            grouped[key] = grouped.get(key, 0) + item["amount"]

        sorted_rows = sorted(grouped.items(), key=lambda x: x[1], reverse=True)

        today_str = datetime.now(ZoneInfo(TIMEZONE)).strftime("%d.%m.%Y")
        total_amount = sum(amount for _, amount in sorted_rows)

        text = f"Сегодня занесли ({today_str}):\n\n"

        for i, ((manager, project), amount) in enumerate(sorted_rows, start=1):
            text += f"{i}. {manager} [{project}] — {format_amount(amount)}\n"

        text += f"\nИтого за сегодня: {format_amount(total_amount)}"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /today: {str(e)}")


@dp.message(Command("todayteam"))
async def todayteam_handler(message: Message):
    try:
        sales = get_today_sales_rows()

        if not sales:
            today_str = datetime.now(ZoneInfo(TIMEZONE)).strftime("%d.%m.%Y")
            await message.answer(f"Сегодня ({today_str}) по командам оплат пока нет.")
            return

        teams = {}

        for item in sales:
            teams[item["project"]] = teams.get(item["project"], 0) + item["amount"]

        sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

        today_str = datetime.now(ZoneInfo(TIMEZONE)).strftime("%d.%m.%Y")
        total_amount = sum(amount for _, amount in sorted_teams)

        text = f"Сегодня по командам ({today_str}):\n\n"

        for i, (project, amount) in enumerate(sorted_teams, start=1):
            text += f"{i}. {project} — {format_amount(amount)}\n"

        text += f"\nИтого за сегодня: {format_amount(total_amount)}"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /todayteam: {str(e)}")


# =========================
# ЗАПУСК
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
