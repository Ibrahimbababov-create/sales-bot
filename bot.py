import os
import json
import asyncio
import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ===== ПЕРЕМЕННЫЕ =====
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_CREDENTIALS_RAW = os.getenv("GOOGLE_CREDENTIALS_JSON")

SPREADSHEET_ID = "19psuYsJk6s6Si-9vh7LaAvHp5LdmpiQvHFJVHiCwmnc"
SHEET_NAME = "Sales-Bot"

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

if not GOOGLE_CREDENTIALS_RAW:
    raise ValueError("Не найден GOOGLE_CREDENTIALS_JSON в переменных окружения")

# ===== BOT =====
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ===== GOOGLE SHEETS =====
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

google_creds = json.loads(GOOGLE_CREDENTIALS_RAW)

creds = Credentials.from_service_account_info(
    google_creds,
    scopes=scope
)

client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def parse_amount(value: str) -> int:
    """
    Преобразует сумму из таблицы в число.
    Убирает пробелы, неразрывные пробелы, запятые.
    Если значение кривое, возвращает 0.
    """
    amount_text = str(value).replace(" ", "").replace("\xa0", "").replace(",", "").strip()

    try:
        return int(amount_text)
    except Exception:
        return 0


def get_sheet_data():
    """
    Возвращает все строки таблицы, кроме заголовка.
    """
    data = sheet.get_all_values()

    if len(data) <= 1:
        return []

    return data[1:]


# ===== HANDLERS =====
@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "Бот работает.\n\n"
        "Команды:\n"
        "/top5 — топ 5 менеджеров\n"
        "/topall — весь рейтинг менеджеров\n"
        "/topteam — топ команд"
    )


@dp.message(Command("top5"))
async def top5_handler(message: Message):
    try:
        data = get_sheet_data()
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
            text += f"{i}. {name} — {amount:,}\n".replace(",", " ")

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /top5: {str(e)}")


@dp.message(Command("topall"))
async def topall_handler(message: Message):
    try:
        data = get_sheet_data()
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
            text += f"{i}. {name} — {amount:,}\n".replace(",", " ")

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /topall: {str(e)}")


@dp.message(Command("topteam"))
async def topteam_handler(message: Message):
    try:
        data = get_sheet_data()
        teams = {}

        for row in data:
            if len(row) < 4:
                continue

            team = row[1].strip()
            amount = parse_amount(row[3])

            if not team:
                continue

            if team in teams:
                teams[team] += amount
            else:
                teams[team] = amount

        sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

        if not sorted_teams:
            await message.answer("Нет данных по командам.")
            return

        text = "Топ команд:\n\n"

        for i, (team, amount) in enumerate(sorted_teams, start=1):
            text += f"{i}. {team} — {amount:,}\n".replace(",", " ")

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка в /topteam: {str(e)}")


# ===== START =====
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())