import asyncio
import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

TOKEN = "TOKEN_FROM_RENDER"
SPREADSHEET_ID = "19psuYsJk6s6Si-9vh7LaAvHp5LdmpiQvHFJVHiCwmnc"
SHEET_NAME = "Sales-Bot"

bot = Bot(token=TOKEN)
dp = Dispatcher()

scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_file(
    "sales-bot-491907-364b6b0fd523.json",
    scopes=scope
)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


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
    data = sheet.get_all_values()[1:]

    managers = []

    for row in data:
        name = row[0].strip()
        amount_text = str(row[3]).replace(" ", "").replace("\xa0", "").replace(",", "").strip()

        try:
            amount = int(amount_text)
        except:
            amount = 0

        managers.append((name, amount))

    managers.sort(key=lambda x: x[1], reverse=True)

    text = "Топ 5 менеджеров:\n\n"

    for i, (name, amount) in enumerate(managers[:5], start=1):
        text += f"{i}. {name} — {amount:,}\n".replace(",", " ")

    await message.answer(text)


@dp.message(Command("topall"))
async def topall_handler(message: Message):
    data = sheet.get_all_values()[1:]

    managers = []

    for row in data:
        name = row[0].strip()
        amount_text = str(row[3]).replace(" ", "").replace("\xa0", "").replace(",", "").strip()

        try:
            amount = int(amount_text)
        except:
            amount = 0

        managers.append((name, amount))

    managers.sort(key=lambda x: x[1], reverse=True)

    text = "Общий рейтинг менеджеров:\n\n"

    for i, (name, amount) in enumerate(managers, start=1):
        text += f"{i}. {name} — {amount:,}\n".replace(",", " ")

    await message.answer(text)


@dp.message(Command("topteam"))
async def topteam_handler(message: Message):
    data = sheet.get_all_values()[1:]

    teams = {}

    for row in data:
        team = row[1].strip()
        amount_text = str(row[3]).replace(" ", "").replace("\xa0", "").replace(",", "").strip()

        try:
            amount = int(amount_text)
        except:
            amount = 0

        if team in teams:
            teams[team] += amount
        else:
            teams[team] = amount

    sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)

    text = "Топ команд:\n\n"

    for i, (team, amount) in enumerate(sorted_teams, start=1):
        text += f"{i}. {team} — {amount:,}\n".replace(",", " ")

    await message.answer(text)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())