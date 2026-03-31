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

# ===== BOT =====
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ===== GOOGLE =====
scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

google_creds = json.loads(GOOGLE_CREDENTIALS_RAW)

creds = Credentials.from_service_account_info(
    google_creds,
    scopes=scope
)

client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# ===== HANDLERS =====
@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Бот работает")

@dp.message(Command("test"))
async def test_handler(message: Message):
    try:
        data = sheet.get_all_values()
        await message.answer(f"Строк в таблице: {len(data)}")
    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")

# ===== START =====
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())