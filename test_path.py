import os
from dotenv import load_dotenv
from telegram import Bot
import asyncio

load_dotenv()

async def test():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    me = await bot.get_me()
    print(f"Bot: @{me.username}")
    # Try a fake file_id to see the error behavior
    try:
        file = await bot.get_file("BAACAgUAAx0CZtVppAACAQ9nvy1S3A4xVj3cW6grHRIF0pHIZAACbBgAAjVxIVfXnl4lYqaz-wE")
        print(f"file_path: {repr(file.file_path)}")
    except Exception as e:
        print(f"Expected error: {e}")

asyncio.run(test())
