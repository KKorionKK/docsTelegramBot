from aiogram import Bot, types
from aiogram.dispatcher.dispatcher import Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
import asyncio
import logging
import json

from bot.config import BaseConfig
from bot.database import Database
from bot.text import help_error_message, hello_message

logging.basicConfig(level=logging.INFO)

config: dict = BaseConfig.get_config()
TOKEN = config.get("TOKEN")

session = AiohttpSession()

bot = Bot(TOKEN, session=session)
dp = Dispatcher()

db = Database()


@dp.message(Command("start"))
async def message_handler(message: types.Message):
    return await message.answer(hello_message)


@dp.message()
async def json_message_handler(message: types.Message):
    try:
        json_object = json.loads(message.text)
        try:
            group_type = json_object.get("group_type")
            dt_from = json_object.get("dt_from")
            dt_upto = json_object.get("dt_upto")
            result = await db.get_aggregation(group_type, dt_from, dt_upto)
            return await message.answer(json.dumps(result))
        except AttributeError as e:
            print(e)
            return await message.reply(help_error_message)
    except json.JSONDecodeError as e:
        print(e)
        return await message.reply(help_error_message)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
