import asyncio

from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message

from lexicon.lexicon import LEXICON
from services.storage import save_users_db
from database.database import users_db

router = Router()


@router.message(Command(commands='check'))
async def process_stop_command(message: Message, admin_list):
    if message.chat.id not in admin_list:
        return await message.answer(LEXICON['not admin'])
    text = f'Ботом пользуются:\n\n{set(users_db.keys())}'
    await message.answer(text)


@router.message(Command(commands='stop'))
async def process_stop_command(message: Message, bot: Bot, admin_list):
    if message.chat.id not in admin_list:
        return await message.answer(LEXICON['not admin'])
    await save_users_db(users_db)
    await message.answer(LEXICON[message.text])
    await bot.session.close()
    asyncio.get_event_loop().stop()
