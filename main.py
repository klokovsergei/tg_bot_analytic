import asyncio
import logging
import sys
from copy import deepcopy

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config_data.config import Config, load_config
from handlers import user_handlers, admin_handlers
from keyboards.main_menu import set_main_menu
from database.database import users_db
from services.storage import load_users_db

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(filename)s:%(lineno)d #%(levelname)-8s '
               '[%(asctime)s] - %(name)s - %(message)s')

    logger.info('Starting bot')

    config: Config = load_config('.env')

    bot = Bot(
        token=config.tg_bot.token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    dp['support_chats'] = config.tg_bot.support_channel_ids

    await set_main_menu(bot)

    dp.include_router(admin_handlers.router)
    dp.include_router(user_handlers.router)

    logger.info('Обнуляем очередь апдейтов')
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info('Запускаем polling')

    await dp.start_polling(bot)


asyncio.run(main())
