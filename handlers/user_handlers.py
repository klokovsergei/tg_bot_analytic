from copy import deepcopy
import os
from datetime import datetime

from aiogram import Router, F, Bot, types, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery

from database.database import users_db, user_dict_template
from keyboards.reports_kb import create_reports_keyboard
from lexicon.lexicon import LEXICON
from services.tg_services import download_document
from services.ym_excel_transformer import ym_excel_transformer

router = Router()


@router.message(CommandStart())
async def process_start_command(message: Message, support_chats):
    await message.answer(LEXICON[message.text])
    if message.from_user.id not in users_db:
        users_db[message.from_user.id] = deepcopy(user_dict_template)
        bot_name = await message.bot.get_me()
        for chat_id in support_chats:
            await message.bot.send_message(
                chat_id=chat_id,
                text=LEXICON['new_user'].format(
                    bot_name=bot_name.username,
                    username=message.from_user.username,
                    user_id=message.from_user.id,
                    time_join=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                )
            )


@router.message(Command(commands='help'))
async def process_help_command(message: Message):
    await message.answer(LEXICON[message.text])


@router.message(Command(commands='report_info'))
async def process_report_info_command(message: Message):
    await message.answer(LEXICON[message.text])


@router.message(F.document)
async def process_report_message(message: Message):
    file_name = message.document.file_name
    file_id = message.document.file_id
    users_db[message.from_user.id]['temp_file'].append((file_name, file_id))
    await message.answer(
        text=LEXICON['file'].format(file_name=file_name),
        reply_markup=create_reports_keyboard(
            'ya_m_orders_report'
        ))


@router.callback_query(F.data == 'ya_m_orders_report')
async def process_ya_report_press(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    if not len(users_db[callback.from_user.id]['temp_file']):
        return
    file_info = users_db[callback.from_user.id]['temp_file'].pop()
    file_path = await download_document(file_info, callback.from_user.id, bot)
    msg = await callback.message.edit_text(LEXICON['wait'])
    answer = await ym_excel_transformer(file_path)
    if answer:
        await msg.edit_text(answer)
    else:
        await msg.edit_text(LEXICON['ready'])

    await bot.send_document(
        chat_id=callback.message.chat.id,
        document=types.FSInputFile(file_path)
    )

    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Произошла ошибка при удалении файла: {e}")

    users_db[callback.from_user.id]['user_usage_ym_transformer'].append(file_info)


@router.message()
async def process_forward_to_support(message: Message, support_chats):
    await message.answer(LEXICON['to_support'])

    bot_name = await message.bot.get_me()
    for chat_id in support_chats:
        await message.bot.send_message(
            chat_id=chat_id,
            text=LEXICON['for_support'].format(
                bot_name=bot_name.username,
                username=message.from_user.username,
                user_id=message.from_user.id,
                time_join=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        )
        await message.bot.forward_message(
            chat_id=chat_id,  # ID чата поддержки
            from_chat_id=message.chat.id,  # ID чата пользователя
            message_id=message.message_id  # ID пересылаемого сообщения
        )
