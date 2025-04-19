import aiofiles
from aiogram import Bot


async def download_document(file: tuple[str, str], user_id: int, bot: Bot) -> str:
    file_info = await bot.get_file(file[1])

    telegram_path = file_info.file_path
    local_filename = f"./temp/{user_id}_{file[0]}"

    file = await bot.download_file(telegram_path)

    async with aiofiles.open(local_filename, 'wb') as out_file:
        await out_file.write(file.read())

    return local_filename