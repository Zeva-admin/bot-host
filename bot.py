import asyncio
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

# ВСТАВЬ СЮДА ТОКЕН
TOKEN = "8548607252:AAFFzd__XttKj6GxcFh_IygRQbgTu7-xL68"

bot = Bot(token=TOKEN)
dp = Dispatcher()

bot_start_time = datetime.now()


def safe(value, default="нет"):
    return value if value not in [None, ""] else default


def format_user_info(message: Message, bot_info):
    user = message.from_user
    chat = message.chat
    uptime = datetime.now() - bot_start_time

    full_name = f"{safe(user.first_name, '')} {safe(user.last_name, '')}".strip()
    if not full_name:
        full_name = "нет"

    text = (
        f"🚀 <b>Полная информация:</b>\n\n"

        f"👤 <b>ДАННЫЕ ПОЛЬЗОВАТЕЛЯ</b>\n"
        f"🆔 User ID: <code>{user.id}</code>\n"
        f"👤 Username: {('@' + user.username) if user.username else 'нет'}\n"
        f"📛 Имя: {safe(user.first_name)}\n"
        f"📛 Фамилия: {safe(user.last_name)}\n"
        f"📝 Полное имя: {full_name}\n"
        f"🌐 Язык: {safe(user.language_code, 'неизвестно')}\n"
        f"🤖 Это бот: {'Да' if user.is_bot else 'Нет'}\n"
        f"💎 Premium: {'Да' if getattr(user, 'is_premium', False) else 'Нет'}\n"
        f"🔗 Упоминание: <a href='tg://user?id={user.id}'>{safe(user.first_name)}</a>\n\n"

        f"💬 <b>ДАННЫЕ ЧАТА</b>\n"
        f"🆔 Chat ID: <code>{chat.id}</code>\n"
        f"📂 Тип чата: {safe(chat.type)}\n"
        f"🏷 Название чата: {safe(chat.title)}\n"
        f"🔗 Username чата: {('@' + chat.username) if getattr(chat, 'username', None) else 'нет'}\n"
        f"📝 Описание чата: {safe(getattr(chat, 'bio', None))}\n\n"

        f"📨 <b>ДАННЫЕ СООБЩЕНИЯ</b>\n"
        f"🆔 Message ID: <code>{message.message_id}</code>\n"
        f"📅 Дата сообщения: {message.date}\n"
        f"✍️ Текст: {safe(message.text)}\n\n"

        f"🤖 <b>ДАННЫЕ БОТА</b>\n"
        f"🆔 Bot ID: <code>{bot_info.id}</code>\n"
        f"👤 Имя бота: {safe(bot_info.first_name)}\n"
        f"🔗 Username бота: @{safe(bot_info.username)}\n"
        f"✅ Может писать первым: {'Да' if getattr(bot_info, 'can_join_groups', True) else 'Неизвестно'}\n\n"

        f"⏱ <b>СИСТЕМА</b>\n"
        f"🕒 Бот запущен: {bot_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"⌛ Аптайм: {str(uptime).split('.')[0]}\n"
    )

    return text


@dp.message(CommandStart())
async def start_handler(message: Message):
    bot_info = await bot.get_me()
    text = format_user_info(message, bot_info)
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(F.text)
async def all_messages_handler(message: Message):
    bot_info = await bot.get_me()
    text = format_user_info(message, bot_info)
    await message.answer(
        "Ты отправил сообщение, вот обновлённая информация:\n\n" + text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )


async def main():
    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())