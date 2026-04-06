import asyncio
import logging
import os

# Force NO-AI mode without requiring .env edits.
os.environ["BOT_MODE"] = "noai"

from aiogram import Bot, Dispatcher, types  # noqa: E402
from aiogram.fsm.storage.memory import MemoryStorage  # noqa: E402

from config import BOT_TOKEN  # noqa: E402
from handlers.user import router as user_router  # noqa: E402
from services.database import init_db  # noqa: E402


logging.basicConfig(level=logging.INFO)


async def _setup_commands(bot: Bot) -> None:
    commands = [
        types.BotCommand(command="menu", description="Menu"),
        types.BotCommand(command="til", description="Interface language"),
        types.BotCommand(command="newquiz", description="Create quiz manually"),
        types.BotCommand(command="mytests", description="My quizzes"),
        types.BotCommand(command="cancel", description="Stop active quiz"),
    ]
    try:
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllGroupChats())
        await bot.set_my_commands(commands)
    except Exception as exc:
        logging.warning("Bot commands set failed: %s", exc)
    try:
        await bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())
    except Exception:
        pass


async def main() -> None:
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    await init_db()
    await _setup_commands(bot)
    dp.include_router(user_router)
    logging.info("Bot started (NO-AI mode)...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
