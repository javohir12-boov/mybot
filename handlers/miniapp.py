"""Entry points for the Telegram Mini App; existing chat flows stay available."""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aiogram import Bot, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject

from config import WEBAPP_URL

router = Router(name="miniapp")


@router.message(CommandStart(deep_link=True), F.text.regexp(r"^/start(?:@\w+)?\s+room_[a-f0-9]{32}$"))
async def open_shared_room(message: types.Message, command: CommandObject) -> None:
    if message.chat.type != "private":
        return
    if not WEBAPP_URL.startswith("https://"):
        await message.answer("Web ilova manzili hali sozlanmagan.")
        return
    room_id = (command.args or "").strip().removeprefix("room_")
    parts = urlsplit(WEBAPP_URL)
    query = [(key, value) for key, value in parse_qsl(parts.query) if key != "room"]
    target = urlunsplit(parts._replace(query=urlencode([*query, ("room", room_id)])))
    await message.answer("Birga test ishlash uchun xonaga kiring:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="Test xonasiga kirish", web_app=types.WebAppInfo(url=target))
    ]]))


@router.message(CommandStart(deep_link=True), F.text.regexp(r"^/start(?:@\w+)?\s+webapp$"))
@router.message(Command("app"))
async def open_app(message: types.Message) -> None:
    if message.chat.type != "private":
        return
    if not WEBAPP_URL.startswith("https://"):
        await message.answer("Web ilova manzili hali sozlanmagan. Administrator WEBAPP_URL ni sozlashi kerak.")
        return
    await message.answer(
        "Test yaratish uchun web ilovani oching:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="Web ilovani ochish", web_app=types.WebAppInfo(url=WEBAPP_URL))
        ]]),
    )


@router.message(CommandStart(deep_link=True), F.text.regexp(r"^/start(?:@\w+)?\s+webpremium$"))
async def open_premium(message: types.Message, bot: Bot) -> None:
    # Reuse the current payment/receipt and administrator approval flow.
    from handlers.user import cmd_premium

    await cmd_premium(message, bot)
