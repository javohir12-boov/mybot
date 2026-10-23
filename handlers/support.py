"""Private bot support flow, registered before the generic quiz handlers."""

from aiogram import Bot, F, Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from handlers.utils.i18n import t
from services.database import get_or_create_user_settings
from services.support_service import SupportError, support_service

router = Router(name="support")
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


class SupportStates(StatesGroup):
    message = State()


async def language(user_id):
    return (await get_or_create_user_settings(user_id)).get("ui_lang", "uz")


def cancel_keyboard(lang):
    return types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text=t(lang, "support_cancel"), callback_data="support_cancel")
    ]])


@router.message(Command("help", "support", "yordam"), flags={"support": True})
async def start_support(message: types.Message, state: FSMContext):
    lang = await language(message.from_user.id)
    await state.clear()
    await state.set_state(SupportStates.message)
    await message.answer(t(lang, "support_prompt"), reply_markup=cancel_keyboard(lang))


@router.callback_query(F.data == "menu_support", flags={"support": True})
async def open_support(call: types.CallbackQuery, state: FSMContext):
    lang = await language(call.from_user.id)
    await state.clear()
    await state.set_state(SupportStates.message)
    await call.answer()
    await call.message.answer(t(lang, "support_prompt"), reply_markup=cancel_keyboard(lang))


@router.message(SupportStates.message, Command("cancel", "menu"), flags={"support": True})
@router.callback_query(F.data == "support_cancel", flags={"support": True})
async def cancel_support(event: types.Message | types.CallbackQuery, state: FSMContext):
    lang = await language(event.from_user.id)
    # Old cancel buttons must not clear an unrelated quiz/payment state.
    if isinstance(event, types.CallbackQuery):
        await event.answer()
        if await state.get_state() != SupportStates.message.state:
            return
        message = event.message
    else:
        message = event
    await state.clear()
    from handlers.user import _kb_main_menu

    await message.answer(t(lang, "support_cancelled"), reply_markup=_kb_main_menu(lang, user_id=event.from_user.id))


@router.message(SupportStates.message, ~F.text.startswith("/"), flags={"support": True})
async def send_support(message: types.Message, state: FSMContext, bot: Bot):
    lang = await language(message.from_user.id)
    user = message.from_user.model_dump(include={"id", "first_name", "last_name", "username"})
    try:
        await support_service.send(bot, user, message.text, source="bot")
    except SupportError as exc:
        await message.answer(t(lang, exc.code), reply_markup=cancel_keyboard(lang))
        return
    await state.clear()
    from handlers.user import _kb_main_menu

    await message.answer(t(lang, "support_sent"), reply_markup=_kb_main_menu(lang, user_id=message.from_user.id))
