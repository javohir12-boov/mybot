"""Support delivery and real bot routing; Telegram calls are mocked."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from test_webapp import BOT_TOKEN
from aiogram import Bot, Dispatcher, types
from aiogram.methods import SendMessage
from aiogram.fsm.storage.memory import MemoryStorage

from handlers import support
from middlewares.security import SecurityMiddleware
from services.support_service import SupportError, SupportService


class SupportServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_delivery_succeeds_and_shared_cooldown_blocks_bot(self):
        service = SupportService()
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=[RuntimeError(), None]))
        with patch("services.support_service.ADMIN_IDS", [999, 998, 999]):
            await service.send(bot, {"id": 101, "first_name": "<Ali>", "username": "ali"}, "A & B", source="web")
            self.assertEqual(bot.send_message.await_count, 2)
            self.assertIn("&lt;Ali&gt;", bot.send_message.await_args.args[1])
            self.assertIn("A &amp; B", bot.send_message.await_args.args[1])
            with self.assertRaises(SupportError) as raised:
                await service.send(bot, {"id": 101}, "Again", source="bot")
            self.assertEqual(raised.exception.code, "support_rate_limit")

    async def test_concurrent_requests_only_deliver_once(self):
        service = SupportService()
        started, finish = asyncio.Event(), asyncio.Event()

        async def delivery(*args, **kwargs):
            started.set()
            await finish.wait()

        bot = SimpleNamespace(send_message=AsyncMock(side_effect=delivery))
        with patch("services.support_service.ADMIN_IDS", [999]):
            first = asyncio.create_task(service.send(bot, {"id": 101}, "First", source="web"))
            await started.wait()
            try:
                with self.assertRaises(SupportError):
                    await service.send(bot, {"id": 101}, "Second", source="bot")
            finally:
                finish.set()
                await first
            bot.send_message.assert_awaited_once()

    async def test_no_admins_reports_failure(self):
        with patch("services.support_service.ADMIN_IDS", []), self.assertRaises(SupportError) as raised:
            await SupportService().send(SimpleNamespace(), {"id": 101}, "Help", source="bot")
        self.assertEqual(raised.exception.code, "support_unavailable")


class BotSupportTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.dispatcher = Dispatcher(storage=MemoryStorage())
        cls.dispatcher.include_router(support.router)

    async def asyncSetUp(self):
        self.bot = Bot(BOT_TOKEN)
        self.user = types.User(id=101, is_bot=False, first_name="Ali", username="ali")
        self.context = self.dispatcher.fsm.get_context(bot=self.bot, chat_id=101, user_id=101)
        await self.context.clear()
        response = types.Message(message_id=100, date=datetime.now(timezone.utc), chat=types.Chat(id=101, type="private"))
        self.request_patch = patch.object(self.bot.session, "make_request", AsyncMock(return_value=response))
        self.requests = self.request_patch.start()
        self.language_patch = patch("handlers.support.language", AsyncMock(return_value="uz"))
        self.language_patch.start()
        self.service_patch = patch("handlers.support.support_service", SupportService())
        self.service_patch.start()
        self.admin_patch = patch("services.support_service.ADMIN_IDS", [999])
        self.admin_patch.start()

    async def asyncTearDown(self):
        self.admin_patch.stop()
        self.service_patch.stop()
        self.language_patch.stop()
        self.request_patch.stop()
        await self.bot.session.close()

    def message(self, text=None, **kwargs):
        return types.Message(message_id=1, date=datetime.now(timezone.utc), chat=types.Chat(id=101, type="private"), from_user=self.user, text=text, **kwargs)

    async def dispatch(self, *, message=None, callback=None):
        await self.dispatcher.feed_update(self.bot, types.Update(update_id=1, message=message, callback_query=callback))

    def sent_messages(self):
        return [call.args[1] for call in self.requests.await_args_list if isinstance(call.args[1], SendMessage)]

    async def test_help_command_and_text_reach_admin_then_clear_state(self):
        await self.dispatch(message=self.message("/help"))
        self.assertEqual(await self.context.get_state(), support.SupportStates.message.state)
        await self.dispatch(message=self.message("Testlarim ochilmayapti"))
        self.assertIsNone(await self.context.get_state())
        messages = self.sent_messages()
        self.assertEqual([m.chat_id for m in messages], [101, 999, 101])
        self.assertIn("Bot", messages[1].text)
        self.assertIn("Testlarim ochilmayapti", messages[1].text)
        self.assertIn("adminga yuborildi", messages[2].text)

    async def test_menu_button_opens_support(self):
        callback = types.CallbackQuery(id="help", from_user=self.user, chat_instance="private", message=self.message(), data="menu_support")
        await self.dispatch(callback=callback)
        self.assertEqual(await self.context.get_state(), support.SupportStates.message.state)

    async def test_nontext_and_failed_delivery_keep_composer_open(self):
        await self.dispatch(message=self.message("/help"))
        await self.dispatch(message=self.message(sticker=types.Sticker(file_id="sticker", file_unique_id="unique", type="regular", width=10, height=10, is_animated=False, is_video=False)))
        self.assertEqual(await self.context.get_state(), support.SupportStates.message.state)
        self.assertIn("matnli xabar", self.sent_messages()[-1].text)
        self.requests.side_effect = [RuntimeError("offline"), self.message()]
        await self.dispatch(message=self.message("Help"))
        self.assertEqual(await self.context.get_state(), support.SupportStates.message.state)
        self.assertIn("yuborib bo‘lmadi", self.sent_messages()[-1].text)

    async def test_cancel_and_stale_cancel_do_not_interrupt_other_flows(self):
        await self.dispatch(message=self.message("/help"))
        await self.dispatch(message=self.message("/cancel"))
        self.assertIsNone(await self.context.get_state())
        await self.context.set_state("OtherFlow:message")
        callback = types.CallbackQuery(id="cancel", from_user=self.user, chat_instance="private", message=self.message(), data="support_cancel")
        await self.dispatch(callback=callback)
        self.assertEqual(await self.context.get_state(), "OtherFlow:message")

    async def test_group_help_is_not_handled(self):
        message = self.message("/help").model_copy(update={"chat": types.Chat(id=-123, type="group")})
        await self.dispatch(message=message)
        self.requests.assert_not_awaited()

    async def test_support_flag_allows_help_when_channel_check_fails(self):
        middleware = SecurityMiddleware(required_channel_enabled=True, required_channel="@test_channel")
        handler = AsyncMock()
        with patch.object(middleware, "_refresh_user", AsyncMock()), patch("middlewares.security.record_user_activity", AsyncMock()), patch.object(middleware, "_is_subscribed", AsyncMock(return_value=False)) as membership:
            await middleware(handler, self.message("/help"), {"event_from_user": self.user, "bot": self.bot, "handler": SimpleNamespace(flags={"support": True})})
            handler.assert_awaited_once()
            membership.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
