"""Check bot quiz entry/back navigation without contacting Telegram or AI."""

from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, patch

from test_webapp import BOT_TOKEN
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.methods import EditMessageText, SendMessage

from handlers import user


class BotNavigationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.dispatcher = Dispatcher(storage=MemoryStorage())
        cls.dispatcher.include_router(user.router)

    async def asyncSetUp(self):
        self.bot = Bot(BOT_TOKEN)
        self.person = types.User(id=101, is_bot=False, first_name="Ali")
        self.context = self.dispatcher.fsm.get_context(bot=self.bot, chat_id=101, user_id=101)
        await self.context.clear()
        self.patches = [
            patch.object(self.bot.session, "make_request", AsyncMock(return_value=self.message())),
            patch("handlers.user._get_ui_lang", AsyncMock(return_value="uz")),
            patch("handlers.user._ensure_subscribed", AsyncMock(return_value=True)),
            patch("handlers.user.get_manual_quiz_draft", AsyncMock(return_value=None)),
            patch("handlers.user._persist_manual_draft", AsyncMock()),
            patch("handlers.user.AI_ENABLED", True),
        ]
        values = [item.start() for item in self.patches]
        self.requests, self.draft, self.persist = values[0], values[3], values[4]

    async def asyncTearDown(self):
        for item in reversed(self.patches):
            item.stop()
        await self.bot.session.close()

    def message(self, text=None):
        return types.Message(message_id=1, date=datetime.now(timezone.utc), chat=types.Chat(id=101, type="private"), from_user=self.person, text=text)

    async def click(self, action, *, group=False):
        message = self.message()
        if group:
            message = message.model_copy(update={"chat": types.Chat(id=-123, type="group")})
        call = types.CallbackQuery(id="navigation", from_user=self.person, chat_instance="private", message=message, data=action)
        await self.dispatcher.feed_update(self.bot, types.Update(update_id=1, callback_query=call))

    async def send(self, text):
        await self.dispatcher.feed_update(self.bot, types.Update(update_id=2, message=self.message(text)))

    def last_buttons(self):
        messages = [call.args[1] for call in self.requests.await_args_list if isinstance(call.args[1], (SendMessage, EditMessageText))]
        return [button.callback_data for row in messages[-1].reply_markup.inline_keyboard for button in row]

    async def assert_back_returns_menu(self):
        self.assertIn("quiz_create_back", self.last_buttons())
        await self.click("quiz_create_back")
        self.assertIsNone(await self.context.get_state())
        self.assertEqual(await self.context.get_data(), {})
        self.assertIn("menu_newquiz", self.last_buttons())
        self.assertIn("menu_topic", self.last_buttons())

    async def test_new_quiz_button_and_back(self):
        await self.click("menu_newquiz")
        self.assertEqual(await self.context.get_state(), user.ManualQuizStates.title.state)
        await self.assert_back_returns_menu()

    async def test_topic_button_and_back(self):
        await self.click("menu_topic")
        self.assertEqual(await self.context.get_state(), user.AIQuizStates.choose_topic.state)
        self.assertTrue((await self.context.get_data())["ai_session_id"])
        await self.assert_back_returns_menu()

    async def test_commands_have_same_back_navigation(self):
        for command in ("/newquiz", "/topic"):
            with self.subTest(command=command):
                await self.send(command)
                await self.assert_back_returns_menu()

    async def test_existing_draft_choice_has_back(self):
        self.draft.return_value = {"state": user.ManualQuizStates.question.state}
        await self.click("menu_newquiz")
        self.assertIn("m_draft:resume", self.last_buttons())
        await self.assert_back_returns_menu()

    async def test_back_preserves_manual_work_before_clearing_state(self):
        await self.click("menu_newquiz")
        await self.context.update_data(title="My draft", questions=[{"question": "2 + 2?"}])
        saved = []

        async def preserve(state, **kwargs):
            saved.append(await state.get_data())

        self.persist.side_effect = preserve
        await self.assert_back_returns_menu()
        self.assertEqual(saved[-1]["title"], "My draft")
        self.assertEqual(len(saved[-1]["questions"]), 1)

    async def test_invalid_inputs_keep_back_button(self):
        for action in ("menu_newquiz", "menu_topic"):
            with self.subTest(action=action):
                await self.click(action)
                await self.send("   ")
                await self.assert_back_returns_menu()

    async def test_group_back_does_not_clear_private_flow(self):
        await self.click("menu_topic")
        await self.click("quiz_create_back", group=True)
        self.assertEqual(await self.context.get_state(), user.AIQuizStates.choose_topic.state)


if __name__ == "__main__":
    unittest.main()
