"""Activity and broadcast checks; all Telegram methods are mocked."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

# Configure the isolated database before importing the application.
from test_webapp import db
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.methods import CopyMessage

from services.broadcast_service import BroadcastBusy, BroadcastManager
from middlewares.security import SecurityMiddleware
from aiogram import types


class DatabaseCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async with db.engine.begin() as connection:
            await connection.run_sync(db.Base.metadata.drop_all)
            await connection.run_sync(db.Base.metadata.create_all)

    async def asyncTearDown(self):
        await db.engine.dispose()

    async def seed_users(self, count=5):
        old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        async with db.async_session() as session:
            session.add_all([db.User(id=101 + i, full_name=f"User {i}", created_at=old) for i in range(count)])
            await session.commit()


class ActivityTests(DatabaseCase):
    async def test_security_middleware_tracks_menu_and_callback_use(self):
        await self.seed_users(1)
        middleware = SecurityMiddleware(required_channel_enabled=False)
        user = types.User(id=101, is_bot=False, first_name="Ali")
        message = types.Message(message_id=1, date=datetime.now(timezone.utc), chat=types.Chat(id=101, type="private"), from_user=user, text="/menu")
        handler = AsyncMock()
        await middleware(handler, message, {"event_from_user": user})
        callback = types.CallbackQuery(id="callback", from_user=user, chat_instance="private", message=message, data="menu_upload")
        await middleware(handler, callback, {"event_from_user": user})
        self.assertEqual(handler.await_count, 2)
        self.assertEqual((await db.get_user_counts_summary())["active_users_last_30d"], 1)

    async def test_monthly_active_counts_distinct_users_in_rolling_window(self):
        await self.seed_users(6)
        now = datetime.now(timezone.utc)
        async with db.async_session() as session:
            session.add_all([
                db.UserActivity(user_id=102, last_seen_at=now.isoformat()),
                db.UserActivity(user_id=103, last_seen_at=(now - timedelta(days=10)).isoformat()),
                db.UserActivity(user_id=106, last_seen_at=(now - timedelta(days=40)).isoformat()),
                db.Quiz(id=1, creator_id=101, title="Old quiz"),
            ])
            recent_user = await session.get(db.User, 105)
            recent_user.created_at = (now - timedelta(days=5)).isoformat()
            await session.flush()
            session.add_all([
                db.QuizAttempt(user_id=104, quiz_id=1, completed_at=now.isoformat()),
                db.QuizAttempt(user_id=104, quiz_id=1, completed_at=now.isoformat()),
                db.QuizAttempt(user_id=102, quiz_id=1, completed_at=now.isoformat()),
            ])
            await session.commit()
        stats = await db.get_user_counts_summary()
        self.assertEqual(stats["total_users"], 6)
        self.assertEqual(stats["active_users_last_24h"], 2)
        self.assertEqual(stats["active_users_last_30d"], 4)
        self.assertEqual(stats["joined_last_30d"], 1)
        self.assertEqual(stats["attempts_last_24h"], 3)

    async def test_menu_activity_counts_without_quiz_attempts(self):
        await self.seed_users(1)
        self.assertEqual((await db.get_user_counts_summary())["active_users_last_30d"], 0)
        await db.record_user_activity(101)
        await db.record_user_activity(101)
        stats = await db.get_user_counts_summary()
        self.assertEqual(stats["active_users_last_24h"], 1)
        self.assertEqual(stats["active_users_last_30d"], 1)


class BroadcastTests(DatabaseCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.seed_users()
        self.manager = BroadcastManager(batch_size=2)
        self.manager._pause = AsyncMock(return_value=False)
        self.bot = SimpleNamespace(copy_message=AsyncMock())
        self.message = SimpleNamespace(chat=SimpleNamespace(id=999), message_id=55)
        self.status = SimpleNamespace(edit_text=AsyncMock())

    async def asyncTearDown(self):
        await self.manager.shutdown()
        await super().asyncTearDown()

    async def start(self, **kwargs):
        return await self.manager.start(bot=self.bot, message=self.message, status_message=self.status,
                                        mode=kwargs.get("mode", "users"), admin_id=999, ui_lang="uz")

    async def test_batches_retry_rate_limit_and_count_delivery_outcomes(self):
        attempts = {}
        method = CopyMessage(chat_id=101, from_chat_id=999, message_id=55)

        async def deliver(**kwargs):
            uid = kwargs["chat_id"]
            attempts[uid] = attempts.get(uid, 0) + 1
            if uid == 102:
                raise TelegramForbiddenError(method=method, message="Blocked")
            if uid == 103 and attempts[uid] == 1:
                raise TelegramRetryAfter(method=method, message="Rate limit", retry_after=2)
            if uid == 104:
                raise TelegramBadRequest(method=method, message="Chat not found")

        self.bot.copy_message.side_effect = deliver
        progress = await self.start()
        await self.manager.task
        self.assertEqual((progress.total, progress.sent, progress.failed, progress.blocked, progress.batches), (5, 3, 2, 1, 3))
        self.assertEqual(progress.status, "done")
        self.manager._pause.assert_any_await(2.2)
        self.manager._pause.assert_any_await(self.manager.batch_pause)
        self.assertEqual(attempts[103], 2)
        self.assertEqual(attempts[102], 1)
        self.assertEqual((await db.get_latest_broadcast())["sent"], 3)
        final_text = self.status.edit_text.await_args.args[0]
        self.assertIn("Yuborildi: 3", final_text)
        self.assertIn("Yetib bormadi: 2", final_text)
        for call in self.bot.copy_message.await_args_list:
            self.assertEqual(call.kwargs["from_chat_id"], 999)
            self.assertEqual(call.kwargs["message_id"], 55)

    async def test_stop_leaves_unsent_targets_separate_from_failures(self):
        async def deliver(**kwargs):
            self.manager.stop(999)

        self.bot.copy_message.side_effect = deliver

        async def pause(_):
            return self.manager._stop.is_set()

        self.manager._pause = pause
        progress = await self.start()
        await self.manager.task
        self.assertEqual(progress.status, "cancelled")
        self.assertEqual((progress.sent, progress.failed, progress.remaining), (1, 0, 4))
        self.bot.copy_message.assert_awaited_once()
        self.assertEqual((await db.get_latest_broadcast())["status"], "cancelled")

    async def test_second_broadcast_is_rejected_and_only_owner_can_stop(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def deliver(**kwargs):
            started.set()
            await release.wait()

        self.bot.copy_message.side_effect = deliver
        await self.start()
        await asyncio.wait_for(started.wait(), timeout=5)
        with self.assertRaises(BroadcastBusy):
            await self.start()
        self.assertFalse(self.manager.stop(888))
        self.assertTrue(self.manager.stop(999))
        release.set()
        await self.manager.task
        self.assertEqual(self.manager.progress.status, "cancelled")

    async def test_group_pages_include_negative_ids_and_deduplicate(self):
        async with db.async_session() as session:
            session.add(db.Quiz(id=1, creator_id=101))
            await session.flush()
            for chat_id, chat_type in [(-1002, "group"), (-1002, "group"), (-1001, "supergroup"), (101, "private")]:
                session.add(db.QuizAttempt(quiz_id=1, user_id=101, chat_id=chat_id, chat_type=chat_type,
                                           completed_at=datetime.now(timezone.utc).isoformat()))
            await session.commit()
        progress = await self.start(mode="groups")
        await self.manager.task
        self.assertEqual((progress.total, progress.sent), (2, 2))
        self.assertEqual([call.kwargs["chat_id"] for call in self.bot.copy_message.await_args_list], [-1002, -1001])

    async def test_restart_marks_unfinished_summary_without_resending(self):
        async with db.async_session() as session:
            session.add(db.BroadcastRun(id="unfinished", admin_id=999, status="sending", total=50, sent=25,
                                         created_at=datetime.now(timezone.utc).isoformat()))
            await session.commit()
        await db.interrupt_unfinished_broadcasts()
        latest = await db.get_latest_broadcast()
        self.assertEqual((latest["status"], latest["sent"], latest["total"]), ("interrupted", 25, 50))
        self.bot.copy_message.assert_not_awaited()
