"""Persistent web support, unread markers, and administrator authorization."""

import unittest
from unittest.mock import AsyncMock, patch

import test_webapp as support
from services import database as db
from services.support_inbox import SupportInbox
from services.support_service import SupportService


class InboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = support.ApiTests("test_static_app_and_api_authentication")
        await self.fixture.asyncSetUp()
        self.client = self.fixture.client
        self.fixture.bot.send_message = AsyncMock()
        self.patches = [patch("services.support_service.ADMIN_IDS", [999]),
                        patch("services.support_inbox.ADMIN_IDS", [999]),
                        patch("services.webapp.support_service", SupportService())]
        for value in self.patches:
            value.start()

    async def asyncTearDown(self):
        for value in reversed(self.patches):
            value.stop()
        await self.fixture.asyncTearDown()

    async def request(self, method, path, *, uid=101, body=None, status=200):
        response = await self.client.request(method, "/api/" + path, headers=self.fixture.headers(uid), json=body)
        data = await response.json()
        self.assertEqual(response.status, status, data)
        return data

    async def test_web_message_admin_reply_and_read_markers_survive_new_service(self):
        await self.request("POST", "support", body={"message": "<b>My question</b>"})
        admin_profile = await self.request("GET", "profile", uid=999)
        self.assertTrue(admin_profile["is_admin"])
        self.assertEqual(admin_profile["support"]["admin_unread"], 1)
        threads = await self.request("GET", "admin/support", uid=999)
        self.assertEqual(threads["threads"][0]["user_id"], 101)
        self.assertEqual(threads["threads"][0]["unread"], 1)
        history = await self.request("GET", "admin/support/101/messages", uid=999)
        self.assertEqual(history["messages"][0]["body"], "<b>My question</b>")
        self.assertEqual((await self.request("GET", "admin/support", uid=999))["unread"], 0)
        await self.request("POST", "admin/support/101/messages", uid=999, body={"message": "Administrator reply"})
        self.assertEqual((await self.request("GET", "profile"))["support"]["unread"], 1)
        history = await self.request("GET", "support/messages")
        self.assertEqual([m["sender"] for m in history["messages"]], ["user", "admin"])
        self.assertEqual(history["messages"][-1]["body"], "Administrator reply")
        self.assertNotIn("sender_id", history["messages"][-1])
        self.assertEqual((await self.request("GET", "profile"))["support"]["unread"], 0)
        restored = await SupportInbox().messages(101)
        self.assertEqual(len(restored["messages"]), 2)

    async def test_users_cannot_open_admin_panel_reply_or_read_another_conversation(self):
        await self.request("POST", "support", body={"message": "Private question", "user_id": 202})
        self.assertEqual((await self.request("GET", "support/messages", uid=202))["messages"], [])
        for uid in (101, 202):
            await self.request("GET", "admin/support", uid=uid, status=403)
            await self.request("GET", "admin/support/101/messages", uid=uid, status=403)
            await self.request("POST", "admin/support/101/messages", uid=uid, body={"message": "Fake reply"}, status=403)
        response = await self.client.get("/api/support/messages")
        self.assertEqual(response.status, 401)

    async def test_web_delivery_succeeds_even_when_telegram_notifications_fail(self):
        self.fixture.bot.send_message.side_effect = RuntimeError("blocked")
        await self.request("POST", "support", body={"message": "Web inbox still receives this"})
        self.assertEqual((await self.request("GET", "admin/support", uid=999))["threads"][0]["preview"], "Web inbox still receives this")
        await self.request("POST", "support", body={"message": "Duplicate"}, status=429)

    async def test_persistent_user_cooldown_and_reply_validation(self):
        await self.request("POST", "support", body={"message": "Question"})
        with patch("services.webapp.support_service", SupportService()):
            await self.request("POST", "support", body={"message": "Restart bypass"}, status=429)
        for message in ("", " " * 3, "x" * 3001, None):
            await self.request("POST", "admin/support/101/messages", uid=999, body={"message": message}, status=400)
        await self.request("POST", "admin/support/202/messages", uid=999, body={"message": "No conversation"}, status=404)

    async def test_message_history_paginates_without_duplicates(self):
        await self.request("POST", "support", body={"message": "Question"})
        async with db.async_session() as session:
            messages = [db.SupportMessage(user_id=101, sender_id=999, from_admin=True, body=f"Reply {i}", created_at=100 + i) for i in range(55)]
            session.add_all(messages)
            await session.flush()
            (await session.get(db.SupportThread, 101)).last_message_id = messages[-1].id
            await session.commit()
        latest = await self.request("GET", "support/messages")
        self.assertEqual(len(latest["messages"]), 50)
        older = await self.request("GET", f"support/messages?before={latest['next_before']}")
        self.assertEqual(len(older["messages"]), 6)
        ids = [m["id"] for m in older["messages"] + latest["messages"]]
        self.assertEqual(ids, sorted(set(ids)))
        delta = await self.request("GET", f"support/messages?after={ids[-2]}")
        self.assertEqual([m["id"] for m in delta["messages"]], [ids[-1]])

    async def test_admin_inbox_paginates_without_exposing_threads_to_users(self):
        async with db.async_session() as session:
            session.add_all([db.User(id=uid, full_name=f"User {uid}") for uid in range(201, 233)])
            await session.flush()
            threads = [db.SupportThread(user_id=uid) for uid in range(201, 233)]
            session.add_all(threads)
            await session.flush()
            messages = [db.SupportMessage(user_id=t.user_id, sender_id=t.user_id, from_admin=False,
                                          body=f"Message {t.user_id}", created_at=100) for t in threads]
            session.add_all(messages)
            await session.flush()
            for thread, message in zip(threads, messages):
                thread.last_message_id = message.id
            await session.commit()
        first = await self.request("GET", "admin/support", uid=999)
        self.assertEqual((len(first["threads"]), first["unread"]), (30, 32))
        second = await self.request("GET", f"admin/support?before={first['next']}", uid=999)
        self.assertEqual(len(second["threads"]), 2)
        self.assertEqual(len({t["user_id"] for t in first["threads"] + second["threads"]}), 32)
        await self.request("GET", "admin/support", uid=201, status=403)


if __name__ == "__main__":
    unittest.main()
