"""Exercise shared rooms through HTTP with several signed test users."""

import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import test_webapp as support
from sqlalchemy import select

from services import database as db
from services.interactive_api import SERVICE
from services.interactive_service import InteractiveService


def question(n=1, *, affinity=False):
    q = {"question": f"Question {n}?", "options": ["A", "B", "C", "D"]}
    q.update({"weights": [0, 25, 75, 100]} if affinity else {"correct_index": 2})
    return q


class InteractiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = support.ApiTests("test_static_app_and_api_authentication")
        await self.fixture.asyncSetUp()
        self.client = self.fixture.client
        self.service = self.fixture.app[SERVICE]
        self.clock = [time.time()]
        self.service.now = lambda: self.clock[0]

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def request(self, method, path, *, uid=101, data=None, status=200):
        kwargs = {"headers": self.fixture.headers(uid)}
        if data is not None:
            kwargs["json"] = data
        response = await self.client.request(method, "/api/interactive" + path, **kwargs)
        payload = await response.json()
        self.assertEqual(response.status, status, payload)
        return payload

    async def publish(self, *, uid=101, count=4, affinity=False, **extra):
        data = {"title": "Shared quiz", "category": "love" if affinity else "education", "subject": "general",
                "kind": "affinity" if affinity else "knowledge", "open_period": 10,
                "questions": [question(n, affinity=affinity) for n in range(count)], **extra}
        return (await self.request("POST", "/quizzes", uid=uid, data=data, status=201))["quiz_id"]

    async def start(self, quiz_id, *, uid=101):
        return (await self.request("POST", "/rooms", uid=uid, data={"quiz_id": quiz_id}, status=201))["room_id"]

    async def join(self, room_id, uid):
        await self.request("POST", f"/rooms/{room_id}/join", uid=uid)

    async def answer(self, room_id, uid, index, option=2, *, status=200):
        return await self.request("POST", f"/rooms/{room_id}/answers", uid=uid,
                                  data={"question_index": index, "option_index": option}, status=status)

    async def step(self, seconds=10.01):
        self.clock[0] += seconds
        await self.service.advance()

    async def test_catalogue_has_real_starters_and_subject_filters_without_answer_keys(self):
        lobby = await self.request("GET", "")
        self.assertEqual(len(lobby["quizzes"]), 5)
        self.assertEqual(lobby["rooms"], [])
        self.assertEqual(lobby["leaderboard"], [])
        self.assertEqual(lobby["online_count"], 1)
        self.assertNotIn("correct_index", json.dumps(lobby))
        self.assertNotIn("weights", json.dumps(lobby))
        math = await self.request("GET", "?category=education&subject=math")
        self.assertEqual(len(math["quizzes"]), 1)
        love = await self.request("GET", "?category=love")
        self.assertEqual(love["quizzes"], [])

    async def test_host_can_leave_while_other_members_answer_and_receive_weighted_result(self):
        quiz_id = await self.publish(count=3, affinity=True)
        room_id = await self.start(quiz_id)
        await self.join(room_id, 202)
        await self.join(room_id, 303)
        await self.request("POST", f"/rooms/{room_id}/leave")
        for index, option in enumerate([2, 3, 1]):
            await self.answer(room_id, 202, index, option)
            if index == 0:
                await self.answer(room_id, 303, index, 0)
            await self.step()
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual(room["status"], "finished")
        self.assertEqual(room["my_result"]["percent"], 67)
        self.assertTrue(room["my_result"]["complete"])
        self.assertEqual(room["online_count"], 2)
        self.assertFalse(next(p for p in room["players"] if p["id"] and not p["is_me"] and p["answered"] == 0)["online"])
        await self.request("GET", f"/rooms/{room_id}", status=403)
        ranking = (await self.request("GET", "?category=love", uid=202))["leaderboard"]
        self.assertEqual(ranking[0]["plays"], 1)

    async def test_server_worker_stops_after_three_empty_questions_without_clients(self):
        room_id = await self.start(await self.publish(count=5))
        await self.request("POST", f"/rooms/{room_id}/leave")
        self.clock[0] += 31

        async def wait_for_worker():
            while True:
                async with self.service.lock, db.async_session() as session:
                    room = await session.get(db.InteractiveRoom, room_id)
                    if room.status == "stopped":
                        return room.idle_streak, room.question_index, room.reason
                await asyncio.sleep(0.1)

        streak, index, reason = await asyncio.wait_for(wait_for_worker(), timeout=4)
        self.assertEqual((streak, index, reason), (3, 2, "idle"))
        await self.request("POST", f"/rooms/{room_id}/join", uid=202, status=409)

    async def test_an_answer_resets_consecutive_inactivity(self):
        room_id = await self.start(await self.publish(count=6))
        await self.join(room_id, 202)
        await self.step()
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual((room["question_index"], room["idle_streak"]), (1, 1))
        await self.answer(room_id, 202, 1)
        await self.step()
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual((room["question_index"], room["idle_streak"]), (2, 0))
        await self.step(20.01)
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual((room["status"], room["idle_streak"]), ("active", 2))
        await self.step()
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual((room["status"], room["idle_streak"]), ("stopped", 3))

    async def test_membership_deadlines_and_single_answer_are_enforced(self):
        room_id = await self.start(await self.publish())
        await self.answer(room_id, 202, 0, status=403)
        await self.join(room_id, 202)
        await self.answer(room_id, 202, 1, status=409)
        await self.answer(room_id, 202, 0)
        duplicate = await self.answer(room_id, 202, 0, 3, status=409)
        self.assertEqual(duplicate["error"], "already_answered")
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertNotIn("correct_index", json.dumps(room))
        self.assertNotIn("weights", json.dumps(room))
        self.assertEqual(room["selected"], 2)
        self.assertEqual(room["my_result"]["answered"], 1)
        await self.step()
        stale = await self.answer(room_id, 202, 0, status=409)
        self.assertEqual(stale["error"], "question_closed")

    async def test_author_ranking_counts_distinct_completions_and_excludes_self(self):
        for uid, name in [(101, "Ali"), (303, "Vali")]:
            response = await self.client.patch("/api/profile", headers=self.fixture.headers(uid), json={"display_name": name, "username": f"author_{uid}"})
            self.assertEqual(response.status, 200)
        first = await self.publish(count=1)
        second = await self.publish(uid=303, count=1)
        for quiz_id, uid in [(first, 202), (first, 202), (first, 101), (first, 303), (second, 202)]:
            room_id = await self.start(quiz_id, uid=uid)
            await self.answer(room_id, uid, 0)
            await self.step()
        ranking = (await self.request("GET", ""))["leaderboard"]
        self.assertEqual([(p["rank"], p["name"], p["plays"]) for p in ranking], [(1, "Ali", 2), (2, "Vali", 1)])
        self.assertNotIn("user_id", json.dumps(ranking))

    async def test_partial_answers_do_not_inflate_author_ranking(self):
        room_id = await self.start(await self.publish(count=4))
        await self.join(room_id, 202)
        await self.answer(room_id, 202, 0)
        await self.step(41)
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertFalse(room["my_result"]["complete"])
        self.assertEqual((await self.request("GET", ""))["leaderboard"], [])

    async def test_persistent_room_resumes_after_service_restart(self):
        room_id = await self.start(await self.publish())
        await self.answer(room_id, 101, 0)
        await self.service.close()
        restarted = InteractiveService(now=lambda: self.clock[0])
        try:
            await restarted.start()
            self.clock[0] += 11
            await restarted.advance()
            room = await restarted.room(101, room_id)
            self.assertEqual(room["question_index"], 1)
            self.assertEqual(room["my_result"]["answered"], 1)
            self.assertEqual(room["status"], "active")
        finally:
            await restarted.close()

    async def test_online_presence_expires_without_fake_users(self):
        await self.request("GET", "")
        self.assertEqual((await self.request("GET", "", uid=202))["online_count"], 2)
        self.clock[0] += 46
        lobby = await self.request("GET", "")
        self.assertEqual(lobby["online_count"], 1)
        self.assertEqual(len(lobby["online"]), 1)

    async def test_only_owner_can_publish_a_saved_private_quiz(self):
        response = await self.client.post("/api/quizzes/manual", headers=self.fixture.headers(), json={"title": "Private mathematics", "questions": [support.QUESTION]})
        self.assertEqual(response.status, 201)
        quiz_id = (await response.json())["quiz_id"]
        await self.request("POST", "/quizzes", uid=202, data={"source_quiz_id": quiz_id}, status=404)
        published = await self.request("POST", "/quizzes", data={"source_quiz_id": quiz_id}, status=201)
        room_id = await self.start(published["quiz_id"], uid=202)
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual(room["question"]["text"], support.QUESTION["question"])
        self.assertNotIn("correct_index", json.dumps(room))

    async def test_weight_validation_and_untrusted_image_paths(self):
        bad = question(affinity=True)
        bad["weights"][0] = 101
        response = await self.request("POST", "/quizzes", data={"title": "Bad weights", "category": "love", "kind": "affinity", "questions": [bad]}, status=400)
        self.assertEqual(response["error"], "invalid_input")
        quiz_id = await self.publish(count=1, questions=[{**question(), "image_path": ".env"}])
        room_id = await self.start(quiz_id)
        room = await self.request("GET", f"/rooms/{room_id}")
        self.assertFalse(room["question"]["has_image"])
        await self.request("GET", f"/rooms/{room_id}/images/0", status=404)
        await self.request("GET", f"/rooms/{room_id}/images/1", status=404)

    async def test_invite_link_preserves_webapp_query_and_fragment(self):
        from handlers.miniapp import open_shared_room

        room_id = "a" * 32
        message = SimpleNamespace(chat=SimpleNamespace(type="private"), answer=AsyncMock())
        with patch("handlers.miniapp.WEBAPP_URL", "https://example.test/app?mode=mini#interactive"):
            await open_shared_room(message, SimpleNamespace(args=f"room_{room_id}"))
        button = message.answer.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
        url = urlsplit(button.web_app.url)
        self.assertEqual(parse_qs(url.query), {"mode": ["mini"], "room": [room_id]})
        self.assertEqual(url.fragment, "interactive")

    async def test_private_quiz_is_link_only_and_only_author_can_start_rooms(self):
        quiz_id = await self.publish(visibility="private")
        room_id = await self.start(quiz_id)
        for uid in (101, 202):
            data = await self.request("GET", "", uid=uid)
            self.assertNotIn(quiz_id, [q["id"] for q in data["quizzes"]])
            self.assertNotIn(room_id, [r["id"] for r in data["rooms"]])
        mine = await self.request("GET", "/quizzes/mine")
        self.assertEqual(mine["quizzes"][0]["visibility"], "private")
        self.assertNotIn("questions", mine["quizzes"][0])
        self.assertEqual((await self.request("GET", "/quizzes/mine", uid=202))["quizzes"], [])
        await self.request("POST", "/rooms", uid=202, data={"quiz_id": quiz_id}, status=404)
        await self.request("GET", f"/rooms/{room_id}", uid=202, status=403)
        await self.join(room_id, 202)
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual(room["visibility"], "private")
        self.assertFalse(room["can_restart"])
        self.assertEqual(len(room["question"]["options"]), 4)

    async def test_only_author_can_remove_published_quiz_and_it_stops_all_rooms(self):
        quiz_id = await self.publish()
        room_id = await self.start(quiz_id, uid=202)
        await self.request("DELETE", f"/quizzes/{quiz_id}", uid=202, status=404)
        await self.request("DELETE", f"/quizzes/{quiz_id}")
        self.assertEqual((await self.request("GET", "/quizzes/mine"))["quizzes"], [])
        lobby = await self.request("GET", "", uid=202)
        self.assertNotIn(quiz_id, [q["id"] for q in lobby["quizzes"]])
        self.assertNotIn(room_id, [r["id"] for r in lobby["rooms"]])
        room = await self.request("GET", f"/rooms/{room_id}", uid=202)
        self.assertEqual((room["status"], room["reason"], room["can_restart"]), ("stopped", "deleted", False))
        await self.answer(room_id, 202, 0, status=409)
        await self.request("POST", f"/rooms/{room_id}/join", uid=303, status=404)
        await self.request("POST", "/rooms", data={"quiz_id": quiz_id}, status=404)
        await self.request("DELETE", f"/quizzes/{quiz_id}")  # Idempotent.

    async def test_private_completions_never_leak_into_public_ranking(self):
        quiz_id = await self.publish(count=1, visibility="private")
        room_id = await self.start(quiz_id)
        await self.join(room_id, 202)
        await self.answer(room_id, 202, 0)
        lobby = await self.request("GET", "")
        self.assertEqual(lobby["leaderboard"], [])

    async def test_removal_keeps_original_saved_bot_quiz(self):
        response = await self.client.post("/api/quizzes/manual", json={"title": "Original", "questions": [support.QUESTION]}, headers=self.fixture.headers())
        original = (await response.json())["quiz_id"]
        published = await self.request("POST", "/quizzes", data={"source_quiz_id": original}, status=201)
        await self.request("DELETE", "/quizzes/" + published["quiz_id"])
        response = await self.client.get(f"/api/quizzes/{original}", headers=self.fixture.headers())
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["title"], "Original")

    async def test_published_list_pagination_and_legacy_public_defaults(self):
        async with db.async_session() as session:
            for n in range(21):
                session.add(db.PublicQuiz(id=f"{1000 + n:032x}", author_id=101, title=f"Published {n}",
                                          category="education", questions=json.dumps([question()]), created_at=n))
            await session.commit()
        first = await self.request("GET", "/quizzes/mine")
        self.assertEqual(len(first["quizzes"]), 20)
        self.assertTrue(all(q["visibility"] == "public" for q in first["quizzes"]))
        second = await self.request("GET", f"/quizzes/mine?before={first['next']}")
        self.assertEqual(len(second["quizzes"]), 1)
        self.assertIsNone(second["next"])
        self.assertEqual(len({q["id"] for q in first["quizzes"] + second["quizzes"]}), 21)
        await self.request("GET", f"/quizzes/mine?before={first['next']}", uid=202, status=404)


if __name__ == "__main__":
    unittest.main()
