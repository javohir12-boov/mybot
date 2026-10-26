"""Exercise real HTTP handlers with an isolated DB and mocked Telegram/AI."""

import asyncio
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import time
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

# Do not use developer credentials, make network calls, or touch the bot's DB.
os.environ["BOT_TOKEN"] = "123456789:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["BOT_MODE"] = "ai"
os.environ["REQUIRED_CHANNEL_ENABLED"] = "0"

from aiohttp import FormData, web
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy import func, select

from config import BOT_TOKEN
from services import database as db
from services.ai_service import AIServiceError
from services.webapp import AppError, STATE, setup_webapp, stop_jobs, validate_init_data
from services.support_service import SupportService


QUESTION = {"question": "2 + 2 = ?", "options": ["1", "2", "3", "4"], "correct_index": 3, "explanation": "2 + 2 = 4"}


def signed(user_id=101, *, timestamp=None, fields=None, token=BOT_TOKEN):
    data = fields or {"auth_date": str(int(time.time()) if timestamp is None else timestamp),
                      "user": json.dumps({"id": user_id, "first_name": "O‘quvchi", "username": "test_user"}, ensure_ascii=False),
                      "query_id": "test-query"}
    check = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    signature = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode({**data, "hash": signature})


class AuthTests(unittest.TestCase):
    def test_signed_identity_matches_aiogram(self):
        from aiogram.utils.web_app import safe_parse_webapp_init_data
        raw = signed()
        self.assertEqual(validate_init_data(raw)["id"], 101)
        self.assertEqual(safe_parse_webapp_init_data(BOT_TOKEN, raw).user.id, 101)

    def test_forged_expired_future_duplicate_or_invalid_users_are_rejected(self):
        now = int(time.time())
        invalid = [signed().replace("101", "999"), signed(timestamp=now - 100000), signed(timestamp=now + 90),
                   signed() + "&user=%7B%7D", "", signed(fields={"auth_date": str(now), "user": "[]"}),
                   signed(fields={"auth_date": str(now), "user": '{"id":true}'})]
        for raw in invalid:
            with self.subTest(raw=raw[:30]), self.assertRaises(AppError):
                validate_init_data(raw)


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # A file gives concurrent HTTP requests separate SQLite connections.
        # StaticPool's single in-memory connection lets one session roll back
        # another session's uncommitted support message during live polling.
        self.database_temp = tempfile.TemporaryDirectory()
        database_url = "sqlite+aiosqlite:///" + (Path(self.database_temp.name) / "test.db").as_posix()
        with patch.object(db, "DATABASE_URL", database_url):
            self.test_engine = db._build_engine()
        self.engine_patch = patch.object(db, "engine", self.test_engine)
        self.session_patch = patch.object(db, "async_session", db.sessionmaker(self.test_engine, class_=db.AsyncSession, expire_on_commit=False))
        self.engine_patch.start()
        self.session_patch.start()
        self.avatar_temp = tempfile.TemporaryDirectory()
        self.avatar_patch = patch("services.webapp.AVATARS", Path(self.avatar_temp.name))
        self.avatar_patch.start()
        async with db.engine.begin() as connection:
            await connection.run_sync(db.Base.metadata.drop_all)
            await connection.run_sync(db.Base.metadata.create_all)
        self.bot = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(username="test_quiz_bot")),
                                   get_chat_member=AsyncMock(return_value=SimpleNamespace(status="member")))
        self.app = web.Application(client_max_size=32 * 1024 * 1024)
        setup_webapp(self.app, self.bot)
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()
        await db.get_or_create_user(101, "Test")
        async with db.async_session() as session:
            session.add(db.UserQuota(user_id=101, premium_until="2100-01-01T00:00:00+00:00", files_total=10, topics_total=10))
            await session.commit()

    async def asyncTearDown(self):
        await self.client.close()
        await db.engine.dispose()
        self.session_patch.stop()
        self.engine_patch.stop()
        self.database_temp.cleanup()
        self.avatar_patch.stop()
        self.avatar_temp.cleanup()

    def headers(self, uid=101):
        return {"Authorization": f"tma {signed(uid)}"}

    async def finished_job(self, job_id):
        tasks = list(self.app[STATE]["tasks"])
        await asyncio.gather(*tasks)
        response = await self.client.get(f"/api/jobs/{job_id}", headers=self.headers())
        self.assertEqual(response.status, 200)
        return await response.json()

    async def upload(self, name, contents, options=None):
        form = FormData()
        form.add_field("options", json.dumps(options or {"import_only": True}))
        form.add_field("file", io.BytesIO(contents), filename=name, content_type="application/octet-stream")
        return await self.client.post("/api/quizzes/file", data=form, headers=self.headers())

    async def test_static_app_and_api_authentication(self):
        for path in ("/app", "/app/", "/app/assets/app.js", "/app/assets/app.css", "/app/assets/locales.js", "/app/assets/avatar-cropper.js"):
            response = await self.client.get(path)
            self.assertEqual(response.status, 200, path)
            self.assertEqual(response.headers.get("Cache-Control"), "no-cache", path)
        response = await self.client.get("/api/profile")
        self.assertEqual(response.status, 401)
        self.assertEqual((await response.json())["error"], "unauthorized")

    async def test_profile_uses_existing_quota_and_plans(self):
        response = await self.client.get("/api/profile", headers=self.headers())
        self.assertEqual(response.status, 200)
        profile = await response.json()
        self.assertEqual(profile["user"]["id"], 101)
        self.assertTrue(profile["quota"]["premium_active"])
        self.assertEqual(profile["quota"]["files_left"], 10)
        self.assertIn("30d", profile["plans"])
        self.assertEqual(profile["bot_username"], "test_quiz_bot")

    async def test_public_reopen_info_never_exposes_private_profile(self):
        response = await self.client.get("/app/config.json")
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), {"name": "Create Quiz", "bot_username": "test_quiz_bot"})
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        for path in ("/api/profile", "/api/quizzes", "/api/support"):
            response = await self.client.get(path) if path != "/api/support" else await self.client.post(path, json={"message": "Hello"})
            self.assertEqual(response.status, 401)

    async def test_auth_failure_reports_actual_reason_without_returning_an_empty_list(self):
        cases = [(signed(timestamp=int(time.time()) - 100000), "session_expired"),
                 (signed(token="987654321:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"), "invalid_signature"),
                 ("invalid-data", "invalid_init_data")]
        for raw, reason in cases:
            response = await self.client.get("/api/quizzes", headers={"Authorization": f"tma {raw}"})
            self.assertEqual(response.status, 401)
            self.assertEqual(await response.json(), {"error": "unauthorized", "reason": reason})

    async def test_auth_uses_serving_bot_token(self):
        self.bot.token = "987654321:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
        response = await self.client.get("/api/profile", headers=self.headers())
        self.assertEqual(response.status, 401)
        response = await self.client.get("/api/profile", headers={"Authorization": f"tma {signed(token=self.bot.token)}"})
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["user"]["id"], 101)

    async def test_bot_created_quiz_is_visible_in_web_and_remains_owner_only(self):
        # The bot's existing save path, rather than creating through the web API.
        quiz_id = await db.create_quiz(creator_id=101, title="Existing bot quiz")
        await db.create_questions_bulk(quiz_id, [QUESTION])
        response = await self.client.get("/api/quizzes", headers=self.headers())
        self.assertEqual(response.status, 200)
        rows = (await response.json())["quizzes"]
        self.assertEqual([(q["id"], q["title"], q["question_count"]) for q in rows], [(quiz_id, "Existing bot quiz", 1)])
        response = await self.client.get("/api/quizzes", headers=self.headers(202))
        self.assertEqual((await response.json())["quizzes"], [])

    async def test_support_requires_auth_and_uses_signed_identity(self):
        self.bot.send_message = AsyncMock()
        with patch("services.webapp.support_service", SupportService()), patch("services.support_service.ADMIN_IDS", [999]):
            response = await self.client.post("/api/support", json={"message": "Help"})
            self.assertEqual(response.status, 401)
            self.bot.send_message.assert_not_awaited()
            response = await self.client.post("/api/support", json={"message": " <b>Help</b> ", "user_id": 999}, headers=self.headers())
            self.assertEqual(response.status, 200)
            self.assertTrue((await response.json())["ok"])
            args = self.bot.send_message.await_args
            self.assertEqual(args.args[0], 999)
            self.assertIn("Web ilova", args.args[1])
            self.assertIn("<code>101</code>", args.args[1])
            self.assertIn("&lt;b&gt;Help&lt;/b&gt;", args.args[1])
            response = await self.client.post("/api/support", json={"message": "Again"}, headers=self.headers())
            self.assertEqual(response.status, 429)
            self.assertEqual((await response.json())["error"], "support_rate_limit")
            self.bot.send_message.assert_awaited_once()

    async def test_support_validation_and_web_inbox_survives_notification_failure(self):
        self.bot.send_message = AsyncMock(side_effect=RuntimeError("unavailable"))
        with patch("services.webapp.support_service", SupportService()), patch("services.support_service.ADMIN_IDS", [999]):
            for message in (None, "", "  ", "x" * 3001, 123, {"text": "Help"}):
                response = await self.client.post("/api/support", json={"message": message}, headers=self.headers())
                self.assertEqual(response.status, 400)
            self.bot.send_message.assert_not_awaited()
            response = await self.client.post("/api/support", json={"message": "Help"}, headers=self.headers())
            self.assertEqual(response.status, 200)
            history = await self.client.get("/api/support/messages", headers=self.headers())
            self.assertEqual((await history.json())["messages"][0]["body"], "Help")
            self.bot.send_message.side_effect = None
            response = await self.client.post("/api/support", json={"message": "Help"}, headers=self.headers())
            self.assertEqual(response.status, 429)

    async def test_language_saved_to_existing_user_settings(self):
        response = await self.client.patch("/api/settings", json={"ui_lang": "en"}, headers=self.headers())
        self.assertEqual(response.status, 200)
        self.assertEqual((await db.get_or_create_user_settings(101))["ui_lang"], "en")

    async def test_edited_profile_persists_without_changing_telegram_identity(self):
        response = await self.client.patch("/api/profile", json={"display_name": "Ali Valiyev", "username": "@Ali_Vali", "user_id": 202}, headers=self.headers())
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["profile"]["username"], "ali_vali")
        await db.get_or_create_user(101, "Telegram name", "telegram_handle")
        response = await self.client.get("/api/profile", headers=self.headers())
        data = await response.json()
        self.assertEqual(data["profile"]["display_name"], "Ali Valiyev")
        self.assertEqual(data["user"]["first_name"], "O‘quvchi")
        self.assertIsNone(await db.get_web_profile(202))

    async def test_username_conflicts_and_invalid_profile_values(self):
        response = await self.client.patch("/api/profile", json={"display_name": "Ali", "username": "ali_user"}, headers=self.headers())
        self.assertEqual(response.status, 200)
        response = await self.client.patch("/api/profile", json={"display_name": "Vali", "username": "ALI_USER"}, headers=self.headers(202))
        self.assertEqual(response.status, 409)
        self.assertEqual((await response.json())["error"], "username_taken")
        for name, username in [("", "valid_name"), ("a" * 81, "valid_name"), ("Ali", "12bad"), ("Ali", "bad name"), ("Ali", "ab")]:
            response = await self.client.patch("/api/profile", json={"display_name": name, "username": username}, headers=self.headers())
            self.assertEqual(response.status, 400)
        self.assertEqual((await db.get_web_profile(101))["username"], "ali_user")

    async def test_avatar_upload_replace_remove_and_owner_access(self):
        import fitz

        pixmap = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 30, 20), False)
        pixmap.clear_with(200)
        png = pixmap.tobytes("png")

        async def upload():
            body = FormData()
            body.add_field("avatar", io.BytesIO(png), filename="my-photo.png", content_type="image/png")
            return await self.client.post("/api/profile/avatar", data=body, headers=self.headers())

        response = await upload()
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assertTrue(data["profile"]["has_avatar"])
        self.assertNotIn("avatar_path", data["profile"])
        old_path = Path((await db.get_web_profile(101))["avatar_path"])
        response = await self.client.get("/api/profile/avatar", headers=self.headers())
        self.assertEqual(response.status, 200)
        self.assertTrue((await response.read()).startswith(b"\x89PNG"))
        response = await self.client.get("/api/profile/avatar", headers=self.headers(202))
        self.assertEqual(response.status, 404)
        response = await upload()
        self.assertEqual(response.status, 200)
        self.assertFalse(old_path.exists())
        self.assertEqual(len(list(Path(self.avatar_temp.name).iterdir())), 1)
        response = await self.client.delete("/api/profile/avatar", headers=self.headers())
        self.assertFalse((await response.json())["profile"]["has_avatar"])
        self.assertEqual(list(Path(self.avatar_temp.name).iterdir()), [])
        response = await self.client.get("/api/profile/avatar", headers=self.headers())
        self.assertEqual(response.status, 404)

    async def test_avatar_fit_preserves_transparent_margins(self):
        import fitz

        image = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 768, 768), True)
        image.clear_with()
        image.set_rect(fitz.IRect(300, 50, 468, 718), (255, 0, 0, 255))
        body = FormData()
        body.add_field("avatar", io.BytesIO(image.tobytes("png")), filename="fit.png", content_type="image/png")
        response = await self.client.post("/api/profile/avatar", data=body, headers=self.headers())
        self.assertEqual(response.status, 200)
        response = await self.client.get("/api/profile/avatar", headers=self.headers())
        result = fitz.Pixmap(await response.read())
        self.assertEqual((result.width, result.height), (384, 384))
        self.assertEqual(result.pixel(0, 0)[-1], 0)
        self.assertEqual(result.pixel(192, 192), (255, 0, 0, 255))

    async def test_avatar_rejects_disguised_and_oversized_uploads(self):
        for contents, status in [(b"<svg><script>alert(1)</script></svg>", 400), (b"x" * (5 * 1024 * 1024 + 1), 413)]:
            body = FormData()
            body.add_field("avatar", io.BytesIO(contents), filename="photo.png", content_type="image/png")
            response = await self.client.post("/api/profile/avatar", data=body, headers=self.headers())
            self.assertEqual(response.status, status)
        self.assertEqual(list(Path(self.avatar_temp.name).iterdir()), [])

    async def test_manual_quiz_saved_and_private_to_creator(self):
        response = await self.client.post("/api/quizzes/manual", json={"title": "Math", "questions": [QUESTION]}, headers=self.headers())
        self.assertEqual(response.status, 201)
        quiz_id = (await response.json())["quiz_id"]
        response = await self.client.get(f"/api/quizzes/{quiz_id}", headers=self.headers())
        quiz = await response.json()
        self.assertEqual(quiz["questions"][0]["correct_index"], 3)
        self.assertNotIn("image_path", quiz["questions"][0])
        response = await self.client.get(f"/api/quizzes/{quiz_id}", headers=self.headers(202))
        self.assertEqual(response.status, 404)
        response = await self.client.get("/api/quizzes", headers=self.headers(202))
        self.assertEqual((await response.json())["quizzes"], [])
        quota = await db.get_user_quota_status(101)
        self.assertEqual((quota["files_used"], quota["topics_used"]), (0, 0))

    async def test_shuffle_preserves_correct_answer(self):
        response = await self.client.post("/api/quizzes/manual", json={"title": "Math", "questions": [QUESTION], "shuffle_mode": "both"}, headers=self.headers())
        self.assertEqual(response.status, 201)
        quiz = await db.get_quiz_with_questions((await response.json())["quiz_id"])
        q = quiz["questions"][0]
        self.assertEqual(q["options"][q["correct_index"]], "4")

    async def test_invalid_questions_never_create_empty_quiz(self):
        for questions in ([], [{**QUESTION, "correct_index": 4}], [{**QUESTION, "options": ["a", "b"]}], [{**QUESTION, "question": "x" * 301}]):
            response = await self.client.post("/api/quizzes/manual", json={"title": "Invalid", "questions": questions}, headers=self.headers())
            self.assertEqual(response.status, 400)
        self.assertEqual(await db.list_user_quizzes(101), [])

    async def test_topic_job_generates_quiz_and_charges_once(self):
        ai = SimpleNamespace(generate_quiz_from_topic=AsyncMock(return_value=[QUESTION]))
        with patch("services.webapp.AIService", return_value=ai):
            response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            self.assertEqual(response.status, 202)
            job_id = (await response.json())["job_id"]
            result = await self.finished_job(job_id)
        self.assertEqual(result["status"], "done")
        self.assertEqual((await db.get_user_quota_status(101))["topics_used"], 1)
        response = await self.client.get(f"/api/jobs/{job_id}", headers=self.headers(202))
        self.assertEqual(response.status, 404)

    async def test_provider_failure_refunds_reserved_quota(self):
        ai = SimpleNamespace(generate_quiz_from_topic=AsyncMock(side_effect=AIServiceError("mock failure")))
        with patch("services.webapp.AIService", return_value=ai):
            response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            self.assertEqual(response.status, 202)
            result = await self.finished_job((await response.json())["job_id"])
        self.assertEqual(result["error"], "generation_failed")
        self.assertEqual((await db.get_user_quota_status(101))["topics_used"], 0)
        self.assertEqual(await db.list_user_quizzes(101), [])

    async def test_json_file_import_is_free_and_cleans_temp_files(self):
        response = await self.upload("ready.json", json.dumps({"title": "Imported", "questions": [QUESTION]}).encode())
        self.assertEqual(response.status, 202)
        result = await self.finished_job((await response.json())["job_id"])
        self.assertEqual(result["status"], "done")
        self.assertEqual((await db.get_user_quota_status(101))["files_used"], 0)
        quiz = await db.get_quiz_with_questions(result["quiz_id"])
        self.assertFalse(quiz["is_ai_generated"])
        self.assertEqual(quiz["title"], "Imported")

    async def test_file_text_generation_uses_same_ai_and_quota(self):
        ai = SimpleNamespace(generate_quiz_from_text=AsyncMock(return_value=[QUESTION]))
        with patch("services.webapp.AIService", return_value=ai):
            response = await self.upload("source.txt", ("A long educational source about mathematics. " * 15).encode(), {"import_only": False})
            self.assertEqual(response.status, 202)
            result = await self.finished_job((await response.json())["job_id"])
        self.assertEqual(result["status"], "done")
        self.assertEqual((await db.get_user_quota_status(101))["files_used"], 1)
        ai.generate_quiz_from_text.assert_awaited_once()

    async def test_ready_text_auto_import_refunds_quota(self):
        text = "1. 2 + 2 = ?\nA) 1\nB) 2\nC) 3\nD) 4\nAnswer: D"
        response = await self.upload("ready.txt", text.encode(), {"import_only": False})
        self.assertEqual(response.status, 202)
        result = await self.finished_job((await response.json())["job_id"])
        self.assertEqual(result["status"], "done")
        self.assertEqual((await db.get_user_quota_status(101))["files_used"], 0)

    async def test_image_survives_upload_cleanup_and_requires_owner(self):
        source = None

        async def image_ai(paths, **kwargs):
            nonlocal source
            source = Path(paths[0])
            return [{**QUESTION, "image_path": str(source)}]

        with patch("services.webapp.AIService", return_value=SimpleNamespace(extract_quiz_from_images=image_ai)):
            response = await self.upload("source.png", b"test-image-bytes", {"import_only": False})
            self.assertEqual(response.status, 202)
            result = await self.finished_job((await response.json())["job_id"])
        self.assertEqual(result["status"], "done")
        self.assertFalse(source.exists())
        quiz = await db.get_quiz_with_questions(result["quiz_id"])
        question = quiz["questions"][0]
        image_path = Path(question["image_path"])
        try:
            self.assertTrue(image_path.is_file())
            url = f"/api/quizzes/{quiz['id']}/images/{question['question_id']}"
            response = await self.client.get(url, headers=self.headers())
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.read(), b"test-image-bytes")
            response = await self.client.get(url, headers=self.headers(202))
            self.assertEqual(response.status, 404)
        finally:
            image_path.unlink(missing_ok=True)

    async def test_shutdown_refunds_unfinished_generation(self):
        started = asyncio.Event()

        async def slow_ai(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        with patch("services.webapp.AIService", return_value=SimpleNamespace(generate_quiz_from_topic=slow_ai)):
            response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            self.assertEqual(response.status, 202)
            job_id = (await response.json())["job_id"]
            await asyncio.wait_for(started.wait(), timeout=5)
            self.assertEqual((await db.get_user_quota_status(101))["topics_used"], 1)
            await stop_jobs(self.app)
            self.assertEqual((await self.finished_job(job_id))["error"], "interrupted")
        self.assertEqual((await db.get_user_quota_status(101))["topics_used"], 0)

    async def test_unsupported_empty_and_oversized_files_rejected(self):
        for name, contents, status in (("bad.exe", b"bad", 400), ("empty.txt", b"", 400), ("big.txt", b"x" * (5 * 1024 * 1024 + 1), 413)):
            response = await self.upload(name, contents)
            self.assertEqual(response.status, status)
        self.assertFalse(self.app[STATE]["jobs"])

    async def test_only_one_active_generation_per_user(self):
        event = asyncio.Event()

        async def slow_ai(*args, **kwargs):
            await event.wait()
            return [QUESTION]

        with patch("services.webapp.AIService", return_value=SimpleNamespace(generate_quiz_from_topic=slow_ai)):
            response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            job_id = (await response.json())["job_id"]
            second = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            self.assertEqual(second.status, 409)
            event.set()
            self.assertEqual((await self.finished_job(job_id))["status"], "done")
        self.assertEqual((await db.get_user_quota_status(101))["topics_used"], 1)

    async def test_subscription_gate_applies_to_web_creation(self):
        self.bot.get_chat_member.return_value = SimpleNamespace(status="left")
        with patch("services.webapp.REQUIRED_CHANNEL_ENABLED", True), patch("services.webapp.REQUIRED_CHANNEL", "@test_channel"):
            response = await self.client.post("/api/quizzes/manual", json={"title": "Math", "questions": [QUESTION]}, headers=self.headers())
        self.assertEqual(response.status, 403)
        self.assertEqual((await response.json())["error"], "subscription_required")

    async def test_exhausted_quota_and_noai_mode(self):
        async with db.async_session() as session:
            quota = await session.get(db.UserQuota, 101)
            quota.topics_used = 10
            await session.commit()
        response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
        self.assertEqual(response.status, 429)
        self.assertEqual((await response.json())["error"], "quota_exceeded")
        with patch("services.webapp.AI_ENABLED", False):
            response = await self.client.post("/api/quizzes/topic", json={"topic": "Mathematics"}, headers=self.headers())
            self.assertEqual(response.status, 403)
            response = await self.upload("ready.json", json.dumps({"questions": [QUESTION]}).encode())
            self.assertEqual(response.status, 202)
            self.assertEqual((await self.finished_job((await response.json())["job_id"]))["status"], "done")


if __name__ == "__main__":
    unittest.main()
