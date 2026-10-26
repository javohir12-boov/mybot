"""Same-origin Mini App API. Telegram signs identity; the server owns all data."""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import shutil
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict, deque
from pathlib import Path
from urllib.parse import parse_qsl

from aiohttp import web
from sqlalchemy.exc import IntegrityError

from config import AI_ENABLED, BOT_TOKEN, REQUIRED_CHANNEL, REQUIRED_CHANNEL_ENABLED, WEBAPP_AUTH_MAX_AGE
from services import database as db
from services.ai_service import AIService, AIServiceError, extract_text_from_file
from services.import_service import ImportServiceError, parse_quiz_payload, parse_quiz_xlsx
from services.profile_service import ProfileError, normalize_avatar, remove_avatar, validate_profile
from services.support_service import SupportError, support_service

STATIC = Path(__file__).resolve().parent.parent / "webapp"
AVATARS = STATIC.parent / "media" / "avatars"
LANGUAGES = {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}
SUFFIXES = {".pdf", ".docx", ".pptx", ".xlsx", ".txt", ".md", ".json", ".png", ".jpg", ".jpeg", ".webp"}
STATE = web.AppKey("miniapp", dict)
USER = "telegram_user"


class AppError(Exception):
    def __init__(self, code, status=400, *, reason=None):
        self.code, self.status, self.reason = code, status, reason


def validate_init_data(raw, token=BOT_TOKEN, *, now=None, max_age=WEBAPP_AUTH_MAX_AGE):
    """Validate Telegram's HMAC and timestamp before trusting the user payload."""
    reason = "invalid_init_data"
    try:
        pairs = parse_qsl(raw, keep_blank_values=True, strict_parsing=True, max_num_fields=30)
        fields = dict(pairs)
        if len(fields) != len(pairs):
            raise ValueError("Duplicate fields")
        received = fields.pop("hash")
        check = "\n".join(f"{key}={value}" for key, value in sorted(fields.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, received):
            reason = "invalid_signature"
            raise ValueError("Invalid signature")
        age = (time.time() if now is None else now) - int(fields["auth_date"])
        if age < -30 or age > max_age:
            reason = "session_expired" if age > max_age else "session_clock"
            raise ValueError("Expired session")
        user = json.loads(fields["user"])
        if not isinstance(user, dict) or type(user.get("id")) is not int or user["id"] <= 0:
            raise ValueError("Invalid user")
        return user
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise AppError("unauthorized", 401, reason=reason) from exc


@web.middleware
async def api_middleware(request, handler):
    if not request.path.startswith("/api/"):
        response = await handler(request)
        if request.path in {"/app", "/app/"} or request.path.startswith("/app/assets/"):
            response.headers["Cache-Control"] = "no-cache"
        return response
    try:
        header = request.headers.get("Authorization", "")
        if not header.startswith("tma "):
            raise AppError("unauthorized", 401, reason="telegram_required")
        state = request.app[STATE]
        # Authenticate against the bot serving this app, including when it is
        # constructed with a token supplied outside the config module.
        token = getattr(state["bot"], "token", BOT_TOKEN)
        request[USER] = validate_init_data(header[4:], token=token)
        now = time.monotonic()
        # Expire inactive rate buckets; repeated sessions must not grow memory forever.
        if len(state["rates"]) > 2000:
            for uid, hits in list(state["rates"].items()):
                if not hits or now - hits[-1] > 60:
                    del state["rates"][uid]
        hits = state["rates"][request[USER]["id"]]
        while hits and now - hits[0] > 60:
            hits.popleft()
        if len(hits) >= 90:
            raise AppError("rate_limit", 429)
        hits.append(now)
        response = await handler(request)
    except AppError as exc:
        payload = {"error": exc.code}
        if exc.reason:
            payload["reason"] = exc.reason
            # Never log initData, signatures, tokens or the Authorization header.
            logging.warning("Mini App authentication rejected: reason=%s path=%s", exc.reason, request.path)
        response = web.json_response(payload, status=exc.status)
    except SupportError as exc:
        response = web.json_response({"error": exc.code}, status=exc.status)
    except ProfileError as exc:
        response = web.json_response({"error": str(exc)}, status=400)
    except db.QuotaExceeded:
        response = web.json_response({"error": "quota_exceeded"}, status=429)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, KeyError):
        response = web.json_response({"error": "invalid_input"}, status=400)
    except web.HTTPException:
        raise
    except Exception:
        logging.exception("Mini App request failed: %s", request.path)
        response = web.json_response({"error": "server_error"}, status=500)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


async def read_json(request):
    if request.content_length and request.content_length > 256_000:
        raise AppError("too_large", 413)
    raw = await request.content.read(256_001)
    if len(raw) > 256_000:
        raise AppError("too_large", 413)
    # read() may return a partial chunk for streamed bodies.
    while not request.content.at_eof():
        raw += await request.content.read(256_001 - len(raw))
        if len(raw) > 256_000:
            raise AppError("too_large", 413)
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise AppError("invalid_input")
    return data


def number(value, low, high):
    if isinstance(value, bool):
        raise AppError("invalid_input")
    n = int(value)
    if str(n) != str(value) or not low <= n <= high:
        raise AppError("invalid_input")
    return n


def settings(data):
    result = {
        "question_count": number(data.get("question_count", 10), 1, 50),
        "open_period": number(data.get("open_period", 30), 5, 600),
        "output_language": data.get("output_language", "source"),
        "difficulty": data.get("difficulty", "mixed"),
        "shuffle_mode": data.get("shuffle_mode", "none"),
    }
    if result["output_language"] not in LANGUAGES or result["difficulty"] not in {"easy", "medium", "hard", "mixed"}:
        raise AppError("invalid_input")
    if result["shuffle_mode"] not in {"none", "questions", "options", "both"}:
        raise AppError("invalid_input")
    return result


def normalize_questions(items):
    if not isinstance(items, list) or not 1 <= len(items) <= 100:
        raise AppError("invalid_questions")
    result = []
    for item in items:
        if not isinstance(item, dict):
            raise AppError("invalid_questions")
        text = str(item.get("question", "")).strip()
        options = item.get("options")
        if not text or len(text) > 300 or not isinstance(options, list) or len(options) != 4:
            raise AppError("invalid_questions")
        if any(not isinstance(o, str) or not o.strip() or len(o.strip()) > 100 for o in options):
            raise AppError("invalid_questions")
        explanation = str(item.get("explanation", "")).strip()
        if len(explanation) > 200:
            raise AppError("invalid_questions")
        result.append({"question": text, "options": [o.strip() for o in options],
                       "correct_index": number(item.get("correct_index", 0), 0, 3), "explanation": explanation})
    return result


async def ensure_user(user):
    await db.get_or_create_user(user["id"], " ".join(filter(None, [user.get("first_name"), user.get("last_name")])), user.get("username"))


async def check_subscription(request):
    if REQUIRED_CHANNEL_ENABLED and REQUIRED_CHANNEL:
        try:
            member = await request.app[STATE]["bot"].get_chat_member(REQUIRED_CHANNEL, request[USER]["id"])
            joined = member.status in {"creator", "administrator", "member"} or (member.status == "restricted" and member.is_member)
        except Exception as exc:
            logging.warning("Mini App subscription check failed: %s", type(exc).__name__)
            raise AppError("subscription_check_failed", 503) from exc
        if not joined:
            raise AppError("subscription_required", 403)


async def profile(request):
    from services.support_inbox import inbox, is_admin
    user = request[USER]
    await ensure_user(user)
    # Settings may be inserted on first access. Finish that transaction before
    # opening other sessions (SQLite may share a single underlying connection).
    user_settings = await db.get_or_create_user_settings(user["id"])
    quota = await db.get_user_quota_status(user["id"])
    referral = await db.get_referral_status(user["id"])
    saved_profile = await public_profile(user)
    support_status = await inbox.summary(user["id"])
    state = request.app[STATE]
    if not state["username"]:
        state["username"] = (await state["bot"].get_me()).username
    # Keep the same prices and limits as the existing bot.
    from handlers.user import _PREMIUM_PLANS

    return web.json_response({"user": user, "profile": saved_profile, "settings": user_settings, "quota": quota, "referral": referral,
                              "bot_username": state["username"], "plans": _PREMIUM_PLANS, "ai_enabled": AI_ENABLED,
                              "required_channel": REQUIRED_CHANNEL if REQUIRED_CHANNEL_ENABLED else "",
                              "is_admin": is_admin(user["id"]), "support": support_status})


async def app_info(request):
    """Public bot identity for reopening a Mini App whose login was rejected."""
    state = request.app[STATE]
    if not state["username"]:
        try:
            state["username"] = (await asyncio.wait_for(state["bot"].get_me(), timeout=5)).username
        except Exception:
            logging.warning("Mini App bot identity unavailable")
            return web.json_response({"error": "server_error"}, status=503, headers={"Cache-Control": "no-store"})
    return web.json_response({"name": "Create Quiz", "bot_username": state["username"]},
                             headers={"Cache-Control": "no-store"})


async def public_profile(user):
    saved = await db.get_web_profile(user["id"])
    name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")]))
    return {"display_name": (saved["display_name"] or name) if saved else name,
            "username": saved["username"] if saved else "",
            "has_avatar": bool(saved and saved["avatar_path"]), "updated_at": saved["updated_at"] if saved else ""}


async def edit_profile(request):
    data = await read_json(request)
    name, username = validate_profile(data)
    await ensure_user(request[USER])
    async with request.app[STATE]["profile_lock"]:
        try:
            await db.update_web_profile(request[USER]["id"], display_name=name, username=username)
        except IntegrityError as exc:
            raise AppError("username_taken", 409) from exc
    return web.json_response({"profile": await public_profile(request[USER])})


async def upload_avatar(request):
    reader = await request.multipart()
    part = await reader.next()
    if part is None or part.name != "avatar":
        raise AppError("invalid_avatar")
    raw = bytearray()
    while chunk := await part.read_chunk():
        raw.extend(chunk)
        if len(raw) > 5 * 1024 * 1024:
            raise AppError("too_large", 413)
    if await reader.next() is not None:
        raise AppError("invalid_input")
    normalized = await asyncio.to_thread(normalize_avatar, bytes(raw))
    await ensure_user(request[USER])
    async with request.app[STATE]["profile_lock"]:
        AVATARS.mkdir(parents=True, exist_ok=True)
        path = AVATARS / f"{uuid.uuid4().hex}.png"
        try:
            await asyncio.to_thread(path.write_bytes, normalized)
            old = await db.update_web_profile(request[USER]["id"], avatar_path=str(path))
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        remove_avatar(old, AVATARS)
    return web.json_response({"profile": await public_profile(request[USER])})


async def get_avatar(request):
    profile = await db.get_web_profile(request[USER]["id"])
    if not profile or not profile["avatar_path"]:
        raise AppError("not_found", 404)
    path = Path(profile["avatar_path"]).resolve()
    if not path.is_relative_to(AVATARS.resolve()) or not path.is_file():
        raise AppError("not_found", 404)
    return web.FileResponse(path)


async def delete_avatar(request):
    await ensure_user(request[USER])
    async with request.app[STATE]["profile_lock"]:
        old = await db.update_web_profile(request[USER]["id"], avatar_path="")
        remove_avatar(old, AVATARS)
    return web.json_response({"profile": await public_profile(request[USER])})


async def send_support(request):
    data = await read_json(request)
    await ensure_user(request[USER])
    await support_service.send(request.app[STATE]["bot"], request[USER], data.get("message"), source="web", persist=True)
    return web.json_response({"ok": True})


async def update_settings(request):
    data = await read_json(request)
    lang = data.get("ui_lang")
    if lang not in LANGUAGES - {"source"}:
        raise AppError("invalid_input")
    await ensure_user(request[USER])
    await db.set_user_ui_lang(request[USER]["id"], lang)
    from handlers.user import _set_ui_lang_cache

    _set_ui_lang_cache(request[USER]["id"], lang)
    return web.json_response({"ok": True})


async def list_quizzes(request):
    before = number(request.query.get("before", "0"), 0, 2**63 - 1)
    rows = await db.list_user_quizzes(request[USER]["id"], limit=20, before_id=before)
    return web.json_response({"quizzes": rows, "next": rows[-1]["id"] if len(rows) == 20 else None})


async def get_quiz(request):
    quiz = await db.get_quiz_with_questions(number(request.match_info["id"], 1, 2**63 - 1))
    if not quiz or quiz["creator_id"] != request[USER]["id"]:
        raise AppError("not_found", 404)
    # Local filesystem paths and Telegram file IDs are not browser URLs.
    for q in quiz["questions"]:
        q["has_image"] = bool(q.get("image_path"))
        q.pop("image_path", None)
        q.pop("image_file_id", None)
    return web.json_response(quiz)


async def get_question_image(request):
    quiz = await db.get_quiz_with_questions(number(request.match_info["id"], 1, 2**63 - 1))
    if not quiz or quiz["creator_id"] != request[USER]["id"]:
        raise AppError("not_found", 404)
    question_id = number(request.match_info["question_id"], 1, 2**63 - 1)
    question = next((q for q in quiz["questions"] if q["question_id"] == question_id), None)
    if not question or not question.get("image_path"):
        raise AppError("not_found", 404)
    path = Path(question["image_path"]).resolve()
    if not path.is_relative_to((STATIC.parent / "media").resolve()) or not path.is_file():
        raise AppError("not_found", 404)
    return web.FileResponse(path)


async def save_quiz(user_id, title, questions, opts, *, is_ai=False, image_paths=None):
    """Store quiz and questions in one transaction to avoid empty saved quizzes."""
    questions = normalize_questions(questions)
    if image_paths:
        for question, image in zip(questions, image_paths):
            question["image_path"] = image
    mode = opts["shuffle_mode"]
    if mode in {"questions", "both"}:
        random.shuffle(questions)
    if mode in {"options", "both"}:
        for q in questions:
            indices = list(range(4))
            random.shuffle(indices)
            q["correct_index"] = indices.index(q["correct_index"])
            q["options"] = [q["options"][i] for i in indices]
    async with db.async_session() as session:
        async with session.begin():
            quiz = db.Quiz(title=title[:200], creator_id=user_id, is_ai_generated=is_ai,
                           open_period=opts["open_period"], shuffle_mode=mode, shuffle_strategy="saved")
            session.add(quiz)
            await session.flush()
            session.add_all([db.Question(quiz_id=quiz.id, text=q["question"], options=json.dumps(q["options"], ensure_ascii=False),
                                         correct_answer=q["correct_index"], explanation=q["explanation"],
                                         image_path=q.get("image_path", "")) for q in questions])
        return quiz.id


async def create_manual(request):
    data = await read_json(request)
    title = str(data.get("title", "")).strip()
    if not title or len(title) > 200:
        raise AppError("invalid_input")
    questions = normalize_questions(data.get("questions"))
    opts = settings(data)
    await check_subscription(request)
    await ensure_user(request[USER])
    quiz_id = await save_quiz(request[USER]["id"], title, questions, opts)
    return web.json_response({"quiz_id": quiz_id}, status=201)


def prune_jobs(state):
    now = time.monotonic()
    for key, job in list(state["jobs"].items()):
        if job["status"] != "working" and now - job["created"] > 3600:
            del state["jobs"][key]


def check_job_capacity(state, user_id):
    prune_jobs(state)
    if any(j["user_id"] == user_id and j["status"] == "working" for j in state["jobs"].values()):
        raise AppError("already_working", 409)
    if len(state["jobs"]) >= 1000 or sum(j["status"] == "working" for j in state["jobs"].values()) >= 12:
        raise AppError("busy", 503)


def start_job(request, kind, opts, *, topic="", path=None, temp=None, import_only=False):
    state, user_id = request.app[STATE], request[USER]["id"]
    check_job_capacity(state, user_id)
    job_id = uuid.uuid4().hex
    job = {"user_id": user_id, "status": "working", "created": time.monotonic()}
    state["jobs"][job_id] = job
    task = asyncio.create_task(generate(state, job, kind, opts, topic=topic, path=path, temp=temp, import_only=import_only))
    state["tasks"].add(task)
    task.add_done_callback(state["tasks"].discard)
    return web.json_response({"job_id": job_id}, status=202)


async def create_topic(request):
    if not AI_ENABLED:
        raise AppError("ai_disabled", 403)
    check_job_capacity(request.app[STATE], request[USER]["id"])
    data = await read_json(request)
    topic = str(data.get("topic", "")).strip()
    if not 3 <= len(topic) <= 200:
        raise AppError("invalid_input")
    opts = settings(data)
    await check_subscription(request)
    await ensure_user(request[USER])
    await db.check_user_quota(request[USER]["id"], "topic")
    return start_job(request, "topic", opts, topic=topic)


def validate_archive(path):
    if path.suffix in {".docx", ".pptx", ".xlsx"}:
        try:
            with zipfile.ZipFile(path) as archive:
                entries = archive.infolist()
                if len(entries) > 5000 or sum(item.file_size for item in entries) > 100 * 1024 * 1024:
                    raise AppError("too_large", 413)
        except zipfile.BadZipFile as exc:
            raise AppError("invalid_file") from exc


async def create_file(request):
    state, uid = request.app[STATE], request[USER]["id"]
    check_job_capacity(state, uid)
    if request.content_length and request.content_length > 32 * 1024 * 1024:
        raise AppError("too_large", 413)
    await check_subscription(request)
    reader = await request.multipart()
    folder = STATIC.parent / "downloads" / "webapp"
    folder.mkdir(parents=True, exist_ok=True)
    temp = tempfile.TemporaryDirectory(dir=folder)
    path, data = None, {}
    try:
        async for part in reader:
            if part.name == "options":
                raw = bytearray()
                while chunk := await part.read_chunk():
                    raw.extend(chunk)
                    if len(raw) > 4096:
                        raise AppError("invalid_input")
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise AppError("invalid_input")
            elif part.name == "file" and path is None:
                suffix = Path(part.filename or "").suffix.lower()
                if suffix not in SUFFIXES:
                    raise AppError("unsupported_file")
                max_mb = int(os.getenv("MAX_UPLOAD_PPTX_MB" if suffix == ".pptx" else "MAX_UPLOAD_MB", "25" if suffix == ".pptx" else "5"))
                path = Path(temp.name) / ("upload" + suffix)
                size = 0
                with path.open("wb") as stream:
                    while chunk := await part.read_chunk(64 * 1024):
                        size += len(chunk)
                        if size > max_mb * 1024 * 1024:
                            raise AppError("too_large", 413)
                        stream.write(chunk)
                if not size:
                    raise AppError("invalid_file")
            else:
                raise AppError("invalid_input")
        if path is None:
            raise AppError("invalid_file")
        await asyncio.to_thread(validate_archive, path)
        opts = settings(data)
        # Parsing ready questions is free, as it is in the chat bot.
        import_only = data.get("import_only", False)
        if type(import_only) is not bool:
            raise AppError("invalid_input")
        if not import_only and path.suffix != ".json":
            if not AI_ENABLED:
                raise AppError("ai_disabled", 403)
            await db.check_user_quota(uid, "file")
        await ensure_user(request[USER])
        return start_job(request, "file", opts, path=path, temp=temp, import_only=import_only)
    except BaseException:
        temp.cleanup()
        raise


async def file_questions(ai, path, opts, import_only):
    title = "Fayldan test"
    if path.suffix in {".png", ".jpg", ".jpeg", ".webp"}:
        if import_only:
            raise AppError("image_needs_ai")
        questions = await ai.extract_quiz_from_images([str(path)], max_questions=opts["question_count"], output_language=opts["output_language"], shuffle_options=False)
        return title, questions, True
    if path.suffix == ".xlsx":
        title, ready = await asyncio.to_thread(parse_quiz_xlsx, str(path), title_fallback=title)
        if ready:
            return title, ready[:opts["question_count"]], False
    raw = await asyncio.to_thread(path.read_text, encoding="utf-8-sig") if path.suffix == ".json" else await asyncio.to_thread(extract_text_from_file, str(path))
    parsed_title, ready = parse_quiz_payload(raw, title_fallback=title)
    if ready:
        return parsed_title, ready[:opts["question_count"]], False
    if import_only or path.suffix == ".json":
        raise AppError("no_questions")
    if len(raw.strip()) < 200:
        raise AppError("text_too_short")
    questions = await ai.generate_quiz_from_text(raw, question_count=opts["question_count"], output_language=opts["output_language"], difficulty=opts["difficulty"], shuffle_options=False)
    return title, questions, True


async def generate(state, job, kind, opts, *, topic, path, temp, import_only):
    reservation, saved = None, False
    copied_images = []
    try:
        async with state["semaphore"]:
            async with asyncio.timeout(600):
                ai = AIService()
                if kind == "topic":
                    reservation = await db.reserve_user_quota(job["user_id"], "topic")
                    questions = await ai.generate_quiz_from_topic(topic, question_count=opts["question_count"], output_language=opts["output_language"], difficulty=opts["difficulty"], shuffle_options=False)
                    title, used_ai = topic, True
                else:
                    # Reserve before costly work; refund imports and failed requests.
                    if not import_only and path.suffix != ".json":
                        reservation = await db.reserve_user_quota(job["user_id"], "file")
                    title, questions, used_ai = await file_questions(ai, path, opts, import_only)
                    if not used_ai and reservation:
                        await db.refund_user_quota(reservation)
                        reservation = None
                if not questions:
                    raise AppError("no_questions")
                questions = questions[:opts["question_count"]]
                image_paths = []
                image_copies = {}
                for question in questions:
                    original = Path(question["image_path"]).resolve() if question.get("image_path") and temp else None
                    if original and original.is_relative_to(Path(temp.name).resolve()) and original.is_file():
                        if original not in image_copies:
                            media = STATIC.parent / "media" / "webapp"
                            media.mkdir(parents=True, exist_ok=True)
                            destination = media / f"{uuid.uuid4().hex}{original.suffix}"
                            copied_images.append(destination)
                            await asyncio.to_thread(shutil.copyfile, original, destination)
                            image_copies[original] = str(destination)
                        image_paths.append(image_copies[original])
                    else:
                        image_paths.append("")
                quiz_id = await save_quiz(job["user_id"], title, questions, opts, is_ai=used_ai, image_paths=image_paths)
                saved = True
                job.update(status="done", quiz_id=quiz_id)
    except db.QuotaExceeded:
        job.update(status="failed", error="quota_exceeded")
    except AppError as exc:
        job.update(status="failed", error=exc.code)
    except asyncio.CancelledError:
        job.update(status="failed", error="interrupted")
        raise
    except (AIServiceError, ImportServiceError):
        logging.warning("Mini App generation/provider error", exc_info=True)
        job.update(status="failed", error="generation_failed")
    except Exception:
        logging.exception("Mini App generation failed")
        job.update(status="failed", error="generation_failed")
    finally:
        try:
            if reservation and not saved:
                await db.refund_user_quota(reservation)
        finally:
            if not saved:
                for image in copied_images:
                    image.unlink(missing_ok=True)
            if temp:
                temp.cleanup()


async def job_status(request):
    job = request.app[STATE]["jobs"].get(request.match_info["id"])
    if not job or job["user_id"] != request[USER]["id"]:
        raise AppError("not_found", 404)
    return web.json_response({key: value for key, value in job.items() if key not in {"user_id", "created"}})


async def stop_jobs(app):
    tasks = list(app[STATE]["tasks"])
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def setup_webapp(app, bot):
    from services.interactive_api import setup_interactive
    from services.support_api import setup_support

    setup_interactive(app)
    setup_support(app)
    app[STATE] = {"bot": bot, "username": "", "jobs": {}, "tasks": set(), "rates": defaultdict(deque), "semaphore": asyncio.Semaphore(4), "profile_lock": asyncio.Lock()}
    app.middlewares.append(api_middleware)

    async def index(_):
        return web.FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    app.router.add_get("/app", index)
    app.router.add_get("/app/", index)
    app.router.add_get("/app/config.json", app_info)
    app.router.add_static("/app/assets/", STATIC / "assets", show_index=False)
    app.router.add_get("/api/profile", profile)
    app.router.add_patch("/api/profile", edit_profile)
    app.router.add_get("/api/profile/avatar", get_avatar)
    app.router.add_post("/api/profile/avatar", upload_avatar)
    app.router.add_delete("/api/profile/avatar", delete_avatar)
    app.router.add_patch("/api/settings", update_settings)
    app.router.add_post("/api/support", send_support)
    app.router.add_get("/api/quizzes", list_quizzes)
    app.router.add_get("/api/quizzes/{id}", get_quiz)
    app.router.add_get("/api/quizzes/{id}/images/{question_id}", get_question_image)
    app.router.add_post("/api/quizzes/manual", create_manual)
    app.router.add_post("/api/quizzes/topic", create_topic)
    app.router.add_post("/api/quizzes/file", create_file)
    app.router.add_get("/api/jobs/{id}", job_status)
    app.on_cleanup.append(stop_jobs)
