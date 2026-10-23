"""Authenticated HTTP endpoints for public tests and shared rooms."""

from functools import wraps
from pathlib import Path

from aiohttp import web

from services import database as db
from services.interactive_service import InteractiveError, InteractiveService
from services.webapp import AppError, STATIC, USER, check_subscription, ensure_user, read_json

SERVICE = web.AppKey("interactive_service", InteractiveService)


def endpoint(handler):
    @wraps(handler)
    async def wrapped(request):
        try:
            return await handler(request)
        except InteractiveError as exc:
            raise AppError(exc.code, exc.status) from exc
    return wrapped


@endpoint
async def lobby(request):
    await ensure_user(request[USER])
    data = await request.app[SERVICE].lobby(request[USER]["id"], category=request.query.get("category", "education"),
                                            subject=request.query.get("subject", "all"))
    return web.json_response(data)


@endpoint
async def publish(request):
    data = await read_json(request)
    await check_subscription(request)
    await ensure_user(request[USER])
    images = None
    if "source_quiz_id" in data:
        source_id = data["source_quiz_id"]
        if type(source_id) is not int or source_id <= 0:
            raise AppError("invalid_input")
        quiz = await db.get_quiz_with_questions(source_id)
        if not quiz or quiz["creator_id"] != request[USER]["id"]:
            raise AppError("not_found", 404)
        data = {**data, "kind": "knowledge", "questions": quiz["questions"],
                "title": data.get("title", quiz["title"][:120]),
                "open_period": data.get("open_period", min(120, max(10, quiz["open_period"])))}
        images = [q.get("image_path", "") for q in quiz["questions"]]
    quiz_id = await request.app[SERVICE].publish(request[USER]["id"], data, trusted_images=images)
    return web.json_response({"quiz_id": quiz_id}, status=201)


@endpoint
async def create_room(request):
    data = await read_json(request)
    quiz_id = data.get("quiz_id")
    if not isinstance(quiz_id, str) or len(quiz_id) != 32:
        raise AppError("invalid_input")
    await check_subscription(request)
    await ensure_user(request[USER])
    room_id = await request.app[SERVICE].create_room(request[USER]["id"], quiz_id)
    return web.json_response({"room_id": room_id}, status=201)


@endpoint
async def join(request):
    await check_subscription(request)
    await ensure_user(request[USER])
    await request.app[SERVICE].join(request[USER]["id"], request.match_info["id"])
    return web.json_response({"ok": True})


@endpoint
async def leave(request):
    await request.app[SERVICE].leave(request[USER]["id"], request.match_info["id"])
    return web.json_response({"ok": True})


@endpoint
async def room(request):
    data = await request.app[SERVICE].room(request[USER]["id"], request.match_info["id"])
    return web.json_response(data)


@endpoint
async def answer(request):
    data = await read_json(request)
    await request.app[SERVICE].answer(request[USER]["id"], request.match_info["id"], data.get("question_index"), data.get("option_index"))
    return web.json_response({"ok": True})


@endpoint
async def image(request):
    try:
        index = int(request.match_info["index"])
    except ValueError as exc:
        raise AppError("not_found", 404) from exc
    path = await request.app[SERVICE].image_path(request[USER]["id"], request.match_info["id"], index)
    if not path:
        raise AppError("not_found", 404)
    resolved = Path(path).resolve()
    if not resolved.is_relative_to((STATIC.parent / "media").resolve()) or not resolved.is_file():
        raise AppError("not_found", 404)
    return web.FileResponse(resolved)


def setup_interactive(app):
    app[SERVICE] = InteractiveService()

    async def lifecycle(application):
        await application[SERVICE].start()
        try:
            yield
        finally:
            await application[SERVICE].close()

    app.cleanup_ctx.append(lifecycle)
    app.router.add_get("/api/interactive", lobby)
    app.router.add_post("/api/interactive/quizzes", publish)
    app.router.add_post("/api/interactive/rooms", create_room)
    app.router.add_post("/api/interactive/rooms/{id}/join", join)
    app.router.add_post("/api/interactive/rooms/{id}/leave", leave)
    app.router.add_get("/api/interactive/rooms/{id}", room)
    app.router.add_post("/api/interactive/rooms/{id}/answers", answer)
    app.router.add_get("/api/interactive/rooms/{id}/images/{index}", image)
