"""Owner-only conversations and an admin-only web inbox."""

from aiohttp import web

from services.support_inbox import inbox, require_admin
from services.webapp import USER, number, read_json


def cursor(request, key):
    return number(request.query.get(key, "0"), 0, 2**63 - 1)


async def messages(request):
    return web.json_response(await inbox.messages(request[USER]["id"], after=cursor(request, "after"), before=cursor(request, "before")))


async def threads(request):
    require_admin(request[USER]["id"])
    return web.json_response(await inbox.threads(request[USER]["id"], before=cursor(request, "before")))


async def admin_messages(request):
    admin_id = request[USER]["id"]
    require_admin(admin_id)
    user_id = number(request.match_info["id"], 1, 2**63 - 1)
    return web.json_response(await inbox.messages(user_id, admin_id=admin_id, after=cursor(request, "after"), before=cursor(request, "before")))


async def reply(request):
    admin_id = request[USER]["id"]
    require_admin(admin_id)
    user_id = number(request.match_info["id"], 1, 2**63 - 1)
    data = await read_json(request)
    await inbox.reply(admin_id, user_id, data.get("message"))
    return web.json_response({"ok": True})


def setup_support(app):
    app.router.add_get("/api/support/messages", messages)
    app.router.add_get("/api/admin/support", threads)
    app.router.add_get("/api/admin/support/{id}/messages", admin_messages)
    app.router.add_post("/api/admin/support/{id}/messages", reply)
