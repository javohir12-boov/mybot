"""Preview the Mini App without a bot token, database or Telegram connection."""

import argparse
from pathlib import Path

from aiohttp import web


def create_preview():
    root = Path(__file__).resolve().parent.parent / "webapp"
    @web.middleware
    async def revalidate(request, handler):
        response = await handler(request)
        response.headers["Cache-Control"] = "no-cache"
        return response

    app = web.Application(middlewares=[revalidate])

    async def index(_):
        return web.FileResponse(root / "index.html")

    app.router.add_get("/", index)
    app.router.add_get("/app", index)
    app.router.add_get("/app/", index)
    app.router.add_static("/app/assets/", root / "assets")
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    print(f"Mini App preview: http://127.0.0.1:{args.port}/app", flush=True)
    web.run_app(create_preview(), host="127.0.0.1", port=args.port)
