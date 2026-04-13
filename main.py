import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage

from config import (
    AI_ENABLED,
    BOT_TOKEN,
    WEBHOOK_PATH,
    WEBHOOK_SECRET_TOKEN,
    WEBHOOK_URL,
    WEB_SERVER_HOST,
    WEB_SERVER_PORT,
)
from handlers.user import router as user_router
from middlewares.security import SecurityMiddleware
from services.database import init_db


def _setup_logging() -> None:
    level_name = str(os.getenv("LOG_LEVEL", "INFO")).upper().strip()
    level = getattr(logging, level_name, logging.INFO)

    log_file = str(os.getenv("LOG_FILE", "logs/bot.log")).strip() or "logs/bot.log"
    handlers = [logging.StreamHandler()]

    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_file,
                maxBytes=int(os.getenv("LOG_MAX_BYTES", "2000000") or 2000000),
                backupCount=int(os.getenv("LOG_BACKUP_COUNT", "3") or 3),
                encoding="utf-8",
            )
        )
    except Exception:
        # If file logging fails, keep stdout logging.
        pass

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


async def _setup_commands(bot: Bot) -> None:
    commands = [
        types.BotCommand(command="menu", description="Menu"),
        types.BotCommand(command="til", description="Interface language"),
        types.BotCommand(command="newquiz", description="Create quiz manually"),
        types.BotCommand(command="mytests", description="My quizzes"),
        types.BotCommand(command="cancel", description="Stop active quiz"),
        types.BotCommand(command="premium", description="Premium"),
    ]
    if AI_ENABLED:
        # Keep only one command for topic quizzes.
        commands.append(types.BotCommand(command="topic", description="AI quiz by topic"))

    # Show commands in Telegram's Menu (both private and group chats).
    try:
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(commands, scope=types.BotCommandScopeAllGroupChats())
        await bot.set_my_commands(commands)
    except Exception as exc:
        logging.warning("Bot commands set failed: %s", exc)

    # Ensure the left "Menu" button opens commands (not a web app).
    try:
        await bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())
    except Exception:
        pass


async def _run_health_server(*, host: str, port: int) -> object:
    """Small HTTP server for Render health checks."""

    from aiohttp import web

    app = web.Application()

    async def _health(_: web.Request) -> web.Response:
        return web.Response(text='ok')

    app.router.add_get('/', _health)
    app.router.add_get('/healthz', _health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=str(host or '0.0.0.0'), port=int(port or 8080))
    await site.start()
    logging.info('Health server started on %s:%s', host, port)
    return runner

def _pick_keepalive_url() -> str:
    """Best-effort URL to ping periodically to reduce Render idling."""

    url = str(os.getenv('KEEPALIVE_URL', '') or '').strip()
    if not url:
        url = str(WEBHOOK_URL or '').strip()
    if not url:
        url = str(os.getenv('RENDER_EXTERNAL_URL', '') or '').strip()

    url = url.strip().rstrip('/')

    # If we have an external URL, prefer pinging it (counts as an incoming request).
    if url:
        if not (url.startswith('http://') or url.startswith('https://')):
            url = 'https://' + url
        return url + '/healthz'

    # Fallback: local ping.
    return f"http://127.0.0.1:{int(WEB_SERVER_PORT or 8080)}/healthz"


async def _keepalive_loop(url: str, *, interval_sec: int = 840) -> None:
    """Ping a URL periodically. Interval default is 14 minutes."""

    try:
        import aiohttp
    except Exception:
        return

    interval_sec = int(interval_sec or 840)
    interval_sec = max(60, min(3600, interval_sec))

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Initial small delay to avoid slowing down startup.
        await asyncio.sleep(5)
        while True:
            try:
                async with session.get(str(url), allow_redirects=True) as resp:
                    # Consume body to reuse connection.
                    await resp.text()
                logging.debug('keepalive ok: %s', url)
            except Exception as exc:
                logging.debug('keepalive failed: %s', exc)
            await asyncio.sleep(interval_sec)

async def _run_polling(bot: Bot, dp: Dispatcher) -> None:
    logging.info("Bot started (polling)")
    await dp.start_polling(bot)


async def _run_webhook(bot: Bot, dp: Dispatcher) -> None:
    from aiohttp import web
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

    base = str(WEBHOOK_URL or "").strip().rstrip("/")
    path = str(WEBHOOK_PATH or "/webhook").strip() or "/webhook"
    if not path.startswith("/"):
        path = "/" + path

    secret = str(WEBHOOK_SECRET_TOKEN or "").strip() or None

    app = web.Application()

    async def _health(_: web.Request) -> web.Response:
        return web.Response(text='ok')

    app.router.add_get('/', _health)
    app.router.add_get('/healthz', _health)
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=secret).register(app, path=path)
    setup_application(app, dp, bot=bot)

    async def _on_startup(_: web.Application) -> None:
        await bot.set_webhook(url=f"{base}{path}", secret_token=secret)
        logging.info("Bot started (webhook): %s%s", base, path)

    async def _on_shutdown(_: web.Application) -> None:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass

    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=str(WEB_SERVER_HOST or "0.0.0.0"), port=int(WEB_SERVER_PORT or 8080))
    await site.start()

    # Keep running.
    await asyncio.Event().wait()


async def main() -> None:
    _setup_logging()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Security middleware: anti-flood + best-effort user registration.
    sec = SecurityMiddleware(
        rate_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "1.5") or 1.5),
        burst=int(os.getenv("RATE_LIMIT_BURST", "6") or 6),
        block_seconds=int(os.getenv("RATE_LIMIT_BLOCK_SEC", "10") or 10),
    )
    dp.message.middleware(sec)
    dp.callback_query.middleware(sec)
    dp.poll_answer.middleware(sec)

    # Database init (create tables if not exist)
    await init_db()

    await _setup_commands(bot)

    dp.include_router(user_router)

    health_runner = None
    # Render Web Service requires a listening port. If WEBHOOK_URL is not set,
    # we still start a tiny health server so polling deployments don't fail.
    if (not str(WEBHOOK_URL or '').strip()) and str(os.getenv('PORT') or '').strip():
        try:
            health_runner = await _run_health_server(host=str(WEB_SERVER_HOST or '0.0.0.0'), port=int(WEB_SERVER_PORT or 8080))
        except Exception as exc:
            logging.warning('Health server start failed: %s', exc)
            health_runner = None

    keepalive_task = None
    try:
        auto_on = bool(
            str(os.getenv('RENDER_EXTERNAL_URL') or '').strip()
            or str(os.getenv('PORT') or '').strip()
            or str(WEBHOOK_URL or '').strip()
            or str(os.getenv('KEEPALIVE_URL') or '').strip()
        )
        raw = str(os.getenv('KEEPALIVE_ENABLED', '') or '').strip().lower()
        if raw in {'0', 'false', 'no', 'off'}:
            enabled = False
        elif raw in {'1', 'true', 'yes', 'on'}:
            enabled = True
        else:
            enabled = auto_on

        if enabled:
            url = _pick_keepalive_url()
            interval = int(os.getenv('KEEPALIVE_INTERVAL_SEC', '840') or 840)
            keepalive_task = asyncio.create_task(_keepalive_loop(url, interval_sec=interval))
            logging.info('Keepalive enabled (every %ss): %s', interval, url)
    except Exception as exc:
        logging.warning('Keepalive setup failed: %s', exc)
    try:
        if str(WEBHOOK_URL or '').strip():
            await _run_webhook(bot, dp)
        else:
            await _run_polling(bot, dp)
    finally:
        if keepalive_task is not None:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        if health_runner is not None:
            try:
                await health_runner.cleanup()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot to'xtatildi")

