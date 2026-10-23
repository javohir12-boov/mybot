"""One paced, cancellable broadcast at a time, with durable delivery totals."""

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from aiogram import types
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from sqlalchemy import func, select

from handlers.utils.i18n import t
from services import database as db


class BroadcastBusy(Exception):
    pass


@dataclass
class Progress:
    id: str
    admin_id: int
    mode: str
    total: int
    upper: int
    cutoff: str
    sent: int = 0
    failed: int = 0
    blocked: int = 0
    batches: int = 0
    status: str = "sending"

    @property
    def remaining(self):
        return max(0, self.total - self.sent - self.failed)


def audience_query(mode, cutoff):
    if mode == "users":
        return select(db.User.id.label("target")).where(db.User.id > 0, db.User.created_at <= cutoff)
    return select(db.QuizAttempt.chat_id.label("target")).where(
        db.QuizAttempt.chat_id != 0, db.QuizAttempt.chat_type.in_(["group", "supergroup"]),
        db.QuizAttempt.completed_at <= cutoff,
    ).distinct()


async def audience_snapshot(mode):
    cutoff = datetime.now(timezone.utc).isoformat()
    targets = audience_query(mode, cutoff).subquery()
    async with db.async_session() as session:
        row = (await session.execute(select(func.count(), func.max(targets.c.target)).select_from(targets))).one()
    return int(row[0]), int(row[1] or 0), cutoff


async def audience_pages(progress, size):
    cursor = None
    while True:
        query = audience_query(progress.mode, progress.cutoff)
        targets = query.subquery()
        query = select(targets.c.target).where(targets.c.target <= progress.upper)
        if cursor is not None:
            query = query.where(targets.c.target > cursor)
        async with db.async_session() as session:
            page = list((await session.execute(query.order_by(targets.c.target).limit(size))).scalars())
        if not page:
            return
        yield page
        cursor = page[-1]


async def persist(progress, *, create=False):
    async with db.async_session() as session:
        row = db.BroadcastRun(id=progress.id, admin_id=progress.admin_id, mode=progress.mode,
                              created_at=progress.cutoff) if create else await session.get(db.BroadcastRun, progress.id)
        if row is None:
            raise RuntimeError("Missing broadcast run")
        if create:
            session.add(row)
        for key in ("status", "total", "sent", "failed", "blocked", "batches"):
            setattr(row, key, getattr(progress, key))
        if progress.status != "sending":
            row.finished_at = datetime.now(timezone.utc).isoformat()
        await session.commit()


class BroadcastManager:
    def __init__(self, *, batch_size=None, message_delay=None, batch_pause=None):
        self.batch_size = max(1, min(100, int(batch_size or os.getenv("BROADCAST_BATCH_SIZE", "25"))))
        self.message_delay = max(0.05, float(message_delay if message_delay is not None else os.getenv("BROADCAST_MESSAGE_DELAY", "0.1")))
        self.batch_pause = max(1.0, float(batch_pause if batch_pause is not None else os.getenv("BROADCAST_BATCH_PAUSE", "2")))
        self.task = None
        self.progress = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()

    @property
    def busy(self):
        return self.task is not None and not self.task.done()

    async def start(self, *, bot, message, status_message, mode, admin_id, ui_lang):
        async with self._lock:
            if self.busy:
                raise BroadcastBusy()
            if mode not in {"users", "groups"}:
                raise ValueError("Invalid audience")
            total, upper, cutoff = await audience_snapshot(mode)
            if not total:
                return None
            progress = Progress(uuid.uuid4().hex, admin_id, mode, total, upper, cutoff)
            await persist(progress, create=True)
            self._stop = asyncio.Event()
            self.progress = progress
            self.task = asyncio.create_task(self._run(bot, message, status_message, progress, ui_lang))
            return progress

    def stop(self, admin_id):
        if not self.busy or self.progress.admin_id != admin_id:
            return False
        self._stop.set()
        return True

    async def _pause(self, seconds):
        if self._stop.is_set():
            return True
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _deliver(self, bot, message, target):
        for attempt in range(4):
            if self._stop.is_set():
                return "stopped"
            try:
                await bot.copy_message(chat_id=int(target), from_chat_id=message.chat.id, message_id=message.message_id)
                return "sent"
            except TelegramRetryAfter as exc:
                if attempt == 3:
                    return "failed"
                # 429 confirms rejection. Retry the same recipient after Telegram's delay.
                if await self._pause(float(exc.retry_after) + 0.2):
                    return "stopped"
            except TelegramForbiddenError:
                return "blocked"
            except Exception as exc:
                # A network timeout has uncertain delivery; do not risk duplicate messages.
                logging.warning("Broadcast delivery failed for %s: %s", target, type(exc).__name__)
                return "failed"
        return "failed"

    async def _show(self, status_message, progress, lang):
        keys = {"sending": "admin_broadcast_progress", "done": "admin_broadcast_done",
                "cancelled": "admin_broadcast_stopped", "interrupted": "admin_broadcast_interrupted"}
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text=t(lang, "btn_admin_broadcast_stop"), callback_data="admin_broadcast:stop")
        ]]) if progress.status == "sending" else types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text=t(lang, "btn_admin_users"), callback_data="menu_admin_users")
        ]])
        try:
            await status_message.edit_text(t(lang, keys[progress.status], total=progress.total, sent=progress.sent,
                                           failed=progress.failed, blocked=progress.blocked,
                                           remaining=progress.remaining, batches=progress.batches), reply_markup=keyboard)
        except Exception:
            logging.warning("Could not update broadcast progress message")

    async def _run(self, bot, message, status_message, progress, lang):
        try:
            await self._show(status_message, progress, lang)
            async for page in audience_pages(progress, self.batch_size):
                if self._stop.is_set():
                    break
                progress.batches += 1
                for target in page:
                    result = await self._deliver(bot, message, target)
                    if result == "stopped":
                        break
                    if result == "sent":
                        progress.sent += 1
                    else:
                        progress.failed += 1
                        progress.blocked += int(result == "blocked")
                    if await self._pause(self.message_delay):
                        break
                progress.total = max(progress.total, progress.sent + progress.failed)
                await persist(progress)
                await self._show(status_message, progress, lang)
                if self._stop.is_set():
                    break
                if progress.remaining and await self._pause(self.batch_pause):
                    break
            progress.status = "cancelled" if self._stop.is_set() else "done"
            if progress.status == "done":
                progress.total = progress.sent + progress.failed
        except asyncio.CancelledError:
            progress.status = "interrupted"
            raise
        except Exception:
            progress.status = "interrupted"
            logging.exception("Broadcast interrupted")
        finally:
            try:
                await persist(progress)
            except Exception:
                logging.exception("Could not persist final broadcast totals")
            await self._show(status_message, progress, lang)

    async def shutdown(self):
        if self.busy:
            self._stop.set()
            try:
                await asyncio.wait_for(asyncio.shield(self.task), timeout=10)
            except asyncio.TimeoutError:
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)


broadcast_manager = BroadcastManager()
