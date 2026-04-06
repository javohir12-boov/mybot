from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from aiogram import types
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from config import ADMIN_IDS
from handlers.utils.i18n import norm_ui_lang, t
from services.database import get_or_create_user, get_or_create_user_settings


@dataclass
class _Bucket:
    tokens: float
    updated_at: float
    strikes: int = 0
    blocked_until: float = 0.0


class SecurityMiddleware(BaseMiddleware):
    """Basic security middleware.

    - Anti-flood rate limiting (per-user, in-memory)
    - Best-effort user registration (DB) with caching

    This is not a replacement for VPS security (firewall, HTTPS termination, etc.),
    but it reduces bot-side abuse.
    """

    def __init__(
        self,
        *,
        rate_per_sec: float = 1.5,
        burst: int = 6,
        block_seconds: int = 10,
        warn_every_sec: int = 4,
        user_refresh_ttl_sec: int = 3600,
        ui_lang_cache_ttl_sec: int = 600,
    ) -> None:
        super().__init__()
        self.rate = max(0.1, float(rate_per_sec))
        self.burst = max(1, int(burst))
        self.block_seconds = max(1, int(block_seconds))
        self.warn_every_sec = max(1, int(warn_every_sec))
        self.user_refresh_ttl_sec = max(60, int(user_refresh_ttl_sec))
        self.ui_lang_cache_ttl_sec = max(60, int(ui_lang_cache_ttl_sec))

        self._buckets: Dict[int, _Bucket] = {}
        self._last_warn: Dict[int, float] = {}
        self._last_user_refresh: Dict[int, float] = {}
        self._ui_lang: Dict[int, Tuple[str, float]] = {}

        self._gc_ops = 0

    def _gc(self) -> None:
        # Best-effort cleanup to avoid unbounded memory growth.
        self._gc_ops += 1
        if self._gc_ops % 5000 != 0:
            return
        now = time.monotonic()
        for d in (self._buckets, self._last_warn, self._last_user_refresh, self._ui_lang):
            for k, v in list(d.items()):
                ts = v.updated_at if isinstance(v, _Bucket) else (v[1] if isinstance(v, tuple) else v)
                if now - float(ts) > 24 * 60 * 60:
                    d.pop(k, None)

    def _allow(self, user_id: int) -> Tuple[bool, int]:
        now = time.monotonic()
        b = self._buckets.get(user_id)
        if b is None:
            b = _Bucket(tokens=float(self.burst), updated_at=now)
            self._buckets[user_id] = b

        if b.blocked_until and now < b.blocked_until:
            return False, max(1, int(round(b.blocked_until - now)))

        elapsed = max(0.0, now - float(b.updated_at))
        b.updated_at = now
        b.tokens = min(float(self.burst), float(b.tokens) + elapsed * float(self.rate))

        if b.tokens >= 1.0:
            b.tokens -= 1.0
            b.strikes = 0
            return True, 0

        # No tokens left.
        b.strikes += 1
        if b.strikes >= 5:
            b.blocked_until = now + float(self.block_seconds)
            b.strikes = 0
            return False, int(self.block_seconds)

        return False, 2

    def _get_ui_lang_cached(self, user_id: int) -> str:
        now = time.monotonic()
        item = self._ui_lang.get(user_id)
        if not item:
            return 'uz'
        lang, ts = item
        if now - float(ts) > float(self.ui_lang_cache_ttl_sec):
            return 'uz'
        return norm_ui_lang(lang)

    async def _warn_rate_limited(self, event: Any, *, user_id: int, wait_sec: int) -> None:
        now = time.monotonic()
        last = float(self._last_warn.get(user_id, 0.0) or 0.0)
        if now - last < float(self.warn_every_sec):
            return
        self._last_warn[user_id] = now

        ui_lang = self._get_ui_lang_cached(user_id)
        msg = t(ui_lang, 'rate_limited', sec=int(max(1, wait_sec)))

        try:
            if isinstance(event, types.CallbackQuery):
                await event.answer(msg, show_alert=False)
            elif isinstance(event, types.Message):
                await event.answer(msg)
        except Exception:
            # Never fail the handler chain due to warning delivery.
            pass

    async def _refresh_user(self, user: types.User) -> None:
        uid = int(user.id)
        now = time.monotonic()
        last = float(self._last_user_refresh.get(uid, 0.0) or 0.0)
        if now - last < float(self.user_refresh_ttl_sec):
            return

        self._last_user_refresh[uid] = now
        try:
            await get_or_create_user(uid, full_name=str(user.full_name or ''), username=str(getattr(user, 'username', '') or ''))
            settings = await get_or_create_user_settings(uid)
            ui_lang = norm_ui_lang(str((settings or {}).get('ui_lang') or 'uz'))
            self._ui_lang[uid] = (ui_lang, now)
        except Exception:
            pass

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        user: Optional[types.User] = None
        try:
            user = data.get('event_from_user')
        except Exception:
            user = None

        if not user:
            return await handler(event, data)

        uid = int(user.id)
        if uid in set(ADMIN_IDS or []):
            # Admins are trusted; avoid blocking them by rate limit.
            await self._refresh_user(user)
            self._gc()
            return await handler(event, data)

        allowed, wait_sec = self._allow(uid)
        if not allowed:
            await self._warn_rate_limited(event, user_id=uid, wait_sec=wait_sec)
            self._gc()
            return None

        await self._refresh_user(user)
        self._gc()
        return await handler(event, data)
