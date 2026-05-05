from __future__ import annotations

import time
from typing import Any, Awaitable, Callable, Dict

from aiogram import types
from aiogram.dispatcher.middlewares.base import BaseMiddleware


class ThrottlingMiddleware(BaseMiddleware):
    """Simple per-user rate limiter for aiogram 3.x.

    Replaced the old aiogram 2.x implementation which used removed APIs
    (Dispatcher.get_current, current_handler, CancelHandler, Throttled).

    NOTE: SecurityMiddleware in security.py already handles rate limiting for
    all message/callback/poll_answer events — this class is kept for legacy
    compatibility but is NOT registered in main.py to avoid double-limiting.
    """

    def __init__(self, rate_limit: float = 2.0) -> None:
        super().__init__()
        self.rate_limit = max(0.1, float(rate_limit))
        self._last_call: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        user: Any = data.get("event_from_user")
        if user is None:
            return await handler(event, data)

        uid = int(user.id)
        now = time.monotonic()
        last = self._last_call.get(uid, 0.0)

        if now - last < self.rate_limit:
            if isinstance(event, types.Message):
                await event.answer("Juda ko'p xabar yozmang!")
            elif isinstance(event, types.CallbackQuery):
                await event.answer("Juda tez bosyapsiz!", show_alert=False)
            return None

        self._last_call[uid] = now
        return await handler(event, data)
