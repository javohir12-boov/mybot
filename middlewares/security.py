from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from aiogram import types
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from config import ADMIN_IDS, REQUIRED_CHANNEL
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
        required_channel: str = REQUIRED_CHANNEL,
        sub_cache_ttl_sec: int = 300,
        sub_prompt_every_sec: float = 0,
    ) -> None:
        super().__init__()
        self.rate = max(0.1, float(rate_per_sec))
        self.burst = max(1, int(burst))
        self.block_seconds = max(1, int(block_seconds))
        self.warn_every_sec = max(1, int(warn_every_sec))
        self.user_refresh_ttl_sec = max(60, int(user_refresh_ttl_sec))
        self.ui_lang_cache_ttl_sec = max(60, int(ui_lang_cache_ttl_sec))

        self.required_channel = str(required_channel or "").strip()
        self.sub_cache_ttl_sec = max(5, int(sub_cache_ttl_sec))
        self.sub_prompt_every_sec = max(0.0, float(sub_prompt_every_sec))

        # Normalize channel username/id for get_chat_member.
        ch = self.required_channel
        if ch.startswith("https://t.me/"):
            ch = "@" + ch.rsplit("/", 1)[-1].split("?", 1)[0]
        elif ch.startswith("t.me/"):
            ch = "@" + ch.rsplit("/", 1)[-1].split("?", 1)[0]
        if ch and not ch.startswith("@") and not str(ch).lstrip("-").isdigit():
            ch = "@" + ch.lstrip("@")
        self.required_channel = ch

        self._buckets: Dict[int, _Bucket] = {}
        self._last_warn: Dict[int, float] = {}
        self._last_user_refresh: Dict[int, float] = {}
        self._ui_lang: Dict[int, Tuple[str, float]] = {}
        self._sub_cache: Dict[int, Tuple[bool, float]] = {}
        self._last_sub_prompt: Dict[int, float] = {}

        self._gc_ops = 0

    def _gc(self) -> None:
        # Best-effort cleanup to avoid unbounded memory growth.
        self._gc_ops += 1
        if self._gc_ops % 5000 != 0:
            return
        now = time.monotonic()
        for d in (self._buckets, self._last_warn, self._last_user_refresh, self._ui_lang, self._sub_cache, self._last_sub_prompt):
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

    def _required_channel_url(self) -> str:
        ch = str(self.required_channel or "").strip()
        if not ch:
            return ""
        if ch.startswith("https://") or ch.startswith("http://"):
            return ch
        if ch.startswith("@"):
            return "https://t.me/" + ch[1:]
        if ch.startswith("t.me/"):
            return "https://" + ch
        return ""

    def _sub_keyboard(self, ui_lang: str) -> types.InlineKeyboardMarkup:
        url = self._required_channel_url()
        join_text = t(ui_lang, "btn_join_channel")
        check_text = t(ui_lang, "btn_check_sub")
        rows = []
        if url:
            rows.append([types.InlineKeyboardButton(text=join_text, url=url)])
        else:
            # Fallback: no URL can be built (e.g., numeric chat id).
            rows.append([types.InlineKeyboardButton(text=join_text, callback_data="check_sub")])
        rows.append([types.InlineKeyboardButton(text=check_text, callback_data="check_sub")])
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    async def _is_subscribed(self, bot: Any, user_id: int, *, force: bool = False) -> bool:
        if not str(self.required_channel or "").strip():
            return True
        now = time.monotonic()
        cached = self._sub_cache.get(int(user_id))
        if cached and (not force):
            ok, ts = bool(cached[0]), float(cached[1])
            ttl = float(self.sub_cache_ttl_sec) if ok else min(15.0, float(self.sub_cache_ttl_sec))
            if now - ts < ttl:
                return ok

        ok = False
        try:
            member = await bot.get_chat_member(chat_id=self.required_channel, user_id=int(user_id))
            status = str(getattr(member, "status", "") or "").lower()
            ok = status in {"creator", "administrator", "member"}
        except Exception:
            ok = False
        self._sub_cache[int(user_id)] = (ok, now)
        return ok

    async def _prompt_must_join(self, event: Any, *, user_id: int) -> None:
        now = time.monotonic()
        if float(self.sub_prompt_every_sec) > 0:
            last = float(self._last_sub_prompt.get(int(user_id), 0.0) or 0.0)
            if now - last < float(self.sub_prompt_every_sec):
                return
            self._last_sub_prompt[int(user_id)] = now

        ui_lang = self._get_ui_lang_cached(int(user_id))
        ch = str(self.required_channel or "").strip()
        text = t(ui_lang, "must_join_channel", channel=ch)
        kb = self._sub_keyboard(ui_lang)

        try:
            if isinstance(event, types.CallbackQuery):
                try:
                    await event.answer(t(ui_lang, "sub_required_alert"), show_alert=False)
                except Exception:
                    pass
                if getattr(event, "message", None):
                    await event.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            elif isinstance(event, types.Message):
                await event.answer(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
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

    def _is_ui_language_event(self, event: Any) -> bool:
        """Allow language selection without mandatory channel subscription."""
        try:
            if isinstance(event, types.CallbackQuery):
                data = str(getattr(event, "data", "") or "")
                return data == "menu_ui_language" or data.startswith("set_ui_lang:")
            if isinstance(event, types.Message):
                text = str(getattr(event, "text", "") or "").strip()
                if not text.startswith("/"):
                    return False
                cmd = text.split(maxsplit=1)[0].split("@", 1)[0].lower()
                return cmd in {"/start", "/til", "/lang"}
        except Exception:
            return False
        return False

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
        is_admin = uid in set(ADMIN_IDS or [])

        await self._refresh_user(user)

        # Mandatory channel subscription gate (optional).
        is_check_sub = isinstance(event, types.CallbackQuery) and str(getattr(event, "data", "") or "") == "check_sub"
        is_ui_lang = self._is_ui_language_event(event)
        if (not is_ui_lang) and str(self.required_channel or "").strip():
            bot = None
            try:
                bot = data.get("bot") or getattr(event, "bot", None)
            except Exception:
                bot = None

            if bot is None:
                await self._prompt_must_join(event, user_id=uid)
                self._gc()
                return None

            # Allow the explicit check callback to refresh cached membership status.
            if is_check_sub:
                try:
                    await self._is_subscribed(bot, uid, force=True)
                except Exception:
                    pass
            else:
                subscribed = await self._is_subscribed(bot, uid)
                if not subscribed:
                    await self._prompt_must_join(event, user_id=uid)
                    self._gc()
                    return None

        # Rate limit: skip for admins and the subscription check button,
        # enforce for everyone else who has already passed the join gate.
        if not is_admin and not is_check_sub:
            allowed, wait_sec = self._allow(uid)
            if not allowed:
                await self._warn_rate_limited(event, user_id=uid, wait_sec=wait_sec)
                self._gc()
                return None

        self._gc()
        return await handler(event, data)
