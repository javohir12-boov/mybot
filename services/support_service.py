"""Deliver support requests from both the Mini App and private bot chats."""

import html
import logging
import time

from config import ADMIN_IDS

MAX_MESSAGE_LENGTH = 3000
COOLDOWN_SECONDS = 60


class SupportError(Exception):
    def __init__(self, code, status=400):
        self.code, self.status = code, status
        super().__init__(code)


class SupportService:
    def __init__(self):
        self._sent = {}
        self._pending = set()

    async def send(self, bot, user, message, *, source):
        if not isinstance(message, str) or not 1 <= len(message.strip()) <= MAX_MESSAGE_LENGTH:
            raise SupportError("invalid_support_message")
        user_id = user["id"]
        now = time.monotonic()
        self._sent = {uid: sent for uid, sent in self._sent.items() if now - sent < COOLDOWN_SECONDS}
        if user_id in self._pending or user_id in self._sent:
            raise SupportError("support_rate_limit", 429)
        admins = list(dict.fromkeys(ADMIN_IDS))
        if not admins:
            raise SupportError("support_unavailable", 503)

        name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")]))[:200] or str(user_id)
        username = str(user.get("username") or "")[:64]
        text = (
            f"<b>Yordam so‘rovi · {'Web ilova' if source == 'web' else 'Bot'}</b>\n"
            f'Foydalanuvchi: <a href="tg://user?id={user_id}">{html.escape(name)}</a>\n'
            f"Telegram ID: <code>{user_id}</code>\n"
            + (f"Username: @{html.escape(username)}\n" if username else "")
            + f"\n{html.escape(message.strip())}"
        )
        self._pending.add(user_id)
        delivered = 0
        try:
            for admin_id in admins:
                try:
                    await bot.send_message(admin_id, text, parse_mode="HTML", disable_web_page_preview=True)
                    delivered += 1
                except Exception as exc:
                    logging.warning("Support delivery to admin %s failed: %s", admin_id, type(exc).__name__)
            if not delivered:
                raise SupportError("support_unavailable", 503)
            self._sent[user_id] = time.monotonic()
        finally:
            self._pending.discard(user_id)


support_service = SupportService()
