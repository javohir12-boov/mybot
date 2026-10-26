"""Persistent Mini App conversations, with server-checked administrator access."""

import asyncio
import time

from sqlalchemy import func, select

from config import ADMIN_IDS
from services import database as db
from services.support_service import SupportError


def is_admin(user_id):
    return user_id in ADMIN_IDS


def require_admin(user_id):
    if not is_admin(user_id):
        raise SupportError("admin_only", 403)


def clean_message(body):
    if not isinstance(body, str) or not 1 <= len(body.strip()) <= 3000:
        raise SupportError("invalid_support_message")
    return body.strip()


class SupportInbox:
    def __init__(self):
        self.lock = asyncio.Lock()

    async def append_user(self, user_id, body):
        body = clean_message(body)
        async with self.lock, db.async_session() as session:
            thread = await session.get(db.SupportThread, user_id)
            now = time.time()
            if thread and now - thread.last_user_at < 60:
                raise SupportError("support_rate_limit", 429)
            if thread is None:
                thread = db.SupportThread(user_id=user_id)
                session.add(thread)
                await session.flush()
            message = db.SupportMessage(user_id=user_id, sender_id=user_id, from_admin=False, body=body, created_at=now)
            session.add(message)
            await session.flush()
            thread.last_message_id, thread.last_user_at = message.id, now
            await session.commit()

    async def reply(self, admin_id, user_id, body):
        require_admin(admin_id)
        body = clean_message(body)
        async with self.lock, db.async_session() as session:
            thread = await session.get(db.SupportThread, user_id)
            if thread is None:
                raise SupportError("not_found", 404)
            message = db.SupportMessage(user_id=user_id, sender_id=admin_id, from_admin=True,
                                        body=body, created_at=time.time())
            session.add(message)
            await session.flush()
            thread.last_message_id = message.id
            await session.commit()

    async def messages(self, user_id, *, admin_id=None, after=0, before=0):
        if admin_id is not None:
            require_admin(admin_id)
        if after and before:
            raise SupportError("invalid_input")
        async with self.lock, db.async_session() as session:
            thread = await session.get(db.SupportThread, user_id)
            if thread is None:
                return {"messages": [], "next_before": None}
            query = select(db.SupportMessage).where(db.SupportMessage.user_id == user_id)
            if after:
                query = query.where(db.SupportMessage.id > after).order_by(db.SupportMessage.id)
            else:
                if before:
                    query = query.where(db.SupportMessage.id < before)
                query = query.order_by(db.SupportMessage.id.desc())
            rows = list((await session.execute(query.limit(51))).scalars())
            more = len(rows) > 50
            rows = rows[:50]
            if not after:
                rows.reverse()
            if rows:
                key = "admin_read_id" if admin_id is not None else "user_read_id"
                setattr(thread, key, max(getattr(thread, key), max(row.id for row in rows)))
                await session.commit()
            return {"messages": [{"id": row.id, "sender": "admin" if row.from_admin else "user",
                                  "body": row.body, "created_at": row.created_at} for row in rows],
                    "next_before": rows[0].id if more and not after else None}

    @staticmethod
    def unread_query():
        return select(func.count()).select_from(db.SupportMessage).join(
            db.SupportThread, db.SupportThread.user_id == db.SupportMessage.user_id,
        ).where(db.SupportMessage.from_admin.is_(False), db.SupportMessage.id > db.SupportThread.admin_read_id)

    async def summary(self, user_id):
        async with self.lock, db.async_session() as session:
            unread = int((await session.execute(select(func.count()).select_from(db.SupportMessage).join(
                db.SupportThread, db.SupportThread.user_id == db.SupportMessage.user_id,
            ).where(db.SupportMessage.user_id == user_id, db.SupportMessage.from_admin.is_(True),
                    db.SupportMessage.id > db.SupportThread.user_read_id))).scalar() or 0)
            admin_unread = int((await session.execute(self.unread_query())).scalar() or 0) if is_admin(user_id) else 0
            return {"unread": unread, "admin_unread": admin_unread}

    async def threads(self, admin_id, *, before=0):
        require_admin(admin_id)
        async with self.lock, db.async_session() as session:
            unread = select(db.SupportMessage.user_id, func.count().label("unread")).join(
                db.SupportThread, db.SupportThread.user_id == db.SupportMessage.user_id,
            ).where(db.SupportMessage.from_admin.is_(False), db.SupportMessage.id > db.SupportThread.admin_read_id
                    ).group_by(db.SupportMessage.user_id).subquery()
            query = select(db.SupportThread, db.User, db.WebProfile, db.SupportMessage, unread.c.unread).join(
                db.User, db.User.id == db.SupportThread.user_id,
            ).outerjoin(db.WebProfile, db.WebProfile.user_id == db.User.id).join(
                db.SupportMessage, db.SupportMessage.id == db.SupportThread.last_message_id,
            ).outerjoin(unread, unread.c.user_id == db.SupportThread.user_id)
            if before:
                query = query.where(db.SupportThread.last_message_id < before)
            rows = (await session.execute(query.order_by(db.SupportThread.last_message_id.desc()).limit(31))).all()
            return {"threads": [{"user_id": t.user_id, "name": p.display_name if p and p.display_name else u.full_name or str(u.id),
                                 "preview": m.body[:150], "updated_at": m.created_at, "unread": int(count or 0)}
                                for t, u, p, m, count in rows[:30]],
                    "next": rows[29][0].last_message_id if len(rows) > 30 else None,
                    "unread": int((await session.execute(self.unread_query())).scalar() or 0)}


inbox = SupportInbox()
