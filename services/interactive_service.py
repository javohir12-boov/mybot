"""Persistent shared quizzes. Deadlines belong to the server, never the host."""

import asyncio
import json
import logging
import time
import uuid

from sqlalchemy import func, select

from services import database as db
from services.interactive_starters import STARTERS

CATEGORIES = ("education", "love", "friendship", "interests", "other")
SUBJECTS = ("general", "math", "history", "science", "languages", "technology")
ONLINE_SECONDS = 45


class InteractiveError(Exception):
    def __init__(self, code, status=400):
        self.code, self.status = code, status


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise InteractiveError("invalid_input")
    return value


def public_person(user, profile):
    return {"name": (profile.display_name if profile and profile.display_name else user.full_name if user else "Create Quiz") or "Create Quiz",
            "username": profile.username or "" if profile else ""}


def normalize_public_quiz(data):
    title = data.get("title")
    category, subject, kind = data.get("category", "education"), data.get("subject", "general"), data.get("kind", "knowledge")
    if not isinstance(title, str) or not 3 <= len(title.strip()) <= 120:
        raise InteractiveError("invalid_public_quiz")
    if category not in CATEGORIES or subject not in SUBJECTS or kind not in {"knowledge", "affinity"}:
        raise InteractiveError("invalid_public_quiz")
    if category != "education":
        subject = "general"
    period = integer(data.get("open_period", 30), 10, 120)
    visibility = data.get("visibility", "public")
    if visibility not in {"public", "private"}:
        raise InteractiveError("invalid_public_quiz")
    questions = data.get("questions")
    if not isinstance(questions, list) or not 1 <= len(questions) <= 50:
        raise InteractiveError("invalid_public_quiz")
    cleaned = []
    for q in questions:
        if not isinstance(q, dict) or not isinstance(q.get("question"), str) or not 1 <= len(q["question"].strip()) <= 300:
            raise InteractiveError("invalid_public_quiz")
        options = q.get("options")
        if not isinstance(options, list) or len(options) != 4 or any(not isinstance(o, str) or not 1 <= len(o.strip()) <= 100 for o in options):
            raise InteractiveError("invalid_public_quiz")
        item = {"question": q["question"].strip(), "options": [o.strip() for o in options]}
        if kind == "knowledge":
            item["correct_index"] = integer(q.get("correct_index", 0), 0, 3)
        else:
            weights = q.get("weights")
            if not isinstance(weights, list) or len(weights) != 4:
                raise InteractiveError("invalid_public_quiz")
            item["weights"] = [integer(w, 0, 100) for w in weights]
        cleaned.append(item)
    return {"title": title.strip(), "category": category, "subject": subject, "kind": kind,
            "open_period": period, "questions": cleaned, "visibility": visibility}


class InteractiveService:
    def __init__(self, *, now=time.time):
        self.now = now
        self.lock = asyncio.Lock()
        self.task = None
        self._has_active_rooms = False

    async def start(self):
        async with self.lock, db.async_session() as session:
            for subject, title, items in STARTERS:
                quiz_id = uuid.uuid5(uuid.NAMESPACE_URL, "quiz-miniapp-starter/" + subject).hex
                if await session.get(db.PublicQuiz, quiz_id) is None:
                    questions = [{"question": q, "options": options, "correct_index": correct} for q, options, correct in items]
                    session.add(db.PublicQuiz(id=quiz_id, title=title, category="education", subject=subject,
                                             kind="knowledge", questions=json.dumps(questions, ensure_ascii=False),
                                             open_period=30, created_at=0))
            await session.commit()
            self._has_active_rooms = bool((await session.execute(select(db.InteractiveRoom.id).where(db.InteractiveRoom.status == "active").limit(1))).scalar())
        self.task = asyncio.create_task(self._clock())

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def _clock(self):
        while True:
            try:
                if self._has_active_rooms:
                    await self.advance()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Interactive room clock failed; will retry")
            await asyncio.sleep(1)

    async def _advance(self, session, room, now):
        questions = json.loads(room.questions)
        while room.status == "active" and room.deadline <= now:
            answered = int((await session.execute(select(func.count()).select_from(db.InteractiveAnswer).where(
                db.InteractiveAnswer.room_id == room.id, db.InteractiveAnswer.question_index == room.question_index,
            ))).scalar() or 0)
            room.idle_streak = 0 if answered else room.idle_streak + 1
            if room.idle_streak >= 3:
                room.status, room.reason = "stopped", "idle"
            elif room.question_index + 1 >= len(questions):
                room.status, room.reason = "finished", "complete"
            else:
                room.question_index += 1
                room.deadline += room.open_period

    async def advance(self):
        async with self.lock, db.async_session() as session:
            rooms = (await session.execute(select(db.InteractiveRoom).where(db.InteractiveRoom.status == "active"))).scalars().all()
            for room in rooms:
                await self._advance(session, room, self.now())
            self._has_active_rooms = any(r.status == "active" for r in rooms)
            await session.commit()

    async def _presence(self, session, user_id, now):
        row = await session.get(db.InteractivePresence, user_id)
        if row is None:
            session.add(db.InteractivePresence(user_id=user_id, last_seen_at=now))
        elif now - row.last_seen_at >= 10:
            row.last_seen_at = now

    async def publish(self, user_id, data, *, trusted_images=None):
        clean = normalize_public_quiz(data)
        if trusted_images:
            for q, path in zip(clean["questions"], trusted_images):
                if path:
                    q["image_path"] = path
        async with self.lock, db.async_session() as session:
            count = int((await session.execute(select(func.count()).select_from(db.PublicQuiz).where(
                db.PublicQuiz.author_id == user_id, db.PublicQuiz.created_at >= self.now() - 86400,
            ))).scalar() or 0)
            if count >= 20:
                raise InteractiveError("public_quiz_limit", 429)
            quiz_id = uuid.uuid4().hex
            session.add(db.PublicQuiz(id=quiz_id, author_id=user_id, title=clean["title"], category=clean["category"],
                                     subject=clean["subject"], kind=clean["kind"], open_period=clean["open_period"],
                                     questions=json.dumps(clean["questions"], ensure_ascii=False), created_at=self.now()))
            await session.flush()
            session.add(db.PublicQuizSettings(quiz_id=quiz_id, visibility=clean["visibility"]))
            await session.commit()
            return quiz_id

    async def own_quizzes(self, user_id, *, before=None):
        async with self.lock, db.async_session() as session:
            query = select(db.PublicQuiz, db.PublicQuizSettings).outerjoin(
                db.PublicQuizSettings, db.PublicQuizSettings.quiz_id == db.PublicQuiz.id,
            ).where(db.PublicQuiz.author_id == user_id, db.PublicQuizSettings.deleted_at.is_(None))
            # IDs are random; use a stable creation-time + ID tuple for pagination.
            if before:
                previous = await session.get(db.PublicQuiz, before)
                if previous is None or previous.author_id != user_id:
                    raise InteractiveError("not_found", 404)
                from sqlalchemy import or_, and_
                query = query.where(or_(db.PublicQuiz.created_at < previous.created_at,
                                        and_(db.PublicQuiz.created_at == previous.created_at, db.PublicQuiz.id < previous.id)))
            rows = (await session.execute(query.order_by(db.PublicQuiz.created_at.desc(), db.PublicQuiz.id.desc()).limit(21))).all()
            rooms = (await session.execute(select(db.InteractiveRoom).where(
                db.InteractiveRoom.creator_id == user_id, db.InteractiveRoom.status == "active",
            ).order_by(db.InteractiveRoom.created_at.desc()))).scalars().all()
            for room in rooms:
                await self._advance(session, room, self.now())
            await session.commit()
            return {"quizzes": [{"id": q.id, "title": q.title, "category": q.category, "kind": q.kind,
                                 "question_count": len(json.loads(q.questions)), "visibility": s.visibility if s else "public",
                                 "rooms": [{"id": r.id} for r in rooms if r.quiz_id == q.id and r.status == "active"]}
                                for q, s in rows[:20]], "next": rows[19][0].id if len(rows) > 20 else None}

    async def remove_quiz(self, user_id, quiz_id):
        async with self.lock, db.async_session() as session:
            quiz = await session.get(db.PublicQuiz, quiz_id)
            if quiz is None or quiz.author_id != user_id:
                raise InteractiveError("not_found", 404)
            settings = await session.get(db.PublicQuizSettings, quiz_id)
            if settings is None:
                settings = db.PublicQuizSettings(quiz_id=quiz_id, visibility="public")
                session.add(settings)
            settings.deleted_at = self.now()
            rooms = (await session.execute(select(db.InteractiveRoom).where(
                db.InteractiveRoom.quiz_id == quiz_id, db.InteractiveRoom.status == "active",
            ))).scalars().all()
            for room in rooms:
                room.status, room.reason = "stopped", "deleted"
            await session.commit()

    async def lobby(self, user_id, *, category="education", subject="all"):
        if category not in CATEGORIES or subject not in ("all", *SUBJECTS):
            raise InteractiveError("invalid_input")
        async with self.lock, db.async_session() as session:
            now = self.now()
            await self._presence(session, user_id, now)
            active = (await session.execute(select(db.InteractiveRoom).outerjoin(
                db.PublicQuizSettings, db.PublicQuizSettings.quiz_id == db.InteractiveRoom.quiz_id,
            ).where(db.InteractiveRoom.status == "active", db.PublicQuizSettings.deleted_at.is_(None),
                    func.coalesce(db.PublicQuizSettings.visibility, "public") == "public"))).scalars().all()
            for room in active:
                await self._advance(session, room, now)
            await session.flush()
            counts = select(db.PublicQuizCompletion.quiz_id, func.count().label("plays")).join(
                db.PublicQuiz, db.PublicQuiz.id == db.PublicQuizCompletion.quiz_id,
            ).where(db.PublicQuizCompletion.user_id != db.PublicQuiz.author_id).group_by(db.PublicQuizCompletion.quiz_id).subquery()
            query = select(db.PublicQuiz, db.User, db.WebProfile, counts.c.plays).outerjoin(
                db.User, db.User.id == db.PublicQuiz.author_id,
            ).outerjoin(db.WebProfile, db.WebProfile.user_id == db.PublicQuiz.author_id).outerjoin(
                counts, counts.c.quiz_id == db.PublicQuiz.id,
            ).outerjoin(db.PublicQuizSettings, db.PublicQuizSettings.quiz_id == db.PublicQuiz.id).where(
                db.PublicQuiz.category == category, db.PublicQuizSettings.deleted_at.is_(None),
                func.coalesce(db.PublicQuizSettings.visibility, "public") == "public")
            if category == "education" and subject != "all":
                query = query.where(db.PublicQuiz.subject == subject)
            rows = (await session.execute(query.order_by(db.PublicQuiz.created_at.desc(), db.PublicQuiz.id).limit(60))).all()
            quizzes = [{"id": q.id, "title": q.title, "category": q.category, "subject": q.subject, "kind": q.kind,
                        "question_count": len(json.loads(q.questions)), "open_period": q.open_period,
                        "author": public_person(user, profile), "is_mine": q.author_id == user_id,
                        "builtin": q.author_id is None, "plays": int(plays or 0)} for q, user, profile, plays in rows]
            room_cards = []
            for room in active:
                if room.status != "active" or room.category != category or (category == "education" and subject != "all" and room.subject != subject):
                    continue
                online = int((await session.execute(select(func.count()).select_from(db.InteractiveMember).where(
                    db.InteractiveMember.room_id == room.id, db.InteractiveMember.left.is_(False), db.InteractiveMember.last_seen_at >= now - ONLINE_SECONDS,
                ))).scalar() or 0)
                room_cards.append({"id": room.id, "title": room.title, "kind": room.kind, "subject": room.subject,
                                   "question_number": room.question_index + 1, "question_count": len(json.loads(room.questions)), "online": online})
            people = (await session.execute(select(db.User, db.WebProfile).join(
                db.InteractivePresence, db.InteractivePresence.user_id == db.User.id,
            ).outerjoin(db.WebProfile, db.WebProfile.user_id == db.User.id).where(
                db.InteractivePresence.last_seen_at >= now - ONLINE_SECONDS,
            ).order_by(db.InteractivePresence.last_seen_at.desc(), db.User.id).limit(12))).all()
            online_total = int((await session.execute(select(func.count()).select_from(db.InteractivePresence).where(
                db.InteractivePresence.last_seen_at >= now - ONLINE_SECONDS,
            ))).scalar() or 0)
            ranking_query = select(db.PublicQuiz.author_id.label("author"), func.count().label("plays")).join(
                db.PublicQuizCompletion, db.PublicQuizCompletion.quiz_id == db.PublicQuiz.id,
            ).outerjoin(db.PublicQuizSettings, db.PublicQuizSettings.quiz_id == db.PublicQuiz.id).where(
                db.PublicQuiz.author_id.is_not(None), db.PublicQuizCompletion.user_id != db.PublicQuiz.author_id,
                db.PublicQuiz.category == category, db.PublicQuizSettings.deleted_at.is_(None),
                func.coalesce(db.PublicQuizSettings.visibility, "public") == "public")
            if category == "education" and subject != "all":
                ranking_query = ranking_query.where(db.PublicQuiz.subject == subject)
            ranking = ranking_query.group_by(db.PublicQuiz.author_id).subquery()
            ranked = (await session.execute(select(db.User, db.WebProfile, ranking.c.plays).join(
                ranking, ranking.c.author == db.User.id,
            ).outerjoin(db.WebProfile, db.WebProfile.user_id == db.User.id).order_by(ranking.c.plays.desc(), db.User.id).limit(20))).all()
            await session.commit()
            return {"quizzes": quizzes, "rooms": sorted(room_cards, key=lambda r: (-r["online"], r["id"]))[:50],
                    "online_count": online_total, "online": [public_person(u, p) for u, p in people],
                    "leaderboard": [{"rank": n + 1, **public_person(u, p), "plays": int(plays)} for n, (u, p, plays) in enumerate(ranked)],
                    "server_time": now}

    async def create_room(self, user_id, quiz_id):
        async with self.lock, db.async_session() as session:
            now = self.now()
            running = (await session.execute(select(db.InteractiveRoom).where(db.InteractiveRoom.status == "active"))).scalars().all()
            for room in running:
                await self._advance(session, room, now)
            active = [r for r in running if r.status == "active"]
            if len(active) >= 100 or sum(r.creator_id == user_id for r in active) >= 3:
                raise InteractiveError("room_limit", 429)
            quiz = await session.get(db.PublicQuiz, quiz_id)
            if quiz is None:
                raise InteractiveError("not_found", 404)
            settings = await session.get(db.PublicQuizSettings, quiz_id)
            if settings and (settings.deleted_at is not None or (settings.visibility == "private" and quiz.author_id != user_id)):
                raise InteractiveError("not_found", 404)
            room_id = uuid.uuid4().hex
            session.add(db.InteractiveRoom(id=room_id, quiz_id=quiz.id, creator_id=user_id, title=quiz.title,
                                          category=quiz.category, subject=quiz.subject, kind=quiz.kind, questions=quiz.questions,
                                          open_period=quiz.open_period, question_index=0, deadline=now + quiz.open_period,
                                          idle_streak=0, status="active", created_at=now))
            await session.flush()
            session.add(db.InteractiveMember(room_id=room_id, user_id=user_id, public_id=uuid.uuid4().hex,
                                            joined_at=now, last_seen_at=now, left=False))
            await self._presence(session, user_id, now)
            await session.commit()
            self._has_active_rooms = True
            return room_id

    async def join(self, user_id, room_id):
        async with self.lock, db.async_session() as session:
            now = self.now()
            room = await session.get(db.InteractiveRoom, room_id)
            if room is None:
                raise InteractiveError("not_found", 404)
            settings = await session.get(db.PublicQuizSettings, room.quiz_id)
            if settings and settings.deleted_at is not None:
                raise InteractiveError("not_found", 404)
            await self._advance(session, room, now)
            member = await session.get(db.InteractiveMember, (room_id, user_id))
            if room.status != "active" and member is None:
                await session.commit()
                raise InteractiveError("room_finished", 409)
            if member is None:
                size = int((await session.execute(select(func.count()).select_from(db.InteractiveMember).where(db.InteractiveMember.room_id == room_id))).scalar() or 0)
                if size >= 200:
                    raise InteractiveError("room_full", 409)
                session.add(db.InteractiveMember(room_id=room_id, user_id=user_id, public_id=uuid.uuid4().hex,
                                                joined_at=now, last_seen_at=now, left=False))
            else:
                member.left, member.last_seen_at = False, now
            await self._presence(session, user_id, now)
            await session.commit()

    async def leave(self, user_id, room_id):
        async with self.lock, db.async_session() as session:
            member = await session.get(db.InteractiveMember, (room_id, user_id))
            if member:
                member.left = True
            await session.commit()

    async def answer(self, user_id, room_id, index, option):
        index, option = integer(index, 0, 49), integer(option, 0, 3)
        async with self.lock, db.async_session() as session:
            now = self.now()
            room = await session.get(db.InteractiveRoom, room_id)
            if room is None:
                raise InteractiveError("not_found", 404)
            await self._advance(session, room, now)
            # Preserve advancement even when a stale answer is rejected.
            await session.commit()
            member = await session.get(db.InteractiveMember, (room_id, user_id))
            if member is None or member.left:
                raise InteractiveError("join_room_first", 403)
            if room.status != "active" or room.question_index != index or now >= room.deadline:
                raise InteractiveError("question_closed", 409)
            if await session.get(db.InteractiveAnswer, (room_id, user_id, index)) is not None:
                raise InteractiveError("already_answered", 409)
            questions = json.loads(room.questions)
            q = questions[index]
            points = q["weights"][option] if room.kind == "affinity" else 100 * int(option == q["correct_index"])
            session.add(db.InteractiveAnswer(room_id=room_id, user_id=user_id, question_index=index,
                                             option_index=option, points=points, answered_at=now))
            member.last_seen_at = now
            await self._presence(session, user_id, now)
            await session.flush()
            values = list((await session.execute(select(db.InteractiveAnswer.points).where(
                db.InteractiveAnswer.room_id == room_id, db.InteractiveAnswer.user_id == user_id,
            ))).scalars())
            if len(values) == len(questions) and await session.get(db.PublicQuizCompletion, (room.quiz_id, user_id)) is None:
                percent = int(sum(values) / len(values) + 0.5)
                session.add(db.PublicQuizCompletion(quiz_id=room.quiz_id, user_id=user_id, percent=percent, completed_at=now))
            await session.commit()

    async def room(self, user_id, room_id):
        async with self.lock, db.async_session() as session:
            now = self.now()
            room = await session.get(db.InteractiveRoom, room_id)
            if room is None:
                raise InteractiveError("not_found", 404)
            await self._advance(session, room, now)
            member = await session.get(db.InteractiveMember, (room_id, user_id))
            if member is None or member.left:
                await session.commit()
                raise InteractiveError("join_room_first", 403)
            if now - member.last_seen_at >= 10:
                member.last_seen_at = now
            await self._presence(session, user_id, now)
            questions = json.loads(room.questions)
            answers = (await session.execute(select(db.InteractiveAnswer).where(db.InteractiveAnswer.room_id == room_id))).scalars().all()
            own = [a for a in answers if a.user_id == user_id]
            current = next((a for a in own if a.question_index == room.question_index), None)
            members = (await session.execute(select(db.InteractiveMember, db.User, db.WebProfile).join(
                db.User, db.User.id == db.InteractiveMember.user_id,
            ).outerjoin(db.WebProfile, db.WebProfile.user_id == db.User.id).where(db.InteractiveMember.room_id == room_id))).all()
            players = []
            for m, user, profile in members:
                values = [a.points for a in answers if a.user_id == m.user_id]
                players.append({"id": m.public_id, **public_person(user, profile), "answered": len(values),
                                "score": sum(values) // 100 if room.kind == "knowledge" else None,
                                "online": not m.left and m.last_seen_at >= now - ONLINE_SECONDS, "is_me": m.user_id == user_id})
            players.sort(key=lambda p: (-(p["score"] or 0), -p["answered"], p["id"]))
            q = questions[room.question_index]
            quiz = await session.get(db.PublicQuiz, room.quiz_id)
            settings = await session.get(db.PublicQuizSettings, room.quiz_id)
            can_restart = bool(quiz and not (settings and settings.deleted_at is not None)
                               and (not settings or settings.visibility == "public" or quiz.author_id == user_id))
            percent = int(sum(a.points for a in own) / (len(own) if room.kind == "affinity" and own else len(questions)) + 0.5)
            payload = {"id": room.id, "quiz_id": room.quiz_id, "title": room.title, "category": room.category,
                       "kind": room.kind, "status": room.status, "reason": room.reason, "idle_streak": room.idle_streak,
                       "can_restart": can_restart, "visibility": settings.visibility if settings else "public",
                       "question_index": room.question_index, "question_count": len(questions), "deadline": room.deadline,
                       "server_time": now, "online_count": sum(p["online"] for p in players), "member_count": len(players),
                       "players": players[:20], "question": {"text": q["question"], "options": q["options"], "has_image": bool(q.get("image_path"))} if room.status == "active" else None,
                       "selected": current.option_index if current else None,
                       "my_result": {"answered": len(own), "correct": sum(a.points == 100 for a in own) if room.kind == "knowledge" else None,
                                     "percent": percent, "complete": len(own) == len(questions)}}
            await session.commit()
            return payload

    async def image_path(self, user_id, room_id, index):
        async with self.lock, db.async_session() as session:
            room = await session.get(db.InteractiveRoom, room_id)
            member = await session.get(db.InteractiveMember, (room_id, user_id))
            if room is None or member is None or member.left:
                raise InteractiveError("not_found", 404)
            settings = await session.get(db.PublicQuizSettings, room.quiz_id)
            if settings and settings.deleted_at is not None:
                raise InteractiveError("not_found", 404)
            if not 0 <= index <= room.question_index:
                raise InteractiveError("not_found", 404)
            return json.loads(room.questions)[index].get("image_path", "")
