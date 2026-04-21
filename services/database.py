import json
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy import Boolean, Column, ForeignKey, Integer, BigInteger, Text, desc, func, select
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL, SQL_ECHO

Base = declarative_base()

# --- MODELLAR BOSHLANISHI ---
class User(Base):
    __tablename__ = 'users'
    id = Column(BigInteger, primary_key=True)
    full_name = Column(Text, default="")
    username = Column(Text, default="")
    is_premium = Column(Boolean, default=False)
    daily_limit = Column(Integer, default=5)
    quizzes = relationship('Quiz', backref='creator')
    attempts = relationship('QuizAttempt', backref='user')


class UserSettings(Base):
    __tablename__ = "user_settings"
    user_id = Column(BigInteger, ForeignKey("users.id"), primary_key=True)
    # Default output language for AI quizzes: source | uz | ru | en
    default_lang = Column(Text, default="source")
    # UI language for bot messages: uz | ru | en
    ui_lang = Column(Text, default="uz")

class FreeTrialQuota(Base):
    __tablename__ = 'free_trial_quotas'
    user_id = Column(BigInteger, ForeignKey('users.id'), primary_key=True)
    started_at = Column(Text, default='')
    expires_at = Column(Text, default='')  # ISO UTC
    files_total = Column(Integer, default=0)
    files_used = Column(Integer, default=0)
    topics_total = Column(Integer, default=0)
    topics_used = Column(Integer, default=0)


class UserQuota(Base):
    __tablename__ = 'user_quotas'
    user_id = Column(BigInteger, ForeignKey('users.id'), primary_key=True)
    premium_until = Column(Text, default='')  # ISO UTC, e.g. 2026-01-01T00:00:00+00:00
    plan_code = Column(Text, default='')
    files_total = Column(Integer, default=0)
    files_used = Column(Integer, default=0)
    topics_total = Column(Integer, default=0)
    topics_used = Column(Integer, default=0)
    updated_at = Column(Text, default='')


class DailyUsage(Base):
    __tablename__ = 'daily_usage'
    user_id = Column(BigInteger, ForeignKey('users.id'), primary_key=True)
    day = Column(Text, primary_key=True)  # YYYY-MM-DD (Asia/Tashkent)
    total_used = Column(Integer, default=0)
    files_used = Column(Integer, default=0)
    topics_used = Column(Integer, default=0)


class PremiumRequest(Base):
    __tablename__ = 'premium_requests'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey('users.id'))
    plan_code = Column(Text, default='')
    status = Column(Text, default='pending')  # pending|approved|rejected
    created_at = Column(Text, default='')
    reviewed_at = Column(Text, default='')
    reviewed_by = Column(BigInteger, default=0)
    screenshot_file_id = Column(Text, default='')
    screenshot_type = Column(Text, default='')  # photo|document
    ai_verdict = Column(Text, default='')

class ReferralInvite(Base):
    __tablename__ = 'referral_invites'
    id = Column(Integer, primary_key=True, autoincrement=True)
    referrer_id = Column(BigInteger, ForeignKey('users.id'), index=True)
    referred_user_id = Column(BigInteger, ForeignKey('users.id'), unique=True, index=True)
    created_at = Column(Text, default='')
    qualified_at = Column(Text, default='')
    rewarded_at = Column(Text, default='')


class Quiz(Base):
    __tablename__ = 'quizzes'
    id = Column(Integer, primary_key=True)
    creator_id = Column(BigInteger, ForeignKey('users.id'))
    title = Column(Text, default="")
    is_ai_generated = Column(Boolean, default=False)
    open_period = Column(Integer, default=30)  # seconds per question (poll open_period)
    questions = relationship('Question', backref='quiz')

class Question(Base):
    __tablename__ = 'questions'
    id = Column(Integer, primary_key=True)
    quiz_id = Column(Integer, ForeignKey('quizzes.id'))
    text = Column(Text, default="")
    options = Column(Text, default="[]")  # JSON string (["A","B","C","D"])
    correct_answer = Column(Integer, default=0)  # 0..3
    explanation = Column(Text, default="")
    # Optional question image. One of:
    # - image_file_id: Telegram file_id (photo/document)
    # - image_path: local filesystem path (persisted under media/)
    image_file_id = Column(Text, default="")
    image_path = Column(Text, default="")

class QuizAttempt(Base):
    __tablename__ = 'quiz_attempts'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey('users.id'))
    quiz_id = Column(Integer, ForeignKey('quizzes.id'))
    # "score" kept for backward compatibility (represents correct answers count).
    score = Column(Integer, default=0)
    answered = Column(Integer, default=0)
    total_time = Column(Integer, default=0)  # seconds
    total_questions = Column(Integer, default=0)
    open_period = Column(Integer, default=0)
    chat_id = Column(BigInteger, default=0)
    chat_type = Column(Text, default="")
    finished = Column(Boolean, default=True)
    completed_at = Column(Text, default="")  # ISO timestamp


class ManualQuizDraft(Base):
    __tablename__ = "manual_quiz_drafts"
    user_id = Column(BigInteger, primary_key=True)
    chat_id = Column(BigInteger, default=0)
    state = Column(Text, default="")  # aiogram FSM state string
    data = Column(Text, default="{}")  # JSON-encoded FSM data
    updated_at = Column(Text, default="")  # ISO UTC
# --- MODELLAR TUGASHI ---

# Ma'lumotlar bazasiga ulanish sozlamalari (Async)


def _asyncpg_ssl_args(sslmode: str) -> dict:
    """Map libpq-style sslmode to asyncpg ssl context args.

    NOTE: sslmode=require means encrypted transport without certificate verification,
    which matches libpq behavior. Prefer using internal DB URLs or sslmode=verify-full
    when possible.
    """

    mode = str(sslmode or '').strip().lower()
    if not mode or mode in {'disable', 'off', '0', 'false', 'no'}:
        return {}

    import ssl

    ctx = ssl.create_default_context()

    if mode in {'require'}:
        # Encrypt but do not validate cert/hostname (libpq sslmode=require).
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return {'ssl': ctx}

    if mode in {'verify-ca'}:
        # Validate cert chain, but do not validate hostname.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        return {'ssl': ctx}

    if mode in {'verify-full'}:
        # Default context validates both cert and hostname.
        return {'ssl': ctx}

    return {}


def _build_engine():
    url_raw = str(DATABASE_URL or '').strip()
    engine_kwargs = {'echo': SQL_ECHO}

    try:
        url = make_url(url_raw)
    except Exception:
        return create_async_engine(url_raw, **engine_kwargs)

    # Postgres asyncpg: handle sslmode=... in query (asyncpg doesn't accept sslmode).
    if str(getattr(url, 'drivername', '') or '').startswith('postgresql+asyncpg'):
        q = dict(getattr(url, 'query', {}) or {})

        sslmode = str(os.getenv('DB_SSLMODE', '') or q.pop('sslmode', '') or '').strip()
        connect_args = {}
        if sslmode:
            connect_args.update(_asyncpg_ssl_args(sslmode))

        # Pool tuning for production DBs.
        try:
            engine_kwargs['pool_pre_ping'] = True
            engine_kwargs['pool_size'] = int(os.getenv('DB_POOL_SIZE', '5') or 5)
            engine_kwargs['max_overflow'] = int(os.getenv('DB_MAX_OVERFLOW', '10') or 10)
            engine_kwargs['pool_timeout'] = int(os.getenv('DB_POOL_TIMEOUT', '30') or 30)
        except Exception:
            pass

        if connect_args:
            engine_kwargs['connect_args'] = connect_args

        try:
            url = url.set(query=q)
        except Exception:
            pass

        return create_async_engine(url, **engine_kwargs)

    return create_async_engine(url_raw, **engine_kwargs)


try:
    engine = _build_engine()
except ModuleNotFoundError as exc:
    missing = getattr(exc, "name", "")
    if str(DATABASE_URL).startswith("mysql+aiomysql://") and missing in {"aiomysql", "pymysql"}:
        raise RuntimeError(
            "DATABASE_URL MySQL (mysql+aiomysql) ga o'xshaydi, lekin driver yo'q.\n"
            "1) O'rnatish: pip install aiomysql pymysql\n"
            "2) Yoki .env da sqlite ishlating: DATABASE_URL=sqlite+aiosqlite:///quiz_bot.db"
        ) from exc
    if ('+asyncpg' in str(DATABASE_URL)) and (missing == 'asyncpg'):
        raise RuntimeError(
            "DATABASE_URL Postgres (postgresql+asyncpg) ga o'xshaydi, lekin driver yo'q.\n"
            "1) O'rnatish: pip install asyncpg\n"
            "2) Yoki sqlite ishlating: DATABASE_URL=sqlite+aiosqlite:///quiz_bot.db"
        ) from exc

    raise
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_or_create_user(user_id: int, full_name: Optional[str] = None, username: Optional[str] = None) -> int:
    async with async_session() as session:
        user = await session.get(User, user_id)
        if user is None:
            user = User(id=user_id, full_name=full_name or "", username=(username or "").strip())
            session.add(user)
            await session.commit()
            return user.id

        changed = False
        if full_name is not None:
            fn = (full_name or "").strip()
            if fn and fn != str(user.full_name or ""):
                user.full_name = fn
                changed = True
        if username is not None:
            un = (username or "").strip().lstrip("@")
            if un and un != str(getattr(user, "username", "") or ""):
                user.username = un
                changed = True
        if changed:
            await session.commit()
        return int(user.id)


async def get_or_create_user_settings(user_id: int) -> dict:
    async with async_session() as session:
        settings = await session.get(UserSettings, user_id)
        if settings is None:
            settings = UserSettings(user_id=user_id, default_lang="source", ui_lang="uz")
            session.add(settings)
            await session.commit()
            await session.refresh(settings)
        return {
            "default_lang": str(settings.default_lang or "source"),
            "ui_lang": str(getattr(settings, "ui_lang", "") or "uz"),
        }


async def set_user_default_lang(user_id: int, default_lang: str) -> None:
    default_lang = (default_lang or "source").strip().lower()
    if default_lang not in {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        raise ValueError("default_lang must be one of: source | uz | ru | en | de | tr | kk | ar | zh | ko")

    async with async_session() as session:
        settings = await session.get(UserSettings, user_id)
        if settings is None:
            settings = UserSettings(user_id=user_id, default_lang=default_lang)
            session.add(settings)
        else:
            settings.default_lang = default_lang
        await session.commit()

async def set_user_ui_lang(user_id: int, ui_lang: str) -> None:
    ui_lang = (ui_lang or "uz").strip().lower()
    if ui_lang not in {"uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        raise ValueError("ui_lang must be one of: uz | ru | en | de | tr | kk | ar | zh | ko")

    async with async_session() as session:
        settings = await session.get(UserSettings, user_id)
        if settings is None:
            settings = UserSettings(user_id=user_id, default_lang="source", ui_lang=ui_lang)
            session.add(settings)
        else:
            settings.ui_lang = ui_lang
        await session.commit()


async def create_quiz(title: str, creator_id: int, is_ai_generated: bool = False, open_period: int = 30) -> int:
    async with async_session() as session:
        quiz = Quiz(
            title=title,
            creator_id=creator_id,
            is_ai_generated=is_ai_generated,
            open_period=max(5, min(600, int(open_period or 30))),
        )
        session.add(quiz)
        await session.commit()
        await session.refresh(quiz)
        return quiz.id


async def update_quiz_meta(
    quiz_id: int,
    creator_id: int,
    *,
    title: Optional[str] = None,
    open_period: Optional[int] = None,
) -> bool:
    """Update quiz title/open_period. Returns True when an update was applied.

    Security: only the quiz creator can update.
    """
    quiz_id = int(quiz_id)
    creator_id = int(creator_id)
    if quiz_id <= 0 or creator_id <= 0:
        return False

    title_val = None
    if title is not None:
        title_val = str(title or "").strip()
        if not title_val:
            return False
        title_val = title_val[:120]

    open_period_val: Optional[int] = None
    if open_period is not None:
        try:
            open_period_val = int(open_period)
        except Exception:
            return False
        open_period_val = max(5, min(600, open_period_val))

    if title_val is None and open_period_val is None:
        return False

    async with async_session() as session:
        quiz = await session.get(Quiz, quiz_id)
        if quiz is None:
            return False
        if int(getattr(quiz, "creator_id", 0) or 0) != creator_id:
            return False

        changed = False
        if title_val is not None and title_val != str(getattr(quiz, "title", "") or ""):
            quiz.title = title_val
            changed = True
        if open_period_val is not None and open_period_val != int(getattr(quiz, "open_period", 30) or 30):
            quiz.open_period = open_period_val
            changed = True

        if changed:
            session.add(quiz)
            await session.commit()
        return changed


async def create_question(
    quiz_id: int,
    text: str,
    options: list,
    correct_index: int,
    explanation: str = "",
    image_file_id: str = "",
    image_path: str = "",
) -> None:
    async with async_session() as session:
        options_str = json.dumps(options, ensure_ascii=False)
        question = Question(
            quiz_id=quiz_id,
            text=text,
            options=options_str,
            correct_answer=int(correct_index),
            explanation=explanation or "",
            image_file_id=(image_file_id or "").strip(),
            image_path=(image_path or "").strip(),
        )
        session.add(question)
        await session.commit()


async def create_questions_bulk(quiz_id: int, questions: List[dict]) -> int:
    if not questions:
        return 0

    objects = []
    for q in questions:
        text = str(q.get("question") or q.get("text") or "").strip()
        options = q.get("options") or []
        if not text or not isinstance(options, list) or len(options) != 4:
            continue
        correct_index = int(q.get("correct_index") or q.get("correct_answer") or 0)
        explanation = str(q.get("explanation") or "").strip()
        image_file_id = str(q.get("image_file_id") or "").strip()
        image_path = str(q.get("image_path") or "").strip()
        objects.append(
            Question(
                quiz_id=quiz_id,
                text=text,
                options=json.dumps(options, ensure_ascii=False),
                correct_answer=max(0, min(3, correct_index)),
                explanation=explanation,
                image_file_id=image_file_id,
                image_path=image_path,
            )
        )

    if not objects:
        return 0

    async with async_session() as session:
        session.add_all(objects)
        await session.commit()

    return len(objects)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Lightweight migrations for Postgres (handle Telegram IDs > int32).
        try:
            if str(engine.url).startswith("postgresql"):
                # Drop FK constraints that reference users.id so we can alter types.
                drop_fks = [
                    "ALTER TABLE user_settings DROP CONSTRAINT IF EXISTS user_settings_user_id_fkey",
                    "ALTER TABLE free_trial_quotas DROP CONSTRAINT IF EXISTS free_trial_quotas_user_id_fkey",
                    "ALTER TABLE user_quotas DROP CONSTRAINT IF EXISTS user_quotas_user_id_fkey",
                    "ALTER TABLE daily_usage DROP CONSTRAINT IF EXISTS daily_usage_user_id_fkey",
                    "ALTER TABLE premium_requests DROP CONSTRAINT IF EXISTS premium_requests_user_id_fkey",
                    "ALTER TABLE quizzes DROP CONSTRAINT IF EXISTS quizzes_creator_id_fkey",
                    "ALTER TABLE quiz_attempts DROP CONSTRAINT IF EXISTS quiz_attempts_user_id_fkey",
                ]
                for sql in drop_fks:
                    await conn.exec_driver_sql(sql)

                # Alter columns to BIGINT.
                alter_cols = [
                    "ALTER TABLE users ALTER COLUMN id TYPE BIGINT",
                    "ALTER TABLE user_settings ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE free_trial_quotas ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE user_quotas ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE daily_usage ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE premium_requests ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE premium_requests ALTER COLUMN reviewed_by TYPE BIGINT",
                    "ALTER TABLE quizzes ALTER COLUMN creator_id TYPE BIGINT",
                    "ALTER TABLE quiz_attempts ALTER COLUMN user_id TYPE BIGINT",
                    "ALTER TABLE quiz_attempts ALTER COLUMN chat_id TYPE BIGINT",
                ]
                for sql in alter_cols:
                    await conn.exec_driver_sql(sql)

                # Recreate FK constraints.
                add_fks = [
                    "ALTER TABLE user_settings ADD CONSTRAINT user_settings_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                    "ALTER TABLE free_trial_quotas ADD CONSTRAINT free_trial_quotas_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                    "ALTER TABLE user_quotas ADD CONSTRAINT user_quotas_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                    "ALTER TABLE daily_usage ADD CONSTRAINT daily_usage_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                    "ALTER TABLE premium_requests ADD CONSTRAINT premium_requests_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                    "ALTER TABLE quizzes ADD CONSTRAINT quizzes_creator_id_fkey FOREIGN KEY (creator_id) REFERENCES users(id)",
                    "ALTER TABLE quiz_attempts ADD CONSTRAINT quiz_attempts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)",
                ]
                for sql in add_fks:
                    await conn.exec_driver_sql(sql)
        except Exception as exc:
            logging.warning("Postgres bigint migration skipped/failed: %s", exc)
        # Lightweight migrations for SQLite (create_all doesn't alter existing tables).
        try:
            if str(engine.url).startswith("sqlite"):
                res = await conn.exec_driver_sql("PRAGMA table_info(user_settings)")
                cols = {row[1] for row in res.fetchall()}
                if "ui_lang" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE user_settings ADD COLUMN ui_lang TEXT DEFAULT 'uz'")
                    await conn.exec_driver_sql("UPDATE user_settings SET ui_lang='uz' WHERE ui_lang IS NULL OR ui_lang=''")

                res = await conn.exec_driver_sql("PRAGMA table_info(quizzes)")
                cols = {row[1] for row in res.fetchall()}
                if "open_period" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quizzes ADD COLUMN open_period INTEGER DEFAULT 30")
                    await conn.exec_driver_sql("UPDATE quizzes SET open_period=30 WHERE open_period IS NULL")

                res = await conn.exec_driver_sql("PRAGMA table_info(questions)")
                cols = {row[1] for row in res.fetchall()}
                if "image_file_id" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE questions ADD COLUMN image_file_id TEXT DEFAULT ''")
                    await conn.exec_driver_sql("UPDATE questions SET image_file_id='' WHERE image_file_id IS NULL")
                if "image_path" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE questions ADD COLUMN image_path TEXT DEFAULT ''")
                    await conn.exec_driver_sql("UPDATE questions SET image_path='' WHERE image_path IS NULL")

                res = await conn.exec_driver_sql("PRAGMA table_info(users)")
                cols = {row[1] for row in res.fetchall()}
                if "username" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE users ADD COLUMN username TEXT DEFAULT ''")
                    await conn.exec_driver_sql("UPDATE users SET username='' WHERE username IS NULL")

                res = await conn.exec_driver_sql("PRAGMA table_info(quiz_attempts)")
                cols = {row[1] for row in res.fetchall()}
                if "answered" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN answered INTEGER DEFAULT 0")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET answered=0 WHERE answered IS NULL")
                if "total_time" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN total_time INTEGER DEFAULT 0")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET total_time=0 WHERE total_time IS NULL")
                if "total_questions" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN total_questions INTEGER DEFAULT 0")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET total_questions=0 WHERE total_questions IS NULL")
                if "open_period" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN open_period INTEGER DEFAULT 0")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET open_period=0 WHERE open_period IS NULL")
                if "chat_id" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN chat_id INTEGER DEFAULT 0")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET chat_id=0 WHERE chat_id IS NULL")
                if "chat_type" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN chat_type TEXT DEFAULT ''")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET chat_type='' WHERE chat_type IS NULL")
                if "finished" not in cols:
                    await conn.exec_driver_sql("ALTER TABLE quiz_attempts ADD COLUMN finished INTEGER DEFAULT 1")
                    await conn.exec_driver_sql("UPDATE quiz_attempts SET finished=1 WHERE finished IS NULL")
        except Exception:
            # Ignore migration errors; worst case open_period will fall back to defaults in code.
            pass


def _utc_now_iso() -> str:
    # ISO format keeps lexicographical order (useful for MAX on SQLite text).
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


async def upsert_manual_quiz_draft(*, user_id: int, chat_id: int, state: str, data: dict) -> None:
    user_id = int(user_id or 0)
    chat_id = int(chat_id or 0)
    payload = json.dumps(data or {}, ensure_ascii=False)
    st = str(state or "").strip()
    now = _utc_now_iso()

    async with async_session() as session:
        row = await session.get(ManualQuizDraft, user_id)
        if row is None:
            row = ManualQuizDraft(user_id=user_id, chat_id=chat_id, state=st, data=payload, updated_at=now)
            session.add(row)
        else:
            row.chat_id = chat_id
            row.state = st
            row.data = payload
            row.updated_at = now
        await session.commit()


async def get_manual_quiz_draft(*, user_id: int) -> Optional[dict]:
    user_id = int(user_id or 0)
    if user_id <= 0:
        return None

    async with async_session() as session:
        row = await session.get(ManualQuizDraft, user_id)
        if row is None:
            return None
        try:
            data = json.loads(str(row.data or "{}"))
        except Exception:
            data = {}
        return {
            "user_id": int(row.user_id),
            "chat_id": int(getattr(row, "chat_id", 0) or 0),
            "state": str(getattr(row, "state", "") or ""),
            "data": data if isinstance(data, dict) else {},
            "updated_at": str(getattr(row, "updated_at", "") or ""),
        }


async def clear_manual_quiz_draft(*, user_id: int) -> None:
    user_id = int(user_id or 0)
    if user_id <= 0:
        return
    async with async_session() as session:
        row = await session.get(ManualQuizDraft, user_id)
        if row is not None:
            await session.delete(row)
            await session.commit()


async def create_quiz_attempts_bulk(
    quiz_id: int,
    attempts: List[dict],
    *,
    chat_id: int = 0,
    chat_type: str = "",
    total_questions: int = 0,
    open_period: int = 0,
    finished: bool = True,
) -> int:
    if not attempts:
        return 0

    now_iso = _utc_now_iso()
    total_questions = max(0, int(total_questions or 0))
    open_period = max(0, int(open_period or 0))
    chat_id = int(chat_id or 0)
    chat_type = str(chat_type or "")

    objects: List[QuizAttempt] = []
    user_updates: dict[int, tuple[str, str]] = {}

    for a in attempts:
        try:
            uid = int(a.get("user_id") or 0)
        except Exception:
            uid = 0
        if uid <= 0:
            continue

        full_name = str(a.get("full_name") or a.get("name") or "").strip()
        username = str(a.get("username") or "").strip().lstrip("@")
        user_updates[uid] = (full_name, username)

        correct = int(a.get("correct") or a.get("score") or 0)
        answered = int(a.get("answered") or 0)
        try:
            total_time = int(round(float(a.get("total_time") or 0.0)))
        except Exception:
            total_time = 0

        objects.append(
            QuizAttempt(
                quiz_id=int(quiz_id),
                user_id=int(uid),
                score=max(0, correct),
                answered=max(0, answered),
                total_time=max(0, total_time),
                total_questions=total_questions,
                open_period=open_period,
                chat_id=chat_id,
                chat_type=chat_type,
                finished=bool(finished),
                completed_at=now_iso,
            )
        )

    if not objects:
        return 0

    async with async_session() as session:
        # Upsert user profiles (best-effort).
        for uid, (full_name, username) in user_updates.items():
            u = await session.get(User, int(uid))
            if u is None:
                session.add(User(id=int(uid), full_name=full_name or "", username=username or ""))
                continue
            changed = False
            if full_name and full_name != str(u.full_name or ""):
                u.full_name = full_name
                changed = True
            if username and username != str(getattr(u, "username", "") or ""):
                u.username = username
                changed = True
            if changed:
                session.add(u)

        session.add_all(objects)
        await session.commit()

    return len(objects)


async def get_quiz_attempt_stats(quiz_id: int, limit: int = 30) -> List[dict]:
    """Return per-user best attempt stats for a quiz."""
    quiz_id = int(quiz_id)
    limit = max(1, min(100, int(limit or 30)))

    async with async_session() as session:
        stmt = (
            select(
                QuizAttempt.id,
                QuizAttempt.user_id,
                QuizAttempt.score,
                QuizAttempt.answered,
                QuizAttempt.total_time,
                QuizAttempt.total_questions,
                QuizAttempt.completed_at,
                QuizAttempt.finished,
                User.full_name,
                User.username,
            )
            .join(User, User.id == QuizAttempt.user_id)
            .where(QuizAttempt.quiz_id == quiz_id)
            .order_by(desc(QuizAttempt.id))
        )
        rows = (await session.execute(stmt)).all()

    best: dict[int, dict] = {}
    counts: dict[int, int] = {}

    def _key(item: dict) -> tuple:
        # Higher score is better, higher answered is better, lower time is better.
        return (int(item.get("score") or 0), int(item.get("answered") or 0), -int(item.get("total_time") or 0))

    for r in rows:
        uid = int(r.user_id)
        counts[uid] = int(counts.get(uid, 0)) + 1
        item = {
            "user_id": uid,
            "name": str(r.full_name or "").strip() or str(uid),
            "username": str(r.username or "").strip().lstrip("@"),
            "score": int(r.score or 0),
            "answered": int(r.answered or 0),
            "total_time": int(r.total_time or 0),
            "total_questions": int(r.total_questions or 0),
            "finished": bool(r.finished),
            "completed_at": str(r.completed_at or ""),
        }
        prev = best.get(uid)
        if prev is None or _key(item) > _key(prev):
            best[uid] = item

    out: List[dict] = []
    for uid, item in best.items():
        item["attempts"] = int(counts.get(uid, 1))
        out.append(item)

    out.sort(key=lambda x: (-int(x.get("score") or 0), -int(x.get("answered") or 0), int(x.get("total_time") or 0), str(x.get("name") or "").lower()))
    return out[:limit]


async def list_user_quizzes(user_id: int, limit: int = 20) -> List[dict]:
    async with async_session() as session:
        stmt = (
            select(
                Quiz.id,
                Quiz.title,
                Quiz.is_ai_generated,
                func.count(Question.id).label("question_count"),
            )
            .outerjoin(Question, Question.quiz_id == Quiz.id)
            .where(Quiz.creator_id == user_id)
            .group_by(Quiz.id)
            .order_by(desc(Quiz.id))
            .limit(limit)
        )
        rows = (await session.execute(stmt)).all()
        return [
            {
                "id": int(r.id),
                "title": r.title or "",
                "is_ai_generated": bool(r.is_ai_generated),
                "question_count": int(r.question_count or 0),
            }
            for r in rows
        ]


async def get_quiz_with_questions(quiz_id: int) -> Optional[dict]:
    async with async_session() as session:
        quiz = await session.get(Quiz, int(quiz_id))
        if quiz is None:
            return None

        stmt = (
            select(
                Question.id,
                Question.text,
                Question.options,
                Question.correct_answer,
                Question.explanation,
                Question.image_file_id,
                Question.image_path,
            )
            .where(Question.quiz_id == quiz.id)
            .order_by(Question.id.asc())
        )
        rows = (await session.execute(stmt)).all()

        questions: List[dict] = []
        for r in rows:
            try:
                opts = json.loads(r.options or "[]")
            except Exception:
                opts = []
            if not isinstance(opts, list):
                opts = []
            opts = [str(o) for o in opts][:4]

            questions.append(
                {
                    "question": str(r.text or ""),
                    "options": opts,
                    "correct_index": int(r.correct_answer or 0),
                    "explanation": str(r.explanation or ""),
                    "image_file_id": str(r.image_file_id or ""),
                    "image_path": str(r.image_path or ""),
                }
            )

        return {
            "id": int(quiz.id),
            "title": str(quiz.title or ""),
            "creator_id": int(quiz.creator_id or 0),
            "is_ai_generated": bool(quiz.is_ai_generated),
            "open_period": int(getattr(quiz, "open_period", 30) or 30),
            "questions": questions,
        }


async def get_quiz_summary(quiz_id: int) -> Optional[dict]:
    async with async_session() as session:
        stmt = (
            select(
                Quiz.id,
                Quiz.title,
                Quiz.creator_id,
                Quiz.is_ai_generated,
                Quiz.open_period,
                func.count(Question.id).label("question_count"),
            )
            .outerjoin(Question, Question.quiz_id == Quiz.id)
            .where(Quiz.id == int(quiz_id))
            .group_by(Quiz.id)
        )
        row = (await session.execute(stmt)).first()
        if not row:
            return None
        return {
            "id": int(row.id),
            "title": str(row.title or ""),
            "creator_id": int(row.creator_id or 0),
            "is_ai_generated": bool(row.is_ai_generated),
            "open_period": int(getattr(row, "open_period", 30) or 30),
            "question_count": int(row.question_count or 0),
        }
        





class QuotaExceeded(Exception):
    """Raised when a user exceeds their free-trial or premium limits."""

    def __init__(self, *, scope: str, kind: str, status: dict):
        self.scope = str(scope)
        self.kind = str(kind)
        self.status = status or {}
        super().__init__(f"quota exceeded: {self.scope}:{self.kind}")


def _parse_iso_dt(value: str) -> Optional[datetime]:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_premium_active(premium_until: str) -> bool:
    dt = _parse_iso_dt(premium_until)
    if not dt:
        return False
    return dt > datetime.now(timezone.utc)


def _trial_defaults() -> tuple[int, int, int]:
    """Return (files_total, topics_total, duration_days)."""

    try:
        files_total = int(os.getenv('FREE_TRIAL_FILES', '2') or 2)
    except Exception:
        files_total = 2
    try:
        topics_total = int(os.getenv('FREE_TRIAL_TOPICS', '1') or 1)
    except Exception:
        topics_total = 1
    try:
        days = int(os.getenv('FREE_TRIAL_DAYS', '1') or 1)
    except Exception:
        days = 1

    files_total = max(0, files_total)
    topics_total = max(0, topics_total)
    days = max(1, days)
    return files_total, topics_total, days


def _is_trial_active(expires_at: str) -> bool:
    dt = _parse_iso_dt(expires_at)
    if not dt:
        return False
    return dt > datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --- Referral -------------------------------------------------------

async def record_referral_invite(*, referrer_id: int, referred_user_id: int) -> bool:
    """Record that `referred_user_id` came via `referrer_id` deep link.

    Returns True only if a new invite row was created.
    """

    referrer_id = int(referrer_id or 0)
    referred_user_id = int(referred_user_id or 0)
    if referrer_id <= 0 or referred_user_id <= 0:
        return False
    if referrer_id == referred_user_id:
        return False

    now = _utc_now_iso()
    async with async_session() as session:
        existing = await session.execute(select(ReferralInvite).where(ReferralInvite.referred_user_id == referred_user_id))
        row = existing.scalar_one_or_none()
        if row is not None:
            return False

        inv = ReferralInvite(
            referrer_id=referrer_id,
            referred_user_id=referred_user_id,
            created_at=now,
            qualified_at='',
            rewarded_at='',
        )
        session.add(inv)
        await session.commit()
        return True


async def get_referral_status(user_id: int) -> dict:
    user_id = int(user_id or 0)
    if user_id <= 0:
        return {
            'total': 0,
            'qualified': 0,
            'pending': 0,
            'rewarded_referrals': 0,
            'unrewarded_qualified': 0,
            'to_next_reward': 3,
        }

    async with async_session() as session:
        total = await session.execute(select(func.count()).select_from(ReferralInvite).where(ReferralInvite.referrer_id == user_id))
        total_n = int(total.scalar() or 0)

        qualified = await session.execute(
            select(func.count()).select_from(ReferralInvite).where(
                ReferralInvite.referrer_id == user_id,
                ReferralInvite.qualified_at != '',
            )
        )
        qualified_n = int(qualified.scalar() or 0)

        pending = await session.execute(
            select(func.count()).select_from(ReferralInvite).where(
                ReferralInvite.referrer_id == user_id,
                ReferralInvite.qualified_at == '',
            )
        )
        pending_n = int(pending.scalar() or 0)

        unrewarded = await session.execute(
            select(func.count()).select_from(ReferralInvite).where(
                ReferralInvite.referrer_id == user_id,
                ReferralInvite.qualified_at != '',
                ReferralInvite.rewarded_at == '',
            )
        )
        unrewarded_n = int(unrewarded.scalar() or 0)

        rewarded = await session.execute(
            select(func.count()).select_from(ReferralInvite).where(
                ReferralInvite.referrer_id == user_id,
                ReferralInvite.rewarded_at != '',
            )
        )
        rewarded_n = int(rewarded.scalar() or 0)

    to_next = 3 - (unrewarded_n % 3) if unrewarded_n % 3 else 0
    if to_next == 0 and unrewarded_n == 0:
        to_next = 3

    return {
        'total': total_n,
        'qualified': qualified_n,
        'pending': pending_n,
        'rewarded_referrals': rewarded_n,
        'unrewarded_qualified': unrewarded_n,
        'to_next_reward': int(to_next),
    }


async def _grant_referral_bonus(user_id: int, *, files: int = 2, topics: int = 1) -> None:
    """Grant referral bonus quotas.

    - If premium is active: increase premium totals (does not extend time).
    - Otherwise: increase free trial totals and ensure trial is active for at least 1 day.
    """

    user_id = int(user_id or 0)
    files = max(0, int(files or 0))
    topics = max(0, int(topics or 0))
    if user_id <= 0 or (files == 0 and topics == 0):
        return

    now = datetime.now(timezone.utc).replace(microsecond=0)
    trial_files_total, trial_topics_total, trial_days = _trial_defaults()
    min_trial_days = 1

    async with async_session() as session:
        q = await session.get(UserQuota, user_id)
        if q and _is_premium_active(str(getattr(q, 'premium_until', '') or '')):
            q.files_total = int(getattr(q, 'files_total', 0) or 0) + files
            q.topics_total = int(getattr(q, 'topics_total', 0) or 0) + topics
            q.updated_at = _utc_now_iso()
            session.add(q)
            await session.commit()
            return

        tr = await session.get(FreeTrialQuota, user_id)
        if tr is None:
            expires = (now + timedelta(days=int(max(min_trial_days, trial_days)))).isoformat()
            tr = FreeTrialQuota(
                user_id=user_id,
                started_at=now.isoformat(),
                expires_at=expires,
                files_total=int(trial_files_total),
                files_used=0,
                topics_total=int(trial_topics_total),
                topics_used=0,
            )
            session.add(tr)
            await session.commit()
            tr = await session.get(FreeTrialQuota, user_id)

        if tr is None:
            return

        expires_at = str(getattr(tr, 'expires_at', '') or '')
        # If trial expired, extend it so the user can use the referral bonus.
        if not _is_trial_active(expires_at):
            tr.expires_at = (now + timedelta(days=int(min_trial_days))).isoformat()
            if not str(getattr(tr, 'started_at', '') or '').strip():
                tr.started_at = now.isoformat()

        tr.files_total = int(getattr(tr, 'files_total', 0) or 0) + files
        tr.topics_total = int(getattr(tr, 'topics_total', 0) or 0) + topics
        session.add(tr)
        await session.commit()


async def qualify_referral_if_any(*, referred_user_id: int) -> dict:
    """Mark referral as qualified for this user (once), and award bonuses per 3 qualified.

    Returns dict with keys:
    - qualified (bool)
    - rewarded (bool)
    - referrer_id (int)
    - unrewarded_qualified (int)
    """

    referred_user_id = int(referred_user_id or 0)
    if referred_user_id <= 0:
        return {'qualified': False, 'rewarded': False, 'referrer_id': 0, 'unrewarded_qualified': 0}

    now = _utc_now_iso()
    async with async_session() as session:
        res = await session.execute(select(ReferralInvite).where(ReferralInvite.referred_user_id == referred_user_id))
        inv = res.scalar_one_or_none()
        if inv is None:
            return {'qualified': False, 'rewarded': False, 'referrer_id': 0, 'unrewarded_qualified': 0}

        if str(getattr(inv, 'qualified_at', '') or '').strip():
            # Already qualified.
            ref_id = int(getattr(inv, 'referrer_id', 0) or 0)
            cnt = await session.execute(
                select(func.count()).select_from(ReferralInvite).where(
                    ReferralInvite.referrer_id == ref_id,
                    ReferralInvite.qualified_at != '',
                    ReferralInvite.rewarded_at == '',
                )
            )
            return {
                'qualified': False,
                'rewarded': False,
                'referrer_id': ref_id,
                'unrewarded_qualified': int(cnt.scalar() or 0),
            }

        inv.qualified_at = now
        session.add(inv)
        await session.commit()

        ref_id = int(getattr(inv, 'referrer_id', 0) or 0)
        if ref_id <= 0:
            return {'qualified': True, 'rewarded': False, 'referrer_id': 0, 'unrewarded_qualified': 0}

        # Check if we have 3 qualified (unrewarded) referrals to award.
        q = await session.execute(
            select(ReferralInvite)
            .where(
                ReferralInvite.referrer_id == ref_id,
                ReferralInvite.qualified_at != '',
                ReferralInvite.rewarded_at == '',
            )
            .order_by(ReferralInvite.qualified_at.asc())
            .limit(3)
        )
        rows = list(q.scalars().all())
        unrewarded_now = await session.execute(
            select(func.count()).select_from(ReferralInvite).where(
                ReferralInvite.referrer_id == ref_id,
                ReferralInvite.qualified_at != '',
                ReferralInvite.rewarded_at == '',
            )
        )
        unrewarded_n = int(unrewarded_now.scalar() or 0)

        rewarded = False
        if len(rows) >= 3:
            for r in rows:
                r.rewarded_at = now
                session.add(r)
            await session.commit()
            rewarded = True

    if rewarded:
        await _grant_referral_bonus(ref_id, files=2, topics=1)

    return {
        'qualified': True,
        'rewarded': rewarded,
        'referrer_id': int(ref_id),
        'unrewarded_qualified': int(max(0, unrewarded_n - (3 if rewarded else 0))),
    }


async def get_user_quota_status(user_id: int) -> dict:
    user_id = int(user_id or 0)

    trial_files_total, trial_topics_total, trial_days = _trial_defaults()

    out = {
        'user_id': user_id,
        'premium_active': False,
        'premium_until': '',
        'plan_code': '',
        'files_total': 0,
        'files_used': 0,
        'files_left': 0,
        'topics_total': 0,
        'topics_used': 0,
        'topics_left': 0,
        'trial_active': True,
        'trial_until': '',
        'trial_days': int(trial_days),
        'trial_files_total': int(trial_files_total),
        'trial_files_used': 0,
        'trial_files_left': int(trial_files_total),
        'trial_topics_total': int(trial_topics_total),
        'trial_topics_used': 0,
        'trial_topics_left': int(trial_topics_total),
    }

    if user_id <= 0:
        return out

    async with async_session() as session:
        q = await session.get(UserQuota, user_id)
        if q and _is_premium_active(str(getattr(q, 'premium_until', '') or '')):
            files_total = int(getattr(q, 'files_total', 0) or 0)
            files_used = int(getattr(q, 'files_used', 0) or 0)
            topics_total = int(getattr(q, 'topics_total', 0) or 0)
            topics_used = int(getattr(q, 'topics_used', 0) or 0)
            out.update(
                {
                    'premium_active': True,
                    'premium_until': str(getattr(q, 'premium_until', '') or ''),
                    'plan_code': str(getattr(q, 'plan_code', '') or ''),
                    'files_total': files_total,
                    'files_used': files_used,
                    'files_left': max(0, files_total - files_used),
                    'topics_total': topics_total,
                    'topics_used': topics_used,
                    'topics_left': max(0, topics_total - topics_used),
                    # Trial is irrelevant when premium is active.
                    'trial_active': False,
                    'trial_files_left': 0,
                    'trial_topics_left': 0,
                }
            )
            return out

        tr = await session.get(FreeTrialQuota, user_id)
        if tr is None:
            return out

        expires_at = str(getattr(tr, 'expires_at', '') or '')
        active = _is_trial_active(expires_at)
        files_total = int(getattr(tr, 'files_total', 0) or 0)
        files_used = int(getattr(tr, 'files_used', 0) or 0)
        topics_total = int(getattr(tr, 'topics_total', 0) or 0)
        topics_used = int(getattr(tr, 'topics_used', 0) or 0)

        out.update(
            {
                'trial_active': bool(active),
                'trial_until': expires_at,
                'trial_days': int(trial_days),
                'trial_files_total': files_total,
                'trial_files_used': files_used,
                'trial_files_left': max(0, files_total - files_used) if active else 0,
                'trial_topics_total': topics_total,
                'trial_topics_used': topics_used,
                'trial_topics_left': max(0, topics_total - topics_used) if active else 0,
            }
        )
        return out


async def check_user_quota(user_id: int, kind: str) -> None:
    """Check quota availability without consuming it.

    This prevents charging a user when they only open a menu or abandon a flow.
    Consumption happens via `reserve_user_quota()` after a quiz is successfully generated.
    """

    user_id = int(user_id or 0)
    kind = str(kind or '').strip().lower()
    if kind not in {'file', 'topic'}:
        raise ValueError('kind must be file|topic')

    if user_id <= 0:
        raise QuotaExceeded(scope='free', kind=kind, status={})

    trial_files_total, trial_topics_total, trial_days = _trial_defaults()

    async with async_session() as session:
        # Premium
        q = await session.get(UserQuota, user_id)
        if q and _is_premium_active(str(getattr(q, 'premium_until', '') or '')):
            files_total = int(getattr(q, 'files_total', 0) or 0)
            files_used = int(getattr(q, 'files_used', 0) or 0)
            topics_total = int(getattr(q, 'topics_total', 0) or 0)
            topics_used = int(getattr(q, 'topics_used', 0) or 0)

            if kind == 'file' and files_used >= files_total:
                raise QuotaExceeded(
                    scope='premium',
                    kind=kind,
                    status={
                        'premium_until': str(getattr(q, 'premium_until', '') or ''),
                        'files_left': 0,
                        'topics_left': max(0, topics_total - topics_used),
                    },
                )
            if kind == 'topic' and topics_used >= topics_total:
                raise QuotaExceeded(
                    scope='premium',
                    kind=kind,
                    status={
                        'premium_until': str(getattr(q, 'premium_until', '') or ''),
                        'files_left': max(0, files_total - files_used),
                        'topics_left': 0,
                    },
                )
            return

        # Free trial
        tr = await session.get(FreeTrialQuota, user_id)
        if tr is None:
            # Trial will be created on first actual consumption.
            return

        expires_at = str(getattr(tr, 'expires_at', '') or '')
        if not _is_trial_active(expires_at):
            raise QuotaExceeded(scope='free', kind=kind, status={'trial_expired': True, 'trial_until': expires_at})

        files_total = int(getattr(tr, 'files_total', 0) or int(trial_files_total))
        files_used = int(getattr(tr, 'files_used', 0) or 0)
        topics_total = int(getattr(tr, 'topics_total', 0) or int(trial_topics_total))
        topics_used = int(getattr(tr, 'topics_used', 0) or 0)

        if kind == 'file' and files_used >= files_total:
            raise QuotaExceeded(
                scope='free',
                kind=kind,
                status={
                    'trial_until': expires_at,
                    'trial_files_left': 0,
                    'trial_topics_left': max(0, topics_total - topics_used),
                },
            )
        if kind == 'topic' and topics_used >= topics_total:
            raise QuotaExceeded(
                scope='free',
                kind=kind,
                status={
                    'trial_until': expires_at,
                    'trial_files_left': max(0, files_total - files_used),
                    'trial_topics_left': 0,
                },
            )
        return


async def reserve_user_quota(user_id: int, kind: str) -> dict:
    """Reserve 1 usage for `kind` (file|topic).

    Free tier is a one-time trial window (default 1 day) with separate quotas for:
    - file uploads (AI from file)
    - topic quizzes

    Premium uses separate quota counters and an expiry datetime.

    Returns a reservation dict that can be passed to `refund_user_quota()`.
    Raises QuotaExceeded when limit is exceeded.
    """

    user_id = int(user_id or 0)
    kind = str(kind or '').strip().lower()
    if kind not in {'file', 'topic'}:
        raise ValueError('kind must be file|topic')

    if user_id <= 0:
        raise QuotaExceeded(scope='free', kind=kind, status={})

    trial_files_total, trial_topics_total, trial_days = _trial_defaults()
    now = datetime.now(timezone.utc).replace(microsecond=0)

    async with async_session() as session:
        # Premium
        q = await session.get(UserQuota, user_id)
        if q and _is_premium_active(str(getattr(q, 'premium_until', '') or '')):
            files_total = int(getattr(q, 'files_total', 0) or 0)
            files_used = int(getattr(q, 'files_used', 0) or 0)
            topics_total = int(getattr(q, 'topics_total', 0) or 0)
            topics_used = int(getattr(q, 'topics_used', 0) or 0)

            if kind == 'file' and files_used >= files_total:
                raise QuotaExceeded(
                    scope='premium',
                    kind=kind,
                    status={
                        'premium_until': str(getattr(q, 'premium_until', '') or ''),
                        'files_left': 0,
                        'topics_left': max(0, topics_total - topics_used),
                    },
                )
            if kind == 'topic' and topics_used >= topics_total:
                raise QuotaExceeded(
                    scope='premium',
                    kind=kind,
                    status={
                        'premium_until': str(getattr(q, 'premium_until', '') or ''),
                        'files_left': max(0, files_total - files_used),
                        'topics_left': 0,
                    },
                )

            if kind == 'file':
                q.files_used = files_used + 1
            else:
                q.topics_used = topics_used + 1
            q.updated_at = _utc_now_iso()
            session.add(q)
            await session.commit()

            return {'scope': 'premium', 'user_id': user_id, 'kind': kind, 'ts': _utc_now_iso()}

        # Free trial
        tr = await session.get(FreeTrialQuota, user_id)
        if tr is None:
            # Create trial on first use.
            expires = (now + timedelta(days=int(trial_days))).replace(microsecond=0).isoformat()
            tr = FreeTrialQuota(
                user_id=user_id,
                started_at=now.isoformat(),
                expires_at=expires,
                files_total=int(trial_files_total),
                files_used=0,
                topics_total=int(trial_topics_total),
                topics_used=0,
            )
            session.add(tr)
            await session.commit()
            tr = await session.get(FreeTrialQuota, user_id)

        if tr is None:
            raise QuotaExceeded(scope='free', kind=kind, status={})

        expires_at = str(getattr(tr, 'expires_at', '') or '')
        if not _is_trial_active(expires_at):
            raise QuotaExceeded(scope='free', kind=kind, status={'trial_expired': True, 'trial_until': expires_at})

        files_total = int(getattr(tr, 'files_total', 0) or 0)
        files_used = int(getattr(tr, 'files_used', 0) or 0)
        topics_total = int(getattr(tr, 'topics_total', 0) or 0)
        topics_used = int(getattr(tr, 'topics_used', 0) or 0)

        if kind == 'file':
            if files_used >= files_total:
                raise QuotaExceeded(
                    scope='free',
                    kind=kind,
                    status={
                        'trial_until': expires_at,
                        'trial_files_left': 0,
                        'trial_topics_left': max(0, topics_total - topics_used),
                    },
                )
            tr.files_used = files_used + 1
        else:
            if topics_used >= topics_total:
                raise QuotaExceeded(
                    scope='free',
                    kind=kind,
                    status={
                        'trial_until': expires_at,
                        'trial_files_left': max(0, files_total - files_used),
                        'trial_topics_left': 0,
                    },
                )
            tr.topics_used = topics_used + 1

        session.add(tr)
        await session.commit()

        return {'scope': 'free', 'user_id': user_id, 'kind': kind, 'ts': _utc_now_iso(), 'trial': True}


async def refund_user_quota(reservation: dict) -> None:
    """Best-effort refund for a previous reservation."""

    if not isinstance(reservation, dict):
        return
    scope = str(reservation.get('scope') or '').strip().lower()
    kind = str(reservation.get('kind') or '').strip().lower()
    try:
        user_id = int(reservation.get('user_id') or 0)
    except Exception:
        user_id = 0

    if user_id <= 0 or kind not in {'file', 'topic'}:
        return

    async with async_session() as session:
        if scope == 'premium':
            q = await session.get(UserQuota, user_id)
            if q is None:
                return
            if kind == 'file':
                q.files_used = max(0, int(getattr(q, 'files_used', 0) or 0) - 1)
            else:
                q.topics_used = max(0, int(getattr(q, 'topics_used', 0) or 0) - 1)
            q.updated_at = _utc_now_iso()
            session.add(q)
            await session.commit()
            return

        if scope == 'free':
            tr = await session.get(FreeTrialQuota, user_id)
            if tr is None:
                return
            if kind == 'file':
                tr.files_used = max(0, int(getattr(tr, 'files_used', 0) or 0) - 1)
            else:
                tr.topics_used = max(0, int(getattr(tr, 'topics_used', 0) or 0) - 1)
            session.add(tr)
            await session.commit()
            return

async def grant_user_premium(
    user_id: int,
    *,
    plan_code: str,
    duration_days: int,
    files_quota: int,
    topics_quota: int,
) -> dict:
    """Grant/extend premium for a user and add quotas."""

    user_id = int(user_id or 0)
    duration_days = max(1, int(duration_days or 1))
    files_quota = max(0, int(files_quota or 0))
    topics_quota = max(0, int(topics_quota or 0))
    plan_code = str(plan_code or '').strip().lower()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    add = timedelta(days=duration_days)

    async with async_session() as session:
        q = await session.get(UserQuota, user_id)
        if q is None:
            q = UserQuota(
                user_id=user_id,
                premium_until=(now + add).isoformat(),
                plan_code=plan_code,
                files_total=files_quota,
                files_used=0,
                topics_total=topics_quota,
                topics_used=0,
                updated_at=_utc_now_iso(),
            )
            session.add(q)
            await session.commit()
        else:
            until_dt = _parse_iso_dt(str(getattr(q, 'premium_until', '') or ''))
            if until_dt is None or until_dt <= now:
                until_dt = now
                q.files_used = 0
                q.topics_used = 0
                q.files_total = 0
                q.topics_total = 0

            q.premium_until = (until_dt + add).replace(microsecond=0).isoformat()
            q.plan_code = plan_code
            q.files_total = int(getattr(q, 'files_total', 0) or 0) + files_quota
            q.topics_total = int(getattr(q, 'topics_total', 0) or 0) + topics_quota
            q.updated_at = _utc_now_iso()
            session.add(q)
            await session.commit()

        q2 = await session.get(UserQuota, user_id)
        if q2 is None:
            return {'premium_active': False}
        files_total = int(getattr(q2, 'files_total', 0) or 0)
        files_used = int(getattr(q2, 'files_used', 0) or 0)
        topics_total = int(getattr(q2, 'topics_total', 0) or 0)
        topics_used = int(getattr(q2, 'topics_used', 0) or 0)
        return {
            'premium_active': _is_premium_active(str(getattr(q2, 'premium_until', '') or '')),
            'premium_until': str(getattr(q2, 'premium_until', '') or ''),
            'plan_code': str(getattr(q2, 'plan_code', '') or ''),
            'files_total': files_total,
            'files_used': files_used,
            'files_left': max(0, files_total - files_used),
            'topics_total': topics_total,
            'topics_used': topics_used,
            'topics_left': max(0, topics_total - topics_used),
        }


async def create_premium_request(
    user_id: int,
    *,
    plan_code: str,
    screenshot_file_id: str,
    screenshot_type: str,
    ai_verdict: str = '',
) -> int:
    user_id = int(user_id or 0)
    plan_code = str(plan_code or '').strip().lower()
    screenshot_file_id = str(screenshot_file_id or '').strip()
    screenshot_type = str(screenshot_type or '').strip().lower()

    async with async_session() as session:
        req = PremiumRequest(
            user_id=user_id,
            plan_code=plan_code,
            status='pending',
            created_at=_utc_now_iso(),
            reviewed_at='',
            reviewed_by=0,
            screenshot_file_id=screenshot_file_id,
            screenshot_type=screenshot_type,
            ai_verdict=str(ai_verdict or '').strip(),
        )
        session.add(req)
        await session.commit()
        await session.refresh(req)
        return int(req.id)


async def get_premium_request(request_id: int) -> Optional[dict]:
    rid = int(request_id or 0)
    if rid <= 0:
        return None

    async with async_session() as session:
        req = await session.get(PremiumRequest, rid)
        if req is None:
            return None
        return {
            'id': int(getattr(req, 'id', 0) or 0),
            'user_id': int(getattr(req, 'user_id', 0) or 0),
            'plan_code': str(getattr(req, 'plan_code', '') or ''),
            'status': str(getattr(req, 'status', '') or ''),
            'created_at': str(getattr(req, 'created_at', '') or ''),
            'reviewed_at': str(getattr(req, 'reviewed_at', '') or ''),
            'reviewed_by': int(getattr(req, 'reviewed_by', 0) or 0),
            'screenshot_file_id': str(getattr(req, 'screenshot_file_id', '') or ''),
            'screenshot_type': str(getattr(req, 'screenshot_type', '') or ''),
            'ai_verdict': str(getattr(req, 'ai_verdict', '') or ''),
        }


async def set_premium_request_status(request_id: int, *, status: str, reviewed_by: int = 0) -> bool:
    rid = int(request_id or 0)
    if rid <= 0:
        return False

    status = str(status or '').strip().lower()
    if status not in {'pending', 'approved', 'rejected'}:
        return False

    async with async_session() as session:
        req = await session.get(PremiumRequest, rid)
        if req is None:
            return False
        req.status = status
        req.reviewed_by = int(reviewed_by or 0)
        req.reviewed_at = _utc_now_iso() if status in {'approved', 'rejected'} else ''
        session.add(req)
        await session.commit()
        return True
