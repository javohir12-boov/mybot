import asyncio
import html
import os
import random
import re
import shutil
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from aiogram import Bot, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import AI_ENABLED, ADMIN_IDS, REQUIRED_CHANNEL, get_about_text
from handlers.utils.i18n import lang_name, norm_ui_lang, t
from services.ai_service import AIService, AIServiceError, extract_text_from_file
from services.topic_context_service import fetch_topic_context
from services.import_service import import_format_example, parse_quiz_payload
from services.export_service import ExportServiceError, export_quiz_to_docx, suggest_docx_filename
from services.database import (
    QuotaExceeded,
    check_user_quota,
    clear_manual_quiz_draft,
    create_premium_request,
    create_quiz_attempts_bulk,
    create_questions_bulk,
    create_quiz,
    get_manual_quiz_draft,
    get_or_create_user,
    get_or_create_user_settings,
    get_premium_request,
    get_quiz_attempt_stats,
    get_quiz_summary,
    get_quiz_with_questions,
    get_user_counts_summary,
    get_user_quota_status,
    grant_user_premium,
    list_user_quizzes,
    refund_user_quota,
    get_referral_status,
    qualify_referral_if_any,
    record_referral_invite,
    reserve_user_quota,
    set_premium_request_status,
    set_user_default_lang,
    set_user_ui_lang,
    upsert_manual_quiz_draft,
    update_quiz_meta,
    update_question_correct_answer,
)

router = Router()
ai_service = AIService()

_DOWNLOAD_DIR = Path("downloads")
_ALLOWED_SUFFIXES = {".pdf", ".docx", ".pptx", ".txt", ".md", ".json", ".png", ".jpg", ".jpeg", ".webp"}
# Invisible placeholder keeps the Telegram message bubble compact for menu-only messages.
# --- Quotas / limits -------------------------------------------------
_PAYMENT_CARD_NUMBER = str(os.getenv('PAYMENT_CARD_NUMBER', '') or '').strip()
_PAYMENT_CARD_HOLDER = str(os.getenv('PAYMENT_CARD_HOLDER', '') or '').strip()
_TOPIC_MAX_CHARS = int(os.getenv('TOPIC_MAX_CHARS', '200') or 200)
_PREMIUM_RECEIPT_AI = str(os.getenv('PREMIUM_RECEIPT_AI', '1') or '1').strip().lower() in {'1','true','yes','y','on'}
_PREMIUM_RECEIPT_AUTOAPPROVE = str(os.getenv('PREMIUM_RECEIPT_AUTOAPPROVE', '1') or '1').strip().lower() in {'1','true','yes','y','on'}
try:
    _PREMIUM_RECEIPT_APPROVE_CONF = float(os.getenv('PREMIUM_RECEIPT_APPROVE_CONF', '0.9') or 0.9)
except Exception:
    _PREMIUM_RECEIPT_APPROVE_CONF = 0.9
_PREMIUM_RECEIPT_NOTIFY_ADMINS = str(os.getenv('PREMIUM_RECEIPT_NOTIFY_ADMINS', '1') or '1').strip().lower() in {'1','true','yes','y','on'}

def _max_upload_mb_for_suffix(suffix: str) -> int:
    s = str(suffix or '').strip().lower()
    if s == '.pptx':
        return int(os.getenv('MAX_UPLOAD_PPTX_MB', '25') or 25)
    return int(os.getenv('MAX_UPLOAD_MB', '5') or 5)

# Premium plans (manual approval via screenshot)
_PREMIUM_BASE_DAY_PRICE_UZS = 7890
_PREMIUM_PLANS = {
    '1d': {'days': 1, 'price': 7890, 'files': 2, 'topics': 6, 'disc': 12},
    '7d': {'days': 7, 'price': 29890, 'files': 14, 'topics': 30, 'disc': 0},
    '30d': {'days': 30, 'price': 59890, 'files': 40, 'topics': 120, 'disc': 0},
}

def _plan_discount_pct(days: int, price: int) -> int:
    try:
        base = float(_PREMIUM_BASE_DAY_PRICE_UZS) * float(max(1, int(days)))
        if base <= 0:
            return 0
        return max(0, int(round((1.0 - (float(price) / base)) * 100)))
    except Exception:
        return 0

for _code, _p in _PREMIUM_PLANS.items():
    if not int(_p.get('disc') or 0):
        _p['disc'] = _plan_discount_pct(int(_p.get('days') or 1), int(_p.get('price') or 0))

_ACTIVE_RUNS: Dict[str, "QuizRun"] = {}
_POLL_CTX: Dict[str, "PollContext"] = {}
_BOT_USERNAME: Optional[str] = None
_UI_LANG_CACHE: Dict[int, tuple[str, float]] = {}
_UI_LANG_TTL_SEC = 600.0
_PAUSED_RUNS: Dict[str, "PausedRun"] = {}
_PAUSED_RUNS_TTL_SEC = 24 * 60 * 60  # best-effort, in-memory only
_PENDING_AFTER_SUB: Dict[int, str] = {}
_MANUAL_CORRECT_LOCKS: Dict[int, asyncio.Lock] = {}
_START_LANG_PROMPT = "🌐 Interfeys tili / Interface language / Язык интерфейса"


def _manual_correct_lock(user_id: int) -> asyncio.Lock:
    user_id = int(user_id or 0)
    lock = _MANUAL_CORRECT_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _MANUAL_CORRECT_LOCKS[user_id] = lock
    return lock


async def _get_ui_lang(user_id: int) -> str:
    if not user_id:
        return "uz"
    now = time.monotonic()
    cached = _UI_LANG_CACHE.get(int(user_id))
    if cached and (now - float(cached[1])) < _UI_LANG_TTL_SEC:
        return norm_ui_lang(cached[0])
    settings = await get_or_create_user_settings(user_id=int(user_id))
    lang = norm_ui_lang(str(settings.get("ui_lang") or "uz"))
    _UI_LANG_CACHE[int(user_id)] = (lang, now)
    return lang


def _set_ui_lang_cache(user_id: int, ui_lang: str) -> None:
    if not user_id:
        return
    _UI_LANG_CACHE[int(user_id)] = (norm_ui_lang(ui_lang), time.monotonic())


def _set_pending_after_sub(user_id: int, action: str) -> None:
    if not user_id:
        return
    action = str(action or "").strip()
    if action:
        _PENDING_AFTER_SUB[int(user_id)] = action


def _pop_pending_after_sub(user_id: int) -> str:
    if not user_id:
        return ""
    return str(_PENDING_AFTER_SUB.pop(int(user_id), "") or "")


def _lang_flag(code: str) -> str:
    c = str(code or "").strip().lower()
    return {
        "uz": "🇺🇿",
        "ru": "🇷🇺",
        "en": "🇬🇧",
        "de": "🇩🇪",
        "tr": "🇹🇷",
        "kk": "🇰🇿",
        "ar": "🇸🇦",
        "zh": "🇨🇳",
        "ko": "🇰🇷",
    }.get(c, "🌐")


def _lang_self_name(code: str) -> str:
    c = str(code or "").strip().lower()
    return {
        "uz": "O'zbek",
        "ru": "Русский",
        "en": "English",
        "de": "Deutsch",
        "tr": "Türkçe",
        "kk": "Қазақша",
        "ar": "العربية",
        "zh": "中文",
        "ko": "한국어",
    }.get(c, c or "")


def _lang_label_with_flag(code: str) -> str:
    return f"{_lang_flag(code)} {_lang_self_name(code)}".strip()


@dataclass
class AnswerRecord:
    option_id: int
    is_correct: bool
    elapsed: float


@dataclass
class UserScore:
    name: str
    username: str = ""
    correct: int = 0
    answered: int = 0
    total_time: float = 0.0


@dataclass
class PollContext:
    run_id: str
    question_index: int
    started_at: float
    correct_option_id: int
    # Snapshot of users that must answer for early-advance (groups only).
    expected_users: set[int] = field(default_factory=set)


@dataclass
class QuizRun:
    run_id: str
    chat_id: int
    chat_type: str
    created_by: int
    title: str
    questions: List[Dict[str, Any]]
    open_period: int
    output_language: str
    ui_lang: str = "uz"
    quiz_id: Optional[int] = None
    shuffle_mode: str = "none"
    shuffle_strategy: str = "saved"
    current_index: int = 0
    cancelled: bool = False
    started: bool = True
    task: Optional[asyncio.Task] = None
    lobby_message_id: Optional[int] = None
    current_poll_message_id: Optional[int] = None
    current_poll_id: Optional[str] = None
    current_question_index: Optional[int] = None
    advance_event: asyncio.Event = field(default_factory=asyncio.Event)
    # Joined users are required for early-advance. In private chats it's just the creator.
    participants: Dict[int, str] = field(default_factory=dict)
    # Users who already answered the current question (participants only).
    answered_users: set[int] = field(default_factory=set)
    # poll ids created for this run (cleanup at the end).
    poll_ids: List[str] = field(default_factory=list)
    # Per-user score for the whole run.
    scores: Dict[int, UserScore] = field(default_factory=dict)
    # Per-question answers (question_index -> user_id -> AnswerRecord)
    answers: Dict[int, Dict[int, AnswerRecord]] = field(default_factory=dict)
    # Per-user consecutive "no answer" streak (used to pause/disable inactive users).
    no_answer_streak: Dict[int, int] = field(default_factory=dict)
    # Per-user start index (0-based question index) of the current no-answer streak.
    no_answer_streak_start: Dict[int, int] = field(default_factory=dict)
    # In groups: users removed after 3 missed questions can be re-activated on next answer.
    inactive_participants: Dict[int, str] = field(default_factory=dict)


@dataclass
class PausedRun:
    token: str
    user_id: int
    chat_id: int
    chat_type: str
    quiz_id: Optional[int]
    title: str
    questions: List[Dict[str, Any]]
    open_period: int
    output_language: str
    ui_lang: str
    shuffle_mode: str
    shuffle_strategy: str
    current_index: int
    scores: Dict[int, UserScore]
    created_at: float


def _cleanup_paused_runs() -> None:
    now = time.monotonic()
    for tok, pr in list(_PAUSED_RUNS.items()):
        try:
            if now - float(getattr(pr, "created_at", 0.0)) > float(_PAUSED_RUNS_TTL_SEC):
                _PAUSED_RUNS.pop(tok, None)
        except Exception:
            _PAUSED_RUNS.pop(tok, None)


def _store_paused_run(run: QuizRun, *, user_id: int, resume_index: int) -> str:
    _cleanup_paused_runs()
    token = uuid.uuid4().hex
    resume_index = max(0, min(len(run.questions), int(resume_index or 0)))
    _PAUSED_RUNS[token] = PausedRun(
        token=token,
        user_id=int(user_id),
        chat_id=int(run.chat_id),
        chat_type=str(run.chat_type or "private"),
        quiz_id=int(run.quiz_id) if run.quiz_id is not None else None,
        title=str(run.title or ""),
        questions=[dict(q) for q in (run.questions or [])],
        open_period=int(run.open_period or 30),
        output_language=str(run.output_language or "source"),
        ui_lang=str(run.ui_lang or "uz"),
        shuffle_mode=str(run.shuffle_mode or "none"),
        shuffle_strategy=str(run.shuffle_strategy or "saved"),
        current_index=resume_index,
        scores=dict(run.scores or {}),
        created_at=time.monotonic(),
    )
    return token


def _kb_resume(token: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_resume"), callback_data=f"run_resume:{token}")
    kb.adjust(1)
    return kb.as_markup()


def _safe_filename(name: str) -> str:
    name = (name or "file").strip()
    name = Path(name).name
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name or "file"

async def _send_quiz_docx(
    bot: Bot,
    *,
    quiz_id: int,
    target_chat_id: int,
    ui_lang: str,
) -> bool:
    """Generate a .docx for the quiz and send it. Returns True on success."""

    try:
        quiz = await get_quiz_with_questions(int(quiz_id))
    except Exception:
        quiz = None

    if not quiz:
        try:
            await bot.send_message(int(target_chat_id), t(ui_lang, "quiz_not_found"))
        except Exception:
            pass
        return False

    filename = suggest_docx_filename(str(quiz.get("title") or "Quiz"), int(quiz_id))

    # Keep uniqueness to avoid concurrent collisions.
    export_dir = Path("media") / "exports"
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    tmp_path = export_dir / f"{uuid.uuid4().hex}_{_safe_filename(filename)}"

    answer_title = t(ui_lang, "export_answer_key_title")
    try:
        await asyncio.to_thread(
            export_quiz_to_docx,
            quiz,
            tmp_path,
            answer_title=answer_title,
            include_answer_text=True,
            include_explanations=False,
            include_images=True,
        )
    except ExportServiceError as exc:
        try:
            await bot.send_message(int(target_chat_id), t(ui_lang, "export_failed", err=str(exc)))
        except Exception:
            pass
        return False
    except Exception as exc:
        try:
            await bot.send_message(int(target_chat_id), t(ui_lang, "export_failed", err=str(exc)))
        except Exception:
            pass
        return False

    try:
        caption = t(
            ui_lang,
            "export_docx_caption",
            title=str(quiz.get("title") or ""),
            id=int(quiz_id),
        )
        await bot.send_document(int(target_chat_id), types.FSInputFile(str(tmp_path)), caption=caption)
        return True
    except Exception as exc:
        try:
            await bot.send_message(int(target_chat_id), t(ui_lang, "export_failed", err=str(exc)))
        except Exception:
            pass
        return False
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass



def _is_under_dir(path: Path, base_dir: Path) -> bool:
    try:
        p = path.resolve()
        b = base_dir.resolve()
        return p == b or b in p.parents
    except Exception:
        s = str(path).replace("\\", "/").lower()
        b = str(base_dir).replace("\\", "/").lower().rstrip("/")
        return s == b or s.startswith(b + "/")


def _render_pdf_pages_to_images(pdf_path: Path, out_dir: Path, *, max_pages: int = 30, zoom: float = 2.0) -> List[str]:
    """Render PDF pages to PNG images. Intended for scanned PDFs (no text)."""
    try:
        import fitz  # PyMuPDF
    except Exception as exc:
        raise RuntimeError("PyMuPDF (fitz) kerak. O'rnatish: pip install PyMuPDF") from exc

    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[str] = []
    max_pages = max(1, min(80, int(max_pages or 30)))
    z = float(zoom or 2.0)
    z = max(1.0, min(3.0, z))

    with fitz.open(str(pdf_path)) as doc:
        total = min(int(getattr(doc, "page_count", 0) or 0), max_pages)
        mat = fitz.Matrix(z, z)
        for i in range(total):
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img_path = out_dir / f"page_{i+1}.png"
            pix.save(str(img_path))
            paths.append(str(img_path))

    return paths


def _render_pdf_page_range_to_images(
    pdf_path: Path,
    out_dir: Path,
    page_from: int,
    page_to: int,
    *,
    max_pages: int = 30,
    zoom: float = 2.0,
) -> List[str]:
    """Render a 1-based inclusive PDF page range to PNG images."""
    try:
        import fitz  # PyMuPDF
    except Exception as exc:
        raise RuntimeError("PyMuPDF (fitz) kerak. O'rnatish: pip install PyMuPDF") from exc

    p_from = int(page_from or 0)
    p_to = int(page_to or 0)
    if p_from < 1 or p_to < 1 or p_to < p_from:
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[str] = []
    max_pages = max(1, min(80, int(max_pages or 30)))
    z = float(zoom or 2.0)
    z = max(1.0, min(3.0, z))

    with fitz.open(str(pdf_path)) as doc:
        total_pages = int(getattr(doc, "page_count", 0) or 0)
        if total_pages <= 0:
            return []

        start_i = max(0, min(total_pages - 1, p_from - 1))
        end_i = max(0, min(total_pages - 1, p_to - 1))
        if end_i < start_i:
            return []

        mat = fitz.Matrix(z, z)
        for i in range(start_i, end_i + 1):
            if len(paths) >= max_pages:
                break
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img_path = out_dir / f"page_{i+1}.png"
            pix.save(str(img_path))
            paths.append(str(img_path))

    return paths


def _extract_pptx_media_images(pptx_path: Path, out_dir: Path, *, max_images: int = 30) -> List[str]:
    """Extract embedded images from a PPTX (ppt/media/*) into out_dir."""
    import zipfile

    out_dir.mkdir(parents=True, exist_ok=True)
    max_images = max(1, min(200, int(max_images or 30)))
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    paths: List[str] = []

    with zipfile.ZipFile(str(pptx_path)) as z:
        names = [
            n
            for n in z.namelist()
            if n.startswith("ppt/media/") and Path(n).suffix.lower() in exts
        ]

        def _key(name: str) -> tuple[int, str]:
            stem = Path(name).stem
            m = re.search(r"(\d+)", stem)
            num = int(m.group(1)) if m else 0
            return (num, name)

        names.sort(key=_key)

        for i, name in enumerate(names[:max_images], start=1):
            try:
                data = z.read(name)
            except Exception:
                continue
            ext = Path(name).suffix.lower() or ".png"
            dst = out_dir / f"slide_{i}{ext}"
            try:
                dst.write_bytes(data)
            except Exception:
                continue
            paths.append(str(dst))

    return paths


def _pdf_page_count(pdf_path: Path) -> int:
    try:
        import fitz  # PyMuPDF
    except Exception as exc:
        raise RuntimeError("PyMuPDF (fitz) kerak. O'rnatish: pip install PyMuPDF") from exc
    with fitz.open(str(pdf_path)) as doc:
        return int(getattr(doc, "page_count", 0) or 0)


def _pptx_slide_count(pptx_path: Path) -> int:
    import zipfile

    try:
        with zipfile.ZipFile(str(pptx_path)) as z:
            slides = [
                n
                for n in z.namelist()
                if n.startswith("ppt/slides/slide") and n.endswith(".xml")
            ]
        return len(slides)
    except Exception:
        return 0


def _extract_pptx_text_range(pptx_path: Path, slide_from: int, slide_to: int, *, char_limit: int = 200000) -> str:
    """Extract text from a 1-based inclusive slide range inside a PPTX."""
    import zipfile
    import xml.etree.ElementTree as ET

    s_from = int(slide_from or 0)
    s_to = int(slide_to or 0)
    if s_from < 1 or s_to < 1 or s_to < s_from:
        return ""

    parts: List[str] = []
    total = 0

    with zipfile.ZipFile(str(pptx_path)) as z:
        slides = [
            n
            for n in z.namelist()
            if n.startswith("ppt/slides/slide") and n.endswith(".xml")
        ]

        def _key(name: str) -> int:
            m = re.search(r"slide(\d+)\.xml$", name)
            return int(m.group(1)) if m else 0

        slides.sort(key=_key)
        max_slide = len(slides)
        if max_slide <= 0:
            return ""

        start_i = max(1, min(max_slide, s_from))
        end_i = max(1, min(max_slide, s_to))
        if end_i < start_i:
            return ""

        for i in range(start_i - 1, end_i):
            name = slides[i]
            try:
                root = ET.fromstring(z.read(name))
            except Exception:
                continue
            texts: List[str] = []
            for el in root.iter():
                if el.tag.endswith("}t") and el.text:
                    t = el.text.strip()
                    if t:
                        texts.append(t)
            if not texts:
                continue
            chunk = " ".join(texts)
            parts.append(chunk)
            total += len(chunk)
            if total >= int(char_limit or 200000):
                break

    return "\n".join(parts)


def _extract_pdf_text_range(pdf_path: Path, page_from: int, page_to: int, *, char_limit: int = 200000) -> str:
    """Extract text from a 1-based inclusive page range."""
    try:
        import fitz  # PyMuPDF
    except Exception as exc:
        raise RuntimeError("PyMuPDF (fitz) kerak. O'rnatish: pip install PyMuPDF") from exc

    p_from = int(page_from or 0)
    p_to = int(page_to or 0)
    if p_from < 1 or p_to < 1:
        return ""
    if p_to < p_from:
        return ""

    parts: List[str] = []
    total = 0
    with fitz.open(str(pdf_path)) as doc:
        max_page = int(getattr(doc, "page_count", 0) or 0)
        if max_page <= 0:
            return ""
        start_i = max(0, min(max_page - 1, p_from - 1))
        end_i = max(0, min(max_page - 1, p_to - 1))
        if end_i < start_i:
            return ""
        for i in range(start_i, end_i + 1):
            page = doc.load_page(i)
            txt = page.get_text() or ""
            if not txt:
                continue
            parts.append(txt)
            total += len(txt)
            if total >= int(char_limit or 200000):
                break
    return "\n".join(parts)


def _user_mention_html(user_id: int, name: str, username: str = "") -> str:
    safe_name = html.escape(str(name or str(user_id)))
    link = f'<a href="tg://user?id={int(user_id)}">{safe_name}</a>'
    un = (username or "").strip().lstrip("@")
    if un:
        return f"{link} (@{html.escape(un)})"
    return link


def _rank_icon(i: int) -> str:
    if i == 1:
        return "🥇"
    if i == 2:
        return "🥈"
    if i == 3:
        return "🥉"
    return f"{i}."


def _format_scoreboard(run: QuizRun, *, limit: int = 20) -> str:
    total_questions = len(run.questions)
    ui_lang = norm_ui_lang(getattr(run, "ui_lang", "uz"))

    rows: List[dict] = []
    for user_id, score in (run.scores or {}).items():
        answered = int(score.answered or 0)
        correct = int(score.correct or 0)
        total_time_s = int(round(float(score.total_time or 0.0)))
        avg = (total_time_s / answered) if answered else 0.0
        rows.append(
            {
                "user_id": int(user_id),
                "name": str(score.name or f"{user_id}"),
                "username": str(getattr(score, "username", "") or ""),
                "correct": correct,
                "answered": answered,
                "total_time": total_time_s,
                "avg": avg,
            }
        )

    rows.sort(key=lambda x: (-x["correct"], -x["answered"], x["total_time"], str(x["name"]).lower()))

    title = t(ui_lang, "scoreboard_title").rstrip(":").strip() or "Results"
    lines: List[str] = [f"🏆 <b>{html.escape(title)}</b>"]

    if run.chat_type in {"group", "supergroup"} and run.participants:
        lines.append("👥 " + html.escape(t(ui_lang, "participants_joined", n=len(run.participants))))
    lines.append("🧾 " + html.escape(t(ui_lang, "total_questions", n=total_questions)))
    lines.append("")

    shown = rows[: max(1, int(limit or 20))]
    for i, r in enumerate(shown, start=1):
        icon = _rank_icon(i)
        mention = _user_mention_html(r["user_id"], r["name"], r.get("username") or "")
        missed = max(0, int(total_questions) - int(r["answered"]))
        avg_s = f"{float(r['avg']):.1f}".rstrip("0").rstrip(".")

        lines.append(f"{icon} {mention}")

        detail_parts = [f"✅ {int(r['correct'])}/{int(r['answered'])}"]
        detail_parts.append(f"⏱ {int(r['total_time'])}s")
        if int(r["answered"]):
            detail_parts.append(f"⌀ {avg_s}s")
        if missed:
            detail_parts.append(f"⏭ {missed}")
        lines.append("   " + " | ".join(detail_parts))
        lines.append("")

    if len(rows) > len(shown):
        lines.append(html.escape(t(ui_lang, "scoreboard_more", n=(len(rows) - len(shown)))))

    return "\n".join(lines).strip()


async def _get_bot_username(bot: Bot) -> str:
    global _BOT_USERNAME
    if _BOT_USERNAME:
        return _BOT_USERNAME
    me = await bot.get_me()
    _BOT_USERNAME = str(getattr(me, "username", "") or "")
    return _BOT_USERNAME


def _quiz_start_link(bot_username: str, quiz_id: int) -> str:
    u = (bot_username or "").strip().lstrip("@")
    if not u:
        return ""
    return f"https://t.me/{u}?start=quiz_{int(quiz_id)}"


def _quiz_startgroup_link(bot_username: str, quiz_id: int) -> str:
    u = (bot_username or "").strip().lstrip("@")
    if not u:
        return ""
    return f"https://t.me/{u}?startgroup=quiz_{int(quiz_id)}"


def _telegram_share_url(url: str, text: str = "") -> str:
    base = "https://t.me/share/url"
    u = quote_plus(str(url or ""))
    if text:
        t = quote_plus(str(text))
        return f"{base}?url={u}&text={t}"
    return f"{base}?url={u}"


def _kb_quiz_share(
    bot_username: str,
    quiz_id: int,
    *,
    title: str = "",
    question_count: int = 0,
    chat_type: str = "private",
    ui_lang: str = "uz",
    show_stats: bool = False,
    show_edit: bool = False,
) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    start_link = _quiz_start_link(bot_username, quiz_id)
    startgroup_link = _quiz_startgroup_link(bot_username, quiz_id)

    kb.button(text=t(ui_lang, "btn_start_quiz"), callback_data=f"quiz_run:{quiz_id}")

    if start_link:
        share_text = t(
            ui_lang,
            "quiz_brief",
            title=str(title or f"Quiz {int(quiz_id)}"),
            count=int(question_count or 0),
            id=int(quiz_id),
        )
        kb.button(text=t(ui_lang, "btn_share_quiz"), url=_telegram_share_url(start_link, share_text))
    else:
        kb.button(text=t(ui_lang, "btn_share_quiz"), callback_data=f"quiz_share:{quiz_id}")

    ct = (chat_type or "private").lower()
    if ct not in {"group", "supergroup"}:
        if startgroup_link:
            kb.button(text=t(ui_lang, "btn_start_group"), url=startgroup_link)
        else:
            kb.button(text=t(ui_lang, "btn_start_group"), callback_data=f"quiz_startgroup_fallback:{quiz_id}")

    if show_stats:
        kb.button(text=t(ui_lang, "btn_stats"), callback_data=f"quiz_stats:{quiz_id}")

    if show_edit:
        kb.button(text=t(ui_lang, "btn_edit_quiz"), callback_data=f"quiz_edit:{quiz_id}")
        kb.button(text=t(ui_lang, "btn_export_docx"), callback_data=f"quiz_export:{quiz_id}")

    kb.adjust(2)
    return kb.as_markup()


def _kb_quiz_result_actions(
    bot_username: str,
    quiz_id: int,
    *,
    title: str = "",
    question_count: int = 0,
    chat_type: str = "private",
    ui_lang: str = "uz",
) -> types.InlineKeyboardMarkup:
    """Actions shown under the final results message."""
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    start_link = _quiz_start_link(bot_username, quiz_id)
    startgroup_link = _quiz_startgroup_link(bot_username, quiz_id)

    # Retry in the current chat
    kb.button(text=t(ui_lang, "btn_retry_quiz"), callback_data=f"quiz_run:{quiz_id}")

    # Share deep link
    if start_link:
        share_text = t(
            ui_lang,
            "quiz_brief",
            title=str(title or f"Quiz {int(quiz_id)}"),
            count=int(question_count or 0),
            id=int(quiz_id),
        )
        kb.button(text=t(ui_lang, "btn_share_quiz"), url=_telegram_share_url(start_link, share_text))
    else:
        kb.button(text=t(ui_lang, "btn_share_quiz"), callback_data=f"quiz_share:{quiz_id}")

    # Start in group (link or fallback)
    if startgroup_link:
        kb.button(text=t(ui_lang, "btn_start_group"), url=startgroup_link)
    else:
        kb.button(text=t(ui_lang, "btn_start_group"), callback_data=f"quiz_startgroup_fallback:{quiz_id}")

    kb.adjust(2)
    return kb.as_markup()


async def _start_saved_quiz(
    bot: Bot,
    *,
    chat_id: int,
    chat_type: str,
    user: types.User,
    quiz_id: int,
) -> None:
    ui_lang = await _get_ui_lang(int(getattr(user, "id", 0) or 0))
    quiz = await get_quiz_with_questions(int(quiz_id))
    if not quiz:
        await bot.send_message(chat_id, t(ui_lang, "quiz_not_found"))
        return

    questions = quiz.get("questions") or []
    if not questions:
        await bot.send_message(chat_id, t(ui_lang, "quiz_no_questions"))
        return

    # Cancel user's previous runs in this chat to avoid confusion.
    await _cancel_user_runs(bot, chat_id=chat_id, user_id=user.id)
    # Only one active test per chat to avoid chaos in groups.
    for existing in list(_ACTIVE_RUNS.values()):
        if existing.chat_id == chat_id and not existing.cancelled:
            await bot.send_message(chat_id, t(ui_lang, "chat_has_active_quiz"))
            return

    title = str(quiz.get("title") or f"Quiz {quiz_id}")
    open_period = int(quiz.get("open_period") or 30)
    shuffle_mode = str(quiz.get("shuffle_mode") or "none").strip().lower()
    shuffle_strategy = _normalize_shuffle_strategy(quiz.get("shuffle_strategy") or "saved")

    run_id = uuid.uuid4().hex
    ct = (chat_type or "private").strip().lower()
    questions_prepared = [dict(q) for q in (questions or [])]
    if shuffle_strategy == "runtime":
        shuffle_questions, shuffle_options = _shuffle_mode_flags(shuffle_mode)
        if shuffle_questions or shuffle_options:
            questions_prepared = _apply_quiz_shuffle(
                questions_prepared,
                shuffle_questions=shuffle_questions,
                shuffle_options=shuffle_options,
            )

    run = QuizRun(
        run_id=run_id,
        chat_id=int(chat_id),
        chat_type=ct or "private",
        created_by=user.id,
        title=title,
        questions=questions_prepared,
        open_period=max(5, min(600, open_period)),
        output_language="source",
        ui_lang=ui_lang,
        quiz_id=int(quiz_id),
        shuffle_mode=shuffle_mode,
        shuffle_strategy=shuffle_strategy,
        started=ct not in {"group", "supergroup"},
        participants={user.id: user.full_name or str(user.id)},
    )
    run.scores.setdefault(user.id, UserScore(name=run.participants[user.id], username=(getattr(user, "username", "") or "")))
    run.scores[user.id].username = (getattr(user, "username", "") or run.scores[user.id].username)
    _ACTIVE_RUNS[run_id] = run

    total_sec = max(0, len(questions) * max(0, run.open_period))
    est = f"{total_sec//60}m {total_sec%60}s" if total_sec >= 60 else f"{total_sec}s"

    if ct in {"group", "supergroup"}:
        m = await bot.send_message(chat_id, _format_lobby(run), reply_markup=_kb_lobby(run_id, ui_lang=ui_lang))
        run.lobby_message_id = m.message_id
        return

    await bot.send_message(
        chat_id,
        t(
            ui_lang,
            "quiz_started_private",
            count=len(questions),
            sec=int(run.open_period),
            est=est,
            quiz_id=int(quiz_id),
        ),
        reply_markup=_kb_run_controls(run_id, ui_lang=ui_lang),
    )
    run.task = asyncio.create_task(_run_quiz(bot, run))


async def _send_poll(
    bot: Bot, chat_id: int, number: int, q: Dict[str, Any], open_period: int, *, ui_lang: str = "uz"
) -> tuple[Optional[int], Optional[str]]:
    question = str(q.get("question") or "").strip()
    options = q.get("options") or []
    correct_index = int(q.get("correct_index") or 0)
    explanation = str(q.get("explanation") or "").strip()
    image_file_id = str(q.get("image_file_id") or "").strip()
    image_path = str(q.get("image_path") or "").strip()

    has_image = bool(image_file_id or (image_path and Path(image_path).exists()))

    if (not question and not has_image) or not isinstance(options, list) or len(options) != 4:
        return None, None

    if has_image:
        caption_text = question or t(ui_lang, "image_question")
        caption = f"{number}. {caption_text}"[:1024]
        try:
            if image_file_id:
                try:
                    await bot.send_photo(chat_id, image_file_id, caption=caption)
                except Exception:
                    await bot.send_document(chat_id, image_file_id, caption=caption)
            else:
                if image_path and Path(image_path).exists():
                    f = types.FSInputFile(image_path)
                    try:
                        await bot.send_photo(chat_id, f, caption=caption)
                    except Exception:
                        await bot.send_document(chat_id, f, caption=caption)
        except Exception:
            pass

    kwargs: Dict[str, Any] = {
        "chat_id": chat_id,
        "question": (f"{number}. {t(ui_lang, 'choose_answer')}" if has_image else f"{number}. {question}")[:300],
        "options": [str(o)[:100] for o in options],
        "type": "quiz",
        "correct_option_id": max(0, min(3, correct_index)),
        "is_anonymous": False,
    }
    if open_period and open_period >= 5:
        kwargs["open_period"] = min(600, open_period)
    if explanation:
        kwargs["explanation"] = explanation[:200]

    try:
        msg = await bot.send_poll(**kwargs)
        poll_id = getattr(getattr(msg, "poll", None), "id", None)
        return msg.message_id, poll_id
    except TypeError:
        kwargs.pop("explanation", None)
        msg = await bot.send_poll(**kwargs)
        if explanation:
            await bot.send_message(chat_id, t(ui_lang, "explanation_prefix", text=explanation))
        poll_id = getattr(getattr(msg, "poll", None), "id", None)
        return msg.message_id, poll_id
    except Exception:
        return None, None


async def _run_quiz(bot: Bot, run: QuizRun) -> None:
    try:
        total = len(run.questions)
        while run.current_index < total and not run.cancelled:
            idx = run.current_index
            run.current_index += 1

            run.answered_users.clear()
            run.current_question_index = idx

            msg_id, poll_id = await _send_poll(
                bot,
                run.chat_id,
                idx + 1,
                run.questions[idx],
                run.open_period,
                ui_lang=run.ui_lang,
            )
            run.current_poll_message_id = msg_id
            run.current_poll_id = poll_id

            if poll_id:
                started_at = time.monotonic()
                q = run.questions[idx]
                correct_id = int(q.get("correct_index") or 0)
                correct_id = max(0, min(3, correct_id))
                expected_users = set(run.participants.keys())
                # In groups, avoid closing polls early if only 1 user has joined so far.
                if run.chat_type in {"group", "supergroup"} and len(expected_users) < 2:
                    expected_users = set()
                _POLL_CTX[poll_id] = PollContext(
                    run_id=run.run_id,
                    question_index=idx,
                    started_at=started_at,
                    correct_option_id=correct_id,
                    expected_users=expected_users,
                )
                run.poll_ids.append(poll_id)

            # Wait for the poll time, unless the creator skips ahead.
            run.advance_event.clear()
            timeout = max(1.0, float(run.open_period))
            try:
                await asyncio.wait_for(run.advance_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

            # Ensure the poll is closed before moving on (especially in groups).
            if run.current_poll_message_id:
                try:
                    await bot.stop_poll(run.chat_id, run.current_poll_message_id)
                except Exception:
                    pass
            # Update per-user "no answer" streaks and pause/disable inactive users.
            if run.participants:
                # Reset streak for users who answered; increment for users who didn't.
                for uid in list(run.participants.keys()):
                    if uid in run.answered_users:
                        run.no_answer_streak[uid] = 0
                        run.no_answer_streak_start.pop(uid, None)
                        continue
                    prev = int(run.no_answer_streak.get(uid, 0) or 0)
                    if prev <= 0:
                        run.no_answer_streak_start[uid] = int(idx)
                    run.no_answer_streak[uid] = prev + 1

                # Private chat: pause the whole quiz after 3 consecutive missed questions.
                if run.chat_type == "private":
                    uid = int(run.created_by)
                    if uid in run.participants and int(run.no_answer_streak.get(uid, 0) or 0) >= 3:
                        resume_from = int(run.no_answer_streak_start.get(uid, idx) or idx)
                        token = _store_paused_run(run, user_id=uid, resume_index=resume_from)
                        run.cancelled = True
                        if run.current_poll_message_id:
                            try:
                                await bot.stop_poll(run.chat_id, run.current_poll_message_id)
                            except Exception:
                                pass
                        await bot.send_message(
                            run.chat_id,
                            t(run.ui_lang, "quiz_paused_inactive"),
                            reply_markup=_kb_resume(token, ui_lang=run.ui_lang),
                        )
                        return

                # Groups: temporarily disable inactive participants after 3 missed questions.
                if run.chat_type in {"group", "supergroup"}:
                    to_disable: List[int] = []
                    for uid in list(run.participants.keys()):
                        if int(run.no_answer_streak.get(uid, 0) or 0) >= 3:
                            to_disable.append(uid)
                    for uid in to_disable:
                        run.inactive_participants[uid] = run.participants.pop(uid, str(uid))
                        run.answered_users.discard(uid)

                    # If everyone became inactive, stop the quiz to avoid spamming the chat.
                    if not run.participants:
                        run.cancelled = True
                        await bot.send_message(run.chat_id, t(run.ui_lang, "quiz_stopped_no_participants"))
                        break

        # Persist best-effort attempt stats for this quiz (creator can view per-quiz statistics).
        any_answered = any(int(getattr(sc, "answered", 0) or 0) > 0 for sc in (run.scores or {}).values())
        if run.quiz_id and run.scores and any_answered:
            try:
                payload = []
                for uid, sc in (run.scores or {}).items():
                    payload.append(
                        {
                            "user_id": int(uid),
                            "full_name": str(sc.name or uid),
                            "username": str(getattr(sc, "username", "") or ""),
                            "correct": int(sc.correct or 0),
                            "answered": int(sc.answered or 0),
                            "total_time": float(sc.total_time or 0.0),
                        }
                    )
                await create_quiz_attempts_bulk(
                    int(run.quiz_id),
                    payload,
                    chat_id=int(run.chat_id),
                    chat_type=str(run.chat_type or ""),
                    total_questions=int(total),
                    open_period=int(run.open_period or 0),
                    finished=not bool(run.cancelled),
                )
            except Exception:
                pass

        if not run.cancelled:
            try:
                await bot.send_message(run.chat_id, t(run.ui_lang, "quiz_finished"))
            except Exception:
                pass

        if run.scores:
            reply_markup = None
            if run.quiz_id:
                try:
                    username = await _get_bot_username(bot)
                except Exception:
                    username = ""
                reply_markup = _kb_quiz_result_actions(
                    username,
                    int(run.quiz_id),
                    title=str(run.title or ""),
                    question_count=len(run.questions),
                    chat_type=str(run.chat_type or ""),
                    ui_lang=run.ui_lang,
                )

            score_text = _format_scoreboard(run)
            try:
                await bot.send_message(
                    run.chat_id,
                    score_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=reply_markup,
                )
            except Exception:
                # Fallback: send without HTML if Telegram rejects formatting.
                try:
                    import re as _re
                    plain = _re.sub(r"<[^>]+>", "", score_text)
                    if len(plain) > 3900:
                        plain = plain[:3900] + "..."
                    await bot.send_message(
                        run.chat_id,
                        plain,
                        disable_web_page_preview=True,
                        reply_markup=reply_markup,
                    )
                except Exception:
                    pass

    except asyncio.CancelledError:
        run.cancelled = True
        raise
    finally:
        for pid in run.poll_ids:
            _POLL_CTX.pop(pid, None)
        _ACTIVE_RUNS.pop(run.run_id, None)


def _kb_main_menu(ui_lang: str, *, user_id: int = 0, show_start_lang: bool = False) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()

    if show_start_lang:
        for code in ("uz", "en", "ru"):
            kb.button(text=_lang_label_with_flag(code), callback_data=f"set_ui_lang:{code}")

    kb.button(text=t(ui_lang, "btn_upload"), callback_data="menu_upload")
    if AI_ENABLED:
        kb.button(text=t(ui_lang, "btn_topic"), callback_data="menu_topic")
    kb.button(text=t(ui_lang, "btn_newquiz"), callback_data="menu_newquiz")
    kb.button(text=t(ui_lang, "btn_ui_lang"), callback_data="menu_ui_language")
    kb.button(text=t(ui_lang, "btn_premium"), callback_data="menu_premium")
    if int(user_id or 0) in set(int(x) for x in (ADMIN_IDS or [])):
        kb.button(text=t(ui_lang, "btn_admin_users"), callback_data="menu_admin_users")

    rows: list[int] = []
    if show_start_lang:
        rows.append(3)
    rows.extend([2, 2, 1])
    if int(user_id or 0) in set(int(x) for x in (ADMIN_IDS or [])):
        rows.append(1)
    kb.adjust(*rows)

    return kb.as_markup()




async def _open_upload_flow(message: types.Message, state: FSMContext, *, ui_lang: str, user_id: int = 0) -> None:
    # If the user was building a manual quiz and switched to another flow, persist the draft first.
    if int(user_id or 0):
        await _persist_manual_draft(state, user_id=int(user_id), chat_id=int(message.chat.id))
    await state.clear()
    await state.set_state(UploadStates.await_file)
    key = "upload_hint" if AI_ENABLED else "upload_hint_noai"
    await message.answer(t(ui_lang, key))


async def _open_topic_flow(message: types.Message, state: FSMContext, *, user_id: int, ui_lang: str) -> None:
    # If the user was building a manual quiz and switched to another flow, persist the draft first.
    if int(user_id or 0):
        await _persist_manual_draft(state, user_id=int(user_id), chat_id=int(message.chat.id))
    await state.clear()
    if not AI_ENABLED:
        await message.answer(t(ui_lang, "ai_disabled"))
        return
    session_id = uuid.uuid4().hex
    await state.update_data(
        ai_session_id=session_id,
        ai_mode="topic",
        ai_difficulty="",
        ai_title="",
        ai_ui_lang=ui_lang,
        ai_chat_id=message.chat.id,
        ai_chat_type=message.chat.type,
        ai_user_id=user_id,
        ai_topic_return="count",
    )
    await state.set_state(AIQuizStates.choose_topic)
    await message.answer(t(ui_lang, "topic_prompt"))


async def _open_manual_quiz_flow(message: types.Message, state: FSMContext, *, user_id: int, ui_lang: str) -> None:
    # If the user was building a manual quiz and re-opened /newquiz, persist the draft first.
    if int(user_id or 0):
        await _persist_manual_draft(state, user_id=int(user_id), chat_id=int(message.chat.id))
    await state.clear()
    draft = await get_manual_quiz_draft(user_id=user_id)
    if draft and str(draft.get("state") or "").strip():
        await message.answer(t(ui_lang, "manual_draft_found"), reply_markup=_kb_manual_draft_choice(ui_lang=ui_lang))
        return
    await state.update_data(m_ui_lang=ui_lang)
    await state.set_state(ManualQuizStates.title)
    await _persist_manual_draft(state, user_id=user_id, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_title_prompt"))


async def _resume_pending_after_sub(call: types.CallbackQuery, state: FSMContext) -> bool:
    action = _pop_pending_after_sub(call.from_user.id if call.from_user else 0)
    if not action or not call.message or not call.from_user:
        return False
    ui_lang = await _get_ui_lang(call.from_user.id)
    if action == "menu_upload":
        await _open_upload_flow(call.message, state, ui_lang=ui_lang, user_id=call.from_user.id)
        return True
    if action == "menu_topic":
        await _open_topic_flow(call.message, state, user_id=call.from_user.id, ui_lang=ui_lang)
        return True
    if action == "menu_newquiz":
        await _open_manual_quiz_flow(call.message, state, user_id=call.from_user.id, ui_lang=ui_lang)
        return True
    return False


def _required_channel_url() -> str:
    ch = str(REQUIRED_CHANNEL or "").strip()
    if not ch:
        return ""
    if ch.startswith("https://") or ch.startswith("http://"):
        return ch
    if ch.startswith("@"):
        return "https://t.me/" + ch[1:]
    if ch.startswith("t.me/"):
        return "https://" + ch
    return ""

def _kb_required_channel(ui_lang: str) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    url = _required_channel_url()
    if url:
        kb.button(text=t(ui_lang, "btn_join_channel"), url=url)
    else:
        kb.button(text=t(ui_lang, "btn_join_channel"), callback_data="check_sub")
    kb.button(text=t(ui_lang, "btn_check_sub"), callback_data="check_sub")
    kb.adjust(1)
    return kb.as_markup()

async def _is_user_subscribed(bot: Bot, user_id: int) -> bool:
    ch = str(REQUIRED_CHANNEL or "").strip()
    if not ch:
        return True
    try:
        member = await bot.get_chat_member(chat_id=ch, user_id=int(user_id))
        status = str(getattr(member, "status", "") or "").lower()
        return status in {"creator", "administrator", "member"}
    except Exception:
        return False

async def _ensure_subscribed(event: object, bot: Bot, user_id: int, *, pending_action: str = "") -> bool:
    user_id = int(user_id or 0)
    ch = str(REQUIRED_CHANNEL or "").strip()
    if not ch:
        return True
    if not user_id:
        return True

    ok = await _is_user_subscribed(bot, user_id)
    if ok:
        # If this user came via a referral link, mark it qualified once they join the required channel.
        try:
            info = await qualify_referral_if_any(referred_user_id=user_id)
            ref_id = int(info.get('referrer_id') or 0)
            if ref_id > 0 and bool(info.get('qualified')):
                ui_lang = await _get_ui_lang(ref_id)
                st = await get_referral_status(ref_id)
                # Notify referrer in private (best-effort).
                try:
                    if bool(info.get('rewarded')):
                        await bot.send_message(ref_id, t(ui_lang, 'ref_rewarded', files=2, topics=1))
                    else:
                        await bot.send_message(ref_id, t(ui_lang, 'ref_progress', n=int(st.get('unrewarded_qualified') or 0), need=3))
                except Exception:
                    pass
        except Exception:
            pass
        return True

    if pending_action:
        _set_pending_after_sub(user_id, pending_action)

    ui_lang = await _get_ui_lang(user_id)
    try:
        if isinstance(event, types.CallbackQuery):
            try:
                await event.answer(t(ui_lang, "sub_required_alert"), show_alert=False)
            except Exception:
                pass
            if getattr(event, "message", None):
                await event.message.answer(
                    t(ui_lang, "must_join_channel", channel=ch),
                    reply_markup=_kb_required_channel(ui_lang),
                    disable_web_page_preview=True,
                )
        elif isinstance(event, types.Message):
            await event.answer(
                t(ui_lang, "must_join_channel", channel=ch),
                reply_markup=_kb_required_channel(ui_lang),
                disable_web_page_preview=True,
            )
    except Exception:
        pass

    return False

async def _cancel_user_runs(bot: Bot, chat_id: int, user_id: int) -> int:
    cancelled = 0
    for run in list(_ACTIVE_RUNS.values()):
        if run.chat_id != chat_id or run.created_by != user_id:
            continue
        run.cancelled = True
        cancelled += 1
        if run.current_poll_message_id:
            try:
                await bot.stop_poll(run.chat_id, run.current_poll_message_id)
            except Exception:
                pass
        if run.task and not run.task.done():
            run.task.cancel()
        _ACTIVE_RUNS.pop(run.run_id, None)
    return cancelled


@router.message(CommandStart(deep_link=True))
async def cmd_start_deeplink(
    message: types.Message,
    command: CommandObject,
    state: FSMContext,
    bot: Bot,
) -> None:
    # If the user was building a manual quiz, persist the draft before clearing the FSM state.
    if message.from_user:
        await _persist_manual_draft(state, user_id=message.from_user.id, chat_id=int(message.chat.id))
    await state.clear()
    if message.from_user:
        await get_or_create_user(
            user_id=message.from_user.id,
            full_name=message.from_user.full_name,
            username=getattr(message.from_user, "username", None),
        )
        await get_or_create_user_settings(user_id=message.from_user.id)

    payload = (command.args or "").strip()
    # Referral deep link: start=ref_<referrer_id>
    if payload.startswith('ref_'):
        try:
            ref_id = int(re.search(r'\d+', payload).group(0))  # type: ignore[union-attr]
        except Exception:
            ref_id = 0
        if message.from_user and ref_id > 0:
            try:
                await record_referral_invite(referrer_id=ref_id, referred_user_id=message.from_user.id)
            except Exception:
                pass
        payload = ''

    if payload.startswith("quiz_"):
        raw_id = payload.split("_", 1)[1]
        try:
            quiz_id = int(re.search(r"\d+", raw_id).group(0))  # type: ignore[union-attr]
        except Exception:
            ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
            await message.answer(t(ui_lang, "bad_link"))
            return

        summary = await get_quiz_summary(quiz_id)
        if not summary:
            ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
            await message.answer(t(ui_lang, "quiz_not_found"))
            return

        username = await _get_bot_username(bot)
        title = str(summary.get("title") or "")
        count = int(summary.get("question_count") or 0)
        open_period = int(summary.get("open_period") or 30)
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
        await message.answer(
            t(ui_lang, "quiz_card", title=title, count=count, sec=open_period, id=quiz_id),
            reply_markup=_kb_quiz_share(
                username,
                quiz_id,
                title=title,
                question_count=count,
                chat_type=message.chat.type,
                ui_lang=ui_lang,
            ),
        )
        return

    user_id = message.from_user.id if message.from_user else 0
    settings = await get_or_create_user_settings(user_id=user_id) if user_id else {"ui_lang": "uz", "ui_lang_picked": True}
    ui_lang = norm_ui_lang(str(settings.get("ui_lang") or "uz"))
    _set_ui_lang_cache(int(user_id or 0), ui_lang)

    await message.answer(
        get_about_text(ui_lang),
        reply_markup=_kb_main_menu(ui_lang, user_id=user_id, show_start_lang=True),
    )


@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext, bot: Bot) -> None:
    # If the user was building a manual quiz, persist the draft before clearing the FSM state.
    if message.from_user:
        await _persist_manual_draft(state, user_id=message.from_user.id, chat_id=int(message.chat.id))
    await state.clear()
    if message.from_user:
        await get_or_create_user(
            user_id=message.from_user.id,
            full_name=message.from_user.full_name,
            username=getattr(message.from_user, "username", None),
        )
        await get_or_create_user_settings(user_id=message.from_user.id)
    user_id = message.from_user.id if message.from_user else 0
    settings = await get_or_create_user_settings(user_id=user_id) if user_id else {"ui_lang": "uz", "ui_lang_picked": True}
    ui_lang = norm_ui_lang(str(settings.get("ui_lang") or "uz"))
    _set_ui_lang_cache(int(user_id or 0), ui_lang)

    await message.answer(
        get_about_text(ui_lang),
        reply_markup=_kb_main_menu(ui_lang, user_id=user_id, show_start_lang=True),
    )


@router.message(Command("menu"))
async def cmd_menu(message: types.Message, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    await message.answer(t(ui_lang, "menu_help"), reply_markup=_kb_main_menu(ui_lang, user_id=message.from_user.id if message.from_user else 0))


@router.callback_query(F.data == "check_sub")
async def check_subscription(call: types.CallbackQuery, bot: Bot, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id if call.from_user else 0)
    ch = str(REQUIRED_CHANNEL or "").strip()

    ok = await _is_user_subscribed(bot, call.from_user.id if call.from_user else 0)
    if not ok:
        await call.answer(t(ui_lang, "sub_check_fail"), show_alert=True)
        if call.message:
            await call.message.answer(
                t(ui_lang, "must_join_channel", channel=ch),
                reply_markup=_kb_required_channel(ui_lang),
                disable_web_page_preview=True,
            )
        return

    await call.answer(t(ui_lang, "sub_check_ok"), show_alert=False)
    resumed = await _resume_pending_after_sub(call, state)
    if call.message and not resumed:
        await call.message.answer(
            get_about_text(ui_lang),
            reply_markup=_kb_main_menu(ui_lang, user_id=call.from_user.id if call.from_user else 0, show_start_lang=False),
        )



@router.message(Command("topic"))
async def cmd_topic(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(message, bot, message.from_user.id if message.from_user else 0, pending_action="menu_topic"):
        return
    if not message.from_user:
        return
    if not AI_ENABLED:
        ui_lang = await _get_ui_lang(message.from_user.id)
        await message.answer(t(ui_lang, "ai_disabled"))
        return
    await _persist_manual_draft(state, user_id=message.from_user.id, chat_id=int(message.chat.id))
    await state.clear()
    ui_lang = await _get_ui_lang(message.from_user.id)
    session_id = uuid.uuid4().hex
    await state.update_data(
        ai_session_id=session_id,
        ai_mode="topic",
        ai_difficulty="",
        ai_title="",
        ai_ui_lang=ui_lang,
        ai_chat_id=message.chat.id,
        ai_chat_type=message.chat.type,
        ai_user_id=message.from_user.id,
        ai_topic_return="count",
    )
    await state.set_state(AIQuizStates.choose_topic)
    await message.answer(t(ui_lang, "topic_prompt"))


@router.callback_query(F.data.startswith("quiz_share:"))
async def quiz_share_fallback(call: types.CallbackQuery, bot: Bot) -> None:
    if not await _ensure_subscribed(call, bot, call.from_user.id if call.from_user else 0):
        return
    # Fallback if URL buttons can't be built (no username).
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    username = await _get_bot_username(bot)
    link = _quiz_start_link(username, quiz_id)
    if not link:
        await call.answer(t(ui_lang, "bot_username_missing"), show_alert=True)
        return

    await call.answer()
    if call.message:
        await call.message.answer(t(ui_lang, "share_link", link=link))



@router.callback_query(F.data.startswith("quiz_export:"))
async def quiz_export_docx(call: types.CallbackQuery, bot: Bot) -> None:
    if not await _ensure_subscribed(call, bot, call.from_user.id if call.from_user else 0):
        return
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(int(quiz_id))
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return

    creator_id = int(summary.get("creator_id") or 0)
    if (int(call.from_user.id) != creator_id) and (int(call.from_user.id) not in ADMIN_IDS):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await call.answer(t(ui_lang, "export_working"))

    chat_id = int(call.message.chat.id) if call.message else int(call.from_user.id)
    chat_type = str(call.message.chat.type) if call.message else "private"
    ct = (chat_type or "").strip().lower()

    if ct in {"group", "supergroup"}:
        sent_private = await _send_quiz_docx(
            bot,
            quiz_id=int(quiz_id),
            target_chat_id=int(call.from_user.id),
            ui_lang=ui_lang,
        )
        if sent_private:
            try:
                await bot.send_message(int(chat_id), t(ui_lang, "export_sent_private_notice"))
            except Exception:
                pass
            return

        # Fallback to current chat if private is unavailable.
        await _send_quiz_docx(bot, quiz_id=int(quiz_id), target_chat_id=int(chat_id), ui_lang=ui_lang)
        return

    await _send_quiz_docx(bot, quiz_id=int(quiz_id), target_chat_id=int(chat_id), ui_lang=ui_lang)


@router.callback_query(F.data.startswith("quiz_startgroup_fallback:"))
async def quiz_startgroup_fallback(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    username = await _get_bot_username(bot)
    link = _quiz_startgroup_link(username, quiz_id)
    if not link:
        await call.answer(t(ui_lang, "bot_username_missing"), show_alert=True)
        return

    await call.answer()
    if call.message:
        await call.message.answer(t(ui_lang, "group_start_link", link=link))


@router.callback_query(F.data.startswith("quiz_run:"))
async def quiz_run_here(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    if not call.message:
        await call.answer(t(ui_lang, "chat_not_found"), show_alert=True)
        return

    await call.answer(t(ui_lang, "starting"))
    await _start_saved_quiz(
        bot,
        chat_id=call.message.chat.id,
        chat_type=call.message.chat.type,
        user=call.from_user,
        quiz_id=quiz_id,
    )


@router.callback_query(F.data.startswith("quiz_stats:"))
async def quiz_stats(call: types.CallbackQuery) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return

    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "stats_creator_only"), show_alert=True)
        return

    stats = await get_quiz_attempt_stats(quiz_id, limit=30)
    await call.answer()
    if not call.message:
        return

    if not stats:
        await call.message.answer(t(ui_lang, "stats_no_attempts"))
        return

    title = str(summary.get("title") or f"Quiz {quiz_id}").strip()
    qcount = int(summary.get("question_count") or 0)

    head = t(ui_lang, "stats_title", title=title)
    lines: List[str] = [f"📈 <b>{html.escape(head)}</b>"]
    lines.append(f"🆔 ID: {int(quiz_id)}")
    lines.append(f"📍 {html.escape(t(ui_lang, 'total_questions', n=qcount))}")
    lines.append("")

    for i, item in enumerate(stats[:30], start=1):
        uid = int(item.get("user_id") or 0)
        name = str(item.get("name") or uid)
        username = str(item.get("username") or "")
        correct = int(item.get("score") or 0)
        answered = int(item.get("answered") or 0)
        total_time_s = int(item.get("total_time") or 0)
        attempts = int(item.get("attempts") or 1)
        avg = (total_time_s / answered) if answered else 0.0
        avg_s = f"{avg:.1f}".rstrip("0").rstrip(".")

        mention = _user_mention_html(uid, name, username)
        lines.append(f"{_rank_icon(i)} {mention}")
        details = [f"✅ {correct}/{answered}", f"⏱ {total_time_s}s"]
        if answered:
            details.append(f"⌀ {avg_s}s")
        details.append(f"🔁 {attempts}x")
        lines.append("   " + " | ".join(details))
        lines.append("")

    await call.message.answer(
        "\n".join(lines).strip(),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


class QuizEditStates(StatesGroup):
    title = State()
    open_period = State()


def _kb_quiz_edit_menu(quiz_id: int, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_edit_title"), callback_data=f"quiz_edit_title:{int(quiz_id)}")
    kb.button(text=t(ui_lang, "btn_edit_time"), callback_data=f"quiz_edit_time:{int(quiz_id)}")
    kb.button(text=t(ui_lang, "btn_edit_answers"), callback_data=f"quiz_edit_answers:{int(quiz_id)}")
    kb.button(text=t(ui_lang, "btn_back"), callback_data=f"quiz_edit_back:{int(quiz_id)}")
    kb.adjust(2, 2)
    return kb.as_markup()


def _kb_quiz_edit_questions(
    quiz_id: int,
    *,
    questions: List[Dict[str, Any]],
    offset: int,
    ui_lang: str,
    page_size: int = 20,
) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    offset = max(0, int(offset or 0))
    page_size = max(5, min(30, int(page_size or 20)))

    total = len(questions or [])
    page = (questions or [])[offset : offset + page_size]

    kb = InlineKeyboardBuilder()
    num_count = 0
    for i, q in enumerate(page, start=offset + 1):
        qid = int(q.get("question_id") or 0)
        if not qid:
            continue
        kb.button(text=str(i), callback_data=f"quiz_edit_answer_q:{int(quiz_id)}:{qid}:{offset}")
        num_count += 1

    nav_count = 0
    if offset > 0:
        prev_off = max(0, offset - page_size)
        kb.button(text=t(ui_lang, "btn_prev_page"), callback_data=f"quiz_edit_answers:{int(quiz_id)}:{prev_off}")
        nav_count += 1
    if (offset + page_size) < total:
        next_off = offset + page_size
        kb.button(text=t(ui_lang, "btn_next_page"), callback_data=f"quiz_edit_answers:{int(quiz_id)}:{next_off}")
        nav_count += 1

    kb.button(text=t(ui_lang, "btn_back"), callback_data=f"quiz_edit:{int(quiz_id)}")

    sizes: List[int] = []
    rem = num_count
    while rem > 0:
        sizes.append(min(5, rem))
        rem -= 5
    if nav_count:
        sizes.append(nav_count)
    sizes.append(1)
    kb.adjust(*sizes)
    return kb.as_markup()


def _kb_quiz_edit_correct_answer(
    quiz_id: int,
    *,
    question_id: int,
    offset: int,
    ui_lang: str,
) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for i in range(4):
        kb.button(text=str(i + 1), callback_data=f"quiz_edit_answer_set:{int(quiz_id)}:{int(question_id)}:{i}:{int(offset)}")
    kb.button(text=t(ui_lang, "btn_back"), callback_data=f"quiz_edit_answers:{int(quiz_id)}:{int(offset)}")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def _kb_quiz_edit_cancel(quiz_id: int, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_back"), callback_data=f"quiz_edit:{int(quiz_id)}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"quiz_edit_cancel:{int(quiz_id)}")
    kb.adjust(2)
    return kb.as_markup()


async def _send_quiz_card_for_creator(
    bot: Bot,
    *,
    chat_id: int,
    chat_type: str,
    ui_lang: str,
    quiz_id: int,
) -> None:
    summary = await get_quiz_summary(int(quiz_id))
    if not summary:
        await bot.send_message(chat_id, t(ui_lang, "quiz_not_found"))
        return

    username = await _get_bot_username(bot)
    title = str(summary.get("title") or f"Quiz {quiz_id}").strip()
    count = int(summary.get("question_count") or 0)
    sec = int(summary.get("open_period") or 30)
    await bot.send_message(
        chat_id,
        t(ui_lang, "quiz_card", title=title, count=count, sec=sec, id=int(quiz_id)),
        reply_markup=_kb_quiz_share(
            username,
            int(quiz_id),
            title=title,
            question_count=count,
            chat_type=chat_type,
            ui_lang=ui_lang,
            show_stats=True,
            show_edit=True,
        ),
    )


@router.callback_query(F.data.startswith("quiz_edit_back:"))
async def quiz_edit_back(call: types.CallbackQuery, bot: Bot, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await state.clear()
    await call.answer()
    if call.message:
        await _send_quiz_card_for_creator(
            bot,
            chat_id=call.message.chat.id,
            chat_type=call.message.chat.type,
            ui_lang=ui_lang,
            quiz_id=quiz_id,
        )


@router.callback_query(F.data.startswith("quiz_edit_cancel:"))
async def quiz_edit_cancel(call: types.CallbackQuery, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    await state.clear()
    await call.answer(t(ui_lang, "cancelled"))


@router.callback_query(F.data.startswith("quiz_edit:"))
async def quiz_edit_menu(call: types.CallbackQuery, bot: Bot, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await state.clear()
    await call.answer()
    if call.message:
        title = str(summary.get("title") or f"Quiz {quiz_id}").strip()
        count = int(summary.get("question_count") or 0)
        sec = int(summary.get("open_period") or 30)
        await call.message.answer(
            t(ui_lang, "edit_menu", title=title, count=count, sec=sec, id=int(quiz_id)),
            reply_markup=_kb_quiz_edit_menu(quiz_id, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("quiz_edit_answers:"))
async def quiz_edit_answers(call: types.CallbackQuery, bot: Bot, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    parts = str(call.data or "").split(":")
    if len(parts) < 2:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    try:
        quiz_id = int(parts[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    offset = 0
    if len(parts) >= 3:
        try:
            offset = int(parts[2])
        except Exception:
            offset = 0

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    quiz = await get_quiz_with_questions(quiz_id)
    if not quiz:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return

    questions = quiz.get("questions") or []
    if not isinstance(questions, list) or not questions:
        await call.answer(t(ui_lang, "quiz_no_questions"), show_alert=True)
        return

    await call.answer()
    if call.message:
        await call.message.answer(
            t(ui_lang, "edit_answers_choose_question", count=len(questions)),
            reply_markup=_kb_quiz_edit_questions(quiz_id, questions=questions, offset=offset, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("quiz_edit_answer_q:"))
async def quiz_edit_answer_pick(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    parts = str(call.data or "").split(":")
    if len(parts) < 4:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    try:
        quiz_id = int(parts[1])
        question_id = int(parts[2])
        offset = int(parts[3])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    quiz = await get_quiz_with_questions(quiz_id)
    if not quiz:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return

    questions = quiz.get("questions") or []
    if not isinstance(questions, list) or not questions:
        await call.answer(t(ui_lang, "quiz_no_questions"), show_alert=True)
        return

    picked: Optional[Dict[str, Any]] = None
    for q in questions:
        if int(q.get("question_id") or 0) == int(question_id):
            picked = q
            break

    if not picked:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return

    opts = picked.get("options") or []
    if not isinstance(opts, list):
        opts = []
    opts = [str(o) for o in opts][:4]

    lines: List[str] = []
    qtext = str(picked.get("question") or "").strip()
    if qtext:
        lines.append(qtext)
    if opts:
        for i, o in enumerate(opts, start=1):
            lines.append(f"{i}) {o}")
    lines.append("")
    lines.append(t(ui_lang, "edit_answers_choose_correct"))

    await call.answer()
    if call.message:
        await call.message.answer(
            "\n".join(lines).strip(),
            reply_markup=_kb_quiz_edit_correct_answer(quiz_id, question_id=question_id, offset=offset, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("quiz_edit_answer_set:"))
async def quiz_edit_answer_set(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    parts = str(call.data or "").split(":")
    if len(parts) < 5:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    try:
        quiz_id = int(parts[1])
        question_id = int(parts[2])
        correct_index = int(parts[3])
        offset = int(parts[4])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    ok = await update_question_correct_answer(quiz_id=quiz_id, question_id=question_id, correct_index=correct_index)
    if not ok:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    await call.answer(t(ui_lang, "edit_answers_updated"), show_alert=False)

    # Re-open the same question view for quick verification.
    quiz = await get_quiz_with_questions(quiz_id)
    if not quiz:
        return
    questions = quiz.get("questions") or []
    if not isinstance(questions, list) or not questions:
        return
    picked: Optional[Dict[str, Any]] = None
    for q in questions:
        if int(q.get("question_id") or 0) == int(question_id):
            picked = q
            break
    if not picked:
        return

    opts = picked.get("options") or []
    if not isinstance(opts, list):
        opts = []
    opts = [str(o) for o in opts][:4]
    lines: List[str] = []
    qtext = str(picked.get("question") or "").strip()
    if qtext:
        lines.append(qtext)
    if opts:
        for i, o in enumerate(opts, start=1):
            lines.append(f"{i}) {o}")
    lines.append("")
    lines.append(t(ui_lang, "edit_answers_choose_correct"))

    if call.message:
        await call.message.answer(
            "\n".join(lines).strip(),
            reply_markup=_kb_quiz_edit_correct_answer(quiz_id, question_id=question_id, offset=offset, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("quiz_edit_title:"))
async def quiz_edit_title_start(call: types.CallbackQuery, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await call.answer()
    await state.clear()
    await state.update_data(e_quiz_id=int(quiz_id), e_ui_lang=ui_lang)
    await state.set_state(QuizEditStates.title)
    if call.message:
        await call.message.answer(t(ui_lang, "edit_title_prompt"), reply_markup=_kb_quiz_edit_cancel(quiz_id, ui_lang=ui_lang))


@router.callback_query(F.data.startswith("quiz_edit_time:"))
async def quiz_edit_time_start(call: types.CallbackQuery, state: FSMContext) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await call.answer()
    await state.clear()
    await state.update_data(e_quiz_id=int(quiz_id), e_ui_lang=ui_lang)
    await state.set_state(QuizEditStates.open_period)
    if call.message:
        await call.message.answer(t(ui_lang, "edit_time_prompt"), reply_markup=_kb_quiz_edit_time_presets(quiz_id, ui_lang=ui_lang))


@router.callback_query(F.data.startswith("quiz_edit_time_set:"))
async def quiz_edit_time_set(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not call.from_user:
        return
    parts = str(call.data or "").split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("e_ui_lang") or "")) or await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(parts[1])
        sec = int(parts[2])
    except Exception:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    if sec not in _TIME_PRESET_VALUES:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    state_quiz_id = int(data.get("e_quiz_id") or 0)
    if state_quiz_id and state_quiz_id != quiz_id:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await state.clear()
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if int(summary.get("creator_id") or 0) != int(call.from_user.id):
        await state.clear()
        await call.answer(t(ui_lang, "edit_creator_only"), show_alert=True)
        return

    await call.answer(t(ui_lang, "accepted"))
    await update_quiz_meta(quiz_id, call.from_user.id, open_period=int(sec))
    await state.clear()
    if call.message:
        await call.message.answer(t(ui_lang, "edit_saved"))
        await _send_quiz_card_for_creator(
            bot,
            chat_id=call.message.chat.id,
            chat_type=call.message.chat.type,
            ui_lang=ui_lang,
            quiz_id=quiz_id,
        )


@router.message(QuizEditStates.title)
async def quiz_edit_title_apply(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("e_ui_lang") or "")) or await _get_ui_lang(message.from_user.id)
    quiz_id = int(data.get("e_quiz_id") or 0)
    title = (message.text or "").strip()
    if not title:
        await message.answer(t(ui_lang, "manual_title_required"))
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await state.clear()
        await message.answer(t(ui_lang, "quiz_not_found"))
        return
    if int(summary.get("creator_id") or 0) != int(message.from_user.id):
        await state.clear()
        await message.answer(t(ui_lang, "edit_creator_only"))
        return

    await update_quiz_meta(quiz_id, message.from_user.id, title=title)
    await state.clear()
    await message.answer(t(ui_lang, "edit_saved"))
    await _send_quiz_card_for_creator(
        bot,
        chat_id=message.chat.id,
        chat_type=message.chat.type,
        ui_lang=ui_lang,
        quiz_id=quiz_id,
    )


@router.message(QuizEditStates.open_period)
async def quiz_edit_time_apply(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("e_ui_lang") or "")) or await _get_ui_lang(message.from_user.id)
    quiz_id = int(data.get("e_quiz_id") or 0)
    sec = _first_int(message.text or "")
    if sec is None or sec < 5 or sec > 600:
        await message.answer(t(ui_lang, "time_invalid"), reply_markup=_kb_quiz_edit_time_presets(quiz_id, ui_lang=ui_lang))
        return

    summary = await get_quiz_summary(quiz_id)
    if not summary:
        await state.clear()
        await message.answer(t(ui_lang, "quiz_not_found"))
        return
    if int(summary.get("creator_id") or 0) != int(message.from_user.id):
        await state.clear()
        await message.answer(t(ui_lang, "edit_creator_only"))
        return

    await update_quiz_meta(quiz_id, message.from_user.id, open_period=int(sec))
    await state.clear()
    await message.answer(t(ui_lang, "edit_saved"))
    await _send_quiz_card_for_creator(
        bot,
        chat_id=message.chat.id,
        chat_type=message.chat.type,
        ui_lang=ui_lang,
        quiz_id=quiz_id,
    )


# Backward compatibility for old messages (previous button name).
@router.callback_query(F.data.startswith("quiz_start:"))
async def quiz_start_here(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id)
    try:
        quiz_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "error_short"), show_alert=True)
        return

    if not call.message:
        await call.answer(t(ui_lang, "chat_not_found"), show_alert=True)
        return

    await call.answer(t(ui_lang, "starting"))
    await _start_saved_quiz(
        bot,
        chat_id=call.message.chat.id,
        chat_type=call.message.chat.type,
        user=call.from_user,
        quiz_id=quiz_id,
    )


def _kb_ai_language_settings(ui_lang: str) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "lang_source"), callback_data="set_lang:source")
    for code in ("uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"):
        kb.button(text=lang_name(code), callback_data=f"set_lang:{code}")
    kb.adjust(3, 3, 3, 1)
    return kb.as_markup()


def _kb_ui_language_settings(ui_lang: str) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for code in ("uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"):
        kb.button(text=_lang_label_with_flag(code), callback_data=f"set_ui_lang:{code}")
    kb.adjust(3, 3, 3)
    return kb.as_markup()



@router.message(Command("til"))
@router.message(Command("lang"))
async def cmd_ui_language(message: types.Message, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    await message.answer(t(ui_lang, "ui_lang_choose"), reply_markup=_kb_ui_language_settings(ui_lang))


@router.callback_query(F.data == "menu_ui_language")
async def menu_ui_language(call: types.CallbackQuery, bot: Bot) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if call.message:
        await call.message.answer(t(ui_lang, "ui_lang_choose"), reply_markup=_kb_ui_language_settings(ui_lang))


@router.message(Command("language"))
async def cmd_language(message: types.Message, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not AI_ENABLED:
        await message.answer(t(ui_lang, "ai_disabled"))
        return
    await message.answer(t(ui_lang, "ai_lang_choose"), reply_markup=_kb_ai_language_settings(ui_lang))


@router.callback_query(F.data == "menu_language")
async def menu_language(call: types.CallbackQuery, bot: Bot) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if not AI_ENABLED:
        if call.message:
            await call.message.answer(t(ui_lang, "ai_disabled"))
        return
    if call.message:
        await call.message.answer(t(ui_lang, "ai_lang_choose"), reply_markup=_kb_ai_language_settings(ui_lang))


@router.callback_query(F.data.startswith("set_ui_lang:"))
async def set_ui_lang(call: types.CallbackQuery) -> None:
    ui_lang = call.data.split(":", 1)[1].strip().lower()
    if ui_lang not in {"uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        await call.answer(t("uz", "invalid_button"), show_alert=True)
        return

    was_picked = True
    try:
        prev = await get_or_create_user_settings(user_id=call.from_user.id)
        was_picked = bool(prev.get("ui_lang_picked"))
    except Exception:
        was_picked = True

    await set_user_ui_lang(call.from_user.id, ui_lang)
    _set_ui_lang_cache(call.from_user.id, ui_lang)
    await call.answer(t(ui_lang, "saved_short"))
    if call.message:
        if not was_picked:
            await call.message.answer(get_about_text(ui_lang), reply_markup=_kb_main_menu(ui_lang, user_id=call.from_user.id if call.from_user else 0))
        else:
            await call.message.answer(
                t(ui_lang, "ui_lang_saved", lang_name=lang_name(ui_lang)),
                reply_markup=_kb_main_menu(ui_lang, user_id=call.from_user.id if call.from_user else 0),
            )


@router.callback_query(F.data.startswith("set_lang:"))
async def set_lang(call: types.CallbackQuery) -> None:
    lang = call.data.split(":", 1)[1].strip().lower()
    if lang not in {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await set_user_default_lang(call.from_user.id, lang)
    ui_lang = await _get_ui_lang(call.from_user.id)
    await call.answer(t(ui_lang, "saved_short"))
    if call.message:
        name = t(ui_lang, "lang_source") if lang == "source" else lang_name(lang)
        await call.message.answer(t(ui_lang, "ai_lang_saved", lang_name=name))


@router.callback_query(F.data == "menu_upload")
async def menu_upload(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(call, bot, call.from_user.id if call.from_user else 0, pending_action="menu_upload"):
        return
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if call.message:
        await _open_upload_flow(call.message, state, ui_lang=ui_lang, user_id=call.from_user.id)


@router.callback_query(F.data == "menu_topic")
async def menu_topic(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(call, bot, call.from_user.id if call.from_user else 0, pending_action="menu_topic"):
        return
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if call.message:
        await _open_topic_flow(call.message, state, user_id=call.from_user.id, ui_lang=ui_lang)


@router.callback_query(F.data == "menu_newquiz")
async def menu_newquiz(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(call, bot, call.from_user.id if call.from_user else 0, pending_action="menu_newquiz"):
        return
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if call.message:
        await _open_manual_quiz_flow(call.message, state, user_id=call.from_user.id, ui_lang=ui_lang)




class PremiumStates(StatesGroup):
    await_screenshot = State()




class UploadStates(StatesGroup):
    await_file = State()
def _fmt_money_uzs(amount: int) -> str:
    try:
        return f"{int(amount):,}".replace(",", " ")
    except Exception:
        return str(amount)


def _fmt_premium_until(iso: str) -> str:
    raw = str(iso or '').strip()
    if not raw:
        return ''
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        try:
            from zoneinfo import ZoneInfo

            dt = dt.astimezone(ZoneInfo('Asia/Tashkent'))
        except Exception:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception:
        return raw


def _kb_premium_plans(ui_lang: str) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for code in ('1d', '7d', '30d'):
        p = _PREMIUM_PLANS.get(code) or {}
        price = int(p.get('price') or 0)
        disc = int(p.get('disc') or 0)
        label = t(ui_lang, f'premium_plan_{code}')
        price_txt = _fmt_money_uzs(price)
        btn = f"{label} - {price_txt} UZS"
        if disc:
            btn += f" (-{disc}%)"
        kb.button(text=btn[:64], callback_data=f"prem_buy:{code}")
    kb.button(text=t(ui_lang, 'btn_referral'), callback_data='prem_ref')
    kb.button(text=t(ui_lang, 'btn_back'), callback_data='prem_back')
    kb.adjust(1)
    return kb.as_markup()


def _quota_exceeded_text(ui_lang: str, exc: QuotaExceeded) -> str:
    ui_lang = norm_ui_lang(ui_lang)
    scope = str(getattr(exc, 'scope', '') or '').strip().lower()
    status = getattr(exc, 'status', {}) or {}
    if scope == 'premium':
        until = _fmt_premium_until(str(status.get('premium_until') or ''))
        return t(ui_lang, 'limit_premium_reached', until=until or '-')
    return t(ui_lang, 'limit_free_reached')


def _premium_menu_text(ui_lang: str, status: dict) -> str:
    ui_lang = norm_ui_lang(ui_lang)
    if status.get('premium_active'):
        until = _fmt_premium_until(str(status.get('premium_until') or ''))
        return (
            t(
                ui_lang,
                'premium_status_premium',
                until=until or '-',
                f_left=int(status.get('files_left') or 0),
                f_total=int(status.get('files_total') or 0),
                t_left=int(status.get('topics_left') or 0),
                t_total=int(status.get('topics_total') or 0),
            )
            + "\n\n"
            + t(ui_lang, 'premium_choose_plan')
        )
    return (
        t(
            ui_lang,
            'premium_status_free',
            f_left=int(status.get('trial_files_left') or 0),
            f_total=int(status.get('trial_files_total') or 0),
            t_left=int(status.get('trial_topics_left') or 0),
            t_total=int(status.get('trial_topics_total') or 0),
            days=int(status.get('trial_days') or 1),
            until=_fmt_premium_until(str(status.get('trial_until') or '')) or '-',
        )
        + "\n\n"
        + t(ui_lang, 'premium_choose_plan')
    )


@router.message(Command('premium'))
async def cmd_premium(message: types.Message, bot: Bot) -> None:
    if not message.from_user:
        return
    ui_lang = await _get_ui_lang(message.from_user.id)
    st = await get_user_quota_status(message.from_user.id)
    await message.answer(_premium_menu_text(ui_lang, st), reply_markup=_kb_premium_plans(ui_lang))


@router.callback_query(F.data == 'menu_premium')
async def menu_premium(call: types.CallbackQuery, bot: Bot) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    st = await get_user_quota_status(call.from_user.id)
    if call.message:
        await call.message.answer(_premium_menu_text(ui_lang, st), reply_markup=_kb_premium_plans(ui_lang))


@router.callback_query(F.data == 'prem_ref')
async def prem_ref(call: types.CallbackQuery, bot: Bot) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    username = await _get_bot_username(bot)
    link = ''
    if username:
        link = f"https://t.me/{username}?start=ref_{call.from_user.id}"
    st = await get_referral_status(call.from_user.id)
    total = int(st.get('total') or 0)
    qualified = int(st.get('qualified') or 0)
    pending = int(st.get('pending') or 0)
    unr = int(st.get('unrewarded_qualified') or 0)
    to_next = int(st.get('to_next_reward') or 3)
    msg = t(ui_lang, 'ref_info', link=link or '-', total=total, qualified=qualified, pending=pending, unr=unr, to_next=to_next)
    if call.message:
        await call.message.answer(msg, disable_web_page_preview=True)


@router.callback_query(F.data == 'prem_back')
async def prem_back(call: types.CallbackQuery) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    if call.message:
        await call.message.answer(t(ui_lang, "menu_help"), reply_markup=_kb_main_menu(ui_lang, user_id=call.from_user.id if call.from_user else 0))


@router.callback_query(F.data == 'prem_back_plans')
async def prem_back_plans(call: types.CallbackQuery) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    st = await get_user_quota_status(call.from_user.id)
    if call.message:
        await call.message.answer(_premium_menu_text(ui_lang, st), reply_markup=_kb_premium_plans(ui_lang))


@router.callback_query(F.data.startswith('prem_pay:'))
async def prem_pay(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    code = call.data.split(':', 1)[1].strip().lower()
    if code not in _PREMIUM_PLANS:
        await call.answer(t(ui_lang, 'invalid_button'), show_alert=True)
        return

    if not _PAYMENT_CARD_NUMBER:
        if call.message:
            await call.message.answer(t(ui_lang, 'payment_card_missing'))
        return

    p = _PREMIUM_PLANS.get(code) or {}
    price = int(p.get('price') or 0)

    await state.clear()
    await state.update_data(prem_ui_lang=ui_lang, prem_plan_code=code)
    await state.set_state(PremiumStates.await_screenshot)

    holder = _PAYMENT_CARD_HOLDER or '-'
    if call.message:
        await call.message.answer(
            t(
                ui_lang,
                'payment_card_info',
                card=_PAYMENT_CARD_NUMBER,
                holder=holder,
                plan=t(ui_lang, f'premium_plan_{code}'),
                price=_fmt_money_uzs(price),
            )
        )
        await call.message.answer(t(ui_lang, 'premium_send_screenshot'))


@router.callback_query(F.data.startswith('prem_buy:'))
async def prem_buy(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    code = call.data.split(':', 1)[1].strip().lower()
    if code not in _PREMIUM_PLANS:
        await call.answer(t(ui_lang, 'invalid_button'), show_alert=True)
        return

    p = _PREMIUM_PLANS.get(code) or {}
    days = int(p.get('days') or 1)
    price = int(p.get('price') or 0)
    files_q = int(p.get('files') or 0)
    topics_q = int(p.get('topics') or 0)
    disc = int(p.get('disc') or 0)

    await state.clear()
    await state.update_data(prem_ui_lang=ui_lang, prem_plan_code=code)

    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, 'btn_pay'), callback_data=f'prem_pay:{code}')
    kb.button(text=t(ui_lang, 'btn_back'), callback_data='prem_back_plans')
    kb.adjust(1)

    if call.message:
        await call.message.answer(
            t(
                ui_lang,
                'premium_plan_details',
                plan=t(ui_lang, f'premium_plan_{code}'),
                days=days,
                price=_fmt_money_uzs(price),
                disc=disc,
                files=files_q,
                topics=topics_q,
            ),
            reply_markup=kb.as_markup(),
        )


def _is_screenshot_document(message: types.Message) -> bool:
    doc = message.document
    if not doc:
        return False
    name = (doc.file_name or '').lower()
    if any(name.endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.webp', '.pdf')):
        return True
    mt = (doc.mime_type or '').lower()
    return mt.startswith('image/')


def _receipt_ai_note(review: Optional[dict], expected_amount_uzs: int, *, err: str = "") -> str:
    if err:
        return f"AI receipt error: {err}".strip()
    if not review:
        return ""
    try:
        verdict = str(review.get("verdict") or "").strip().lower()
        conf = float(review.get("confidence") or 0.0)
        amount = int(review.get("amount_uzs") or 0)
        reason = str(review.get("reason") or "").strip()
    except Exception:
        return ""
    expected = int(expected_amount_uzs or 0)
    return f"AI receipt: verdict={verdict} conf={conf:.2f} amount={amount} expected={expected} reason={reason}".strip()


def _receipt_can_autoapprove(review: Optional[dict], expected_amount_uzs: int) -> bool:
    if not review:
        return False
    try:
        verdict = str(review.get("verdict") or "").strip().lower()
        conf = float(review.get("confidence") or 0.0)
        amount = int(review.get("amount_uzs") or 0)
        expected = int(expected_amount_uzs or 0)
    except Exception:
        return False
    return verdict == "approve" and amount == expected and conf >= float(_PREMIUM_RECEIPT_APPROVE_CONF or 0.9)


async def _notify_admins_autoapproved(
    *,
    bot: Bot,
    user: types.User,
    plan_code: str,
    rid: int,
    note: str,
    file_id: str,
    file_type: str,
) -> None:
    if not ADMIN_IDS:
        return
    if not _PREMIUM_RECEIPT_NOTIFY_ADMINS:
        return

    p = _PREMIUM_PLANS.get(plan_code) or {}
    caption = (
        f"Premium AUTO-APPROVED (AI) #{rid}\n"
        f"User: {user.full_name} (@{getattr(user, 'username', '') or '-'})\n"
        f"User ID: {user.id}\n"
        f"Plan: {plan_code} | days={p.get('days')} | price={p.get('price')} UZS\n"
        f"Quota: files={p.get('files')} | topics={p.get('topics')}\n"
    )
    if note:
        trimmed = note if len(note) <= 1200 else (note[:1200] + "...")
        caption += f"AI note: {trimmed}\n"

    for admin_id in ADMIN_IDS:
        try:
            if file_type == 'photo':
                await bot.send_photo(admin_id, file_id, caption=caption)
            elif file_type == 'document':
                await bot.send_document(admin_id, file_id, caption=caption)
            else:
                await bot.send_message(admin_id, caption)
        except Exception:
            try:
                await bot.send_message(admin_id, caption)
            except Exception:
                pass


async def _autoapprove_premium_request(
    *,
    bot: Bot,
    user: types.User,
    ui_lang: str,
    plan_code: str,
    file_id: str,
    file_type: str,
    note: str,
) -> str:
    p = _PREMIUM_PLANS.get(plan_code) or {}
    rid = await create_premium_request(
        user.id,
        plan_code=plan_code,
        screenshot_file_id=file_id,
        screenshot_type=file_type,
        ai_verdict=str(note or '').strip(),
    )
    status = await grant_user_premium(
        int(user.id),
        plan_code=plan_code,
        duration_days=int(p.get('days') or 1),
        files_quota=int(p.get('files') or 0),
        topics_quota=int(p.get('topics') or 0),
    )
    await set_premium_request_status(rid, status='approved', reviewed_by=0)

    until = _fmt_premium_until(str(status.get('premium_until') or ''))

    await _notify_admins_autoapproved(
        bot=bot,
        user=user,
        plan_code=plan_code,
        rid=rid,
        note=note,
        file_id=file_id,
        file_type=file_type,
    )

    return until or '-'


async def _submit_premium_request(
    *,
    bot: Bot,
    user: types.User,
    ui_lang: str,
    plan_code: str,
    file_id: str,
    file_type: str,
    note: str = '',
    notify_user: bool = True,
) -> int:
    rid = await create_premium_request(
        user.id,
        plan_code=plan_code,
        screenshot_file_id=file_id,
        screenshot_type=file_type,
        ai_verdict=str(note or '').strip(),
    )

    if notify_user:
        try:
            await bot.send_message(user.id, t(ui_lang, 'premium_received'))
        except Exception:
            pass

    if not ADMIN_IDS:
        return int(rid)

    p = _PREMIUM_PLANS.get(plan_code) or {}
    caption = (
        f"Premium request #{rid}\n"
        f"User: {user.full_name} (@{getattr(user, 'username', '') or '-'})\n"
        f"User ID: {user.id}\n"
        f"Plan: {plan_code} | days={p.get('days')} | price={p.get('price')} UZS\n"
        f"Quota: files={p.get('files')} | topics={p.get('topics')}\n"
    )
    note = str(note or '').strip()
    if note:
        trimmed = note if len(note) <= 1200 else (note[:1200] + '...')
        caption += f"Receipt note: {trimmed}\n"

    for admin_id in ADMIN_IDS:
        admin_lang = await _get_ui_lang(int(admin_id))
        kb = InlineKeyboardBuilder()
        kb.button(text=t(admin_lang, 'btn_premium_approve'), callback_data=f"prem_ok:{rid}")
        kb.button(text=t(admin_lang, 'btn_premium_reject'), callback_data=f"prem_no:{rid}")
        kb.adjust(2)
        try:
            if file_type == 'photo':
                await bot.send_photo(admin_id, file_id, caption=caption, reply_markup=kb.as_markup())
            elif file_type == 'document':
                await bot.send_document(admin_id, file_id, caption=caption, reply_markup=kb.as_markup())
            else:
                await bot.send_message(admin_id, caption, reply_markup=kb.as_markup())
        except Exception:
            try:
                await bot.send_message(admin_id, caption, reply_markup=kb.as_markup())
            except Exception:
                pass

    return int(rid)


@router.message(PremiumStates.await_screenshot, F.photo)
async def prem_screenshot_photo(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get('prem_ui_lang') or '')) or await _get_ui_lang(message.from_user.id)
    code = str(data.get('prem_plan_code') or '').strip().lower()
    if code not in _PREMIUM_PLANS:
        await state.clear()
        await message.answer(t(ui_lang, 'invalid_button'))
        return

    photo = message.photo[-1] if message.photo else None
    if not photo:
        await message.answer(t(ui_lang, 'premium_need_image'))
        return

    await state.clear()

    p = _PREMIUM_PLANS.get(code) or {}
    price = int(p.get('price') or 0)

    status_msg = await message.answer(t(ui_lang, 'receipt_checking'))

    local_path: Optional[Path] = None
    review: Optional[dict] = None
    note = ''

    try:
        if AI_ENABLED and _PREMIUM_RECEIPT_AI:
            tg_file = await bot.get_file(photo.file_id)
            _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            local_path = _DOWNLOAD_DIR / f"prem_{uuid.uuid4().hex}.jpg"
            await bot.download_file(tg_file.file_path, str(local_path))

            try:
                review = await ai_service.review_payment_receipt_image(str(local_path), expected_amount_uzs=price)
                note = _receipt_ai_note(review, price)
            except Exception as exc:
                review = None
                note = _receipt_ai_note(None, price, err=str(exc))
    finally:
        if local_path is not None:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass

    if _PREMIUM_RECEIPT_AUTOAPPROVE and _receipt_can_autoapprove(review, price):
        until = await _autoapprove_premium_request(
            bot=bot,
            user=message.from_user,
            ui_lang=ui_lang,
            plan_code=code,
            file_id=photo.file_id,
            file_type='photo',
            note=note,
        )
        try:
            await status_msg.edit_text(t(ui_lang, 'premium_approved_user', until=until))
        except Exception:
            pass
        return

    await _submit_premium_request(
        bot=bot,
        user=message.from_user,
        ui_lang=ui_lang,
        plan_code=code,
        file_id=photo.file_id,
        file_type='photo',
        note=note,
        notify_user=False,
    )
    try:
        await status_msg.edit_text(t(ui_lang, 'premium_received'))
    except Exception:
        pass


@router.message(PremiumStates.await_screenshot, F.document)
async def prem_screenshot_doc(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get('prem_ui_lang') or '')) or await _get_ui_lang(message.from_user.id)
    code = str(data.get('prem_plan_code') or '').strip().lower()
    if code not in _PREMIUM_PLANS:
        await state.clear()
        await message.answer(t(ui_lang, 'invalid_button'))
        return
    if not _is_screenshot_document(message):
        await message.answer(t(ui_lang, 'premium_need_image'))
        return
    doc = message.document
    if not doc:
        await message.answer(t(ui_lang, 'premium_need_image'))
        return

    await state.clear()

    p = _PREMIUM_PLANS.get(code) or {}
    price = int(p.get('price') or 0)

    status_msg = await message.answer(t(ui_lang, 'receipt_checking'))

    local_path: Optional[Path] = None
    rendered_path: Optional[Path] = None
    review: Optional[dict] = None
    note = ''

    try:
        if AI_ENABLED and _PREMIUM_RECEIPT_AI:
            name = (doc.file_name or 'file').lower()
            suffix = Path(name).suffix.lower()

            tg_file = await bot.get_file(doc.file_id)
            _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            local_path = _DOWNLOAD_DIR / f"prem_{uuid.uuid4().hex}{suffix or '.bin'}"
            await bot.download_file(tg_file.file_path, str(local_path))

            try:
                if suffix in {'.jpg', '.jpeg', '.png', '.webp'}:
                    review = await ai_service.review_payment_receipt_image(str(local_path), expected_amount_uzs=price)
                elif suffix == '.pdf':
                    txt = ''
                    try:
                        txt = (extract_text_from_file(str(local_path)) or '')
                    except Exception:
                        txt = ''
                    txt = txt.strip()
                    if len(txt) >= 20:
                        review = await ai_service.review_payment_receipt_text(txt[:8000], expected_amount_uzs=price)
                    else:
                        # Try rendering first page and using vision.
                        try:
                            import fitz  # PyMuPDF

                            rendered_path = _DOWNLOAD_DIR / f"prem_{uuid.uuid4().hex}.png"
                            with fitz.open(str(local_path)) as pdf:
                                page = pdf[0]
                                pix = page.get_pixmap(dpi=200)
                                pix.save(str(rendered_path))
                            review = await ai_service.review_payment_receipt_image(str(rendered_path), expected_amount_uzs=price)
                        except Exception:
                            review = None
                else:
                    review = None

                note = _receipt_ai_note(review, price) if review else note
            except Exception as exc:
                review = None
                note = _receipt_ai_note(None, price, err=str(exc))
    finally:
        for pth in (rendered_path, local_path):
            if pth is not None:
                try:
                    pth.unlink(missing_ok=True)
                except Exception:
                    pass

    if _PREMIUM_RECEIPT_AUTOAPPROVE and _receipt_can_autoapprove(review, price):
        until = await _autoapprove_premium_request(
            bot=bot,
            user=message.from_user,
            ui_lang=ui_lang,
            plan_code=code,
            file_id=doc.file_id,
            file_type='document',
            note=note,
        )
        try:
            await status_msg.edit_text(t(ui_lang, 'premium_approved_user', until=until))
        except Exception:
            pass
        return

    await _submit_premium_request(
        bot=bot,
        user=message.from_user,
        ui_lang=ui_lang,
        plan_code=code,
        file_id=doc.file_id,
        file_type='document',
        note=note,
        notify_user=False,
    )
    try:
        await status_msg.edit_text(t(ui_lang, 'premium_received'))
    except Exception:
        pass


@router.message(PremiumStates.await_screenshot, F.text)
async def prem_screenshot_text(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get('prem_ui_lang') or '')) or await _get_ui_lang(message.from_user.id)
    code = str(data.get('prem_plan_code') or '').strip().lower()
    if code not in _PREMIUM_PLANS:
        await state.clear()
        await message.answer(t(ui_lang, 'invalid_button'))
        return

    payload = (message.text or '').strip()
    if not payload:
        await message.answer(t(ui_lang, 'premium_need_image'))
        return

    await state.clear()

    p = _PREMIUM_PLANS.get(code) or {}
    price = int(p.get('price') or 0)

    status_msg = await message.answer(t(ui_lang, 'receipt_checking'))

    review: Optional[dict] = None
    note = ''
    if AI_ENABLED and _PREMIUM_RECEIPT_AI:
        try:
            review = await ai_service.review_payment_receipt_text(payload[:8000], expected_amount_uzs=price)
            note = _receipt_ai_note(review, price)
        except Exception as exc:
            review = None
            note = _receipt_ai_note(None, price, err=str(exc))

    if _PREMIUM_RECEIPT_AUTOAPPROVE and _receipt_can_autoapprove(review, price):
        until = await _autoapprove_premium_request(
            bot=bot,
            user=message.from_user,
            ui_lang=ui_lang,
            plan_code=code,
            file_id=payload,
            file_type='text',
            note=note or payload,
        )
        try:
            await status_msg.edit_text(t(ui_lang, 'premium_approved_user', until=until))
        except Exception:
            pass
        return

    await _submit_premium_request(
        bot=bot,
        user=message.from_user,
        ui_lang=ui_lang,
        plan_code=code,
        file_id=payload,
        file_type='text',
        note=note or payload,
        notify_user=False,
    )
    try:
        await status_msg.edit_text(t(ui_lang, 'premium_received'))
    except Exception:
        pass


@router.message(PremiumStates.await_screenshot)
async def prem_screenshot_other(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get('prem_ui_lang') or '')) or await _get_ui_lang(message.from_user.id if message.from_user else 0)
    await message.answer(t(ui_lang, 'premium_need_image'))


@router.callback_query(F.data.startswith('prem_ok:'))
async def prem_admin_ok(call: types.CallbackQuery, bot: Bot) -> None:
    if call.from_user.id not in set(int(x) for x in (ADMIN_IDS or [])):
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, 'admin_only'), show_alert=True)
        return

    try:
        rid = int(call.data.split(':', 1)[1])
    except Exception:
        await call.answer('bad id', show_alert=True)
        return

    req = await get_premium_request(rid)
    if not req or req.get('status') != 'pending':
        await call.answer('not found', show_alert=True)
        return

    code = str(req.get('plan_code') or '').strip().lower()
    p = _PREMIUM_PLANS.get(code)
    if not p:
        await call.answer('bad plan', show_alert=True)
        return

    status = await grant_user_premium(
        int(req.get('user_id') or 0),
        plan_code=code,
        duration_days=int(p.get('days') or 1),
        files_quota=int(p.get('files') or 0),
        topics_quota=int(p.get('topics') or 0),
    )
    await set_premium_request_status(rid, status='approved', reviewed_by=call.from_user.id)

    until = _fmt_premium_until(str(status.get('premium_until') or ''))
    try:
        user_id = int(req.get('user_id') or 0)
        user_lang = await _get_ui_lang(user_id)
        await bot.send_message(user_id, t(user_lang, 'premium_approved_user', until=until or '-'))
    except Exception:
        pass

    await call.answer('OK')


@router.callback_query(F.data.startswith('prem_no:'))
async def prem_admin_no(call: types.CallbackQuery, bot: Bot) -> None:
    if call.from_user.id not in set(int(x) for x in (ADMIN_IDS or [])):
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, 'admin_only'), show_alert=True)
        return

    try:
        rid = int(call.data.split(':', 1)[1])
    except Exception:
        await call.answer('bad id', show_alert=True)
        return

    req = await get_premium_request(rid)
    if not req or req.get('status') != 'pending':
        await call.answer('not found', show_alert=True)
        return

    await set_premium_request_status(rid, status='rejected', reviewed_by=call.from_user.id)
    try:
        user_id = int(req.get('user_id') or 0)
        user_lang = await _get_ui_lang(user_id)
        await bot.send_message(user_id, t(user_lang, 'premium_rejected_user'))
    except Exception:
        pass

    await call.answer('Rejected')
@router.callback_query(F.data == "menu_admin_users")
async def menu_admin_users(call: types.CallbackQuery, bot: Bot) -> None:
    ui_lang = await _get_ui_lang(call.from_user.id if call.from_user else 0)
    if call.from_user.id not in set(int(x) for x in (ADMIN_IDS or [])):
        await call.answer(t(ui_lang, 'admin_only'), show_alert=True)
        return
    try:
        stats = await get_user_counts_summary()
    except Exception as exc:
        await call.message.answer(t(ui_lang, 'err_unexpected', err=str(exc)))
        return
    await call.message.answer(
        t(
            ui_lang,
            'admin_users_stats',
            total=int(stats.get('total_users') or 0),
            joined=int(stats.get('joined_last_24h') or 0),
            active=int(stats.get('active_users_last_24h') or 0),
            quizzes=int(stats.get('total_quizzes') or 0),
            attempts=int(stats.get('attempts_last_24h') or 0),
        )
    )
    await call.answer()

@router.callback_query(F.data == "menu_myquizzes")
async def menu_myquizzes(call: types.CallbackQuery, bot: Bot) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    quizzes = await list_user_quizzes(call.from_user.id, limit=20)
    if not quizzes:
        if call.message:
            await call.message.answer(t(ui_lang, "no_quizzes_yet"))
        return
    if not call.message:
        return

    username = await _get_bot_username(bot)
    shown = quizzes[:10]
    for q in shown:
        qid = int(q["id"])
        title = str(q["title"] or "")
        count = int(q["question_count"] or 0)
        text = t(ui_lang, "quiz_brief", title=title, count=count, id=qid)
        await call.message.answer(
            text,
            reply_markup=_kb_quiz_share(
                username,
                qid,
                title=title,
                question_count=count,
                chat_type=call.message.chat.type,
                ui_lang=ui_lang,
                show_stats=True,
                show_edit=True,
            ),
        )

    if len(quizzes) > len(shown):
        await call.message.answer(t(ui_lang, "more_quizzes", n=(len(quizzes) - len(shown))))


@router.message(Command("mytests"))
async def cmd_mytests(message: types.Message, bot: Bot) -> None:
    if not message.from_user:
        return
    ui_lang = await _get_ui_lang(message.from_user.id)
    quizzes = await list_user_quizzes(message.from_user.id, limit=20)
    if not quizzes:
        await message.answer(t(ui_lang, "no_quizzes_yet"))
        return
    username = await _get_bot_username(bot)
    shown = quizzes[:10]
    for q in shown:
        qid = int(q["id"])
        title = str(q["title"] or "")
        count = int(q["question_count"] or 0)
        text = t(ui_lang, "quiz_brief", title=title, count=count, id=qid)
        await message.answer(
            text,
            reply_markup=_kb_quiz_share(
                username,
                qid,
                title=title,
                question_count=count,
                chat_type=message.chat.type,
                ui_lang=ui_lang,
                show_stats=True,
                show_edit=True,
            ),
        )

    if len(quizzes) > len(shown):
        await message.answer(t(ui_lang, "more_quizzes", n=(len(quizzes) - len(shown))))


@router.callback_query(F.data == "menu_cancel")
async def menu_cancel(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await call.answer()
    chat_id = call.message.chat.id if call.message else 0
    cancelled = await _cancel_user_runs(bot, chat_id=chat_id, user_id=call.from_user.id)
    await state.clear()
    await clear_manual_quiz_draft(user_id=call.from_user.id)
    if call.message:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.message.answer(t(ui_lang, "stopped_n", n=cancelled))


@router.message(Command("newquiz"))
async def cmd_newquiz(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(message, bot, message.from_user.id if message.from_user else 0, pending_action="menu_newquiz"):
        return
    ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    await _open_manual_quiz_flow(message, state, user_id=message.from_user.id if message.from_user else 0, ui_lang=ui_lang)


class ManualQuizStates(StatesGroup):
    title = State()
    open_period = State()
    choose_shuffle = State()
    choose_shuffle_strategy = State()
    question = State()
    question_image = State()
    options = State()
    choose_correct = State()


def _kb_manual_draft_choice(*, ui_lang: str) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_manual_continue"), callback_data="m_draft:resume")
    kb.button(text=t(ui_lang, "btn_manual_restart"), callback_data="m_draft:restart")
    kb.adjust(2)
    return kb.as_markup()


def _manual_prompt_for_state(*, ui_lang: str, state_str: str, data: Dict[str, Any]) -> tuple[str, Optional[types.InlineKeyboardMarkup]]:
    ui_lang = norm_ui_lang(ui_lang)
    st = str(state_str or "").strip()

    if st == ManualQuizStates.title.state or st.endswith(":title"):
        return t(ui_lang, "manual_title_prompt"), None
    if st == ManualQuizStates.open_period.state or st.endswith(":open_period"):
        return t(ui_lang, "choose_time"), _kb_manual_time_presets(ui_lang=ui_lang)
    if st == ManualQuizStates.choose_shuffle.state or st.endswith(":choose_shuffle"):
        return t(ui_lang, "shuffle_prompt_manual"), _kb_manual_shuffle(ui_lang=ui_lang)
    if st == ManualQuizStates.choose_shuffle_strategy.state or st.endswith(":choose_shuffle_strategy"):
        mode = str(data.get("m_shuffle_mode") or "none").strip().lower()
        return t(ui_lang, "shuffle_strategy_prompt"), _kb_manual_shuffle_strategy(mode=mode, ui_lang=ui_lang)
    if st == ManualQuizStates.question.state or st.endswith(":question"):
        questions = data.get("questions") or []
        if isinstance(questions, list) and len(questions) > 0:
            return t(ui_lang, "manual_next_question"), None
        return t(ui_lang, "manual_first_question"), None
    if st == ManualQuizStates.question_image.state or st.endswith(":question_image"):
        return t(ui_lang, "manual_has_image"), None
    if st == ManualQuizStates.options.state or st.endswith(":options"):
        return t(ui_lang, "manual_send_4_options"), None
    if st == ManualQuizStates.choose_correct.state or st.endswith(":choose_correct"):
        q = str(data.get("current_question") or "").strip()
        opts = data.get("current_options") or []
        if not q or not (isinstance(opts, list) and len(opts) == 4):
            questions = data.get("questions") or []
            n = len(questions) if isinstance(questions, list) else 0
            kb2 = InlineKeyboardBuilder()
            kb2.button(text=t(ui_lang, "btn_manual_add_more"), callback_data="m_add_more")
            kb2.button(text=t(ui_lang, "btn_manual_finish"), callback_data="m_finish")
            kb2.adjust(2)
            return t(ui_lang, "manual_saved_total", n=n), kb2.as_markup()

        kb = InlineKeyboardBuilder()
        for i in range(4):
            kb.button(text=str(i + 1), callback_data=f"m_correct:{i}")
        kb.adjust(2)

        lines: List[str] = []
        if q:
            lines.append(q)
        if isinstance(opts, list) and opts:
            for i, o in enumerate(opts[:4], start=1):
                lines.append(f"{i}) {str(o).strip()}")
        lines.append(t(ui_lang, "manual_choose_correct"))
        return "\n".join([x for x in lines if x]), kb.as_markup()

    return t(ui_lang, "manual_title_prompt"), None


def _manual_draft_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    allow = {
        "m_ui_lang",
        "title",
        "m_open_period",
        "m_shuffle_mode",
        "m_shuffle_strategy",
        "questions",
        "current_question",
        "current_options",
        "current_image_file_id",
    }
    out: Dict[str, Any] = {}
    for k in allow:
        if k in data:
            out[k] = data.get(k)
    return out


async def _persist_manual_draft(state: FSMContext, *, user_id: int, chat_id: int) -> None:
    try:
        st = await state.get_state()
        if not st or not str(st).startswith("ManualQuizStates:"):
            return
        data = await state.get_data()
        await upsert_manual_quiz_draft(
            user_id=int(user_id or 0),
            chat_id=int(chat_id or 0),
            state=str(st),
            data=_manual_draft_payload(data if isinstance(data, dict) else {}),
        )
    except Exception:
        # Best-effort only.
        return


async def _restore_manual_draft_if_needed(state: FSMContext, *, user_id: int, ui_lang: str) -> Dict[str, Any]:
    data = await state.get_data()
    st = await state.get_state()
    data = data if isinstance(data, dict) else {}

    draft = await get_manual_quiz_draft(user_id=user_id)
    if not draft:
        return data

    draft_data = draft.get("data") or {}
    if not isinstance(draft_data, dict):
        draft_data = {}
    restored_lang = norm_ui_lang(str(draft_data.get("m_ui_lang") or data.get("m_ui_lang") or ui_lang))
    restored_state = str(draft.get("state") or st or "").strip()
    if not restored_state.startswith("ManualQuizStates:"):
        restored_state = ManualQuizStates.title.state

    if data and st and str(st).startswith("ManualQuizStates:"):
        merged = dict(data)
        for key in (
            "m_ui_lang",
            "title",
            "m_open_period",
            "m_shuffle_mode",
            "m_shuffle_strategy",
            "questions",
            "current_question",
            "current_options",
            "current_image_file_id",
        ):
            value = merged.get(key)
            if value in (None, "", [], {}):
                draft_value = draft_data.get(key)
                if draft_value not in (None, "", [], {}):
                    merged[key] = draft_value
        merged["m_ui_lang"] = restored_lang
        if merged != data:
            await state.update_data(**merged)
        return merged

    await state.clear()
    await state.update_data(**draft_data, m_ui_lang=restored_lang)
    await state.set_state(restored_state)
    return await state.get_data()


def _manual_finish_keyboard(*, ui_lang: str, show_edit_last: bool = True) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    if show_edit_last:
        kb.button(text=t(ui_lang, "btn_edit_quiz"), callback_data="m_edit_last")
    kb.button(text=t(ui_lang, "btn_manual_add_more"), callback_data="m_add_more")
    kb.button(text=t(ui_lang, "btn_manual_finish"), callback_data="m_finish")
    if show_edit_last:
        kb.adjust(1, 2)
    else:
        kb.adjust(2)
    return kb.as_markup()


@router.callback_query(F.data == "m_draft:restart")
async def manual_draft_restart(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    await clear_manual_quiz_draft(user_id=call.from_user.id)
    await state.clear()
    await state.update_data(m_ui_lang=ui_lang)
    await state.set_state(ManualQuizStates.title)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        await call.message.answer(t(ui_lang, "manual_title_prompt"))


@router.callback_query(F.data == "m_draft:resume")
async def manual_draft_resume(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    data = await _restore_manual_draft_if_needed(state, user_id=call.from_user.id, ui_lang=ui_lang)
    st = await state.get_state()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ui_lang))
    st = str(st or ManualQuizStates.title.state)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        prompt, markup = _manual_prompt_for_state(ui_lang=ui_lang, state_str=st, data=await state.get_data())
        await call.message.answer(prompt, reply_markup=markup)

@router.message(ManualQuizStates.title)
async def manual_title(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    title = (message.text or "").strip()
    if not title:
        await message.answer(t(ui_lang, "manual_title_required"))
        return

    await state.update_data(title=title, questions=[], m_open_period=None, m_shuffle_mode="none", m_shuffle_strategy="saved")
    await state.set_state(ManualQuizStates.open_period)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "choose_time"), reply_markup=_kb_manual_time_presets(ui_lang=ui_lang))


@router.callback_query(F.data.startswith("m_time:"))
async def manual_open_period_pick(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)

    try:
        sec = int(str(call.data or "").split(":", 1)[1])
    except Exception:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    if sec not in _TIME_PRESET_VALUES:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await state.update_data(m_open_period=int(sec))
    await state.set_state(ManualQuizStates.choose_shuffle)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id if call.from_user else 0, chat_id=call.message.chat.id)
        await call.message.answer(t(ui_lang, "shuffle_prompt_manual"), reply_markup=_kb_manual_shuffle(ui_lang=ui_lang))


@router.message(ManualQuizStates.open_period)
async def manual_open_period(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    sec = _first_int(message.text or "")
    if sec is None or sec < 5 or sec > 600:
        await message.answer(t(ui_lang, "time_invalid"), reply_markup=_kb_manual_time_presets(ui_lang=ui_lang))
        return
    await state.update_data(m_open_period=int(sec))
    await state.set_state(ManualQuizStates.choose_shuffle)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "shuffle_prompt_manual"), reply_markup=_kb_manual_shuffle(ui_lang=ui_lang))


@router.callback_query(F.data.startswith("m_shuffle:"))
async def manual_choose_shuffle(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)

    choice = str(call.data.split(":", 1)[1] if ":" in call.data else "").strip().lower()
    if choice not in {"questions", "options", "both", "none"}:
        choice = "none"

    if choice == "none":
        await state.update_data(m_shuffle_mode="none", m_shuffle_strategy="saved")
        await state.set_state(ManualQuizStates.question)
        if call.message:
            await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
            await call.message.answer(t(ui_lang, "manual_first_question"))
        return

    await state.update_data(m_shuffle_mode=choice)
    await state.set_state(ManualQuizStates.choose_shuffle_strategy)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        await call.message.answer(
            t(ui_lang, "shuffle_strategy_prompt"),
            reply_markup=_kb_manual_shuffle_strategy(mode=choice, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("m_shuffle_strategy:"))
async def manual_choose_shuffle_strategy(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)

    choice = str(call.data.split(":", 1)[1] if ":" in call.data else "").strip().lower()
    strategy = _normalize_shuffle_strategy(choice)
    await state.update_data(m_shuffle_strategy=strategy)
    await state.set_state(ManualQuizStates.question)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        await call.message.answer(t(ui_lang, "manual_first_question"))


def _is_image_document(message: types.Message) -> bool:
    doc = message.document
    if not doc:
        return False
    name = (doc.file_name or "").lower()
    return any(name.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp"))


@router.message(ManualQuizStates.question, F.photo)
async def manual_question_photo(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    photo = message.photo[-1] if message.photo else None
    if not photo:
        await message.answer(t(ui_lang, "manual_image_not_found"))
        return
    caption = (message.caption or "").strip()
    q_text = caption or t(ui_lang, "image_question")
    await state.update_data(current_question=q_text, current_image_file_id=photo.file_id)
    await state.set_state(ManualQuizStates.options)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_send_4_options"))


@router.message(ManualQuizStates.question, F.document)
async def manual_question_doc_image(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not _is_image_document(message):
        await message.answer(t(ui_lang, "manual_send_image_or_text"))
        return
    doc = message.document
    caption = (message.caption or "").strip()
    q_text = caption or t(ui_lang, "image_question")
    await state.update_data(current_question=q_text, current_image_file_id=doc.file_id)
    await state.set_state(ManualQuizStates.options)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_send_4_options"))


@router.message(ManualQuizStates.question)
async def manual_question_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    text = (message.text or "").strip()
    if not text:
        await message.answer(t(ui_lang, "manual_send_question_or_image"))
        return

    await state.update_data(current_question=text, current_image_file_id="")
    await state.set_state(ManualQuizStates.question_image)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_has_image"))


@router.message(ManualQuizStates.question_image, Command("skip"))
async def manual_question_image_skip(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    await state.update_data(current_image_file_id="")
    await state.set_state(ManualQuizStates.options)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_send_4_options"))


@router.message(ManualQuizStates.question_image, F.photo)
async def manual_question_image_photo(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    photo = message.photo[-1] if message.photo else None
    if not photo:
        await message.answer(t(ui_lang, "manual_image_or_skip"))
        return
    await state.update_data(current_image_file_id=photo.file_id)
    await state.set_state(ManualQuizStates.options)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_send_4_options"))


@router.message(ManualQuizStates.question_image, F.document)
async def manual_question_image_doc(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not _is_image_document(message):
        await message.answer(t(ui_lang, "manual_image_or_skip"))
        return
    doc = message.document
    await state.update_data(current_image_file_id=doc.file_id)
    await state.set_state(ManualQuizStates.options)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)
    await message.answer(t(ui_lang, "manual_send_4_options"))


@router.message(ManualQuizStates.question_image)
async def manual_question_image_text_fallback(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    # Allow "skip" without slash.
    if (message.text or "").strip().lower() in {"skip", "yo'q", "yoq", "kerak emas"}:
        await manual_question_image_skip(message, state)
        return
    await message.answer(t(ui_lang, "manual_image_or_skip_short"))


@router.message(ManualQuizStates.options)
async def manual_options(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ""))
    if not data.get("m_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    raw = (message.text or "").strip()
    options = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(options) != 4:
        await message.answer(t(ui_lang, "manual_need_4_lines"))
        return

    await state.update_data(current_options=options)
    await state.set_state(ManualQuizStates.choose_correct)
    await _persist_manual_draft(state, user_id=message.from_user.id if message.from_user else 0, chat_id=message.chat.id)

    kb = InlineKeyboardBuilder()
    for i in range(4):
        kb.button(text=str(i + 1), callback_data=f"m_correct:{i}")
    kb.adjust(2)

    await message.answer(t(ui_lang, "manual_choose_correct"), reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("m_correct:"))
async def manual_correct(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    if call.message:
        with suppress(Exception):
            await call.message.edit_reply_markup(reply_markup=None)
    user_id = call.from_user.id if call.from_user else 0
    async with _manual_correct_lock(user_id):
        ui_lang = await _get_ui_lang(user_id)
        data = await _restore_manual_draft_if_needed(state, user_id=user_id, ui_lang=ui_lang)
        ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ui_lang))
        try:
            correct_index = int(call.data.split(":", 1)[1])
        except Exception:
            if call.message:
                await call.message.answer(t(ui_lang, "manual_callback_error"))
            return

        question_text = str(data.get("current_question") or "").strip()
        options = data.get("current_options") or []
        image_file_id = str(data.get("current_image_file_id") or "").strip()
        questions: List[Dict[str, Any]] = data.get("questions") or []

        if not question_text or not isinstance(options, list) or len(options) != 4:
            if call.message:
                if questions:
                    current_state = await state.get_state()
                    if not current_state or not str(current_state).startswith("ManualQuizStates:"):
                        await state.set_state(ManualQuizStates.choose_correct)
                    await state.update_data(
                        questions=questions,
                        current_question="",
                        current_options=[],
                        current_image_file_id="",
                    )
                    await _persist_manual_draft(state, user_id=user_id, chat_id=call.message.chat.id)
                    await call.message.answer(
                        t(ui_lang, "manual_saved_total", n=len(questions)),
                        reply_markup=_manual_finish_keyboard(ui_lang=ui_lang, show_edit_last=bool(questions)),
                    )
                else:
                    await call.message.answer(t(ui_lang, "manual_question_missing"))
            return

        questions.append(
            {
                "question": question_text,
                "options": options,
                "correct_index": max(0, min(3, correct_index)),
                "explanation": "",
                "image_file_id": image_file_id,
            }
        )

        await state.update_data(
            questions=questions,
            current_question="",
            current_options=[],
            current_image_file_id="",
        )
        await state.set_state(ManualQuizStates.choose_correct)

        if call.message:
            await _persist_manual_draft(state, user_id=user_id, chat_id=call.message.chat.id)
            await call.message.answer(
                t(ui_lang, "manual_saved_total", n=len(questions)),
                reply_markup=_manual_finish_keyboard(ui_lang=ui_lang, show_edit_last=bool(questions)),
            )


@router.callback_query(F.data == "m_edit_last")
async def manual_edit_last(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    data = await _restore_manual_draft_if_needed(state, user_id=call.from_user.id, ui_lang=ui_lang)
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ui_lang))
    questions = data.get("questions") or []
    if not isinstance(questions, list) or not questions:
        if call.message:
            await call.message.answer(t(ui_lang, "manual_empty"))
        return

    last = questions[-1]
    remaining = questions[:-1]
    current_question = str(last.get("question") or "").strip()
    current_options = last.get("options") or []
    current_image_file_id = str(last.get("image_file_id") or "").strip()
    if not current_question or not isinstance(current_options, list) or len(current_options) != 4:
        if call.message:
            await call.message.answer(t(ui_lang, "manual_question_missing"))
        return

    await state.update_data(
        questions=remaining,
        current_question=current_question,
        current_options=current_options,
        current_image_file_id=current_image_file_id,
    )
    await state.set_state(ManualQuizStates.choose_correct)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        prompt, markup = _manual_prompt_for_state(
            ui_lang=ui_lang,
            state_str=ManualQuizStates.choose_correct.state,
            data=await state.get_data(),
        )
        await call.message.answer(prompt, reply_markup=markup)


@router.callback_query(F.data == "m_add_more")
async def manual_add_more(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ui_lang = await _get_ui_lang(call.from_user.id)
    data = await _restore_manual_draft_if_needed(state, user_id=call.from_user.id, ui_lang=ui_lang)
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ui_lang))
    await state.set_state(ManualQuizStates.question)
    if call.message:
        await _persist_manual_draft(state, user_id=call.from_user.id, chat_id=call.message.chat.id)
        await call.message.answer(t(ui_lang, "manual_next_question"))


@router.callback_query(F.data == "m_finish")
async def manual_finish(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await call.answer()

    ui_lang = await _get_ui_lang(call.from_user.id)
    data = await _restore_manual_draft_if_needed(state, user_id=call.from_user.id, ui_lang=ui_lang)
    ui_lang = norm_ui_lang(str(data.get("m_ui_lang") or ui_lang))
    title = str(data.get("title") or "").strip()
    questions: List[Dict[str, Any]] = data.get("questions") or []

    if not title or not questions:
        draft = await get_manual_quiz_draft(user_id=call.from_user.id)
        draft_data = (draft or {}).get("data") or {}
        if isinstance(draft_data, dict):
            if not title:
                title = str(draft_data.get("title") or "").strip()
            if not questions:
                restored_questions = draft_data.get("questions") or []
                if isinstance(restored_questions, list):
                    questions = restored_questions

    if not title or not questions:
        if call.message:
            await call.message.answer(t(ui_lang, "manual_empty"))
        return

    user = call.from_user
    await get_or_create_user(user_id=user.id, full_name=user.full_name, username=getattr(user, "username", None))

    open_period = int(data.get("m_open_period") or 30)
    open_period = max(5, min(600, open_period))
    m_shuffle_mode = str(data.get("m_shuffle_mode") or "none").strip().lower()
    m_shuffle_strategy = _normalize_shuffle_strategy(data.get("m_shuffle_strategy") or "saved")
    m_shuffle_questions, m_shuffle_options = _shuffle_mode_flags(m_shuffle_mode)
    if m_shuffle_strategy != "runtime" and (m_shuffle_questions or m_shuffle_options):
        questions = _apply_quiz_shuffle(questions, shuffle_questions=m_shuffle_questions, shuffle_options=m_shuffle_options)
    quiz_id = await create_quiz(
        title=title,
        creator_id=user.id,
        is_ai_generated=False,
        open_period=open_period,
        shuffle_mode=m_shuffle_mode,
        shuffle_strategy=m_shuffle_strategy,
    )
    inserted = await create_questions_bulk(quiz_id, questions)

    await state.clear()
    await clear_manual_quiz_draft(user_id=call.from_user.id)
    await call.message.answer(t(ui_lang, "manual_created", id=quiz_id, n=inserted))

    username = await _get_bot_username(bot)
    text = t(ui_lang, "quiz_brief", title=title, count=inserted, id=quiz_id)
    await call.message.answer(
        text,
        reply_markup=_kb_quiz_share(
            username,
            quiz_id,
            title=title,
            question_count=inserted,
            chat_type=call.message.chat.type,
            ui_lang=ui_lang,
            show_stats=True,
            show_edit=True,
        ),
    )


class AIQuizStates(StatesGroup):
    choose_topic = State()
    choose_difficulty = State()
    choose_pages = State()
    choose_count = State()
    choose_time = State()
    choose_translate = State()
    choose_lang = State()
    choose_shuffle = State()
    choose_shuffle_strategy = State()



@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext, bot: Bot) -> None:
    if not await _ensure_subscribed(message, bot, message.from_user.id if message.from_user else 0):
        return
    user_id = message.from_user.id if message.from_user else 0
    cancelled = await _cancel_user_runs(bot, chat_id=message.chat.id, user_id=user_id)
    await state.clear()
    await clear_manual_quiz_draft(user_id=user_id)
    ui_lang = await _get_ui_lang(user_id)
    await message.answer(t(ui_lang, "stopped_n", n=cancelled))


def _kb_counts(
    session_id: str,
    max_n: int = 50,
    *,
    ui_lang: str = "uz",
    show_pages: bool = False,
) -> types.InlineKeyboardMarkup:
    max_n = int(max_n or 50)
    max_n = max(1, min(50, max_n))
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    presets = [5, 10, 15, 20]
    nums = [n for n in presets if n <= max_n]
    if max_n < 5:
        nums = list(range(1, max_n + 1))
    elif max_n not in nums and max_n <= 20:
        nums.append(max_n)
    for n in sorted(set(nums)):
        kb.button(text=f"{n} ta", callback_data=f"ai_count:{session_id}:{n}")
    if show_pages:
        kb.button(text=t(ui_lang, "btn_pages_optional"), callback_data=f"ai_pages:{session_id}:count")
    kb.button(text=t(ui_lang, "btn_topic_optional"), callback_data=f"ai_topic:{session_id}:count")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(2)
    return kb.as_markup()


def _kb_translate(
    session_id: str,
    default_lang: str = "source",
    *,
    ui_lang: str = "uz",
    show_pages: bool = False,
) -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_no_translate"), callback_data=f"ai_translate:{session_id}:source")
    kb.button(text=t(ui_lang, "btn_translate_choose"), callback_data=f"ai_translate:{session_id}:choose")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(2)
    return kb.as_markup()


def _kb_langs(session_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for code in ("uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"):
        kb.button(text=lang_name(code), callback_data=f"ai_lang:{session_id}:{code}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(3, 3, 3, 1)
    return kb.as_markup()


def _kb_page_presets(session_id: str, total_pages: int, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    total_pages = int(total_pages or 0)
    presets = [25, 50, 100]

    ends: List[int] = []
    for n in presets:
        end = int(n)
        if total_pages > 0:
            end = min(total_pages, end)
        end = max(1, end)
        if end not in ends:
            ends.append(end)

    kb = InlineKeyboardBuilder()
    for end in ends:
        kb.button(text=f"1-{end}", callback_data=f"ai_pageset:{session_id}:1:{end}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(3, 1)
    return kb.as_markup()

def _kb_difficulty(session_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_diff_easy"), callback_data=f"ai_diff:{session_id}:easy")
    kb.button(text=t(ui_lang, "btn_diff_medium"), callback_data=f"ai_diff:{session_id}:medium")
    kb.button(text=t(ui_lang, "btn_diff_hard"), callback_data=f"ai_diff:{session_id}:hard")
    kb.button(text=t(ui_lang, "btn_diff_mixed"), callback_data=f"ai_diff:{session_id}:mixed")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(2)
    return kb.as_markup()


_TIME_PRESET_VALUES = (20, 30, 40, 50)


def _kb_manual_time_presets(*, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for sec in _TIME_PRESET_VALUES:
        kb.button(text=f"{sec} s", callback_data=f"m_time:{sec}")
    kb.adjust(2)
    return kb.as_markup()


def _kb_ai_time_presets(session_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for sec in _TIME_PRESET_VALUES:
        kb.button(text=f"{sec} s", callback_data=f"ai_time:{session_id}:{sec}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def _kb_quiz_edit_time_presets(quiz_id: int, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    for sec in _TIME_PRESET_VALUES:
        kb.button(text=f"{sec} s", callback_data=f"quiz_edit_time_set:{int(quiz_id)}:{sec}")
    kb.button(text=t(ui_lang, "btn_back"), callback_data=f"quiz_edit:{int(quiz_id)}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"quiz_edit_cancel:{int(quiz_id)}")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


def _kb_manual_shuffle(*, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_shuffle_questions"), callback_data="m_shuffle:questions")
    kb.button(text=t(ui_lang, "btn_shuffle_answers"), callback_data="m_shuffle:options")
    kb.button(text=t(ui_lang, "btn_shuffle_both"), callback_data="m_shuffle:both")
    kb.button(text=t(ui_lang, "btn_shuffle_keep"), callback_data="m_shuffle:none")
    kb.adjust(2, 2)
    return kb.as_markup()


def _kb_ai_shuffle(session_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_shuffle_questions"), callback_data=f"ai_shuffle:{session_id}:questions")
    kb.button(text=t(ui_lang, "btn_shuffle_answers"), callback_data=f"ai_shuffle:{session_id}:options")
    kb.button(text=t(ui_lang, "btn_shuffle_both"), callback_data=f"ai_shuffle:{session_id}:both")
    kb.button(text=t(ui_lang, "btn_shuffle_keep"), callback_data=f"ai_shuffle:{session_id}:none")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def _kb_manual_shuffle_strategy(*, mode: str, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_shuffle_saved_once"), callback_data="m_shuffle_strategy:saved")
    kb.button(text=t(ui_lang, "btn_shuffle_every_run"), callback_data="m_shuffle_strategy:runtime")
    kb.adjust(1)
    return kb.as_markup()


def _kb_ai_shuffle_strategy(session_id: str, *, mode: str, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_shuffle_saved_once"), callback_data=f"ai_shuffle_strategy:{session_id}:saved")
    kb.button(text=t(ui_lang, "btn_shuffle_every_run"), callback_data=f"ai_shuffle_strategy:{session_id}:runtime")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(1)
    return kb.as_markup()


def _kb_topic_no_source(session_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_topic_continue_anyway"), callback_data=f"ai_topic_anyway:{session_id}")
    kb.button(text=t(ui_lang, "btn_cancel"), callback_data=f"ai_cancel:{session_id}")
    kb.adjust(1)
    return kb.as_markup()




def _kb_run_controls(run_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_next"), callback_data=f"run_next:{run_id}")
    kb.button(text=t(ui_lang, "btn_stop"), callback_data=f"run_cancel:{run_id}")
    kb.adjust(2)
    return kb.as_markup()


def _format_lobby(run: QuizRun) -> str:
    total = len(run.questions)
    pid_count = len(run.participants or {})
    ui_lang = norm_ui_lang(getattr(run, "ui_lang", "uz"))
    qid = (t(ui_lang, "quiz_id_line", id=run.quiz_id) + "\n") if run.quiz_id else ""
    return qid + t(ui_lang, "lobby_ready", total=total, sec=int(run.open_period), n=pid_count)


def _kb_lobby(run_id: str, *, ui_lang: str = "uz") -> types.InlineKeyboardMarkup:
    ui_lang = norm_ui_lang(ui_lang)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(ui_lang, "btn_join"), callback_data=f"lobby_join:{run_id}")
    kb.button(text=t(ui_lang, "btn_start"), callback_data=f"lobby_start:{run_id}")
    kb.button(text=t(ui_lang, "btn_stop"), callback_data=f"run_cancel:{run_id}")
    kb.adjust(2)
    return kb.as_markup()


@router.callback_query(F.data.startswith("lobby_join:"))
async def lobby_join(call: types.CallbackQuery) -> None:
    run_id = call.data.split(":", 1)[1]
    run = _ACTIVE_RUNS.get(run_id)
    if not run:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "lobby_not_found"), show_alert=True)
        return
    if run.cancelled:
        await call.answer(t(run.ui_lang, "quiz_cancelled"), show_alert=True)
        return
    if run.started:
        await call.answer(t(run.ui_lang, "cannot_join_started"), show_alert=True)
        return

    uid = call.from_user.id
    if uid in run.participants:
        await call.answer(t(run.ui_lang, "lobby_already_joined"))
        return

    run.participants[uid] = call.from_user.full_name or str(uid)
    run.scores.setdefault(uid, UserScore(name=run.participants[uid], username=(getattr(call.from_user, "username", "") or "")))
    run.scores[uid].username = (getattr(call.from_user, "username", "") or run.scores[uid].username)
    await call.answer(t(run.ui_lang, "joined_ok"))
    if call.message:
        try:
            await call.message.edit_text(_format_lobby(run), reply_markup=_kb_lobby(run_id, ui_lang=run.ui_lang))
        except Exception:
            pass


@router.callback_query(F.data.startswith("lobby_start:"))
async def lobby_start(call: types.CallbackQuery, bot: Bot) -> None:
    run_id = call.data.split(":", 1)[1]
    run = _ACTIVE_RUNS.get(run_id)
    if not run:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "lobby_not_found"), show_alert=True)
        return
    if call.from_user.id != run.created_by:
        await call.answer(t(run.ui_lang, "lobby_creator_only"), show_alert=True)
        return
    if run.cancelled:
        await call.answer(t(run.ui_lang, "quiz_cancelled"), show_alert=True)
        return
    if run.started:
        await call.answer(t(run.ui_lang, "already_started"))
        return

    # Make sure creator is always in participants.
    if run.created_by not in run.participants:
        run.participants[run.created_by] = call.from_user.full_name or str(run.created_by)
    for uid, name in run.participants.items():
        run.scores.setdefault(uid, UserScore(name=name))

    run.started = True
    await call.answer(t(run.ui_lang, "started_ok"))

    total_sec = max(0, len(run.questions) * max(0, run.open_period))
    est = f"{total_sec//60}m {total_sec%60}s" if total_sec >= 60 else f"{total_sec}s"
    if call.message:
        try:
            await call.message.edit_text(
                t(
                    run.ui_lang,
                    "group_started_status",
                    count=len(run.questions),
                    sec=int(run.open_period),
                    est=est,
                    n=len(run.participants),
                ),
                reply_markup=_kb_run_controls(run_id, ui_lang=run.ui_lang),
            )
        except Exception:
            pass

    run.task = asyncio.create_task(_run_quiz(bot, run))


@router.callback_query(F.data.startswith("run_next:"))
async def run_next(call: types.CallbackQuery, bot: Bot) -> None:
    run_id = call.data.split(":", 1)[1]
    run = _ACTIVE_RUNS.get(run_id)
    if not run:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if call.from_user.id != run.created_by:
        await call.answer(t(run.ui_lang, "lobby_creator_only"), show_alert=True)
        return
    if not run.started:
        await call.answer(t(run.ui_lang, "quiz_not_started"), show_alert=True)
        return

    if run.current_poll_message_id:
        try:
            await bot.stop_poll(run.chat_id, run.current_poll_message_id)
        except Exception:
            pass

    run.advance_event.set()
    await call.answer(t(run.ui_lang, "next_question"))


@router.callback_query(F.data.startswith("run_cancel:"))
async def run_cancel(call: types.CallbackQuery, bot: Bot) -> None:
    run_id = call.data.split(":", 1)[1]
    run = _ACTIVE_RUNS.get(run_id)
    if not run:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "quiz_not_found"), show_alert=True)
        return
    if call.from_user.id != run.created_by:
        await call.answer(t(run.ui_lang, "lobby_creator_only"), show_alert=True)
        return

    await call.answer(t(run.ui_lang, "stopped"))
    run.cancelled = True
    if run.current_poll_message_id:
        try:
            await bot.stop_poll(run.chat_id, run.current_poll_message_id)
        except Exception:
            pass
    if run.task and not run.task.done():
        run.task.cancel()
    _ACTIVE_RUNS.pop(run_id, None)

    if call.message:
        try:
            await call.message.edit_text(t(run.ui_lang, "stopped"))
        except Exception:
            pass


@router.callback_query(F.data.startswith("run_resume:"))
async def run_resume(call: types.CallbackQuery, bot: Bot) -> None:
    token = call.data.split(":", 1)[1]
    ui_lang = await _get_ui_lang(call.from_user.id)
    _cleanup_paused_runs()

    pr = _PAUSED_RUNS.get(token)
    if pr is None:
        await call.answer(t(ui_lang, "resume_not_found"), show_alert=True)
        return

    if int(call.from_user.id) != int(pr.user_id):
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    if not call.message or int(call.message.chat.id) != int(pr.chat_id):
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    # Cancel any existing runs for this user in this chat.
    await _cancel_user_runs(bot, chat_id=int(pr.chat_id), user_id=int(pr.user_id))

    run_id = uuid.uuid4().hex
    ct = (str(pr.chat_type or "private")).strip().lower()
    questions = list(pr.questions or [])

    run = QuizRun(
        run_id=run_id,
        chat_id=int(pr.chat_id),
        chat_type=ct or "private",
        created_by=int(pr.user_id),
        title=str(pr.title or ""),
        questions=questions,
        open_period=max(5, min(600, int(pr.open_period or 30))),
        output_language=str(pr.output_language or "source"),
        ui_lang=ui_lang,
        quiz_id=int(pr.quiz_id) if pr.quiz_id is not None else None,
        shuffle_mode=str(pr.shuffle_mode or "none"),
        shuffle_strategy=_normalize_shuffle_strategy(pr.shuffle_strategy or "saved"),
        started=ct not in {"group", "supergroup"},
        participants={int(pr.user_id): call.from_user.full_name or str(pr.user_id)},
    )
    run.current_index = max(0, min(len(questions), int(pr.current_index or 0)))

    # Restore score from the paused run (private chat: single user).
    run.scores = dict(pr.scores or {})
    uname = getattr(call.from_user, "username", "") or ""
    if int(pr.user_id) in run.scores:
        run.scores[int(pr.user_id)].name = run.participants[int(pr.user_id)]
        if uname:
            run.scores[int(pr.user_id)].username = uname
    else:
        run.scores[int(pr.user_id)] = UserScore(name=run.participants[int(pr.user_id)], username=uname)

    _ACTIVE_RUNS[run_id] = run
    _PAUSED_RUNS.pop(token, None)

    await call.answer(t(ui_lang, "resumed_short"))
    if call.message:
        try:
            await call.message.edit_text(t(ui_lang, "resumed_short"), reply_markup=_kb_run_controls(run_id, ui_lang=ui_lang))
        except Exception:
            pass

    if run.current_index >= len(run.questions):
        await bot.send_message(run.chat_id, t(ui_lang, "quiz_finished"))
        if run.scores:
            await bot.send_message(
                run.chat_id,
                _format_scoreboard(run),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        _ACTIVE_RUNS.pop(run_id, None)
        return

    remaining = len(run.questions) - int(run.current_index or 0)
    total_sec = max(0, remaining * max(0, run.open_period))
    est = f"{total_sec//60}m {total_sec%60}s" if total_sec >= 60 else f"{total_sec}s"
    await bot.send_message(
        run.chat_id,
        t(ui_lang, "quiz_resumed_private", remaining=remaining, sec=int(run.open_period), est=est),
        reply_markup=_kb_run_controls(run_id, ui_lang=ui_lang),
    )
    run.task = asyncio.create_task(_run_quiz(bot, run))


@router.poll_answer()
async def on_poll_answer(poll_answer: types.PollAnswer, bot: Bot) -> None:
    ctx = _POLL_CTX.get(poll_answer.poll_id)
    if ctx is None:
        return

    run = _ACTIVE_RUNS.get(ctx.run_id)
    if run is None or run.cancelled:
        return

    user = poll_answer.user
    user_id = int(user.id)

    # In groups we auto-join anyone who answers; the lobby Join button is optional.
    if user_id not in run.participants:
        # Allow users removed for inactivity to "resume" by answering again (groups only).
        if user_id in run.inactive_participants:
            prev_name = run.inactive_participants.pop(user_id, "")
            run.participants[user_id] = user.full_name or prev_name or str(user_id)
            run.scores.setdefault(user_id, UserScore(name=run.participants[user_id], username=(getattr(user, "username", "") or "")))
            run.scores[user_id].username = (getattr(user, "username", "") or run.scores[user_id].username)
            run.no_answer_streak[user_id] = 0
            run.no_answer_streak_start.pop(user_id, None)
        elif run.chat_type in {"group", "supergroup"}:
            run.participants[user_id] = user.full_name or str(user_id)
            run.scores.setdefault(user_id, UserScore(name=run.participants[user_id], username=(getattr(user, "username", "") or "")))
            run.scores[user_id].username = (getattr(user, "username", "") or run.scores[user_id].username)
            run.no_answer_streak[user_id] = 0
            run.no_answer_streak_start.pop(user_id, None)
        else:
            return


    if not poll_answer.option_ids:
        return
    option_id = int(poll_answer.option_ids[0])

    per_q = run.answers.setdefault(ctx.question_index, {})
    if user_id in per_q:
        # Ignore duplicate/changed answers.
        return

    elapsed = max(0.0, time.monotonic() - float(ctx.started_at))
    is_correct = option_id == int(ctx.correct_option_id)
    per_q[user_id] = AnswerRecord(option_id=option_id, is_correct=is_correct, elapsed=elapsed)

    score = run.scores.get(user_id)
    if score is None:
        score = UserScore(
            name=run.participants.get(user_id) or (user.full_name or str(user_id)),
            username=(getattr(user, "username", "") or ""),
        )
        run.scores[user_id] = score
    else:
        if getattr(user, "username", ""):
            score.username = getattr(user, "username", "") or score.username
    score.answered += 1
    if is_correct:
        score.correct += 1
    score.total_time += elapsed

    # In groups we keep the current question open until the configured seconds expire.
    # Private chats may still auto-advance when all expected participants answered.
    if run.current_poll_id == poll_answer.poll_id and run.current_question_index == ctx.question_index:
        run.answered_users.add(user_id)
        expected = set(getattr(ctx, "expected_users", set()) or set())
        if run.chat_type not in {"group", "supergroup"} and expected and len(run.answered_users.intersection(expected)) >= len(expected):
            if run.current_poll_message_id:
                try:
                    await bot.stop_poll(run.chat_id, run.current_poll_message_id)
                except Exception:
                    pass
            run.advance_event.set()


@router.callback_query(F.data.startswith("ai_cancel:"))
async def ai_cancel(call: types.CallbackQuery, state: FSMContext) -> None:
    session_id = call.data.split(":", 1)[1]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    image_paths = list(data.get("ai_image_paths") or [])
    pdf_path = str(data.get("ai_pdf_path") or "").strip()
    pptx_path = str(data.get("ai_pptx_path") or "").strip()
    await call.answer()
    await state.clear()
    # Cleanup temporary scanned-PDF images (only under downloads/).
    for p in image_paths:
        try:
            pp = Path(str(p))
            if not _is_under_dir(pp, _DOWNLOAD_DIR):
                continue
            if pp.exists():
                pp.unlink(missing_ok=True)
        except Exception:
            pass
    # Try removing empty scan_* folders under downloads.
    try:
        for d in _DOWNLOAD_DIR.glob("scan_*"):
            if d.is_dir() and not any(d.iterdir()):
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass
    # Cleanup temporary uploaded PDF (file-mode only), only under downloads/.
    if pdf_path:
        try:
            pp = Path(pdf_path)
            if _is_under_dir(pp, _DOWNLOAD_DIR) and pp.exists():
                pp.unlink(missing_ok=True)
        except Exception:
            pass
    # Cleanup temporary uploaded PPTX (file-mode only), only under downloads/.
    if pptx_path:
        try:
            pp = Path(pptx_path)
            if _is_under_dir(pp, _DOWNLOAD_DIR) and pp.exists():
                pp.unlink(missing_ok=True)
        except Exception:
            pass
    if call.message:
        await call.message.edit_text(t(ui_lang, "cancelled"))


@router.callback_query(F.data.startswith("ai_pages:"))
async def ai_pages(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, return_to = parts[1], parts[2]
    if return_to not in {"count", "translate"}:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    has_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    if not has_pages:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await call.answer()
    await state.update_data(ai_pages_return=return_to)
    await state.set_state(AIQuizStates.choose_pages)

    total = int(data.get("ai_pages_total") or 0)
    if total <= 0 and data.get("ai_image_paths"):
        try:
            total = len(list(data.get("ai_image_paths") or []))
        except Exception:
            total = 0
    cur_from = int(data.get("ai_page_from") or 0)
    cur_to = int(data.get("ai_page_to") or 0)
    hint = ""
    if cur_from >= 1 and cur_to >= cur_from:
        hint = t(ui_lang, "current_pages", p_from=cur_from, p_to=cur_to) + "\n"
    if call.message:
        await call.message.answer(
            hint + t(ui_lang, "pages_prompt", total=total or 0),
            reply_markup=_kb_page_presets(session_id, total, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("ai_pageset:"))
async def ai_pageset(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 4:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    session_id = parts[1]
    try:
        p_from = int(parts[2])
        p_to = int(parts[3])
    except Exception:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    has_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    if not has_pages:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    total_pages = int(data.get("ai_pages_total") or 0)
    if total_pages <= 0 and data.get("ai_image_paths"):
        try:
            total_pages = len(list(data.get("ai_image_paths") or []))
        except Exception:
            total_pages = 0

    if p_from < 1 or p_to < 1 or p_to < p_from:
        await call.answer(t(ui_lang, "pages_invalid", total=total_pages or 0), show_alert=True)
        return
    if total_pages > 0 and (p_from > total_pages or p_to > total_pages):
        await call.answer(t(ui_lang, "pages_invalid", total=total_pages), show_alert=True)
        return

    await call.answer()
    await state.update_data(ai_page_from=int(p_from), ai_page_to=int(p_to))

    image_paths = list(data.get("ai_image_paths") or [])
    if image_paths:
        new_max = max(1, min(len(image_paths), int(p_to) - int(p_from) + 1))
        await state.update_data(ai_max_questions=int(new_max))
        qcount = int(data.get("ai_question_count") or 0)
        if qcount and qcount > new_max:
            await state.update_data(ai_question_count=int(new_max))

    if call.message:
        await call.message.answer(t(ui_lang, "pages_set", p_from=int(p_from), p_to=int(p_to)))

    data = await state.get_data()
    show_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    return_to = str(data.get("ai_pages_return") or "count").strip().lower()
    if return_to == "translate":
        await state.set_state(AIQuizStates.choose_translate)
        settings = await get_or_create_user_settings(user_id=call.from_user.id)
        default_lang = str(settings.get("default_lang") or "source")
        if call.message:
            await call.message.answer(
                t(ui_lang, "need_translation"),
                reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
            )
        return

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    await state.set_state(AIQuizStates.choose_count)
    mode = str(data.get("ai_mode") or "").strip().lower()
    prompt_key = "choose_count"
    prompt_kwargs: Dict[str, Any] = {"max_n": max_n}
    if mode == "scanpdf":
        pages = int(data.get("ai_max_questions") or data.get("ai_pages_total") or 0)
        pages = max(1, int(pages or 0))
        prompt_key = "scan_pdf_choose_count"
        prompt_kwargs = {"pages": pages, "max_n": min(50, pages)}
    if call.message:
        await call.message.answer(
            t(ui_lang, prompt_key, **prompt_kwargs),
            reply_markup=_kb_counts(session_id, max_n=max_n, ui_lang=ui_lang, show_pages=show_pages),
        )


@router.callback_query(F.data.startswith("ai_topic:"))
async def ai_topic(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, return_to = parts[1], parts[2]
    if return_to not in {"count", "translate"}:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    await call.answer()
    await state.update_data(ai_topic_return=return_to)
    await state.set_state(AIQuizStates.choose_topic)
    current = str(data.get("ai_topic") or "").strip()
    hint = (t(ui_lang, "current_topic", topic=current) + "\n") if current else ""
    mode = str(data.get("ai_mode") or "").strip().lower()
    prompt_key = "topic_prompt" if mode == "topic" else "file_topic_prompt"
    if call.message:
        await call.message.answer(hint + t(ui_lang, prompt_key))


def _first_int(text: str) -> Optional[int]:
    match = re.search(r"\d+", text or "")
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


_COUNT_HINT_RE = re.compile(r"(?i)\b(\d{1,2})\s*(ta|savol|test|mat|question|questions)\b")
_TIME_HINT_RE = re.compile(r"(?i)\b(\d{1,3})\s*(s|sec|sek|sekund|soniya)\b")



_DIFF_MIXED_RE = re.compile(r"(?i)\b(aralash|mixed|mix|random|default)\b")
_DIFF_EASY_RE = re.compile(r"(?i)\b(oson|yengil|easy|beginner|basic|a1|a2|kolay)\b")
_DIFF_MED_RE = re.compile(r"(?i)\b(o['?]?rta|orta|ortacha|medium|intermediate|b1|b2)\b")
_DIFF_HARD_RE = re.compile(r"(?i)\b(qiyin|murakkab|hard|advanced|c1|c2|zor)\b")


def _difficulty_from_text(raw_text: str) -> Optional[str]:
    raw = (raw_text or "").strip().lower()
    if not raw:
        return None

    hits: set[str] = set()
    if _DIFF_MIXED_RE.search(raw):
        hits.add("mixed")
    if _DIFF_EASY_RE.search(raw):
        hits.add("easy")
    if _DIFF_MED_RE.search(raw):
        hits.add("medium")
    if _DIFF_HARD_RE.search(raw):
        hits.add("hard")

    non_mixed = {h for h in hits if h != "mixed"}
    if len(non_mixed) == 1:
        return next(iter(non_mixed))
    if not non_mixed and "mixed" in hits:
        return "mixed"
    return None


def _strip_difficulty(topic: str) -> str:
    x = str(topic or "")
    x = _DIFF_MIXED_RE.sub(" ", x)
    x = _DIFF_EASY_RE.sub(" ", x)
    x = _DIFF_MED_RE.sub(" ", x)
    x = _DIFF_HARD_RE.sub(" ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def _parse_topic_count_time_difficulty(raw_text: str, *, max_count: int = 50) -> tuple[str, List[int], List[int], Optional[str]]:
    topic, counts, secs = _parse_topic_count_time(raw_text, max_count=max_count)
    diff = _difficulty_from_text(raw_text)
    if diff:
        topic = _strip_difficulty(topic)
    return topic, counts, secs, diff

def _uniq_ints(ints: List[int]) -> List[int]:
    out: List[int] = []
    for n in ints:
        if n not in out:
            out.append(n)
    return out


def _question_identity(q: Dict[str, Any]) -> str:
    return re.sub(r"\s+", " ", str(q.get("question") or "")).strip().lower()


def _merge_unique_questions(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for q in list(items or []):
        if not isinstance(q, dict):
            continue
        key = _question_identity(q)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out


def _shuffle_question_options_local(question: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(question or {})
    options = list(payload.get("options") or [])
    try:
        correct_index = int(payload.get("correct_index"))
    except Exception:
        return payload
    if len(options) != 4 or correct_index < 0 or correct_index >= len(options):
        return payload
    order = list(range(len(options)))
    random.shuffle(order)
    payload["options"] = [options[i] for i in order]
    payload["correct_index"] = int(order.index(correct_index))
    return payload


def _apply_quiz_shuffle(items: List[Dict[str, Any]], *, shuffle_questions: bool, shuffle_options: bool) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for q in list(items or []):
        payload = dict(q or {})
        if shuffle_options:
            payload = _shuffle_question_options_local(payload)
        out.append(payload)
    if shuffle_questions and len(out) > 1:
        random.shuffle(out)
    return out


def _should_offer_ai_shuffle(data: Dict[str, Any]) -> bool:
    mode = str(data.get("ai_mode") or "").strip().lower()
    return mode in {"file", "image"}


def _shuffle_mode_flags(mode: str) -> tuple[bool, bool]:
    m = str(mode or "none").strip().lower()
    if m == "questions":
        return True, False
    if m == "options":
        return False, True
    if m == "both":
        return True, True
    return False, False


def _normalize_shuffle_strategy(strategy: Any) -> str:
    s = str(strategy or "saved").strip().lower()
    if s == "runtime":
        return "runtime"
    return "saved"


async def _fill_questions_from_text(
    *,
    existing: List[Dict[str, Any]],
    source_text: str,
    question_count: int,
    output_language: str,
    difficulty: str,
    focus_topic: str,
    allow_generate: bool,
    shuffle_options: bool,
) -> tuple[List[Dict[str, Any]], bool]:
    target = max(1, int(question_count or 0))
    questions = _merge_unique_questions(existing)
    source = str(source_text or "").strip()
    if len(questions) >= target or not allow_generate or len(source) < 200:
        return questions[:target], False

    used_ai = False
    stagnant = 0
    for _ in range(3):
        if len(questions) >= target:
            break
        remaining = target - len(questions)
        request_n = min(50, max(remaining, remaining + min(5, max(1, remaining // 2))))
        try:
            extra = await ai_service.generate_quiz_from_text(
                source,
                question_count=request_n,
                output_language=output_language,
                difficulty=difficulty,
                focus_topic=focus_topic,
                shuffle_options=shuffle_options,
            )
        except Exception:
            break
        used_ai = True
        before = len(questions)
        questions = _merge_unique_questions(questions + list(extra or []))
        if len(questions) == before:
            stagnant += 1
            if stagnant >= 1:
                break
        else:
            stagnant = 0

    return questions[:target], used_ai


def _parse_topic_count_time(raw_text: str, *, max_count: int = 50) -> tuple[str, List[int], List[int]]:
    """Parse one message that may contain topic + count/time hints."""
    raw = (raw_text or "").strip()
    if not raw:
        return "", [], []

    counts = _uniq_ints([int(m.group(1)) for m in _COUNT_HINT_RE.finditer(raw) if m.group(1).isdigit()])
    secs = _uniq_ints([int(m.group(1)) for m in _TIME_HINT_RE.finditer(raw) if m.group(1).isdigit()])

    topic = raw
    if secs:
        topic = _TIME_HINT_RE.sub(" ", topic)
    if counts:
        topic = _COUNT_HINT_RE.sub(" ", topic)
    else:
        # Fallback: if message ends with a number and includes words, treat it as count.
        m = re.search(r"\b(\d{1,2})\b\s*$", raw)
        if m:
            try:
                n = int(m.group(1))
            except Exception:
                n = 0
            prefix = raw[: m.start()].strip()
            if prefix and re.search(r"[A-Za-zРђ-РЇР°-СЏРЋСћТљТ›Т’Т“ТІТіРЃС‘Д°Д±]", prefix):
                counts = [n]
                topic = raw[: m.start()].strip()

    # Normalize topic text.
    topic = re.sub(r"(?i)\b(?:mavzu|topic|focus)\s*[:\\-]\s*", "", topic).strip()
    topic = re.sub(r"(?i)\b(?:test|quiz|savol|qil|qiling|qilib|yarat|tuz|tuzib|ber|qilib\s*ber)\b", " ", topic)
    topic = re.sub(r"[;,|]+", " ", topic)
    topic = re.sub(r"\s+", " ", topic).strip()
    topic = re.sub(r"(?i)\b(?:dan|bo'yicha|boвЂyicha|uchun|haqida)\b\s*$", "", topic).strip()

    # Clamp values to reasonable ranges.
    max_count = max(1, min(50, int(max_count or 50)))
    counts = [n for n in counts if 1 <= int(n) <= max_count]
    secs = [n for n in secs if 5 <= int(n) <= 600]

    return topic, counts, secs


def _parse_page_range(raw_text: str) -> Optional[tuple[int, int]]:
    raw = (raw_text or "").strip()
    if not raw:
        return None
    nums = [int(x) for x in re.findall(r"\d{1,4}", raw)]
    if not nums:
        return None
    if len(nums) == 1:
        return nums[0], nums[0]
    return nums[0], nums[1]


@router.message(AIQuizStates.choose_pages)
async def ai_choose_pages_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not data.get("ai_session_id"):
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return
    if data.get("ai_user_id") != (message.from_user.id if message.from_user else None):
        await message.answer(t(ui_lang, "session_owner_only"))
        return

    has_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    if not has_pages:
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return

    raw = (message.text or "").strip()
    total_pages = int(data.get("ai_pages_total") or 0)
    if total_pages <= 0 and data.get("ai_image_paths"):
        try:
            total_pages = len(list(data.get("ai_image_paths") or []))
        except Exception:
            total_pages = 0
    if not raw:
        await message.answer(
            t(ui_lang, "pages_prompt", total=total_pages or 0),
            reply_markup=_kb_page_presets(str(data.get("ai_session_id") or ""), total_pages or 0, ui_lang=ui_lang),
        )
        return

    low = raw.strip().lower()
    if low in {"-", "0", "yoq", "yo'q", "yoвЂq", "none", "no", "skip", "all", "hammasi"}:
        await message.answer(t(ui_lang, "pages_required"))
        await message.answer(
            t(ui_lang, "pages_prompt", total=total_pages or 0),
            reply_markup=_kb_page_presets(str(data.get("ai_session_id") or ""), total_pages or 0, ui_lang=ui_lang),
        )
        return
    else:
        pr = _parse_page_range(raw)
        if not pr:
            await message.answer(t(ui_lang, "pages_invalid", total=total_pages or 0))
            return
        p_from, p_to = pr
        if p_from < 1 or p_to < 1 or p_to < p_from:
            await message.answer(t(ui_lang, "pages_invalid", total=total_pages or 0))
            return
        if total_pages > 0 and (p_from > total_pages or p_to > total_pages):
            await message.answer(t(ui_lang, "pages_invalid", total=total_pages))
            return
        await state.update_data(ai_page_from=int(p_from), ai_page_to=int(p_to))

        image_paths = list(data.get("ai_image_paths") or [])
        if image_paths:
            new_max = max(1, min(len(image_paths), int(p_to) - int(p_from) + 1))
            await state.update_data(ai_max_questions=int(new_max))
            qcount = int(data.get("ai_question_count") or 0)
            if qcount and qcount > new_max:
                await state.update_data(ai_question_count=int(new_max))

        await message.answer(t(ui_lang, "pages_set", p_from=int(p_from), p_to=int(p_to)))

    data = await state.get_data()
    session_id = str(data.get("ai_session_id"))
    show_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    return_to = str(data.get("ai_pages_return") or "count").strip().lower()
    if return_to == "translate":
        await state.set_state(AIQuizStates.choose_translate)
        settings = await get_or_create_user_settings(user_id=message.from_user.id if message.from_user else 0)
        default_lang = str(settings.get("default_lang") or "source")
        await message.answer(
            t(ui_lang, "need_translation"),
            reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
        )
        return

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    await state.set_state(AIQuizStates.choose_count)
    mode = str(data.get("ai_mode") or "").strip().lower()
    prompt_key = "choose_count"
    prompt_kwargs: Dict[str, Any] = {"max_n": max_n}
    if mode == "scanpdf":
        pages = int(data.get("ai_max_questions") or data.get("ai_pages_total") or 0)
        pages = max(1, int(pages or 0))
        prompt_key = "scan_pdf_choose_count"
        prompt_kwargs = {"pages": pages, "max_n": min(50, pages)}
    await message.answer(
        t(ui_lang, prompt_key, **prompt_kwargs),
        reply_markup=_kb_counts(session_id, max_n=max_n, ui_lang=ui_lang, show_pages=show_pages),
    )




async def _continue_after_topic_settings(
    message: types.Message,
    state: FSMContext,
    *,
    ui_lang: str,
    user_id: int,
    max_n: int,
) -> None:
    data = await state.get_data()
    session_id = str(data.get("ai_session_id") or "")
    if not session_id:
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return

    return_to = str(data.get("ai_topic_return") or "count").strip().lower()
    show_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())

    # If user clicked "topic optional" during count/pages stage, we jump back to translation.
    if return_to == "translate":
        await state.set_state(AIQuizStates.choose_translate)
        settings = await get_or_create_user_settings(user_id=user_id)
        default_lang = str(settings.get("default_lang") or "source")
        await message.answer(
            t(ui_lang, "need_translation"),
            reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
        )
        return

    # If user already provided count/time in one message, skip straight to translation.
    if data.get("ai_question_count") and data.get("ai_open_period"):
        await state.set_state(AIQuizStates.choose_translate)
        settings = await get_or_create_user_settings(user_id=user_id)
        default_lang = str(settings.get("default_lang") or "source")
        await message.answer(
            t(ui_lang, "need_translation"),
            reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
        )
        return

    if data.get("ai_question_count"):
        await state.set_state(AIQuizStates.choose_time)
        await message.answer(t(ui_lang, "choose_time"), reply_markup=_kb_ai_time_presets(session_id, ui_lang=ui_lang))
        return

    await state.set_state(AIQuizStates.choose_count)
    await message.answer(
        t(ui_lang, "choose_count", max_n=max_n),
        reply_markup=_kb_counts(session_id, max_n=max_n, ui_lang=ui_lang, show_pages=show_pages),
    )

@router.message(AIQuizStates.choose_topic)
async def ai_choose_topic_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not data.get("ai_session_id"):
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return
    if data.get("ai_user_id") != (message.from_user.id if message.from_user else None):
        await message.answer(t(ui_lang, "session_owner_only"))
        return

    raw = (message.text or "").strip()
    if raw and len(raw) > _TOPIC_MAX_CHARS:
        await message.answer(t(ui_lang, 'topic_too_long', n=_TOPIC_MAX_CHARS))
        return
    if not raw:
        mode = str(data.get("ai_mode") or "").strip().lower()
        prompt_key = "topic_prompt" if mode == "topic" else "file_topic_prompt"
        await message.answer(t(ui_lang, prompt_key))
        return

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    # Topic input: accept only the topic text; count/time are selected separately via buttons.
    topic, _counts, _secs, diff = _parse_topic_count_time_difficulty(raw, max_count=max_n)

    low = raw.strip().lower()
    if low in {'-', '0', 'yoq', "yo'q", 'none', 'no', 'skip'}:
        topic = ''

    await state.update_data(ai_topic=topic)
    if diff:
        await state.update_data(ai_difficulty=diff)

    mode = str(data.get("ai_mode") or "").strip().lower()
    if mode == "topic" and topic:
        await state.update_data(ai_title=topic[:120], ai_text="")

    data = await state.get_data()
    session_id = str(data.get("ai_session_id") or "")

    # Topic-only mode: ask for difficulty before going to count/time/translation.
    if mode == "topic":
        cur_diff = str(data.get("ai_difficulty") or "").strip().lower()
        if not cur_diff:
            await state.set_state(AIQuizStates.choose_difficulty)
            await message.answer(t(ui_lang, "choose_difficulty"), reply_markup=_kb_difficulty(session_id, ui_lang=ui_lang))
            return

    user_id = message.from_user.id if message.from_user else 0
    await _continue_after_topic_settings(message, state, ui_lang=ui_lang, user_id=user_id, max_n=max_n)


@router.message(AIQuizStates.choose_difficulty)
async def ai_choose_difficulty_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not data.get("ai_session_id"):
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return
    if data.get("ai_user_id") != (message.from_user.id if message.from_user else None):
        await message.answer(t(ui_lang, "session_owner_only"))
        return

    diff = _difficulty_from_text(message.text or "")
    if diff not in {"easy", "medium", "hard", "mixed"}:
        await message.answer(t(ui_lang, "difficulty_invalid"))
        return

    await state.update_data(ai_difficulty=diff)

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    user_id = message.from_user.id if message.from_user else 0
    await _continue_after_topic_settings(message, state, ui_lang=ui_lang, user_id=user_id, max_n=max_n)


@router.callback_query(F.data.startswith("ai_topic_anyway:"))
async def ai_topic_anyway(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":", 1)
    if len(parts) != 2:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id = parts[1]

    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    await call.answer()

    # Proceed without external source; keep ai_text empty so AI uses topic-only prompting.
    mode = str(data.get("ai_mode") or "").strip().lower()
    if mode != "topic":
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    cur_diff = str(data.get("ai_difficulty") or "").strip().lower()
    if not cur_diff:
        await state.set_state(AIQuizStates.choose_difficulty)
        if call.message:
            await call.message.answer(t(ui_lang, "choose_difficulty"), reply_markup=_kb_difficulty(session_id, ui_lang=ui_lang))
        return

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    if call.message:
        await _continue_after_topic_settings(call.message, state, ui_lang=ui_lang, user_id=call.from_user.id, max_n=max_n)


@router.message(AIQuizStates.choose_count)
async def ai_choose_count_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not data.get("ai_session_id"):
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return
    if data.get("ai_user_id") != (message.from_user.id if message.from_user else None):
        await message.answer(t(ui_lang, "session_owner_only"))
        return

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    text = message.text or ""

    # Allow typing a page range in this step (PDF only), e.g. "20-30".
    has_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    if has_pages:
        pr = _parse_page_range(text)
        looks_like_pages = bool(re.search(r"\d+\s*(?:-|\\.\\.)\s*\d+", text)) or bool(
            re.search(r"(?i)\b(sahifa|bet|page|pages|СЃС‚СЂ|СЃС‚СЂР°РЅРёС†)\b", text)
        )
        if pr and looks_like_pages:
            p_from, p_to = pr
            total_pages = int(data.get("ai_pages_total") or 0)
            image_paths = list(data.get("ai_image_paths") or [])
            if total_pages <= 0 and image_paths:
                total_pages = len(image_paths)
            if p_from < 1 or p_to < 1 or p_to < p_from:
                await message.answer(t(ui_lang, "pages_invalid", total=total_pages or 0))
                return
            if total_pages > 0 and (p_from > total_pages or p_to > total_pages):
                await message.answer(t(ui_lang, "pages_invalid", total=total_pages))
                return

            await state.update_data(ai_page_from=int(p_from), ai_page_to=int(p_to))
            pages = 0
            if image_paths:
                new_max = max(1, min(len(image_paths), int(p_to) - int(p_from) + 1))
                await state.update_data(ai_max_questions=int(new_max))
                pages = int(new_max)
                max_n = max(1, min(50, int(new_max)))

            await message.answer(t(ui_lang, "pages_set", p_from=int(p_from), p_to=int(p_to)))
            session_id = str(data.get("ai_session_id"))
            await state.set_state(AIQuizStates.choose_count)
            mode = str(data.get("ai_mode") or "").strip().lower()
            prompt_key = "choose_count"
            prompt_kwargs: Dict[str, Any] = {"max_n": max_n}
            if mode == "scanpdf":
                if not pages:
                    pages = max_n
                prompt_key = "scan_pdf_choose_count"
                prompt_kwargs = {"pages": pages, "max_n": min(50, pages)}
            await message.answer(
                t(ui_lang, prompt_key, **prompt_kwargs),
                reply_markup=_kb_counts(session_id, max_n=max_n, ui_lang=ui_lang, show_pages=True),
            )
            return

    # In this step we accept ONLY the question count.
    # Topic must be set via the "Mavzu (ixtiyoriy)" button (ai_topic callback).
    m = re.fullmatch(r"\s*(\d{1,3})\s*(?:ta|savol|test|mat|question|questions)?\s*", text, flags=re.I)
    if not m:
        await message.answer(t(ui_lang, "count_invalid", max_n=max_n))
        return

    n = int(m.group(1))
    if n < 1 or n > max_n:
        await message.answer(t(ui_lang, "count_invalid", max_n=max_n))
        return

    await state.update_data(ai_question_count=int(n))

    await state.set_state(AIQuizStates.choose_time)
    await message.answer(t(ui_lang, "choose_time"), reply_markup=_kb_ai_time_presets(session_id, ui_lang=ui_lang))


@router.message(AIQuizStates.choose_time)
async def ai_choose_time_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    if not data.get("ai_session_id"):
        await state.clear()
        await message.answer(t(ui_lang, "session_missing"))
        return
    if data.get("ai_user_id") != (message.from_user.id if message.from_user else None):
        await message.answer(t(ui_lang, "session_owner_only"))
        return

    sec = _first_int(message.text or "")
    if sec is None or sec < 5 or sec > 600:
        await message.answer(t(ui_lang, "time_invalid"), reply_markup=_kb_ai_time_presets(str(data.get("ai_session_id") or ""), ui_lang=ui_lang))
        return

    session_id = str(data.get("ai_session_id"))
    await state.update_data(ai_open_period=int(sec))
    await state.set_state(AIQuizStates.choose_translate)
    settings = await get_or_create_user_settings(user_id=message.from_user.id if message.from_user else 0)
    default_lang = str(settings.get("default_lang") or "source")
    show_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
    await message.answer(
        t(ui_lang, "need_translation"),
        reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
    )


@router.callback_query(F.data.startswith("ai_count:"))
async def ai_choose_count(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, raw_n = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    try:
        n = int(raw_n)
    except Exception:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    if n < 1 or n > max_n:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await call.answer(t(ui_lang, "accepted"))
    await state.update_data(ai_question_count=n)
    await state.set_state(AIQuizStates.choose_time)
    if call.message:
        prompt = t(ui_lang, "count_chosen", n=n) + "\n" + t(ui_lang, "choose_time")
        markup = _kb_ai_time_presets(session_id, ui_lang=ui_lang)
        try:
            await call.message.edit_text(prompt, reply_markup=markup)
        except Exception:
            await call.message.answer(prompt, reply_markup=markup)


@router.callback_query(F.data.startswith("ai_time:"))
async def ai_choose_time(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, raw_sec = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    try:
        sec = int(raw_sec)
    except Exception:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    if sec not in _TIME_PRESET_VALUES:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await call.answer()
    await state.update_data(ai_open_period=sec)
    await state.set_state(AIQuizStates.choose_translate)
    if call.message:
        settings = await get_or_create_user_settings(user_id=call.from_user.id)
        default_lang = str(settings.get("default_lang") or "source")
        show_pages = bool(data.get("ai_pages_required")) or bool(data.get("ai_image_paths")) or bool(str(data.get("ai_pdf_path") or "").strip()) or bool(str(data.get("ai_pptx_path") or "").strip())
        await call.message.answer(
            t(ui_lang, "need_translation"),
            reply_markup=_kb_translate(session_id, default_lang=default_lang, ui_lang=ui_lang, show_pages=show_pages),
        )


async def _animate_working_message(
    bot: Bot,
    *,
    chat_id: int,
    message_id: int,
    base_text: str,
    stop: asyncio.Event,
    interval_sec: float = 4.0,
) -> None:
    raw = str(os.getenv("AI_WORKING_ANIM_ENABLED", "1") or "1").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return

    base = str(base_text or "").strip()
    base = base.rstrip(".").rstrip("…").strip() or str(base_text or "").strip()
    frames = [".", "..", "..."]
    i = 0
    while not stop.is_set():
        i = (i + 1) % len(frames)
        try:
            await asyncio.wait_for(
                bot.edit_message_text(chat_id=int(chat_id), message_id=int(message_id), text=f"{base}{frames[i]}"),
                timeout=float(os.getenv("AI_WORKING_ANIM_TIMEOUT_SEC", "2.5") or 2.5),
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=float(interval_sec))
        except asyncio.TimeoutError:
            continue


async def _start_ai_quiz(bot: Bot, state: FSMContext, *, chat_id: int, user: types.User, output_language: str) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(int(getattr(user, "id", 0) or 0))
    text = str(data.get("ai_text") or "").strip()
    orig_image_paths: List[str] = list(data.get("ai_image_paths") or [])
    image_paths: List[str] = list(orig_image_paths)
    topic = str(data.get("ai_topic") or "").strip()
    difficulty = str(data.get("ai_difficulty") or "").strip().lower() or "mixed"
    title = str(data.get("ai_title") or "AI Quiz").strip()
    if not title:
        title = "AI Quiz"
    open_period = int(data.get("ai_open_period") or 30)
    question_count = int(data.get("ai_question_count") or 5)
    chat_type = str(data.get("ai_chat_type") or "private").strip().lower()
    pdf_path = str(data.get("ai_pdf_path") or "").strip()
    pptx_path = str(data.get("ai_pptx_path") or "").strip()
    page_from = int(data.get("ai_page_from") or 0)
    page_to = int(data.get("ai_page_to") or 0)

    mode = str(data.get('ai_mode') or '').strip().lower()
    quota_kind = 'topic' if mode == 'topic' else 'file'
    reservation: Optional[dict] = None
    should_offer_shuffle = mode in {"file", "image"}
    shuffle_mode = str(data.get("ai_shuffle_mode") or "none").strip().lower() if should_offer_shuffle else "none"
    shuffle_strategy = _normalize_shuffle_strategy(data.get("ai_shuffle_strategy") or "saved") if should_offer_shuffle else "saved"
    generation_shuffle_options = not should_offer_shuffle

    # Apply optional page-range selection for scanned PDFs (images) ONLY when images come from state.
    if orig_image_paths and page_from >= 1 and page_to >= page_from:
        start_i = max(1, page_from)
        end_i = min(len(image_paths), page_to)
        if end_i >= start_i:
            image_paths = image_paths[start_i - 1 : end_i]
        else:
            image_paths = []

    if not text and not image_paths and not topic and not pdf_path and not pptx_path:
        await state.clear()
        await bot.send_message(chat_id, t(ui_lang, "no_input_for_ai"))
        return

    # Upload qilingan barcha fayllar uchun sahifa oralig'i majburiy.
    if bool(data.get("ai_pages_required")) and not (page_from >= 1 and page_to >= page_from):
        await state.clear()
        await bot.send_message(chat_id, t(ui_lang, "pages_required"))
        return

    # Check quota without consuming. Consume only after AI returns successfully.
    try:
        await check_user_quota(user.id, quota_kind)
    except QuotaExceeded as exc:
        await state.clear()
        await bot.send_message(chat_id, _quota_exceeded_text(ui_lang, exc), reply_markup=_kb_premium_plans(ui_lang))
        return

    msg = await bot.send_message(chat_id, t(ui_lang, "ai_working"))
    anim_stop = asyncio.Event()
    anim_task = asyncio.create_task(
        _animate_working_message(
            bot,
            chat_id=int(chat_id),
            message_id=int(getattr(msg, "message_id", 0) or 0),
            base_text=t(ui_lang, "ai_working"),
            stop=anim_stop,
        )
    )
    quiz_id: Optional[int] = None
    imported_ready = False
    used_ai_generation = False
    try:
        use_paths: List[str] = []
        if image_paths:
            # One question per image (e.g., scanned PDF pages).
            use_paths = [str(p) for p in image_paths[:question_count] if str(p).strip()]
            used_ai_generation = True
            questions = await ai_service.generate_quiz_from_images(use_paths, output_language=output_language, shuffle_options=generation_shuffle_options)
            for q, p in zip(questions, use_paths):
                q["image_path"] = p
        elif pdf_path or pptx_path:
            # File-mode PDF/PPTX: extract ONLY selected pages/slides, then generate. If no text, fall back to vision.
            use_text = ""
            if pptx_path:
                try:
                    use_text = await asyncio.to_thread(
                        _extract_pptx_text_range,
                        Path(pptx_path),
                        page_from,
                        page_to,
                        char_limit=int(os.getenv("EXTRACT_CHAR_LIMIT", "200000")),
                    )
                except Exception as exc:
                    raise AIServiceError(f"PPTX slaydlardan matn ajratib bo'lmadi: {exc}") from exc

                # If the selected range already contains a ready quiz (Q + options + Answer),
                # import it directly (keeps order, doesn't lose questions).
                try:
                    parsed_title, ready_questions = parse_quiz_payload(use_text, title_fallback=title)
                except Exception:
                    parsed_title, ready_questions = ("", [])
                if ready_questions:
                    imported_ready = True
                    if parsed_title:
                        title = parsed_title.strip()[:120]
                    questions = ready_questions
                elif len(use_text.strip()) < 200:
                    try:
                        provider = ai_service._pick_provider()
                    except AIServiceError as exc:
                        raise
                    if provider not in {"openai", "gemini"}:
                        raise AIServiceError(t(ui_lang, "scan_pdf_need_gemini"))

                    out_dir = _DOWNLOAD_DIR / f"scan_{uuid.uuid4().hex}"
                    max_images = int(os.getenv("PPTX_SCAN_MAX_IMAGES", "30") or 30)
                    image_paths = await asyncio.to_thread(
                        _extract_pptx_media_images,
                        Path(pptx_path),
                        out_dir,
                        max_images=max_images,
                    )
                    if not image_paths:
                        raise AIServiceError(t(ui_lang, "scan_pdf_no_images"))

                    use_paths = [str(p) for p in image_paths[:question_count] if str(p).strip()]
                    used_ai_generation = True
                    questions = await ai_service.generate_quiz_from_images(use_paths, output_language=output_language, shuffle_options=generation_shuffle_options)
                    for q, p in zip(questions, use_paths):
                        q["image_path"] = p
                else:
                    used_ai_generation = True
                    questions = await ai_service.generate_quiz_from_text(
                        use_text,
                        question_count=question_count,
                        output_language=output_language,
                        difficulty=difficulty,
                        focus_topic=topic,
                        shuffle_options=generation_shuffle_options,
                    )

                if use_text.strip():
                    questions, topped_up = await _fill_questions_from_text(
                        existing=questions,
                        source_text=use_text,
                        question_count=question_count,
                        output_language=output_language,
                        difficulty=difficulty,
                        focus_topic=topic,
                        allow_generate=AI_ENABLED,
                        shuffle_options=generation_shuffle_options,
                    )
                    used_ai_generation = used_ai_generation or topped_up
            else:
                try:
                    use_text = await asyncio.to_thread(
                        _extract_pdf_text_range,
                        Path(pdf_path),
                        page_from,
                        page_to,
                        char_limit=int(os.getenv("EXTRACT_CHAR_LIMIT", "200000")),
                    )
                except Exception as exc:
                    raise AIServiceError(f"PDF sahifalaridan matn ajratib bo'lmadi: {exc}") from exc

                # If the selected range already contains a ready quiz (Q + options + Answer),
                # import it directly (keeps order, doesn't lose questions).
                try:
                    parsed_title, ready_questions = parse_quiz_payload(use_text, title_fallback=title)
                except Exception:
                    parsed_title, ready_questions = ("", [])
                if ready_questions:
                    imported_ready = True
                    if parsed_title:
                        title = parsed_title.strip()[:120]
                    questions = ready_questions
                elif len(use_text.strip()) < 200:
                    try:
                        provider = ai_service._pick_provider()
                    except AIServiceError as exc:
                        raise
                    if provider not in {"openai", "gemini"}:
                        raise AIServiceError(t(ui_lang, "scan_pdf_need_gemini"))

                    out_dir = _DOWNLOAD_DIR / f"scan_{uuid.uuid4().hex}"
                    max_pages = int(os.getenv("PDF_SCAN_MAX_PAGES", "30") or 30)
                    zoom = float(os.getenv("PDF_SCAN_ZOOM", "2.0") or 2.0)
                    image_paths = await asyncio.to_thread(
                        _render_pdf_page_range_to_images,
                        Path(pdf_path),
                        out_dir,
                        page_from,
                        page_to,
                        max_pages=max_pages,
                        zoom=zoom,
                    )
                    if not image_paths:
                        raise AIServiceError(t(ui_lang, "scan_pdf_no_images"))

                    use_paths = [str(p) for p in image_paths[:question_count] if str(p).strip()]
                    used_ai_generation = True
                    questions = await ai_service.generate_quiz_from_images(use_paths, output_language=output_language, shuffle_options=generation_shuffle_options)
                    for q, p in zip(questions, use_paths):
                        q["image_path"] = p
                else:
                    used_ai_generation = True
                    questions = await ai_service.generate_quiz_from_text(
                        use_text,
                        question_count=question_count,
                        output_language=output_language,
                        difficulty=difficulty,
                        focus_topic=topic,
                        shuffle_options=generation_shuffle_options,
                    )

                if use_text.strip():
                    questions, topped_up = await _fill_questions_from_text(
                        existing=questions,
                        source_text=use_text,
                        question_count=question_count,
                        output_language=output_language,
                        difficulty=difficulty,
                        focus_topic=topic,
                        allow_generate=AI_ENABLED,
                        shuffle_options=generation_shuffle_options,
                    )
                    used_ai_generation = used_ai_generation or topped_up
        elif text:
            try:
                parsed_title, ready_questions = parse_quiz_payload(text, title_fallback=title)
            except Exception:
                parsed_title, ready_questions = ("", [])
            if ready_questions:
                imported_ready = True
                if parsed_title:
                    title = parsed_title.strip()[:120]
                questions = ready_questions
            else:
                if bool(data.get("ai_import_only")) or (not AI_ENABLED):
                    raise AIServiceError(t(ui_lang, "import_failed"))
                used_ai_generation = True
                questions = await ai_service.generate_quiz_from_text(
                    text,
                    question_count=question_count,
                    output_language=output_language,
                    difficulty=difficulty,
                    focus_topic=topic,
                    shuffle_options=generation_shuffle_options,
                )

            questions, topped_up = await _fill_questions_from_text(
                existing=questions,
                source_text=text,
                question_count=question_count,
                output_language=output_language,
                difficulty=difficulty,
                focus_topic=topic,
                allow_generate=AI_ENABLED and not bool(data.get("ai_import_only")),
                shuffle_options=generation_shuffle_options,
            )
            used_ai_generation = used_ai_generation or topped_up
        else:
            ctx = None
            if topic:
                try:
                    ctx = await fetch_topic_context(topic, ui_lang=ui_lang)
                except Exception:
                    ctx = None

            ctx_text = str(getattr(ctx, "text", "") or "").strip() if ctx else ""
            if ctx_text:
                ctx_title = str(getattr(ctx, "title", "") or "").strip()
                if ctx_title:
                    title = ctx_title[:120]
                used_ai_generation = True
                questions = await ai_service.generate_quiz_from_text(
                    ctx_text,
                    question_count=question_count,
                    output_language=output_language,
                    difficulty=difficulty,
                    focus_topic=topic,
                    shuffle_options=generation_shuffle_options,
                )
            else:
                used_ai_generation = True
                questions = await ai_service.generate_quiz_from_topic(
                    topic,
                    question_count=question_count,
                    output_language=output_language,
                    difficulty=difficulty,
                    shuffle_options=generation_shuffle_options,
                )

        if not questions:
            raise AIServiceError("Savollar chiqmadi. Iltimos, sahifa oralig'ini o'zgartirib qayta urinib ko'ring.")

        reservation = await reserve_user_quota(user.id, quota_kind)

        await get_or_create_user(user_id=user.id, full_name=user.full_name, username=getattr(user, "username", None))
        quiz_id = await create_quiz(
            title=title,
            creator_id=user.id,
            is_ai_generated=used_ai_generation,
            open_period=open_period,
            shuffle_mode=shuffle_mode,
            shuffle_strategy=shuffle_strategy,
        )

        # Persist images under media/quizzes/<quiz_id>/ so the quiz can be re-run any time.
        moved_sources: set[str] = set()
        if image_paths:
            used_set = {str(p) for p in use_paths}
            media_dir = Path("media") / "quizzes" / str(quiz_id)
            media_dir.mkdir(parents=True, exist_ok=True)
            for i, q in enumerate(questions, start=1):
                src = str(q.get("image_path") or "").strip()
                if not src:
                    continue
                src_p = Path(src)
                if not src_p.exists():
                    continue
                suffix = src_p.suffix or ".png"
                dst = media_dir / f"q{i}{suffix}"
                try:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    src_p.replace(dst)
                    moved_sources.add(str(src_p))
                    q["image_path"] = str(dst)
                except Exception:
                    # If move fails, keep original path; it might still exist.
                    pass

            # Clean up unused temporary images.
            for p in image_paths:
                try:
                    pp = Path(str(p))
                    if str(pp) in used_set or str(pp) in moved_sources:
                        continue
                    if pp.exists():
                        pp.unlink(missing_ok=True)
                except Exception:
                    pass

        if should_offer_shuffle and shuffle_strategy != "runtime":
            shuffle_questions, shuffle_options = _shuffle_mode_flags(shuffle_mode)
            if shuffle_questions or shuffle_options:
                questions = _apply_quiz_shuffle(questions, shuffle_questions=shuffle_questions, shuffle_options=shuffle_options)

        await create_questions_bulk(quiz_id, questions)

        username = await _get_bot_username(bot)
        total_sec = max(0, len(questions) * max(0, open_period))
        est = f"{total_sec//60}m {total_sec%60}s" if total_sec >= 60 else f"{total_sec}s"
        meta_line = ""
        if topic:
            meta_line += t(ui_lang, "topic_line", topic=topic)
        if page_from >= 1 and page_to >= page_from:
            meta_line += t(ui_lang, "pages_line", p_from=page_from, p_to=page_to)
            meta_line += t(ui_lang, "done_line", p_from=page_from, p_to=page_to, n=len(questions))
        text_out = t(
            ui_lang,
            "ai_quiz_ready",
            title=title,
            topic_line=meta_line,
            count=len(questions),
            sec=int(open_period),
            est=est,
            id=int(quiz_id or 0),
        )
        if len(questions) < int(question_count or 0):
            if imported_ready and not used_ai_generation:
                text_out = text_out + "\n\n" + t(ui_lang, "import_partial", wanted=int(question_count), found=int(len(questions)))
            else:
                text_out = text_out + "\n\n" + t(ui_lang, "ai_partial", wanted=int(question_count), made=int(len(questions)))
        anim_stop.set()
        with suppress(Exception):
            await anim_task

        await msg.edit_text(
            text_out,
            reply_markup=_kb_quiz_share(
                username,
                quiz_id,
                title=title,
                question_count=len(questions),
                chat_type=chat_type,
                ui_lang=ui_lang,
                show_stats=True,
                show_edit=True,
            ),
        )
    except QuotaExceeded as exc:
        anim_stop.set()
        with suppress(Exception):
            await anim_task
        await msg.edit_text(_quota_exceeded_text(ui_lang, exc), reply_markup=_kb_premium_plans(ui_lang))
    except AIServiceError as exc:
        if reservation and quiz_id is None:
            await refund_user_quota(reservation)
        anim_stop.set()
        with suppress(Exception):
            await anim_task
        await msg.edit_text(t(ui_lang, "err_ai", err=str(exc)))
    except Exception as exc:
        if reservation and quiz_id is None:
            await refund_user_quota(reservation)
        anim_stop.set()
        with suppress(Exception):
            await anim_task
        await msg.edit_text(t(ui_lang, "err_unexpected", err=str(exc)))
    finally:
        anim_stop.set()
        if anim_task and not anim_task.done():
            anim_task.cancel()
            with suppress(Exception):
                await anim_task
        # If we were processing scanned-PDF images and the quiz was NOT saved, cleanup the temp images.
        if quiz_id is None and image_paths:
            for p in image_paths:
                try:
                    Path(str(p)).unlink(missing_ok=True)
                except Exception:
                    pass
        # Cleanup temporary uploaded PDF (file-mode only), only under downloads/.
        if pdf_path:
            try:
                pp = Path(pdf_path)
                if _is_under_dir(pp, _DOWNLOAD_DIR) and pp.exists():
                    pp.unlink(missing_ok=True)
            except Exception:
                pass
        # Cleanup temporary uploaded PPTX (file-mode only), only under downloads/.
        if pptx_path:
            try:
                pp = Path(pptx_path)
                if _is_under_dir(pp, _DOWNLOAD_DIR) and pp.exists():
                    pp.unlink(missing_ok=True)
            except Exception:
                pass
        await state.clear()



@router.callback_query(F.data.startswith("ai_diff:"))
async def ai_choose_difficulty(call: types.CallbackQuery, state: FSMContext) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    session_id, diff = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    if diff not in {"easy", "medium", "hard", "mixed"}:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await call.answer()
    await state.update_data(ai_difficulty=diff)

    max_n = int(data.get("ai_max_questions") or 50)
    max_n = max(1, min(50, max_n))
    if call.message:
        await _continue_after_topic_settings(call.message, state, ui_lang=ui_lang, user_id=call.from_user.id, max_n=max_n)


async def _start_ai_quiz_after_language(bot: Bot, state: FSMContext, *, chat_id: int, user: types.User, output_language: str) -> None:
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(int(getattr(user, "id", 0) or 0))

    if _should_offer_ai_shuffle(data):
        session_id = str(data.get("ai_session_id") or "")
        await state.update_data(ai_output_language=output_language)
        await state.set_state(AIQuizStates.choose_shuffle)
        await bot.send_message(chat_id, t(ui_lang, "shuffle_prompt_ai"), reply_markup=_kb_ai_shuffle(session_id, ui_lang=ui_lang))
        return

    await _start_ai_quiz(bot, state, chat_id=chat_id, user=user, output_language=output_language)


@router.callback_query(F.data.startswith("ai_translate:"))
async def ai_translate(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, action = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    if action == "choose":
        await call.answer()
        await state.set_state(AIQuizStates.choose_lang)
        if call.message:
            await call.message.answer(t(ui_lang, "choose_translation_lang"), reply_markup=_kb_langs(session_id, ui_lang=ui_lang))
        return

    if action == "default":
        await call.answer()
        settings = await get_or_create_user_settings(user_id=call.from_user.id)
        default_lang = str(settings.get("default_lang") or "source").strip().lower()
        if default_lang in {"uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
            await _start_ai_quiz_after_language(
                bot,
                state,
                chat_id=call.message.chat.id if call.message else int(data.get("ai_chat_id") or 0),
                user=call.from_user,
                output_language=default_lang,
            )
            return
        await state.set_state(AIQuizStates.choose_lang)
        if call.message:
            await call.message.answer(t(ui_lang, "choose_translation_lang"), reply_markup=_kb_langs(session_id, ui_lang=ui_lang))
        return

    if action == "source":
        await call.answer()
        await _start_ai_quiz_after_language(
            bot,
            state,
            chat_id=call.message.chat.id if call.message else 0,
            user=call.from_user,
            output_language="source",
        )
        return

    await call.answer(t(ui_lang, "invalid_button"), show_alert=True)


@router.callback_query(F.data.startswith("ai_lang:"))
async def ai_choose_lang(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, lang = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    if lang not in {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    await call.answer()

    await _start_ai_quiz_after_language(
        bot,
        state,
        chat_id=int(data.get("ai_chat_id") or call.message.chat.id if call.message else 0),
        user=call.from_user,
        output_language=lang,
    )


@router.callback_query(F.data.startswith("ai_shuffle:"))
async def ai_choose_shuffle(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, choice = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return
    if choice not in {"questions", "options", "both", "none"}:
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return

    await call.answer()
    output_language = str(data.get("ai_output_language") or "source").strip().lower() or "source"
    if output_language not in {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        output_language = "source"

    if choice == "none":
        await state.update_data(ai_shuffle_mode="none", ai_shuffle_strategy="saved")
        await _start_ai_quiz(
            bot,
            state,
            chat_id=int(data.get("ai_chat_id") or call.message.chat.id if call.message else 0),
            user=call.from_user,
            output_language=output_language,
        )
        return

    await state.update_data(ai_shuffle_mode=choice, ai_output_language=output_language)
    await state.set_state(AIQuizStates.choose_shuffle_strategy)
    if call.message:
        await call.message.answer(
            t(ui_lang, "shuffle_strategy_prompt"),
            reply_markup=_kb_ai_shuffle_strategy(session_id, mode=choice, ui_lang=ui_lang),
        )


@router.callback_query(F.data.startswith("ai_shuffle_strategy:"))
async def ai_choose_shuffle_strategy(call: types.CallbackQuery, state: FSMContext, bot: Bot) -> None:
    parts = call.data.split(":")
    if len(parts) != 3:
        ui_lang = await _get_ui_lang(call.from_user.id)
        await call.answer(t(ui_lang, "invalid_button"), show_alert=True)
        return
    session_id, strategy = parts[1], parts[2]
    data = await state.get_data()
    ui_lang = norm_ui_lang(str(data.get("ai_ui_lang") or ""))
    if not data.get("ai_ui_lang"):
        ui_lang = await _get_ui_lang(call.from_user.id)
    if data.get("ai_session_id") != session_id:
        await call.answer(t(ui_lang, "session_missing"), show_alert=True)
        return
    if data.get("ai_user_id") != call.from_user.id:
        await call.answer(t(ui_lang, "session_owner_only"), show_alert=True)
        return

    await call.answer()
    output_language = str(data.get("ai_output_language") or "source").strip().lower() or "source"
    if output_language not in {"source", "uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        output_language = "source"
    await state.update_data(ai_shuffle_strategy=_normalize_shuffle_strategy(strategy))
    await _start_ai_quiz(
        bot,
        state,
        chat_id=int(data.get("ai_chat_id") or call.message.chat.id if call.message else 0),
        user=call.from_user,
        output_language=output_language,
    )


@router.message(F.photo)
async def on_photo_upload(message: types.Message, bot: Bot, state: FSMContext) -> None:
    if not await _ensure_subscribed(message, bot, message.from_user.id if message.from_user else 0):
        return
    # Only handle photo uploads when user is not inside another FSM flow (manual/AI/etc).
    st = await state.get_state()
    if st != UploadStates.await_file.state:
        return

    user_id = message.from_user.id if message.from_user else 0
    ui_lang = await _get_ui_lang(user_id)

    if not AI_ENABLED:
        await message.answer(t(ui_lang, "upload_hint_noai"))
        return

    try:
        provider = ai_service._pick_provider()
    except AIServiceError as exc:
        await message.answer(t(ui_lang, "err_ai", err=str(exc)))
        return
    if provider not in {"openai", "gemini"}:
        await message.answer(t(ui_lang, "scan_pdf_need_gemini"))
        return

    photo = message.photo[-1] if message.photo else None
    if not photo:
        return

    max_mb = int(os.getenv("MAX_UPLOAD_MB", "5") or 5)
    if getattr(photo, "file_size", None) and int(photo.file_size) > max_mb * 1024 * 1024:
        await message.answer(t(ui_lang, "file_too_large", mb=max_mb))
        return

    await state.clear()
    status = await message.answer(t(ui_lang, "file_received_downloading"))
    local_path: Path | None = None
    try:
        tg_file = await bot.get_file(photo.file_id)
        _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        local_path = _DOWNLOAD_DIR / f"{uuid.uuid4().hex}.jpg"
        await bot.download_file(tg_file.file_path, str(local_path))

        title = (message.caption or "").strip()
        if not title:
            title = "Image Quiz"

        session_id = uuid.uuid4().hex
        await state.update_data(
            ai_session_id=session_id,
            ai_mode="image",
            ai_difficulty="",
            ai_ui_lang=ui_lang,
            ai_image_paths=[str(local_path)],
            ai_max_questions=1,
            ai_pages_total=1,
            ai_title=title[:120],
            ai_chat_id=message.chat.id,
            ai_chat_type=message.chat.type,
            ai_user_id=user_id,
            ai_question_count=1,
        )
        await state.set_state(AIQuizStates.choose_time)
        await status.edit_text(t(ui_lang, "choose_time"), reply_markup=_kb_ai_time_presets(session_id, ui_lang=ui_lang))
    except Exception as exc:
        await status.edit_text(t(ui_lang, "err_unexpected", err=str(exc)))
        if local_path is not None:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass
        await state.clear()


@router.message(F.document)
async def on_document(message: types.Message, bot: Bot, state: FSMContext) -> None:
    if not await _ensure_subscribed(message, bot, message.from_user.id if message.from_user else 0):
        return
    # Ignore documents while user is in another FSM flow (manual quiz, premium, etc).
    st = await state.get_state()
    if st != UploadStates.await_file.state:
        return

    doc = message.document
    if doc is None:
        return

    ui_lang = await _get_ui_lang(message.from_user.id if message.from_user else 0)
    file_name = doc.file_name or "file"
    suffix = Path(file_name).suffix.lower()
    if suffix not in _ALLOWED_SUFFIXES:
        await message.answer(t(ui_lang, "file_type_only"))
        return

    max_mb = _max_upload_mb_for_suffix(suffix)
    if doc.file_size and int(doc.file_size) > max_mb * 1024 * 1024:
        await message.answer(t(ui_lang, "file_too_large", mb=max_mb))
        return

    await state.clear()
    status = await message.answer(t(ui_lang, "file_received_downloading"))
    local_path: Path | None = None
    cleanup_local = True

    try:
        tg_file = await bot.get_file(doc.file_id)
        _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        local_path = _DOWNLOAD_DIR / f"{uuid.uuid4().hex}_{_safe_filename(file_name)}"
        await bot.download_file(tg_file.file_path, str(local_path))

        caption = (message.caption or "").strip()

        # Image upload (.jpg/.png): handled via vision (OpenAI/Gemini).
        if suffix in {".png", ".jpg", ".jpeg", ".webp"}:
            if not AI_ENABLED:
                await status.edit_text(t(ui_lang, "upload_hint_noai"))
                return
            try:
                provider = ai_service._pick_provider()
            except AIServiceError as exc:
                await status.edit_text(t(ui_lang, "err_ai", err=str(exc)))
                return
            if provider not in {"openai", "gemini"}:
                await status.edit_text(t(ui_lang, "scan_pdf_need_gemini"))
                return

            session_id = uuid.uuid4().hex
            await state.update_data(
                ai_session_id=session_id,
                ai_mode="image",
                ai_difficulty="",
                ai_ui_lang=ui_lang,
                ai_image_paths=[str(local_path)],
                ai_max_questions=1,
                ai_pages_total=1,
                ai_title=Path(file_name).stem,
                ai_chat_id=message.chat.id,
                ai_chat_type=message.chat.type,
                ai_user_id=message.from_user.id if message.from_user else 0,
                ai_question_count=1,
            )
            cleanup_local = False
            await state.set_state(AIQuizStates.choose_time)
            await status.edit_text(t(ui_lang, "choose_time"), reply_markup=_kb_ai_time_presets(session_id, ui_lang=ui_lang))
            return

        # Import mode: JSON is always treated as a ready quiz file.
        # In NO-AI mode we import from prepared files instead of generating via AI.
        if (not AI_ENABLED) or suffix == ".json":
            title = Path(file_name).stem
            open_period = 30

            if caption:
                for line in caption.splitlines():
                    m = re.match(r"(?is)^\s*(?:title|nom)\s*:\s*(.+)$", line.strip())
                    if m and (m.group(1) or "").strip():
                        title = (m.group(1) or "").strip()[:120]
                        break

                m = re.search(r"(?i)\b(?:time|sec|sek|sekund|soniya)\s*[:\-]\s*(\d{1,3})\b", caption)
                if m and m.group(1).isdigit():
                    open_period = int(m.group(1))
                else:
                    m = _TIME_HINT_RE.search(caption)
                    if m and m.group(1).isdigit():
                        open_period = int(m.group(1))
                open_period = max(5, min(600, int(open_period or 30)))

            await status.edit_text(t(ui_lang, "extracting_text"))
            raw = local_path.read_text(encoding="utf-8", errors="ignore") if suffix == ".json" else await asyncio.to_thread(extract_text_from_file, str(local_path))

            session_id = uuid.uuid4().hex
            await state.update_data(
                ai_session_id=session_id,
                ai_mode="file",
                ai_difficulty="",
                ai_ui_lang=ui_lang,
                ai_text=raw[:120000],
                ai_title=title,
                ai_open_period=open_period,
                ai_chat_id=message.chat.id,
                ai_chat_type=message.chat.type,
                ai_user_id=message.from_user.id if message.from_user else 0,
                ai_pages_total=1,
                ai_pages_return="count",
                ai_pages_required=True,
                ai_import_only=True,
            )
            await state.set_state(AIQuizStates.choose_pages)
            await status.edit_text(
                t(ui_lang, "pages_prompt", total=1),
                reply_markup=_kb_page_presets(session_id, 1, ui_lang=ui_lang),
            )
            return

        topic = ""
        if caption:
            m = re.match(r"(?is)^(?:mavzu|topic|focus)\\s*:\\s*(.+)$", caption)
            if m:
                topic = (m.group(1) or "").strip()
            elif len(caption) <= 120:
                topic = caption

        # PDF/PPTX: sahifa oralig'ini majburiy tanlatamiz, shunda butun faylni ajratib/tahlil qilib vaqt ketmaydi.
        if suffix in {".pdf", ".pptx"}:
            pages_total = 0
            try:
                if suffix == ".pdf":
                    pages_total = await asyncio.to_thread(_pdf_page_count, local_path)
                else:
                    pages_total = await asyncio.to_thread(_pptx_slide_count, local_path)
            except Exception:
                pages_total = 0

            session_id = uuid.uuid4().hex
            data_out: Dict[str, Any] = dict(
                ai_session_id=session_id,
                ai_mode="file",
                ai_difficulty="",
                ai_ui_lang=ui_lang,
                ai_text="",
                ai_title=Path(file_name).stem,
                ai_chat_id=message.chat.id,
                ai_chat_type=message.chat.type,
                ai_user_id=message.from_user.id if message.from_user else 0,
                ai_topic=topic,
                ai_pages_total=int(pages_total or 0),
                ai_pages_return="count",
                ai_pages_required=True,
            )
            if suffix == ".pdf":
                data_out["ai_pdf_path"] = str(local_path)
            else:
                data_out["ai_pptx_path"] = str(local_path)

            await state.update_data(**data_out)
            await state.set_state(AIQuizStates.choose_pages)
            await status.edit_text(
                t(ui_lang, "pages_prompt", total=int(pages_total or 0)),
                reply_markup=_kb_page_presets(session_id, int(pages_total or 0), ui_lang=ui_lang),
            )
            cleanup_local = False
            return

        await status.edit_text(t(ui_lang, "extracting_text"))
        text = await asyncio.to_thread(extract_text_from_file, str(local_path))

        session_id = uuid.uuid4().hex
        await state.update_data(
            ai_session_id=session_id,
            ai_mode="file",
            ai_difficulty="",
            ai_ui_lang=ui_lang,
            ai_text=text[:120000],
            ai_title=Path(file_name).stem,
            ai_chat_id=message.chat.id,
            ai_chat_type=message.chat.type,
            ai_user_id=message.from_user.id if message.from_user else 0,
            ai_topic=topic,
            ai_pages_total=1,
            ai_pages_return="count",
            ai_pages_required=True,
        )
        await state.set_state(AIQuizStates.choose_pages)
        await status.edit_text(
            t(ui_lang, "pages_prompt", total=1),
            reply_markup=_kb_page_presets(session_id, 1, ui_lang=ui_lang),
        )

    except AIServiceError as exc:
        await status.edit_text(t(ui_lang, "err_ai", err=str(exc)))
    except Exception as exc:
        await status.edit_text(t(ui_lang, "err_unexpected", err=str(exc)))
    finally:
        if local_path is not None and cleanup_local:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass








