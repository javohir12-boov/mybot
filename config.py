import os
from pathlib import Path
from typing import Any, Dict

# --- tiny .env loader ---------------------------------
def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    # Do not override environment variables already set outside of the file,
    # but within the .env file let later lines override earlier ones (last wins).
    preexisting = set(os.environ.keys())
    text = dotenv_path.read_text(encoding="utf-8")
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        # Common placeholders should not overwrite real values.
        if val.strip().upper() in {
            "PASTE_YOUR_KEY_HERE",
            "PASTE_YOUR_TOKEN_HERE",
            "YOUR_KEY_HERE",
            "YOUR_TOKEN_HERE",
        }:
            continue
        if not key:
            continue
        if key in preexisting:
            # If an env var exists but is empty/blank, allow .env to fill it.
            existing = str(os.environ.get(key, "") or "")
            if existing.strip():
                continue
        os.environ[key] = val

_ROOT = Path(__file__).parent
_load_dotenv(_ROOT / ".env")

def _get_env(name: str, default: Any = None, required: bool = False) -> Any:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Required config environment variable not set: {name}")
    return val

# --- configuration values ----------------------------------------
# Telegram bot token
BOT_TOKEN = _get_env("BOT_TOKEN", required=True)

# AI keys (at least one should be set if you want AI quiz generation)
GEMINI_API_KEY = _get_env("GEMINI_API_KEY")
OPENAI_API_KEY = _get_env("OPENAI_API_KEY")
CLAUDE_API_KEY = _get_env("CLAUDE_API_KEY") or _get_env("ANTHROPIC_API_KEY")

# AI provider/model configuration
# AI_PROVIDER: "auto" | "gemini" | "openai" | "claude"
AI_PROVIDER = str(_get_env("AI_PROVIDER", "auto")).strip().lower()
OPENAI_MODEL = _get_env("OPENAI_MODEL", "gpt-4o-mini")
# NOTE: gemini-1.5-flash has been removed for many accounts. Use a "latest" alias by default.
# You can paste either "models/..." or the plain name into .env (see scripts/list_gemini_models.py).
GEMINI_MODEL = _get_env("GEMINI_MODEL", "gemini-flash-latest")
CLAUDE_MODEL = _get_env("CLAUDE_MODEL", "claude-haiku-4-5-20251001")

# Bot mode
# BOT_MODE: "ai" (default) | "noai"
BOT_MODE = str(_get_env("BOT_MODE", "ai")).strip().lower()
if BOT_MODE not in {"ai", "noai"}:
    BOT_MODE = "ai"
AI_ENABLED = BOT_MODE != "noai"

# MA'LUMOTLAR BAZASI MANZILI
# Railway/Render/Heroku DATABASE_URL ni quyidagi ko'rinishda berishi mumkin:
#   postgres://...  yoki  postgresql://...
# SQLAlchemy async esa postgresql+asyncpg:// formatini kutadi.
_DATABASE_URL_RAW = _get_env("DATABASE_URL", "sqlite+aiosqlite:///quiz_bot.db")
_db = str(_DATABASE_URL_RAW or '').strip()
# Faqat scheme qismini (://gacha) tekshiramiz — parol yoki query string'da
# bo'lishi mumkin bo'lgan '+' belgisi noto'g'ri o'zgartirishga olib kelmasligi uchun.
_scheme_sep = _db.find('://')
_scheme = _db[:_scheme_sep].lower() if _scheme_sep > 0 else ''
_rest = _db[_scheme_sep + 3:] if _scheme_sep > 0 else ''
if _scheme == 'postgres' or _scheme == 'postgresql':
    _db = 'postgresql+asyncpg://' + _rest
# Agar foydalanuvchi 'postgresql+psycopg2://' yoki boshqa sync driver ishlatgan bo'lsa,
# uni ham asyncpg'ga aylantiramiz (chunki kodimiz async engine ishlatadi).
elif _scheme.startswith('postgresql+') and not _scheme.endswith('+asyncpg'):
    _db = 'postgresql+asyncpg://' + _rest
DATABASE_URL = _db or 'sqlite+aiosqlite:///quiz_bot.db'

# Print which DB scheme we're using (mask credentials) — helps debug Railway/Render setup.
def _mask_db_url(u: str) -> str:
    try:
        if '://' not in u:
            return u
        scheme, rest = u.split('://', 1)
        # If the URL contains another scheme inside (e.g. ".../railwaypostgresql://..."),
        # the env var has TWO URLs concatenated — flag this loudly.
        if '://' in rest:
            return f"!!! MALFORMED — TWO URLs concatenated in DATABASE_URL: {scheme}://...{rest[:60]}..."
        # Multiple '@' usually means embedded credentials in the password part — also suspicious.
        if rest.count('@') > 1:
            return f"!!! SUSPICIOUS — multiple '@' in DATABASE_URL ({rest.count('@')} found). Possibly duplicated URL."
        if '@' in rest:
            _, hostpart = rest.split('@', 1)
            return f"{scheme}://****@{hostpart}"
        return u
    except Exception:
        return "<unprintable>"

import sys as _sys
print(f"[config] DATABASE_URL = {_mask_db_url(DATABASE_URL)}", file=_sys.stderr)

SQL_ECHO = str(_get_env("SQL_ECHO", "0")).strip().lower() in {"1", "true", "yes", "y", "on"}

SQL_PASSWORD = _get_env("SQL_PASSWORD", "0000")

_admin_raw = _get_env("ADMIN_IDS", "123456789,987654321")
ADMIN_IDS = [int(x.strip()) for x in str(_admin_raw).split(",") if x.strip()]

# Required channel subscription (optional). Users must join this channel to use the bot.
_req_ch = str(_get_env("REQUIRED_CHANNEL", "@CreateQuiz_news") or "").strip()
if _req_ch.startswith("https://t.me/"):
    _req_ch = "@" + _req_ch.rsplit("/", 1)[-1].split("?", 1)[0]
elif _req_ch.startswith("t.me/"):
    _req_ch = "@" + _req_ch.rsplit("/", 1)[-1].split("?", 1)[0]
elif _req_ch and not _req_ch.startswith("@") and not str(_req_ch).lstrip('-').isdigit():
    _req_ch = "@" + _req_ch.lstrip("@")
REQUIRED_CHANNEL = _req_ch
REQUIRED_CHANNEL_ENABLED = str(_get_env("REQUIRED_CHANNEL_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "y", "on"}


THROTTLED_USERS: Dict[int, float] = {}

# --- Webhook (optional) ------------------------------------------
# If WEBHOOK_URL is set (https://...), main.py will run webhook mode instead of polling.
WEBHOOK_URL = str(_get_env("WEBHOOK_URL", "") or "").strip()
WEBHOOK_PATH = str(_get_env("WEBHOOK_PATH", "/webhook") or "/webhook").strip() or "/webhook"
WEBHOOK_SECRET_TOKEN = str(_get_env("WEBHOOK_SECRET_TOKEN", "") or "").strip()
WEB_SERVER_HOST = str(_get_env("WEB_SERVER_HOST", "0.0.0.0") or "0.0.0.0").strip() or "0.0.0.0"
_port_raw = str(os.getenv('WEB_SERVER_PORT') or os.getenv('PORT') or '8080').strip()
try:
    WEB_SERVER_PORT = int(_port_raw or 8080)
except Exception:
    WEB_SERVER_PORT = 8080

# Text shown on /start. Put your own text into .env as ABOUT_TEXT="..."
ABOUT_TEXT = _get_env(
    "ABOUT_TEXT",
    "Bu bot siz yuklagan fayl yoki elektron kitob asosida testlar tuzib beradi. "
    "Shuningdek, agar ma'lum bir mavzu bo'yicha testlar kerak bo'lsa, ularni ham yaratib beradi. "
    "Bundan tashqari, kitob yuklash orqali undagi mavzular asosida testlar tuzishda ham yordam beradi.",
)
ABOUT_TEXT = str(ABOUT_TEXT).replace("\\n", "\n")

# Optional per-UI-language about texts.
ABOUT_TEXT_UZ = str(_get_env("ABOUT_TEXT_UZ", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_RU = str(_get_env("ABOUT_TEXT_RU", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_EN = str(_get_env("ABOUT_TEXT_EN", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_DE = str(_get_env("ABOUT_TEXT_DE", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_TR = str(_get_env("ABOUT_TEXT_TR", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_KK = str(_get_env("ABOUT_TEXT_KK", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_AR = str(_get_env("ABOUT_TEXT_AR", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_ZH = str(_get_env("ABOUT_TEXT_ZH", "") or "").replace("\\n", "\n").strip()
ABOUT_TEXT_KO = str(_get_env("ABOUT_TEXT_KO", "") or "").replace("\\n", "\n").strip()


def get_about_text(ui_lang: str) -> str:
    lang = str(ui_lang or "").strip().lower()
    if lang == "ru" and ABOUT_TEXT_RU:
        return ABOUT_TEXT_RU
    if lang == "en" and ABOUT_TEXT_EN:
        return ABOUT_TEXT_EN
    if lang == "de" and ABOUT_TEXT_DE:
        return ABOUT_TEXT_DE
    if lang == "tr" and ABOUT_TEXT_TR:
        return ABOUT_TEXT_TR
    if lang == "kk" and ABOUT_TEXT_KK:
        return ABOUT_TEXT_KK
    if lang == "ar" and ABOUT_TEXT_AR:
        return ABOUT_TEXT_AR
    if lang == "zh" and ABOUT_TEXT_ZH:
        return ABOUT_TEXT_ZH
    if lang == "ko" and ABOUT_TEXT_KO:
        return ABOUT_TEXT_KO
    if lang == "uz" and ABOUT_TEXT_UZ:
        return ABOUT_TEXT_UZ
    return ABOUT_TEXT

# --- Export qilinadigan o'zgaruvchilar ro'yxati ---
__all__ = [
    "BOT_TOKEN",
    "GEMINI_API_KEY",
    "OPENAI_API_KEY",
    "CLAUDE_API_KEY",
    "AI_PROVIDER",
    "OPENAI_MODEL",
    "GEMINI_MODEL",
    "CLAUDE_MODEL",
    "BOT_MODE",
    "AI_ENABLED",
    "DATABASE_URL",
    "SQL_ECHO",
    "SQL_PASSWORD",
    "ADMIN_IDS",
    "REQUIRED_CHANNEL",
    "REQUIRED_CHANNEL_ENABLED",
    "THROTTLED_USERS",
    "WEBHOOK_URL",
    "WEBHOOK_PATH",
    "WEBHOOK_SECRET_TOKEN",
    "WEB_SERVER_HOST",
    "WEB_SERVER_PORT",
    "ABOUT_TEXT",
    "ABOUT_TEXT_UZ",
    "ABOUT_TEXT_RU",
    "ABOUT_TEXT_EN",
    "ABOUT_TEXT_DE",
    "ABOUT_TEXT_TR",
    "ABOUT_TEXT_KK",
    "ABOUT_TEXT_AR",
    "ABOUT_TEXT_ZH",
    "ABOUT_TEXT_KO",
    "get_about_text",
]

