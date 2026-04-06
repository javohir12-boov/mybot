from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import GEMINI_API_KEY


def main() -> int:
    try:
        import google.generativeai as genai
    except ImportError:
        print("google-generativeai not installed. Run: pip install google-generativeai")
        return 2

    if not (GEMINI_API_KEY or "").strip():
        print("GEMINI_API_KEY is not set in .env")
        return 2

    genai.configure(api_key=GEMINI_API_KEY)
    try:
        models = list(genai.list_models())
    except Exception as exc:
        print(f"ListModels failed: {exc}")
        return 1

    usable = []
    for m in models:
        methods = getattr(m, "supported_generation_methods", None) or []
        if "generateContent" in methods:
            usable.append(getattr(m, "name", str(m)))

    if not usable:
        print("No models found with generateContent")
        return 1

    for name in usable:
        print(name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
