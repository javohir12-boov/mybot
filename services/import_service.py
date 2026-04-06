import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class ImportServiceError(RuntimeError):
    pass


_Q_START_RE = re.compile(r"^\s*(?:Q(?:uestion)?\s*)?(\d{1,3})\s*[).:\-]\s*(.+?)\s*$", re.IGNORECASE)
_OPT_RE = re.compile(r"^\s*(?:[-•●]\s*)?([A-Da-dАВСДа-всд])\s*[).:\-]\s*(.+?)\s*$")
_ANSWER_RE = re.compile(
    r"^\s*(?:answer|ans|javob|to'g'ri\s*j?avob|correct(?:\s*answer)?|ответ|правильн(?:ый|ая)\s*ответ)\s*(?:[:\-=]|\s)\s*([A-Da-dАВСДа-всд]|[1-4])\b.*$",
    re.IGNORECASE,
)
_EXPL_RE = re.compile(r"^\s*(?:explanation|izoh)\s*[:\-]\s*(.+?)\s*$", re.IGNORECASE)

_ANSWER_SECTION_RE = re.compile(r"(?i)^\s*(?:answers?|answer\s*key|javoblar|javoblari|kalit|ответы)\b")
_ANSWER_PAIR_RE = re.compile(r"(?i)\b(\d{1,3})\s*[).:\-]\s*([A-Da-dАВСДа-всд]|[1-4])\b")
_ANSWER_PAIR_COMPACT_RE = re.compile(r"(?i)\b(\d{1,3})\s*([A-Da-dАВСДа-всд])\b")

_CYR_MAP = {
    "А": "A",
    "В": "B",
    "С": "C",
    "Д": "D",
    "а": "A",
    "в": "B",
    "с": "C",
    "д": "D",
}


def _answer_to_index(token: str) -> Optional[int]:
    t = (token or "").strip()
    if not t:
        return None
    t = re.sub(r"[\s\(\)\[\]\{\}\.\-:]+", "", t)
    if t in _CYR_MAP:
        t = _CYR_MAP[t]
    if t.isdigit():
        n = int(t)
        if 1 <= n <= 4:
            return n - 1
        return None
    c = t.upper()[0]
    c = _CYR_MAP.get(c, c)
    if c in {"A", "B", "C", "D"}:
        return ord(c) - ord("A")
    return None


def _opt_key(token: str) -> Optional[str]:
    t = (token or "").strip()
    if not t:
        return None
    t = re.sub(r"[\s\(\)\[\]\{\}\.\-:]+", "", t)
    if t in _CYR_MAP:
        t = _CYR_MAP[t]
    c = (t.upper() or "")[:1]
    c = _CYR_MAP.get(c, c)
    if c in {"A", "B", "C", "D"}:
        return c
    return None


def _strip_correct_marker(text: str) -> tuple[str, bool]:
    """Detect simple 'correct option' markers inside option text."""
    v = str(text or "").strip()
    if not v:
        return v, False

    marked = False
    # Leading markers: "*", "✔", "✅"
    while v and v[0] in {"*", "✔", "✅", "✓"}:
        marked = True
        v = v[1:].lstrip()
    # Trailing marker: "*"
    if v.endswith("*") and len(v) > 1:
        marked = True
        v = v[:-1].rstrip()

    # Parenthetical words (keep it conservative).
    if re.search(r"(?i)\b(correct|to['’]g['’]ri|правильн)\b", v):
        marked = True
        v = re.sub(r"(?i)\b(correct|to['’]g['’]ri|правильн(?:ый|ая)?)\b", "", v).strip(" -:()")

    return v, marked


def _normalize_question(q: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    text = str(q.get("question") or q.get("text") or "").strip()
    options = q.get("options") or q.get("variants") or []
    if isinstance(options, str):
        # Allow "A|B|C|D" style.
        options = [x.strip() for x in options.split("|") if x.strip()]
    if not text or not isinstance(options, list) or len(options) != 4:
        return None
    options = [str(x).strip() for x in options]
    if any(not x for x in options):
        return None

    correct_index = q.get("correct_index")
    if correct_index is None:
        correct_index = q.get("correct_answer")
    if correct_index is None:
        correct_index = q.get("answer")
    if isinstance(correct_index, str):
        ci = _answer_to_index(correct_index)
    else:
        try:
            ci = int(correct_index)
        except Exception:
            ci = None
    if ci is None or ci < 0 or ci > 3:
        return None

    explanation = str(q.get("explanation") or q.get("comment") or "").strip()
    return {
        "question": text,
        "options": options,
        "correct_index": int(ci),
        "explanation": explanation,
    }


def parse_quiz_from_json(data: Any, *, title_fallback: str = "") -> Tuple[str, List[Dict[str, Any]]]:
    title = title_fallback
    questions_raw: Any = None

    if isinstance(data, dict):
        if isinstance(data.get("title"), str) and data.get("title").strip():
            title = data.get("title").strip()
        questions_raw = data.get("quiz") if data.get("quiz") is not None else data.get("questions")
        if questions_raw is None and isinstance(data.get("items"), list):
            questions_raw = data.get("items")
    elif isinstance(data, list):
        questions_raw = data
    else:
        raise ImportServiceError("JSON format not supported")

    if not isinstance(questions_raw, list):
        raise ImportServiceError("JSON must contain a list of questions under 'quiz' or 'questions'")

    out: List[Dict[str, Any]] = []
    for q in questions_raw:
        if not isinstance(q, dict):
            continue
        nq = _normalize_question(q)
        if nq:
            out.append(nq)

    return title, out


def parse_quiz_from_text(text: str) -> List[Dict[str, Any]]:
    lines = (text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out_raw: List[Dict[str, Any]] = []

    cur_num: Optional[int] = None
    cur_q: Optional[str] = None
    opts: Dict[str, str] = {}
    correct: Optional[int] = None
    expl: str = ""
    last_opt: Optional[str] = None

    def _flush() -> None:
        nonlocal cur_num, cur_q, opts, correct, expl, last_opt
        if not cur_q:
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            return
        if len(opts) != 4:
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            return
        ordered = [opts.get("A", ""), opts.get("B", ""), opts.get("C", ""), opts.get("D", "")]
        if any(not x for x in ordered):
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            return
        out_raw.append(
            {
                "number": cur_num,
                "question": cur_q.strip(),
                "options": [x.strip() for x in ordered],
                "correct_index": int(correct) if correct is not None else None,
                "explanation": (expl or "").strip(),
            }
        )
        cur_num = None
        cur_q = None
        opts = {}
        correct = None
        expl = ""
        last_opt = None

    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue

        # Common "answers" section header at the end of documents. Prevents it from
        # being appended to the last option when text wrapping is messy.
        if _ANSWER_SECTION_RE.search(line):
            _flush()
            continue

        m_q = _Q_START_RE.match(line)
        if m_q:
            _flush()
            try:
                cur_num = int(m_q.group(1))
            except Exception:
                cur_num = None
            cur_q = (m_q.group(2) or "").strip()
            continue

        m_opt = _OPT_RE.match(line)
        if m_opt and cur_q:
            key = _opt_key(m_opt.group(1) or "")
            val_raw = (m_opt.group(2) or "").strip()
            val, marked = _strip_correct_marker(val_raw)
            if key and val:
                if key in opts:
                    opts[key] = (opts[key] + " " + val).strip()
                else:
                    opts[key] = val
                last_opt = key
                if marked and correct is None:
                    correct = ord(key) - ord("A")
            continue

        m_ans = _ANSWER_RE.match(line)
        if m_ans and cur_q:
            correct = _answer_to_index(m_ans.group(1) or "")
            continue

        m_ex = _EXPL_RE.match(line)
        if m_ex and cur_q:
            expl = (m_ex.group(1) or "").strip()
            continue

        # Multi-line question text before options start.
        if cur_q and not opts:
            cur_q = (cur_q + " " + line).strip()
            continue

        # Multi-line option text (PDF/DOCX extraction often wraps).
        if cur_q and opts and last_opt:
            opts[last_opt] = (opts.get(last_opt, "") + " " + line).strip()

    _flush()

    def _extract_answer_key() -> Dict[int, int]:
        mapping: Dict[int, int] = {}
        in_section = False
        for raw2 in lines:
            l2 = (raw2 or "").strip()
            if not l2:
                continue
            if _ANSWER_SECTION_RE.search(l2):
                in_section = True
            if not in_section:
                continue
            for m in _ANSWER_PAIR_RE.finditer(l2):
                num_s, tok = m.group(1), m.group(2)
                if not (num_s or "").isdigit():
                    continue
                idx = _answer_to_index(tok or "")
                if idx is None:
                    continue
                mapping[int(num_s)] = int(idx)
            for m in _ANSWER_PAIR_COMPACT_RE.finditer(l2):
                num_s, tok = m.group(1), m.group(2)
                if not (num_s or "").isdigit():
                    continue
                idx = _answer_to_index(tok or "")
                if idx is None:
                    continue
                mapping[int(num_s)] = int(idx)
        return mapping

    answer_key = _extract_answer_key()
    if not answer_key:
        # Heuristic: if a line contains many (num, answer) pairs, treat it as an answer key line.
        for raw2 in lines:
            l2 = (raw2 or "").strip()
            if not l2:
                continue
            pairs = list(_ANSWER_PAIR_RE.finditer(l2))
            compact = list(_ANSWER_PAIR_COMPACT_RE.finditer(l2))
            if len(pairs) >= 2 or len(compact) >= 3:
                for m in pairs:
                    if (m.group(1) or "").isdigit():
                        idx = _answer_to_index(m.group(2) or "")
                        if idx is not None:
                            answer_key[int(m.group(1))] = int(idx)
                for m in compact:
                    if (m.group(1) or "").isdigit():
                        idx = _answer_to_index(m.group(2) or "")
                        if idx is not None:
                            answer_key[int(m.group(1))] = int(idx)

    out: List[Dict[str, Any]] = []
    for q in out_raw:
        ci = q.get("correct_index")
        if ci is None:
            num = q.get("number")
            if isinstance(num, int) and num in answer_key:
                q["correct_index"] = int(answer_key[num])
                ci = q["correct_index"]
        if ci is None:
            continue
        q.pop("number", None)
        out.append(
            {
                "question": str(q.get("question") or "").strip(),
                "options": list(q.get("options") or [])[:4],
                "correct_index": int(q.get("correct_index") or 0),
                "explanation": str(q.get("explanation") or "").strip(),
            }
        )

    return out


def parse_quiz_payload(raw_text: str, *, title_fallback: str = "") -> Tuple[str, List[Dict[str, Any]]]:
    raw = (raw_text or "").strip()
    if not raw:
        return title_fallback, []

    # Try JSON first when it looks like JSON.
    if raw[:1] in {"{", "["}:
        try:
            data = json.loads(raw)
            return parse_quiz_from_json(data, title_fallback=title_fallback)
        except Exception:
            pass

    questions = parse_quiz_from_text(raw)
    return title_fallback, questions


def import_format_example() -> str:
    return (
        "TXT format namunasi:\n"
        "1) Savol matni?\n"
        "A) Variant A\n"
        "B) Variant B\n"
        "C) Variant C\n"
        "D) Variant D\n"
        "Answer: B\n"
        "Explanation: qisqa izoh (ixtiyoriy)\n\n"
        "2) Keyingi savol?\n"
        "A) ...\n"
        "B) ...\n"
        "C) ...\n"
        "D) ...\n"
        "Answer: 1\n\n"
        "Yoki javoblar kaliti alohida bo'lishi mumkin:\n"
        "Answers:\n"
        "1 - B\n"
        "2 - 1"
    )
