import json
import random
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class ImportServiceError(RuntimeError):
    pass


_Q_START_RE = re.compile(r"^\s*(?:Q(?:uestion)?\s*)?(\d{1,4})\s*[).:\-\u2013\u2014]\s*(.+?)\s*$", re.IGNORECASE)
_Q_NUM_ONLY_RE = re.compile(r"^\s*(?:Q(?:uestion)?\s*)?(\d{1,4})\s*[).:\-\u2013\u2014]\s*$", re.IGNORECASE)
_QUESTION_ANY_RE = re.compile(
    r"^\s*(?:Q(?:uestion)?\s*|S:\s*|№\s*)?(\d{1,4})?\s*[).:\-\u2013\u2014]?\s*(.+?)\s*$",
    re.IGNORECASE,
)
_PLUS_MINUS_OPT_RE = re.compile(r"^\s*([+-])\s*[:.)\-]?\s*(.+?)\s*$")
_OPT_RE = re.compile(
    r"^\s*(?:[-*\u2022\u25CF\u25E6\u2013\u2014]\s*)?([A-Da-d\u0410\u0430\u0411\u0431\u0412\u0432\u0413\u0433\u0421\u0441\u0414\u0434]|[1-4])\s*[\).:\-\u2013\u2014]\s*(.+?)\s*$",
    re.IGNORECASE,
)
_OPT_KEY_ONLY_RE = re.compile(
    r"^\s*(?:[-*\u2022\u25CF\u25E6\u2013\u2014]\s*)?([A-Da-d\u0410\u0430\u0411\u0431\u0412\u0432\u0413\u0433\u0421\u0441\u0414\u0434]|[1-4])\s*[\).:\-\u2013\u2014]\s*$",
    re.IGNORECASE,
)
_ANSWER_RE = re.compile(
    r"^\s*(?:answer|ans|javob|to'g'ri\s*j?avob|correct(?:\s*answer)?|\u043E\u0442\u0432\u0435\u0442|\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D(?:\u044B\u0439|\u0430\u044F|\u043E\u0435|\u044B\u0435)?\s*\u043E\u0442\u0432\u0435\u0442)\s*(?:[:\-=]|\s)\s*([A-Da-d\u0410\u0430\u0411\u0431\u0412\u0432\u0413\u0433\u0421\u0441\u0414\u0434]|[1-4])\b.*$",
    re.IGNORECASE,
)
_EXPL_RE = re.compile(r"^\s*(?:explanation|izoh)\s*[:\-]\s*(.+?)\s*$", re.IGNORECASE)

_ANSWER_SECTION_RE = re.compile(r"(?i)^\s*(?:answers|answer\s*key|javoblar|javoblari|kalit|\u043E\u0442\u0432\u0435\u0442\u044B)\b")
_ANSWER_PAIR_RE = re.compile(
    r"(?i)\b(\d{1,4})\s*[\).:\-\u2013\u2014]\s*([A-Da-d\u0410\u0430\u0411\u0431\u0412\u0432\u0413\u0433\u0421\u0441\u0414\u0434]|[1-4])\b"
)
_ANSWER_PAIR_COMPACT_RE = re.compile(r"(?i)\b(\d{1,4})\s*([A-Da-d\u0410\u0430\u0411\u0431\u0412\u0432\u0413\u0433\u0421\u0441\u0414\u0434])\b")

_CYR_MAP = {
    "\u0410": "A",
    "\u0430": "A",
    "\u0411": "B",
    "\u0431": "B",
    # Russian option labels: А, Б, В, Г correspond to A, B, C, D.
    "\u0412": "C",
    "\u0432": "C",
    "\u0413": "D",
    "\u0433": "D",
    # Sometimes C/D are written with similar-looking Cyrillic letters.
    "\u0421": "C",
    "\u0441": "C",
    "\u0414": "D",
    "\u0434": "D",
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
    # Allow numeric option labels: 1..4 -> A..D
    if t.isdigit():
        try:
            n = int(t)
        except Exception:
            n = 0
        if 1 <= n <= 4:
            return chr(ord("A") + (n - 1))
    c = (t.upper() or "")[:1]
    c = _CYR_MAP.get(c, c)
    if c in {"A", "B", "C", "D"}:
        return c
    return None


def _strip_correct_marker(text: str) -> tuple[str, bool]:
    """Detect simple "correct option" markers inside option text."""
    v = str(text or "").strip()
    if not v:
        return v, False

    marked = False
    marker_tokens = (
        "*",
        "☑️",
        "✅",
        "✔",
        "✓",
        "☑",
        "???",
        "???",
        "???",
    )

    while v and any(v.startswith(m) for m in marker_tokens):
        marked = True
        for m in marker_tokens:
            if v.startswith(m):
                v = v[len(m):].lstrip()
                break

    while len(v) > 1 and any(v.endswith(m) for m in marker_tokens):
        marked = True
        for m in marker_tokens:
            if v.endswith(m):
                v = v[:-len(m)].rstrip()
                break

    if re.search(r"(?i)\b(correct|to['?]g['?]ri)\b", v):
        marked = True
        v = re.sub(r"(?i)\b(correct|to['?]g['?]ri)\b", "", v).strip(" -:()")

    return v, marked


def _shuffle_question_options(payload: Dict[str, Any]) -> Dict[str, Any]:
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


def _compact_ws(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _parse_quiz_table_rows(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    for raw in normalized.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if re.search(r"(?i)savollar|to'g'ri\s+javob|muqobil\s+javob", line):
            continue
        parts = [p.strip() for p in re.split(r"\t+|\s{3,}", line) if p.strip()]
        if len(parts) < 5:
            continue
        num = parts[0]
        if not re.fullmatch(r"\d{1,4}", num):
            continue
        question = _compact_ws(parts[1])
        options = [_compact_ws(p) for p in parts[2:6] if _compact_ws(p)]
        if question and len(options) >= 4:
            out.append(
                {
                    "question": question,
                    "options": options[:4],
                    "correct_index": 0,
                    "explanation": "",
                }
            )
    return out


def _parse_quiz_plus_minus(text: str) -> List[Dict[str, Any]]:
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    out: List[Dict[str, Any]] = []
    q_text = ""
    options: List[str] = []
    correct_index: Optional[int] = None

    def flush() -> None:
        nonlocal q_text, options, correct_index
        if q_text and len(options) == 4 and correct_index is not None:
            out.append(
                {
                    "question": _compact_ws(q_text),
                    "options": [_compact_ws(x) for x in options[:4]],
                    "correct_index": int(correct_index),
                    "explanation": "",
                }
            )
        q_text = ""
        options = []
        correct_index = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        m_q = _QUESTION_ANY_RE.match(line)
        if m_q and not _PLUS_MINUS_OPT_RE.match(line) and not _OPT_RE.match(line):
            lead = (m_q.group(2) or "").strip()
            if lead:
                flush()
                q_text = lead
                continue

        if line.lower().startswith('s:'):
            content = _compact_ws(line[2:])
            if content:
                q_text = _compact_ws((q_text + ' ' + content) if q_text else content)
            continue

        m_pm = _PLUS_MINUS_OPT_RE.match(line)
        if m_pm and q_text:
            sign = m_pm.group(1)
            value = _compact_ws(m_pm.group(2))
            if value:
                if len(options) < 4:
                    options.append(value)
                    if sign == '+':
                        correct_index = len(options) - 1
                else:
                    options[-1] = _compact_ws(options[-1] + ' ' + value)
            continue

        if q_text and options and not _QUESTION_ANY_RE.match(line):
            options[-1] = _compact_ws(options[-1] + ' ' + line)
            continue

        if q_text and not options:
            q_text = _compact_ws(q_text + ' ' + line)
            continue

    flush()
    return out



def _parse_quiz_unlabeled_blocks(text: str) -> List[Dict[str, Any]]:
    raw = (text or '')
    if _OPT_RE.search(raw) or _PLUS_MINUS_OPT_RE.search(raw):
        return []

    lines = raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: List[Dict[str, Any]] = []
    q_text = ''
    options: List[str] = []
    correct_index: Optional[int] = None
    pending_question_num = False

    def is_number_only(value: str) -> bool:
        token = str(value or '').strip()
        if not token:
            return False
        token = token.replace('?', '').strip()
        token = token.rstrip(').:-').strip()
        return token.isdigit()

    def flush() -> None:
        nonlocal q_text, options, correct_index, pending_question_num
        if q_text and len(options) == 4:
            out.append(
                {
                    'question': _compact_ws(q_text),
                    'options': [_compact_ws(x) for x in options[:4]],
                    'correct_index': int(correct_index) if correct_index is not None else 0,
                    'explanation': '',
                }
            )
        q_text = ''
        options = []
        correct_index = None
        pending_question_num = False

    for raw_line in lines:
        line = (raw_line or '').strip()
        if not line:
            continue

        if is_number_only(line):
            if q_text and options:
                flush()
            pending_question_num = True
            continue

        if _ANSWER_SECTION_RE.search(line):
            flush()
            continue

        if line.lower().startswith('s:'):
            content = _compact_ws(line[2:])
            if content:
                if q_text and options:
                    flush()
                q_text = content
                pending_question_num = False
            continue

        if pending_question_num and not q_text:
            q_text = _compact_ws(line)
            pending_question_num = False
            continue

        m_ans = _ANSWER_RE.match(line)
        if m_ans and q_text and len(options) == 4:
            idx = _answer_to_index(m_ans.group(1))
            if idx is not None:
                correct_index = idx
            continue

        if line.lower().startswith('javob:') and q_text and len(options) == 4:
            answer_text = _compact_ws(line.split(':', 1)[1])
            idx = _answer_to_index(answer_text)
            if idx is not None:
                correct_index = idx
            else:
                for i, opt in enumerate(options):
                    if _compact_ws(opt).lower() == answer_text.lower():
                        correct_index = i
                        break
            continue

        if not q_text:
            continue

        if len(options) < 4:
            clean, marked = _strip_correct_marker(line)
            options.append(_compact_ws(clean))
            if marked and correct_index is None:
                correct_index = len(options) - 1
            continue

        if options:
            options[-1] = _compact_ws(options[-1] + ' ' + line)

    flush()
    return out

def parse_quiz_from_text(text: str) -> List[Dict[str, Any]]:
    lines = (text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out_raw: List[Dict[str, Any]] = []

    cur_num: Optional[int] = None
    cur_q: Optional[str] = None
    opts: Dict[str, str] = {}
    correct: Optional[int] = None
    expl: str = ""
    last_opt: Optional[str] = None
    pending_question_num: Optional[int] = None
    pending_option_key: Optional[str] = None

    def _flush() -> None:
        nonlocal cur_num, cur_q, opts, correct, expl, last_opt, pending_question_num, pending_option_key
        if not cur_q:
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            pending_question_num = None
            pending_option_key = None
            return
        if len(opts) != 4:
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            pending_question_num = None
            pending_option_key = None
            return
        ordered = [opts.get("A", ""), opts.get("B", ""), opts.get("C", ""), opts.get("D", "")]
        if any(not x for x in ordered):
            cur_num = None
            cur_q = None
            opts = {}
            correct = None
            expl = ""
            last_opt = None
            pending_question_num = None
            pending_option_key = None
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
        pending_question_num = None
        pending_option_key = None

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

        m_q_only = _Q_NUM_ONLY_RE.match(line)
        if m_q_only:
            _flush()
            try:
                pending_question_num = int(m_q_only.group(1))
            except Exception:
                pending_question_num = None
            continue

        if pending_question_num is not None and not cur_q:
            cur_num = pending_question_num
            cur_q = line
            pending_question_num = None
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

        m_opt_only = _OPT_KEY_ONLY_RE.match(line)
        if m_opt_only and cur_q:
            pending_option_key = _opt_key(m_opt_only.group(1) or "")
            continue

        if pending_option_key and cur_q:
            val, marked = _strip_correct_marker(line)
            if val:
                if pending_option_key in opts:
                    opts[pending_option_key] = (opts[pending_option_key] + " " + val).strip()
                else:
                    opts[pending_option_key] = val
                last_opt = pending_option_key
                if marked and correct is None:
                    correct = ord(pending_option_key) - ord("A")
            pending_option_key = None
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
            val, marked = _strip_correct_marker(line)
            if val:
                opts[last_opt] = (opts.get(last_opt, "") + " " + val).strip()
            if marked and correct is None:
                correct = ord(last_opt) - ord("A")
            continue

        # Some documents place the checkmark on a separate line right after the option.
        if cur_q and last_opt and line in {"?", "?", "?", "?", "??", "???", "???", "???"}:
            if correct is None:
                correct = ord(last_opt) - ord("A")
            continue

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

    buckets: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for parser in (parse_quiz_from_text, _parse_quiz_plus_minus, _parse_quiz_table_rows, _parse_quiz_unlabeled_blocks):
        try:
            parsed = parser(raw)
        except Exception:
            parsed = []
        for q in parsed or []:
            question = _compact_ws(str(q.get("question") or ""))
            options: List[str] = []
            for raw_opt in list(q.get("options") or [])[:4]:
                opt = _compact_ws(raw_opt)
                m_pm = _PLUS_MINUS_OPT_RE.match(opt)
                if m_pm:
                    opt = _compact_ws(m_pm.group(2))
                opt, _ = _strip_correct_marker(opt)
                options.append(_compact_ws(opt))
            if not question or len(options) != 4:
                continue
            key = (question.lower(), "|".join(o.lower() for o in options))
            if key in seen:
                continue
            seen.add(key)
            buckets.append({
                "question": question,
                "options": options,
                "correct_index": int(q.get("correct_index") or 0),
                "explanation": str(q.get("explanation") or "").strip(),
            })

    return title_fallback, buckets


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











