from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus


@dataclass
class TopicContext:
    topic: str
    title: str
    text: str
    sources: List[str]


def _norm_title(s: str) -> str:
    x = (s or "").strip().lower()
    x = re.sub(r"[^\w\s]+", " ", x, flags=re.UNICODE)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def _tokens(s: str) -> List[str]:
    x = _norm_title(s)
    return [t for t in x.split() if len(t) >= 2]


def _title_score(query: str, candidate: str) -> float:
    q = _norm_title(query)
    c = _norm_title(candidate)
    if not q or not c:
        return 0.0
    if q == c:
        return 1.0
    if q in c or c in q:
        return 0.9
    qt = set(_tokens(q))
    ct = set(_tokens(c))
    if not qt or not ct:
        return 0.0
    inter = len(qt & ct)
    union = len(qt | ct)
    return float(inter) / float(union or 1)


def _compact(text: str, *, max_chars: int) -> str:
    x = (text or "").strip()
    x = re.sub(r"\r\n?", "\n", x)
    x = re.sub(r"\n{3,}", "\n\n", x).strip()
    if len(x) <= max_chars:
        return x
    return x[: max_chars - 1].rstrip() + "…"


async def _fetch_json(session, url: str, *, timeout_sec: float) -> Optional[Dict[str, Any]]:
    try:
        async with session.get(url, timeout=timeout_sec, headers={"User-Agent": "AiQuezBot/1.0"}) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def _fetch_text(session, url: str, *, timeout_sec: float) -> Optional[str]:
    try:
        async with session.get(url, timeout=timeout_sec, headers={"User-Agent": "AiQuezBot/1.0"}) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None


async def _wikipedia_summary(session, topic: str, *, lang: str, timeout_sec: float) -> Tuple[str, str, str]:
    """
    Returns (title, extract, source_label) or ("","","") when not found.
    """
    base = f"https://{lang}.wikipedia.org"
    q = quote_plus(topic)
    search_url = f"{base}/w/api.php?action=opensearch&search={q}&limit=5&namespace=0&format=json"
    data = await _fetch_json(session, search_url, timeout_sec=timeout_sec)
    if not data or not isinstance(data, list) or len(data) < 2:
        return "", "", ""
    titles = data[1] if isinstance(data[1], list) else []
    if not titles:
        return "", "", ""

    best = ""
    best_score = 0.0
    for t in titles[:5]:
        sc = _title_score(topic, str(t))
        if sc > best_score:
            best_score = sc
            best = str(t)
    if not best or best_score < 0.25:
        best = str(titles[0])

    # REST summary is cleaner than API extract.
    rest_title = quote_plus(best.replace(" ", "_"))
    sum_url = f"{base}/api/rest_v1/page/summary/{rest_title}"
    summ = await _fetch_json(session, sum_url, timeout_sec=timeout_sec)
    if not summ:
        return "", "", ""
    extract = str(summ.get("extract") or "").strip()
    title_out = str(summ.get("title") or best).strip()
    if len(extract) < 200:
        return "", "", ""
    return title_out, extract, f"Wikipedia({lang})"


async def _google_books(session, topic: str, *, timeout_sec: float) -> Tuple[str, str, str]:
    def _build_text(vi: Dict[str, Any], *, snippet: str) -> Tuple[str, str]:
        title = str(vi.get("title") or "").strip() or topic
        authors = vi.get("authors") or []
        if not isinstance(authors, list):
            authors = []
        published = str(vi.get("publishedDate") or "").strip()
        categories = vi.get("categories") or []
        if not isinstance(categories, list):
            categories = []
        desc = str(vi.get("description") or "").strip()
        desc = re.sub(r"<[^>]+>", " ", desc)
        desc = re.sub(r"\s+", " ", desc).strip()
        if not desc:
            desc = (snippet or "").strip()
        desc = re.sub(r"\s+", " ", desc).strip()

        parts: List[str] = [f"Title: {title}"]
        if authors:
            parts.append("Authors: " + ", ".join(str(a).strip() for a in authors if str(a).strip()))
        if published:
            parts.append(f"Published: {published}")
        if categories:
            parts.append("Categories: " + ", ".join(str(c).strip() for c in categories if str(c).strip()))
        parts.append("")
        parts.append(desc)
        return title, "\n".join(parts).strip()

    # Try a couple of queries: strict title match, then broader.
    queries = [f"intitle:{topic}", topic]
    for qraw in queries:
        q = quote_plus(qraw)
        url = f"https://www.googleapis.com/books/v1/volumes?q={q}&maxResults=8&printType=books"
        data = await _fetch_json(session, url, timeout_sec=timeout_sec)
        if not data:
            continue
        items = data.get("items") or []
        if not isinstance(items, list) or not items:
            continue

        best_item = None
        best_score = 0.0
        for it in items[:8]:
            vi = it.get("volumeInfo") or {}
            title = str(vi.get("title") or "").strip()
            if not title:
                continue
            sc = _title_score(topic, title)
            if sc > best_score:
                best_score = sc
                best_item = it
        if best_item is None:
            best_item = items[0]

        vi = best_item.get("volumeInfo") or {}
        snippet = ""
        si = best_item.get("searchInfo") or {}
        if isinstance(si, dict):
            snippet = str(si.get("textSnippet") or "").strip()
            snippet = re.sub(r"<[^>]+>", " ", snippet)
        title, text = _build_text(vi, snippet=snippet)
        # Many entries have no description; allow shorter snippets but still require some substance.
        if len(re.sub(r"\s+", " ", text).strip()) >= 350:
            return title, text, "Google Books"

    return "", "", ""


async def _openlibrary(session, topic: str, *, timeout_sec: float) -> Tuple[str, str, str]:
    async def _search(url: str) -> Optional[Dict[str, Any]]:
        return await _fetch_json(session, url, timeout_sec=timeout_sec)

    q = quote_plus(topic)
    urls = [
        f"https://openlibrary.org/search.json?title={q}&limit=8",
        f"https://openlibrary.org/search.json?q={q}&limit=8",
    ]
    data = None
    for u in urls:
        data = await _search(u)
        if data and isinstance(data.get("docs") or [], list) and (data.get("docs") or []):
            break
    if not data:
        return "", "", ""
    docs = data.get("docs") or []
    if not isinstance(docs, list) or not docs:
        return "", "", ""

    best = None
    best_score = 0.0
    for d in docs[:5]:
        title = str(d.get("title") or "").strip()
        if not title:
            continue
        sc = _title_score(topic, title)
        if sc > best_score:
            best_score = sc
            best = d
    if best is None:
        best = docs[0]

    title = str(best.get("title") or "").strip() or topic
    authors = best.get("author_name") or []
    if not isinstance(authors, list):
        authors = []
    year = best.get("first_publish_year")
    key = str(best.get("key") or "").strip()  # /works/OL..W

    desc = ""
    if key.startswith("/works/"):
        work_url = f"https://openlibrary.org{key}.json"
        work = await _fetch_json(session, work_url, timeout_sec=timeout_sec)
        if work:
            d = work.get("description")
            if isinstance(d, dict):
                desc = str(d.get("value") or "").strip()
            else:
                desc = str(d or "").strip()

    desc = re.sub(r"\s+", " ", desc).strip()
    subjects = best.get("subject") or []
    if not isinstance(subjects, list):
        subjects = []
    subj = ", ".join(str(s).strip() for s in subjects[:18] if str(s).strip())
    if len(desc) < 120 and len(subj) < 120:
        return "", "", ""

    parts: List[str] = [f"Title: {title}"]
    if authors:
        parts.append("Authors: " + ", ".join(str(a).strip() for a in authors if str(a).strip()))
    if year:
        parts.append(f"First publish year: {year}")
    if subj:
        parts.append("Subjects: " + subj)
    parts.append("")
    parts.append(desc or subj)
    text = "\n".join(parts).strip()
    return title, text, "OpenLibrary"


async def fetch_topic_context(topic: str, *, ui_lang: str = "uz") -> TopicContext:
    """
    Best-effort: try to find a book/article context for a given topic.
    Returns empty text if nothing useful was found.
    """
    import aiohttp

    raw = (topic or "").strip()
    if len(raw) < 3:
        return TopicContext(topic=raw, title=raw, text="", sources=[])

    timeout_sec = float(os.getenv("TOPIC_CONTEXT_TIMEOUT_SEC", "6.5") or 6.5)
    timeout_sec = max(1.0, min(15.0, timeout_sec))
    max_chars = int(os.getenv("TOPIC_CONTEXT_MAX_CHARS", "15000") or 15000)
    max_chars = max(1500, min(60000, max_chars))

    # Language priority: try UI lang wiki first, then RU, then EN.
    wiki_langs = []
    if str(ui_lang or "").strip().lower() in {"uz", "ru", "en", "de", "tr", "kk", "ar", "zh", "ko"}:
        if ui_lang in {"uz", "ru", "en"}:
            wiki_langs = [ui_lang]
        else:
            wiki_langs = ["uz"]
    else:
        wiki_langs = ["uz"]
    for extra in ["ru", "en"]:
        if extra not in wiki_langs:
            wiki_langs.append(extra)

    async with aiohttp.ClientSession() as session:
        tasks = []
        tasks.append(_google_books(session, raw, timeout_sec=timeout_sec))
        tasks.append(_openlibrary(session, raw, timeout_sec=timeout_sec))
        for wl in wiki_langs[:3]:
            tasks.append(_wikipedia_summary(session, raw, lang=wl, timeout_sec=timeout_sec))

        results: List[Tuple[str, str, str]] = []
        # Run in parallel but bound overall time.
        try:
            done = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_sec + 1.5)
        except Exception:
            done = []
        for item in done or []:
            if isinstance(item, Exception):
                continue
            if not isinstance(item, tuple) or len(item) != 3:
                continue
            t, txt, src = item
            if str(txt or "").strip():
                results.append((str(t or "").strip(), str(txt or "").strip(), str(src or "").strip()))

    if not results:
        return TopicContext(topic=raw, title=raw, text="", sources=[])

    # Prefer sources with higher title similarity.
    scored: List[Tuple[float, str, str, str]] = []
    for t, txt, src in results:
        sc = _title_score(raw, t)
        scored.append((sc, t, txt, src))
    scored.sort(key=lambda x: x[0], reverse=True)

    # Assemble a compact context text; include up to 2 best chunks.
    used_sources: List[str] = []
    chunks: List[str] = []
    title_out = scored[0][1] or raw
    for sc, t, txt, src in scored[:4]:
        if not txt or src in used_sources:
            continue
        used_sources.append(src)
        chunks.append(f"SOURCE: {src}\n{txt}".strip())
        if len("\n\n".join(chunks)) >= max_chars:
            break

    combined = _compact("\n\n".join(chunks), max_chars=max_chars)
    return TopicContext(topic=raw, title=title_out, text=combined, sources=used_sources)
