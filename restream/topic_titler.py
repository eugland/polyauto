"""KeyBERT-based title + tag generator for a topic transcript.

Reuses the same all-MiniLM-L6-v2 model as the topic detector, so loading
is essentially free if the detector's already initialised it.

Title format: top 3-4 keyphrases joined by ", " then title-cased and
truncated to 70 chars. Falls back to "Topic at HH:MM:SS" when KeyBERT
returns nothing useful (e.g. transcript too short).
"""
from __future__ import annotations

import logging
import re
from typing import Optional

log = logging.getLogger("restream.topic_titler")

_kw_model = None
_TITLE_MAX = 70
_DESC_MAX = 4500
_TAG_MAX = 10


def _kw():
    global _kw_model
    if _kw_model is not None:
        return _kw_model
    from keybert import KeyBERT  # lazy
    from sentence_transformers import SentenceTransformer  # lazy
    log.info("Loading KeyBERT (all-MiniLM-L6-v2)")
    st = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    _kw_model = KeyBERT(model=st)
    return _kw_model


def _seconds_to_clock(s: float) -> str:
    s = int(max(0, s))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h:d}:{m:02d}:{sec:02d}"
    return f"{m:d}:{sec:02d}"


def _clean_title(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    # capitalize words but keep short joiners lowercase if not first
    out: list[str] = []
    for i, w in enumerate(s.split(" ")):
        if i > 0 and w.lower() in {"a", "an", "and", "or", "of", "the", "to", "in", "on", "for", "vs"}:
            out.append(w.lower())
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)[:_TITLE_MAX]


def make_title(text: str, fallback_start_s: float = 0.0) -> str:
    text = (text or "").strip()
    if not text:
        return f"Topic at {_seconds_to_clock(fallback_start_s)}"
    try:
        kw = _kw().extract_keywords(
            text,
            keyphrase_ngram_range=(1, 3),
            stop_words="english",
            use_mmr=True,
            diversity=0.55,
            top_n=4,
        )
    except Exception as e:
        log.debug("KeyBERT extract failed: %s", e)
        kw = []
    phrases = [p for p, _score in kw if p and len(p) > 2]
    if not phrases:
        return f"Topic at {_seconds_to_clock(fallback_start_s)}"
    title = ", ".join(phrases[:3])
    return _clean_title(title)


def make_tags(text: str, n: int = _TAG_MAX) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    try:
        kw = _kw().extract_keywords(
            text,
            keyphrase_ngram_range=(1, 2),
            stop_words="english",
            use_mmr=True,
            diversity=0.6,
            top_n=n,
        )
    except Exception as e:
        log.debug("KeyBERT extract_keywords (tags) failed: %s", e)
        return []
    return [p for p, _ in kw if p][:n]


def make_description(transcript: str, channel: str, when: str, extra_footer: Optional[str] = None) -> str:
    """Compose a YouTube description body. No URLs / domains — some YT
    accounts cannot post links, which causes the description to be
    stripped or the upload to be flagged."""
    body = (transcript or "").strip()
    if len(body) > _DESC_MAX - 200:
        body = body[: _DESC_MAX - 200].rsplit(" ", 1)[0] + "…"
    footer_parts = [f"Auto-cut on {when}"]
    if channel:
        footer_parts.append(f"Source channel: {channel}")
    if extra_footer:
        footer_parts.append(extra_footer)
    footer = "\n\n— " + " | ".join(footer_parts)
    return (body + footer)[:_DESC_MAX]
