"""Online topic-boundary detection on streaming sentence transcripts.

Algorithm: sliding-window cosine on sentence embeddings.

For each newly-arrived sentence, we maintain a rolling list. Whenever the
"unconfirmed tail" has at least 2K + 1 sentences past the last confirmed
boundary, we look for the candidate boundary that maximises the cosine
DISTANCE between the mean embedding of the K sentences before vs the K
after, anywhere in the unconfirmed range. If that peak exceeds
`threshold` AND the candidate is at least `min_topic_seconds` past the
previous boundary AND at least K sentences from the end (so the right
window is real, not still being filled), we confirm a topic boundary
there.

A hard upper bound `max_topic_seconds` force-emits when the speaker
truly stays on one topic for too long, keeping uploads flowing.

On stop, the final pending topic is emitted if it spans at least
`min_topic_seconds`.

Standalone smoke test on a JSON list of {"text","start_s","end_s","seg_idx"}:
    python -m restream.topic_detector path/to/sentences.json
"""
from __future__ import annotations

import argparse
import json
import logging
import queue
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream.topic_detector")


@dataclass
class TopicEvent:
    start_s: float
    end_s: float
    start_seg_idx: int
    end_seg_idx: int
    transcript: str
    sentences: list[dict] = field(default_factory=list)


class _Embedder:
    """Wraps sentence-transformers with lazy load + per-instance cache."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None

    def _load(self):
        if self._model is not None:
            return self._model
        from sentence_transformers import SentenceTransformer  # lazy
        log.info("Loading sentence-transformer %r", self.model_name)
        self._model = SentenceTransformer(self.model_name)
        return self._model

    def encode(self, texts: list[str]):
        import numpy as np  # lazy
        m = self._load()
        emb = m.encode(texts, convert_to_numpy=True, normalize_embeddings=True,
                       show_progress_bar=False)
        return np.asarray(emb, dtype="float32")


def _cos_dist_mean(a, b) -> float:
    """Mean of normalized vectors → 1 - dot. Inputs already L2-normalized."""
    import numpy as np
    if a.shape[0] == 0 or b.shape[0] == 0:
        return 0.0
    ma = a.mean(axis=0)
    mb = b.mean(axis=0)
    # ma/mb are means of unit vectors; renormalize for cosine.
    na = np.linalg.norm(ma)
    nb = np.linalg.norm(mb)
    if na == 0 or nb == 0:
        return 0.0
    return float(1.0 - (ma @ mb) / (na * nb))


@dataclass
class _Sentence:
    text: str
    start_s: float
    end_s: float
    seg_idx: int
    embedding: object = None  # numpy array


class TopicDetector(threading.Thread):
    """Consume SentenceEvent from in_q, emit TopicEvent into out_q."""

    def __init__(self, in_q: "queue.Queue", out_q: "queue.Queue",
                 stop_event: threading.Event,
                 *, threshold: float = 0.55, min_topic_s: int = 90,
                 max_topic_s: int = 1800, window_k: int = 5,
                 model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        super().__init__(name="TopicDetector", daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.threshold = threshold
        self.min_topic_s = min_topic_s
        self.max_topic_s = max_topic_s
        self.window_k = window_k
        self.embedder = _Embedder(model_name)
        self._sentences: list[_Sentence] = []
        self._last_boundary_idx = 0  # index INTO self._sentences

    def _emit_topic(self, end_idx: int) -> None:
        """Emit a TopicEvent for sentences [_last_boundary_idx, end_idx)."""
        slice_ = self._sentences[self._last_boundary_idx:end_idx]
        if not slice_:
            return
        first, last = slice_[0], slice_[-1]
        ev = TopicEvent(
            start_s=first.start_s,
            end_s=last.end_s,
            start_seg_idx=min(s.seg_idx for s in slice_),
            end_seg_idx=max(s.seg_idx for s in slice_),
            transcript=" ".join(s.text for s in slice_),
            sentences=[
                {"text": s.text, "start_s": s.start_s, "end_s": s.end_s, "seg_idx": s.seg_idx}
                for s in slice_
            ],
        )
        log.info("Topic boundary at t=%.1fs (span %.1fs, %d sentences)",
                 ev.end_s, ev.end_s - ev.start_s, len(slice_))
        self.out_q.put(ev)
        self._last_boundary_idx = end_idx

    def _consider_boundary(self) -> bool:
        """Look for a confirmable boundary in the unconfirmed tail.

        Returns True if a boundary was emitted.
        """
        K = self.window_k
        n = len(self._sentences)
        unconfirmed_start = self._last_boundary_idx
        unconfirmed_n = n - unconfirmed_start
        # Need at least K sentences before AND K sentences after the candidate.
        if unconfirmed_n < 2 * K + 1:
            return False
        # Candidate boundary position (in absolute sentence index): from
        # unconfirmed_start + K to n - K (right window must be K real sentences
        # AND the most recent K sentences are still "in flight" so we don't
        # cut at the very edge).
        lo = unconfirmed_start + K
        hi = n - K  # exclusive
        if lo >= hi:
            return False
        # Score every candidate; pick the peak. Cheap enough for K=5, n<200.
        import numpy as np
        best_score = -1.0
        best_idx = -1
        for i in range(lo, hi):
            left = np.stack([self._sentences[j].embedding for j in range(i - K, i)])
            right = np.stack([self._sentences[j].embedding for j in range(i, i + K)])
            s = _cos_dist_mean(left, right)
            if s > best_score:
                best_score = s
                best_idx = i
        if best_idx < 0 or best_score < self.threshold:
            return False
        # Time gate.
        topic_start = self._sentences[unconfirmed_start].start_s
        candidate_t = self._sentences[best_idx].start_s
        if candidate_t - topic_start < self.min_topic_s:
            return False
        log.debug("Confirming topic boundary at sentence %d (score=%.3f)", best_idx, best_score)
        self._emit_topic(best_idx)
        return True

    def _consider_max_span(self) -> bool:
        if not self._sentences:
            return False
        topic_start = self._sentences[self._last_boundary_idx].start_s
        last_t = self._sentences[-1].end_s
        if last_t - topic_start < self.max_topic_s:
            return False
        log.info("Force-emitting topic: max-span %.1fs reached", last_t - topic_start)
        self._emit_topic(len(self._sentences))
        return True

    def push_sentence(self, text: str, start_s: float, end_s: float, seg_idx: int) -> None:
        emb = self.embedder.encode([text])[0]
        self._sentences.append(_Sentence(
            text=text, start_s=start_s, end_s=end_s, seg_idx=seg_idx, embedding=emb,
        ))
        # Try a boundary; force max-span if applicable.
        while self._consider_boundary():
            pass
        self._consider_max_span()

    def flush(self) -> None:
        """Emit a final pending topic on shutdown if it meets min_topic_s."""
        if self._last_boundary_idx >= len(self._sentences):
            return
        topic_start = self._sentences[self._last_boundary_idx].start_s
        last_t = self._sentences[-1].end_s
        if last_t - topic_start >= self.min_topic_s:
            self._emit_topic(len(self._sentences))

    def run(self) -> None:  # pragma: no cover (threaded)
        try:
            self.embedder._load()
        except Exception:
            log.exception("Sentence-transformer failed to load; topic detector exiting")
            self.out_q.put(None)
            return
        try:
            while True:
                ev = self.in_q.get()
                if ev is None:
                    break
                try:
                    self.push_sentence(ev.text, ev.start_s, ev.end_s, ev.seg_idx)
                except Exception:
                    log.exception("push_sentence failed for %r", ev.text[:60])
            self.flush()
        finally:
            self.out_q.put(None)
            log.debug("TopicDetector exiting (%d sentences seen)", len(self._sentences))


def _smoke_main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
    parser = argparse.ArgumentParser(description="Run TopicDetector against a JSON sentence list")
    parser.add_argument("path", type=Path)
    parser.add_argument("--threshold", type=float, default=0.55)
    parser.add_argument("--min-topic-s", type=int, default=90)
    parser.add_argument("--max-topic-s", type=int, default=1800)
    parser.add_argument("--window-k", type=int, default=5)
    args = parser.parse_args(argv)
    sents = json.loads(args.path.read_text(encoding="utf-8"))
    in_q: queue.Queue = queue.Queue()
    out_q: queue.Queue = queue.Queue()
    det = TopicDetector(in_q, out_q, threading.Event(),
                        threshold=args.threshold, min_topic_s=args.min_topic_s,
                        max_topic_s=args.max_topic_s, window_k=args.window_k)
    det.embedder._load()
    for s in sents:
        det.push_sentence(s["text"], float(s["start_s"]), float(s["end_s"]),
                          int(s.get("seg_idx", 0)))
    det.flush()
    out_q.put(None)
    while True:
        ev = out_q.get()
        if ev is None:
            break
        print(f"[{ev.start_s:8.2f} – {ev.end_s:8.2f}]  ({len(ev.sentences)} sents)  {ev.transcript[:80]}…")
    return 0


if __name__ == "__main__":
    sys.exit(_smoke_main())
