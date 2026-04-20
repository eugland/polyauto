"""faster-whisper transcription worker.

Consumes SegmentEvent objects from `in_q`, runs the whisper model on each
chunk, and produces SentenceEvent items into `out_q` with timestamps that
are GLOBAL across the session (offset by event.global_offset_s).

Whisper's `segments` are usually sentence-shaped; that's accurate enough
for downstream MiniLM embedding + cosine segmentation.

Standalone smoke test:
    python -m restream.transcriber path/to/seg.mp4
"""
from __future__ import annotations

import argparse
import logging
import os
import queue
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

log = logging.getLogger("restream.transcriber")


@dataclass
class SentenceEvent:
    text: str
    start_s: float       # global session timestamp (seconds)
    end_s: float         # global session timestamp (seconds)
    seg_idx: int         # source segment index (for cutter file-range lookup)


_PUNCT_SPLIT = re.compile(r"(?<=[\.!\?])\s+")


def _split_sentences(text: str) -> list[str]:
    """Cheap sentence splitter — whisper output is mostly already sentences."""
    text = (text or "").strip()
    if not text:
        return []
    parts = [p.strip() for p in _PUNCT_SPLIT.split(text)]
    return [p for p in parts if p]


class TranscriberWorker(threading.Thread):
    def __init__(self, in_q: "queue.Queue", out_q: "queue.Queue",
                 stop_event: threading.Event,
                 model_name: str = "base.en", device: str = "cpu",
                 compute_type: str = "int8",
                 language: Optional[str] = None,
                 vad_filter: bool = True):
        super().__init__(name="TranscriberWorker", daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self.language = language
        self.vad_filter = vad_filter
        self._model = None

    def _load_model(self):
        if self._model is not None:
            return self._model
        from faster_whisper import WhisperModel  # lazy import (heavy)
        log.info("Loading faster-whisper model %r (device=%s, compute=%s)",
                 self.model_name, self.device, self.compute_type)
        t0 = time.time()
        self._model = WhisperModel(
            self.model_name, device=self.device, compute_type=self.compute_type
        )
        log.info("Whisper loaded in %.1fs", time.time() - t0)
        return self._model

    def transcribe_segment(self, path: Path, global_offset_s: float, seg_idx: int) -> Iterable[SentenceEvent]:
        model = self._load_model()
        kwargs = {
            "vad_filter": self.vad_filter,
            "beam_size": 1,
        }
        if self.language:
            kwargs["language"] = self.language
        segments_iter, info = model.transcribe(str(path), **kwargs)
        for s in segments_iter:
            text = (s.text or "").strip()
            if not text:
                continue
            start = float(s.start) + global_offset_s
            end = float(s.end) + global_offset_s
            # Whisper sometimes emits multi-sentence chunks — split.
            sentences = _split_sentences(text)
            if not sentences:
                continue
            if len(sentences) == 1:
                yield SentenceEvent(text=sentences[0], start_s=start, end_s=end, seg_idx=seg_idx)
            else:
                # Apportion start/end uniformly across sentences (good enough).
                span = max(end - start, 0.001)
                step = span / len(sentences)
                cur = start
                for sent in sentences:
                    yield SentenceEvent(
                        text=sent, start_s=cur, end_s=cur + step, seg_idx=seg_idx
                    )
                    cur += step

    def run(self) -> None:  # pragma: no cover (threaded)
        try:
            self._load_model()
        except Exception:
            log.exception("Whisper model failed to load; transcriber exiting")
            self.out_q.put(None)
            return
        try:
            while True:
                ev = self.in_q.get()
                if ev is None:
                    break
                try:
                    t0 = time.time()
                    n = 0
                    for sent_ev in self.transcribe_segment(ev.path, ev.global_offset_s, ev.seg_idx):
                        self.out_q.put(sent_ev)
                        n += 1
                    log.info("Transcribed %s -> %d sentences in %.1fs",
                             ev.path.name, n, time.time() - t0)
                except Exception:
                    log.exception("Transcription failed for %s", ev.path)
        finally:
            self.out_q.put(None)
            log.debug("TranscriberWorker exiting")


def _smoke_main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
    parser = argparse.ArgumentParser(description="Transcribe a single audio/video file with faster-whisper")
    parser.add_argument("path", type=Path)
    parser.add_argument("--model", default=os.environ.get("RESTREAM_WHISPER_MODEL", "base.en"))
    parser.add_argument("--device", default=os.environ.get("RESTREAM_WHISPER_DEVICE", "cpu"))
    parser.add_argument("--compute-type", default=os.environ.get("RESTREAM_WHISPER_COMPUTE_TYPE", "int8"))
    parser.add_argument("--offset", type=float, default=0.0, help="Pretend this file starts at OFFSET seconds")
    args = parser.parse_args(argv)
    if not args.path.exists():
        sys.exit(f"file not found: {args.path}")
    w = TranscriberWorker(
        in_q=queue.Queue(), out_q=queue.Queue(), stop_event=threading.Event(),
        model_name=args.model, device=args.device, compute_type=args.compute_type,
    )
    for ev in w.transcribe_segment(args.path, args.offset, seg_idx=0):
        print(f"[{ev.start_s:8.2f} – {ev.end_s:8.2f}] {ev.text}")
    return 0


if __name__ == "__main__":
    sys.exit(_smoke_main())
