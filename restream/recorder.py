"""Per-session recording context + finalised-segment watcher.

ffmpeg writes seg_%05d.mp4 chunks into SessionCtx.dir via the tee muxer
(see restreamer._build_ffmpeg_cmd). SegmentWatcher polls the directory and
emits a finalised event for each chunk as soon as it's safe to read:

  - normal case: seg_NNNNN.mp4 is "done" the moment seg_(NNNNN+1).mp4
    appears (ffmpeg has rotated to a new file).
  - tail case (after stop_event is set): the final still-open chunk is
    considered done after its size has been stable for >= STABLE_SECONDS.

Polling once per second is plenty given chunks are 30s.
"""
from __future__ import annotations

import logging
import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream.recorder")

_SEG_RE = re.compile(r"^seg_(\d{5})\.mp4$")
_STABLE_SECONDS = 3.0


@dataclass
class SegmentEvent:
    seg_idx: int
    path: Path
    seg_seconds: int       # nominal segment duration (last chunk may be shorter)
    global_offset_s: float  # seg_idx * seg_seconds


@dataclass
class SessionCtx:
    session_id: str
    dir: Path
    segment_seconds: int
    started_at: float
    channel: str = ""
    title: str = ""


def start_session(root: Path, *, segment_seconds: int, channel: str = "",
                  title: str = "") -> SessionCtx:
    """Create a new session subdir under `root` and return its context."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    sid = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    sdir = root / sid
    sdir.mkdir(parents=True, exist_ok=True)
    log.info("Recording session %s -> %s", sid, sdir)
    return SessionCtx(
        session_id=sid,
        dir=sdir,
        segment_seconds=segment_seconds,
        started_at=time.time(),
        channel=channel,
        title=title,
    )


def list_segments(d: Path) -> list[tuple[int, Path]]:
    """Return [(idx, path), ...] sorted by idx for any seg_NNNNN.mp4 in d."""
    out: list[tuple[int, Path]] = []
    if not d.exists():
        return out
    for p in d.iterdir():
        if not p.is_file():
            continue
        m = _SEG_RE.match(p.name)
        if m:
            out.append((int(m.group(1)), p))
    out.sort(key=lambda x: x[0])
    return out


class SegmentWatcher(threading.Thread):
    """Daemon thread that emits SegmentEvent into out_q when a chunk is finalised.

    Set stop_event when ffmpeg has exited; the watcher will then flush the
    last in-progress chunk after it has been size-stable for >= STABLE_SECONDS.
    """

    def __init__(self, ctx: SessionCtx, out_q: "queue.Queue[Optional[SegmentEvent]]",
                 stop_event: threading.Event):
        super().__init__(name="SegmentWatcher", daemon=True)
        self.ctx = ctx
        self.out_q = out_q
        self.stop_event = stop_event
        self._emitted: set[int] = set()

    def _emit(self, idx: int, path: Path) -> None:
        if idx in self._emitted:
            return
        self._emitted.add(idx)
        ev = SegmentEvent(
            seg_idx=idx,
            path=path,
            seg_seconds=self.ctx.segment_seconds,
            global_offset_s=idx * self.ctx.segment_seconds,
        )
        log.debug("Segment finalised: %s (offset %.1fs)", path.name, ev.global_offset_s)
        self.out_q.put(ev)

    def run(self) -> None:  # pragma: no cover (threaded I/O)
        last_size: dict[int, tuple[int, float]] = {}
        try:
            while True:
                segs = list_segments(self.ctx.dir)
                if segs:
                    # Any segment with a successor is finalised.
                    for i in range(len(segs) - 1):
                        self._emit(segs[i][0], segs[i][1])

                if self.stop_event.is_set():
                    # Drain mode: emit the final (still-being-written) segment
                    # once its size has stopped changing.
                    if not segs:
                        break
                    last_idx, last_path = segs[-1]
                    if last_idx in self._emitted:
                        break
                    try:
                        size = last_path.stat().st_size
                    except FileNotFoundError:
                        time.sleep(0.5)
                        continue
                    prev_size, prev_t = last_size.get(last_idx, (-1, 0.0))
                    now = time.time()
                    if size != prev_size:
                        last_size[last_idx] = (size, now)
                    elif now - prev_t >= _STABLE_SECONDS and size > 0:
                        self._emit(last_idx, last_path)
                        break
                    time.sleep(0.5)
                    continue

                time.sleep(1.0)
        finally:
            # Sentinel so consumers can exit their loop.
            self.out_q.put(None)
            log.debug("SegmentWatcher exiting (emitted %d segments)", len(self._emitted))
