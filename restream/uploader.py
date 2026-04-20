"""Upload finished topic cuts to YouTube.

Includes a soft daily-quota guard backed by a tiny JSON counter, since
YouTube's default quota is 10,000 units/day and `videos.insert` is ~1600
units per call (≈6 uploads/day baseline).
"""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream.uploader")

_DEFAULT_LOG = Path("restream/upload_log.json")


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_count(path: Path) -> tuple[str, int]:
    if not path.exists():
        return _today_utc(), 0
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return str(d.get("date", _today_utc())), int(d.get("count", 0))
    except Exception:
        return _today_utc(), 0


def _save_count(path: Path, date: str, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"date": date, "count": count}), encoding="utf-8")


class UploaderWorker(threading.Thread):
    def __init__(self, in_q: "queue.Queue", stop_event: threading.Event,
                 *, privacy: str = "unlisted", daily_limit: int = 5,
                 enabled: bool = True, log_path: Path = _DEFAULT_LOG):
        super().__init__(name="UploaderWorker", daemon=True)
        self.in_q = in_q
        self.stop_event = stop_event
        self.privacy = privacy
        self.daily_limit = daily_limit
        self.enabled = enabled
        self.log_path = Path(log_path)

    def _check_and_inc(self) -> bool:
        date, count = _load_count(self.log_path)
        today = _today_utc()
        if date != today:
            date, count = today, 0
        if count >= self.daily_limit:
            log.warning("Daily upload limit reached (%d) — skipping further uploads today",
                        self.daily_limit)
            return False
        _save_count(self.log_path, today, count + 1)
        return True

    def run(self) -> None:  # pragma: no cover (threaded I/O)
        try:
            while True:
                cut = self.in_q.get()
                if cut is None:
                    break
                if not self.enabled:
                    log.info("Upload disabled — would have uploaded %s", cut.path.name)
                    continue
                if not self._check_and_inc():
                    continue
                try:
                    from . import youtube_upload  # lazy
                    vid = youtube_upload.upload_video(
                        path=cut.path,
                        title=cut.title,
                        description=cut.description,
                        tags=cut.tags,
                        privacy=self.privacy,
                    )
                    log.info("Uploaded topic %s -> %s", cut.path.name, vid)
                except Exception:
                    log.exception("Upload failed for %s", cut.path)
        finally:
            log.debug("UploaderWorker exiting")
