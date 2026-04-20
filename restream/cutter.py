"""Cut a topic span out of recorded segment chunks using ffmpeg concat.

For each TopicEvent, identify the inclusive range of seg_NNNNN.mp4 files
covering [start_s, end_s], write a concat-demuxer list, and run:

    ffmpeg -f concat -safe 0 -i list.txt -ss <trim_lead> -to <trim_lead+dur>
           -c copy <out.mp4>

`-c copy` keeps cuts cheap; precision is bounded by the GOP (~2s with
the recorder's -g 60). For frame-accurate cuts, set RESTREAM_CUT_REENCODE=1.
"""
from __future__ import annotations

import logging
import os
import queue
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import recorder

log = logging.getLogger("restream.cutter")


@dataclass
class CutResult:
    path: Path
    title: str
    description: str
    tags: list[str]
    start_s: float
    end_s: float


class CutterWorker(threading.Thread):
    def __init__(self, in_q: "queue.Queue", out_q: "queue.Queue",
                 stop_event: threading.Event,
                 *, ctx: recorder.SessionCtx, cut_dir: Path,
                 channel: str = "",
                 reencode: bool = False):
        super().__init__(name="CutterWorker", daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.ctx = ctx
        self.cut_dir = Path(cut_dir)
        self.channel = channel
        self.reencode = reencode
        self.cut_dir.mkdir(parents=True, exist_ok=True)
        self._counter = 0

    def _segments_in_range(self, first_idx: int, last_idx: int) -> list[Path]:
        all_segs = recorder.list_segments(self.ctx.dir)
        out = []
        for idx, p in all_segs:
            if first_idx <= idx <= last_idx:
                out.append(p)
        return out

    def _cut(self, ev) -> Optional[CutResult]:
        # ev: topic_detector.TopicEvent
        seg_seconds = self.ctx.segment_seconds
        start_s = float(ev.start_s)
        end_s = float(ev.end_s)
        first_idx = max(0, int(start_s // seg_seconds))
        last_idx = max(first_idx, int(end_s // seg_seconds))
        seg_paths = self._segments_in_range(first_idx, last_idx)
        if not seg_paths:
            log.warning("No segments found for topic [%.1f, %.1f] (idx %d..%d)",
                        start_s, end_s, first_idx, last_idx)
            return None
        # Wait briefly if the *last* expected segment isn't present yet (race
        # between detector and recorder finalisation).
        deadline = time.time() + 30.0
        while seg_paths and seg_paths[-1].stat().st_size == 0 and time.time() < deadline:
            time.sleep(0.5)

        first_present_idx = int(seg_paths[0].name.split("_")[1].split(".")[0])
        trim_lead = max(0.0, start_s - first_present_idx * seg_seconds)
        duration = max(0.5, end_s - start_s)

        # Build concat list (use absolute paths; ffmpeg concat requires single-quote escaping)
        list_path = self.cut_dir / f"_concat_{int(time.time() * 1000)}_{self._counter:03d}.txt"
        self._counter += 1
        with list_path.open("w", encoding="utf-8") as f:
            for p in seg_paths:
                # ffmpeg concat demuxer escape: ' -> '\''
                escaped = str(p.resolve()).replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        out_name = f"topic_{int(start_s):07d}_{int(end_s):07d}.mp4"
        out_path = self.cut_dir / out_name

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-f", "concat", "-safe", "0", "-i", str(list_path),
            "-ss", f"{trim_lead:.3f}",
            "-t", f"{duration:.3f}",
        ]
        if self.reencode:
            cmd += [
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
                "-c:a", "aac", "-b:a", "160k",
            ]
        else:
            cmd += ["-c", "copy", "-avoid_negative_ts", "make_zero"]
        cmd += [
            "-movflags", "+faststart",
            str(out_path),
        ]
        log.info("Cutting topic [%.1f, %.1f] -> %s (%d segments, lead=%.2fs, dur=%.1fs)",
                 start_s, end_s, out_name, len(seg_paths), trim_lead, duration)
        try:
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                log.warning("ffmpeg cut rc=%d: %s", r.returncode, r.stderr.strip().splitlines()[-3:])
                return None
        finally:
            try:
                list_path.unlink()
            except OSError:
                pass
        if not out_path.exists() or out_path.stat().st_size == 0:
            log.warning("Cut produced empty file: %s", out_path)
            return None

        # Title + description from the topic transcript
        from . import topic_titler
        when = time.strftime("%Y-%m-%d", time.localtime(self.ctx.started_at))
        title = topic_titler.make_title(ev.transcript, fallback_start_s=start_s)
        if self.ctx.title:
            # Prefix the live broadcast title for context
            prefix = self.ctx.title.strip()[:60]
            if prefix:
                title = f"{prefix} — {title}"
                title = title[:97] + "…" if len(title) > 100 else title
        description = topic_titler.make_description(
            transcript=ev.transcript, channel=self.channel or self.ctx.channel,
            when=when,
            extra_footer=f"Span: {int(start_s)}s – {int(end_s)}s",
        )
        tags = topic_titler.make_tags(ev.transcript)
        return CutResult(
            path=out_path, title=title, description=description, tags=tags,
            start_s=start_s, end_s=end_s,
        )

    def run(self) -> None:  # pragma: no cover (threaded)
        try:
            while True:
                ev = self.in_q.get()
                if ev is None:
                    break
                try:
                    result = self._cut(ev)
                    if result is not None:
                        self.out_q.put(result)
                except Exception:
                    log.exception("Cut failed for topic [%.1f, %.1f]", ev.start_s, ev.end_s)
        finally:
            self.out_q.put(None)
            log.debug("CutterWorker exiting")
