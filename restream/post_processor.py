"""Post-processor pipeline: SegmentWatcher -> Transcriber -> TopicDetector
-> Cutter -> Uploader.

`start(ctx, channel)` builds and starts all worker threads for a recording
session and returns a Pipeline. `Pipeline.stop_and_drain(timeout)` signals
the watcher to flush any in-progress segment, then waits for downstream
queues to drain (or the timeout).

Replay mode (offline test):
    python -m restream.post_processor --replay path/to/session_dir
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import recorder

log = logging.getLogger("restream.post_processor")


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "", "false", "no")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


@dataclass
class Pipeline:
    threads: list[threading.Thread]
    queues: dict
    stop_event: threading.Event
    watcher_stop: threading.Event
    ctx: recorder.SessionCtx

    def stop_and_drain(self, timeout_s: float) -> None:
        log.info("Pipeline draining (timeout=%.0fs)...", timeout_s)
        # 1. Signal SegmentWatcher to flush + emit final segment.
        self.watcher_stop.set()
        deadline = time.time() + timeout_s
        # 2. Wait for each thread in pipeline order. The sentinel (None) is
        #    propagated downstream by each worker, so once the watcher exits
        #    the rest follow naturally.
        for t in self.threads:
            remaining = max(0.0, deadline - time.time())
            t.join(timeout=remaining)
            if t.is_alive():
                log.warning("Worker %s did not exit within drain timeout", t.name)
        log.info("Pipeline drained")


def start(ctx: recorder.SessionCtx, channel: str = "") -> Pipeline:
    """Build and start all worker threads for `ctx`. Returns the Pipeline."""
    cut_root = Path(os.environ.get("RESTREAM_CUT_OUTPUT_DIR", "restream/cuts"))
    cut_dir = cut_root / ctx.session_id

    seg_q: queue.Queue = queue.Queue(maxsize=64)
    sent_q: queue.Queue = queue.Queue(maxsize=512)
    topic_q: queue.Queue = queue.Queue(maxsize=64)
    cut_q: queue.Queue = queue.Queue(maxsize=64)

    stop_event = threading.Event()
    watcher_stop = threading.Event()

    watcher = recorder.SegmentWatcher(ctx, seg_q, watcher_stop)

    from . import transcriber, topic_detector, cutter, uploader  # lazy
    trans = transcriber.TranscriberWorker(
        in_q=seg_q, out_q=sent_q, stop_event=stop_event,
        model_name=os.environ.get("RESTREAM_WHISPER_MODEL", "base.en"),
        device=os.environ.get("RESTREAM_WHISPER_DEVICE", "cpu"),
        compute_type=os.environ.get("RESTREAM_WHISPER_COMPUTE_TYPE", "int8"),
        language=os.environ.get("RESTREAM_WHISPER_LANGUAGE") or None,
    )
    detector = topic_detector.TopicDetector(
        in_q=sent_q, out_q=topic_q, stop_event=stop_event,
        threshold=_env_float("RESTREAM_TOPIC_THRESHOLD", 0.55),
        min_topic_s=_env_int("RESTREAM_MIN_TOPIC_SECONDS", 90),
        max_topic_s=_env_int("RESTREAM_MAX_TOPIC_SECONDS", 1800),
        window_k=_env_int("RESTREAM_TOPIC_WINDOW_K", 5),
    )
    cut = cutter.CutterWorker(
        in_q=topic_q, out_q=cut_q, stop_event=stop_event,
        ctx=ctx, cut_dir=cut_dir, channel=channel,
        reencode=_env_bool("RESTREAM_CUT_REENCODE", "0"),
    )
    up = uploader.UploaderWorker(
        in_q=cut_q, stop_event=stop_event,
        privacy=os.environ.get("RESTREAM_UPLOAD_PRIVACY", "unlisted").strip() or "unlisted",
        daily_limit=_env_int("RESTREAM_DAILY_UPLOAD_LIMIT", 5),
        enabled=_env_bool("RESTREAM_UPLOAD_ENABLED", "1"),
    )

    threads = [watcher, trans, detector, cut, up]
    for t in threads:
        t.start()
    log.info("Post-processor pipeline started for session %s", ctx.session_id)
    return Pipeline(
        threads=threads,
        queues={"seg": seg_q, "sent": sent_q, "topic": topic_q, "cut": cut_q},
        stop_event=stop_event,
        watcher_stop=watcher_stop,
        ctx=ctx,
    )


def _replay_main(argv: Optional[list[str]] = None) -> int:
    """Replay a previously-recorded session dir through the pipeline.

    Drops the existing seg_*.mp4 files into the watcher queue at realistic
    spacing (segment_seconds), without touching ffmpeg or RTMP. Useful for
    end-to-end testing of transcription/segmentation/cutting/upload.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
    parser = argparse.ArgumentParser(description="Replay a recorded session through the post-processor")
    parser.add_argument("session_dir", type=Path)
    parser.add_argument("--segment-seconds", type=int, default=30)
    parser.add_argument("--realtime", action="store_true",
                        help="Sleep `segment_seconds` between chunks (otherwise ASAP)")
    parser.add_argument("--channel", default="replay")
    parser.add_argument("--title", default="")
    parser.add_argument("--no-upload", action="store_true",
                        help="Force RESTREAM_UPLOAD_ENABLED=0 for this run")
    args = parser.parse_args(argv)

    if args.no_upload:
        os.environ["RESTREAM_UPLOAD_ENABLED"] = "0"

    sdir = args.session_dir.resolve()
    if not sdir.is_dir():
        sys.exit(f"not a directory: {sdir}")

    # Build a SessionCtx that points at a *copy* dir we'll feed in order.
    # Simpler: reuse sdir directly; the watcher uses list_segments anyway.
    ctx = recorder.SessionCtx(
        session_id=sdir.name,
        dir=sdir,
        segment_seconds=args.segment_seconds,
        started_at=time.time(),
        channel=args.channel,
        title=args.title,
    )
    # Skip the SegmentWatcher: synthesise events directly so we can pace.
    seg_q: queue.Queue = queue.Queue(maxsize=64)
    sent_q: queue.Queue = queue.Queue(maxsize=512)
    topic_q: queue.Queue = queue.Queue(maxsize=64)
    cut_q: queue.Queue = queue.Queue(maxsize=64)

    stop_event = threading.Event()
    from . import transcriber, topic_detector, cutter, uploader
    trans = transcriber.TranscriberWorker(
        in_q=seg_q, out_q=sent_q, stop_event=stop_event,
        model_name=os.environ.get("RESTREAM_WHISPER_MODEL", "base.en"),
        device=os.environ.get("RESTREAM_WHISPER_DEVICE", "cpu"),
        compute_type=os.environ.get("RESTREAM_WHISPER_COMPUTE_TYPE", "int8"),
    )
    det = topic_detector.TopicDetector(
        in_q=sent_q, out_q=topic_q, stop_event=stop_event,
        threshold=_env_float("RESTREAM_TOPIC_THRESHOLD", 0.55),
        min_topic_s=_env_int("RESTREAM_MIN_TOPIC_SECONDS", 90),
        max_topic_s=_env_int("RESTREAM_MAX_TOPIC_SECONDS", 1800),
        window_k=_env_int("RESTREAM_TOPIC_WINDOW_K", 5),
    )
    cut_dir = Path(os.environ.get("RESTREAM_CUT_OUTPUT_DIR", "restream/cuts")) / sdir.name
    cu = cutter.CutterWorker(
        in_q=topic_q, out_q=cut_q, stop_event=stop_event,
        ctx=ctx, cut_dir=cut_dir, channel=args.channel,
        reencode=_env_bool("RESTREAM_CUT_REENCODE", "0"),
    )
    up = uploader.UploaderWorker(
        in_q=cut_q, stop_event=stop_event,
        privacy=os.environ.get("RESTREAM_UPLOAD_PRIVACY", "unlisted").strip() or "unlisted",
        daily_limit=_env_int("RESTREAM_DAILY_UPLOAD_LIMIT", 99),
        enabled=_env_bool("RESTREAM_UPLOAD_ENABLED", "1"),
    )
    for t in (trans, det, cu, up):
        t.start()

    segs = recorder.list_segments(sdir)
    log.info("Replaying %d segments from %s", len(segs), sdir)
    for idx, p in segs:
        seg_q.put(recorder.SegmentEvent(
            seg_idx=idx, path=p, seg_seconds=args.segment_seconds,
            global_offset_s=idx * args.segment_seconds,
        ))
        if args.realtime:
            time.sleep(args.segment_seconds)
    seg_q.put(None)  # signal transcriber to drain

    for t in (trans, det, cu, up):
        t.join()
    log.info("Replay complete")
    return 0


if __name__ == "__main__":
    sys.exit(_replay_main())
