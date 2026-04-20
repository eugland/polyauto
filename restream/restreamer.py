"""Streamlink -> ffmpeg pipe to YouTube RTMP.

No Twitch API credentials needed: streamlink reads the public HLS playlist.
When the channel is offline, streamlink exits with a non-zero code and we
just sleep + retry.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

log = logging.getLogger("restream")

YOUTUBE_RTMP_INGEST = "rtmp://a.rtmp.youtube.com/live2"


def _load_env() -> None:
    """Tiny .env loader (no python-dotenv dep). Reads restream/.env (this
    directory), isolated from the repo-root .env used by the Polymarket bots."""
    env_file = Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        # strip inline comments + surrounding quotes/whitespace
        val = val.split("#", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), val)


def _require_binary(name: str) -> None:
    if shutil.which(name) is None:
        sys.exit(
            f"`{name}` not found on PATH. On Ubuntu: sudo apt install -y {name}"
        )


def run_one_cycle(channel: str, stream_key: str, quality: str,
                  hls_live_edge: int, buffer_mb: int) -> int:
    """Spawn streamlink piped to ffmpeg, block until ffmpeg exits, return rc.

    Buffering strategy: start `hls_live_edge` segments behind the live edge
    (Twitch segments are ~2s each, so 15 ≈ 30s lag) and give streamlink a
    `buffer_mb` ring buffer + parallel segment fetcher with retries. When
    a single segment is slow / 404s during an ad marker, we have headroom
    to recover before the gap reaches YouTube's RTMP ingest. ffmpeg's
    -thread_queue_size and FLV live flags absorb any residual jitter."""
    twitch_url = f"https://twitch.tv/{channel}"
    youtube_url = f"{YOUTUBE_RTMP_INGEST}/{stream_key}"

    streamlink_cmd = [
        "streamlink",
        "--stdout",
        "--twitch-disable-ads",
        "--hls-live-restart",
        "--hls-live-edge", str(hls_live_edge),     # start N segments behind live (~2s/seg)
        "--ringbuffer-size", f"{buffer_mb}M",      # in-memory buffer between fetcher + stdout
        "--stream-segment-attempts", "5",          # retry a flaky segment 5x
        "--stream-segment-timeout", "10",          # per-segment HTTP timeout
        "--stream-segment-threads", "3",           # parallel segment fetch
        twitch_url,
        quality,
    ]
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "+genpts+discardcorrupt",       # repair PTS gaps + drop bad packets
        "-thread_queue_size", "4096",              # deeper input packet queue
        "-i", "pipe:0",
        "-c", "copy",
        "-max_muxing_queue_size", "1024",          # deeper output mux queue
        "-flvflags", "no_duration_filesize",       # FLV-live correct flags
        "-f", "flv",
        youtube_url,
    ]

    log.info("Starting pipe: streamlink (%s @ %s) -> ffmpeg -> YouTube",
             channel, quality)
    sl = subprocess.Popen(streamlink_cmd, stdout=subprocess.PIPE)
    # Allow streamlink to receive SIGPIPE if ffmpeg dies first
    assert sl.stdout is not None
    ff = subprocess.Popen(ffmpeg_cmd, stdin=sl.stdout)
    sl.stdout.close()

    try:
        rc = ff.wait()
    except KeyboardInterrupt:
        log.info("Interrupt received, terminating pipe")
        ff.terminate()
        sl.terminate()
        try:
            ff.wait(timeout=5)
        except subprocess.TimeoutExpired:
            ff.kill()
        try:
            sl.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sl.kill()
        raise
    finally:
        if sl.poll() is None:
            sl.terminate()
            try:
                sl.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sl.kill()

    return rc


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
    )
    _load_env()
    _require_binary("streamlink")
    _require_binary("ffmpeg")

    channel = os.environ.get("TWITCH_CHANNEL", "").strip()
    key = os.environ.get("YOUTUBE_STREAM_KEY", "").strip()
    quality = os.environ.get("RESTREAM_QUALITY", "best").strip() or "best"
    try:
        retry_seconds = int(os.environ.get("RESTREAM_RETRY_SECONDS", "30"))
    except ValueError:
        retry_seconds = 30
    try:
        # Twitch segments ≈ 2s each, so 15 ≈ 30s behind live edge.
        hls_live_edge = int(os.environ.get("RESTREAM_HLS_LIVE_EDGE", "15"))
    except ValueError:
        hls_live_edge = 15
    try:
        buffer_mb = int(os.environ.get("RESTREAM_BUFFER_MB", "64"))
    except ValueError:
        buffer_mb = 64

    if not channel:
        sys.exit("TWITCH_CHANNEL not set in .env")
    if not key:
        sys.exit("YOUTUBE_STREAM_KEY not set in .env")

    log.info("Restream daemon starting: twitch.tv/%s -> YouTube (quality=%s)",
             channel, quality)
    log.info("Buffering: ~%ds behind live edge (%d segments), %dMB ring buffer",
             hls_live_edge * 2, hls_live_edge, buffer_mb)
    log.info("Retry interval when offline: %ds", retry_seconds)

    backoff = retry_seconds
    while True:
        try:
            rc = run_one_cycle(channel, key, quality, hls_live_edge, buffer_mb)
        except KeyboardInterrupt:
            log.info("Exiting on Ctrl-C")
            return
        except Exception:
            log.exception("Unexpected error in cycle")
            rc = -1

        if rc == 0:
            log.info("Pipe exited cleanly (channel ended stream)")
            backoff = retry_seconds
        else:
            log.warning("Pipe exited rc=%d (channel offline or disconnect)", rc)
            # mild exponential backoff capped at 5 min, resets on clean exit
            backoff = min(backoff * 2, 300) if rc < 0 else retry_seconds

        log.info("Sleeping %ds before retry", backoff)
        try:
            time.sleep(backoff)
        except KeyboardInterrupt:
            log.info("Exiting on Ctrl-C")
            return
