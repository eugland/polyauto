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


def run_one_cycle(channel: str, stream_key: str, quality: str) -> int:
    """Spawn streamlink piped to ffmpeg, block until ffmpeg exits, return rc."""
    twitch_url = f"https://twitch.tv/{channel}"
    youtube_url = f"{YOUTUBE_RTMP_INGEST}/{stream_key}"

    streamlink_cmd = [
        "streamlink",
        "--stdout",
        "--twitch-disable-ads",
        "--hls-live-restart",
        twitch_url,
        quality,
    ]
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-i", "pipe:0",
        "-c", "copy",
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

    if not channel:
        sys.exit("TWITCH_CHANNEL not set in .env")
    if not key:
        sys.exit("YOUTUBE_STREAM_KEY not set in .env")

    log.info("Restream daemon starting: twitch.tv/%s -> YouTube (quality=%s)",
             channel, quality)
    log.info("Retry interval when offline: %ds", retry_seconds)

    backoff = retry_seconds
    while True:
        try:
            rc = run_one_cycle(channel, key, quality)
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
