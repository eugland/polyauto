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
                  hls_live_edge: int, buffer_mb: int,
                  reencode_audio: bool, reencode_video: bool,
                  video_bitrate_k: int) -> int:
    """Spawn streamlink piped to ffmpeg, block until ffmpeg exits, return rc.

    Buffering: start `hls_live_edge` segments behind the live edge
    (Twitch segments are ~2s each, so 15 ≈ 30s lag) plus a `buffer_mb`
    ring buffer and parallel segment fetcher with retries.

    Re-encoding: audio re-encode (default ON) eliminates beeps/clicks at
    Twitch ad-break boundaries by resampling across PTS jumps. Video
    re-encode (default OFF, ~1 CPU core) smooths video timestamps at
    the same boundaries — enable only if pure-passthrough video still
    drops mid-stream."""
    twitch_url = f"https://twitch.tv/{channel}"
    youtube_url = f"{YOUTUBE_RTMP_INGEST}/{stream_key}"

    streamlink_cmd = [
        "streamlink",
        "--stdout",
        "--twitch-disable-ads",
        "--hls-live-restart",
        "--hls-live-edge", str(hls_live_edge),
        "--ringbuffer-size", f"{buffer_mb}M",
        "--stream-segment-attempts", "5",
        "--stream-segment-timeout", "10",
        "--stream-segment-threads", "3",
        twitch_url,
        quality,
    ]

    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "+genpts+discardcorrupt+igndts",  # repair PTS, drop bad pkts, ignore decode TS
        "-err_detect", "ignore_err",                 # don't abort on minor decode errors
        "-thread_queue_size", "4096",
        "-i", "pipe:0",
    ]
    if reencode_video:
        ffmpeg_cmd += [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-b:v", f"{video_bitrate_k}k",
            "-maxrate", f"{video_bitrate_k}k",
            "-bufsize", f"{video_bitrate_k * 2}k",
            "-g", "60",                              # keyframe every 2s @ 30fps (YouTube wants ≤2s)
            "-pix_fmt", "yuv420p",
        ]
    else:
        ffmpeg_cmd += ["-c:v", "copy"]
    if reencode_audio:
        ffmpeg_cmd += [
            "-c:a", "aac",
            "-b:a", "160k",
            "-ar", "48000",
            "-af", "aresample=async=1000",           # resample across audio PTS jumps -> kills beeps
        ]
    else:
        ffmpeg_cmd += ["-c:a", "copy"]
    ffmpeg_cmd += [
        "-max_muxing_queue_size", "1024",
        "-flvflags", "no_duration_filesize",
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
    # Audio re-encode default ON: kills the beeps at ad-break boundaries
    # (resamples across audio PTS jumps), costs ~5% CPU.
    reencode_audio = os.environ.get("RESTREAM_REENCODE_AUDIO", "1").strip() not in ("0", "", "false", "no")
    # Video re-encode default OFF: only flip on if you still see drops with
    # audio re-encode enabled. Costs ~1 CPU core for libx264 veryfast 1080p.
    reencode_video = os.environ.get("RESTREAM_REENCODE_VIDEO", "0").strip() in ("1", "true", "yes", "on")
    try:
        video_bitrate_k = int(os.environ.get("RESTREAM_VIDEO_BITRATE_KBPS", "6000"))
    except ValueError:
        video_bitrate_k = 6000

    if not channel:
        sys.exit("TWITCH_CHANNEL not set in .env")
    if not key:
        sys.exit("YOUTUBE_STREAM_KEY not set in .env")

    log.info("Restream daemon starting: twitch.tv/%s -> YouTube (quality=%s)",
             channel, quality)
    log.info("Buffering: ~%ds behind live edge (%d segments), %dMB ring buffer",
             hls_live_edge * 2, hls_live_edge, buffer_mb)
    log.info("Codecs: video=%s  audio=%s%s",
             "re-encode H.264 veryfast @ %dk" % video_bitrate_k if reencode_video else "passthrough",
             "re-encode AAC 160k (smooths ad-break beeps)" if reencode_audio else "passthrough",
             "" if reencode_audio else "  ⚠ beeps at ad boundaries are likely")
    log.info("Retry interval when offline: %ds", retry_seconds)

    backoff = retry_seconds
    while True:
        try:
            rc = run_one_cycle(channel, key, quality, hls_live_edge, buffer_mb,
                               reencode_audio, reencode_video, video_bitrate_k)
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
