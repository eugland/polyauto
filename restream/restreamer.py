"""Streamlink -> ffmpeg pipe to YouTube RTMP.

When TWITCH_CLIENT_ID/SECRET are set, the daemon polls Helix to detect
liveness and (if RESTREAM_USE_DYNAMIC_BROADCAST=1) mints a fresh YouTube
broadcast per session whose title mirrors the live Twitch title. Without
those, behaviour is the legacy passive HLS-probe + static stream key.

When RESTREAM_RECORD_ENABLED=1, ffmpeg uses the tee muxer to also write
30s MP4 chunks to restream/recordings/<session_id>/, which the
post_processor pipeline transcribes, segments by topic, and uploads as
separate YouTube videos concurrently with the live stream.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream")

YOUTUBE_RTMP_INGEST = "rtmp://a.rtmp.youtube.com/live2"


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "", "false", "no")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


@dataclass
class CycleSession:
    """Per-cycle resources + IDs that need cleanup after ffmpeg exits."""
    stream_key: str
    broadcast_id: Optional[str] = None
    stream_id: Optional[str] = None
    record_dir: Optional[Path] = None
    pipeline: object = None  # post_processor.Pipeline; opaque to avoid cycles
    title: str = ""
    transition_thread: object = None  # threading.Thread polling stream health
    transition_stop: object = None    # threading.Event
    extras: dict = field(default_factory=dict)


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


def _build_streamlink_cmd(channel: str, quality: str, hls_live_edge: int, buffer_mb: int) -> list[str]:
    twitch_url = f"https://twitch.tv/{channel}"
    return [
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


def _build_ffmpeg_cmd(session: CycleSession, reencode_audio: bool,
                      reencode_video: bool, video_bitrate_k: int,
                      segment_seconds: int) -> list[str]:
    """Construct ffmpeg command with one or two outputs.

    Output 1: FLV → YouTube RTMP (always).
    Output 2: segmented MP4 → session.record_dir (only if record_dir set).
    Two outputs are wired via the `tee` muxer so a single ffmpeg drives both.
    """
    youtube_url = f"{YOUTUBE_RTMP_INGEST}/{session.stream_key}"

    cmd: list[str] = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "+genpts+discardcorrupt+igndts",  # repair PTS, drop bad pkts, ignore decode TS
        "-err_detect", "ignore_err",                 # don't abort on minor decode errors
        "-thread_queue_size", "4096",
        "-i", "pipe:0",
    ]
    if reencode_video:
        cmd += [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-b:v", f"{video_bitrate_k}k",
            "-maxrate", f"{video_bitrate_k}k",
            "-bufsize", f"{video_bitrate_k * 2}k",
            "-g", "60",                              # keyframe every 2s @ 30fps (YouTube wants ≤2s)
            "-pix_fmt", "yuv420p",
        ]
    else:
        cmd += ["-c:v", "copy"]
    if reencode_audio:
        cmd += [
            "-c:a", "aac",
            "-b:a", "160k",
            "-ar", "48000",
            "-af", "aresample=async=1000",           # resample across audio PTS jumps -> kills beeps
        ]
    else:
        cmd += ["-c:a", "copy"]
    cmd += [
        "-max_muxing_queue_size", "1024",
    ]

    if session.record_dir is None:
        # Single FLV/RTMP output (legacy path).
        cmd += [
            "-flvflags", "no_duration_filesize",
            "-f", "flv",
            youtube_url,
        ]
    else:
        # Two outputs via tee: RTMP + segmented MP4.
        rec_pattern = str(session.record_dir / "seg_%05d.mp4").replace("\\", "/")
        # tee branch syntax: [opts]url|[opts]url
        # `onfail=ignore` on the RTMP branch prevents ffmpeg dying if YouTube
        # ingest hiccups; `f=segment` rotates files every segment_seconds.
        # `slash_escape` must be set if any branch contains pipes/colons; URLs
        # use only colons which are fine here.
        flv_branch = (
            f"[f=flv:onfail=ignore:flvflags=no_duration_filesize]{youtube_url}"
        )
        seg_branch = (
            f"[f=segment:segment_time={segment_seconds}:reset_timestamps=1"
            f":segment_format=mp4]{rec_pattern}"
        )
        # -map 0:v -map 0:a only — Twitch HLS now ships a `timed_id3`
        # data stream that FLV doesn't accept ("Data codec timed_id3 not
        # compatible with flv"). Selecting only audio+video drops it
        # cleanly so the FLV branch of the tee stays alive.
        cmd += [
            "-map", "0:v:0", "-map", "0:a:0",
            "-f", "tee",
            f"{flv_branch}|{seg_branch}",
        ]
    return cmd


def run_one_cycle(channel: str, session: CycleSession, quality: str,
                  hls_live_edge: int, buffer_mb: int,
                  reencode_audio: bool, reencode_video: bool,
                  video_bitrate_k: int, segment_seconds: int) -> int:
    """Spawn streamlink piped to ffmpeg, block until ffmpeg exits, return rc.

    Buffering: start `hls_live_edge` segments behind the live edge
    (Twitch segments are ~2s each, so 15 ≈ 30s lag) plus a `buffer_mb`
    ring buffer and parallel segment fetcher with retries.

    Re-encoding: audio re-encode (default ON) eliminates beeps/clicks at
    Twitch ad-break boundaries by resampling across PTS jumps. Video
    re-encode (default ON) smooths video timestamps at the same
    boundaries — disable only if passthrough is reliable for your source.
    """
    streamlink_cmd = _build_streamlink_cmd(channel, quality, hls_live_edge, buffer_mb)
    ffmpeg_cmd = _build_ffmpeg_cmd(
        session, reencode_audio, reencode_video, video_bitrate_k, segment_seconds
    )

    extras = []
    if session.broadcast_id:
        extras.append(f"broadcast={session.broadcast_id}")
    if session.record_dir:
        extras.append(f"record={session.record_dir.name}")
    extras_str = f" ({', '.join(extras)})" if extras else ""
    log.info("Starting pipe: streamlink (%s @ %s) -> ffmpeg -> YouTube%s",
             channel, quality, extras_str)
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


def _prepare_session(channel: str, static_key: str, use_dynamic: bool,
                     record_enabled: bool, record_root: Path, segment_seconds: int,
                     post_processor_enabled: bool) -> Optional[CycleSession]:
    """Probe Twitch (when creds present), mint a dynamic YouTube broadcast
    (when enabled), set up a recording session dir, and start the
    post-processor pipeline. Returns None if the channel is known offline."""
    from . import twitch_meta  # lazy import keeps startup fast

    title = ""
    description = ""
    if twitch_meta.has_credentials():
        info = twitch_meta.get_live_stream(channel)
        if info is None:
            log.info("Helix says %s is offline; skipping cycle", channel)
            return None
        title = (info.get("title") or "").strip()
        game = (info.get("game_name") or "").strip()
        # No URLs in description — accounts that can't post links would
        # otherwise have broadcasts blocked / descriptions stripped.
        if game:
            description = f"Game: {game}"
        else:
            description = ""
        log.info("Helix: %s is LIVE — title=%r, game=%r", channel, title, game)
    else:
        log.debug("No TWITCH_CLIENT_ID/SECRET; skipping Helix probe")

    stream_key = static_key
    broadcast_id: Optional[str] = None
    stream_id: Optional[str] = None
    if use_dynamic:
        try:
            from . import youtube_live, youtube_auth  # lazy import
            youtube_auth.ensure_credentials(interactive=False)  # raises if missing
        except Exception as e:
            log.warning("Dynamic broadcast disabled (%s); falling back to static stream key", e)
        else:
            try:
                broadcast_id, stream_id, stream_key = youtube_live.create_broadcast(
                    title=title or f"twitch.tv/{channel}",
                    description=description,
                    privacy=os.environ.get("YOUTUBE_BROADCAST_PRIVACY", "unlisted").strip() or "unlisted",
                )
            except Exception:
                log.exception("create_broadcast failed; falling back to static stream key")
                broadcast_id = None
                stream_id = None
                stream_key = static_key

    if not stream_key:
        log.warning("No YouTube stream key available (no static key, dynamic creation failed). Skipping cycle.")
        return None

    # If we minted a dynamic broadcast via the API, start a background
    # thread that waits for the stream to be healthy and then transitions
    # the broadcast to live. (API-created broadcasts often get stuck in
    # `liveStarting` and never go fully live to viewers — see notes in
    # youtube_live.wait_until_live.)
    #
    # If we're using the persistent Stream Now key (no broadcast_id), start
    # a thread that finds the auto-created broadcast (which YouTube spawns
    # when RTMP data hits the persistent key) and updates its title to
    # match the Twitch title.
    transition_thread = None
    transition_stop = None
    if broadcast_id and stream_id:
        import threading
        from . import youtube_live  # lazy
        transition_stop = threading.Event()

        def _runner(bid=broadcast_id, sid=stream_id, stop=transition_stop):
            try:
                youtube_live.wait_until_live(bid, sid, max_wait_s=300, poll_s=5,
                                             stop_event=stop)
            except Exception:
                log.exception("wait_until_live raised")

        transition_thread = threading.Thread(target=_runner, name="BroadcastTransitioner", daemon=True)
        transition_thread.start()
    elif title and not use_dynamic:
        # Stream Now mode — try to rename the auto-created broadcast.
        import threading
        try:
            from . import youtube_live, youtube_auth  # lazy
            youtube_auth.ensure_credentials(interactive=False)
        except Exception:
            log.debug("YouTube creds missing; skipping Stream-Now title sync")
        else:
            transition_stop = threading.Event()
            sync_title = title
            sync_desc = description

            def _runner(t=sync_title, d=sync_desc, stop=transition_stop):
                try:
                    bid = youtube_live.find_active_broadcast_for_default_stream(
                        max_wait_s=180, poll_s=5, stop_event=stop,
                    )
                    if bid:
                        log.info("Stream-Now auto-broadcast detected: %s", bid)
                        youtube_live.update_broadcast_title(bid, t, d)
                    else:
                        log.warning("No active broadcast found for Stream-Now key within 180s")
                except Exception:
                    log.exception("Stream-Now title sync raised")

            transition_thread = threading.Thread(target=_runner, name="StreamNowTitleSync", daemon=True)
            transition_thread.start()

    record_dir: Optional[Path] = None
    pipeline = None
    session_id = ""
    if record_enabled:
        from . import recorder  # lazy import (Phase 2 module)
        ctx = recorder.start_session(record_root, segment_seconds=segment_seconds, channel=channel, title=title)
        record_dir = ctx.dir
        session_id = ctx.session_id
        if post_processor_enabled:
            try:
                from . import post_processor  # lazy import (Phase 5 module)
                pipeline = post_processor.start(ctx, channel=channel)
            except Exception:
                log.exception("post_processor.start failed; continuing without topic pipeline")
                pipeline = None

    return CycleSession(
        stream_key=stream_key,
        broadcast_id=broadcast_id,
        stream_id=stream_id,
        record_dir=record_dir,
        pipeline=pipeline,
        title=title,
        transition_thread=transition_thread,
        transition_stop=transition_stop,
        extras={"session_id": session_id},
    )


def _teardown_session(session: CycleSession, drain_timeout: float) -> None:
    # Tell the transitioner to bail in case it's still polling.
    if session.transition_stop is not None:
        try:
            session.transition_stop.set()
        except Exception:
            pass
    if session.pipeline is not None:
        try:
            session.pipeline.stop_and_drain(drain_timeout)
        except Exception:
            log.exception("pipeline.stop_and_drain raised")
    if session.broadcast_id:
        try:
            from . import youtube_live  # lazy
            youtube_live.transition_broadcast(session.broadcast_id, "complete")
        except Exception:
            log.debug("transition_broadcast failed (best-effort)")
    if session.stream_id:
        try:
            from . import youtube_live  # lazy
            youtube_live.delete_stream(session.stream_id)
        except Exception:
            log.debug("delete_stream failed (best-effort)")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
    )
    _load_env()
    _require_binary("streamlink")
    _require_binary("ffmpeg")

    channel = os.environ.get("TWITCH_CHANNEL", "").strip()
    static_key = os.environ.get("YOUTUBE_STREAM_KEY", "").strip()
    quality = os.environ.get("RESTREAM_QUALITY", "best").strip() or "best"
    retry_seconds = _env_int("RESTREAM_RETRY_SECONDS", 30)
    # Twitch segments ≈ 2s each, so 15 ≈ 30s behind live edge.
    hls_live_edge = _env_int("RESTREAM_HLS_LIVE_EDGE", 15)
    buffer_mb = _env_int("RESTREAM_BUFFER_MB", 64)
    # Audio re-encode default ON: kills the beeps at ad-break boundaries
    # (resamples across audio PTS jumps), costs ~5% CPU.
    reencode_audio = _env_bool("RESTREAM_REENCODE_AUDIO", "1")
    # Video re-encode default ON: smooths video PTS across Twitch ad-break
    # boundaries (pairs with audio re-encode for max stability). Costs ~1
    # CPU core for libx264 veryfast 1080p. Set RESTREAM_REENCODE_VIDEO=0
    # to fall back to passthrough.
    reencode_video = _env_bool("RESTREAM_REENCODE_VIDEO", "1")
    video_bitrate_k = _env_int("RESTREAM_VIDEO_BITRATE_KBPS", 6000)

    use_dynamic = _env_bool("RESTREAM_USE_DYNAMIC_BROADCAST", "1")
    record_enabled = _env_bool("RESTREAM_RECORD_ENABLED", "1")
    segment_seconds = _env_int("RESTREAM_SEGMENT_SECONDS", 30)
    record_root = Path(os.environ.get("RESTREAM_RECORDINGS_DIR", "restream/recordings"))
    drain_timeout = float(_env_int("RESTREAM_DRAIN_TIMEOUT_SECONDS", 1800))
    post_processor_enabled = _env_bool("RESTREAM_POST_PROCESSOR_ENABLED", "1")

    if not channel:
        sys.exit("TWITCH_CHANNEL not set in .env")
    if not static_key and not use_dynamic:
        sys.exit("YOUTUBE_STREAM_KEY not set and RESTREAM_USE_DYNAMIC_BROADCAST=0")

    log.info("Restream daemon starting: twitch.tv/%s -> YouTube (quality=%s)",
             channel, quality)
    log.info("Buffering: ~%ds behind live edge (%d segments), %dMB ring buffer",
             hls_live_edge * 2, hls_live_edge, buffer_mb)
    log.info("Codecs: video=%s  audio=%s%s",
             "re-encode H.264 veryfast @ %dk" % video_bitrate_k if reencode_video else "passthrough",
             "re-encode AAC 160k (smooths ad-break beeps)" if reencode_audio else "passthrough",
             "" if reencode_audio else "  ⚠ beeps at ad boundaries are likely")
    log.info("Dynamic broadcast=%s  Record=%s  PostProcessor=%s",
             use_dynamic, record_enabled, post_processor_enabled)
    log.info("Retry interval when offline: %ds", retry_seconds)

    backoff = retry_seconds
    while True:
        session: Optional[CycleSession] = None
        try:
            session = _prepare_session(
                channel, static_key, use_dynamic, record_enabled,
                record_root, segment_seconds, post_processor_enabled,
            )
            if session is None:
                # Either Helix says offline or no usable stream key.
                rc = 1
            else:
                rc = run_one_cycle(channel, session, quality, hls_live_edge, buffer_mb,
                                   reencode_audio, reencode_video, video_bitrate_k,
                                   segment_seconds)
        except KeyboardInterrupt:
            log.info("Exiting on Ctrl-C")
            if session is not None:
                _teardown_session(session, drain_timeout=10)
            return
        except Exception:
            log.exception("Unexpected error in cycle")
            rc = -1

        if session is not None:
            _teardown_session(session, drain_timeout=drain_timeout)

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
