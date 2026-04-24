"""Twitch -> YouTube restreamer with session persistence across restarts."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import sysconfig
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream")

YOUTUBE_RTMP_INGEST = "rtmp://a.rtmp.youtube.com/live2"

# Persistent session state, so Ctrl-C + restart can reuse the same YouTube
# broadcast instead of minting a new one each time.
SESSION_FILE = Path(__file__).resolve().parent / ".session.json"

# A cycle shorter than this is treated as a failure for backoff purposes.
# Anything >= this is treated as "made progress" and resets the streak.
MIN_STABLE_SECONDS = 30
# Cap on the exponential backoff between reconnect attempts.
MAX_RECONNECT_DELAY = 300
# YouTube lifeCycleStatus values that allow RTMP push to resume. Anything
# else (complete, revoked, ...) means we must mint a new broadcast.
REUSABLE_BROADCAST_STATES = frozenset({"created", "ready", "testing", "live"})

# Prepended to the description of every minted YouTube broadcast.
DESCRIPTION_PREAMBLE = (
    "Twitch is a reactionary, anti-worker platform. It punishes real worker "
    "voices like Hasan while promoting reactionary, Epstein-class slop like "
    "Asmongold. YouTube isn't perfect either, but at least it doesn't pretend "
    "to be some kind of moral authority.\n\n"
    "My corp doesn't allow Twitch, so I watch Hasan here on YouTube. Hopefully "
    "one day he can stream here directly.  This stream is for those who need "
    "an easier way to watch on YouTube — not monetized."
)
# YouTube's maximum description length, minus a small safety margin.
# `youtube_live.create_broadcast` also clips to 4900, but we clip here so
# the preamble is preserved even if the Twitch bio is very long.
MAX_DESCRIPTION_CHARS = 4900

# Prepended to every broadcast title. `create_broadcast` clips the final
# title to 100 chars, so the Twitch-side title may lose trailing chars.
TITLE_PREFIX = "[Fanstream] "


@dataclass
class CycleSession:
    stream_key: str
    broadcast_id: str
    stream_id: str
    transition_thread: Optional[threading.Thread] = None
    transition_stop: Optional[threading.Event] = None


def _load_env() -> None:
    """Tiny .env loader (no python-dotenv dep). Reads restream/.env,
    isolated from the repo-root .env used by the Polymarket bots."""
    env_file = Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.split("#", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), val)


def _candidate_script_dirs() -> list[str]:
    """Dirs to probe for pip-installed CLIs (streamlink) when they aren't
    on PATH. Covers:
      - the current interpreter's scripts dir (works for non-venv installs)
      - every Python installation under %USERPROFILE%\\AppData\\Local\\Programs\\Python\\*
        (covers the common case of running from a venv while the CLI is
        pip-installed into the base per-user Python)
    """
    out: list[str] = []
    cur = sysconfig.get_path("scripts")
    if cur:
        out.append(cur)
    userprofile = os.environ.get("USERPROFILE", "")
    if userprofile:
        base = Path(userprofile) / "AppData" / "Local" / "Programs" / "Python"
        if base.is_dir():
            for sub in sorted(base.iterdir(), reverse=True):  # newest-looking first
                scripts = sub / "Scripts"
                if scripts.is_dir():
                    out.append(str(scripts))
    return out


def _require_binary(name: str) -> None:
    """Resolve `name` on PATH; if missing, also probe known fallback dirs
    and prepend the resolving dir to PATH so subprocesses inherit it.
    Exits on failure."""
    if shutil.which(name) is not None:
        return
    for cand in _candidate_script_dirs():
        if shutil.which(name, path=cand) is not None:
            os.environ["PATH"] = cand + os.pathsep + os.environ.get("PATH", "")
            log.info("Added %s to PATH to resolve `%s`", cand, name)
            return
    sys.exit(f"`{name}` not found on PATH. On Ubuntu: sudo apt install -y {name}")


def _build_streamlink_cmd(channel: str, quality: str, hls_live_edge: int, buffer_mb: int) -> list[str]:
    return [
        "streamlink",
        "--stdout",
        "--hls-live-restart",
        "--hls-live-edge", str(hls_live_edge),
        "--ringbuffer-size", f"{buffer_mb}M",
        "--stream-segment-attempts", "5",
        "--stream-segment-timeout", "10",
        "--stream-segment-threads", "3",
        f"https://twitch.tv/{channel}",
        quality,
    ]


def _build_ffmpeg_cmd(stream_key: str, reencode_audio: bool,
                      reencode_video: bool, video_bitrate_k: int) -> list[str]:
    youtube_url = f"{YOUTUBE_RTMP_INGEST}/{stream_key}"
    cmd: list[str] = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "+genpts+discardcorrupt+igndts",  # repair PTS, drop bad pkts, ignore decode TS
        "-err_detect", "ignore_err",
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
    # Select only audio+video — Twitch HLS ships a `timed_id3` data stream
    # that FLV doesn't accept.
    cmd += [
        "-max_muxing_queue_size", "1024",
        "-map", "0:v:0", "-map", "0:a:0",
        "-flvflags", "no_duration_filesize",
        "-f", "flv",
        youtube_url,
    ]
    return cmd


def _compose_description(channel: str, info: dict) -> str:
    """Build the YouTube broadcast description:
        <preamble>
        <Twitch channel 'About' bio, if any>
        Category: <current game>
    The whole thing is clipped to MAX_DESCRIPTION_CHARS; preamble never loses
    — the bio gets truncated first."""
    from . import twitch_meta  # lazy

    preamble = DESCRIPTION_PREAMBLE
    game = (info.get("game_name") or "").strip()
    game_line = f"\n\nCategory: {game}" if game else ""

    # Budget whatever is left after preamble + game line for the Twitch bio.
    fixed = preamble + game_line
    budget = MAX_DESCRIPTION_CHARS - len(fixed) - 4  # spacer + ellipsis
    bio = twitch_meta.get_channel_description(channel) or ""
    if bio and budget > 20:
        if len(bio) > budget:
            bio = bio[:budget - 1].rstrip() + "…"
        body = preamble + "\n\n" + bio + game_line
    else:
        body = fixed
    return body[:MAX_DESCRIPTION_CHARS]


def _mint_broadcast(channel: str, info: dict, privacy: str,
                    latency: str = "normal") -> CycleSession:
    """Create a fresh bound YouTube broadcast+stream for a live Twitch session
    and start the background transitioner. `info` is the Helix /streams payload.
    `latency` is one of 'normal' | 'low' | 'ultraLow'."""
    from . import youtube_live, youtube_auth  # lazy imports

    raw_title = (info.get("title") or "").strip() or f"twitch.tv/{channel}"
    title = TITLE_PREFIX + raw_title
    description = _compose_description(channel, info)

    youtube_auth.ensure_credentials(interactive=False)  # raises if missing
    broadcast_id, stream_id, stream_key = youtube_live.create_broadcast(
        title=title, description=description, privacy=privacy, latency=latency,
    )
    log.info("YouTube watch:  https://www.youtube.com/watch?v=%s", broadcast_id)
    log.info("YouTube studio: https://studio.youtube.com/video/%s/livestreaming",
             broadcast_id)

    # API-created broadcasts often get stuck in `liveStarting` and never go
    # fully live to viewers — poll stream health in a background thread and
    # push the `live` transition as soon as it goes active+good.
    transition_stop = threading.Event()

    def _runner(bid=broadcast_id, sid=stream_id, stop=transition_stop):
        try:
            youtube_live.wait_until_live(bid, sid, max_wait_s=300, poll_s=5,
                                         stop_event=stop)
        except Exception:
            log.exception("wait_until_live raised")

    transition_thread = threading.Thread(
        target=_runner, name="BroadcastTransitioner", daemon=True,
    )
    transition_thread.start()

    return CycleSession(
        stream_key=stream_key,
        broadcast_id=broadcast_id,
        stream_id=stream_id,
        transition_thread=transition_thread,
        transition_stop=transition_stop,
    )


def run_one_cycle(channel: str, session: CycleSession, quality: str,
                  hls_live_edge: int, buffer_mb: int,
                  reencode_audio: bool, reencode_video: bool,
                  video_bitrate_k: int) -> int:
    """Spawn streamlink piped to ffmpeg, block until ffmpeg exits, return rc."""
    streamlink_cmd = _build_streamlink_cmd(channel, quality, hls_live_edge, buffer_mb)
    ffmpeg_cmd = _build_ffmpeg_cmd(
        session.stream_key, reencode_audio, reencode_video, video_bitrate_k,
    )

    log.info("Starting pipe: streamlink (%s @ %s) -> ffmpeg -> YouTube (broadcast=%s)",
             channel, quality, session.broadcast_id)
    sl = subprocess.Popen(streamlink_cmd, stdout=subprocess.PIPE)
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


def _teardown_session(session: CycleSession) -> None:
    if session.transition_stop is not None:
        try:
            session.transition_stop.set()
        except Exception:
            pass
    from . import youtube_live  # lazy
    try:
        youtube_live.transition_broadcast(session.broadcast_id, "complete")
    except Exception:
        log.debug("transition_broadcast failed (best-effort)")
    try:
        youtube_live.delete_stream(session.stream_id)
    except Exception:
        log.debug("delete_stream failed (best-effort)")


def _save_session(session: CycleSession, channel: str) -> None:
    """Write the broadcast/stream identifiers so a future run of
    `python -m restream` can resume this broadcast instead of minting a
    new one. Overwrites any existing file."""
    try:
        SESSION_FILE.write_text(json.dumps({
            "broadcast_id": session.broadcast_id,
            "stream_id": session.stream_id,
            "stream_key": session.stream_key,
            "channel": channel,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2), encoding="utf-8")
        log.debug("Saved session state -> %s", SESSION_FILE)
    except Exception:
        log.exception("Failed to save session file")


def _load_session_file() -> Optional[tuple[CycleSession, str]]:
    """Return (session, channel) for a persisted session, or None if the
    file is missing/unreadable. The returned session has no background
    transitioner — call _spawn_transitioner() after verifying it's still
    reusable on YouTube's side."""
    if not SESSION_FILE.exists():
        return None
    try:
        data = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
        session = CycleSession(
            stream_key=data["stream_key"],
            broadcast_id=data["broadcast_id"],
            stream_id=data["stream_id"],
        )
        return session, data.get("channel", "")
    except Exception:
        log.exception("Failed to load session file %s; ignoring", SESSION_FILE)
        return None


def _clear_session_file() -> None:
    try:
        SESSION_FILE.unlink(missing_ok=True)
    except Exception:
        log.debug("Failed to remove %s", SESSION_FILE, exc_info=True)


def _spawn_transitioner(session: CycleSession) -> None:
    """Start the background thread that flips broadcast `ready/testing -> live`
    once YouTube reports the ingest stream as active+good. Safe to call on
    an already-live broadcast — wait_until_live short-circuits."""
    from . import youtube_live  # lazy

    if session.transition_stop is not None:
        return
    stop = threading.Event()

    def _runner(bid=session.broadcast_id, sid=session.stream_id, s=stop):
        try:
            youtube_live.wait_until_live(bid, sid, max_wait_s=300, poll_s=5,
                                         stop_event=s)
        except Exception:
            log.exception("wait_until_live raised")

    t = threading.Thread(target=_runner, name="BroadcastTransitioner", daemon=True)
    t.start()
    session.transition_thread = t
    session.transition_stop = stop


def _handle_status_command() -> int:
    """`python -m restream --status`: print broadcast+stream health for the
    persisted session, then exit. Does not affect the running daemon."""
    loaded = _load_session_file()
    if loaded is None:
        print(f"No persisted session ({SESSION_FILE} not found).")
        return 0
    session, channel = loaded
    from . import youtube_live
    s = youtube_live.get_detailed_status(session.broadcast_id, session.stream_id)
    print(f"channel:          {channel or '?'}")
    print(f"broadcast:        {session.broadcast_id}")
    print(f"  title:          {s.get('broadcast_title', '?')}")
    print(f"  state:          {s.get('broadcast_status', '?')}")
    print(f"  watch:          https://www.youtube.com/watch?v={session.broadcast_id}")
    print(f"  studio:         https://studio.youtube.com/video/{session.broadcast_id}/livestreaming")
    print(f"stream:           {session.stream_id}")
    print(f"  state:          {s.get('stream_status', '?')}")
    print(f"  health:         {s.get('health', '?')}")
    issues = s.get("configuration_issues") or []
    if issues:
        print("  issues:")
        for it in issues:
            sev = it.get("severity", "?")
            typ = it.get("type", "?")
            reason = it.get("reason", "")
            print(f"    - [{sev}] {typ}: {reason}")
    else:
        print("  issues:         (none)")
    if "error" in s:
        print(f"api error:        {s['error']}")
    return 0


def _handle_shutdown_command() -> int:
    """`python -m restream --shutdown`: end any persisted broadcast and exit."""
    loaded = _load_session_file()
    if loaded is None:
        log.info("No saved session to shut down (%s not found)", SESSION_FILE)
        return 0
    session, channel = loaded
    log.info("Shutting down persisted broadcast %s (channel=%s)",
             session.broadcast_id, channel or "?")
    _teardown_session(session)
    _clear_session_file()
    log.info("Shutdown complete")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="restream")
    parser.add_argument(
        "--shutdown", action="store_true",
        help="End the persisted YouTube broadcast (if any) and exit. "
             "Use this when you're done with the current broadcast — "
             "Ctrl-C alone preserves it for the next restart.",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Print the current health of the persisted broadcast (from "
             "YouTube's side) and exit. Safe to run alongside a running daemon.",
    )
    encode_group = parser.add_mutually_exclusive_group()
    encode_group.add_argument(
        "--passthrough", action="store_true",
        help="Disable ffmpeg re-encode entirely (-c:v copy -c:a copy). "
             "Zero CPU, but expect beep artifacts at Twitch ad boundaries.",
    )
    encode_group.add_argument(
        "--full-reencode", action="store_true",
        help="Force full re-encode (-c:v libx264 + -c:a aac). Highest CPU, "
             "maximum stability at Twitch ad-break PTS jumps. Use this if "
             "you see visible video glitches with the default.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
    )
    _load_env()

    if args.shutdown:
        sys.exit(_handle_shutdown_command())
    if args.status:
        sys.exit(_handle_status_command())

    _require_binary("streamlink")
    _require_binary("ffmpeg")

    from . import config  # lazy — resolves env > config.toml > default
    channel = config.get_str("TWITCH_CHANNEL", "twitch_channel", "").strip()
    quality = config.get_str("RESTREAM_QUALITY", "quality", "best").strip() or "best"
    offline_poll_seconds = config.get_int("RESTREAM_RETRY_SECONDS", "retry_seconds", 30)
    reconnect_seconds = config.get_int("RESTREAM_RECONNECT_SECONDS", "reconnect_seconds", 5)
    hls_live_edge = config.get_int("RESTREAM_HLS_LIVE_EDGE", "hls_live_edge", 15)
    buffer_mb = config.get_int("RESTREAM_BUFFER_MB", "buffer_mb", 64)
    reencode_audio = config.get_bool("RESTREAM_REENCODE_AUDIO", "reencode_audio", True)
    reencode_video = config.get_bool("RESTREAM_REENCODE_VIDEO", "reencode_video", True)
    video_bitrate_k = config.get_int("RESTREAM_VIDEO_BITRATE_KBPS", "video_bitrate_kbps", 6000)
    if args.passthrough:
        reencode_audio = False
        reencode_video = False
        log.warning("--passthrough: forwarding raw Twitch packets "
                    "(-c:v copy -c:a copy). Expect beep artifacts at ad breaks.")
    elif args.full_reencode:
        reencode_audio = True
        reencode_video = True
        log.info("--full-reencode: libx264 + AAC (max CPU, max stability)")
    privacy = config.get_str("YOUTUBE_BROADCAST_PRIVACY", "youtube_broadcast_privacy", "public").strip() or "public"
    latency = config.get_str("YOUTUBE_LATENCY", "youtube_latency", "normal").strip() or "normal"
    if latency not in ("normal", "low", "ultraLow"):
        log.warning("YOUTUBE_LATENCY=%r invalid; falling back to 'normal'", latency)
        latency = "normal"

    if not channel:
        sys.exit("TWITCH_CHANNEL not set in .env")
    from . import twitch_meta  # lazy
    if not twitch_meta.has_credentials():
        sys.exit(
            "TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET not set in .env — "
            "required for Twitch liveness probe"
        )

    log.info("Restream daemon: twitch.tv/%s -> YouTube (quality=%s, privacy=%s, latency=%s)",
             channel, quality, privacy, latency)
    log.info("Buffering: ~%ds behind live edge (%d segments), %dMB ring buffer",
             hls_live_edge * 2, hls_live_edge, buffer_mb)
    log.info("Codecs: video=%s  audio=%s",
             "re-encode H.264 veryfast @ %dk" % video_bitrate_k if reencode_video else "passthrough",
             "re-encode AAC 160k" if reencode_audio else "passthrough")
    log.info("Offline poll: %ds   Reconnect delay: %ds",
             offline_poll_seconds, reconnect_seconds)

    from . import youtube_live  # lazy

    # Load a persisted session if the previous run was Ctrl-C'd. If it's
    # still usable on YouTube's side, we'll reconnect to it instead of
    # minting a new broadcast.
    session: Optional[CycleSession] = None
    loaded = _load_session_file()
    if loaded is not None:
        saved_session, saved_channel = loaded
        if saved_channel and saved_channel != channel:
            log.warning("Saved session was for channel %r but current is %r; "
                        "tearing down the old broadcast",
                        saved_channel, channel)
            _teardown_session(saved_session)
            _clear_session_file()
        else:
            status = youtube_live.get_broadcast_status(saved_session.broadcast_id)
            if status in REUSABLE_BROADCAST_STATES:
                log.info("Resuming persisted broadcast %s (status=%s)",
                         saved_session.broadcast_id, status)
                log.info("YouTube watch:  https://www.youtube.com/watch?v=%s",
                         saved_session.broadcast_id)
                log.info("YouTube studio: https://studio.youtube.com/video/%s/livestreaming",
                         saved_session.broadcast_id)
                _spawn_transitioner(saved_session)
                session = saved_session
            else:
                log.info("Persisted broadcast %s is in state %r; discarding",
                         saved_session.broadcast_id, status)
                _clear_session_file()

    logged_offline = False
    consecutive_failures = 0
    try:
        while True:
            # 1. Probe Twitch liveness.
            try:
                info = twitch_meta.get_live_stream(channel)
            except Exception:
                log.exception("Helix probe raised; treating as offline")
                info = None

            # 2. Twitch offline — end broadcast if one is live, then idle-poll.
            if info is None:
                if session is not None:
                    log.info("Twitch %s went offline — tearing down broadcast %s",
                             channel, session.broadcast_id)
                    _teardown_session(session)
                    _clear_session_file()
                    session = None
                    consecutive_failures = 0
                    logged_offline = True
                elif not logged_offline:
                    log.info("Helix: %s is offline; polling every %ds",
                             channel, offline_poll_seconds)
                    logged_offline = True
                time.sleep(offline_poll_seconds)
                continue

            # 3. Twitch is live.
            logged_offline = False

            # 3a. If we think we already have a broadcast, verify it's still
            #     reusable on YouTube's side before pushing to it. If YouTube
            #     has force-ended it (long outage → `complete`/`revoked`), we
            #     discard the session and mint a new one below.
            if session is not None:
                status = youtube_live.get_broadcast_status(session.broadcast_id)
                if status is not None and status not in REUSABLE_BROADCAST_STATES:
                    log.warning("Broadcast %s is in terminal state %r; "
                                "minting a new broadcast",
                                session.broadcast_id, status)
                    _teardown_session(session)
                    _clear_session_file()
                    session = None
                    consecutive_failures = 0

            if session is None:
                title = (info.get("title") or "").strip() or f"twitch.tv/{channel}"
                game = (info.get("game_name") or "").strip()
                log.info("Helix: %s LIVE — title=%r, game=%r", channel, title, game)
                try:
                    session = _mint_broadcast(channel, info, privacy, latency)
                    _save_session(session, channel)
                except Exception:
                    log.exception("Failed to mint YouTube broadcast; retrying in %ds",
                                  offline_poll_seconds)
                    time.sleep(offline_poll_seconds)
                    continue
            else:
                log.info("Twitch still live — reconnecting to existing broadcast %s",
                         session.broadcast_id)

            # 4. Run the pipe to completion. When ffmpeg exits we do NOT
            #    teardown — the next iteration's Helix probe + status check
            #    decides whether to reuse, mint new, or teardown.
            cycle_start = time.monotonic()
            rc = run_one_cycle(
                channel, session, quality, hls_live_edge, buffer_mb,
                reencode_audio, reencode_video, video_bitrate_k,
            )
            cycle_duration = time.monotonic() - cycle_start

            if rc == 0:
                log.info("Pipe exited cleanly after %.0fs; re-probing Twitch",
                         cycle_duration)
                consecutive_failures = 0
            elif cycle_duration >= MIN_STABLE_SECONDS:
                # The pipe ran long enough to count as healthy. Any drop now
                # is likely transient (network blip) — reset the streak.
                log.warning("Pipe exited rc=%d after %.0fs; reconnecting in %ds",
                            rc, cycle_duration, reconnect_seconds)
                consecutive_failures = 0
            else:
                # Pipe died fast — something is wrong. Back off exponentially
                # so we don't hammer YouTube RTMP / Twitch on a loop.
                consecutive_failures += 1
                log.warning("Pipe exited rc=%d after only %.0fs (failure #%d); "
                            "backing off", rc, cycle_duration, consecutive_failures)

            # Sleep: base reconnect delay, doubled per consecutive rapid
            # failure, capped at MAX_RECONNECT_DELAY.
            if consecutive_failures == 0:
                delay = reconnect_seconds
            else:
                delay = min(reconnect_seconds * (2 ** consecutive_failures),
                            MAX_RECONNECT_DELAY)
            log.info("Sleeping %ds before next iteration", delay)
            time.sleep(delay)
    except KeyboardInterrupt:
        log.info("Exiting on Ctrl-C")
    except Exception:
        log.exception("Fatal error in main loop")
    finally:
        # Stop the transitioner thread but DO NOT tear down the broadcast.
        # The session file persists so the next `python -m restream` can
        # reconnect to the same broadcast at the same watch URL. To end
        # the broadcast explicitly, run `python -m restream --shutdown`.
        if session is not None:
            if session.transition_stop is not None:
                try:
                    session.transition_stop.set()
                except Exception:
                    pass
            log.info("Broadcast %s preserved. Watch: https://www.youtube.com/watch?v=%s",
                     session.broadcast_id, session.broadcast_id)
            log.info("Restart `python -m restream` to reconnect, or "
                     "`python -m restream --shutdown` to end the broadcast.")
