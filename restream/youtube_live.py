"""YouTube live broadcast / stream management via the YouTube Data API v3.

Mints a fresh broadcast + stream per Twitch session, binds them, and returns
the RTMP stream key for ffmpeg to push to. Also handles transitions and
cleanup of completed broadcasts.

CLI bootstrap (one time, opens browser):
    python -m restream.youtube_live --auth
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from . import youtube_auth

log = logging.getLogger("restream.youtube_live")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def create_broadcast(
    title: str,
    description: str = "",
    privacy: str = "unlisted",
    *,
    enable_auto_start: bool = False,   # we transition manually for reliability
    enable_auto_stop: bool = False,
    enable_dvr: bool = True,
    record_from_start: bool = True,
    latency: str = "low",
) -> tuple[str, str, str]:
    """Create a fresh live broadcast + stream, bind them, return identifiers.

    Returns (broadcast_id, stream_id, stream_key). The stream_key is the
    RTMP key to push to rtmp://a.rtmp.youtube.com/live2/<key>.
    """
    yt = youtube_auth.build_youtube_service()

    # Sanitize title — YouTube max 100 chars, no <, >.
    title = (title or "Live stream")[:100].replace("<", "(").replace(">", ")")
    description = (description or "")[:4900]

    bc_body = {
        "snippet": {
            "title": title,
            "description": description,
            "scheduledStartTime": _now_iso(),
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
        "contentDetails": {
            "enableAutoStart": enable_auto_start,
            "enableAutoStop": enable_auto_stop,
            "enableDvr": enable_dvr,
            "recordFromStart": record_from_start,
            "latencyPreference": latency,
            "monitorStream": {"enableMonitorStream": False},
        },
    }
    bc = yt.liveBroadcasts().insert(
        part="snippet,status,contentDetails", body=bc_body
    ).execute()
    broadcast_id = bc["id"]
    log.info("Created broadcast %s (%s, privacy=%s)", broadcast_id, title, privacy)

    stream_body = {
        "snippet": {"title": f"{title} — ingest"},
        "cdn": {
            "frameRate": "variable",
            "ingestionType": "rtmp",
            "resolution": "variable",
        },
        "contentDetails": {"isReusable": False},
    }
    st = yt.liveStreams().insert(part="snippet,cdn,contentDetails", body=stream_body).execute()
    stream_id = st["id"]
    stream_key = st["cdn"]["ingestionInfo"]["streamName"]
    log.info("Created stream %s", stream_id)

    yt.liveBroadcasts().bind(
        part="id,contentDetails", id=broadcast_id, streamId=stream_id
    ).execute()
    log.info("Bound broadcast %s -> stream %s", broadcast_id, stream_id)

    return broadcast_id, stream_id, stream_key


def get_broadcast_status(broadcast_id: str) -> Optional[str]:
    """Return lifeCycleStatus of the broadcast, or None on error / not found.

    Possible values: created, ready, testing, live, complete, revoked.
    `complete` and `revoked` mean the broadcast is terminal and cannot be
    resumed — caller must mint a new one.
    """
    if not broadcast_id:
        return None
    try:
        yt = youtube_auth.build_youtube_service()
        r = yt.liveBroadcasts().list(part="status", id=broadcast_id).execute()
        items = r.get("items") or []
        if not items:
            return None
        return (items[0].get("status") or {}).get("lifeCycleStatus")
    except Exception as e:
        log.debug("get_broadcast_status(%s) failed: %s", broadcast_id, e)
        return None


def get_detailed_status(broadcast_id: str, stream_id: str) -> dict:
    """Fetch broadcast + stream status for human display. Keys:
    broadcast_status, broadcast_title, stream_status, health,
    configuration_issues (list of {severity, type, reason, lastUpdateTimeSeconds}),
    error (only on API failure)."""
    out: dict = {"broadcast_id": broadcast_id, "stream_id": stream_id}
    try:
        yt = youtube_auth.build_youtube_service()
        if broadcast_id:
            bc = yt.liveBroadcasts().list(part="status,snippet", id=broadcast_id).execute()
            items = bc.get("items") or []
            if items:
                out["broadcast_status"] = (items[0].get("status") or {}).get("lifeCycleStatus")
                out["broadcast_title"] = (items[0].get("snippet") or {}).get("title")
        if stream_id:
            st = yt.liveStreams().list(part="status", id=stream_id).execute()
            items = st.get("items") or []
            if items:
                s = items[0].get("status") or {}
                out["stream_status"] = s.get("streamStatus")
                health = s.get("healthStatus") or {}
                out["health"] = health.get("status")
                out["configuration_issues"] = health.get("configurationIssues") or []
    except Exception as e:
        out["error"] = str(e)
    return out


def get_stream_health(stream_id: str) -> tuple[Optional[str], Optional[str]]:
    """Return (streamStatus, healthStatus.status) for a liveStream resource."""
    if not stream_id:
        return None, None
    try:
        yt = youtube_auth.build_youtube_service()
        r = yt.liveStreams().list(part="status", id=stream_id).execute()
        items = r.get("items") or []
        if not items:
            return None, None
        st = items[0].get("status") or {}
        return st.get("streamStatus"), (st.get("healthStatus") or {}).get("status")
    except Exception as e:
        log.debug("get_stream_health(%s) failed: %s", stream_id, e)
        return None, None


def wait_until_live(broadcast_id: str, stream_id: str, *,
                    max_wait_s: float = 300.0, poll_s: float = 5.0,
                    stop_event=None) -> bool:
    """Poll stream health and transition the broadcast to live as soon as
    YouTube reports the stream is active. Returns True on success.

    Lifecycle expected: created -> ready (immediate) -> testing (after we
    push the first transition) -> live (after we push the second transition).

    With enableMonitorStream=False, you can transition straight ready -> live
    without going through testing. We try that path first; if YouTube rejects
    it (e.g. monitor stream is enabled), we fall back to ready -> testing
    -> live.
    """
    import time as _time
    if not (broadcast_id and stream_id):
        return False
    # If the broadcast is already `live` (we're reusing from a previous run),
    # skip the transition handshake entirely.
    if get_broadcast_status(broadcast_id) == "live":
        log.info("Broadcast %s already live; skipping transition", broadcast_id)
        return True
    deadline = _time.time() + max_wait_s
    healthy_seen = False
    while _time.time() < deadline:
        if stop_event is not None and stop_event.is_set():
            return False
        ss, hs = get_stream_health(stream_id)
        log.debug("Stream %s: status=%s health=%s", stream_id, ss, hs)
        if ss == "active" and hs in ("good", "ok"):
            healthy_seen = True
            break
        _time.sleep(poll_s)
    if not healthy_seen:
        log.warning("Stream %s never reached active+good within %.0fs",
                    stream_id, max_wait_s)
        return False

    # Try direct ready -> live first.
    if _try_transition(broadcast_id, "live"):
        log.info("Broadcast %s transitioned to live", broadcast_id)
        return True
    # Fall back: ready -> testing -> live.
    if _try_transition(broadcast_id, "testing"):
        # Give YouTube a moment to settle in testing.
        _time.sleep(3)
        if _try_transition(broadcast_id, "live"):
            log.info("Broadcast %s transitioned testing -> live", broadcast_id)
            return True
    log.warning("Broadcast %s failed to transition to live", broadcast_id)
    return False


def _try_transition(broadcast_id: str, status: str) -> bool:
    try:
        yt = youtube_auth.build_youtube_service()
        yt.liveBroadcasts().transition(
            broadcastStatus=status, id=broadcast_id, part="status"
        ).execute()
        return True
    except Exception as e:
        log.debug("transition(%s, %s) -> %s", broadcast_id, status, e)
        return False


def transition_broadcast(broadcast_id: str, status: str) -> None:
    """Transition broadcast to one of: testing, live, complete.

    With enableAutoStart/enableAutoStop=True, YouTube handles testing→live
    and live→complete automatically when the RTMP feed starts/stops, so this
    is mostly best-effort cleanup."""
    if not broadcast_id:
        return
    try:
        yt = youtube_auth.build_youtube_service()
        yt.liveBroadcasts().transition(
            broadcastStatus=status, id=broadcast_id, part="status"
        ).execute()
        log.info("Transitioned broadcast %s -> %s", broadcast_id, status)
    except Exception as e:
        log.debug("transition_broadcast(%s, %s) failed: %s", broadcast_id, status, e)


def delete_stream(stream_id: str) -> None:
    if not stream_id:
        return
    try:
        yt = youtube_auth.build_youtube_service()
        yt.liveStreams().delete(id=stream_id).execute()
    except Exception as e:
        log.debug("delete_stream(%s) failed: %s", stream_id, e)


def _cli_auth() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
    youtube_auth.ensure_credentials(interactive=True)
    log.info("Auth OK. Token saved.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="YouTube live broadcast helpers")
    parser.add_argument("--auth", action="store_true", help="Run one-time OAuth bootstrap")
    args = parser.parse_args()
    if args.auth:
        return _cli_auth()
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
