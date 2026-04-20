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


def find_active_broadcast_for_default_stream(*, max_wait_s: float = 90.0,
                                              poll_s: float = 5.0,
                                              stop_event=None) -> Optional[str]:
    """For the 'persistent stream key' (Stream Now) mode: poll the channel's
    active broadcasts and return the broadcast_id of the one that just went
    live as a result of our RTMP push. Returns None if nothing comes up.

    Identification heuristic: we pick the most-recently-created `active`
    broadcast on the channel. Stream Now mints exactly one when ingest
    begins, so this is reliable in practice.
    """
    import time as _time
    deadline = _time.time() + max_wait_s
    last_err = None
    while _time.time() < deadline:
        if stop_event is not None and stop_event.is_set():
            return None
        try:
            yt = youtube_auth.build_youtube_service()
            r = yt.liveBroadcasts().list(
                part="id,snippet,status",
                broadcastStatus="active",
                maxResults=10,
            ).execute()
            items = r.get("items") or []
            if items:
                items.sort(
                    key=lambda b: b.get("snippet", {}).get("publishedAt", ""),
                    reverse=True,
                )
                return items[0]["id"]
        except Exception as e:
            last_err = e
        _time.sleep(poll_s)
    if last_err:
        log.debug("find_active_broadcast_for_default_stream gave up: %s", last_err)
    return None


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


def update_broadcast_title(broadcast_id: str, title: str, description: Optional[str] = None) -> None:
    if not broadcast_id:
        return
    try:
        yt = youtube_auth.build_youtube_service()
        # Read current snippet so we preserve all required fields (YouTube
        # treats `update` as a full replacement of the snippet part).
        cur = yt.liveBroadcasts().list(part="snippet", id=broadcast_id).execute()
        items = cur.get("items") or []
        if not items:
            log.warning("update_broadcast_title: broadcast %s not found", broadcast_id)
            return
        cur_snip = items[0]["snippet"]
        snippet = {
            "title": (title or cur_snip.get("title") or "Live stream")[:100],
            "description": (description if description is not None else cur_snip.get("description", ""))[:4900],
        }
        # Only re-send scheduledStartTime when the broadcast is in a state that
        # accepts modifying it (i.e. before it starts). After ingest begins,
        # YouTube rejects any presence of the field with
        # `scheduledStartTimeModificationNotAllowed`, even if the value is
        # unchanged. Best-effort: omit by default; retry with it on failure.
        try:
            yt.liveBroadcasts().update(
                part="snippet", body={"id": broadcast_id, "snippet": snippet}
            ).execute()
            log.info("Updated broadcast %s title -> %s", broadcast_id, snippet["title"])
            return
        except Exception as e1:
            # Retry once including scheduledStartTime, in case the broadcast
            # is still in `created`/`ready` and the field is required.
            log.debug("update without scheduledStartTime failed (%s); retrying with it", e1)
            snippet["scheduledStartTime"] = cur_snip.get("scheduledStartTime", _now_iso())
            yt.liveBroadcasts().update(
                part="snippet", body={"id": broadcast_id, "snippet": snippet}
            ).execute()
            log.info("Updated broadcast %s title -> %s", broadcast_id, snippet["title"])
    except Exception as e:
        log.warning("update_broadcast_title failed: %s", e)


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
