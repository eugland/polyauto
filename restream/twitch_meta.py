"""Twitch Helix client — fetches the live stream's current title.

Uses the client-credentials grant (app access token), cached in memory and
refreshed on 401 or after expiry. No user OAuth — this only reads public
stream metadata.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

import requests

log = logging.getLogger("restream.twitch_meta")

_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
_HELIX_STREAMS = "https://api.twitch.tv/helix/streams"

_lock = threading.Lock()
_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _creds() -> tuple[Optional[str], Optional[str]]:
    cid = (os.environ.get("TWITCH_CLIENT_ID") or "").strip() or None
    sec = (os.environ.get("TWITCH_CLIENT_SECRET") or "").strip() or None
    return cid, sec


def has_credentials() -> bool:
    cid, sec = _creds()
    return bool(cid and sec)


def get_app_token(force_refresh: bool = False) -> Optional[str]:
    """Return a valid app access token, refreshing if needed.

    Returns None if creds are not configured."""
    cid, sec = _creds()
    if not (cid and sec):
        return None
    with _lock:
        now = time.time()
        tok = _token_cache.get("access_token")
        exp = _token_cache.get("expires_at", 0.0)
        if tok and not force_refresh and now < exp - 30:
            return tok
        try:
            r = requests.post(
                _TOKEN_URL,
                params={
                    "client_id": cid,
                    "client_secret": sec,
                    "grant_type": "client_credentials",
                },
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.warning("Twitch token fetch failed: %s", e)
            return None
        _token_cache["access_token"] = data.get("access_token")
        # expires_in is seconds; default to 1h if missing.
        _token_cache["expires_at"] = now + float(data.get("expires_in", 3600))
        return _token_cache["access_token"]


def get_live_stream(channel: str) -> Optional[dict]:
    """Return live-stream metadata for `channel`, or None if offline / error.

    Returned dict fields: id, user_login, title, game_name, started_at,
    viewer_count, language, type. Returns None if Helix creds are missing or
    the channel is currently offline.
    """
    cid, _ = _creds()
    tok = get_app_token()
    if not (cid and tok):
        return None
    headers = {"Client-Id": cid, "Authorization": f"Bearer {tok}"}
    try:
        r = requests.get(
            _HELIX_STREAMS,
            params={"user_login": channel},
            headers=headers,
            timeout=10,
        )
        if r.status_code == 401:
            tok = get_app_token(force_refresh=True)
            if not tok:
                return None
            headers["Authorization"] = f"Bearer {tok}"
            r = requests.get(
                _HELIX_STREAMS,
                params={"user_login": channel},
                headers=headers,
                timeout=10,
            )
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning("Helix /streams failed for %s: %s", channel, e)
        return None
    data = (r.json() or {}).get("data") or []
    if not data:
        return None
    s = data[0]
    if (s.get("type") or "").lower() != "live":
        return None
    return s
