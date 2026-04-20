"""Shared YouTube Data API credentials (OAuth installed-app flow).

One-time bootstrap (browser flow):
    python -m restream.youtube_live --auth

Token persisted to YOUTUBE_TOKEN_FILE (default restream/youtube_token.json).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("restream.youtube_auth")

# Combined scope: manage broadcasts + upload videos.
SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
]

_DEFAULT_CLIENT_FILE = "restream/youtube_oauth_client.json"
_DEFAULT_TOKEN_FILE = "restream/youtube_token.json"


def _client_secret_path() -> Path:
    return Path(os.environ.get("YOUTUBE_OAUTH_CLIENT_FILE", _DEFAULT_CLIENT_FILE))


def _token_path() -> Path:
    return Path(os.environ.get("YOUTUBE_TOKEN_FILE", _DEFAULT_TOKEN_FILE))


def _load_credentials():
    """Return google.oauth2.credentials.Credentials or None."""
    from google.oauth2.credentials import Credentials  # lazy import
    p = _token_path()
    if not p.exists():
        return None
    try:
        return Credentials.from_authorized_user_info(json.loads(p.read_text()), SCOPES)
    except Exception as e:
        log.warning("Failed to load YouTube token at %s: %s", p, e)
        return None


def _save_credentials(creds) -> None:
    p = _token_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(creds.to_json(), encoding="utf-8")
    log.info("Saved YouTube credentials to %s", p)


def ensure_credentials(interactive: bool = False):
    """Load (and refresh) cached credentials. If `interactive=True` and no
    valid credentials exist, run the browser OAuth flow.

    Returns Credentials, or raises RuntimeError if none available and
    interactive is False.
    """
    from google.auth.transport.requests import Request  # lazy import
    creds = _load_credentials()
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_credentials(creds)
            return creds
        except Exception as e:
            log.warning("Refresh failed (%s); re-authentication required", e)
            creds = None
    if not interactive:
        raise RuntimeError(
            "No valid YouTube credentials. Run: python -m restream.youtube_live --auth"
        )
    return _run_install_flow()


def _run_install_flow():
    from google_auth_oauthlib.flow import InstalledAppFlow  # lazy import
    cs = _client_secret_path()
    if not cs.exists():
        raise RuntimeError(
            f"OAuth client secret not found at {cs}. Download it from the GCP "
            f"console (OAuth 2.0 Client ID, type 'Desktop app') and save it there."
        )
    flow = InstalledAppFlow.from_client_secrets_file(str(cs), SCOPES)
    try:
        creds = flow.run_local_server(port=0, open_browser=True)
    except Exception as e:
        log.warning("Local-server flow failed (%s); falling back to console flow", e)
        # google-auth-oauthlib >= 1.0 removed run_console; emulate manually
        creds = flow.run_local_server(port=0, open_browser=False)
    _save_credentials(creds)
    return creds


def build_youtube_service():
    """Return a youtube_v3 service client built from cached creds."""
    from googleapiclient.discovery import build  # lazy import
    creds = ensure_credentials(interactive=False)
    return build("youtube", "v3", credentials=creds, cache_discovery=False)
