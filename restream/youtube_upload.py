"""YouTube Data API v3 video upload (videos.insert, resumable).

Resumable upload retries on transient errors. Quota: ~1600 units per call.
"""
from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path
from typing import Optional

from . import youtube_auth

log = logging.getLogger("restream.youtube_upload")

_MAX_RETRIES = 5
_RETRIABLE_STATUSES = {500, 502, 503, 504}


def upload_video(path: Path, title: str, description: str = "",
                 tags: Optional[list[str]] = None, privacy: str = "unlisted",
                 category_id: str = "22") -> str:
    """Upload `path` to YouTube; return video_id.

    category_id 22 = "People & Blogs" (broad default for stream cuts).
    """
    from googleapiclient.http import MediaFileUpload  # lazy
    from googleapiclient.errors import HttpError  # lazy

    yt = youtube_auth.build_youtube_service()
    body = {
        "snippet": {
            "title": (title or "Untitled")[:100].replace("<", "(").replace(">", ")"),
            "description": (description or "")[:4900],
            "tags": (tags or [])[:25],
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(str(path), chunksize=4 * 1024 * 1024, resumable=True,
                            mimetype="video/mp4")
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    retry = 0
    while response is None:
        try:
            status, response = req.next_chunk()
            if status:
                log.info("Upload %s: %d%%", path.name, int(status.progress() * 100))
        except HttpError as e:
            if e.resp.status in _RETRIABLE_STATUSES and retry < _MAX_RETRIES:
                retry += 1
                sleep = (2 ** retry) + random.random()
                log.warning("Upload %s transient error %s; retrying in %.1fs",
                            path.name, e.resp.status, sleep)
                time.sleep(sleep)
                continue
            log.exception("Upload %s failed: %s", path.name, e)
            raise
        except Exception as e:
            if retry < _MAX_RETRIES:
                retry += 1
                sleep = (2 ** retry) + random.random()
                log.warning("Upload %s error %s; retrying in %.1fs",
                            path.name, type(e).__name__, sleep)
                time.sleep(sleep)
                continue
            raise
    vid = response.get("id", "")
    log.info("Uploaded %s -> https://youtu.be/%s (privacy=%s)", path.name, vid, privacy)
    return vid
