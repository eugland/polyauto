from __future__ import annotations

import re

DEFAULT_RUN_HOST = "127.0.0.1"
DEFAULT_RUN_PORT = 5000
DEFAULT_RUN_DEBUG = True
EVENT_HORIZON_HOURS = 30
POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"

EVENT_SLUG_RE = re.compile(r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$")
EVENT_DATE_IN_TITLE_RE = re.compile(
    r"\bon\s+([A-Za-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})\b", re.IGNORECASE
)
EVENT_DATE_IN_SLUG_RE = re.compile(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$")
