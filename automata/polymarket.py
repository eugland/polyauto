from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any

import requests

EVENT_HORIZON_HOURS = 30
EVENTS_PAGE_SIZE = 10
EVENTS_MAX_PAGES = 20
POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# Matches: highest-temperature-in-<city>-on-<month>-<day>-<year>
EVENT_SLUG_RE = re.compile(
    r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$",
    re.IGNORECASE,
)


def _end_date_max_iso() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=EVENT_HORIZON_HOURS)).isoformat()


def fetch_temperature_markets_payload() -> dict[str, Any]:
    """
    Fetch open temperature events from Gamma and flatten to a markets list.
    Returns: {"market_count": int, "markets": list[dict]}.
    """
    end_date_max_iso = _end_date_max_iso()
    events: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()

    for page_index in range(EVENTS_MAX_PAGES):
        offset = page_index * EVENTS_PAGE_SIZE
        resp = requests.get(
            POLYMARKET_EVENTS_URL,
            params={
                "tag_slug": "temperature",
                "closed": "false",
                "limit": EVENTS_PAGE_SIZE,
                "offset": offset,
                "end_date_max": end_date_max_iso,
            },
            timeout=15,
        )
        resp.raise_for_status()
        page_events = resp.json()
        if not isinstance(page_events, list) or not page_events:
            break

        for event in page_events:
            key = str(event.get("id") or "")
            if key and key in seen_event_ids:
                continue
            if key:
                seen_event_ids.add(key)
            events.append(event)

        if len(page_events) < EVENTS_PAGE_SIZE:
            break

    markets: list[dict[str, Any]] = []
    for event in events:
        event_id = event.get("id")
        event_title = event.get("title")
        event_slug = event.get("slug")
        event_description = event.get("description")
        for market in (event.get("markets") or []):
            if not isinstance(market, dict):
                continue
            markets.append({
                **market,
                "event_id": event_id,
                "event_title": event_title,
                "event_slug": event_slug,
                "event_description": event_description,
            })

    return {"market_count": len(markets), "markets": markets}
