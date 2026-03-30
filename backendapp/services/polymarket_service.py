from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from backendapp.domains.constants import EVENT_HORIZON_HOURS, POLYMARKET_EVENTS_URL


def fetch_temperature_markets_payload() -> dict[str, Any]:
    horizon = datetime.now(timezone.utc) + timedelta(hours=EVENT_HORIZON_HOURS)
    params = {
        "tag_slug": "temperature",
        "closed": "false",
        "limit": 200,
        "end_date_max": horizon.isoformat(),
    }
    response = requests.get(POLYMARKET_EVENTS_URL, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    events = payload

    markets: list[dict[str, Any]] = []
    for event in events:
        event_id = event.get("id")
        event_title = event.get("title")
        event_slug = event.get("slug")
        event_description = event.get("description") or ""
        for market in event.get("markets", []):
            row = dict(market)
            row["event_id"] = event_id
            row["event_title"] = event_title
            row["event_slug"] = event_slug
            row["event_description"] = event_description
            markets.append(row)

    return {
        "market_count": len(markets),
        "markets": markets,
    }
