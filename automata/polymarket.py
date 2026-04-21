from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import re
import time
from typing import Any

import requests

EVENT_HORIZON_HOURS = 30
EVENTS_PAGE_SIZE = 10
EVENTS_MAX_PAGES = 20
POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
WEATHER_TAG_SLUGS = {"weather", "highest-temperature"}

# Gamma occasionally stalls under load; separate connect vs. read timeout and
# retry transient failures so a single slow response doesn't kill the caller.
GAMMA_CONNECT_TIMEOUT = 10
GAMMA_READ_TIMEOUT = 30
GAMMA_MAX_ATTEMPTS = 4
GAMMA_BACKOFF_BASE = 1.5

_log = logging.getLogger(__name__)


def _gamma_get(url: str, params: dict[str, Any]) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, GAMMA_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=(GAMMA_CONNECT_TIMEOUT, GAMMA_READ_TIMEOUT),
            )
            # Retry on 429 and 5xx; raise on other 4xx immediately.
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(
                    f"{resp.status_code} from Gamma", response=resp
                )
            resp.raise_for_status()
            return resp
        except (
            requests.Timeout,
            requests.ConnectionError,
            requests.HTTPError,
        ) as exc:
            last_exc = exc
            if attempt == GAMMA_MAX_ATTEMPTS:
                break
            delay = GAMMA_BACKOFF_BASE ** attempt
            _log.warning(
                "Gamma request failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt, GAMMA_MAX_ATTEMPTS, exc, delay,
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc

# Matches: highest-temperature-in-<city>-on-<month>-<day>-<year>
EVENT_SLUG_RE = re.compile(
    r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$",
    re.IGNORECASE,
)


def _end_date_max_iso() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=EVENT_HORIZON_HOURS)).isoformat()


def _event_tag_slugs(event: dict[str, Any]) -> set[str]:
    tags = event.get("tags") or []
    slugs: set[str] = set()
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                slug = str(tag.get("slug") or "").strip().lower()
                if slug:
                    slugs.add(slug)
    return slugs


def _is_temperature_market_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    return (
        "highest temperature" in t
        or t.startswith("highest-temperature-in-")
    )


def _event_is_temperature(event: dict[str, Any]) -> bool:
    fields = [
        str(event.get("title") or ""),
        str(event.get("slug") or ""),
        str(event.get("description") or ""),
    ]
    return any(_is_temperature_market_text(v) for v in fields)


def _market_is_temperature(market: dict[str, Any]) -> bool:
    fields = [
        str(market.get("question") or ""),
        str(market.get("groupItemTitle") or ""),
        str(market.get("slug") or ""),
        str(market.get("title") or ""),
    ]
    return any(_is_temperature_market_text(v) for v in fields)


def fetch_temperature_markets_payload() -> dict[str, Any]:
    """
    Fetch open temperature events from Gamma and flatten to a markets list.
    Returns: {"market_count": int, "markets": list[dict]}.
    """
    end_date_max_iso = _end_date_max_iso()
    events: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()

    # Query using both tag slugs known to classify these weather markets, then
    # filter by tags[].slug for correctness because tag_slug fields may be null.
    for query_tag in ("weather", "highest-temperature"):
        for page_index in range(EVENTS_MAX_PAGES):
            offset = page_index * EVENTS_PAGE_SIZE
            resp = _gamma_get(
                POLYMARKET_EVENTS_URL,
                params={
                    "tag_slug": query_tag,
                    "closed": "false",
                    "limit": EVENTS_PAGE_SIZE,
                    "offset": offset,
                    "end_date_max": end_date_max_iso,
                },
            )
            page_events = resp.json()
            if not isinstance(page_events, list) or not page_events:
                break

            for event in page_events:
                tag_slugs = _event_tag_slugs(event)
                if not WEATHER_TAG_SLUGS.intersection(tag_slugs):
                    continue
                if not _event_is_temperature(event):
                    continue
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
            if not _market_is_temperature(market):
                continue
            markets.append({
                **market,
                "event_id": event_id,
                "event_title": event_title,
                "event_slug": event_slug,
                "event_description": event_description,
            })

    return {"market_count": len(markets), "markets": markets}
