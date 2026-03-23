#!/usr/bin/env python3
"""
Fetch active Polymarket weather markets related to highest temperature bets.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

GAMMA_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
DEFAULT_LIMIT = 200
REQUEST_TIMEOUT_SECONDS = 20
DEFAULT_MAX_PAGES = 1
DEFAULT_QUERY = "temperature"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket weather markets for highest temperature bets."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Results per type for search calls (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximum pages to fetch before stopping (default: {DEFAULT_MAX_PAGES})",
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Include closed markets as well (default: active/open only)",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"Search keyword for market discovery (default: {DEFAULT_QUERY})",
    )
    return parser.parse_args()


def fetch_markets(
    page_limit: int,
    max_pages: int,
    include_closed: bool,
    query: str,
) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    page = 1

    page_count = 0
    while True:
        if max_pages > 0 and page_count >= max_pages:
            break

        params = {
            "q": query,
            "limit_per_type": page_limit,
            "page": page,
            "search_tags": "true",
        }
        params["keep_closed_markets"] = 1 if include_closed else 0

        print(f"Sending request: GET {GAMMA_SEARCH_URL} params={params}")
        response = requests.get(
            GAMMA_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        page_data = response.json()
        print(f"Received response: status={response.status_code} body:")
        print(json.dumps(page_data, indent=2))

        if not isinstance(page_data, dict):
            break

        page_events = page_data.get("events") or []
        page_markets: list[dict[str, Any]] = []
        for event in page_events:
            if not isinstance(event, dict):
                continue
            event_markets = event.get("markets") or []
            if isinstance(event_markets, list):
                page_markets.extend(m for m in event_markets if isinstance(m, dict))

        if not page_markets:
            break

        markets.extend(page_markets)
        page_count += 1

        # If search returned fewer results than requested, likely the last page.
        if len(page_events) < page_limit:
            break

        page += 1

    return markets


def get_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from get_strings(v)
    elif isinstance(value, list):
        for item in value:
            yield from get_strings(item)


def looks_like_weather_market(market: dict[str, Any]) -> bool:
    text_parts = [
        str(market.get("question", "")),
        str(market.get("title", "")),
        str(market.get("description", "")),
        str(market.get("slug", "")),
    ]

    tag_values = list(get_strings(market.get("tags", [])))
    text_parts.extend(tag_values)
    haystack = " ".join(text_parts).lower()

    weather_keywords = (
        "weather",
        "temperature",
        "high temp",
        "highest temp",
        "hottest",
    )
    return any(keyword in haystack for keyword in weather_keywords)


def looks_like_highest_temperature_market(market: dict[str, Any]) -> bool:
    text_parts = [
        str(market.get("question", "")),
        str(market.get("title", "")),
        str(market.get("description", "")),
        str(market.get("slug", "")),
    ]
    haystack = " ".join(text_parts).lower()

    target_phrases = (
        "highest temperature",
        "highest temp",
        "daily high temperature",
        "daily high temp",
        "high temperature",
        "high temp",
        "hottest",
        "max temperature",
        "maximum temperature",
    )
    return any(phrase in haystack for phrase in target_phrases)


def parse_end_date(raw: Any) -> str:
    if not raw:
        return "-"
    if isinstance(raw, str):
        # Keep output compact and stable.
        raw = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except ValueError:
            return str(raw)
    return str(raw)


def market_url(market: dict[str, Any]) -> str:
    slug = market.get("slug")
    if slug:
        return f"https://polymarket.com/event/{slug}"
    return "-"


def print_markets(markets: list[dict[str, Any]]) -> None:
    print(json.dumps(markets, indent=2))


def main() -> int:
    args = parse_args()
    try:
        all_markets = fetch_markets(
            page_limit=args.limit,
            max_pages=args.max_pages,
            include_closed=args.include_closed,
            query=args.query,
        )
    except requests.RequestException as exc:
        print(f"Failed to fetch Polymarket markets: {exc}", file=sys.stderr)
        return 1

    filtered = [m for m in all_markets if looks_like_highest_temperature_market(m)]

    # Deduplicate by market id (or slug fallback), preserving order.
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for market in filtered:
        key = str(market.get("id") or market.get("slug") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(market)

    print_markets(deduped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
