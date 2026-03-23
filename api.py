#!/usr/bin/env python3
"""
Fetch Polymarket temperature markets using typed DTOs (dataclasses).
"""

from __future__ import annotations

import argparse
import json
import sys

import requests

from clients.polymarket import fetch_markets, looks_like_highest_temperature_market
from data.dto import ApiMarket, MarketsOutput
from data.schema import write_schema

DEFAULT_LIMIT = 200
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
    parser.add_argument(
        "--schema-out",
        default="polymarket_markets.schema.json",
        help="Path to write inferred JSON Schema for the final structured output.",
    )
    parser.add_argument(
        "--no-schema",
        action="store_true",
        help="Disable schema generation.",
    )
    return parser.parse_args()


def dedupe_markets(markets: list[ApiMarket]) -> list[ApiMarket]:
    seen: set[str] = set()
    deduped: list[ApiMarket] = []
    for market in markets:
        key = str(market.id or market.slug or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(market)
    return deduped


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
    deduped = dedupe_markets(filtered)

    output_dto = MarketsOutput(
        query=args.query,
        include_closed=args.include_closed,
        market_count=len(deduped),
        markets=deduped,
    )
    output_payload = output_dto.to_dict()

    if not args.no_schema:
        write_schema(output_payload, args.schema_out)
        print(f"Wrote schema: {args.schema_out}", file=sys.stderr)

    print(json.dumps(output_payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

