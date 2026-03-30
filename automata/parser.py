from __future__ import annotations

import json
import re
from typing import Any

from backendapp.domains.constants import EVENT_SLUG_RE
from automata.models import Market, ParsedMarket

# "95°F or higher" | "50°F or below" | "30°C or higher"
THRESHOLD_RE = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+(or\s+higher|or\s+below|or\s+lower)",
    re.IGNORECASE,
)

# "90-91°F" | "28-30°C"
RANGE_RE = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\b",
    re.IGNORECASE,
)

# "12°C" | "72°F"
EXACT_RE = re.compile(
    r"^\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s*$",
    re.IGNORECASE,
)


def _parse_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return []


def _extract_no_token_id(market: dict[str, Any]) -> str | None:
    outcomes = _parse_json_list(market.get("outcomes"))
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    for i, name in enumerate(outcomes):
        if str(name).strip().lower() == "no" and i < len(token_ids):
            return str(token_ids[i])
    return None


def _extract_yes_token_id(market: dict[str, Any]) -> str | None:
    outcomes = _parse_json_list(market.get("outcomes"))
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    for i, name in enumerate(outcomes):
        if str(name).strip().lower() == "yes" and i < len(token_ids):
            return str(token_ids[i])
    return None


def _extract_no_price(market: dict[str, Any]) -> float | None:
    outcomes = _parse_json_list(market.get("outcomes"))
    prices = _parse_json_list(market.get("outcomePrices"))
    for i, name in enumerate(outcomes):
        if str(name).strip().lower() == "no" and i < len(prices):
            try:
                return float(prices[i])
            except (TypeError, ValueError):
                return None
    return None


def _parse_threshold(text: str) -> tuple[float, float | None, str, str] | None:
    m = THRESHOLD_RE.search(text)
    if m:
        value = float(m.group(1))
        unit = m.group(2).upper()
        direction = "higher" if "higher" in m.group(3).lower() else "below"
        return value, None, unit, direction

    m = RANGE_RE.search(text)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper(), "range"

    m = EXACT_RE.search(text)
    if m:
        return float(m.group(1)), None, m.group(2).upper(), "exact"

    return None


def parse_markets(
    raw_markets: list[dict[str, Any]],
    mapping: dict,
) -> list[ParsedMarket]:
    results: list[ParsedMarket] = []

    for raw in raw_markets:
        if raw.get("closed"):
            continue
        if raw.get("active") is not None and not raw.get("active"):
            continue

        event_slug = str(raw.get("event_slug") or "")
        slug_match = EVENT_SLUG_RE.match(event_slug)
        if not slug_match:
            continue

        location_key = slug_match.group(1)
        location_cfg = mapping.get(location_key)
        if not location_cfg or not location_cfg.enabled:
            continue

        question = str(raw.get("groupItemTitle") or raw.get("question") or "")
        parsed = _parse_threshold(question)
        if not parsed:
            continue

        no_token_id = _extract_no_token_id(raw)
        no_price = _extract_no_price(raw)
        if no_token_id is None or no_price is None:
            continue

        market = Market(
            market_id=str(raw.get("id") or ""),
            question=question,
            event_slug=event_slug,
            no_token_id=no_token_id,
            no_price=no_price,
        )

        lo, hi, unit, direction = parsed
        results.append(ParsedMarket(
            market=market,
            location_key=location_key,
            threshold_lo=lo,
            threshold_hi=hi,
            unit=unit,
            direction=direction,
        ))

    return results
