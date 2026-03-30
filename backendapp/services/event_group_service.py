from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from backendapp.domains.constants import EVENT_DATE_IN_SLUG_RE, EVENT_DATE_IN_TITLE_RE, EVENT_SLUG_RE
from backendapp.domains.models import LocationConfig
from backendapp.services.timezone_service import (
    build_local_time_now,
    format_local_time,
    local_offset_sort_value,
)


def _parse_json_list(raw: Any) -> list[Any]:
    return json.loads(raw) if raw else []


def _extract_first_number(text: str) -> float:
    import re

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return float("inf")
    return float(match.group(0))


def _selection_sort_key(selection: str) -> tuple[int, float, str]:
    normalized = selection.strip().lower()
    if "or below" in normalized:
        category = 0
    elif "or higher" in normalized:
        category = 2
    else:
        category = 1
    return (category, _extract_first_number(normalized), normalized)


def _is_unbuyable_price(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return abs(float(value) - 0.9995) < 1e-12
    if isinstance(value, str):
        try:
            return abs(float(value.strip()) - 0.9995) < 1e-12
        except ValueError:
            return False
    return False


def _parse_month_number(value: str) -> int | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    for fmt in ("%B", "%b"):
        try:
            return datetime.strptime(cleaned, fmt).month
        except ValueError:
            continue
    return None


def _extract_event_date_ordinal(event_title: Any, event_slug: Any) -> int | None:
    title = str(event_title or "").strip()
    slug = str(event_slug or "").strip().lower()

    match = EVENT_DATE_IN_TITLE_RE.search(title)
    if match:
        month_name, day_raw, year_raw = match.groups()
        month = _parse_month_number(month_name)
        if month is not None:
            try:
                return date(int(year_raw), int(month), int(day_raw)).toordinal()
            except ValueError:
                pass

    match = EVENT_DATE_IN_SLUG_RE.search(slug)
    if match:
        month_name, day_raw, year_raw = match.groups()
        month = _parse_month_number(month_name)
        if month is not None:
            try:
                return date(int(year_raw), int(month), int(day_raw)).toordinal()
            except ValueError:
                pass

    return None


def _event_group_sort_key(group: dict[str, Any]) -> tuple[int, int, int, int, str]:
    event_date_ordinal = group.get("event_date_sort_ordinal")
    if isinstance(event_date_ordinal, int):
        date_sort = event_date_ordinal
        date_missing = 0
    else:
        date_sort = 0
        date_missing = 1

    offset_minutes = local_offset_sort_value(group.get("local_time_now"))
    if offset_minutes is None:
        return (date_missing, date_sort, 1, 0, str(group.get("event_title", "")))
    return (date_missing, date_sort, 0, -offset_minutes, str(group.get("event_title", "")))


def _event_location_key(event_slug: str) -> str:
    slug = str(event_slug or "").strip().lower()
    match = EVENT_SLUG_RE.match(slug)
    if match:
        return match.group(1)
    return ""


def _parse_price_to_cents(value: Any) -> str:
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{as_float * 100:.1f}c"


def _format_int_volume(value: Any) -> str:
    if isinstance(value, bool):
        return "-"
    if isinstance(value, (int, float)):
        return f"{int(round(float(value))):,}"
    if isinstance(value, str):
        try:
            return f"{int(round(float(value.strip()))):,}"
        except ValueError:
            return value
    return "-"


def _preferred_weather_units(location_cfg: LocationConfig | None) -> str:
    if location_cfg is None:
        return "m"
    accuweather_url = str(location_cfg.source.get("accuweather", "")).lower()
    if "/en/us/" in accuweather_url:
        return "e"
    return "m"


def build_event_groups(
    payload: dict[str, Any],
    mapping: dict[str, LocationConfig],
) -> list[dict[str, Any]]:
    groups_map: dict[str, dict[str, Any]] = {}
    local_time_cache: dict[str, str | None] = {}
    for market in payload.get("markets", []):
        if market.get("closed"):
            continue
        if market.get("active") is not None and not market.get("active"):
            continue

        event_slug = str(market.get("event_slug") or "")
        event_key = event_slug or str(market.get("event_id") or market.get("id") or "")
        event_title = market.get("event_title") or "Unknown event"
        event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else "-"
        end_date = market.get("endDate") or "-"
        location_key = _event_location_key(event_slug)
        location_cfg = mapping.get(location_key)
        has_location_config = location_cfg is not None

        source = dict(location_cfg.source) if location_cfg is not None else {}
        timezone_name = location_cfg.timezone if location_cfg is not None else ""
        station_code = location_cfg.station if location_cfg is not None else ""
        weather_units = _preferred_weather_units(location_cfg)

        if location_key not in local_time_cache:
            local_time_cache[location_key] = build_local_time_now(location_key, mapping)
        local_time_now = local_time_cache.get(location_key)

        if event_key not in groups_map:
            groups_map[event_key] = {
                "event_title": event_title,
                "event_slug": event_slug,
                "event_url": event_url,
                "end_date": end_date,
                "source": source,
                "station_code": station_code,
                "weather_units": weather_units,
                "timezone": timezone_name,
                "local_time_now": local_time_now,
                "local_time_display": format_local_time(local_time_now),
                "event_date_sort_ordinal": _extract_event_date_ordinal(event_title, event_slug),
                "is_secondary": not has_location_config,
                "selections": [],
            }

        outcomes = _parse_json_list(market.get("outcomes"))
        outcome_prices = _parse_json_list(market.get("outcomePrices"))
        if any(_is_unbuyable_price(price) for price in outcome_prices):
            continue

        paired_outcomes: list[dict[str, Any]] = []
        max_len = max(len(outcomes), len(outcome_prices))
        for i in range(max_len):
            name = outcomes[i] if i < len(outcomes) else f"Outcome {i + 1}"
            price = outcome_prices[i] if i < len(outcome_prices) else "-"
            paired_outcomes.append({"name": name, "price": price})

        yes_price: Any = "-"
        no_price: Any = "-"
        for outcome in paired_outcomes:
            name = str(outcome.get("name", "")).strip().lower()
            if name == "yes":
                yes_price = outcome.get("price", "-")
            elif name == "no":
                no_price = outcome.get("price", "-")

        groups_map[event_key]["selections"].append(
            {
                "selection": market.get("groupItemTitle") or market.get("question") or "-",
                "volume": _format_int_volume(market.get("volumeNum", market.get("volume", "-"))),
                "yes_price": _parse_price_to_cents(yes_price),
                "no_price": _parse_price_to_cents(no_price),
            }
        )

    groups = [g for g in groups_map.values() if g["selections"]]
    for group in groups:
        group["selections"].sort(
            key=lambda s: _selection_sort_key(str(s.get("selection", "")))
        )
    groups.sort(key=_event_group_sort_key)
    return groups


def print_filtered_results(event_groups: list[dict[str, Any]]) -> None:
    for group in event_groups:
        event_title = str(group.get("event_title") or "-")
        print(f"Event: {event_title}")
        # selections = group.get("selections", [])
        # if not selections:
        #     print("  Choices: -")
        #     continue
        # choice_text = ", ".join(str(item.get("selection") or "-") for item in selections)
        # print(f"  Choices: {choice_text}")
