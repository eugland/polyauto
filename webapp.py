#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from flask import Flask, render_template

from app import fetch_temperature_tag_events

app = Flask(__name__)

DEFAULT_RUN_HOST = "127.0.0.1"
DEFAULT_RUN_PORT = 5000
DEFAULT_RUN_DEBUG = True
WEBAPP_CONFIG_PATH = Path(__file__).resolve().with_name("webapp.json")
EVENT_SLUG_RE = re.compile(r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$")
EVENT_DATE_IN_TITLE_RE = re.compile(
    r"\bon\s+([A-Za-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})\b", re.IGNORECASE
)
EVENT_DATE_IN_SLUG_RE = re.compile(r"-on-([a-z]+)-(\d{1,2})-(\d{4})$")


def build_output() -> dict[str, Any]:
    events = fetch_temperature_tag_events()
    markets: list[dict[str, Any]] = []
    for event in events:
        event_id = event.id
        event_title = event.title
        event_slug = event.slug
        for market in event.markets:
            row = market.to_dict()
            row["event_id"] = event_id
            row["event_title"] = event_title
            row["event_slug"] = event_slug
            markets.append(row)
    return {
        "market_count": len(markets),
        "markets": markets,
    }


def _parse_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return []
    return []


def _extract_first_number(text: str) -> float:
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


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _is_unbuyable_price(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return abs(float(value) - 0.9995) < 1e-12
    if isinstance(value, str):
        try:
            return abs(float(value.strip()) - 0.9995) < 1e-12
        except ValueError:
            return False
    return False


def _parse_local_time(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.strip())
    except ValueError:
        return None


def _format_local_time(raw: Any) -> str:
    dt = _parse_local_time(raw)
    if dt is None:
        return "-"
    return dt.strftime("%m-%d %I:%M%p").replace(" 0", " ")


def _local_offset_sort_value(raw: Any) -> int | None:
    dt = _parse_local_time(raw)
    if dt is None:
        return None
    offset = dt.utcoffset()
    if offset is None:
        return None
    return int(offset.total_seconds() // 60)


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

    offset_minutes = _local_offset_sort_value(group.get("local_time_now"))
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


def _read_webapp_config_payload() -> dict[str, Any]:
    if not WEBAPP_CONFIG_PATH.exists():
        return {}
    try:
        payload = json.loads(WEBAPP_CONFIG_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_int(raw: Any, default: int) -> int:
    if isinstance(raw, bool):
        return default
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str):
        try:
            return int(raw.strip())
        except ValueError:
            return default
    return default


def _load_webapp_settings(payload: dict[str, Any]) -> dict[str, Any]:
    raw_settings = payload.get("settings")
    settings = raw_settings if isinstance(raw_settings, dict) else {}
    return {
        "run_host": str(settings.get("run_host") or DEFAULT_RUN_HOST).strip() or DEFAULT_RUN_HOST,
        "run_port": _parse_int(settings.get("run_port"), DEFAULT_RUN_PORT),
        "run_debug": _is_true(settings.get("run_debug", DEFAULT_RUN_DEBUG)),
    }


def _load_webapp_mapping(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    def _normalize_source(item: dict[str, Any]) -> dict[str, str]:
        source_payload = item.get("source")
        source: dict[str, str] = {}
        if isinstance(source_payload, dict):
            for source_key, source_value in source_payload.items():
                source_name = str(source_key or "").strip().lower()
                source_url = str(source_value or "").strip()
                if source_name and source_url:
                    source[source_name] = source_url
        return source

    def _parse_offset_minutes(raw: Any) -> int | None:
        if isinstance(raw, bool):
            return None
        if isinstance(raw, (int, float)):
            return int(raw)
        if isinstance(raw, str):
            try:
                return int(raw.strip())
            except ValueError:
                return None
        return None

    out: dict[str, dict[str, Any]] = {}
    locations_payload = payload.get("locations")
    if not isinstance(locations_payload, list):
        return out

    for value in locations_payload:
        if not isinstance(value, dict):
            continue
        key = str(value.get("key") or "").strip().lower()
        if not key:
            continue
        out[key] = {
            "station": str(value.get("station") or "").strip(),
            "source": _normalize_source(value),
            "timezone": str(value.get("timezone") or "").strip(),
            "utc_offset_minutes": _parse_offset_minutes(value.get("utc_offset_minutes")),
        }
    return out


def _build_local_time_now(location_key: str, mapping: dict[str, dict[str, Any]]) -> str | None:
    info = mapping.get(location_key, {})
    timezone_name = str(info.get("timezone") or "").strip()
    offset_raw = info.get("utc_offset_minutes")
    offset_minutes: int | None = None
    if isinstance(offset_raw, (int, float)):
        offset_minutes = int(offset_raw)
    elif isinstance(offset_raw, str):
        try:
            offset_minutes = int(offset_raw.strip())
        except ValueError:
            offset_minutes = None

    if not timezone_name:
        if offset_minutes is None:
            print(f"[LOCALTIME] key={location_key} timezone=<missing> local_time=<none>")
            return None
        tz_offset = timezone(timedelta(minutes=offset_minutes))
        local_time = datetime.now(timezone.utc).astimezone(tz_offset).isoformat()
        print(
            f"[LOCALTIME] key={location_key} timezone=<missing> "
            f"fallback_offset_minutes={offset_minutes} local_time={local_time}"
        )
        return local_time
    try:
        local_time = datetime.now(ZoneInfo(timezone_name)).isoformat()
        print(f"[LOCALTIME] key={location_key} timezone={timezone_name} local_time={local_time}")
        return local_time
    except Exception as exc:
        if offset_minutes is None:
            print(
                f"[LOCALTIME] key={location_key} timezone={timezone_name} "
                f"error={exc} fallback_offset_minutes=<missing> local_time=<none>"
            )
            return None
        tz_offset = timezone(timedelta(minutes=offset_minutes))
        local_time = datetime.now(timezone.utc).astimezone(tz_offset).isoformat()
        print(
            f"[LOCALTIME] key={location_key} timezone={timezone_name} "
            f"error={exc} fallback_offset_minutes={offset_minutes} local_time={local_time}"
        )
        return local_time


def build_event_groups(
    payload: dict[str, Any],
    mapping: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    groups_map: dict[str, dict[str, Any]] = {}
    local_time_cache: dict[str, str | None] = {}
    for market in payload.get("markets", []):
        if not isinstance(market, dict):
            continue
        if _is_true(market.get("closed")):
            continue
        if market.get("active") is not None and not _is_true(market.get("active")):
            continue

        event_slug = str(market.get("event_slug") or "")
        event_key = event_slug or str(market.get("event_id") or market.get("id") or "")
        event_title = market.get("event_title") or "Unknown event"
        event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else "-"
        end_date = market.get("endDate") or "-"
        location_key = _event_location_key(event_slug)
        links = mapping.get(location_key, {})

        raw_source = links.get("source")
        source: dict[str, str] = {}
        if isinstance(raw_source, dict):
            for source_key, source_value in raw_source.items():
                source_name = str(source_key or "").strip().lower()
                source_url = str(source_value or "").strip()
                if source_name and source_url:
                    source[source_name] = source_url

        timezone_name = str(links.get("timezone") or "").strip()
        if location_key not in local_time_cache:
            local_time_cache[location_key] = _build_local_time_now(location_key, mapping)
        local_time_now = local_time_cache.get(location_key)

        if event_key not in groups_map:
            groups_map[event_key] = {
                "event_title": event_title,
                "event_slug": event_slug,
                "event_url": event_url,
                "end_date": end_date,
                "source": source,
                "timezone": timezone_name,
                "local_time_now": local_time_now,
                "local_time_display": _format_local_time(local_time_now),
                "event_date_sort_ordinal": _extract_event_date_ordinal(event_title, event_slug),
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
    print("Filtered results:")
    print(json.dumps(event_groups, indent=2))


@app.route("/", methods=["GET"])
def index() -> Any:
    event_groups: list[dict[str, Any]] = []
    market_count = 0
    error = ""

    try:
        config_payload = _read_webapp_config_payload()
        settings = _load_webapp_settings(config_payload)
        mapping = _load_webapp_mapping(config_payload)
        payload = build_output()
        event_groups = build_event_groups(payload, mapping)
        market_count = sum(len(group["selections"]) for group in event_groups)
        print_filtered_results(event_groups)
    except requests.RequestException as exc:
        error = f"API error: {exc}"
    except ValueError:
        error = "Invalid config values."

    return render_template(
        "index.html",
        event_groups=event_groups,
        market_count=market_count,
        error=error,
    )


if __name__ == "__main__":
    config_payload = _read_webapp_config_payload()
    settings = _load_webapp_settings(config_payload)
    app.run(
        host=settings["run_host"],
        port=settings["run_port"],
        debug=settings["run_debug"],
    )
