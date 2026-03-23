#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request as flask_request

from api import dedupe_markets
from clients.polymarket import fetch_markets, looks_like_highest_temperature_market
from data.dto import MarketsOutput

app = Flask(__name__)

DEFAULT_QUERY = "temperature"
DEFAULT_LIMIT = 50
DEFAULT_MAX_PAGES = 1
DEFAULT_INCLUDE_CLOSED = False
ACCUWEATHER_BASE = "https://www.accuweather.com/en/cn"
ACCUWEATHER_TIMEOUT_SECONDS = 20
LOCATION_PATHS: dict[str, str] = {
    "shanghai": "shanghai-pudong-international-airport/1804_poi/weather-forecast/1804_poi",
}


def build_output(
    query: str, limit: int, max_pages: int, include_closed: bool
) -> dict[str, Any]:
    all_markets = fetch_markets(
        page_limit=limit,
        max_pages=max_pages,
        include_closed=include_closed,
        query=query,
    )
    filtered = [m for m in all_markets if looks_like_highest_temperature_market(m)]
    deduped = dedupe_markets(filtered)
    output = MarketsOutput(
        query=query,
        include_closed=include_closed,
        market_count=len(deduped),
        markets=deduped,
    )
    return output.to_dict()


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


def _parse_iso_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_within_next_24_hours(end_date: Any) -> bool:
    end_dt = _parse_iso_datetime(end_date)
    if end_dt is None:
        return False
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=24)
    return now <= end_dt <= horizon


def build_event_groups(payload: dict[str, Any]) -> list[dict[str, Any]]:
    groups_map: dict[str, dict[str, Any]] = {}
    for market in payload.get("markets", []):
        if not isinstance(market, dict):
            continue
        # Only show currently tradable/open selections in the UI.
        if _is_true(market.get("closed")):
            continue
        if market.get("active") is not None and not _is_true(market.get("active")):
            continue
        if not _is_within_next_24_hours(market.get("endDate")):
            continue

        event_slug = str(market.get("event_slug") or "")
        event_key = event_slug or str(market.get("event_id") or market.get("id") or "")
        event_title = market.get("event_title") or "Unknown event"
        event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else "-"
        end_date = market.get("endDate") or "-"

        if event_key not in groups_map:
            groups_map[event_key] = {
                "event_title": event_title,
                "event_url": event_url,
                "end_date": end_date,
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
                "status": "Closed" if market.get("closed") else "Open",
                "last_price": market.get("lastTradePrice", "-"),
                "volume": market.get("volumeNum", market.get("volume", "-")),
                "yes_price": yes_price,
                "no_price": no_price,
            }
        )

    groups = [g for g in groups_map.values() if g["selections"]]
    for group in groups:
        group["selections"].sort(
            key=lambda s: _selection_sort_key(str(s.get("selection", "")))
        )
    groups.sort(key=lambda g: str(g.get("event_title", "")))
    return groups


def print_filtered_results(event_groups: list[dict[str, Any]]) -> None:
    print("Filtered results:")
    print(json.dumps(event_groups, indent=2))


def parse_accuweather_keywords() -> list[str]:
    if flask_request.method == "POST":
        payload = flask_request.get_json(silent=True) or {}
        raw = payload.get("keywords")
        if isinstance(raw, list):
            return [str(x).strip().lower() for x in raw if str(x).strip()]
        if isinstance(raw, str):
            return [p.strip().lower() for p in raw.split(",") if p.strip()]

    keyword = (flask_request.args.get("keyword") or "").strip().lower()
    keywords = (flask_request.args.get("keywords") or "").strip().lower()
    items: list[str] = []
    if keyword:
        items.append(keyword)
    if keywords:
        items.extend([p.strip() for p in keywords.split(",") if p.strip()])
    return items


def html_to_text(html: str) -> str:
    no_script = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    no_style = re.sub(r"<style[\s\S]*?</style>", " ", no_script, flags=re.IGNORECASE)
    no_tags = re.sub(r"<[^>]+>", " ", no_style)
    return re.sub(r"\s+", " ", no_tags).strip()


def extract_first_temperature_f(text: str) -> int | None:
    patterns = [
        r"Today's Weather[^.]{0,220}?Hi:\s*(-?\d+)\s*°",
        r"Current Weather[^.]{0,220}?(-?\d+)\s*°\s*F",
        r"10-Day Weather Forecast[^.]{0,400}?(-?\d+)\s*°",
        r"(-?\d+)\s*°\s*F",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def fetch_accuweather_first_temperature(keyword: str, path: str) -> dict[str, Any]:
    url = f"{ACCUWEATHER_BASE}/{path}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=ACCUWEATHER_TIMEOUT_SECONDS)
    response.raise_for_status()
    text = html_to_text(response.text)
    first_temp_f = extract_first_temperature_f(text)
    return {
        "keyword": keyword,
        "url": url,
        "temperature_f_first_entry": first_temp_f,
    }


@app.route("/", methods=["GET"])
def index() -> Any:
    event_groups: list[dict[str, Any]] = []
    market_count = 0
    error = ""

    try:
        payload = build_output(
            query=DEFAULT_QUERY,
            limit=DEFAULT_LIMIT,
            max_pages=DEFAULT_MAX_PAGES,
            include_closed=DEFAULT_INCLUDE_CLOSED,
        )
        event_groups = build_event_groups(payload)
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


@app.route("/api/markets", methods=["GET"])
def markets_api() -> Any:
    try:
        payload = build_output(
            query=DEFAULT_QUERY,
            limit=DEFAULT_LIMIT,
            max_pages=DEFAULT_MAX_PAGES,
            include_closed=DEFAULT_INCLUDE_CLOSED,
        )
        print_filtered_results(build_event_groups(payload))
        return jsonify(payload)
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502


@app.route("/api/accuweather/temperature", methods=["GET", "POST"])
def accuweather_temperature_api() -> Any:
    keywords = parse_accuweather_keywords()
    if not keywords:
        return (
            jsonify(
                {
                    "error": "Provide `keyword` or `keywords` query param, or JSON body `keywords`.",
                    "examples": [
                        "/api/accuweather/temperature?keyword=shanghai",
                        "/api/accuweather/temperature?keywords=shanghai,beijing",
                    ],
                }
            ),
            400,
        )

    unknown_keywords: list[str] = []
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for keyword in keywords:
        path = LOCATION_PATHS.get(keyword)
        if not path:
            unknown_keywords.append(keyword)
            continue
        try:
            results.append(fetch_accuweather_first_temperature(keyword, path))
        except requests.RequestException as exc:
            errors.append(
                {
                    "keyword": keyword,
                    "url": f"{ACCUWEATHER_BASE}/{path}",
                    "error": str(exc),
                }
            )

    return jsonify(
        {
            "requested_keywords": keywords,
            "resolved_count": len(results),
            "unknown_keywords": unknown_keywords,
            "results": results,
            "errors": errors,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
