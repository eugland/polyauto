#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request as flask_request

from api import dedupe_markets
from clients.openweather import (
    Coordinates,
    LocationQuery,
    fetch_current_weather,
    fetch_current_weather_by_coordinates,
    fetch_intraday_forecast,
    get_openweather_api_key,
    resolve_location_coordinates,
    resolve_location_query,
)
from clients.polymarket import fetch_markets, looks_like_highest_temperature_market
from data.dto import MarketsOutput

app = Flask(__name__)

DEFAULT_QUERY = "temperature"
DEFAULT_LIMIT = 50
DEFAULT_MAX_PAGES = 1
DEFAULT_INCLUDE_CLOSED = False


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


def parse_weather_keywords() -> list[str]:
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


def parse_coordinate(value: str | None, name: str) -> float:
    if value is None or not value.strip():
        raise ValueError(f"Missing required query parameter '{name}'.")
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Query parameter '{name}' must be a number.") from exc


def resolve_request_coordinates() -> tuple[Coordinates, str]:
    location = (flask_request.args.get("location") or "").strip()
    if location:
        return resolve_location_coordinates(location), "location"

    location_key = (flask_request.args.get("location_key") or "").strip()
    if location_key:
        return resolve_location_coordinates(location_key), "location"

    return (
        Coordinates(
            lat=parse_coordinate(flask_request.args.get("lat"), "lat"),
            lon=parse_coordinate(flask_request.args.get("lon"), "lon"),
        ),
        "coordinates",
    )


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


@app.route("/api/openweather/temperature", methods=["GET", "POST"])
@app.route("/api/accuweather/temperature", methods=["GET", "POST"])
def weather_temperature_api() -> Any:
    keywords = parse_weather_keywords()
    if not keywords:
        return (
            jsonify(
                {
                    "error": "Provide `keyword` or `keywords` query param, or JSON body `keywords`.",
                    "examples": [
                        "/api/openweather/temperature?keyword=shanghai",
                        "/api/openweather/temperature?keywords=shanghai,beijing",
                    ],
                }
            ),
            400,
        )

    unknown_keywords: list[str] = []
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    try:
        api_key = get_openweather_api_key()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    for keyword in keywords:
        query = resolve_location_query(keyword)
        if not query:
            unknown_keywords.append(keyword)
            continue
        try:
            location = LocationQuery(keyword=keyword, query=query)
            results.append(fetch_current_weather(location, api_key))
        except (requests.RequestException, ValueError) as exc:
            errors.append(
                {
                    "keyword": keyword,
                    "query": query,
                    "error": str(exc),
                }
            )

    return jsonify(
        {
            "provider": "openweather",
            "requested_keywords": keywords,
            "resolved_count": len(results),
            "unknown_keywords": unknown_keywords,
            "results": results,
            "errors": errors,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/api/openweather/by-coords", methods=["GET"])
def weather_by_coordinates_api() -> Any:
    try:
        api_key = get_openweather_api_key()
        coordinates = Coordinates(
            lat=parse_coordinate(flask_request.args.get("lat"), "lat"),
            lon=parse_coordinate(flask_request.args.get("lon"), "lon"),
        )
        result = fetch_current_weather_by_coordinates(coordinates, api_key)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify(
        {
            "provider": "openweather",
            "request_type": "coordinates",
            "units": "metric",
            "requested_coordinates": {"lat": coordinates.lat, "lon": coordinates.lon},
            "result": result,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/api/openweather/by-location-key", methods=["GET"])
def weather_by_location_key_api() -> Any:
    location_key = (flask_request.args.get("location_key") or "").strip()
    if not location_key:
        return (
            jsonify(
                {
                    "error": "Provide `location_key` query param.",
                    "examples": [
                        "/api/openweather/by-location-key?location_key=shanghai",
                    ],
                }
            ),
            400,
        )

    try:
        api_key = get_openweather_api_key()
        coordinates = resolve_location_coordinates(location_key)
        result = fetch_current_weather_by_coordinates(coordinates, api_key)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify(
        {
            "provider": "openweather",
            "request_type": "location_key",
            "units": "metric",
            "location_key": location_key,
            "requested_coordinates": {"lat": coordinates.lat, "lon": coordinates.lon},
            "result": result,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/api/openweather/forecast/intraday", methods=["GET"])
def weather_intraday_forecast_api() -> Any:
    try:
        api_key = get_openweather_api_key()
        coordinates, request_type = resolve_request_coordinates()
        result = fetch_intraday_forecast(coordinates, api_key)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

    response: dict[str, Any] = {
        "provider": "openweather",
        "request_type": request_type,
        "units": "metric",
        "requested_coordinates": {"lat": coordinates.lat, "lon": coordinates.lon},
        "result": result,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    location = (flask_request.args.get("location") or "").strip()
    if location:
        response["location"] = location
    location_key = (flask_request.args.get("location_key") or "").strip()
    if location_key:
        response["location_key"] = location_key
    return jsonify(response)


@app.route("/api/openweather/day-max", methods=["GET"])
def weather_day_max_api() -> Any:
    date = (flask_request.args.get("date") or "").strip()

    try:
        api_key = get_openweather_api_key()
        coordinates, request_type = resolve_request_coordinates()
        current = fetch_current_weather_by_coordinates(coordinates, api_key)
        if not date:
            date = str(current.get("local_date_now") or "").strip()
        if not date:
            return jsonify({"error": "Unable to determine local date for this location."}), 502
        result = fetch_intraday_forecast(coordinates, api_key)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

    max_temp_c = None
    min_temp_c = None
    matching_points: list[dict[str, Any]] = []
    used_current_observation = False
    forecast_points = result.get("forecast_points")
    if isinstance(forecast_points, list):
        for point in forecast_points:
            if not isinstance(point, dict):
                continue
            local_date = point.get("local_date")
            if not isinstance(local_date, str) or local_date != date:
                continue
            matching_points.append(point)
            raw_max = point.get("temp_max_c")
            raw_min = point.get("temp_min_c")
            if isinstance(raw_max, (int, float)):
                value = float(raw_max)
                max_temp_c = value if max_temp_c is None else max(max_temp_c, value)
            if isinstance(raw_min, (int, float)):
                value = float(raw_min)
                min_temp_c = value if min_temp_c is None else min(min_temp_c, value)

    local_date_now = current.get("local_date_now")
    local_temperature_c_now = current.get("temperature_c")
    if local_date_now == date and isinstance(local_temperature_c_now, (int, float)):
        current_value = float(local_temperature_c_now)
        max_temp_c = current_value if max_temp_c is None else max(max_temp_c, current_value)
        min_temp_c = current_value if min_temp_c is None else min(min_temp_c, current_value)
        used_current_observation = True

    response = {
        "provider": "openweather",
        "request_type": request_type,
        "units": "metric",
        "requested_coordinates": {"lat": coordinates.lat, "lon": coordinates.lon},
        "date": date,
        "timezone_offset_seconds": current.get("timezone_offset_seconds"),
        "local_time_now": current.get("local_time_now"),
        "local_date_now": local_date_now,
        "local_temperature_c_now": local_temperature_c_now,
        "max_temp_c": max_temp_c,
        "min_temp_c": min_temp_c,
        "used_current_observation": used_current_observation,
        "historical_observations_available": False,
        "coverage_note": (
            "Free OpenWeather does not provide past-hours history. "
            "For today's local date this result uses the current local reading plus remaining 3-hour forecast points."
            if local_date_now == date
            else "Free OpenWeather does not provide past-hours history. This result is based on 3-hour forecast points only."
        ),
        "forecast_points": matching_points,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    location = (flask_request.args.get("location") or "").strip()
    if location:
        response["location"] = location
    location_key = (flask_request.args.get("location_key") or "").strip()
    if location_key:
        response["location_key"] = location_key
    return jsonify(response)


@app.route("/api/openweather/history", methods=["GET"])
def weather_history_api() -> Any:
    return (
        jsonify(
            {
                "provider": "openweather",
                "units": "metric",
                "error": "Historical weather is not available on the free OpenWeather plan used here.",
                "detail": "Current weather and 5 day / 3 hour forecast are supported. History requires a paid product.",
            }
        ),
        501,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
