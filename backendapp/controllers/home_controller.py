from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

from backendapp.services.config_service import (
    load_location_mapping,
    load_runtime_settings,
    read_config_payload,
)
from backendapp.services.event_group_service import build_event_groups, print_filtered_results
from backendapp.services.polymarket_service import fetch_temperature_markets_payload
from backendapp.services.weather_history_service import compute_daily_high, fetch_all_stations_metric, fetch_temperature_history

WEATHER_CURRENT_URL = "https://api.weather.com/v3/wx/observations/current"
DEFAULT_WEATHER_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

WEATHER_CACHE_TTL = 600  # seconds
_weather_cache: dict[str, Any] = {"data": None, "fetched_at": 0.0}
_weather_lock = threading.Lock()
_fetch_lock = threading.Lock()   # trylock — prevents concurrent background fetches
_cache_ready = threading.Event() # signals when cache has data for the first time


def _refresh_weather_cache(config_path: Path) -> None:
    if not _fetch_lock.acquire(blocking=False):
        return  # another refresh is already running
    try:
        config_payload = read_config_payload(config_path)
        mapping = load_location_mapping(config_payload)
        station_codes = list({loc.station for loc in mapping.values() if loc.station})
        data = fetch_all_stations_metric(station_codes)
        with _weather_lock:
            _weather_cache["data"] = data
            _weather_cache["fetched_at"] = time.monotonic()
        _cache_ready.set()
    except Exception:
        _cache_ready.set()  # unblock waiters even on failure
    finally:
        _fetch_lock.release()


def register_home_routes(app: Flask, config_path: Path) -> None:
    # Warm the cache in the background as soon as the app starts.
    threading.Thread(target=_refresh_weather_cache, args=(config_path,), daemon=True).start()
    @app.route("/", methods=["GET"])
    def index() -> Any:
        event_groups: list[dict[str, Any]] = []
        market_count = 0
        error = ""

        try:
            config_payload = read_config_payload(config_path)
            mapping = load_location_mapping(config_payload)
            payload = fetch_temperature_markets_payload()
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

    @app.route("/api/weather/current", methods=["GET"])
    def weather_current() -> Any:
        location_code = (request.args.get("location_code") or request.args.get("icao_code") or "").strip().upper()
        units = (request.args.get("units") or "m").strip().lower()

        if not location_code:
            return jsonify({"error": "Missing required query param: location_code"}), 400
        if not location_code.isalnum() or not (3 <= len(location_code) <= 8):
            return jsonify({"error": "Invalid location_code. Use ICAO code format, e.g. NZWN"}), 400
        if units not in {"m", "e"}:
            return jsonify({"error": "Invalid units. Allowed values: m or e"}), 400

        api_key = os.environ.get("WEATHER_API_KEY", DEFAULT_WEATHER_API_KEY).strip()
        if not api_key:
            return jsonify({"error": "Missing WEATHER_API_KEY"}), 500

        params = {
            "apiKey": api_key,
            "language": "en-US",
            "units": units,
            "format": "json",
            "icaoCode": location_code,
        }

        try:
            response = requests.get(WEATHER_CURRENT_URL, params=params, timeout=20)
            if response.status_code >= 400:
                return (
                    jsonify(
                        {
                            "error": "Weather API request failed",
                            "status_code": response.status_code,
                            "details": response.text,
                        }
                    ),
                    response.status_code,
                )
            return jsonify(response.json())
        except requests.RequestException as exc:
            return jsonify({"error": f"Weather API error: {exc}"}), 502

    @app.route("/api/weather/all", methods=["GET"])
    def weather_all() -> Any:
        with _weather_lock:
            cached_data = _weather_cache["data"]
            cached_at   = _weather_cache["fetched_at"]

        is_stale = cached_data is None or (time.monotonic() - cached_at) >= WEATHER_CACHE_TTL

        if is_stale:
            # Kick off a background refresh (no-op if one is already running).
            threading.Thread(target=_refresh_weather_cache, args=(config_path,), daemon=True).start()

        if cached_data is not None:
            # Return whatever we have immediately — fresh or stale.
            return jsonify(cached_data)

        # Very first cold start: wait for the startup fetch (max 30s).
        _cache_ready.wait(timeout=30)
        with _weather_lock:
            cached_data = _weather_cache["data"]
        if cached_data is None:
            return jsonify({"error": "Weather data not yet available, try again shortly"}), 503
        return jsonify(cached_data)

    @app.route("/api/weather/history", methods=["GET"])
    def weather_history() -> Any:
        icao_code = (request.args.get("icao_code") or "").strip().upper()
        units = (request.args.get("units") or "m").strip().lower()
        try:
            hours = int(request.args.get("hours") or 24)
        except ValueError:
            return jsonify({"error": "Invalid hours value. Must be an integer."}), 400

        if not icao_code:
            return jsonify({"error": "Missing required query param: icao_code"}), 400
        if not icao_code.isalnum() or not (3 <= len(icao_code) <= 8):
            return jsonify({"error": "Invalid icao_code format, e.g. KSEA"}), 400
        if units not in {"m", "e"}:
            return jsonify({"error": "Invalid units. Allowed values: m or e"}), 400
        if not (1 <= hours <= 168):
            return jsonify({"error": "hours must be between 1 and 168"}), 400

        try:
            readings = fetch_temperature_history(icao_code, hours=hours, units=units)
        except requests.RequestException as exc:
            return jsonify({"error": f"Weather history fetch failed: {exc}"}), 502

        return jsonify({
            "icao_code": icao_code,
            "units": units,
            "hours": hours,
            "readings": readings,
        })

    @app.route("/api/weather/daily-high", methods=["GET"])
    def weather_daily_high() -> Any:
        icao_code = (request.args.get("icao_code") or "").strip().upper()
        units = (request.args.get("units") or "m").strip().lower()

        if not icao_code:
            return jsonify({"error": "Missing required query param: icao_code"}), 400
        if not icao_code.isalnum() or not (3 <= len(icao_code) <= 8):
            return jsonify({"error": "Invalid icao_code format, e.g. KSEA"}), 400
        if units not in {"m", "e"}:
            return jsonify({"error": "Invalid units. Allowed values: m or e"}), 400

        try:
            readings = fetch_temperature_history(icao_code, hours=24, units=units)
        except requests.RequestException as exc:
            return jsonify({"error": f"Weather history fetch failed: {exc}"}), 502

        daily_high = compute_daily_high(readings)
        return jsonify({
            "icao_code": icao_code,
            "units": units,
            "daily_high": daily_high,
            "reading_count": len(readings),
        })


def load_runtime_from_config(config_path: Path):
    payload = read_config_payload(config_path)
    return load_runtime_settings(payload)
