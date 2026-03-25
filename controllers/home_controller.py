from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

from services.config_service import (
    load_location_mapping,
    load_runtime_settings,
    read_config_payload,
)
from services.event_group_service import build_event_groups, print_filtered_results
from services.polymarket_service import fetch_temperature_markets_payload

WEATHER_CURRENT_URL = "https://api.weather.com/v3/wx/observations/current"
DEFAULT_WEATHER_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"


def register_home_routes(app: Flask, config_path: Path) -> None:
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


def load_runtime_from_config(config_path: Path):
    payload = read_config_payload(config_path)
    return load_runtime_settings(payload)
