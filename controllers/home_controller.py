from __future__ import annotations

from pathlib import Path
from typing import Any

import requests
from flask import Flask, render_template

from services.config_service import (
    load_location_mapping,
    load_runtime_settings,
    read_config_payload,
)
from services.event_group_service import build_event_groups, print_filtered_results
from services.polymarket_service import fetch_temperature_markets_payload


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


def load_runtime_from_config(config_path: Path):
    payload = read_config_payload(config_path)
    return load_runtime_settings(payload)
