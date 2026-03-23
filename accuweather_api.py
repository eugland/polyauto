#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

ACCUWEATHER_BASE = "https://www.accuweather.com/en/cn"
REQUEST_TIMEOUT_SECONDS = 20

# Internal keyword -> path mapping.
LOCATION_PATHS: dict[str, str] = {
    "shanghai": "shanghai-pudong-international-airport/1804_poi/weather-forecast/1804_poi",
}


@dataclass(frozen=True)
class LocationQuery:
    keyword: str
    path: str

    @property
    def url(self) -> str:
        return f"{ACCUWEATHER_BASE}/{self.path}"


def parse_keywords_from_request() -> list[str]:
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        raw = payload.get("keywords")
        if isinstance(raw, list):
            return [str(x).strip().lower() for x in raw if str(x).strip()]
        if isinstance(raw, str):
            return [p.strip().lower() for p in raw.split(",") if p.strip()]

    # GET query style: ?keyword=shanghai or ?keywords=shanghai,beijing
    keyword = (request.args.get("keyword") or "").strip().lower()
    keywords = (request.args.get("keywords") or "").strip().lower()
    items: list[str] = []
    if keyword:
        items.append(keyword)
    if keywords:
        items.extend([p.strip() for p in keywords.split(",") if p.strip()])
    return items


def html_to_text(html: str) -> str:
    # Remove scripts/styles and tags to make regex extraction stable.
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


def fetch_first_temperature_f(location: LocationQuery) -> dict[str, Any]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(location.url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    text = html_to_text(response.text)
    first_temp_f = extract_first_temperature_f(text)
    return {
        "keyword": location.keyword,
        "url": location.url,
        "temperature_f_first_entry": first_temp_f,
    }


@app.route("/api/accuweather/temperature", methods=["GET", "POST"])
def accuweather_temperature() -> Any:
    keywords = parse_keywords_from_request()
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

    resolved: list[LocationQuery] = []
    unknown: list[str] = []
    for kw in keywords:
        path = LOCATION_PATHS.get(kw)
        if not path:
            unknown.append(kw)
            continue
        resolved.append(LocationQuery(keyword=kw, path=path))

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for loc in resolved:
        try:
            results.append(fetch_first_temperature_f(loc))
        except requests.RequestException as exc:
            errors.append({"keyword": loc.keyword, "error": str(exc), "url": loc.url})

    return jsonify(
        {
            "requested_keywords": keywords,
            "resolved_count": len(resolved),
            "unknown_keywords": unknown,
            "results": results,
            "errors": errors,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8081"))
    app.run(host="0.0.0.0", port=port, debug=True)

