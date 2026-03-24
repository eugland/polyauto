#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime

import requests

PAGE_URL = "https://www.wunderground.com/weather/KSEA"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch_page_html(url: str) -> str:
    response = requests.get(url, headers=UA, timeout=30)
    response.raise_for_status()
    return response.text


def extract_current_endpoint(html: str) -> str:
    match = re.search(
        r"https://api\.weather\.com/v2/pws/observations/current\?[^\"']+",
        html,
    )
    if not match:
        raise RuntimeError("Could not find Wunderground current observation endpoint in page.")
    return match.group(0)


def extract_query_param(url: str, name: str) -> str | None:
    match = re.search(rf"(?:\?|&){name}=([^&]+)", url)
    if not match:
        return None
    return requests.utils.unquote(match.group(1))


def fetch_json(url: str, params: dict | None = None) -> dict:
    response = requests.get(url, params=params, headers=UA, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON payload type from {url}")
    return payload


def main() -> int:
    html = fetch_page_html(PAGE_URL)
    current_endpoint = extract_current_endpoint(html)

    api_key = extract_query_param(current_endpoint, "apiKey")
    station_id = extract_query_param(current_endpoint, "stationId")
    units = extract_query_param(current_endpoint, "units") or "e"
    if not api_key or not station_id:
        raise RuntimeError("Could not parse apiKey/stationId from current endpoint URL.")

    current = fetch_json(current_endpoint)
    observations = current.get("observations") or []
    if not observations:
        raise RuntimeError("No current observations returned.")

    now = observations[0]
    now_local = str(now.get("obsTimeLocal") or "")
    local_date = now_local.split(" ")[0]
    if not local_date:
        raise RuntimeError("Could not determine local date from current observation.")
    date_for_history = datetime.strptime(local_date, "%Y-%m-%d").strftime("%Y%m%d")

    history = fetch_json(
        "https://api.weather.com/v2/pws/history/all",
        params={
            "apiKey": api_key,
            "stationId": station_id,
            "units": units,
            "format": "json",
            "date": date_for_history,
        },
    )

    history_points = history.get("observations") or []
    temp_history = []
    for item in history_points:
        imperial = item.get("imperial") or {}
        temp_f = imperial.get("tempAvg")
        temp_high_f = imperial.get("tempHigh")
        temp_low_f = imperial.get("tempLow")
        temp_history.append(
            {
                "obs_time_local": item.get("obsTimeLocal"),
                "temp_f": temp_f,
                "temp_high_f": temp_high_f,
                "temp_low_f": temp_low_f,
            }
        )

    max_temp_f = None
    for point in temp_history:
        tf = point.get("temp_high_f")
        if isinstance(tf, (int, float)):
            max_temp_f = float(tf) if max_temp_f is None else max(max_temp_f, float(tf))

    current_imperial = now.get("imperial") or {}
    output = {
        "source": PAGE_URL,
        "station_id": station_id,
        "api_endpoints": {
            "current": current_endpoint,
            "history_all": (
                "https://api.weather.com/v2/pws/history/all"
                f"?apiKey=***&stationId={station_id}&units={units}&format=json&date={date_for_history}"
            ),
        },
        "local_date": local_date,
        "current_temperature": {
            "obs_time_local": now.get("obsTimeLocal"),
            "temp_f": current_imperial.get("temp"),
        },
        "max_temperature_today": {
            "temp_f": max_temp_f,
        },
        "history_temperature_today": temp_history,
    }
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
