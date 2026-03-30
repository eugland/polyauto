from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import requests

METAR_URL = "https://aviationweather.gov/api/data/metar"


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def fetch_temperature_history(
    icao_code: str, hours: int = 24, units: str = "m"
) -> list[dict[str, Any]]:
    """Fetch hourly temperature readings for the past `hours` hours via METAR."""
    params = {
        "ids": icao_code,
        "hours": hours,
        "format": "json",
    }
    response = requests.get(METAR_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    readings: list[dict[str, Any]] = []
    for obs in data if isinstance(data, list) else []:
        temp_c = obs.get("temp")
        obs_time = obs.get("obsTime")
        if temp_c is None or obs_time is None:
            continue
        temp = _celsius_to_fahrenheit(float(temp_c)) if units == "e" else float(temp_c)
        time_utc = datetime.fromtimestamp(int(obs_time), tz=timezone.utc).isoformat()
        readings.append({"time_utc": time_utc, "temperature": round(temp, 1)})

    readings.sort(key=lambda r: r["time_utc"])
    return readings


def compute_daily_high(readings: list[dict[str, Any]]) -> float | None:
    temps = [r["temperature"] for r in readings if r.get("temperature") is not None]
    return max(temps) if temps else None


def fetch_all_stations_metric(
    station_codes: list[str], hours: int = 24
) -> dict[str, dict[str, Any]]:
    """Fetch 24h temperature history for all stations in parallel. Always returns Celsius."""
    results: dict[str, dict[str, Any]] = {}

    def _fetch(code: str) -> tuple[str, dict[str, Any]]:
        try:
            readings = fetch_temperature_history(code, hours=hours, units="m")
            return code, {
                "readings": readings,
                "daily_high": compute_daily_high(readings),
                "current": readings[-1]["temperature"] if readings else None,
            }
        except Exception:
            return code, {"readings": [], "daily_high": None, "current": None}

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, code): code for code in station_codes}
        for future in as_completed(futures):
            code, data = future.result()
            results[code] = data

    return results
