from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

WEATHER_CURRENT_URL = "https://api.weather.com/v3/wx/observations/current"
DEFAULT_WEATHER_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

_ALL_URLS_RE = re.compile(r"https?://[^\s\"'<>)\]]+", re.IGNORECASE)

# "recorded at/by the London City Airport Station in degrees"
# "recorded by the Hong Kong Observatory in degrees"
_STATION_NAME_RE = re.compile(
    r"recorded (?:at|by) the (.+?) in degrees",
    re.IGNORECASE,
)

# Last path segment of a Wunderground URL, e.g. /history/daily/gb/london/EGLC → EGLC
_WUNDERGROUND_STATION_RE = re.compile(
    r"wunderground\.com/[^\s\"']+/([A-Z0-9]{3,6})(?:[/?#]|$)",
    re.IGNORECASE,
)


def extract_all_urls(text: str) -> list[str]:
    """Extract every URL found in a block of text."""
    return [u.rstrip(".,") for u in _ALL_URLS_RE.findall(text)]


def extract_station_name(text: str) -> str | None:
    """Extract the weather station name from a Polymarket event description."""
    m = _STATION_NAME_RE.search(text)
    return m.group(1).strip() if m else None


def extract_unit(text: str) -> str:
    """Return 'F' or 'C' based on 'degrees Fahrenheit/Celsius' in description."""
    if re.search(r"degrees fahrenheit", text, re.IGNORECASE):
        return "F"
    return "C"


def extract_icao_from_wunderground_url(url: str) -> str | None:
    """Extract ICAO station code from the last path segment of a Wunderground URL."""
    m = _WUNDERGROUND_STATION_RE.search(url)
    return m.group(1).upper() if m else None


def fetch_station_weather(icao: str, units: str = "e") -> dict[str, Any]:
    """
    Fetch current conditions for a station from Weather.com API.
    Returns dict with: current_temp, forecast_high, local_time (all may be None on failure).
    units: "e" = imperial (°F), "m" = metric (°C)
    """
    api_key = os.environ.get("WEATHER_API_KEY", DEFAULT_WEATHER_API_KEY).strip()
    params = {
        "apiKey": api_key,
        "icaoCode": icao,
        "units": units,
        "language": "en-US",
        "format": "json",
    }
    try:
        resp = requests.get(WEATHER_CURRENT_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {
            "current_temp": data.get("temperature"),
            "local_time": data.get("obsTimeLocal"),
        }
    except Exception:
        return {"current_temp": None, "forecast_high": None, "local_time": None}


def fetch_weather_for_stations(
    stations: list[str], units: str = "e"
) -> dict[str, dict[str, Any]]:
    """Fetch weather for multiple stations in parallel."""
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_station_weather, icao, units): icao for icao in stations}
        for future in as_completed(futures):
            icao = futures[future]
            results[icao] = future.result()
    return results


def c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


# ── Station coordinates ───────────────────────────────────────────────────────

def fetch_station_coords(icao: str) -> tuple[float, float] | None:
    """Return (lat, lon) for a station. Tries METAR first, falls back to airport API."""
    # Try METAR (has recent coords)
    try:
        resp = requests.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "hours": 3, "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data and isinstance(data, list):
            lat, lon = data[0].get("lat"), data[0].get("lon")
            if lat is not None and lon is not None:
                return float(lat), float(lon)
    except Exception:
        pass

    # Fallback: static airport info
    try:
        resp = requests.get(
            "https://aviationweather.gov/api/data/airport",
            params={"ids": icao, "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data and isinstance(data, list):
            lat, lon = data[0].get("lat"), data[0].get("lon")
            if lat is not None and lon is not None:
                return float(lat), float(lon)
    except Exception:
        pass

    return None


def fetch_coords_for_stations(icao_list: list[str]) -> dict[str, tuple[float, float] | None]:
    """Fetch (lat, lon) for multiple stations in parallel."""
    results: dict[str, tuple[float, float] | None] = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_station_coords, icao): icao for icao in icao_list}
        for future in as_completed(futures):
            icao = futures[future]
            results[icao] = future.result()
    return results


# ── Forecast high for a specific date ────────────────────────────────────────

def fetch_open_meteo_high(lat: float, lon: float, date_str: str, unit: str = "C") -> float | None:
    """
    Fetch forecast/historical daily max temp from Open-Meteo for a specific date.
    date_str: "YYYY-MM-DD"
    """
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit" if unit == "F" else "celsius",
                "timezone": "auto",
                "start_date": date_str,
                "end_date": date_str,
            },
            timeout=10,
        )
        resp.raise_for_status()
        temps = resp.json().get("daily", {}).get("temperature_2m_max", [])
        return float(temps[0]) if temps else None
    except Exception:
        return None


def fetch_noaa_forecast_high(lat: float, lon: float, date_str: str) -> float | None:
    """
    Fetch NOAA forecast daily high for a specific date (US stations only, returns °F).
    date_str: "YYYY-MM-DD"
    """
    try:
        r = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers={"User-Agent": "automata-bot"},
            timeout=10,
        )
        r.raise_for_status()
        forecast_url = r.json()["properties"]["forecast"]

        r2 = requests.get(forecast_url, headers={"User-Agent": "automata-bot"}, timeout=10)
        r2.raise_for_status()
        for period in r2.json()["properties"]["periods"]:
            if period.get("isDaytime") and date_str in period.get("startTime", ""):
                return float(period["temperature"])
    except Exception:
        pass
    return None


def fetch_forecasts_for_events(
    event_list: list[dict],  # each: {"icao": str, "date": str, "unit": str}
    coords: dict[str, tuple[float, float] | None],
) -> dict[tuple[str, str], dict[str, float | None]]:
    """
    Fetch Open-Meteo and NOAA forecast highs for each (icao, date) pair in parallel.
    Returns {(icao, date): {"open_meteo": float|None, "noaa": float|None}}
    """
    # Deduplicate
    tasks: set[tuple[str, str, str]] = set()
    for ev in event_list:
        if ev["icao"] and coords.get(ev["icao"]):
            tasks.add((ev["icao"], ev["date"], ev["unit"]))

    results: dict[tuple[str, str], dict[str, float | None]] = {}

    def _fetch(icao: str, date: str, unit: str) -> tuple[tuple[str, str], dict]:
        lat, lon = coords[icao]
        open_meteo = fetch_open_meteo_high(lat, lon, date, unit)
        noaa = fetch_noaa_forecast_high(lat, lon, date)
        return (icao, date), {"open_meteo": open_meteo, "noaa": noaa}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch, icao, date, unit) for icao, date, unit in tasks]
        for future in as_completed(futures):
            key, val = future.result()
            results[key] = val

    return results
