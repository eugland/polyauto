"""
Weather betting daemon + weather data helpers.

Usage:
  python -m automata.weather
  python -m automata.weather --bet
  python -m automata.weather --bet --once --max-balance 30

Flags:
  --bet          Place real orders (default is dry-run).
  --interval     Loop interval in seconds (default 60).
  --once         Run one cycle, place at most one order, then exit.
  --max-balance  Max USDC this process can spend.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

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


def _derive_clob_credentials() -> None:
    import os

    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    from automata.client import derive_api_credentials

    creds = derive_api_credentials(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def run_weather_daemon(
    bet: bool = False,
    interval_seconds: int = 60,
    once: bool = False,
    max_balance_usdc: float | None = None,
) -> None:
    import logging
    import os
    import time

    from dotenv import load_dotenv

    load_dotenv()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log = logging.getLogger("automata.weather")

    from automata.db import init_db
    from automata.weather_bot import _scan_positions, run

    if bet:
        _derive_clob_credentials()
    init_db()

    iteration = 0
    while True:
        iteration += 1
        log.info("[weather] Iteration %d", iteration)

        if bet:
            _scan_positions(dry_run=False)
            bet_shares = float(os.getenv("BET_SIZE_SHARES", "20.0"))
            try:
                from automata.client import build_client, get_usdc_balance

                client = build_client(
                    host=os.environ["POLYMARKET_HOST"],
                    private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                    api_key=os.environ["CLOB_API_KEY"],
                    api_secret=os.environ["CLOB_SECRET"],
                    api_passphrase=os.environ["CLOB_PASS"],
                    funder=os.getenv("POLYMARKET_FUNDER") or None,
                    signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
                )
                balance = get_usdc_balance(client)
                max_no_price = float(os.getenv("MAX_NO_PRICE", "0.998"))
                min_required = bet_shares * max_no_price
                log.info(
                    "[weather] USDC balance: $%.2f  (need at least $%.2f for %g shares)",
                    balance,
                    min_required,
                    bet_shares,
                )
            except Exception as exc:
                log.warning("[weather] Balance check failed: %s — skipping cycle", exc)
                if once:
                    log.info("[weather] --once set, exiting after failed pre-check")
                    break
                time.sleep(interval_seconds)
                continue

            capped_balance = min(balance, max_balance_usdc) if max_balance_usdc is not None else balance
            if balance < min_required:
                log.warning("[weather] Balance $%.2f < $%.2f needed — skipping cycle", balance, min_required)
                if once:
                    log.info("[weather] --once set, exiting after insufficient balance")
                    break
                time.sleep(interval_seconds)
                continue
            if capped_balance < min_required:
                log.warning(
                    "[weather] Capped balance $%.2f < $%.2f needed — skipping cycle",
                    capped_balance,
                    min_required,
                )
                if once:
                    log.info("[weather] --once set, exiting after capped-balance check")
                    break
                time.sleep(interval_seconds)
                continue

        run(
            dry_run=not bet,
            max_spend_usdc=max_balance_usdc if bet else None,
            max_orders=1 if (bet and once) else None,
        )
        if once:
            log.info("[weather] --once set, exiting after single cycle")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Weather betting daemon")
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds (default: 60)")
    parser.add_argument("--once", action="store_true", help="Run one cycle, place at most one order, then exit")
    parser.add_argument("--max-balance", type=float, default=None, help="Max USDC this process is allowed to spend")
    args = parser.parse_args()

    run_weather_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.interval),
        once=args.once,
        max_balance_usdc=args.max_balance,
    )
