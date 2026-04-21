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

# NOAA timeseries URL: https://www.weather.gov/wrh/timeseries?site=LTFM → LTFM
_NOAA_TIMESERIES_RE = re.compile(
    r"weather\.gov/[^\s\"']*[?&]site=([A-Z0-9]{3,6})(?:[&#]|$)",
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


def extract_icao_from_noaa_url(url: str) -> str | None:
    """Extract ICAO from a NOAA timeseries URL, e.g. weather.gov/wrh/timeseries?site=LTFM."""
    m = _NOAA_TIMESERIES_RE.search(url)
    return m.group(1).upper() if m else None


def extract_icao_from_url(url: str) -> str | None:
    """Try all known URL formats that embed an ICAO station code."""
    return extract_icao_from_wunderground_url(url) or extract_icao_from_noaa_url(url)


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


def fetch_metar_temp(icao: str, unit: str = "C") -> float | None:
    """
    Most recent METAR observation temperature for `icao`, returned in `unit` (C or F).
    Returns None if unavailable.
    """
    try:
        resp = requests.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "hours": 3, "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None
    if not data or not isinstance(data, list):
        return None
    temp_c = data[0].get("temp")
    if temp_c is None:
        return None
    try:
        temp_c = float(temp_c)
    except (TypeError, ValueError):
        return None
    if unit.upper() == "F":
        return temp_c * 9.0 / 5.0 + 32
    return temp_c


def fetch_coords_for_stations(icao_list: list[str]) -> dict[str, tuple[float, float] | None]:
    """Fetch (lat, lon) for multiple stations in parallel."""
    results: dict[str, tuple[float, float] | None] = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_station_coords, icao): icao for icao in icao_list}
        for future in as_completed(futures):
            icao = futures[future]
            results[icao] = future.result()
    return results


def fetch_open_meteo_archive_high(lat: float, lon: float, date_str: str, unit: str = "C") -> float | None:
    """Historical daily max from Open-Meteo archive (for past dates)."""
    try:
        resp = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
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
        vals = (resp.json().get("daily") or {}).get("temperature_2m_max") or []
        return float(vals[0]) if vals and vals[0] is not None else None
    except Exception:
        return None


def fetch_noaa_high(lat: float, lon: float, date_str: str, unit: str = "C") -> float | None:
    """
    NOAA NWS daily high for (lat, lon) on date_str. US-only — returns None
    for points outside US forecast coverage.
    """
    try:
        pr = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers={"User-Agent": "weather-bot/1.0"},
            timeout=10,
        )
        pr.raise_for_status()
        forecast_url = ((pr.json() or {}).get("properties") or {}).get("forecast")
        if not forecast_url:
            return None
        fr = requests.get(forecast_url, headers={"User-Agent": "weather-bot/1.0"}, timeout=10)
        fr.raise_for_status()
        periods = ((fr.json() or {}).get("properties") or {}).get("periods") or []
    except Exception:
        return None

    highs: list[float] = []
    for p in periods:
        start = str(p.get("startTime") or "")
        if not start.startswith(date_str):
            continue
        if not p.get("isDaytime"):
            continue
        temp = p.get("temperature")
        t_unit = (p.get("temperatureUnit") or "F").upper()
        if temp is None:
            continue
        try:
            temp_f = float(temp)
        except (TypeError, ValueError):
            continue
        # NOAA returns F; convert if market unit is C.
        if t_unit == "F" and unit == "C":
            temp_f = (temp_f - 32) * 5.0 / 9.0
        elif t_unit == "C" and unit == "F":
            temp_f = temp_f * 9.0 / 5.0 + 32
        highs.append(temp_f)
    return max(highs) if highs else None


def fetch_city_coords(city: str) -> tuple[float, float] | None:
    """
    Resolve a city name to (lat, lon) via Open-Meteo's free geocoding API.
    Reliable for international cities — unlike aviationweather.gov which
    rate-limits and has spotty coverage outside the US.
    """
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city, "count": 1, "language": "en", "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None
    results = data.get("results") or []
    if not results:
        return None
    first = results[0]
    lat, lon = first.get("latitude"), first.get("longitude")
    if lat is None or lon is None:
        return None
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


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


# ── Station sanity check / verification ─────────────────────────────────────

def fetch_metar_history(icao: str, hours: int = 168) -> list[dict]:
    """Return METAR observations for the past `hours` hours ([] on failure)."""
    try:
        resp = requests.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "hours": hours, "format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def assess_freshness(icao: str, days: int = 7) -> dict:
    """
    METAR coverage over the past `days` days.

    Returns: {
      total_hours, hours_with_obs, coverage_pct, max_gap_hours, obs_count,
      latest_obs_age_hours
    }
    """
    import time as _time

    hours = days * 24
    obs = fetch_metar_history(icao, hours=hours)
    now = int(_time.time())
    window_start = now - hours * 3600

    obs_hour_buckets: set[int] = set()
    latest_ts = 0
    for o in obs:
        ts = o.get("obsTime")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < window_start:
            continue
        latest_ts = max(latest_ts, ts)
        obs_hour_buckets.add((ts - window_start) // 3600)

    if not obs_hour_buckets:
        return {
            "total_hours": hours,
            "hours_with_obs": 0,
            "coverage_pct": 0.0,
            "max_gap_hours": hours,
            "obs_count": len(obs),
            "latest_obs_age_hours": None,
        }

    sorted_hrs = sorted(obs_hour_buckets)
    # Leading gap (window_start → first obs)
    max_gap = sorted_hrs[0]
    for i in range(1, len(sorted_hrs)):
        gap = sorted_hrs[i] - sorted_hrs[i - 1] - 1
        if gap > max_gap:
            max_gap = gap
    # Trailing gap (last obs → now)
    trailing = (hours - 1) - sorted_hrs[-1]
    if trailing > max_gap:
        max_gap = trailing

    return {
        "total_hours": hours,
        "hours_with_obs": len(obs_hour_buckets),
        "coverage_pct": round(len(obs_hour_buckets) * 100.0 / hours, 1),
        "max_gap_hours": int(max_gap),
        "obs_count": len(obs),
        "latest_obs_age_hours": round((now - latest_ts) / 3600.0, 1),
    }


def _to_float(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def find_markets_for_city(city: str) -> list[dict]:
    """Return open temperature markets whose question/title/slug mentions `city`."""
    from automata.polymarket import fetch_temperature_markets_payload

    payload = fetch_temperature_markets_payload()
    markets = payload.get("markets") or []
    needle = city.strip().lower()
    matches: list[dict] = []
    for m in markets:
        haystacks = [
            str(m.get("question") or ""),
            str(m.get("groupItemTitle") or ""),
            str(m.get("slug") or ""),
            str(m.get("event_title") or ""),
            str(m.get("event_slug") or ""),
            str(m.get("event_description") or ""),
        ]
        if any(needle in h.lower() for h in haystacks):
            matches.append(m)
    return matches


def get_volume_stats(market: dict) -> dict:
    """Extract volume/liquidity signals from a Polymarket market dict."""
    return {
        "volume_total": _to_float(market.get("volumeNum")) or _to_float(market.get("volume")),
        "volume_24h": _to_float(market.get("volume24hr")),
        "liquidity": _to_float(market.get("liquidityNum")) or _to_float(market.get("liquidity")),
    }


def verify_station(city: str) -> dict:
    """Run all sanity checks for a city's weather market."""
    from datetime import date

    report: dict = {"city": city}
    markets = find_markets_for_city(city)
    if not markets:
        report["error"] = f"no open temperature market found for '{city}'"
        return report

    # Prefer the highest-volume matching market
    markets.sort(key=lambda m: _to_float(m.get("volumeNum")) or 0.0, reverse=True)
    market = markets[0]

    report["market_question"] = market.get("question")
    report["event_title"] = market.get("event_title")
    report["event_slug"] = market.get("event_slug")
    report["matched_markets"] = len(markets)

    desc = str(market.get("event_description") or "")
    report["station_name"] = extract_station_name(desc)
    report["unit"] = extract_unit(desc)

    icao = None
    for u in extract_all_urls(desc):
        icao = extract_icao_from_url(u)
        if icao:
            break
    report["icao"] = icao

    report["volume"] = get_volume_stats(market)

    if not icao:
        report["error"] = "could not extract ICAO from market description"
        return report

    report["coords"] = fetch_station_coords(icao)
    report["freshness"] = assess_freshness(icao, days=7)

    today = date.today().isoformat()
    if report["coords"]:
        lat, lon = report["coords"]
        report["forecast_open_meteo"] = fetch_open_meteo_high(lat, lon, today, unit=report["unit"])
        report["forecast_noaa"] = fetch_noaa_high(lat, lon, today, unit=report["unit"])
    else:
        report["forecast_open_meteo"] = None
        report["forecast_noaa"] = None

    return report


def _rating(pass_: bool, warn: bool = False) -> str:
    if pass_:
        return "PASS"
    if warn:
        return "WARN"
    return "FAIL"


def print_verify_report(report: dict) -> None:
    city = report.get("city", "?")
    print(f"\n=== {city} ===")
    if err := report.get("error"):
        q = report.get("market_question")
        if q:
            print(f"  Market   : {q}")
            print(f"  Station  : {report.get('station_name')}")
        print(f"  [FAIL] {err}")
        return

    print(f"  Market   : {report.get('market_question')}")
    print(f"  Station  : {report.get('station_name')}  (ICAO: {report.get('icao')})")
    if report.get("matched_markets", 0) > 1:
        print(f"  (matched {report['matched_markets']} markets — showing highest-volume)")

    coords = report.get("coords")
    if coords:
        print(f"  [PASS] Coords found         : {coords[0]:.3f}, {coords[1]:.3f}")
    else:
        print(f"  [FAIL] Coords found         : none (station not in METAR/airport DB)")

    f = report.get("freshness") or {}
    if f.get("hours_with_obs", 0) > 0:
        pct = f.get("coverage_pct", 0.0)
        gap = f.get("max_gap_hours", 0)
        age = f.get("latest_obs_age_hours")
        pass_ = pct >= 95.0 and gap <= 3 and (age is None or age <= 3)
        warn = (not pass_) and pct >= 80.0 and gap <= 12
        rating = _rating(pass_, warn)
        print(f"  [{rating}] Past-7d METAR coverage : "
              f"{f['hours_with_obs']}/{f['total_hours']}h ({pct}%), "
              f"max gap {gap}h, latest {age}h ago")
    else:
        print(f"  [FAIL] Past-7d METAR coverage : no observations returned")

    unit = report.get("unit", "C")
    om = report.get("forecast_open_meteo")
    om_str = "none" if om is None else f"{om:.1f}°{unit}"
    print(f"  [{_rating(om is not None)}] Open-Meteo forecast    : {om_str}")

    noaa = report.get("forecast_noaa")
    if noaa is not None:
        print(f"  [PASS] NOAA forecast          : {noaa:.1f}°{unit}")
    else:
        print(f"  [N/A ] NOAA forecast          : not available (likely non-US)")

    vol = report.get("volume") or {}
    total = vol.get("volume_total") or 0
    v24 = vol.get("volume_24h") or 0
    liq = vol.get("liquidity") or 0
    pass_ = total >= 10_000 and v24 >= 500
    warn = (not pass_) and total >= 1_000
    rating = _rating(pass_, warn)
    print(f"  [{rating}] Polymarket volume      : "
          f"total ${total:,.0f}, 24h ${v24:,.0f}, liquidity ${liq:,.0f}")


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
                max_no_price = float(os.getenv("MAX_NO_PRICE", "0.997"))
                # Minimum viable: enough for at least 1 share (run() scales bet_shares to 90% of balance)
                min_required = max_no_price
                effective_balance = min(balance, max_balance_usdc) if max_balance_usdc is not None else balance
                log.info(
                    "[weather] USDC balance: $%.2f  effective: $%.2f  will bet up to $%.2f (90%%)",
                    balance,
                    effective_balance,
                    round(0.9 * effective_balance, 2),
                )
            except Exception as exc:
                log.warning("[weather] Balance check failed: %s — skipping cycle", exc)
                if once:
                    log.info("[weather] --once set, exiting after failed pre-check")
                    break
                time.sleep(interval_seconds)
                continue

            capped_balance = min(balance, max_balance_usdc) if max_balance_usdc is not None else balance
            if capped_balance < 10.0:
                log.warning("[weather] Balance $%.2f < $10.00 minimum — skipping cycle", capped_balance)
                if once:
                    log.info("[weather] --once set, exiting after insufficient balance")
                    break
                time.sleep(interval_seconds)
                continue

        try:
            run(
                dry_run=not bet,
                max_spend_usdc=max_balance_usdc if bet else None,
                max_orders=1 if (bet and once) else None,
            )
        except Exception as exc:
            log.warning("[weather] Cycle failed: %s: %s — continuing", type(exc).__name__, exc)
            if once:
                log.info("[weather] --once set, exiting after failed cycle")
                break
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
    parser.add_argument(
        "--verify", nargs="+", metavar="CITY",
        help="Run station sanity checks for the given cities and exit (no daemon, no orders)",
    )
    args = parser.parse_args()

    if args.verify:
        import sys
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
        for city in args.verify:
            try:
                report = verify_station(city)
                print_verify_report(report)
            except Exception as exc:
                print(f"\n=== {city} ===\n  [ERROR] {type(exc).__name__}: {exc}")
        raise SystemExit(0)

    run_weather_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.interval),
        once=args.once,
        max_balance_usdc=args.max_balance,
    )
