"""
Overnight highest-temperature market collector.

Discovers all "highest-temperature-in-<city>-on-<date>" events on Polymarket,
polls the CLOB order book for every bucket token every N minutes, and fetches
METAR + Open-Meteo weather observations. All data lands in DuckDB at
experiment/temp_market.db.

Usage:
  python -m experiment.temp_market_collector            # dry-run, poll every 2 min
  python -m experiment.temp_market_collector --poll 5   # poll every 5 min
  python -m experiment.temp_market_collector --once     # one pass then exit
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests

log = logging.getLogger("temp_market_collector")

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _REPO_ROOT / "db" / "temp_market.db"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# Matches: highest-temperature-in-<city>-on-<month>-<day>-<year>
HIGH_TEMP_SLUG_RE = re.compile(
    r"^highest-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$",
    re.IGNORECASE,
)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

RANGE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.IGNORECASE)
THRESHOLD_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+(or\s+higher|or\s+below|or\s+lower)", re.IGNORECASE)
EXACT_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s*$", re.IGNORECASE)
WUNDERGROUND_STATION_RE = re.compile(r"wunderground\.com/[^\s\"']+/([A-Z0-9]{3,6})(?:[/?#]|$)", re.IGNORECASE)
STATION_NAME_RE = re.compile(r"recorded (?:at|by) the (.+?) in degrees", re.IGNORECASE)


# ── DB init ────────────────────────────────────────────────────────────────────

def init_db(path: str | Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            condition_id   VARCHAR PRIMARY KEY,
            event_slug     VARCHAR,
            city           VARCHAR,
            event_date     VARCHAR,
            question       VARCHAR,
            bucket_lo      DOUBLE,
            bucket_hi      DOUBLE,
            unit           VARCHAR,
            direction      VARCHAR,
            yes_token_id   VARCHAR,
            no_token_id    VARCHAR,
            icao           VARCHAR,
            lat            DOUBLE,
            lon            DOUBLE,
            tz             VARCHAR,
            discovered_at  TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS book_snapshots (
            id           BIGINT,
            condition_id VARCHAR,
            sampled_at   TIMESTAMPTZ,
            local_hour   DOUBLE,
            best_bid     DOUBLE,
            best_ask     DOUBLE,
            mid          DOUBLE,
            spread       DOUBLE
        )
    """)
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_snapshot START 1
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS weather_obs (
            id          BIGINT,
            city        VARCHAR,
            icao        VARCHAR,
            observed_at TIMESTAMPTZ,
            temp_c      DOUBLE,
            source      VARCHAR
        )
    """)
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_weather START 1
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id              BIGINT,
            city            VARCHAR,
            icao            VARCHAR,
            sampled_at      TIMESTAMPTZ,
            forecast_date   VARCHAR,
            forecast_high_c DOUBLE,
            source          VARCHAR
        )
    """)
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_forecast START 1
    """)
    return con


# ── Gamma market discovery ─────────────────────────────────────────────────────

def _parse_event_date(slug: str) -> str | None:
    """Return YYYY-MM-DD from slug like highest-temperature-in-london-on-april-17-2026."""
    m = HIGH_TEMP_SLUG_RE.match(slug)
    if not m:
        return None
    month_name, day, year = m.group(2).lower(), int(m.group(3)), int(m.group(4))
    month = MONTH_MAP.get(month_name)
    if not month:
        return None
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_city_from_slug(slug: str) -> str | None:
    m = HIGH_TEMP_SLUG_RE.match(slug)
    return m.group(1).replace("-", " ").title() if m else None


def _parse_json_list(raw: Any) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return []


def _parse_bucket(question: str) -> tuple[float | None, float | None, str, str]:
    """Return (lo, hi, unit, direction) from a bucket question string."""
    m = RANGE_RE.search(question)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper(), "range"
    m = THRESHOLD_RE.search(question)
    if m:
        direction = "higher" if "higher" in m.group(3).lower() else "below"
        return float(m.group(1)), None, m.group(2).upper(), direction
    m = EXACT_RE.search(question)
    if m:
        return float(m.group(1)), None, m.group(2).upper(), "exact"
    return None, None, "C", "unknown"


def _extract_icao(description: str) -> str | None:
    m = WUNDERGROUND_STATION_RE.search(description or "")
    return m.group(1).upper() if m else None


def fetch_highest_temp_markets() -> list[dict]:
    """Page through Gamma events tagged 'temperature' and return all highest-temp market rows."""
    end_max = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()
    markets: list[dict] = []
    seen: set[str] = set()
    for page in range(30):
        resp = requests.get(
            GAMMA_EVENTS_URL,
            params={
                "tag_slug": "temperature",
                "closed": "false",
                "limit": 20,
                "offset": page * 20,
                "end_date_max": end_max,
            },
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json()
        if not isinstance(events, list) or not events:
            break
        for event in events:
            slug = str(event.get("slug") or "")
            if not HIGH_TEMP_SLUG_RE.match(slug):
                continue
            eid = str(event.get("id") or slug)
            if eid in seen:
                continue
            seen.add(eid)
            event_date = _parse_event_date(slug)
            city = _parse_city_from_slug(slug)
            desc = event.get("description") or ""
            icao = _extract_icao(desc)
            for mkt in (_parse_json_list(event.get("markets")) or []):
                if not isinstance(mkt, dict):
                    continue
                cid = str(mkt.get("conditionId") or mkt.get("id") or "")
                question = str(mkt.get("question") or "")
                lo, hi, unit, direction = _parse_bucket(question)
                outcomes = _parse_json_list(mkt.get("outcomes"))
                token_ids = _parse_json_list(mkt.get("clobTokenIds"))
                yes_tok = no_tok = None
                for i, name in enumerate(outcomes):
                    s = str(name).strip().lower()
                    if s == "yes" and i < len(token_ids):
                        yes_tok = str(token_ids[i])
                    elif s == "no" and i < len(token_ids):
                        no_tok = str(token_ids[i])
                # Gamma embeds best_bid / best_ask / spread directly on each market
                def _safe_float(v: Any) -> float | None:
                    try:
                        return float(v) if v is not None else None
                    except (TypeError, ValueError):
                        return None

                # outcomePrices[0] = YES price (mid), [1] = NO price
                outcome_prices = _parse_json_list(mkt.get("outcomePrices"))
                yes_mid = _safe_float(outcome_prices[0]) if outcome_prices else None

                markets.append({
                    "condition_id": cid,
                    "event_slug": slug,
                    "city": city,
                    "event_date": event_date,
                    "question": question,
                    "bucket_lo": lo,
                    "bucket_hi": hi,
                    "unit": unit,
                    "direction": direction,
                    "yes_token_id": yes_tok,
                    "no_token_id": no_tok,
                    "icao": icao,
                    # live book data embedded by Gamma
                    "best_bid": _safe_float(mkt.get("bestBid")),
                    "best_ask": _safe_float(mkt.get("bestAsk")),
                    "spread": _safe_float(mkt.get("spread")),
                    "yes_mid": yes_mid,
                })
        if len(events) < 20:
            break
    return markets


# ── Coordinates + timezone ────────────────────────────────────────────────────

def _resolve_coords_and_tz(icao: str | None, city: str | None) -> tuple[float | None, float | None, str | None]:
    lat = lon = tz = None
    if icao:
        try:
            r = requests.get(
                "https://aviationweather.gov/api/data/metar",
                params={"ids": icao, "hours": 3, "format": "json"},
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()
            if data and isinstance(data, list):
                lat, lon = data[0].get("lat"), data[0].get("lon")
                if lat is not None:
                    lat, lon = float(lat), float(lon)
        except Exception:
            pass
    if (lat is None) and city:
        try:
            r = requests.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": city, "count": 1, "language": "en", "format": "json"},
                timeout=8,
            )
            r.raise_for_status()
            results = r.json().get("results") or []
            if results:
                lat, lon = float(results[0]["latitude"]), float(results[0]["longitude"])
        except Exception:
            pass
    if lat is not None:
        try:
            from timezonefinder import TimezoneFinder
            tz = TimezoneFinder().timezone_at(lng=lon, lat=lat)
        except Exception:
            pass
    return lat, lon, tz



# ── Weather fetch ─────────────────────────────────────────────────────────────

def fetch_metar_temp(icao: str) -> float | None:
    """Return current observed temp °C from latest METAR."""
    try:
        r = requests.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "hours": 2, "format": "json"},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data and isinstance(data, list):
            return data[0].get("temp")
    except Exception:
        pass
    return None


def fetch_open_meteo_current(lat: float, lon: float, date_str: str) -> tuple[float | None, float | None]:
    """Return (current_temp_c, forecast_high_c) from Open-Meteo."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m",
                "daily": "temperature_2m_max",
                "temperature_unit": "celsius",
                "timezone": "auto",
                "start_date": date_str,
                "end_date": date_str,
            },
            timeout=8,
        )
        r.raise_for_status()
        body = r.json()
        current = (body.get("current") or {}).get("temperature_2m")
        daily_max = ((body.get("daily") or {}).get("temperature_2m_max") or [None])[0]
        return (
            float(current) if current is not None else None,
            float(daily_max) if daily_max is not None else None,
        )
    except Exception:
        return None, None


# ── Upsert helpers ─────────────────────────────────────────────────────────────

def upsert_market(con: duckdb.DuckDBPyConnection, m: dict, lat: float | None, lon: float | None, tz: str | None) -> None:
    existing = con.execute(
        "SELECT condition_id FROM markets WHERE condition_id = ?", [m["condition_id"]]
    ).fetchone()
    if existing:
        con.execute(
            "UPDATE markets SET lat=?, lon=?, tz=?, icao=? WHERE condition_id=?",
            [lat, lon, tz, m["icao"], m["condition_id"]],
        )
    else:
        con.execute(
            """INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                m["condition_id"], m["event_slug"], m["city"], m["event_date"],
                m["question"], m["bucket_lo"], m["bucket_hi"], m["unit"], m["direction"],
                m["yes_token_id"], m["no_token_id"], m["icao"],
                lat, lon, tz,
                datetime.now(timezone.utc),
            ],
        )


def insert_snapshot(
    con: duckdb.DuckDBPyConnection,
    condition_id: str,
    sampled_at: datetime,
    local_hour: float,
    best_bid: float | None,
    best_ask: float | None,
) -> None:
    mid = ((best_bid or 0) + (best_ask or 0)) / 2 if (best_bid is not None and best_ask is not None) else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
    con.execute(
        "INSERT INTO book_snapshots VALUES (nextval('seq_snapshot'),?,?,?,?,?,?,?)",
        [condition_id, sampled_at, local_hour, best_bid, best_ask, mid, spread],
    )


def insert_weather_obs(
    con: duckdb.DuckDBPyConnection,
    city: str,
    icao: str | None,
    observed_at: datetime,
    temp_c: float,
    source: str,
) -> None:
    con.execute(
        "INSERT INTO weather_obs VALUES (nextval('seq_weather'),?,?,?,?,?)",
        [city, icao, observed_at, temp_c, source],
    )


def insert_forecast(
    con: duckdb.DuckDBPyConnection,
    city: str,
    icao: str | None,
    sampled_at: datetime,
    forecast_date: str,
    forecast_high_c: float,
    source: str,
) -> None:
    con.execute(
        "INSERT INTO forecasts VALUES (nextval('seq_forecast'),?,?,?,?,?,?)",
        [city, icao, sampled_at, forecast_date, forecast_high_c, source],
    )


# ── Main collect loop ──────────────────────────────────────────────────────────

_coords_cache: dict[str, tuple[float | None, float | None, str | None]] = {}


def _get_coords(icao: str | None, city: str | None) -> tuple[float | None, float | None, str | None]:
    key = f"{icao}|{city}"
    if key not in _coords_cache:
        _coords_cache[key] = _resolve_coords_and_tz(icao, city)
    return _coords_cache[key]


def collect_once(con: duckdb.DuckDBPyConnection) -> None:
    now_utc = datetime.now(timezone.utc)
    log.info("Discovering highest-temp markets...")
    markets = fetch_highest_temp_markets()
    log.info("Found %d bucket markets", len(markets))

    # Group by (city, event_date) to avoid duplicate weather fetches
    city_date_seen: set[tuple[str, str]] = set()

    for m in markets:
        cid = m["condition_id"]
        city = m["city"] or ""
        date = m["event_date"] or ""

        icao = m["icao"]
        lat, lon, tz = _get_coords(icao, city)

        # Upsert market metadata
        upsert_market(con, m, lat, lon, tz)

        # Book snapshot — use Gamma-embedded bid/ask (CLOB /book requires auth for these tokens)
        local_hour = _local_hour(now_utc, tz)
        bid = m.get("best_bid")
        ask = m.get("best_ask")
        # Fall back to outcomePrices mid if bid/ask missing
        if bid is None and ask is None and m.get("yes_mid") is not None:
            mid_val = m["yes_mid"]
            bid = max(0.0, mid_val - 0.001)
            ask = min(1.0, mid_val + 0.001)
        insert_snapshot(con, cid, now_utc, local_hour, bid, ask)
        log.debug("  %s  bid=%s ask=%s", m["question"][:50], bid, ask)

        # Weather once per (city, date)
        if city and date and (city, date) not in city_date_seen:
            city_date_seen.add((city, date))
            # METAR
            if icao:
                temp_c = fetch_metar_temp(icao)
                if temp_c is not None:
                    insert_weather_obs(con, city, icao, now_utc, temp_c, "metar")
                    log.debug("  METAR %s: %.1f°C", icao, temp_c)
            # Open-Meteo current + forecast
            if lat is not None:
                current_c, high_c = fetch_open_meteo_current(lat, lon, date)
                if current_c is not None:
                    insert_weather_obs(con, city, icao, now_utc, current_c, "open_meteo")
                if high_c is not None:
                    insert_forecast(con, city, icao, now_utc, date, high_c, "open_meteo")

    con.commit()
    log.info("Collect pass done. %d markets processed.", len(markets))


def _local_hour(utc: datetime, tz: str | None) -> float:
    if not tz:
        return utc.hour + utc.minute / 60
    try:
        from zoneinfo import ZoneInfo
        local = utc.astimezone(ZoneInfo(tz))
        return local.hour + local.minute / 60
    except Exception:
        return utc.hour + utc.minute / 60


def run_collector(poll_minutes: int = 2, once: bool = False) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    con = init_db()
    log.info("DB: %s", DB_PATH)
    while True:
        try:
            collect_once(con)
        except Exception as exc:
            log.error("collect_once failed: %s", exc, exc_info=True)
        if once:
            break
        log.info("Sleeping %d min...", poll_minutes)
        time.sleep(poll_minutes * 60)
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Overnight highest-temp market collector")
    parser.add_argument("--poll", type=int, default=2, help="Poll interval in minutes (default: 2)")
    parser.add_argument("--once", action="store_true", help="Run one pass then exit")
    args = parser.parse_args()
    run_collector(poll_minutes=args.poll, once=args.once)
