"""Cached observed-temperature lookup for /weather-pro activity rows.

Resolves city → (lat,lon) once via Open-Meteo geocoding, then fetches hourly
observed temperatures (also from Open-Meteo) and stores them per (city, hour)
so repeated requests for the same hour are free.

The request handler only ever reads the cache (never blocks on HTTP). Missing
(city, day) buckets are warmed by a background thread launched from
`annotate_activity` so the next page refresh has the data.
"""
from __future__ import annotations

import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

# Track (city, day_iso) pairs already being warmed so we don't spawn duplicate work
# for the same bucket across overlapping requests.
_WARMING: set[tuple[str, str]] = set()
_WARMING_LOCK = threading.Lock()

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _REPO_ROOT / "db" / "bets.db"

# Cities the weather bots actively trade — used to extract a city from a
# Polymarket activity title like "Mexico City be 20°C on April 21".
KNOWN_CITIES: list[str] = [
    "Ankara", "Atlanta", "Austin", "Beijing", "Buenos Aires", "Chengdu", "Chicago",
    "Chongqing", "Dallas", "Denver", "Hong Kong", "Houston", "Istanbul", "London",
    "Los Angeles", "Lucknow", "Madrid", "Mexico City", "Miami", "Milan", "Moscow",
    "Munich", "NYC", "New York", "Paris", "San Francisco", "Sao Paulo", "Seattle",
    "Seoul", "Shanghai", "Shenzhen", "Singapore", "Taipei", "Tel Aviv", "Tokyo",
    "Toronto", "Warsaw", "Wellington", "Wuhan",
]
# Match longest names first so "Mexico City" beats "Mexico", "Hong Kong" beats "Hong".
_CITY_RE = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(KNOWN_CITIES, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)
# Map back to canonical capitalisation.
_CITY_CANONICAL = {c.lower(): c for c in KNOWN_CITIES}


def _ensure_schema() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS city_coords ("
            "  city TEXT PRIMARY KEY, lat REAL NOT NULL, lon REAL NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS temp_obs_cache ("
            "  city TEXT NOT NULL, hour_iso TEXT NOT NULL, temp_c REAL,"
            "  PRIMARY KEY(city, hour_iso)"
            ")"
        )


def extract_city(title: str | None) -> str | None:
    if not title:
        return None
    m = _CITY_RE.search(title)
    if not m:
        return None
    return _CITY_CANONICAL.get(m.group(1).lower())


def _get_coords(city: str) -> tuple[float, float] | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT lat, lon FROM city_coords WHERE city = ?", (city,)
        ).fetchone()
    if row:
        return float(row[0]), float(row[1])
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city, "count": 1, "language": "en", "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        results = (resp.json() or {}).get("results") or []
    except Exception:
        return None
    if not results:
        return None
    lat, lon = results[0].get("latitude"), results[0].get("longitude")
    if lat is None or lon is None:
        return None
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO city_coords (city, lat, lon) VALUES (?, ?, ?)",
            (city, float(lat), float(lon)),
        )
    return float(lat), float(lon)


def _hour_bucket(ts_epoch: float) -> str:
    dt = datetime.fromtimestamp(float(ts_epoch), tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    return dt.strftime("%Y-%m-%dT%H:00")  # matches Open-Meteo "time" format (UTC)


def _populate_day(city: str, lat: float, lon: float, day_iso: str) -> None:
    """Fetch and cache hourly temps for `city` on UTC `day_iso` (YYYY-MM-DD)."""
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "timezone": "UTC",
                "start_date": day_iso,
                "end_date": day_iso,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = (resp.json() or {}).get("hourly") or {}
    except Exception:
        return
    times = data.get("time") or []
    temps = data.get("temperature_2m") or []
    if not times:
        return
    rows = [
        (city, t[:13] + ":00", float(temp))
        for t, temp in zip(times, temps)
        if temp is not None
    ]
    if not rows:
        return
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO temp_obs_cache (city, hour_iso, temp_c) VALUES (?, ?, ?)",
            rows,
        )


def _temp_from_cache(city: str, bucket: str) -> float | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT temp_c FROM temp_obs_cache WHERE city = ? AND hour_iso = ?",
            (city, bucket),
        ).fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


def _warm_buckets(missing: set[tuple[str, str]]) -> None:
    """Background worker: populate (city, day) buckets that are not yet cached."""
    try:
        for city, day in missing:
            coords = _get_coords(city)
            if not coords:
                continue
            _populate_day(city, coords[0], coords[1], day)
    finally:
        with _WARMING_LOCK:
            for key in missing:
                _WARMING.discard(key)


def annotate_activity(items: Iterable[dict]) -> None:
    """Add `obs_temp_c` to each activity item from the cache (no blocking I/O).
    Schedules a background fetch for any missing (city, day) buckets so the next
    refresh has the data.
    """
    _ensure_schema()
    missing: set[tuple[str, str]] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        title = it.get("title") or it.get("market") or it.get("question") or ""
        if "°" not in title:
            continue  # non-temperature item, skip
        city = extract_city(title)
        if not city:
            continue
        ts = it.get("timestamp") or it.get("created_at") or it.get("createdAt") or it.get("time")
        if ts is None:
            continue
        try:
            ts_f = float(ts)
        except (TypeError, ValueError):
            continue
        if ts_f > 1e12:  # ms → s
            ts_f /= 1000.0
        bucket = _hour_bucket(ts_f)
        cached = _temp_from_cache(city, bucket)
        if cached is not None:
            it["obs_temp_c"] = round(cached, 1)
            it["obs_city"] = city
        else:
            missing.add((city, bucket[:10]))

    if missing:
        with _WARMING_LOCK:
            new = {key for key in missing if key not in _WARMING}
            _WARMING.update(new)
        if new:
            t = threading.Thread(target=_warm_buckets, args=(new,), daemon=True)
            t.start()
