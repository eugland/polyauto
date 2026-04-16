"""
Backfill realized daily-high temperatures and per-bracket outcomes into
market_snapshots rows for events whose decide time has passed.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone

from automata.db import DB_PATH
from automata.weather import (
    fetch_city_coords,
    fetch_noaa_high,
    fetch_open_meteo_archive_high,
    fetch_open_meteo_high,
)

log = logging.getLogger(__name__)


def _outcome(
    direction: str,
    threshold: float,
    realized: float,
    threshold_hi: float | None = None,
) -> str:
    """Return 'win' if YES resolves true for this bracket, else 'loss'."""
    if direction == "higher":
        return "win" if realized >= threshold else "loss"
    if direction == "below":
        return "win" if realized <= threshold else "loss"
    if direction == "range" and threshold_hi is not None:
        return "win" if threshold <= realized <= threshold_hi else "loss"
    if direction == "exact":
        return "win" if abs(realized - threshold) < 0.5 else "loss"
    return "loss"


def _fetch_high(city: str, lat: float, lon: float, event_date: str, unit: str) -> float | None:
    # Archive API is authoritative for past dates.
    high = fetch_open_meteo_archive_high(lat, lon, event_date, unit)
    if high is not None:
        return high
    # Recent dates may not yet be in the archive — try the forecast endpoint.
    high = fetch_open_meteo_high(lat, lon, event_date, unit)
    if high is not None:
        return high
    return fetch_noaa_high(lat, lon, event_date, unit)


def resolve_snapshots() -> int:
    """
    For each (city, event_date, unit) in market_snapshots with unresolved rows
    whose event_date is strictly in the past, fetch the realized daily high
    and backfill resolved_temp + outcome on every matching row.
    Returns the number of rows updated.
    """
    today = date.today()
    with sqlite3.connect(DB_PATH) as conn:
        unresolved = conn.execute(
            """
            SELECT DISTINCT city, event_date, unit
              FROM market_snapshots
             WHERE resolved_temp IS NULL
               AND event_date < ?
            """,
            (today.isoformat(),),
        ).fetchall()

    if not unresolved:
        return 0

    log.info("Resolver: %d unresolved (city, date, unit) combos", len(unresolved))

    # Cache coords per city.
    coord_cache: dict[str, tuple[float, float] | None] = {}
    updated_rows = 0

    for city, event_date, unit in unresolved:
        if city not in coord_cache:
            coord_cache[city] = fetch_city_coords(city)
        coords = coord_cache[city]
        if coords is None:
            log.warning("Resolver: no coords for %s — skipping", city)
            continue

        lat, lon = coords
        high = _fetch_high(city, lat, lon, event_date, unit or "C")
        if high is None:
            log.warning("Resolver: no archive high for %s %s (%s) — retry later", city, event_date, unit)
            continue

        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT id, bracket_direction, bracket_threshold
                  FROM market_snapshots
                 WHERE city = ? AND event_date = ? AND resolved_temp IS NULL
                """,
                (city, event_date),
            ).fetchall()

            for row_id, direction, br_thr in rows:
                if direction is None or br_thr is None:
                    continue
                outcome = _outcome(direction, float(br_thr), high)
                conn.execute(
                    "UPDATE market_snapshots SET resolved_temp = ?, outcome = ? WHERE id = ?",
                    (high, outcome, row_id),
                )
                updated_rows += 1
            conn.commit()

        log.info("Resolver: %s %s high=%.2f°%s → updated rows", city, event_date, high, unit or "C")

    return updated_rows
