"""
Backfill the weather_model bias/σ EMAs from historical Open-Meteo forecasts
and Polymarket-resolved outcomes.

What this does
--------------
1. Walks Gamma's *closed* temperature events over a configurable window.
2. For each event, parses:
     - station ICAO from the description (Wunderground or NOAA URL)
     - winning-bucket midpoint → `actual_high` (ground truth)
     - city / date / unit
3. Fetches **Open-Meteo's historical forecast** (what the model predicted for
   that day, from the archived model runs) for the station's (lat, lon).
4. Writes one synthesized scan into `db/weather_model.db.scans` with
   `source_used="openmeteo"`, `hours_to_res=30`, `lead_bucket="24-48h"` so the
   training signal lands in the mid-range bucket.
5. Calls `record_outcome(...)` which invokes `update_from_outcome()` →
   residual = actual − forecast → updates the `bias[icao, "openmeteo"]` and
   `sigma[icao, "24-48h"]` EMAs.

Idempotency
-----------
`record_outcome` deduplicates by PK (icao, event_date). Re-running the script
is safe: already-trained (icao, event_date) pairs are skipped and don't
re-update the EMAs.

Limitations
-----------
- NOAA NWS historical forecasts and archived TAFs are not available via a
  clean free API, so this only trains the "openmeteo" source. NOAA and TAF
  bias/σ still learn live from resolved outcomes.
- Open-Meteo's Historical Forecast API returns the best-available archived
  forecast per target date — it doesn't resolve to a specific lead time, so
  all synthesized scans go into the "24-48h" bucket by design.

Usage
-----
    python -m scripts.backfill_training --days 180
    python -m scripts.backfill_training --days 30 --dry-run
    python -m scripts.backfill_training --days 90 --stations EGLC,KATL,MMMX
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import requests

from automata import weather_model
from automata.polymarket import fetch_closed_temperature_events
from automata.weather import (
    extract_all_urls,
    extract_icao_from_url,
    extract_station_name,
    extract_unit,
    fetch_station_coords,
)


log = logging.getLogger("backfill_training")


HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"


def _extract_city(title: str) -> str:
    m = re.search(r"in\s+(.+?)\s+on\s+", title, re.IGNORECASE)
    return m.group(1).strip() if m else title


def _extract_event_date(event: dict[str, Any]) -> str | None:
    """YYYY-MM-DD of the event's resolution day, from endDateIso or endDate."""
    raw = event.get("endDateIso") or event.get("endDate") or ""
    return raw[:10] if raw else None


def _find_winner_market(event: dict[str, Any]) -> dict[str, Any] | None:
    for m in event.get("markets") or []:
        prices = m.get("outcomePrices")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = None
        if not isinstance(prices, list) or not prices:
            continue
        try:
            p_yes = float(prices[0])
        except (TypeError, ValueError):
            continue
        if p_yes >= 0.99:
            return m
    return None


def fetch_historical_forecast_high(
    lat: float,
    lon: float,
    date_str: str,
    unit: str = "C",
) -> float | None:
    """Open-Meteo Historical Forecast API — what the model predicted for date_str."""
    try:
        resp = requests.get(
            HISTORICAL_FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": date_str,
                "end_date": date_str,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit" if unit.upper() == "F" else "celsius",
                "timezone": "auto",
            },
            timeout=20,
        )
        resp.raise_for_status()
        vals = ((resp.json() or {}).get("daily") or {}).get("temperature_2m_max") or []
        return float(vals[0]) if vals and vals[0] is not None else None
    except Exception as exc:
        log.debug("historical forecast %s %s failed: %s", lat, date_str, exc)
        return None


def _extract_icao(event: dict[str, Any]) -> str | None:
    desc = str(event.get("description") or "")
    for u in extract_all_urls(desc):
        code = extract_icao_from_url(u)
        if code:
            return code
    return None


def _build_training_row(event: dict[str, Any],
                        coord_cache: dict[str, tuple[float, float] | None]
                        ) -> dict[str, Any] | None:
    """One pre-normalised training row from a raw Gamma event, or None if unusable."""
    title = str(event.get("title") or "")
    event_slug = str(event.get("slug") or "")
    event_date = _extract_event_date(event)
    if not event_date:
        return None

    icao = _extract_icao(event)
    if not icao:
        return None

    desc = str(event.get("description") or "")
    unit = extract_unit(desc)
    city = _extract_city(title)

    winner = _find_winner_market(event)
    if winner is None:
        return None
    q = str(winner.get("groupItemTitle") or winner.get("question") or "")
    lo, hi, u = weather_model._parse_bucket_range(q)
    if lo is None or hi is None:
        return None
    actual_high = (lo + hi) / 2.0
    unit = unit or u or "C"

    # Station coords — fetch once, cache across rows.
    if icao not in coord_cache:
        coord_cache[icao] = fetch_station_coords(icao)
    coords = coord_cache[icao]
    if coords is None:
        return None
    lat, lon = coords

    return {
        "icao": icao,
        "event_date": event_date,
        "event_slug": event_slug,
        "city": city,
        "unit": unit,
        "threshold": None,
        "direction": None,
        "actual_high": actual_high,
        "bucket_lo": lo,
        "bucket_hi": hi,
        "bucket_title": q,
        "lat": lat,
        "lon": lon,
    }


def _fetch_forecast_only(row: dict[str, Any]) -> dict[str, Any]:
    """Thread-safe phase: just the Open-Meteo HTTP call, no DB writes."""
    forecast = fetch_historical_forecast_high(
        row["lat"], row["lon"], row["event_date"], row["unit"],
    )
    return {**row, "forecast": forecast}


def _write_training_row(row: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    """Serial phase: DuckDB writes. Must run on one thread because DuckDB's
    file-lock model doesn't permit concurrent writer connections on the same DB."""
    forecast = row.get("forecast")
    icao = row["icao"]
    event_date = row["event_date"]
    unit = row["unit"]
    if forecast is None:
        return {"icao": icao, "event_date": event_date, "status": "no-historical-forecast"}

    residual = row["actual_high"] - forecast
    result = {
        "icao": icao, "event_date": event_date, "city": row["city"],
        "forecast": round(forecast, 2),
        "actual": round(row["actual_high"], 2),
        "residual": round(residual, 2),
        "bucket": row["bucket_title"],
    }

    if dry_run:
        result["status"] = "dry-run"
        return result

    rec = weather_model.ScanRecord(
        city=row["city"],
        icao=icao,
        event_date=event_date,
        event_slug=row["event_slug"],
        question=f"[backfill] {row['bucket_title']}",
        token_id=None,
        yes_token_id=None,
        resolution_dt=None,
        unit=unit,
        threshold=None,
        threshold_hi=None,
        direction=None,
        openmeteo_high=forecast,
        noaa_high=None,
        taf_high=None,
        metar_current=None,
        metar_max_so_far=None,
        no_bid=None,
        no_ask=None,
        yes_bid=None,
        yes_ask=None,
    )
    weather_model.record_scan_batch([rec])
    out = weather_model.record_outcome(
        icao=icao,
        event_date=event_date,
        city=row["city"],
        unit=unit,
        actual_high=row["actual_high"],
        bucket_lo=row["bucket_lo"],
        bucket_hi=row["bucket_hi"],
        source="backfill_training",
        event_slug=row["event_slug"],
    )
    result["status"] = out.get("status", "trained")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap the weather_model bias/σ from historical Open-Meteo + Polymarket outcomes.",
    )
    parser.add_argument("--days", type=int, default=180,
                        help="Look back this many days for closed events (default: 180)")
    parser.add_argument("--stations", type=str, default=None,
                        help="Comma-separated ICAOs to restrict training to (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write any scans/outcomes; just report residuals")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after this many trained rows (for quick tests)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel forecast fetchers (default: 8)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    weather_model.init_db()

    allowed = None
    if args.stations:
        allowed = {s.strip().upper() for s in args.stations.split(",") if s.strip()}
        log.info("Restricting to stations: %s", sorted(allowed))

    t0 = time.time()
    log.info("Fetching closed temperature events from Gamma (%d days back)...", args.days)
    events = fetch_closed_temperature_events(days_back=args.days)
    log.info("  %d closed events fetched in %.1fs", len(events), time.time() - t0)

    coord_cache: dict[str, tuple[float, float] | None] = {}
    rows: list[dict[str, Any]] = []
    dropped = {"no-date": 0, "no-icao": 0, "no-winner": 0, "no-bucket-parse": 0,
               "no-coords": 0, "station-filter": 0}
    for event in events:
        row = _build_training_row(event, coord_cache)
        if row is None:
            # We don't know exactly which step failed from here; re-probe for
            # counter accuracy, cheap because _build_training_row is already cached.
            if not _extract_event_date(event):         dropped["no-date"] += 1
            elif not _extract_icao(event):             dropped["no-icao"] += 1
            elif _find_winner_market(event) is None:   dropped["no-winner"] += 1
            else:
                w = _find_winner_market(event)
                q = str(w.get("groupItemTitle") or w.get("question") or "")
                lo, hi, _ = weather_model._parse_bucket_range(q)
                if lo is None or hi is None:           dropped["no-bucket-parse"] += 1
                else:                                  dropped["no-coords"] += 1
            continue
        if allowed and row["icao"] not in allowed:
            dropped["station-filter"] += 1
            continue
        rows.append(row)

    log.info("  %d trainable rows after parse; dropped: %s", len(rows), dropped)
    if args.limit:
        rows = rows[: args.limit]
        log.info("  limited to first %d rows", len(rows))
    if not rows:
        log.warning("Nothing to train on — exiting.")
        return 0

    # Phase 1 — fetch historical forecasts in parallel (HTTP-bound, safe).
    # Phase 2 — write to DuckDB serially on the main thread (DuckDB doesn't
    # tolerate concurrent writer connections on the same file).
    station_updates: dict[str, int] = {}
    trained = 0
    skipped_existing = 0
    no_forecast = 0
    fetch_errors = 0

    fetched: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(_fetch_forecast_only, r): r for r in rows}
        for fut in as_completed(futures):
            r = futures[fut]
            try:
                fetched.append(fut.result())
            except Exception as exc:
                log.warning("fetch failed %s %s: %s", r["icao"], r["event_date"], exc)
                fetch_errors += 1

    log.info("Phase 1 done: %d forecasts fetched, %d errors; writing to DB...",
             len(fetched), fetch_errors)

    for r in fetched:
        try:
            res = _write_training_row(r, args.dry_run)
        except Exception as exc:
            log.warning("write failed %s %s: %s", r["icao"], r["event_date"], exc)
            continue
        status = res.get("status")
        if status == "no-historical-forecast":
            no_forecast += 1
        elif status == "already-recorded":
            skipped_existing += 1
        else:
            trained += 1
            station_updates[r["icao"]] = station_updates.get(r["icao"], 0) + 1
            if trained <= 20 or trained % 100 == 0:
                log.info(
                    "  %s %s %s  fcst=%.1f  actual=%.1f  resid=%+.2f  (%s)",
                    r["city"], r["event_date"], r["icao"],
                    res["forecast"], res["actual"], res["residual"], status,
                )

    log.info("────")
    log.info("Trained:           %d", trained)
    log.info("Already-recorded:  %d (skipped, EMA unchanged)", skipped_existing)
    log.info("No historical forecast: %d", no_forecast)
    log.info("Stations touched:  %d", len(station_updates))
    if station_updates:
        top = sorted(station_updates.items(), key=lambda kv: -kv[1])[:15]
        log.info("Top stations by sample count:")
        for icao, n in top:
            log.info("  %s  n=%d", icao, n)

    if not args.dry_run and trained:
        log.info("")
        log.info("Bias/σ tables now populated — live weather bot will read these")
        log.info("on its next scan (no restart required, EMAs are read per scan).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
