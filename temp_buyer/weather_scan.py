"""
temp_buyer/weather_scan.py

Reusable weather-model scan recorder. Given a list of Polymarket
"highest temperature" events (Gamma-shaped dicts with ``description``,
``markets``, ``slug`` …), fetches forecasts + METAR for each (ICAO, date)
pair and writes one ``ScanRecord`` per market-bucket via
``weather_model.record_scan_batch``.

Extracted from ``temp_buyer/weather_bot.run()`` so non-weather bots
(``temp_buyer``, ad-hoc scripts) can populate the weather-model DB
without booting the buy logic.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from temp_buyer import config

log = logging.getLogger(__name__)


def _city_from_slug(slug: str) -> str | None:
    from temp_buyer.view import _city_date_from_slug
    city, _dt, _ = _city_date_from_slug(slug or "")
    return city


def _resolution_dt_for_event(event: dict[str, Any], slug: str) -> datetime | None:
    from temp_buyer.view import _city_date_from_slug, _resolution_dt
    city, dt, _ = _city_date_from_slug(slug or "")
    return _resolution_dt(event, city, dt)


def _event_date_from_market(market: dict[str, Any], event: dict[str, Any]) -> str:
    raw = (
        market.get("endDateIso")
        or market.get("endDate")
        or event.get("endDate")
        or event.get("endDateIso")
        or ""
    )
    return str(raw)[:10]


def scan_and_record_events(
    events: Iterable[dict[str, Any]],
    *,
    host: str | None = None,
    log_label: str = "weather-model",
) -> dict[str, Any]:
    """
    For every event/market bucket in ``events``, fetch forecasts + METAR
    and write a ``ScanRecord`` to the weather_model DB. Returns a summary
    dict ``{scanned, skipped, edges_flagged}``.

    Network work (coords, forecasts, METAR, order books) is done in
    parallel and deduped by (ICAO, date) so passing 50 events does not
    fan out to 500 forecast calls.

    Idempotency: each call inserts a fresh row in ``scans``; the model's
    bias/sigma update only fires when an outcome lands, so repeated
    scans on the same event are safe and expected (more rows = more
    granular UI history).
    """
    from temp_buyer.client import get_best_books_bulk
    from temp_buyer.parser import (
        _extract_no_token_id,
        _extract_yes_token_id,
        _parse_threshold,
    )
    from temp_buyer.weather import (
        extract_all_urls,
        extract_icao_from_url,
        extract_station_name,
        extract_unit,
        fetch_coords_for_stations,
        fetch_forecasts_for_events,
        fetch_metar_today_stats,
    )
    from temp_buyer import weather_model
    from temp_buyer.view import _parse_json_list

    summary = {"scanned": 0, "skipped_no_market": 0, "skipped_no_threshold": 0,
               "edges_flagged": 0, "events": 0, "buckets": 0}

    host = host or config.get_str(
        "POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com"
    )
    model_edge_max_bps = config.get_float(
        "MODEL_EDGE_MAX_BPS", "weather", "model_edge_max_bps", 200.0
    )

    # Per-event metadata: ICAO (from description), unit, station name,
    # resolution_dt, event_date string. Resolution_dt is what the model
    # uses for hours_to_res / lead_bucket, so it must be the same 3pm-local
    # value that weather_bot uses.
    event_meta: dict[str, dict[str, Any]] = {}
    for ev in events:
        slug = str(ev.get("slug") or "")
        if not slug:
            continue
        desc = str(ev.get("description") or "")
        urls = extract_all_urls(desc)
        icao = next(
            (code for u in urls if (code := extract_icao_from_url(u)) is not None),
            None,
        )
        end_date = _event_date_from_market({}, ev)
        event_meta[slug] = {
            "icao": icao,
            "station_name": extract_station_name(desc),
            "unit": extract_unit(desc),
            "event_date": end_date,
            "city": _city_from_slug(slug),
            "resolution_dt": _resolution_dt_for_event(ev, slug),
            "raw": ev,
        }

    summary["events"] = len(event_meta)
    if not event_meta:
        return summary

    # ── Build candidate list (one per market-bucket) ────────────────────
    candidates: list[dict[str, Any]] = []
    for slug, meta in event_meta.items():
        markets = _parse_json_list(meta["raw"].get("markets"))
        for raw in markets:
            if not isinstance(raw, dict):
                continue
            if raw.get("closed") or (raw.get("active") is not None and not raw.get("active")):
                continue
            question = str(raw.get("groupItemTitle") or raw.get("question") or "").strip()
            if not question:
                continue
            parsed = _parse_threshold(question)
            if not parsed:
                summary["skipped_no_threshold"] += 1
                continue
            no_token = _extract_no_token_id(raw)
            yes_token = _extract_yes_token_id(raw)
            if not no_token:
                continue
            threshold, threshold_hi, _unit_q, direction = parsed
            candidates.append({
                "slug":          slug,
                "city":          meta["city"],
                "icao":          meta["icao"],
                "unit":          meta["unit"],
                "event_date":    meta["event_date"],
                "resolution_dt": meta["resolution_dt"],
                "question":      question,
                "no_token":      no_token,
                "yes_token":     yes_token,
                "threshold":     threshold,
                "threshold_hi": threshold_hi,
                "direction":    direction,
                "no_bid":        None,
                "no_ask":        None,
                "yes_bid":       None,
                "yes_ask":       None,
                "openmeteo":     None,
                "noaa":          None,
                "taf":           None,
                "metar_current": None,
                "metar_max":     None,
            })
    summary["buckets"] = len(candidates)
    if not candidates:
        return summary

    # ── Bulk-fetch order books for every NO + YES token in scope ────────
    token_ids: list[str] = []
    for c in candidates:
        token_ids.append(c["no_token"])
        if c["yes_token"]:
            token_ids.append(c["yes_token"])
    try:
        books = get_best_books_bulk(host, token_ids)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] order-book fetch failed: %s — pricing fields blank", log_label, exc)
        books = {}
    for c in candidates:
        nb = books.get(c["no_token"]) or {}
        c["no_bid"] = nb.get("bid")
        c["no_ask"] = nb.get("ask")
        if c["yes_token"]:
            yb = books.get(c["yes_token"]) or {}
            c["yes_bid"] = yb.get("bid")
            c["yes_ask"] = yb.get("ask")

    # ── Forecasts: dedupe by (icao, date) ───────────────────────────────
    icaos = {c["icao"] for c in candidates if c["icao"]}
    coords = fetch_coords_for_stations(list(icaos)) if icaos else {}
    event_list = [
        {"icao": c["icao"], "date": c["event_date"], "unit": c["unit"]}
        for c in candidates if c["icao"] and c["event_date"]
    ]
    forecasts = fetch_forecasts_for_events(event_list, coords) if event_list else {}
    for c in candidates:
        fc = forecasts.get((c["icao"], c["event_date"])) if c["icao"] else None
        if fc:
            c["openmeteo"] = fc.get("open_meteo")
            c["noaa"] = fc.get("noaa")
            c["taf"] = fc.get("taf")

    # ── METAR: dedupe by (icao, tz, date, unit) ─────────────────────────
    CITY_TZ: dict[str, str] = {
        "Ankara":        "Europe/Istanbul",
        "Atlanta":       "America/New_York",
        "Austin":        "America/Chicago",
        "Beijing":       "Asia/Shanghai",
        "Buenos Aires":  "America/Argentina/Buenos_Aires",
        "Chengdu":       "Asia/Shanghai",
        "Chicago":       "America/Chicago",
        "Chongqing":     "Asia/Shanghai",
        "Dallas":        "America/Chicago",
        "Denver":        "America/Denver",
        "Hong Kong":     "Asia/Hong_Kong",
        "Houston":       "America/Chicago",
        "Istanbul":      "Europe/Istanbul",
        "London":        "Europe/London",
        "Los Angeles":   "America/Los_Angeles",
        "Lucknow":       "Asia/Kolkata",
        "Madrid":        "Europe/Madrid",
        "Mexico City":   "America/Mexico_City",
        "Miami":         "America/New_York",
        "Milan":         "Europe/Rome",
        "Moscow":        "Europe/Moscow",
        "Munich":        "Europe/Berlin",
        "NYC":           "America/New_York",
        "Paris":         "Europe/Paris",
        "San Francisco": "America/Los_Angeles",
        "Sao Paulo":     "America/Sao_Paulo",
        "Seattle":       "America/Los_Angeles",
        "Seoul":         "Asia/Seoul",
        "Shanghai":      "Asia/Shanghai",
        "Shenzhen":      "Asia/Shanghai",
        "Singapore":     "Asia/Singapore",
        "Taipei":        "Asia/Taipei",
        "Tel Aviv":      "Asia/Jerusalem",
        "Tokyo":         "Asia/Tokyo",
        "Toronto":       "America/Toronto",
        "Warsaw":        "Europe/Warsaw",
        "Wellington":    "Pacific/Auckland",
        "Wuhan":         "Asia/Shanghai",
    }
    metar_keys: set[tuple[str, str, str, str]] = set()
    for c in candidates:
        if not c["icao"]:
            continue
        tz_name = CITY_TZ.get(c["city"] or "", "UTC")
        metar_keys.add((c["icao"], tz_name, c["event_date"], c["unit"] or "C"))
    metar_stats: dict[tuple[str, str], tuple[float | None, float | None]] = {}
    if metar_keys:
        with ThreadPoolExecutor(max_workers=min(16, max(1, len(metar_keys)))) as ex:
            futs = {
                ex.submit(fetch_metar_today_stats, icao, tz, ed, u): (icao, ed)
                for (icao, tz, ed, u) in metar_keys
            }
            for fut in as_completed(futs):
                try:
                    metar_stats[futs[fut]] = fut.result()
                except Exception:
                    metar_stats[futs[fut]] = (None, None)
    for c in candidates:
        key = (c["icao"] or "", c["event_date"])
        cur, mx = metar_stats.get(key, (None, None))
        c["metar_current"] = cur
        c["metar_max"] = mx

    # ── Build & insert ScanRecords ──────────────────────────────────────
    weather_model.init_db()
    recs = [
        weather_model.ScanRecord(
            city=c["city"] or "",
            icao=c["icao"],
            event_date=c["event_date"],
            event_slug=c["slug"],
            question=c["question"],
            token_id=c["no_token"],
            yes_token_id=c["yes_token"],
            resolution_dt=c["resolution_dt"],
            unit=c["unit"],
            threshold=c["threshold"],
            threshold_hi=c["threshold_hi"],
            direction=c["direction"],
            openmeteo_high=c["openmeteo"],
            noaa_high=c["noaa"],
            taf_high=c["taf"],
            metar_current=c["metar_current"],
            metar_max_so_far=c["metar_max"],
            no_bid=c["no_bid"],
            no_ask=c["no_ask"],
            yes_bid=c["yes_bid"],
            yes_ask=c["yes_ask"],
        )
        for c in candidates
    ]
    try:
        model_out = weather_model.record_scan_batch(recs)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] record_scan_batch failed: %s", log_label, exc)
        return summary

    summary["scanned"] = len(recs)

    # Compact log of largest market-vs-fair edges (≥50 bps) for visibility.
    edges: list[tuple[str, float]] = []
    for rec, m in zip(candidates, model_out):
        fp = m.get("fair_no_prob")
        ask = rec["no_ask"]
        if fp is None or ask is None:
            continue
        edge = (ask - fp) * 10000.0
        if abs(edge) >= 50:
            edges.append((f"{rec['city']} {rec['question']}", edge))
        if edge > model_edge_max_bps:
            summary["edges_flagged"] += 1
    log.info(
        "[%s] scanned %d candidates across %d events; %d flagged-edge buckets",
        log_label, summary["scanned"], summary["events"], len(edges),
    )
    for label, edge in sorted(edges, key=lambda t: -abs(t[1]))[:5]:
        log.info("[%s]  edge %+.0fbps  %s", log_label, edge, label)
    return summary


def maybe_backfill_outcomes(log_label: str = "weather-model") -> dict[str, Any] | None:
    """Wrapper that swallows exceptions — safe to call from a hot loop."""
    from temp_buyer import weather_model
    try:
        stats = weather_model.backfill_outcomes_from_polymarket()
        if stats.get("resolved"):
            log.info(
                "[%s] backfill: resolved=%d checked=%d skipped=%d errors=%d",
                log_label, stats.get("resolved", 0), stats.get("checked", 0),
                stats.get("skipped", 0), stats.get("errors", 0),
            )
        return stats
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] backfill_outcomes_from_polymarket failed: %s", log_label, exc)
        return None
