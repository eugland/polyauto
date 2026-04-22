from __future__ import annotations

import io
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from automata import config

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

from dotenv import load_dotenv

# Ensure UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv()

_LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "automata.log"
_file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
_file_handler.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), _file_handler],
)
# basicConfig is a no-op if root logger already has handlers — force-attach file handler
_root = logging.getLogger()
if _file_handler not in _root.handlers:
    _root.addHandler(_file_handler)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("automata")

CONFIG_PATH = Path(__file__).resolve().parent.parent / "webapp.json"


def _fmt_end_date(end_date: str | None) -> str:
    if not end_date:
        return "?"
    return end_date[:10]


def _extract_city(event_title: str) -> str:
    """'Highest temperature in Atlanta on March 30?' → 'Atlanta'"""
    import re
    m = re.search(r"in (.+?) on ", event_title, re.IGNORECASE)
    return m.group(1).strip() if m else event_title


def _extract_title_date(event_title: str) -> str:
    """'Highest temperature in Atlanta on March 30?' → 'March 30'"""
    import re
    m = re.search(r" on (.+?)\??$", event_title, re.IGNORECASE)
    return m.group(1).strip() if m else ""


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _compute_resolution_dt(city: str, title_date: str, end_date_utc_hint: str) -> datetime | None:
    """3 PM local time in the city's tz on the title's date, returned as UTC-aware datetime."""
    if not title_date:
        return None
    import re
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})", title_date.strip())
    if not m:
        return None
    month = _MONTHS.get(m.group(1).lower())
    if not month:
        return None
    try:
        day = int(m.group(2))
    except ValueError:
        return None
    try:
        year = int((end_date_utc_hint or "")[:4]) if end_date_utc_hint else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    tz_name = CITY_TZ.get(city, "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    try:
        local_dt = datetime(year, month, day, 15, 0, 0, tzinfo=tz)
    except ValueError:
        return None
    return local_dt.astimezone(timezone.utc)


_FAR_FUTURE = datetime(9999, 12, 31, tzinfo=timezone.utc)


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _order_open_shares(order: dict[str, Any]) -> float:
    """
    Best-effort remaining shares from an open-order payload.
    """
    for key in ("remaining_size", "size_left", "sizeLeft", "open_size"):
        v = _as_float(order.get(key))
        if v is not None and v >= 0:
            return v
    size = _as_float(order.get("size")) or _as_float(order.get("original_size")) or 0.0
    filled = _as_float(order.get("matched_size")) or _as_float(order.get("filled_size")) or 0.0
    return max(0.0, size - filled)


def _order_price(order: dict[str, Any]) -> float | None:
    return _as_float(order.get("price"))


def _round_down_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 6)
    return math.floor((price + 1e-12) / tick) * tick


def _round_up_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 6)
    return math.ceil((price - 1e-12) / tick) * tick


def _compute_maker_buy_price(
    best_bid: float | None,
    best_ask: float | None,
    min_no_price: float,
    max_no_price: float,
    tick_size: float,
    join_bid_ticks: int,
) -> float | None:
    """
    Build a buy quote within price limits. Quotes ONE TICK above the best ask
    to aggressively cross the spread and win the race against other takers.
    Ceils to tick first (handles between-tick asks), then adds one tick, then
    clamps to max_no_price (0.997).
    """
    if best_ask is None:
        return None  # no counterparty — passive fallback will handle this
    quote = min(best_ask, max_no_price)
    quote = round(_round_up_to_tick(quote, tick_size), 6)
    quote = round(quote + tick_size, 6)       # 1 tick over ask for aggressive take
    quote = min(quote, max_no_price)          # respect the hard cap
    if quote < min_no_price:
        return None
    return quote


def run(
    dry_run: bool = True,
    max_spend_usdc: float | None = None,
    max_orders: int | None = None,
) -> None:
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.parser import _parse_threshold
    from automata.weather import (
        extract_all_urls, extract_icao_from_url,
        extract_station_name, extract_unit,
        fetch_coords_for_stations,
        fetch_forecasts_for_events,
    )

    min_no_price  = config.get_float("MIN_NO_PRICE", "weather", "min_no_price", 0.97)
    max_no_price  = config.get_float("MAX_NO_PRICE", "weather", "max_no_price", 0.997)
    bet_threshold = config.get_float("BET_THRESHOLD", "weather", "bet_threshold", 0.95)   # auto-bet above this
    bet_shares    = 22.0   # first-fill target per city
    max_shares    = 80.0   # top-up ceiling per city
    # Model edge threshold: log (but don't block) candidates where market NO
    # ask is more than this many bps above the model's fair NO prob.
    model_edge_max_bps  = config.get_float("MODEL_EDGE_MAX_BPS", "weather", "model_edge_max_bps", 200.0)
    mm_tick_size = config.get_float("MM_TICK_SIZE", "weather", "mm_tick_size", 0.001)
    mm_join_bid_ticks = config.get_int("MM_JOIN_BID_TICKS", "weather", "mm_join_bid_ticks", 1)
    mm_reprice_cents = config.get_float("MM_REPRICE_CENTS", "weather", "mm_reprice_cents", 0.10)
    mm_reprice_delta = mm_reprice_cents / 100.0
    city_blacklist = set(config.get_list_str(
        "CITY_BLACKLIST", "weather", "city_blacklist",
        ["Seoul", "Taipei", "Lagos", "Denver", "Jakarta"],
    ))

    # ── Fetch markets ─────────────────────────────────────────────────────────
    log.info("Fetching Polymarket temperature markets...")
    payload = fetch_temperature_markets_payload()
    raw_markets = payload["markets"]
    log.info("  %d raw markets fetched", len(raw_markets))

    # ── Group by event, extract station per event ─────────────────────────────
    events: dict[str, dict[str, Any]] = {}
    for raw in raw_markets:
        event_slug = str(raw.get("event_slug") or "")
        event_title = str(raw.get("event_title") or event_slug)

        if event_slug not in events:
            description = str(raw.get("event_description") or "")
            urls = extract_all_urls(description)
            icao = next(
                (code for u in urls if (code := extract_icao_from_url(u)) is not None),
                None,
            )
            end_raw = raw.get("endDateIso") or raw.get("endDate") or ""
            event_date = end_raw[:10] if end_raw else ""
            events[event_slug] = {
                "title": event_title,
                "station_name": extract_station_name(description),
                "icao": icao,
                "unit": extract_unit(description),
                "date": event_date,
                "urls": urls,
                "markets": [],
            }
        events[event_slug]["markets"].append(raw)

    # ── Compute per-event resolution_dt, drop events past grace cutoff ─────
    now_utc_dt = datetime.now(timezone.utc)
    grace_hours = config.get_float("POST_RESOLUTION_GRACE_HOURS", "weather", "post_resolution_grace_hours", 7.0)
    cutoff = now_utc_dt - timedelta(hours=grace_hours)
    for ev in events.values():
        ev_city = _extract_city(ev["title"])
        ev_title_date = _extract_title_date(ev["title"])
        end_hint = ev["date"] or ""
        ev["resolution_dt"] = _compute_resolution_dt(ev_city, ev_title_date, end_hint)
    events = {
        k: v for k, v in events.items()
        if v["resolution_dt"] is not None and v["resolution_dt"] >= cutoff
    }
    sorted_event_items = sorted(events.items(), key=lambda kv: kv[1]["resolution_dt"])
    log.info(
        "  %d events after resolution filter (now=%s UTC, grace=%.1fh)",
        len(sorted_event_items), now_utc_dt.strftime("%Y-%m-%d %H:%M"), grace_hours,
    )

    # ── Scan in batches of 15, earliest-resolution first. Fetch order books
    #    first (cheap); accumulate viable candidates across batches and stop
    #    once we have enough distinct viable cities to cover every bet slot
    #    (or we run out of events). Forecast + METAR only run on whatever
    #    we accumulated. ────────────────────────────────────────────────
    from automata.parser import _extract_no_token_id, _extract_yes_token_id
    from automata.client import get_best_books_bulk
    BATCH_SIZE = 15
    scan_target_cities = config.get_int("SCAN_TARGET_CITIES", "weather", "scan_target_cities", 10)
    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")

    selected_events: dict[str, dict[str, Any]] = {}
    all_candidates: list[dict[str, Any]] = []
    distinct_viable_cities: set[str] = set()

    for batch_start in range(0, len(sorted_event_items), BATCH_SIZE):
        batch_slice = sorted_event_items[batch_start:batch_start + BATCH_SIZE]
        batch_events = dict(batch_slice)
        batch_num = batch_start // BATCH_SIZE + 1
        first_res = batch_slice[0][1]["resolution_dt"].strftime("%H:%MZ")
        last_res  = batch_slice[-1][1]["resolution_dt"].strftime("%H:%MZ")
        log.info("  Batch %d: %d events (%s → %s)", batch_num, len(batch_events), first_res, last_res)
        for ev in batch_events.values():
            res_str = ev["resolution_dt"].strftime("%Y-%m-%d %H:%MZ")
            log.info("    - %s  [res=%s, %s]", ev["title"], res_str, ev["icao"] or "?")

        # Build candidate dicts (parse only — no network yet)
        batch_candidates: list[dict[str, Any]] = []
        for event_slug, event in sorted(batch_events.items()):
            visible = [
                r for r in event["markets"]
                if not r.get("closed") and not (r.get("active") is not None and not r.get("active"))
            ]
            for raw in visible:
                question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
                end_date = _fmt_end_date(raw.get("endDateIso") or raw.get("endDate"))
                parsed = _parse_threshold(question)
                if not parsed:
                    continue
                token_id = _extract_no_token_id(raw)
                if not token_id:
                    continue
                yes_token_id = _extract_yes_token_id(raw)
                threshold, threshold_hi, _unit, direction = parsed
                city_name = _extract_city(event["title"])
                title_date = _extract_title_date(event["title"])
                end_dt_raw = raw.get("endDateIso") or raw.get("endDate") or ""
                batch_candidates.append({
                    "question": question,
                    "event_slug": event_slug,
                    "city": city_name,
                    "title_date": title_date,
                    "token_id": token_id,
                    "yes_token_id": yes_token_id,
                    "price": 0.0,
                    "yes_price": None,
                    "end_date": end_date,
                    "end_datetime": end_dt_raw,
                    "resolution_dt": _compute_resolution_dt(city_name, title_date, end_dt_raw),
                    "icao": event["icao"],
                    "unit": event["unit"],
                    "threshold": threshold,
                    "threshold_hi": threshold_hi,
                    "direction": direction,
                    "skip_reason": None,
                })

        if not batch_candidates:
            log.info("  batch %d: no parseable candidates — next batch", batch_num)
            continue

        no_ids  = [c["token_id"]     for c in batch_candidates]
        yes_ids = [c["yes_token_id"] for c in batch_candidates if c["yes_token_id"]]
        log.info("  batch %d: fetching order books for %d candidates...", batch_num, len(batch_candidates))
        books = get_best_books_bulk(host, no_ids + yes_ids)
        for c in batch_candidates:
            book = books.get(c["token_id"], {})
            live_ask = book.get("ask")
            live_bid = book.get("bid")
            if c["yes_token_id"]:
                c["yes_price"] = books.get(c["yes_token_id"], {}).get("ask")
            c["price"] = live_ask or 0.0
            c["bid"] = live_bid
            if live_ask is None:
                c["skip_reason"] = "no asks in book"
            elif live_ask > max_no_price:
                c["skip_reason"] = f"ask {live_ask*100:.2f}¢ > max {max_no_price*100:.2f}¢ (unreachable)"
            elif live_bid is None:
                c["skip_reason"] = "no bids in book"
            elif live_bid > max_no_price:
                c["skip_reason"] = f"bid {live_bid*100:.2f}¢ > max {max_no_price*100:.2f}¢"
            elif live_bid < min_no_price:
                c["skip_reason"] = f"bid {live_bid*100:.2f}¢ < min {min_no_price*100:.2f}¢"

        viable = [c for c in batch_candidates if not c["skip_reason"] and c["city"] not in city_blacklist]
        if not viable:
            log.info("  batch %d: 0 viable (price/blacklist) — next batch", batch_num)
            continue

        all_candidates.extend(batch_candidates)
        for slug, ev in batch_events.items():
            selected_events[slug] = ev
        distinct_viable_cities.update(c["city"] for c in viable)
        log.info(
            "  batch %d: +%d viable; cumulative distinct cities = %d/%d",
            batch_num, len(viable), len(distinct_viable_cities), scan_target_cities,
        )
        if len(distinct_viable_cities) >= scan_target_cities:
            log.info("  reached target %d distinct viable cities — stopping scan", scan_target_cities)
            break

    if not all_candidates:
        log.info("  no viable candidates across any batch — nothing to do")
        return

    # ── Fetch station coords + forecasts for the selected batch only ────────
    all_icaos = [ev["icao"] for ev in selected_events.values() if ev["icao"]]
    coords = fetch_coords_for_stations(list(set(all_icaos))) if all_icaos else {}
    event_list = [
        {"icao": ev["icao"], "date": ev["date"], "unit": ev["unit"]}
        for ev in selected_events.values() if ev["icao"] and ev["date"]
    ]
    log.info("Fetching forecasts for %d event/station pairs...", len(set(
        (e["icao"], e["date"]) for e in event_list
    )))
    forecasts = fetch_forecasts_for_events(event_list, coords)

    for c in all_candidates:
        fc = forecasts.get((c["icao"], c["end_date"])) if c.get("icao") else None
        c["forecast_high"] = fc["open_meteo"] if fc else None
        c["noaa_high"] = fc["noaa"] if fc else None
        c["taf_high"] = fc["taf"] if fc else None

    # ── Fetch METAR current + max-so-far-today for each unique ICAO ─────────
    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
    from automata.weather import fetch_metar_today_stats
    metar_keys: set[tuple[str, str, str, str]] = set()
    for c in all_candidates:
        if not c.get("icao"):
            continue
        tz_name = CITY_TZ.get(c["city"], "UTC")
        metar_keys.add((c["icao"], tz_name, c["end_date"], c.get("unit") or "C"))
    metar_stats: dict[tuple[str, str], tuple[float | None, float | None]] = {}
    if metar_keys:
        with _TPE(max_workers=min(16, max(1, len(metar_keys)))) as _ex:
            _futs = {
                _ex.submit(fetch_metar_today_stats, icao, tz, ed, u): (icao, ed)
                for (icao, tz, ed, u) in metar_keys
            }
            for fut in _ac(_futs):
                try:
                    metar_stats[_futs[fut]] = fut.result()
                except Exception:
                    metar_stats[_futs[fut]] = (None, None)
    for c in all_candidates:
        key = (c.get("icao") or "", c["end_date"])
        cur, mx = metar_stats.get(key, (None, None))
        c["metar_current"] = cur
        c["metar_max_so_far"] = mx

    # ── Record every candidate into the weather_model DB (shadow mode) ───────
    try:
        from automata import weather_model
        weather_model.init_db()
        scan_recs = [
            weather_model.ScanRecord(
                city=c["city"],
                icao=c.get("icao"),
                event_date=c["end_date"],
                event_slug=c.get("event_slug"),
                question=c["question"],
                token_id=c.get("token_id"),
                yes_token_id=c.get("yes_token_id"),
                resolution_dt=c.get("resolution_dt"),
                unit=c.get("unit"),
                threshold=c.get("threshold"),
                threshold_hi=c.get("threshold_hi"),
                direction=c.get("direction"),
                openmeteo_high=c.get("forecast_high"),
                noaa_high=c.get("noaa_high"),
                taf_high=c.get("taf_high"),
                metar_current=c.get("metar_current"),
                metar_max_so_far=c.get("metar_max_so_far"),
                no_bid=c.get("bid"),
                no_ask=c.get("price") if c.get("price") else None,
                yes_bid=None,
                yes_ask=c.get("yes_price"),
            )
            for c in all_candidates
        ]
        model_out = weather_model.record_scan_batch(scan_recs)
        # Compact per-scan log line so the operator can follow bias corrections
        # even before any UI is open.
        edges: list[tuple[str, float]] = []
        would_veto = 0
        for rec, m in zip(all_candidates, model_out):
            fp = m.get("fair_no_prob")
            ask = rec.get("price")
            if fp is not None and ask:
                edge = (ask - fp) * 10000.0
                if abs(edge) >= 50:  # only flag edges >= 50 bps (0.5¢)
                    edges.append((f"{rec['city']} {rec['question']}", edge))
                if edge > model_edge_max_bps:
                    would_veto += 1
                    log.warning(
                        "[weather-model] would-veto %s %s — ask %.1f¢ > fair %.1f¢ (%+0.f bps > %.0f)",
                        rec["city"], rec["question"],
                        ask * 100, fp * 100, edge, model_edge_max_bps,
                    )
        log.info(
            "[weather-model] scanned %d candidates across %d cities; "
            "%d flagged edges; %d would-veto (not enforced)",
            len(all_candidates),
            len({c["city"] for c in all_candidates}),
            len(edges),
            would_veto,
        )
        for label, edge in sorted(edges, key=lambda t: -abs(t[1]))[:5]:
            log.info("  edge %+.0fbps  %s", edge, label)
    except Exception as exc:
        log.warning("[weather-model] record_scan_batch failed: %s — continuing", exc)

    # ── Ranked candidates per city, capped to 3 closest-resolving markets ───
    bettable = [c for c in all_candidates if not c["skip_reason"] and c["city"] not in city_blacklist]

    ranked_per_city: dict[str, list[dict]] = {}
    for c in bettable:
        ranked_per_city.setdefault(c["city"], []).append(c)
    for city in ranked_per_city:
        ranked_per_city[city].sort(key=lambda c: c.get("resolution_dt") or _FAR_FUTURE)

    # Pick the best candidate per city, then take the 10 soonest globally
    top_n = sorted(
        [ranked[0] for ranked in ranked_per_city.values()],
        key=lambda c: c.get("resolution_dt") or _FAR_FUTURE,
    )[:10]
    ranked_per_city = {c["city"]: [c] for c in top_n}
    log.info("  %d qualifying market(s) selected (top 10 by resolution)", len(ranked_per_city))

    # ── Dry run: show only autobet (★) items per city ───────────────────────────
    if dry_run:
        now_utc = datetime.now(timezone.utc)
        autobet_items = sorted(
            [ranked[0] for ranked in ranked_per_city.values()],
            key=lambda c: c.get("resolution_dt") or _FAR_FUTURE,
        )

        # Fetch METAR observations in parallel for the sanity guard preview.
        from concurrent.futures import ThreadPoolExecutor
        from automata.weather import fetch_metar_temp
        guard_margin_c = config.get_float("METAR_GUARD_MARGIN_C", "weather", "metar_guard_margin_c", 2.0)
        guard_margin_f = config.get_float("METAR_GUARD_MARGIN_F", "weather", "metar_guard_margin_f", 4.0)
        metar_obs: dict[str, float | None] = {}
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(autobet_items)))) as ex:
            futures = {
                ex.submit(fetch_metar_temp, c["icao"], c.get("unit") or "C"): c["icao"]
                for c in autobet_items if c.get("icao")
            }
            for fut, icao in futures.items():
                try:
                    metar_obs[icao] = fut.result()
                except Exception:
                    metar_obs[icao] = None

        print(f"  [DRY RUN] {len(autobet_items)} autobet item(s) (closest resolution first):")
        print(f"    {'':1}  {'No':>7}  {'Yes':>7}  {'shares':>6}  {'cost':>6}  {'resolves (PT)':<16}  {'city':<15}  {'date':<10}  {'forecast':>8}  {'guard':<22}  question")
        print(f"    {'-'*1}  {'-'*7}  {'-'*7}  {'-'*6}  {'-'*6}  {'-'*16}  {'-'*15}  {'-'*10}  {'-'*8}  {'-'*22}  {'-'*22}")
        user_tz = ZoneInfo(config.get_str("USER_TZ", "weather", "user_tz", "America/Los_Angeles"))
        for c in autobet_items:
            cost = round(bet_shares * c["price"], 2)
            res_dt = c.get("resolution_dt")
            if res_dt is not None:
                now_local = res_dt.astimezone(user_tz).strftime("%b %d %H:%M")
            else:
                now_local = now_utc.astimezone(user_tz).strftime("%b %d %H:%M")
            no_str  = f"{c['price']*100:6.2f}¢"
            yes_str = f"{c['yes_price']*100:6.2f}¢" if c["yes_price"] is not None else "   n/a "
            fcast_val = c.get("forecast_high")
            fcast_str = f"{fcast_val:.1f}°{c['unit']}" if fcast_val is not None else "n/a"

            obs = metar_obs.get(c.get("icao")) if c.get("icao") else None
            thr = c.get("threshold")
            unit_g = c.get("unit") or "C"
            margin_g = guard_margin_f if unit_g.upper() == "F" else guard_margin_c
            if obs is None or thr is None:
                guard_str = "n/a"
            else:
                delta = abs(obs - float(thr))
                verdict = "SKIP" if delta < margin_g else "OK"
                guard_str = f"{verdict} {obs:.1f}°{unit_g} Δ{delta:.1f}"

            print(f"    \u2605  {no_str}  {yes_str}  {bet_shares:>6.0f}sh  ${cost:>5.2f}  {now_local:<16}  {c['city']:<15}  {c['title_date']:<10}  {fcast_str:>8}  {guard_str:<22}  {c['question']}")
        print()
        log.info("  %d autobet item(s): %s", len(autobet_items),
                 ", ".join(f"{c['city']} {c['title_date']} {c['question']}" for c in autobet_items))
        return

    # ── Live betting: one bet per city-date, skip if position or open order exists ──
    from automata.client import (
        build_client,
        cancel_order,
        get_all_open_orders,
        get_best_bid_ask,
        get_positions,
        get_usdc_balance,
        place_no_order,
    )

    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        log.error("Missing .env keys for live betting: %s", ", ".join(missing))
        return

    funder = os.getenv("POLYMARKET_FUNDER") or ""
    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=funder or None,
        signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
    )
    try:
        balance = get_usdc_balance(client)
    except Exception as exc:
        log.error("Failed to fetch USDC balance: %s", exc)
        return
    if max_spend_usdc is not None:
        capped_balance = min(balance, max_spend_usdc)
        log.info("Balance cap enabled: available $%.2f, capped spend $%.2f", balance, capped_balance)
        balance = capped_balance

    if balance < bet_shares:
        bet_shares = round(0.9 * balance / max_no_price, 2)
        log.info("Balance-adjusted bet_shares: %.2f (90%% of $%.2f)", bet_shares, balance)
    else:
        n_bets = math.ceil(balance / bet_shares)
        bet_shares = round((balance - 0.10) / n_bets / max_no_price, 2)
        log.info("Balance-adjusted bet_shares: %.2f (split $%.2f across %d bets, 0.10 margin)", bet_shares, balance, n_bets)

    # Build token_id → (city, date) lookup from all candidates
    token_to_city_date: dict[str, tuple[str, str]] = {
        c["token_id"]: (c["city"], c["end_date"]) for c in all_candidates
    }

    # Supplement token→city-date lookup with DB history (covers tokens not in current market fetch)
    from automata.db import get_token_city_map, get_token_city_date_map
    db_token_city = get_token_city_map()
    db_token_city_date = get_token_city_date_map()
    for tid, cd in db_token_city_date.items():
        if tid not in token_to_city_date:
            token_to_city_date[tid] = cd

    import time as _time
    stale_minutes = config.get_float("STALE_ORDER_MINUTES", "weather", "stale_order_minutes", 1.0)
    max_passes = config.get_int("BET_PASSES", "weather", "bet_passes", 3)
    orders_placed = 0

    for pass_num in range(1, max_passes + 1):
        log.info("── Bet pass %d/%d ──", pass_num, max_passes)

        # Re-fetch positions and open orders fresh each pass
        position_size: dict[str, float] = {
            p["token_id"]: p["size"] for p in (get_positions(funder) if funder else [])
        }

        # ── Cancel stale buy orders + opportunistic upgrade ──────────────────────
        stale_cutoff = _time.time() - stale_minutes * 60
        cancelled_order_ids: set[str] = set()
        all_open_orders = get_all_open_orders(client)
        for o in all_open_orders:
            if str(o.get("side", "")).upper() != "BUY":
                continue
            if _order_open_shares(o) <= 0:
                continue
            order_id = str(o.get("id") or o.get("orderID") or "")
            if not order_id:
                continue

            # Opportunistic upgrade: if a better bracket for this city now has an ask, cancel
            # the sitting passive order so the next bets_to_place build picks it up instead.
            token_id = str(o.get("asset_id") or o.get("token_id", ""))
            cd = token_to_city_date.get(token_id)
            city_for_order = cd[0] if cd else None
            if city_for_order and city_for_order in ranked_per_city:
                for candidate in ranked_per_city[city_for_order]:
                    if candidate["token_id"] == token_id:
                        continue  # same bracket — skip
                    _, cand_ask = get_best_bid_ask(os.environ["POLYMARKET_HOST"], candidate["token_id"])
                    if cand_ask is not None:
                        try:
                            cancel_order(client, order_id)
                            cancelled_order_ids.add(order_id)
                            log.info(
                                "Upgrade: cancelled passive order %s for %s — bracket '%s' now has ask %.2f¢",
                                order_id[:12], city_for_order, candidate["question"], cand_ask * 100,
                            )
                        except Exception as exc:
                            log.warning("Failed to cancel order for upgrade %s: %s", order_id[:12], exc)
                        break

            if order_id in cancelled_order_ids:
                continue

            # Stale cancel
            created_at = o.get("created_at")
            if created_at is None or float(created_at) > stale_cutoff:
                continue
            try:
                cancel_order(client, order_id)
                cancelled_order_ids.add(order_id)
                log.info("Cancelled stale order %s (%.0f min old)", order_id[:12], ((_time.time() - float(created_at)) / 60))
            except Exception as exc:
                log.warning("Failed to cancel stale order %s: %s", order_id[:12], exc)

        # city-dates with an open buy order in the book (order in flight — don't touch)
        open_buy_orders_by_city_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for o in all_open_orders:
            order_id = str(o.get("id") or o.get("orderID") or "")
            if order_id in cancelled_order_ids:
                continue
            if str(o.get("side", "")).upper() == "BUY":
                token_id = str(o.get("asset_id") or o.get("token_id", ""))
                cd = token_to_city_date.get(token_id)
                if cd:
                    open_buy_orders_by_city_date[cd].append(o)

        # city-dates where we hold any position (possibly a different token than today's best)
        held_city_dates: set[tuple[str, str]] = set()
        held_cities: set[str] = set()
        for tid in position_size:
            cd = token_to_city_date.get(tid)
            if cd:
                held_city_dates.add(cd)
                held_cities.add(cd[0])

        # ── Build bets_to_place (fillable brackets first, passive fallback if none) ──
        bets_to_place: list[dict] = []
        for city, ranked in ranked_per_city.items():
            immediate_candidate = None
            passive_candidate = None

            for best in ranked:
                key = (city, best["end_date"])
                if key in held_city_dates:
                    log.info("Already hold a position for %s %s — skipping", city, best["end_date"])
                    break
                open_orders_for_key = open_buy_orders_by_city_date.get(key, [])
                open_orders_same_token = [
                    o for o in open_orders_for_key
                    if str(o.get("asset_id") or o.get("token_id", "")) == best["token_id"]
                ]

                if any(
                    str(o.get("asset_id") or o.get("token_id", "")) != best["token_id"]
                    for o in open_orders_for_key
                ):
                    log.info("Open buy in different token  %s %s — skipping", city, best["end_date"])
                    break

                held = position_size.get(best["token_id"], 0.0)
                open_order_shares = sum(_order_open_shares(o) for o in open_orders_same_token)

                if held + open_order_shares >= bet_shares:
                    log.info(
                        "Target already covered  %s %s  held=%.2f open=%.2f — skipping",
                        city, best["end_date"], held, open_order_shares,
                    )
                    break

                if held == 0 and key in held_city_dates:
                    log.info("Position in different token  %s %s — skipping", city, best["end_date"])
                    break

                shares_needed = round(bet_shares - held - open_order_shares, 2)
                if shares_needed <= 0:
                    break

                best["_shares_to_buy"] = shares_needed
                best["_top_up"] = held > 0
                best["_held_shares"] = held
                best["_existing_orders"] = open_orders_same_token
                best["_fill_round"] = 1

                _, _ask = get_best_bid_ask(os.environ["POLYMARKET_HOST"], best["token_id"])
                best["_live_ask"] = _ask
                if _ask is not None:
                    immediate_candidate = best
                    break
                elif passive_candidate is None:
                    best["_passive_fallback"] = True
                    passive_candidate = best
                    log.info("No ask counterparty  %s %s — trying next bracket", city, best["question"])

            chosen = immediate_candidate or passive_candidate
            if chosen is not None:
                bets_to_place.append(chosen)
                if chosen.get("_passive_fallback"):
                    log.info("Passive fallback queued  %s %s — letting market come to us", city, chosen["question"])

        # ── Top-up pass ───────────────────────────────────────────────────────
        held_city_tokens: dict[str, str] = {}
        for tid in position_size:
            cd = token_to_city_date.get(tid)
            city = cd[0] if cd else db_token_city.get(tid)
            if city and city not in held_city_tokens:
                held_city_tokens[city] = tid

        cities_queued = {b["city"] for b in bets_to_place}
        for city, tid in held_city_tokens.items():
            if city in city_blacklist or city in cities_queued:
                continue
            held_tu = position_size.get(tid, 0.0)
            cd = token_to_city_date.get(tid)
            existing_orders = [
                o for o in open_buy_orders_by_city_date.get(cd, [])
                if str(o.get("asset_id") or o.get("token_id", "")) == tid
            ] if cd else []
            open_tu_shares = sum(_order_open_shares(o) for o in existing_orders)
            if held_tu + open_tu_shares >= max_shares:
                continue
            topup_needed = round(max_shares - held_tu - open_tu_shares, 2)
            if topup_needed <= 0:
                continue
            bid, ask = get_best_bid_ask(os.environ["POLYMARKET_HOST"], tid)
            if bid is None or bid < min_no_price:
                log.info("Top-up skipped %s — bid %.2f¢ below min", city, (bid or 0) * 100)
                continue
            if ask is not None and ask > max_no_price:
                continue
            candidate = next((c for c in all_candidates if c["token_id"] == tid), {})
            bets_to_place.append({
                "token_id": tid,
                "city": city,
                "question": candidate.get("question", "?"),
                "end_date": candidate.get("end_date", ""),
                "title_date": candidate.get("title_date", ""),
                "yes_token_id": candidate.get("yes_token_id"),
                "yes_price": candidate.get("yes_price"),
                "price": ask or bid,
                "bid": bid,
                "icao": candidate.get("icao"),
                "unit": candidate.get("unit"),
                "threshold": candidate.get("threshold"),
                "threshold_hi": candidate.get("threshold_hi"),
                "direction": candidate.get("direction"),
                "forecast_high": candidate.get("forecast_high"),
                "resolution_dt": candidate.get("resolution_dt"),
                "_top_up": True,
                "_held_shares": held_tu,
                "_existing_orders": existing_orders,
                "_shares_to_buy": topup_needed,
                "_fill_round": 2,
            })
            log.info("Top-up queued %s — bid %.2f¢  (+%.0f shares to reach %.0f)", city, bid * 100, topup_needed, max_shares)

        if not bets_to_place and not cancelled_order_ids:
            log.info("Pass %d: nothing to do — stopping.", pass_num)
            break

        if not bets_to_place:
            log.info("Pass %d: no bets to place (only cancellations this pass).", pass_num)
        else:
            # Round 1 before Round 2; within each round, closest resolution first
            bets_to_place.sort(key=lambda b: (b.get("_fill_round", 1), b.get("resolution_dt") or _FAR_FUTURE))

            print(f"  Pass {pass_num} — placing {len(bets_to_place)} order(s)...")
            print()

            for b in bets_to_place:
                if max_orders is not None and orders_placed >= max_orders:
                    log.info("Reached max_orders=%d for this run — stopping order placement", max_orders)
                    break
                best_bid, best_ask = get_best_bid_ask(os.environ["POLYMARKET_HOST"], b["token_id"])
                quote_price = _compute_maker_buy_price(
                    best_bid=best_bid,
                    best_ask=best_ask,
                    min_no_price=min_no_price,
                    max_no_price=max_no_price,
                    tick_size=mm_tick_size,
                    join_bid_ticks=mm_join_bid_ticks,
                )
                log.info(
                    "[quote] %s %s  bid=%s  ask=%s  quote=%s  tick=%s",
                    b["city"], b["question"],
                    f"{best_bid:.4f}" if best_bid is not None else "None",
                    f"{best_ask:.4f}" if best_ask is not None else "None",
                    f"{quote_price:.4f}" if quote_price is not None else "None",
                    f"{mm_tick_size:.4f}",
                )
                if quote_price is None:
                    log.info("No valid maker quote  %s %s — skipping", b["city"], b["question"])
                    continue

                shares_to_buy = b.get("_shares_to_buy", bet_shares)
                if shares_to_buy <= 0:
                    continue

                # Sanity guard (advisory only — does NOT block the bet).
                # Logs a warning when observed METAR temp is within margin of threshold.
                unit_g = b.get("unit") or "C"
                if unit_g.upper() == "F":
                    guard_margin = config.get_float("METAR_GUARD_MARGIN_F", "weather", "metar_guard_margin_f", 4.0)
                else:
                    guard_margin = config.get_float("METAR_GUARD_MARGIN_C", "weather", "metar_guard_margin_c", 2.0)
                icao_g = b.get("icao")
                threshold_g = b.get("threshold")
                if icao_g and threshold_g is not None:
                    from automata.weather import fetch_metar_temp
                    obs = fetch_metar_temp(icao_g, unit_g)
                    if obs is not None and abs(obs - float(threshold_g)) < guard_margin:
                        log.warning(
                            "[guard] %s %s — METAR %.1f°%s within %.1f°%s of threshold %.1f°%s (advisory, not blocking)",
                            b["city"], b["question"], obs, unit_g,
                            guard_margin, unit_g, float(threshold_g), unit_g,
                        )

                existing_orders = b.get("_existing_orders", [])
                needs_reprice = any(
                    (_order_price(o) is not None) and (abs(_order_price(o) - quote_price) >= mm_reprice_delta)
                    for o in existing_orders
                    if _order_open_shares(o) > 0
                )
                if needs_reprice:
                    cancel_failed = False
                    for o in existing_orders:
                        if _order_open_shares(o) <= 0:
                            continue
                        order_id = str(o.get("id") or o.get("orderID") or "")
                        if not order_id:
                            continue
                        try:
                            cancel_order(client, order_id)
                        except Exception as exc:
                            cancel_failed = True
                            log.warning("Failed to cancel order %s: %s", order_id, exc)
                    if cancel_failed:
                        continue

                cost = round(shares_to_buy * quote_price, 2)
                if cost > balance:
                    log.warning("  Insufficient balance $%.2f for %s %s (need $%.2f) — skipping",
                                balance, b["city"], b["question"], cost)
                    continue
                if b["_top_up"]:
                    label = f"TOP-UP +{shares_to_buy}"
                elif b.get("_passive_fallback"):
                    label = f"{shares_to_buy:.0f} shares (passive)"
                else:
                    label = f"{shares_to_buy:.0f} shares"
                try:
                    resp = place_no_order(client, b["token_id"], quote_price, shares_to_buy, post_only=False)
                    order_id = resp.get("orderID") or resp.get("id") or "?"
                    status   = resp.get("status") or "submitted"
                    result   = f"{status}  id={order_id}"
                    log.info("[order-resp] %s %s  raw=%s", b["city"], b["question"], resp)
                    balance = round(balance - cost, 2)
                    orders_placed += 1
                    from automata.db import record_bet
                    record_bet(
                        city=b["city"],
                        icao=b.get("icao"),
                        event_date=b["end_date"],
                        question=b["question"],
                        option="No",
                        token_id=b["token_id"],
                        order_id=order_id,
                        shares=shares_to_buy,
                        no_price=quote_price,
                        yes_price=b.get("yes_price"),
                        cost_usdc=cost,
                        unit=b.get("unit"),
                        threshold=b.get("threshold"),
                        threshold_hi=b.get("threshold_hi"),
                        direction=b.get("direction"),
                        forecast_high=b.get("forecast_high"),
                    )
                except Exception as exc:
                    result = f"ERROR: {exc}"
                print(
                    f"  BUY No (maker) @ {quote_price*100:.2f}¢  {label} (${cost:.2f})"
                    f"  {b['city']}  {b['title_date']}  {b['question']}  → {result}"
                )
            print()
            if max_orders is not None and orders_placed >= max_orders:
                break

        if pass_num < max_passes:
            wait = config.get_int("BET_PASS_WAIT_SECONDS", "weather", "bet_pass_wait_seconds", 10)
            log.info("Waiting %ds before pass %d...", wait, pass_num + 1)
            _time.sleep(wait)


def _scan_positions(dry_run: bool = True) -> None:
    """
    For each open position, ensure a sell order at TAKE_PROFIT_PRICE exists.
    If not, place one (unless dry_run).
    """
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        return

    take_profit = config.get_float("TAKE_PROFIT_PRICE", "weather", "take_profit_price", 0.999)

    funder = os.getenv("POLYMARKET_FUNDER") or ""
    if not funder:
        log.warning("POLYMARKET_FUNDER not set — cannot scan positions")
        return

    from automata.client import build_client, get_positions, get_open_orders, place_sell_order, place_market_sell, get_best_bid
    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=funder,
        signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
    )

    positions = get_positions(funder)
    if not positions:
        log.info("Positions: none")
        return

    log.info("Positions: %d open", len(positions))
    for pos in positions:
        token_id = pos["token_id"]
        size     = pos["size"]
        if size < 5:
            host = os.environ["POLYMARKET_HOST"]
            bid = get_best_bid(host, token_id)
            if bid is not None and bid >= take_profit:
                if dry_run:
                    log.info("  token %s  %.2f shares — [DRY RUN] bid %.1f¢ >= %.1f¢, would market sell", token_id[:12], size, bid * 100, take_profit * 100)
                else:
                    try:
                        resp = place_market_sell(client, token_id, bid, size)
                        order_id = resp.get("orderID") or resp.get("id") or "?"
                        log.info("  token %s  %.2f shares — market sell @ %.1f¢  id=%s", token_id[:12], size, bid * 100, order_id)
                    except Exception as exc:
                        log.error("  token %s — market sell failed: %s", token_id[:12], exc)
            else:
                log.info("  token %s  %.2f shares — too small, bid not at target yet", token_id[:12], size)
            continue
        orders   = get_open_orders(client, token_id)
        existing_sells = [o for o in orders if str(o.get("side", "")).upper() == "SELL"]
        sell_remaining_total = sum(_order_open_shares(o) for o in existing_sells)
        gap = round(size - sell_remaining_total, 2)
        # Minimum top-up is $1 / take_profit shares (Polymarket min-order rule) — skip noise below that
        min_topup_shares = max(1.01, 1.0 / max(take_profit, 0.01))

        if existing_sells and gap < min_topup_shares:
            existing_price = float(existing_sells[0].get("price", 0))
            log.debug(
                "  token %s  %.2f shares — sell order already covers position "
                "(%d order(s) totaling %.2f sh @ ~%.1f¢, gap %.2f)",
                token_id[:12], size, len(existing_sells), sell_remaining_total,
                existing_price * 100, gap,
            )
            continue

        topup_shares = gap if existing_sells else size
        if topup_shares < min_topup_shares:
            log.info(
                "  token %s  %.2f shares — gap %.2f < min $1 order, skipping top-up",
                token_id[:12], size, gap,
            )
            continue

        if existing_sells:
            log.info(
                "  token %s  %.2f shares — sells total %.2f, gap %.2f → topping up",
                token_id[:12], size, sell_remaining_total, topup_shares,
            )
        if dry_run:
            log.info(
                "  token %s  %.2f shares — [DRY RUN] would place sell @ %.1f¢ for %.2f sh",
                token_id[:12], size, take_profit * 100, topup_shares,
            )
        else:
            try:
                resp = place_sell_order(client, token_id, take_profit, topup_shares)
                order_id = resp.get("orderID") or resp.get("id") or "?"
                log.info(
                    "  token %s  %.2f shares — sell @ %.1f¢ for %.2f sh placed  id=%s",
                    token_id[:12], size, take_profit * 100, topup_shares, order_id,
                )
            except Exception as exc:
                log.error("  token %s — sell order failed: %s", token_id[:12], exc)
