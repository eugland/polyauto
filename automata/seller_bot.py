"""
Dust-fade daemon for held YES weather positions.

Operates on YES weather positions you ALREADY HOLD (this module never SPLITs —
that's `automata.splitter`). For each held YES of a multi-bucket temperature
event, it:

  1. Sorts the event's peer buckets by threshold and locates the peak (the
     bucket with the highest max(yes_bid, yes_ask)).
  2. For below-peak buckets only, if the gap rule passes and
     max(yes_bid, yes_ask) <= DUST_ASK_THRESHOLD ($0.02), posts a GTC sell
     walked to the current bid (so it crosses immediately).
  3. All other buckets — peak, above-peak, mid-priced, model-vetoed — are
     left alone (no orders placed, no orders cancelled).

Idempotent: never double-posts the same price. Existing orders priced
elsewhere on a still-eligible bucket are cancelled and replaced.

Usage:
  python -m automata.seller_bot                        # dry-run, single pass
  python -m automata.seller_bot --live                 # post real orders
  python -m automata.seller_bot --live --loop          # loop forever
  python -m automata.seller_bot --min-gap 2
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from automata import config

load_dotenv()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = _REPO_ROOT / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "seller_bot.log"
# Shared log file the stock UI reads (stock/app.py /api/weather-log).
_SHARED_LOG_FILE = _LOGS_DIR / "automata.log"
_DB_DIR = _REPO_ROOT / "db"
_DB_DIR.mkdir(exist_ok=True)
_STATE_DB = _DB_DIR / "seller_state.db"


def _attach_file_handler(root: logging.Logger, path: Path) -> None:
    """Add a FileHandler for `path` to root if one isn't already attached.
    Uses the same format weather_bot.py uses so all modules render uniformly
    in the stock UI weather-log viewer."""
    if any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(path)
        for h in root.handlers
    ):
        return
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    fh.setLevel(logging.INFO)
    root.addHandler(fh)


def _setup_logging() -> logging.Logger:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler()],
        )
    # Per-module file (focused debug)
    _attach_file_handler(root, _LOG_FILE)
    # Shared file the stock UI reads
    _attach_file_handler(root, _SHARED_LOG_FILE)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.seller_bot")


def _init_state_db() -> None:
    with sqlite3.connect(_STATE_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fade_orders (
                token_id      TEXT NOT NULL,
                event_slug    TEXT NOT NULL,
                question      TEXT,
                bucket_idx    INTEGER,
                peak_idx      INTEGER,
                gap           INTEGER,
                target_price  REAL NOT NULL,
                target_size   REAL NOT NULL,
                stage         TEXT NOT NULL,            -- 'edge' | 'closure'
                order_id      TEXT,
                placed_at_utc TEXT NOT NULL,
                PRIMARY KEY (token_id, target_price)
            )
        """)


def _record_fade(
    token_id: str,
    event_slug: str,
    question: str,
    bucket_idx: int,
    peak_idx: int | None,
    gap: int,
    target_price: float,
    target_size: float,
    stage: str,
    order_id: str | None,
) -> None:
    with sqlite3.connect(_STATE_DB) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fade_orders (token_id, event_slug, question, bucket_idx, peak_idx, gap, target_price, target_size, stage, order_id, placed_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                token_id, event_slug, question, bucket_idx, peak_idx, gap,
                round(target_price, 6), round(target_size, 4), stage,
                order_id, datetime.now(timezone.utc).isoformat(),
            ),
        )


def _order_open_shares(order: dict[str, Any]) -> float:
    for k in ("remaining_size", "size_left", "sizeLeft", "open_size"):
        v = order.get(k)
        try:
            f = float(v) if v is not None else None
        except (TypeError, ValueError):
            f = None
        if f is not None and f >= 0:
            return f
    try:
        size = float(order.get("size") or order.get("original_size") or 0)
        filled = float(order.get("matched_size") or order.get("filled_size") or 0)
        return max(0.0, size - filled)
    except (TypeError, ValueError):
        return 0.0


def _order_price(order: dict[str, Any]) -> float | None:
    try:
        return float(order["price"])
    except (KeyError, TypeError, ValueError):
        return None


def _stage_for(gap: int, minutes_to_resolution: float | None, min_gap: int, closure_minutes: float) -> str | None:
    """Return 'edge' or 'closure' or None (=skip this bucket). Used by older code paths."""
    if gap >= min_gap:
        return "edge"
    if gap == max(1, min_gap - 1) and minutes_to_resolution is not None and minutes_to_resolution <= closure_minutes:
        return "closure"
    return None


# ───────────── Classification ─────────────────────────────────────────────────
# Rules:
#   * below-peak, max(ask, bid) <= DUST_ASK_THRESHOLD → DUST-FADE @ sliding px
#   * everything else                                  → skip (no order placed)
# Gap rule (non-peak buckets):
#   * gap >= min_gap              → eligible always
#   * gap == min_gap-1 (adjacent) → eligible only in closure window
#   * gap < min_gap-1             → skip
#
# Dust-fade pricing: walk to the current bid so the sell CROSSES immediately
# instead of resting one tick above the spread. The gap-based prices below are
# only used as a floor when there's no real bid to walk to (bid is 0 or
# sub-tick). Empirical floor calibrated from experiment/aapang_dump.json.
DUST_ASK_THRESHOLD = 0.02       # max(ask, bid) <= this → dust eligible
# No-bid fallback floor (USDC per share) — only used when bid is sub-tick.
DUST_FADE_PRICE_NEAR = 0.001    # gap 1-2 (adjacent / borderline)
DUST_FADE_PRICE_MID  = 0.003    # gap 3-4 (sweet-spot for retail nibbles)
DUST_FADE_PRICE_FAR  = 0.002    # gap 5+ (far edge)
# Backward-compat alias for old call sites that pass it through unchanged.
DUST_FADE_PRICE = DUST_FADE_PRICE_NEAR

def _dust_fade_price_for_gap(gap: int) -> float:
    """No-bid fallback floor (only used when the book has no real bid)."""
    if gap >= 5:
        return DUST_FADE_PRICE_FAR
    if gap >= 3:
        return DUST_FADE_PRICE_MID
    return DUST_FADE_PRICE_NEAR


def _dust_fade_price(gap: int, yes_bid: float | None) -> float:
    """
    Walk the fade price down to the current bid so the sell crosses and fills
    instead of resting one tick above the spread. Falls back to the gap-based
    floor only when the bid is sub-tick (effectively no bid).
    """
    bid = yes_bid or 0.0
    if bid >= 0.001:
        return round(bid, 4)
    return _dust_fade_price_for_gap(gap)


def _classify_bucket(
    yes_bid: float | None,
    yes_ask: float | None,
    gap: int,
    min_gap: int,
    minutes_to_resolution: float | None,
    closure_minutes: float,
    is_above_peak: bool = False,
) -> tuple[str, float, str] | None:
    """
    Return (action, target_price, reason) or None to skip this bucket.

    action ∈ {'dust-fade'}.
    """
    bid = yes_bid or 0.0
    ask = yes_ask or 0.0

    in_closure = (
        minutes_to_resolution is not None
        and minutes_to_resolution <= closure_minutes
    )
    if gap < min_gap:
        if not (in_closure and gap == max(1, min_gap - 1)):
            return None

    # Dust gate uses max(ask, bid) so we still classify as dust when there's
    # no real ask but the bid is sub-2¢.
    if max(ask, bid) > DUST_ASK_THRESHOLD:
        return None
    # Above-peak buckets are upside lottery tickets — never fade.
    if is_above_peak:
        return None
    return ("dust-fade", _dust_fade_price(gap, yes_bid),
            f"price-dust bid={bid:.4f} ask={ask:.4f}")


def scan(
    *,
    min_gap: int,
    edge_px: float,
    closure_px: float,
    closure_minutes: float,
    min_remaining_shares: float,
    dry_run: bool,
) -> dict:
    """One pass — find held YES weather buckets, post (or log) fade-sells."""
    log = logging.getLogger("automata.seller_bot")

    from automata.client import (
        build_client, cancel_order, derive_api_credentials,
        get_all_open_orders, get_open_orders, get_positions,
        place_sell_order,
    )
    from automata.parser import _extract_no_token_id, _extract_yes_token_id, _parse_threshold
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.weather_bot import _compute_resolution_dt, _extract_city, _extract_title_date

    funder = os.getenv("POLYMARKET_FUNDER") or ""
    if not funder:
        log.error("POLYMARKET_FUNDER not set")
        return {"posted": 0, "considered": 0, "error": "no funder"}

    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")

    # 1) Held positions
    log.info("Fetching open positions for %s ...", funder)
    positions = get_positions(funder)
    held_by_token: dict[str, float] = {p["token_id"]: p["size"] for p in positions if p["size"] > 0}
    log.info("  %d positions with size > 0", len(held_by_token))
    if not held_by_token:
        return {"posted": 0, "considered": 0}

    # 2) Live weather markets — to map token_id → bucket + event
    log.info("Fetching live weather market list ...")
    payload = fetch_temperature_markets_payload()
    raw_markets = payload["markets"]
    log.info("  %d raw markets fetched", len(raw_markets))

    # Build event → list of (bucket_dict). We intentionally do NOT filter
    # closed/inactive buckets here — Polymarket marks the *winning* bucket as
    # closed=true the moment Gamma confirms a winner, which would drop it from
    # peer_books and skew peak detection. Held-position guard below ensures we
    # only classify buckets we actually own.
    events: dict[str, dict[str, Any]] = {}
    for raw in raw_markets:
        event_slug = str(raw.get("event_slug") or "")
        event_title = str(raw.get("event_title") or event_slug)
        question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
        parsed = _parse_threshold(question)
        if not parsed:
            continue
        yes_token = _extract_yes_token_id(raw)
        no_token = _extract_no_token_id(raw)
        threshold, threshold_hi, unit, direction = parsed

        ev = events.setdefault(event_slug, {
            "title": event_title,
            "city": _extract_city(event_title),
            "title_date": _extract_title_date(event_title),
            "end_date": (raw.get("endDateIso") or raw.get("endDate") or "")[:10],
            "buckets": [],
        })
        ev["buckets"].append({
            "question": question,
            "yes_token": yes_token,
            "no_token": no_token,
            "threshold": threshold,
            "threshold_hi": threshold_hi,
            "unit": unit,
            "direction": direction,
        })

    # 3) For each event, fetch live YES books, sort by threshold, mark peak idx
    from automata.client import get_best_books_bulk
    all_yes_tokens: list[str] = []
    for ev in events.values():
        for b in ev["buckets"]:
            if b["yes_token"]:
                all_yes_tokens.append(b["yes_token"])
    if not all_yes_tokens:
        log.info("No yes-token books to fetch — done")
        return {"posted": 0, "considered": 0}
    log.info("Fetching YES books for %d tokens ...", len(all_yes_tokens))
    books = get_best_books_bulk(host, all_yes_tokens)

    # Pre-resolve forecast metadata per event so the model classifier can score
    # each bucket. We pull station/forecast from automata.weather, exactly the
    # same path weather_bot uses, so the model values match what's recorded
    # in scans table.
    from automata.weather import (
        extract_all_urls, extract_icao_from_url, extract_unit,
        fetch_coords_for_stations, fetch_forecasts_for_events,
    )
    from automata import weather_model

    event_meta: dict[str, dict[str, Any]] = {}
    for slug, ev in events.items():
        # We need the original raw market for description (URLs → ICAO).
        # Reuse the first market in this event from raw_markets.
        ev_raw = next(
            (r for r in raw_markets if str(r.get("event_slug") or "") == slug),
            None,
        )
        description = str((ev_raw or {}).get("event_description") or "")
        urls = extract_all_urls(description)
        icao = next((c for u in urls if (c := extract_icao_from_url(u))), None)
        event_meta[slug] = {
            "icao": icao,
            "unit": extract_unit(description),
            "date": ev["end_date"],
        }

    icaos = list({m["icao"] for m in event_meta.values() if m["icao"]})
    coords = fetch_coords_for_stations(icaos) if icaos else {}
    fcast_input = [
        {"icao": m["icao"], "date": m["date"], "unit": m["unit"]}
        for m in event_meta.values() if m["icao"] and m["date"]
    ]
    forecasts: dict[tuple[str, str], dict] = (
        fetch_forecasts_for_events(fcast_input, coords) if fcast_input else {}
    )

    # Resolve peak idx per event + classify each held bucket via three-tier rules.
    # Peak detection uses max(yes_bid, yes_ask) — using only yes_ask is unreliable
    # on thin or post-resolution books where the favored bucket has no sellers
    # (no ask) but a high bid.
    held_targets: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    for slug, ev in events.items():
        peers_sorted = sorted(
            ev["buckets"],
            key=lambda b: (b["threshold"], b.get("threshold_hi") or b["threshold"]),
        )
        peak_idx, peak_score = None, -1.0
        peer_books: list[dict[str, float | None]] = []
        for i, b in enumerate(peers_sorted):
            yb = books.get(b["yes_token"], {}) if b["yes_token"] else {}
            yes_bid = yb.get("bid")
            yes_ask = yb.get("ask")
            score_components = [v for v in (yes_bid, yes_ask) if v is not None]
            score = max(score_components) if score_components else None
            peer_books.append({"yes_bid": yes_bid, "yes_ask": yes_ask, "score": score})
            if score is not None and score > peak_score:
                peak_score = score
                peak_idx = i

        # Resolution time (used for closure-stage gating)
        resolution_dt = _compute_resolution_dt(ev["city"], ev["title_date"], ev["end_date"])
        minutes_to_resolution = (
            (resolution_dt - now_utc).total_seconds() / 60
            if resolution_dt is not None else None
        )
        hours_to_resolution = (
            minutes_to_resolution / 60 if minutes_to_resolution is not None else None
        )

        # Forecast for this event (None if station not extracted)
        ev_meta_local = event_meta.get(slug, {})
        ev_icao = ev_meta_local.get("icao")
        fcast = forecasts.get((ev_icao, ev_meta_local.get("date"))) if ev_icao else None
        # Pick the highest-priority available forecast (NOAA > TAF > Open-Meteo)
        forecast_high = None
        forecast_source = None
        if fcast:
            for src_key, src_name in (("noaa", "noaa"), ("taf", "taf"), ("open_meteo", "openmeteo")):
                v = fcast.get(src_key)
                if v is not None:
                    forecast_high, forecast_source = v, src_name
                    break

        for i, b in enumerate(peers_sorted):
            if not b["yes_token"]:
                continue
            held_size = held_by_token.get(b["yes_token"], 0.0)
            if held_size < min_remaining_shares:
                continue
            yes_bid = peer_books[i]["yes_bid"]
            yes_ask = peer_books[i]["yes_ask"]
            gap = abs(i - peak_idx) if peak_idx is not None else 999
            is_above_peak = peak_idx is not None and i > peak_idx

            # Read-only model query for this bucket
            model_out = weather_model.score_bucket_readonly(
                icao=ev_icao,
                forecast_high=forecast_high,
                threshold=b["threshold"],
                threshold_hi=b.get("threshold_hi"),
                direction=b["direction"],
                hours_to_resolution=hours_to_resolution,
                source_used=forecast_source,
            )
            fair_yes = model_out["fair_yes_prob"]
            model_n = min(int(model_out["bias_n"] or 0), int(model_out["sigma_n"] or 0))

            classification = _classify_bucket(
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                gap=gap,
                min_gap=min_gap,
                minutes_to_resolution=minutes_to_resolution,
                closure_minutes=closure_minutes,
                is_above_peak=is_above_peak,
            )
            if classification is None:
                continue
            action, target_px, reason = classification

            held_targets.append({
                "event_slug": slug,
                "event_title": ev["title"],
                "city": ev["city"],
                "title_date": ev["title_date"],
                "question": b["question"],
                "yes_token": b["yes_token"],
                "bucket_idx": i,
                "peak_idx": peak_idx,
                "gap": gap,
                "action": action,
                "stage": action,  # alias for back-compat with logging/db code
                "reason": reason,
                "target_price": target_px,
                "held_size": held_size,
                "minutes_to_resolution": minutes_to_resolution,
                "yes_bid": yes_bid or 0.0,
                "yes_ask": yes_ask or 0.0,
                "fair_yes_prob": fair_yes,
                "model_n": model_n,
                "forecast_high": forecast_high,
                "forecast_source": forecast_source,
                "icao": ev_icao,
            })

    log.info("  %d held bucket(s) qualify for fade-sell", len(held_targets))
    if not held_targets:
        return {"posted": 0, "considered": 0}

    # 4) Live: build CLOB client to place orders; dry-run: just log
    client = None
    if not dry_run:
        required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            log.error("Missing env: %s", ", ".join(missing))
            return {"posted": 0, "considered": len(held_targets), "error": "missing env"}
        # Derive CLOB API creds if not set
        if not (os.getenv("CLOB_API_KEY") and os.getenv("CLOB_SECRET") and os.getenv("CLOB_PASS")):
            creds = derive_api_credentials(
                host=os.environ["POLYMARKET_HOST"],
                private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                funder=funder,
                signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
            )
            os.environ["CLOB_API_KEY"] = creds.api_key
            os.environ["CLOB_SECRET"] = creds.api_secret
            os.environ["CLOB_PASS"] = creds.api_passphrase
        client = build_client(
            host=os.environ["POLYMARKET_HOST"],
            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
            api_key=os.environ["CLOB_API_KEY"],
            api_secret=os.environ["CLOB_SECRET"],
            api_passphrase=os.environ["CLOB_PASS"],
            funder=funder,
            signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
        )

    posted = 0
    for t in held_targets:
        # Open sell orders we already have on this token
        existing_sells = []
        if client is not None:
            existing_sells = [o for o in get_open_orders(client, t["yes_token"]) if str(o.get("side", "")).upper() == "SELL"]
        elif client is None:
            # dry-run: skip the API call to keep this fast
            pass

        # Already-covered shares at the right price
        existing_at_target = sum(
            _order_open_shares(o) for o in existing_sells
            if _order_price(o) is not None and abs(_order_price(o) - t["target_price"]) < 0.0005
        )
        existing_at_other = [
            o for o in existing_sells
            if _order_price(o) is not None and abs(_order_price(o) - t["target_price"]) >= 0.0005
        ]

        gap_to_fill = round(t["held_size"] - existing_at_target, 4)
        fair_yes = t.get("fair_yes_prob")
        model_str = (
            f" | fair_yes={fair_yes:.3f} (n={t.get('model_n', 0)})"
            if fair_yes is not None else " | model=N/A"
        )
        log.info(
            "[%-13s] %s %s | %s | gap=%d | yes_bid=%.4f ask=%.4f%s | held=%.2f new=%.2f @ $%.4f | reason=%s",
            t["action"].upper(),
            t["city"],
            t["title_date"],
            t["question"],
            t["gap"],
            t["yes_bid"],
            t["yes_ask"],
            model_str,
            t["held_size"],
            gap_to_fill,
            t["target_price"],
            t.get("reason", "?"),
        )

        if existing_at_other and gap_to_fill > 0:
            # Cancel mispriced existing sell orders so the new one can land at target_price
            for o in existing_at_other:
                oid = str(o.get("id") or o.get("orderID") or "")
                px = _order_price(o)
                rem = _order_open_shares(o)
                log.info("  cancelling mispriced sell  id=%s  px=%.4f  rem=%.2f", oid[:12], px or 0, rem)
                if not dry_run and client is not None and oid:
                    try:
                        cancel_order(client, oid)
                    except Exception as exc:
                        log.warning("  cancel failed: %s", exc)

        if gap_to_fill < min_remaining_shares:
            continue

        if dry_run:
            log.info(
                "  [DRY RUN] would SELL %.2f sh @ $%.4f on token %s ($%.2f notional)",
                gap_to_fill, t["target_price"], t["yes_token"][:12], gap_to_fill * t["target_price"],
            )
            posted += 1
            continue

        try:
            resp = place_sell_order(client, t["yes_token"], t["target_price"], gap_to_fill)
            order_id = resp.get("orderID") or resp.get("id") or "?"
            log.info("  PLACED sell  id=%s  %.2f sh @ $%.4f", order_id, gap_to_fill, t["target_price"])
            _record_fade(
                token_id=t["yes_token"],
                event_slug=t["event_slug"],
                question=t["question"],
                bucket_idx=t["bucket_idx"],
                peak_idx=t["peak_idx"],
                gap=t["gap"],
                target_price=t["target_price"],
                target_size=gap_to_fill,
                stage=t["stage"],
                order_id=str(order_id),
            )
            posted += 1
        except Exception as exc:
            log.error("  PLACE FAILED on %s: %s", t["question"], exc)

    log.info("Pass complete  considered=%d posted=%d", len(held_targets), posted)
    return {"posted": posted, "considered": len(held_targets)}


def explain_event(
    event_slug: str,
    *,
    min_gap: int = 2,
    closure_minutes: float = 60.0,
) -> dict:
    """
    Walk a SINGLE event end-to-end and print the classifier's per-bucket
    decision, including the model's fair_yes_prob and which trigger fired.
    Pretends we hold 1 YES of every bucket (regardless of actual positions)
    so every bucket is evaluated. Read-only — never posts orders.

    Used to sanity-check the model + classifier before going live.
    """
    log = _setup_logging()
    from automata.client import get_best_books_bulk
    from automata.parser import _extract_yes_token_id, _extract_no_token_id, _parse_threshold
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.weather_bot import _compute_resolution_dt, _extract_city, _extract_title_date
    from automata.weather import (
        extract_all_urls, extract_icao_from_url, extract_unit,
        fetch_coords_for_stations, fetch_forecasts_for_events,
    )
    from automata import weather_model

    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")

    log.info("──────── EXPLAIN — %s ────────", event_slug)
    payload = fetch_temperature_markets_payload()
    raws = [r for r in payload["markets"] if str(r.get("event_slug") or "") == event_slug]
    if not raws:
        log.error("event '%s' not found in current Gamma payload", event_slug)
        return {"ok": False, "error": "not found"}

    # Build event metadata identical to scan()
    event_title = str(raws[0].get("event_title") or event_slug)
    description = str(raws[0].get("event_description") or "")
    urls = extract_all_urls(description)
    icao = next((c for u in urls if (c := extract_icao_from_url(u))), None)
    unit = extract_unit(description)
    end_date = (raws[0].get("endDateIso") or raws[0].get("endDate") or "")[:10]
    city = _extract_city(event_title)
    title_date = _extract_title_date(event_title)

    # Fetch forecast for the event's station
    coords = fetch_coords_for_stations([icao]) if icao else {}
    forecasts = (
        fetch_forecasts_for_events(
            [{"icao": icao, "date": end_date, "unit": unit}],
            coords,
        ) if icao and end_date else {}
    )
    fcast = forecasts.get((icao, end_date)) if icao else None
    forecast_high, forecast_source = None, None
    if fcast:
        for src_key, src_name in (("noaa", "noaa"), ("taf", "taf"), ("open_meteo", "openmeteo")):
            v = fcast.get(src_key)
            if v is not None:
                forecast_high, forecast_source = v, src_name
                break

    # Build bucket list + fetch books
    buckets: list[dict] = []
    for r in raws:
        if r.get("closed") or (r.get("active") is not None and not r.get("active")):
            pass  # include for transparency; gap rule may still skip
        question = str(r.get("groupItemTitle") or r.get("question") or "-")
        parsed = _parse_threshold(question)
        if not parsed:
            continue
        threshold, threshold_hi, bucket_unit, direction = parsed
        buckets.append({
            "question": question,
            "yes_token": _extract_yes_token_id(r),
            "threshold": threshold,
            "threshold_hi": threshold_hi,
            "unit": bucket_unit,
            "direction": direction,
            "closed": r.get("closed"),
        })
    yes_tokens = [b["yes_token"] for b in buckets if b["yes_token"]]
    books = get_best_books_bulk(host, yes_tokens) if yes_tokens else {}

    peers_sorted = sorted(buckets, key=lambda b: (b["threshold"], b.get("threshold_hi") or b["threshold"]))

    # Peak detection
    peak_idx, peak_score = None, -1.0
    peer_books: list[dict] = []
    for i, b in enumerate(peers_sorted):
        yb = books.get(b["yes_token"], {}) if b["yes_token"] else {}
        bid, ask = yb.get("bid"), yb.get("ask")
        score = max([v for v in (bid, ask) if v is not None] or [None])
        peer_books.append({"yes_bid": bid, "yes_ask": ask})
        if score is not None and score > peak_score:
            peak_score, peak_idx = score, i

    # Resolution time
    res_dt = _compute_resolution_dt(city, title_date, end_date)
    now_utc = datetime.now(timezone.utc)
    minutes_to_res = ((res_dt - now_utc).total_seconds() / 60) if res_dt else None
    hours_to_res = (minutes_to_res / 60) if minutes_to_res else None

    log.info("Event:        %s", event_title)
    log.info("ICAO station: %s   forecast=%s°%s (source=%s)",
             icao or "?", f"{forecast_high:.1f}" if forecast_high is not None else "n/a",
             unit or "?", forecast_source or "n/a")
    log.info("Resolves:     %s   (in %s)",
             res_dt.strftime("%Y-%m-%d %H:%M %Z") if res_dt else "?",
             f"{hours_to_res:.2f} hours" if hours_to_res else "n/a")
    if peak_idx is not None:
        peak = peers_sorted[peak_idx]
        log.info("Peak bucket:  %s  yes_bid=%.4f", peak["question"], peer_books[peak_idx]["yes_bid"] or 0.0)
    log.info("")
    log.info("%-13s %-15s  gap  yes_bid  yes_ask    fair_yes  μ      σ    n  closed  reason  →  action @ price",
             "BUCKET", "")
    log.info("%s", "-" * 130)

    # Per-bucket walkthrough — pretend held_size=999 so min_size doesn't filter
    rows = []
    for i, b in enumerate(peers_sorted):
        yes_bid, yes_ask = peer_books[i]["yes_bid"], peer_books[i]["yes_ask"]
        is_peak = (peak_idx is not None and i == peak_idx)
        gap = abs(i - peak_idx) if peak_idx is not None else 999
        is_above_peak = peak_idx is not None and i > peak_idx

        model_out = weather_model.score_bucket_readonly(
            icao=icao,
            forecast_high=forecast_high,
            threshold=b["threshold"],
            threshold_hi=b.get("threshold_hi"),
            direction=b["direction"],
            hours_to_resolution=hours_to_res,
            source_used=forecast_source,
        )
        fair_yes = model_out["fair_yes_prob"]
        model_n = min(int(model_out["bias_n"] or 0), int(model_out["sigma_n"] or 0))

        classification = _classify_bucket(
            yes_bid=yes_bid, yes_ask=yes_ask,
            gap=gap,
            min_gap=min_gap,
            minutes_to_resolution=minutes_to_res,
            closure_minutes=closure_minutes,
            is_above_peak=is_above_peak,
        )

        if classification is None:
            action_str, price_str, reason = "SKIP", "-", "skipped"
        else:
            act, px, reason = classification
            action_str, price_str = act.upper(), f"${px:.4f}"

        peak_marker = "[PEAK]" if is_peak else ""
        log.info(
            "%-10s %-3s  %2d  %7.4f  %7.4f   %s  %s  %s  %2d  %-6s  %-30s →  %-13s %s",
            b["question"][:10], peak_marker, gap,
            yes_bid or 0.0, yes_ask or 0.0,
            f"{fair_yes:.3f}" if fair_yes is not None else " n/a ",
            f"{model_out['mu']:.1f}" if model_out["mu"] is not None else " n/a",
            f"{model_out['sigma']:.2f}" if model_out["sigma"] is not None else "n/a ",
            model_n,
            "yes" if b.get("closed") else "no",
            reason[:30],
            action_str, price_str,
        )
        rows.append({
            "bucket": b["question"],
            "is_peak": is_peak,
            "gap": gap,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "fair_yes_prob": fair_yes,
            "mu": model_out["mu"],
            "sigma": model_out["sigma"],
            "model_n": model_n,
            "closed": b.get("closed"),
            "action": action_str,
            "price": price_str,
            "reason": reason,
        })
    log.info("")
    return {
        "ok": True,
        "event_title": event_title,
        "event_slug": event_slug,
        "icao": icao,
        "forecast_high": forecast_high,
        "forecast_source": forecast_source,
        "minutes_to_resolution": minutes_to_res,
        "rows": rows,
    }


def run(
    *,
    dry_run: bool = True,
    loop: bool = False,
    interval_seconds: int = 60,
    min_gap: int = 2,
    edge_px: float = 0.002,
    closure_px: float = 0.001,
    closure_minutes: float = 60.0,
    min_remaining_shares: float = 5.0,
) -> None:
    log = _setup_logging()
    _init_state_db()
    log.info(
        "seller_bot start  dry_run=%s loop=%s interval=%ds  min_gap=%d edge=$%.4f closure=$%.4f closure_minutes=%.0f min_size=%.0f",
        dry_run, loop, interval_seconds, min_gap, edge_px, closure_px, closure_minutes, min_remaining_shares,
    )
    iteration = 0
    while True:
        iteration += 1
        log.info("── Iteration %d ──", iteration)
        try:
            scan(
                min_gap=min_gap,
                edge_px=edge_px,
                closure_px=closure_px,
                closure_minutes=closure_minutes,
                min_remaining_shares=min_remaining_shares,
                dry_run=dry_run,
            )
        except Exception as exc:
            log.exception("scan failed: %s", exc)
        if not loop:
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Aapang-style fade-sell daemon (Phase 1+3)")
    p.add_argument("--live", action="store_true", help="Place real CLOB sell orders. Default is dry-run.")
    p.add_argument("--loop", action="store_true", help="Loop forever (default: single pass)")
    p.add_argument("--interval", type=int, default=60, help="Loop interval seconds (default 60)")
    p.add_argument("--min-gap", type=int, default=2, help="Minimum bucket-distance from peak-YES to fade. Default 2.")
    p.add_argument("--edge-px", type=float, default=0.002, help="Edge-burst fade price (default $0.002)")
    p.add_argument("--closure-px", type=float, default=0.001, help="Closure-stage fade price (default $0.001)")
    p.add_argument("--closure-minutes", type=float, default=60.0, help="Minutes-to-resolution at which closure stage activates (default 60)")
    p.add_argument("--min-shares", type=float, default=5.0, help="Skip buckets with fewer than this many remaining shares (default 5 — matches Polymarket's order-size minimum; lower values fail at place_sell)")
    p.add_argument("--explain", help="Walk one event slug and show classifier decision per bucket (read-only). Bypasses held-position filter so every bucket is shown.")
    args = p.parse_args()

    if args.explain:
        explain_event(args.explain, min_gap=args.min_gap, closure_minutes=args.closure_minutes)
    else:
        run(
            dry_run=not args.live,
            loop=args.loop,
            interval_seconds=max(1, args.interval),
            min_gap=args.min_gap,
            edge_px=args.edge_px,
            closure_px=args.closure_px,
            closure_minutes=args.closure_minutes,
            min_remaining_shares=args.min_shares,
        )
