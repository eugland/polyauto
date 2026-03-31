from __future__ import annotations

import io
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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


def _local_datetime(end_datetime: str, city: str) -> str:
    """Return full local datetime string (e.g. '2026-03-30 15:00') for the city."""
    if not end_datetime:
        return "?"
    try:
        dt = datetime.fromisoformat(end_datetime.replace("Z", "+00:00"))
        tz_name = CITY_TZ.get(city, "UTC")
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return end_datetime[:16]

from dotenv import load_dotenv

# Ensure UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
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


def _temp_sort_key(question: str) -> float:
    """Extract the first number from a question string for sort ordering."""
    import re
    m = re.search(r"-?\d+(?:\.\d+)?", question)
    return float(m.group()) if m else 0.0


def run(dry_run: bool = True) -> None:
    from backendapp.services.polymarket_service import fetch_temperature_markets_payload
    from automata.parser import _parse_threshold
    from automata.weather import (
        extract_all_urls, extract_icao_from_wunderground_url,
        extract_station_name, extract_unit,
        fetch_coords_for_stations,
        fetch_forecasts_for_events,
    )

    min_no_price  = float(os.getenv("MIN_NO_PRICE", "0.95"))
    max_no_price  = float(os.getenv("MAX_NO_PRICE", "0.998"))
    bet_threshold = float(os.getenv("BET_THRESHOLD", "0.95"))   # auto-bet above this
    bet_shares    = float(os.getenv("BET_SIZE_SHARES", "10.0"))
    city_blacklist = {c.strip() for c in os.getenv("CITY_BLACKLIST", "").split(",") if c.strip()}

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
                (extract_icao_from_wunderground_url(u) for u in urls if "wunderground" in u.lower()),
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

    # ── Fetch station coords then forecast highs (Open-Meteo) ────────────────
    all_icaos = [ev["icao"] for ev in events.values() if ev["icao"]]
    coords = fetch_coords_for_stations(list(set(all_icaos))) if all_icaos else {}

    event_list = [
        {"icao": ev["icao"], "date": ev["date"], "unit": ev["unit"]}
        for ev in events.values() if ev["icao"] and ev["date"]
    ]
    log.info("Fetching forecasts for %d event/station pairs...", len(set(
        (e["icao"], e["date"]) for e in event_list
    )))
    forecasts = fetch_forecasts_for_events(event_list, coords)

    # ── Collect ALL candidates (no price filter, no dedup yet) ───────────────
    from automata.parser import _extract_no_token_id
    total_markets = 0
    all_candidates: list[dict[str, Any]] = []

    for event_slug, event in sorted(events.items()):
        visible = [
            r for r in event["markets"]
            if not r.get("closed") and not (r.get("active") is not None and not r.get("active"))
        ]

        for raw in visible:
            total_markets += 1
            question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
            end_date = _fmt_end_date(raw.get("endDateIso") or raw.get("endDate"))

            parsed = _parse_threshold(question)
            if not parsed:
                continue
            token_id = _extract_no_token_id(raw)
            if not token_id:
                continue
            from automata.parser import _extract_yes_token_id
            yes_token_id = _extract_yes_token_id(raw)

            threshold, threshold_hi, _unit, direction = parsed
            all_candidates.append({
                "question": question,
                "city": _extract_city(event["title"]),
                "title_date": _extract_title_date(event["title"]),
                "token_id": token_id,
                "yes_token_id": yes_token_id,
                "price": 0.0,
                "yes_price": None,
                "end_date": end_date,
                "end_datetime": raw.get("endDateIso") or raw.get("endDate") or "",
                "icao": event["icao"],
                "unit": event["unit"],
                "threshold": threshold,
                "threshold_hi": threshold_hi,
                "direction": direction,
                "skip_reason": None,
            })

    # ── Fetch live order book prices in bulk for ALL candidates ───────────────
    if all_candidates:
        from automata.client import get_best_asks_bulk
        host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
        no_ids  = [c["token_id"]     for c in all_candidates]
        yes_ids = [c["yes_token_id"] for c in all_candidates if c["yes_token_id"]]
        log.info("Fetching live order book prices for %d candidates (bulk)...", len(all_candidates))
        asks = get_best_asks_bulk(host, no_ids + yes_ids)
        for c in all_candidates:
            live_ask = asks.get(c["token_id"])
            if c["yes_token_id"]:
                c["yes_price"] = asks.get(c["yes_token_id"])
            if live_ask is None:
                c["skip_reason"] = "no asks in book"
            elif live_ask > max_no_price:
                c["price"] = live_ask
                c["skip_reason"] = f"ask {live_ask*100:.2f}¢ > max {max_no_price*100:.2f}¢"
            elif live_ask < min_no_price:
                c["price"] = live_ask
                c["skip_reason"] = f"ask {live_ask*100:.2f}¢ < min {min_no_price*100:.2f}¢"
            else:
                c["price"] = live_ask

    # ── Attach forecast high for each candidate ────────────────────────────────
    for c in all_candidates:
        fc = forecasts.get((c["icao"], c["end_date"])) if c.get("icao") else None
        c["forecast_high"] = fc["open_meteo"] if fc else None

    # ── Find best candidate per city (lowest Yes = farthest from resolving Yes) ─
    bettable = [c for c in all_candidates if not c["skip_reason"] and c["city"] not in city_blacklist]

    def _yes_key(c: dict) -> float:
        return c["yes_price"] if c["yes_price"] is not None else (1.0 - c["price"])

    best_per_city: dict[str, dict] = {}
    for c in bettable:
        city = c["city"]
        if city not in best_per_city or _yes_key(c) < _yes_key(best_per_city[city]):
            best_per_city[city] = c

    # ── Dry run: show only autobet (★) items per city ───────────────────────────
    if dry_run:
        now_utc = datetime.now(timezone.utc)
        autobet_items = sorted(
            best_per_city.values(),
            key=lambda c: now_utc.astimezone(ZoneInfo(CITY_TZ.get(c["city"], "UTC"))).utcoffset(),
            reverse=True,
        )
        print(f"  [DRY RUN] {len(autobet_items)} autobet item(s) (latest local time first):")
        print(f"    {'':1}  {'No':>7}  {'Yes':>7}  {'shares':>6}  {'cost':>6}  {'local time':<16}  {'city':<15}  {'date':<10}  {'forecast':>8}  question")
        print(f"    {'-'*1}  {'-'*7}  {'-'*7}  {'-'*6}  {'-'*6}  {'-'*16}  {'-'*15}  {'-'*10}  {'-'*8}  {'-'*22}")
        for c in autobet_items:
            cost = round(bet_shares * c["price"], 2)
            tz_name = CITY_TZ.get(c["city"], "UTC")
            now_local = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M")
            no_str  = f"{c['price']*100:6.2f}¢"
            yes_str = f"{c['yes_price']*100:6.2f}¢" if c["yes_price"] is not None else "   n/a "
            fcast_val = c.get("forecast_high")
            fcast_str = f"{fcast_val:.1f}°{c['unit']}" if fcast_val is not None else "n/a"
            print(f"    \u2605  {no_str}  {yes_str}  {bet_shares:>6.0f}sh  ${cost:>5.2f}  {now_local:<16}  {c['city']:<15}  {c['title_date']:<10}  {fcast_str:>8}  {c['question']}")
        print()
        log.info("  %d autobet item(s): %s", len(autobet_items),
                 ", ".join(f"{c['city']} {c['title_date']} {c['question']}" for c in autobet_items))
        return

    # ── Live betting: one bet per city-date, skip if position or open order exists ──
    from automata.client import build_client, place_no_order, get_positions, get_all_open_orders

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
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )

    # Build token_id → (city, date) lookup from all candidates
    token_to_city_date: dict[str, tuple[str, str]] = {
        c["token_id"]: (c["city"], c["end_date"]) for c in all_candidates
    }

    # position_size: token_id → shares held
    position_size: dict[str, float] = {
        p["token_id"]: p["size"] for p in (get_positions(funder) if funder else [])
    }

    # city-dates with an open buy order in the book (order in flight — don't touch)
    open_buy_city_dates: set[tuple[str, str]] = set()
    for o in get_all_open_orders(client):
        if str(o.get("side", "")).upper() == "BUY":
            cd = token_to_city_date.get(str(o.get("asset_id") or o.get("token_id", "")))
            if cd:
                open_buy_city_dates.add(cd)

    # city-dates where we hold any position (possibly a different token than today's best)
    held_city_dates: set[tuple[str, str]] = set()
    for tid in position_size:
        cd = token_to_city_date.get(tid)
        if cd:
            held_city_dates.add(cd)

    bets_to_place: list[dict] = []  # (candidate, shares_to_buy)
    for city, best in best_per_city.items():
        key = (city, best["end_date"])

        # Open order already in flight for this city-date → skip entirely
        if key in open_buy_city_dates:
            log.info("Open buy order in book  %s %s — skipping", city, best["end_date"])
            continue

        held = position_size.get(best["token_id"], 0.0)

        if held >= bet_shares:
            # Fully filled — nothing to do
            log.info("Position full  %s %s  %.2f shares — skipping", city, best["end_date"], held)
            continue

        if held == 0 and key in held_city_dates:
            # Position exists but in a different token — don't mix
            log.info("Position in different token  %s %s — skipping", city, best["end_date"])
            continue

        shares_needed = round(bet_shares - held, 2)
        best["_shares_to_buy"] = shares_needed
        best["_top_up"] = held > 0
        bets_to_place.append(best)

    if not bets_to_place:
        log.info("No new bets to place.")
        return

    print("  Placing orders...")
    print()

    for b in bets_to_place:
        shares_to_buy = b["_shares_to_buy"]
        cost = round(shares_to_buy * b["price"], 2)
        if cost > balance:
            log.warning("  Insufficient balance $%.2f for %s %s (need $%.2f) — skipping",
                        balance, b["city"], b["question"], cost)
            continue
        label = f"TOP-UP +{shares_to_buy}" if b["_top_up"] else f"{shares_to_buy:.0f} shares"
        try:
            resp = place_no_order(client, b["token_id"], b["price"], shares_to_buy)
            order_id = resp.get("orderID") or resp.get("id") or "?"
            status   = resp.get("status") or "submitted"
            result   = f"{status}  id={order_id}"
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
                no_price=b["price"],
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
        print(f"  BUY No @ {b['price']*100:.2f}¢  {label} (${cost:.2f})  {b['city']}  {b['title_date']}  {b['question']}  → {result}")

    print()


def _scan_positions(dry_run: bool = True) -> None:
    """
    For each open position, ensure a sell order at TAKE_PROFIT_PRICE exists.
    If not, place one (unless dry_run).
    """
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        return

    take_profit = float(os.getenv("TAKE_PROFIT_PRICE", "0.999"))

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
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
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
        has_tp   = any(
            abs(float(o.get("price", 0)) - take_profit) < 0.0001
            and str(o.get("side", "")).upper() == "SELL"
            for o in orders
        )
        if has_tp:
            log.info("  token %s  %.2f shares — take-profit order already exists", token_id[:12], size)
            continue
        if dry_run:
            log.info("  token %s  %.2f shares — [DRY RUN] would place sell @ %.1f¢", token_id[:12], size, take_profit * 100)
        else:
            try:
                resp = place_sell_order(client, token_id, take_profit, size)
                order_id = resp.get("orderID") or resp.get("id") or "?"
                log.info("  token %s  %.2f shares — sell @ %.1f¢ placed  id=%s", token_id[:12], size, take_profit * 100, order_id)
            except Exception as exc:
                log.error("  token %s — sell order failed: %s", token_id[:12], exc)



if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry run)")
    args = parser.parse_args()

    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]

    # ── Derive fresh API credentials from the private key ─────────────────────
    from automata.client import derive_api_credentials
    try:
        _creds = derive_api_credentials(
            host=os.environ["POLYMARKET_HOST"],
            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
            funder=os.getenv("POLYMARKET_FUNDER") or None,
            signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
        )
        os.environ["CLOB_API_KEY"]  = _creds.api_key
        os.environ["CLOB_SECRET"]   = _creds.api_secret
        os.environ["CLOB_PASS"]     = _creds.api_passphrase
    except Exception as exc:
        log.error("Failed to derive API credentials: %s", exc)
        raise SystemExit(1)

    from automata.db import init_db
    init_db()

    iteration = 0
    while True:
        iteration += 1
        log.info("=== Iteration %d ===", iteration)

        if args.bet:
            # ── Position scan ─────────────────────────────────────────────────
            _scan_positions(dry_run=False)

            # ── Balance check — gates new bets ────────────────────────────────
            bet_shares = float(os.getenv("BET_SIZE_SHARES", "10.0"))
            try:
                from automata.client import build_client, get_usdc_balance
                _client = build_client(
                    host=os.environ["POLYMARKET_HOST"],
                    private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                    api_key=os.environ["CLOB_API_KEY"],
                    api_secret=os.environ["CLOB_SECRET"],
                    api_passphrase=os.environ["CLOB_PASS"],
                    funder=os.getenv("POLYMARKET_FUNDER") or None,
                    signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
                )
                balance = get_usdc_balance(_client)
                max_no_price = float(os.getenv("MAX_NO_PRICE", "0.998"))
                min_required = bet_shares * max_no_price
                log.info("USDC balance: $%.2f  (need at least $%.2f for %g shares)", balance, min_required, bet_shares)
            except Exception as exc:
                log.warning("Balance check failed: %s — skipping new bets this cycle", exc)
                log.info("Sleeping 60 s...")
                time.sleep(60)
                continue

            if balance < min_required:
                log.warning("Balance $%.2f < $%.2f needed — skipping new bets", balance, min_required)
                log.info("Sleeping 60 s...")
                time.sleep(60)
                continue

        run(dry_run=not args.bet)

        log.info("Sleeping 60 s...")
        time.sleep(60)
