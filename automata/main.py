from __future__ import annotations

import io
import logging
import math
import os
import sys
from collections import defaultdict
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


def _compute_maker_buy_price(
    best_bid: float | None,
    best_ask: float | None,
    min_no_price: float,
    max_no_price: float,
    tick_size: float,
    join_bid_ticks: int,
) -> float | None:
    """
    Build a buy quote within price limits. Will pay up to the ask price but
    never above max_no_price (0.998).
    """
    if best_ask is None:
        return None  # no counterparty — passive fallback will handle this
    quote = min(best_ask, max_no_price)
    quote = round(_round_down_to_tick(quote, tick_size), 6)
    if quote < min_no_price:
        return None
    return quote


def run(dry_run: bool = True) -> None:
    from backendapp.services.polymarket_service import fetch_temperature_markets_payload
    from automata.parser import _parse_threshold
    from automata.weather import (
        extract_all_urls, extract_icao_from_wunderground_url,
        extract_station_name, extract_unit,
        fetch_coords_for_stations,
        fetch_forecasts_for_events,
    )

    min_no_price  = float(os.getenv("MIN_NO_PRICE", "0.97"))
    max_no_price  = float(os.getenv("MAX_NO_PRICE", "0.998"))
    bet_threshold = float(os.getenv("BET_THRESHOLD", "0.95"))   # auto-bet above this
    bet_shares    = 20.0   # first-fill target per city
    max_shares    = 40.0   # top-up ceiling per city
    mm_tick_size = float(os.getenv("MM_TICK_SIZE", "0.001"))
    mm_join_bid_ticks = int(os.getenv("MM_JOIN_BID_TICKS", "1"))
    mm_reprice_cents = float(os.getenv("MM_REPRICE_CENTS", "0.10"))
    mm_reprice_delta = mm_reprice_cents / 100.0
    city_blacklist = {c.strip() for c in os.getenv("CITY_BLACKLIST", "Seoul,Taipei").split(",") if c.strip()}

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
        from automata.client import get_best_books_bulk
        host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
        no_ids  = [c["token_id"]     for c in all_candidates]
        yes_ids = [c["yes_token_id"] for c in all_candidates if c["yes_token_id"]]
        log.info("Fetching live order book prices for %d candidates (bulk)...", len(all_candidates))
        books = get_best_books_bulk(host, no_ids + yes_ids)
        for c in all_candidates:
            book = books.get(c["token_id"], {})
            live_ask = book.get("ask")
            live_bid = book.get("bid")
            if c["yes_token_id"]:
                c["yes_price"] = books.get(c["yes_token_id"], {}).get("ask")
            c["price"] = live_ask or 0.0
            c["bid"] = live_bid
            if live_bid is None:
                c["skip_reason"] = "no bids in book"
            elif live_bid > max_no_price:
                c["skip_reason"] = f"bid {live_bid*100:.2f}¢ > max {max_no_price*100:.2f}¢"
            elif live_bid < min_no_price:
                c["skip_reason"] = f"bid {live_bid*100:.2f}¢ < min {min_no_price*100:.2f}¢"

    # ── Attach forecast high for each candidate ────────────────────────────────
    for c in all_candidates:
        fc = forecasts.get((c["icao"], c["end_date"])) if c.get("icao") else None
        c["forecast_high"] = fc["open_meteo"] if fc else None

    # ── Ranked candidates per city (safest first = lowest Yes price) ────────────
    bettable = [c for c in all_candidates if not c["skip_reason"] and c["city"] not in city_blacklist]

    def _yes_key(c: dict) -> float:
        return c["yes_price"] if c["yes_price"] is not None else (1.0 - c["price"])

    ranked_per_city: dict[str, list[dict]] = {}
    for c in bettable:
        ranked_per_city.setdefault(c["city"], []).append(c)
    for city in ranked_per_city:
        ranked_per_city[city].sort(key=_yes_key)

    # ── Dry run: show only autobet (★) items per city ───────────────────────────
    if dry_run:
        now_utc = datetime.now(timezone.utc)
        autobet_items = sorted(
            [ranked[0] for ranked in ranked_per_city.values()],
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
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    try:
        balance = get_usdc_balance(client)
    except Exception as exc:
        log.error("Failed to fetch USDC balance: %s", exc)
        return

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
    stale_minutes = float(os.getenv("STALE_ORDER_MINUTES", "5"))
    max_passes = int(os.getenv("BET_PASSES", "3"))

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
            # Round 1 before Round 2; within each round, lowest ask first
            bets_to_place.sort(key=lambda b: (b.get("_fill_round", 1), b.get("_live_ask") or 1.0))

            print(f"  Pass {pass_num} — placing {len(bets_to_place)} order(s)...")
            print()

            for b in bets_to_place:
                best_bid, best_ask = get_best_bid_ask(os.environ["POLYMARKET_HOST"], b["token_id"])
                quote_price = _compute_maker_buy_price(
                    best_bid=best_bid,
                    best_ask=best_ask,
                    min_no_price=min_no_price,
                    max_no_price=max_no_price,
                    tick_size=mm_tick_size,
                    join_bid_ticks=mm_join_bid_ticks,
                )
                if quote_price is None:
                    log.info("No valid maker quote  %s %s — skipping", b["city"], b["question"])
                    continue

                shares_to_buy = b.get("_shares_to_buy", bet_shares)
                if shares_to_buy <= 0:
                    continue

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
                    balance = round(balance - cost, 2)
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

        if pass_num < max_passes:
            wait = int(os.getenv("BET_PASS_WAIT_SECONDS", "10"))
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
            bet_shares = float(os.getenv("BET_SIZE_SHARES", "20.0"))
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
