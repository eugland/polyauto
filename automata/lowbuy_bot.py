"""
Low-bracket safe-NO buyer.

Bets NO at near-1 prices on the LOWEST temperature brackets of the
closest-resolving "Highest Temperature in <city>" markets, restricted to
brackets where today's observed METAR max-so-far has already climbed above
the bracket's upper bound by a configurable safety margin. At that point
the bracket is mathematically locked out of winning YES, so the NO leg is
near-deterministic.

This is the inverse of `automata.weather_bot`, which bets NO on HIGH
brackets that the forecast says won't be reached. Here we bet NO on LOW
brackets that the day has already climbed past.
"""
from __future__ import annotations

import logging
import math
import os
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from automata import config
from automata.weather_bot import (
    CITY_TZ,
    _FAR_FUTURE,
    _compute_maker_buy_price,
    _compute_resolution_dt,
    _extract_city,
    _extract_title_date,
    _fmt_end_date,
    _order_open_shares,
    _order_price,
)

log = logging.getLogger("automata.lowbuy")


def _bracket_upper(direction: str | None, threshold: float | None, threshold_hi: float | None) -> float | None:
    """
    Return the upper-bound °temperature for a bracket — the value the day's
    max-so-far must exceed for the bracket to be locked out of YES.

    - "below"  ("Below 40°F")    → threshold (40)
    - "range"  ("40-45°F")       → threshold_hi (45)
    - "exact"  ("40°F")          → threshold (40)
    - "higher" ("Above 75°F")    → None (these are HIGH brackets, not for us)
    """
    if direction == "higher":
        return None
    if direction == "range":
        return threshold_hi if threshold_hi is not None else threshold
    if direction in ("below", "exact"):
        return threshold
    return None


def _bracket_lower(direction: str | None, threshold: float | None, threshold_hi: float | None) -> float | None:
    """
    Lower bound °temperature for a bracket — minimum the day's max needs to
    hit for YES.
      - "higher"  ("Above 75°F")  → threshold (75)
      - "range"   ("70-75°F")     → threshold (70)
      - "below" / "exact"         → None (no meaningful lower bound)
    """
    if direction == "higher":
        return threshold
    if direction == "range":
        return threshold
    return None


def _safety_margin(unit: str | None) -> float:
    if (unit or "C").upper() == "F":
        return config.get_float("LOWBUY_SAFETY_MARGIN_F", "lowbuy", "safety_margin_f", 6.0)
    return config.get_float("LOWBUY_SAFETY_MARGIN_C", "lowbuy", "safety_margin_c", 3.0)


def run(
    dry_run: bool = True,
    max_spend_usdc: float | None = None,
    max_orders: int | None = None,
) -> None:
    from automata.client import (
        build_client,
        cancel_order,
        get_all_open_orders,
        get_best_bid_ask,
        get_best_books_bulk,
        get_positions,
        get_usdc_balance,
        place_no_order,
    )
    from automata.parser import (
        _extract_no_token_id,
        _parse_threshold,
    )
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.weather import fetch_metar_today_stats

    min_no_price = config.get_float("LOWBUY_MIN_NO_PRICE", "lowbuy", "min_no_price", 0.97)
    max_no_price = config.get_float("LOWBUY_MAX_NO_PRICE", "lowbuy", "max_no_price", 0.997)
    bet_shares = config.get_float("LOWBUY_BET_SIZE_SHARES", "lowbuy", "bet_size_shares", 22.0)
    min_hours = config.get_float("LOWBUY_MIN_HOURS", "lowbuy", "min_hours_to_resolution", 0.0)
    max_hours = config.get_float("LOWBUY_MAX_HOURS", "lowbuy", "max_hours_to_resolution", 24.0)
    target_events = config.get_int("LOWBUY_TARGET_EVENTS", "lowbuy", "target_events", 50)
    max_brackets = config.get_int("LOWBUY_MAX_BRACKETS_PER_EVENT", "lowbuy", "max_brackets_per_event", 3)
    mm_tick_size = config.get_float("LOWBUY_MM_TICK_SIZE", "lowbuy", "mm_tick_size", 0.001)
    mm_join_bid_ticks = config.get_int("LOWBUY_MM_JOIN_BID_TICKS", "lowbuy", "mm_join_bid_ticks", 1)
    mm_reprice_cents = config.get_float("LOWBUY_MM_REPRICE_CENTS", "lowbuy", "mm_reprice_cents", 0.10)
    mm_reprice_delta = mm_reprice_cents / 100.0
    city_blacklist = set(config.get_list_str("LOWBUY_CITY_BLACKLIST", "lowbuy", "city_blacklist", []))
    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")

    log.info("Fetching Polymarket temperature markets...")
    payload = fetch_temperature_markets_payload()
    raw_markets = payload["markets"]
    log.info("  %d raw markets fetched", len(raw_markets))

    # ── Group markets by event, compute resolution_dt, filter by window ──
    from automata.weather import (
        extract_all_urls,
        extract_icao_from_url,
        extract_station_name,
        extract_unit,
    )

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
                "city": _extract_city(event_title),
                "title_date": _extract_title_date(event_title),
                "station_name": extract_station_name(description),
                "icao": icao,
                "unit": extract_unit(description),
                "date": event_date,
                "markets": [],
            }
        events[event_slug]["markets"].append(raw)

    now_utc = datetime.now(timezone.utc)
    min_cutoff = now_utc + timedelta(hours=min_hours)
    max_cutoff = now_utc + timedelta(hours=max_hours)
    for ev in events.values():
        ev["resolution_dt"] = _compute_resolution_dt(ev["city"], ev["title_date"], ev["date"])

    in_window = [
        (slug, ev) for slug, ev in events.items()
        if ev["resolution_dt"] is not None
        and min_cutoff <= ev["resolution_dt"] <= max_cutoff
        and ev["city"] not in city_blacklist
    ]
    in_window.sort(key=lambda t: t[1]["resolution_dt"])
    selected = in_window[:target_events]

    if not selected:
        log.info(
            "  no events resolving within %.1f–%.1fh from now — nothing to do",
            min_hours, max_hours,
        )
        return

    log.info("  %d closest-resolving events selected:", len(selected))
    for slug, ev in selected:
        log.info(
            "    - %s  [res=%s, %s]",
            ev["title"], ev["resolution_dt"].strftime("%Y-%m-%d %H:%MZ"), ev["icao"] or "?",
        )

    # ── Fetch METAR (current + max-so-far-today) per (icao, event_date) ──
    from concurrent.futures import ThreadPoolExecutor, as_completed
    metar_keys: set[tuple[str, str, str, str]] = set()
    for _, ev in selected:
        if not ev["icao"]:
            continue
        tz_name = CITY_TZ.get(ev["city"], "UTC")
        metar_keys.add((ev["icao"], tz_name, ev["date"], ev["unit"]))
    metar_stats: dict[tuple[str, str], tuple[float | None, float | None]] = {}
    if metar_keys:
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(metar_keys)))) as ex:
            futs = {
                ex.submit(fetch_metar_today_stats, icao, tz, ed, u): (icao, ed)
                for (icao, tz, ed, u) in metar_keys
            }
            for fut in as_completed(futs):
                try:
                    metar_stats[futs[fut]] = fut.result()
                except Exception:
                    metar_stats[futs[fut]] = (None, None)

    # ── Pre-parse brackets for every event + collect NO token ids ────────
    parsed_by_slug: dict[str, list[tuple[dict, tuple[float, float | None, str, str]]]] = {}
    all_no_token_ids: list[str] = []
    for slug, ev in selected:
        parsed_markets: list[tuple[dict, tuple[float, float | None, str, str]]] = []
        for raw in ev["markets"]:
            if raw.get("closed") or raw.get("active") is False:
                continue
            question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
            parsed = _parse_threshold(question)
            if not parsed:
                continue
            parsed_markets.append((raw, parsed))
            no_token = _extract_no_token_id(raw)
            if no_token:
                all_no_token_ids.append(no_token)
        parsed_by_slug[slug] = parsed_markets

    # ── Bulk-fetch NO books for every parsed bracket (used by both the
    #    per-event log line and the candidate-gate further down) ─────────
    log.info("  fetching books for %d brackets across %d events...", len(all_no_token_ids), len(selected))
    no_books: dict[str, dict] = get_best_books_bulk(host, all_no_token_ids) if all_no_token_ids else {}

    # ── Open-Meteo daily-max forecast per event (parallel) ───────────────
    from automata.weather import fetch_coords_for_stations, fetch_open_meteo_high
    icao_set = {ev["icao"] for _, ev in selected if ev["icao"]}
    station_coords = fetch_coords_for_stations(list(icao_set)) if icao_set else {}
    forecast_highs: dict[tuple[str, str], float | None] = {}
    forecast_tasks: list[tuple[str, str, str, tuple[float, float]]] = []
    for _, ev in selected:
        if not ev["icao"] or not ev["date"]:
            continue
        coords = station_coords.get(ev["icao"])
        if not coords:
            continue
        forecast_tasks.append((ev["icao"], ev["date"], ev["unit"] or "C", coords))
    if forecast_tasks:
        with ThreadPoolExecutor(max_workers=min(10, len(forecast_tasks))) as ex:
            futs = {
                ex.submit(fetch_open_meteo_high, lat, lon, date, unit): (icao, date)
                for (icao, date, unit, (lat, lon)) in forecast_tasks
            }
            for fut in as_completed(futs):
                key = futs[fut]
                try:
                    forecast_highs[key] = fut.result()
                except Exception:
                    forecast_highs[key] = None

    def _bracket_sort_key(parsed_t: tuple[float, float | None, str, str]) -> tuple[float, float]:
        # Below-X → (-inf, X);  range A-B → (A, B);  Above-X → (X, +inf)
        threshold, threshold_hi, _u, direction = parsed_t
        lo = _bracket_lower(direction, threshold, threshold_hi)
        up = _bracket_upper(direction, threshold, threshold_hi)
        return (lo if lo is not None else -float("inf"), up if up is not None else float("inf"))

    # ── Build per-event candidate brackets ───────────────────────────────
    candidates: list[dict[str, Any]] = []
    scan_fmt = "  %-14s  %6s  %6s  %6s  %6s  %-7s  %-13s  %-5s  %4s  %7s"
    log.info(
        scan_fmt,
        "city", "t_res", "cur", "max", "fc",
        "low", "no(bid/ask)", "exp", "yes%", "dist",
    )
    log.info(
        scan_fmt,
        "-" * 14, "-" * 6, "-" * 6, "-" * 6, "-" * 6,
        "-" * 7, "-" * 13, "-" * 5, "-" * 4, "-" * 7,
    )
    for slug, ev in selected:
        unit = ev["unit"] or "C"
        margin = _safety_margin(unit)
        metar_key = (ev["icao"] or "", ev["date"])
        cur_temp, max_so_far = metar_stats.get(metar_key, (None, None))
        parsed_markets = parsed_by_slug[slug]

        # Expected = bracket the market thinks is most likely (highest implied
        # YES, i.e., lowest NO bid). Computed first so it can anchor the
        # "lowest_bet" filter below. Pick a representative °temp: midpoint for
        # ranges, threshold for higher/below/exact.
        expected: dict[str, Any] | None = None
        expected_no_bid = float("inf")
        for raw, parsed in parsed_markets:
            no_tok = _extract_no_token_id(raw)
            if not no_tok:
                continue
            bid = no_books.get(no_tok, {}).get("bid")
            if bid is None:
                continue
            if bid < expected_no_bid:
                expected_no_bid = bid
                threshold, threshold_hi, parsed_unit, direction = parsed
                if direction == "range" and threshold_hi is not None:
                    exp_temp = (threshold + threshold_hi) / 2
                else:
                    exp_temp = threshold
                expected = {
                    "question": str(raw.get("groupItemTitle") or raw.get("question") or "-"),
                    "yes_implied": 1.0 - bid,
                    "temp": exp_temp,
                    "unit": parsed_unit or unit,
                }

        # Lowest bettable LOW bracket: lowest "Below/range/exact" whose upper
        # bound is strictly below the market-expected high AND whose NO bid
        # falls in the display window (96¢, 99.90¢). The window drops books
        # locked at near-1 (no upside) and below 96¢ (too uncertain).
        display_min_no = 0.96
        display_max_no = 0.999
        lowest_bet: dict[str, Any] | None = None
        if expected is not None:
            for raw, parsed in sorted(parsed_markets, key=lambda rp: _bracket_sort_key(rp[1])):
                threshold, threshold_hi, parsed_unit, direction = parsed
                upper = _bracket_upper(direction, threshold, threshold_hi)
                if upper is None or upper >= expected["temp"]:
                    continue
                no_tok = _extract_no_token_id(raw)
                if not no_tok:
                    continue
                book = no_books.get(no_tok, {})
                bid = book.get("bid")
                ask = book.get("ask")
                if bid is None or bid <= display_min_no or bid >= display_max_no:
                    continue
                lowest_bet = {
                    "question": str(raw.get("groupItemTitle") or raw.get("question") or "-"),
                    "no_bid": bid,
                    "no_ask": ask,
                    "temp": upper,
                    "unit": parsed_unit or unit,
                }
                break

        forecast_high = forecast_highs.get((ev["icao"] or "", ev["date"]))
        hours_to_res = (ev["resolution_dt"] - now_utc).total_seconds() / 3600.0

        cur_str = f"{cur_temp:.1f}°{unit}" if cur_temp is not None else "n/a"
        max_str = f"{max_so_far:.1f}°{unit}" if max_so_far is not None else "n/a"
        fc_str = f"{forecast_high:.1f}°{unit}" if forecast_high is not None else "n/a"
        t_res_str = f"{hours_to_res:.2f}h"

        if lowest_bet is not None:
            low_str = f"≤{lowest_bet['temp']:.0f}°{lowest_bet['unit']}"
            bid_str = f"{lowest_bet['no_bid']*100:.2f}¢"
            ask = lowest_bet.get("no_ask")
            ask_cell = f"{ask*100:.2f}¢" if ask is not None else "—"
            no_str = f"{bid_str}/{ask_cell}"
        else:
            low_str = "n/a"
            no_str = "n/a"

        if expected is not None:
            exp_str = f"{expected['temp']:.0f}°{expected['unit']}"
            yes_pct_str = f"{expected['yes_implied']*100:.0f}%"
        else:
            exp_str = "n/a"
            yes_pct_str = "n/a"

        if lowest_bet is not None and expected is not None:
            distance = expected["temp"] - lowest_bet["temp"]
            sign = "+" if distance >= 0 else ""
            dist_str = f"{sign}{distance:.1f}°{unit}"
        else:
            dist_str = "n/a"

        log.info(
            scan_fmt,
            ev["city"], t_res_str, cur_str, max_str, fc_str,
            low_str, no_str, exp_str, yes_pct_str, dist_str,
        )

        if max_so_far is None:
            continue

        evidence_temp = max_so_far

        # Each market in the event is a bracket — find safe LOW ones.
        event_brackets: list[dict[str, Any]] = []
        for raw, (threshold, threshold_hi, parsed_unit, direction) in parsed_markets:
            upper = _bracket_upper(direction, threshold, threshold_hi)
            if upper is None:
                continue  # "above" / unknown — not a low bracket
            slack = evidence_temp - upper
            if slack < margin:
                continue
            question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
            no_token = _extract_no_token_id(raw)
            if not no_token:
                continue
            event_brackets.append({
                "event_slug": slug,
                "city": ev["city"],
                "title_date": ev["title_date"],
                "icao": ev["icao"],
                "unit": parsed_unit or unit,
                "question": question,
                "token_id": no_token,
                "end_date": _fmt_end_date(raw.get("endDateIso") or raw.get("endDate")),
                "resolution_dt": ev["resolution_dt"],
                "threshold": threshold,
                "threshold_hi": threshold_hi,
                "direction": direction,
                "bracket_upper": upper,
                "max_so_far": max_so_far,
                "metar_current": cur_temp,
                "forecast_high": None,
                "slack": slack,
            })

        # Sort by safest first (largest slack), then take top N
        event_brackets.sort(key=lambda c: -c["slack"])
        for c in event_brackets[:max_brackets]:
            candidates.append(c)

    if not candidates:
        log.info("  no safe low brackets across selected events — nothing to do")
        return

    # ── Gate on bid/ask using the already-fetched NO books. The bot is
    #    NO-only, so no YES fetch is needed.
    bettable: list[dict[str, Any]] = []
    for c in candidates:
        b = no_books.get(c["token_id"], {})
        ask = b.get("ask")
        bid = b.get("bid")
        c["bid"] = bid
        c["ask"] = ask
        if ask is None:
            c["skip_reason"] = "no asks in book"
        elif ask > max_no_price:
            c["skip_reason"] = f"ask {ask*100:.2f}¢ > max {max_no_price*100:.2f}¢"
        elif bid is None:
            c["skip_reason"] = "no bids in book"
        elif bid > max_no_price:
            c["skip_reason"] = f"bid {bid*100:.2f}¢ > max {max_no_price*100:.2f}¢ (market already past cap)"
        elif bid < min_no_price:
            c["skip_reason"] = f"bid {bid*100:.2f}¢ < min {min_no_price*100:.2f}¢"
        else:
            c["skip_reason"] = None
            bettable.append(c)
        log.info(
            "    [%s] %s %s — bid=%s ask=%s slack=%.1f°%s  %s",
            "BET " if not c["skip_reason"] else "skip",
            c["city"], c["question"],
            f"{bid*100:.2f}¢" if bid is not None else "n/a",
            f"{ask*100:.2f}¢" if ask is not None else "n/a",
            c["slack"], c["unit"],
            c["skip_reason"] or "",
        )

    if not bettable:
        log.info("  no bettable brackets after order-book filter — nothing to do")
        return

    # Sort: closest resolution first, then largest slack
    bettable.sort(key=lambda c: (c["resolution_dt"] or _FAR_FUTURE, -c["slack"]))

    # ── Always print the table of valid candidates (both modes) ──────────
    user_tz = ZoneInfo(config.get_str("USER_TZ", "weather", "user_tz", "America/Los_Angeles"))
    label = "[DRY RUN]" if dry_run else "[LIVE]"
    print(f"  {label} {len(bettable)} VALID candidates (both bid+ask, NO ≥{min_no_price*100:.0f}¢, sorted by closest-resolution then largest-slack):")
    print(f"    {'No':>7}  {'shares':>6}  {'cost':>6}  {'resolves (PT)':<16}  {'city':<15}  {'bracket up':>10}  {'evidence':>10}  {'slack':>6}  question")
    print(f"    {'-'*7}  {'-'*7}  {'-'*6}  {'-'*16}  {'-'*15}  {'-'*10}  {'-'*10}  {'-'*6}  {'-'*22}")
    for c in bettable:
        cost = round(bet_shares * c["ask"], 2)
        res_local = c["resolution_dt"].astimezone(user_tz).strftime("%b %d %H:%M") if c["resolution_dt"] else "?"
        print(
            f"    {c['ask']*100:6.2f}¢  "
            f"{bet_shares:>6.0f}sh  ${cost:>5.2f}  "
            f"{res_local:<16}  {c['city']:<15}  "
            f"{c['bracket_upper']:>7.1f}°{c['unit']}  "
            f"{c['max_so_far']:>7.1f}°{c['unit']}  "
            f"{c['slack']:>5.1f}°  "
            f"{c['question']}"
        )
    print()

    if dry_run:
        return

    # ── Live: place orders ────────────────────────────────────────────────
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
        balance = min(balance, max_spend_usdc)
        log.info("Balance cap enabled, capped to $%.2f", balance)

    if balance < bet_shares * max_no_price:
        bet_shares = round(0.9 * balance / max_no_price, 2)
        log.info("Balance-adjusted bet_shares: %.2f", bet_shares)

    # Skip brackets we already hold or have an open buy on.
    position_size: dict[str, float] = {
        p["token_id"]: p["size"] for p in (get_positions(funder) if funder else [])
    }
    open_orders = get_all_open_orders(client)
    open_buys_by_token: dict[str, list[dict]] = defaultdict(list)
    for o in open_orders:
        if str(o.get("side", "")).upper() != "BUY":
            continue
        tid = str(o.get("asset_id") or o.get("token_id", ""))
        if tid:
            open_buys_by_token[tid].append(o)

    # Optional stale-cancel pass for our own bracket tokens
    stale_minutes = config.get_float("LOWBUY_STALE_ORDER_MINUTES", "lowbuy", "stale_order_minutes", 1.0)
    stale_cutoff = _time.time() - stale_minutes * 60
    cancelled: set[str] = set()
    candidate_tokens = {c["token_id"] for c in bettable}
    for tid in candidate_tokens:
        for o in open_buys_by_token.get(tid, []):
            if _order_open_shares(o) <= 0:
                continue
            order_id = str(o.get("id") or o.get("orderID") or "")
            if not order_id:
                continue
            created_at = o.get("created_at")
            if created_at is None or float(created_at) > stale_cutoff:
                continue
            try:
                cancel_order(client, order_id)
                cancelled.add(order_id)
                log.info("Cancelled stale order %s (%.0f min old)", order_id[:12], (_time.time() - float(created_at)) / 60)
            except Exception as exc:
                log.warning("Failed to cancel stale order %s: %s", order_id[:12], exc)

    orders_placed = 0
    for c in bettable:
        if max_orders is not None and orders_placed >= max_orders:
            log.info("Reached max_orders=%d — stopping", max_orders)
            break
        held = position_size.get(c["token_id"], 0.0)
        existing_orders = [o for o in open_buys_by_token.get(c["token_id"], []) if str(o.get("id") or o.get("orderID") or "") not in cancelled]
        open_open_shares = sum(_order_open_shares(o) for o in existing_orders)
        if held + open_open_shares >= bet_shares:
            log.info("Already covered  %s %s  held=%.2f open=%.2f — skipping",
                     c["city"], c["question"], held, open_open_shares)
            continue
        shares_needed = round(bet_shares - held - open_open_shares, 2)
        if shares_needed <= 0:
            continue

        best_bid, best_ask = get_best_bid_ask(host, c["token_id"])
        quote_price = _compute_maker_buy_price(
            best_bid=best_bid,
            best_ask=best_ask,
            min_no_price=min_no_price,
            max_no_price=max_no_price,
            tick_size=mm_tick_size,
            join_bid_ticks=mm_join_bid_ticks,
        )
        if quote_price is None:
            log.info("No valid maker quote  %s %s — skipping", c["city"], c["question"])
            continue

        # Reprice path: cancel existing if quote diverges meaningfully.
        needs_reprice = any(
            (_order_price(o) is not None) and (abs(_order_price(o) - quote_price) >= mm_reprice_delta)
            for o in existing_orders
            if _order_open_shares(o) > 0
        )
        if needs_reprice:
            for o in existing_orders:
                if _order_open_shares(o) <= 0:
                    continue
                order_id = str(o.get("id") or o.get("orderID") or "")
                if not order_id:
                    continue
                try:
                    cancel_order(client, order_id)
                    cancelled.add(order_id)
                except Exception as exc:
                    log.warning("Failed to cancel %s: %s", order_id[:12], exc)

        cost = round(shares_needed * quote_price, 2)
        if cost > balance:
            log.warning("Insufficient balance $%.2f for %s %s (need $%.2f) — skipping",
                        balance, c["city"], c["question"], cost)
            continue
        try:
            resp = place_no_order(client, c["token_id"], quote_price, shares_needed, post_only=False)
            order_id = resp.get("orderID") or resp.get("id") or "?"
            status = resp.get("status") or "submitted"
            log.info(
                "[lowbuy] %s %s  BUY No @ %.2f¢  %.0f sh  ($%.2f)  → %s id=%s",
                c["city"], c["question"], quote_price * 100, shares_needed, cost, status, order_id,
            )
            balance = round(balance - cost, 2)
            orders_placed += 1
            from automata.db import record_bet
            record_bet(
                city=c["city"],
                icao=c.get("icao"),
                event_date=c["end_date"],
                question=c["question"],
                option="No",
                token_id=c["token_id"],
                order_id=order_id,
                shares=shares_needed,
                no_price=quote_price,
                yes_price=None,
                cost_usdc=cost,
                unit=c.get("unit"),
                threshold=c.get("threshold"),
                threshold_hi=c.get("threshold_hi"),
                direction=c.get("direction"),
                forecast_high=c.get("forecast_high"),
            )
        except Exception as exc:
            log.error("[lowbuy] order failed for %s %s: %s", c["city"], c["question"], exc)
