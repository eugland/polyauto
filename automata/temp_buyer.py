"""
Auto NO-buyer for top-of-list Polymarket "Highest Temperature" markets.

For every temperature event resolving in the next N hours, pick the
lowest-temp bucket (= farthest below the YES-favorite on the bucket
ladder) whose NO ask is in the configured band. The cap defaults to
99.8¢ so the saturated top-of-stack 99.9¢ bucket is skipped in favor
of the next bucket down, which has more upside per share.

Then, for the top eligible item that we don't already hold:

    * NO ask ≥ TEMPBUY_MIN_NO_ASK              (default 0.97)
    * bucket distance to YES-favorite ≥ TEMPBUY_MIN_BUCKET_DISTANCE
      (default 2; counts ladder steps below the favorite, unit-free)
    * city not in [weather].city_blacklist
    * no live position + no open buy + no DB row on (city, event_date)

…place one GTC limit buy on the NO token at the live ask for
TEMPBUY_BET_SHARES (default 20 sh). Skipped if free USDC balance is
below the resulting cost.

By default we only consider events resolving 4–18 hours from now —
sub-4h events are skipped because they're too close to resolution to
leave reaction room, and >18h events are skipped because the YES
favorite hasn't settled yet (the picker would just chase noise).

Candidates are sorted by closest resolution first, then displayed as
an aligned table for quick scanning.

Usage:
  python -m automata.temp_buyer                   # dry-run, top candidate
  python -m automata.temp_buyer --bet             # live: place the order
  python -m automata.temp_buyer --within 24       # 24h upper bound
  python -m automata.temp_buyer --min-hours 8     # require >=8h to resolution
  python -m automata.temp_buyer --bet --shares 20
"""
from __future__ import annotations

import argparse
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from automata import config

load_dotenv()

_LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "temp_buyer.log"
_SHARED_LOG_FILE = _LOGS_DIR / "automata.log"


def _setup_logging() -> logging.Logger:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler()],
        )
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for path in (_LOG_FILE, _SHARED_LOG_FILE):
        if not any(
            isinstance(h, logging.FileHandler)
            and getattr(h, "baseFilename", "") == str(path)
            for h in root.handlers
        ):
            fh = logging.FileHandler(path, encoding="utf-8")
            fh.setFormatter(fmt)
            fh.setLevel(logging.INFO)
            root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.temp_buyer")


log = logging.getLogger("automata.temp_buyer")


def _select_for_event(
    event: dict, *, min_no_ask: float, max_no_ask: float, min_no_bid: float = 0.0,
) -> dict | None:
    """
    Per-event picker: among buckets strictly below the YES-favorite whose
    NO ask is in [min_no_ask, max_no_ask], return the one farthest down
    the bucket ladder (= lowest temperature). The default cap is 0.998
    so the saturated 99.9¢ top-of-stack bucket is skipped in favor of the
    next bucket down, which carries more upside per share.

    Returns the NO token id, detected unit (C/F), bucket distance to the
    favorite, and resolution time so the caller can place an order. No
    min-distance filter here — caller layers that on top.
    """
    from experiment.view import (
        _bucket_label,
        _bucket_sort_key,
        _city_date_from_slug,
        _parse_json_list,
        _resolution_dt,
        market_row,
    )
    from automata.parser import _extract_no_token_id, _extract_yes_token_id

    slug = str(event.get("slug") or "")
    if not slug:
        return None
    city, event_date, _ = _city_date_from_slug(slug)
    if not city or not event_date:
        return None

    markets = _parse_json_list(event.get("markets")) or []
    pairs: list[tuple[dict, dict]] = [
        (m, market_row(m)) for m in markets if isinstance(m, dict)
    ]
    if not pairs:
        return None

    fav_pair = max(
        ((m, r) for m, r in pairs if r["yes_bid"] is not None),
        key=lambda mr: mr[1]["yes_bid"],
        default=None,
    )
    if fav_pair is None:
        return None
    fav_temp = _bucket_sort_key(fav_pair[1]["question"])

    # Bucket ladder: unique sort_keys across all pairs (including closed
    # ones) sorted ascending. Index in this list is the bucket's position
    # on the ladder, and the difference of indices is the bucket distance.
    ladder = sorted({_bucket_sort_key(r["question"]) for _, r in pairs})
    try:
        fav_index = ladder.index(fav_temp)
    except ValueError:
        return None

    qualifying: list[tuple[dict, dict]] = []
    for m, r in pairs:
        if r["closed"]:
            continue
        if r["no_ask"] is None or r["no_ask"] < min_no_ask or r["no_ask"] > max_no_ask:
            continue
        # Exit-liquidity guard: no_bid is derived as 1 - yes_ask. If yes_ask is
        # None, no_bid is None — meaning no one is bidding to BUY our NO at any
        # price, so we'd have no exit if forecast shifts. Reject the bucket.
        if r["no_bid"] is None or r["no_bid"] < min_no_bid:
            continue
        if _bucket_sort_key(r["question"]) >= fav_temp:
            continue
        qualifying.append((m, r))
    if not qualifying:
        return None
    qualifying.sort(key=lambda mr: _bucket_sort_key(mr[1]["question"]))
    m, r = qualifying[0]

    no_token_id = _extract_no_token_id(m)
    if not no_token_id:
        return None

    bucket_label = _bucket_label(r["question"])
    unit = "F" if "°F" in bucket_label else "C"
    cand_index = ladder.index(_bucket_sort_key(r["question"]))
    bucket_distance = fav_index - cand_index
    rdt = _resolution_dt(event, city, event_date)

    # Collect every token_id (NO + YES) across every bucket of this event,
    # so dedup can detect "we already hold a different bucket of this event"
    # without consulting the bets DB.
    event_token_ids: set[str] = set()
    for mm, _rr in pairs:
        for tid in (_extract_no_token_id(mm), _extract_yes_token_id(mm)):
            if tid:
                event_token_ids.add(tid)

    return {
        "city": city,
        "event_date": event_date.strftime("%Y-%m-%d"),
        "title_date": event_date.strftime("%m-%d"),
        "slug": slug,
        "url": f"https://polymarket.com/event/{slug}",
        "question": r["question"],
        "bucket_label": bucket_label,
        "unit": unit,
        "no_token_id": no_token_id,
        "yes_token_id": _extract_yes_token_id(m),
        "event_token_ids": event_token_ids,
        "no_ask": r["no_ask"],
        "no_bid": r["no_bid"],
        "fav_label": _bucket_label(fav_pair[1]["question"]),
        "fav_yes_bid": fav_pair[1]["yes_bid"],
        "bucket_distance": bucket_distance,
        "resolution_dt": rdt,
    }


def _render_candidates_table(candidates: list[dict], *, now: datetime | None = None) -> list[str]:
    """Build an aligned table for the candidate list.

    Returns header + rule + one row per candidate; columns auto-size to
    the widest cell so output stays compact regardless of city length.
    """
    if not candidates:
        return []

    now = now or datetime.now(timezone.utc)
    rows: list[dict[str, str]] = []
    for c in candidates:
        bd = c.get("bucket_distance") or 0
        rdt = c.get("resolution_dt")
        fav_yes = c.get("fav_yes_bid")
        hours_to_res = (rdt - now).total_seconds() / 3600 if rdt else None
        rows.append({
            "city":  c["city"],
            "date":  c["title_date"],
            "bkt":   (c["bucket_label"] or "").strip(),
            "ask":   f"{c['no_ask'] * 100:.1f}c",
            "exp":   (c.get("fav_label") or "").strip(),
            "fav":   f"{int(round(fav_yes * 100))}c" if fav_yes is not None else "",
            "dist":  f"-{bd}b",
            "in":    f"{hours_to_res:.1f}h" if hours_to_res is not None else "",
            "res":   rdt.astimezone().strftime("%m-%d %H:%M") if rdt else "?",
        })

    cols: list[tuple[str, str, str]] = [
        ("city",  "city",     "<"),
        ("date",  "date",     "<"),
        ("bkt",   "bucket",   "<"),
        ("ask",   "no",       ">"),
        ("exp",   "exp",      "<"),
        ("fav",   "fav",      ">"),
        ("dist",  "dist",     ">"),
        ("in",    "in",       ">"),
        ("res",   "resolves", "<"),
    ]
    widths = {
        key: max(len(label), max((len(r[key]) for r in rows), default=0))
        for key, label, _ in cols
    }

    out: list[str] = []
    out.append("  ".join(f"{label:<{widths[key]}}" for key, label, _ in cols))
    out.append("  ".join("─" * widths[key] for key, _, _ in cols))
    for r in rows:
        out.append("  ".join(
            f"{r[key]:{align}{widths[key]}}" for key, _, align in cols
        ))
    return out


def _far_future() -> datetime:
    return datetime(9999, 12, 31, tzinfo=timezone.utc)


def _order_open_shares(order: dict[str, Any]) -> float:
    """Best-effort remaining-share count from an open-order payload."""
    for key in ("remaining_size", "size_left", "sizeLeft", "open_size"):
        v = order.get(key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f >= 0:
            return f
    try:
        size = float(order.get("size") or order.get("original_size") or 0)
        filled = float(order.get("matched_size") or order.get("filled_size") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, size - filled)


def _cancel_stale_buy_orders(
    client: Any,
    db_token_city_date: dict[str, tuple[str, str]],
    stale_minutes: float,
) -> tuple[set[str], list[dict]]:
    """Cancel our own BUY orders older than `stale_minutes`. Returns
    (cancelled_order_ids, all_open_orders) — caller filters the second
    by the first when building dedup. Only touches orders on tokens we
    have a DB record for, to avoid stepping on other accounts' orders."""
    import time as _time
    from automata.client import cancel_order, get_all_open_orders

    cancelled: set[str] = set()
    try:
        orders = get_all_open_orders(client)
    except Exception as exc:
        log.warning("[stale-cancel] get_all_open_orders failed: %s", exc)
        return cancelled, []
    if stale_minutes <= 0:
        return cancelled, orders

    cutoff = _time.time() - stale_minutes * 60
    for o in orders:
        if str(o.get("side", "")).upper() != "BUY":
            continue
        if _order_open_shares(o) <= 0:
            continue
        tid = str(o.get("asset_id") or o.get("token_id") or "")
        if not tid or tid not in db_token_city_date:
            continue
        created_at = o.get("created_at")
        if created_at is None:
            continue
        try:
            ts = float(created_at)
        except (TypeError, ValueError):
            continue
        if ts > cutoff:
            continue
        order_id = str(o.get("id") or o.get("orderID") or "")
        if not order_id:
            continue
        age_min = (_time.time() - ts) / 60
        city = db_token_city_date[tid][0]
        try:
            cancel_order(client, order_id)
            cancelled.add(order_id)
            log.info("[stale-cancel] %s on %s (%.1fm old)  id=%s",
                     "BUY", city, age_min, order_id[:14])
        except Exception as exc:
            log.warning("[stale-cancel] %s id=%s failed: %s",
                        city, order_id[:14], exc)
    if cancelled:
        log.info("[stale-cancel] cancelled %d order(s)", len(cancelled))
    return cancelled, orders


def _place_take_profit_orders(
    client: Any,
    positions: list[dict],
    open_orders: list[dict],
    *,
    sell_price: float,
) -> int:
    """Top up GTC limit SELLs at `sell_price` so every position is fully
    covered. For each held token, sum the open SELL shares; if the gap
    to the position size is at or above Polymarket's $1 minimum, place
    one new SELL for that gap. Reuses caller-provided position + order
    snapshots. Returns the count of new sells placed."""
    from automata.client import place_sell_order

    if not positions:
        return 0

    existing_sell_shares: dict[str, float] = {}
    for o in open_orders:
        if str(o.get("side", "")).upper() != "SELL":
            continue
        open_sh = _order_open_shares(o)
        if open_sh <= 0:
            continue
        tid = str(o.get("asset_id") or o.get("token_id") or "")
        if tid:
            existing_sell_shares[tid] = existing_sell_shares.get(tid, 0.0) + open_sh

    # Polymarket min order is $1 — gaps below that can't be placed.
    min_gap_shares = max(1.01, 1.0 / max(sell_price, 0.01))

    placed = 0
    for p in positions:
        tid = p.get("token_id") or ""
        size = float(p.get("size") or 0)
        if not tid or size < 5:
            continue
        covered = existing_sell_shares.get(tid, 0.0)
        gap = round(size - covered, 2)
        sell_size = math.floor(gap * 100) / 100
        if sell_size < min_gap_shares:
            continue
        try:
            resp = place_sell_order(client, tid, sell_price, sell_size)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            log.info(
                "[take-profit] SELL %.2f sh @ %.2fc  token=%s  "
                "(pos=%.2f covered=%.2f gap=%.2f)  → %s id=%s",
                sell_size, sell_price * 100, tid[:14],
                size, covered, gap,
                resp.get("status") or "submitted", order_id[:18],
            )
            placed += 1
        except Exception as exc:
            log.warning("[take-profit] sell %s failed: %s", tid[:14], exc)
    if placed:
        log.info("[take-profit] placed %d new sell order(s) at %.2fc",
                 placed, sell_price * 100)
    return placed


def run(
    *,
    dry_run: bool = True,
    hours: int | None = None,
    min_hours: float | None = None,
    bet_shares: float | None = None,
    max_orders: int = 1,
) -> None:
    from experiment.view import fetch_events_resolving_within

    min_no_ask  = config.get_float("TEMPBUY_MIN_NO_ASK", "temp_buyer", "min_no_ask", 0.97)
    max_no_ask  = config.get_float("TEMPBUY_MAX_NO_ASK", "temp_buyer", "max_no_ask", 0.998)
    min_no_bid  = config.get_float("TEMPBUY_MIN_NO_BID", "temp_buyer", "min_no_bid", 0.0)
    min_bucket_distance = config.get_int(
        "TEMPBUY_MIN_BUCKET_DISTANCE", "temp_buyer", "min_bucket_distance", 2
    )
    cfg_shares  = config.get_float("TEMPBUY_BET_SHARES", "temp_buyer", "bet_shares", 20.0)
    cfg_min_h   = config.get_float("TEMPBUY_MIN_HOURS", "temp_buyer", "min_hours", 4.0)
    cfg_max_h   = config.get_int("TEMPBUY_MAX_HOURS", "temp_buyer", "max_hours", 18)
    cfg_min_bal = config.get_float("TEMPBUY_MIN_BALANCE_USDC", "temp_buyer", "min_balance_usdc", 10.0)
    target_shares = bet_shares if bet_shares is not None else cfg_shares
    floor_hours = min_hours if min_hours is not None else cfg_min_h
    window_hours = hours if hours is not None else cfg_max_h

    # Same blacklist source as the weather bot — single source of truth.
    city_blacklist = set(config.get_list_str(
        "CITY_BLACKLIST", "weather", "city_blacklist",
        ["Seoul", "Taipei", "Lagos", "Denver", "Jakarta"],
    ))

    log.info("Fetching events resolving in %.1f–%dh...", floor_hours, window_hours)
    events = fetch_events_resolving_within(window_hours, min_hours=floor_hours)
    log.info("  %d temperature events fetched", len(events))

    candidates: list[dict] = []
    for ev in events:
        sel = _select_for_event(
            ev, min_no_ask=min_no_ask, max_no_ask=max_no_ask, min_no_bid=min_no_bid,
        )
        if sel is None:
            continue
        if sel["city"] in city_blacklist:
            log.info("  skip (blacklist) %s %s", sel["city"], sel["title_date"])
            continue
        if (sel["bucket_distance"] or 0) < min_bucket_distance:
            log.info(
                "  skip (dist %d < %d) %s %s",
                sel["bucket_distance"] or 0, min_bucket_distance,
                sel["city"], sel["bucket_label"],
            )
            continue
        candidates.append(sel)

    if candidates:
        # Rank: earliest resolution first (don't skip near-term events for a
        # higher-distance pick later), then within the same resolution cluster
        # prefer the bucket farthest below the favorite.
        candidates.sort(key=lambda c: (
            c.get("resolution_dt") or _far_future(),
            -(c.get("bucket_distance") or 0),
        ))
        log.info(
            "  %d eligible candidate(s) (earliest resolution, then highest distance):",
            len(candidates),
        )
        for line in _render_candidates_table(candidates):
            log.info("    %s", line)
    else:
        log.info("  no buy candidates after filters")

    if dry_run:
        log.info("[DRY-RUN] no orders placed (use --bet to send)")
        return
    # Live (--bet) mode falls through even when candidates is empty, so
    # the take-profit pass still runs against existing positions.

    # ── Live: build client, fetch balance, dedup, place order(s) ──────────
    from automata.client import (
        build_client,
        derive_api_credentials,
        get_best_bid_ask,
        get_positions,
        get_usdc_balance,
        place_no_order,
    )
    from automata.db import get_token_city_date_map, init_db, record_bet

    # CLOB creds (CLOB_API_KEY / CLOB_SECRET / CLOB_PASS) are derived on
    # demand from POLYMARKET_PRIVATE_KEY — only the private key + host need
    # to be in .env. Mirrors automata.runner._setup behavior.
    base_required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in base_required if not os.getenv(k)]
    if missing:
        log.error("Missing .env keys for live betting: %s", ", ".join(missing))
        return
    if not (os.getenv("CLOB_API_KEY") and os.getenv("CLOB_SECRET") and os.getenv("CLOB_PASS")):
        try:
            creds = derive_api_credentials(
                host=os.environ["POLYMARKET_HOST"],
                private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                funder=os.getenv("POLYMARKET_FUNDER"),
                signature_type=config.get_int(
                    "POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0,
                ),
            )
            os.environ["CLOB_API_KEY"] = creds.api_key
            os.environ["CLOB_SECRET"] = creds.api_secret
            os.environ["CLOB_PASS"] = creds.api_passphrase
        except Exception as exc:
            log.error("Failed to derive CLOB API credentials: %s", exc)
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

    log.info("Balance $%.2f (target %.2f sh, floor $%.2f)",
             balance, target_shares, cfg_min_bal)

    # ── Stale-cancel pass: free USDC tied up in old unfilled BUY orders ───
    init_db()  # still record bets for analytics, even though dedup no longer reads it
    db_token_city_date = get_token_city_date_map()
    stale_minutes = config.get_float("TEMPBUY_STALE_MINUTES", "temp_buyer", "stale_minutes", 10.0)
    cancelled_order_ids, all_open_orders = _cancel_stale_buy_orders(
        client, db_token_city_date, stale_minutes,
    )
    # Refresh balance — cancellations release reserved USDC.
    if cancelled_order_ids:
        try:
            balance = get_usdc_balance(client)
            log.info("[stale-cancel] balance after cancels: $%.2f", balance)
        except Exception as exc:
            log.warning("[stale-cancel] post-cancel balance refresh failed: %s", exc)

    # ── Live dedup: positions + open orders only, no DB. We bail if the
    #    positions API fails — better to skip a run than risk a duplicate.
    if not funder:
        log.error("POLYMARKET_FUNDER not set — cannot dedup against live positions")
        return
    try:
        live_positions = get_positions(funder)
    except Exception as exc:
        log.error("get_positions failed (%s) — aborting to avoid duplicate bets", exc)
        return
    held_token_ids: set[str] = {p["token_id"] for p in live_positions if p.get("token_id")}
    for o in all_open_orders:
        order_id = str(o.get("id") or o.get("orderID") or "")
        if order_id in cancelled_order_ids:
            continue
        if str(o.get("side", "")).upper() != "BUY":
            continue
        tid = str(o.get("asset_id") or o.get("token_id") or "")
        if tid:
            held_token_ids.add(tid)
    log.info("[dedup] %d held token(s), %d open buy order(s) (post stale-cancel)",
             len(live_positions), sum(
                 1 for o in all_open_orders
                 if str(o.get("side", "")).upper() == "BUY"
                 and str(o.get("id") or o.get("orderID") or "") not in cancelled_order_ids
             ))

    # ── Take-profit pass: 99.9¢ GTC sell on every position lacking one ──
    take_profit_price = config.get_float(
        "TEMPBUY_TAKE_PROFIT_PRICE", "temp_buyer", "take_profit_price", 0.999,
    )
    active_open_orders = [
        o for o in all_open_orders
        if str(o.get("id") or o.get("orderID") or "") not in cancelled_order_ids
    ]
    _place_take_profit_orders(
        client, live_positions, active_open_orders,
        sell_price=take_profit_price,
    )

    if balance < cfg_min_bal:
        log.info("Balance $%.2f below $%.2f floor — skipping buy pass "
                 "(take-profit completed)", balance, cfg_min_bal)
        return
    if not candidates:
        log.info("[temp-buyer] done — take-profit pass only (no buy candidates)")
        return

    placed = 0
    for c in candidates:
        if placed >= max_orders:
            break

        # Event-level dedup: skip if we already hold (or have an open buy on)
        # ANY bucket of this event — NO or YES, not just the one we'd bet now.
        overlap = c["event_token_ids"] & held_token_ids
        if overlap:
            log.info("  already in event %s %s (token %s held) — skipping",
                     c["city"], c["event_date"], next(iter(overlap))[:14])
            continue

        # Re-pull live book; quote at the tighter of book ask and our cap.
        host = os.environ["POLYMARKET_HOST"]
        live_bid, live_ask = get_best_bid_ask(host, c["no_token_id"])
        if live_ask is None:
            log.info("  no live ask  %s %s — skipping", c["city"], c["bucket_label"])
            continue
        if live_ask < min_no_ask:
            log.info("  ask dropped to %.2fc < %.2fc  %s — skipping",
                     live_ask * 100, min_no_ask * 100, c["city"])
            continue
        price = round(min(live_ask, max_no_ask), 4)

        shares = math.floor(target_shares * 100) / 100
        cost = round(shares * price, 2)
        if balance < cost:
            if balance < 5.0:
                log.info("  balance $%.2f < $5 floor — skipping %s %s",
                         balance, c["city"], c["bucket_label"])
                continue
            affordable_shares = math.floor((balance / price) * 100) / 100
            if affordable_shares <= 0:
                continue
            shares = affordable_shares
            cost = round(shares * price, 2)
            log.info("  partial size %.2f sh @ %.3f ($%.2f) — using available balance $%.2f",
                     shares, price, cost, balance)

        try:
            resp = place_no_order(client, c["no_token_id"], price, shares, post_only=False)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            status = resp.get("status") or "submitted"
            log.info(
                "[temp-buyer] %s %s  BUY No @ %.2fc  %.2f sh ($%.2f)  → %s id=%s",
                c["city"], c["bucket_label"], price * 100, shares, cost, status, order_id[:18],
            )
            balance = round(balance - cost, 2)
            placed += 1
            # Mark the whole event as held so a --max-orders > 1 run doesn't
            # double-tap a sibling bucket we just bought into.
            held_token_ids |= c["event_token_ids"]

            try:
                record_bet(
                    city=c["city"],
                    icao=None,
                    event_date=c["event_date"],
                    question=c["question"],
                    option="No",
                    token_id=c["no_token_id"],
                    order_id=order_id,
                    shares=shares,
                    no_price=price,
                    yes_price=c.get("fav_yes_bid"),
                    cost_usdc=cost,
                    unit=c["unit"],
                    threshold=None,
                    threshold_hi=None,
                    direction=None,
                    forecast_high=None,
                )
            except Exception as exc:
                log.warning("record_bet failed for %s %s: %s",
                            c["city"], c["bucket_label"], exc)
        except Exception as exc:
            log.error("[temp-buyer] order failed for %s %s: %s",
                      c["city"], c["bucket_label"], exc)

    log.info("  done — %d order(s) placed, $%.2f balance remaining", placed, balance)


def main() -> int:
    import time as _time

    p = argparse.ArgumentParser(
        description="Auto NO-buyer for top-of-list Polymarket temperature markets",
    )
    p.add_argument("--bet", "--live", dest="bet", action="store_true",
                   help="Place orders (default: dry run). --live is an alias.")
    p.add_argument("--within", type=int, default=None,
                   help="Resolution window in hours (default: [temp_buyer].max_hours = 18)")
    p.add_argument("--min-hours", type=float, default=None,
                   help="Drop events resolving sooner than this many hours from now "
                        "(default: [temp_buyer].min_hours = 4)")
    p.add_argument("--shares", type=float, default=None,
                   help="Shares per bet (default: [temp_buyer].bet_shares = 20)")
    p.add_argument("--max-orders", type=int, default=1,
                   help="How many bets to place this run (default 1, top-of-list)")
    p.add_argument("--stale-minutes", type=float, default=None,
                   help="Cancel our own BUY orders older than this many minutes "
                        "before placing new ones (default: [temp_buyer].stale_minutes = 10). "
                        "Set 0 to disable.")
    p.add_argument("--interval", type=int, default=300,
                   help="Loop interval in seconds (default 300 = 5 min). Use --once to disable looping.")
    p.add_argument("--once", action="store_true",
                   help="Run a single cycle and exit (default: loop forever)")
    args = p.parse_args()
    if args.stale_minutes is not None:
        os.environ["TEMPBUY_STALE_MINUTES"] = str(args.stale_minutes)

    logger = _setup_logging()
    interval = max(15, args.interval)

    def _cycle() -> None:
        run(
            dry_run=not args.bet,
            hours=args.within,
            min_hours=args.min_hours,
            bet_shares=args.shares,
            max_orders=max(1, args.max_orders),
        )

    if args.once:
        _cycle()
        return 0

    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info("── [temp-buyer] cycle %d ──", iteration)
            try:
                _cycle()
            except Exception as exc:
                logger.exception("[temp-buyer] cycle %d failed: %s — continuing", iteration, exc)
            logger.info("[temp-buyer] sleeping %ds before next cycle", interval)
            _time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("[temp-buyer] interrupted — exiting after cycle %d", iteration)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
