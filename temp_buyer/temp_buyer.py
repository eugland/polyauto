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
TEMPBUY_BET_SHARES (default 30 sh). Skipped if free USDC balance is
below the resulting cost.

By default we only consider events resolving 4–18 hours from now —
sub-4h events are skipped because they're too close to resolution to
leave reaction room, and >18h events are skipped because the YES
favorite hasn't settled yet (the picker would just chase noise).

Candidates are sorted by closest resolution first, then displayed as
an aligned table for quick scanning.

After each live cycle, scans funder positions and redeems any whose
condition has resolved on-chain (relayer mode by default). Held to
resolution = no take-profit sell needed; this closes the loop. Disable
with --no-redeem, or switch to direct onchain mode with
--redeem-mode onchain --polygon-rpc <url>.

Usage:
  python -m temp_buyer.temp_buyer                   # dry-run, top candidate
  python -m temp_buyer.temp_buyer --bet             # live: place the order + redeem resolved
  python -m temp_buyer.temp_buyer --within 24       # 24h upper bound
  python -m temp_buyer.temp_buyer --min-hours 8     # require >=8h to resolution
  python -m temp_buyer.temp_buyer --bet --shares 30
  python -m temp_buyer.temp_buyer --bet --no-redeem # live, but skip redeem scan
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from temp_buyer import config

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
            fh = logging.handlers.RotatingFileHandler(
                path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            fh.setLevel(logging.INFO)
            root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("temp_buyer.temp_buyer")


log = logging.getLogger("temp_buyer.temp_buyer")


def _select_for_event(
    event: dict, *, min_no_ask: float, max_no_ask: float, min_no_bid: float = 0.0,
    held_no_token_id: str | None = None,
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

    When `held_no_token_id` is provided, return the candidate dict for
    that exact NO token (used by the top-up pass) instead of the
    lowest-temp pick. The same quality gates apply; if the held bucket
    fails any gate, returns None.
    """
    from temp_buyer.view import (
        _bucket_label,
        _bucket_sort_key,
        _city_date_from_slug,
        _parse_json_list,
        _resolution_dt,
        market_row,
    )
    from temp_buyer.parser import _extract_no_token_id, _extract_yes_token_id

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

    def _passes_gates(r: dict) -> bool:
        if r["closed"]:
            return False
        if r["no_ask"] is None or r["no_ask"] < min_no_ask or r["no_ask"] > max_no_ask:
            return False
        # Exit-liquidity guard: no_bid is derived as 1 - yes_ask. If yes_ask is
        # None, no_bid is None — meaning no one is bidding to BUY our NO at any
        # price, so we'd have no exit if forecast shifts. Reject the bucket.
        if r["no_bid"] is None or r["no_bid"] < min_no_bid:
            return False
        if _bucket_sort_key(r["question"]) >= fav_temp:
            return False
        return True

    if held_no_token_id is not None:
        # Top-up path: locate the held bucket and apply the same gates.
        match = None
        for m, r in pairs:
            if _extract_no_token_id(m) == held_no_token_id:
                match = (m, r)
                break
        if match is None:
            return None
        m, r = match
        if not _passes_gates(r):
            return None
    else:
        qualifying: list[tuple[dict, dict]] = [(m, r) for m, r in pairs if _passes_gates(r)]
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
        "volume": r.get("volume"),
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
        score = c.get("score")
        vol = c.get("volume")
        conv = c.get("bid_pct_above")
        p25 = c.get("bid_p25")
        p50 = c.get("bid_p50")
        p75 = c.get("bid_p75")
        rows.append({
            "city":  c["city"],
            "date":  c["title_date"],
            "bkt":   (c["bucket_label"] or "").strip(),
            "ask":   f"{c['no_ask'] * 100:.1f}c",
            "exp":   (c.get("fav_label") or "").strip(),
            "fav":   f"{int(round(fav_yes * 100))}c" if fav_yes is not None else "",
            "dist":  f"-{bd}b",
            "vol":   f"{vol:.0f}" if vol is not None else "",
            "conv":  f"{conv:.0f}%" if conv is not None else "",
            "p25":   f"{p25 * 100:.0f}c" if p25 is not None else "",
            "p50":   f"{p50 * 100:.0f}c" if p50 is not None else "",
            "p75":   f"{p75 * 100:.0f}c" if p75 is not None else "",
            "in":    f"{hours_to_res:.1f}h" if hours_to_res is not None else "",
            "score": f"{score:.1f}" if score is not None else "",
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
        ("vol",   "vol",      ">"),
        ("conv",  "conv",     ">"),
        ("p25",   "p25",      ">"),
        ("p50",   "p50",      ">"),
        ("p75",   "p75",      ">"),
        ("in",    "in",       ">"),
        ("score", "score",    ">"),
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


def _place_resting_sells(
    client: Any,
    *,
    live_positions: list[dict],
    open_orders: list[dict],
    sell_price: float = 0.999,
    dry_run: bool = False,
) -> None:
    """For every held NO position, ensure a GTC sell order at sell_price exists
    for the full position size. Cancels + replaces any existing sell whose size
    differs. Silently skips if the market's tick size doesn't support the price
    (precision error from the API)."""
    from temp_buyer.client import cancel_order, place_sell_order

    sell_orders_by_token: dict[str, list[dict]] = {}
    for o in open_orders:
        if str(o.get("side", "")).upper() != "SELL":
            continue
        tid = str(o.get("asset_id") or o.get("token_id") or "")
        if tid:
            sell_orders_by_token.setdefault(tid, []).append(o)

    no_positions = [
        p for p in live_positions
        if str(p.get("outcome", "")).lower() == "no"
        and float(p.get("size") or 0) > 0
    ]
    if not no_positions:
        log.info("[resting-sell] no held NO positions")
        return

    for pos in no_positions:
        tid = str(pos["token_id"])
        target_size = float(pos["size"])
        existing_sells = sell_orders_by_token.get(tid, [])
        open_sell_sh = round(sum(_order_open_shares(o) for o in existing_sells), 2)

        if abs(open_sell_sh - target_size) < 0.01:
            log.info("[resting-sell] %s  sell %.2f sh already resting — skip", tid[:14], open_sell_sh)
            continue

        # Cancel any mismatched sell orders on this token
        for o in existing_sells:
            order_id = str(o.get("id") or o.get("orderID") or "")
            if not order_id:
                continue
            if dry_run:
                log.info("[resting-sell] WOULD CANCEL sell %s (%.2f sh)", order_id[:14], _order_open_shares(o))
                continue
            try:
                cancel_order(client, order_id)
                log.info("[resting-sell] cancelled sell %s (%.2f sh)", order_id[:14], _order_open_shares(o))
            except Exception as exc:
                log.warning("[resting-sell] cancel failed %s: %s", order_id[:14], exc)

        if dry_run:
            log.info("[resting-sell] WOULD SELL %s  No @ %.1fc  %.2f sh", tid[:14], sell_price * 100, target_size)
            continue

        try:
            resp = place_sell_order(client, tid, sell_price, target_size)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            status = resp.get("status") or "submitted"
            log.info("[resting-sell] SELL %s  No @ %.1fc  %.2f sh → %s id=%s",
                     tid[:14], sell_price * 100, target_size, status, order_id[:18])
        except Exception as exc:
            err = str(exc).lower()
            if any(kw in err for kw in ("precision", "tick", "increment", "decimal", "invalid price", "price level")):
                log.info("[resting-sell] skip %s — %.3f not a valid tick (precision): %s",
                         tid[:14], sell_price, exc)
            else:
                log.warning("[resting-sell] sell failed %s: %s", tid[:14], exc)


def _cancel_stale_buy_orders(
    client: Any,
    db_token_city_date: dict[str, tuple[str, str]],
    stale_minutes: float,
    *,
    dry_run: bool = False,
) -> tuple[set[str], list[dict]]:
    """Cancel our own BUY orders older than `stale_minutes`. Returns
    (cancelled_order_ids, all_open_orders) — caller filters the second
    by the first when building dedup. Only touches orders on tokens we
    have a DB record for, to avoid stepping on other accounts' orders.
    When dry_run=True, logs `WOULD CANCEL` and returns the IDs in
    `cancelled` so downstream dedup matches a live run, but never
    actually cancels."""
    import time as _time
    from temp_buyer.client import cancel_order, get_all_open_orders

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
        if dry_run:
            cancelled.add(order_id)
            log.info("[stale-cancel] WOULD CANCEL BUY on %s (%.1fm old)  id=%s",
                     city, age_min, order_id[:14])
            continue
        try:
            cancel_order(client, order_id)
            cancelled.add(order_id)
            log.info("[stale-cancel] %s on %s (%.1fm old)  id=%s",
                     "BUY", city, age_min, order_id[:14])
        except Exception as exc:
            log.warning("[stale-cancel] %s id=%s failed: %s",
                        city, order_id[:14], exc)
    if cancelled:
        verb = "would cancel" if dry_run else "cancelled"
        log.info("[stale-cancel] %s %d order(s)", verb, len(cancelled))
    return cancelled, orders


def _open_buy_shares_by_token(
    open_orders: list[dict], cancelled_order_ids: set[str],
) -> dict[str, float]:
    """Sum live (non-cancelled) BUY shares per token_id. Used by the
    top-up pass so we don't double-up on a token already covered by an
    open buy order."""
    out: dict[str, float] = {}
    for o in open_orders:
        order_id = str(o.get("id") or o.get("orderID") or "")
        if order_id in cancelled_order_ids:
            continue
        if str(o.get("side", "")).upper() != "BUY":
            continue
        tid = str(o.get("asset_id") or o.get("token_id") or "")
        if not tid:
            continue
        out[tid] = out.get(tid, 0.0) + _order_open_shares(o)
    return out


def _run_topup_pass(
    client: Any,
    *,
    host: str,
    live_positions: list[dict],
    open_orders: list[dict],
    cancelled_order_ids: set[str],
    target_shares: float,
    min_no_ask: float,
    max_no_ask: float,
    min_no_bid: float,
    min_bucket_distance: int,
    city_blacklist: set[str],
    min_topup_shares: float,
    min_ask_size: float,
    min_bid_pct_above: float,
    min_bid_p75: float,
    balance: float,
    min_balance: float,
    max_orders_remaining: int,
    record_bet_fn: Any,
    dry_run: bool = False,
) -> tuple[int, float]:
    """Top up held NO positions to `target_shares`. Skips the time
    window check (held positions were validated at original entry) but
    applies every other gate the new-entry buyer logic uses: ask band,
    bucket distance, blacklist, NO bid floor, book-conviction
    (min_bid_pct_above) and book-shape (min_bid_p75) — so a position is
    topped up only when a fresh buy on it would also pass. Returns
    (orders_placed, updated_balance).
    Spending is capped at (balance - min_balance) so the configured
    USDC reserve is never breached. When dry_run=True, logs `WOULD BUY`
    lines and decrements `balance` as if orders were placed, but never
    calls place_no_order or record_bet_fn."""
    from temp_buyer.view import _parse_json_list, fetch_events_resolving_within
    from temp_buyer.client import get_book_depth, place_no_order
    from temp_buyer.parser import _extract_no_token_id

    if max_orders_remaining <= 0:
        log.info("[top-up] max_orders cap already consumed — skip")
        return 0, balance
    if balance <= min_balance:
        log.info("[top-up] balance $%.2f ≤ reserve $%.2f — skip top-up pass",
                 balance, min_balance)
        return 0, balance

    held_no = [
        p for p in live_positions
        if str(p.get("outcome", "")).lower() == "no"
        and (p.get("token_id") or "")
        and float(p.get("size") or 0) > 0
    ]
    if not held_no:
        log.info("[top-up] no held NO positions")
        return 0, balance

    # Wide window so we don't drop events that drifted past the normal
    # 4–18h band. The time gate is irrelevant for top-ups.
    try:
        events = fetch_events_resolving_within(72, min_hours=0.0)
    except Exception as exc:
        log.warning("[top-up] event fetch failed: %s — skipping top-up pass", exc)
        return 0, balance

    event_by_no_token: dict[str, dict] = {}
    for ev in events:
        for m in _parse_json_list(ev.get("markets")) or []:
            if not isinstance(m, dict):
                continue
            tid = _extract_no_token_id(m)
            if tid:
                event_by_no_token[tid] = ev

    open_buy_shares = _open_buy_shares_by_token(open_orders, cancelled_order_ids)

    placed = 0
    for pos in held_no:
        if placed >= max_orders_remaining:
            break
        tid = str(pos["token_id"])
        held_sh = float(pos["size"])
        open_sh = open_buy_shares.get(tid, 0.0)
        gap = round(target_shares - held_sh - open_sh, 2)

        if gap < min_topup_shares:
            log.info(
                "[top-up] gap %.2f sh < %.2f  token=%s held=%.2f open=%.2f target=%.2f — skip",
                gap, min_topup_shares, tid[:14], held_sh, open_sh, target_shares,
            )
            continue

        ev = event_by_no_token.get(tid)
        if ev is None:
            log.info(
                "[top-up] held token %s not found in any current event — skip",
                tid[:14],
            )
            continue

        sel = _select_for_event(
            ev, min_no_ask=min_no_ask, max_no_ask=max_no_ask,
            min_no_bid=min_no_bid, held_no_token_id=tid,
        )
        if sel is None:
            log.info(
                "[top-up] %s rejected gates (band/closed/no_bid/below-fav) — skip",
                tid[:14],
            )
            continue
        if sel["city"] in city_blacklist:
            log.info("[top-up] %s blacklisted city — skip", sel["city"])
            continue
        if (sel["bucket_distance"] or 0) < min_bucket_distance:
            log.info(
                "[top-up] dist %d < %d  %s %s — skip",
                sel["bucket_distance"] or 0, min_bucket_distance,
                sel["city"], sel["bucket_label"],
            )
            continue

        book = get_book_depth(host, tid)
        live_ask = book["ask"]
        ask_size = book["ask_size"]
        bid_pct = book.get("bid_pct_above")
        log.info(
            "[top-up] book %s %s  ask=%.2fc  ask_size=%.0f sh  bid≥95%%=%.0f%%",
            sel["city"], sel["bucket_label"], live_ask * 100 if live_ask else 0,
            ask_size or 0, bid_pct if bid_pct is not None else 0,
        )
        if live_ask is None:
            log.info("[top-up] no live ask  %s %s — skip", sel["city"], sel["bucket_label"])
            continue
        if live_ask < min_no_ask or live_ask > max_no_ask:
            log.info(
                "[top-up] ask %.2fc out of band  %s — skip",
                live_ask * 100, sel["city"],
            )
            continue
        if min_ask_size > 0 and (ask_size or 0) < min_ask_size:
            log.info("[top-up] ask_size %.0f sh < %.0f minimum  %s — skip (thin book)",
                     ask_size or 0, min_ask_size, sel["city"])
            continue
        # Same book-conviction / book-shape gates the new-entry candidate
        # loop applies, so a held position is only topped up while its book
        # would still qualify for a fresh buy (not a collapsing book).
        if bid_pct is None or bid_pct < min_bid_pct_above:
            log.info(
                "[top-up] conv %.0f%% < %.0f%%  %s %s — skip (too little support near ask)",
                bid_pct if bid_pct is not None else 0, min_bid_pct_above,
                sel["city"], sel["bucket_label"],
            )
            continue
        p75 = book.get("bid_p75")
        if p75 is None or p75 < min_bid_p75:
            log.info(
                "[top-up] p75 %.0fc < %.0fc  %s %s — skip (bid book collapses below top quartile)",
                (p75 or 0) * 100, min_bid_p75 * 100,
                sel["city"], sel["bucket_label"],
            )
            continue
        price = round(min(live_ask, max_no_ask), 4)

        shares = math.floor(gap * 100) / 100
        cost = round(shares * price, 2)
        # Floor spendable so we never overshoot actual on-wire balance (the
        # CLOB rejects orders whose raw micro-USDC amount exceeds raw balance,
        # and `round(..., 2)` happily rounds 62.507 → 62.51, manufacturing
        # phantom $0.003 of spend). Keep a $0.01 epsilon to absorb the gap
        # between our display price (4dp) and the on-wire price the CLOB
        # actually charges, which can include sub-tick precision and fees.
        spendable = max(0.0, math.floor((balance - min_balance - 0.01) * 100) / 100)
        if cost > spendable:
            if spendable <= 0:
                log.info("[top-up] balance $%.2f at/below reserve $%.2f — abort top-up pass",
                         balance, min_balance)
                break
            affordable = math.floor((spendable / price) * 100) / 100
            if affordable < min_topup_shares:
                log.info(
                    "[top-up] affordable %.2f sh < %.2f (reserve $%.2f) %s — skip",
                    affordable, min_topup_shares, min_balance, sel["city"],
                )
                continue
            shares = affordable
            cost = round(shares * price, 2)
            log.info(
                "[top-up] partial %.2f sh @ %.3f ($%.2f) — keeping $%.2f reserve "
                "(balance $%.2f → $%.2f)",
                shares, price, cost, min_balance, balance, round(balance - cost, 2),
            )

        log.info(
            "[top-up] queued %s %s  +%.2f sh @ %.2fc ($%.2f)  "
            "(held=%.2f open=%.2f gap=%.2f target=%.2f)",
            sel["city"], sel["bucket_label"], shares, price * 100, cost,
            held_sh, open_sh, gap, target_shares,
        )

        if dry_run:
            log.info(
                "[top-up] WOULD BUY %s %s  No @ %.2fc  %.2f sh ($%.2f)",
                sel["city"], sel["bucket_label"], price * 100, shares, cost,
            )
            balance = round(balance - cost, 2)
            placed += 1
            continue

        try:
            resp = place_no_order(client, tid, price, shares, post_only=False)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            status = resp.get("status") or "submitted"
            log.info(
                "[top-up] %s %s  BUY No @ %.2fc  %.2f sh ($%.2f)  → %s id=%s",
                sel["city"], sel["bucket_label"], price * 100, shares, cost,
                status, order_id[:18],
            )
            balance = round(balance - cost, 2)
            placed += 1
            try:
                record_bet_fn(
                    city=sel["city"],
                    icao=None,
                    event_date=sel["event_date"],
                    question=sel["question"],
                    option="No",
                    token_id=tid,
                    order_id=order_id,
                    shares=shares,
                    no_price=price,
                    yes_price=sel.get("fav_yes_bid"),
                    cost_usdc=cost,
                    unit=sel["unit"],
                    threshold=None,
                    threshold_hi=None,
                    direction=None,
                    forecast_high=None,
                )
            except Exception as exc:
                log.warning("[top-up] record_bet failed: %s", exc)
        except Exception as exc:
            log.error(
                "[top-up] order failed for %s %s: %s",
                sel["city"], sel["bucket_label"], exc,
            )

    if placed:
        verb = "would place" if dry_run else "placed"
        log.info(
            "[top-up] %s %d top-up order(s), balance now $%.2f",
            verb, placed, balance,
        )
    else:
        log.info("[top-up] no top-ups placed")
    return placed, balance


def run(
    *,
    dry_run: bool = True,
    hours: int | None = None,
    min_hours: float | None = None,
    bet_shares: float | None = None,
    max_orders: int = 1,
) -> None:
    from temp_buyer.view import fetch_events_resolving_within

    min_no_ask  = config.get_float("TEMPBUY_MIN_NO_ASK", "temp_buyer", "min_no_ask", 0.97)
    max_no_ask  = config.get_float("TEMPBUY_MAX_NO_ASK", "temp_buyer", "max_no_ask", 0.998)
    min_no_bid  = config.get_float("TEMPBUY_MIN_NO_BID", "temp_buyer", "min_no_bid", 0.0)
    min_bucket_distance = config.get_int(
        "TEMPBUY_MIN_BUCKET_DISTANCE", "temp_buyer", "min_bucket_distance", 2
    )
    min_ask_size = config.get_float(
        "TEMPBUY_MIN_ASK_SIZE", "temp_buyer", "min_ask_size", 20.0,
    )
    cfg_shares  = config.get_float("TEMPBUY_BET_SHARES", "temp_buyer", "bet_shares", 60.0)
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

    log.info("Target shares per position: %.2f (top-ups gap up to this)", target_shares)
    log.info("Fetching events resolving in %.1f–%dh...", floor_hours, window_hours)
    events = fetch_events_resolving_within(window_hours, min_hours=floor_hours)
    log.info("  %d temperature events fetched", len(events))

    # Shadow-record every fetched event into the weather_model DB so the
    # bias/sigma EMAs stay warm and the /weather-model UI has data to plot
    # — completely independent of temp_buyer's buy logic.
    try:
        from temp_buyer import weather_scan
        weather_scan.scan_and_record_events(events, log_label="weather-model")
    except Exception as exc:
        log.warning("[weather-model] scan_and_record_events failed: %s", exc)

    # Two complementary book-shape floors:
    #  - min_bid_pct_above: % of real bid size at ≥95c. Captures "exit liquidity
    #    at near-full value." Cheap, single-point metric.
    #  - min_bid_p75: top-75% conviction price (within bids ≥10c). Captures the
    #    *shape* of the book — does the bottom quartile of real bidders still
    #    pay >= this much? Catches collapsing books that the binary 95c cut
    #    misses. Calibrated 2026-05-20: 0.70c is the natural break between
    #    weak-shape (57-60c) and acceptable-shape (72-97c) markets.
    min_bid_pct_above = config.get_float(
        "TEMPBUY_MIN_BID_PCT_ABOVE", "temp_buyer", "min_bid_pct_above", 4.0,
    )
    min_bid_p75 = config.get_float(
        "TEMPBUY_MIN_BID_P75", "temp_buyer", "min_bid_p75", 0.69,
    )
    from temp_buyer.client import get_book_depth as _get_book_depth_for_rank
    _book_host = os.environ.get("POLYMARKET_HOST", "")

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
        try:
            book = _get_book_depth_for_rank(_book_host, sel["no_token_id"])
        except Exception as exc:
            log.info("  skip (book err) %s %s — %s", sel["city"], sel["bucket_label"], exc)
            continue
        bid_pct = book.get("bid_pct_above")
        sel["bid_pct_above"] = bid_pct
        sel["bid_p25"] = book.get("bid_p25")
        sel["bid_p50"] = book.get("bid_p50")
        sel["bid_p75"] = book.get("bid_p75")
        sel["ask_size_book"] = book.get("ask_size")
        if bid_pct is None or bid_pct < min_bid_pct_above:
            log.info(
                "  skip (conv %.0f%% < %.0f%%) %s %s — too little support near ask",
                bid_pct if bid_pct is not None else 0, min_bid_pct_above,
                sel["city"], sel["bucket_label"],
            )
            continue
        p75 = sel.get("bid_p75")
        if p75 is None or p75 < min_bid_p75:
            log.info(
                "  skip (p75 %.0fc < %.0fc) %s %s — bid book collapses below the top quartile",
                (p75 or 0) * 100, min_bid_p75 * 100,
                sel["city"], sel["bucket_label"],
            )
            continue
        candidates.append(sel)

    if candidates:
        # Score = dist*w_d − hours*w_h + log(1+volume)*w_v
        # Defaults: w_d=3, w_h=1, w_v=1
        #   - bucket distance: more steps below favorite = stronger NO conviction
        #   - hours: prefer closer resolution (less time for surprise reversal)
        #   - volume: more traded shares = more market participants agree
        # Tune via [temp_buyer].score_bucket_weight / score_hours_weight / score_volume_weight
        score_bucket_w = config.get_float(
            "TEMPBUY_SCORE_BUCKET_WEIGHT", "temp_buyer", "score_bucket_weight", 3.0,
        )
        score_hours_w = config.get_float(
            "TEMPBUY_SCORE_HOURS_WEIGHT", "temp_buyer", "score_hours_weight", 1.0,
        )
        score_volume_w = config.get_float(
            "TEMPBUY_SCORE_VOLUME_WEIGHT", "temp_buyer", "score_volume_weight", 1.0,
        )
        score_conv_w = config.get_float(
            "TEMPBUY_SCORE_CONVICTION_WEIGHT", "temp_buyer", "score_conviction_weight", 0.1,
        )
        # p75 ranges ~0.70–0.97 above the floor → 0.27 dollar units of spread.
        # Weight 8.0 → ~2 pts score spread, comparable to dist (1pt × 3–7 = 4pt).
        score_p75_w = config.get_float(
            "TEMPBUY_SCORE_P75_WEIGHT", "temp_buyer", "score_p75_weight", 8.0,
        )
        now = datetime.now(timezone.utc)
        for c in candidates:
            rdt = c.get("resolution_dt")
            hours = (rdt - now).total_seconds() / 3600 if rdt else 1e6
            vol_score = math.log1p(c.get("volume") or 0) * score_volume_w
            conv_score = (c.get("bid_pct_above") or 0) * score_conv_w
            p75_score = (c.get("bid_p75") or 0) * score_p75_w
            c["score"] = (
                (c.get("bucket_distance") or 0) * score_bucket_w
                - hours * score_hours_w
                + vol_score
                + conv_score
                + p75_score
            )
        candidates.sort(key=lambda c: c["score"], reverse=True)
        log.info(
            "  %d eligible candidate(s) (score = dist*%.1f − hours*%.1f + log(vol)*%.1f + conv%%*%.2f + p75$*%.1f):",
            len(candidates), score_bucket_w, score_hours_w, score_volume_w, score_conv_w, score_p75_w,
        )
        for line in _render_candidates_table(candidates):
            log.info("    %s", line)
    else:
        log.info("  no buy candidates after filters")

    if dry_run:
        log.info("[DRY-RUN] previewing live actions — no orders will be placed")
    # Live and dry-run both fall through: dry-run logs what live would do.

    # ── Live: build client, fetch balance, dedup, place order(s) ──────────
    from temp_buyer.client import (
        build_client,
        derive_api_credentials,
        get_best_bid_ask,
        get_book_depth,
        get_positions,
        get_usdc_balance,
        place_no_order,
    )
    from temp_buyer.db import get_token_city_date_map, init_db, record_bet

    # CLOB creds (CLOB_API_KEY / CLOB_SECRET / CLOB_PASS) are derived on
    # demand from POLYMARKET_PRIVATE_KEY — only the private key + host need
    # to be in .env. Mirrors temp_buyer.runner._setup behavior.
    base_required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in base_required if not os.getenv(k)]
    if missing:
        verb = "preview" if dry_run else "live betting"
        log.error("Missing .env keys for %s: %s", verb, ", ".join(missing))
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
        client, db_token_city_date, stale_minutes, dry_run=dry_run,
    )
    # Refresh balance — cancellations release reserved USDC. Skip in
    # dry-run (no real cancels happened, so balance hasn't changed).
    if cancelled_order_ids and not dry_run:
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

    # ── Top-up pass: bring held NO positions up to target_shares ─────────
    min_topup_shares = config.get_float(
        "TEMPBUY_MIN_TOPUP_SHARES", "temp_buyer", "min_topup_shares", 5.0,
    )
    topup_placed, balance = _run_topup_pass(
        client,
        host=os.environ["POLYMARKET_HOST"],
        live_positions=live_positions,
        open_orders=all_open_orders,
        cancelled_order_ids=cancelled_order_ids,
        target_shares=target_shares,
        min_no_ask=min_no_ask,
        max_no_ask=max_no_ask,
        min_no_bid=min_no_bid,
        min_bucket_distance=min_bucket_distance,
        city_blacklist=city_blacklist,
        min_topup_shares=min_topup_shares,
        min_ask_size=min_ask_size,
        min_bid_pct_above=min_bid_pct_above,
        min_bid_p75=min_bid_p75,
        balance=balance,
        min_balance=cfg_min_bal,
        max_orders_remaining=max_orders,
        record_bet_fn=record_bet,
        dry_run=dry_run,
    )

    if balance <= cfg_min_bal:
        log.info("Balance $%.2f at/below $%.2f reserve — skipping buy pass "
                 "(top-up completed)", balance, cfg_min_bal)
        return
    if not candidates:
        log.info("[temp-buyer] done — no new-entry candidates (top-up=%d)", topup_placed)
        return

    placed = 0
    for c in candidates:
        if (placed + topup_placed) >= max_orders:
            log.info("[temp-buyer] max_orders cap reached (top-up=%d new=%d) — stopping",
                     topup_placed, placed)
            break

        # Token-level dedup: skip only if we already hold (or have an open buy on)
        # this specific NO token — siblings in the same event are still eligible.
        if c["no_token_id"] in held_token_ids:
            log.info("  already hold NO token %s %s — skipping",
                     c["city"], c["bucket_label"])
            continue

        # Re-pull live book; quote at the tighter of book ask and our cap.
        host = os.environ["POLYMARKET_HOST"]
        book = get_book_depth(host, c["no_token_id"])
        live_ask = book["ask"]
        ask_size = book["ask_size"]
        if live_ask is None:
            log.info("  no live ask  %s %s — skipping", c["city"], c["bucket_label"])
            continue
        if live_ask < min_no_ask:
            log.info("  ask dropped to %.2fc < %.2fc  %s — skipping",
                     live_ask * 100, min_no_ask * 100, c["city"])
            continue
        bid_pct = book.get("bid_pct_above")
        bid_above = book.get("bid_size_above") or 0
        bid_below = book.get("bid_size_below") or 0
        log.info(
            "  book %s %s  ask=%.2fc  ask_size=%.0f sh  "
            "bid≥95¢=%.0f sh  bid<95¢=%.0f sh  bid≥95%%=%.0f%%  vol=%.0f",
            c["city"], c["bucket_label"], live_ask * 100,
            ask_size or 0, bid_above, bid_below,
            bid_pct if bid_pct is not None else 0,
            c.get("volume") or 0,
        )
        if min_ask_size > 0 and (ask_size or 0) < min_ask_size:
            log.info("  ask_size %.0f sh < %.0f minimum  %s %s — skipping (thin book)",
                     ask_size or 0, min_ask_size, c["city"], c["bucket_label"])
            continue
        price = round(min(live_ask, max_no_ask), 4)

        shares = math.floor(target_shares * 100) / 100
        cost = round(shares * price, 2)
        # Same floor-with-epsilon trick as the top-up pass; see comment there.
        spendable = max(0.0, math.floor((balance - cfg_min_bal - 0.01) * 100) / 100)
        if cost > spendable:
            affordable_shares = math.floor((spendable / price) * 100) / 100
            if affordable_shares < min_topup_shares:
                log.info("  affordable %.2f sh < %.2f floor (spendable $%.2f, reserve $%.2f) "
                         "— skipping %s %s",
                         affordable_shares, min_topup_shares, spendable, cfg_min_bal,
                         c["city"], c["bucket_label"])
                continue
            shares = affordable_shares
            cost = round(shares * price, 2)
            log.info("  partial size %.2f sh @ %.3f ($%.2f) — keeping $%.2f reserve "
                     "(balance $%.2f → $%.2f)",
                     shares, price, cost, cfg_min_bal, balance, round(balance - cost, 2))

        if dry_run:
            log.info(
                "[temp-buyer] WOULD BUY %s %s  No @ %.2fc  %.2f sh ($%.2f)",
                c["city"], c["bucket_label"], price * 100, shares, cost,
            )
            balance = round(balance - cost, 2)
            placed += 1
            held_token_ids.add(c["no_token_id"])
            continue

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
            held_token_ids.add(c["no_token_id"])

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

    verb = "would place" if dry_run else "placed"
    log.info("  done — %d order(s) %s, $%.2f balance remaining", placed, verb, balance)


# ── Redeem helpers (formerly in temp_buyer/eth_1h.py) ──────────────────────────

_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_POLYGON_CHAIN_ID = 137
_POLYGON_RPC_FALLBACKS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]
_CTF_ABI = [
    {
        "inputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "bytes32", "name": "", "type": "bytes32"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


def _get_web3_and_ctf(rpc_url: str | None = None):
    from web3 import Web3

    rpc_candidates = [rpc_url] if rpc_url else []
    rpc_candidates.extend(_POLYGON_RPC_FALLBACKS)
    last_err = None
    for url in [u for u in rpc_candidates if u]:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
            _ = w3.eth.block_number
            ctf = w3.eth.contract(address=Web3.to_checksum_address(_CTF_ADDRESS), abi=_CTF_ABI)
            return w3, ctf
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"No working Polygon RPC for redeem: {last_err}")


def _as_bytes32(hex_value: str) -> bytes:
    h = (hex_value or "").lower().replace("0x", "")
    if len(h) != 64:
        raise ValueError(f"Invalid bytes32 hex length: {hex_value}")
    return bytes.fromhex(h)


def _resolved_side_from_chain(ctf: Any, condition_id: str) -> str | None:
    cid = _as_bytes32(condition_id)
    denom = int(ctf.functions.payoutDenominator(cid).call())
    if denom <= 0:
        return None
    n0 = int(ctf.functions.payoutNumerators(cid, 0).call())
    n1 = int(ctf.functions.payoutNumerators(cid, 1).call())
    if n0 > n1:
        return "up"
    if n1 > n0:
        return "down"
    return "invalid"


def _redeem_condition_positions(private_key: str, condition_id: str, rpc_url: str | None = None) -> str | None:
    from web3 import Web3

    w3, ctf = _get_web3_and_ctf(rpc_url)
    account = w3.eth.account.from_key(private_key)
    cid = _as_bytes32(condition_id)
    try:
        tx = ctf.functions.redeemPositions(
            Web3.to_checksum_address(_USDC_E_ADDRESS),
            b"\x00" * 32,
            cid,
            [1, 2],
        ).build_transaction(
            {
                "from": account.address,
                "nonce": w3.eth.get_transaction_count(account.address),
                "chainId": _POLYGON_CHAIN_ID,
                "gas": 350000,
                "gasPrice": w3.eth.gas_price,
            }
        )
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        return tx_hash.hex()
    except Exception as exc:
        log.warning("[redeem] redeemPositions failed for %s: %s", condition_id, exc)
        return None


def _redeem_condition_positions_relayer(private_key: str, condition_id: str, relayer_url: str | None = None) -> str | None:
    relayer_base = relayer_url or config.get_str("POLYMARKET_RELAYER_URL", "polymarket", "relayer_url", "https://relayer-v2.polymarket.com")
    relayer_key = os.getenv("RELAYER_API_KEY")
    relayer_addr = os.getenv("RELAYER_API_KEY_ADDRESS")

    if relayer_key and relayer_addr:
        try:
            import requests
            from py_builder_relayer_client.builder.safe import build_safe_transaction_request
            from py_builder_relayer_client.config import get_contract_config
            from py_builder_relayer_client.models import SafeTransaction, OperationType, SafeTransactionArgs, TransactionType
            from py_builder_relayer_client.signer import Signer
            from web3 import Web3

            headers = {
                "RELAYER_API_KEY": relayer_key,
                "RELAYER_API_KEY_ADDRESS": relayer_addr,
                "Content-Type": "application/json",
            }

            signer = Signer(private_key, _POLYGON_CHAIN_ID)
            from_address = signer.address()
            nonce_resp = requests.get(
                f"{relayer_base}/nonce",
                params={"address": from_address, "type": TransactionType.SAFE.value},
                headers=headers,
                timeout=20,
            )
            if nonce_resp.status_code != 200:
                raise RuntimeError(f"nonce failed: {nonce_resp.status_code} {nonce_resp.text[:200]}")
            nonce_payload = nonce_resp.json() or {}
            nonce = nonce_payload.get("nonce")
            if nonce is None:
                raise RuntimeError(f"invalid nonce payload: {nonce_payload}")

            _, ctf = _get_web3_and_ctf(os.getenv("POLYGON_RPC_URL"))
            data = ctf.encode_abi(
                "redeemPositions",
                args=[
                    Web3.to_checksum_address(_USDC_E_ADDRESS),
                    b"\x00" * 32,
                    _as_bytes32(condition_id),
                    [1, 2],
                ],
            )
            tx = SafeTransaction(
                to=Web3.to_checksum_address(_CTF_ADDRESS),
                operation=OperationType.Call,
                data=data,
                value="0",
            )
            req = build_safe_transaction_request(
                signer=signer,
                args=SafeTransactionArgs(
                    from_address=from_address,
                    nonce=nonce,
                    chain_id=_POLYGON_CHAIN_ID,
                    transactions=[tx],
                ),
                config=get_contract_config(_POLYGON_CHAIN_ID),
                metadata="redeem positions",
            ).to_dict()
            submit_resp = requests.post(f"{relayer_base}/submit", headers=headers, json=req, timeout=20)
            if submit_resp.status_code != 200:
                raise RuntimeError(f"submit failed: {submit_resp.status_code} {submit_resp.text[:300]}")
            body = submit_resp.json() or {}
            tx_hash = body.get("transactionHash")
            tx_id = body.get("transactionID")
            if tx_hash:
                return tx_hash
            if tx_id:
                for _ in range(30):
                    q = requests.get(f"{relayer_base}/transaction", params={"id": tx_id}, headers=headers, timeout=20)
                    if q.status_code != 200:
                        break
                    arr = q.json() if q.text else []
                    if isinstance(arr, list) and arr:
                        st = arr[0].get("state")
                        h = arr[0].get("transactionHash")
                        if h:
                            return h
                        if st in ("STATE_FAILED", "STATE_INVALID"):
                            break
            return None
        except Exception as exc:
            log.warning("[redeem] relayer API-key redeem failed for %s: %s", condition_id, exc)

    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
        from web3 import Web3
    except Exception as exc:
        log.warning("[redeem] relayer SDK unavailable: %s", exc)
        return None

    key = os.getenv("POLY_BUILDER_API_KEY")
    secret = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not key or not secret or not passphrase:
        log.warning("[redeem] Missing builder creds for relayer redeem (POLY_BUILDER_API_KEY/SECRET/PASSPHRASE)")
        return None

    try:
        builder_cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))
        client = RelayClient(relayer_base, _POLYGON_CHAIN_ID, private_key=private_key, builder_config=builder_cfg)
        _, ctf = _get_web3_and_ctf(os.getenv("POLYGON_RPC_URL"))
        data = ctf.encode_abi(
            "redeemPositions",
            args=[
                Web3.to_checksum_address(_USDC_E_ADDRESS),
                b"\x00" * 32,
                _as_bytes32(condition_id),
                [1, 2],
            ],
        )
        tx = SafeTransaction(
            to=Web3.to_checksum_address(_CTF_ADDRESS),
            operation=OperationType.Call,
            data=data,
            value="0",
        )
        resp = client.execute([tx], "redeem positions")
        waited = resp.wait()
        tx_hash = waited.get("transactionHash") if isinstance(waited, dict) else None
        if not tx_hash:
            tx_hash = getattr(resp, "transaction_hash", None)
        return tx_hash
    except Exception as exc:
        log.warning("[redeem] relayer redeem failed for %s: %s", condition_id, exc)
        return None


def _settle_resolved_trades(private_key: str | None, rpc_url: str | None = None, redeem_mode: str = "relayer") -> None:
    if not private_key:
        log.warning("[redeem] POLYMARKET_PRIVATE_KEY missing; cannot redeem")
        return

    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        log.warning("[redeem] POLYMARKET_FUNDER missing; cannot scan positions for redeem")
        return

    import requests as _requests
    try:
        r = _requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": funder, "sizeThreshold": "0.01"},
            timeout=10,
        )
        r.raise_for_status()
        positions = r.json()
    except Exception as exc:
        log.warning("[redeem] get_positions failed: %s", exc)
        return

    if not positions:
        return

    condition_to_tokens: dict[str, set[str]] = {}
    for p in positions:
        cid = str(p.get("conditionId") or "")
        tid = str(p.get("asset") or "")
        size = float(p.get("size", 0) or 0)
        if cid and tid and size > 0:
            condition_to_tokens.setdefault(cid, set()).add(tid)

    if not condition_to_tokens:
        return

    _, ctf = _get_web3_and_ctf(rpc_url)

    for cid, token_ids in condition_to_tokens.items():
        side = _resolved_side_from_chain(ctf, str(cid))
        if side is None:
            continue
        if redeem_mode == "relayer":
            tx_hash = _redeem_condition_positions_relayer(private_key=private_key, condition_id=str(cid))
        else:
            tx_hash = _redeem_condition_positions(private_key=private_key, condition_id=str(cid), rpc_url=rpc_url)
        if not tx_hash:
            log.warning("[redeem] Redeem submit failed for condition=%s side=%s", cid, side)
            continue
        log.info("[redeem] Settled condition=%s tokens=%s side=%s redeem_tx=%s", cid, token_ids, side, tx_hash)


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
                   help="Shares per bet (default: [temp_buyer].bet_shares = 30)")
    p.add_argument("--max-orders", type=int, default=2,
                   help="How many bets to place this run (default 2, top-of-list)")
    p.add_argument("--stale-minutes", type=float, default=None,
                   help="Cancel our own BUY orders older than this many minutes "
                        "before placing new ones (default: [temp_buyer].stale_minutes = 10). "
                        "Set 0 to disable.")
    p.add_argument("--interval", type=int, default=300,
                   help="Loop interval in seconds (default 300 = 5 min). Use --once to disable looping.")
    p.add_argument("--once", action="store_true",
                   help="Run a single cycle and exit (default: loop forever)")
    p.add_argument("--redeem", dest="redeem", action="store_true", default=None,
                   help="After each live cycle, redeem any resolved positions held by the funder "
                        "(default: [temp_buyer].redeem_resolved = true). Ignored in dry-run.")
    p.add_argument("--no-redeem", dest="redeem", action="store_false",
                   help="Disable post-cycle redeem scan.")
    p.add_argument("--redeem-mode", choices=["relayer", "onchain"], default=None,
                   help="Redeem flow when --redeem is on (default: [temp_buyer].redeem_mode = relayer).")
    p.add_argument("--polygon-rpc", type=str, default=None,
                   help="Polygon RPC URL for onchain redeem mode (default: $POLYGON_RPC_URL).")
    args = p.parse_args()
    if args.stale_minutes is not None:
        os.environ["TEMPBUY_STALE_MINUTES"] = str(args.stale_minutes)

    redeem_enabled = (
        args.redeem if args.redeem is not None
        else config.get_bool("TEMPBUY_REDEEM_RESOLVED", "temp_buyer", "redeem_resolved", True)
    )
    redeem_mode = args.redeem_mode or config.get_str(
        "TEMPBUY_REDEEM_MODE", "temp_buyer", "redeem_mode", "relayer",
    )
    polygon_rpc = args.polygon_rpc or os.getenv("POLYGON_RPC_URL")

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

    def _redeem() -> None:
        # Live-only: scan funder positions and redeem any whose chain-side
        # condition has resolved. _settle_resolved_trades is source-agnostic
        # — it picks up temp_buyer's NO positions alongside anything else.
        if not args.bet or not redeem_enabled:
            return
        try:
            _settle_resolved_trades(
                private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                rpc_url=polygon_rpc,
                redeem_mode=redeem_mode,
            )
        except Exception:
            logger.exception("[temp-buyer] redeem scan failed")

    if args.once:
        _cycle()
        _redeem()
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
            _redeem()
            # Backfill resolved-event outcomes every ~10 cycles so the
            # weather_model bias/sigma EMAs absorb fresh data without
            # spamming the Gamma API on every loop.
            if iteration % 10 == 1:
                try:
                    from temp_buyer import weather_scan
                    weather_scan.maybe_backfill_outcomes(log_label="weather-model")
                except Exception:
                    logger.exception("[weather-model] backfill failed")
            logger.info("[temp-buyer] sleeping %ds before next cycle", interval)
            _time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("[temp-buyer] interrupted — exiting after cycle %d", iteration)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
