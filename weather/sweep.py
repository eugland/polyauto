"""
Sweep open NO positions from weather bets and ensure a GTC take-profit sell
is resting on the book at TAKE_PROFIT_PRICE (default 0.999).

Mirrors automata.weather_bot._scan_positions semantics, but restricts to
token_ids that appear in bets.db `placed_bets` with option='No' so we don't
accidentally sell ETH-bot positions held for redemption.
"""
from __future__ import annotations

import logging
import os
import sqlite3

from automata.client import (
    get_best_bid,
    get_open_orders,
    get_positions,
    place_market_sell,
    place_sell_order,
)
from automata.db import DB_PATH

log = logging.getLogger("weather.sweep")


def _weather_no_token_ids() -> set[str]:
    """token_ids of all weather NO bets we've ever recorded."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT DISTINCT token_id FROM placed_bets "
                "WHERE option = 'No' AND token_id IS NOT NULL"
            ).fetchall()
        return {str(r[0]) for r in rows if r[0]}
    except Exception as exc:
        log.warning("Could not load weather token ids from DB: %s", exc)
        return set()


def sweep_take_profits(
    client,
    funder: str,
    *,
    take_profit: float | None = None,
    dry_run: bool = False,
) -> None:
    """
    For each open weather NO position, ensure a GTC sell exists at `take_profit`.

    size < 5 shares: can't rest a limit (below min size) — try a market sell
                    only if the current bid has already reached the target.
    size >= 5     : post a GTC sell @ `take_profit` if none already exists.
    """
    if not funder:
        log.warning("POLYMARKET_FUNDER not set — cannot sweep positions")
        return

    if take_profit is None:
        take_profit = float(os.getenv("TAKE_PROFIT_PRICE", "0.999"))

    positions = get_positions(funder)
    if not positions:
        log.info("Sweep: no open positions")
        return

    weather_tokens = _weather_no_token_ids()
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")

    weather_positions = [p for p in positions if p["token_id"] in weather_tokens]
    log.info(
        "Sweep: %d open positions / %d tracked weather NO positions  target=%.1f¢",
        len(positions), len(weather_positions), take_profit * 100,
    )

    for pos in weather_positions:
        token_id = pos["token_id"]
        size = pos["size"]

        # Too small to rest a limit — try a market sell if bid already at target.
        if size < 5:
            bid = get_best_bid(host, token_id)
            if bid is not None and bid >= take_profit:
                if dry_run:
                    log.info(
                        "  token %s  %.2f sh — [DRY] bid %.1f¢ >= target, would market sell",
                        token_id[:12], size, bid * 100,
                    )
                else:
                    try:
                        resp = place_market_sell(client, token_id, bid, size)
                        oid = resp.get("orderID") or resp.get("id") or "?"
                        log.info(
                            "  token %s  %.2f sh — market sell @ %.1f¢  id=%s",
                            token_id[:12], size, bid * 100, oid,
                        )
                    except Exception as exc:
                        log.error("  token %s — market sell failed: %s", token_id[:12], exc)
            else:
                log.info(
                    "  token %s  %.2f sh — too small, bid not yet at target",
                    token_id[:12], size,
                )
            continue

        # Full-size position — check for an existing TP sell, else post one.
        try:
            orders = get_open_orders(client, token_id)
        except Exception as exc:
            log.warning("  token %s — get_open_orders failed: %s", token_id[:12], exc)
            continue

        has_tp = any(
            abs(float(o.get("price", 0)) - take_profit) < 1e-4
            and str(o.get("side", "")).upper() == "SELL"
            for o in orders
        )
        if has_tp:
            log.info(
                "  token %s  %.2f sh — TP @ %.1f¢ already resting",
                token_id[:12], size, take_profit * 100,
            )
            continue

        if dry_run:
            log.info(
                "  token %s  %.2f sh — [DRY] would place sell @ %.1f¢",
                token_id[:12], size, take_profit * 100,
            )
        else:
            try:
                resp = place_sell_order(client, token_id, take_profit, size)
                oid = resp.get("orderID") or resp.get("id") or "?"
                log.info(
                    "  token %s  %.2f sh — sell @ %.1f¢ placed  id=%s",
                    token_id[:12], size, take_profit * 100, oid,
                )
            except Exception as exc:
                log.error("  token %s — sell order failed: %s", token_id[:12], exc)
