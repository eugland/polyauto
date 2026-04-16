"""
Select executable weather brackets and place NO bets.
"""
from __future__ import annotations

import logging
import os

from automata.client import place_no_order, place_sell_order
from automata.db import record_bet
from weather.markets import Bracket, RankedMarket

log = logging.getLogger("weather.bet")

# Polymarket CLOB supports 0.1¢ ticks on weather markets.
TICK = 0.001
# Don't post a buy ≥ this — no room left between fill and resale.
MAX_SUGGESTED_BID = 0.995
# Price we'll try to resell at after filling (GTC sell).
RESALE_PRICE = 0.999


def suggested_bid(br: Bracket) -> float | None:
    """
    Suggested limit-buy price for this bracket's NO token.

    Strategy: sit 0.1¢ below the best ask — aggressive (takes front of book,
    will often cross if ask drops) while still being a maker when possible.
    Returns None if the price would be >= MAX_SUGGESTED_BID.
    """
    if br.ask is None:
        return None
    price = round(br.ask - TICK, 3)
    if br.bid is not None:
        price = max(price, round(br.bid, 3))
    if price >= MAX_SUGGESTED_BID:
        return None
    return price


def expected_value_no(br: Bracket) -> float | None:
    """
    Model EV per share for buying NO at the executable bid price:
      EV = P(no) - price = (1 - P(yes)) - price
    """
    if br.p_yes is None:
        return None
    price = suggested_bid(br)
    if price is None:
        return None
    return (1.0 - br.p_yes) - price


def pick_best_executable(
    rm: RankedMarket,
    *,
    min_no_price: float,
    max_no_price: float,
) -> tuple[Bracket, float] | None:
    """
    Return the executable bracket with the highest positive EV, restricted to
    NO asks in [min_no_price, max_no_price].
    Returns (bracket, ev) or None if nothing tradable with positive EV.
    """
    candidates: list[tuple[Bracket, float]] = []
    for br in rm.brackets:
        if br.p_yes is None or br.ask is None:
            continue
        if not (min_no_price <= br.ask <= max_no_price):
            continue
        ev = expected_value_no(br)
        if ev is None:
            continue
        candidates.append((br, ev))

    if not candidates:
        return None
    best_br, best_ev = max(candidates, key=lambda item: item[1])
    if best_ev <= 0.0:
        return None
    return best_br, best_ev


def pick_least_likely(
    rm: RankedMarket,
    *,
    min_no_price: float,
    max_no_price: float,
) -> Bracket | None:
    """
    Backward-compatible shim: returns the bracket from pick_best_executable.
    """
    best = pick_best_executable(rm, min_no_price=min_no_price, max_no_price=max_no_price)
    if best is None:
        return None
    return best[0]


def place_bet(
    client,
    rm: RankedMarket,
    bracket: Bracket,
    shares: float,
    *,
    dry_run: bool,
) -> dict:
    """
    Dry-run: log the intended order. Live: call place_no_order + record_bet.
    Returns the CLOB response dict (or an empty dict in dry-run).
    """
    price = suggested_bid(bracket)
    if price is None:
        log.warning(
            "No postable bid for %s / %s (ask=%s, bid=%s) — skipping",
            rm.city, bracket.question, bracket.ask, bracket.bid,
        )
        logging.getLogger("weather.signal").info(
            "SKIP_NOT_POSTABLE city=%s date=%s question=%s ask=%s bid=%s",
            rm.city, rm.event_date, bracket.question, bracket.ask, bracket.bid,
        )
        return {}

    cost = round(shares * price, 2)
    label = (
        f"{rm.city} {rm.title_date} | {bracket.question} | "
        f"NO @ {price*100:.2f}¢ x {shares:g} = ${cost:.2f} "
        f"(resale @ {RESALE_PRICE*100:.1f}¢, "
        f"P_yes={bracket.p_yes*100:.1f}%  P_no={(1-bracket.p_yes)*100:.1f}%)"
    )

    if dry_run:
        log.info("[DRY RUN] would buy %s", label)
        logging.getLogger("weather.signal").info(
            "DRY_BUY city=%s date=%s question=%s price=%.4f shares=%s cost=%.2f",
            rm.city, rm.event_date, bracket.question, price, shares, cost,
        )
        return {}

    log.info("Placing NO order: %s", label)
    resp = place_no_order(client, bracket.no_token_id, price, shares, post_only=False)
    logging.getLogger("weather.signal").info(
        "BET_PLACED city=%s date=%s question=%s price=%.4f shares=%s cost=%.2f order_id=%s status=%s",
        rm.city,
        rm.event_date,
        bracket.question,
        price,
        shares,
        cost,
        resp.get("orderID") or resp.get("id") or "?",
        resp.get("status") or "?",
    )
    order_id = resp.get("orderID") or resp.get("id") or "?"

    # If the buy matched (fully or partially), immediately post a GTC sell at
    # the resale target for the filled shares. If it rested on the book, skip
    # — we'll post the resale on a later run once it fills.
    status = str(resp.get("status") or "").lower()
    try:
        making = float(resp.get("makingAmount") or 0.0)  # USDC spent
    except (TypeError, ValueError):
        making = 0.0
    filled_shares = round(making / price, 2) if price > 0 and making > 0 else 0.0
    if status == "matched" and filled_shares > 0:
        try:
            sell_resp = place_sell_order(client, bracket.no_token_id, RESALE_PRICE, filled_shares)
            log.info(
                "Posted resale: %.2f shares NO @ %.3f  id=%s",
                filled_shares, RESALE_PRICE,
                sell_resp.get("orderID") or sell_resp.get("id") or "?",
            )
        except Exception as exc:
            log.warning("Resale post failed: %s", exc)
    elif status:
        log.info("Buy status=%s — resale will be posted once filled", status)

    record_bet(
        city=rm.city,
        icao=rm.icao,
        event_date=rm.event_date,
        question=bracket.question,
        option="No",
        token_id=bracket.no_token_id,
        order_id=str(order_id),
        shares=shares,
        no_price=price,
        yes_price=None,  # analyzer doesn't carry yes ask
        cost_usdc=cost,
        unit=bracket.unit,
        threshold=bracket.threshold,
        threshold_hi=bracket.threshold_hi,
        direction=bracket.direction,
        forecast_high=bracket.forecast_mean,
    )
    return resp


def _blacklist() -> set[str]:
    return {c.strip() for c in os.getenv("CITY_BLACKLIST", "Seoul,Taipei").split(",") if c.strip()}


def should_skip_city(city: str) -> bool:
    return city in _blacklist()
