"""
XRP 1H Polymarket — near-resolution decay buy.

Run:  python -m btc.monitor [--dry-run]

Strategy:
    With 5-15 min left, if XRP has $0.02+ margin from candle open
    and winning shares < 0.94, buy and immediately post sell at 0.99.
    Either fills before expiry or resolves at $1.00.
"""

import sys
import time
import logging
import io
from datetime import datetime, timezone, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("btc")

from .market    import current_et, build_slug, fetch_market, parse_market, ET_OFFSET
from .price     import get_current_candle
from .signal    import compute_signal, SELL_TARGET
from .ledger    import record_entry, record_exit, load_open_position

from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

POLL_IDLE    = 30   # seconds when no position
POLL_HOLDING =  5   # seconds when holding

BET_SHARES = 5
DRY_RUN    = "--dry-run" in sys.argv

# ── In-memory position state ──────────────────────────────────────────────────
position = {
    "active":       False,
    "trade_id":     None,
    "direction":    None,
    "token_id":     None,
    "entry_price":  None,
    "shares":       0,
    "buy_order_id": None,
    "sell_order_id":None,
    "entry_time":   None,
    "candle_slug":  None,
}


def _reset():
    global position
    position = {k: None for k in position}
    position["active"] = False
    position["shares"] = 0


def _build_clob():
    if DRY_RUN:
        return None
    from .clob import build_client
    return build_client()


def _place_buy(client, token_id, price, shares) -> str | None:
    if DRY_RUN:
        log.info("  [DRY-RUN] BUY %s shares @ %.3f", shares, price)
        return "dry-buy"
    try:
        args   = OrderArgs(token_id=token_id, price=price, size=shares, side=BUY)
        signed = client.create_order(args)
        resp   = client.post_order(signed, OrderType.GTC)
        oid    = resp.get("orderID") or resp.get("order_id") or str(resp)
        log.info("  BUY placed: %s @ %.3f x %s", oid, price, shares)
        return oid
    except Exception as e:
        log.error("  BUY failed: %s", e)
        return None


def _place_sell(client, token_id, price, shares) -> str | None:
    if DRY_RUN:
        log.info("  [DRY-RUN] SELL %s shares @ %.3f", shares, price)
        return "dry-sell"
    try:
        args   = OrderArgs(token_id=token_id, price=price, size=shares, side=SELL)
        signed = client.create_order(args)
        resp   = client.post_order(signed, OrderType.GTC)
        oid    = resp.get("orderID") or resp.get("order_id") or str(resp)
        log.info("  SELL placed: %s @ %.3f x %s", oid, price, shares)
        return oid
    except Exception as e:
        log.error("  SELL failed: %s", e)
        return None


def _is_filled(client, order_id: str, cur_price: float = 0) -> bool:
    if DRY_RUN:
        # Simulate fill: if market price has reached or passed the sell target
        return cur_price >= SELL_TARGET
    try:
        from py_clob_client.clob_types import OpenOrderParams
        orders = client.get_orders(OpenOrderParams(asset_id=position["token_id"]))
        ids    = [o.get("id") or o.get("order_id") for o in (orders or [])]
        return order_id not in ids
    except Exception:
        return False


def run_cycle(client):
    now_et  = current_et()
    now_utc = datetime.now(timezone.utc)

    candle_start_et = now_et.replace(minute=0, second=0, microsecond=0)
    candle_end_utc  = (candle_start_et + timedelta(hours=1)).astimezone(timezone.utc)
    time_remaining  = int((candle_end_utc - now_utc).total_seconds())
    current_slug    = build_slug(candle_start_et)

    log.info("--- %s  T-%dm%ds ---",
             current_slug[-24:], time_remaining // 60, time_remaining % 60)

    # ── Fetch Binance price ───────────────────────────────────────────────
    try:
        candle      = get_current_candle()["current_candle"]
        btc_price   = candle["close"]
        candle_open = candle["open"]
        log.info("XRP open=$%s  now=$%s  %s  (%+.4f)",
                 f"{candle_open:,.2f}", f"{btc_price:,.2f}",
                 candle["direction"], candle["change"])
    except Exception as e:
        log.error("Binance error: %s", e)
        return

    # ── Fetch Polymarket market ───────────────────────────────────────────
    try:
        event = fetch_market(current_slug)
        if not event:
            log.warning("Market not found: %s", current_slug)
            return
        mkt         = parse_market(event)
        up_odds     = mkt["prices"].get("Up", 0.5)
        up_token    = mkt["clob_token_up"]
        dn_token    = mkt["clob_token_down"]
        log.info("Odds  UP=%.3f  DOWN=%.3f", up_odds, 1 - up_odds)
    except Exception as e:
        log.error("Market error: %s", e)
        return

    # ── New candle → auto-resolve any open dry-run position ──────────────
    if position["active"] and position["candle_slug"] != current_slug:
        if DRY_RUN:
            # Use previous candle result from Binance to determine win/loss
            try:
                prev          = get_current_candle()["prev_candle"]
                prev_result   = prev["direction"]   # "UP" or "DOWN"
                won           = prev_result == position["direction"]
                exit_price    = 1.00 if won else 0.00
                exit_reason   = f"resolved_{'win' if won else 'loss'} (candle={prev_result})"
                net_pnl       = record_exit(
                    trade_id    = position["trade_id"],
                    exit_price  = exit_price,
                    exit_reason = exit_reason,
                )
                log.info("Candle resolved — %s  net P&L: %+.4f USDC",
                         exit_reason, net_pnl)
            except Exception as e:
                log.error("Auto-resolve failed: %s", e)
        else:
            log.info("New candle — resetting position")
        _reset()

    # ── HOLDING: check sell filled or force exit ─────────────────────────
    if position["active"] and position["sell_order_id"]:

        entry  = position["entry_price"]
        cur    = up_odds if position["direction"] == "UP" else (1 - up_odds)
        token_id = up_token if position["direction"] == "UP" else dn_token

        FORCE_EXIT_MIN_PRICE = 0.60   # exit if share price drops below this
        FORCE_EXIT_MAX_SEC   = 5 * 60 # exit if less than 5 min remaining

        price_too_low  = cur < FORCE_EXIT_MIN_PRICE
        time_is_up     = time_remaining <= FORCE_EXIT_MAX_SEC

        if price_too_low or time_is_up:
            reason_str = f"price_below_0.60 ({cur:.3f})" if price_too_low else f"time_limit (T-{time_remaining//60}m)"
            log.info("FORCE EXIT — %s", reason_str)
            if not DRY_RUN:
                try:
                    client.cancel(position["sell_order_id"])
                except Exception as e:
                    log.warning("Cancel failed: %s", e)
            exit_price = round(cur - 0.01, 2)
            _place_sell(client, token_id, exit_price, position["shares"])
            net_pnl = record_exit(
                trade_id    = position["trade_id"],
                exit_price  = exit_price,
                exit_reason = f"force_exit_{reason_str}",
            )
            log.info("Force exit recorded — net P&L: %+.4f USDC", net_pnl)
            _reset()

        elif _is_filled(client, position["sell_order_id"], cur_price=cur):
            net_pnl = record_exit(
                trade_id    = position["trade_id"],
                exit_price  = SELL_TARGET,
                exit_reason = "sell_filled",
            )
            log.info("SELL filled — net P&L: %+.4f USDC", net_pnl)
            _reset()

        else:
            log.info("Holding %s — entry=%.3f  now=%.3f  unrealised %+.4f",
                     position["direction"], entry, cur, (cur - entry) * position["shares"])
        return

    # ── ENTRY SIGNAL ──────────────────────────────────────────────────────
    if position["active"]:
        return  # already in — waiting for sell to be posted

    sig = compute_signal(
        time_remaining_sec = time_remaining,
        candle_open        = candle_open,
        btc_price          = btc_price,
        up_odds            = up_odds,
    )

    for r in sig["reasons"]:
        log.info("  %s", r)

    if not sig["direction"]:
        log.info("No signal.")
        return

    direction  = sig["direction"]
    token_id   = up_token if direction == "UP" else dn_token
    buy_price  = sig["buy_price"]

    log.info("SIGNAL %s — buy @ %.3f  sell @ %.3f  shares=%s",
             direction, buy_price, SELL_TARGET, BET_SHARES)

    buy_oid = _place_buy(client, token_id, buy_price, BET_SHARES)
    if not buy_oid:
        return

    sell_oid = _place_sell(client, token_id, SELL_TARGET, BET_SHARES)

    entry_time = now_utc.isoformat()
    trade_id   = record_entry(
        candle_slug = current_slug,
        direction   = direction,
        entry_price = buy_price,
        shares      = BET_SHARES,
        order_id    = buy_oid,
        entry_time  = entry_time,
        mode        = "DRY-RUN" if DRY_RUN else "LIVE",
    )
    log.info("Entry recorded — trade #%s", trade_id)

    position.update({
        "active":        True,
        "trade_id":      trade_id,
        "direction":     direction,
        "token_id":      token_id,
        "entry_price":   buy_price,
        "shares":        BET_SHARES,
        "buy_order_id":  buy_oid,
        "sell_order_id": sell_oid,
        "entry_time":    entry_time,
        "candle_slug":   current_slug,
    })


def main():
    global position
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info("XRP monitor starting — %s — %s shares", mode, BET_SHARES)

    restored = load_open_position()
    if restored:
        position = restored
        log.info("Restored open position — trade #%s  %s @ %.3f",
                 position["trade_id"], position["direction"], position["entry_price"])

    client = _build_clob()

    while True:
        try:
            run_cycle(client)
        except KeyboardInterrupt:
            log.info("Stopped.")
            break
        except Exception as e:
            log.error("Cycle error: %s", e, exc_info=True)

        time.sleep(POLL_HOLDING if position["active"] else POLL_IDLE)


if __name__ == "__main__":
    main()
