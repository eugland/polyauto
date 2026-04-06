"""
XRP 1H signal — near-resolution decay buy.

Logic:
    With 5-15 minutes left, if XRP has a safe margin from the candle open
    and the winning shares are still priced below MAX_BUY_PRICE,
    buy and immediately post a sell at SELL_TARGET.

    Either the sell fills as price decays toward 1.00,
    or the candle resolves and each share pays $1.00.
"""

# Time window — only enter in this band (seconds remaining)
ENTRY_MIN_SEC = 5  * 60    # 5  min remaining  (lower bound — tail risk)
ENTRY_MAX_SEC = 15 * 60    # 15 min remaining  (upper bound — enter here)

# XRP must be at least this far from the candle open to be "safe"
MIN_MARGIN = 0.02           # $0.02 (~1% at $2 XRP, medium risk)

# Immediately post a sell at this price after buying
SELL_TARGET = 0.99


def compute_signal(
    time_remaining_sec: int,
    candle_open:        float,
    btc_price:          float,
    up_odds:            float,   # implied probability of UP (0-1)
) -> dict:
    """
    Returns:
        direction : "UP" | "DOWN" | None
        buy_price : float — current ask to buy at
        reasons   : list[str]
    """
    reasons = []

    # 1. Time window
    in_window = ENTRY_MIN_SEC <= time_remaining_sec <= ENTRY_MAX_SEC
    reasons.append(
        f"time_window={'PASS' if in_window else 'FAIL'} "
        f"({time_remaining_sec // 60}m{time_remaining_sec % 60:02d}s left, "
        f"window {ENTRY_MIN_SEC//60}-{ENTRY_MAX_SEC//60}m)"
    )

    # 2. Safe margin
    distance   = btc_price - candle_open
    direction  = "UP" if distance > 0 else "DOWN"
    safe       = abs(distance) >= MIN_MARGIN
    reasons.append(
        f"margin={'PASS' if safe else 'FAIL'} "
        f"(BTC {distance:+.2f} vs min ${MIN_MARGIN})"
    )

    winning_odds = up_odds if direction == "UP" else (1 - up_odds)

    if not (in_window and safe):
        return {"direction": None, "reasons": reasons}

    return {
        "direction": direction,
        "buy_price": winning_odds,
        "reasons":   reasons,
    }
