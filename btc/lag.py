"""
Polymarket vs Binance lag scanner.

Compares the implied BTC price from Polymarket order book odds
against the real Binance spot price, and measures how far behind
the market is in pricing in the current candle direction.

Run: python -m btc.lag
"""

import time
import sys
import io
from datetime import datetime, timezone, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from .price     import get_current_candle
from .market    import current_et, build_slug, fetch_market, parse_market, ET_OFFSET
from .orderbook import fetch_book, parse_book

SAMPLES    = 20       # number of samples to collect
INTERVAL   = 10       # seconds between samples


def implied_probability(distance: float, total_seconds_elapsed: float, total_candle_sec: float = 3600) -> float:
    """
    Rough true probability estimate based on:
    - How far BTC is from open (in $)
    - How much of the candle has elapsed (less time = harder to reverse)
    Uses a simple linear decay model.
    """
    if total_candle_sec <= 0:
        return 0.5
    time_factor     = total_seconds_elapsed / total_candle_sec   # 0=start, 1=end
    distance_factor = min(abs(distance) / 500.0, 1.0)            # maxes at $500
    # More time elapsed + bigger distance = higher confidence
    confidence = 0.5 + (distance_factor * 0.5 * time_factor)
    return round(min(confidence, 0.99), 4)


def scan_once() -> dict:
    now_et      = current_et()
    now_utc     = datetime.now(timezone.utc)

    candle_start_et  = now_et.replace(minute=0, second=0, microsecond=0)
    candle_end_utc   = (candle_start_et + timedelta(hours=1)).astimezone(timezone.utc)
    candle_start_utc = candle_start_et.astimezone(timezone.utc)

    time_remaining   = int((candle_end_utc - now_utc).total_seconds())
    time_elapsed     = int((now_utc - candle_start_utc).total_seconds())
    current_slug     = build_slug(candle_start_et)

    # Binance
    price_data   = get_current_candle()
    candle       = price_data["current_candle"]
    btc_price    = candle["close"]
    candle_open  = candle["open"]
    distance     = btc_price - candle_open
    btc_dir      = "UP" if distance > 0 else "DOWN"
    binance_ts   = price_data["fetched_at_utc"]

    # Polymarket
    event        = fetch_market(current_slug)
    mkt          = parse_market(event) if event else None
    up_odds      = mkt["prices"].get("Up", 0.5) if mkt else 0.5
    poly_dir     = "UP" if up_odds >= 0.5 else "DOWN"
    poly_conf    = up_odds if poly_dir == "UP" else (1 - up_odds)

    # Order book mid
    up_book = parse_book(fetch_book(mkt["clob_token_up"])) if mkt else None
    ob_mid  = up_book["mid"] if up_book else up_odds

    # True probability estimate
    true_prob = implied_probability(distance, time_elapsed)

    # Lag = how much polymarket is lagging behind true probability
    lag = round(true_prob - poly_conf, 4) if btc_dir == poly_dir else None
    agreement = btc_dir == poly_dir

    return {
        "timestamp":      now_utc.strftime("%H:%M:%S"),
        "time_elapsed":   time_elapsed,
        "time_remaining": time_remaining,
        "btc_open":       candle_open,
        "btc_price":      btc_price,
        "distance":       round(distance, 2),
        "btc_dir":        btc_dir,
        "poly_odds_up":   up_odds,
        "poly_dir":       poly_dir,
        "poly_conf":      round(poly_conf, 4),
        "ob_mid":         round(ob_mid, 4) if ob_mid else None,
        "true_prob_est":  true_prob,
        "lag":            lag,
        "agreement":      agreement,
    }


def print_sample(s: dict, i: int, total: int):
    lag_str = f"{s['lag']:+.4f}" if s["lag"] is not None else "  N/A  (disagree)"
    agree   = "AGREE" if s["agreement"] else "DISAGREE"
    print(
        f"  [{i:>2}/{total}]  {s['timestamp']}  "
        f"BTC ${s['btc_price']:>10,.2f}  ({s['distance']:>+8.2f})  {s['btc_dir']:<5}  "
        f"| Poly {s['poly_dir']:<5} {s['poly_conf']:.3f}  OB_mid {s['ob_mid']:.3f}  "
        f"| TrueEst {s['true_prob_est']:.3f}  Lag {lag_str}  {agree}"
    )


def run():
    print(f"\n  BTC Polymarket Lag Scanner  --  {SAMPLES} samples x {INTERVAL}s")
    print(f"  {'='*100}")
    print(f"  {'':4}  {'TIME':8}  {'BTC PRICE':>12}  {'DIST':>9}  {'DIR':<5}  "
          f"  {'POLY DIR':<5} {'CONF':>6}  {'OB MID':>7}  "
          f"  {'TRUE EST':>8}  {'LAG':>8}  AGREE?")
    print(f"  {'-'*100}")

    samples = []

    for i in range(1, SAMPLES + 1):
        try:
            s = scan_once()
            samples.append(s)
            print_sample(s, i, SAMPLES)
        except Exception as e:
            print(f"  [{i:>2}/{SAMPLES}]  ERROR: {e}")

        if i < SAMPLES:
            time.sleep(INTERVAL)

    # ── Summary ──────────────────────────────────────────────────────────
    if not samples:
        print("  No samples collected.")
        return

    valid_lags  = [s["lag"] for s in samples if s["lag"] is not None]
    agrees      = [s for s in samples if s["agreement"]]
    disagrees   = [s for s in samples if not s["agreement"]]

    print(f"\n  {'='*100}")
    print(f"  SUMMARY  ({len(samples)} samples)")
    print(f"  {'-'*50}")
    print(f"  Direction agreement:  {len(agrees)}/{len(samples)} samples  ({len(agrees)/len(samples)*100:.0f}%)")
    print(f"  Direction disagreement: {len(disagrees)} samples")

    if valid_lags:
        avg_lag = sum(valid_lags) / len(valid_lags)
        max_lag = max(valid_lags)
        min_lag = min(valid_lags)
        print(f"\n  Polymarket lag vs true probability estimate (when directions agree):")
        print(f"    Avg lag:  {avg_lag:+.4f}  ({'underpriced' if avg_lag > 0 else 'overpriced'} by Polymarket)")
        print(f"    Max lag:  {max_lag:+.4f}")
        print(f"    Min lag:  {min_lag:+.4f}")
        print(f"\n  Interpretation:")
        if avg_lag > 0.02:
            print(f"    Polymarket is consistently SLOW — avg {avg_lag:.3f} behind true probability.")
            print(f"    Edge exists: buying the winning side captures this lag.")
        elif avg_lag < -0.02:
            print(f"    Polymarket is AHEAD of true probability — market is efficient or overreacting.")
        else:
            print(f"    Polymarket is roughly in sync with true probability (lag < 2%).")
    else:
        print("  No valid lag samples (directions disagreed all samples).")

    print()


if __name__ == "__main__":
    run()
