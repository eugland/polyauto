"""
Binance XRP/USDT real-time price — same source Polymarket uses for resolution.

Endpoints used:
  Spot price:  GET https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT
  1H kline:    GET https://api.binance.com/api/v3/klines?symbol=XRPUSDT&interval=1h&limit=1

Kline fields (by index):
  0  open_time          ms timestamp
  1  open               candle open price  ← this becomes priceToBeat
  2  high
  3  low
  4  close              current close (updates every second while candle is live)
  5  volume
  6  close_time         ms timestamp
  7  quote_asset_volume
  8  number_of_trades
  9  taker_buy_base_vol
  10 taker_buy_quote_vol
  11 ignore
"""

import json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT"
BINANCE_KLINE_URL = "https://api.binance.com/api/v3/klines?symbol=XRPUSDT&interval=1h&limit=2"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def get_spot_price() -> float:
    """Current XRP/USDT spot price."""
    data = _get(BINANCE_PRICE_URL)
    return float(data["price"])


def get_current_candle() -> dict:
    """
    Returns the live 1H candle — the one Polymarket resolves against.
    Also returns the previous closed candle for reference.
    """
    data = _get(BINANCE_KLINE_URL)
    prev_raw, curr_raw = data[0], data[1]

    def parse(raw: list, is_live: bool) -> dict:
        open_time  = datetime.fromtimestamp(raw[0] / 1000, tz=timezone.utc)
        close_time = datetime.fromtimestamp(raw[6] / 1000, tz=timezone.utc)
        o, h, l, c = float(raw[1]), float(raw[2]), float(raw[3]), float(raw[4])
        direction = "UP" if c >= o else "DOWN"
        change    = c - o
        change_pct = (change / o) * 100
        return {
            "open_time_utc":   open_time.isoformat(),
            "close_time_utc":  close_time.isoformat(),
            "open":            o,
            "high":            h,
            "low":             l,
            "close":           c,
            "volume":          float(raw[5]),
            "trades":          int(raw[8]),
            "direction":       direction,
            "change":          round(change, 2),
            "change_pct":      round(change_pct, 4),
            "is_live":         is_live,
        }

    return {
        "spot_price":     get_spot_price(),
        "current_candle": parse(curr_raw, is_live=True),
        "prev_candle":    parse(prev_raw, is_live=False),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def print_price():
    try:
        data = get_current_candle()
        curr = data["current_candle"]
        prev = data["prev_candle"]

        print(f"\n  BINANCE XRP/USDT — REAL-TIME")
        print(f"  {'-'*46}")
        print(f"  Spot price:      ${data['spot_price']:>12,.2f}")
        print(f"  Fetched (UTC):   {data['fetched_at_utc']}")

        print(f"\n  CURRENT 1H CANDLE (live -- used for resolution)")
        print(f"  {'-'*46}")
        print(f"  Candle open (UTC): {curr['open_time_utc']}")
        print(f"  Open:   ${curr['open']:>12,.2f}   <- price to beat")
        print(f"  High:   ${curr['high']:>12,.2f}")
        print(f"  Low:    ${curr['low']:>12,.2f}")
        print(f"  Close:  ${curr['close']:>12,.2f}   <- current close")
        print(f"  Change: ${curr['change']:>+12,.2f}  ({curr['change_pct']:+.4f}%)")
        print(f"  Direction so far: {curr['direction']}")
        print(f"  Trades this hour: {curr['trades']:,}")

        print(f"\n  PREVIOUS 1H CANDLE (closed)")
        print(f"  {'-'*46}")
        print(f"  Candle open (UTC): {prev['open_time_utc']}")
        print(f"  Open:   ${prev['open']:>12,.2f}")
        print(f"  Close:  ${prev['close']:>12,.2f}")
        print(f"  Result: {prev['direction']}  ({prev['change_pct']:+.4f}%)")

    except (URLError, KeyError, ValueError) as e:
        print(f"  [error fetching Binance price] {e}")


if __name__ == "__main__":
    print_price()
