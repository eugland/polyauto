"""
automata/crypto_common.py

Shared helpers for the ETH 1H and BTC 1H bots and the BTC backtest harness.

Pulled out of automata/eth_1h.py without behavior change. The only
generalization is `build_slug(dt, asset=...)`, which now accepts an asset
prefix; the default ("ethereum") preserves the original ETH bot behavior.

`_fetch_1m_momentum` previously referenced `time.time()` without importing
`time` — calls fell into the function's bare `except` and silently set
`vol_accel = None`. The import is now present so the value is computed.
"""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen


MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}


def build_slug(dt: datetime, asset: str = "ethereum") -> str:
    """{asset}-up-or-down-april-5-2026-3pm-et"""
    month = MONTH_NAMES[dt.month]
    h24   = dt.hour
    h12   = h24 % 12 or 12
    return f"{asset}-up-or-down-{month}-{dt.day}-{dt.year}-{h12}{'am' if h24 < 12 else 'pm'}-et"


def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _realized_annual_vol(symbol: str = "ETHUSDT", lookback_hours: int = 168) -> float | None:
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={lookback_hours}"
        )
    except Exception:
        return None
    if not isinstance(klines, list) or len(klines) < 3:
        return None
    closes: list[float] = []
    for row in klines:
        try:
            closes.append(float(row[4]))
        except Exception:
            continue
    if len(closes) < 3:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    hourly_vol = math.sqrt(max(var, 0.0))
    return hourly_vol * math.sqrt(365.0 * 24.0)


def _black_scholes_digital_up_prob(spot: float, strike: float, years_to_expiry: float, sigma: float, r: float = 0.0) -> float | None:
    """Risk-neutral P(S_T >= K) for a cash-or-nothing digital call."""
    if spot <= 0 or strike <= 0 or years_to_expiry <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(years_to_expiry)
    d2 = (math.log(spot / strike) + (r - 0.5 * sigma * sigma) * years_to_expiry) / (sigma * sqrt_t)
    return min(1.0, max(0.0, _normal_cdf(d2)))


def _fetch_1m_momentum(symbol: str = "ETHUSDT", lookback: int = 7) -> dict | None:
    """
    Fetch the last `lookback+1` 1-minute klines and compute momentum metrics.

    Returns dict:
      taker_ratio    – taker buy vol / total vol for last completed candle
                       (0 = all sellers, 1 = all buyers)
      consecutive_dir – +N consecutive up candles, -N consecutive down candles
      vol_accel      – current-minute projected vol rate vs prior N-minute average
      trade_count    – number of trades in last completed 1m candle
      sigma_1m       – realized 1m return std-dev as a fraction (e.g. 0.001 = 0.1%)
    or None on any fetch failure.
    """
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines"
            f"?symbol={symbol}&interval=1m&limit={lookback + 1}"
        )
    except Exception:
        return None

    if not isinstance(klines, list) or len(klines) < 3:
        return None

    completed = klines[:-1]
    current   = klines[-1]

    try:
        last       = completed[-1]
        base_vol   = float(last[5])
        taker_buy  = float(last[9])
        taker_ratio = taker_buy / base_vol if base_vol > 0 else 0.5
    except Exception:
        taker_ratio = 0.5

    try:
        trade_count = int(completed[-1][8])
    except Exception:
        trade_count = None

    consecutive = 0
    try:
        for k in reversed(completed):
            o, c = float(k[1]), float(k[4])
            if c > o:
                if consecutive >= 0:
                    consecutive += 1
                else:
                    break
            elif c < o:
                if consecutive <= 0:
                    consecutive -= 1
                else:
                    break
            else:
                break
    except Exception:
        consecutive = 0

    vol_accel = None
    try:
        open_ms    = int(current[0])
        now_ms     = int(time.time() * 1000)
        elapsed_ms = max(now_ms - open_ms, 1_000)
        projected  = float(current[5]) * (60_000 / elapsed_ms)
        prior_vols = [float(k[5]) for k in completed[-min(5, len(completed)):]]
        avg_prior  = sum(prior_vols) / len(prior_vols) if prior_vols else None
        if avg_prior and avg_prior > 0:
            vol_accel = projected / avg_prior
    except Exception:
        pass

    sigma_1m = None
    try:
        closes = [float(k[4]) for k in completed]
        if len(closes) >= 3:
            rets = [
                math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i - 1] > 0 and closes[i] > 0
            ]
            if len(rets) >= 2:
                mean     = sum(rets) / len(rets)
                var      = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
                sigma_1m = math.sqrt(max(var, 0.0))
    except Exception:
        pass

    return {
        "taker_ratio":     taker_ratio,
        "consecutive_dir": consecutive,
        "vol_accel":       vol_accel,
        "trade_count":     trade_count,
        "sigma_1m":        sigma_1m,
    }
