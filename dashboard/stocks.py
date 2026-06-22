"""Market data via yfinance — equities and crypto (e.g. SPY, AAPL, BTC-USD)."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf

log = logging.getLogger("dashboard.stocks")

# range -> (yfinance period, candle interval). Intraday for short ranges so the
# 1D/1W charts have more than a couple of points.
_RANGE = {
    "1d":  ("1d", "5m"),
    "5d":  ("5d", "30m"),
    "1mo": ("1mo", "1d"),
    "1y":  ("1y", "1d"),
    "5y":  ("5y", "1wk"),
    "10y": ("10y", "1mo"),
}


def history(symbol: str, rng: str = "1y") -> dict:
    """Close series for ``symbol`` over ``rng`` (e.g. '1y', '1d', '10y')."""
    period, interval = _RANGE.get(rng, ("1y", "1d"))
    fmt = "%Y-%m-%d %H:%M" if interval.endswith("m") else "%Y-%m-%d"
    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
        points = [
            {"t": idx.strftime(fmt), "close": round(float(row["Close"]), 4)}
            for idx, row in df.iterrows()
            if row["Close"] == row["Close"]  # drop NaN
        ]
        return {"symbol": symbol, "range": rng, "points": points}
    except Exception as exc:  # noqa: BLE001
        log.warning("history(%s) failed: %s", symbol, exc)
        return {"symbol": symbol, "range": rng, "points": [], "error": str(exc)}


_QUOTE_CACHE: dict[str, tuple[float, dict]] = {}  # symbol -> (fetched_at, quote)
_QUOTE_TTL = 30.0


def _quote_one(sym: str) -> dict:
    now = time.time()
    hit = _QUOTE_CACHE.get(sym)
    if hit and now - hit[0] < _QUOTE_TTL:
        return hit[1]
    try:
        df = yf.Ticker(sym).history(period="5d", auto_adjust=True)
        closes = [float(c) for c in df["Close"].tolist() if c == c]
        if not closes:
            return {"symbol": sym, "price": None, "error": "no data"}
        last = closes[-1]
        prev = closes[-2] if len(closes) > 1 else last
        change = last - prev
        q = {
            "symbol": sym,
            "price": round(last, 4),
            "change": round(change, 4),
            "change_pct": round((change / prev * 100) if prev else 0.0, 2),
        }
        _QUOTE_CACHE[sym] = (now, q)  # cache successes only
        return q
    except Exception as exc:  # noqa: BLE001
        log.warning("quote(%s) failed: %s", sym, exc)
        return hit[1] if hit else {"symbol": sym, "price": None, "error": str(exc)}


def quotes(symbols: list[str]) -> list[dict]:
    """Last close + day change per symbol — fetched concurrently, cached ~30s."""
    if not symbols:
        return []
    with ThreadPoolExecutor(max_workers=min(8, len(symbols))) as ex:
        return list(ex.map(_quote_one, symbols))
