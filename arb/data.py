"""Hourly OHLC fetch + parquet/CSV cache.

yfinance hourly history is capped at ~730 days. We grab the max window in one
shot per ticker, cache by ticker+interval, and reuse on subsequent runs unless
the cache is older than --max-age-hours.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd
import yfinance as yf

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)


def _cache_path(ticker: str, interval: str) -> Path:
    return CACHE_DIR / f"{ticker.replace('/', '_')}_{interval}.csv"


def _is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    age_h = (time.time() - path.stat().st_mtime) / 3600.0
    return age_h < max_age_hours


def fetch_one(
    ticker: str,
    interval: str = "1h",
    period: str = "730d",
    max_age_hours: float = 12.0,
    force: bool = False,
) -> pd.DataFrame:
    """Return a single-ticker DataFrame with a tz-naive UTC DatetimeIndex.

    Columns: open, high, low, close, volume.
    """
    path = _cache_path(ticker, interval)
    if not force and _is_fresh(path, max_age_hours):
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        return df

    raw = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if raw is None or raw.empty:
        raise RuntimeError(f"no data returned for {ticker}")

    # yfinance returns a MultiIndex column even for one ticker — flatten.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw = raw.rename(columns=str.lower)
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in raw.columns]
    df = raw[keep].copy()

    # Normalize index to tz-naive UTC for clean joins across tickers.
    idx = pd.to_datetime(df.index, utc=True)
    df.index = idx.tz_convert(None)
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    df.to_csv(path)
    return df


def fetch_many(
    tickers: Iterable[str],
    interval: str = "1h",
    period: str = "730d",
    max_age_hours: float = 12.0,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for t in tickers:
        try:
            out[t] = fetch_one(t, interval=interval, period=period,
                               max_age_hours=max_age_hours, force=force)
            print(f"  [data] {t}: {len(out[t])} bars  "
                  f"{out[t].index.min().date()} .. {out[t].index.max().date()}")
        except Exception as exc:
            print(f"  [data] {t}: FAILED ({exc})")
    return out


def aligned_closes(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Inner-join close prices across all tickers on the shared timestamps."""
    parts = []
    for t, df in frames.items():
        if "close" not in df.columns:
            continue
        s = df["close"].rename(t)
        parts.append(s)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, axis=1, join="inner").dropna()
    return out
