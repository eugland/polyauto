"""
Pre-candle features for the "early-entry" backtest.

For each target hour H (a Polymarket BTC 1H market resolving at H+60min),
the bot fires at some minute m before H opens, where m ∈ [-40, -10] (i.e.
70–100 min before resolution). At fire time the target candle has NOT yet
started, so we cannot use intra-candle displacement. Instead, all features
come from data observable BEFORE H opens — multi-hour returns, 1H-bar
indicators (RSI), and rolling taker-buy flow.

Strict no-look-ahead:
  - 1H-bar features (RSI, sigma_24h, ret_Nh, taker_ratio_Nh) computed from
    1H bars whose CLOSE_TIME <= fire_time.
  - At fire time t = H + m where m ∈ [-40,-10], the most recent COMPLETED 1H
    bar is the one with open_time = H - 2:00 (close_time = H - 1:00).
    So we shift bars_1h by 2 hours when joining onto target_hour H.
  - spot_at_fire is the close of the 1m bar at time t (observable end-of-minute).
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def load_klines_1m_and_1h(parquet_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = duckdb.sql(f"select * from '{parquet_path}' order by open_time").df()
    df["ts_utc"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    bars_1m = df.set_index("ts_utc").sort_index()
    bars_1h = bars_1m[["open", "high", "low", "close", "volume", "taker_buy_base"]].resample("1h").agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
        "volume": "sum", "taker_buy_base": "sum",
    })
    return bars_1m, bars_1h


def compute_rsi_wilder(closes: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI — uses exponential smoothing of gains/losses."""
    delta = closes.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def annotate_1h(bars_1h: pd.DataFrame) -> pd.DataFrame:
    """Add per-1H-bar indicators. Each row is observable AT THE BAR'S CLOSE."""
    h = bars_1h.copy()
    h["ret_1h"] = np.log(h["close"] / h["close"].shift(1))
    h["ret_2h"] = np.log(h["close"] / h["close"].shift(2))
    h["ret_3h"] = np.log(h["close"] / h["close"].shift(3))
    h["ret_4h"] = np.log(h["close"] / h["close"].shift(4))
    h["taker_ratio_1h"] = np.where(h["volume"] > 0, h["taker_buy_base"] / h["volume"], np.nan)
    h["taker_ratio_2h"] = h["taker_ratio_1h"].rolling(2, min_periods=2).mean()
    h["taker_ratio_3h"] = h["taker_ratio_1h"].rolling(3, min_periods=3).mean()
    h["sigma_24h"] = h["ret_1h"].rolling(24, min_periods=12).std()
    h["sigma_24h_med_30d"] = h["sigma_24h"].rolling(720, min_periods=240).median()
    h["rsi_7"]  = compute_rsi_wilder(h["close"], period=7)
    h["rsi_14"] = compute_rsi_wilder(h["close"], period=14)
    h["rsi_28"] = compute_rsi_wilder(h["close"], period=28)
    # Bollinger bands on 1H closes (20-period, 2σ)
    sma20 = h["close"].rolling(20, min_periods=20).mean()
    sd20  = h["close"].rolling(20, min_periods=20).std()
    h["bb_pct"] = (h["close"] - sma20) / (2.0 * sd20)   # 1.0 = upper band, -1.0 = lower
    # MACD: 12/26 EMAs, 9-EMA signal, hist = macd - signal.
    ema12 = h["close"].ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = h["close"].ewm(span=26, adjust=False, min_periods=26).mean()
    macd_line = ema12 - ema26
    macd_sig  = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    h["macd_hist"] = macd_line - macd_sig
    # Z-score MACD-hist by its 30-day rolling std so the threshold is regime-agnostic.
    h["macd_hist_z"] = h["macd_hist"] / h["macd_hist"].rolling(720, min_periods=240).std()
    # Distance from 24-hour high / low (in σ_1m·√24·60 units roughly)
    rolling_max = h["close"].rolling(24, min_periods=24).max()
    rolling_min = h["close"].rolling(24, min_periods=24).min()
    h["dist_to_24h_hi_pct"] = (h["close"] - rolling_max) / rolling_max
    h["dist_to_24h_lo_pct"] = (h["close"] - rolling_min) / rolling_min
    return h


def build_pre_candle_features(
    bars_1m: pd.DataFrame,
    bars_1h: pd.DataFrame,
    fire_offsets_min: list[int] | None = None,
) -> pd.DataFrame:
    """
    Returns one row per (target_hour, fire_offset_min). Features are observable
    at fire_time = target_hour + fire_offset_min. fire_offset_min is negative.

    The target candle is the 1H Polymarket market that opens at `target_hour`
    and resolves 60 min later. The bot fires `|fire_offset_min|` minutes BEFORE
    target_hour, i.e. fire_offset_min ∈ [-40, -10] = 70–100 min before close.
    """
    if fire_offsets_min is None:
        fire_offsets_min = list(range(-40, -9))  # -40, -39, ..., -10

    h = annotate_1h(bars_1h)

    # Features available at fire time t are from the 1H bar whose CLOSE_TIME <= t.
    # For fire time t = H + m with m in [-40, -10]:
    #   - The 1H bar at open_time = H-1:00 closes at H:00 — NOT yet closed at t.
    #   - The 1H bar at open_time = H-2:00 closes at H-1:00 — IS closed at t.
    # So the "most recent completed bar" is at open_time = H - 2 hours.
    # Shift `h` by 2 rows (2 hours) so h_shift.loc[H] holds the features available
    # at fire time. Equivalent to: align bars closed at H-1:00 onto the index H.
    feats_at_target = h[[
        "ret_1h","ret_2h","ret_3h","ret_4h",
        "taker_ratio_1h","taker_ratio_2h","taker_ratio_3h",
        "sigma_24h","sigma_24h_med_30d",
        "rsi_7","rsi_14","rsi_28",
        "bb_pct","macd_hist","macd_hist_z",
        "dist_to_24h_hi_pct","dist_to_24h_lo_pct",
    ]].shift(2)

    # spot_at_fire: 1m bar's close at fire_time.
    close_1m = bars_1m["close"]

    # Build (target_hour, fire_offset) rows
    target_hours = bars_1h.index  # tz-aware UTC
    pieces: list[pd.DataFrame] = []
    for m in fire_offsets_min:
        ft = target_hours + pd.Timedelta(minutes=m)
        spot = close_1m.reindex(ft).values
        feats = feats_at_target.reindex(target_hours)
        df = pd.DataFrame({
            "target_hour": target_hours,
            "fire_offset_min": m,
            "fire_time": ft,
            "spot_at_fire": spot,
            "ret_1h": feats["ret_1h"].values,
            "ret_2h": feats["ret_2h"].values,
            "ret_3h": feats["ret_3h"].values,
            "ret_4h": feats["ret_4h"].values,
            "taker_ratio_1h": feats["taker_ratio_1h"].values,
            "taker_ratio_2h": feats["taker_ratio_2h"].values,
            "taker_ratio_3h": feats["taker_ratio_3h"].values,
            "sigma_24h": feats["sigma_24h"].values,
            "sigma_24h_med_30d": feats["sigma_24h_med_30d"].values,
            "rsi_7":  feats["rsi_7"].values,
            "rsi_14": feats["rsi_14"].values,
            "rsi_28": feats["rsi_28"].values,
            "bb_pct": feats["bb_pct"].values,
            "macd_hist": feats["macd_hist"].values,
            "macd_hist_z": feats["macd_hist_z"].values,
            "dist_to_24h_hi_pct": feats["dist_to_24h_hi_pct"].values,
            "dist_to_24h_lo_pct": feats["dist_to_24h_lo_pct"].values,
        })
        # UTC hour-of-day at target_hour for time-of-day strategies
        df["target_utc_hour"] = pd.to_datetime(df["target_hour"]).dt.hour
        df["target_dow"] = pd.to_datetime(df["target_hour"]).dt.dayofweek
        pieces.append(df)

    return pd.concat(pieces, ignore_index=True)


def build_outcomes(bars_1h: pd.DataFrame) -> pd.DataFrame:
    """One row per target_hour: open, close, winner. Polymarket rule: close >= open → Up."""
    out = bars_1h[["open", "close"]].copy()
    out["winner"] = np.where(out["close"] >= out["open"], "up", "down")
    out = out.reset_index().rename(columns={"ts_utc": "target_hour"})
    return out
