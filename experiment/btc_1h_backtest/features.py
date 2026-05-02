"""
Features for the BTC 1H Up/Down backtest.

Inputs: parquet of 1m BTCUSDT klines.
Output: dataframe keyed by (hour_id_et, fire_minute) with all features needed
        by every strategy. All features at minute t use ONLY data ending
        at minute t (close of t-th completed bar is observable). The 1m close
        at minute t is what the bot would see when it polls right after the
        t-th minute closes.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

MINUTES_PER_YEAR = 365.0 * 24.0 * 60.0


def load_klines(parquet_path: Path) -> pd.DataFrame:
    df = duckdb.sql(f"select * from '{parquet_path}' order by open_time").df()
    # open_time is ms epoch UTC. Re-derive to keep it tz-aware and consistent.
    df["ts_utc"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("ts_utc").sort_index()
    return df


def attach_hour_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add UTC-hour key + minute-of-hour to every 1m bar.

    Polymarket BTC 1H markets are anchored to UTC-hour-aligned Binance candles
    (the slug just labels the hour in ET wall-clock; e.g. '3PM ET' → endDate
    20:00 UTC during EDT or 20:00 UTC during EST). Grouping by UTC hour matches
    the actual candle boundaries and avoids DST ambiguity.

    The column is named `hour_id_et` for backward compat with downstream code,
    but the value is the UTC-hour floor.
    """
    df = df.copy()
    df["hour_id_et"] = df.index.floor("h").tz_convert("UTC").tz_localize(None)
    df["minute_of_hour"] = df.index.minute
    return df


def compute_per_bar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds per-bar columns that depend only on data up to and including the bar:
      log_ret_1m, sigma_min (60-bar trailing std of log_ret, NOT including the bar's own ret),
      bar_range_pct, taker_ratio.

    'sigma_min' at row i is std(log_ret[i-60..i-1]) — i.e. trailing 60 bars
    ending the previous bar. Strictly no look-ahead from this bar's return.
    """
    df = df.copy()
    df["log_ret_1m"] = np.log(df["close"] / df["close"].shift(1))
    # Trailing 60-bar std ending at PREVIOUS bar (shift by 1 to drop current bar's ret).
    df["sigma_min"] = (
        df["log_ret_1m"]
        .shift(1)
        .rolling(window=60, min_periods=30)
        .std(ddof=1)
    )
    df["bar_range_pct"] = (df["high"] - df["low"]) / df["open"]
    # taker buy as fraction of volume; if volume==0, leave NaN
    df["taker_ratio"] = np.where(df["volume"] > 0, df["taker_buy_base"] / df["volume"], np.nan)
    return df


def build_hour_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    """One row per ET hour: hour_id_et, hour_open, hour_close, winner ('up'/'down'), n_bars."""
    g = df.groupby("hour_id_et", sort=True)
    out = pd.DataFrame({
        "hour_open":  g["open"].first(),
        "hour_close": g["close"].last(),
        "n_bars":     g.size(),
    }).reset_index()
    # Polymarket rule: close >= open → Up wins (ties resolve Up).
    out["winner"] = np.where(out["hour_close"] >= out["hour_open"], "up", "down")
    return out


def build_fire_features(
    bars: pd.DataFrame,
    fire_minutes: range = range(30, 51),
) -> pd.DataFrame:
    """
    For each (hour_id_et, fire_minute), compute features the strategies need:
      hour_open, spot_t, log_disp, sigma_min, displacement_z,
      tau_years, sigma_ann, p_bs_up,
      taker_ratio_5m (rolling-5 mean ending at fire_minute, includes minute t),
      vol_burst (1 if last-5-min max range > 3 * 60-min median range, ending at minute t-1).

    Strict no-look-ahead: every feature is computed from bars whose close <= close
    of fire_minute t. (Equivalent to: we observe close at end of minute t, then act.)
    """
    bars = bars.copy()
    # Pre-compute hour_open per row from groupby.first
    hour_open_map = bars.groupby("hour_id_et")["open"].transform("first")
    bars["hour_open"] = hour_open_map

    # Rolling features ENDING AT MINUTE t (i.e. include the bar at minute t).
    # taker_ratio rolling mean over 5 bars ending at t.
    bars["taker_ratio_5m"] = bars["taker_ratio"].rolling(5, min_periods=3).mean()

    # vol_burst computed from bars ending at minute t-1 (so the bot's decision at
    # minute t uses only data through t-1's range). Use shift(1).
    range_shift1 = bars["bar_range_pct"].shift(1)
    bars["range_5m_max_prev"] = range_shift1.rolling(5, min_periods=3).max()
    bars["range_60m_med_prev"] = range_shift1.rolling(60, min_periods=30).median()
    bars["vol_burst"] = (bars["range_5m_max_prev"] > 3.0 * bars["range_60m_med_prev"]).astype(int)

    # Filter to fire-minute rows only
    fm_set = set(fire_minutes)
    fire = bars[bars["minute_of_hour"].isin(fm_set)].copy()
    fire["fire_minute"] = fire["minute_of_hour"].astype(int)

    # log displacement at end of minute t vs hour open
    fire["spot_t"] = fire["close"]
    fire["log_disp"] = np.log(fire["spot_t"] / fire["hour_open"])

    # Z-score of log displacement: under iid GBM with sigma per minute,
    # std(ln(S_t/S_0)) at minute t = sigma_min * sqrt(t).
    # Note fire_minute is 0..59, but the displacement at the END of minute t
    # has had (t+1) minutes of returns aggregated (minutes 0..t).
    # We use t+1 here for correct units.
    minutes_elapsed = fire["fire_minute"].astype(float) + 1.0
    fire["displacement_z"] = fire["log_disp"] / (fire["sigma_min"] * np.sqrt(minutes_elapsed))

    # Black-Scholes digital up probability at minute t.
    # tau is the minutes remaining in the hour (60 - (t+1)) since at end of minute t
    # there are 60-t-1 remaining minutes... actually:
    #   Hour spans minutes 0..59. At END of minute t, 59 - t full minutes remain
    #   PLUS the partial completion of minute 59's tick? No — at end of minute t,
    #   minutes t+1..59 remain. That's (59 - t) minutes.
    minutes_remaining = 59.0 - fire["fire_minute"].astype(float)
    fire["minutes_remaining"] = minutes_remaining
    tau_years = minutes_remaining / MINUTES_PER_YEAR
    fire["tau_years"] = tau_years
    sigma_ann = fire["sigma_min"] * np.sqrt(MINUTES_PER_YEAR)
    fire["sigma_ann"] = sigma_ann

    # d2 = (ln(S/K) + (-0.5 * sigma_ann^2 * tau)) / (sigma_ann * sqrt(tau))
    sqrt_tau = np.sqrt(tau_years)
    d2 = (np.log(fire["spot_t"] / fire["hour_open"]) - 0.5 * sigma_ann**2 * tau_years) / (sigma_ann * sqrt_tau)
    fire["p_bs_up"] = 0.5 * (1.0 + _erf(d2 / np.sqrt(2.0)))
    fire["p_bs_up"] = fire["p_bs_up"].clip(lower=0.0, upper=1.0)

    keep = [
        "hour_id_et", "fire_minute", "minutes_remaining",
        "hour_open", "spot_t", "log_disp", "sigma_min", "sigma_ann",
        "displacement_z", "tau_years", "p_bs_up",
        "taker_ratio_5m", "vol_burst",
    ]
    return fire[keep].reset_index(drop=True)


def _erf(x):
    """Vectorized erf via numpy's vectorized math."""
    # numpy ships scipy-free erf via np.special? np doesn't have erf directly.
    # Use math.erf via np.vectorize, or use the series? Simplest: np.frompyfunc(math.erf,1,1)
    import math
    if isinstance(x, pd.Series):
        return x.apply(math.erf)
    return np.frompyfunc(math.erf, 1, 1)(x).astype(float)
