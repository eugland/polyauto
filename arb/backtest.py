"""Two backtest engines, both bar-based with realistic frictions.

(1) Spread mean-reversion
    - Fit hedge ratio via rolling OLS on the in-sample window (no look-ahead).
    - z-score the residual spread on a rolling lookback.
    - Enter long-spread when z <= -entry (short leader, long follower scaled);
      enter short-spread when z >= +entry; flatten when |z| <= exit.
    - PnL = position * d(spread).

(2) Lead-lag momentum
    - Signal at bar t: sign of leader's return at t (or t-1 if leader truly leads).
    - Trade follower from t -> t+1 in that direction.
    - PnL = signal * follower_return.

Both report annualized Sharpe (252*6.5 bars/yr), max drawdown, hit rate, total
return, and turnover. Transaction cost subtracted on every position change.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict

import numpy as np
import pandas as pd

# US-equity intraday year: ~252 sessions * 6.5 hours = 1638 bars.
BARS_PER_YEAR = 252 * 6.5


@dataclass
class BacktestResult:
    strategy: str
    leader: str
    follower: str
    n_bars: int
    total_return: float
    annual_return: float
    annual_vol: float
    sharpe: float
    max_drawdown: float
    hit_rate: float
    turnover: float
    n_trades: int

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


def _metrics(pnl: pd.Series, position: pd.Series, strategy: str,
             leader: str, follower: str) -> BacktestResult:
    pnl = pnl.fillna(0.0)
    equity = (1.0 + pnl).cumprod()
    total_ret = float(equity.iloc[-1] - 1.0) if len(equity) else 0.0
    n = len(pnl)
    years = max(n / BARS_PER_YEAR, 1e-9)
    ann_ret = float((1.0 + total_ret) ** (1.0 / years) - 1.0) if total_ret > -1 else float("nan")
    ann_vol = float(pnl.std(ddof=0) * np.sqrt(BARS_PER_YEAR))
    sharpe = float(ann_ret / ann_vol) if ann_vol > 0 else float("nan")
    dd = float((equity / equity.cummax() - 1.0).min()) if len(equity) else 0.0
    nonzero = pnl[pnl != 0]
    hit = float((nonzero > 0).mean()) if len(nonzero) else float("nan")
    pos_change = position.diff().abs().fillna(0.0)
    turnover = float(pos_change.sum())
    n_trades = int((pos_change > 1e-9).sum())
    return BacktestResult(
        strategy=strategy, leader=leader, follower=follower,
        n_bars=n, total_return=total_ret,
        annual_return=ann_ret, annual_vol=ann_vol, sharpe=sharpe,
        max_drawdown=dd, hit_rate=hit,
        turnover=turnover, n_trades=n_trades,
    )


# ---------- (1) Spread mean-reversion ----------

def backtest_spread_mr(
    leader_close: pd.Series, follower_close: pd.Series,
    leader_name: str, follower_name: str,
    *,
    fit_window: int = 252,
    z_window: int = 60,
    z_entry: float = 2.0,
    z_exit: float = 0.5,
    cost_bps: float = 2.0,
) -> tuple[BacktestResult, pd.Series]:
    """Returns (metrics, equity_curve).

    Hedge ratio recomputed on a rolling window so it doesn't peek. Position
    is 'units of spread': +1 long spread = long follower, short leader*beta.
    """
    df = pd.concat(
        [leader_close.rename("L"), follower_close.rename("F")],
        axis=1, join="inner",
    ).dropna()
    if len(df) < max(fit_window + z_window + 10, 200):
        empty = pd.Series(dtype=float)
        return _metrics(empty, empty, "spread_mr", leader_name, follower_name), empty

    L = df["L"]
    F = df["F"]

    # Rolling OLS beta of F on L; rolling alpha too.
    mean_L = L.rolling(fit_window).mean()
    mean_F = F.rolling(fit_window).mean()
    var_L = L.rolling(fit_window).var(ddof=0)
    cov_LF = L.rolling(fit_window).cov(F)
    beta = (cov_LF / var_L).rename("beta")
    alpha = (mean_F - beta * mean_L).rename("alpha")

    spread = (F - (alpha + beta * L)).rename("spread")

    z_mean = spread.rolling(z_window).mean()
    z_std = spread.rolling(z_window).std(ddof=0)
    z = (spread - z_mean) / z_std

    # Position in spread units. Walk z-state without look-ahead.
    pos = pd.Series(0.0, index=df.index)
    cur = 0.0
    for ts, zv in z.items():
        if not np.isfinite(zv):
            pos.loc[ts] = 0.0
            continue
        if cur == 0:
            if zv >= z_entry:
                cur = -1.0   # short spread (spread is high, will revert down)
            elif zv <= -z_entry:
                cur = +1.0   # long spread
        else:
            if abs(zv) <= z_exit:
                cur = 0.0
        pos.loc[ts] = cur

    # PnL: position(t-1) * change in spread from t-1 to t, scaled by gross notional.
    # We approximate notional as |F| + |beta*L| at the prior bar; then return-ize.
    notional = (F.abs() + (beta.abs() * L.abs())).shift(1)
    d_spread = spread.diff()
    raw_pnl = pos.shift(1) * d_spread / notional

    # Transaction cost: charge cost_bps on the gross turnover at each rebalance.
    cost = (cost_bps / 1e4) * pos.diff().abs().fillna(0.0)
    pnl = (raw_pnl - cost).fillna(0.0)

    equity = (1.0 + pnl).cumprod()
    res = _metrics(pnl, pos, "spread_mr", leader_name, follower_name)
    return res, equity


# ---------- (2) Lead-lag momentum ----------

def backtest_lead_lag(
    leader_close: pd.Series, follower_close: pd.Series,
    leader_name: str, follower_name: str,
    *,
    lag: int = 1,
    threshold: float = 0.0,
    cost_bps: float = 2.0,
) -> tuple[BacktestResult, pd.Series]:
    """If leader's return at t-lag exceeds +threshold, go long follower for
    bar t->t+1; below -threshold, go short. Otherwise flat."""
    df = pd.concat(
        [leader_close.rename("L"), follower_close.rename("F")],
        axis=1, join="inner",
    ).dropna()
    if len(df) < 50:
        empty = pd.Series(dtype=float)
        return _metrics(empty, empty, "lead_lag", leader_name, follower_name), empty

    rL = np.log(df["L"]).diff()
    rF = np.log(df["F"]).diff()

    signal_src = rL.shift(max(lag, 1))   # leader's return from `lag` bars ago
    sig = pd.Series(0.0, index=df.index)
    sig[signal_src > threshold] = +1.0
    sig[signal_src < -threshold] = -1.0

    pnl_gross = sig * rF
    cost = (cost_bps / 1e4) * sig.diff().abs().fillna(0.0)
    pnl = (pnl_gross - cost).fillna(0.0)

    equity = (1.0 + pnl).cumprod()
    res = _metrics(pnl, sig, "lead_lag", leader_name, follower_name)
    return res, equity


# ---------- benchmark: long follower ----------

def benchmark_buy_hold(close: pd.Series, leader_name: str, follower_name: str) -> BacktestResult:
    r = np.log(close).diff().fillna(0.0)
    # Convert log returns to simple for compounding metrics.
    r_simple = np.expm1(r)
    pos = pd.Series(1.0, index=close.index)
    return _metrics(r_simple, pos, "buy_hold_follower", leader_name, follower_name)
