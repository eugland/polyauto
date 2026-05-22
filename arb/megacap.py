"""Mega-cap concentration mean-reversion.

Hypothesis: when the top-heavy / cap-weighted side of an index drifts too far
from the equal-weight or broad-market side, the gap mean-reverts. The basket-
NAV test surprised us by showing this at +0.82 Sharpe (low cost) for SPY's
top-10 vs the rest of SPY. We now test the cleaner formulations:

  - SPY vs RSP        : same 500 stocks, cap-weighted vs equal-weighted.
                        Pure "concentration" factor.
  - MAG7 vs RSP       : equal-weighted basket of Mag-7 vs broad market.
                        Raw "mega-cap" factor.
  - QQQ vs SPY        : tradeable proxy. Nasdaq-100 (tech-heavy mega-cap) vs S&P 500.

Backtest: spread = log(long) - (alpha + beta * log(short)), rolling OLS, no
look-ahead. Trade when |z| >= entry, flatten when |z| <= exit. Cost charged on
every leg of the trade — basket trades cost more.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

MAG7 = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]


def equal_weight_log_basket(closes: pd.DataFrame, tickers: List[str]
                            ) -> Tuple[pd.Series, int]:
    """Equal-weight log-price basket. Returns (log_basket, n_used)."""
    avail = [t for t in tickers if t in closes.columns]
    if not avail:
        return pd.Series(dtype=float), 0
    log_p = np.log(closes[avail])
    return log_p.mean(axis=1), len(avail)


def _halflife(spread: pd.Series) -> float:
    s = spread.dropna()
    if len(s) < 100: return float("nan")
    lag = s.shift(1).dropna()
    delta = s.diff().dropna()
    df = pd.concat([delta, lag], axis=1, join="inner").dropna()
    if len(df) < 100: return float("nan")
    y = df.iloc[:, 0].values
    X = np.column_stack([np.ones(len(df)), df.iloc[:, 1].values])
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    phi = coef[1]
    if phi >= 0 or not np.isfinite(phi): return float("nan")
    return float(-np.log(2.0) / phi)


@dataclass
class SpreadResult:
    name: str
    interval: str
    n_bars: int
    return_corr: float            # corr of long & short log returns
    halflife_bars: float
    rolling_beta_mean: float
    sharpe: float
    annual_return: float
    annual_vol: float
    max_drawdown: float
    hit_rate: float
    n_trades: int
    legs_per_flip: int
    cost_bps_per_leg: float

    def to_dict(self): return asdict(self)


def backtest_spread(
    long_log: pd.Series,
    short_log: pd.Series,
    name: str,
    *,
    interval: str,                 # "1h" or "1d", for annualization
    legs_per_flip: int,            # total legs to round-trip the whole spread
    fit_window: int,               # OLS window for hedge ratio
    z_window: int,                 # rolling window for z-score
    z_entry: float,
    z_exit: float,
    cost_bps_per_leg: float,
) -> Tuple[SpreadResult, pd.Series, pd.Series, pd.Series]:
    """Returns (metrics, equity_curve, spread_z, rolling_beta)."""
    bars_per_year = 252 * 6.5 if interval == "1h" else 252

    df = pd.concat(
        [long_log.rename("L"), short_log.rename("S")], axis=1, join="inner"
    ).dropna()
    if len(df) < max(fit_window + z_window + 10, 200):
        empty = pd.Series(dtype=float)
        return SpreadResult(name, interval, len(df), float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"), float("nan"),
                            float("nan"), float("nan"), 0, legs_per_flip,
                            cost_bps_per_leg), empty, empty, empty

    L = df["L"]; S = df["S"]
    rL = L.diff(); rS = S.diff()
    ret_corr = float(rL.corr(rS))

    # Rolling OLS on log prices: L = alpha + beta * S
    mL = L.rolling(fit_window).mean()
    mS = S.rolling(fit_window).mean()
    vS = S.rolling(fit_window).var(ddof=0)
    cov = L.rolling(fit_window).cov(S)
    beta = (cov / vS).clip(0.1, 5.0).rename("beta")
    alpha = (mL - beta * mS).rename("alpha")

    spread = (L - (alpha + beta * S)).rename("spread")
    hl = _halflife(spread)

    zmean = spread.rolling(z_window).mean()
    zstd = spread.rolling(z_window).std(ddof=0)
    z = (spread - zmean) / zstd

    # Spread-return per bar: d(spread)/dt = dL - beta * dS (beta as-of t-1 to avoid peek)
    spread_ret = rL - beta.shift(1) * rS

    pos = pd.Series(0.0, index=df.index)
    cur = 0.0
    for ts, zv in z.items():
        if not np.isfinite(zv):
            pos.loc[ts] = 0.0; continue
        if cur == 0:
            if zv >= z_entry: cur = -1.0    # long-leg rich -> short long, long short
            elif zv <= -z_entry: cur = +1.0
        else:
            if abs(zv) <= z_exit: cur = 0.0
        pos.loc[ts] = cur

    raw_pnl = pos.shift(1) * spread_ret
    cost = (cost_bps_per_leg / 1e4) * legs_per_flip * pos.diff().abs().fillna(0.0)
    pnl = (raw_pnl - cost).fillna(0.0)

    equity = (1.0 + pnl).cumprod()
    n = len(pnl)
    years = max(n / bars_per_year, 1e-9)
    total = float(equity.iloc[-1] - 1.0) if n else 0.0
    ann_ret = float((1.0 + total) ** (1.0 / years) - 1.0) if total > -1 else float("nan")
    ann_vol = float(pnl.std(ddof=0) * np.sqrt(bars_per_year))
    sharpe = float(ann_ret / ann_vol) if ann_vol > 0 else float("nan")
    dd = float((equity / equity.cummax() - 1.0).min()) if n else 0.0
    nonzero = pnl[pnl != 0]
    hit = float((nonzero > 0).mean()) if len(nonzero) else float("nan")
    n_trades = int((pos.diff().abs() > 1e-9).sum())

    res = SpreadResult(
        name=name, interval=interval, n_bars=n,
        return_corr=ret_corr, halflife_bars=hl,
        rolling_beta_mean=float(beta.dropna().mean()),
        sharpe=sharpe, annual_return=ann_ret, annual_vol=ann_vol,
        max_drawdown=dd, hit_rate=hit, n_trades=n_trades,
        legs_per_flip=legs_per_flip, cost_bps_per_leg=cost_bps_per_leg,
    )
    return res, equity, z, beta
