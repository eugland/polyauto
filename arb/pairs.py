"""Diagnostics that quantify whether a pair has tradable structure.

Three measures per pair:
  - Pearson correlation of log returns (co-movement strength)
  - Engle-Granger cointegration p-value (is the spread stationary?)
  - Lead-lag cross-correlation of returns at lags -5..+5 hours (who moves first?)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant


@dataclass
class PairDiagnostics:
    leader: str
    follower: str
    n_bars: int
    return_corr: float            # Pearson correlation of log returns
    coint_pvalue: float           # Engle-Granger p; lower => more cointegrated
    hedge_ratio: float            # follower ~ alpha + beta * leader (OLS on prices)
    best_lag: int                 # +k => leader leads follower by k bars (max |corr|)
    best_lag_corr: float          # correlation at best_lag
    spread_halflife_bars: float   # OU half-life of the spread (NaN if non-stationary)


def _log_returns(series: pd.Series) -> pd.Series:
    return np.log(series).diff().dropna()


def _ols_hedge(y: pd.Series, x: pd.Series) -> Tuple[float, float, pd.Series]:
    """Fit y = alpha + beta * x. Return (alpha, beta, spread=y - (alpha+beta*x))."""
    X = add_constant(x.values)
    res = OLS(y.values, X).fit()
    alpha = float(res.params[0])
    beta = float(res.params[1])
    spread = y - (alpha + beta * x)
    return alpha, beta, spread


def _halflife(spread: pd.Series) -> float:
    """OU half-life: regress d_spread on lag(spread); hl = -ln(2) / phi."""
    s = spread.dropna()
    if len(s) < 30:
        return float("nan")
    lag = s.shift(1).dropna()
    delta = s.diff().dropna()
    aligned = pd.concat([delta, lag], axis=1, join="inner").dropna()
    if len(aligned) < 30:
        return float("nan")
    y = aligned.iloc[:, 0].values
    X = add_constant(aligned.iloc[:, 1].values)
    res = OLS(y, X).fit()
    phi = float(res.params[1])
    if phi >= 0 or not np.isfinite(phi):
        return float("nan")
    return float(-np.log(2.0) / phi)


def _lead_lag(rx: pd.Series, ry: pd.Series, max_lag: int = 5) -> Tuple[int, float]:
    """Return (best_lag, corr_at_best_lag).

    lag = +k means leader's return at t-k correlates with follower's return at t
    (leader leads). lag = -k means the reverse.
    """
    best_lag, best_corr = 0, 0.0
    for k in range(-max_lag, max_lag + 1):
        if k >= 0:
            a = rx.shift(k)
            b = ry
        else:
            a = rx
            b = ry.shift(-k)
        merged = pd.concat([a, b], axis=1, join="inner").dropna()
        if len(merged) < 50:
            continue
        c = float(merged.iloc[:, 0].corr(merged.iloc[:, 1]))
        if abs(c) > abs(best_corr):
            best_lag, best_corr = k, c
    return best_lag, best_corr


def analyze(leader: pd.Series, follower: pd.Series,
            leader_name: str, follower_name: str) -> PairDiagnostics:
    aligned = pd.concat(
        [leader.rename("L"), follower.rename("F")], axis=1, join="inner"
    ).dropna()
    if len(aligned) < 100:
        return PairDiagnostics(
            leader=leader_name, follower=follower_name,
            n_bars=len(aligned),
            return_corr=float("nan"), coint_pvalue=float("nan"),
            hedge_ratio=float("nan"), best_lag=0, best_lag_corr=float("nan"),
            spread_halflife_bars=float("nan"),
        )

    rL = _log_returns(aligned["L"])
    rF = _log_returns(aligned["F"])
    r_corr = float(rL.corr(rF))

    try:
        _, p, _ = coint(aligned["L"].values, aligned["F"].values)
        coint_p = float(p)
    except Exception:
        coint_p = float("nan")

    _, beta, spread = _ols_hedge(aligned["F"], aligned["L"])
    hl = _halflife(spread)
    lag, lag_c = _lead_lag(rL, rF, max_lag=5)

    return PairDiagnostics(
        leader=leader_name, follower=follower_name,
        n_bars=len(aligned),
        return_corr=r_corr,
        coint_pvalue=coint_p,
        hedge_ratio=beta,
        best_lag=lag,
        best_lag_corr=lag_c,
        spread_halflife_bars=hl,
    )
