"""ETF basket-vs-NAV statistical arbitrage.

Idea: a sector ETF is supposed to track the market-cap-weighted average of its
holdings. We build a *synthetic NAV* from the top-N constituents we can fetch,
measure the log-price residual `r_t = log(ETF_t) - sum_i w_i * log(P_i_t)`,
z-score it on a rolling window, and trade dollar-neutral:

    z >= +entry  -> short ETF, long basket (ETF is rich)
    z <= -entry  -> long ETF, short basket (ETF is cheap)
    |z| <= exit  -> flat

PnL per bar (dollar-neutral with unit gross-per-leg notional):
    pnl_t = pos_etf(t-1) * etf_logret_t + pos_basket(t-1) * basket_logret_t
          = pos_etf(t-1) * (etf_logret_t - basket_logret_t)        (since pos_basket = -pos_etf)
          = -pos_etf(t-1) * d_spread_t

Cost accounting is honest: every time the position flips, we pay ETF leg cost
PLUS one stock leg cost per constituent. That's the real make-or-break.

Holdings are hardcoded as fixed weights — approximations of recent SSGA
sector-SPDR snapshots. The backtest is best-effort, not production-grade index
arb (which needs live constituent updates and rebalance dates).
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

BARS_PER_YEAR = 252 * 6.5


# Approximate top-10 weights as of late-2024 / early-2025 snapshots from SSGA.
# Treat as static for the backtest — real ETFs rebalance quarterly, but
# market-cap-weighted sector funds drift with prices anyway between rebalances.
SECTOR_HOLDINGS: Dict[str, List[Tuple[str, float]]] = {
    "XLK": [
        ("AAPL", 0.21), ("MSFT", 0.21), ("NVDA", 0.20),
        ("AVGO", 0.05), ("ORCL", 0.03), ("CRM",  0.03),
        ("CSCO", 0.02), ("IBM",  0.02), ("AMD",  0.02), ("ADBE", 0.02),
    ],
    "XLF": [
        ("BRK-B", 0.12), ("JPM", 0.10), ("V",   0.07), ("MA",  0.06),
        ("BAC",   0.04), ("WFC", 0.04), ("GS",  0.03), ("MS",  0.03),
        ("BLK",   0.03), ("AXP", 0.03),
    ],
    "XLE": [
        ("XOM", 0.22), ("CVX", 0.17), ("COP", 0.08), ("EOG", 0.04),
        ("WMB", 0.04), ("OKE", 0.04), ("KMI", 0.03), ("MPC", 0.03),
        ("PSX", 0.03), ("SLB", 0.03),
    ],
    "XLV": [
        ("LLY", 0.09), ("JNJ", 0.08), ("UNH", 0.07), ("MRK", 0.05),
        ("ABBV",0.05), ("ABT", 0.04), ("TMO", 0.03), ("PFE", 0.03),
        ("AMGN",0.03), ("ISRG",0.03),
    ],
    "XLY": [
        ("AMZN", 0.22), ("TSLA",0.14), ("HD",   0.08), ("MCD", 0.05),
        ("BKNG", 0.04), ("LOW", 0.04), ("TJX",  0.03), ("NKE", 0.02),
        ("SBUX", 0.02), ("ORLY",0.02),
    ],
    "XLC": [
        ("META", 0.22), ("GOOGL",0.12), ("GOOG", 0.11), ("NFLX",0.05),
        ("TMUS", 0.05), ("VZ",   0.04), ("DIS",  0.04), ("T",   0.04),
        ("EA",   0.03), ("CMCSA",0.03),
    ],
    "SPY": [
        ("AAPL", 0.07), ("MSFT", 0.07), ("NVDA", 0.07), ("AMZN",0.04),
        ("GOOGL",0.02), ("META", 0.02), ("BRK-B",0.017),("GOOG",0.017),
        ("TSLA", 0.015),("JPM",  0.015),
    ],
}


def constituents_universe() -> List[str]:
    seen, out = set(), []
    for etf, holdings in SECTOR_HOLDINGS.items():
        if etf not in seen:
            seen.add(etf); out.append(etf)
        for t, _ in holdings:
            if t not in seen:
                seen.add(t); out.append(t)
    return out


@dataclass
class BasketResult:
    etf: str
    n_constituents_used: int
    coverage_weight: float        # sum of weights actually used (after dropping missing tickers)
    n_bars: int
    track_corr: float             # corr(etf_logret, basket_logret) — should be > 0.97
    tracking_error_bps: float     # std of per-bar residual, in bps
    halflife_bars: float          # OU half-life of the residual
    sharpe: float
    annual_return: float
    annual_vol: float
    max_drawdown: float
    hit_rate: float
    n_trades: int
    cost_bps_per_leg: float

    def to_dict(self):
        return asdict(self)


def synthetic_basket(closes: pd.DataFrame, weights: List[Tuple[str, float]]
                     ) -> Tuple[pd.Series, float, int, List[str]]:
    """Return (basket_log_price, coverage_weight, n_used, used_tickers).

    basket_log_price = sum_i w_i * log(P_i_t) with weights renormalized over
    tickers we actually have data for. NaN-bars are dropped upstream.
    """
    available = [(t, w) for t, w in weights if t in closes.columns]
    if not available:
        return pd.Series(dtype=float), 0.0, 0, []
    coverage = float(sum(w for _, w in available))
    norm = [(t, w / coverage) for t, w in available]
    log_prices = np.log(closes[[t for t, _ in norm]])
    w_arr = np.array([w for _, w in norm])
    basket_log = (log_prices * w_arr).sum(axis=1)
    return basket_log, coverage, len(norm), [t for t, _ in norm]


def _halflife(x: pd.Series) -> float:
    s = x.dropna()
    if len(s) < 100:
        return float("nan")
    lag = s.shift(1).dropna()
    delta = s.diff().dropna()
    df = pd.concat([delta, lag], axis=1, join="inner").dropna()
    if len(df) < 100:
        return float("nan")
    y = df.iloc[:, 0].values
    X = np.column_stack([np.ones(len(df)), df.iloc[:, 1].values])
    # OLS: y = a + phi*x
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    phi = coef[1]
    if phi >= 0 or not np.isfinite(phi):
        return float("nan")
    return float(-np.log(2.0) / phi)


def backtest_basket_nav(
    closes: pd.DataFrame,
    etf: str,
    *,
    z_window: int = 60,
    z_entry: float = 2.0,
    z_exit: float = 0.5,
    cost_bps_per_leg: float = 1.0,
) -> Tuple[BasketResult, pd.Series, pd.Series]:
    """Backtest basket-NAV arb for one ETF. Returns (metrics, equity, spread_z).

    Cost model: every time the position flips, pay (1 + n_constituents) legs
    of `cost_bps_per_leg`. Half-spread bid-ask + commissions for one side.
    """
    holdings = SECTOR_HOLDINGS[etf]
    df = closes.dropna()
    if etf not in df.columns:
        empty = pd.Series(dtype=float)
        return BasketResult(etf, 0, 0.0, 0, float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"), 0,
                            cost_bps_per_leg), empty, empty

    basket_log, coverage, n_used, used = synthetic_basket(df, holdings)
    if n_used == 0:
        empty = pd.Series(dtype=float)
        return BasketResult(etf, 0, 0.0, 0, float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"), 0,
                            cost_bps_per_leg), empty, empty

    etf_log = np.log(df[etf])
    aligned = pd.concat(
        [etf_log.rename("e"), basket_log.rename("b")], axis=1, join="inner"
    ).dropna()

    # Per-bar log returns
    e_ret = aligned["e"].diff()
    b_ret = aligned["b"].diff()
    track_corr = float(e_ret.corr(b_ret))

    # Residual / spread
    spread = aligned["e"] - aligned["b"]   # log-price residual
    # tracking error: stddev of per-bar return residual in bps
    residual_ret = (e_ret - b_ret).dropna()
    te_bps = float(residual_ret.std(ddof=0) * 1e4)

    z_mean = spread.rolling(z_window).mean()
    z_std = spread.rolling(z_window).std(ddof=0)
    z = (spread - z_mean) / z_std

    hl = _halflife(spread - spread.rolling(z_window).mean())

    # State machine for position on ETF side (basket = -etf side, dollar-neutral)
    pos_etf = pd.Series(0.0, index=aligned.index)
    cur = 0.0
    for ts, zv in z.items():
        if not np.isfinite(zv):
            pos_etf.loc[ts] = 0.0
            continue
        if cur == 0:
            if zv >= z_entry:
                cur = -1.0    # ETF rich vs basket -> short ETF
            elif zv <= -z_entry:
                cur = +1.0    # ETF cheap vs basket -> long ETF
        else:
            if abs(zv) <= z_exit:
                cur = 0.0
        pos_etf.loc[ts] = cur

    # Dollar-neutral PnL: gross per leg = 1 unit. Position uses log returns.
    raw_pnl = pos_etf.shift(1) * e_ret + (-pos_etf.shift(1)) * b_ret
    # equivalently: -pos_etf.shift(1) * residual_ret

    # Cost: position change * (etf leg + n_used basket legs) * bps/leg.
    # Each leg pays bps on its half. A flip from +1 to -1 traverses 2 units.
    legs = 1 + n_used
    cost = (cost_bps_per_leg / 1e4) * legs * pos_etf.diff().abs().fillna(0.0)
    pnl = (raw_pnl - cost).fillna(0.0)

    equity = (1.0 + pnl).cumprod()

    n = len(pnl)
    years = max(n / BARS_PER_YEAR, 1e-9)
    total_ret = float(equity.iloc[-1] - 1.0) if n else 0.0
    ann_ret = float((1.0 + total_ret) ** (1.0 / years) - 1.0) if total_ret > -1 else float("nan")
    ann_vol = float(pnl.std(ddof=0) * np.sqrt(BARS_PER_YEAR))
    sharpe = float(ann_ret / ann_vol) if ann_vol > 0 else float("nan")
    dd = float((equity / equity.cummax() - 1.0).min()) if n else 0.0
    nonzero = pnl[pnl != 0]
    hit = float((nonzero > 0).mean()) if len(nonzero) else float("nan")
    n_trades = int((pos_etf.diff().abs() > 1e-9).sum())

    res = BasketResult(
        etf=etf,
        n_constituents_used=n_used,
        coverage_weight=coverage,
        n_bars=n,
        track_corr=track_corr,
        tracking_error_bps=te_bps,
        halflife_bars=hl,
        sharpe=sharpe,
        annual_return=ann_ret,
        annual_vol=ann_vol,
        max_drawdown=dd,
        hit_rate=hit,
        n_trades=n_trades,
        cost_bps_per_leg=cost_bps_per_leg,
    )
    return res, equity, z
