"""Cash-secured short-put backtest with simulated option premiums.

We don't have free historical options chains, so we *simulate* premiums via
Black-Scholes using 30-day realized vol scaled up by an `iv_multiplier` to
approximate the volatility risk premium that real short-vol sellers capture.

Caveats this skips:
  - Real put skew (BS underprices far-OTM puts vs reality, so the simulated
    premium received is *conservative*; the real strategy collects somewhat more).
  - Borrow cost / margin requirements (we assume cash-secured at strike).
  - Early assignment, dividend ex-dates, slippage on rolls (we hold to expiry).
  - Wing-of-skew compression after vol spikes (real IV behaves non-linearly).
  - We use ETF closing prices for both entry and expiry settlement; real
    settlement is morning-of-expiry AM print, slightly different.

Strategy mechanics (single-position cycle):
  1. Every `cycle_days` (default 30), sell one ATM-(otm_pct) put expiring in
     `dte_days`.
  2. Hold to expiry. If close >= strike: keep full premium (less haircuts).
     If close < strike: realize loss = (strike - close) - premium.
  3. Capital required per cycle = strike (cash-secured).
  4. Track per-trade PnL, equity, drawdowns, assignment rate.

Returns are reported as fraction of *capital at risk* per cycle (= strike).
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List

import numpy as np
import pandas as pd
from scipy.stats import norm


def bs_put(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
    """Black-Scholes put price. T in years, sigma annualized."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(K - S, 0.0)
    sqrtT = np.sqrt(T)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    return float(K * np.exp(-r * T) * norm.cdf(-d2)
                 - S * np.exp(-q * T) * norm.cdf(-d1))


def realized_vol(log_returns: pd.Series, window: int = 30) -> pd.Series:
    """Annualized rolling realized vol from daily log returns."""
    return log_returns.rolling(window).std(ddof=0) * np.sqrt(252)


@dataclass
class SimConfig:
    otm_pct: float = 0.20       # strike = entry * (1 - otm_pct). 0.20 = 20% OTM.
    dte_days: int = 30          # days to expiry at sale
    cycle_days: int = 30        # how often we sell a new put. >= dte_days = single-position.
    iv_multiplier: float = 1.2  # IV = realized_vol * this. Captures VRP.
    rv_window: int = 30
    risk_free: float = 0.045
    bid_ask_haircut: float = 0.05  # 5% premium haircut to fill at mid-of-bid
    contracts_per_cycle: int = 1   # nominal; we report % returns, so this just scales abs $


def backtest_short_put(close: pd.Series, cfg: SimConfig) -> pd.DataFrame:
    """Run the strategy on a daily close series. Returns one row per trade."""
    log_ret = np.log(close).diff()
    rv = realized_vol(log_ret, cfg.rv_window)

    trades = []
    warmup = cfg.rv_window + 5
    i = warmup
    n = len(close)
    while i + cfg.dte_days < n:
        entry_date = close.index[i]
        entry_price = float(close.iloc[i])
        sigma = float(rv.iloc[i]) * cfg.iv_multiplier
        if not np.isfinite(sigma) or sigma <= 0:
            i += cfg.cycle_days
            continue

        strike = entry_price * (1.0 - cfg.otm_pct)
        T = cfg.dte_days / 365.0
        premium_theo = bs_put(entry_price, strike, T, cfg.risk_free, sigma)
        premium = premium_theo * (1.0 - cfg.bid_ask_haircut)

        expiry_idx = i + cfg.dte_days
        expiry_price = float(close.iloc[expiry_idx])
        expiry_date = close.index[expiry_idx]

        if expiry_price >= strike:
            assigned = False
            pnl_per_share = premium
        else:
            assigned = True
            intrinsic = strike - expiry_price
            pnl_per_share = premium - intrinsic

        # min underlying over the trade window — useful diagnostic for drawdown
        path = close.iloc[i:expiry_idx + 1]
        path_min = float(path.min())
        ever_breached = path_min < strike    # any intraday close below strike
        max_paper_loss_per_share = max(strike - path_min, 0.0) - premium

        trades.append({
            "entry_date": entry_date,
            "expiry_date": expiry_date,
            "entry_price": entry_price,
            "strike": strike,
            "expiry_price": expiry_price,
            "rv_30d": float(rv.iloc[i]),
            "iv_used": sigma,
            "premium": premium,
            "pnl_per_share": pnl_per_share,
            "return_on_capital": pnl_per_share / strike,
            "assigned": assigned,
            "ever_breached_intra": ever_breached,
            "max_paper_loss_per_share": max_paper_loss_per_share,
        })

        i += cfg.cycle_days

    return pd.DataFrame(trades)


@dataclass
class SummaryStats:
    underlying: str
    otm_pct: float
    n_trades: int
    assignment_rate: float
    breach_rate: float
    win_rate: float
    avg_premium_pct: float     # premium / strike, average per trade
    total_return: float        # compounded return-on-capital
    annual_return: float
    annual_vol: float
    sharpe: float
    max_drawdown: float
    worst_trade_return: float  # most negative single-trade return on capital
    bh_total_return: float     # buy-and-hold underlying over same window
    bh_annual_return: float

    def to_dict(self): return asdict(self)


def summarize(trades: pd.DataFrame, close: pd.Series,
              underlying: str, otm_pct: float,
              cycle_days: int = 30) -> SummaryStats:
    if trades.empty:
        return SummaryStats(underlying, otm_pct, 0, *([float("nan")] * 11))

    rets = trades["return_on_capital"].astype(float)
    equity = (1.0 + rets).cumprod()

    # Annualization: each trade ~ cycle_days; trades per year = 365/cycle_days
    trades_per_year = 365.0 / cycle_days
    years = len(rets) / trades_per_year
    total = float(equity.iloc[-1] - 1.0)
    ann_ret = float((1.0 + total) ** (1.0 / max(years, 1e-9)) - 1.0) if total > -1 else float("nan")
    ann_vol = float(rets.std(ddof=0) * np.sqrt(trades_per_year))
    sharpe = float(ann_ret / ann_vol) if ann_vol > 0 else float("nan")
    dd = float((equity / equity.cummax() - 1.0).min())
    worst = float(rets.min())

    # Buy-and-hold comparison over the same window
    bh_start = trades["entry_date"].iloc[0]
    bh_end = trades["expiry_date"].iloc[-1]
    bh_window = close.loc[bh_start:bh_end]
    if len(bh_window) >= 2:
        bh_total = float(bh_window.iloc[-1] / bh_window.iloc[0] - 1.0)
        bh_years = (bh_window.index[-1] - bh_window.index[0]).days / 365.0
        bh_ann = float((1.0 + bh_total) ** (1.0 / max(bh_years, 1e-9)) - 1.0) if bh_total > -1 else float("nan")
    else:
        bh_total = bh_ann = float("nan")

    return SummaryStats(
        underlying=underlying,
        otm_pct=otm_pct,
        n_trades=len(rets),
        assignment_rate=float(trades["assigned"].mean()),
        breach_rate=float(trades["ever_breached_intra"].mean()),
        win_rate=float((rets > 0).mean()),
        avg_premium_pct=float((trades["premium"] / trades["strike"]).mean()),
        total_return=total,
        annual_return=ann_ret,
        annual_vol=ann_vol,
        sharpe=sharpe,
        max_drawdown=dd,
        worst_trade_return=worst,
        bh_total_return=bh_total,
        bh_annual_return=bh_ann,
    )
