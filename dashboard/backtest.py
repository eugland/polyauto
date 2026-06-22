"""Quick backtester: DCA vs lump-sum on a yfinance ticker.

DCA invests a fixed ``amount`` every period (weekly/monthly) buying shares at
that day's close. Lump-sum invests the same total capital all at once on the
first day. Both return an equity curve + summary metrics.
"""
from __future__ import annotations

import logging

import pandas as pd
import yfinance as yf

log = logging.getLogger("dashboard.backtest")

_FREQ = {"weekly": "W-MON", "monthly": "MS", "daily": "D"}


def _load_closes(symbol: str, start: str, end: str) -> pd.Series:
    df = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=True)
    closes = df["Close"].dropna()
    closes.index = closes.index.tz_localize(None)
    return closes


def _metrics(dates, equity, invested_total, first_dt, last_dt) -> dict:
    final_value = float(equity[-1]) if equity else 0.0
    total_return_pct = ((final_value / invested_total - 1.0) * 100.0) if invested_total else 0.0
    years = max((last_dt - first_dt).days / 365.25, 1e-9)
    cagr = (((final_value / invested_total) ** (1.0 / years) - 1.0) * 100.0) if invested_total > 0 and final_value > 0 else 0.0
    # max drawdown of the equity curve
    peak = float("-inf")
    max_dd = 0.0
    for v in equity:
        peak = max(peak, v)
        if peak > 0:
            max_dd = min(max_dd, (v - peak) / peak)
    return {
        "invested": round(invested_total, 2),
        "final_value": round(final_value, 2),
        "total_return_pct": round(total_return_pct, 2),
        "cagr": round(cagr, 2),
        "max_drawdown": round(max_dd * 100.0, 2),
    }


def _simulate(closes: pd.Series, strategy: str, amount: float, freq: str, initial: float = 0.0) -> dict:
    """Return {series:[{t,value,invested}], metrics:{...}} for one strategy.

    ``initial`` is a one-time lump bought on the first contribution date, on top
    of the recurring ``amount`` (DCA only).
    """
    if closes.empty:
        return {"series": [], "metrics": {}, "error": "no price data"}

    rule = _FREQ.get(freq, "MS")
    # Contribution dates snapped to the next available trading day.
    contrib_dates = pd.date_range(closes.index[0], closes.index[-1], freq=rule)
    snapped = []
    for d in contrib_dates:
        future = closes.index[closes.index >= d]
        if len(future):
            snapped.append(future[0])
    snapped = sorted(set(snapped)) or [closes.index[0]]
    n = len(snapped)

    if strategy == "lumpsum":
        total = amount * n + initial  # match DCA's total capital for a fair comparison
        buys = {snapped[0]: total}
    else:  # dca
        buys = {d: amount for d in snapped}
        if initial:
            buys[snapped[0]] = buys.get(snapped[0], 0.0) + initial

    shares = 0.0
    invested = 0.0
    invested_total = 0.0
    series = []
    for dt, price in closes.items():
        if dt in buys and price > 0:
            spend = buys[dt]
            shares += spend / float(price)
            invested += spend
            invested_total += spend
        value = shares * float(price)
        series.append({"t": dt.strftime("%Y-%m-%d"), "value": round(value, 2), "invested": round(invested, 2)})

    equity = [pt["value"] for pt in series]
    metrics = _metrics(
        [pt["t"] for pt in series], equity, invested_total,
        closes.index[0], closes.index[-1],
    )
    return {"series": series, "metrics": metrics}


def run(symbol: str, strategy: str, amount: float, freq: str, start: str, end: str, initial: float = 0.0) -> dict:
    """Run a single strategy ('dca' or 'lumpsum'); 'compare' runs both."""
    try:
        closes = _load_closes(symbol, start, end)
    except Exception as exc:  # noqa: BLE001
        log.warning("backtest load(%s) failed: %s", symbol, exc)
        return {"error": f"failed to load {symbol}: {exc}"}

    if closes.empty:
        return {"error": f"no price data for {symbol} in {start}..{end}"}

    out: dict = {"symbol": symbol, "freq": freq, "amount": amount, "initial": initial, "start": start, "end": end}
    if strategy == "compare":
        out["dca"] = _simulate(closes, "dca", amount, freq, initial)
        out["lumpsum"] = _simulate(closes, "lumpsum", amount, freq, initial)
    else:
        out[strategy] = _simulate(closes, strategy, amount, freq, initial)
    return out
