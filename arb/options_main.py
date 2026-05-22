"""Run the short-put simulation on leveraged ETFs.

Grid: 4 underlyings × 7 OTM levels, 5 years of daily data.
Reports: per-(ETF, OTM%) Sharpe/return/DD, yearly breakdown, worst-trade,
assignment rate, and a benchmark vs buy-and-hold the underlying.

Usage:
  /home/weugene/dev/polyauto/.venv/bin/python -m arb.options_main
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from arb import data as data_mod
from arb.options_sim import (
    SimConfig, backtest_short_put, summarize,
)

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# 3x leveraged ETFs to test.
UNDERLYINGS = [
    ("TQQQ", "3x Nasdaq-100"),
    ("SOXL", "3x Semiconductors"),
    ("UPRO", "3x S&P 500"),
    ("TNA",  "3x Russell 2000 small-caps"),
]

OTM_LEVELS = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]


def _print_table(title: str, df: pd.DataFrame, fmt: str = "{:.4f}") -> None:
    print(); print("=" * 110); print(title); print("=" * 110)
    if df.empty: print("  (empty)"); return
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 240,
                           "display.float_format", fmt.format):
        print(df.to_string(index=False))


def run(force_refresh: bool = False, years: int = 5) -> None:
    tickers = [t for t, _ in UNDERLYINGS]
    print(f"[opts] fetching {tickers} daily, {years}y")
    frames = data_mod.fetch_many(
        tickers, interval="1d", period=f"{years}y",
        max_age_hours=12.0, force=force_refresh,
    )
    closes_panel = data_mod.aligned_closes(frames)
    if closes_panel.empty:
        print("[opts] no aligned data; bailing")
        return
    print(f"[opts] panel: {closes_panel.shape[0]} days, "
          f"{closes_panel.index.min().date()} .. {closes_panel.index.max().date()}")

    summary_rows: List[Dict] = []
    all_trades: Dict[str, pd.DataFrame] = {}
    equity_curves: Dict[str, pd.Series] = {}

    for ticker, label in UNDERLYINGS:
        if ticker not in closes_panel.columns: continue
        close = closes_panel[ticker]
        for otm in OTM_LEVELS:
            cfg = SimConfig(otm_pct=otm, dte_days=30, cycle_days=30,
                            iv_multiplier=1.2)
            trades = backtest_short_put(close, cfg)
            if trades.empty: continue
            stats = summarize(trades, close, ticker, otm, cfg.cycle_days)
            summary_rows.append({"label": label, **stats.to_dict()})
            key = f"{ticker}_otm{int(otm*100)}"
            all_trades[key] = trades
            equity_curves[key] = (1.0 + trades["return_on_capital"]).cumprod()

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(OUTPUT_DIR / "options_summary.csv", index=False)

    _print_table(
        "SHORT FAR-OTM PUTS on 3x LEVERAGED ETFs — full-period summary",
        summary[["underlying", "otm_pct", "n_trades",
                 "win_rate", "assignment_rate", "breach_rate",
                 "avg_premium_pct", "annual_return",
                 "annual_vol", "sharpe", "max_drawdown",
                 "worst_trade_return", "bh_annual_return"]],
    )

    # ---- Per-year breakdown for the most representative grid (OTM=20%) ----
    print(); print("=" * 110)
    print("PER-YEAR RETURN ON CAPITAL — OTM=20% across underlyings")
    print("=" * 110)
    yearly_rows: List[Dict] = []
    for ticker, _ in UNDERLYINGS:
        key = f"{ticker}_otm20"
        if key not in all_trades: continue
        td = all_trades[key].copy()
        td["year"] = pd.to_datetime(td["entry_date"]).dt.year
        for year, grp in td.groupby("year"):
            rets = grp["return_on_capital"]
            equity = (1.0 + rets).cumprod()
            yearly_rows.append({
                "underlying": ticker,
                "year": int(year),
                "n_trades": len(rets),
                "year_return": float(equity.iloc[-1] - 1.0),
                "n_assignments": int(grp["assigned"].sum()),
                "worst_trade": float(rets.min()),
            })
    yearly = pd.DataFrame(yearly_rows).sort_values(["underlying", "year"])
    yearly.to_csv(OUTPUT_DIR / "options_yearly.csv", index=False)
    with pd.option_context("display.float_format", "{:.4f}".format):
        print(yearly.to_string(index=False))

    # ---- Worst trades across the whole grid ----
    print(); print("=" * 110); print("WORST 10 TRADES (entire grid)"); print("=" * 110)
    worst_rows: List[Dict] = []
    for ticker, _ in UNDERLYINGS:
        for otm in OTM_LEVELS:
            key = f"{ticker}_otm{int(otm*100)}"
            if key not in all_trades: continue
            td = all_trades[key]
            for _, row in td.iterrows():
                worst_rows.append({
                    "underlying": ticker,
                    "otm_pct": otm,
                    "entry_date": row["entry_date"],
                    "strike": round(row["strike"], 2),
                    "expiry_price": round(row["expiry_price"], 2),
                    "drawdown_pct": (row["expiry_price"] / row["entry_price"]) - 1.0,
                    "premium": round(row["premium"], 3),
                    "pnl_pct": row["return_on_capital"],
                })
    worst_df = pd.DataFrame(worst_rows).nsmallest(10, "pnl_pct")
    with pd.option_context("display.float_format", "{:.4f}".format):
        print(worst_df.to_string(index=False))

    # ---- IV-multiplier sensitivity: TQQQ @ 20% OTM ----
    if "TQQQ" in closes_panel.columns:
        print(); print("=" * 110)
        print("IV-MULTIPLIER SENSITIVITY (TQQQ, 20% OTM, 30d cycle)")
        print("=" * 110)
        sens_rows = []
        for ivm in (1.0, 1.1, 1.2, 1.3, 1.5):
            cfg = SimConfig(otm_pct=0.20, iv_multiplier=ivm)
            td = backtest_short_put(closes_panel["TQQQ"], cfg)
            if td.empty: continue
            st = summarize(td, closes_panel["TQQQ"], "TQQQ", 0.20)
            sens_rows.append({
                "iv_mult": ivm,
                "avg_premium_pct": st.avg_premium_pct,
                "annual_return": st.annual_return,
                "sharpe": st.sharpe,
                "max_dd": st.max_drawdown,
                "worst_trade": st.worst_trade_return,
            })
        with pd.option_context("display.float_format", "{:.4f}".format):
            print(pd.DataFrame(sens_rows).to_string(index=False))

    # ---- Plots ----
    if equity_curves:
        # Equity curves at OTM=20% across underlyings
        fig, ax = plt.subplots(figsize=(11, 5))
        for ticker, _ in UNDERLYINGS:
            key = f"{ticker}_otm20"
            if key in equity_curves:
                eq = equity_curves[key]
                eq.index = all_trades[key]["expiry_date"]
                eq.plot(ax=ax, label=key)
        ax.set_title("Short-put equity (20% OTM, monthly, on capital-at-risk)")
        ax.set_ylabel("equity (1.0 start)")
        ax.legend(loc="best"); ax.grid(True, alpha=0.3)
        fig.tight_layout()
        p = OUTPUT_DIR / "options_equity_otm20.png"
        fig.savefig(p, dpi=120); print(f"\n[plot] {p}")

        # Sharpe-vs-OTM heatmap-ish line chart
        fig, ax = plt.subplots(figsize=(10, 5))
        for ticker, _ in UNDERLYINGS:
            sub = summary[summary["underlying"] == ticker].sort_values("otm_pct")
            ax.plot(sub["otm_pct"], sub["sharpe"], marker="o", label=ticker)
        ax.axhline(0, color="black", lw=0.5, alpha=0.4)
        ax.set_xlabel("OTM %"); ax.set_ylabel("Sharpe")
        ax.set_title("Sharpe vs OTM% (short put, 30d cycle, IV=RV*1.2)")
        ax.legend(loc="best"); ax.grid(True, alpha=0.3)
        fig.tight_layout()
        p = OUTPUT_DIR / "options_sharpe_vs_otm.png"
        fig.savefig(p, dpi=120); print(f"[plot] {p}")

    # ---- Insights ----
    print(); print("=" * 110); print("HEADLINE INSIGHTS"); print("=" * 110)

    best = summary.sort_values("sharpe", ascending=False).head(5)
    print("\n[1] Best 5 (ETF × OTM) combos by Sharpe:")
    with pd.option_context("display.float_format", "{:.4f}".format):
        print(best[["underlying", "otm_pct", "n_trades", "annual_return",
                    "sharpe", "max_drawdown", "worst_trade_return",
                    "bh_annual_return"]].to_string(index=False))

    print("\n[2] Tail check — for each ETF, the worst single-trade return on capital "
          "(any OTM level):")
    tail = summary.loc[summary.groupby("underlying")["worst_trade_return"].idxmin()]
    with pd.option_context("display.float_format", "{:.4f}".format):
        print(tail[["underlying", "otm_pct", "worst_trade_return",
                    "max_drawdown", "annual_return"]].to_string(index=False))

    print(f"\n[done] outputs in {OUTPUT_DIR}/")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-refresh", action="store_true")
    ap.add_argument("--years", type=int, default=5)
    args = ap.parse_args()
    run(force_refresh=args.force_refresh, years=args.years)


if __name__ == "__main__":
    main()
