"""Run basket-vs-NAV backtest on every sector ETF, plus cost-sensitivity sweep.

Usage:
  /home/weugene/dev/polyauto/.venv/bin/python -m arb.basket_main
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from arb import data as data_mod
from arb.basket import (
    SECTOR_HOLDINGS, constituents_universe,
    backtest_basket_nav, synthetic_basket,
)

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def _print_table(title: str, df: pd.DataFrame) -> None:
    print()
    print("=" * 96)
    print(title)
    print("=" * 96)
    if df.empty:
        print("  (empty)"); return
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 220,
                           "display.float_format", "{:.4f}".format):
        print(df.to_string(index=False))


def run(force_refresh: bool = False) -> None:
    tickers = constituents_universe()
    print(f"[basket] universe: {len(tickers)} tickers "
          f"({len(SECTOR_HOLDINGS)} ETFs + constituents)")

    frames = data_mod.fetch_many(
        tickers, interval="1h", period="730d",
        max_age_hours=12.0, force=force_refresh,
    )
    closes = data_mod.aligned_closes(frames)
    if closes.empty:
        print("[basket] no aligned data"); return
    print(f"[basket] aligned panel: {closes.shape[0]} bars, "
          f"{closes.shape[1]} tickers, "
          f"{closes.index.min().date()} .. {closes.index.max().date()}")

    # ----------------------------------------------------------------------
    # Pass 1: tracking-quality sanity check at coarse cost level.
    # ----------------------------------------------------------------------
    rows: List[Dict] = []
    equity_curves: Dict[str, pd.Series] = {}
    spread_z_series: Dict[str, pd.Series] = {}

    for etf in SECTOR_HOLDINGS.keys():
        res, eq, z = backtest_basket_nav(
            closes, etf,
            z_window=60, z_entry=2.0, z_exit=0.5,
            cost_bps_per_leg=1.0,
        )
        rows.append(res.to_dict())
        if not eq.empty:
            equity_curves[etf] = eq
            spread_z_series[etf] = z

    headline = pd.DataFrame(rows)
    headline.to_csv(OUTPUT_DIR / "basket_headline.csv", index=False)
    _print_table(
        "BASKET-vs-NAV — headline (cost=1bp/leg, z_entry=2, z_exit=0.5)",
        headline[["etf", "n_constituents_used", "coverage_weight",
                  "n_bars", "track_corr", "tracking_error_bps",
                  "halflife_bars", "sharpe", "annual_return",
                  "max_drawdown", "hit_rate", "n_trades"]],
    )

    # ----------------------------------------------------------------------
    # Pass 2: cost sensitivity sweep — 0.25, 0.5, 1, 2 bps per leg.
    # ----------------------------------------------------------------------
    sens_rows: List[Dict] = []
    for cost in (0.25, 0.5, 1.0, 2.0):
        for etf in SECTOR_HOLDINGS.keys():
            res, _, _ = backtest_basket_nav(
                closes, etf, cost_bps_per_leg=cost,
            )
            sens_rows.append({
                "etf": etf, "cost_bps_per_leg": cost,
                "sharpe": res.sharpe, "annual_return": res.annual_return,
                "max_dd": res.max_drawdown, "n_trades": res.n_trades,
            })
    sens = pd.DataFrame(sens_rows)
    pivot = sens.pivot_table(index="etf", columns="cost_bps_per_leg",
                             values="sharpe")
    pivot.to_csv(OUTPUT_DIR / "basket_cost_sensitivity.csv")
    print()
    print("=" * 96)
    print("SHARPE vs cost (bps per leg)")
    print("=" * 96)
    with pd.option_context("display.float_format", "{:.3f}".format):
        print(pivot.to_string())

    # ----------------------------------------------------------------------
    # Pass 3: parameter sweep — z_entry on the best-tracked ETF only.
    # ----------------------------------------------------------------------
    best_etf = headline.sort_values("track_corr", ascending=False).iloc[0]["etf"]
    print(f"\n[sweep] z_entry sweep on best-tracked ETF: {best_etf}")
    sweep_rows = []
    for ze in (1.0, 1.5, 2.0, 2.5, 3.0):
        for zx in (0.0, 0.25, 0.5, 1.0):
            if zx >= ze: continue
            res, _, _ = backtest_basket_nav(
                closes, best_etf,
                z_entry=ze, z_exit=zx, cost_bps_per_leg=1.0,
            )
            sweep_rows.append({
                "z_entry": ze, "z_exit": zx, "sharpe": res.sharpe,
                "annual_return": res.annual_return,
                "max_dd": res.max_drawdown, "n_trades": res.n_trades,
            })
    sweep = pd.DataFrame(sweep_rows).sort_values("sharpe", ascending=False)
    sweep.to_csv(OUTPUT_DIR / f"basket_sweep_{best_etf}.csv", index=False)
    _print_table(f"{best_etf}: z_entry/z_exit sweep (cost=1bp/leg)", sweep)

    # ----------------------------------------------------------------------
    # Headline insights
    # ----------------------------------------------------------------------
    print()
    print("=" * 96)
    print("HEADLINE INSIGHTS")
    print("=" * 96)
    bad_track = headline[headline["track_corr"] < 0.85]
    if not bad_track.empty:
        print("\n[!] These ETFs are NOT well replicated by top-10 holdings — "
              "interpret with caution:")
        print(bad_track[["etf", "track_corr", "coverage_weight"]].to_string(index=False))
    good = headline[headline["track_corr"] >= 0.85].copy()
    good = good.sort_values("sharpe", ascending=False)
    print("\n[1] Best risk-adjusted basket-NAV backtests (good tracking only):")
    print(good[["etf", "track_corr", "tracking_error_bps",
                "halflife_bars", "sharpe", "annual_return",
                "max_drawdown", "n_trades"]].to_string(index=False))

    print("\n[2] How quickly does the residual mean-revert? "
          "(half-life in 1h bars; <7 = same-day):")
    print(headline[["etf", "halflife_bars"]].to_string(index=False))

    # ----------------------------------------------------------------------
    # Plots
    # ----------------------------------------------------------------------
    if equity_curves:
        # Pick top-3 by Sharpe among well-tracked
        top3 = good.head(3)["etf"].tolist() if not good.empty else []
        if top3:
            fig, ax = plt.subplots(figsize=(10, 5))
            for etf in top3:
                if etf in equity_curves:
                    equity_curves[etf].plot(ax=ax, label=etf)
            ax.set_title("Basket-vs-NAV equity (top-3 Sharpe, 1bp/leg)")
            ax.set_ylabel("equity (1.0 = start)")
            ax.legend(loc="best"); ax.grid(True, alpha=0.3)
            fig.tight_layout()
            p = OUTPUT_DIR / "basket_equity_top.png"
            fig.savefig(p, dpi=120)
            print(f"\n[plot] {p}")

        # Residual z plot for top-tracked ETF
        if best_etf in spread_z_series:
            fig, ax = plt.subplots(figsize=(11, 4))
            spread_z_series[best_etf].plot(ax=ax, lw=0.6)
            ax.axhline(2, color="red", lw=0.8, ls="--", alpha=0.6)
            ax.axhline(-2, color="red", lw=0.8, ls="--", alpha=0.6)
            ax.axhline(0, color="black", lw=0.5, alpha=0.4)
            ax.set_title(f"{best_etf} basket-NAV residual z-score "
                         f"(60-bar rolling)")
            ax.set_ylabel("z-score"); ax.grid(True, alpha=0.3)
            fig.tight_layout()
            p = OUTPUT_DIR / f"basket_residual_z_{best_etf}.png"
            fig.savefig(p, dpi=120)
            print(f"[plot] {p}")

    print(f"\n[done] outputs in {OUTPUT_DIR}/")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-refresh", action="store_true")
    args = ap.parse_args()
    run(force_refresh=args.force_refresh)


if __name__ == "__main__":
    main()
