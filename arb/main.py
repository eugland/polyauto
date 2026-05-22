"""Run all scopes, write CSV summary tables, save equity-curve plots, print
the headline insights to stdout.

Usage:
  /home/weugene/dev/polyauto/.venv/bin/python -m arb.main
  /home/weugene/dev/polyauto/.venv/bin/python -m arb.main --force-refresh
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Headless plotting; safe in any environment.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from arb import data as data_mod
from arb import pairs as pairs_mod
from arb import backtest as bt_mod
from arb.universe import all_pairs, all_tickers, Pair

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def _format_pct(x: float) -> str:
    if pd.isna(x):
        return "  nan"
    return f"{x*100:+6.2f}%"


def _print_table(title: str, df: pd.DataFrame) -> None:
    print()
    print("=" * 88)
    print(title)
    print("=" * 88)
    if df.empty:
        print("  (empty)")
        return
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 200,
                           "display.float_format", "{:.4f}".format):
        print(df.to_string(index=False))


def run(force_refresh: bool = False, cost_bps: float = 2.0) -> None:
    print(f"[arb] universe: {len(all_pairs())} pairs across "
          f"{len(all_tickers())} tickers")

    # --- Fetch data once for every ticker we need ---
    frames = data_mod.fetch_many(
        all_tickers(),
        interval="1h", period="730d",
        max_age_hours=12.0, force=force_refresh,
    )

    closes = data_mod.aligned_closes(frames)
    if closes.empty:
        print("[arb] no aligned data — bailing.")
        return
    print(f"[arb] aligned panel: {closes.shape[0]} bars, "
          f"{closes.shape[1]} tickers, "
          f"{closes.index.min().date()} .. {closes.index.max().date()}")

    diag_rows: List[Dict] = []
    bt_rows: List[Dict] = []
    equity_curves: Dict[str, pd.Series] = {}

    for p in all_pairs():
        if p.leader not in closes.columns or p.follower not in closes.columns:
            print(f"  [skip] {p.key}: missing data")
            continue

        L = closes[p.leader]
        F = closes[p.follower]

        # ---- Diagnostics ----
        diag = pairs_mod.analyze(L, F, p.leader, p.follower)
        diag_rows.append({
            "scope": p.scope,
            "pair": p.key,
            "note": p.note,
            "n_bars": diag.n_bars,
            "return_corr": diag.return_corr,
            "coint_p": diag.coint_pvalue,
            "hedge_beta": diag.hedge_ratio,
            "lead_lag": diag.best_lag,
            "ll_corr": diag.best_lag_corr,
            "halflife_bars": diag.spread_halflife_bars,
        })

        # ---- Backtest 1: spread mean-reversion ----
        mr_res, mr_eq = bt_mod.backtest_spread_mr(
            L, F, p.leader, p.follower, cost_bps=cost_bps,
        )
        bt_rows.append({"scope": p.scope, "pair": p.key, **mr_res.to_dict()})
        if not mr_eq.empty:
            equity_curves[f"{p.key}|spread_mr"] = mr_eq

        # ---- Backtest 2: lead-lag momentum ----
        ll_lag = max(1, abs(diag.best_lag))
        ll_res, ll_eq = bt_mod.backtest_lead_lag(
            L, F, p.leader, p.follower,
            lag=ll_lag, threshold=0.0, cost_bps=cost_bps,
        )
        bt_rows.append({"scope": p.scope, "pair": p.key, **ll_res.to_dict()})
        if not ll_eq.empty:
            equity_curves[f"{p.key}|lead_lag"] = ll_eq

        # ---- Benchmark: buy & hold follower ----
        bh_res = bt_mod.benchmark_buy_hold(F, p.leader, p.follower)
        bt_rows.append({"scope": p.scope, "pair": p.key, **bh_res.to_dict()})

    diag_df = pd.DataFrame(diag_rows).sort_values(["scope", "coint_p"])
    bt_df = pd.DataFrame(bt_rows)

    diag_df.to_csv(OUTPUT_DIR / "diagnostics.csv", index=False)
    bt_df.to_csv(OUTPUT_DIR / "backtests.csv", index=False)

    # --- Reports ---
    _print_table(
        "DIAGNOSTICS (sorted by cointegration p-value within scope)",
        diag_df[["scope", "pair", "note", "n_bars", "return_corr",
                 "coint_p", "hedge_beta", "lead_lag", "ll_corr",
                 "halflife_bars"]],
    )

    mr_only = bt_df[bt_df["strategy"] == "spread_mr"].copy()
    ll_only = bt_df[bt_df["strategy"] == "lead_lag"].copy()
    bh_only = bt_df[bt_df["strategy"] == "buy_hold_follower"].copy()

    summary = (
        mr_only[["scope", "pair", "sharpe", "annual_return", "max_drawdown",
                 "hit_rate", "n_trades"]]
        .rename(columns={
            "sharpe": "mr_sharpe",
            "annual_return": "mr_ann_ret",
            "max_drawdown": "mr_max_dd",
            "hit_rate": "mr_hit",
            "n_trades": "mr_trades",
        })
        .merge(
            ll_only[["pair", "sharpe", "annual_return", "max_drawdown",
                     "hit_rate", "n_trades"]].rename(columns={
                "sharpe": "ll_sharpe",
                "annual_return": "ll_ann_ret",
                "max_drawdown": "ll_max_dd",
                "hit_rate": "ll_hit",
                "n_trades": "ll_trades",
            }),
            on="pair", how="left",
        )
        .merge(
            bh_only[["pair", "annual_return"]].rename(
                columns={"annual_return": "bh_ann_ret"}),
            on="pair", how="left",
        )
    )
    summary["mr_vs_bh"] = summary["mr_ann_ret"] - summary["bh_ann_ret"]
    summary["ll_vs_bh"] = summary["ll_ann_ret"] - summary["bh_ann_ret"]
    summary = summary.sort_values(["scope", "mr_sharpe"], ascending=[True, False])
    summary.to_csv(OUTPUT_DIR / "summary.csv", index=False)

    _print_table("BACKTEST SUMMARY (per-pair Sharpe / ann.return / drawdown)", summary)

    # --- Headline insights ---
    print()
    print("=" * 88)
    print("HEADLINE INSIGHTS")
    print("=" * 88)

    cointegrated = diag_df[(diag_df["coint_p"] < 0.05) & (diag_df["n_bars"] > 200)]
    print(f"\n[1] Cointegrated pairs (p<0.05): {len(cointegrated)} / {len(diag_df)}")
    if not cointegrated.empty:
        top = cointegrated.nsmallest(5, "coint_p")[
            ["pair", "note", "coint_p", "halflife_bars", "hedge_beta"]
        ]
        print(top.to_string(index=False))

    strong_lead = diag_df[(diag_df["lead_lag"] > 0) & (diag_df["ll_corr"].abs() > 0.10)]
    print(f"\n[2] Pairs where leader meaningfully leads follower "
          f"(|corr|>0.10, lag>0): {len(strong_lead)}")
    if not strong_lead.empty:
        top = strong_lead.nlargest(5, "ll_corr")[
            ["pair", "note", "lead_lag", "ll_corr"]
        ]
        print(top.to_string(index=False))

    best_mr = summary.nlargest(5, "mr_sharpe")[
        ["pair", "scope", "mr_sharpe", "mr_ann_ret", "mr_max_dd",
         "mr_trades", "bh_ann_ret"]
    ]
    print("\n[3] Best spread-MR backtests by Sharpe:")
    print(best_mr.to_string(index=False))

    best_ll = summary.nlargest(5, "ll_sharpe")[
        ["pair", "scope", "ll_sharpe", "ll_ann_ret", "ll_max_dd",
         "ll_trades", "bh_ann_ret"]
    ]
    print("\n[4] Best lead-lag backtests by Sharpe:")
    print(best_ll.to_string(index=False))

    # --- Equity-curve plot: top-3 spread MR ---
    top_keys = []
    for pair in best_mr["pair"].head(3):
        key = f"{pair}|spread_mr"
        if key in equity_curves:
            top_keys.append(key)
    if top_keys:
        fig, ax = plt.subplots(figsize=(10, 5))
        for k in top_keys:
            equity_curves[k].plot(ax=ax, label=k)
        ax.set_title("Spread mean-reversion equity curves (top by Sharpe)")
        ax.set_ylabel("equity (1.0 = start)")
        ax.legend(loc="best", fontsize=8)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        out_path = OUTPUT_DIR / "equity_top_mr.png"
        fig.savefig(out_path, dpi=120)
        print(f"\n[plot] saved {out_path}")

    top_ll_keys = []
    for pair in best_ll["pair"].head(3):
        key = f"{pair}|lead_lag"
        if key in equity_curves:
            top_ll_keys.append(key)
    if top_ll_keys:
        fig, ax = plt.subplots(figsize=(10, 5))
        for k in top_ll_keys:
            equity_curves[k].plot(ax=ax, label=k)
        ax.set_title("Lead-lag momentum equity curves (top by Sharpe)")
        ax.set_ylabel("equity (1.0 = start)")
        ax.legend(loc="best", fontsize=8)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        out_path = OUTPUT_DIR / "equity_top_ll.png"
        fig.savefig(out_path, dpi=120)
        print(f"[plot] saved {out_path}")

    print(f"\n[done] CSVs + plots in {OUTPUT_DIR}/")


def main() -> None:
    ap = argparse.ArgumentParser(description="Statistical arb research backtest.")
    ap.add_argument("--force-refresh", action="store_true",
                    help="ignore cached data and re-download from yfinance")
    ap.add_argument("--cost-bps", type=float, default=2.0,
                    help="round-trip cost per side, in bps of notional (default 2)")
    args = ap.parse_args()
    run(force_refresh=args.force_refresh, cost_bps=args.cost_bps)


if __name__ == "__main__":
    main()
