"""Run the mega-cap concentration mean-reversion tests.

Three spreads × two timescales = six tests, plus cost sensitivity and a
threshold sweep on the winner.

Usage:
  /home/weugene/dev/polyauto/.venv/bin/python -m arb.megacap_main
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
from arb.megacap import (
    MAG7, equal_weight_log_basket, backtest_spread,
)

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# (long_leg, short_leg, n_long_legs, n_short_legs, spread_name)
SPREADS = [
    ("SPY",  "RSP", 1, 1, "SPY vs RSP"),     # concentration factor (same 500 stocks)
    ("MAG7", "RSP", 7, 1, "MAG7 vs RSP"),    # raw mega-cap factor
    ("QQQ",  "SPY", 1, 1, "QQQ vs SPY"),     # tradeable proxy
]


def _print_table(title: str, df: pd.DataFrame) -> None:
    print(); print("=" * 100); print(title); print("=" * 100)
    if df.empty:
        print("  (empty)"); return
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 220,
                           "display.float_format", "{:.4f}".format):
        print(df.to_string(index=False))


def _build_legs(closes: pd.DataFrame, leg_id: str):
    if leg_id == "MAG7":
        log_basket, n = equal_weight_log_basket(closes, MAG7)
        return log_basket, n
    if leg_id in closes.columns:
        return np.log(closes[leg_id]), 1
    return pd.Series(dtype=float), 0


def _run_for_interval(interval: str, force_refresh: bool):
    period = "730d" if interval == "1h" else "5y"
    fit_window = 252 if interval == "1h" else 60   # ~6 wks hourly, ~3 mo daily
    z_window   = 60  if interval == "1h" else 20   # ~9 days hourly, ~1 mo daily

    universe = list({*MAG7, "SPY", "RSP", "QQQ"})
    print(f"\n[mega] interval={interval}, fetching {len(universe)} tickers")
    frames = data_mod.fetch_many(
        universe, interval=interval, period=period,
        max_age_hours=12.0, force=force_refresh,
    )
    closes = data_mod.aligned_closes(frames)
    if closes.empty:
        print("  no aligned data")
        return None, None, None
    print(f"  panel: {closes.shape[0]} bars, "
          f"{closes.index.min().date()} .. {closes.index.max().date()}")

    rows: List[Dict] = []
    eq_curves: Dict[str, pd.Series] = {}
    z_series: Dict[str, pd.Series] = {}

    for long_id, short_id, n_long, n_short, name in SPREADS:
        L_log, nL = _build_legs(closes, long_id)
        S_log, nS = _build_legs(closes, short_id)
        if L_log.empty or S_log.empty:
            print(f"  [{name}] missing data, skip"); continue
        legs = nL + nS                          # one round-trip flip = +pos -> -pos -> +pos
        res, eq, z, beta = backtest_spread(
            L_log, S_log, name,
            interval=interval, legs_per_flip=legs,
            fit_window=fit_window, z_window=z_window,
            z_entry=2.0, z_exit=0.5,
            cost_bps_per_leg=1.0,
        )
        rows.append(res.to_dict())
        key = f"{name}|{interval}"
        if not eq.empty: eq_curves[key] = eq
        if not z.empty:  z_series[key] = z

    df = pd.DataFrame(rows)
    return df, eq_curves, z_series


def _cost_sweep(interval: str, force_refresh: bool):
    period = "730d" if interval == "1h" else "5y"
    fit_window = 252 if interval == "1h" else 60
    z_window   = 60  if interval == "1h" else 20

    universe = list({*MAG7, "SPY", "RSP", "QQQ"})
    frames = data_mod.fetch_many(
        universe, interval=interval, period=period,
        max_age_hours=12.0, force=force_refresh,
    )
    closes = data_mod.aligned_closes(frames)
    if closes.empty: return pd.DataFrame()

    rows = []
    for cost in (0.25, 0.5, 1.0, 2.0, 5.0):
        for long_id, short_id, n_long, n_short, name in SPREADS:
            L_log, nL = _build_legs(closes, long_id)
            S_log, nS = _build_legs(closes, short_id)
            if L_log.empty or S_log.empty: continue
            legs = nL + nS
            res, _, _, _ = backtest_spread(
                L_log, S_log, name,
                interval=interval, legs_per_flip=legs,
                fit_window=fit_window, z_window=z_window,
                z_entry=2.0, z_exit=0.5,
                cost_bps_per_leg=cost,
            )
            rows.append({
                "spread": name, "interval": interval,
                "cost_bps_per_leg": cost,
                "sharpe": res.sharpe, "annual_return": res.annual_return,
                "max_dd": res.max_drawdown, "n_trades": res.n_trades,
            })
    return pd.DataFrame(rows)


def _threshold_sweep(interval: str, force_refresh: bool, spread_name: str):
    period = "730d" if interval == "1h" else "5y"
    fit_window = 252 if interval == "1h" else 60
    z_window   = 60  if interval == "1h" else 20

    universe = list({*MAG7, "SPY", "RSP", "QQQ"})
    frames = data_mod.fetch_many(
        universe, interval=interval, period=period,
        max_age_hours=12.0, force=force_refresh,
    )
    closes = data_mod.aligned_closes(frames)
    if closes.empty: return pd.DataFrame()

    target = next(s for s in SPREADS if s[4] == spread_name)
    long_id, short_id, n_long, n_short, name = target
    L_log, nL = _build_legs(closes, long_id)
    S_log, nS = _build_legs(closes, short_id)
    legs = nL + nS

    rows = []
    for ze in (1.0, 1.5, 2.0, 2.5, 3.0):
        for zx in (0.0, 0.25, 0.5, 1.0):
            if zx >= ze: continue
            res, _, _, _ = backtest_spread(
                L_log, S_log, name,
                interval=interval, legs_per_flip=legs,
                fit_window=fit_window, z_window=z_window,
                z_entry=ze, z_exit=zx,
                cost_bps_per_leg=1.0,
            )
            rows.append({
                "z_entry": ze, "z_exit": zx,
                "sharpe": res.sharpe, "annual_return": res.annual_return,
                "max_dd": res.max_drawdown, "n_trades": res.n_trades,
            })
    return pd.DataFrame(rows).sort_values("sharpe", ascending=False)


def run(force_refresh: bool = False) -> None:
    # ---- Headline across intervals ----
    df_h, eq_h, z_h = _run_for_interval("1h", force_refresh)
    df_d, eq_d, z_d = _run_for_interval("1d", force_refresh)

    combined = pd.concat(
        [d for d in (df_h, df_d) if d is not None and not d.empty],
        ignore_index=True,
    )
    if combined.empty:
        print("[mega] no results — bailing"); return

    combined.to_csv(OUTPUT_DIR / "mega_headline.csv", index=False)
    _print_table(
        "MEGA-CAP CONCENTRATION — headline (cost=1bp/leg, z_entry=2, z_exit=0.5)",
        combined[["name", "interval", "n_bars", "return_corr",
                  "halflife_bars", "rolling_beta_mean",
                  "sharpe", "annual_return",
                  "max_drawdown", "hit_rate",
                  "n_trades", "legs_per_flip"]],
    )

    # ---- Cost sensitivity (both intervals) ----
    cost_h = _cost_sweep("1h", force_refresh)
    cost_d = _cost_sweep("1d", force_refresh)
    cost_all = pd.concat([cost_h, cost_d], ignore_index=True)
    cost_all.to_csv(OUTPUT_DIR / "mega_cost_sensitivity.csv", index=False)

    print(); print("=" * 100)
    print("SHARPE vs cost (bps per leg)")
    print("=" * 100)
    pivot = cost_all.pivot_table(
        index=["spread", "interval"], columns="cost_bps_per_leg", values="sharpe"
    )
    with pd.option_context("display.float_format", "{:.3f}".format):
        print(pivot.to_string())

    # ---- Threshold sweep on the best (interval, spread) ----
    if not combined["sharpe"].dropna().empty:
        winner = combined.sort_values("sharpe", ascending=False).iloc[0]
        print(f"\n[sweep] winning combo: {winner['name']} @ {winner['interval']}, "
              f"running z-threshold sweep...")
        sweep = _threshold_sweep(winner["interval"], force_refresh, winner["name"])
        sweep.to_csv(
            OUTPUT_DIR / f"mega_sweep_{winner['name'].replace(' ', '_')}_{winner['interval']}.csv",
            index=False,
        )
        _print_table(
            f"{winner['name']} @ {winner['interval']} — z_entry/z_exit sweep",
            sweep,
        )

    # ---- Plots ----
    eq_all = {**(eq_h or {}), **(eq_d or {})}
    if eq_all:
        # equity curves grouped per interval
        for interval, eq_dict in (("1h", eq_h or {}), ("1d", eq_d or {})):
            if not eq_dict: continue
            fig, ax = plt.subplots(figsize=(11, 5))
            for k, eq in eq_dict.items():
                eq.plot(ax=ax, label=k)
            ax.set_title(f"Mega-cap concentration spread equity ({interval}, "
                         f"1bp/leg, z±2/0.5)")
            ax.set_ylabel("equity (1.0 start)")
            ax.legend(loc="best"); ax.grid(True, alpha=0.3)
            fig.tight_layout()
            p = OUTPUT_DIR / f"mega_equity_{interval}.png"
            fig.savefig(p, dpi=120)
            print(f"[plot] {p}")

        # z-score history on the winner
        winner_key = f"{winner['name']}|{winner['interval']}"
        z_dict = z_h if winner["interval"] == "1h" else z_d
        if z_dict and winner_key in z_dict:
            fig, ax = plt.subplots(figsize=(11, 4))
            z_dict[winner_key].plot(ax=ax, lw=0.6)
            for thr in (2, -2): ax.axhline(thr, color="red", lw=0.8, ls="--", alpha=0.6)
            for thr in (0.5, -0.5): ax.axhline(thr, color="green", lw=0.6, ls="--", alpha=0.4)
            ax.axhline(0, color="black", lw=0.5, alpha=0.3)
            ax.set_title(f"{winner['name']} ({winner['interval']}) "
                         f"residual z-score")
            ax.set_ylabel("z"); ax.grid(True, alpha=0.3)
            fig.tight_layout()
            p = OUTPUT_DIR / f"mega_z_{winner['name'].replace(' ', '_')}_{winner['interval']}.png"
            fig.savefig(p, dpi=120)
            print(f"[plot] {p}")

    # ---- Insights ----
    print(); print("=" * 100); print("HEADLINE INSIGHTS"); print("=" * 100)
    print("\n[1] Top-line ranking by Sharpe (all 6 combos, cost=1bp/leg):")
    print(combined.sort_values("sharpe", ascending=False)[
        ["name", "interval", "sharpe", "annual_return", "max_drawdown",
         "halflife_bars", "n_trades"]
    ].to_string(index=False))

    print("\n[done] outputs in", OUTPUT_DIR)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-refresh", action="store_true")
    args = ap.parse_args()
    run(force_refresh=args.force_refresh)


if __name__ == "__main__":
    main()
