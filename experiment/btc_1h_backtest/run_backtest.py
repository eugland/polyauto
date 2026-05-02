"""
Run the BTC 1H Up/Down backtest.

  python -m experiment.btc_1h_backtest.run_backtest \\
      --parquet db/binance_archive/BTCUSDT_1m.parquet \\
      --entry 0.54

Writes:
  experiment/btc_1h_backtest/results.parquet     # one row per signal fire
  experiment/btc_1h_backtest/REPORT.md           # human-readable
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from experiment.btc_1h_backtest import features as F
from experiment.btc_1h_backtest import strategies as S
from experiment.btc_1h_backtest import report as R

REPO = Path(__file__).resolve().parent.parent.parent
DEFAULT_PARQUET = REPO / "db" / "binance_archive" / "BTCUSDT_1m.parquet"
RESULTS_PARQUET = REPO / "experiment" / "btc_1h_backtest" / "results.parquet"
REPORT_MD = REPO / "experiment" / "btc_1h_backtest" / "REPORT.md"


def run(parquet_path: Path, entry: float, friction: float) -> pd.DataFrame:
    print(f"loading {parquet_path}")
    bars = F.load_klines(parquet_path)
    bars = F.attach_hour_keys(bars)
    bars = F.compute_per_bar_features(bars)

    # Drop hours with missing bars (Binance outages)
    hour_counts = bars.groupby("hour_id_et").size()
    full_hours = set(hour_counts[hour_counts == 60].index)
    n_dropped_hours = (hour_counts != 60).sum()
    print(f"hours: {len(hour_counts):,} total, {n_dropped_hours:,} dropped (incomplete bars)")
    bars = bars[bars["hour_id_et"].isin(full_hours)].copy()

    print("computing per-bar features (sigma, range, taker)…")
    print("computing fire-minute features (z, p_bs, vol_burst)…")
    fire = F.build_fire_features(bars)
    print(f"fire-minute rows: {len(fire):,}")

    print("computing hour outcomes…")
    hours = F.build_hour_outcomes(bars)

    # Run all strategies → one big results frame
    all_rows: list[pd.DataFrame] = []
    for name, fn in S.STRATEGY_VARIANTS:
        sig = fn(fire)
        if sig.empty:
            print(f"  {name}: 0 fires")
            continue
        sig = sig.merge(hours[["hour_id_et", "winner"]], on="hour_id_et", how="left")
        sig["won"] = (sig["side"] == sig["winner"]).astype(int)
        sig["strategy"] = name
        all_rows.append(sig)
        print(f"  {name}: {len(sig):,} fires, {sig['won'].mean()*100:.2f}% raw hit rate")

    if not all_rows:
        raise SystemExit("no strategies fired")

    res = pd.concat(all_rows, ignore_index=True)
    res = res.merge(hours[["hour_id_et", "hour_open", "hour_close"]], on="hour_id_et", how="left")
    # carry features for diagnostics
    res = res.merge(
        fire[["hour_id_et", "fire_minute", "displacement_z", "p_bs_up", "taker_ratio_5m"]],
        on=["hour_id_et", "fire_minute"], how="left",
    )

    # Save raw per-fire rows for follow-up
    duckdb.from_df(res).to_parquet(str(RESULTS_PARQUET))
    print(f"\nwrote {RESULTS_PARQUET}")

    # Build the report
    R.build_report(res, hours, entry=entry, friction=friction, out_path=REPORT_MD)
    print(f"wrote {REPORT_MD}")
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default=str(DEFAULT_PARQUET))
    ap.add_argument("--entry", type=float, default=0.54)
    ap.add_argument("--friction", type=float, default=0.02)
    args = ap.parse_args()
    run(Path(args.parquet), entry=args.entry, friction=args.friction)


if __name__ == "__main__":
    main()
