"""
Pre-candle backtest: fire 70-100 min BEFORE the target candle's close.

Run:
  python -m experiment.btc_1h_backtest.run_backtest_pre_candle --entry 0.50

Writes:
  experiment/btc_1h_backtest/results_pre_candle.parquet
  experiment/btc_1h_backtest/REPORT_PRE_CANDLE.md
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from experiment.btc_1h_backtest import features_pre_candle as F
from experiment.btc_1h_backtest import strategies_pre_candle as S

REPO = Path(__file__).resolve().parent.parent.parent
DEFAULT_PARQUET = REPO / "db" / "binance_archive" / "BTCUSDT_1m.parquet"
RESULTS_PARQUET = REPO / "experiment" / "btc_1h_backtest" / "results_pre_candle.parquet"
REPORT_MD = REPO / "experiment" / "btc_1h_backtest" / "REPORT_PRE_CANDLE.md"


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt((p * (1 - p) / n) + z * z / (4 * n * n)) / denom
    return centre - half, centre + half


def first_fire_per_hour(res: pd.DataFrame) -> pd.DataFrame:
    return (res.sort_values(["strategy", "target_hour", "fire_offset_min"])
              .drop_duplicates(["strategy", "target_hour"], keep="first"))


def summarize(res: pd.DataFrame, label: str, entry: float, friction: float) -> pd.DataFrame:
    rows = []
    for name, sub in res.groupby("strategy", sort=False):
        n = len(sub)
        wins = int(sub["won"].sum())
        if n == 0:
            continue
        hit = wins / n
        wlo, whi = wilson(wins, n)
        ev = hit * (1.0 - entry) - (1.0 - hit) * entry - friction
        rows.append({
            "strategy": name, "set": label,
            "n": n, "wins": wins, "hit_rate": hit,
            "wilson_lo": wlo, "wilson_hi": whi,
            "ev_per_share": ev,
            "annual_pnl_per_share": ev * n,
        })
    return pd.DataFrame(rows).sort_values("wilson_lo", ascending=False)


def monthly_table(res: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if res.empty:
        return pd.DataFrame(), pd.DataFrame()
    df = res.copy()
    df["month"] = pd.to_datetime(df["target_hour"]).dt.to_period("M").astype(str)
    g = df.groupby(["strategy", "month"])
    pivot = g["won"].mean().unstack("month")
    counts = g.size().unstack("month").fillna(0).astype(int)
    return pivot, counts


def offset_bucket(res: pd.DataFrame) -> pd.DataFrame:
    df = res.copy()
    bucket = pd.cut(
        df["fire_offset_min"],
        bins=[-41, -30, -20, -10],
        labels=["[-40,-30]", "(-30,-20]", "(-20,-10]"],
    )
    df["offset_bucket"] = bucket
    g = df.groupby(["strategy", "offset_bucket"], observed=True)
    out = pd.DataFrame({"n": g.size(), "hit_rate": g["won"].mean()}).reset_index()
    return out


def side_table(res: pd.DataFrame) -> pd.DataFrame:
    g = res.groupby(["strategy", "side"], observed=True)
    out = pd.DataFrame({"n": g.size(), "hit_rate": g["won"].mean()}).reset_index()
    return out


def sanity_random_shuffle(res: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for name, sub in res.groupby("strategy", sort=False):
        winners = sub["winner"].sample(n=len(sub), random_state=int(rng.integers(1_000_000_000))).reset_index(drop=True)
        won_shuf = (sub["side"].reset_index(drop=True) == winners).astype(int)
        rows.append({"strategy": name, "n": len(sub), "hit_rate_shuffled": won_shuf.mean()})
    return pd.DataFrame(rows)


def run(parquet: Path, entry: float, friction: float):
    print(f"loading 1m + 1h klines from {parquet}")
    bars_1m, bars_1h = F.load_klines_1m_and_1h(parquet)
    print(f"  1m bars: {len(bars_1m):,}; 1h bars: {len(bars_1h):,}")

    print("building per-1H indicators (RSI, ret_Nh, taker, sigma_24h)")
    print("building (target_hour x fire_offset) feature rows")
    fire = F.build_pre_candle_features(bars_1m, bars_1h, fire_offsets_min=list(range(-40, -9)))
    print(f"  fire rows: {len(fire):,} ({fire['target_hour'].nunique():,} target hours x {fire['fire_offset_min'].nunique()} offsets)")

    outcomes = F.build_outcomes(bars_1h)
    # Polymarket up rate for sanity
    up_rate = (outcomes["winner"] == "up").mean()
    print(f"  up_rate over {len(outcomes):,} hours: {up_rate*100:.2f}%")

    # Verify "near 50-50" is true at fire time. We approximate fair value as
    # P(close[H+60min] >= open[H]) where open[H] is unknown. Under a driftless
    # GBM, P_up ≈ 0.5 + small. We check that the historical UP-rate within each
    # bucket of multi-hour-momentum is close to 50% — this is the "near 50-50"
    # claim the user wanted.
    near5050 = []
    for col, label in [("ret_3h", "ret_3h"), ("ret_1h", "ret_1h"), ("rsi_14", "rsi_14")]:
        merged = fire.drop_duplicates("target_hour").merge(outcomes[["target_hour","winner"]], on="target_hour", how="left").dropna(subset=[col])
        merged["bucket"] = pd.qcut(merged[col], q=5, duplicates="drop")
        for b, grp in merged.groupby("bucket", observed=True):
            up = (grp["winner"] == "up").mean()
            near5050.append({"feature": label, "bucket": str(b), "n": len(grp), "up_rate": up})
    near5050_df = pd.DataFrame(near5050)

    # Run strategies
    rows: list[pd.DataFrame] = []
    for name, fn in S.get_registry(outcomes):
        sig = fn(fire)
        if sig.empty:
            print(f"  {name}: 0 fires")
            continue
        sig = sig.merge(outcomes[["target_hour", "winner"]], on="target_hour", how="left")
        sig["won"] = (sig["side"] == sig["winner"]).astype(int)
        sig["strategy"] = name
        rows.append(sig)
        print(f"  {name}: {len(sig):,} fires, {sig['won'].mean()*100:.2f}% raw hit rate")

    if not rows:
        raise SystemExit("no strategies fired")

    res = pd.concat(rows, ignore_index=True)
    duckdb.from_df(res).to_parquet(str(RESULTS_PARQUET))
    print(f"\nwrote {RESULTS_PARQUET}")

    build_report(res, outcomes, near5050_df, entry=entry, friction=friction)
    print(f"wrote {REPORT_MD}")


def build_report(res: pd.DataFrame, outcomes: pd.DataFrame, near5050: pd.DataFrame, entry: float, friction: float):
    ff = first_fire_per_hour(res)
    sum_ff = summarize(ff, "first-fire-per-hour", entry=entry, friction=friction)
    sum_all = summarize(res, "every-fire", entry=entry, friction=friction)
    pivot_m, count_m = monthly_table(ff)
    bucket_off = offset_bucket(ff)
    bucket_side = side_table(ff)
    shuf = sanity_random_shuffle(ff)

    L = []
    L.append("# BTC 1H Up/Down — pre-candle (70–100 min before close) backtest")
    L.append("")
    L.append("## What this tests")
    L.append("")
    L.append("Bot fires 10–40 min BEFORE the target candle even opens (= 70–100 min before market resolution). At that point the candle's open price is in the future and Polymarket's fair value is naturally close to 50¢. Strategies look at multi-hour context (3-hour return, RSI, sustained flow, time-of-day) to predict the next-hour candle's direction. Exit = hold to candle close, redeem at $1 if won, $0 if lost.")
    L.append("")
    L.append("## Data")
    L.append(f"- ET hours backtested: **{len(outcomes):,}**, span **{outcomes['target_hour'].min()}** → **{outcomes['target_hour'].max()}**")
    L.append(f"- Up-share of hours overall: **{(outcomes['winner']=='up').mean()*100:.2f}%** (close to 50% — symmetric)")
    L.append(f"- Entry assumed flat at **${entry:.2f}**; friction **${friction:.2f}** per round-trip.")
    L.append(f"- Breakeven hit rate at this entry = **{(entry+friction)*100:.1f}%**.")
    L.append("")
    L.append("## Sanity checks")
    L.append("")
    L.append("**Random-outcome shuffle** — should collapse to ~50%:")
    L.append("")
    L.append(shuf.to_markdown(index=False, floatfmt=".4f"))
    L.append("")

    L.append("## 'Near 50-50' verification — Up-rate by feature quintile")
    L.append("")
    L.append("Each row asks: when this feature is in this quintile bucket, what fraction of hours close Up? If pre-candle entries really are 50-50, every bucket should be near 0.50. Sharp deviations would be exploitable signals.")
    L.append("")
    L.append(near5050.to_markdown(index=False, floatfmt=".4f"))
    L.append("")

    L.append("## Headline: first-fire-per-hour, flat 0.50 entry")
    L.append("")
    L.append(sum_ff.to_markdown(index=False, floatfmt=".4f"))
    L.append("")

    L.append("## Every-fire (footnote — 31 fires per hour, inflated N)")
    L.append("")
    L.append(sum_all.to_markdown(index=False, floatfmt=".4f"))
    L.append("")

    if not pivot_m.empty:
        L.append("## Monthly hit-rate stability (first-fire-per-hour)")
        L.append("")
        L.append(pivot_m.to_markdown(floatfmt=".3f"))
        L.append("")
        L.append("Per-month sample counts:")
        L.append("")
        L.append(count_m.to_markdown())
        L.append("")

    if not bucket_off.empty:
        L.append("## Hit rate by fire-time offset (minutes before target candle opens)")
        L.append("")
        L.append(bucket_off.pivot(index="strategy", columns="offset_bucket", values="hit_rate").to_markdown(floatfmt=".4f"))
        L.append("")

    if not bucket_side.empty:
        L.append("## Hit rate by side")
        L.append("")
        L.append(bucket_side.pivot(index="strategy", columns="side", values="hit_rate").to_markdown(floatfmt=".4f"))
        L.append("")
        L.append("Sample counts:")
        L.append("")
        L.append(bucket_side.pivot(index="strategy", columns="side", values="n").fillna(0).astype(int).to_markdown())
        L.append("")

    REPORT_MD.write_text("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default=str(DEFAULT_PARQUET))
    ap.add_argument("--entry", type=float, default=0.50)
    ap.add_argument("--friction", type=float, default=0.02)
    args = ap.parse_args()
    run(Path(args.parquet), entry=args.entry, friction=args.friction)


if __name__ == "__main__":
    main()
