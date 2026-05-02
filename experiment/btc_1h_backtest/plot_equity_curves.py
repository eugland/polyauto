"""
Plot per-strategy cumulative PnL equity curves over the 1-year backtest.

PnL framing matches REPORT_PRE_CANDLE.md:
  entry $0.50, $0.02 friction per round-trip
  win  → +$0.48
  loss → -$0.52
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

ROOT = Path(__file__).resolve().parent
PARQUET = ROOT / "results_pre_candle.parquet"
OUT_PNG = ROOT / "equity_curves_1y.png"

ENTRY = 0.50
FRICTION = 0.02
PNL_WIN = (1.0 - ENTRY) - FRICTION   # +0.48
PNL_LOSS = -(ENTRY + FRICTION)       # -0.52
FIRE_OFFSET = -40                    # mirrors live bot's enter-once point


def main() -> None:
    df = duckdb.sql(f"""
        SELECT strategy, target_hour, won
        FROM '{PARQUET}'
        WHERE fire_offset_min = {FIRE_OFFSET}
          AND winner IS NOT NULL
        ORDER BY target_hour
    """).df()
    df["target_hour"] = pd.to_datetime(df["target_hour"], utc=True)
    df["pnl"] = df["won"].map({1: PNL_WIN, 0: PNL_LOSS}).astype(float)

    summary = (
        df.groupby("strategy")
          .agg(n=("won", "size"), wins=("won", "sum"), total_pnl=("pnl", "sum"))
          .assign(hit_rate=lambda d: d.wins / d.n,
                  ev_per_share=lambda d: d.total_pnl / d.n)
          .sort_values("total_pnl", ascending=False)
    )
    print(summary.to_string())

    # All-strategies plot
    fig, ax = plt.subplots(figsize=(14, 9))
    cmap = plt.get_cmap("tab20")
    for i, strat in enumerate(summary.index):
        sub = df[df.strategy == strat].sort_values("target_hour")
        if len(sub) < 5:
            continue
        eq = sub["pnl"].cumsum()
        ls = "--" if strat.startswith("baseline") else "-"
        lw = 1.0 if strat.startswith("baseline") else 1.6
        ax.plot(sub["target_hour"], eq, label=strat, color=cmap(i % 20),
                linestyle=ls, linewidth=lw)
    ax.axhline(0, color="black", linewidth=0.6, alpha=0.4)
    ax.set_title(
        f"BTC 1H pre-candle strategies — 1y equity curves "
        f"(entry ${ENTRY}, friction ${FRICTION}, fire_offset {FIRE_OFFSET}m)"
    )
    ax.set_ylabel("cumulative PnL per share ($)")
    ax.set_xlabel("target hour (UTC)")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.grid(True, alpha=0.25)
    ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5),
              fontsize=8, frameon=False, ncol=1)
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=120, bbox_inches="tight")
    print(f"\nwrote {OUT_PNG}")

    # Top-12 plot for readability
    top = summary.head(12).index.tolist()
    fig2, ax2 = plt.subplots(figsize=(14, 7))
    for i, strat in enumerate(top):
        sub = df[df.strategy == strat].sort_values("target_hour")
        eq = sub["pnl"].cumsum()
        ax2.plot(sub["target_hour"], eq, label=strat, color=cmap(i % 20), linewidth=1.7)
    ax2.axhline(0, color="black", linewidth=0.6, alpha=0.4)
    ax2.set_title("Top-12 strategies by total PnL — 1y equity curves")
    ax2.set_ylabel("cumulative PnL per share ($)")
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax2.grid(True, alpha=0.25)
    ax2.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=9, frameon=False)
    fig2.tight_layout()
    out2 = ROOT / "equity_curves_top12_1y.png"
    fig2.savefig(out2, dpi=120, bbox_inches="tight")
    print(f"wrote {out2}")


if __name__ == "__main__":
    main()
