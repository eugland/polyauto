#!/usr/bin/env python3
"""
SPX 0DTE Box Spread Scanner

Fetches SPX options expiring today and prices every box spread within
a configurable wing around spot.

A box spread is delta-neutral and pays exactly (K2 - K1) at expiry:
  BUY  box:  long call K1 + short call K2 + long put K2 + short put K1
  SELL box: short call K1 + long call K2 + short put K2 + long put K1

Key columns:
  buy_cost     -- worst-case debit to buy the box (ask on longs, bid on shorts)
  sell_credit  -- worst-case credit to sell the box (bid on longs, ask on shorts)
  buy_edge     -- width - buy_cost  (>0 means box is cheap, pick it up)
  sell_edge    -- sell_credit - width  (>0 means box is rich, sell it)
  ba_spread    -- buy_cost - sell_credit  (bid/ask cost of the entire box)

Usage:
  python -m arb.spx_box
  python -m arb.spx_box --wing 500 --min-width 50 --max-width 300
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd
import yfinance as yf


def fetch_chain(ticker: str = "^SPX") -> tuple[pd.DataFrame, pd.DataFrame, float, str]:
    t = yf.Ticker(ticker)
    info = t.fast_info
    spot = getattr(info, "last_price", None) or getattr(info, "regularMarketPrice", None)
    if spot is None:
        hist = t.history(period="1d")
        spot = float(hist["Close"].iloc[-1]) if not hist.empty else 0.0

    expirations = t.options
    today = date.today().strftime("%Y-%m-%d")

    if today not in expirations:
        print(f"[!] No {ticker} options expiring today ({today})")
        print(f"    Nearest expirations: {list(expirations[:5])}")
        sys.exit(1)

    chain = t.option_chain(today)
    return chain.calls, chain.puts, float(spot), today


def build_merged(
    calls: pd.DataFrame,
    puts: pd.DataFrame,
    spot: float,
    wing: float,
) -> pd.DataFrame:
    def prep(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        df = df[["strike", "bid", "ask", "volume", "openInterest"]].copy()
        df.columns = ["strike", f"{prefix}_bid", f"{prefix}_ask", f"{prefix}_vol", f"{prefix}_oi"]
        df = df[df["strike"].between(spot - wing, spot + wing)]
        return df

    m = pd.merge(prep(calls, "c"), prep(puts, "p"), on="strike").dropna()
    # require a real market on both sides
    m = m[(m["c_bid"] > 0) & (m["p_bid"] > 0) & (m["c_ask"] > 0) & (m["p_ask"] > 0)]
    return m.set_index("strike")


def scan_boxes(
    merged: pd.DataFrame,
    min_width: float,
    max_width: float,
) -> pd.DataFrame:
    strikes = sorted(merged.index.unique())
    rows = []
    for i, k1 in enumerate(strikes):
        r1 = merged.loc[k1]
        for k2 in strikes[i + 1 :]:
            w = k2 - k1
            if w < min_width:
                continue
            if w > max_width:
                break
            r2 = merged.loc[k2]
            buy  = (r1["c_ask"] - r2["c_bid"]) + (r2["p_ask"] - r1["p_bid"])
            sell = (r1["c_bid"] - r2["c_ask"]) + (r2["p_bid"] - r1["p_ask"])
            rows.append({
                "K1": k1,
                "K2": k2,
                "width": w,
                "buy_cost":    round(buy,  2),
                "sell_credit": round(sell, 2),
                "buy_edge":    round(w - buy,  2),   # >0 = cheap box
                "sell_edge":   round(sell - w, 2),   # >0 = rich box
                "ba_spread":   round(buy - sell, 2),
            })
    return pd.DataFrame(rows)


def print_table(title: str, df: pd.DataFrame, cols: list[str] | None = None) -> None:
    print(f"\n{'=' * 80}")
    print(title)
    print(f"{'=' * 80}")
    print((df[cols] if cols else df).to_string(index=False))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="SPX 0DTE box spread scanner")
    ap.add_argument("--wing",      type=float, default=300.0, help="±pts around spot to search (default 300)")
    ap.add_argument("--min-width", type=float, default=25.0,  help="min box width in points (default 25)")
    ap.add_argument("--max-width", type=float, default=300.0, help="max box width in points (default 300)")
    ap.add_argument("--top",       type=int,   default=15,    help="rows to show in edge tables (default 15)")
    ap.add_argument("--all",       action="store_true",       help="print every combo, not just the edge tables")
    args = ap.parse_args(argv)

    print(f"Fetching SPX options expiring {date.today()} …")
    calls, puts, spot, expiry = fetch_chain("^SPX")
    print(f"  SPX spot : {spot:,.2f}")
    print(f"  Calls    : {len(calls)} strikes")
    print(f"  Puts     : {len(puts)} strikes")

    merged = build_merged(calls, puts, spot, args.wing)
    print(f"  Liquid strikes in ±{args.wing:.0f} wing: {len(merged)}")

    df = scan_boxes(merged, args.min_width, args.max_width)
    if df.empty:
        print("No box spreads found — check wing/width parameters or market hours.")
        return

    print(f"\n  Boxes scanned: {len(df)}")

    pd.set_option("display.float_format", "{:.2f}".format)

    if args.all:
        print_table(
            f"ALL {len(df)} BOX SPREADS  (SPX spot={spot:.0f}, wing=±{args.wing:.0f}, width {args.min_width:.0f}–{args.max_width:.0f})",
            df.sort_values(["K1", "K2"]),
        )

    # --- Buy-side: cheapest boxes (buy below fair value) ---
    buy_ops = df[df["buy_edge"] > 0].sort_values("buy_edge", ascending=False).head(args.top)
    if not buy_ops.empty:
        print_table(
            f"TOP {len(buy_ops)} BUY OPPORTUNITIES  (buy_cost < width → positive edge at expiry)",
            buy_ops,
            ["K1", "K2", "width", "buy_cost", "buy_edge", "ba_spread"],
        )
    else:
        print("\n[buy] No boxes available below fair value (all buy_cost >= width).")

    # --- Sell-side: richest boxes (sell above fair value) ---
    sell_ops = df[df["sell_edge"] > 0].sort_values("sell_edge", ascending=False).head(args.top)
    if not sell_ops.empty:
        print_table(
            f"TOP {len(sell_ops)} SELL OPPORTUNITIES  (sell_credit > width → positive edge at expiry)",
            sell_ops,
            ["K1", "K2", "width", "sell_credit", "sell_edge", "ba_spread"],
        )
    else:
        print("\n[sell] No boxes available above fair value (all sell_credit <= width).")

    # --- Summary: tightest bid/ask on the box (most liquid) ---
    tightest = df.nsmallest(args.top, "ba_spread")
    print_table(
        f"TOP {len(tightest)} TIGHTEST BOX BID/ASK  (most liquid, sorted by ba_spread)",
        tightest,
        ["K1", "K2", "width", "buy_cost", "sell_credit", "buy_edge", "sell_edge", "ba_spread"],
    )

    # sanity: boxes where the mid roughly equals width (efficient pricing)
    df["mid"] = (df["buy_cost"] + df["sell_credit"]) / 2
    df["mid_vs_width"] = df["mid"] - df["width"]
    print(f"\nMid vs width  — mean: {df['mid_vs_width'].mean():.2f}  "
          f"std: {df['mid_vs_width'].std():.2f}  "
          f"min: {df['mid_vs_width'].min():.2f}  "
          f"max: {df['mid_vs_width'].max():.2f}")


if __name__ == "__main__":
    main()
