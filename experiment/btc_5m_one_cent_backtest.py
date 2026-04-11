#!/usr/bin/env python3
"""
Analyze a "1-cent near expiry" BTC 5m strategy from logged observer snapshots.

This script uses rows from experiment/logs/btc_5m_49c_check.jsonl (or another JSONL file),
selects one snapshot per slug near expiry, and simulates buying a side when ask <= max_ask.

Outcome resolution is proxied from Binance BTCUSDT 5m candle open/close at slug timestamp:
  - Up wins if close > open
  - Down wins otherwise
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen


BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


@dataclass
class Trade:
    slug: str
    side: str
    ask: float
    seconds_remaining: int
    winner: str
    pnl_per_share: float


def _get_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_rows(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except json.JSONDecodeError:
                continue
    return rows


def _winner_for_slug(slug: str, cache: dict[str, str]) -> str | None:
    if slug in cache:
        return cache[slug]
    try:
        ts = int(slug.rsplit("-", 1)[-1])
    except Exception:
        return None
    url = f"{BINANCE_KLINES}?symbol=BTCUSDT&interval=5m&startTime={ts * 1000}&limit=1"
    try:
        kline = _get_json(url)
    except Exception:
        return None
    if not isinstance(kline, list) or not kline:
        return None
    try:
        o = float(kline[0][1])
        c = float(kline[0][4])
    except Exception:
        return None
    winner = "up" if c > o else "down"
    cache[slug] = winner
    return winner


def _pick_snapshot_per_slug(rows: list[dict[str, Any]], min_seconds: int, max_seconds: int) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        slug = str(r.get("slug") or "")
        secs = r.get("seconds_remaining")
        if not slug or not isinstance(secs, int):
            continue
        if secs < min_seconds or secs > max_seconds:
            continue
        grouped.setdefault(slug, []).append(r)

    picked: dict[str, dict[str, Any]] = {}
    for slug, items in grouped.items():
        # "towards very end": use the smallest seconds_remaining snapshot in the window.
        items_sorted = sorted(items, key=lambda x: int(x.get("seconds_remaining", 10**9)))
        picked[slug] = items_sorted[0]
    return picked


def _choose_side(snapshot: dict[str, Any], max_ask: float, mode: str) -> list[tuple[str, float]]:
    up_ask = snapshot.get("up_best_ask")
    down_ask = snapshot.get("down_best_ask")
    up_edge = snapshot.get("up_edge")
    down_edge = snapshot.get("down_edge")

    candidates: list[tuple[str, float, float | None]] = []
    if isinstance(up_ask, (int, float)) and up_ask <= max_ask:
        candidates.append(("up", float(up_ask), float(up_edge) if isinstance(up_edge, (int, float)) else None))
    if isinstance(down_ask, (int, float)) and down_ask <= max_ask:
        candidates.append(("down", float(down_ask), float(down_edge) if isinstance(down_edge, (int, float)) else None))

    if not candidates:
        return []

    if mode == "both":
        return [(side, ask) for side, ask, _ in candidates]
    if mode == "cheapest":
        side, ask, _ = min(candidates, key=lambda x: x[1])
        return [(side, ask)]
    if mode == "best-edge":
        side, ask, _ = max(candidates, key=lambda x: x[2] if x[2] is not None else -999.0)
        return [(side, ask)]

    # direct side mode
    for side, ask, _ in candidates:
        if side == mode:
            return [(side, ask)]
    return []


def run(
    log_file: str,
    max_ask: float,
    min_seconds: int,
    max_seconds: int,
    shares: float,
    mode: str,
) -> int:
    rows = _load_rows(log_file)
    chosen = _pick_snapshot_per_slug(rows, min_seconds=min_seconds, max_seconds=max_seconds)
    outcome_cache: dict[str, str] = {}
    trades: list[Trade] = []

    for slug, snap in sorted(chosen.items()):
        winner = _winner_for_slug(slug, outcome_cache)
        if winner is None:
            continue
        secs = int(snap["seconds_remaining"])
        picks = _choose_side(snap, max_ask=max_ask, mode=mode)
        for side, ask in picks:
            won = side == winner
            pnl_per_share = (1.0 - ask) if won else (-ask)
            trades.append(
                Trade(
                    slug=slug,
                    side=side,
                    ask=ask,
                    seconds_remaining=secs,
                    winner=winner,
                    pnl_per_share=pnl_per_share,
                )
            )

    n = len(trades)
    gross_cost = sum(t.ask * shares for t in trades)
    total_pnl = sum(t.pnl_per_share * shares for t in trades)
    wins = sum(1 for t in trades if t.side == t.winner)
    win_rate = (wins / n) if n > 0 else 0.0
    avg_ask = (sum(t.ask for t in trades) / n) if n > 0 else 0.0
    avg_pnl = (total_pnl / n) if n > 0 else 0.0
    roi_on_cost = (total_pnl / gross_cost) if gross_cost > 0 else 0.0

    print("=" * 72)
    print("BTC 5m one-cent strategy check")
    print("-" * 72)
    print(f"log_file={log_file}")
    print(f"slugs_in_window={len(chosen)}")
    print(f"mode={mode}  max_ask={max_ask:.4f}  window=[{min_seconds},{max_seconds}] sec")
    print(f"shares_per_trade={shares:.2f}")
    print("-" * 72)
    print(f"trades={n}  wins={wins}  win_rate={win_rate * 100:.2f}%")
    print(f"avg_ask={avg_ask:.4f}  gross_cost=${gross_cost:.4f}")
    print(f"total_pnl=${total_pnl:.4f}  avg_pnl_per_trade=${avg_pnl:.4f}")
    print(f"roi_on_cost={roi_on_cost * 100:.2f}%")
    if n:
        print("-" * 72)
        print("sample_trades(last 10):")
        for t in trades[-10:]:
            print(
                f"  {t.slug}  sec={t.seconds_remaining:>3}  side={t.side:<4}  "
                f"ask={t.ask:.4f}  winner={t.winner:<4}  pnl/share={t.pnl_per_share:+.4f}"
            )
    print("=" * 72)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Check a 1-cent near-expiry BTC 5m strategy from JSONL logs")
    parser.add_argument("--log-file", default="experiment/logs/btc_5m_49c_check.jsonl")
    parser.add_argument("--max-ask", type=float, default=0.01, help="Entry ask cap per side")
    parser.add_argument("--min-seconds", type=int, default=1, help="Minimum seconds remaining")
    parser.add_argument("--max-seconds", type=int, default=30, help="Maximum seconds remaining")
    parser.add_argument("--shares", type=float, default=1.0, help="Shares per selected trade")
    parser.add_argument(
        "--mode",
        choices=["best-edge", "cheapest", "up", "down", "both"],
        default="best-edge",
        help="How to select side(s) when one or both asks are <= max-ask",
    )
    args = parser.parse_args()
    raise SystemExit(
        run(
            log_file=args.log_file,
            max_ask=args.max_ask,
            min_seconds=args.min_seconds,
            max_seconds=args.max_seconds,
            shares=args.shares,
            mode=args.mode,
        )
    )


if __name__ == "__main__":
    main()
