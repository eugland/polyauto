#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from experiment.eth_tail_backtest import (
    SimParams,
    _digital_up_fair,
    _hourly_sigma_annual_from_closes,
    _load_1m_klines_cached,
    _synthetic_ask,
    _synthetic_bid,
)


@dataclass(frozen=True)
class Config:
    years: float = 1.0
    lookback_hours: int = 168
    initial_balance: float = 100.0
    balance_fraction: float = 0.25
    cache_db_path: str = "experiment/cache/binance_klines.sqlite"
    out_dir: str = "experiment"
    k_min: float = 0.05
    k_max: float = 1.00
    k_step: float = 0.05
    t_seconds_values: tuple[int, ...] = (120, 60)
    sell_target: float = 0.999
    stop_loss: float = 0.75
    stop_fill_below_bid: float = 0.01


def _frange(start: float, stop: float, step: float) -> list[float]:
    vals: list[float] = []
    cur = start
    while cur <= stop + 1e-12:
        vals.append(round(cur, 10))
        cur += step
    return vals


def _build_minute_rows(rows: list[list[Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        ts = datetime.fromtimestamp(int(r[0]) / 1000, tz=timezone.utc)
        out.append({"ts": ts, "open": float(r[1]), "close": float(r[4])})
    return out


def _min_bid_from_seconds(k_value: float, seconds_remaining: int) -> float:
    t_minutes = max(float(seconds_remaining) / 60.0, 1.0 / 60.0)
    return 1.0 - k_value / math.sqrt(t_minutes)


def _simulate_one(
    minute_rows: list[dict[str, Any]],
    cfg: Config,
    k_value: float,
    t_seconds: int,
) -> dict[str, Any]:
    balance = cfg.initial_balance
    trades = 0
    wins = 0
    stop_loss_exits = 0
    target_exits = 0
    resolution_wins = 0

    hourly_closes: deque[float] = deque(maxlen=cfg.lookback_hours + 1)
    prev_row: dict[str, Any] | None = None
    prev_hour: tuple[int, int, int, int] | None = None

    by_candle: dict[datetime, list[dict[str, Any]]] = {}
    for r in minute_rows:
        ts = r["ts"]
        start_min = (ts.minute // 15) * 15
        cstart = ts.replace(minute=start_min, second=0, microsecond=0)
        by_candle.setdefault(cstart, []).append(r)

    candle_keys = sorted(by_candle.keys())
    params = SimParams()
    t_minutes_entry = max(float(t_seconds) / 60.0, 1.0 / 60.0)
    entry_bucket_minutes = max(1, min(14, math.ceil(t_minutes_entry)))
    entry_idx = 15 - entry_bucket_minutes

    for cstart in candle_keys:
        bars = sorted(by_candle[cstart], key=lambda x: x["ts"])
        if len(bars) < 15:
            continue

        strike = bars[0]["open"]
        close_bar = bars[14]
        if entry_idx < 0 or entry_idx >= 15:
            continue
        entry_bar = bars[entry_idx]

        for b in bars:
            hkey = (b["ts"].year, b["ts"].month, b["ts"].day, b["ts"].hour)
            if prev_hour is not None and hkey != prev_hour and prev_row is not None:
                hourly_closes.append(prev_row["close"])
            prev_hour = hkey
            prev_row = b
            if b["ts"] >= entry_bar["ts"]:
                break

        sigma_eth_annual = _hourly_sigma_annual_from_closes(hourly_closes)
        if sigma_eth_annual is None or len(hourly_closes) < cfg.lookback_hours:
            continue

        if strike <= 0:
            continue

        t_years_entry = t_minutes_entry / (365.0 * 24.0 * 60.0)
        fair_up = _digital_up_fair(
            spot=entry_bar["close"],
            strike=strike,
            years_to_expiry=t_years_entry,
            sigma=sigma_eth_annual,
        )
        if fair_up is None:
            continue
        fair_down = 1.0 - fair_up

        up_favored = fair_up >= 0.5
        down_favored = fair_down >= 0.5
        up_ask = _synthetic_ask(fair_up, t_minutes_entry, up_favored, params)
        up_bid = _synthetic_bid(fair_up, t_minutes_entry, up_favored, params)
        down_ask = _synthetic_ask(fair_down, t_minutes_entry, down_favored, params)
        down_bid = _synthetic_bid(fair_down, t_minutes_entry, down_favored, params)
        min_bid = _min_bid_from_seconds(k_value, t_seconds)

        side: str | None = None
        ask = 0.0
        if up_bid >= min_bid and up_ask > 0:
            side = "up"
            ask = up_ask
        elif down_bid >= min_bid and down_ask > 0:
            side = "down"
            ask = down_ask
        if side is None:
            continue

        stake = balance * cfg.balance_fraction
        if stake <= 0 or ask <= 0:
            continue

        shares = stake / ask
        cost = shares * ask
        balance -= cost

        exited = False
        for idx in range(entry_idx, 15):
            bar = bars[idx]
            rem_minutes = max(float(15 - idx), 1.0 / 60.0)
            t_years = rem_minutes / (365.0 * 24.0 * 60.0)
            fair_up_step = _digital_up_fair(
                spot=bar["close"],
                strike=strike,
                years_to_expiry=t_years,
                sigma=sigma_eth_annual,
            )
            if fair_up_step is None:
                continue
            side_fair = fair_up_step if side == "up" else (1.0 - fair_up_step)
            favored = side_fair >= 0.5
            bid = _synthetic_bid(side_fair, rem_minutes, favored, params)

            if bid >= cfg.sell_target:
                balance += shares * cfg.sell_target
                wins += 1
                target_exits += 1
                trades += 1
                exited = True
                break

            if bid < cfg.stop_loss:
                exit_price = max(round(bid - cfg.stop_fill_below_bid, 3), 0.01)
                balance += shares * exit_price
                stop_loss_exits += 1
                trades += 1
                exited = True
                break

        if exited:
            continue

        is_up = close_bar["close"] >= strike
        won = is_up if side == "up" else (not is_up)
        payout = shares if won else 0.0
        balance += payout
        trades += 1
        wins += int(won)
        resolution_wins += int(won)

    losses = trades - wins
    success_rate = (wins / trades * 100.0) if trades else 0.0
    return {
        "k_value": k_value,
        "t_seconds": t_seconds,
        "entry_bucket_minutes": entry_bucket_minutes,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "success_rate_pct": round(success_rate, 4),
        "target_exits": target_exits,
        "stop_loss_exits": stop_loss_exits,
        "resolution_wins": resolution_wins,
        "start_balance": cfg.initial_balance,
        "end_balance": round(balance, 6),
        "return_pct": round(((balance / cfg.initial_balance) - 1.0) * 100.0, 6),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETH 15m live-rule backtest (seconds-based threshold + sell target + stop-loss)")
    p.add_argument("--years", type=float, default=1.0)
    p.add_argument("--initial-balance", type=float, default=100.0)
    p.add_argument("--balance-fraction", type=float, default=0.25)
    p.add_argument("--lookback-hours", type=int, default=168)
    p.add_argument("--k-min", type=float, default=0.05)
    p.add_argument("--k-max", type=float, default=1.00)
    p.add_argument("--k-step", type=float, default=0.05)
    p.add_argument("--t-seconds", default="120,60", help="Comma-separated seconds-to-expiry sweep, e.g. 120,90,60")
    p.add_argument("--sell-target", type=float, default=0.999)
    p.add_argument("--stop-loss", type=float, default=0.75)
    p.add_argument("--stop-fill-below-bid", type=float, default=0.01)
    p.add_argument("--cache-db", default="experiment/cache/binance_klines.sqlite")
    p.add_argument("--out-dir", default="experiment")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    t_seconds_values = tuple(int(x.strip()) for x in args.t_seconds.split(",") if x.strip())
    if not t_seconds_values:
        raise SystemExit("No t-seconds values provided")
    if any(t < 1 or t > 14 * 60 for t in t_seconds_values):
        raise SystemExit("All t-seconds values must be between 1 and 840 for 15m candles")

    cfg = Config(
        years=args.years,
        initial_balance=args.initial_balance,
        balance_fraction=args.balance_fraction,
        lookback_hours=args.lookback_hours,
        k_min=args.k_min,
        k_max=args.k_max,
        k_step=args.k_step,
        t_seconds_values=t_seconds_values,
        sell_target=args.sell_target,
        stop_loss=args.stop_loss,
        stop_fill_below_bid=args.stop_fill_below_bid,
        cache_db_path=args.cache_db,
        out_dir=args.out_dir,
    )

    end_dt = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=365 * cfg.years)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print("Loading ETHUSDT 1m...")
    eth_raw = _load_1m_klines_cached("ETHUSDT", start_ms, end_ms, cfg.cache_db_path)
    minute_rows = _build_minute_rows(eth_raw)
    if len(minute_rows) < 100_000:
        raise RuntimeError(f"Not enough 1m rows: {len(minute_rows)}")

    k_values = _frange(cfg.k_min, cfg.k_max, cfg.k_step)
    rows: list[dict[str, Any]] = []

    for t_seconds in cfg.t_seconds_values:
        for k in k_values:
            s = _simulate_one(minute_rows, cfg, k, t_seconds)
            rows.append(s)
            print(
                f"T-{t_seconds}s k={k:.2f} -> trades={s['trades']} "
                f"win={s['success_rate_pct']}% end={s['end_balance']} "
                f"target={s['target_exits']} stop={s['stop_loss_exits']}"
            )

    rows_sorted = sorted(rows, key=lambda r: (r["end_balance"], r["success_rate_pct"]), reverse=True)
    best = rows_sorted[0] if rows_sorted else {}

    report = {
        "config": {
            "years": cfg.years,
            "start_utc": start_dt.isoformat(),
            "end_utc": end_dt.isoformat(),
            "market": "ETH 15m up/down",
            "entry_constraint": "Live-style entry gate: bid >= min_bid(seconds), no ask cap",
            "balance_fraction": cfg.balance_fraction,
            "k_values": k_values,
            "t_seconds_values": list(cfg.t_seconds_values),
            "lookback_hours": cfg.lookback_hours,
            "sell_target": cfg.sell_target,
            "stop_loss": cfg.stop_loss,
            "stop_fill_below_bid": cfg.stop_fill_below_bid,
            "cache_db_path": cfg.cache_db_path,
        },
        "summary": {
            "runs": len(rows),
            "best": best,
            "top_10": rows_sorted[:10],
        },
        "results": rows,
    }

    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "eth15_btc_tail_sweep_report.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\nOptimization complete")
    print(
        f"Best: T-{best.get('t_seconds')}s, k={best.get('k_value')} "
        f"end={best.get('end_balance')} return={best.get('return_pct')}%"
    )
    print(f"Report: {out_json}")


if __name__ == "__main__":
    main()
