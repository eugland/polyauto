#!/usr/bin/env python3
"""
ETH tail-capture backtest with:
- 1-minute Binance candles
- Entry window scan: T-20 through T-3 (first valid signal)
- Fair -> ask/bid emulation calibrated from live observations
- Past-only rolling sigma (no future leakage)
- Position sizing: 25% of current balance per trade
- Stop-loss emulation: trigger on synthetic bid < stop, fill at (bid - 0.01)

Outputs:
- experiment/eth_tail_backtest_report.json
- experiment/eth_tail_backtest_report.html
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from time import perf_counter
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


@dataclass
class SimParams:
    initial_balance: float = 100.0
    years: float = 2.0
    lookback_hours: int = 168
    k_value: float = 0.12
    buy_max: float = 0.98
    entry_start_minute: int = 40  # T-20
    entry_end_minute: int = 57    # T-3
    close_minute: int = 59        # T-1
    balance_fraction: float = 0.25
    stop_loss: float = 0.75
    # favored side premium ~= 8.5c at T-10 and ~=5.6c at T-6
    favored_premium_intercept: float = 0.0125
    favored_premium_slope: float = 0.00725
    favored_premium_min: float = 0.02
    favored_premium_max: float = 0.09
    non_favored_premium: float = 0.005
    spread_floor: float = 0.004
    spread_slope: float = 0.0002
    stop_fill_below_bid: float = 0.01
    cache_db_path: str = "experiment/cache/binance_klines.sqlite"


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _digital_up_fair(spot: float, strike: float, years_to_expiry: float, sigma: float, r: float = 0.0) -> float | None:
    if spot <= 0 or strike <= 0 or years_to_expiry <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(years_to_expiry)
    d2 = (math.log(spot / strike) + (r - 0.5 * sigma * sigma) * years_to_expiry) / (sigma * sqrt_t)
    return min(1.0, max(0.0, _norm_cdf(d2)))


def _favored_premium(mins_remaining: int, p: SimParams) -> float:
    raw = p.favored_premium_intercept + p.favored_premium_slope * mins_remaining
    return _clamp(raw, p.favored_premium_min, p.favored_premium_max)


def _synthetic_ask(fair: float, mins_remaining: int, favored: bool, p: SimParams) -> float:
    premium = _favored_premium(mins_remaining, p) if favored else p.non_favored_premium
    return _clamp(fair + premium, 0.01, 0.995)


def _synthetic_bid(fair: float, mins_remaining: int, favored: bool, p: SimParams) -> float:
    ask = _synthetic_ask(fair, mins_remaining, favored, p)
    spread = p.spread_floor + p.spread_slope * mins_remaining
    return _clamp(ask - spread, 0.01, 0.995)


def _fetch_1m_klines(symbol: str, start_ms: int, end_ms: int, limit: int = 1000) -> list[list[Any]]:
    out: list[list[Any]] = []
    cur = start_ms
    retry_sleep = 0.5
    while cur < end_ms:
        for attempt in range(5):
            try:
                resp = requests.get(
                    BINANCE_KLINES_URL,
                    params={
                        "symbol": symbol,
                        "interval": "1m",
                        "startTime": cur,
                        "endTime": end_ms,
                        "limit": limit,
                    },
                    timeout=25,
                )
                resp.raise_for_status()
                chunk = resp.json()
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(retry_sleep * (attempt + 1))
        if not chunk:
            break
        out.extend(chunk)
        last_open_ms = int(chunk[-1][0])
        cur = last_open_ms + 60_000
        if len(chunk) < limit:
            break
        time.sleep(0.02)
    return out


def _cache_connect(path: str) -> sqlite3.Connection:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume TEXT NOT NULL,
            close_time INTEGER NOT NULL,
            quote_asset_volume TEXT NOT NULL,
            number_of_trades INTEGER NOT NULL,
            taker_buy_base_asset_volume TEXT NOT NULL,
            taker_buy_quote_asset_volume TEXT NOT NULL,
            ignore_field TEXT NOT NULL,
            PRIMARY KEY (symbol, interval, open_time)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_sym_int_time ON klines(symbol, interval, open_time)")
    return conn


def _cache_insert_rows(conn: sqlite3.Connection, symbol: str, interval: str, rows: list[list[Any]]) -> None:
    if not rows:
        return
    payload = [
        (
            symbol,
            interval,
            int(r[0]),
            str(r[1]),
            str(r[2]),
            str(r[3]),
            str(r[4]),
            str(r[5]),
            int(r[6]),
            str(r[7]),
            int(r[8]),
            str(r[9]),
            str(r[10]),
            str(r[11]),
        )
        for r in rows
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO klines (
            symbol, interval, open_time, open, high, low, close, volume,
            close_time, quote_asset_volume, number_of_trades,
            taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_field
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()


def _cache_range_bounds(conn: sqlite3.Connection, symbol: str, interval: str) -> tuple[int | None, int | None]:
    row = conn.execute(
        "SELECT MIN(open_time), MAX(open_time) FROM klines WHERE symbol = ? AND interval = ?",
        (symbol, interval),
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _cache_count_in_range(conn: sqlite3.Connection, symbol: str, interval: str, start_ms: int, end_ms: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM klines
        WHERE symbol = ? AND interval = ? AND open_time >= ? AND open_time < ?
        """,
        (symbol, interval, start_ms, end_ms),
    ).fetchone()
    return int(row[0] if row else 0)


def _cache_load_range(conn: sqlite3.Connection, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list[Any]]:
    rows = conn.execute(
        """
        SELECT open_time, open, high, low, close, volume, close_time, quote_asset_volume,
               number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_field
        FROM klines
        WHERE symbol = ? AND interval = ? AND open_time >= ? AND open_time < ?
        ORDER BY open_time
        """,
        (symbol, interval, start_ms, end_ms),
    ).fetchall()
    return [
        [
            int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[6]), r[7], int(r[8]), r[9], r[10], r[11]
        ]
        for r in rows
    ]


def _load_1m_klines_cached(symbol: str, start_ms: int, end_ms: int, cache_db_path: str) -> list[list[Any]]:
    interval = "1m"
    expected = max(0, (end_ms - start_ms) // 60_000)
    conn = _cache_connect(cache_db_path)
    try:
        lo, hi = _cache_range_bounds(conn, symbol, interval)
        if lo is None or hi is None:
            fetched = _fetch_1m_klines(symbol, start_ms, end_ms)
            _cache_insert_rows(conn, symbol, interval, fetched)
        else:
            # Extend cache backward/forward only if needed.
            if start_ms < lo:
                fetched = _fetch_1m_klines(symbol, start_ms, lo)
                _cache_insert_rows(conn, symbol, interval, fetched)
            if end_ms > (hi + 60_000):
                fetched = _fetch_1m_klines(symbol, hi + 60_000, end_ms)
                _cache_insert_rows(conn, symbol, interval, fetched)

        cached_count = _cache_count_in_range(conn, symbol, interval, start_ms, end_ms)
        if cached_count < expected:
            # If there are holes, refill full range once and upsert.
            fetched = _fetch_1m_klines(symbol, start_ms, end_ms)
            _cache_insert_rows(conn, symbol, interval, fetched)

        out = _cache_load_range(conn, symbol, interval, start_ms, end_ms)
        return out
    finally:
        conn.close()


def _hourly_sigma_annual_from_closes(hourly_closes: deque[float]) -> float | None:
    vals = list(hourly_closes)
    if len(vals) < 3:
        return None
    rets: list[float] = []
    for i in range(1, len(vals)):
        prev, cur = vals[i - 1], vals[i]
        if prev > 0 and cur > 0:
            rets.append(math.log(cur / prev))
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    sigma_hourly = math.sqrt(max(0.0, var))
    sigma_annual = sigma_hourly * math.sqrt(365.0 * 24.0)
    return sigma_annual if sigma_annual > 0 else None


def run_backtest(params: SimParams) -> dict[str, Any]:
    t0 = perf_counter()
    end_dt = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=365 * params.years)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    t_fetch0 = perf_counter()
    klines_1m = _load_1m_klines_cached("ETHUSDT", start_ms, end_ms, params.cache_db_path)
    t_fetch1 = perf_counter()
    if len(klines_1m) < 1_000:
        raise RuntimeError(f"Not enough 1m data fetched: {len(klines_1m)} rows")

    by_hour: dict[str, list[dict[str, Any]]] = {}
    for row in klines_1m:
        ts = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc)
        hour_key = ts.strftime("%Y-%m-%dT%H:00:00+00:00")
        by_hour.setdefault(hour_key, []).append(
            {"ts": ts, "minute": ts.minute, "open": float(row[1]), "close": float(row[4])}
        )
    t_group1 = perf_counter()

    hour_keys = sorted(by_hour.keys())
    hourly_closes: deque[float] = deque(maxlen=params.lookback_hours + 1)
    balance = params.initial_balance
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    for idx, hour_key in enumerate(hour_keys):
        bars = sorted(by_hour[hour_key], key=lambda b: b["minute"])
        bars_by_minute = {b["minute"]: b for b in bars}
        if params.close_minute not in bars_by_minute:
            continue
        close_bar = bars_by_minute[params.close_minute]

        if idx > 0:
            prev = sorted(by_hour[hour_keys[idx - 1]], key=lambda b: b["minute"])
            if prev:
                hourly_closes.append(prev[-1]["close"])

        sigma_annual = _hourly_sigma_annual_from_closes(hourly_closes)
        if sigma_annual is None or len(hourly_closes) < params.lookback_hours:
            equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue

        strike = bars[0]["open"]
        entry_bar: dict[str, Any] | None = None
        side = ""
        fair_up_at_entry = 0.0
        fair_down_at_entry = 0.0
        fair_side_at_entry = 0.0
        mins_remaining_entry = 0
        entry_ask = 0.0
        entry_bid = 0.0

        for b in bars:
            m = b["minute"]
            if m < params.entry_start_minute or m > params.entry_end_minute:
                continue
            mins_remaining = 60 - m
            t_years = mins_remaining / (365.0 * 24.0 * 60.0)
            fair_up = _digital_up_fair(spot=b["close"], strike=strike, years_to_expiry=t_years, sigma=sigma_annual)
            if fair_up is None:
                continue
            fair_down = 1.0 - fair_up
            candidate_side = "up" if fair_up >= fair_down else "down"
            candidate_fair = fair_up if candidate_side == "up" else fair_down
            favored = candidate_fair >= 0.5
            ask = _synthetic_ask(candidate_fair, mins_remaining, favored, params)
            bid = _synthetic_bid(candidate_fair, mins_remaining, favored, params)
            min_bid = 1.0 - params.k_value / math.sqrt(mins_remaining)
            if bid >= min_bid and ask <= params.buy_max:
                entry_bar = b
                side = candidate_side
                fair_up_at_entry = fair_up
                fair_down_at_entry = fair_down
                fair_side_at_entry = candidate_fair
                mins_remaining_entry = mins_remaining
                entry_ask = ask
                entry_bid = bid
                break

        if entry_bar is None:
            equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue

        stake = balance * params.balance_fraction
        if stake <= 0 or entry_ask <= 0:
            equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue
        shares = stake / entry_ask
        cost = shares * entry_ask
        balance -= cost

        exit_reason = "resolution"
        exit_price: float | None = None
        payout = 0.0

        for b in bars:
            if b["ts"] <= entry_bar["ts"] or b["minute"] > params.close_minute:
                continue
            mins_remaining = max(1, 60 - b["minute"])
            t_years = mins_remaining / (365.0 * 24.0 * 60.0)
            fair_up = _digital_up_fair(spot=b["close"], strike=strike, years_to_expiry=t_years, sigma=sigma_annual)
            if fair_up is None:
                continue
            fair_down = 1.0 - fair_up
            held_fair = fair_up if side == "up" else fair_down
            favored = held_fair >= 0.5
            held_bid = _synthetic_bid(held_fair, mins_remaining, favored, params)
            if held_bid < params.stop_loss:
                exit_reason = "stop_loss"
                exit_price = _clamp(held_bid - params.stop_fill_below_bid, 0.01, 0.995)
                payout = shares * exit_price
                break

        if exit_reason == "resolution":
            is_up = close_bar["close"] >= strike
            won = is_up if side == "up" else (not is_up)
            payout = shares if won else 0.0
            resolved_win = won
        else:
            resolved_win = payout > cost

        balance += payout
        pnl = payout - cost
        trades.append(
            {
                "ts": entry_bar["ts"].isoformat(),
                "side": side,
                "mins_remaining_entry": mins_remaining_entry,
                "sigma_annual": round(sigma_annual, 6),
                "fair_up_entry": round(fair_up_at_entry, 6),
                "fair_down_entry": round(fair_down_at_entry, 6),
                "fair_side_entry": round(fair_side_at_entry, 6),
                "entry_ask": round(entry_ask, 6),
                "entry_bid": round(entry_bid, 6),
                "shares": round(shares, 6),
                "stake_usdc": round(stake, 6),
                "cost": round(cost, 6),
                "exit_reason": exit_reason,
                "exit_price": round(exit_price, 6) if exit_price is not None else None,
                "payout": round(payout, 6),
                "pnl": round(pnl, 6),
                "won": bool(resolved_win),
                "hour_open": round(strike, 6),
                "hour_close": round(close_bar["close"], 6),
            }
        )
        equity_curve.append({"ts": close_bar["ts"].isoformat(), "balance": round(balance, 6)})
    t_sim1 = perf_counter()

    n = len(trades)
    wins = sum(1 for t in trades if t["won"])
    stop_losses = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
    success_rate = (wins / n * 100.0) if n else 0.0
    total_return_pct = ((balance / params.initial_balance) - 1.0) * 100.0

    return {
        "params": {
            "initial_balance": params.initial_balance,
            "years": params.years,
            "lookback_hours": params.lookback_hours,
            "k_value": params.k_value,
            "buy_max": params.buy_max,
            "entry_window": f"T-{60 - params.entry_start_minute}..T-{60 - params.entry_end_minute}",
            "close_minute": params.close_minute,
            "balance_fraction": params.balance_fraction,
            "stop_loss": params.stop_loss,
            "stop_fill_below_bid": params.stop_fill_below_bid,
            "favored_premium_model": "premium=clamp(0.02,0.09,0.0125+0.00725*mins_remaining)",
            "non_favored_premium": params.non_favored_premium,
            "spread_model": f"spread={params.spread_floor}+{params.spread_slope}*mins_remaining",
            "pricing": "entry cost uses synthetic ask from fair; resolution win pays 1.0 per share",
            "sigma_note": "Rolling past-only hourly sigma (no future data).",
            "cache_db_path": params.cache_db_path,
        },
        "summary": {
            "hours_evaluated": len(hour_keys),
            "trades": n,
            "wins": wins,
            "losses": n - wins,
            "stop_loss_exits": stop_losses,
            "success_rate_pct": round(success_rate, 4),
            "start_balance": round(params.initial_balance, 6),
            "end_balance": round(balance, 6),
            "return_pct": round(total_return_pct, 6),
            "timing_sec": {
                "fetch_and_cache": round(t_fetch1 - t_fetch0, 3),
                "group_hours": round(t_group1 - t_fetch1, 3),
                "simulate": round(t_sim1 - t_group1, 3),
                "run_backtest_total": round(t_sim1 - t0, 3),
            },
        },
        "equity_curve": equity_curve,
        "trades": trades,
    }


def _render_html(report: dict[str, Any]) -> str:
    s = report["summary"]
    p = report["params"]
    payload = json.dumps(report)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>ETH Tail Capture Backtest</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui; margin: 24px; background: #f6f7fb; color: #111827; }}
    .grid {{ display: grid; grid-template-columns: repeat(5, minmax(150px, 1fr)); gap: 10px; margin: 16px 0; }}
    .card {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:10px; }}
    .k {{ font-size:12px; color:#6b7280; text-transform:uppercase; }}
    .v {{ font-size:22px; font-weight:700; margin-top:4px; }}
    .panel {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:12px; margin-top:12px; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #eef0f4; font-size:13px; }}
    th {{ background:#f9fafb; }}
    canvas {{ width:100%; height:340px; }}
  </style>
</head>
<body>
  <h1>ETH Tail Capture Backtest</h1>
  <div>1m data, entry window {p["entry_window"]}, quarter-balance sizing, fair->ask/bid microstructure, stop-loss on synthetic bid.</div>

  <div class="grid">
    <div class="card"><div class="k">Trades</div><div class="v">{s["trades"]}</div></div>
    <div class="card"><div class="k">Success Rate</div><div class="v">{s["success_rate_pct"]}%</div></div>
    <div class="card"><div class="k">Stop Exits</div><div class="v">{s["stop_loss_exits"]}</div></div>
    <div class="card"><div class="k">End Balance</div><div class="v">{s["end_balance"]}</div></div>
    <div class="card"><div class="k">Return</div><div class="v">{s["return_pct"]}%</div></div>
  </div>

  <div class="panel"><canvas id="eq"></canvas></div>
  <div class="panel">
    <table>
      <thead><tr><th>Param</th><th>Value</th></tr></thead>
      <tbody>
        <tr><td>Years</td><td>{p["years"]}</td></tr>
        <tr><td>Lookback Hours</td><td>{p["lookback_hours"]}</td></tr>
        <tr><td>k</td><td>{p["k_value"]}</td></tr>
        <tr><td>Buy Max</td><td>{p["buy_max"]}</td></tr>
        <tr><td>Balance Fraction</td><td>{p["balance_fraction"]}</td></tr>
        <tr><td>Stop Loss</td><td>{p["stop_loss"]}</td></tr>
        <tr><td>Premium Model</td><td>{p["favored_premium_model"]}</td></tr>
        <tr><td>Spread Model</td><td>{p["spread_model"]}</td></tr>
        <tr><td>Sigma</td><td>{p["sigma_note"]}</td></tr>
      </tbody>
    </table>
  </div>

  <script>
    const report = {payload};
    const points = report.equity_curve.map(x => x.balance);
    const canvas = document.getElementById('eq');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.clientWidth || 1000;
    const H = 340;
    canvas.width = W * dpr; canvas.height = H * dpr; ctx.scale(dpr, dpr);
    ctx.clearRect(0,0,W,H); ctx.fillStyle = '#fff'; ctx.fillRect(0,0,W,H);
    if (points.length === 0) {{
      ctx.fillStyle = '#111827'; ctx.fillText('No data', 20, 20);
    }} else {{
      const min = Math.min(...points), max = Math.max(...points), pad = 24;
      const x = i => pad + (i / Math.max(1, points.length - 1)) * (W - pad * 2);
      const y = v => H - pad - ((v - min) / Math.max(1e-9, max - min)) * (H - pad * 2);
      ctx.strokeStyle = '#e5e7eb'; ctx.beginPath();
      ctx.moveTo(pad,pad); ctx.lineTo(pad,H-pad); ctx.lineTo(W-pad,H-pad); ctx.stroke();
      ctx.strokeStyle = '#2563eb'; ctx.lineWidth = 2; ctx.beginPath();
      points.forEach((v,i)=> i===0 ? ctx.moveTo(x(i),y(v)) : ctx.lineTo(x(i),y(v)));
      ctx.stroke();
      ctx.fillStyle = '#111827'; ctx.font = '12px system-ui';
      ctx.fillText(`min: ${{min.toFixed(2)}}`, pad+4, pad+12);
      ctx.fillText(`max: ${{max.toFixed(2)}}`, pad+120, pad+12);
    }}
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest ETH tail capture using 1m data and fair->ask/bid emulation")
    parser.add_argument("--years", type=float, default=2.0)
    parser.add_argument("--initial-balance", type=float, default=100.0)
    parser.add_argument("--lookback-hours", type=int, default=168)
    parser.add_argument("--k", type=float, default=0.12)
    parser.add_argument("--buy-max", type=float, default=0.98)
    parser.add_argument("--entry-start-minute", type=int, default=40)
    parser.add_argument("--entry-end-minute", type=int, default=57)
    parser.add_argument("--close-minute", type=int, default=59)
    parser.add_argument("--balance-fraction", type=float, default=0.25)
    parser.add_argument("--stop-loss", type=float, default=0.75)
    parser.add_argument("--stop-fill-below-bid", type=float, default=0.01)
    parser.add_argument("--cache-db", default="experiment/cache/binance_klines.sqlite")
    parser.add_argument("--out-dir", default="experiment")
    return parser.parse_args()


def main() -> None:
    t_main0 = perf_counter()
    args = parse_args()
    params = SimParams(
        initial_balance=args.initial_balance,
        years=args.years,
        lookback_hours=args.lookback_hours,
        k_value=args.k,
        buy_max=args.buy_max,
        entry_start_minute=args.entry_start_minute,
        entry_end_minute=args.entry_end_minute,
        close_minute=args.close_minute,
        balance_fraction=args.balance_fraction,
        stop_loss=args.stop_loss,
        stop_fill_below_bid=args.stop_fill_below_bid,
        cache_db_path=args.cache_db,
    )

    report = run_backtest(params)
    t_bt_done = perf_counter()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "eth_tail_backtest_report.json"
    html_path = out / "eth_tail_backtest_report.html"
    t_render0 = perf_counter()
    html = _render_html(report)
    t_render1 = perf_counter()
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    t_json1 = perf_counter()
    html_path.write_text(html, encoding="utf-8")
    t_html1 = perf_counter()

    s = report["summary"]
    print("Backtest complete")
    print(f"  Trades: {s['trades']}")
    print(f"  Success rate: {s['success_rate_pct']}%")
    print(f"  Stop-loss exits: {s['stop_loss_exits']}")
    print(f"  Start: {s['start_balance']}  End: {s['end_balance']}  Return: {s['return_pct']}%")
    print(f"  JSON: {json_path}")
    print(f"  HTML: {html_path}")
    timing = s.get("timing_sec", {})
    print("Timing (seconds)")
    print(f"  fetch/cache: {timing.get('fetch_and_cache', 'n/a')}")
    print(f"  group hours: {timing.get('group_hours', 'n/a')}")
    print(f"  simulate:    {timing.get('simulate', 'n/a')}")
    print(f"  run total:   {timing.get('run_backtest_total', 'n/a')}")
    print(f"  render html: {round(t_render1 - t_render0, 3)}")
    print(f"  write json:  {round(t_json1 - t_render1, 3)}")
    print(f"  write html:  {round(t_html1 - t_json1, 3)}")
    print(f"  post-backtest: {round(t_html1 - t_bt_done, 3)}")
    print(f"  main total:  {round(t_html1 - t_main0, 3)}")


if __name__ == "__main__":
    main()
