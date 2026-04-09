#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from experiment.eth_tail_backtest import (
    SimParams,
    _digital_up_fair,
    _hourly_sigma_annual_from_closes,
    _load_1m_klines_cached,
    _synthetic_ask,
    _synthetic_bid,
    _clamp,
)


PHI = (1.0 + math.sqrt(5.0)) / 2.0


@dataclass(frozen=True)
class SweepConfig:
    years: float
    lookback_hours: int
    initial_balance: float
    buy_max: float
    stop_loss: float
    stop_fill_below_bid: float
    cache_db_path: str
    out_dir: str
    k_min: float
    k_max: float
    k_step: float
    t_max: int
    t_min: int
    fractions: list[float]
    end_window_seconds: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sweep ETH tail backtest across k, T, and bet fractions")
    p.add_argument("--years", type=float, default=2.0)
    p.add_argument("--lookback-hours", type=int, default=168)
    p.add_argument("--initial-balance", type=float, default=100.0)
    p.add_argument("--buy-max", type=float, default=0.98)
    p.add_argument("--stop-loss", type=float, default=0.75)
    p.add_argument("--stop-fill-below-bid", type=float, default=0.01)
    p.add_argument("--cache-db", default="experiment/cache/binance_klines.sqlite")
    p.add_argument("--out-dir", default="experiment")
    p.add_argument("--k-min", type=float, default=0.12)
    p.add_argument("--k-max", type=float, default=2.0)
    p.add_argument("--k-step", type=float, default=0.08)
    p.add_argument("--t-max", type=int, default=20)
    p.add_argument("--t-min", type=int, default=1)
    p.add_argument(
        "--fractions",
        default=",".join([
            f"{1.0/5.0:.12f}",
            f"{1.0/4.0:.12f}",
            f"{1.0/3.0:.12f}",
            f"{1.0/2.0:.12f}",
            f"{1.0/PHI:.12f}",
        ]),
        help="Comma-separated balance fractions",
    )
    p.add_argument("--end-window-seconds", type=int, default=10)
    return p.parse_args()


def _frange(start: float, stop: float, step: float) -> list[float]:
    out: list[float] = []
    cur = start
    guard = 0
    while cur <= stop + 1e-12:
        out.append(round(cur, 10))
        cur += step
        guard += 1
        if guard > 100000:
            raise RuntimeError("frange guard tripped")
    return out


def _load_hour_bars(years: float, cache_db_path: str) -> tuple[dict[str, list[dict[str, Any]]], list[str], datetime, datetime]:
    end_dt = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=365 * years)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    klines_1m = _load_1m_klines_cached("ETHUSDT", start_ms, end_ms, cache_db_path)
    if len(klines_1m) < 1000:
        raise RuntimeError(f"Not enough 1m data fetched: {len(klines_1m)} rows")

    by_hour: dict[str, list[dict[str, Any]]] = {}
    for row in klines_1m:
        ts = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
        hour_key = ts.strftime("%Y-%m-%dT%H:00:00+00:00")
        by_hour.setdefault(hour_key, []).append(
            {"ts": ts, "minute": ts.minute, "open": float(row[1]), "close": float(row[4])}
        )

    hour_keys = sorted(by_hour.keys())
    return by_hour, hour_keys, start_dt, end_dt


def _simulate(
    params: SimParams,
    by_hour: dict[str, list[dict[str, Any]]],
    hour_keys: list[str],
    collect_curve: bool = False,
) -> dict[str, Any]:
    hourly_closes: deque[float] = deque(maxlen=params.lookback_hours + 1)
    balance = params.initial_balance
    trades = 0
    wins = 0
    stop_losses = 0
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
            if collect_curve:
                equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue

        strike = bars[0]["open"]
        entry_bar: dict[str, Any] | None = None
        side = ""
        mins_remaining_entry = 0
        entry_ask = 0.0

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
                mins_remaining_entry = mins_remaining
                entry_ask = ask
                break

        if entry_bar is None:
            if collect_curve:
                equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue

        stake = balance * params.balance_fraction
        if stake <= 0 or entry_ask <= 0:
            if collect_curve:
                equity_curve.append({"ts": bars[-1]["ts"].isoformat(), "balance": round(balance, 6)})
            continue

        shares = stake / entry_ask
        cost = shares * entry_ask
        balance -= cost

        exit_reason = "resolution"
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
        else:
            won = payout > cost

        balance += payout
        trades += 1
        wins += int(bool(won))
        stop_losses += int(exit_reason == "stop_loss")

        if collect_curve:
            equity_curve.append({"ts": close_bar["ts"].isoformat(), "balance": round(balance, 6)})

    win_rate = (wins / trades * 100.0) if trades else 0.0
    return {
        "hours_evaluated": len(hour_keys),
        "trades": trades,
        "wins": wins,
        "losses": trades - wins,
        "stop_loss_exits": stop_losses,
        "success_rate_pct": round(win_rate, 4),
        "start_balance": round(params.initial_balance, 6),
        "end_balance": round(balance, 6),
        "return_pct": round(((balance / params.initial_balance) - 1.0) * 100.0, 6),
        "entry_t_minutes": mins_remaining_entry if trades else None,
        "equity_curve": equity_curve if collect_curve else None,
    }


def _render_html(report: dict[str, Any]) -> str:
    payload = json.dumps(report)
    template = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>ETH Tail Sweep</title>
  <style>
    :root {{ --bg:#f6f7fb; --panel:#fff; --ink:#111827; --muted:#6b7280; --line:#e5e7eb; }}
    body {{ font-family: ui-sans-serif, system-ui; margin: 24px; background: var(--bg); color: var(--ink); }}
    .grid {{ display:grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap:10px; margin:12px 0 14px; }}
    .card {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:10px; }}
    .k {{ font-size:12px; color:var(--muted); text-transform:uppercase; }}
    .v {{ font-size:20px; font-weight:700; margin-top:4px; }}
    .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:12px; margin-top:12px; }}
    canvas {{ width:100%; height:320px; }}
    table {{ width:100%; border-collapse:collapse; margin-top:8px; }}
    th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #eef0f4; font-size:13px; }}
    th {{ background:#f9fafb; }}
  </style>
</head>
<body>
  <h1>ETH Tail Sweep</h1>
  <div id=\"meta\"></div>

  <div class=\"grid\" id=\"cards\"></div>

  <div class=\"panel\"><h3>Win Rate vs T (best k per T, by fraction)</h3><canvas id=\"winRate\"></canvas></div>
  <div class=\"panel\"><h3>End Balance vs T (log10, best k per T, by fraction)</h3><canvas id=\"endBal\"></canvas></div>
  <div class=\"panel\"><h3>Balance Growth Over Time (best run per fraction)</h3><canvas id=\"growth\"></canvas></div>

  <div class=\"panel\">
    <h3>Top 15 Configurations by End Balance</h3>
    <table>
      <thead><tr><th>#</th><th>Fraction</th><th>k</th><th>T (min)</th><th>Win Rate</th><th>Trades</th><th>End Balance</th><th>Return %</th></tr></thead>
      <tbody id=\"topRows\"></tbody>
    </table>
  </div>

  <script>
    const report = __REPORT_PAYLOAD__;
    const colors = ['#2563eb', '#16a34a', '#dc2626', '#ea580c', '#7c3aed', '#0891b2'];

    function drawLines(canvasId, datasets, yAccessor, yLabel, logScale=false) {{
      const canvas = document.getElementById(canvasId);
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const W = canvas.clientWidth || 1000;
      const H = 320;
      const pad = 36;
      canvas.width = W * dpr;
      canvas.height = H * dpr;
      ctx.scale(dpr, dpr);

      const xs = datasets.flatMap(d => d.points.map(p => p.x));
      const ys = datasets.flatMap(d => d.points.map(p => logScale ? Math.log10(Math.max(1e-12, yAccessor(p))) : yAccessor(p)));
      if (!xs.length || !ys.length) return;
      const xMin = Math.min(...xs), xMax = Math.max(...xs);
      const yMin = Math.min(...ys), yMax = Math.max(...ys);

      const x = v => pad + ((v - xMin) / Math.max(1e-9, xMax - xMin)) * (W - pad*2);
      const y = v => H - pad - ((v - yMin) / Math.max(1e-9, yMax - yMin)) * (H - pad*2);

      ctx.clearRect(0,0,W,H);
      ctx.fillStyle = '#fff'; ctx.fillRect(0,0,W,H);
      ctx.strokeStyle = '#e5e7eb'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad,pad); ctx.lineTo(pad,H-pad); ctx.lineTo(W-pad,H-pad); ctx.stroke();

      datasets.forEach((d, i) => {{
        ctx.strokeStyle = colors[i % colors.length];
        ctx.lineWidth = 2;
        ctx.beginPath();
        d.points.forEach((p, idx) => {{
          const yv = logScale ? Math.log10(Math.max(1e-12, yAccessor(p))) : yAccessor(p);
          if (idx === 0) ctx.moveTo(x(p.x), y(yv));
          else ctx.lineTo(x(p.x), y(yv));
        }});
        ctx.stroke();
      }});

      ctx.fillStyle = '#111827';
      ctx.font = '12px system-ui';
      ctx.fillText(`y: ${{yLabel}}`, pad + 6, pad - 10);
      let lx = pad + 6;
      datasets.forEach((d, i) => {{
        ctx.fillStyle = colors[i % colors.length];
        ctx.fillRect(lx, H - pad + 10, 10, 10);
        ctx.fillStyle = '#111827';
        ctx.fillText(d.label, lx + 14, H - pad + 19);
        lx += 110;
      }});
    }}

    function fmt(v) {{
      if (!Number.isFinite(v)) return String(v);
      if (Math.abs(v) >= 1e6) return v.toExponential(2);
      return v.toFixed(4);
    }}

    document.getElementById('meta').textContent =
      `Range: k=${{report.config.k_min}}..${{report.config.k_max}} step ${{report.config.k_step}} | T=${{report.config.t_max}}..${{report.config.t_min}} min | years=${{report.config.years}} | end-window requested=${{report.config.end_window_seconds}}s mapped to minute=${{report.config.mapped_end_minute}} (1m data)`;

    const cards = [
      ['Total Runs', report.summary.total_runs],
      ['Best Fraction', report.summary.best_overall.balance_fraction],
      ['Best k/T', `${{report.summary.best_overall.k_value}} / T-${{report.summary.best_overall.t_minutes}}m`],
      ['Best End Balance', report.summary.best_overall.end_balance],
    ];
    const cardsEl = document.getElementById('cards');
    cards.forEach(([k, v]) => {{
      const d = document.createElement('div');
      d.className = 'card';
      d.innerHTML = `<div class=\"k\">${{k}}</div><div class=\"v\">${{fmt(Number(v))}}</div>`;
      cardsEl.appendChild(d);
    }});

    const byFraction = report.series.by_fraction_t_best;
    const winSets = byFraction.map(x => ({ label: x.fraction_label, points: x.points.map(p => ({x: p.t_minutes, y: p.success_rate_pct})) }));
    const balSets = byFraction.map(x => ({ label: x.fraction_label, points: x.points.map(p => ({x: p.t_minutes, y: p.end_balance})) }));
    drawLines('winRate', winSets, p => p.y, 'win rate %', false);
    drawLines('endBal', balSets, p => p.y, 'log10(end balance)', true);

    const growthSets = report.series.best_equity_curves.map(x => ({
      label: x.fraction_label,
      points: x.equity_curve.map((p, i) => ({x: i, y: p.balance}))
    }));
    drawLines('growth', growthSets, p => p.y, 'balance', true);

    const topRows = document.getElementById('topRows');
    report.summary.top_15.forEach((r, i) => {{
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${{i+1}}</td><td>${{r.balance_fraction}}</td><td>${{r.k_value}}</td><td>${{r.t_minutes}}</td><td>${{r.success_rate_pct}}</td><td>${{r.trades}}</td><td>${{fmt(r.end_balance)}}</td><td>${{r.return_pct}}</td>`;
      topRows.appendChild(tr);
    }});
  </script>
</body>
</html>
"""
    return template.replace("__REPORT_PAYLOAD__", payload)


def main() -> None:
    args = parse_args()
    fractions = [float(x.strip()) for x in args.fractions.split(",") if x.strip()]
    cfg = SweepConfig(
        years=args.years,
        lookback_hours=args.lookback_hours,
        initial_balance=args.initial_balance,
        buy_max=args.buy_max,
        stop_loss=args.stop_loss,
        stop_fill_below_bid=args.stop_fill_below_bid,
        cache_db_path=args.cache_db,
        out_dir=args.out_dir,
        k_min=args.k_min,
        k_max=args.k_max,
        k_step=args.k_step,
        t_max=args.t_max,
        t_min=args.t_min,
        fractions=fractions,
        end_window_seconds=args.end_window_seconds,
    )

    by_hour, hour_keys, start_dt, end_dt = _load_hour_bars(cfg.years, cfg.cache_db_path)

    # 1m candles only; closest representation of T-10s is minute 59 (T-1m).
    mapped_end_minute = 59

    k_values = _frange(cfg.k_min, cfg.k_max, cfg.k_step)
    t_values = list(range(cfg.t_max, cfg.t_min - 1, -1))

    rows: list[dict[str, Any]] = []
    for frac in cfg.fractions:
        for t in t_values:
            minute = 60 - t
            for k in k_values:
                p = SimParams(
                    initial_balance=cfg.initial_balance,
                    years=cfg.years,
                    lookback_hours=cfg.lookback_hours,
                    k_value=k,
                    buy_max=cfg.buy_max,
                    entry_start_minute=minute,
                    entry_end_minute=minute,
                    close_minute=mapped_end_minute,
                    balance_fraction=frac,
                    stop_loss=cfg.stop_loss,
                    stop_fill_below_bid=cfg.stop_fill_below_bid,
                    cache_db_path=cfg.cache_db_path,
                )
                s = _simulate(p, by_hour, hour_keys, collect_curve=False)
                rows.append(
                    {
                        "balance_fraction": round(frac, 12),
                        "fraction_label": f"{frac:.6f}",
                        "k_value": k,
                        "t_minutes": t,
                        "entry_minute": minute,
                        "trades": s["trades"],
                        "wins": s["wins"],
                        "losses": s["losses"],
                        "stop_loss_exits": s["stop_loss_exits"],
                        "success_rate_pct": s["success_rate_pct"],
                        "end_balance": s["end_balance"],
                        "return_pct": s["return_pct"],
                    }
                )

    sorted_rows = sorted(rows, key=lambda r: (r["end_balance"], r["success_rate_pct"]), reverse=True)
    best_overall = sorted_rows[0] if sorted_rows else {}

    by_fraction_best: list[dict[str, Any]] = []
    best_equity_curves: list[dict[str, Any]] = []
    fraction_to_rows: dict[float, list[dict[str, Any]]] = {}
    for r in rows:
        fraction_to_rows.setdefault(float(r["balance_fraction"]), []).append(r)

    for frac, frows in sorted(fraction_to_rows.items(), key=lambda kv: kv[0]):
        fbest = sorted(frows, key=lambda r: (r["end_balance"], r["success_rate_pct"]), reverse=True)[0]
        by_fraction_best.append(fbest)

        # Recompute with curve for best-per-fraction growth chart.
        best_p = SimParams(
            initial_balance=cfg.initial_balance,
            years=cfg.years,
            lookback_hours=cfg.lookback_hours,
            k_value=fbest["k_value"],
            buy_max=cfg.buy_max,
            entry_start_minute=fbest["entry_minute"],
            entry_end_minute=fbest["entry_minute"],
            close_minute=mapped_end_minute,
            balance_fraction=float(fbest["balance_fraction"]),
            stop_loss=cfg.stop_loss,
            stop_fill_below_bid=cfg.stop_fill_below_bid,
            cache_db_path=cfg.cache_db_path,
        )
        curve_summary = _simulate(best_p, by_hour, hour_keys, collect_curve=True)
        best_equity_curves.append(
            {
                "fraction": round(frac, 12),
                "fraction_label": f"{frac:.6f}",
                "k_value": fbest["k_value"],
                "t_minutes": fbest["t_minutes"],
                "equity_curve": curve_summary["equity_curve"],
            }
        )

    # Best k for each (fraction, T)
    by_fraction_t_best: list[dict[str, Any]] = []
    for frac, frows in sorted(fraction_to_rows.items(), key=lambda kv: kv[0]):
        t_points: list[dict[str, Any]] = []
        for t in t_values:
            rows_t = [r for r in frows if int(r["t_minutes"]) == int(t)]
            if not rows_t:
                continue
            best_t = sorted(rows_t, key=lambda r: (r["end_balance"], r["success_rate_pct"]), reverse=True)[0]
            t_points.append(
                {
                    "t_minutes": t,
                    "k_value": best_t["k_value"],
                    "success_rate_pct": best_t["success_rate_pct"],
                    "end_balance": best_t["end_balance"],
                }
            )
        by_fraction_t_best.append(
            {
                "fraction": round(frac, 12),
                "fraction_label": f"{frac:.6f}",
                "points": t_points,
            }
        )

    # Golden-ratio specific and empirical best fraction.
    golden_fraction = round(1.0 / PHI, 12)
    golden_rows = [r for r in rows if abs(float(r["balance_fraction"]) - golden_fraction) < 1e-9]
    best_golden_ratio = sorted(golden_rows, key=lambda r: (r["end_balance"], r["success_rate_pct"]), reverse=True)[0]

    frac_medians = []
    for frac, frows in fraction_to_rows.items():
        balances = sorted([float(r["end_balance"]) for r in frows])
        n = len(balances)
        if n == 0:
            continue
        if n % 2 == 1:
            med = balances[n // 2]
        else:
            med = 0.5 * (balances[n // 2 - 1] + balances[n // 2])
        frac_medians.append((frac, med))
    empirical_best_fraction, empirical_best_median = sorted(frac_medians, key=lambda x: x[1], reverse=True)[0]

    report = {
        "config": {
            "years": cfg.years,
            "lookback_hours": cfg.lookback_hours,
            "initial_balance": cfg.initial_balance,
            "buy_max": cfg.buy_max,
            "stop_loss": cfg.stop_loss,
            "stop_fill_below_bid": cfg.stop_fill_below_bid,
            "k_min": cfg.k_min,
            "k_max": cfg.k_max,
            "k_step": cfg.k_step,
            "k_values": k_values,
            "t_max": cfg.t_max,
            "t_min": cfg.t_min,
            "t_values": t_values,
            "fractions": [round(x, 12) for x in cfg.fractions],
            "end_window_seconds": cfg.end_window_seconds,
            "mapped_end_minute": mapped_end_minute,
            "start_utc": start_dt.isoformat(),
            "end_utc": end_dt.isoformat(),
            "cache_db_path": cfg.cache_db_path,
        },
        "summary": {
            "total_runs": len(rows),
            "best_overall": best_overall,
            "best_per_fraction": by_fraction_best,
            "best_golden_ratio_fraction": best_golden_ratio,
            "empirical_best_fraction_by_median_end_balance": {
                "fraction": round(empirical_best_fraction, 12),
                "median_end_balance": round(empirical_best_median, 6),
            },
            "top_15": sorted_rows[:15],
        },
        "series": {
            "all_results": rows,
            "by_fraction_t_best": by_fraction_t_best,
            "best_equity_curves": best_equity_curves,
        },
    }

    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "eth_tail_sweep_report.json"
    html_path = out_dir / "eth_tail_sweep_report.html"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    html_path.write_text(_render_html(report), encoding="utf-8")

    print("Sweep complete")
    print(f"  Runs: {len(rows)}")
    print(f"  Best overall: fraction={best_overall.get('balance_fraction')} k={best_overall.get('k_value')} T={best_overall.get('t_minutes')}m")
    print(f"  End balance: {best_overall.get('end_balance')}  Win rate: {best_overall.get('success_rate_pct')}%")
    print(f"  Golden ratio fraction (1/phi={1.0/PHI:.12f}) best: k={best_golden_ratio.get('k_value')} T={best_golden_ratio.get('t_minutes')}m end={best_golden_ratio.get('end_balance')}")
    print(f"  Empirical best fraction by median end balance: {empirical_best_fraction:.12f} (median={empirical_best_median:.6f})")
    print(f"  JSON: {json_path}")
    print(f"  HTML: {html_path}")


if __name__ == "__main__":
    main()
