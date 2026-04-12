#!/usr/bin/env python3
"""
Unified trading dashboard â€” Flask web app.

Tabs:
  1. Crypto 5m signals  (reads experiment/crypto_5m.db)
  2. ETH 1H bot         (reads bets.db â†’ eth_1h_trades; streams eth_1h.log)
  3. Weather bets        (reads bets.db â†’ placed_bets)

Start:  python -m experiment.crypto_5m_stats
Opens:  http://localhost:5051   (accessible to any device on your LAN)
"""
from __future__ import annotations

import argparse
import os
import sqlite3
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request

CRYPTO_DB_PATH = os.path.join("experiment", "crypto_5m.db")
BETS_DB_PATH   = str(Path(__file__).resolve().parent.parent / "bets.db")
ETH_LOG_PATH   = os.path.join("experiment", "logs", "eth_1h.log")
BS_LOG_PATH    = os.path.join("experiment", "logs", "crypto_5m_scanner.log")
TIERS          = [("1c", 0.01), ("2c", 0.02), ("3c", 0.03)]
LOG_TAIL       = 200

app = Flask(__name__)


# â”€â”€ config helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _crypto_db() -> str:
    return app.config.get("CRYPTO_DB_PATH", CRYPTO_DB_PATH)

def _bets_db() -> str:
    return app.config.get("BETS_DB_PATH", BETS_DB_PATH)

def _eth_log() -> str:
    return app.config.get("ETH_LOG_PATH", ETH_LOG_PATH)

def _bs_log() -> str:
    return app.config.get("BS_LOG_PATH", BS_LOG_PATH)


# â”€â”€ crypto 5m â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _query_crypto_stats() -> dict:
    if not os.path.exists(_crypto_db()):
        return {"error": "DB not found â€” start crypto_5m_scanner first.", "assets": []}

    conn = sqlite3.connect(_crypto_db())
    conn.row_factory = sqlite3.Row
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    if "tier" not in cols:
        conn.close()
        return {
            "error": "Tier view is disabled in edge mode. Use the 'BS Forward Test' tab.",
            "assets": [],
        }

    rows = conn.execute("""
        SELECT asset, side, tier, entry_price, candle_start, won, pnl
        FROM signals WHERE won IS NOT NULL
        ORDER BY asset, candle_start
    """).fetchall()
    all_rows = conn.execute("SELECT asset, side, tier, won FROM signals").fetchall()
    total_candles  = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    total_resolved = conn.execute(
        "SELECT COUNT(*) FROM candles WHERE winner IS NOT NULL AND winner != '?'"
    ).fetchone()[0]
    last_signal = conn.execute("SELECT MAX(signal_ts) FROM signals").fetchone()[0]
    conn.close()

    by_asset: dict[str, list] = {}
    for row in rows:
        by_asset.setdefault(row["asset"], []).append(dict(row))

    all_by_asset: dict[str, list] = {}
    for row in all_rows:
        all_by_asset.setdefault(row["asset"], []).append(dict(row))

    all_assets = sorted(set(by_asset.keys()) | set(all_by_asset.keys()))
    assets_data: dict[str, dict] = {}

    for asset in all_assets:
        asset_rows     = by_asset.get(asset, [])
        all_asset_rows = all_by_asset.get(asset, [])
        candle_starts  = sorted(set(r["candle_start"] for r in asset_rows))

        series:  dict[str, list[float]] = {"1c": [], "2c": [], "3c": []}
        labels:  list[str]              = []
        running: dict[str, float]       = {"1c": 0.0, "2c": 0.0, "3c": 0.0}

        for cs in candle_starts:
            candle_rows = [r for r in asset_rows if r["candle_start"] == cs]
            dt = datetime.fromtimestamp(cs, tz=timezone.utc)
            labels.append(dt.strftime("%m/%d %H:%M"))
            for tier_key, threshold in TIERS:
                tier_rows = [r for r in candle_rows if round(r["tier"], 2) <= threshold]
                running[tier_key] += sum(r["pnl"] for r in tier_rows)
                series[tier_key].append(round(running[tier_key], 4))

        stats: dict[str, dict] = {}
        for tier_key, threshold in TIERS:
            resolved = [r for r in asset_rows     if round(r["tier"], 2) <= threshold]
            pending  = [r for r in all_asset_rows if round(r["tier"], 2) <= threshold and r["won"] is None]
            wins     = [r for r in resolved if r["won"] == 1]
            total_pnl  = sum(r["pnl"] for r in resolved)
            total_cost = sum(r["entry_price"] for r in resolved)
            stats[tier_key] = {
                "signals":   len(resolved) + len(pending),
                "resolved":  len(resolved),
                "pending":   len(pending),
                "wins":      len(wins),
                "losses":    len(resolved) - len(wins),
                "win_rate":  round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
                "total_pnl": round(total_pnl, 4),
                "roi":       round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
            }

        assets_data[asset] = {
            "chart": {"labels": labels, "series_1c": series["1c"],
                      "series_2c": series["2c"], "series_3c": series["3c"]},
            "stats": stats,
        }

    last_update = (
        datetime.fromtimestamp(last_signal, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if last_signal else "â€”"
    )
    return {
        "assets": all_assets, "data": assets_data,
        "total_candles": total_candles, "total_resolved": total_resolved,
        "last_update": last_update,
    }


# â”€â”€ ETH 1H â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_WIN_OUTCOMES  = {"win"}
_LOSS_OUTCOMES = {"loss", "stop_loss", "expired"}

def _query_eth_stats() -> dict:
    if not os.path.exists(_bets_db()):
        return {"error": "bets.db not found â€” start automata.eth first."}

    conn = sqlite3.connect(_bets_db())
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT id, slug, direction, shares, entry_price, cost_usdc,
                   placed_at, mins_remaining, outcome, dry_run,
                   redeem_tx_hash, redeemed_at
            FROM eth_1h_trades
            ORDER BY placed_at ASC
        """).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return {"error": "eth_1h_trades table not found â€” start automata.eth first."}
    conn.close()

    trades  = [dict(r) for r in rows]
    labels, cum_series = [], []
    running = 0.0

    for t in trades:
        if t["outcome"] in _WIN_OUTCOMES:
            t["pnl"] = round(t["shares"] * (1.0 - t["entry_price"]), 4)
        elif t["outcome"] in _LOSS_OUTCOMES:
            t["pnl"] = round(-t["shares"] * t["entry_price"], 4)
        else:
            t["pnl"] = None

        if t["pnl"] is not None:
            running += t["pnl"]
            try:
                dt = datetime.fromisoformat(t["placed_at"].replace("Z", "+00:00"))
                labels.append(dt.strftime("%m/%d %H:%M"))
            except Exception:
                labels.append((t["placed_at"] or "?")[:16])
            cum_series.append(round(running, 4))

    resolved   = [t for t in trades if t["outcome"] in (_WIN_OUTCOMES | _LOSS_OUTCOMES)]
    wins       = [t for t in resolved if t["outcome"] in _WIN_OUTCOMES]
    pending    = [t for t in trades if t["outcome"] is None]
    total_pnl  = sum(t["pnl"] for t in resolved)
    total_cost = sum(t["cost_usdc"] or 0 for t in trades)

    stats = {
        "total":      len(trades),
        "resolved":   len(resolved),
        "pending":    len(pending),
        "wins":       len(wins),
        "losses":     len(resolved) - len(wins),
        "win_rate":   round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
        "total_pnl":  round(total_pnl, 4),
        "total_cost": round(total_cost, 2),
        "roi":        round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
    }
    return {
        "stats":  stats,
        "chart":  {"labels": labels, "series": cum_series},
        "trades": list(reversed(trades)),   # newest first for table
    }


# â”€â”€ log tail â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _read_log_tail() -> list[str]:
    try:
        with open(_eth_log(), "r", encoding="utf-8", errors="replace") as f:
            return list(deque(f, maxlen=LOG_TAIL))
    except FileNotFoundError:
        return [f"Log file not found: {_eth_log()}"]


def _read_bs_log_tail() -> list[str]:
    try:
        with open(_bs_log(), "r", encoding="utf-8", errors="replace") as f:
            return list(deque(f, maxlen=LOG_TAIL))
    except FileNotFoundError:
        return [f"Log file not found: {_bs_log()}"]


# â”€â”€ weather â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _query_weather_stats() -> dict:
    if not os.path.exists(_bets_db()):
        return {"error": "bets.db not found.", "bets": []}

    conn = sqlite3.connect(_bets_db())
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT id, placed_at_utc, city, event_date, question, option,
                   shares, no_price, cost_usdc, forecast_high,
                   forecast_minus_threshold, outcome, resolved_temp
            FROM placed_bets
            ORDER BY placed_at_utc DESC
        """).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return {"error": "placed_bets table not found.", "bets": []}
    conn.close()

    bets     = [dict(r) for r in rows]
    wins     = [b for b in bets if b["outcome"] == "win"]
    losses   = [b for b in bets if b["outcome"] == "loss"]
    pending  = [b for b in bets if b["outcome"] is None]
    resolved = wins + losses
    total_pnl = sum(
        (b["shares"] * (1.0 - (b["no_price"] or 0))) if b["outcome"] == "win"
        else (-b["shares"] * (b["no_price"] or 0))
        for b in resolved
    )
    total_cost = sum(b["cost_usdc"] or 0 for b in bets)

    stats = {
        "total":      len(bets),
        "resolved":   len(resolved),
        "pending":    len(pending),
        "wins":       len(wins),
        "losses":     len(losses),
        "win_rate":   round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
        "total_pnl":  round(total_pnl, 4),
        "total_cost": round(total_cost, 2),
    }
    return {"stats": stats, "bets": bets}


# â”€â”€ BS forward test â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _query_bs_forward(min_edge_filter: float = 0.0) -> dict:
    """
    Reads from the live scanner's `signals` table.
    Filters to rows that have a BS fair_price recorded (i.e. scanner ran with
    --min-edge), and optionally applies a minimum edge filter.
    """
    db = _crypto_db()
    if not os.path.exists(db):
        return {"error": "DB not found â€” start crypto_5m_scanner first.", "assets": []}

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    # Check columns exist (older DBs may not have fair_price/edge yet)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    has_bs = "fair_price" in cols and "edge" in cols

    if has_bs:
        rows = conn.execute("""
            SELECT slug, asset, candle_start, signal_ts, secs_remaining,
                   side, entry_price, shares,
                   fair_price, edge, winner, won, pnl
            FROM signals
            WHERE fair_price IS NOT NULL
              AND asset = 'BTC'
              AND edge >= ?
            ORDER BY signal_ts ASC
        """, (min_edge_filter,)).fetchall()
    else:
        rows = []

    all_rows_count = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE fair_price IS NOT NULL AND asset = 'BTC'"
    ).fetchone()[0] if has_bs else 0
    conn.close()

    if not rows:
        msg = (
            "No BTC BS signals yet â€” run:  "
            "python -m experiment.crypto_5m_scanner --min-edge 0.05"
            if has_bs or all_rows_count == 0
            else f"No signals with edge â‰¥ {min_edge_filter:.3f}"
        )
        return {"error": msg, "assets": [], "total": 0, "total_pnl": 0}

    by_asset: dict[str, list] = {}
    for r in rows:
        by_asset.setdefault(r["asset"], []).append(dict(r))

    all_assets = sorted(by_asset.keys())
    assets_data: dict[str, dict] = {}

    for asset in all_assets:
        asset_rows = by_asset[asset]

        # cumulative P&L series (time-ordered, resolved only)
        resolved_rows  = [r for r in asset_rows if r["won"] is not None]
        labels:    list[str]   = []
        cum_series: list[float] = []
        running = 0.0
        for r in resolved_rows:
            running += r["pnl"] or 0
            dt = datetime.fromtimestamp(r["signal_ts"], tz=timezone.utc)
            labels.append(dt.strftime("%m/%d %H:%M"))
            cum_series.append(round(running, 4))

        wins      = [r for r in resolved_rows if r["won"] == 1]
        pending   = [r for r in asset_rows    if r["won"] is None]
        total_pnl  = sum(r["pnl"] or 0 for r in resolved_rows)
        total_cost = sum(r["entry_price"] * r["shares"] for r in asset_rows)
        avg_edge   = (
            sum(r["edge"] for r in asset_rows if r["edge"] is not None)
            / max(1, len([r for r in asset_rows if r["edge"] is not None]))
        )

        stats = {
            "signals":  len(asset_rows),
            "resolved": len(resolved_rows),
            "pending":  len(pending),
            "wins":     len(wins),
            "losses":   len(resolved_rows) - len(wins),
            "win_rate": round(len(wins) / len(resolved_rows) * 100, 2) if resolved_rows else 0,
            "total_pnl":  round(total_pnl, 4),
            "total_cost": round(total_cost, 4),
            "roi":        round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
            "avg_edge":   round(avg_edge, 4),
        }

        assets_data[asset] = {
            "chart":  {"labels": labels, "series": cum_series},
            "stats":  stats,
            "trades": list(reversed(asset_rows)),   # newest first
        }

    total_pnl = sum(r["pnl"] or 0 for r in rows if r["won"] is not None)
    return {
        "assets":    all_assets,
        "data":      assets_data,
        "total":     len(rows),
        "total_pnl": round(total_pnl, 4),
    }


# â”€â”€ template â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_TEMPLATE = r"""<!doctype html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Trading Dashboard</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    body { background:#0d1117; color:#e6edf3; }
    .card { background:#161b22; border:1px solid #30363d; }
    .card-header { background:#21262d; border-bottom:1px solid #30363d; }
    .stat-box { background:#21262d; border-radius:8px; padding:12px 16px; min-width:110px; }
    .stat-val { font-size:1.35rem; font-weight:700; }
    .pos { color:#3fb950; }
    .neg { color:#f85149; }
    .neu { color:#8b949e; }
    canvas { max-height:340px; }
    .nav-tabs { border-bottom-color:#30363d; }
    .nav-tabs .nav-link { color:#8b949e; border-color:transparent; }
    .nav-tabs .nav-link.active { color:#e6edf3; background:#161b22; border-color:#30363d #30363d #161b22; }
    .nav-tabs .nav-link:hover { color:#e6edf3; border-color:transparent; }
    .log-box {
      background:#0d1117; border:1px solid #30363d; border-radius:6px;
      padding:10px; font-family:monospace; font-size:12px;
      height:380px; overflow-y:scroll; white-space:pre-wrap; word-break:break-all;
    }
    .log-trade { color:#3fb950; font-weight:bold; }
    .log-win   { color:#3fb950; }
    .log-loss  { color:#f85149; }
    .log-error { color:#f85149; font-weight:bold; }
    .log-warn  { color:#fbbf24; }
    .log-info  { color:#8b949e; }
    .log-dim   { color:#3a404a; }
    .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; }
    .pill-win  { background:#14532d; color:#4ade80; }
    .pill-loss { background:#450a0a; color:#f87171; }
    .pill-open { background:#1e3a5f; color:#93c5fd; }
    .pill-dry  { background:#2d2d0a; color:#fbbf24; }
    .dir-up   { color:#3fb950; font-weight:700; }
    .dir-down { color:#f85149; font-weight:700; }
    table { font-size:13px; }
    th { font-size:11px !important; text-transform:uppercase; letter-spacing:.05em;
         color:#8b949e !important; background:#161b22 !important; }
    td { border-color:#21262d !important; vertical-align:middle; }
    tr:hover td { background:#1a2236 !important; }
    .table-scroll { overflow-x:auto; max-height:420px; overflow-y:auto; }
    .edit-input {
      width:60px; background:#0d1117; border:1px solid #444; color:#e6edf3;
      padding:2px 4px; border-radius:4px; font-size:11px;
    }
    .edit-select {
      background:#0d1117; border:1px solid #444; color:#e6edf3;
      padding:2px 4px; border-radius:4px; font-size:11px;
    }
    .edit-btn {
      background:#2563eb; border:none; color:#fff;
      padding:2px 8px; border-radius:4px; cursor:pointer; font-size:11px;
    }
    .edit-btn:hover { background:#1d4ed8; }
  </style>
</head>
<body>
<div class="container-fluid py-3">

  <div class="d-flex align-items-center justify-content-between mb-3">
    <h4 class="mb-0">Trading Dashboard</h4>
    <button class="btn btn-sm btn-outline-secondary" onclick="refreshAll()">&#8635; Refresh All</button>
  </div>

  <ul class="nav nav-tabs mb-3" id="mainTabs" role="tablist">
<li class="nav-item" role="presentation">
      <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-eth" type="button">
        ETH 1H Bot
      </button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-weather" type="button">
        Weather Bets
      </button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-bs" type="button"
              onclick="if(!ftLoaded)loadFT()">
        BTC 5m BS Test
      </button>
    </li>
  </ul>

  <div class="tab-content">

    <!-- â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• CRYPTO 5m â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• -->

    <!-- ETH 1H â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• -->
    <div class="tab-pane fade show active" id="tab-eth" role="tabpanel">

      <!-- stat row -->
      <div class="d-flex flex-wrap gap-3 mb-3" id="eth-stats">
        <div class="stat-box text-muted text-center">loadingâ€¦</div>
      </div>

      <!-- P/L chart -->
      <div class="card mb-3">
        <div class="card-header fw-bold">Cumulative P/L</div>
        <div class="card-body">
          <canvas id="eth-chart" style="max-height:300px"></canvas>
        </div>
      </div>

      <!-- trades table -->
      <div class="card mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span class="fw-bold">Trades</span>
          <div class="form-check form-switch mb-0">
            <input class="form-check-input" type="checkbox" id="showDryRun" checked>
            <label class="form-check-label small" for="showDryRun">Show dry-run</label>
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-scroll">
            <table class="table table-sm mb-0">
              <thead style="position:sticky;top:0;z-index:1">
                <tr>
                  <th>Time (UTC)</th><th>Slug</th><th>Dir</th><th>Entry</th>
                  <th>Shares</th><th>Cost</th><th>Mins left</th><th>Mode</th>
                  <th>Outcome</th><th>P/L</th>
                </tr>
              </thead>
              <tbody id="eth-trades-body"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- live log -->
      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span class="fw-bold">
            Live Log
            <small class="text-muted ms-2" id="log-ts"></small>
          </span>
          <div class="d-flex gap-3 align-items-center">
            <div class="form-check form-switch mb-0">
              <input class="form-check-input" type="checkbox" id="hideNoise" checked>
              <label class="form-check-label small" for="hideNoise">Hide idle lines</label>
            </div>
            <div class="form-check form-switch mb-0">
              <input class="form-check-input" type="checkbox" id="autoScroll" checked>
              <label class="form-check-label small" for="autoScroll">Auto-scroll</label>
            </div>
          </div>
        </div>
        <div class="card-body p-2">
          <div class="log-box" id="log-box"></div>
        </div>
      </div>
    </div>

    <!-- â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• WEATHER â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• -->
    <div class="tab-pane fade" id="tab-weather" role="tabpanel">
      <div class="d-flex flex-wrap gap-3 mb-3" id="weather-stats">
        <div class="stat-box text-muted text-center">loadingâ€¦</div>
      </div>
      <div class="card">
        <div class="card-header fw-bold">Weather Bets</div>
        <div class="card-body p-0">
          <div class="table-scroll" style="max-height:600px">
            <table class="table table-sm mb-0">
              <thead style="position:sticky;top:0;z-index:1">
                <tr>
                  <th>Time</th><th>City</th><th>Event</th><th>Question</th>
                  <th>Shares</th><th>Price</th><th>Cost</th>
                  <th>Forecast</th><th>Margin</th>
                  <th>Outcome</th><th>Res. Temp</th><th>Edit</th>
                </tr>
              </thead>
              <tbody id="weather-body"></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <!-- â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• BS FORWARD TEST â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â• -->
    <div class="tab-pane fade" id="tab-bs" role="tabpanel">

      <!-- filter row -->
      <div class="d-flex align-items-center gap-3 mb-3 flex-wrap">
        <div class="d-flex align-items-center gap-2">
          <label class="small text-muted">Min edge filter:</label>
          <input type="number" id="ft-min-edge" value="0.00" step="0.01" min="0" max="1"
                 class="edit-input" style="width:70px">
          <button class="edit-btn" onclick="loadFT()">Apply</button>
        </div>
        <small class="text-muted" id="ft-meta"></small>
      </div>

      <!-- global stats -->
      <div class="d-flex flex-wrap gap-3 mb-3" id="ft-global-stats"></div>

      <!-- per-asset panels -->
      <div id="ft-content"></div>

      <div class="card mt-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span class="fw-bold">BS Bet Log</span>
          <small class="text-muted" id="ft-log-ts"></small>
        </div>
        <div class="card-body p-2">
          <div class="log-box" id="ft-log-box"></div>
        </div>
      </div>
    </div>

  </div><!-- /tab-content -->
</div><!-- /container -->

<script>
// â”€â”€ shared â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function esc(s) {
  return String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}
function statCard(label, value, cls) {
  return `<div class="stat-box text-center">
    <div class="text-muted small">${label}</div>
    <div class="stat-val ${cls}">${value}</div>
  </div>`;
}
function pnlClass(v) { return v > 0 ? "pos" : v < 0 ? "neg" : "neu"; }
function pnlFmt(v)   { return (v >= 0 ? "+" : "") + "$" + Math.abs(v).toFixed(3); }

// â”€â”€ CRYPTO 5m â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const C_COLORS = {
  "1c": { border:"#f87171", bg:"rgba(248,113,113,0.12)" },
  "2c": { border:"#fbbf24", bg:"rgba(251,191,36,0.12)"  },
  "3c": { border:"#34d399", bg:"rgba(52,211,153,0.12)"  },
};
let cryptoCharts = {};

async function loadCrypto() {
  const d = await fetch("/api/data").then(r => r.json());
  const el = document.getElementById("crypto-content");
  if (d.error) {
    el.innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  document.getElementById("crypto-meta").textContent =
    `${d.total_candles} candles Â· ${d.total_resolved} resolved Â· last signal: ${d.last_update}`;

  el.innerHTML = "";
  d.assets.forEach(asset => {
    const ad = d.data[asset];
    let boxes = "";
    ["1c","2c","3c"].forEach(t => {
      const s  = ad.stats[t];
      const pc = pnlClass(s.total_pnl);
      const lbl = t === "1c" ? "â‰¤ 1Â¢" : t === "2c" ? "â‰¤ 2Â¢" : "â‰¤ 3Â¢";
      boxes += `
        <div class="col">
          <div class="stat-box h-100">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <span class="fw-bold" style="color:${C_COLORS[t].border}">${lbl}</span>
              <span class="badge bg-secondary">${s.resolved} resolved / ${s.pending} pending</span>
            </div>
            <div class="row g-2 text-center">
              <div class="col-4">
                <div class="text-muted small">Win rate</div>
                <div class="stat-val">${s.win_rate}%</div>
                <div class="text-muted small">${s.wins}W / ${s.losses}L</div>
              </div>
              <div class="col-4">
                <div class="text-muted small">Net P/L</div>
                <div class="stat-val ${pc}">${s.total_pnl >= 0 ? "+" : ""}$${s.total_pnl.toFixed(3)}</div>
              </div>
              <div class="col-4">
                <div class="text-muted small">ROI</div>
                <div class="stat-val ${pc}">${s.roi >= 0 ? "+" : ""}${s.roi.toFixed(1)}%</div>
                <div class="text-muted small">${s.signals} signals</div>
              </div>
            </div>
          </div>
        </div>`;
    });
    el.innerHTML += `
      <div class="card mb-4">
        <div class="card-header fw-bold fs-5">${esc(asset)}</div>
        <div class="card-body">
          <div class="row g-3 mb-3">${boxes}</div>
          <canvas id="chart-${asset}" style="max-height:320px"></canvas>
        </div>
      </div>`;
  });
  setTimeout(() => d.assets.forEach(a => buildCryptoChart(a, d.data[a])), 50);
}

function buildCryptoChart(asset, ad) {
  const ctx = document.getElementById(`chart-${asset}`);
  if (!ctx) return;
  if (cryptoCharts[asset]) cryptoCharts[asset].destroy();
  cryptoCharts[asset] = new Chart(ctx, {
    type: "line",
    data: {
      labels: ad.chart.labels,
      datasets: [
        { label:"â‰¤1Â¢", data:ad.chart.series_1c, borderColor:C_COLORS["1c"].border, backgroundColor:C_COLORS["1c"].bg, fill:true, tension:0.3, pointRadius:2, borderWidth:2 },
        { label:"â‰¤2Â¢", data:ad.chart.series_2c, borderColor:C_COLORS["2c"].border, backgroundColor:C_COLORS["2c"].bg, fill:true, tension:0.3, pointRadius:2, borderWidth:2 },
        { label:"â‰¤3Â¢", data:ad.chart.series_3c, borderColor:C_COLORS["3c"].border, backgroundColor:C_COLORS["3c"].bg, fill:true, tension:0.3, pointRadius:2, borderWidth:2 },
      ],
    },
    options: {
      responsive:true,
      interaction:{ mode:"index", intersect:false },
      plugins:{
        legend:{ position:"top" },
        tooltip:{ callbacks:{ label: c => ` ${c.dataset.label}: ${c.parsed.y>=0?"+":""}$${c.parsed.y.toFixed(3)}` } },
      },
      scales:{
        x:{ ticks:{ color:"#8b949e", maxTicksLimit:16, maxRotation:45 }, grid:{ color:"#21262d" } },
        y:{ ticks:{ color:"#8b949e", callback: v=>(v>=0?"+":"")+"$"+v.toFixed(2) }, grid:{ color:"#21262d" },
            title:{ display:true, text:"Cumulative P/L (USDC)", color:"#8b949e" } },
      },
    },
  });
}

// â”€â”€ ETH 1H â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let ethChart = null;
let allTrades = [];

async function loadEth() {
  const d = await fetch("/api/eth").then(r => r.json());
  if (d.error) {
    document.getElementById("eth-stats").innerHTML =
      `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  const s  = d.stats;
  const pc = pnlClass(s.total_pnl);
  document.getElementById("eth-stats").innerHTML = [
    statCard("Total",      s.total,                          ""),
    statCard("Wins",       s.wins,                           "pos"),
    statCard("Losses",     s.losses,                         "neg"),
    statCard("Pending",    s.pending,                        "neu"),
    statCard("Win Rate",   s.resolved ? s.win_rate+"%" : "â€”", s.win_rate>=50?"pos":"neg"),
    statCard("Net P/L",    pnlFmt(s.total_pnl),              pc),
    statCard("ROI",        (s.roi>=0?"+":"")+s.roi.toFixed(1)+"%", pc),
    statCard("Total Cost", "$"+s.total_cost.toFixed(2),      ""),
  ].join("");

  allTrades = d.trades;
  renderTrades();
  buildEthChart(d.chart);
}

function renderTrades() {
  const showDry = document.getElementById("showDryRun").checked;
  const trades  = allTrades.filter(t => showDry || !t.dry_run);
  const tbody   = document.getElementById("eth-trades-body");
  if (!trades.length) {
    tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted py-3">No trades yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = trades.map(t => {
    const time  = (t.placed_at || "â€”").substring(0,16).replace("T"," ");
    const slug  = (t.slug || "â€”").split("-").slice(-4).join("-");
    const dir   = t.direction === "Up"
      ? `<span class="dir-up">Up</span>`
      : `<span class="dir-down">Down</span>`;
    const mode  = t.dry_run
      ? `<span class="pill pill-dry">dry</span>`
      : `<span class="badge bg-success">live</span>`;
    const oc    = t.outcome;
    const pill  = oc === "win"       ? `<span class="pill pill-win">win</span>`
                : oc === "loss"      ? `<span class="pill pill-loss">loss</span>`
                : oc === "stop_loss" ? `<span class="pill pill-loss">stop</span>`
                : oc === "expired"   ? `<span class="pill pill-loss">expired</span>`
                :                     `<span class="pill pill-open">open</span>`;
    const pnl   = t.pnl !== null
      ? `<span class="${pnlClass(t.pnl)}">${t.pnl>=0?"+":""}$${Math.abs(t.pnl).toFixed(3)}</span>`
      : `<span class="text-muted">â€”</span>`;
    const mins  = t.mins_remaining !== null ? t.mins_remaining.toFixed(1) : "â€”";
    return `<tr>
      <td>${esc(time)}</td>
      <td class="text-muted" style="font-size:11px">${esc(slug)}</td>
      <td>${dir}</td>
      <td>${t.entry_price.toFixed(3)}</td>
      <td>${t.shares}</td>
      <td>$${(t.cost_usdc||0).toFixed(2)}</td>
      <td>${mins}</td>
      <td>${mode}</td>
      <td>${pill}</td>
      <td>${pnl}</td>
    </tr>`;
  }).join("");
}
document.getElementById("showDryRun").addEventListener("change", renderTrades);

function buildEthChart(chart) {
  const ctx = document.getElementById("eth-chart");
  if (!ctx) return;
  if (ethChart) ethChart.destroy();
  ethChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: chart.labels,
      datasets: [{
        label: "Cumulative P/L",
        data:  chart.series,
        borderColor:"#58a6ff", backgroundColor:"rgba(88,166,255,0.12)",
        fill:true, tension:0.3, pointRadius:3, borderWidth:2,
      }],
    },
    options: {
      responsive:true,
      interaction:{ mode:"index", intersect:false },
      plugins:{
        legend:{ position:"top" },
        tooltip:{ callbacks:{ label: c=>`  P/L: ${c.parsed.y>=0?"+":""}$${c.parsed.y.toFixed(3)}` } },
      },
      scales:{
        x:{ ticks:{ color:"#8b949e", maxTicksLimit:16, maxRotation:45 }, grid:{ color:"#21262d" } },
        y:{ ticks:{ color:"#8b949e", callback: v=>(v>=0?"+":"")+"$"+v.toFixed(2) }, grid:{ color:"#21262d" },
            title:{ display:true, text:"Cumulative P/L (USDC)", color:"#8b949e" } },
      },
    },
  });
}

// â”€â”€ log â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const NOISE_RE = /outside entry window|min remaining/;

async function loadLog() {
  const d       = await fetch("/api/log").then(r => r.json());
  const hide    = document.getElementById("hideNoise").checked;
  const logBox  = document.getElementById("log-box");
  const lines   = hide ? d.lines.filter(l => !NOISE_RE.test(l)) : d.lines;
  logBox.innerHTML = lines.map(raw => {
    const l = raw.trimEnd();
    let cls = "log-info";
    if (/ERROR|CRITICAL/i.test(l))                         cls = "log-error";
    else if (/WARN/i.test(l))                              cls = "log-warn";
    else if (/win|redeemed/i.test(l))                      cls = "log-win";
    else if (/loss|stop_loss|expired/i.test(l))            cls = "log-loss";
    else if (/BUY|TRADE|DRY.RUN|placed|entry|signal/i.test(l)) cls = "log-trade";
    else if (NOISE_RE.test(l))                             cls = "log-dim";
    return `<div class="${cls}">${esc(l)}</div>`;
  }).join("");
  document.getElementById("log-ts").textContent =
    "updated " + new Date().toLocaleTimeString();
  if (document.getElementById("autoScroll").checked)
    logBox.scrollTop = logBox.scrollHeight;
}
document.getElementById("hideNoise").addEventListener("change", loadLog);

// â”€â”€ weather â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function loadWeather() {
  const d  = await fetch("/api/weather").then(r => r.json());
  const pc = pnlClass(d.stats?.total_pnl ?? 0);
  if (d.error) {
    document.getElementById("weather-stats").innerHTML =
      `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  const s = d.stats;
  document.getElementById("weather-stats").innerHTML = [
    statCard("Total Bets",  s.total,                          ""),
    statCard("Wins",        s.wins,                           "pos"),
    statCard("Losses",      s.losses,                         "neg"),
    statCard("Pending",     s.pending,                        "neu"),
    statCard("Win Rate",    s.resolved ? s.win_rate+"%" : "â€”", s.win_rate>=50?"pos":"neg"),
    statCard("Net P/L",     pnlFmt(s.total_pnl),              pc),
    statCard("Total Cost",  "$"+s.total_cost.toFixed(2),      ""),
  ].join("");

  const tbody = document.getElementById("weather-body");
  if (!d.bets.length) {
    tbody.innerHTML = `<tr><td colspan="12" class="text-center text-muted py-3">No bets yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = d.bets.map(b => {
    const time = (b.placed_at_utc || "â€”").substring(0,16).replace("T"," ");
    const q    = b.question && b.question.length > 48
                 ? b.question.substring(0,48)+"â€¦" : (b.question || "â€”");
    const mrg  = b.forecast_minus_threshold !== null
                 ? `<span class="${b.forecast_minus_threshold<=0?"pos":"neg"}">${b.forecast_minus_threshold>=0?"+":""}${b.forecast_minus_threshold.toFixed(1)}</span>`
                 : "â€”";
    const oc   = b.outcome === "win"  ? `<span class="pill pill-win">win</span>`
               : b.outcome === "loss" ? `<span class="pill pill-loss">loss</span>`
               :                       `<span class="pill pill-open">open</span>`;
    return `<tr>
      <td>${esc(time)}</td>
      <td><b>${esc(b.city)}</b></td>
      <td>${esc(b.event_date||"â€”")}</td>
      <td title="${esc(b.question||"")}">${esc(q)}</td>
      <td>${b.shares}</td>
      <td>${b.no_price!==null ? b.no_price.toFixed(3) : "â€”"}</td>
      <td>$${(b.cost_usdc||0).toFixed(2)}</td>
      <td>${b.forecast_high!==null ? b.forecast_high.toFixed(1) : "â€”"}</td>
      <td>${mrg}</td>
      <td>${oc}</td>
      <td>${b.resolved_temp!==null ? b.resolved_temp : "â€”"}</td>
      <td>
        <div style="display:inline-flex;gap:4px;align-items:center">
          <input type="number" step="0.1" placeholder="temp" class="edit-input"
            id="rt-${b.id}" value="${b.resolved_temp!==null?b.resolved_temp:""}">
          <select class="edit-select" id="oc-${b.id}">
            <option value=""   ${!b.outcome?"selected":""}>â€”</option>
            <option value="win"  ${b.outcome==="win" ?"selected":""}>win</option>
            <option value="loss" ${b.outcome==="loss"?"selected":""}>loss</option>
          </select>
          <button class="edit-btn" onclick="updateWeather(${b.id})">âœ“</button>
        </div>
      </td>
    </tr>`;
  }).join("");
}

async function updateWeather(id) {
  const rt = document.getElementById(`rt-${id}`).value;
  const oc = document.getElementById(`oc-${id}`).value;
  await fetch(`/api/weather/update/${id}`, {
    method:  "POST",
    headers: {"Content-Type":"application/json"},
    body:    JSON.stringify({ resolved_temp: rt ? parseFloat(rt) : null, outcome: oc || null }),
  });
  loadWeather();
}

// â”€â”€ BS Forward Test â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let ftCharts = {};
let ftLoaded = false;

async function loadFT() {
  ftLoaded = true;
  const minEdge = parseFloat(document.getElementById("ft-min-edge").value) || 0;
  const d = await fetch(`/api/bs-forward?min_edge=${minEdge}`).then(r => r.json());

  const globalEl  = document.getElementById("ft-global-stats");
  const contentEl = document.getElementById("ft-content");
  const metaEl    = document.getElementById("ft-meta");

  if (d.error) {
    globalEl.innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    contentEl.innerHTML = "";
    return;
  }

  const pc = pnlClass(d.total_pnl);
  metaEl.textContent = `${d.total} BTC signals | real ask price, BS edge filter`;
  globalEl.innerHTML = [
    statCard("Total Signals", d.total,                    ""),
    statCard("Net P/L",       pnlFmt(d.total_pnl),        pc),
  ].join("");

  contentEl.innerHTML = "";
  d.assets.forEach(asset => {
    const ad = d.data[asset];
    const s  = ad.stats;
    const pc = pnlClass(s.total_pnl);

    const statBoxes = `
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Signals</div>
        <div class="stat-val">${s.signals}</div>
        <div class="text-muted small">${s.resolved} resolved / ${s.pending} pending</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Win Rate</div>
        <div class="stat-val ${s.win_rate>=50?"pos":"neg"}">${s.resolved ? s.win_rate+"%" : "â€”"}</div>
        <div class="text-muted small">${s.wins}W / ${s.losses}L</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Net P/L</div>
        <div class="stat-val ${pc}">${s.total_pnl>=0?"+":""}$${s.total_pnl.toFixed(4)}</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">ROI</div>
        <div class="stat-val ${pc}">${s.roi>=0?"+":""}${s.roi.toFixed(1)}%</div>
        <div class="text-muted small">cost $${s.total_cost.toFixed(3)}</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Avg BS Edge</div>
        <div class="stat-val ${s.avg_edge>=0?"pos":"neg"}">${s.avg_edge>=0?"+":""}${(s.avg_edge*100).toFixed(2)}%</div>
      </div></div>`;

    const tbody = (ad.trades || []).slice(0, 200).map(r => {
      const dt   = new Date(r.signal_ts * 1000).toISOString().substring(0,16).replace("T"," ");
      const dir  = r.side === "Up"
        ? `<span class="dir-up">Up</span>`
        : `<span class="dir-down">Down</span>`;
      const won  = r.won === 1  ? `<span class="pill pill-win">win</span>`
                 : r.won === 0  ? `<span class="pill pill-loss">loss</span>`
                 :                `<span class="pill pill-open">open</span>`;
      const pnl  = r.pnl != null
        ? `<span class="${pnlClass(r.pnl)}">${r.pnl>=0?"+":""}$${Math.abs(r.pnl).toFixed(4)}</span>`
        : `<span class="text-muted">â€”</span>`;
      const edge = r.edge != null
        ? `<span class="${r.edge>=0?"pos":"neg"}">${(r.edge*100).toFixed(2)}%</span>`
        : "â€”";
      const fair = r.fair_price != null ? r.fair_price.toFixed(4) : "â€”";
      const market = r.slug || "â€”";
      return `<tr>
        <td class="text-muted" style="font-size:11px">${esc(dt)}</td>
        <td class="text-muted" style="font-size:11px">${esc(market)}</td>
        <td>${dir}</td>
        <td>$${r.entry_price.toFixed(4)}</td>
        <td>${fair}</td>
        <td>${edge}</td>
        <td>${r.secs_remaining != null ? r.secs_remaining+"s" : "â€”"}</td>
        <td>${won}</td>
        <td>${pnl}</td>
      </tr>`;
    }).join("");

    contentEl.innerHTML += `
      <div class="card mb-4">
        <div class="card-header fw-bold fs-5">${esc(asset)}</div>
        <div class="card-body">
          <div class="row g-2 mb-3">${statBoxes}</div>
          <canvas id="ft-chart-${asset}" style="max-height:260px"></canvas>
          <div class="mt-3 table-scroll" style="max-height:400px">
            <table class="table table-sm mb-0">
              <thead style="position:sticky;top:0;z-index:1">
                <tr>
                  <th>Time (UTC)</th><th>Market</th><th>Side</th>
                  <th>Ask (entry)</th><th>Fair price</th><th>BS Edge</th>
                  <th>Secs left</th><th>Result</th><th>P/L</th>
                </tr>
              </thead>
              <tbody>${tbody}</tbody>
            </table>
          </div>
        </div>
      </div>`;
  });

  setTimeout(() => d.assets.forEach(a => buildFtChart(a, d.data[a])), 50);
  loadFTLog();
}

function buildFtChart(asset, ad) {
  const ctx = document.getElementById(`ft-chart-${asset}`);
  if (!ctx) return;
  if (ftCharts[asset]) ftCharts[asset].destroy();
  ftCharts[asset] = new Chart(ctx, {
    type: "line",
    data: {
      labels: ad.chart.labels,
      datasets: [{
        label: "Cumulative P/L",
        data:  ad.chart.series,
        borderColor: "#58a6ff", backgroundColor: "rgba(88,166,255,0.12)",
        fill: true, tension: 0.3, pointRadius: 3, borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: { callbacks: { label: c => `  P/L: ${c.parsed.y>=0?"+":""}$${c.parsed.y.toFixed(4)}` } },
      },
      scales: {
        x: { ticks: { color:"#8b949e", maxTicksLimit:16, maxRotation:45 }, grid: { color:"#21262d" } },
        y: { ticks: { color:"#8b949e", callback: v=>(v>=0?"+":"")+"$"+v.toFixed(3) }, grid: { color:"#21262d" },
             title: { display:true, text:"Cumulative P/L (USDC)", color:"#8b949e" } },
      },
    },
  });
}

async function loadFTLog() {
  const box = document.getElementById("ft-log-box");
  const ts = document.getElementById("ft-log-ts");
  if (!box || !ts) return;
  const d = await fetch("/api/bs-log").then(r => r.json());
  const lines = d.lines || [];
  box.innerHTML = lines.map(raw => {
    const l = raw.trimEnd();
    let cls = "log-info";
    if (/BET-OPEN/i.test(l)) cls = "log-trade";
    else if (/BET-RESOLVE|MARKET-RESOLVED/i.test(l)) cls = "log-win";
    else if (/ERROR|CRITICAL/i.test(l)) cls = "log-error";
    else if (/WARN/i.test(l)) cls = "log-warn";
    return `<div class="${cls}">${esc(l)}</div>`;
  }).join("");
  ts.textContent = "updated " + new Date().toLocaleTimeString();
  box.scrollTop = box.scrollHeight;
}

// â”€â”€ bootstrap â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function refreshAll() { loadEth(); loadLog(); loadWeather(); if(ftLoaded) { loadFT(); loadFTLog(); } }

loadEth();      setInterval(loadEth,     30_000);
loadLog();      setInterval(loadLog,     10_000);
loadWeather();
loadFTLog();    setInterval(loadFTLog,   10_000);
</script>
</body>
</html>
"""


# â”€â”€ routes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.route("/")
def index():
    return render_template_string(_TEMPLATE)


@app.route("/api/data")
def api_data():
    return jsonify(_query_crypto_stats())


@app.route("/api/eth")
def api_eth():
    return jsonify(_query_eth_stats())


@app.route("/api/log")
def api_log():
    return jsonify({"lines": _read_log_tail()})

@app.route("/api/bs-log")
def api_bs_log():
    return jsonify({"lines": _read_bs_log_tail()})


@app.route("/api/weather")
def api_weather():
    return jsonify(_query_weather_stats())


@app.route("/api/bs-forward")
def api_bs_forward():
    try:
        min_edge = float(request.args.get("min_edge", 0))
    except (TypeError, ValueError):
        min_edge = 0.0
    return jsonify(_query_bs_forward(min_edge))


@app.route("/api/weather/update/<int:bet_id>", methods=["POST"])
def api_weather_update(bet_id: int):
    data         = request.get_json(force=True) or {}
    resolved_temp = data.get("resolved_temp")
    outcome       = data.get("outcome") or None
    with sqlite3.connect(_bets_db()) as conn:
        conn.execute(
            "UPDATE placed_bets SET resolved_temp = ?, outcome = ? WHERE id = ?",
            (resolved_temp, outcome, bet_id),
        )
    return jsonify({"ok": True})


# â”€â”€ entrypoint â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main() -> None:
    p = argparse.ArgumentParser(description="Unified trading stats web UI")
    p.add_argument("--port",    type=int, default=5051)
    p.add_argument("--host",    default="0.0.0.0")
    p.add_argument("--db",      default=CRYPTO_DB_PATH, dest="crypto_db",
                   help="Path to crypto_5m.db")
    p.add_argument("--bets-db", default=BETS_DB_PATH,
                   help="Path to bets.db (ETH 1H + Weather)")
    args = p.parse_args()

    import socket
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  Stats UI:  http://localhost:{args.port}")
    print(f"  On LAN:    http://{local_ip}:{args.port}")
    print(f"  Tabs:      ETH 1H Bot | Weather Bets | BTC 5m BS Test")

    app.config["CRYPTO_DB_PATH"] = args.crypto_db
    app.config["BETS_DB_PATH"]   = args.bets_db
    app.config["BS_LOG_PATH"]    = BS_LOG_PATH
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()





