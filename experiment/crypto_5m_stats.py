#!/usr/bin/env python3
"""
Crypto 5m penny stats — Flask web dashboard.

Reads experiment/crypto_5m.db (written by crypto_5m_scanner) and renders
per-asset cumulative P/L charts at three price thresholds plus summary stats.

Start:  python -m experiment.crypto_5m_stats
Opens:  http://localhost:5051   (accessible to any device on your LAN)
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string

DB_PATH = os.path.join("experiment", "crypto_5m.db")
TIERS = [("1c", 0.01), ("2c", 0.02), ("3c", 0.03)]

app = Flask(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_path() -> str:
    return app.config.get("DB_PATH", DB_PATH)


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _query_stats() -> dict:
    """Return all data needed for the dashboard."""
    if not os.path.exists(_db_path()):
        return {"error": "DB not found — start crypto_5m_scanner first.", "assets": []}

    conn = _db()

    # All resolved signals, ordered by candle_start
    rows = conn.execute("""
        SELECT asset, side, min_price, candle_start, won, pnl
        FROM signals
        WHERE won IS NOT NULL
        ORDER BY asset, candle_start
    """).fetchall()

    # All signals including unresolved (for pending count)
    all_rows = conn.execute("""
        SELECT asset, side, min_price, won
        FROM signals
    """).fetchall()

    total_candles = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    total_resolved = conn.execute(
        "SELECT COUNT(*) FROM candles WHERE winner IS NOT NULL AND winner != '?'"
    ).fetchone()[0]
    last_signal = conn.execute(
        "SELECT MAX(signal_ts) FROM signals"
    ).fetchone()[0]
    conn.close()

    # Group resolved signals by asset
    by_asset: dict[str, list] = {}
    for row in rows:
        by_asset.setdefault(row["asset"], []).append(dict(row))

    # Group all signals by asset for pending count
    all_by_asset: dict[str, list] = {}
    for row in all_rows:
        all_by_asset.setdefault(row["asset"], []).append(dict(row))

    # Include all assets that have any signal (resolved or pending)
    all_assets = sorted(set(by_asset.keys()) | set(all_by_asset.keys()))
    assets_sorted = all_assets

    assets_data: dict[str, dict] = {}
    for asset in assets_sorted:
        asset_rows = by_asset.get(asset, [])
        all_asset_rows = all_by_asset.get(asset, [])

        # Build chart series: one point per unique candle_start (resolved only)
        # For each tier, accumulate P/L in candle_start order
        candle_starts = sorted(set(r["candle_start"] for r in asset_rows))

        series: dict[str, list[float]] = {"1c": [], "2c": [], "3c": []}
        labels: list[str] = []
        running: dict[str, float] = {"1c": 0.0, "2c": 0.0, "3c": 0.0}

        for cs in candle_starts:
            candle_rows = [r for r in asset_rows if r["candle_start"] == cs]
            dt = datetime.fromtimestamp(cs, tz=timezone.utc)
            labels.append(dt.strftime("%m/%d %H:%M"))

            for tier_key, threshold in TIERS:
                tier_rows = [r for r in candle_rows if r["min_price"] <= threshold]
                running[tier_key] += sum(r["pnl"] for r in tier_rows)
                series[tier_key].append(round(running[tier_key], 4))

        # Summary stats per tier
        stats: dict[str, dict] = {}
        for tier_key, threshold in TIERS:
            resolved = [r for r in asset_rows if r["min_price"] <= threshold]
            pending  = [r for r in all_asset_rows
                        if r["min_price"] <= threshold and r["won"] is None]
            wins     = [r for r in resolved if r["won"] == 1]
            total_pnl = sum(r["pnl"] for r in resolved)
            total_cost = sum(r["min_price"] for r in resolved)
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
            "chart": {
                "labels": labels,
                "series_1c": series["1c"],
                "series_2c": series["2c"],
                "series_3c": series["3c"],
            },
            "stats": stats,
        }

    last_update = (
        datetime.fromtimestamp(last_signal, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if last_signal else "—"
    )

    return {
        "assets": assets_sorted,
        "data": assets_data,
        "total_candles": total_candles,
        "total_resolved": total_resolved,
        "last_update": last_update,
    }


# ── template ──────────────────────────────────────────────────────────────────

_TEMPLATE = r"""<!doctype html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Crypto 5m Penny Stats</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    body { background: #0d1117; color: #e6edf3; }
    .card { background: #161b22; border: 1px solid #30363d; }
    .card-header { background: #21262d; border-bottom: 1px solid #30363d; }
.stat-box { background: #21262d; border-radius: 8px; padding: 12px 16px; }
    .stat-val { font-size: 1.4rem; font-weight: 700; }
    .pos { color: #3fb950; }
    .neg { color: #f85149; }
    .neu { color: #8b949e; }
    canvas { max-height: 340px; }
    #spinner { display:none; }
  </style>
</head>
<body>
<div class="container-fluid py-3">

  <!-- header -->
  <div class="d-flex align-items-center justify-content-between mb-3">
    <div>
      <h4 class="mb-0">Crypto 5m Penny Signal Stats</h4>
      <small class="text-muted" id="meta-line">loading…</small>
    </div>
    <button class="btn btn-sm btn-outline-secondary" onclick="loadData()">
      &#8635; Refresh
    </button>
  </div>

  <!-- all assets stacked -->
  <div id="content"></div>

</div>

<script>
const COLORS = {
  "1c": { border: "#f87171", bg: "rgba(248,113,113,0.12)" },
  "2c": { border: "#fbbf24", bg: "rgba(251,191,36,0.12)"  },
  "3c": { border: "#34d399", bg: "rgba(52,211,153,0.12)"  },
};

let charts = {};

async function loadData() {
  const resp = await fetch("/api/data");
  const d = await resp.json();
  const contentEl = document.getElementById("content");

  if (d.error) {
    contentEl.innerHTML = `<div class="alert alert-danger">${d.error}</div>`;
    return;
  }

  document.getElementById("meta-line").textContent =
    `${d.total_candles} candles total · ${d.total_resolved} resolved · last signal: ${d.last_update}`;

  contentEl.innerHTML = "";

  d.assets.forEach(asset => {
    const ad = d.data[asset];

    let statBoxes = "";
    ["1c","2c","3c"].forEach(t => {
      const s = ad.stats[t];
      const pnlClass = s.total_pnl > 0 ? "pos" : s.total_pnl < 0 ? "neg" : "neu";
      const pnlSign  = s.total_pnl >= 0 ? "+" : "";
      const roiSign  = s.roi >= 0 ? "+" : "";
      const tier_label = t === "1c" ? "≤ 1¢" : t === "2c" ? "≤ 2¢" : "≤ 3¢";
      statBoxes += `
        <div class="col">
          <div class="stat-box h-100">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <span class="fw-bold" style="color:${COLORS[t].border}">${tier_label}</span>
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
                <div class="stat-val ${pnlClass}">${pnlSign}$${s.total_pnl.toFixed(3)}</div>
              </div>
              <div class="col-4">
                <div class="text-muted small">ROI</div>
                <div class="stat-val ${pnlClass}">${roiSign}${s.roi.toFixed(1)}%</div>
                <div class="text-muted small">${s.signals} signals</div>
              </div>
            </div>
          </div>
        </div>`;
    });

    const canvasId = `chart-${asset}`;
    contentEl.innerHTML += `
      <div class="card mb-4">
        <div class="card-header fw-bold fs-5">${asset}</div>
        <div class="card-body">
          <div class="row g-3 mb-3">${statBoxes}</div>
          <canvas id="${canvasId}"></canvas>
        </div>
      </div>`;
  });

  setTimeout(() => {
    d.assets.forEach(asset => buildChart(asset, d.data[asset]));
  }, 50);
}

function buildChart(asset, ad) {
  const canvasId = `chart-${asset}`;
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (charts[asset]) {
    charts[asset].destroy();
  }

  const labels = ad.chart.labels;
  const datasets = [
    {
      label: "≤1¢",
      data: ad.chart.series_1c,
      borderColor: COLORS["1c"].border,
      backgroundColor: COLORS["1c"].bg,
      fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2,
    },
    {
      label: "≤2¢",
      data: ad.chart.series_2c,
      borderColor: COLORS["2c"].border,
      backgroundColor: COLORS["2c"].bg,
      fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2,
    },
    {
      label: "≤3¢",
      data: ad.chart.series_3c,
      borderColor: COLORS["3c"].border,
      backgroundColor: COLORS["3c"].bg,
      fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2,
    },
  ];

  charts[asset] = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        title: {
          display: true,
          text: `${asset} — Cumulative P/L by price threshold`,
          color: "#e6edf3",
        },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y >= 0 ? "+" : ""}${ctx.parsed.y.toFixed(4)}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8b949e", maxTicksLimit: 16, maxRotation: 45 },
          grid: { color: "#21262d" },
        },
        y: {
          ticks: {
            color: "#8b949e",
            callback: v => (v >= 0 ? "+" : "") + v.toFixed(3),
          },
          grid: { color: "#21262d" },
          title: { display: true, text: "Cumulative P/L (USDC)", color: "#8b949e" },
        },
      },
    },
  });
}

loadData();
setInterval(loadData, 30000); // auto-refresh every 30s
</script>
</body>
</html>
"""


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(_TEMPLATE)


@app.route("/api/data")
def api_data():
    return jsonify(_query_stats())


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Crypto 5m penny stats web UI")
    p.add_argument("--port", type=int, default=5051)
    p.add_argument("--db", default=DB_PATH)
    p.add_argument("--host", default="0.0.0.0")
    args = p.parse_args()

    import socket
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  Stats UI:  http://localhost:{args.port}")
    print(f"  On LAN:    http://{local_ip}:{args.port}")
    app.config["DB_PATH"] = args.db
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
