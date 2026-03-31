"""
Serve a simple browser UI for bets.db.
Run:  python -m automata.view_bets
Then open http://localhost:5050
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template_string, request, url_for

DB_PATH = Path(__file__).resolve().parent.parent / "bets.db"

app = Flask(__name__)

# ── HTML template ─────────────────────────────────────────────────────────────

TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bets DB</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; font-size: 13px; }

  h1 { padding: 18px 24px 4px; font-size: 20px; color: #f8fafc; }

  .summary {
    display: flex; gap: 16px; padding: 10px 24px 16px; flex-wrap: wrap;
  }
  .card {
    background: #1e2330; border-radius: 8px; padding: 12px 20px; min-width: 130px;
  }
  .card .label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; }
  .card .value { font-size: 22px; font-weight: 700; color: #f1f5f9; margin-top: 2px; }
  .card .value.green { color: #4ade80; }
  .card .value.red   { color: #f87171; }
  .card .value.amber { color: #fbbf24; }

  .filters { padding: 0 24px 12px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
  .filters select, .filters input {
    background: #1e2330; border: 1px solid #334155; color: #e2e8f0;
    padding: 5px 10px; border-radius: 6px; font-size: 12px;
  }
  .filters label { font-size: 12px; color: #94a3b8; }

  .wrap { overflow-x: auto; padding: 0 24px 40px; }
  table { border-collapse: collapse; width: 100%; white-space: nowrap; }
  th {
    background: #1e2330; color: #94a3b8; font-size: 11px; text-transform: uppercase;
    letter-spacing: .05em; padding: 8px 10px; text-align: left;
    position: sticky; top: 0; z-index: 1; border-bottom: 1px solid #334155;
  }
  td { padding: 7px 10px; border-bottom: 1px solid #1e2330; vertical-align: middle; }
  tr:hover td { background: #1a2236; }

  .pill {
    display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600;
  }
  .pill.win  { background: #14532d; color: #4ade80; }
  .pill.loss { background: #450a0a; color: #f87171; }
  .pill.open { background: #1e3a5f; color: #93c5fd; }

  .margin.pos { color: #4ade80; }
  .margin.neg { color: #f87171; }

  /* inline edit */
  .edit-form { display: inline-flex; gap: 4px; align-items: center; }
  .edit-form input[type=number], .edit-form select {
    background: #0f1117; border: 1px solid #475569; color: #e2e8f0;
    padding: 3px 6px; border-radius: 4px; font-size: 12px; width: 70px;
  }
  .edit-form select { width: auto; }
  .edit-form button {
    background: #2563eb; border: none; color: #fff; padding: 3px 8px;
    border-radius: 4px; cursor: pointer; font-size: 11px;
  }
  .edit-form button:hover { background: #1d4ed8; }

  .empty { color: #475569; }
</style>
</head>
<body>

<h1>Bets DB</h1>

<div class="summary">
  <div class="card">
    <div class="label">Total bets</div>
    <div class="value">{{ stats.total }}</div>
  </div>
  <div class="card">
    <div class="label">Total cost</div>
    <div class="value">${{ "%.2f"|format(stats.total_cost) }}</div>
  </div>
  <div class="card">
    <div class="label">Wins</div>
    <div class="value green">{{ stats.wins }}</div>
  </div>
  <div class="card">
    <div class="label">Losses</div>
    <div class="value red">{{ stats.losses }}</div>
  </div>
  <div class="card">
    <div class="label">Open</div>
    <div class="value amber">{{ stats.open }}</div>
  </div>
  <div class="card">
    <div class="label">Win rate</div>
    <div class="value {% if stats.win_rate >= 50 %}green{% else %}red{% endif %}">
      {% if stats.resolved > 0 %}{{ stats.win_rate }}%{% else %}—{% endif %}
    </div>
  </div>
</div>

<div class="filters">
  <form method="get" style="display:contents">
    <label>City
      <select name="city" onchange="this.form.submit()">
        <option value="">All</option>
        {% for c in cities %}
        <option value="{{ c }}" {% if filter_city == c %}selected{% endif %}>{{ c }}</option>
        {% endfor %}
      </select>
    </label>
    <label>Outcome
      <select name="outcome" onchange="this.form.submit()">
        <option value="">All</option>
        <option value="win"  {% if filter_outcome == 'win'  %}selected{% endif %}>Win</option>
        <option value="loss" {% if filter_outcome == 'loss' %}selected{% endif %}>Loss</option>
        <option value="open" {% if filter_outcome == 'open' %}selected{% endif %}>Open</option>
      </select>
    </label>
  </form>
</div>

<div class="wrap">
<table>
  <thead>
    <tr>
      <th>#</th>
      <th>Placed (UTC)</th>
      <th>City</th>
      <th>ICAO</th>
      <th>Event date</th>
      <th>Question</th>
      <th>Option</th>
      <th>Shares</th>
      <th>No price</th>
      <th>Yes price</th>
      <th>Cost $</th>
      <th>Unit</th>
      <th>Threshold</th>
      <th>Direction</th>
      <th>Forecast</th>
      <th>Margin</th>
      <th>Days out</th>
      <th>Month</th>
      <th>Resolved temp</th>
      <th>Outcome</th>
    </tr>
  </thead>
  <tbody>
  {% for r in rows %}
    <tr>
      <td class="empty">{{ r.id }}</td>
      <td>{{ r.placed_at_utc[:16].replace("T"," ") if r.placed_at_utc else "—" }}</td>
      <td><b>{{ r.city }}</b></td>
      <td class="empty">{{ r.icao or "—" }}</td>
      <td>{{ r.event_date }}</td>
      <td>{{ r.question }}</td>
      <td>{{ r.option }}</td>
      <td>{{ r.shares }}</td>
      <td>{{ "%.3f"|format(r.no_price) if r.no_price is not none else "—" }}</td>
      <td>{{ "%.3f"|format(r.yes_price) if r.yes_price is not none else "—" }}</td>
      <td>${{ "%.2f"|format(r.cost_usdc) if r.cost_usdc is not none else "—" }}</td>
      <td>{{ r.unit or "—" }}</td>
      <td>{{ r.threshold if r.threshold is not none else "—" }}{% if r.threshold_hi is not none %}–{{ r.threshold_hi }}{% endif %}</td>
      <td>{{ r.direction or "—" }}</td>
      <td>{{ "%.1f"|format(r.forecast_high) if r.forecast_high is not none else "—" }}</td>
      <td>
        {% if r.forecast_minus_threshold is not none %}
          <span class="margin {{ 'pos' if r.forecast_minus_threshold <= 0 else 'neg' }}">
            {{ "%+.1f"|format(r.forecast_minus_threshold) }}
          </span>
        {% else %}—{% endif %}
      </td>
      <td>{{ r.days_until_event if r.days_until_event is not none else "—" }}</td>
      <td>{{ r.month or "—" }}</td>
      <td>
        <form class="edit-form" method="post" action="/update/{{ r.id }}">
          <input type="number" step="0.1" name="resolved_temp"
                 value="{{ r.resolved_temp if r.resolved_temp is not none else '' }}"
                 placeholder="temp">
          <select name="outcome">
            <option value="" {% if not r.outcome %}selected{% endif %}>—</option>
            <option value="win"  {% if r.outcome == 'win'  %}selected{% endif %}>win</option>
            <option value="loss" {% if r.outcome == 'loss' %}selected{% endif %}>loss</option>
          </select>
          <button type="submit">✓</button>
        </form>
      </td>
      <td>
        {% if r.outcome == 'win' %}
          <span class="pill win">win</span>
        {% elif r.outcome == 'loss' %}
          <span class="pill loss">loss</span>
        {% else %}
          <span class="pill open">open</span>
        {% endif %}
      </td>
    </tr>
  {% else %}
    <tr><td colspan="20" style="text-align:center;padding:30px;color:#475569">No bets recorded yet.</td></tr>
  {% endfor %}
  </tbody>
</table>
</div>

</body>
</html>
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _all_rows(city: str = "", outcome: str = "") -> list[sqlite3.Row]:
    clauses, params = [], []
    if city:
        clauses.append("city = ?")
        params.append(city)
    if outcome == "open":
        clauses.append("outcome IS NULL")
    elif outcome in ("win", "loss"):
        clauses.append("outcome = ?")
        params.append(outcome)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _get_conn() as conn:
        return conn.execute(
            f"SELECT * FROM placed_bets {where} ORDER BY placed_at_utc DESC",
            params,
        ).fetchall()


def _stats() -> dict:
    with _get_conn() as conn:
        rows = conn.execute("SELECT outcome, cost_usdc FROM placed_bets").fetchall()
    total = len(rows)
    total_cost = sum(r["cost_usdc"] or 0 for r in rows)
    wins   = sum(1 for r in rows if r["outcome"] == "win")
    losses = sum(1 for r in rows if r["outcome"] == "loss")
    resolved = wins + losses
    return {
        "total": total,
        "total_cost": total_cost,
        "wins": wins,
        "losses": losses,
        "open": total - resolved,
        "resolved": resolved,
        "win_rate": round(wins / resolved * 100) if resolved else 0,
    }


def _cities() -> list[str]:
    with _get_conn() as conn:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT city FROM placed_bets ORDER BY city"
        ).fetchall()]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    city    = request.args.get("city", "")
    outcome = request.args.get("outcome", "")
    rows    = _all_rows(city, outcome)
    return render_template_string(
        TEMPLATE,
        rows=rows,
        stats=_stats(),
        cities=_cities(),
        filter_city=city,
        filter_outcome=outcome,
    )


@app.post("/update/<int:bet_id>")
def update(bet_id: int):
    raw_temp = request.form.get("resolved_temp", "").strip()
    outcome  = request.form.get("outcome", "").strip() or None
    resolved_temp = float(raw_temp) if raw_temp else None
    with _get_conn() as conn:
        conn.execute(
            "UPDATE placed_bets SET resolved_temp = ?, outcome = ? WHERE id = ?",
            (resolved_temp, outcome, bet_id),
        )
    return redirect(url_for("index"))


@app.get("/api/bets")
def api_bets():
    """JSON endpoint for the ML pipeline — returns all rows as dicts."""
    with _get_conn() as conn:
        rows = conn.execute("SELECT * FROM placed_bets ORDER BY placed_at_utc").fetchall()
    return jsonify([dict(r) for r in rows])


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"No bets.db found at {DB_PATH} — start the automata bot first.")
    else:
        print(f"Opening {DB_PATH}")
    print("Serving at http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)
