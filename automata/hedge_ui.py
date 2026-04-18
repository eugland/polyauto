from __future__ import annotations

import argparse
import json
import re
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string

DEFAULT_LOG_PATH = Path("experiment") / "logs" / "btc_hedge.log"
MAX_TAIL_LINES = 1200

EVENT_PATTERN = re.compile(
    r"ERROR|WARNING|FILLED|CANCELLED|PLACED sell|New candle|Split inventory confirmed|PHASE|WS connected",
    re.IGNORECASE,
)

STATE_PREFIX = "STATE  "

_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BTC Hedge Live Monitor</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #111827;
      --muted: #6b7280;
      --ok: #0f766e;
      --warn: #b45309;
      --err: #b91c1c;
      --line: #e5e7eb;
      --accent: #0b6e99;
    }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.4 "IBM Plex Sans", "Segoe UI", sans-serif; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 20px; }
    h1 { margin: 0 0 12px; font-size: 24px; }
    .muted { color: var(--muted); }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; margin: 14px 0; }
    .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px; }
    .k { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .v { font-size: 20px; font-weight: 700; margin-top: 4px; }
    .row { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; }
    pre { margin: 0; background: #0f172a; color: #e2e8f0; border-radius: 10px; padding: 12px; overflow: auto; max-height: 55vh; }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .err { color: var(--err); }
    table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
    th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); font-size: 13px; vertical-align: top; }
    th { background: #f1f5f9; }
    .pill { display: inline-block; font-size: 11px; padding: 2px 7px; border-radius: 999px; border: 1px solid var(--line); }
    .p-ok { background: #ecfdf5; color: #065f46; border-color: #bbf7d0; }
    .p-warn { background: #fffbeb; color: #92400e; border-color: #fde68a; }
    .p-err { background: #fef2f2; color: #991b1b; border-color: #fecaca; }
    @media (max-width: 900px) {
      .grid { grid-template-columns: repeat(2, minmax(0,1fr)); }
      .row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>BTC Hedge Live Monitor</h1>
    <div class="muted" id="meta">loading...</div>

    <div class="grid">
      <div class="card"><div class="k">Bot Status</div><div class="v" id="status">-</div></div>
      <div class="card"><div class="k">Market</div><div class="v" id="market">-</div></div>
      <div class="card"><div class="k">Open Orders</div><div class="v" id="orders">-</div></div>
      <div class="card"><div class="k">Fills / Recv</div><div class="v" id="fillsRecv">-</div></div>
    </div>

    <div class="grid">
      <div class="card"><div class="k">Warnings (5m)</div><div class="v warn" id="warn5m">0</div></div>
      <div class="card"><div class="k">Errors (5m)</div><div class="v err" id="err5m">0</div></div>
      <div class="card"><div class="k">Placed (5m)</div><div class="v" id="placed5m">0</div></div>
      <div class="card"><div class="k">Filled (5m)</div><div class="v ok" id="filled5m">0</div></div>
    </div>

    <div class="row">
      <div class="card">
        <div class="k">Latest STATE</div>
        <pre id="stateLine">-</pre>
      </div>
      <div class="card">
        <div class="k">Recent Events</div>
        <table>
          <thead><tr><th style="width:90px">Level</th><th style="width:160px">Time</th><th>Message</th></tr></thead>
          <tbody id="events"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    function levelClass(level) {
      if (level === "ERROR") return "p-err";
      if (level === "WARNING") return "p-warn";
      return "p-ok";
    }

    async function refresh() {
      const resp = await fetch("/api/status");
      const data = await resp.json();

      document.getElementById("meta").textContent =
        `log=${data.log_path} | file_mtime=${data.file_mtime || "n/a"} | refreshed=${data.now}`;
      document.getElementById("status").textContent =
        data.state.phase || data.state.status || "unknown";
      document.getElementById("market").textContent = data.state.market || "-";
      document.getElementById("orders").textContent = data.state.orders || "-";
      document.getElementById("fillsRecv").textContent =
        `${data.state.fills || "-"} / ${data.state.recv || "-"}`;

      document.getElementById("warn5m").textContent = data.window.warning;
      document.getElementById("err5m").textContent = data.window.error;
      document.getElementById("placed5m").textContent = data.window.placed;
      document.getElementById("filled5m").textContent = data.window.filled;

      document.getElementById("stateLine").textContent = data.state.raw || "(no STATE line yet)";

      const tbody = document.getElementById("events");
      tbody.innerHTML = "";
      for (const ev of data.events) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><span class="pill ${levelClass(ev.level)}">${ev.level}</span></td>
          <td>${ev.ts || ""}</td>
          <td>${ev.msg}</td>
        `;
        tbody.appendChild(tr);
      }
    }

    refresh();
    setInterval(refresh, 2500);
  </script>
</body>
</html>
"""


def _parse_ts(line: str) -> datetime | None:
    head = line[:23]
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%H:%M:%S,%f"):
        try:
            dt = datetime.strptime(head, fmt)
            if fmt == "%H:%M:%S,%f":
                now = datetime.now()
                return dt.replace(year=now.year, month=now.month, day=now.day)
            return dt
        except ValueError:
            continue
    return None


def _read_tail_lines(path: Path, max_lines: int = MAX_TAIL_LINES) -> list[str]:
    if not path.exists():
        return []
    out: deque[str] = deque(maxlen=max_lines)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            out.append(line.rstrip("\n"))
    return list(out)


def _extract_level(line: str) -> str:
    m = re.search(r"\b(INFO|WARNING|ERROR|DEBUG)\b", line)
    return m.group(1) if m else "INFO"


def _parse_state(line: str) -> dict[str, str]:
    state: dict[str, str] = {"raw": line}
    m_market = re.search(r"STATE\s+(\S+)\s+", line)
    if m_market:
        state["market"] = m_market.group(1)
    for key in ("phase", "status", "btc", "up", "dn", "drift", "pair_sum", "buf", "fills"):
        m = re.search(rf"{key}=([^\s]+)", line)
        if m:
            state[key] = m.group(1)
    m_recv = re.search(r"recv=\$([0-9.]+)", line)
    if m_recv:
        state["recv"] = "$" + m_recv.group(1)
    m_orders = re.search(r"orders=\[(.*?)\]", line)
    if m_orders:
        state["orders"] = m_orders.group(1) or "none"
    return state


def _build_status(log_path: Path) -> dict[str, Any]:
    lines = _read_tail_lines(log_path)
    now = datetime.now()
    since = now - timedelta(minutes=5)

    latest_state: dict[str, str] = {}
    events: list[dict[str, str]] = []
    window_counts = {"warning": 0, "error": 0, "filled": 0, "placed": 0, "cancelled": 0}

    for line in lines:
        if STATE_PREFIX in line:
            latest_state = _parse_state(line)

        ts = _parse_ts(line)
        if ts and ts >= since:
            lvl = _extract_level(line)
            if lvl == "WARNING":
                window_counts["warning"] += 1
            elif lvl == "ERROR":
                window_counts["error"] += 1
            if "FILLED" in line:
                window_counts["filled"] += 1
            if "PLACED sell" in line:
                window_counts["placed"] += 1
            if "CANCELLED" in line:
                window_counts["cancelled"] += 1

        if EVENT_PATTERN.search(line):
            msg = re.sub(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+\s+\w+\s+", "", line)
            events.append(
                {
                    "ts": line[:23] if len(line) >= 23 else "",
                    "level": _extract_level(line),
                    "msg": msg,
                }
            )

    file_mtime = None
    if log_path.exists():
        file_mtime = datetime.fromtimestamp(log_path.stat().st_mtime).isoformat(timespec="seconds")

    return {
        "now": now.isoformat(timespec="seconds"),
        "log_path": str(log_path),
        "file_mtime": file_mtime,
        "state": latest_state,
        "window": window_counts,
        "events": events[-35:][::-1],
    }


def create_app(log_path: Path) -> Flask:
    app = Flask(__name__)
    app.config["LOG_PATH"] = log_path

    @app.route("/")
    def index() -> str:
        return render_template_string(_TEMPLATE)

    @app.route("/api/status")
    def api_status() -> Any:
        return jsonify(_build_status(app.config["LOG_PATH"]))

    @app.route("/api/raw")
    def api_raw() -> Any:
        lines = _read_tail_lines(app.config["LOG_PATH"], max_lines=500)
        return jsonify({"log_path": str(app.config["LOG_PATH"]), "lines": lines})

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick live UI for btc_hedge log")
    parser.add_argument("--log", default=str(DEFAULT_LOG_PATH), help="Path to btc_hedge log file")
    parser.add_argument("--host", default="127.0.0.1", help="Host for Flask server")
    parser.add_argument("--port", type=int, default=8787, help="Port for Flask server")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    args = parser.parse_args()

    app = create_app(Path(args.log))
    print(json.dumps({"ui": f"http://{args.host}:{args.port}", "log": str(Path(args.log))}, indent=2))
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

