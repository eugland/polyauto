"""
Live stdout tail for the automata.eth daemon.

Usage:
  1. Start the daemon with stdout redirected to a log file:
       python -m automata.eth --bet > eth.log 2>&1
  2. Start this UI:
       python -m automata.eth_ui
  3. Open http://localhost:5052

Options:
  --log PATH    Path to the log file (default: ./eth.log at repo root)
  --port N      HTTP port (default: 5052)
  --poll MS     Browser poll interval in ms (default: 1000)
"""
from __future__ import annotations

import argparse
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request

REPO_ROOT = Path(__file__).resolve().parent.parent

app = Flask(__name__)
_CONFIG = {
    "log_path": REPO_ROOT / "eth.log",
    "poll_ms": 1000,
}


TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ETH daemon log</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; }
  body {
    background: #000; color: #d1fae5;
    font-family: ui-monospace, "Cascadia Mono", Menlo, Consolas, monospace;
    font-size: 18px; line-height: 1.35;
    display: flex; flex-direction: column;
  }
  header {
    padding: 10px 18px; background: #052e16; color: #6ee7b7;
    display: flex; justify-content: space-between; align-items: center;
    border-bottom: 1px solid #065f46; flex: 0 0 auto;
  }
  header .path { font-size: 13px; color: #34d399; opacity: .8; }
  header .status { font-size: 13px; }
  header .status.live  { color: #4ade80; }
  header .status.stale { color: #fbbf24; }
  header .status.gone  { color: #f87171; }
  header label { font-size: 12px; color: #a7f3d0; margin-left: 12px; }
  header input[type=checkbox] { vertical-align: middle; }
  main {
    flex: 1 1 auto; overflow-y: auto;
    padding: 14px 18px 24px; white-space: pre-wrap; word-break: break-word;
  }
  /* crude line-level highlights */
  .line.buy    { color: #fde68a; }
  .line.sell   { color: #67e8f9; }
  .line.win    { color: #4ade80; font-weight: 600; }
  .line.loss   { color: #f87171; font-weight: 600; }
  .line.err    { color: #f87171; }
  .line.redeem { color: #c4b5fd; }
  .line.hold   { color: #94a3b8; }
</style>
</head>
<body>
<header>
  <div>
    <b>ETH daemon log</b>
    <span class="path">{{ log_path }}</span>
  </div>
  <div>
    <span id="status" class="status live">connecting...</span>
    <label><input id="autoscroll" type="checkbox" checked> auto-scroll</label>
    <label><input id="pause" type="checkbox"> pause</label>
  </div>
</header>
<main id="log"></main>

<script>
const POLL_MS = {{ poll_ms }};
const logEl = document.getElementById("log");
const statusEl = document.getElementById("status");
const autoscrollEl = document.getElementById("autoscroll");
const pauseEl = document.getElementById("pause");
let offset = 0;
let lastReceivedAt = Date.now();

function classify(line) {
  const l = line.toLowerCase();
  if (/\\berror\\b|\\btraceback\\b|\\bexception\\b/.test(l)) return "err";
  if (/\\bredeem|settle|resolved/.test(l)) return "redeem";
  if (/\\bwin\\b|\\bfilled\\b|\\bentry\\b/.test(l)) return "win";
  if (/\\bloss\\b|\\bmissed\\b|\\bskipp/.test(l)) return "loss";
  if (/\\bbuy\\b|\\bplaced\\b/.test(l)) return "buy";
  if (/\\bsell\\b|\\btp\\b|take.profit/.test(l)) return "sell";
  if (/\\bhold\\b/.test(l)) return "hold";
  return "";
}

function append(content) {
  if (!content) return;
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 8;
  const lines = content.split(/\\r?\\n/);
  const frag = document.createDocumentFragment();
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (i === lines.length - 1 && raw === "") continue;  // trailing newline
    const div = document.createElement("div");
    const cls = classify(raw);
    div.className = "line" + (cls ? " " + cls : "");
    div.textContent = raw;
    frag.appendChild(div);
  }
  logEl.appendChild(frag);
  if (autoscrollEl.checked && atBottom) {
    logEl.scrollTop = logEl.scrollHeight;
  }
}

async function poll() {
  if (pauseEl.checked) { scheduleNext(); return; }
  try {
    const resp = await fetch("/tail?offset=" + offset);
    const data = await resp.json();
    if (data.reset) {
      logEl.innerHTML = "";
      offset = 0;
    }
    if (data.missing) {
      statusEl.className = "status gone";
      statusEl.textContent = "log file not found";
    } else {
      offset = data.offset;
      if (data.content) {
        append(data.content);
        lastReceivedAt = Date.now();
      }
      const secsSince = Math.round((Date.now() - lastReceivedAt) / 1000);
      if (secsSince < 30) {
        statusEl.className = "status live";
        statusEl.textContent = "live (" + offset + " bytes)";
      } else {
        statusEl.className = "status stale";
        statusEl.textContent = "stale " + secsSince + "s (" + offset + " bytes)";
      }
    }
  } catch (e) {
    statusEl.className = "status gone";
    statusEl.textContent = "fetch error";
  }
  scheduleNext();
}

function scheduleNext() { setTimeout(poll, POLL_MS); }
poll();
</script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(
        TEMPLATE,
        log_path=str(_CONFIG["log_path"]),
        poll_ms=_CONFIG["poll_ms"],
    )


@app.get("/tail")
def tail():
    offset = request.args.get("offset", default=0, type=int)
    path: Path = _CONFIG["log_path"]
    if not path.exists():
        return jsonify({
            "content": "", "offset": 0, "missing": True, "reset": False,
        })
    size = path.stat().st_size
    reset = False
    if offset > size:
        # Log was rotated or truncated — start fresh
        offset = 0
        reset = True
    with path.open("rb") as f:
        f.seek(offset)
        data = f.read()
    return jsonify({
        "content": data.decode("utf-8", errors="replace"),
        "offset": size,
        "missing": False,
        "reset": reset,
    })


def main() -> None:
    ap = argparse.ArgumentParser(description="Live stdout tail for automata.eth")
    ap.add_argument("--log", type=str, default=str(REPO_ROOT / "eth.log"),
                    help="Path to the log file (default: ./eth.log)")
    ap.add_argument("--port", type=int, default=5052, help="HTTP port (default: 5052)")
    ap.add_argument("--poll", type=int, default=1000, help="Browser poll interval in ms (default: 1000)")
    args = ap.parse_args()
    _CONFIG["log_path"] = Path(args.log).expanduser().resolve()
    _CONFIG["poll_ms"] = max(200, args.poll)
    print(f"Tailing {_CONFIG['log_path']}")
    print(f"Serving at http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
