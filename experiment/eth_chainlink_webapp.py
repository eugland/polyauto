from __future__ import annotations

import argparse
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, render_template_string


ETH_USD_FEED = "0xF9680D99D6C9589e2a93a78A04A279e509205945"
USDT_ETH_FEED = "0xf9d5AAC6E5572AEFa6bd64108ff86a222F69B64d"
DEFAULT_RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ETH Chainlink Stream</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #0b1320;
      --panel: #111d31;
      --text: #e7edf7;
      --muted: #9ab0cd;
      --a: #42a5f5;
      --b: #ffa726;
      --c: #66bb6a;
      --d: #ef5350;
    }
    body {
      margin: 0;
      font-family: Segoe UI, Tahoma, sans-serif;
      background: radial-gradient(circle at 20% 0%, #122038, var(--bg) 50%);
      color: var(--text);
    }
    .wrap { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
    h1 { margin: 0 0 8px; font-size: 24px; }
    .sub { color: var(--muted); margin-bottom: 16px; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    .card {
      background: linear-gradient(165deg, #142744, #0f1b2d);
      border: 1px solid #223856;
      border-radius: 12px;
      padding: 12px;
    }
    .k { color: var(--muted); font-size: 12px; }
    .v { font-size: 24px; margin-top: 4px; }
    .panel {
      background: var(--panel);
      border: 1px solid #203350;
      border-radius: 12px;
      padding: 12px;
      margin-bottom: 12px;
    }
    canvas { width: 100% !important; height: 330px !important; }
    .status { color: var(--muted); font-size: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Polygon Chainlink ETH Stream</h1>
    <div class="sub">Source 1: ETH/USD feed | Source 2: USDT/ETH feed (and derived ETH/USDT)</div>

    <div class="cards">
      <div class="card"><div class="k">ETH/USD</div><div class="v" id="ethUsd">-</div></div>
      <div class="card"><div class="k">USDT/ETH</div><div class="v" id="usdtEth">-</div></div>
      <div class="card"><div class="k">ETH/USDT (derived)</div><div class="v" id="ethUsdt">-</div></div>
      <div class="card"><div class="k">Difference (ETH/USD - ETH/USDT)</div><div class="v" id="diff">-</div></div>
    </div>

    <div class="panel"><canvas id="prices"></canvas></div>
    <div class="panel"><canvas id="spread"></canvas></div>

    <div class="status" id="status">Loading...</div>
    <div class="status" id="sourceTimes"></div>
  </div>

<script>
const maxPoints = 300;
let lastTs = 0;

const pricesChart = new Chart(document.getElementById('prices'), {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      { label: 'ETH/USD', data: [], borderColor: '#42a5f5', tension: 0.2 },
      { label: 'ETH/USDT', data: [], borderColor: '#66bb6a', tension: 0.2 }
    ]
  },
  options: {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    scales: { y: { ticks: { color: '#cfd9ea' } }, x: { ticks: { color: '#cfd9ea' } } },
    plugins: { legend: { labels: { color: '#cfd9ea' } } }
  }
});

const spreadChart = new Chart(document.getElementById('spread'), {
  type: 'line',
  data: {
    labels: [],
    datasets: [{ label: 'Diff (ETH/USD - ETH/USDT)', data: [], borderColor: '#ef5350', tension: 0.2 }]
  },
  options: {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    scales: { y: { ticks: { color: '#cfd9ea' } }, x: { ticks: { color: '#cfd9ea' } } },
    plugins: { legend: { labels: { color: '#cfd9ea' } } }
  }
});

function pushPoint(p) {
  const label = new Date(p.t * 1000).toLocaleTimeString();
  pricesChart.data.labels.push(label);
  spreadChart.data.labels.push(label);
  pricesChart.data.datasets[0].data.push(p.eth_usd);
  pricesChart.data.datasets[1].data.push(p.eth_usdt);
  spreadChart.data.datasets[0].data.push(p.diff);

  while (pricesChart.data.labels.length > maxPoints) {
    pricesChart.data.labels.shift();
    spreadChart.data.labels.shift();
    pricesChart.data.datasets[0].data.shift();
    pricesChart.data.datasets[1].data.shift();
    spreadChart.data.datasets[0].data.shift();
  }
  pricesChart.update();
  spreadChart.update();
}

function updateCards(p) {
  document.getElementById('ethUsd').textContent = p.eth_usd.toFixed(4);
  document.getElementById('usdtEth').textContent = p.usdt_eth.toFixed(8);
  document.getElementById('ethUsdt').textContent = p.eth_usdt.toFixed(4);
  document.getElementById('diff').textContent = p.diff.toFixed(6);
  document.getElementById('sourceTimes').textContent =
    `src1 ETH/USD updated: ${new Date(p.eth_usd_updated_at * 1000).toLocaleString()} | ` +
    `src2 USDT/ETH updated: ${new Date(p.usdt_eth_updated_at * 1000).toLocaleString()}`;
}

async function loadHistory() {
  const r = await fetch('/api/history');
  const j = await r.json();
  if (!j.ok) {
    document.getElementById('status').textContent = `Error: ${j.error || 'history unavailable'}`;
    return;
  }
  for (const p of j.history) {
    pushPoint(p);
    lastTs = Math.max(lastTs, p.t);
  }
  document.getElementById('status').textContent = `Connected RPC: ${j.rpc_url}`;
}

async function tick() {
  try {
    const r = await fetch('/api/latest');
    const j = await r.json();
    if (!j.ok) {
      document.getElementById('status').textContent = `Error: ${j.error || 'fetch failed'}`;
      return;
    }
    document.getElementById('status').textContent = `Connected RPC: ${j.rpc_url} | Last poll: ${new Date().toLocaleTimeString()}`;
    const p = j.latest;
    updateCards(p);
    if (p.t > lastTs) {
      pushPoint(p);
      lastTs = p.t;
    }
  } catch (e) {
    document.getElementById('status').textContent = `Network error: ${e}`;
  }
}

loadHistory().then(() => {
  tick();
  setInterval(tick, 2000);
});
</script>
</body>
</html>
"""


class ChainlinkStreamer:
    def __init__(self, rpc_urls: list[str], poll_interval: float, history: int):
        self.rpc_urls = rpc_urls
        self.poll_interval = poll_interval
        self.history = deque(maxlen=history)
        self.lock = threading.Lock()
        self.latest: dict[str, Any] | None = None
        self.error: str | None = None
        self.rpc_url: str | None = None

        from web3 import Web3

        self.w3 = self._connect(Web3)
        self.eth_usd = self._make_feed(ETH_USD_FEED)
        self.usdt_eth = self._make_feed(USDT_ETH_FEED)

    def _connect(self, Web3: Any):
        errors: list[str] = []
        for url in self.rpc_urls:
            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
                _ = w3.eth.block_number
                self.rpc_url = url
                return w3
            except Exception as exc:
                errors.append(f"{url} -> {exc}")
        raise RuntimeError("No working RPC endpoint. " + " | ".join(errors))

    def _make_feed(self, address: str):
        from web3 import Web3

        c = self.w3.eth.contract(address=Web3.to_checksum_address(address), abi=AGGREGATOR_V3_ABI)
        decimals = int(c.functions.decimals().call())
        return {"contract": c, "decimals": decimals}

    @staticmethod
    def _latest(feed: dict[str, Any]) -> tuple[float, int]:
        _, answer, _, updated_at, _ = feed["contract"].functions.latestRoundData().call()
        if answer <= 0:
            raise RuntimeError("Feed answer is non-positive")
        return float(answer) / (10 ** feed["decimals"]), int(updated_at)

    def poll_once(self) -> None:
        eth_usd, eth_usd_updated_at = self._latest(self.eth_usd)
        usdt_eth, usdt_eth_updated_at = self._latest(self.usdt_eth)
        eth_usdt = 1.0 / usdt_eth
        point = {
            "t": int(time.time()),
            "iso": datetime.now(timezone.utc).isoformat(),
            "eth_usd": eth_usd,
            "usdt_eth": usdt_eth,
            "eth_usdt": eth_usdt,
            "diff": eth_usd - eth_usdt,
            "eth_usd_updated_at": eth_usd_updated_at,
            "usdt_eth_updated_at": usdt_eth_updated_at,
        }
        with self.lock:
            self.latest = point
            self.history.append(point)
            self.error = None

    def run_loop(self) -> None:
        while True:
            try:
                self.poll_once()
            except Exception as exc:
                with self.lock:
                    self.error = str(exc)
            time.sleep(max(0.5, self.poll_interval))


def create_app(streamer: ChainlinkStreamer) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return render_template_string(HTML)

    @app.get("/api/latest")
    def api_latest():
        with streamer.lock:
            if streamer.latest is None:
                return jsonify(ok=False, error=streamer.error or "No data yet")
            return jsonify(ok=True, rpc_url=streamer.rpc_url, latest=streamer.latest)

    @app.get("/api/history")
    def api_history():
        with streamer.lock:
            return jsonify(
                ok=streamer.latest is not None,
                rpc_url=streamer.rpc_url,
                error=streamer.error,
                history=list(streamer.history),
            )

    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Web app: real-time ETH Chainlink feeds on Polygon")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--history", type=int, default=300)
    p.add_argument("--rpc-url", action="append", default=[])
    return p.parse_args()


def main() -> None:
    args = parse_args()

    try:
        import web3  # noqa: F401
    except ImportError as exc:
        raise SystemExit("Missing dependency: web3. Install with: pip install web3") from exc

    rpc_urls = args.rpc_url or DEFAULT_RPC_URLS
    streamer = ChainlinkStreamer(rpc_urls=rpc_urls, poll_interval=args.poll_interval, history=args.history)

    t = threading.Thread(target=streamer.run_loop, daemon=True)
    t.start()

    app = create_app(streamer)
    print(f"Serving web app at http://{args.host}:{args.port}")
    print(f"Using Polygon RPC: {streamer.rpc_url}")
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
