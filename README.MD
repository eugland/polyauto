# PythonProject

## Folder quick map

- `automata/`: betting engine modules (`weather`, `eth`, `btc_reach`, `eth_1h`, `view_bets`).
- `experiment/`: standalone scanners, backtests, and analysis scripts (no live orders).
- `docs/`: static docs/demo files (`index.html`, `app.js`, `styles.css`, `running.md`).
- `.venv/`: local virtual environment.
- `.idea/`: IDE project settings.
- `.claude/`: local agent/tool metadata.

## Setup

```bash
pip install -r requirements.txt
```

## How to run (with examples)

- Combined loop (dry run):

```bash
python -m automata
```

- Combined loop (live orders):

```bash
python -m automata --bet
```

- Combined loop with per-daemon spend caps:

```bash
python -m automata --bet --weather-max-balance 40 --eth-max-balance 20
```

- Weather daemon only:

```bash
python -m automata.weather --bet --once --max-balance 30
```

- ETH daemon only:

```bash
python -m automata.eth --bet --once --max-balance 30
```

- BTC range analyzer:

```bash
python -m automata.btc_reach
```

- ETH 1H analyzer:

```bash
python -m automata.eth_1h
```

- Bets UI:

```bash
python -m automata.view_bets
```

Open: `http://localhost:5050`

## Crypto 5m penny scanner (experiment)

Records signals across **all** active crypto 5m Up/Down markets (BTC, ETH, XRP, BNB, SOL, DOGE, and any others listed). No orders are placed — paper-only signal recorder.

**Step 1 — start the scanner** (runs continuously, Ctrl+C to stop):

```bash
python -m experiment.crypto_5m_scanner
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--max-price` | `0.03` | Record signals when ask ≤ this price ($0.01, $0.02, or $0.03) |
| `--poll` | `5` | Seconds between order-book polls |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

**Step 2 — open the stats UI** (in a separate terminal):

```bash
python -m experiment.crypto_5m_stats
```

Opens at `http://localhost:5051` — also accessible from any device on your LAN (IP printed on startup).

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `5051` | HTTP port |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

**What the stats UI shows:**

- Tab per asset (BTC, ETH, XRP, etc.)
- **Cumulative P/L chart** with three series per asset:
  - ≤1¢ — only signals where the cheapest ask was $0.01
  - ≤2¢ — signals at $0.01 or $0.02
  - ≤3¢ — signals at $0.01, $0.02, or $0.03
- Per-tier stats: win rate, net P/L, ROI, resolved vs pending count
- Auto-refreshes every 30 seconds

**How signals work:**

One signal row per (candle, side). The scanner records the *cheapest ask ever seen* for that side during the candle (`min_price`). After the candle closes, the winner is fetched from Polymarket and P/L is written: `(1 - min_price)` if won, `(-min_price)` if lost. The chart assumes 1 share per signal.

## Parameters

- `automata`: `--bet`, `--weather-interval`, `--eth-interval`, `--weather-max-balance`, `--eth-max-balance`
- `automata.weather`: `--bet`, `--interval`, `--once`, `--max-balance`
- `automata.eth`: `--bet`, `--interval`, `--once`, `--max-balance`
