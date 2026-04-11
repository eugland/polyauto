# automata

Runnable modules and quick usage.

## 1) `automata.weather` (weather betting daemon)

Run:

```bash
python -m automata.weather
```

What it does:
- Runs the weather betting loop.
- Scans/maintains positions.

Flags:

| Flag | Description |
|---|---|
| `--bet` | Place real on-chain orders. Without this flag, it is dry-run only. |
| `--interval` | Loop interval in seconds (default `60`). |
| `--once` | Run one cycle, place at most one order, then exit. |
| `--max-balance` | Max USDC this process is allowed to spend. |

Example:

```bash
python -m automata.weather --bet
```

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST` (CLOB creds are derived at startup)
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`, `MIN_NO_PRICE`, `MAX_NO_PRICE`, `BET_THRESHOLD`, `BET_SIZE_SHARES`, `MM_TICK_SIZE`, `MM_JOIN_BID_TICKS`, `MM_REPRICE_CENTS`, `CITY_BLACKLIST`

## 2) `automata.eth` (ETH 1H betting daemon)

Run:

```bash
python -m automata.eth
```

What it does:
- Runs ETH 1H strategy in a daemon loop.
- In `--bet` mode, places live orders.

Flags:

| Flag | Description |
|---|---|
| `--bet` | Place real on-chain orders. Without this flag, it is dry-run only. |
| `--interval` | Loop interval in seconds (default `10`). |
| `--once` | Run one cycle, then exit. |
| `--max-balance` | Max USDC this process is allowed to spend. |

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST` (CLOB creds are derived at startup)
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`

## 3) `automata.btc_reach` (BTC daily range analyzer)

Run:

```bash
python -m automata.btc_reach
```

What it does:
- One-shot BTC range analysis.
- Prints model probabilities vs market prices.
- Prints YES/NO edge and momentum-based bet suggestion.

Flags:
- None

Environment:
- Optional: `POLYMARKET_HOST` (default: `https://clob.polymarket.com`)

## 4) `automata.eth_1h` (ETH 1H analyzer)

Run:

```bash
python -m automata.eth_1h
```

What it does:
- One-shot ETH 1H market analysis.
- Shows time remaining, bid/ask, threshold, and active signal.
- Does not place orders by itself.

Flags:
- None

Environment:
- Optional: `POLYMARKET_HOST` (default: `https://clob.polymarket.com`)

## 5) `automata.view_bets` (bets DB web view)

Run:

```bash
python -m automata.view_bets
```

What it does:
- Starts a Flask UI for `bets.db`.
- Includes filters and inline edits.
- Exposes `GET /api/bets` JSON endpoint.

Flags:
- None

Default URL:
- `http://localhost:5050`

## 7) `experiment.crypto_5m_scanner` (multi-asset 5m penny scanner)

Run in one terminal:

```bash
python -m experiment.crypto_5m_scanner
```

What it does:
- Discovers all active `*-updown-5m-*` markets on Polymarket (BTC, ETH, XRP, BNB, SOL, DOGE, and any new assets) automatically.
- Every 5 seconds, fetches all order books in a single bulk request.
- When either side's best ask ≤ `--max-price` ($0.03 by default), records a signal row to `experiment/crypto_5m.db`.
- Tracks the **cheapest ask ever seen** per (candle, side) — if the price drops further, the row is updated.
- After each candle closes, resolves the winner from Polymarket and writes P/L to the signal row.
- No orders are placed — purely a paper/observation recorder.

Flags:

| Flag | Default | Description |
|---|---|---|
| `--max-price` | `0.03` | Max ask to record ($0.01 / $0.02 / $0.03) |
| `--poll` | `5` | Seconds between order-book polls |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

DB schema:
- `candles` — one row per slug: `asset`, `candle_start`, `candle_end`, `up_token`, `down_token`, `winner`
- `signals` — one row per (slug, side): `min_price`, `secs_remaining`, `won`, `pnl`

---

## 8) `experiment.crypto_5m_stats` (penny signal stats UI)

Run in a second terminal:

```bash
python -m experiment.crypto_5m_stats
```

What it does:
- Flask web app that reads `experiment/crypto_5m.db` and shows interactive charts.
- Available at `http://localhost:5051` and on your LAN (IP shown at startup).
- Auto-refreshes every 30 seconds.

Charts — per asset tab:
- **Cumulative P/L line chart** with three series:
  - ≤1¢ (red) — signals where `min_price ≤ $0.01`
  - ≤2¢ (yellow) — signals where `min_price ≤ $0.02`
  - ≤3¢ (green) — signals where `min_price ≤ $0.03`
- Each series is cumulative and assumes 1 share per signal entry.
- **Stats boxes** per tier: win rate, W/L count, net P/L, ROI, resolved vs pending.

Flags:

| Flag | Default | Description |
|---|---|---|
| `--port` | `5051` | HTTP port |
| `--host` | `0.0.0.0` | Bind address (0.0.0.0 = all interfaces = LAN-accessible) |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

---

## 6) `automata` (combined runner)

Run:

```bash
python -m automata
```

What it does:
- Runs weather daemon logic and ETH daemon logic together in one process.

Flags:

| Flag | Description |
|---|---|
| `--bet` | Enable live orders for both daemons. |
| `--weather-interval` | Weather loop interval in seconds (default `60`). |
| `--eth-interval` | ETH loop interval in seconds (default `10`). |
| `--weather-max-balance` | Max USDC weather daemon can spend. |
| `--eth-max-balance` | Max USDC ETH daemon can spend. |
