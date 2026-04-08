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
