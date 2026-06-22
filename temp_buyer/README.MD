# temp_buyer

Runnable modules and quick usage.

## 1) `temp_buyer.weather` (weather betting daemon)

Run:

```bash
python -m temp_buyer.weather
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
python -m temp_buyer.weather --bet
```

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST` (CLOB creds are derived at startup)
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`, `MIN_NO_PRICE`, `MAX_NO_PRICE`, `BET_THRESHOLD`, `BET_SIZE_SHARES`, `MM_TICK_SIZE`, `MM_JOIN_BID_TICKS`, `MM_REPRICE_CENTS`, `CITY_BLACKLIST`

## 2) `temp_buyer.eth` (ETH 1H betting daemon)

Run:

```bash
python -m temp_buyer.eth
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

## 3) `temp_buyer.btc_reach` (BTC daily range analyzer)

Run:

```bash
python -m temp_buyer.btc_reach
```

What it does:
- One-shot BTC range analysis.
- Prints model probabilities vs market prices.
- Prints YES/NO edge and momentum-based bet suggestion.

Flags:
- None

Environment:
- Optional: `POLYMARKET_HOST` (default: `https://clob.polymarket.com`)

## 4) `temp_buyer.eth_1h` (ETH 1H analyzer)

Run:

```bash
python -m temp_buyer.eth_1h
```

What it does:
- One-shot ETH 1H market analysis.
- Shows time remaining, bid/ask, threshold, and active signal.
- Does not place orders by itself.

Flags:
- None

Environment:
- Optional: `POLYMARKET_HOST` (default: `https://clob.polymarket.com`)

## 5) `temp_buyer.view_bets` (bets DB web view)

Run:

```bash
python -m temp_buyer.view_bets
```

What it does:
- Starts a Flask UI for `bets.db`.
- Includes filters and inline edits.
- Exposes `GET /api/bets` JSON endpoint.

Flags:
- None

Default URL:
- `http://localhost:5050`

## 6) `temp_buyer.eth_5min` (ETH 5m scanner/trader)

Run:

```bash
python -m temp_buyer.eth_5min
python -m temp_buyer.eth_5min --bet
```

What it does:
- ETH-only 5m strategy (scanner-style BS edge logic from `crypto_5m_scanner`).
- Finds the active `eth-updown-5m-*` market and evaluates both sides each poll.
- Uses a **single limit buy order** per `(slug, side)`:
  maker-leaning price selection but `post_only=false` so it can execute as maker or taker.
- Applies a taker-fee-aware guard via `--min-net-edge-taker`.
- Writes attempts/orders to `eth_5min.db`.

Flags:

| Flag | Description |
|---|---|
| `--bet` | Place real orders. Without this flag, it is dry-run only. |
| `--once` | Run one evaluation cycle then exit. |
| `--poll` | Seconds between evaluations (default `1`). |
| `--shares` | Target shares per signal (default `5`). |
| `--min-edge` | Minimum BS edge (default `0.03`). |
| `--max-edge` | Edge cap to avoid model-failure outliers (default `0.12`). |
| `--max-price` | Skip entries above this price. |
| `--min-net-edge-taker` | Minimum edge after worst-case taker fee. |
| `--maker-offset` | Bid improvement amount for maker-leaning submit price. |
| `--tick-size` | Tick-size rounding for submit prices. |
| `--max-balance` | Max USDC this process is allowed to spend. |

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST`
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`
- Derived automatically at startup in `--bet` mode: `CLOB_API_KEY`, `CLOB_SECRET`, `CLOB_PASS`

## 7) `temp_buyer.eth3cents` (ETH 5m dual 3c orders)

Run:

```bash
python -m temp_buyer.eth3cents
python -m temp_buyer.eth3cents --bet
```

What it does:
- At the start of each ETH 5m market, submits two limit buys:
  - `Up` at configured price (default `0.03`)
  - `Down` at configured price (default `0.03`)
- Near expiry, cancels remaining open orders (default `10s` before end).
- Records all attempts/orders to `eth3cents.db`.

Flags:

| Flag | Description |
|---|---|
| `--bet` | Place real orders. Without this flag, it is dry-run only. |
| `--poll` | Seconds between evaluations (default `0.5`). |
| `--shares` | Target shares per signal (default `5`). |
| `--price` | Limit buy price for both Up and Down (default `0.03`). |
| `--cancel-before-sec` | Cancel open orders this many seconds before close (default `10`). |
| `--max-balance` | Max USDC this process is allowed to spend. |

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST`
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`
- Derived automatically at startup in `--bet` mode: `CLOB_API_KEY`, `CLOB_SECRET`, `CLOB_PASS`

## 10) `temp_buyer` (combined runner)

Run:

```bash
python -m temp_buyer
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
