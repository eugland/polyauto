# automata

Runnable modules and quick usage.

## 1) `automata.main` (main bot loop)

Run:

```bash
python -m automata.main
```

What it does:
- Runs the weather betting loop.
- Scans/maintains positions.
- Runs ETH 1H strategy in a background thread.

Flag:

| Flag | Description |
|---|---|
| `--bet` | Place real on-chain orders. Without this flag, it is dry-run only. |

Example:

```bash
python -m automata.main --bet
```

Environment:
- Required for live mode: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_HOST`, `CLOB_API_KEY`, `CLOB_SECRET`, `CLOB_PASS`
- Optional: `POLYMARKET_FUNDER`, `POLYMARKET_SIG_TYPE`, `MIN_NO_PRICE`, `MAX_NO_PRICE`, `BET_THRESHOLD`, `BET_SIZE_SHARES`, `MM_TICK_SIZE`, `MM_JOIN_BID_TICKS`, `MM_REPRICE_CENTS`, `CITY_BLACKLIST`

## 2) `automata.btc_reach` (BTC daily range analyzer)

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

## 3) `automata.eth_1h` (ETH 1H analyzer)

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

## 4) `automata.view_bets` (bets DB web view)

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
