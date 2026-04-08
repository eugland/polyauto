# Running This Project

## Prerequisites

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your keys
```

Required `.env` keys (live trading):

| Key | Description |
|---|---|
| `POLYMARKET_HOST` | `https://clob.polymarket.com` |
| `POLYMARKET_PRIVATE_KEY` | Your wallet private key |
| `POLYMARKET_FUNDER` | Proxy wallet address (if using one) |
| `POLYMARKET_SIG_TYPE` | `0` (EOA) or `1` (proxy) |
| `MIN_NO_PRICE` | Minimum No ask to consider (default `0.95`) |
| `MAX_NO_PRICE` | Maximum No ask to consider (default `0.998`) |
| `BET_SIZE_SHARES` | Shares per bet (default `10.0`) |
| `CITY_BLACKLIST` | Comma-separated cities to skip (optional) |
| `TAKE_PROFIT_PRICE` | Sell trigger for open positions (default `0.999`) |

---

## Automata (betting engine)

Backend app was removed. Current runnable modules are under `automata/`.

Dry run (no real orders):

```bash
python -m automata.main
```

Live betting (real orders):

```bash
python -m automata.main --bet
```

Runs in a loop (60 s between iterations).

---

## Other modules

BTC daily range analyzer:

```bash
python -m automata.btc_reach
```

ETH 1H analyzer:

```bash
python -m automata.eth_1h
```

Bets database web UI:

```bash
python -m automata.view_bets
```

Open: `http://localhost:5050`

---

## docs/ folder (static files)

`docs/` contains static HTML/CSS/JS only.

Preview locally:

```bash
python -m http.server 8000 --directory docs
```

Open: `http://localhost:8000`
