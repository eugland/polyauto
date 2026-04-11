# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Automated betting bots for [Polymarket](https://polymarket.com) prediction markets. Three active strategies:

1. **Weather bot** — bets NO on daily temperature markets (e.g. "Will London record above X°C?") using Open-Meteo/NOAA forecasts vs. the current NO token price.
2. **ETH 1H bot** — tail-capture on ETH Up/Down 1-hour candle markets. Enters when the favored outcome trades ≥ threshold (Brownian Bridge formula: `1 - k/sqrt(mins_remaining)`) with 0–7 minutes left, then holds to resolution (REDEEM_ONLY_MODE).
3. **ETH 15M bot** — same tail-capture idea on 15-minute ETH candles, but posts a GTC sell at 0.999 after entry (not redeem-only).

There is also `automata/btc_reach.py`, a read-only analyzer for BTC daily price-range markets.

`experiment/` contains backtesting, forward-test, and scanning scripts that are standalone (not part of the `automata` package).

4. **Crypto 5m penny scanner** (`experiment/crypto_5m_scanner.py`) — monitors ALL active crypto 5m Up/Down markets (BTC, ETH, XRP, BNB, SOL, DOGE, etc.) and records every signal where either side's ask drops to ≤$0.03. Writes to `experiment/crypto_5m.db`. Companion stats UI at `experiment/crypto_5m_stats.py`.

## Running the Bots

```bash
# Dry-run (no real orders) — both daemons together
python -m automata

# Live betting
python -m automata --bet --eth-max-balance 50 --weather-max-balance 40

# ETH 1H only
python -m automata.eth
python -m automata.eth --bet --once --max-balance 30

# ETH 15M only
python -m automata.eth_15m --bet

# Weather only
python -m automata.weather --bet --interval 60

# BTC daily range analyzer (read-only)
python -m automata.btc_reach

# Bets DB web UI (http://localhost:5050)
python -m automata.view_bets

# Crypto 5m penny scanner — records signals for ALL crypto 5m markets to experiment/crypto_5m.db
python -m experiment.crypto_5m_scanner
python -m experiment.crypto_5m_scanner --max-price 0.03 --poll 5

# Crypto 5m stats UI — http://localhost:5051 (also LAN-accessible, IP shown on startup)
python -m experiment.crypto_5m_stats
python -m experiment.crypto_5m_stats --port 5051
```

## Environment Variables (`.env`)

```
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_HOST=https://clob.polymarket.com
POLYMARKET_FUNDER=0x...         # proxy wallet address
POLYMARKET_SIG_TYPE=0
# Derived from private key (can be auto-populated):
CLOB_API_KEY=...
CLOB_SECRET=...
CLOB_PASS=...
POLYGON_RPC_URL=https://...     # for onchain redeem mode
BET_SIZE_SHARES=20.0
MAX_NO_PRICE=0.998
```

## Architecture

```
automata/
  __main__.py     # Combined runner: weather (main thread) + ETH (background thread)
  client.py       # Thin wrapper over py-clob-client: build_client, place_no_order, etc.
  db.py           # SQLite helpers — init_db(), record_bet(); DB at bets.db (repo root)
  models.py       # Dataclasses: Market, ParsedMarket, BetOrder
  polymarket.py   # Fetches open temperature markets from Gamma API
  parser.py       # Parses market questions into ParsedMarket (threshold, direction, unit)
  weather.py      # Weather data (Open-Meteo, NOAA, METAR coords) + run_weather_daemon()
  weather_bot.py  # Core weather strategy: scan positions, run()
  eth.py          # run_eth_daemon() — orchestrates eth_1h + redeem loop
  eth_1h.py       # ETH 1H tail-capture strategy + self-calibrating k
  eth_15m.py      # ETH 15M tail-capture strategy
  btc_reach.py    # BTC daily range probability analyzer (log-normal model)
  view_bets.py    # Flask app for browsing bets.db

experiment/       # Standalone backtests and scanners (not imported by automata)
  crypto_5m_scanner.py  # Multi-asset 5m penny signal recorder (writes crypto_5m.db)
  crypto_5m_stats.py    # Flask stats UI for crypto_5m.db (port 5051)
  user_analyzer.py      # Ad-hoc Polymarket wallet behavior analyzer
  btc_5m_*.py           # BTC 5m backtests, forward tests, scanners
bets.db           # SQLite database (all placed bets + outcomes)
experiment/crypto_5m.db  # Signal DB written by crypto_5m_scanner
```

**Data flow (ETH 1H):** `eth.py` → `eth_1h.run_eth_1h()` polls Gamma API for the current-hour slug → checks `minutes_remaining` → if in window (0–7 min) and price ≥ `min_bid` → places buy via `client.place_no_order()` → `_settle_resolved_trades()` handles redeem on subsequent cycles.

**Data flow (Weather):** `weather_bot.run()` → `polymarket.fetch_temperature_markets_payload()` → `parser` extracts thresholds → `weather.fetch_open_meteo_high()` for forecast → if forecast strongly favors NO outcome → places order via `client`.

## Key Design Decisions

- **`bets.db` deduplication**: `placed_bets` has `UNIQUE(city, event_date, question)` for weather and similar guards for ETH — prevents double-betting the same market.
- **Self-calibrating k**: `eth_1h.py` and `eth_15m.py` compute `k` from the win/loss record in `bets.db` when ≥ 10 resolved outcomes exist; otherwise fall back to `K_DEFAULT`.
- **REDEEM_ONLY_MODE** (`eth_1h.py`): when `True`, no take-profit sell is placed — position is held to resolution and redeemed via the relayer or onchain flow.
- **CLOB credentials**: derived on demand from `POLYMARKET_PRIVATE_KEY` via `client.derive_api_credentials()`. The daemon calls this once at startup in live mode.
- All monetary amounts are USDC with 6 decimal precision from the API (divide raw integer by `1e6`).
- **`crypto_5m.db` schema**: two tables — `candles` (one row per slug: asset, start/end epoch, token IDs, winner) and `signals` (one row per slug+side: `min_price` = cheapest ask ever seen ≤ $0.03, `pnl` filled after resolution). Stats UI filters signals by `min_price <= threshold` to build ≤1¢ / ≤2¢ / ≤3¢ P/L series.
