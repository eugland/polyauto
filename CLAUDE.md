# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Automated betting bots for [Polymarket](https://polymarket.com) prediction markets. Two active strategies:

1. **Weather bot** — bets NO on daily temperature markets (e.g. "Will London record above X°C?") using Open-Meteo/NOAA forecasts vs. the current NO token price.
2. **ETH 1H bot** — tail-capture on ETH Up/Down 1-hour candle markets. Enters when the favored outcome trades ≥ threshold (Brownian Bridge formula: `1 - k/sqrt(mins_remaining)`) with 0–7 minutes left, then holds to resolution (REDEEM_ONLY_MODE).

`experiment/tempscanner` collects highest-temperature bucket markets overnight and provides a strategy UI.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your keys
```

## Running the Bots

```bash
# ETH 1H bot
python -m automata.eth
python -m automata.eth --bet --once --max-balance 30

# Weather bot
python -m automata.weather --bet --interval 60

# Overnight highest-temp market scanner + web UI (http://localhost:5055)
python -m experiment.tempscanner
python -m experiment.tempscanner --poll 5 --port 5055

# Highest-temp collector only (no UI)
python -m experiment.temp_market_collector
python -m experiment.temp_market_collector --poll 5 --once
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

Optional weather-only env vars: `MIN_NO_PRICE`, `BET_THRESHOLD`, `MM_TICK_SIZE`, `MM_JOIN_BID_TICKS`, `MM_REPRICE_CENTS`, `CITY_BLACKLIST`, `TAKE_PROFIT_PRICE`.

## Architecture

```
automata/
  client.py       # Thin wrapper over py-clob-client: build_client, place_no_order, etc.
  db.py           # SQLite helpers — init_db(), record_bet(); DB at bets.db (repo root)
  models.py       # Dataclasses: Market, ParsedMarket, BetOrder
  polymarket.py   # Fetches open temperature markets from Gamma API
  parser.py       # Parses market questions into ParsedMarket (threshold, direction, unit)
  weather.py      # Weather data (Open-Meteo, NOAA, METAR coords) + run_weather_daemon()
  weather_bot.py  # Core weather strategy: scan positions, run()
  eth.py          # run_eth_daemon() — orchestrates eth_1h + redeem loop
  eth_1h.py       # ETH 1H tail-capture strategy + self-calibrating k

experiment/
  temp_market_collector.py  # Standalone highest-temp collector (writes experiment/temp_market.db)
  tempscanner/              # Self-contained scanner + web UI package
    collector.py            # Gamma discovery, weather fetches, DuckDB writes
    database.py             # Read-only queries for the UI
    app.py                  # Flask routes
    templates/index.html
    static/
    data.db                 # DuckDB (created on first run)

bets.db           # SQLite DB (weather + ETH 1H placed bets + outcomes)
```

**Data flow (ETH 1H):** `eth.py` → `eth_1h.run_eth_1h()` polls Gamma API for the current-hour slug → checks `minutes_remaining` → if in window (0–7 min) and price ≥ `min_bid` → places buy via `client.place_no_order()` → `_settle_resolved_trades()` handles redeem on subsequent cycles.

**Data flow (Weather):** `weather_bot.run()` → `polymarket.fetch_temperature_markets_payload()` → `parser` extracts thresholds → `weather.fetch_open_meteo_high()` for forecast → if forecast strongly favors NO outcome → places order via `client`.

**Data flow (Temp Scanner):** `tempscanner/collector.py` → Gamma events tagged `temperature` filtered to `highest-temperature-in-*` slugs → bid/ask per bucket from Gamma embedded prices → METAR + Open-Meteo weather → DuckDB. Flask UI at `:5055` reads DuckDB read-only.

## Key Design Decisions

- **`bets.db` deduplication**: `placed_bets` has `UNIQUE(city, event_date, question)` for weather and similar guards for ETH — prevents double-betting the same market.
- **Self-calibrating k**: `eth_1h.py` computes `k` from the win/loss record in `bets.db` when ≥ 10 resolved outcomes exist; otherwise falls back to `K_DEFAULT`.
- **REDEEM_ONLY_MODE** (`eth_1h.py`): when `True`, no take-profit sell is placed — position is held to resolution and redeemed via the relayer or onchain flow.
- **CLOB credentials**: derived on demand from `POLYMARKET_PRIVATE_KEY` via `client.derive_api_credentials()`. The daemon calls this once at startup in live mode.
- All monetary amounts are USDC with 6 decimal precision from the API (divide raw integer by `1e6`).
