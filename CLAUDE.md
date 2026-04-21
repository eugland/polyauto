# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Automated betting bots for [Polymarket](https://polymarket.com) prediction markets. Two active strategies:

1. **Weather bot** — bets NO on daily temperature markets (e.g. "Will London record above X°C?") using Open-Meteo/NOAA forecasts vs. the current NO token price.
2. **ETH 1H bot** — tail-capture on ETH Up/Down 1-hour candle markets. Enters when the favored outcome trades ≥ threshold (Brownian Bridge formula: `1 - k/sqrt(mins_remaining)`) with 0–7 minutes left, then holds to resolution (REDEEM_ONLY_MODE).

A self-tuning forecast-bias model (`automata/weather_model.py`) records every weather scan + backfills resolved outcomes from Polymarket, and the `stock` UI exposes the per-city scan timeline, residuals, and calibration at `/weather-model` (port 8081).

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

# Highest-temp collector only (no UI — tempscanner UI was removed, use `stock` UI instead)
python -m experiment.temp_market_collector
python -m experiment.temp_market_collector --poll 5 --once

# Stock dashboard + Weather Model UI (http://localhost:8081)
python -m stock --port 8081
# Weather Model page: http://localhost:8081/weather-model
```

## Configuration

Split across two files:

- **`.env`** (gitignored) — secrets and endpoints only:
  ```
  POLYMARKET_PRIVATE_KEY=0x...
  POLYMARKET_FUNDER=0x...            # proxy wallet address
  POLYMARKET_HOST=https://clob.polymarket.com
  RELAYER_API_KEY=...
  RELAYER_API_KEY_ADDRESS=0x...
  POLYGON_RPC_URL=https://...        # for onchain redeem mode
  # CLOB_API_KEY / CLOB_SECRET / CLOB_PASS are derived at startup
  ```

- **`config.toml`** (git-committed) — every tunable setting. Organized into
  `[polymarket]`, `[weather]`, `[eth_1h]`, `[gamma]`. This is where
  `city_blacklist`, `min_no_price`, `bet_size_shares`, `sell_target`,
  `redeem_only_mode`, etc. live.

Runtime precedence at each call site: **env var > `config.toml` > hardcoded default**.
So you can still override any value with an env var for one-off runs, e.g.
`BET_SIZE_SHARES=5 python -m automata.weather --bet`.

All getters go through `automata/config.py` (`get_str`, `get_int`, `get_float`,
`get_bool`, `get_list_str`). Add a new tunable by: (a) adding a line in
`config.toml`, (b) calling `config.get_*(ENV_NAME, section, key, default)`
at the usage site.

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
  weather_model.py # Self-tuning forecast-bias model: scans → bias/sigma EMA → fair NO prob
  eth.py          # run_eth_daemon() — orchestrates eth_1h + redeem loop
  eth_1h.py       # ETH 1H tail-capture strategy + self-calibrating k

experiment/
  temp_market_collector.py  # Standalone highest-temp collector (writes db/temp_market.db)

stock/                      # Stock + weather dashboard (Flask, port 8081)
  app.py                    # Routes including /weather-model, /api/weather-model/*
  templates/
    index.html
    weather_log.html
    weather_pro.html
    weather_model.html      # Per-city scan timeline, residuals, calibration

db/               # All database files (gitignored, created on first run)
  bets.db         # SQLite — weather + ETH 1H placed bets + outcomes
  temp_market.db  # DuckDB — standalone temp_market_collector snapshots
  weather_model.db # DuckDB — scans, outcomes, per-station bias + sigma (self-tuning)
  stock.db        # DuckDB — stock dashboard data
logs/             # All log files (gitignored, created on first run)
  eth_1h.log
  automata.log
```

**Data flow (ETH 1H):** `eth.py` → `eth_1h.run_eth_1h()` polls Gamma API for the current-hour slug → checks `minutes_remaining` → if in window (0–7 min) and price ≥ `min_bid` → places buy via `client.place_no_order()` → `_settle_resolved_trades()` handles redeem on subsequent cycles.

**Data flow (Weather):** `weather_bot.run()` → `polymarket.fetch_temperature_markets_payload()` → `parser` extracts thresholds → `weather.fetch_open_meteo_high()` for forecast → if forecast strongly favors NO outcome → places order via `client`.

**Data flow (Weather Model):** every `weather_bot.run()` cycle → `weather_model.record_scan_batch()` writes one row per candidate market (Open-Meteo, NOAA, METAR current + max-so-far, NO/YES bid+ask, computed μ̂ = forecast + bias, σ̂ from lead-time bucket, fair NO prob, edge vs market) → every ~10 iterations `backfill_outcomes_from_polymarket()` finds closed events, reads the YES-winning bucket midpoint, writes to `outcomes` → `update_from_outcome()` back-propagates residual into per-(station, source) bias EMA and per-(station, lead-bucket) σ EMA (α=0.10, warm-up 1/n for first 10 samples). Stock UI at `:8081/weather-model` reads DuckDB read-only.

**Model math:** `actual_max ~ N(forecast + bias[icao, source], sigma[icao, lead_bucket])`; `fair_no_prob = Phi((threshold - mu) / sigma)` for "higher" markets, complement for "below" / range-out. Priors: bias=0, σ=1.5–4.0° depending on lead bucket. Shadow-mode only for v1 — scans are recorded and logged with edge vs market, but bet selection still uses the old `min_no_price` gate.

## Key Design Decisions

- **`bets.db` deduplication**: `placed_bets` has `UNIQUE(city, event_date, question)` for weather and similar guards for ETH — prevents double-betting the same market.
- **Self-calibrating k**: `eth_1h.py` computes `k` from the win/loss record in `bets.db` when ≥ 10 resolved outcomes exist; otherwise falls back to `K_DEFAULT`.
- **REDEEM_ONLY_MODE** (`eth_1h.py`): when `True`, no take-profit sell is placed — position is held to resolution and redeemed via the relayer or onchain flow.
- **CLOB credentials**: derived on demand from `POLYMARKET_PRIVATE_KEY` via `client.derive_api_credentials()`. The daemon calls this once at startup in live mode.
- All monetary amounts are USDC with 6 decimal precision from the API (divide raw integer by `1e6`).
