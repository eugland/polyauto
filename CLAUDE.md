# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Automated betting bots for [Polymarket](https://polymarket.com) temperature markets. Two strategies, both in the `temp_buyer/` package:

1. **Temperature NO-buyer** (`temp_buyer.temp_buyer`) — for each "Highest Temperature in <city>" event resolving in the next 4–18h, buys NO on the lowest-temp bucket sitting ≥ `min_bucket_distance` ladder steps below the YES-favorite, when its NO ask is in the configured band. Holds to resolution and redeems on-chain after each live cycle (relayer by default).
2. **Weather bot** (`temp_buyer.weather`) — bets NO on daily high-temperature markets using Open-Meteo / NOAA / METAR forecasts vs. the live NO price.

A self-tuning forecast-bias model (`temp_buyer/weather_model.py`) records every scan and backfills resolved outcomes from Polymarket into DuckDB (`db/weather_model.db`).

## Setup

```bash
pip install -r requirements.txt
# Create .env (gitignored) with the secrets listed under Configuration.
```

## Running the Bots

```bash
# Temperature NO-buyer (dry-run by default; --bet to go live)
python -m temp_buyer
python -m temp_buyer --bet --once

# Weather bot
python -m temp_buyer.weather --bet --interval 60

# Price viewer for one event slug/URL (no args = all temp events resolving <24h)
python -m temp_buyer.view <slug-or-url>
```

## Dashboard (web app)

Flask SPA under `dashboard/` (`python -m dashboard`) — Positions, Pros, Markets,
Backtester tabs. Serves on `[dashboard].port` (default 8000).

**Runs long-lived in a tmux session** named `web`, pane `web:0.0`, in the
foreground (werkzeug + balance poller log there). It runs the venv interpreter:
`.venv/bin/python -m dashboard`.

Restart after editing any `dashboard/*.py` (no reloader unless `--debug`):

```bash
tmux send-keys -t web:0.0 C-c                              # stop
# wait for :8000 to free, then:
tmux send-keys -t web:0.0 '.venv/bin/python -m dashboard' Enter
curl -s localhost:8000/api/health                          # {"ok":true}
```

`.js`/`.html`/`.css` edits need only a browser hard-refresh, not a restart.
Other tmux sessions on this box: `cloudflared`, `weather`, `xmr-22` — leave them.

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

- **`config.toml`** (git-committed) — every tunable. Sections: `[polymarket]`,
  `[weather]`, `[temp_buyer]`, `[gamma]`. Holds `city_blacklist`, `min_no_price`,
  `min_no_ask`, `bet_shares`, `min_bucket_distance`, etc.

Runtime precedence at each call site: **env var > `config.toml` > hardcoded
default**, e.g. `TEMPBUY_BET_SHARES=5 python -m temp_buyer --bet`.

All getters go through `temp_buyer/config.py` (`get_str`, `get_int`, `get_float`,
`get_bool`, `get_list_str`). Add a tunable by: (a) adding a line in `config.toml`,
(b) calling `config.get_*(ENV_NAME, section, key, default)` at the usage site.

## Architecture

```
temp_buyer/
  temp_buyer.py    # NO-buyer strategy + CLI (runnable); post-cycle redeem of resolved positions
  weather.py       # Weather bot daemon (run_weather_daemon) + weather data helpers (runnable)
  view.py          # CLI price viewer + fetch_events_resolving_within() event source (runnable)
  weather_model.py # Self-tuning forecast-bias model: scans → bias/σ EMA → fair NO prob (DuckDB)
  weather_scan.py  # Reusable scan recorder feeding weather_model
  polymarket.py    # Fetches open/closed temperature events from the Gamma API
  parser.py        # Parses market questions into threshold / direction / unit; token-id extractors
  client.py        # py-clob-client-v2 wrapper: build_client, place_no_order, get_best_books_bulk, get_positions, derive_api_credentials
  db.py            # SQLite helpers — init_db(), record_bet(); DB at db/bets.db
  config.py        # config.toml loader (env > toml > default)
  models.py        # Dataclasses

db/     # bets.db (SQLite), weather_model.db (DuckDB) — gitignored
logs/   # Rotating logs (temp_buyer.log, automata.log, …) — gitignored
```

**Data flow (NO-buyer):** `temp_buyer.run()` → `view.fetch_events_resolving_within()`
pulls temp events → per event, `parser` + bucket-ladder logic pick the lowest
qualifying NO bucket below the YES-favorite → `client.place_no_order()` places a
GTC limit buy at the live ask → each cycle also records scans via `weather_scan`
and redeems resolved funder positions.

**Data flow (Weather):** `weather.run_weather_daemon()` →
`polymarket.fetch_temperature_markets_payload()` → `parser` extracts thresholds →
`weather` fetches forecasts → bets NO when the forecast strongly favors NO vs. the
live price; also backfills model outcomes each cycle.

**Data flow (Weather Model):** every NO-buyer cycle →
`weather_scan.scan_and_record_events()` writes one `ScanRecord` per candidate
(forecasts, METAR current + max, NO/YES bid+ask, μ̂ = forecast + bias, σ̂ from
lead-time bucket, fair NO prob, edge vs market) →
`weather_model.backfill_outcomes_from_polymarket()` (called by both bots) reads
YES-winning bucket midpoints into `outcomes` → `update_from_outcome()`
back-propagates the residual into per-(station, source) bias EMA and
per-(station, lead-bucket) σ EMA (α=0.10, warm-up 1/n for the first 10 samples).

**Model math:** `actual_max ~ N(forecast + bias[icao, source], sigma[icao, lead_bucket])`;
`fair_no_prob = Phi((threshold - mu) / sigma)` for "higher" markets, complement for
"below" / range-out. Shadow mode only — scans are recorded with edge vs market, but
bet selection still uses the `min_no_price` / `min_no_ask` gate.

## Key Design Decisions

- **`bets.db` dedup**: `placed_bets` has `UNIQUE(city, event_date, question)` — prevents double-betting the same market.
- **Hold-to-resolution + redeem**: the NO-buyer places no take-profit sell; positions are held to resolution and redeemed via the relayer (default) or onchain (`--redeem-mode onchain --polygon-rpc <url>`). Disable with `--no-redeem`.
- **Bucket ladder**: candidates are ranked closest-resolution first; the picker takes the farthest-below-favorite bucket whose NO ask is in band and at least `min_bucket_distance` steps below the YES-favorite.
- **CLOB credentials**: derived on demand from `POLYMARKET_PRIVATE_KEY` via `client.derive_api_credentials()`, once at startup in live mode.
- All monetary amounts are USDC with 6-decimal precision from the API (divide raw integer by `1e6`).
