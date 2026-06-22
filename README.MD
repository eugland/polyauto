# polyauto

Automated betting bots for [Polymarket](https://polymarket.com) temperature markets.

## Setup

```bash
pip install -r requirements.txt
# Create .env (gitignored) with your secrets — see Configuration below.
```

## Run

Both bots default to dry-run; pass `--bet` to place real orders.

```bash
# Temperature NO-buyer (main bot): buy NO on the lowest bucket below the YES-favorite
python -m temp_buyer                 # dry-run
python -m temp_buyer --bet           # live: place order + redeem resolved
python -m temp_buyer --bet --once    # single cycle

# Weather bot: bet NO on daily high-temp markets from forecast vs NO price
python -m temp_buyer.weather --bet --interval 60
python -m temp_buyer.weather --bet --once --max-balance 30

# Price viewer: YES/NO prices for an event slug/URL (no args = all temp events <24h)
python -m temp_buyer.view <event-slug-or-url>
python -m temp_buyer.view
```

## Configuration

Tunables live in `config.toml` (committed); secrets in `.env` (gitignored).
Sections: `[polymarket]`, `[weather]`, `[temp_buyer]`, `[gamma]`.
Precedence per setting: **env var > `config.toml` > code default**, e.g.
`TEMPBUY_BET_SHARES=5 python -m temp_buyer --bet`.

`.env` keys: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_FUNDER`, `POLYMARKET_HOST`,
`RELAYER_API_KEY`, `RELAYER_API_KEY_ADDRESS`, `POLYGON_RPC_URL` (onchain redeem).

## Weather model

`temp_buyer/weather_model.py` is a self-tuning forecast-bias model. Each
`temp_buyer.temp_buyer` cycle records one scan row per candidate market to DuckDB
(`db/weather_model.db`); resolved outcomes are backfilled from Polymarket and
back-propagated into per-station bias + σ EMAs. Shadow mode only — a fair NO
price is recorded per scan but bet selection still uses the `min_no_price` gate.
No viewer UI; query the DuckDB tables directly.

## Layout

- `temp_buyer/` — bot package. Runnable: `temp_buyer`, `weather`, `view`.
  Support: `client`, `config`, `db`, `models`, `parser`, `polymarket`,
  `weather_model`, `weather_scan`.
- `db/` — SQLite `bets.db`, DuckDB `weather_model.db` (gitignored).
- `logs/` — rotating logs (gitignored).
