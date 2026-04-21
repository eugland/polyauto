# PythonProject

## Folder quick map

- `automata/`: betting engine modules (`weather`, `eth`, `eth3cents`, `btc_reach`, `eth_1h`, `view_bets`).
- `experiment/`: standalone scanners, backtests, and analysis scripts (no live orders).
- `docs/`: static docs/demo files (`index.html`, `app.js`, `styles.css`, `running.md`).
- `.venv/`: local virtual environment.
- `.idea/`: IDE project settings.
- `.claude/`: local agent/tool metadata.

## Setup

```bash
pip install -r requirements.txt
```

## How to run (with examples)

- Combined loop (dry run):

```bash
python -m automata
```

- Combined loop (live orders):

```bash
python -m automata --bet
```

- Combined loop with per-daemon spend caps:

```bash
python -m automata --bet --weather-max-balance 40 --eth-max-balance 20
```

- Weather daemon only:

```bash
python -m automata.weather --bet --once --max-balance 30
```

- ETH daemon only:

```bash
python -m automata.eth --bet --once --max-balance 30
```

- ETH 3-cents daemon only:

```bash
python -m automata.eth3cents --bet --poll 0.5 --max-balance 30
```

- BTC range analyzer:

```bash
python -m automata.btc_reach
```

- ETH 1H analyzer:

```bash
python -m automata.eth_1h
```

- Bets UI:

```bash
python -m automata.view_bets
```

Open: `http://localhost:5050`

## Weather Model (self-tuning forecast-bias model)

`automata/weather_model.py` records every weather-bot scan into DuckDB
(`db/weather_model.db`) and maintains per-station **bias** (EMA of
`actual − forecast`) and **σ** (EMA of residual², bucketed by lead time).
After each market resolves, the YES-winning bucket's midpoint is pulled from
Polymarket and back-propagated into the bias and σ tables automatically — no
manual retraining. Shadow mode only right now: scans + fair NO prices are
logged, but actual bet selection still uses the old `min_no_price` gate.

**Run it:**

```bash
# 1. Start the weather bot (dry-run is fine — scans get recorded either way)
python -m automata.weather --interval 60

# 2. Start the stock UI in another terminal
python -m stock --port 8081
```

Open `http://localhost:8081/weather-model` for the cities table, per-station
bias/σ, calibration chart, and per-city scan timelines (forecast vs METAR
vs actual with market overlay).

**Reminder — next steps after letting it run:**

- [ ] **Run for 1–2 days** so scans accumulate and at least a few markets
      resolve (backfill runs every ~10 iterations).
- [ ] **Check `/weather-model`** — verify per-station bias converges and
      calibration points sit near the diagonal. If Open-Meteo is biased hot
      for London (actual < forecast), `bias[EGLC, openmeteo]` should go
      negative after a few outcomes.
- [ ] **Widen the scan net** if desired — `weather_bot.py:288` caps at the
      15 earliest-resolving events; bump to 25–30 if you want more
      observations per cycle (scales linearly).
- [ ] **Graduate to v2 (let the model drive bet selection):** after ~2 weeks
      with populated bias tables, change the gate in `weather_bot.run()`
      from `market_no ≥ MIN_NO_PRICE` to `market_no ≥ fair_no + EDGE_BPS`.
      The fair NO price is already computed every scan and stored in the
      `scans.fair_no_prob` column — just read it in the bet-selection loop
      instead of using the flat 0.97 threshold.
- [ ] **Add covariates (v3)** once you have ≥200 resolved outcomes per
      station: regress residual on cloud cover, dewpoint, wind,
      `(metar_max_so_far − forecast)`. Keep the same
      `fair_no_price(scan) → prob` interface so the bot doesn't change.

## Crypto 5m penny scanner (experiment)

Records signals across **all** active crypto 5m Up/Down markets (BTC, ETH, XRP, BNB, SOL, DOGE, and any others listed). No orders are placed — paper-only signal recorder.

**Step 1 — start the scanner** (runs continuously, Ctrl+C to stop):

```bash
python -m experiment.crypto_5m_scanner
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--max-price` | `0.03` | Record signals when ask ≤ this price ($0.01, $0.02, or $0.03) |
| `--poll` | `5` | Seconds between order-book polls |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

**Step 2 — open the stats UI** (in a separate terminal):

```bash
python -m experiment.crypto_5m_stats
```

Opens at `http://localhost:5051` — also accessible from any device on your LAN (IP printed on startup).

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `5051` | HTTP port |
| `--db` | `experiment/crypto_5m.db` | SQLite DB path |

**What the stats UI shows:**

- Tab per asset (BTC, ETH, XRP, etc.)
- **Cumulative P/L chart** with three series per asset:
  - ≤1¢ — only signals where the cheapest ask was $0.01
  - ≤2¢ — signals at $0.01 or $0.02
  - ≤3¢ — signals at $0.01, $0.02, or $0.03
- Per-tier stats: win rate, net P/L, ROI, resolved vs pending count
- Auto-refreshes every 30 seconds

**How signals work:**

One signal row per (candle, side). The scanner records the *cheapest ask ever seen* for that side during the candle (`min_price`). After the candle closes, the winner is fetched from Polymarket and P/L is written: `(1 - min_price)` if won, `(-min_price)` if lost. The chart assumes 1 share per signal.

## Parameters

- `automata`: `--bet`, `--weather-interval`, `--eth-interval`, `--weather-max-balance`, `--eth-max-balance`
- `automata.weather`: `--bet`, `--interval`, `--once`, `--max-balance`
- `automata.eth`: `--bet`, `--interval`, `--once`, `--max-balance`
