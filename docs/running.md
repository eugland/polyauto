# Running the App

## Prerequisites

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your keys
```

Required `.env` keys:

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

## Frontend (market dashboard)

```bash
python app.py
```

Opens at **http://localhost:5000** (port configured in `webapp.json`).

Displays active Polymarket temperature markets paired with live ICAO weather data. Refreshes weather cache every 10 minutes in the background.

---

## Automata (betting engine)

**Dry run** — shows what would be bet, no orders placed, no credentials needed beyond `POLYMARKET_HOST`:

```bash
python -m automata.main
```

**Live betting** — places real GTC limit orders on Polymarket:

```bash
python -m automata.main --bet
```

Runs in a loop (60 s sleep between iterations). Each cycle:

1. *(live only)* Scans open positions — places take-profit sell orders where missing.
2. *(live only)* Checks USDC balance — skips new bets if insufficient.
3. Fetches active temperature markets from Polymarket.
4. Pulls ICAO station coordinates and Open-Meteo forecast highs for each event date.
5. Fetches live order-book ask prices in bulk.
6. Selects the best No-token candidate per city (lowest implied Yes probability).
7. Dry run: prints table. Live: places orders and records each bet to `bets.db`.

### Output columns (dry run)

| Column | Description |
|---|---|
| No | Ask price of the No token |
| Yes | Ask price of the Yes token |
| shares | Shares that would be bought |
| cost | USDC cost |
| local time | Current local time in that city |
| city | City name |
| date | Event date |
| forecast | Open-Meteo forecast high for the event date |
| question | The market question |
