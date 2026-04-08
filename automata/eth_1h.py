"""
automata/eth_1h.py

ETH 1H Up/Down tail-capture for Polymarket.

Strategy:
  When one outcome (Up or Down) is priced in the buy zone
  with MIN_MINUTES to MAX_MINUTES remaining in the candle:
    1. Buy BET_SHARES at market ask
    2. Immediately post a GTC limit sell at SELL_TARGET (0.99)

  Entry threshold uses a Brownian Bridge-inspired formula:
    min_bid = 1 - 0.12 / sqrt(mins_remaining)
    T-20: 0.973+   T-10: 0.962+   T-3: 0.931+
  k=0.12 is self-calibrated from trade history when >= 10 outcomes exist.

  Stop-loss: if held position bid drops below STOP_LOSS (0.75),
  cancel the sell and exit immediately at market.

  The probability naturally decays toward 1.0 as the candle closes.
  Either the sell fills before resolution, or it resolves at $1.00.

  One position per candle.  Sizing: 20 shares or balance * 0.9 if short.

Run:  python -m automata.eth_1h
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger("automata.eth_1h")

# ── Parameters ─────────────────────────────────────────────────────────────────

BUY_MAX      = 0.98    # skip if already too close to 1.0 (tiny upside left)
SELL_TARGET  = 0.99    # immediately post sell here after buying
STOP_LOSS    = 0.75    # exit immediately if position bid drops below this
BET_SHARES   = 20      # target shares per trade
K_DEFAULT    = 0.12   # Brownian Bridge k — auto-calibrated when >= 10 outcomes exist
MIN_MINUTES  = 3       # don't enter with less time than this
MAX_MINUTES  = 20      # don't enter too early (price can still flip)

DB_PATH = Path(__file__).resolve().parent.parent / "bets.db"

MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
ET_OFFSET = timedelta(hours=-4)   # EDT (UTC-4)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

GAMMA_API = "https://gamma-api.polymarket.com/events?slug={slug}"

# ── In-memory position state ───────────────────────────────────────────────────

_position: dict = {
    "active":        False,
    "slug":          None,
    "direction":     None,
    "token_id":      None,
    "sell_order_id": None,
    "shares":        0,
    "entry_price":   None,
}


def _reset_position() -> None:
    global _position
    _position = {k: None for k in _position}
    _position["active"] = False
    _position["shares"] = 0


# ── Slug builder ───────────────────────────────────────────────────────────────

def current_et() -> datetime:
    return datetime.now(timezone(ET_OFFSET))


def build_slug(dt: datetime) -> str:
    """ethereum-up-or-down-april-5-2026-3pm-et"""
    month = MONTH_NAMES[dt.month]
    h24   = dt.hour
    h12   = h24 % 12 or 12
    return f"ethereum-up-or-down-{month}-{dt.day}-{dt.year}-{h12}{'am' if h24 < 12 else 'pm'}-et"


# ── Market fetch ───────────────────────────────────────────────────────────────

def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _realized_annual_vol(symbol: str = "ETHUSDT", lookback_hours: int = 168) -> float | None:
    """
    Estimate annualized volatility from hourly log returns.
    """
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={lookback_hours}"
        )
    except Exception:
        return None
    if not isinstance(klines, list) or len(klines) < 3:
        return None
    closes: list[float] = []
    for row in klines:
        try:
            closes.append(float(row[4]))
        except Exception:
            continue
    if len(closes) < 3:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    hourly_vol = math.sqrt(max(var, 0.0))
    return hourly_vol * math.sqrt(365.0 * 24.0)


def _black_scholes_digital_up_prob(spot: float, strike: float, years_to_expiry: float, sigma: float, r: float = 0.0) -> float | None:
    """
    Risk-neutral probability P(S_T >= K) for a cash-or-nothing digital call.
    """
    if spot <= 0 or strike <= 0 or years_to_expiry <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(years_to_expiry)
    d2 = (math.log(spot / strike) + (r - 0.5 * sigma * sigma) * years_to_expiry) / (sigma * sqrt_t)
    return min(1.0, max(0.0, _normal_cdf(d2)))


def _fetch_eth_bs_fair(end_utc: datetime | None, mins_remaining: int | None) -> tuple[float | None, float | None]:
    """
    Returns (fair_up, fair_down) from Black-Scholes, or (None, None) if unavailable.
    Strike is the ETH/USDT open of the target 1h candle; spot is current ETH/USDT.
    """
    if not end_utc or mins_remaining is None:
        return None, None
    try:
        ticker = _get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
        spot = float(ticker["price"])
    except Exception:
        return None, None

    try:
        start_utc = end_utc - timedelta(hours=1)
        start_ms = int(start_utc.timestamp() * 1000)
        kline = _get(
            f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1h&startTime={start_ms}&limit=1"
        )
        strike = float(kline[0][1]) if isinstance(kline, list) and kline else None
    except Exception:
        strike = None
    if strike is None:
        return None, None

    sigma = _realized_annual_vol("ETHUSDT", 168)
    if sigma is None or sigma <= 0:
        return None, None

    # Keep fair probabilistic even at/after 0 min by flooring T to a tiny value.
    effective_mins = max(float(mins_remaining), 1.0 / 60.0)  # 1 second
    years = effective_mins / (365.0 * 24.0 * 60.0)
    fair_up = _black_scholes_digital_up_prob(spot=spot, strike=strike, years_to_expiry=years, sigma=sigma, r=0.0)
    if fair_up is None:
        return None, None
    fair_down = 1.0 - fair_up
    return fair_up, fair_down


def _fmt_matrix_cell(v: float | None) -> str:
    return f"{v * 100:.1f}" if v is not None else "n/a"


def _print_price_matrix(up_ask: float | None, down_ask: float | None, up_bid: float | None, down_bid: float | None, fair_up: float | None, fair_down: float | None) -> None:
    print("              Up     Down")
    print(f"  ask       {_fmt_matrix_cell(up_ask):>6}  {_fmt_matrix_cell(down_ask):>6}")
    print(f"  bid       {_fmt_matrix_cell(up_bid):>6}  {_fmt_matrix_cell(down_bid):>6}")
    print(f"  fair      {_fmt_matrix_cell(fair_up):>6}  {_fmt_matrix_cell(fair_down):>6}")


def fetch_and_parse(slug: str) -> dict | None:
    """
    Returns dict: slug, title, up_token, down_token, minutes_remaining, end_utc
    or None if market is closed / not found.
    """
    try:
        data = _get(GAMMA_API.format(slug=slug))
        event = data[0] if data else None
    except (URLError, json.JSONDecodeError, IndexError):
        return None

    if not event or not event.get("markets"):
        return None
    m = event["markets"][0]
    if m.get("closed") or (m.get("active") is not None and not m.get("active")):
        return None

    def _load(key):
        v = m.get(key, [])
        return json.loads(v) if isinstance(v, str) else v

    outcomes  = _load("outcomes")
    token_ids = _load("clobTokenIds")

    up_token = down_token = None
    for i, name in enumerate(outcomes):
        nl = name.strip().lower()
        if nl == "up"   and i < len(token_ids): up_token   = str(token_ids[i])
        if nl == "down" and i < len(token_ids): down_token = str(token_ids[i])

    if not up_token or not down_token:
        return None

    end_str = m.get("endDate") or event.get("endDate") or ""
    end_utc = None
    try:
        dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    now_utc = datetime.now(timezone.utc)
    minutes_remaining = int((end_utc - now_utc).total_seconds() / 60) if end_utc else 0

    return {
        "slug":              event.get("slug", slug),
        "title":             event.get("title", ""),
        "up_token":          up_token,
        "down_token":        down_token,
        "minutes_remaining": minutes_remaining,
        "end_utc":           end_utc,
    }


def get_books(host: str, token_ids: list[str]) -> dict[str, dict]:
    """Returns {token_id: {bid, ask}} via bulk /books."""
    import requests
    try:
        resp = requests.post(
            f"{host}/books",
            json=[{"token_id": tid} for tid in token_ids],
            timeout=8,
        )
        resp.raise_for_status()
        result = {}
        for book in resp.json():
            tid = book.get("asset_id") or book.get("token_id")
            if not tid:
                continue
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            result[str(tid)] = {
                "bid": max(float(b["price"]) for b in bids) if bids else None,
                "ask": min(float(a["price"]) for a in asks) if asks else None,
            }
        return result
    except Exception:
        return {}


# ── SQLite trade tracking ──────────────────────────────────────────────────────

def _init_table() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eth_1h_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                slug           TEXT NOT NULL,
                direction      TEXT NOT NULL,
                token_id       TEXT NOT NULL,
                buy_order      TEXT,
                sell_order     TEXT,
                shares         REAL NOT NULL,
                entry_price    REAL NOT NULL,
                sell_target    REAL NOT NULL,
                cost_usdc      REAL NOT NULL,
                placed_at      TEXT NOT NULL,
                mins_remaining REAL,
                outcome        TEXT
            )
        """)
        # Migrate existing rows that may lack the new columns
        existing = {r[1] for r in conn.execute("PRAGMA table_info(eth_1h_trades)")}
        if "mins_remaining" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN mins_remaining REAL")
        if "outcome" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN outcome TEXT")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eth_1h_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO eth_1h_settings (key, value) VALUES ('k', ?)",
            (str(K_DEFAULT),)
        )


def _get_k() -> float:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT value FROM eth_1h_settings WHERE key = 'k'"
        ).fetchone()
    return float(row[0]) if row else K_DEFAULT


def _set_k(k: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO eth_1h_settings (key, value) VALUES ('k', ?)",
            (str(round(k, 2)),)
        )
    log.info("[eth_1h] k updated to %.2f in DB", k)


def _has_trade(slug: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM eth_1h_trades WHERE slug = ?", (slug,)
        ).fetchone()
    return row is not None


def _record_trade(slug, direction, token_id, buy_order, sell_order,
                  shares, entry_price, sell_target, cost_usdc,
                  mins_remaining: float | None = None) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO eth_1h_trades
               (slug, direction, token_id, buy_order, sell_order,
                shares, entry_price, sell_target, cost_usdc, placed_at,
                mins_remaining, outcome)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (slug, direction, token_id, buy_order, sell_order,
             shares, entry_price, sell_target, cost_usdc,
             datetime.now(timezone.utc).isoformat(),
             mins_remaining),
        )


def _update_outcome(slug: str, outcome: str) -> None:
    """Set outcome for the trade with this slug (win / stop_loss / expired)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE eth_1h_trades SET outcome = ? WHERE slug = ? AND outcome IS NULL",
            (outcome, slug),
        )
    log.info("[eth_1h] outcome recorded: %s → %s", slug[-24:], outcome)


def _calibrate_k() -> float:
    """
    Grid-search the best k for min_bid = 1 - k / sqrt(mins).
    Uses resolved trades (outcome = 'win' or 'stop_loss') from the DB.
    Auto-saves the best k to the DB when >= 10 outcomes exist.
    Returns the current k (updated or unchanged).
    """
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT entry_price, mins_remaining, outcome
               FROM eth_1h_trades
               WHERE outcome IN ('win', 'stop_loss')
               AND mins_remaining IS NOT NULL"""
        ).fetchall()

    current_k = _get_k()

    if len(rows) < 10:
        log.info("[eth_1h] calibration: only %d resolved trades — need 10, using k=%.2f",
                 len(rows), current_k)
        return current_k

    best_k, best_score = current_k, -1.0
    for k in [round(0.06 + 0.01 * i, 2) for i in range(15)]:  # 0.06 .. 0.20
        taken = [(ep, out) for ep, mins, out in rows
                 if mins and ep >= 1 - k / math.sqrt(mins)]
        if not taken:
            continue
        win_rate = sum(1 for _, out in taken if out == "win") / len(taken)
        score = win_rate * math.log1p(len(taken))
        if score > best_score:
            best_score, best_k = score, k

    wins   = sum(1 for _, _, o in rows if o == "win")
    losses = sum(1 for _, _, o in rows if o == "stop_loss")
    log.info(
        "[eth_1h] calibration: %d resolved trades (%d W / %d L) — "
        "best k=%.2f (score=%.3f)  previous k=%.2f",
        len(rows), wins, losses, best_k, best_score, current_k,
    )
    if best_k != current_k:
        _set_k(best_k)
    return best_k


# ── Main entry point ───────────────────────────────────────────────────────────

def run_eth_1h(dry_run: bool = True, host: str = "https://clob.polymarket.com") -> None:
    """
    Called every 10 s.  Monitors open position for stop-loss,
    then looks for a new entry if none is active.
    """
    import os
    _init_table()

    now_et       = current_et()
    candle_start = now_et.replace(minute=0, second=0, microsecond=0)
    slug         = build_slug(candle_start)

    # ── Monitor open position ──────────────────────────────────────────────────
    if _position["active"]:
        if _position["slug"] != slug:
            # New candle — position resolved, record outcome and calibrate
            prev_slug = _position["slug"] or ""
            log.info("[eth_1h] New candle — position on %s resolved, resetting",
                     prev_slug[-24:])
            _update_outcome(prev_slug, "expired")
            _reset_position()
            _calibrate_k()
        else:
            # Same candle — check stop-loss and win detection
            books    = get_books(host, [_position["token_id"]])
            cur_bid  = books.get(_position["token_id"], {}).get("bid")

            if cur_bid is not None and cur_bid >= SELL_TARGET:
                log.info("[eth_1h] WIN detected — bid=%.3f >= sell target %.2f",
                         cur_bid, SELL_TARGET)
                _update_outcome(_position["slug"], "win")
                _reset_position()
                _calibrate_k()
                return

            if cur_bid is not None and cur_bid < STOP_LOSS:
                log.warning("[eth_1h] STOP-LOSS  bid=%.3f < %.2f — exiting %s",
                            cur_bid, STOP_LOSS, _position["direction"])
                if not dry_run:
                    try:
                        from automata.client import build_client, cancel_order, place_market_sell
                        client = build_client(
                            host=os.environ["POLYMARKET_HOST"],
                            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                            api_key=os.environ["CLOB_API_KEY"],
                            api_secret=os.environ["CLOB_SECRET"],
                            api_passphrase=os.environ["CLOB_PASS"],
                            funder=os.getenv("POLYMARKET_FUNDER") or None,
                            signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
                        )
                        if _position["sell_order_id"] and _position["sell_order_id"] != "?":
                            cancel_order(client, _position["sell_order_id"])
                        exit_price = max(round(cur_bid - 0.01, 2), 0.01)
                        place_market_sell(client, _position["token_id"],
                                          exit_price, _position["shares"])
                        log.info("[eth_1h] Stop-loss sell placed @ %.3f", exit_price)
                    except Exception as exc:
                        log.error("[eth_1h] Stop-loss exit failed: %s", exc)
                else:
                    log.info("[eth_1h] [DRY RUN] Would stop-loss exit @ ~%.3f", cur_bid)
                _update_outcome(_position["slug"], "stop_loss")
                _reset_position()
                _calibrate_k()
            else:
                log.info("[eth_1h] Holding %s — bid=%s  entry=%.3f",
                         _position["direction"],
                         f"{cur_bid:.3f}" if cur_bid else "n/a",
                         _position["entry_price"])
            return

    # ── Entry logic ────────────────────────────────────────────────────────────

    # Already traded this candle (from a previous process run)?
    if _has_trade(slug):
        log.info("[eth_1h] Already traded %s — skip", slug[-24:])
        return

    mkt = fetch_and_parse(slug)
    if not mkt:
        log.info("[eth_1h] Market not found or closed: %s", slug)
        return

    mins = mkt["minutes_remaining"]
    if not (MIN_MINUTES <= mins <= MAX_MINUTES):
        log.info("[eth_1h] %d min remaining — outside entry window [%d-%d]",
                 mins, MIN_MINUTES, MAX_MINUTES)
        return

    k = _calibrate_k()

    # Time-adjusted minimum bid (Brownian Bridge): stricter earlier in the window
    min_bid = round(1 - k / mins ** 0.5, 3)

    # Live order book
    books     = get_books(host, [mkt["up_token"], mkt["down_token"]])
    up_book   = books.get(mkt["up_token"],   {})
    down_book = books.get(mkt["down_token"], {})

    up_ask   = up_book.get("ask")
    down_ask = down_book.get("ask")
    up_bid   = up_book.get("bid")
    down_bid = down_book.get("bid")

    # ── Print current state ────────────────────────────────────────────────────
    div = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 1H TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Time remaining: {mins} min  (window {MIN_MINUTES}-{MAX_MINUTES}min)")
    fair_up, fair_down = _fetch_eth_bs_fair(mkt.get("end_utc"), mins)
    _print_price_matrix(up_ask, down_ask, up_bid, down_bid, fair_up, fair_down)
    print(f"  Min bid (T-{mins}m): {min_bid:.3f}  |  max ask: {BUY_MAX:.2f}  |  sell: {SELL_TARGET:.2f}")

    # ── Find entry ─────────────────────────────────────────────────────────────
    candidate = None
    for direction, ask, bid, token in [
        ("Up",   up_ask,   up_bid,   mkt["up_token"]),
        ("Down", down_ask, down_bid, mkt["down_token"]),
    ]:
        if ask is None or bid is None:
            continue
        if bid >= min_bid and ask <= BUY_MAX:
            candidate = {"direction": direction, "ask": ask, "bid": bid, "token": token}
            break

    if not candidate:
        up_str   = f"{up_ask:.3f}"   if up_ask   else "n/a"
        down_str = f"{down_ask:.3f}" if down_ask else "n/a"
        print(f"  --> No entry: Up={up_str}  Down={down_str}  "
              f"(need bid>={min_bid:.3f}  ask<={BUY_MAX:.2f})")
        print(f"{div}\n")
        return

    direction = candidate["direction"]
    ask       = candidate["ask"]
    token     = candidate["token"]

    if dry_run:
        shares = BET_SHARES
        cost   = round(shares * ask, 2)
        print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
              f"{shares}sh  ${cost:.2f}  sell target {SELL_TARGET:.2f}")
        print(f"{div}\n")
        log.info("[eth_1h] [DRY RUN] Would buy %d %s @ %.3f  $%.2f  then sell @ %.2f",
                 shares, direction, ask, cost, SELL_TARGET)
        return

    # ── Live: buy then immediately post sell ───────────────────────────────────
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET",
                "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        log.error("[eth_1h] Missing .env keys")
        return

    from automata.client import build_client, get_usdc_balance, place_no_order, place_sell_order

    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )

    # Determine shares based on available balance
    try:
        balance = get_usdc_balance(client)
    except Exception as exc:
        log.warning("[eth_1h] Could not fetch balance, defaulting to %d shares: %s", BET_SHARES, exc)
        balance = BET_SHARES * ask  # assume enough

    if balance >= BET_SHARES:
        shares = BET_SHARES
    else:
        shares = round(balance * 0.9, 2)

    cost = round(shares * ask, 2)
    log.info("[eth_1h] Balance $%.2f — using %.2f shares (target %d)", balance, shares, BET_SHARES)

    print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
          f"{shares}sh  ${cost:.2f}  sell target {SELL_TARGET:.2f}")
    print(f"{div}\n")

    if shares <= 0:
        log.error("[eth_1h] Insufficient balance (%.2f), skipping", balance)
        return

    # Step 1 — buy
    try:
        buy_resp = place_no_order(client, token, ask, shares)
        buy_id   = buy_resp.get("orderID") or buy_resp.get("id") or "?"
        log.info("[eth_1h] Bought %.2f %s @ %.3f  $%.2f  id=%s",
                 shares, direction, ask, cost, buy_id)
    except Exception as exc:
        log.error("[eth_1h] Buy failed: %s", exc)
        return

    # Step 2 — post sell at SELL_TARGET immediately
    sell_id = "?"
    try:
        sell_resp = place_sell_order(client, token, SELL_TARGET, shares)
        sell_id   = sell_resp.get("orderID") or sell_resp.get("id") or "?"
        log.info("[eth_1h] Sell posted @ %.2f  id=%s", SELL_TARGET, sell_id)
    except Exception as exc:
        log.warning("[eth_1h] Sell order failed (holding to resolution): %s", exc)

    _record_trade(slug, direction, token, buy_id, sell_id,
                  shares, ask, SELL_TARGET, cost, mins_remaining=mins)

    # Track in memory for stop-loss monitoring
    _position.update({
        "active":        True,
        "slug":          slug,
        "direction":     direction,
        "token_id":      token,
        "sell_order_id": sell_id,
        "shares":        shares,
        "entry_price":   ask,
    })

    print(f"  [eth_1h] BUY {direction} {shares}sh @ {ask:.3f}  ${cost:.2f}"
          f"  buy={buy_id}  sell@{SELL_TARGET}={sell_id}")


def _fmt(v) -> str:
    return f"{v:.3f}" if v is not None else " n/a "


# ── Standalone display ─────────────────────────────────────────────────────────

def analyze(host: str = "https://clob.polymarket.com") -> None:
    now_et       = current_et()
    candle_start = now_et.replace(minute=0, second=0, microsecond=0)

    # Try current and next hour
    for dt in [candle_start, candle_start + timedelta(hours=1)]:
        slug = build_slug(dt)
        mkt  = fetch_and_parse(slug)
        if mkt:
            break
    else:
        print("No active ETH 1H market found")
        return

    books     = get_books(host, [mkt["up_token"], mkt["down_token"]])
    up_book   = books.get(mkt["up_token"],   {})
    down_book = books.get(mkt["down_token"], {})

    mins = mkt["minutes_remaining"]
    min_bid = round(1 - 0.12 / mins ** 0.5, 3) if mins > 0 else None
    div     = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 1H TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Slug:           {mkt['slug']}")
    print(f"  Time remaining: {mins} min")
    fair_up, fair_down = _fetch_eth_bs_fair(mkt.get("end_utc"), mins)
    _print_price_matrix(
        up_book.get("ask"),
        down_book.get("ask"),
        up_book.get("bid"),
        down_book.get("bid"),
        fair_up,
        fair_down,
    )
    min_bid_str = f"{min_bid:.3f}" if min_bid is not None else "n/a"
    print(f"  Min bid (T-{mins}m): {min_bid_str}  |  max ask: {BUY_MAX:.2f}  |  sell: {SELL_TARGET:.2f}")
    print(f"  Stop-loss: {STOP_LOSS:.2f}  |  Entry window: {MIN_MINUTES}-{MAX_MINUTES} min remaining")

    in_window = MIN_MINUTES <= mins <= MAX_MINUTES
    print(f"  Window: {'OPEN' if in_window else f'CLOSED ({mins}min)'}")

    for label, book in [("Up", up_book), ("Down", down_book)]:
        ask = book.get("ask")
        bid = book.get("bid")
        if ask and bid and min_bid is not None and bid >= min_bid and ask <= BUY_MAX and in_window:
            cost = round(BET_SHARES * ask, 2)
            print(f"  --> SIGNAL: buy {label} @ {ask:.3f}  "
                  f"{BET_SHARES}sh (balance-adjusted at trade time)  ${cost:.2f}  then sell @ {SELL_TARGET}")
    print(f"{div}\n")


if __name__ == "__main__":
    import os, sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    analyze(host=host)
