"""
automata/eth_1h.py

ETH 1H Up/Down tail-capture for Polymarket.

Strategy:
  When one outcome (Up or Down) is priced in the buy zone
  with MIN_MINUTES to MAX_MINUTES remaining in the candle:
    1. Buy BET_SHARES at market ask
    2. Immediately post a GTC limit sell at SELL_TARGET (0.99)

  Entry threshold is time-adjusted: require higher bid earlier in the window.
    min_bid = 0.97 - 0.004 * mins_remaining
    T-20: 0.89+   T-10: 0.93+   T-5: 0.95+

  Stop-loss: if held position bid drops below STOP_LOSS (0.75),
  cancel the sell and exit immediately at market.

  The probability naturally decays toward 1.0 as the candle closes.
  Either the sell fills before resolution, or it resolves at $1.00.

  One position per candle.  $10 max per trade.

Run:  python -m automata.eth_1h
"""
from __future__ import annotations

import json
import logging
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
BET_SHARES   = 10
MAX_COST     = 10.0    # hard cap per trade
MIN_MINUTES  = 5       # don't enter with less time than this
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
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                slug        TEXT NOT NULL,
                direction   TEXT NOT NULL,
                token_id    TEXT NOT NULL,
                buy_order   TEXT,
                sell_order  TEXT,
                shares      REAL NOT NULL,
                entry_price REAL NOT NULL,
                sell_target REAL NOT NULL,
                cost_usdc   REAL NOT NULL,
                placed_at   TEXT NOT NULL
            )
        """)


def _has_trade(slug: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM eth_1h_trades WHERE slug = ?", (slug,)
        ).fetchone()
    return row is not None


def _record_trade(slug, direction, token_id, buy_order, sell_order,
                  shares, entry_price, sell_target, cost_usdc) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO eth_1h_trades
               (slug, direction, token_id, buy_order, sell_order,
                shares, entry_price, sell_target, cost_usdc, placed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (slug, direction, token_id, buy_order, sell_order,
             shares, entry_price, sell_target, cost_usdc,
             datetime.now(timezone.utc).isoformat()),
        )


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
            # New candle — position resolved (win or loss), reset
            log.info("[eth_1h] New candle — position on %s resolved, resetting",
                     (_position["slug"] or "")[-24:])
            _reset_position()
        else:
            # Same candle — check stop-loss
            books    = get_books(host, [_position["token_id"]])
            cur_bid  = books.get(_position["token_id"], {}).get("bid")

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
                _reset_position()
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

    # Time-adjusted minimum bid: stricter earlier in the window
    min_bid = round(0.97 - 0.004 * mins, 3)

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
    print(f"  Up   ask={_fmt(up_ask)}  bid={_fmt(up_bid)}")
    print(f"  Down ask={_fmt(down_ask)}  bid={_fmt(down_bid)}")
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
            cost = round(BET_SHARES * ask, 2)
            if cost <= MAX_COST:
                candidate = {"direction": direction, "ask": ask, "bid": bid,
                             "token": token, "cost": cost}
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
    cost      = candidate["cost"]

    print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
          f"{BET_SHARES}sh  ${cost:.2f}  sell target {SELL_TARGET:.2f}")
    print(f"{div}\n")

    if dry_run:
        log.info("[eth_1h] [DRY RUN] Would buy %d %s @ %.3f  $%.2f  then sell @ %.2f",
                 BET_SHARES, direction, ask, cost, SELL_TARGET)
        return

    # ── Live: buy then immediately post sell ───────────────────────────────────
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET",
                "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        log.error("[eth_1h] Missing .env keys")
        return

    from automata.client import build_client, place_no_order, place_sell_order

    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )

    # Step 1 — buy
    try:
        buy_resp = place_no_order(client, token, ask, BET_SHARES)
        buy_id   = buy_resp.get("orderID") or buy_resp.get("id") or "?"
        log.info("[eth_1h] Bought %d %s @ %.3f  $%.2f  id=%s",
                 BET_SHARES, direction, ask, cost, buy_id)
    except Exception as exc:
        log.error("[eth_1h] Buy failed: %s", exc)
        return

    # Step 2 — post sell at SELL_TARGET immediately
    sell_id = "?"
    try:
        sell_resp = place_sell_order(client, token, SELL_TARGET, BET_SHARES)
        sell_id   = sell_resp.get("orderID") or sell_resp.get("id") or "?"
        log.info("[eth_1h] Sell posted @ %.2f  id=%s", SELL_TARGET, sell_id)
    except Exception as exc:
        log.warning("[eth_1h] Sell order failed (holding to resolution): %s", exc)

    _record_trade(slug, direction, token, buy_id, sell_id,
                  BET_SHARES, ask, SELL_TARGET, cost)

    # Track in memory for stop-loss monitoring
    _position.update({
        "active":        True,
        "slug":          slug,
        "direction":     direction,
        "token_id":      token,
        "sell_order_id": sell_id,
        "shares":        BET_SHARES,
        "entry_price":   ask,
    })

    print(f"  [eth_1h] BUY {direction} {BET_SHARES}sh @ {ask:.3f}  ${cost:.2f}"
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

    mins    = mkt["minutes_remaining"]
    min_bid = round(0.97 - 0.004 * mins, 3)
    div     = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 1H TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Slug:           {mkt['slug']}")
    print(f"  Time remaining: {mins} min")
    print(f"  Up   ask={_fmt(up_book.get('ask'))}  bid={_fmt(up_book.get('bid'))}")
    print(f"  Down ask={_fmt(down_book.get('ask'))}  bid={_fmt(down_book.get('bid'))}")
    print(f"  Min bid (T-{mins}m): {min_bid:.3f}  |  max ask: {BUY_MAX:.2f}  |  sell: {SELL_TARGET:.2f}")
    print(f"  Stop-loss: {STOP_LOSS:.2f}  |  Entry window: {MIN_MINUTES}-{MAX_MINUTES} min remaining")

    in_window = MIN_MINUTES <= mins <= MAX_MINUTES
    print(f"  Window: {'OPEN' if in_window else f'CLOSED ({mins}min)'}")

    for label, book in [("Up", up_book), ("Down", down_book)]:
        ask = book.get("ask")
        bid = book.get("bid")
        if ask and bid and bid >= min_bid and ask <= BUY_MAX and in_window:
            cost = round(BET_SHARES * ask, 2)
            print(f"  --> SIGNAL: buy {label} @ {ask:.3f}  "
                  f"{BET_SHARES}sh  ${cost:.2f}  then sell @ {SELL_TARGET}")
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
