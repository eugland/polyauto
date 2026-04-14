"""
ETH 5m live scanner/trader for Polymarket.

Replicates the core signal logic from experiment.crypto_5m_scanner, but only for
ETH 5m and with real order placement when --bet is enabled.

Execution model:
- Detect active ETH 5m market.
- Pull order books every poll cycle.
- Compute Black-Scholes fair price from Binance 5m vol + live spot.
- Apply scanner-style gates (entry window, min edge, opposite-bid confirmation).
- Submit ONE limit buy order per (slug, side):
  maker-leaning price, but post_only=False so it can fill as taker too.
- Taker-fee-aware guard: require minimum net edge after worst-case taker fee.

Run:
  python -m automata.eth_5min
  python -m automata.eth_5min --bet
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from automata.client import build_client, derive_api_credentials, get_usdc_balance

log = logging.getLogger("automata.eth_5min")

# APIs
GAMMA_API = "https://gamma-api.polymarket.com/events"
CLOB_HOST_DEFAULT = "https://clob.polymarket.com"
BINANCE_BASE = "https://api.binance.com/api/v3"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
BINANCE_WS_RECONNECT_SEC = 3

# strategy defaults (close to crypto_5m_scanner)
DEFAULT_POLL = 1.0
DEFAULT_SHARES = 5.0
DEFAULT_MIN_EDGE = 0.03
DEFAULT_MAX_EDGE = 0.12
DEFAULT_MAX_PRICE = 0.99
DEFAULT_MIN_NET_EDGE_TAKER = 0.0
ENTRY_MIN_SECS = 40
ENTRY_MAX_SECS = 240
MIN_ENTRY_PRICE = 0.08
OPPOSITE_BID_MULT = 1.5
MIN_BOOK_SHARES = 1.0

# pricing/execution
DEFAULT_MAKER_OFFSET = 0.01
DEFAULT_TICK_SIZE = 0.01

# refresh cadence
MARKET_REFRESH_SEC = 20
REDEEM_INTERVAL_SEC = 60   # how often to scan for redeemable positions

# WebSocket
WS_URL              = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_RECONNECT_SEC    = 3
WS_PING_INTERVAL    = 10
WS_TOKEN_CHECK_SEC  = 2.0   # how often to check for token changes while connected

# BS model params
BS_VOL_WINDOW = 50
EWMA_LAMBDA = 0.85
MAX_DRIFT_SIGMAS = 1.0

# storage
DB_PATH_DEFAULT = Path("experiment") / "eth_5min.db"
LOG_PATH_DEFAULT = Path("experiment") / "logs" / "eth_5min.log"

ETH_5M_RE = re.compile(r"^eth-updown-5m-(\d+)$", re.IGNORECASE)


@dataclass
class Market:
    slug: str
    candle_start: int
    candle_end: int
    up_token: str
    down_token: str
    price_to_beat: float | None
    maker_fee_bps: int
    taker_fee_bps: int


@dataclass
class Signal:
    slug: str
    side: str
    token_id: str
    ask: float
    bid: float | None
    opp_bid: float | None
    opp_ask: float | None
    size_at_ask: float
    secs_remaining: int
    fair_price: float
    edge: float
    submit_price: float
    taker_net_edge: float
    maker_fee_bps: int
    taker_fee_bps: int


_bs_cache: dict[str, Any] = {"sigma": None, "mu": 0.0, "candle_open": {}}
_bs_updated_at = 0
_spot_cache: float | None = None

# Binance WS state — rolling close buffer for EWMA vol
_binance_closes: list[float] = []   # closed 5m candle closes, newest at end

# Polymarket WebSocket shared state — _book_cache updated by WS thread, read by main thread.
# Dict read/write is GIL-safe; _ws_tokens mutation uses _ws_lock.
_book_cache: dict[str, dict] = {}   # token_id → {"bids": [...], "asks": [...]}
_ws_tokens: list[str] = []
_ws_lock = threading.Lock()


def _init_logging(log_path: Path) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    log.addHandler(fh)


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS eth_5min_orders (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            placed_at        INTEGER NOT NULL,
            slug             TEXT NOT NULL,
            side             TEXT NOT NULL,
            token_id         TEXT NOT NULL,
            ask_price        REAL NOT NULL,
            submit_price     REAL NOT NULL,
            fair_price       REAL NOT NULL,
            edge             REAL NOT NULL,
            taker_net_edge   REAL,
            shares           REAL NOT NULL,
            maker_fee_bps    INTEGER NOT NULL DEFAULT 0,
            taker_fee_bps    INTEGER NOT NULL DEFAULT 0,
            order_id         TEXT,
            order_status     TEXT,
            dry_run          INTEGER NOT NULL DEFAULT 0,
            response_json    TEXT,
            error            TEXT,
            won              INTEGER,
            pnl              REAL,
            winner           TEXT,
            resolved_at      INTEGER,
            UNIQUE(slug, side)
        )
        """
    )
    # Migrate existing DBs that lack the outcome columns
    for col, typedef in [("won", "INTEGER"), ("pnl", "REAL"), ("winner", "TEXT"), ("resolved_at", "INTEGER")]:
        try:
            conn.execute(f"ALTER TABLE eth_5min_orders ADD COLUMN {col} {typedef}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    return conn


def _order_already_attempted(conn: sqlite3.Connection, slug: str, side: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM eth_5min_orders WHERE slug=? AND side=? LIMIT 1",
        (slug, side),
    ).fetchone()
    return row is not None


def _record_attempt(
    conn: sqlite3.Connection,
    signal: Signal,
    shares: float,
    dry_run: bool,
    order_id: str | None,
    order_status: str | None,
    response: dict | None,
    error: str | None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO eth_5min_orders (
            placed_at, slug, side, token_id, ask_price, submit_price,
            fair_price, edge, taker_net_edge, shares,
            maker_fee_bps, taker_fee_bps,
            order_id, order_status, dry_run, response_json, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            signal.slug,
            signal.side,
            signal.token_id,
            signal.ask,
            signal.submit_price,
            signal.fair_price,
            signal.edge,
            signal.taker_net_edge,
            shares,
            signal.maker_fee_bps,
            signal.taker_fee_bps,
            order_id,
            order_status,
            1 if dry_run else 0,
            json.dumps(response or {}, ensure_ascii=True),
            error,
        ),
    )
    conn.commit()


def _derive_clob_credentials() -> None:
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    creds = derive_api_credentials(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def _build_client_from_env():
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    return build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )


def _get_json(url: str, *, params: dict | None = None, timeout: int = 12) -> Any:
    r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.json()


def _post_json(url: str, payload: Any, timeout: int = 12) -> Any:
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _parse_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_json_list(v: Any) -> list:
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return v if isinstance(v, list) else []


def _event_to_market(event: dict, now_ts: int) -> Market | None:
    slug = str(event.get("slug") or "")
    m = ETH_5M_RE.match(slug)
    if not m:
        return None

    markets = event.get("markets") or []
    if not markets:
        return None
    mk = markets[0] if isinstance(markets[0], dict) else None
    if not mk:
        return None

    outcomes = _load_json_list(mk.get("outcomes"))
    token_ids = _load_json_list(mk.get("clobTokenIds"))
    if not outcomes or not token_ids or len(outcomes) != len(token_ids):
        return None

    up_token = down_token = None
    for i, outcome in enumerate(outcomes):
        label = str(outcome).strip().lower()
        if label == "up":
            up_token = str(token_ids[i])
        elif label == "down":
            down_token = str(token_ids[i])

    if not up_token or not down_token:
        return None

    try:
        end_str = mk.get("endDate") or event.get("endDate") or ""
        dt = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        end_ts = int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())
    except Exception:
        return None

    candle_start = int(m.group(1))
    if end_ts <= now_ts or end_ts - now_ts > 1200:
        return None

    return Market(
        slug=slug,
        candle_start=candle_start,
        candle_end=end_ts,
        up_token=up_token,
        down_token=down_token,
        price_to_beat=_parse_float(mk.get("priceToBeat") or mk.get("price_to_beat")),
        maker_fee_bps=0,
        taker_fee_bps=0,
    )


def _candidate_slugs(now_ts: int) -> list[str]:
    bucket = (now_ts // 300) * 300
    return [f"eth-updown-5m-{bucket + d * 300}" for d in (-1, 0, 1, 2)]


def _fetch_active_eth_market() -> Market | None:
    now_ts = int(time.time())
    best: Market | None = None

    for slug in _candidate_slugs(now_ts):
        try:
            data = _get_json(GAMMA_API, params={"slug": slug})
            if isinstance(data, list) and data:
                m = _event_to_market(data[0], now_ts)
                if m and (best is None or m.candle_end < best.candle_end):
                    best = m
        except Exception:
            continue

    if best is not None:
        return best

    # fallback scan
    try:
        for page in range(6):
            data = _get_json(GAMMA_API, params={"closed": "false", "limit": 200, "offset": page * 200})
            if not isinstance(data, list) or not data:
                break
            for ev in data:
                if not isinstance(ev, dict):
                    continue
                m = _event_to_market(ev, now_ts)
                if m and (best is None or m.candle_end < best.candle_end):
                    best = m
            if best is not None:
                break
    except Exception:
        pass

    return best


def _fetch_books(host: str, token_ids: list[str]) -> dict[str, dict]:
    if not token_ids:
        return {}
    try:
        payload = [{"token_id": tid} for tid in token_ids]
        books = _post_json(f"{host}/books", payload, timeout=10)
        out: dict[str, dict] = {}
        for book in books:
            tid = str(book.get("asset_id") or book.get("token_id") or "")
            if not tid:
                continue
            out[tid] = {
                "bids": list(book.get("bids") or []),
                "asks": list(book.get("asks") or []),
            }
        return out
    except Exception as exc:
        log.warning("Book fetch failed: %s", exc)
        return {}


def _best_ask_with_size(book: dict) -> tuple[float | None, float]:
    best_p = None
    best_s = 0.0
    for a in book.get("asks") or []:
        try:
            p = float(a.get("price"))
            s = float(a.get("size"))
        except Exception:
            continue
        if best_p is None or p < best_p:
            best_p = p
            best_s = s
        elif p == best_p:
            best_s += s
    return best_p, best_s


def _best_bid(book: dict) -> float | None:
    best = None
    for b in book.get("bids") or []:
        try:
            p = float(b.get("price"))
        except Exception:
            continue
        if best is None or p > best:
            best = p
    return best


def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def _estimate_vol(closes: list[float], lam: float = EWMA_LAMBDA) -> float:
    if len(closes) < 2:
        return 0.001
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i] > 0 and closes[i - 1] > 0]
    if not rets:
        return 0.001
    var = rets[0] ** 2
    for r in rets[1:]:
        var = lam * var + (1.0 - lam) * r * r
    return math.sqrt(var)


def _estimate_drift(closes: list[float], sigma: float, lam: float = EWMA_LAMBDA) -> float:
    if len(closes) < 2:
        return 0.0
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i] > 0 and closes[i - 1] > 0]
    if not rets:
        return 0.0
    mu = rets[0]
    for r in rets[1:]:
        mu = lam * mu + (1.0 - lam) * r
    cap = MAX_DRIFT_SIGMAS * sigma if sigma > 0 else 0.0
    if cap > 0:
        mu = max(-cap, min(cap, mu))
    return mu


def _bootstrap_bs_data() -> None:
    """One-time REST fetch to seed the close buffer and candle_open before WS takes over."""
    global _bs_updated_at
    try:
        rows = _get_json(
            f"{BINANCE_BASE}/klines",
            params={"symbol": "ETHUSDT", "interval": "5m", "limit": BS_VOL_WINDOW + 2},
        )
        if not rows:
            return
        closed = rows[:-1] if len(rows) > 1 else rows
        closes = [float(r[4]) for r in closed if float(r[4]) > 0]
        # seed the rolling buffer so vol is available immediately
        _binance_closes.clear()
        _binance_closes.extend(closes)

        sigma = _estimate_vol(closes)
        mu = _estimate_drift(closes, sigma=sigma)
        live_open = float(rows[-1][1])
        open_time = int(rows[-1][0] // 1000)

        _bs_cache["sigma"] = sigma
        _bs_cache["mu"] = mu
        _bs_cache["candle_open"] = {open_time: live_open}
        _bs_updated_at = int(time.time())

        # also seed spot from the latest close so we're not blind before WS connects
        global _spot_cache
        if not _spot_cache and closes:
            _spot_cache = closes[-1]

        log.info("BS bootstrap ETH  sigma_5m=%.5f  mu_5m=%+.5f  candle_open=%.4f", sigma, mu, live_open)
    except Exception as exc:
        log.warning("BS bootstrap failed: %s", exc)


async def _binance_ws_loop_async() -> None:
    """Asyncio loop: streams ethusdt@kline_5m + ethusdt@trade from Binance WS.

    - trade events → update _spot_cache
    - kline events (closed) → append close to _binance_closes, recompute vol/mu
    - kline events (open) → update _bs_cache["candle_open"] for current bar
    """
    import websockets
    global _spot_cache, _bs_updated_at

    streams = "ethusdt@kline_5m/ethusdt@trade"
    url = f"{BINANCE_WS_URL}?streams={streams}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2 ** 20) as ws:
                log.info("Binance WS connected (%s)", streams)
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    data = msg.get("data") or msg
                    event_type = data.get("e")

                    if event_type == "trade":
                        price = _parse_float(data.get("p"))
                        if price and price > 0:
                            _spot_cache = price

                    elif event_type == "kline":
                        k = data.get("k") or {}
                        close = _parse_float(k.get("c"))
                        open_price = _parse_float(k.get("o"))
                        open_time_ms = k.get("t")
                        is_closed = bool(k.get("x", False))

                        if open_price and open_price > 0 and open_time_ms:
                            open_time = int(open_time_ms) // 1000
                            _bs_cache["candle_open"] = {open_time: open_price}

                        if is_closed and close and close > 0:
                            _binance_closes.append(close)
                            if len(_binance_closes) > BS_VOL_WINDOW + 2:
                                _binance_closes.pop(0)
                            if len(_binance_closes) >= 2:
                                sigma = _estimate_vol(_binance_closes)
                                mu = _estimate_drift(_binance_closes, sigma=sigma)
                                _bs_cache["sigma"] = sigma
                                _bs_cache["mu"] = mu
                                _bs_updated_at = int(time.time())
                                log.info(
                                    "BS vol update (WS)  sigma_5m=%.5f  mu_5m=%+.5f  closes=%d",
                                    sigma, mu, len(_binance_closes),
                                )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Binance WS disconnected (%s: %s) — reconnecting in %ds",
                type(exc).__name__, exc, BINANCE_WS_RECONNECT_SEC,
            )
            await asyncio.sleep(BINANCE_WS_RECONNECT_SEC)


def _binance_ws_thread_main() -> None:
    asyncio.run(_binance_ws_loop_async())


def _bs_prob_up(spot: float, strike: float, secs_remaining: int, sigma_5m: float, mu_5m: float) -> float:
    t_bars = max(1e-9, secs_remaining / 300.0)
    if sigma_5m <= 1e-9 or spot <= 0 or strike <= 0:
        return 1.0 if spot > strike else (0.0 if spot < strike else 0.5)
    d2 = (math.log(spot / strike) + (mu_5m - 0.5 * sigma_5m * sigma_5m) * t_bars) / (sigma_5m * math.sqrt(t_bars))
    return _norm_cdf(d2)


def _get_bs_inputs(candle_start: int) -> tuple[float | None, float | None, float | None, float]:
    sigma = _bs_cache.get("sigma")
    mu = float(_bs_cache.get("mu") or 0.0)
    candle_open_map = _bs_cache.get("candle_open") or {}
    strike = candle_open_map.get(candle_start)
    return _spot_cache, strike, sigma, mu


def _ws_apply_price_change(book: dict, changes: list[dict]) -> None:
    """Apply incremental level updates to a cached book in place."""
    for ch in changes or []:
        try:
            p  = float(ch["price"])
            s  = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


def _ws_handle_event(ev: dict) -> None:
    tid = str(ev.get("asset_id") or "")
    if not tid:
        return
    et = ev.get("event_type")
    if et == "book":
        _book_cache[tid] = {
            "bids": [dict(b) for b in (ev.get("bids") or [])],
            "asks": [dict(a) for a in (ev.get("asks") or [])],
        }
    elif et == "price_change":
        book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
        _ws_apply_price_change(book, ev.get("changes") or [])


async def _ws_loop_async() -> None:
    import websockets
    subscribed: list[str] = []
    while True:
        with _ws_lock:
            want = list(_ws_tokens)

        if not want:
            await asyncio.sleep(0.5)
            continue

        if want != subscribed:
            subscribed = want
            log.info("WS subscribing to %d tokens", len(subscribed))

        msg = json.dumps({"auth": {}, "type": "market", "assets_ids": subscribed})
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_INTERVAL,
                max_size=2 ** 22,
            ) as ws:
                await ws.send(msg)
                log.info("WS connected")
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_TOKEN_CHECK_SEC)
                    except asyncio.TimeoutError:
                        with _ws_lock:
                            new_want = list(_ws_tokens)
                        if new_want != subscribed:
                            log.info("WS token list changed — reconnecting")
                            break
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    events = payload if isinstance(payload, list) else [payload]
                    for ev in events:
                        if isinstance(ev, dict):
                            _ws_handle_event(ev)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WS disconnected (%s: %s) — reconnecting in %ds",
                        type(exc).__name__, exc, WS_RECONNECT_SEC)
            await asyncio.sleep(WS_RECONNECT_SEC)


def _ws_thread_main() -> None:
    asyncio.run(_ws_loop_async())


def _ws_subscribe(token_ids: list[str]) -> None:
    """Update the desired subscription list; the WS thread reconnects automatically."""
    with _ws_lock:
        _ws_tokens[:] = token_ids


def _get_cached_books(token_ids: list[str]) -> dict[str, dict]:
    """Return the latest cached order books for the given token IDs."""
    return {tid: _book_cache.get(tid, {"bids": [], "asks": []}) for tid in token_ids}


def _round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 3)
    ticks = round(price / tick)
    return round(max(tick, ticks * tick), 6)


def _choose_submit_price(ask: float, bid: float | None, *, maker_offset: float, tick: float, max_price: float) -> float:
    p = ask
    if bid is not None:
        # maker-leaning: join/improve bid if spread allows; otherwise sit at ask.
        candidate = min(ask, bid + max(maker_offset, tick))
        p = candidate
    p = min(p, max_price)
    p = _round_to_tick(p, tick)
    if p > ask:
        p = _round_to_tick(ask, tick)
    return p


def _fee_cost(price: float, fee_bps: int) -> float:
    return price * (max(0, fee_bps) / 10_000.0)


def _evaluate_signals(
    market: Market,
    books: dict[str, dict],
    *,
    min_edge: float,
    max_edge: float,
    max_price: float,
    min_net_edge_taker: float,
    maker_offset: float,
    tick_size: float,
    verbose: bool,
) -> list[Signal]:
    now = int(time.time())
    secs = market.candle_end - now

    up_book = books.get(market.up_token, {})
    down_book = books.get(market.down_token, {})

    up_ask, up_size = _best_ask_with_size(up_book)
    down_ask, down_size = _best_ask_with_size(down_book)
    up_bid = _best_bid(up_book)
    down_bid = _best_bid(down_book)

    if up_ask is None and down_ask is None:
        return []

    spot, strike_bn, sigma, mu = _get_bs_inputs(market.candle_start)
    strike = market.price_to_beat or strike_bn
    if not (spot and strike and sigma and secs > 0):
        return []

    fair_up = _bs_prob_up(spot, strike, max(1, secs), sigma, mu)

    signals: list[Signal] = []
    in_window = ENTRY_MIN_SECS <= secs <= ENTRY_MAX_SECS

    for side, token_id, ask, size, bid, opp_bid, opp_ask in (
        ("Up", market.up_token, up_ask, up_size, up_bid, down_bid, down_ask),
        ("Down", market.down_token, down_ask, down_size, down_bid, up_bid, up_ask),
    ):
        if ask is None or size < MIN_BOOK_SHARES:
            continue

        fair = fair_up if side == "Up" else (1.0 - fair_up)
        edge = fair - ask
        submit = _choose_submit_price(
            ask,
            bid,
            maker_offset=maker_offset,
            tick=tick_size,
            max_price=max_price,
        )
        taker_net = fair - submit - _fee_cost(submit, market.taker_fee_bps)

        if verbose:
            log.info(
                "TICK %-4s ask=%.4f bid=%s opp_bid=%s fair=%.4f edge=%+.4f submit=%.4f taker_net=%+.4f secs=%d",
                side,
                ask,
                f"{bid:.4f}" if bid is not None else "n/a",
                f"{opp_bid:.4f}" if opp_bid is not None else "n/a",
                fair,
                edge,
                submit,
                taker_net,
                max(0, secs),
            )

        if not in_window:
            continue
        if ask < MIN_ENTRY_PRICE:
            continue
        if ask > max_price:
            continue
        if edge < min_edge or edge > max_edge:
            continue
        if opp_bid is None or opp_bid < ask * OPPOSITE_BID_MULT:
            continue
        if taker_net < min_net_edge_taker:
            continue

        signals.append(
            Signal(
                slug=market.slug,
                side=side,
                token_id=token_id,
                ask=ask,
                bid=bid,
                opp_bid=opp_bid,
                opp_ask=opp_ask,
                size_at_ask=size,
                secs_remaining=secs,
                fair_price=fair,
                edge=edge,
                submit_price=submit,
                taker_net_edge=taker_net,
                maker_fee_bps=market.maker_fee_bps,
                taker_fee_bps=market.taker_fee_bps,
            )
        )

    return signals


def _calc_shares(desired: float, max_spend_usdc: float | None, price: float, balance: float | None) -> float:
    cap = balance if balance is not None else float("inf")
    if max_spend_usdc is not None:
        cap = min(cap, max_spend_usdc)
    affordable = cap / max(price, 1e-9)
    use = min(desired, affordable)
    return round(max(0.0, use), 2)


def _submit_limit_buy(client, signal: Signal, shares: float) -> dict:
    args = OrderArgs(
        token_id=signal.token_id,
        price=signal.submit_price,
        size=shares,
        side=BUY,
        # Use taker bps as max fee budget; order may still get maker execution.
        fee_rate_bps=max(0, int(signal.taker_fee_bps)),
    )
    signed = client.create_order(args)
    return client.post_order(signed, OrderType.GTC, post_only=False)


def _parse_winner(event: dict) -> str | None:
    """Return 'Up' or 'Down' if the market is resolved, else None."""
    markets = event.get("markets") or []
    if not markets:
        return None
    mk = markets[0] if isinstance(markets[0], dict) else None
    if not mk:
        return None
    outcomes   = _load_json_list(mk.get("outcomes"))
    prices_raw = mk.get("outcomePrices") or "[]"
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except Exception:
        return None
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None
    for outcome, price in zip(outcomes, prices):
        try:
            if float(price) >= 0.99:
                return str(outcome).strip()
        except (ValueError, TypeError):
            continue
    return None


def _settle_orders(conn: sqlite3.Connection) -> None:
    """Check unresolved orders and update won/pnl once the candle has resolved."""
    now = int(time.time())
    rows = conn.execute(
        """SELECT slug, side, shares, submit_price
           FROM eth_5min_orders
           WHERE won IS NULL AND error IS NULL"""
    ).fetchall()

    for row in rows:
        slug = row["slug"]
        m = ETH_5M_RE.match(slug)
        if not m:
            continue
        candle_end = int(m.group(1)) + 300
        if now < candle_end + 30:   # wait 30s after expected close
            continue
        try:
            data = _get_json(GAMMA_API, params={"slug": slug})
            if not isinstance(data, list) or not data:
                continue
            winner_side = _parse_winner(data[0])
            if winner_side is None:
                continue
            won = 1 if winner_side == row["side"] else 0
            pnl = round(
                row["shares"] * (1.0 - row["submit_price"]) if won
                else -row["shares"] * row["submit_price"],
                4,
            )
            conn.execute(
                """UPDATE eth_5min_orders
                   SET won=?, pnl=?, winner=?, resolved_at=?
                   WHERE slug=? AND side=?""",
                (won, pnl, winner_side, now, slug, row["side"]),
            )
            conn.commit()
            log.info(
                "SETTLED %s %s  won=%d  pnl=%+.4f  winner=%s",
                slug, row["side"], won, pnl, winner_side,
            )
        except Exception as exc:
            log.debug("Settlement check failed for %s: %s", slug, exc)


def run_eth_5min(
    *,
    bet: bool,
    poll: float,
    shares: float,
    min_edge: float,
    max_edge: float,
    max_price: float,
    min_net_edge_taker: float,
    maker_offset: float,
    tick_size: float,
    db_path: Path,
    host: str,
    once: bool,
    max_spend_usdc: float | None,
    verbose: bool,
) -> None:
    conn = _init_db(db_path)

    if bet:
        _derive_clob_credentials()
        client = _build_client_from_env()
    else:
        client = None

    # Polymarket order-book WS
    ws_thread = threading.Thread(target=_ws_thread_main, daemon=True, name="eth5m-ws")
    ws_thread.start()

    # Binance kline + trade WS (replaces REST polling for spot/vol)
    binance_thread = threading.Thread(target=_binance_ws_thread_main, daemon=True, name="eth5m-binance-ws")
    binance_thread.start()

    # Bootstrap: seed close buffer and vol from REST so we don't wait for 50 closed klines
    _bootstrap_bs_data()

    market: Market | None = None
    last_market_refresh = 0
    last_redeem_at = 0.0

    log.info(
        "Started ETH 5m scanner  bet=%s  poll=%.1fs  min_edge=%.3f  max_edge=%.3f  max_price=%.3f  min_net_taker=%.4f",
        bet,
        poll,
        min_edge,
        max_edge,
        max_price,
        min_net_edge_taker,
    )

    while True:
        now = int(time.time())

        if market is None or now >= market.candle_end or (now - last_market_refresh) >= MARKET_REFRESH_SEC:
            prev_slug = market.slug if market else None
            market = _fetch_active_eth_market()
            last_market_refresh = now
            if market:
                if market.slug != prev_slug:
                    log.info(
                        "MARKET %s  ends_in=%ds  maker_fee_bps=%d taker_fee_bps=%d",
                        market.slug,
                        max(0, market.candle_end - now),
                        market.maker_fee_bps,
                        market.taker_fee_bps,
                    )
                    _ws_subscribe([market.up_token, market.down_token])
            else:
                log.warning("No active ETH 5m market found")

        if not market:
            if once:
                break
            time.sleep(max(0.2, poll))
            continue

        books = _get_cached_books([market.up_token, market.down_token])
        signals = _evaluate_signals(
            market,
            books,
            min_edge=min_edge,
            max_edge=max_edge,
            max_price=max_price,
            min_net_edge_taker=min_net_edge_taker,
            maker_offset=maker_offset,
            tick_size=tick_size,
            verbose=verbose,
        )

        for sig in signals:
            if _order_already_attempted(conn, sig.slug, sig.side):
                continue

            bal = None
            if bet and client is not None:
                try:
                    bal = get_usdc_balance(client)
                except Exception as exc:
                    log.warning("Balance fetch failed: %s", exc)

            use_shares = _calc_shares(shares, max_spend_usdc=max_spend_usdc, price=sig.submit_price, balance=bal)
            if use_shares < 0.01:
                log.warning("SKIP %s %s: insufficient buying power", sig.slug, sig.side)
                continue

            log.info(
                "SIGNAL %s %-4s ask=%.4f submit=%.4f fair=%.4f edge=%+.4f taker_net=%+.4f secs=%d shares=%.2f",
                sig.slug,
                sig.side,
                sig.ask,
                sig.submit_price,
                sig.fair_price,
                sig.edge,
                sig.taker_net_edge,
                max(0, sig.secs_remaining),
                use_shares,
            )

            if not bet or client is None:
                _record_attempt(
                    conn,
                    sig,
                    shares=use_shares,
                    dry_run=True,
                    order_id="DRY_RUN",
                    order_status="dry_run",
                    response={
                        "slug": sig.slug,
                        "side": sig.side,
                        "submit_price": sig.submit_price,
                        "shares": use_shares,
                    },
                    error=None,
                )
                continue

            try:
                resp = _submit_limit_buy(client, sig, use_shares)
                order_id = str(resp.get("orderID") or resp.get("id") or "?")
                status = str(resp.get("status") or "submitted")
                log.info("ORDER %s %-4s id=%s status=%s", sig.slug, sig.side, order_id, status)
                _record_attempt(
                    conn,
                    sig,
                    shares=use_shares,
                    dry_run=False,
                    order_id=order_id,
                    order_status=status,
                    response=resp,
                    error=None,
                )
            except Exception as exc:
                log.error("ORDER FAILED %s %-4s: %s", sig.slug, sig.side, exc)
                _record_attempt(
                    conn,
                    sig,
                    shares=use_shares,
                    dry_run=False,
                    order_id=None,
                    order_status="error",
                    response=None,
                    error=str(exc),
                )

        _settle_orders(conn)

        # Redeem resolved positions from the wallet (live mode only)
        now_mono = time.monotonic()
        if bet and now_mono - last_redeem_at >= REDEEM_INTERVAL_SEC:
            try:
                from automata.eth_1h import _settle_resolved_trades
                _settle_resolved_trades(
                    private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                    rpc_url=os.getenv("POLYGON_RPC_URL"),
                    redeem_mode="relayer",
                )
            except Exception as exc:
                log.warning("Redeem scan failed: %s", exc)
            last_redeem_at = now_mono

        if once:
            break

        time.sleep(max(0.2, poll))


def main() -> None:
    load_dotenv()
    p = argparse.ArgumentParser(description="ETH 5m scanner/trader (scanner-style logic)")
    p.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--poll", type=float, default=DEFAULT_POLL, help="Seconds between evaluations")
    p.add_argument("--shares", type=float, default=DEFAULT_SHARES, help="Target shares per signal")
    p.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE, help="Min BS edge (fair-ask)")
    p.add_argument("--max-edge", type=float, default=DEFAULT_MAX_EDGE, help="Max BS edge before treating as model failure")
    p.add_argument("--max-price", type=float, default=DEFAULT_MAX_PRICE, help="Do not enter above this price")
    p.add_argument(
        "--min-net-edge-taker",
        type=float,
        default=DEFAULT_MIN_NET_EDGE_TAKER,
        help="Minimum fair-submit edge after worst-case taker fee",
    )
    p.add_argument("--maker-offset", type=float, default=DEFAULT_MAKER_OFFSET, help="Maker-leaning bid improvement amount")
    p.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE, help="Price tick rounding")
    p.add_argument("--db", type=Path, default=DB_PATH_DEFAULT, help="SQLite DB path")
    p.add_argument("--log", type=Path, default=LOG_PATH_DEFAULT, help="Log file path")
    p.add_argument("--host", default=os.getenv("POLYMARKET_HOST", CLOB_HOST_DEFAULT), help="Polymarket CLOB host")
    p.add_argument("--max-balance", type=float, default=None, help="Max USDC this process may spend")
    p.add_argument("--no-verbose", dest="verbose", action="store_false", help="Suppress per-tick TICK log lines")
    p.set_defaults(verbose=True)
    args = p.parse_args()

    _init_logging(args.log)

    run_eth_5min(
        bet=args.bet,
        poll=max(0.2, args.poll),
        shares=max(0.01, args.shares),
        min_edge=args.min_edge,
        max_edge=args.max_edge,
        max_price=args.max_price,
        min_net_edge_taker=args.min_net_edge_taker,
        maker_offset=max(0.0, args.maker_offset),
        tick_size=max(0.01, args.tick_size),
        db_path=args.db,
        host=args.host,
        once=args.once,
        max_spend_usdc=args.max_balance,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
