#!/usr/bin/env python3
"""
Crypto 5m penny scanner — records signals across ALL active crypto Up/Down 5m markets.

For every *-updown-5m-* market active on Polymarket:
  - Polls order books in bulk every POLL_INTERVAL seconds
  - When either side's best ask <= max_price, records a signal (cheapest ask seen)
  - After the candle closes, resolves the winner and writes P/L

DB:    experiment/crypto_5m.db
Start: python -m experiment.crypto_5m_scanner
       python -m experiment.crypto_5m_scanner --max-price 0.03 --poll 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import math

import requests
import websockets

# ── constants ─────────────────────────────────────────────────────────────────

GAMMA_API        = "https://gamma-api.polymarket.com/events"
CLOB_HOST        = "https://clob.polymarket.com"
BINANCE_BASE     = "https://api.binance.com/api/v3"
DB_PATH          = os.path.join("experiment", "crypto_5m.db")
LOG_PATH         = os.path.join("experiment", "logs", "crypto_5m_scanner.log")
DEFAULT_MAX      = 0.03   # kept for CLI compatibility/logging
DEFAULT_MIN_EDGE = 0.03   # minimum BS edge to record a signal
BASE_SHARES      = 5.0    # flat shares per signal
MIN_SHARES       = 5.0    # minimum shares required to place/update a signal
MAX_SHARES_MULT  = 1.0    # flat sizing — dynamic multiplier disabled
BOOK_SIZE_FRACTION = 0.8  # never size above this fraction of top-of-book size
EDGE_RAMP_WIDTH  = 0.10   # edge above min_edge needed to reach max share multiplier
REPRICE_MIN_EDGE_IMPROV = 0.02  # edge improvement needed to replace prior signal
REPRICE_MIN_PRICE_DROP  = 0.01  # ask improvement needed to replace prior signal
SLUG_SHARE_CAP   = 20.0   # max total shares per 5m slug across both sides
MIN_BOOK_SHARES  = 1      # minimum shares needed in order book to fire
POLL_INTERVAL    = 1      # seconds between signal evaluations (reads local WS cache)
WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_RECONNECT_SEC = 3      # delay before reconnecting on WS failure
WS_PING_INTERVAL = 10     # keepalive ping interval
WS_INITIAL_WARMUP_SEC = 2 # after subscribing, wait this long for first book snapshots
REFRESH_INTERVAL = 60     # seconds between market-list refreshes
RESOLVE_INTERVAL = 30     # seconds between resolution scans
RESOLVE_TIMEOUT  = 7200   # give up resolving after this many seconds post-close (2 hrs)
BS_VOL_WINDOW    = 50     # trailing 5m bars for vol estimation
BS_VOL_REFRESH   = 300    # seconds between per-asset vol refreshes
SECS_PER_YEAR    = 365 * 24 * 3600
ENTRY_MIN_SECS   = 40    # skip signals with < 40 s left (model unreliable near expiry)
ENTRY_MAX_SECS   = 240   # skip signals with > 240 s left (outcome not determined)
MIN_ENTRY_PRICE  = 0.08  # skip asks below this (cheap-premium trap, model over-confident)
MAX_EDGE         = 0.12  # skip signals with edge above this (treat as model failure)
MAX_DRIFT_SIGMAS = 1.0   # cap |μ| at this many σ/bar to prevent runaway drift estimates
OPPOSITE_BID_MULT = 1.5  # opposite side bid must be >= ask × this to confirm cheapness
EWMA_LAMBDA      = 0.85  # RiskMetrics vol smoothing; 0.85 gives ~7-bar memory (35 min),
                         # more responsive to current 5m vol regime than the daily-data 0.94

# Polymarket asset  →  Binance symbol
_BINANCE_SYMBOLS: dict[str, str] = {
    "BTC":   "BTCUSDT",
    "ETH":   "ETHUSDT",
    "XRP":   "XRPUSDT",
    "BNB":   "BNBUSDT",
    "SOL":   "SOLUSDT",
    "DOGE":  "DOGEUSDT",
    "LINK":  "LINKUSDT",
    "AVAX":  "AVAXUSDT",
    "MATIC": "MATICUSDT",
    "DOT":   "DOTUSDT",
    "LTC":   "LTCUSDT",
    "HYPER": "HYPERUSDT",
}

UPDOWN_5M_RE = re.compile(r"^([a-z0-9]+)-updown-5m-(\d+)$", re.IGNORECASE)

# Seed assets — expanded automatically as new slugs are discovered
_SEED_ASSETS = ["btc"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto5m.scanner")
os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
_file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
log.addHandler(_file_handler)


# ── Black-Scholes binary option ────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc (no scipy needed)."""
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _bs_prob_up(S: float, K: float, T_secs: float, sigma_5m: float, mu_5m: float = 0.0) -> float:
    """
    Probability that price ends above K under GBM with per-bar drift μ.

    S        — current spot price
    K        — strike (5m candle open price)
    T_secs   — seconds remaining until expiry
    sigma_5m — per-bar std dev of log returns (5m, NOT annualised)
    mu_5m    — per-bar drift of log returns (5m, NOT annualised); 0 = martingale.
               When nonzero, tilts the distribution in the direction of recent momentum.

    Time is expressed as T_bars = T_secs / 300 (number of 5m bars remaining),
    so σ, μ and T are all in the same units — no annualisation needed.
    """
    T_bars = T_secs / 300.0          # e.g. 150 s remaining → 0.5 bars
    if T_bars <= 0 or sigma_5m <= 1e-9 or S <= 0 or K <= 0:
        return 1.0 if S > K else (0.5 if S == K else 0.0)
    try:
        d2 = (math.log(S / K) + (mu_5m - 0.5 * sigma_5m ** 2) * T_bars) / (sigma_5m * math.sqrt(T_bars))
        return _norm_cdf(d2)
    except (ValueError, ZeroDivisionError):
        return 0.5


def _estimate_vol(closes: list[float], lam: float = EWMA_LAMBDA) -> float:
    """
    Per-bar (5m) vol via EWMA (RiskMetrics).  More responsive to recent vol
    clusters than a simple rolling std dev — if BTC just moved hard, the next
    candle's uncertainty is higher and this reflects it.

    lam=0.94 weights the last bar at 6%, the bar before at 5.6%, etc.
    Returns raw per-bar std dev (NOT annualised).
    """
    if len(closes) < 2:
        return 0.001
    log_rets = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    if not log_rets:
        return 0.001
    var = log_rets[0] ** 2
    for r in log_rets[1:]:
        var = lam * var + (1 - lam) * r ** 2
    return math.sqrt(var)


def _estimate_drift(closes: list[float], lam: float = EWMA_LAMBDA,
                    sigma: float | None = None) -> float:
    """
    Per-bar (5m) drift μ — EWMA of log returns, same smoothing as _estimate_vol.

    λ=0.85 puts ~15% weight on the newest bar, decaying geometrically. Bars
    1–6 carry ~62% of the total weight, so recent pushes dominate but older
    history still anchors the estimate.

    Capped at ±MAX_DRIFT_SIGMAS × σ/bar so a single outlier can't dominate.
    Returning 0 if we don't have enough data falls back to the martingale BS.
    """
    if len(closes) < 2:
        return 0.0
    log_rets = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    if not log_rets:
        return 0.0
    mu = log_rets[0]
    for r in log_rets[1:]:
        mu = lam * mu + (1 - lam) * r
    if sigma is not None and sigma > 0:
        cap = MAX_DRIFT_SIGMAS * sigma
        mu = max(-cap, min(cap, mu))
    return mu


# ── BS data cache ─────────────────────────────────────────────────────────────
#
# Two separate refresh rates:
#   _bs_cache[asset]  — sigma + candle_open  (slow: every BS_VOL_REFRESH secs)
#   _spot_cache[asset] — live ticker price   (fast: every poll cycle)
#
# The previous bug: spot came from the last *closed* 5m bar's close, which
# equals the current candle's open (K).  S ≈ K → P(Up) ≈ 50% always.
# Fix: fetch a live ticker price each poll so S reflects where price actually is.

_bs_cache:    dict[str, dict]  = {}   # asset → {"sigma", "candle_open": {epoch: price}}
_bs_updated_at: dict[str, int] = {}   # asset → last vol/candle-open refresh epoch
_spot_cache:  dict[str, float] = {}   # asset → latest live price (refreshed every poll)


def _refresh_bs_data(asset: str) -> None:
    """
    Fetch vol (sigma) and current candle-open (K) from Binance 5m klines.
    Called at most once every BS_VOL_REFRESH seconds — sigma and K are slow-moving.
    Does NOT set spot price (that's handled by _refresh_spots every poll cycle).
    """
    symbol = _BINANCE_SYMBOLS.get(asset)
    if not symbol:
        log.debug("No Binance symbol for %s — skipping BS data", asset)
        return
    try:
        r = requests.get(
            f"{BINANCE_BASE}/klines",
            params={"symbol": symbol, "interval": "5m", "limit": BS_VOL_WINDOW + 2},
            timeout=12,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        klines = r.json()
        if not klines:
            log.warning("BS data %s: empty klines response", asset)
            return

        # Use all closed bars (all-but-last) for vol — exclude the live candle.
        closed = klines[:-1] if len(klines) > 1 else klines
        closes = [float(k[4]) for k in closed]
        sigma  = _estimate_vol(closes)
        mu     = _estimate_drift(closes, sigma=sigma)

        # Candle open = open field of the current (last, live) kline.
        live_kline          = klines[-1]
        candle_start_epoch  = int(live_kline[0]) // 1000
        candle_open         = float(live_kline[1])

        _bs_cache[asset] = {
            "sigma":       sigma,
            "mu":          mu,
            "candle_open": {candle_start_epoch: candle_open},
        }
        _bs_updated_at[asset] = int(datetime.now(timezone.utc).timestamp())
        log.info(
            "BS vol   %-5s  σ_5m=%.5f (%.4f%%/bar)  μ_5m=%+.5f  K=%.4f",
            asset, sigma, sigma * 100, mu, candle_open,
        )
    except Exception as exc:
        log.warning("BS vol refresh %s: %s", asset, exc)


def _refresh_spots(assets: set[str]) -> None:
    """
    Fetch the current live price for each asset from Binance ticker/price.
    Called every poll cycle so S is always fresh.
    Uses individual single-symbol calls — the most reliable Binance endpoint.
    """
    for asset in assets:
        symbol = _BINANCE_SYMBOLS.get(asset)
        if not symbol:
            continue
        try:
            r = requests.get(
                f"{BINANCE_BASE}/ticker/price",
                params={"symbol": symbol},
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            r.raise_for_status()
            price = float(r.json()["price"])
            _spot_cache[asset] = price
        except Exception as exc:
            log.debug("Spot %s: %s", asset, exc)


def _get_bs_inputs(
    asset: str, candle_start: int,
) -> tuple[float | None, float | None, float | None, float]:
    """Return (live_spot, candle_open_K, sigma, mu) from cache, or (None, None, None, 0.0)."""
    cached = _bs_cache.get(asset)
    if not cached:
        return None, None, None, 0.0
    spot  = _spot_cache.get(asset)          # live price, refreshed every poll
    sigma = cached.get("sigma")
    mu    = float(cached.get("mu", 0.0) or 0.0)
    K     = cached.get("candle_open", {}).get(candle_start)
    # If the cached candle_open is for a different (older) candle_start, K is None.
    return spot, K, sigma, mu


# ── DB schema ─────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS candles (
    slug         TEXT PRIMARY KEY,
    asset        TEXT    NOT NULL,
    candle_start INTEGER NOT NULL,
    candle_end   INTEGER NOT NULL,
    up_token     TEXT    NOT NULL,
    down_token   TEXT    NOT NULL,
    winner       TEXT,               -- 'Up' / 'Down' / NULL (unresolved)
    resolved_at  INTEGER,
    first_seen   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    slug           TEXT    NOT NULL,
    asset          TEXT    NOT NULL,
    side           TEXT    NOT NULL,   -- 'Up' / 'Down'
    entry_price    REAL    NOT NULL,   -- actual ask at time of entry
    shares         REAL    NOT NULL DEFAULT 5,
    secs_remaining INTEGER,
    candle_start   INTEGER NOT NULL,
    signal_ts      INTEGER NOT NULL,
    fair_price     REAL    NOT NULL,   -- BS probability at entry
    edge           REAL    NOT NULL,   -- fair_price - entry_price
    sigma          REAL,               -- per-bar vol (5m) at signal time
    opp_ask        REAL,               -- opposite side best ask at signal time
    winner         TEXT,
    won            INTEGER,            -- 1 / 0 / NULL
    pnl            REAL,               -- shares*(1-entry_price) if won, shares*(-entry_price) if lost
    UNIQUE(slug, side)                 -- one signal per side per candle
);

CREATE INDEX IF NOT EXISTS idx_sig_asset ON signals(asset, candle_start);
CREATE INDEX IF NOT EXISTS idx_sig_resolved ON signals(won);
"""


def _init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Drop old tier-based schema if present so we recreate cleanly.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    if cols and "tier" in cols:
        log.info("Migrating signals table: removing tier column (old data cleared)")
        conn.execute("DROP TABLE IF EXISTS signals")
        cols = set()
    conn.executescript(_DDL)
    # Additive migrations: add new columns without losing existing data.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    for col, typedef in [("sigma", "REAL"), ("opp_ask", "REAL")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
            log.info("DB migration: added signals.%s column", col)
    conn.commit()
    return conn


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 12) -> Any:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _fetch_books_bulk(token_ids: list[str]) -> dict[str, dict]:
    """Kept for cold-start warmup. Main loop now reads from the WS book cache."""
    if not token_ids:
        return {}
    r = requests.post(
        f"{CLOB_HOST}/books",
        json=[{"token_id": tid} for tid in token_ids],
        timeout=15,
    )
    r.raise_for_status()
    out: dict[str, dict] = {}
    for book in r.json():
        tid = str(book.get("asset_id") or book.get("token_id") or "")
        if tid:
            out[tid] = book
    return out


# ── WebSocket book cache ─────────────────────────────────────────────────────
#
# Background thread maintains a live mirror of the CLOB order book per token_id.
# Main loop reads from this cache instead of polling /books.
#
# Message format (from Polymarket CLOB WS):
#   - "book"          : full snapshot; replace cached book
#   - "price_change"  : incremental level updates; apply to cached book
#   - "tick_size_change" / "last_trade_price" : ignored

_book_cache:     dict[str, dict] = {}
_book_lock:      threading.RLock = threading.RLock()
_ws_desired:     set[str]        = set()   # tokens we want subscribed
_ws_subscribed:  set[str]        = set()   # tokens confirmed subscribed on current conn
_ws_tokens_lock: threading.Lock  = threading.Lock()
_ws_state = {
    "connected":   False,
    "msg_count":   0,
    "last_msg_ts": 0,
    "last_error":  "",
}


def _ws_apply_price_change(book: dict, changes: list[dict]) -> None:
    """Apply incremental level updates to a cached book in place."""
    for ch in changes or []:
        try:
            p   = float(ch["price"])
            s   = float(ch["size"])
            sd  = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        target_key = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(target_key, [])
        levels[:] = [
            lv for lv in levels
            if abs(float(lv.get("price", 0) or 0) - p) > 1e-9
        ]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


def _ws_handle_event(ev: dict) -> None:
    et  = ev.get("event_type")
    tid = str(ev.get("asset_id") or "")
    if not tid:
        return
    with _book_lock:
        if et == "book":
            _book_cache[tid] = {
                "bids": [dict(b) for b in (ev.get("bids") or [])],
                "asks": [dict(a) for a in (ev.get("asks") or [])],
            }
        elif et == "price_change":
            book = _book_cache.get(tid)
            if book is None:
                book = {"bids": [], "asks": []}
                _book_cache[tid] = book
            _ws_apply_price_change(book, ev.get("changes") or [])


async def _ws_subscribe(ws, tokens: list[str]) -> None:
    """
    Subscribe to the CLOB market channel.

    Polymarket expects `{"auth": {}, "type": "market", "assets_ids": [...]}`
    (lowercase "market"; the auth field is required even for the public
    market channel — omitting it makes the server silently drop the sub).
    """
    if not tokens:
        return
    await ws.send(json.dumps({
        "auth": {},
        "type": "market",
        "assets_ids": tokens,
    }))


async def _ws_loop() -> None:
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_INTERVAL,
                max_size=2**22,
            ) as ws:
                with _ws_tokens_lock:
                    desired = list(_ws_desired)
                await _ws_subscribe(ws, desired)
                _ws_subscribed.clear()
                _ws_subscribed.update(desired)
                _ws_state["connected"] = True
                _ws_state["last_error"] = ""
                log.info("WS connected  subscribed=%d tokens", len(desired))

                last_resub_check = time.time()
                first_msg_logged = False
                while True:
                    # Timed recv so resubscription check still runs when no
                    # messages are arriving (e.g. we haven't subscribed to
                    # anything yet, or the market is quiet).
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        raw = None

                    now_t = time.time()
                    if now_t - last_resub_check >= 3.0:
                        last_resub_check = now_t
                        with _ws_tokens_lock:
                            want = set(_ws_desired)
                        new = want - _ws_subscribed
                        if new:
                            await _ws_subscribe(ws, list(new))
                            _ws_subscribed.update(new)
                            log.info("WS subscribed %d new tokens (total=%d)",
                                     len(new), len(_ws_subscribed))

                    if raw is None:
                        continue

                    if not first_msg_logged:
                        first_msg_logged = True
                        sample = raw if len(raw) <= 400 else (raw[:400] + "…")
                        log.info("WS first message sample: %s", sample)

                    _ws_state["msg_count"] += 1
                    _ws_state["last_msg_ts"] = int(time.time())
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
            _ws_state["connected"] = False
            _ws_state["last_error"] = f"{type(exc).__name__}: {exc}"
            log.warning("WS disconnected (%s); reconnecting in %ds",
                        _ws_state["last_error"], WS_RECONNECT_SEC)
            await asyncio.sleep(WS_RECONNECT_SEC)


def _start_ws_worker() -> None:
    """Start the background WS thread. Idempotent."""
    if getattr(_start_ws_worker, "_started", False):
        return
    def runner() -> None:
        try:
            asyncio.run(_ws_loop())
        except Exception as exc:
            log.error("WS worker crashed: %s", exc)
    t = threading.Thread(target=runner, daemon=True, name="ws-worker")
    t.start()
    _start_ws_worker._started = True  # type: ignore[attr-defined]


def _set_ws_subscription(tokens: set[str]) -> None:
    """Replace the desired subscription set. Additions are picked up by the WS loop."""
    with _ws_tokens_lock:
        _ws_desired.clear()
        _ws_desired.update(tokens)


def _get_cached_book(token_id: str) -> dict:
    """Thread-safe shallow copy of the cached book for `token_id`."""
    with _book_lock:
        b = _book_cache.get(token_id)
        if not b:
            return {}
        # Shallow copy of level lists so the main thread can iterate safely.
        return {
            "bids": list(b.get("bids", [])),
            "asks": list(b.get("asks", [])),
        }


def _warmup_books_via_rest(token_ids: list[str]) -> None:
    """
    One-shot REST fetch to prime the WS cache for tokens not yet seen.
    Called when a new market is discovered and we don't want to wait one
    WS roundtrip before the signal evaluator can look at the book.
    """
    missing = [t for t in token_ids if not _get_cached_book(t)]
    if not missing:
        return
    try:
        books = _fetch_books_bulk(missing)
    except Exception as exc:
        log.debug("WS warmup REST fetch failed: %s", exc)
        return
    with _book_lock:
        for tid, book in books.items():
            if tid not in _book_cache:
                _book_cache[tid] = {
                    "bids": [dict(b) for b in (book.get("bids") or [])],
                    "asks": [dict(a) for a in (book.get("asks") or [])],
                }


def _best_ask_with_size(book: dict) -> tuple[float | None, float]:
    """Return (best_ask_price, shares_available_at_that_price)."""
    best_price: float | None = None
    best_size: float = 0.0
    for a in book.get("asks") or []:
        try:
            p = float(a["price"])
            s = float(a["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if best_price is None or p < best_price:
            best_price = p
            best_size = s
        elif p == best_price:
            best_size += s
    return best_price, best_size


def _best_bid_price(book: dict) -> float | None:
    """Return best bid price (highest bid), or None if book is empty."""
    best: float | None = None
    for b in book.get("bids") or []:
        try:
            p = float(b["price"])
        except (KeyError, TypeError, ValueError):
            continue
        if best is None or p > best:
            best = p
    return best


def _load_field(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v


# ── market discovery ──────────────────────────────────────────────────────────

@dataclass
class Market:
    slug: str
    asset: str
    candle_start: int        # epoch of candle open
    candle_end: int          # epoch of candle close
    up_token: str
    down_token: str
    price_to_beat: float | None = None   # Polymarket's settlement benchmark (K)


def _extract_price_to_beat(event: dict, mkt: dict) -> float | None:
    """
    Extract the settlement benchmark from Polymarket's eventMetadata.
    This is the actual K used to resolve the market — NOT the Binance open.
    """
    for source in (event.get("eventMetadata"), mkt.get("priceToBeat"),
                   mkt.get("startPrice")):
        if source is None:
            continue
        if isinstance(source, str):
            try:
                source = json.loads(source)
            except Exception:
                try:
                    return float(source)
                except Exception:
                    continue
        if isinstance(source, dict):
            for key in ("priceToBeat", "startPrice", "price_to_beat", "start_price"):
                val = source.get(key)
                if val is not None:
                    try:
                        return float(val)
                    except Exception:
                        pass
        if isinstance(source, (int, float)):
            try:
                return float(source)
            except Exception:
                pass
    return None


def _event_to_market(event: dict, now_ts: int) -> Market | None:
    slug = str(event.get("slug") or "").strip().lower()
    m = UPDOWN_5M_RE.match(slug)
    if not m:
        return None
    asset = m.group(1).upper()
    if asset != "BTC":
        return None
    epoch = int(m.group(2))

    markets = event.get("markets") or []
    if not markets:
        return None
    mkt = markets[0]
    if mkt.get("closed"):
        return None

    outcomes = _load_field(mkt.get("outcomes")) or []
    token_ids = _load_field(mkt.get("clobTokenIds")) or []
    up_token = down_token = None
    for i, name in enumerate(outcomes):
        if i >= len(token_ids):
            continue
        label = str(name).strip().lower()
        if label == "up":
            up_token = str(token_ids[i])
        elif label == "down":
            down_token = str(token_ids[i])
    if not up_token or not down_token:
        return None

    # Derive candle_end from endDate or fallback to epoch+300
    end_str = mkt.get("endDate") or event.get("endDate") or ""
    try:
        end_dt = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        candle_end = int(end_dt.timestamp())
    except Exception:
        candle_end = epoch + 300

    if candle_end <= now_ts:
        return None  # already expired
    if epoch > now_ts:
        return None  # future candle, not started yet

    price_to_beat = _extract_price_to_beat(event, mkt)

    return Market(
        slug=slug, asset=asset,
        candle_start=epoch, candle_end=candle_end,
        up_token=up_token, down_token=down_token,
        price_to_beat=price_to_beat,
    )


def _fetch_active_markets(known_assets: set[str]) -> list[Market]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300
    found: dict[str, Market] = {}

    # 1. Probe deterministic slugs for every known asset (fast path)
    slugs: list[str] = []
    for asset in known_assets:
        for delta in (-1, 0, 1):
            slugs.append(f"{asset.lower()}-updown-5m-{bucket + delta * 300}")

    for slug in slugs:
        try:
            data = _get_json(f"{GAMMA_API}?slug={slug}")
            if isinstance(data, list) and data:
                mkt = _event_to_market(data[0], now_ts)
                if mkt:
                    found[mkt.slug] = mkt
        except Exception:
            pass

    # 2. Gamma API broad scan — catches newly listed assets
    try:
        data = _get_json(f"{GAMMA_API}?closed=false&tag_slug=crypto&limit=200")
        if isinstance(data, list):
            for event in data:
                slug = str(event.get("slug") or "").lower()
                if not UPDOWN_5M_RE.match(slug) or slug in found:
                    continue
                mkt = _event_to_market(event, now_ts)
                if mkt:
                    found[mkt.slug] = mkt
    except Exception as exc:
        log.debug("Gamma broad scan: %s", exc)

    return list(found.values())


# ── DB operations ─────────────────────────────────────────────────────────────

def _upsert_candle(conn: sqlite3.Connection, mkt: Market) -> None:
    now = int(datetime.now(timezone.utc).timestamp())
    conn.execute("""
        INSERT INTO candles (slug, asset, candle_start, candle_end, up_token, down_token, first_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            candle_end = excluded.candle_end,
            up_token   = excluded.up_token,
            down_token = excluded.down_token
    """, (mkt.slug, mkt.asset, mkt.candle_start, mkt.candle_end,
          mkt.up_token, mkt.down_token, now))
    conn.commit()


def _insert_signal(
    conn: sqlite3.Connection,
    slug: str, asset: str, side: str,
    entry_price: float, secs: int | None, candle_start: int,
    fair_price: float, edge: float, shares: float,
    sigma: float | None = None,
    opp_ask: float | None = None,
) -> str | None:
    """
    Insert or upgrade one signal for (slug, side).
    Returns "inserted", "updated", or None when unchanged.
    sigma   — per-bar vol used for the BS calculation (stored for calibration analysis)
    opp_ask — opposite side's best ask at signal time (enables market-implied edge)
    """
    now = int(datetime.now(timezone.utc).timestamp())
    existing = conn.execute(
        "SELECT id, entry_price, edge, won FROM signals WHERE slug=? AND side=?",
        (slug, side),
    ).fetchone()
    if existing:
        if existing["won"] is not None:
            return None
        old_price = float(existing["entry_price"] or 0.0)
        old_edge = float(existing["edge"] or 0.0)
        better_edge = edge >= old_edge + REPRICE_MIN_EDGE_IMPROV
        better_price = entry_price <= old_price - REPRICE_MIN_PRICE_DROP
        if not (better_edge or better_price):
            return None
        conn.execute("""
            UPDATE signals
            SET entry_price=?, shares=?, secs_remaining=?, signal_ts=?,
                fair_price=?, edge=?, sigma=?, opp_ask=?,
                winner=NULL, won=NULL, pnl=NULL
            WHERE id=?
        """, (entry_price, shares, secs, now, fair_price, edge, sigma, opp_ask, existing["id"]))
        conn.commit()
        return "updated"

    conn.execute("""
        INSERT INTO signals
            (slug, asset, side, entry_price, shares, secs_remaining,
             candle_start, signal_ts, fair_price, edge, sigma, opp_ask)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (slug, asset, side, entry_price, shares, secs,
          candle_start, now, fair_price, edge, sigma, opp_ask))
    conn.commit()
    return "inserted"


def _suggest_shares(edge: float, min_edge: float, book_size: float) -> float:
    """
    Dynamic sizing:
      - BASE_SHARES at threshold edge,
      - ramps up with stronger edge,
      - capped by top-of-book liquidity.
    """
    if book_size <= 0:
        return BASE_SHARES
    edge_excess = max(0.0, edge - min_edge)
    ramp = min(1.0, edge_excess / max(1e-6, EDGE_RAMP_WIDTH))
    mult = 1.0 + (MAX_SHARES_MULT - 1.0) * ramp
    by_edge = BASE_SHARES * mult
    by_book = max(MIN_SHARES, book_size * BOOK_SIZE_FRACTION)
    shares = min(by_edge, by_book, BASE_SHARES * MAX_SHARES_MULT)
    return round(max(MIN_SHARES, shares), 2)


def _cap_shares_to_slug_budget(
    conn: sqlite3.Connection,
    slug: str,
    side: str,
    desired_shares: float,
) -> float:
    """
    Cap shares so total slug size never exceeds SLUG_SHARE_CAP.
    If this side already has a row, treat it as a replacement (subtract old shares).
    """
    existing_same_side = conn.execute(
        "SELECT shares FROM signals WHERE slug=? AND side=?",
        (slug, side),
    ).fetchone()
    same_side_shares = 0.0
    if existing_same_side:
        same_side_shares = float(existing_same_side["shares"] or 0.0)

    total_slug_shares = conn.execute(
        "SELECT COALESCE(SUM(shares), 0.0) FROM signals WHERE slug=?",
        (slug,),
    ).fetchone()[0]
    shares_other_sides = float(total_slug_shares or 0.0) - same_side_shares
    remaining_shares = max(0.0, SLUG_SHARE_CAP - shares_other_sides)
    capped = min(desired_shares, remaining_shares)
    return round(max(0.0, capped), 2)


def _apply_winner(conn: sqlite3.Connection, slug: str, winner: str, source: str) -> None:
    """Write winner to candles + signals and log each resolved signal."""
    now = int(datetime.now(timezone.utc).timestamp())
    conn.execute(
        "UPDATE candles SET winner=?, resolved_at=? WHERE slug=?", (winner, now, slug)
    )
    sigs = conn.execute(
        "SELECT id, side, asset, entry_price, shares, fair_price, edge, signal_ts "
        "FROM signals WHERE slug=?",
        (slug,),
    ).fetchall()
    for sig in sigs:
        won = 1 if sig["side"] == winner else 0
        shares = sig["shares"] or BASE_SHARES
        pnl = round(shares * ((1.0 - sig["entry_price"]) if won else -sig["entry_price"]), 6)
        conn.execute(
            "UPDATE signals SET winner=?, won=?, pnl=? WHERE id=?",
            (winner, won, pnl, sig["id"]),
        )
        ts_utc = datetime.fromtimestamp(sig["signal_ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        log.info(
            "BET-RESOLVE  slug=%s  asset=%s  side=%s  winner=%s  won=%d  "
            "ask=%.4f  fair=%.4f  edge=%+.4f  shares=%s  pnl=%+.4f  src=%s  ts=%s",
            slug, sig["asset"], sig["side"], winner, won,
            float(sig["entry_price"]),
            float(sig["fair_price"]) if sig["fair_price"] is not None else 0.0,
            float(sig["edge"]) if sig["edge"] is not None else 0.0,
            sig["shares"], pnl, source, ts_utc,
        )
    conn.commit()
    log.info("MARKET-RESOLVED  slug=%s  winner=%s  signals=%d  src=%s",
             slug, winner, len(sigs), source)


def _resolve_slug(conn: sqlite3.Connection, slug: str) -> bool:
    """
    Resolve a slug using Polymarket/Gamma only.
    Returns True if resolved and saved.
    """
    # ── try Gamma / Polymarket ────────────────────────────────────────────────
    try:
        data = _get_json(f"{GAMMA_API}?slug={slug}")
        if isinstance(data, list) and data:
            event = data[0]
            mkt = (event.get("markets") or [None])[0]
            if mkt:
                outcomes = (
                    _load_field(mkt.get("outcomes"))
                    or _load_field(event.get("outcomes"))
                    or []
                )
                prices_raw = (
                    _load_field(mkt.get("outcomePrices"))
                    or _load_field(event.get("outcomePrices"))
                    or []
                )
                winner: str | None = None
                for i, p in enumerate(prices_raw):
                    if i >= len(outcomes):
                        continue
                    try:
                        price = float(p)
                    except (TypeError, ValueError):
                        continue
                    # Some feeds lag the closed flag; a 1.0 outcome is enough.
                    if price >= 0.999:
                        winner = str(outcomes[i])
                        break
                if winner is not None:
                    _apply_winner(conn, slug, winner, "gamma")
                    return True
    except Exception as exc:
        log.debug("Gamma resolve %s: %s", slug, exc)

    return False


def _pending_slugs(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """
    Return slugs that need resolution — both genuinely pending (winner IS NULL)
    and previously abandoned (winner='?') so stale scanner restarts don't leave
    gaps in the P/L record.
    """
    now = int(datetime.now(timezone.utc).timestamp())
    rows = conn.execute(
        "SELECT slug, candle_end FROM candles "
        "WHERE (winner IS NULL OR winner = '?') AND candle_end < ?",
        (now,),
    ).fetchall()
    return [(r["slug"], r["candle_end"]) for r in rows]


# ── main loop ─────────────────────────────────────────────────────────────────

def run(max_price: float, poll: float, db_path: str, min_edge: float, verbose: bool = False) -> None:
    conn = init_db(db_path)
    known_assets: set[str] = set(_SEED_ASSETS)
    active: dict[str, Market] = {}
    last_refresh = last_resolve = 0
    last_ws_status = 0
    ws_prev_msg_count = 0
    bs_mode = min_edge > 0

    log.info(
        "Started  max_price=$%.2f  min_edge=%.3f  poll=%gs  db=%s%s%s  [WS books]",
        max_price, min_edge, poll, db_path,
        "  [BS-edge mode]" if bs_mode else "",
        "  [verbose]"      if verbose  else "",
    )

    _start_ws_worker()

    while True:
        now = int(datetime.now(timezone.utc).timestamp())

        # ── refresh market list ───────────────────────────────────────────────
        if now - last_refresh >= REFRESH_INTERVAL:
            try:
                fresh = _fetch_active_markets(known_assets)
                new_market_tokens: list[str] = []
                for mkt in fresh:
                    known_assets.add(mkt.asset.lower())
                    if mkt.slug not in active:
                        _upsert_candle(conn, mkt)
                        log.info(
                            "MARKET  %-45s  asset=%-5s  ends_in=%ds",
                            mkt.slug, mkt.asset, mkt.candle_end - now,
                        )
                        new_market_tokens += [mkt.up_token, mkt.down_token]
                    active[mkt.slug] = mkt
                active = {s: m for s, m in active.items() if m.candle_end > now}

                # Refresh the WS subscription set to the current active token universe.
                all_tokens: set[str] = set()
                for mkt in active.values():
                    all_tokens.add(mkt.up_token)
                    all_tokens.add(mkt.down_token)
                _set_ws_subscription(all_tokens)

                # Prime the cache for brand-new tokens so the evaluator doesn't have
                # to wait one WS roundtrip before acting.
                if new_market_tokens:
                    _warmup_books_via_rest(new_market_tokens)

                last_refresh = now
                if not fresh:
                    log.warning("No active 5m markets found — will retry")
            except Exception as exc:
                log.warning("Market refresh error: %s", exc)

        # ── refresh BS data ───────────────────────────────────────────────────
        if (bs_mode or verbose) and active:
            assets_in_play = {m.asset for m in active.values()}

            # Vol + candle-open: slow refresh (every BS_VOL_REFRESH seconds,
            # or immediately when a new candle starts and K is unknown).
            for asset in assets_in_play:
                cached_opens = _bs_cache.get(asset, {}).get("candle_open", {})
                asset_starts = {m.candle_start for m in active.values() if m.asset == asset}
                candle_stale = not any(cs in cached_opens for cs in asset_starts)
                if candle_stale or now - _bs_updated_at.get(asset, 0) >= BS_VOL_REFRESH:
                    _refresh_bs_data(asset)

            # Live spot price: fast refresh every poll cycle.
            _refresh_spots(assets_in_play)

        # ── evaluate signals from WS book cache ───────────────────────────────
        # No network call here — _book_cache is kept fresh by the WS worker.
        if active:
            try:
                # Iterate per-market so both sides are visible simultaneously.
                # This is required for the opposite-side bid check.
                for mkt in active.values():
                    slug  = mkt.slug
                    asset = mkt.asset
                    secs  = mkt.candle_end - now

                    up_book   = _get_cached_book(mkt.up_token)
                    down_book = _get_cached_book(mkt.down_token)
                    up_ask,   up_size   = _best_ask_with_size(up_book)
                    down_ask, down_size = _best_ask_with_size(down_book)
                    up_bid              = _best_bid_price(up_book)
                    down_bid            = _best_bid_price(down_book)

                    # ── BS inputs — computed once per slug ────────────────────
                    fair_up: float | None = None
                    K:       float | None = None
                    sigma:   float | None = None
                    mu:      float = 0.0
                    k_src = "BN"
                    if bs_mode or verbose:
                        spot, K_binance, sigma, mu = _get_bs_inputs(asset, mkt.candle_start)
                        K     = mkt.price_to_beat or K_binance
                        k_src = "PM" if mkt.price_to_beat else "BN"
                        if spot and K and sigma and secs > 0:
                            fair_up = _bs_prob_up(spot, K, max(1, secs), sigma, mu)

                    # ── time-window gate ──────────────────────────────────────
                    in_window = ENTRY_MIN_SECS <= secs <= ENTRY_MAX_SECS

                    # (side, this-side ask, this-side size, opposite-side bid, opposite-side ask)
                    for side, ask, size, opp_bid, opp_ask_val in (
                        ("Up",   up_ask,   up_size,   down_bid, down_ask),
                        ("Down", down_ask, down_size, up_bid,   up_ask),
                    ):
                        if ask is None or size < MIN_BOOK_SHARES:
                            continue

                        fair_price: float | None = None
                        edge:       float | None = None
                        if fair_up is not None:
                            fair_price = fair_up if side == "Up" else (1.0 - fair_up)
                            edge       = round(fair_price - ask, 6)

                        opp_str  = f"${opp_bid:.4f}"   if opp_bid   is not None else "  n/a  "
                        fair_str = f"{fair_price:.4f}"  if fair_price is not None else "  n/a "
                        edge_str = f"{edge:+.4f}"        if edge       is not None else "   n/a"
                        k_str    = f"{K:.4f}"            if K          is not None else "n/a"
                        sig_mark = "►" if (
                            edge is not None and edge >= min_edge and in_window
                        ) else " "

                        # ── real-time tick log ────────────────────────────────
                        #   verbose → every token every poll
                        #   default → only when edge >= 0
                        if verbose:
                            log.info(
                                "%s TICK  %-5s %-4s  opp_bid=%s  ask=$%.4f  "
                                "fair=%s  edge=%s  K=%s[%s]  secs=%d%s",
                                sig_mark, asset, side, opp_str, ask,
                                fair_str, edge_str, k_str, k_src, max(0, secs),
                                "" if in_window else "  [OOW]",
                            )
                        elif fair_price is not None and edge is not None and edge >= 0:
                            log.info(
                                "%s TICK  %-5s %-4s  opp_bid=%s  ask=$%.4f  "
                                "fair=%s  edge=%s  K=%s[%s]  secs=%d%s",
                                sig_mark, asset, side, opp_str, ask,
                                fair_str, edge_str, k_str, k_src, max(0, secs),
                                "" if in_window else "  [OOW]",
                            )

                        # ── signal conditions ─────────────────────────────────

                        # 1. Must be inside the entry window (ENTRY_MIN_SECS..ENTRY_MAX_SECS)
                        if not in_window:
                            continue

                        # 2. Ask must be above price floor (low premiums trap — market
                        #    thinks it's basically dead; the model is usually the one wrong)
                        if ask < MIN_ENTRY_PRICE:
                            if verbose:
                                log.info(
                                    "  SKIP  %-5s %-4s  price_floor  ask=$%.4f < $%.2f",
                                    asset, side, ask, MIN_ENTRY_PRICE,
                                )
                            continue

                        # 3. Must have BS edge within [min_edge, MAX_EDGE].
                        #    Edges above MAX_EDGE are treated as model failure (usually
                        #    caused by stale spot, regime change, or near-expiry noise).
                        if fair_price is None or edge is None or edge < min_edge:
                            continue
                        if edge > MAX_EDGE:
                            if verbose:
                                log.info(
                                    "  SKIP  %-5s %-4s  edge_cap  edge=%+.4f > %.2f (model failure)",
                                    asset, side, edge, MAX_EDGE,
                                )
                            continue

                        # 4. Opposite-side bid must confirm this side is genuinely cheap.
                        #    If UP asks 1¢ but DOWN only bids 0.5¢, the market isn't
                        #    treating DOWN as near-certain — the cheapness isn't real.
                        #    We require: opp_bid >= ask × OPPOSITE_BID_MULT (default 1.5×).
                        if opp_bid is None or opp_bid < ask * OPPOSITE_BID_MULT:
                            if verbose:
                                log.info(
                                    "  SKIP  %-5s %-4s  opp_bid_check FAIL"
                                    "  opp_bid=%s  need>=$%.4f",
                                    asset, side, opp_str, ask * OPPOSITE_BID_MULT,
                                )
                            continue

                        shares = _suggest_shares(edge=edge, min_edge=min_edge, book_size=size)
                        shares = _cap_shares_to_slug_budget(
                            conn=conn,
                            slug=slug,
                            side=side,
                            desired_shares=shares,
                        )
                        if shares < MIN_SHARES:
                            if verbose:
                                log.info(
                                    "  SKIP  %-5s %-4s  slug_share_cap hit  "
                                    "share_cap=%.2f  ask=$%.4f  shares=%.2f < min=%.2f",
                                    asset, side, SLUG_SHARE_CAP, ask, shares, MIN_SHARES,
                                )
                            continue
                        sig_action = _insert_signal(
                            conn, slug, asset, side, ask, secs,
                            mkt.candle_start, fair_price, edge, shares,
                            sigma=sigma if (bs_mode or verbose) else None,
                            opp_ask=opp_ask_val,
                        )
                        if sig_action:
                            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                            log.info(
                                "BET-%s  slug=%s  asset=%s  side=%s  opp_bid=%s  ask=%.4f"
                                "  fair=%s  edge=%s  shares=%.2f  secs_left=%d  ts=%s",
                                sig_action.upper(),
                                slug, asset, side, opp_str, ask,
                                fair_str, edge_str,
                                shares, max(0, secs), ts_utc,
                            )
            except Exception as exc:
                log.warning("Signal eval error: %s", exc)

        # ── WS health heartbeat ───────────────────────────────────────────────
        if now - last_ws_status >= 30:
            last_ws_status = now
            msg_total = _ws_state["msg_count"]
            delta     = msg_total - ws_prev_msg_count
            ws_prev_msg_count = msg_total
            with _book_lock:
                cached_tokens = len(_book_cache)
            age = now - _ws_state["last_msg_ts"] if _ws_state["last_msg_ts"] else -1
            log.info(
                "WS status  connected=%s  subs=%d  cached_books=%d  "
                "msgs_total=%d  msgs_30s=%d  last_msg_age=%ds  err=%s",
                _ws_state["connected"], len(_ws_subscribed), cached_tokens,
                msg_total, delta, age, _ws_state["last_error"] or "-",
            )

        # ── resolve closed candles ────────────────────────────────────────────
        if now - last_resolve >= RESOLVE_INTERVAL:
            for slug, candle_end in _pending_slugs(conn):
                # Always keep trying to resolve stale '?' rows from Polymarket.
                if _resolve_slug(conn, slug):
                    continue

                age = now - candle_end
                if age > RESOLVE_TIMEOUT:
                    row = conn.execute(
                        "SELECT winner FROM candles WHERE slug=?", (slug,)
                    ).fetchone()
                    if row and row["winner"] != "?":
                        log.warning("ABANDON resolving %s (closed %ds ago)", slug, age)
                        conn.execute(
                            "UPDATE candles SET winner='?' WHERE slug=?", (slug,)
                        )
                        conn.commit()
            last_resolve = now

        time.sleep(poll)


# Public alias used by stats module
init_db = _init_db


def main() -> None:
    p = argparse.ArgumentParser(description="Crypto 5m penny signal scanner")
    p.add_argument("--max-price", type=float, default=DEFAULT_MAX,
                   help="Max ask to record as a signal (default 0.03)")
    p.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE,
                   help=(
                       "Minimum Black-Scholes edge (fair_price - ask) required to fire a signal. "
                       "0 disables BS filtering and uses price-only mode (default 0). "
                       "Example: --min-edge 0.05 requires ≥5%% BS edge."
                   ))
    p.add_argument("--poll", type=float, default=POLL_INTERVAL,
                   help="Seconds between book polls (default 5)")
    p.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    p.add_argument("--verbose", "-v", action="store_true",
                   help=(
                       "Print every bid/ask/fair/edge tick for every token every poll cycle. "
                       "Without this flag only positive-edge opportunities are shown."
                   ))
    args = p.parse_args()
    try:
        run(max_price=args.max_price, poll=args.poll, db_path=args.db,
            min_edge=args.min_edge, verbose=args.verbose)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
