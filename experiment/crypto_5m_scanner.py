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
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import math

import requests

# ── constants ─────────────────────────────────────────────────────────────────

GAMMA_API        = "https://gamma-api.polymarket.com/events"
CLOB_HOST        = "https://clob.polymarket.com"
BINANCE_BASE     = "https://api.binance.com/api/v3"
DB_PATH          = os.path.join("experiment", "crypto_5m.db")
LOG_PATH         = os.path.join("experiment", "logs", "crypto_5m_scanner.log")
DEFAULT_MAX      = 0.03   # kept for CLI compatibility/logging
DEFAULT_MIN_EDGE = 0.03   # minimum BS edge to record a signal
BET_SHARES       = 5      # shares per signal
MIN_BOOK_SHARES  = 1      # minimum shares needed in order book to fire
POLL_INTERVAL    = 5      # seconds between book polls
REFRESH_INTERVAL = 60     # seconds between market-list refreshes
RESOLVE_INTERVAL = 30     # seconds between resolution scans
RESOLVE_TIMEOUT  = 600    # give up resolving after this many seconds post-close
BS_VOL_WINDOW    = 50     # trailing 5m bars for vol estimation
BS_VOL_REFRESH   = 300    # seconds between per-asset vol refreshes
SECS_PER_YEAR    = 365 * 24 * 3600

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
_SEED_ASSETS = [
    "btc", "eth", "xrp", "bnb", "sol", "hyper",
    "doge", "link", "avax", "matic", "dot", "ltc",
]

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


def _bs_prob_up(S: float, K: float, T_secs: float, sigma_5m: float) -> float:
    """
    Risk-neutral probability that price ends above K.

    S        — current spot price
    K        — strike (5m candle open price)
    T_secs   — seconds remaining until expiry
    sigma_5m — per-bar std dev of log returns (native 5m units, NOT annualised)

    Time is expressed as T_bars = T_secs / 300 (number of 5m bars remaining),
    so sigma and T are in the same units and no annualisation is needed.
    """
    T_bars = T_secs / 300.0          # e.g. 150 s remaining → 0.5 bars
    if T_bars <= 0 or sigma_5m <= 1e-9 or S <= 0 or K <= 0:
        return 1.0 if S > K else (0.5 if S == K else 0.0)
    try:
        d2 = (math.log(S / K) - 0.5 * sigma_5m ** 2 * T_bars) / (sigma_5m * math.sqrt(T_bars))
        return _norm_cdf(d2)
    except (ValueError, ZeroDivisionError):
        return 0.5


def _estimate_vol(closes: list[float]) -> float:
    """
    Per-bar (5m) log-return std dev — raw, NOT annualised.
    E.g. a value of 0.0007 means ≈0.07% typical move per 5m candle.
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
    mean = sum(log_rets) / len(log_rets)
    var  = sum((r - mean) ** 2 for r in log_rets) / max(1, len(log_rets) - 1)
    return math.sqrt(var)            # pure per-bar std dev


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

        # Candle open = open field of the current (last, live) kline.
        live_kline          = klines[-1]
        candle_start_epoch  = int(live_kline[0]) // 1000
        candle_open         = float(live_kline[1])

        _bs_cache[asset] = {
            "sigma":       sigma,
            "candle_open": {candle_start_epoch: candle_open},
        }
        _bs_updated_at[asset] = int(datetime.now(timezone.utc).timestamp())
        log.info(
            "BS vol   %-5s  σ_5m=%.5f (%.4f%%/bar)  K=%.4f",
            asset, sigma, sigma * 100, candle_open,
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


def _get_bs_inputs(asset: str, candle_start: int) -> tuple[float | None, float | None, float | None]:
    """Return (live_spot, candle_open_K, sigma) from cache, or (None, None, None)."""
    cached = _bs_cache.get(asset)
    if not cached:
        return None, None, None
    spot  = _spot_cache.get(asset)          # live price, refreshed every poll
    sigma = cached.get("sigma")
    K     = cached.get("candle_open", {}).get(candle_start)
    # If the cached candle_open is for a different (older) candle_start, K is None.
    return spot, K, sigma


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
    conn.executescript(_DDL)
    conn.commit()
    return conn


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 12) -> Any:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _fetch_books_bulk(token_ids: list[str]) -> dict[str, dict]:
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
    fair_price: float, edge: float,
) -> bool:
    """Insert one signal for (slug, side) if not already recorded. Returns True if inserted."""
    now = int(datetime.now(timezone.utc).timestamp())
    if conn.execute(
        "SELECT 1 FROM signals WHERE slug=? AND side=?", (slug, side)
    ).fetchone():
        return False
    conn.execute("""
        INSERT INTO signals
            (slug, asset, side, entry_price, shares, secs_remaining,
             candle_start, signal_ts, fair_price, edge)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (slug, asset, side, entry_price, BET_SHARES, secs,
          candle_start, now, fair_price, edge))
    conn.commit()
    return True


def _resolve_slug(conn: sqlite3.Connection, slug: str) -> bool:
    """Query Gamma for winner. Returns True if resolved and saved."""
    try:
        data = _get_json(f"{GAMMA_API}?slug={slug}")
        if not isinstance(data, list) or not data:
            return False
        mkt = (data[0].get("markets") or [None])[0]
        if not mkt or not mkt.get("closed"):
            return False

        outcomes = _load_field(mkt.get("outcomes")) or []
        prices_raw = _load_field(mkt.get("outcomePrices")) or []
        winner: str | None = None
        for i, p in enumerate(prices_raw):
            if i >= len(outcomes):
                continue
            try:
                price = float(p)
            except (TypeError, ValueError):
                continue
            # Gamma can return 1, "1", 1.0, "1.0", etc.
            if price >= 0.999:
                winner = str(outcomes[i])
                break
        if winner is None:
            return False

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
            shares = sig["shares"] or BET_SHARES
            pnl = round(shares * ((1.0 - sig["entry_price"]) if won else -sig["entry_price"]), 6)
            conn.execute(
                "UPDATE signals SET winner=?, won=?, pnl=? WHERE id=?",
                (winner, won, pnl, sig["id"]),
            )
            ts_utc = datetime.fromtimestamp(sig["signal_ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            log.info(
                "BET-RESOLVE  slug=%s  asset=%s  side=%s  winner=%s  won=%d  "
                "ask=%.4f  fair=%.4f  edge=%+.4f  shares=%s  pnl=%+.4f  signal_ts=%s",
                slug,
                sig["asset"],
                sig["side"],
                winner,
                won,
                float(sig["entry_price"]),
                float(sig["fair_price"]) if sig["fair_price"] is not None else 0.0,
                float(sig["edge"]) if sig["edge"] is not None else 0.0,
                sig["shares"],
                pnl,
                ts_utc,
            )
        conn.commit()
        log.info("MARKET-RESOLVED  slug=%s  winner=%s  signals=%d", slug, winner, len(sigs))
        return True
    except Exception as exc:
        log.debug("resolve %s: %s", slug, exc)
        return False


def _pending_slugs(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    now = int(datetime.now(timezone.utc).timestamp())
    rows = conn.execute(
        "SELECT slug, candle_end FROM candles WHERE winner IS NULL AND candle_end < ?",
        (now,),
    ).fetchall()
    return [(r["slug"], r["candle_end"]) for r in rows]


# ── main loop ─────────────────────────────────────────────────────────────────

def run(max_price: float, poll: float, db_path: str, min_edge: float, verbose: bool = False) -> None:
    conn = init_db(db_path)
    known_assets: set[str] = set(_SEED_ASSETS)
    active: dict[str, Market] = {}
    last_refresh = last_resolve = 0
    bs_mode = min_edge > 0

    log.info(
        "Started  max_price=$%.2f  min_edge=%.3f  poll=%gs  db=%s%s%s",
        max_price, min_edge, poll, db_path,
        "  [BS-edge mode]" if bs_mode else "",
        "  [verbose]"      if verbose  else "",
    )

    while True:
        now = int(datetime.now(timezone.utc).timestamp())

        # ── refresh market list ───────────────────────────────────────────────
        if now - last_refresh >= REFRESH_INTERVAL:
            try:
                fresh = _fetch_active_markets(known_assets)
                for mkt in fresh:
                    known_assets.add(mkt.asset.lower())
                    if mkt.slug not in active:
                        _upsert_candle(conn, mkt)
                        log.info(
                            "MARKET  %-45s  asset=%-5s  ends_in=%ds",
                            mkt.slug, mkt.asset, mkt.candle_end - now,
                        )
                    active[mkt.slug] = mkt
                active = {s: m for s, m in active.items() if m.candle_end > now}
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

        # ── poll order books ──────────────────────────────────────────────────
        if active:
            token_ids: list[str] = []
            token_map: dict[str, tuple[str, str, str]] = {}
            for mkt in active.values():
                token_ids += [mkt.up_token, mkt.down_token]
                token_map[mkt.up_token]   = (mkt.slug, "Up",   mkt.asset)
                token_map[mkt.down_token] = (mkt.slug, "Down", mkt.asset)

            try:
                books = _fetch_books_bulk(token_ids)
                for token_id, book in books.items():
                    info = token_map.get(token_id)
                    if not info:
                        continue
                    slug, side, asset = info
                    mkt = active.get(slug)
                    if not mkt:
                        continue
                    ask, size = _best_ask_with_size(book)
                    bid       = _best_bid_price(book)
                    if ask is None or size < MIN_BOOK_SHARES:
                        continue
                    secs = mkt.candle_end - now

                    # ── Black-Scholes fair price ──────────────────────────────
                    fair_price: float | None = None
                    edge:       float | None = None
                    if bs_mode or verbose:
                        spot, K_binance, sigma = _get_bs_inputs(asset, mkt.candle_start)
                        # Prefer Polymarket's own settlement benchmark;
                        # fall back to Binance candle open if not published yet.
                        K = mkt.price_to_beat or K_binance
                        if spot and K and sigma and secs > 0:
                            p_up = _bs_prob_up(spot, K, max(1, secs), sigma)
                            fair_price = p_up if side == "Up" else (1.0 - p_up)
                            edge       = round(fair_price - ask, 6)

                    # ── real-time tick log ────────────────────────────────────
                    #   verbose  → print every token every poll
                    #   default  → print only when edge >= 0 (genuine opportunity)
                    bid_str  = f"${bid:.4f}" if bid  is not None else "  n/a  "
                    fair_str = f"{fair_price:.4f}" if fair_price is not None else "  n/a "
                    edge_str = (f"{edge:+.4f}" if edge is not None else "   n/a")
                    sig_mark = "►" if (edge is not None and edge >= min_edge) else " "

                    k_src = "PM" if mkt.price_to_beat else "BN"  # Polymarket vs Binance
                    if verbose:
                        log.info(
                            "%s TICK  %-5s %-4s  bid=%s  ask=$%.4f  "
                            "fair=%s  edge=%s  K=%s[%s]  secs=%d",
                            sig_mark, asset, side,
                            bid_str, ask, fair_str, edge_str,
                            f"{K:.4f}" if (bs_mode or verbose) and K else "n/a",
                            k_src, max(0, secs),
                        )
                    elif fair_price is not None and edge is not None and edge >= 0:
                        log.info(
                            "%s TICK  %-5s %-4s  bid=%s  ask=$%.4f  "
                            "fair=%s  edge=%s  K=%s[%s]  secs=%d",
                            sig_mark, asset, side,
                            bid_str, ask, fair_str, edge_str,
                            f"{K:.4f}" if K else "n/a",
                            k_src, max(0, secs),
                        )

                    # ── signal condition: pure BS edge ────────────────────────
                    if fair_price is not None and edge is not None and edge >= min_edge:
                        if _insert_signal(
                            conn, slug, asset, side, ask, secs,
                            mkt.candle_start, fair_price, edge,
                        ):
                            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                            log.info(
                                "BET-OPEN  slug=%s  asset=%s  side=%s  bid=%s  ask=%.4f"
                                "  fair=%s  edge=%s  shares=%d  secs_left=%d  ts=%s",
                                slug, asset, side,
                                bid_str, ask, fair_str, edge_str,
                                BET_SHARES, max(0, secs), ts_utc,
                            )
            except Exception as exc:
                log.warning("Book poll error: %s", exc)

        # ── resolve closed candles ────────────────────────────────────────────
        if now - last_resolve >= RESOLVE_INTERVAL:
            for slug, candle_end in _pending_slugs(conn):
                age = now - candle_end
                if age > RESOLVE_TIMEOUT:
                    log.warning("ABANDON resolving %s (closed %ds ago)", slug, age)
                    conn.execute(
                        "UPDATE candles SET winner='?' WHERE slug=?", (slug,)
                    )
                    conn.commit()
                else:
                    _resolve_slug(conn, slug)
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
