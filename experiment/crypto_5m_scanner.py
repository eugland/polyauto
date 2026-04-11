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

import requests

# ── constants ─────────────────────────────────────────────────────────────────

GAMMA_API        = "https://gamma-api.polymarket.com/events"
CLOB_HOST        = "https://clob.polymarket.com"
DB_PATH          = os.path.join("experiment", "crypto_5m.db")
DEFAULT_MAX      = 0.03
BET_SHARES       = 5      # shares placed per order at each tier
MIN_BOOK_SHARES  = 1      # minimum shares needed in order book to fire
TIERS            = [0.03, 0.02, 0.01]  # each tier gets its own independent order
POLL_INTERVAL    = 5      # seconds between book polls
REFRESH_INTERVAL = 60     # seconds between market-list refreshes
RESOLVE_INTERVAL = 30     # seconds between resolution scans
RESOLVE_TIMEOUT  = 600    # give up resolving after this many seconds post-close

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
    tier           REAL    NOT NULL,   -- threshold that triggered this order: 0.01 / 0.02 / 0.03
    entry_price    REAL    NOT NULL,   -- actual ask at time of entry
    shares         REAL    NOT NULL DEFAULT 5,
    secs_remaining INTEGER,
    candle_start   INTEGER NOT NULL,
    signal_ts      INTEGER NOT NULL,
    winner         TEXT,
    won            INTEGER,            -- 1 / 0 / NULL
    pnl            REAL,               -- shares*(1-entry_price) if won, shares*(-entry_price) if lost
    UNIQUE(slug, side, tier)           -- one order per side per tier per candle
);

CREATE INDEX IF NOT EXISTS idx_sig_asset ON signals(asset, candle_start);
CREATE INDEX IF NOT EXISTS idx_sig_resolved ON signals(won);
"""


def _init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Migrate: if signals table uses old schema (no tier column), drop and recreate
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    if cols and "tier" not in cols:
        log.info("Migrating signals table to per-tier schema (old data cleared)")
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
    candle_start: int   # epoch of candle open
    candle_end: int     # epoch of candle close
    up_token: str
    down_token: str


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

    return Market(
        slug=slug, asset=asset,
        candle_start=epoch, candle_end=candle_end,
        up_token=up_token, down_token=down_token,
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
    slug: str, asset: str, side: str, tier: float,
    entry_price: float, secs: int | None, candle_start: int,
) -> bool:
    """Insert one order for this (slug, side, tier) if not already placed. Returns True if inserted."""
    now = int(datetime.now(timezone.utc).timestamp())
    exists = conn.execute(
        "SELECT 1 FROM signals WHERE slug=? AND side=? AND tier=?", (slug, side, tier)
    ).fetchone()
    if exists:
        return False
    conn.execute("""
        INSERT INTO signals
            (slug, asset, side, tier, entry_price, shares, secs_remaining, candle_start, signal_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (slug, asset, side, tier, entry_price, BET_SHARES, secs, candle_start, now))
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

        outcomes  = _load_field(mkt.get("outcomes")) or []
        prices_raw = _load_field(mkt.get("outcomePrices")) or []
        winner: str | None = None
        for i, p in enumerate(prices_raw):
            if i < len(outcomes) and str(p) == "1":
                winner = str(outcomes[i])
                break
        if winner is None:
            return False

        now = int(datetime.now(timezone.utc).timestamp())
        conn.execute(
            "UPDATE candles SET winner=?, resolved_at=? WHERE slug=?", (winner, now, slug)
        )
        sigs = conn.execute(
            "SELECT id, side, entry_price, shares FROM signals WHERE slug=?", (slug,)
        ).fetchall()
        for sig in sigs:
            won = 1 if sig["side"] == winner else 0
            shares = sig["shares"] or BET_SHARES
            pnl = round(shares * ((1.0 - sig["entry_price"]) if won else -sig["entry_price"]), 6)
            conn.execute(
                "UPDATE signals SET winner=?, won=?, pnl=? WHERE id=?",
                (winner, won, pnl, sig["id"]),
            )
        conn.commit()
        log.info("RESOLVED  %-45s  winner=%-4s  signals=%d", slug, winner, len(sigs))
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

def run(max_price: float, poll: float, db_path: str) -> None:
    conn = init_db(db_path)
    known_assets: set[str] = set(_SEED_ASSETS)
    active: dict[str, Market] = {}
    last_refresh = last_resolve = 0

    log.info("Started  max_price=$%.2f  poll=%gs  db=%s", max_price, poll, db_path)

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
                # Evict expired markets
                active = {s: m for s, m in active.items() if m.candle_end > now}
                last_refresh = now
                if not fresh:
                    log.warning("No active 5m markets found — will retry")
            except Exception as exc:
                log.warning("Market refresh error: %s", exc)

        # ── poll order books ──────────────────────────────────────────────────
        if active:
            token_ids: list[str] = []
            token_map: dict[str, tuple[str, str, str]] = {}  # token -> (slug, side, asset)
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
                    if ask is None or size < MIN_BOOK_SHARES:
                        continue
                    secs = mkt.candle_end - now
                    for tier in TIERS:
                        if ask <= tier:
                            if _insert_signal(conn, slug, asset, side, tier, ask, secs, mkt.candle_start):
                                log.info(
                                    "SIGNAL  %-5s %-4s  tier=$%.2f  price=$%.2f  shares=%d  secs=%d",
                                    asset, side, tier, ask, BET_SHARES, max(0, secs),
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
    p.add_argument("--poll", type=float, default=POLL_INTERVAL,
                   help="Seconds between book polls (default 5)")
    p.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    args = p.parse_args()
    try:
        run(max_price=args.max_price, poll=args.poll, db_path=args.db)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
