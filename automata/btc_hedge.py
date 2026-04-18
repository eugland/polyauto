"""
automata/btc_hedge.py — BTC 5m Fast-In Fast-Out Hedging Bot

Strategy: Enter a simultaneous Up + Down hedge only when:
  1. Bollinger Band Width expands rapidly (volatility spike ≥ 1.3× rolling avg)
  2. RSI is between 40–60 (neutral, no heavy trend)
  3. Volume delta is NOT > 70% one-sided (balanced flow)

Exit when:
  1. BTC spot moves ≥ STOP_PCT in either direction → sell both sides immediately
  2. Hedge held for MAX_CANDLES_HELD × 5 min without exit → kill both at market

Data feeds:
  - Binance WS btcusdt@kline_5m  — real-time OHLCV + taker buy volume
  - Polymarket CLOB WS            — real-time Up/Down token book

Usage:
    python -m automata.btc_hedge                          # dry-run
    python -m automata.btc_hedge --bet                    # live
    python -m automata.btc_hedge --bet --budget 10 --stop-pct 0.003
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import websockets
from dotenv import load_dotenv

import sqlite3

from automata.client import (
    build_client,
    cancel_order,
    derive_api_credentials,
    get_all_open_orders,
    get_positions,
    get_usdc_balance,
    place_market_buy,
)

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

log = logging.getLogger("automata.btc_hedge")
LOG_PATH = Path("experiment") / "logs" / "btc_hedge.log"


def _init_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
        )
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(
        LOG_PATH, encoding="utf-8",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
    )
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    log.addHandler(fh)


# ── Constants ─────────────────────────────────────────────────────────────────

GAMMA_API           = "https://gamma-api.polymarket.com/events"
WS_POLY             = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_KLINE_URL   = "https://api.binance.com/api/v3/klines"
BINANCE_WS_URL      = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
BINANCE_WS_RECONNECT = 3

# ── Signal parameters ─────────────────────────────────────────────────────────
BB_WINDOW       = 20      # Bollinger Band rolling period
BB_STD          = 2.0     # standard deviation multiplier
BB_WIDTH_FACTOR = 1.3     # enter only if current BB width > 1.3× rolling avg
BB_ROLL_WINDOW  = 10      # rolling window for BB width baseline
RSI_PERIOD      = 14      # RSI period
RSI_LOW         = 40.0    # RSI must be above this (not oversold)
RSI_HIGH        = 60.0    # RSI must be below this (not overbought)
VOL_DELTA_MAX   = 0.70    # block entry if buy ratio > 70% (strong buy pressure)
VOL_DELTA_MIN   = 0.30    # block entry if buy ratio < 30% (strong sell pressure)
MIN_KLINES_NEEDED = BB_WINDOW + RSI_PERIOD + 2  # minimum candles before any signal
MAX_PAIR_SUM    = 1.00    # require Up ask + Down ask strictly below this

# ── Exit parameters ───────────────────────────────────────────────────────────
DEFAULT_STOP_PCT    = 0.003   # 0.3% BTC spot move triggers exit
MAX_CANDLES_HELD    = 2       # kill both positions after 2 candles (10 min)
CANDLE_SECONDS      = 300     # 5-minute candle duration

# ── Execution parameters ──────────────────────────────────────────────────────
TICK              = 0.01
STRATEGY_TICK     = 2.0    # main loop cadence (seconds)
MARKET_REFRESH    = 10.0   # how often to re-query Gamma API
WS_RECONNECT      = 3
WS_PING           = 20
KLINE_BOOTSTRAP   = 40     # how many historical candles to pre-load at startup

# ── Budget ────────────────────────────────────────────────────────────────────
DEFAULT_BUDGET      = 10.0   # total USDC per hedge (split evenly Up/Down)
DEFAULT_MAX_BALANCE = 200.0
DEFAULT_SHARES      = 5.0    # target shares per side (Up and Down)

# ── Maker-entry parameters ────────────────────────────────────────────────────
MAKER_OFFSET        = 0.01   # initial post: 1 tick below the ask
REPRICE_INTERVAL    = 12.0   # reprice unfilled order every N seconds (cancel + repost at ask)
NEAR_EXPIRY_SECS    = 60     # only abandon a partial hedge within this many seconds of candle end

# ── Persistence ───────────────────────────────────────────────────────────────
HEDGE_DB_PATH   = Path("experiment") / "btc_hedge.db"
SETTLE_INTERVAL = 60.0   # how often to poll for resolution / redemption


def _init_hedge_db() -> None:
    HEDGE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hedge_trades (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                candle_slug   TEXT NOT NULL,
                candle_end_ts REAL,
                condition_id  TEXT,
                up_token      TEXT,
                down_token    TEXT,
                up_price      REAL,
                dn_price      REAL,
                up_shares     REAL,
                dn_shares     REAL,
                entry_btc     REAL,
                entry_ts      REAL,
                exit_reason   TEXT,
                exit_ts       REAL,
                spent         REAL,
                recv          REAL,
                pnl           REAL,
                winner        TEXT,
                redeemed      INTEGER DEFAULT 0,
                redeem_tx     TEXT,
                dry_run       INTEGER DEFAULT 0,
                status        TEXT    DEFAULT 'open'
            )
        """)


def _db_insert_trade(candle: "Candle", up: "LiveOrder", dn: "LiveOrder",
                     entry_btc: float, bet: bool) -> int:
    """Insert a new hedge trade at HOLDING entry. Returns the row id."""
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        cur = conn.execute(
            """
            INSERT INTO hedge_trades
                (candle_slug, candle_end_ts, condition_id,
                 up_token, down_token,
                 up_price, dn_price, up_shares, dn_shares,
                 entry_btc, entry_ts, dry_run, status)
            VALUES (?,?,?, ?,?, ?,?,?,?, ?,?, ?,?)
            """,
            (
                candle.slug, candle.end_ts, candle.condition_id,
                candle.up_token, candle.down_token,
                up.price, dn.price, up.shares, dn.shares,
                entry_btc, time.time(),
                0 if bet else 1,
                "open",
            ),
        )
        return cur.lastrowid


def _db_update_exit(trade_id: int, reason: str, spent: float,
                    recv: float, pnl: float) -> None:
    """Record the exit outcome for a trade."""
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE hedge_trades
               SET exit_reason=?, exit_ts=?, spent=?, recv=?, pnl=?, status='exited'
             WHERE id=?
            """,
            (reason, time.time(), round(spent, 6), round(recv, 6),
             round(pnl, 6), trade_id),
        )


def _db_update_resolution(trade_id: int, winner: str) -> None:
    """Record which side won after market resolution."""
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        conn.execute(
            "UPDATE hedge_trades SET winner=? WHERE id=?",
            (winner, trade_id),
        )


def _db_update_redeemed(trade_id: int, tx_hash: str) -> None:
    """Mark a trade as redeemed with the onchain tx hash."""
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        conn.execute(
            "UPDATE hedge_trades SET redeemed=1, redeem_tx=?, status='redeemed' WHERE id=?",
            (tx_hash, trade_id),
        )


def _db_get_unresolved_past_end() -> list[dict]:
    """
    Return exited/open trades whose candle has ended but winner is not yet recorded.
    Also return open (crash-recovery) trades past end_ts.
    """
    now = time.time()
    with sqlite3.connect(HEDGE_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM hedge_trades
             WHERE winner IS NULL
               AND candle_end_ts IS NOT NULL
               AND candle_end_ts < ?
             ORDER BY entry_ts DESC
             LIMIT 50
            """,
            (now - 30,),    # give 30s grace after end_ts
        ).fetchall()
    return [dict(r) for r in rows]


# ── Shared streaming state ────────────────────────────────────────────────────

_book_cache: dict[str, dict] = {}
_ws_subscribed_tokens: set[str] = set()

# Binance kline buffer — deque of closed 5m candles
_kline_buf: deque[dict] = deque(maxlen=KLINE_BOOTSTRAP + 5)
_latest_btc_price: float = 0.0   # updated on every Binance WS tick


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class Candle:
    slug:         str
    up_token:     str
    down_token:   str
    condition_id: str
    start_ts:     float
    end_ts:       float


@dataclass
class LiveOrder:
    order_id:   str
    token_side: str      # "Up" or "Down"
    token_id:   str
    price:      float    # buy price (what we paid)
    shares:     float
    posted_ts:  float
    status:          str   = "open"   # open | filled | cancelled
    last_reprice_ts: float = 0.0
    reprice_count:   int   = 0

    @property
    def usdc_spent(self) -> float:
        return self.price * self.shares


@dataclass
class HedgeState:
    candle:           Candle | None = None
    entry_btc_price:  float = 0.0       # BTC spot price at hedge entry
    entry_ts:         float = 0.0       # unix ts when hedge was entered
    up_order:         LiveOrder | None = None
    dn_order:         LiveOrder | None = None
    status:           str = "IDLE"      # IDLE | ENTERING
    trade_id:         int | None = None  # current DB row id (set when both legs are filled)
    last_market_refresh: float = 0.0
    last_signal_log:  float = 0.0
    trades:           list[dict] = field(default_factory=list)
    total_spent:      float = 0.0
    total_recv:       float = 0.0       # retained for legacy session display
    entered_candle_slug: str | None = None  # prevent repeated entries on same candle


_state = HedgeState()


# ── Book helpers (identical to btc_seller) ────────────────────────────────────

def _best_bid_ask(token_id: str) -> tuple[float | None, float | None]:
    book  = _book_cache.get(token_id, {})
    bids  = book.get("bids", [])
    asks  = book.get("asks", [])
    best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
    best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None
    return best_bid, best_ask


def _apply_price_change(book: dict, changes: list[dict]) -> None:
    for ch in changes or []:
        try:
            p  = float(ch["price"])
            s  = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key    = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


# ── Market discovery (identical to btc_seller) ────────────────────────────────

def _load_json_field(v: Any) -> list:
    if isinstance(v, str):
        try:
            r = json.loads(v)
            return r if isinstance(r, list) else []
        except Exception:
            return []
    return v if isinstance(v, list) else []


def _fetch_btc_candle() -> Candle | None:
    """Find the next un-started (or currently running) BTC 5m candle."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300
    best: Candle | None = None

    for delta in (0, 1, 2, 3, -1):
        slug = f"btc-updown-5m-{bucket + delta * 300}"
        try:
            data = requests.get(
                f"{GAMMA_API}?slug={slug}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            ).json()
            if not isinstance(data, list) or not data:
                continue
            markets = data[0].get("markets") or []
            if not isinstance(markets, list):
                continue

            up_tok = down_tok = condition_id = None
            for mkt in markets:
                if not isinstance(mkt, dict) or mkt.get("closed"):
                    continue
                outcomes  = _load_json_field(mkt.get("outcomes"))
                token_ids = _load_json_field(mkt.get("clobTokenIds"))
                cand_up = cand_down = None
                for i, name in enumerate(outcomes):
                    if i >= len(token_ids):
                        continue
                    label = str(name).strip().lower()
                    if   label == "up":   cand_up   = str(token_ids[i])
                    elif label == "down": cand_down = str(token_ids[i])
                cand_cid = str(mkt.get("conditionId") or mkt.get("condition_id") or "")
                if cand_up and cand_down and cand_cid:
                    up_tok = cand_up; down_tok = cand_down; condition_id = cand_cid
                    break

            if not up_tok or not down_tok or not condition_id:
                continue
            candle_ts = int(slug.rsplit("-", 1)[-1])
            start_ts  = float(candle_ts)
            end_ts    = float(candle_ts + 300)
            if end_ts <= now_ts or start_ts - now_ts > 1500:
                continue

            candle = Candle(slug=slug, up_token=up_tok, down_token=down_tok,
                            condition_id=condition_id, start_ts=start_ts, end_ts=end_ts)
            if best is None:
                best = candle; continue
            cand_unstarted = candle.start_ts > now_ts
            best_unstarted = best.start_ts   > now_ts
            if cand_unstarted and not best_unstarted:
                best = candle
            elif cand_unstarted == best_unstarted and candle.start_ts < best.start_ts:
                best = candle
        except Exception as exc:
            log.debug("Candle fetch error %s: %s", slug, exc)

    return best


# ── Signal computation (pandas) ───────────────────────────────────────────────

def compute_signals(df: pd.DataFrame) -> dict:
    """
    Compute Bollinger Band Width, RSI, and Volume Delta from a DataFrame of
    closed 5m candles.

    Expected columns: open, high, low, close, volume, buy_volume
    Returns a dict with keys: bb_width, bb_width_avg, rsi, vol_delta, signal (bool)
    """
    close = df["close"]

    # ── Bollinger Band Width ───────────────────────────────────────────────────
    bb_mid   = close.rolling(BB_WINDOW).mean()
    bb_std_s = close.rolling(BB_WINDOW).std(ddof=0)
    bb_upper = bb_mid + BB_STD * bb_std_s
    bb_lower = bb_mid - BB_STD * bb_std_s
    bb_width = (bb_upper - bb_lower) / bb_mid.replace(0, float("nan"))
    bb_width_avg = bb_width.rolling(BB_ROLL_WINDOW).mean()

    # ── RSI (Wilder smoothing via EWM) ────────────────────────────────────────
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta.clip(upper=0))
    avg_gain = gain.ewm(com=RSI_PERIOD - 1, min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(com=RSI_PERIOD - 1, min_periods=RSI_PERIOD).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100.0 - 100.0 / (1.0 + rs)

    # ── Volume Delta ──────────────────────────────────────────────────────────
    total_vol = df["volume"].replace(0, float("nan"))
    vol_delta = df["buy_volume"] / total_vol   # fraction that was taker-buy

    # ── Latest values ─────────────────────────────────────────────────────────
    bw_now  = float(bb_width.iloc[-1])    if not bb_width.isna().iloc[-1]    else float("nan")
    bw_avg  = float(bb_width_avg.iloc[-1]) if not bb_width_avg.isna().iloc[-1] else float("nan")
    rsi_now = float(rsi.iloc[-1])         if not rsi.isna().iloc[-1]         else float("nan")
    vd_now  = float(vol_delta.iloc[-1])   if not vol_delta.isna().iloc[-1]   else float("nan")

    # ── Entry signal ──────────────────────────────────────────────────────────
    signal = (
        not any(v != v for v in (bw_now, bw_avg, rsi_now, vd_now))  # no NaN
        and bw_avg > 0
        and bw_now > BB_WIDTH_FACTOR * bw_avg   # BB width expanding
        and RSI_LOW < rsi_now < RSI_HIGH         # RSI neutral zone
        and VOL_DELTA_MIN < vd_now < VOL_DELTA_MAX  # balanced volume flow
    )

    return {
        "bb_width":     bw_now,
        "bb_width_avg": bw_avg,
        "rsi":          rsi_now,
        "vol_delta":    vd_now,
        "signal":       signal,
    }


def _kline_buf_to_df() -> pd.DataFrame:
    """Convert the global kline buffer to a pandas DataFrame."""
    rows = list(_kline_buf)
    df = pd.DataFrame(rows, columns=["open", "high", "low", "close", "volume", "buy_volume"])
    return df.astype(float)


# ── Exit condition check ──────────────────────────────────────────────────────

# ── Order helpers ─────────────────────────────────────────────────────────────

def _place_maker_buy(
    clob, token_side: str, token_id: str, ask: float, shares: float, bet: bool,
    offset: float = MAKER_OFFSET,
) -> LiveOrder | None:
    """
    Post a GTC limit buy at `ask - offset`.  Pass offset=0.0 when repricing to
    post exactly at the ask (crosses the spread, ensures immediate fill).
    Uses the fee-retry pattern for CLOB fee-mismatch errors.
    In dry-run mode (bet=False) just logs without placing.
    """
    price    = max(TICK, round(ask - offset, 2))
    order_id = "DRY"
    if bet:
        try:
            resp = place_market_buy(clob, token_id, price, shares)
        except Exception as exc:
            m = re.search(r"maker fee[^0-9]*(\d+)", str(exc))
            if m:
                fee_bps = int(m.group(1))
                log.info("Retrying maker buy %s with fee_rate_bps=%d", token_side, fee_bps)
                try:
                    resp = place_market_buy(clob, token_id, price, shares,
                                            fee_rate_bps=fee_bps)
                except Exception as exc2:
                    log.error("place_maker_buy failed for %s (fee=%d): %s",
                              token_side, fee_bps, exc2)
                    return None
            else:
                log.error("place_maker_buy failed for %s: %s", token_side, exc)
                return None
        order_id = str(resp.get("orderID") or resp.get("id") or "?")
        log.info("MAKER BUY %s  id=%s  price=%.4f (ask=%.4f)  shares=%.4f  cost=$%.4f",
                 token_side, order_id, price, ask, shares, price * shares)
    else:
        log.info("[DRY] MAKER BUY %s  price=%.4f (ask=%.4f)  shares=%.4f  cost=$%.4f",
                 token_side, price, ask, shares, price * shares)

    return LiveOrder(
        order_id=order_id, token_side=token_side, token_id=token_id,
        price=price, shares=shares, posted_ts=time.time(),
    )


def _cancel_open(clob, order: LiveOrder, bet: bool) -> None:
    if not bet or order.order_id in ("DRY", "?"):
        return
    try:
        cancel_order(clob, order.order_id)
        log.info("CANCELLED %s  id=%s", order.token_side, order.order_id)
    except Exception as exc:
        log.warning("Cancel failed for %s id=%s: %s", order.token_side, order.order_id, exc)


# ── Binance WebSocket ─────────────────────────────────────────────────────────

def _bootstrap_klines() -> None:
    """Pre-load recent closed 5m klines via Binance REST API."""
    global _latest_btc_price
    try:
        resp = requests.get(
            BINANCE_KLINE_URL,
            params={"symbol": "BTCUSDT", "interval": "5m", "limit": KLINE_BOOTSTRAP + 1},
            timeout=10,
        )
        resp.raise_for_status()
        raw = resp.json()
        # Each kline: [open_time, open, high, low, close, volume, ..., taker_buy_base_vol, ...]
        # Index:        0          1      2     3    4      5           9
        count = 0
        for k in raw[:-1]:   # skip the last (still-open) candle
            _kline_buf.append({
                "open":       float(k[1]),
                "high":       float(k[2]),
                "low":        float(k[3]),
                "close":      float(k[4]),
                "volume":     float(k[5]),
                "buy_volume": float(k[9]),
            })
            count += 1
        if raw:
            _latest_btc_price = float(raw[-1][4])
        log.info("Bootstrapped %d klines from Binance REST  latest_price=%.2f",
                 count, _latest_btc_price)
    except Exception as exc:
        log.warning("Binance kline bootstrap failed: %s", exc)


async def _binance_ws() -> None:
    """Stream closed 5m klines from Binance. Updates _kline_buf and _latest_btc_price."""
    global _latest_btc_price
    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=WS_PING,
                ping_timeout=WS_PING,
            ) as ws:
                log.info("Binance WS connected — %s", BINANCE_WS_URL)
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    k = msg.get("k")
                    if not k:
                        continue
                    _latest_btc_price = float(k.get("c", _latest_btc_price))
                    if k.get("x"):   # candle closed
                        _kline_buf.append({
                            "open":       float(k["o"]),
                            "high":       float(k["h"]),
                            "low":        float(k["l"]),
                            "close":      float(k["c"]),
                            "volume":     float(k["v"]),
                            "buy_volume": float(k["V"]),
                        })
                        log.debug("Kline closed  close=%.2f  vol=%.2f  buy_vol=%.2f  buf_len=%d",
                                  float(k["c"]), float(k["v"]), float(k["V"]), len(_kline_buf))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Binance WS error: %s — retry in %ds", exc, BINANCE_WS_RECONNECT)
            await asyncio.sleep(BINANCE_WS_RECONNECT)


# ── Polymarket WebSocket (identical to btc_seller) ────────────────────────────

async def _poly_ws() -> None:
    """Stream Polymarket CLOB order book for the active BTC candle tokens."""
    while True:
        tokens = list(_ws_subscribed_tokens)
        if not tokens:
            await asyncio.sleep(1)
            continue

        sub = json.dumps({
            "auth":       {},
            "type":       "market",
            "assets_ids": tokens,
        })

        try:
            async with websockets.connect(
                WS_POLY,
                ping_interval=WS_PING,
                ping_timeout=WS_PING,
                max_size=2 ** 22,
            ) as ws:
                await ws.send(sub)
                log.info("Poly WS connected — %d tokens", len(tokens))

                async for raw in ws:
                    current = list(_ws_subscribed_tokens)
                    if set(current) != set(tokens):
                        log.info("Token set changed — reconnecting Poly WS")
                        break
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    events = payload if isinstance(payload, list) else [payload]
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        et  = ev.get("event_type")
                        tid = str(ev.get("asset_id") or "")
                        if not tid:
                            continue
                        if et == "book":
                            _book_cache[tid] = {
                                "bids": list(ev.get("bids") or []),
                                "asks": list(ev.get("asks") or []),
                            }
                        elif et == "price_change":
                            book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
                            _apply_price_change(book, ev.get("changes") or [])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Poly WS error: %s — retry in %ds", exc, WS_RECONNECT)
            await asyncio.sleep(WS_RECONNECT)


# ── Strategy loop ─────────────────────────────────────────────────────────────

async def _strategy_loop(
    clob,
    budget: float,
    shares: float,
    stop_pct: float,
    bet: bool,
) -> None:
    global _latest_btc_price

    while True:
        now = time.time()

        # ── Refresh candle ─────────────────────────────────────────────────────
        if (
            _state.candle is None
            or now >= _state.candle.end_ts
            or now - _state.last_market_refresh > MARKET_REFRESH
        ):
            candle = await asyncio.to_thread(_fetch_btc_candle)
            _state.last_market_refresh = now
            if candle is None:
                log.warning("No active BTC 5m candle found — retrying…")
                await asyncio.sleep(STRATEGY_TICK)
                continue

            if _state.candle is None or candle.slug != _state.candle.slug:
                # Candle rolled — clean up any active pending entry state.
                # Filled positions are held to resolution and managed by settle_loop.
                if _state.status == "ENTERING":
                    log.warning("Candle rolled while ENTERING — cancelling pending orders")
                    if _state.up_order:
                        _cancel_open(clob, _state.up_order, bet)
                    if _state.dn_order:
                        _cancel_open(clob, _state.dn_order, bet)
                    _state.up_order = _state.dn_order = None
                    _state.status   = "IDLE"

                _state.candle = candle
                _ws_subscribed_tokens.update({candle.up_token, candle.down_token})
                _state.entered_candle_slug = None
                log.info("Candle: %s  start=%s  end=%s",
                         candle.slug,
                         datetime.fromtimestamp(candle.start_ts, tz=timezone.utc).strftime("%H:%M:%S"),
                         datetime.fromtimestamp(candle.end_ts,   tz=timezone.utc).strftime("%H:%M:%S UTC"))

        candle = _state.candle
        now    = time.time()

        # ── Periodic state line ────────────────────────────────────────────────
        if now - _state.last_signal_log >= 15.0:
            up_bid, up_ask = _best_bid_ask(candle.up_token)
            dn_bid, dn_ask = _best_bid_ask(candle.down_token)
            pair_sum = round((up_ask or 0) + (dn_ask or 0), 4) if up_ask and dn_ask else None
            log.info(
                "STATE  %s  status=%s  btc=%.2f  up=%s/%s  dn=%s/%s  pair_sum=%s  "
                "buf=%d  spent=$%.4f  recv=$%.4f",
                candle.slug, _state.status, _latest_btc_price,
                f"{up_bid:.2f}" if up_bid else "-", f"{up_ask:.2f}" if up_ask else "-",
                f"{dn_bid:.2f}" if dn_bid else "-", f"{dn_ask:.2f}" if dn_ask else "-",
                f"{pair_sum:.4f}" if pair_sum else "-",
                len(_kline_buf), _state.total_spent, _state.total_recv,
            )
            _state.last_signal_log = now

        # ── IDLE: look for entry signal ────────────────────────────────────────
        if _state.status == "IDLE":
            if _state.entered_candle_slug == candle.slug:
                await asyncio.sleep(STRATEGY_TICK)
                continue

            # Need enough kline history to compute BB + RSI
            if len(_kline_buf) < MIN_KLINES_NEEDED:
                log.debug("Waiting for kline history (%d/%d)", len(_kline_buf), MIN_KLINES_NEEDED)
                await asyncio.sleep(STRATEGY_TICK)
                continue

            sig = compute_signals(_kline_buf_to_df())

            # Check pair-sum opportunity on Polymarket
            up_bid, up_ask = _best_bid_ask(candle.up_token)
            dn_bid, dn_ask = _best_bid_ask(candle.down_token)
            pair_sum = (up_ask + dn_ask) if (up_ask and dn_ask) else None

            if now - _state.last_signal_log >= 5.0:
                log.info(
                    "SCAN  rsi=%.1f  bb_w=%.4f/%.4f  vol_delta=%.2f  "
                    "signal=%s  pair_sum=%s",
                    sig["rsi"], sig["bb_width"], sig["bb_width_avg"], sig["vol_delta"],
                    sig["signal"],
                    f"{pair_sum:.4f}" if pair_sum else "n/a",
                )

            # Hold-to-resolution mode:
            # only enter when pair_sum has edge (< $1.00 total).
            if sig["signal"] and pair_sum is not None:
                if pair_sum >= MAX_PAIR_SUM:
                    log.info("SKIP entry: pair_sum %.4f >= %.2f", pair_sum, MAX_PAIR_SUM)
                    await asyncio.sleep(STRATEGY_TICK)
                    continue

                # Avoid new entries too close to expiry.
                # In BTC 5m markets, requiring >2 candles worth of runway is unrealistic.
                secs_left = candle.end_ts - now
                if secs_left < NEAR_EXPIRY_SECS:
                    log.info("SKIP entry: only %.0fs left on candle (need >%ds)",
                             secs_left, NEAR_EXPIRY_SECS)
                    await asyncio.sleep(STRATEGY_TICK)
                    continue

                # Size: fixed shares per side.
                up_shares = round(shares, 4)
                dn_shares = round(shares, 4)
                est_cost = up_ask * up_shares + dn_ask * dn_shares

                if bet:
                    balance = get_usdc_balance(clob)
                    if balance < est_cost:
                        log.warning("SKIP entry: insufficient balance $%.4f < est_cost $%.4f",
                                    balance, est_cost)
                        await asyncio.sleep(STRATEGY_TICK)
                        continue

                log.info(
                    "ENTER HEDGE (maker)  btc=%.2f  up_ask=%.4f→post@%.4f  "
                    "dn_ask=%.4f→post@%.4f  shares=%.4f/%.4f  est_cost=$%.4f  pair_sum=%.4f  rsi=%.1f  "
                    "bb_exp=%.1f%%  vol_delta=%.2f",
                    _latest_btc_price,
                    up_ask, max(TICK, round(up_ask - MAKER_OFFSET, 2)),
                    dn_ask, max(TICK, round(dn_ask - MAKER_OFFSET, 2)),
                    up_shares, dn_shares, est_cost,
                    pair_sum,
                    sig["rsi"], (sig["bb_width"] / sig["bb_width_avg"] - 1) * 100,
                    sig["vol_delta"],
                )

                # Post maker limit buys 1 tick below the ask on both sides
                up_order = _place_maker_buy(clob, "Up",   candle.up_token,   up_ask, up_shares, bet)
                dn_order = _place_maker_buy(clob, "Down", candle.down_token, dn_ask, dn_shares, bet)

                if up_order and dn_order:
                    _state.up_order  = up_order
                    _state.dn_order  = dn_order
                    _state.entry_ts  = now    # used for ENTERING timeout
                    _state.status    = "ENTERING"
                    log.info("ENTERING  waiting for both maker orders to fill  "
                             "(reprice every %.0fs, abandon if <%.0fs to expiry)",
                             REPRICE_INTERVAL, NEAR_EXPIRY_SECS)
                else:
                    log.error("Entry order placement failed — cancelling any partial order")
                    if up_order:
                        _cancel_open(clob, up_order, bet)
                    if dn_order:
                        _cancel_open(clob, dn_order, bet)

        # ── ENTERING: poll fills; aggressively reprice unfilled orders ──────────
        elif _state.status == "ENTERING":
            waited         = now - _state.entry_ts
            secs_to_expiry = candle.end_ts - now

            if bet:
                try:
                    open_orders = await asyncio.to_thread(get_all_open_orders, clob)
                    open_ids = {
                        str(o.get("id") or o.get("orderID") or "")
                        for o in open_orders
                    }
                    poll_ok = True
                except Exception as exc:
                    log.warning("Could not fetch open orders: %s — will retry", exc)
                    open_ids = None
                    poll_ok  = False
            else:
                # Dry-run: simulate immediate fill
                open_ids = set()
                poll_ok  = True

            if poll_ok and open_ids is not None:
                up_open = _state.up_order.order_id in open_ids
                dn_open = _state.dn_order.order_id in open_ids

                if not up_open and not dn_open:
                    # Both filled — record trade and immediately return to IDLE.
                    # We hold to market resolution; no normal sell exit path here.
                    _state.entry_btc_price = _latest_btc_price
                    _state.total_spent    += _state.up_order.usdc_spent + _state.dn_order.usdc_spent
                    try:
                        _state.trade_id = _db_insert_trade(
                            candle, _state.up_order, _state.dn_order,
                            _state.entry_btc_price, bet,
                        )
                    except Exception as exc:
                        log.warning("DB insert failed: %s", exc)
                    log.info(
                        "BOTH FILLED after %.1fs → HOLD_TO_RESOLUTION  "
                        "entry_btc=%.2f  up_cost=$%.4f  dn_cost=$%.4f  total_spent=$%.4f  "
                        "db_id=%s",
                        waited, _state.entry_btc_price,
                        _state.up_order.usdc_spent, _state.dn_order.usdc_spent,
                        _state.total_spent, _state.trade_id,
                    )
                    _state.status          = "IDLE"
                    _state.entered_candle_slug = candle.slug
                    _state.trade_id        = None
                    _state.up_order        = None
                    _state.dn_order        = None
                    _state.entry_btc_price = 0.0
                    _state.entry_ts        = 0.0

                elif secs_to_expiry < NEAR_EXPIRY_SECS:
                    # Near expiry — stop chasing unfilled leg(s); no sell-back in hold mode.
                    log.warning(
                        "NEAR EXPIRY (%.0fs left)  up=%s  dn=%s — cancelling open leg(s), holding any filled leg",
                        secs_to_expiry,
                        "open" if up_open else "FILLED",
                        "open" if dn_open else "FILLED",
                    )
                    if up_open:
                        _cancel_open(clob, _state.up_order, bet)
                    if dn_open:
                        _cancel_open(clob, _state.dn_order, bet)
                    if not up_open or not dn_open:
                        log.warning("Partial fill carried forward (no forced exit in hold-to-resolution mode)")
                    _state.up_order = _state.dn_order = None
                    _state.status   = "IDLE"

                else:
                    # Still time — reprice only when pair_sum edge remains (< $1.00).
                    cur_up_ask = _best_bid_ask(candle.up_token)[1]
                    cur_dn_ask = _best_bid_ask(candle.down_token)[1]
                    cur_pair_sum = (
                        cur_up_ask + cur_dn_ask
                        if (cur_up_ask is not None and cur_dn_ask is not None)
                        else None
                    )
                    if cur_pair_sum is None:
                        log.info("REPRICE skipped: missing ask(s) on one or both legs")
                    elif cur_pair_sum >= MAX_PAIR_SUM:
                        log.info(
                            "REPRICE skipped: pair_sum %.4f >= %.2f (keeping existing orders)",
                            cur_pair_sum, MAX_PAIR_SUM,
                        )

                    # Reprice each open leg only if edge remains.
                    for order_attr, side, token_id in (
                        ("up_order", "Up",   candle.up_token),
                        ("dn_order", "Down", candle.down_token),
                    ):
                        if cur_pair_sum is None or cur_pair_sum >= MAX_PAIR_SUM:
                            continue
                        order: LiveOrder = getattr(_state, order_attr)
                        if not order or order.order_id not in open_ids:
                            continue  # already filled
                        ref_ts = order.last_reprice_ts or order.posted_ts
                        if now - ref_ts < REPRICE_INTERVAL:
                            continue  # not yet time to reprice
                        _, cur_ask = _best_bid_ask(token_id)
                        if not cur_ask:
                            continue
                        _cancel_open(clob, order, bet)
                        new_order = _place_maker_buy(
                            clob, side, token_id, cur_ask, order.shares, bet,
                            offset=MAKER_OFFSET,   # keep maker edge
                        )
                        if new_order:
                            new_order.last_reprice_ts = now
                            new_order.reprice_count   = order.reprice_count + 1
                            setattr(_state, order_attr, new_order)
                            log.info(
                                "REPRICED %s #%d  old@%.4f → new@%.4f  "
                                "%.0fs to expiry",
                                side, new_order.reprice_count,
                                order.price, new_order.price,
                                secs_to_expiry,
                            )

                    log.info(
                        "ENTERING  waited=%.1fs  Up=%s  Down=%s  btc=%.2f  "
                        "%.0fs to expiry",
                        waited,
                        "open" if up_open else "FILLED",
                        "open" if dn_open else "FILLED",
                        _latest_btc_price,
                        secs_to_expiry,
                    )

        # Deprecated status from legacy fast-exit flow.
        # Hold-to-resolution mode never uses HOLDING.
        elif _state.status == "HOLDING":
            log.warning("Deprecated HOLDING status encountered; resetting to IDLE")
            _state.status          = "IDLE"
            _state.trade_id        = None
            _state.up_order        = None
            _state.dn_order        = None
            _state.entry_btc_price = 0.0
            _state.entry_ts        = 0.0

        await asyncio.sleep(STRATEGY_TICK)


# ── Settlement loop — resolution polling + redemption ─────────────────────────

def _run_settle(bet: bool) -> None:
    """
    Sync worker (runs in a thread):
      1. Find all DB trades past candle_end_ts without a recorded winner.
      2. Check the CTF contract for resolution.
      3. Update winner in DB.
      4. Scan wallet positions (get_positions) for any stranded tokens
         (crash-recovery: bot died while HOLDING).
      5. Redeem via relayer for any resolved winner tokens we still hold.
    """
    trades = _db_get_unresolved_past_end()
    if not trades:
        return

    # Lazy-load web3 + CTF contract (optional — skip if not configured)
    ctf = None
    try:
        from automata.eth_15m import (
            _get_web3_and_ctf,
            _redeem_condition_positions_relayer,
            _resolved_side_from_chain,
        )
        _, ctf = _get_web3_and_ctf()
    except Exception as exc:
        log.debug("web3/CTF unavailable for settlement: %s", exc)
        _redeem_condition_positions_relayer = None
        _resolved_side_from_chain           = None

    # Current wallet positions — for crash-recovery redemption
    funder = os.getenv("POLYMARKET_FUNDER", "")
    held: dict[str, float] = {}
    if funder:
        try:
            for p in get_positions(funder):
                held[p["token_id"]] = float(p["size"])
        except Exception as exc:
            log.warning("get_positions failed in settle: %s", exc)

    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")

    for trade in trades:
        slug   = trade["candle_slug"]
        cid    = trade.get("condition_id")
        tid    = trade["id"]

        # ── Determine winner ──────────────────────────────────────────────────
        winner = None
        if ctf and cid and _resolved_side_from_chain:
            try:
                winner = _resolved_side_from_chain(ctf, cid)
            except Exception as exc:
                log.debug("chain resolution check failed for %s: %s", slug, exc)

        if winner:
            try:
                _db_update_resolution(tid, winner)
            except Exception as exc:
                log.warning("DB resolution update failed: %s", exc)
            pnl_str = f"${trade.get('pnl', 0):+.4f}" if trade.get("pnl") is not None else "n/a"
            log.info("RESOLVED  %s  winner=%s  exit_pnl=%s", slug, winner, pnl_str)
        else:
            log.debug("Not yet resolved: %s", slug)

        # ── Crash-recovery redemption ─────────────────────────────────────────
        up_held = held.get(trade.get("up_token",   ""), 0.0)
        dn_held = held.get(trade.get("down_token", ""), 0.0)
        stranded = up_held > 0.01 or dn_held > 0.01

        if stranded:
            log.info("STRANDED tokens  %s  up_held=%.4f  dn_held=%.4f",
                     slug, up_held, dn_held)

        if stranded and winner and bet and private_key and cid and _redeem_condition_positions_relayer:
            if not trade.get("redeemed"):
                log.info("Redeeming stranded position  %s  winner=%s", slug, winner)
                try:
                    tx = _redeem_condition_positions_relayer(private_key, cid)
                    if tx:
                        _db_update_redeemed(tid, tx)
                        log.info("REDEEMED  %s  tx=%s", slug, tx)
                    else:
                        log.warning("Redeem returned no tx hash for %s", slug)
                except Exception as exc:
                    log.warning("Redeem failed for %s: %s", slug, exc)
        elif stranded and not bet:
            log.info("[DRY] Would redeem stranded position  %s  winner=%s", slug, winner or "pending")


async def _settle_loop(bet: bool) -> None:
    """Async wrapper — runs _run_settle in a thread every SETTLE_INTERVAL seconds."""
    await asyncio.sleep(15)   # let the bot start up first
    while True:
        try:
            await asyncio.to_thread(_run_settle, bet)
        except Exception as exc:
            log.warning("settle_loop error: %s", exc)
        await asyncio.sleep(SETTLE_INTERVAL)


# ── Display loop ──────────────────────────────────────────────────────────────

CLEAR  = "\033[2J\033[H"
BOLD   = "\033[1m"
RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"
SEP    = "  " + "─" * 90


async def _display_loop(budget: float, bet: bool, stop_pct: float) -> None:
    while True:
        await asyncio.sleep(1.0)
        try:
            _render(budget, bet, stop_pct)
        except Exception:
            pass


def _render(budget: float, bet: bool, stop_pct: float) -> None:
    now    = time.time()
    ts     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    candle = _state.candle
    mode   = f"{GREEN}LIVE{RESET}" if bet else f"{YELLOW}DRY-RUN{RESET}"

    lines = [
        "",
        f"  {BOLD}[{ts}]  BTC 5m Hedge Bot — Fast-In Fast-Out  [{mode}{BOLD}]{RESET}",
        SEP,
    ]

    if candle:
        secs_left = max(0, int(candle.end_ts - now))
        up_bid, up_ask = _best_bid_ask(candle.up_token)
        dn_bid, dn_ask = _best_bid_ask(candle.down_token)
        pair_sum = (up_ask + dn_ask) if (up_ask and dn_ask) else None
        ps_col = CYAN
        lines += [
            f"  Candle  {CYAN}{candle.slug}{RESET}  {secs_left}s left",
            f"  Book    Up  bid={up_bid:.4f if up_bid else '-':>6}  ask={up_ask:.4f if up_ask else '-':>6}",
            f"          Dn  bid={dn_bid:.4f if dn_bid else '-':>6}  ask={dn_ask:.4f if dn_ask else '-':>6}",
            f"  Pair sum  {ps_col}{pair_sum:.4f if pair_sum else '-'}{RESET}  "
            f"(target < 1.00)",
        ]

    lines.append(SEP)
    status_col = GREEN if _state.status == "HOLDING" else (YELLOW if _state.status == "ENTERING" else DIM)
    lines += [
        f"  BTC spot   {BOLD}${_latest_btc_price:,.2f}{RESET}",
        f"  Status     {status_col}{_state.status}{RESET}",
        f"  Kline buf  {len(_kline_buf)}/{MIN_KLINES_NEEDED} candles needed",
    ]

    if _state.status == "ENTERING" and _state.up_order and _state.dn_order:
        waited     = int(now - _state.entry_ts)
        secs_left  = int(candle.end_ts - now) if candle else 0
        up_rp  = _state.up_order.reprice_count
        dn_rp  = _state.dn_order.reprice_count
        lines += [
            f"  Waiting    {waited}s elapsed  {secs_left}s to expiry  "
            f"(abandon <{NEAR_EXPIRY_SECS}s)  reprice every {int(REPRICE_INTERVAL)}s",
            f"  Up order   id={_state.up_order.order_id[:12]}  "
            f"post@{_state.up_order.price:.4f}  shares={_state.up_order.shares:.4f}  reprices={up_rp}",
            f"  Down order id={_state.dn_order.order_id[:12]}  "
            f"post@{_state.dn_order.price:.4f}  shares={_state.dn_order.shares:.4f}  reprices={dn_rp}",
        ]

    if _state.status == "HOLDING" and _state.entry_btc_price > 0:
        move_pct = (_latest_btc_price - _state.entry_btc_price) / _state.entry_btc_price * 100
        move_col = GREEN if move_pct > 0 else RED
        held_s   = int(now - _state.entry_ts)
        lines += [
            f"  Entry BTC  ${_state.entry_btc_price:,.2f}  move={move_col}{move_pct:+.3f}%{RESET}  "
            f"(stop at ±{stop_pct*100:.2f}%)",
            f"  Held       {held_s}s / {MAX_CANDLES_HELD * CANDLE_SECONDS}s timeout",
        ]
        if _state.up_order:
            lines.append(f"  Up order   id={_state.up_order.order_id[:12]}  "
                         f"buy@{_state.up_order.price:.4f}  shares={_state.up_order.shares:.4f}")
        if _state.dn_order:
            lines.append(f"  Down order id={_state.dn_order.order_id[:12]}  "
                         f"buy@{_state.dn_order.price:.4f}  shares={_state.dn_order.shares:.4f}")

    lines.append(SEP)
    pnl = _state.total_recv - _state.total_spent
    pnl_col = GREEN if pnl >= 0 else RED
    lines += [
        f"  Session    trades={len(_state.trades)}  "
        f"spent=${_state.total_spent:.4f}  recv=${_state.total_recv:.4f}  "
        f"pnl={pnl_col}{pnl:+.4f}{RESET}",
    ]
    if _state.trades:
        lines.append(f"  Last trade: {_state.trades[-1]}")

    print(CLEAR + "\n".join(lines), flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

def _build_clob_client():
    host         = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    private_key  = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    api_key      = os.getenv("CLOB_API_KEY", "")
    api_secret   = os.getenv("CLOB_SECRET", "")
    api_passphrase = os.getenv("CLOB_PASS", "")
    funder       = os.getenv("POLYMARKET_FUNDER")
    sig_type     = int(os.getenv("POLYMARKET_SIG_TYPE", "0"))

    if not api_key and private_key:
        creds = derive_api_credentials(host, private_key, funder=funder, signature_type=sig_type)
        api_key, api_secret, api_passphrase = creds.api_key, creds.api_secret, creds.api_passphrase

    return build_client(host, private_key, api_key, api_secret, api_passphrase,
                        funder=funder, signature_type=sig_type)


async def _run(budget: float, shares: float, max_balance: float, stop_pct: float, bet: bool) -> None:
    _init_logging()
    _init_hedge_db()

    clob = None
    if bet:
        clob = _build_clob_client()
        balance = get_usdc_balance(clob)
        log.info("USDC balance: $%.4f", balance)
        if balance < budget:
            log.error("Balance $%.4f < budget $%.2f — aborting", balance, budget)
            sys.exit(1)

    # Bootstrap kline history from Binance REST before connecting WS
    await asyncio.to_thread(_bootstrap_klines)

    # Pre-warm candle
    first_candle = await asyncio.to_thread(_fetch_btc_candle)
    if first_candle:
        _state.candle = first_candle
        _ws_subscribed_tokens.update({first_candle.up_token, first_candle.down_token})
        log.info("Initial candle: %s", first_candle.slug)

    log.info("Starting hedge bot  budget=$%.2f  shares=%.4f/side  stop_pct=%.3f%%  bet=%s",
             budget, shares, stop_pct * 100, bet)

    await asyncio.gather(
        _binance_ws(),
        _poly_ws(),
        _strategy_loop(clob, budget, shares, stop_pct, bet),
        _display_loop(budget, bet, stop_pct),
        _settle_loop(bet),
    )


def main() -> None:
    global BB_WIDTH_FACTOR   # must be declared before any read of this name

    p = argparse.ArgumentParser(
        description="BTC 5m Fast-In Fast-Out Hedge Bot",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bet",         action="store_true",
                   help="Place real orders (default: dry-run)")
    p.add_argument("--budget",      type=float, default=DEFAULT_BUDGET,
                   help="Total USDC per hedge entry (split evenly Up/Down)")
    p.add_argument("--shares",      type=float, default=DEFAULT_SHARES,
                   help="Fixed shares per side for each hedge entry")
    p.add_argument("--max-balance", type=float, default=DEFAULT_MAX_BALANCE,
                   help="Safety cap: warn if USDC balance > this")
    p.add_argument("--stop-pct",    type=float, default=DEFAULT_STOP_PCT,
                   help="BTC spot move threshold that triggers exit (0.003 = 0.3%%)")
    p.add_argument("--bb-factor",   type=float, default=BB_WIDTH_FACTOR,
                   help="BB width expansion factor required for entry signal")
    args = p.parse_args()

    BB_WIDTH_FACTOR = args.bb_factor

    try:
        asyncio.run(_run(
            args.budget,
            max(5.0, float(args.shares)),
            args.max_balance,
            args.stop_pct,
            args.bet,
        ))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
