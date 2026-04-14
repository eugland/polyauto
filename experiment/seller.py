#!/usr/bin/env python3
"""
experiment/seller.py — AI-driven simulated sell-side strategy for Polymarket 5m markets.

Strategy
--------
Uses Binance real-time momentum (dual EMA on aggTrade stream) as direction signal:

  BULLISH Binance  →  sell Down token  (Down will resolve $0 when price goes up, keep premium)
  BEARISH Binance  →  sell Up   token  (Up   will resolve $0 when price goes down)
  NEUTRAL          →  no trade

Enters only when ask ≥ --min-ask (default 0.55). This ensures positive expected
value even when the signal is only marginally better than random:
  EV > 0 requires signal_accuracy > (1 - ask_price)
  At ask=0.55, we need >45% accuracy — easy bar to clear with momentum.

Budget
------
$5 USDC per asset split evenly: $2.50 per side (Up/Down).
  shares = 2.50 / ask_price   (target receiving $2.50 in premium)
  max_loss_per_trade = shares × (1 - ask_price)

Fill emulation
--------------
Rule 1 — Level removal (primary):
  A price_change event removes (size→0) the ask level at EXACTLY our posted price.
  Interpretation: a buyer swept asks up to our level, our simulated order was hit.

Rule 2 — Bid overtake (secondary):
  The best bid rises to ≥ our ask price. A buyer is willing to pay more than we ask;
  we'd be matched immediately at our price.

Unfilled orders at candle expiry are marked "expired" (never filled).

P&L (simulated)
---------------
At candle close we compare Binance price to the recorded candle-open price:
  actual_outcome = "Up" if close >= open else "Down"
For each filled sell at price P of token_side T:
  win  (T resolves $0 — T ≠ actual_outcome): pnl = +shares × P
  loss (T resolves $1 — T = actual_outcome):  pnl = -shares × (1 − P)

Usage
-----
    python -m experiment.seller
    python -m experiment.seller --assets ETH DOGE
    python -m experiment.seller --budget 5.0 --min-ask 0.60
    python -m experiment.seller --display 0.5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
import websockets

# ── Config ─────────────────────────────────────────────────────────────────

GAMMA_API       = "https://gamma-api.polymarket.com/events"
WS_POLY         = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_BINANCE      = "wss://stream.binance.com:9443"
WS_RECONNECT    = 3
WS_PING         = 20

# Binance EMA signal
SHORT_EMA_ALPHA = 0.30   # fast (reacts quickly to price changes)
LONG_EMA_ALPHA  = 0.03   # slow baseline
MIN_SIGNAL_STR  = 0.0003  # 0.03% divergence triggers a signal

# Strategy params
REPRICE_TICKS   = 2      # reprice if market best-ask is >2 ticks below our ask
TICK_SIZE       = 0.01

# Book metrics
DEPTH_LEVELS    = 5
RATE_WINDOW     = 10     # seconds window for ToB rate

BINANCE_PAIRS: dict[str, str] = {
    "BTC":  "btcusdt",
    "ETH":  "ethusdt",
    "BNB":  "bnbusdt",
    "XRP":  "xrpusdt",
    "DOGE": "dogeusdt",
    "SOL":  "solusdt",
}

DEFAULT_ASSETS = ["ETH", "DOGE"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("seller")

# ── Shared book state (populated by Polymarket WS) ─────────────────────────

_book_cache: dict[str, dict]  = {}   # token_id → {bids:[…], asks:[…]}
_event_ts:   dict[str, deque] = {}   # token_id → deque[wall_ts]  (for ToB rate)

# ── Dataclasses ─────────────────────────────────────────────────────────────

@dataclass
class BinanceSignal:
    """Dual-EMA momentum signal computed from aggTrade stream."""
    pair:       str
    last_price: float = 0.0
    short_ema:  float = 0.0
    long_ema:   float = 0.0
    ready:      bool  = False

    def update(self, price: float) -> None:
        if not self.ready:
            self.short_ema = self.long_ema = price
            self.ready = True
        else:
            self.short_ema = SHORT_EMA_ALPHA * price + (1 - SHORT_EMA_ALPHA) * self.short_ema
            self.long_ema  = LONG_EMA_ALPHA  * price + (1 - LONG_EMA_ALPHA)  * self.long_ema
        self.last_price = price

    @property
    def direction(self) -> str:
        """BULLISH / BEARISH / NEUTRAL based on EMA crossover."""
        if not self.ready:
            return "NEUTRAL"
        diff = (self.short_ema - self.long_ema) / self.long_ema
        if   diff >  MIN_SIGNAL_STR: return "BULLISH"
        elif diff < -MIN_SIGNAL_STR: return "BEARISH"
        return "NEUTRAL"

    @property
    def strength_pct(self) -> float:
        if not self.ready or self.long_ema == 0:
            return 0.0
        return abs(self.short_ema - self.long_ema) / self.long_ema * 100


@dataclass
class SimOrder:
    """One simulated sell order."""
    order_id:   str
    asset:      str
    token_side: str    # "Up" or "Down" — the token we are selling
    token_id:   str
    ask_price:  float  # price we're selling at (per share, in USDC)
    shares:     float  # how many shares sold
    posted_ts:  float
    candle_end: float
    status:     str   = "pending"   # pending | filled | expired | repriced
    fill_ts:    float | None = None
    pnl:        float | None = None
    outcome:    str   | None = None  # "win" | "loss"

    @property
    def usdc_received(self) -> float:
        """USDC collected at fill (our revenue)."""
        return self.ask_price * self.shares

    @property
    def max_loss(self) -> float:
        """Maximum USDC loss if token resolves $1."""
        return (1.0 - self.ask_price) * self.shares

    @property
    def age_s(self) -> float:
        return time.time() - self.posted_ts


@dataclass
class AssetSim:
    """Per-asset simulator state."""
    asset:             str
    budget:            float   # USDC per asset; budget/2 used per side per trade
    min_ask:           float   # only sell tokens priced >= this
    signal:            BinanceSignal
    up_token:          str | None = None
    down_token:        str | None = None
    slug:              str | None = None
    candle_end:        float | None = None
    candle_open_price: float | None = None  # Binance price at start of current candle
    # active orders (at most one per side)
    pending: dict[str, SimOrder] = field(default_factory=dict)   # "Up"/"Down" → order
    # completed orders
    history: list[SimOrder] = field(default_factory=list)
    # cumulative stats
    total_pnl: float = 0.0
    wins:      int   = 0
    losses:    int   = 0
    fills:     int   = 0
    expirations: int = 0


# Module-level simulator registry (populated in run())
_sims: dict[str, AssetSim] = {}


# ── Order book helpers ──────────────────────────────────────────────────────

def _apply_price_change(book: dict, changes: list[dict]) -> list[tuple[float, str]]:
    """
    Apply incremental level updates. Returns list of (price, "asks"|"bids") for every
    level that was REMOVED (size → 0). These are candidates for fill detection.
    """
    removed: list[tuple[float, str]] = []
    for ch in changes or []:
        try:
            p  = float(ch["price"])
            s  = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        existed = any(abs(float(lv.get("price", 0) or 0) - p) < 1e-9 for lv in levels)
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})
        elif existed:
            removed.append((p, key))
    return removed


def _handle_poly_event(ev: dict) -> list[tuple[str, float, str]]:
    """
    Process one Polymarket WS event. Updates book cache.
    Returns list of (token_id, removed_price, "asks"|"bids") for fill detection.
    """
    now = time.time()
    et  = ev.get("event_type")
    tid = str(ev.get("asset_id") or "")
    if not tid:
        return []

    removals: list[tuple[str, float, str]] = []

    if et == "book":
        _book_cache[tid] = {
            "bids": [dict(b) for b in (ev.get("bids") or [])],
            "asks": [dict(a) for a in (ev.get("asks") or [])],
        }
    elif et == "price_change":
        book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
        for p, side in _apply_price_change(book, ev.get("changes") or []):
            removals.append((tid, p, side))
        # track event timestamp for ToB rate
        dq = _event_ts.setdefault(tid, deque())
        dq.append(now)
        while dq and dq[0] < now - RATE_WINDOW:
            dq.popleft()

    return removals


def _best_bid_ask(token_id: str) -> tuple[float | None, float | None]:
    book = _book_cache.get(token_id, {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = max((float(b["price"]) for b in bids), default=None)
    best_ask = min((float(a["price"]) for a in asks), default=None)
    return best_bid, best_ask


def _book_depth(token_id: str) -> tuple[float, float]:
    """Sum of top DEPTH_LEVELS bid and ask sizes (in shares)."""
    book = _book_cache.get(token_id, {})
    bids = sorted(book.get("bids", []), key=lambda x: -float(x.get("price", 0)))
    asks = sorted(book.get("asks", []), key=lambda x:  float(x.get("price", 0)))
    bd = sum(float(b.get("size", 0)) for b in bids[:DEPTH_LEVELS])
    ad = sum(float(a.get("size", 0)) for a in asks[:DEPTH_LEVELS])
    return bd, ad


def _tob_rate(token_id: str) -> float:
    dq = _event_ts.get(token_id)
    return len(dq) / RATE_WINDOW if dq else 0.0


# ── Market discovery ────────────────────────────────────────────────────────

def _load_field(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v


def _get_current_5m_tokens(asset: str) -> dict[str, Any] | None:
    """Returns {up, down, slug, candle_end} or None."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300
    for delta in (0, -1, 1):
        slug = f"{asset.lower()}-updown-5m-{bucket + delta * 300}"
        try:
            data = requests.get(
                f"{GAMMA_API}?slug={slug}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            ).json()
            if not isinstance(data, list) or not data:
                continue
            mkt = (data[0].get("markets") or [None])[0]
            if not mkt or mkt.get("closed"):
                continue
            outcomes  = _load_field(mkt.get("outcomes")) or []
            token_ids = _load_field(mkt.get("clobTokenIds")) or []
            up_token = down_token = None
            for i, name in enumerate(outcomes):
                if i >= len(token_ids):
                    continue
                label = str(name).strip().lower()
                if   label == "up":   up_token   = str(token_ids[i])
                elif label == "down": down_token = str(token_ids[i])
            if up_token and down_token:
                candle_ts = int(slug.rsplit("-", 1)[-1])
                return {
                    "up":         up_token,
                    "down":       down_token,
                    "slug":       slug,
                    "candle_end": candle_ts + 300,
                }
        except Exception as exc:
            log.debug("Market fetch error %s: %s", slug, exc)
    return None


def _discover_all(assets: list[str]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(assets)) as pool:
        futures = {pool.submit(_get_current_5m_tokens, a): a for a in assets}
        for fut, asset in futures.items():
            result = fut.result()
            if result:
                found[asset] = result
                log.info("Found %-6s  %s", asset, result["slug"])
            else:
                log.warning("No active 5m market for %s", asset)
    return found


# ── Strategy ────────────────────────────────────────────────────────────────

def _decide_sell(sim: AssetSim) -> tuple[str, float] | None:
    """
    Return (token_side, ask_price) to post, or None if no trade.

    BULLISH → sell Down (expect Down resolves $0 as price went up)
    BEARISH → sell Up   (expect Up resolves $0 as price went down)
    """
    direction = sim.signal.direction
    if direction == "NEUTRAL" or not sim.signal.ready:
        return None

    token_side = "Down" if direction == "BULLISH" else "Up"

    # Already have an active order on this side
    if token_side in sim.pending:
        return None

    token_id = sim.down_token if token_side == "Down" else sim.up_token
    if not token_id:
        return None

    _, best_ask = _best_bid_ask(token_id)
    if best_ask is None or best_ask < sim.min_ask:
        return None

    return token_side, best_ask


def _post_order(sim: AssetSim, token_side: str, ask_price: float) -> SimOrder:
    token_id = sim.down_token if token_side == "Down" else sim.up_token
    shares   = (sim.budget / 2.0) / ask_price
    order = SimOrder(
        order_id   = str(uuid.uuid4())[:8],
        asset      = sim.asset,
        token_side = token_side,
        token_id   = token_id,
        ask_price  = ask_price,
        shares     = shares,
        posted_ts  = time.time(),
        candle_end = sim.candle_end,
    )
    sim.pending[token_side] = order
    log.info("[%s] POST sell %s @ $%.4f  shares=%.2f  recv=$%.2f  max_loss=$%.2f",
             sim.asset, token_side, ask_price, shares,
             order.usdc_received, order.max_loss)
    return order


def _fill_order(sim: AssetSim, token_side: str, reason: str) -> None:
    order = sim.pending.pop(token_side, None)
    if not order:
        return
    order.status  = "filled"
    order.fill_ts = time.time()
    sim.fills    += 1
    sim.history.append(order)
    log.info("[%s] FILL  %s @ $%.4f  reason=%s  age=%.1fs",
             sim.asset, token_side, order.ask_price, reason, order.age_s)


def _expire_order(sim: AssetSim, token_side: str, reason: str = "expired") -> None:
    order = sim.pending.pop(token_side, None)
    if not order:
        return
    order.status = reason
    sim.expirations += 1
    sim.history.append(order)
    log.info("[%s] %s %s (unfilled, age=%.1fs)",
             sim.asset, reason.upper(), token_side, order.age_s)


def _settle_order(sim: AssetSim, order: SimOrder) -> None:
    """Determine win/loss P&L using Binance price vs candle open."""
    if order.pnl is not None:
        return
    if sim.candle_open_price is None or sim.signal.last_price == 0:
        return

    actual_up     = sim.signal.last_price >= sim.candle_open_price
    actual_outcome = "Up" if actual_up else "Down"

    if order.token_side != actual_outcome:
        # The token we sold resolves $0 → profit
        order.pnl     = order.shares * order.ask_price
        order.outcome = "win"
        sim.wins     += 1
    else:
        # The token we sold resolves $1 → loss
        order.pnl     = -order.shares * (1.0 - order.ask_price)
        order.outcome = "loss"
        sim.losses   += 1

    sim.total_pnl += order.pnl
    log.info("[%s] SETTLE %s: %s  pnl=$%+.4f  total=$%+.4f",
             sim.asset, order.token_side, order.outcome, order.pnl, sim.total_pnl)


def _check_fills_from_removals(sim: AssetSim, removals: list[tuple[str, float, str]]) -> None:
    """
    Fill Rule 1: ask level at our exact price was removed → filled.
    Called immediately after each WS message that contains level removals.
    """
    for token_side, order in list(sim.pending.items()):
        for tid, removed_price, side in removals:
            if tid == order.token_id and side == "asks":
                if abs(removed_price - order.ask_price) < 5e-4:  # within $0.0005
                    _fill_order(sim, token_side, reason="level-removed")
                    break


def _check_fills_bid_overtake(sim: AssetSim) -> None:
    """
    Fill Rule 2: best bid has crossed above our ask → immediate fill.
    Called on strategy tick (not per-message, to avoid busy-loop).
    """
    for token_side, order in list(sim.pending.items()):
        best_bid, _ = _best_bid_ask(order.token_id)
        if best_bid is not None and best_bid >= order.ask_price:
            _fill_order(sim, token_side, reason="bid-overtook")


def _reprice_if_needed(sim: AssetSim) -> None:
    """
    If the market best ask has dropped > REPRICE_TICKS below ours, cancel and repost.
    This keeps us competitive without chasing every tick.
    """
    for token_side, order in list(sim.pending.items()):
        _, best_ask = _best_bid_ask(order.token_id)
        if best_ask is None:
            continue
        gap = order.ask_price - best_ask
        if gap >= REPRICE_TICKS * TICK_SIZE:
            log.info("[%s] REPRICE %s: $%.4f → $%.4f (gap=%.4f)",
                     sim.asset, token_side, order.ask_price, best_ask, gap)
            _expire_order(sim, token_side, reason="repriced")
            # Fresh decision will be made on next strategy tick


def _check_candle_expiry(sim: AssetSim) -> None:
    """When candle closes: expire pending orders, settle filled ones."""
    if sim.candle_end is None or time.time() < sim.candle_end:
        return
    for ts in list(sim.pending.keys()):
        _expire_order(sim, ts, reason="expired")
    for order in sim.history:
        if order.status == "filled" and order.pnl is None:
            _settle_order(sim, order)


# ── WebSocket loops ──────────────────────────────────────────────────────────

async def _binance_ws(assets: list[str]) -> None:
    """
    Binance combined aggTrade stream for all requested assets.
    Each message updates the dual-EMA signal for the relevant asset.

    Combined stream URL:
      wss://stream.binance.com:9443/stream?streams=ethusdt@aggTrade/dogeusdt@aggTrade
    Each message looks like: {"stream": "ethusdt@aggTrade", "data": { "p": "...", ... }}
    """
    pairs   = [BINANCE_PAIRS[a] for a in assets if a in BINANCE_PAIRS]
    streams = "/".join(f"{p}@aggTrade" for p in pairs)
    url     = f"{WS_BINANCE}/stream?streams={streams}"

    # Reverse map: binance_pair → asset
    pair_to_asset = {BINANCE_PAIRS[a]: a for a in assets if a in BINANCE_PAIRS}

    while True:
        try:
            async with websockets.connect(url, ping_interval=WS_PING, max_size=2**20) as ws:
                log.info("Binance WS connected: %s", streams)
                async for raw in ws:
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        if data.get("e") != "aggTrade":
                            continue
                        pair  = data["s"].lower()
                        price = float(data["p"])
                        asset = pair_to_asset.get(pair)
                        if asset and asset in _sims:
                            _sims[asset].signal.update(price)
                    except Exception:
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Binance WS error: %s — retry in %ds", exc, WS_RECONNECT)
            await asyncio.sleep(WS_RECONNECT)


async def _poly_ws(all_tokens: list[str]) -> None:
    """
    Single Polymarket WS subscription for all tokens.
    Subscribe message must include auth:{} even for public channel (server silently
    drops subscription otherwise — see crypto_5m_orderbook_ws.py).

    On each message batch:
      1. Parse all events → update book cache
      2. Collect ask-level removals
      3. Check fill Rule 1 (level-removed) immediately
    """
    sub = json.dumps({
        "auth":       {},
        "type":       "market",
        "assets_ids": all_tokens,
    })

    while True:
        try:
            async with websockets.connect(
                WS_POLY,
                ping_interval=WS_PING,
                ping_timeout=WS_PING,
                max_size=2**22,
            ) as ws:
                await ws.send(sub)
                log.info("Poly WS connected — %d tokens", len(all_tokens))

                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue

                    events   = payload if isinstance(payload, list) else [payload]
                    removals: list[tuple[str, float, str]] = []
                    for ev in events:
                        if isinstance(ev, dict):
                            removals.extend(_handle_poly_event(ev))

                    # Fill Rule 1: level-removal check on every WS message
                    if removals:
                        for sim in _sims.values():
                            _check_fills_from_removals(sim, removals)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Poly WS error: %s — retry in %ds", exc, WS_RECONNECT)
            await asyncio.sleep(WS_RECONNECT)


async def _strategy_tick(tick_interval: float = 5.0) -> None:
    """
    Periodic strategy loop (every tick_interval seconds):
      1. Expire/settle closed candles
      2. Refresh candle if expired
      3. Fill Rule 2: bid-overtake check
      4. Reprice if market moved
      5. Decide new sell orders
    """
    while True:
        now = time.time()
        for sim in _sims.values():
            try:
                # Step 1
                _check_candle_expiry(sim)

                # Step 2 — fetch new candle when current expired
                if sim.candle_end is None or now >= sim.candle_end:
                    result = await asyncio.to_thread(_get_current_5m_tokens, sim.asset)
                    if result and result.get("slug") != sim.slug:
                        sim.slug       = result["slug"]
                        sim.up_token   = result["up"]
                        sim.down_token = result["down"]
                        sim.candle_end = result["candle_end"]
                        sim.candle_open_price = sim.signal.last_price or None
                        log.info("[%s] New candle: %s  open=%.4f",
                                 sim.asset, result["slug"],
                                 sim.candle_open_price or 0)

                # Step 3
                _check_fills_bid_overtake(sim)

                # Step 4
                _reprice_if_needed(sim)

                # Step 5
                decision = _decide_sell(sim)
                if decision:
                    _post_order(sim, *decision)

            except Exception as exc:
                log.debug("Strategy tick error [%s]: %s", sim.asset, exc)

        await asyncio.sleep(tick_interval)


# ── Display ──────────────────────────────────────────────────────────────────

CLEAR   = "\033[2J\033[H"
HIDE_C  = "\033[?25l"
SHOW_C  = "\033[?25h"
BOLD    = "\033[1m"
RESET   = "\033[0m"
GREEN   = "\033[92m"
RED     = "\033[91m"
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
DIM     = "\033[2m"
MAGENTA = "\033[95m"
SEP     = "  " + "─" * 104


def _sig_colour(d: str) -> str:
    return GREEN if d == "BULLISH" else (RED if d == "BEARISH" else DIM)


def _pnl_colour(v: float) -> str:
    return GREEN if v >= 0 else RED


def _fmt_order_line(order: SimOrder, show_pnl: bool = False) -> str:
    if order.status == "pending":
        age    = int(order.age_s)
        status = f"{YELLOW}PENDING {age:3d}s{RESET}"
    elif order.status == "filled":
        status = f"{GREEN}FILLED{RESET}"
    elif order.status == "repriced":
        status = f"{DIM}REPRICED{RESET}"
    else:
        status = f"{DIM}{order.status.upper()}{RESET}"

    pnl_part = ""
    if show_pnl and order.pnl is not None:
        c = _pnl_colour(order.pnl)
        oc = GREEN if order.outcome == "win" else RED
        pnl_part = f"  {oc}{order.outcome.upper()}{RESET}  pnl={c}${order.pnl:+.4f}{RESET}"

    return (
        f"    {DIM}#{order.order_id}{RESET}  "
        f"sell {CYAN}{order.token_side:4s}{RESET}  "
        f"@${order.ask_price:.4f}  "
        f"shrs={order.shares:.2f}  "
        f"recv={GREEN}${order.usdc_received:.2f}{RESET}  "
        f"maxloss={RED}${order.max_loss:.2f}{RESET}  "
        f"{status}{pnl_part}"
    )


def _render() -> str:
    now = time.time()
    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "",
        f"  {BOLD}[{ts}]  Polymarket 5m Sell Simulator — AI-Driven (Binance momentum){RESET}",
        SEP,
    ]

    for asset, sim in _sims.items():
        sig      = sim.signal
        d        = sig.direction
        sc       = _sig_colour(d)
        candle_s = max(0, int((sim.candle_end or 0) - now))

        up_bid,   up_ask   = _best_bid_ask(sim.up_token   or "")
        down_bid, down_ask = _best_bid_ask(sim.down_token or "")

        up_bd,   up_ad   = _book_depth(sim.up_token   or "")
        down_bd, down_ad = _book_depth(sim.down_token or "")

        up_rate   = _tob_rate(sim.up_token   or "")
        down_rate = _tob_rate(sim.down_token or "")

        pnl_c = _pnl_colour(sim.total_pnl)

        # Header row for this asset
        lines.append(
            f"  {BOLD}{MAGENTA}{asset}{RESET}"
            f"  Binance: {CYAN}${sig.last_price:>10.5f}{RESET}"
            f"  Signal: {sc}{BOLD}{d:7s}{RESET}  ({sc}{sig.strength_pct:.3f}%{RESET})"
            f"  Candle: {candle_s:3d}s"
            f"  P&L: {pnl_c}${sim.total_pnl:+.4f}{RESET}"
            f"  W/L/F/X: {GREEN}{sim.wins}{RESET}/{RED}{sim.losses}{RESET}"
            f"/{GREEN}{sim.fills}{RESET}/{DIM}{sim.expirations}{RESET}"
        )

        # Book summary
        def _p(v: float | None, fmt: str = ".4f") -> str:
            return f"${v:{fmt}}" if v is not None else "  n/a "

        lines.append(
            f"    {DIM}Up  {RESET}"
            f"bid={CYAN}{_p(up_bid)}{RESET}  ask={CYAN}{_p(up_ask)}{RESET}"
            f"  depth B:{up_bd:5.1f} A:{up_ad:5.1f} shrs"
            f"  rate={up_rate:.1f}/s"
            f"    {DIM}Down{RESET}"
            f"  bid={CYAN}{_p(down_bid)}{RESET}  ask={CYAN}{_p(down_ask)}{RESET}"
            f"  depth B:{down_bd:5.1f} A:{down_ad:5.1f} shrs"
            f"  rate={down_rate:.1f}/s"
        )

        # Pending orders
        if sim.pending:
            lines.append(f"  {YELLOW}  ▸ Active orders:{RESET}")
            for order in sim.pending.values():
                lines.append(_fmt_order_line(order))
        else:
            lines.append(f"    {DIM}No active orders{RESET}")

        # Recent settled history (last 6)
        settled = [o for o in reversed(sim.history) if o.pnl is not None][:6]
        if settled:
            lines.append(f"  {DIM}  ▸ Recent fills:{RESET}")
            for order in settled:
                lines.append(_fmt_order_line(order, show_pnl=True))

        lines.append(SEP)

    # Footer totals
    total_pnl  = sum(s.total_pnl for s in _sims.values())
    total_fills = sum(s.fills for s in _sims.values())
    total_exp   = sum(s.expirations for s in _sims.values())
    first_sim   = next(iter(_sims.values()))
    budget_side = first_sim.budget / 2

    lines.append(
        f"  {BOLD}TOTAL  P&L: {_pnl_colour(total_pnl)}${total_pnl:+.4f}{RESET}"
        f"   fills={total_fills}  expirations={total_exp}"
        f"   {DIM}budget/side=${budget_side:.2f}  min-ask=${first_sim.min_ask:.2f}{RESET}"
    )
    lines.append(
        f"  {DIM}Fill rules: (1) ask level removed at our price  "
        f"(2) bid rises ≥ our ask   P&L uses Binance vs candle-open{RESET}"
    )
    lines.append(f"  {DIM}Press Ctrl+C to stop{RESET}")
    return "\n".join(lines)


async def _display_loop(interval: float) -> None:
    sys.stdout.write(HIDE_C)
    sys.stdout.flush()
    try:
        while True:
            sys.stdout.write(CLEAR)
            sys.stdout.write(_render() + "\n")
            sys.stdout.flush()
            await asyncio.sleep(interval)
    finally:
        sys.stdout.write(SHOW_C)
        sys.stdout.flush()


# ── Entry point ────────────────────────────────────────────────────────────

async def run(
    assets:           list[str],
    budget:           float,
    min_ask:          float,
    display_interval: float,
) -> None:
    log.info("Discovering active 5m markets for: %s", ", ".join(assets))
    markets = _discover_all(assets)
    if not markets:
        log.error("No active 5m markets found")
        sys.exit(1)

    now = time.time()
    for asset, mkt in markets.items():
        _sims[asset] = AssetSim(
            asset      = asset,
            budget     = budget,
            min_ask    = min_ask,
            signal     = BinanceSignal(pair=BINANCE_PAIRS.get(asset, "")),
            up_token   = mkt["up"],
            down_token = mkt["down"],
            slug       = mkt["slug"],
            candle_end = mkt["candle_end"],
        )

    assets_found = list(markets.keys())
    all_tokens   = [t for mkt in markets.values() for t in (mkt["up"], mkt["down"])]

    log.info("Starting: %s", ", ".join(assets_found))
    await asyncio.gather(
        _binance_ws(assets_found),
        _poly_ws(all_tokens),
        _strategy_tick(tick_interval=5.0),
        _display_loop(display_interval),
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="AI sell-side simulator for Polymarket 5m Up/Down markets"
    )
    p.add_argument(
        "--assets", nargs="+", type=str.upper, default=DEFAULT_ASSETS,
        help=f"Assets to simulate (default: {' '.join(DEFAULT_ASSETS)})",
    )
    p.add_argument(
        "--budget", type=float, default=5.0,
        help="USDC per asset. Half (budget/2) used per trade side. (default: 5.0)",
    )
    p.add_argument(
        "--min-ask", type=float, default=0.55,
        help=(
            "Only sell tokens with ask ≥ this price (default: 0.55). "
            "Lower = more signals, higher risk. At 0.55 you need >45%% accuracy for +EV."
        ),
    )
    p.add_argument(
        "--display", type=float, default=1.0,
        help="Display refresh interval in seconds (default: 1.0)",
    )
    args = p.parse_args()

    try:
        asyncio.run(run(
            assets           = args.assets,
            budget           = args.budget,
            min_ask          = args.min_ask,
            display_interval = args.display,
        ))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
