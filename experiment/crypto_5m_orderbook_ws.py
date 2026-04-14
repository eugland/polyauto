#!/usr/bin/env python3
"""
Real-time multi-asset order book WebSocket streamer for Polymarket crypto 5m markets.
Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market and streams
live bid/ask updates for any combination of supported assets.

Each asset row shows:
  - Best bid / ask / spread
  - Price direction arrow (over last ~5 s)
  - Top-5 bid/ask depth (in shares)
  - Top-of-book change rate (events/s, 10 s window)
  - WS inter-message latency (median, last 50 msgs) in the footer

Usage:
    python -m experiment.crypto_5m_orderbook_ws
    python -m experiment.crypto_5m_orderbook_ws --assets BTC ETH
    python -m experiment.crypto_5m_orderbook_ws --assets BTC ETH BNB XRP DOGE HYPER SOL
    python -m experiment.crypto_5m_orderbook_ws --display 0.5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import requests
import websockets

# ── Configuration ──────────────────────────────────────────────────────────

GAMMA_API        = "https://gamma-api.polymarket.com/events"
WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_RECONNECT_SEC = 3
WS_PING_INTERVAL = 10
WS_WARMUP_SEC    = 2

DEPTH_LEVELS     = 5    # how many levels to sum for depth
RATE_WINDOW_SEC  = 10   # window for event-rate calculation
DIRECTION_LAG    = 5    # seconds back for price-direction comparison

SUPPORTED_ASSETS = ["BTC", "ETH", "BNB", "XRP", "DOGE", "HYPER", "SOL", "LINK", "AVAX", "LTC"]
DEFAULT_ASSETS   = ["BTC", "ETH", "BNB", "XRP", "DOGE", "HYPER", "SOL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto5m.ws")

# ── In-memory caches ───────────────────────────────────────────────────────

_book_cache: dict[str, dict] = {}          # token_id → {bids:[…], asks:[…]}

# price history: token_id → deque of (wall_ts, best_bid, best_ask)
_price_hist: dict[str, deque] = {}

# event timestamps for rate: token_id → deque of wall_ts (pruned to RATE_WINDOW_SEC)
_event_ts: dict[str, deque] = {}

# WS inter-message latency (seconds between consecutive messages, last 50)
_ws_latency: deque = deque(maxlen=50)
_ws_last_rx: float = 0.0


# ── Order book helpers ─────────────────────────────────────────────────────

def _apply_price_change(book: dict, changes: list[dict]) -> None:
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
        levels[:] = [
            lv for lv in levels
            if abs(float(lv.get("price", 0) or 0) - p) > 1e-9
        ]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


def _handle_event(ev: dict) -> None:
    """Process a single WS event: update book cache, price history, and event rate."""
    global _ws_last_rx

    now = time.time()
    et  = ev.get("event_type")
    tid = str(ev.get("asset_id") or "")
    if not tid:
        return

    if et == "book":
        _book_cache[tid] = {
            "bids": [dict(b) for b in (ev.get("bids") or [])],
            "asks": [dict(a) for a in (ev.get("asks") or [])],
        }
    elif et == "price_change":
        book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
        _apply_price_change(book, ev.get("changes") or [])

        # Record event timestamp for rate
        dq = _event_ts.setdefault(tid, deque())
        dq.append(now)
        cutoff = now - RATE_WINDOW_SEC
        while dq and dq[0] < cutoff:
            dq.popleft()

    # Snapshot best bid/ask into price history after book update
    if et in ("book", "price_change"):
        book = _book_cache.get(tid, {})
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = max((float(b["price"]) for b in bids), default=None)
        best_ask = min((float(a["price"]) for a in asks), default=None)
        dq = _price_hist.setdefault(tid, deque(maxlen=300))
        dq.append((now, best_bid, best_ask))


def _record_ws_msg() -> None:
    """Call once per raw WS message to track inter-message latency."""
    global _ws_last_rx
    now = time.time()
    if _ws_last_rx:
        _ws_latency.append(now - _ws_last_rx)
    _ws_last_rx = now


def _best_bid_ask(token_id: str) -> tuple[float | None, float | None]:
    """Extract best bid and ask from the cached book."""
    book = _book_cache.get(token_id, {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = max((float(b["price"]) for b in bids), default=None)
    best_ask = min((float(a["price"]) for a in asks), default=None)
    return best_bid, best_ask


def _book_depth(token_id: str) -> tuple[float, float]:
    """Return (bid_depth, ask_depth) summed over top DEPTH_LEVELS levels (in shares)."""
    book = _book_cache.get(token_id, {})
    bids = sorted(book.get("bids", []), key=lambda x: -float(x.get("price", 0)))
    asks = sorted(book.get("asks", []), key=lambda x:  float(x.get("price", 0)))
    bid_depth = sum(float(b.get("size", 0)) for b in bids[:DEPTH_LEVELS])
    ask_depth = sum(float(a.get("size", 0)) for a in asks[:DEPTH_LEVELS])
    return bid_depth, ask_depth


def _price_direction(token_id: str) -> str:
    """↑ / ↓ / → based on mid-price change over the last DIRECTION_LAG seconds."""
    hist = _price_hist.get(token_id)
    if not hist or len(hist) < 2:
        return "→"
    now   = time.time()
    cur   = hist[-1]
    pivot = next((h for h in reversed(hist) if now - h[0] >= DIRECTION_LAG), None)
    if not pivot:
        return "→"
    cur_mid   = ((cur[1]   or 0) + (cur[2]   or 0)) / 2
    pivot_mid = ((pivot[1] or 0) + (pivot[2] or 0)) / 2
    delta = cur_mid - pivot_mid
    if   delta >  0.001: return "↑"
    elif delta < -0.001: return "↓"
    return "→"


def _tob_rate(token_id: str) -> float:
    """Top-of-book change events per second (last RATE_WINDOW_SEC seconds)."""
    dq = _event_ts.get(token_id)
    if not dq:
        return 0.0
    return len(dq) / RATE_WINDOW_SEC


def _ws_latency_ms() -> str:
    """Median WS inter-message latency in ms, or 'n/a'."""
    if not _ws_latency:
        return "n/a"
    vals = sorted(_ws_latency)
    med  = vals[len(vals) // 2]
    return f"{med * 1000:.1f} ms"


# ── Market discovery ───────────────────────────────────────────────────────

def _load_field(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v


def _get_current_5m_tokens(asset: str) -> dict[str, str] | None:
    """
    Fetch the current active 5m Up/Down token IDs for the given asset.
    Returns {"up": token_id, "down": token_id, "slug": slug} or None.
    """
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
            event = data[0]
            mkt   = (event.get("markets") or [None])[0]
            if not mkt or mkt.get("closed"):
                continue
            outcomes  = _load_field(mkt.get("outcomes")) or []
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
            if up_token and down_token:
                return {"up": up_token, "down": down_token, "slug": slug}
        except Exception as exc:
            log.debug("Market fetch failed for %s: %s", slug, exc)

    return None


def _discover_all(assets: list[str]) -> dict[str, dict[str, str]]:
    """
    Discover active 5m markets for all requested assets in parallel.
    Returns {asset: {up, down, slug}} for those that have active markets.
    """
    found: dict[str, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=len(assets)) as pool:
        futures = {pool.submit(_get_current_5m_tokens, a): a for a in assets}
        for future, asset in futures.items():
            result = future.result()
            if result:
                found[asset] = result
                log.info("Found %-6s  %s", asset, result["slug"])
            else:
                log.warning("No active 5m market for %s — skipping", asset)
    return found


# ── Display ────────────────────────────────────────────────────────────────

CLEAR  = "\033[2J\033[H"
HIDE_C = "\033[?25l"
SHOW_C = "\033[?25h"
BOLD   = "\033[1m"
RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
DIM    = "\033[2m"
SEP    = "  " + "─" * 100


def _dir_color(d: str) -> str:
    if d == "↑": return GREEN
    if d == "↓": return RED
    return DIM


def _format_row(asset: str, side: str,
                bid: float | None, ask: float | None,
                direction: str,
                bid_depth: float, ask_depth: float,
                rate: float) -> str:
    bid_str    = f"${bid:.4f}"  if bid is not None else "  n/a  "
    ask_str    = f"${ask:.4f}"  if ask is not None else "  n/a  "
    spread     = (ask - bid)    if (bid is not None and ask is not None) else None
    spread_str = f"${spread:.4f} ({spread*100:.2f}%)" if spread is not None else "  n/a  "
    colour     = GREEN if side == "Up" else RED
    dc         = _dir_color(direction)

    depth_str  = f"B:{bid_depth:6.1f}  A:{ask_depth:6.1f} shrs"
    rate_str   = f"{rate:4.1f}/s"

    return (
        f"  {colour}{BOLD}{asset:5s} {side:4s}{RESET}"
        f" {dc}{direction}{RESET}"
        f"  Bid:{CYAN}{bid_str}{RESET}"
        f"  Ask:{CYAN}{ask_str}{RESET}"
        f"  Spr:{DIM}{spread_str}{RESET}"
        f"  Depth:{DIM}{depth_str}{RESET}"
        f"  ToB:{YELLOW}{rate_str}{RESET}"
    )


async def _display_loop(
    markets: dict[str, dict[str, str]],
    interval: float,
) -> None:
    """Redraw the full multi-asset panel in-place."""
    sys.stdout.write(HIDE_C)
    sys.stdout.flush()
    try:
        while True:
            ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            lat  = _ws_latency_ms()
            lines: list[str] = [
                "",
                f"  {BOLD}[{ts}]  Crypto 5m Markets  (WebSocket live){RESET}"
                f"   {DIM}WS latency: {lat}{RESET}",
                SEP,
                f"  {DIM}{'ASSET':<8} DIR  {'BID':>8}  {'ASK':>8}  {'SPREAD':>16}"
                f"  {'DEPTH (top-5)':>22}  {'ToB rate':>8}{RESET}",
                SEP,
            ]

            for asset, tokens in markets.items():
                up_bid,   up_ask   = _best_bid_ask(tokens["up"])
                down_bid, down_ask = _best_bid_ask(tokens["down"])

                up_bd,   up_ad   = _book_depth(tokens["up"])
                down_bd, down_ad = _book_depth(tokens["down"])

                up_dir   = _price_direction(tokens["up"])
                down_dir = _price_direction(tokens["down"])

                up_rate   = _tob_rate(tokens["up"])
                down_rate = _tob_rate(tokens["down"])

                lines.append(_format_row(asset, "Up",   up_bid,   up_ask,   up_dir,   up_bd,   up_ad,   up_rate))
                lines.append(_format_row(asset, "Down", down_bid, down_ask, down_dir, down_bd, down_ad, down_rate))
                lines.append(SEP)

            lines.append(
                f"  {DIM}Depth = top {DEPTH_LEVELS} levels summed · "
                f"ToB rate = top-of-book change events / {RATE_WINDOW_SEC}s · "
                f"WS latency = median inter-message gap{RESET}"
            )
            lines.append(f"  {DIM}Press Ctrl+C to stop{RESET}")

            sys.stdout.write(CLEAR)
            sys.stdout.write("\n".join(lines) + "\n")
            sys.stdout.flush()

            await asyncio.sleep(interval)
    finally:
        sys.stdout.write(SHOW_C)
        sys.stdout.flush()


# ── WebSocket loop ─────────────────────────────────────────────────────────

async def _ws_loop(all_tokens: list[str]) -> None:
    """
    Single WebSocket connection subscribing to all token IDs at once.
    Reconnects automatically on failure.
    """
    subscribe_msg = json.dumps({
        "auth":       {},
        "type":       "market",
        "assets_ids": all_tokens,
    })

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_INTERVAL,
                max_size=2 ** 22,
            ) as ws:
                await ws.send(subscribe_msg)
                log.info("WS connected — subscribed to %d tokens", len(all_tokens))

                first_msg = True
                async for raw in ws:
                    _record_ws_msg()

                    if first_msg:
                        first_msg = False
                        sample = raw if len(raw) <= 200 else raw[:200] + "…"
                        log.info("WS first message: %s", sample)

                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    events = payload if isinstance(payload, list) else [payload]
                    for ev in events:
                        if isinstance(ev, dict):
                            _handle_event(ev)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WS disconnected (%s: %s) — reconnecting in %ds",
                        type(exc).__name__, exc, WS_RECONNECT_SEC)
            await asyncio.sleep(WS_RECONNECT_SEC)


# ── Entry point ────────────────────────────────────────────────────────────

async def stream(assets: list[str], display_interval: float) -> None:
    log.info("Discovering active 5m markets for: %s", ", ".join(assets))
    markets = _discover_all(assets)

    if not markets:
        log.error("No active 5m markets found for any requested asset")
        sys.exit(1)

    log.info("Streaming %d asset(s): %s", len(markets), ", ".join(markets))

    all_tokens = [tid for t in markets.values() for tid in (t["up"], t["down"])]

    log.info("Warming up WebSocket feed for %ds...", WS_WARMUP_SEC)
    await asyncio.gather(
        _ws_loop(all_tokens),
        _display_loop(markets, display_interval),
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Multi-asset WebSocket order book streamer for Polymarket 5m markets"
    )
    p.add_argument(
        "--assets",
        nargs="+",
        type=str.upper,
        default=DEFAULT_ASSETS,
        metavar="ASSET",
        help=(
            f"Assets to stream (default: {' '.join(DEFAULT_ASSETS)}). "
            f"Supported: {', '.join(SUPPORTED_ASSETS)}"
        ),
    )
    p.add_argument(
        "--display",
        type=float,
        default=1.0,
        help="Display refresh interval in seconds (default: 1.0)",
    )
    args = p.parse_args()

    try:
        asyncio.run(stream(assets=args.assets, display_interval=args.display))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
