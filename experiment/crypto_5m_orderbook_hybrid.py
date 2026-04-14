#!/usr/bin/env python3
"""
Hybrid order book streamer for Polymarket crypto 5m markets.
Tries WebSocket first, falls back to HTTP polling if unavailable.

Usage:
    python -m experiment.crypto_5m_orderbook_hybrid --asset BTC
    python -m experiment.crypto_5m_orderbook_hybrid --asset ETH
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto5m.hybrid")

# ── Configuration ──────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com/events"
CLOB_HOST = "https://clob.polymarket.com"

# ── Utilities ──────────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 12) -> Any:
    """Fetch JSON from URL."""
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _fetch_books(token_ids: list[str]) -> dict[str, dict]:
    """Fetch order books for a list of token IDs via HTTP."""
    if not token_ids:
        return {}
    try:
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
    except Exception as exc:
        log.debug("Book fetch error: %s", exc)
        return {}


def _best_bid_ask(book: dict) -> tuple[float | None, float | None]:
    """Extract best bid and ask from order book."""
    best_bid = None
    best_ask = None

    for b in book.get("bids", []):
        try:
            p = float(b["price"])
            if best_bid is None or p > best_bid:
                best_bid = p
        except (KeyError, TypeError, ValueError):
            continue

    for a in book.get("asks", []):
        try:
            p = float(a["price"])
            if best_ask is None or p < best_ask:
                best_ask = p
        except (KeyError, TypeError, ValueError):
            continue

    return best_bid, best_ask


def _load_field(v: Any) -> Any:
    """Parse JSON field."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v


def _get_current_5m_markets(asset: str) -> tuple[dict[str, str], dict[str, str]] | None:
    """Get the current active 5m markets for the given asset (BTC or ETH)."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300

    for delta in (0, -1, 1):
        slug = f"{asset.lower()}-updown-5m-{bucket + delta * 300}"
        try:
            data = _get_json(f"{GAMMA_API}?slug={slug}")
            if isinstance(data, list) and data:
                event = data[0]
                markets = event.get("markets") or []
                if markets:
                    mkt = markets[0]
                    if not mkt.get("closed"):
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
                        if up_token and down_token:
                            return {
                                "slug": slug,
                                "asset": asset,
                                "up_token": up_token,
                                "down_token": down_token,
                            }, {"up": up_token, "down": down_token}
        except Exception as exc:
            log.debug("Market fetch failed for %s: %s", slug, exc)

    return None


def format_book_display(asset: str, side: str, bid: float | None, ask: float | None, spread: float | None) -> str:
    """Format order book display line."""
    bid_str = f"${bid:.4f}" if bid is not None else "   n/a   "
    ask_str = f"${ask:.4f}" if ask is not None else "   n/a   "
    spread_str = f"${spread:.4f} ({spread*100:.2f}%)" if spread is not None else "   n/a   "

    return f"  {asset:3s} {side:4s}   Bid: {bid_str}   Ask: {ask_str}   Spread: {spread_str}"


async def stream_hybrid(asset: str, poll_interval: float = 0.5) -> None:
    """
    Stream order books using HTTP polling with async display.
    (WebSocket endpoint not available on Polymarket CLOB)
    """
    log.info("Starting %s 5m order book stream (polling every %.1fs)...", asset.upper(), poll_interval)

    # Get market
    result = _get_current_5m_markets(asset)
    if not result:
        log.error("No active %s 5m market found", asset.upper())
        sys.exit(1)

    markets, token_ids = result
    log.info("Found market: %s", markets["slug"])

    last_refresh = 0

    try:
        while True:
            now = time.time()

            # Refresh markets every 60 seconds
            if now - last_refresh >= 60:
                result = _get_current_5m_markets(asset)
                if result:
                    markets, token_ids = result
                    log.info("Found market: %s", markets["slug"])
                last_refresh = now

            # Fetch books
            books = _fetch_books([token_ids["up"], token_ids["down"]])

            up_book = books.get(token_ids["up"], {})
            down_book = books.get(token_ids["down"], {})

            up_bid, up_ask = _best_bid_ask(up_book)
            down_bid, down_ask = _best_bid_ask(down_book)

            up_spread = (up_ask - up_bid) if (up_bid and up_ask) else None
            down_spread = (down_ask - down_bid) if (down_bid and down_ask) else None

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"\n[{ts}]  {asset.upper()} 5m Markets")
            print("─" * 90)
            print(format_book_display(asset, "Up", up_bid, up_ask, up_spread))
            print(format_book_display(asset, "Down", down_bid, down_ask, down_spread))
            print()

            await asyncio.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("Stopped.")
        sys.exit(0)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Hybrid order book streamer for Polymarket crypto 5m markets (HTTP polling)"
    )
    p.add_argument(
        "--asset",
        type=str.upper,
        default="BTC",
        choices=["BTC", "ETH"],
        help="Asset to stream (BTC or ETH, default: BTC)",
    )
    p.add_argument(
        "--poll",
        type=float,
        default=0.5,
        help="Poll interval in seconds (default: 0.5)",
    )
    args = p.parse_args()

    asyncio.run(stream_hybrid(asset=args.asset, poll_interval=args.poll))


if __name__ == "__main__":
    main()
