"""
Polymarket CLOB order book for BTC 1H Up/Down markets.

Endpoint:
    GET https://clob.polymarket.com/book?token_id={token_id}

Key insight: UP + DOWN = $1 (binary market).
So the DOWN price is always (1 - UP price).
Both sides are derived from a single UP token order book.

Order book fields:
    bids  — buyers of UP (sorted descending by price, best bid first)
    asks  — sellers of UP (sorted ascending by price, best ask first)

Complementary view:
    buying  UP   at 0.72  == selling DOWN at 0.28
    selling UP   at 0.73  == buying  DOWN at 0.27
"""

import json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

CLOB_BASE = "https://clob.polymarket.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
}

DEPTH = 10  # levels to display per side


def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def fetch_book(token_id: str) -> dict:
    return _get(f"{CLOB_BASE}/book?token_id={token_id}")


def fetch_midpoint(token_id: str) -> float:
    return float(_get(f"{CLOB_BASE}/midpoint?token_id={token_id}")["mid"])


def fetch_spread(token_id: str) -> float:
    return float(_get(f"{CLOB_BASE}/spread?token_id={token_id}")["spread"])


def parse_book(raw: dict) -> dict:
    """
    Normalise raw CLOB response.
    Bids come back ascending — reverse so best bid is index 0.
    Asks come back descending — reverse so best ask is index 0.
    """
    bids = sorted(
        [{"price": float(b["price"]), "size": float(b["size"])} for b in raw["bids"]],
        key=lambda x: x["price"], reverse=True
    )
    asks = sorted(
        [{"price": float(a["price"]), "size": float(a["size"])} for a in raw["asks"]],
        key=lambda x: x["price"]
    )

    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    spread   = round(best_ask - best_bid, 4) if (best_bid and best_ask) else None
    mid      = round((best_bid + best_ask) / 2, 4) if (best_bid and best_ask) else None

    total_bid_liquidity = sum(b["price"] * b["size"] for b in bids)
    total_ask_liquidity = sum(a["price"] * a["size"] for a in asks)

    return {
        "market":       raw["market"],
        "asset_id":     raw["asset_id"],
        "timestamp_ms": int(raw["timestamp"]),
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "last_trade":   float(raw.get("last_trade_price", 0)),
        "best_bid":     best_bid,
        "best_ask":     best_ask,
        "spread":       spread,
        "mid":          mid,
        "bids":         bids,
        "asks":         asks,
        "total_bid_liq": round(total_bid_liquidity, 2),
        "total_ask_liq": round(total_ask_liquidity, 2),
        "bid_levels":   len(bids),
        "ask_levels":   len(asks),
    }


def print_book(up_token_id: str, down_token_id: str, label: str = ""):
    try:
        raw_up   = fetch_book(up_token_id)
        raw_down = fetch_book(down_token_id)
        up   = parse_book(raw_up)
        down = parse_book(raw_down)

        print(f"\n  ORDER BOOK{' -- ' + label if label else ''}")
        print(f"  Fetched: {up['fetched_at']}")
        print(f"  Last trade (UP token): ${up['last_trade']:.3f}")

        # ── Summary row ──────────────────────────────────────────────
        print(f"\n  {'OUTCOME':<8}  {'BID':>8}  {'MID':>8}  {'ASK':>8}  {'SPREAD':>8}  {'BID LIQ':>12}  {'ASK LIQ':>12}")
        print(f"  {'-'*80}")
        for book, name in [(up, "UP"), (down, "DOWN")]:
            print(
                f"  {name:<8}  "
                f"{book['best_bid']:>8.3f}  "
                f"{book['mid']:>8.3f}  "
                f"{book['best_ask']:>8.3f}  "
                f"{book['spread']:>8.3f}  "
                f"${book['total_bid_liq']:>11,.2f}  "
                f"${book['total_ask_liq']:>11,.2f}"
            )

        # ── Depth of market ──────────────────────────────────────────
        for book, name in [(up, "UP"), (down, "DOWN")]:
            print(f"\n  DEPTH OF MARKET — {name}  ({book['bid_levels']} bid levels, {book['ask_levels']} ask levels)")
            print(f"  {'':30}  ASKS (sellers)")
            print(f"  {'SIZE':>12}  {'PRICE':>8}  {'PRICE':>8}  {'SIZE':<12}  BIDS (buyers)")
            print(f"  {'-'*60}")

            asks_top = list(reversed(book["asks"][:DEPTH]))   # show highest ask at top
            bids_top = book["bids"][:DEPTH]

            rows = max(len(asks_top), len(bids_top))
            for i in range(rows):
                ask = asks_top[i] if i < len(asks_top) else None
                bid = bids_top[i] if i < len(bids_top) else None

                ask_str = f"{ask['size']:>12,.2f}  {ask['price']:>8.3f}" if ask else f"{'':>12}  {'':>8}"
                bid_str = f"{bid['price']:>8.3f}  {bid['size']:<12,.2f}" if bid else ""

                # highlight the spread gap
                marker = " <-- spread" if ask and bid and i == len(asks_top) - 1 else ""
                print(f"  {ask_str}  {bid_str}{marker}")

        print()

    except (URLError, KeyError, ValueError) as e:
        print(f"  [error fetching order book] {e}")


def get_books(up_token_id: str, down_token_id: str) -> tuple[dict, dict]:
    """Return parsed (up_book, down_book) for programmatic use."""
    return parse_book(fetch_book(up_token_id)), parse_book(fetch_book(down_token_id))


if __name__ == "__main__":
    # Demo using the 3AM ET market token IDs
    UP_TOKEN   = "61948477671761874473117723361904673199748900491410137914781824674422768363941"
    DOWN_TOKEN = "83481500523287236130182892260670659075844213153103257480739855040042098111900"
    print_book(UP_TOKEN, DOWN_TOKEN, label="April 5 3AM ET (demo)")
