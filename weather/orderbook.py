"""
Full /book depth fetch and ASCII ladder renderer for terminal display.
"""
from __future__ import annotations

import requests


def fetch_full_book(host: str, token_id: str, timeout: float = 5.0) -> dict:
    """
    Returns {"bids": [{"price": float, "size": float}, ...], "asks": [...]}.
    Bids sorted high→low, asks sorted low→high. Empty lists if call fails.
    """
    try:
        resp = requests.get(f"{host}/book", params={"token_id": token_id}, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return {"bids": [], "asks": []}

    def _norm(rows):
        out = []
        for r in rows or []:
            try:
                out.append({"price": float(r["price"]), "size": float(r["size"])})
            except (KeyError, TypeError, ValueError):
                continue
        return out

    bids = sorted(_norm(data.get("bids")), key=lambda r: r["price"], reverse=True)
    asks = sorted(_norm(data.get("asks")), key=lambda r: r["price"])
    return {"bids": bids, "asks": asks}


def summarize_liquidity(book: dict, levels: int = 5) -> dict:
    """Return {'best_bid', 'best_ask', 'mid', 'spread_bps', 'bid_volume', 'ask_volume'}."""
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread_bps = (best_ask - best_bid) * 10000 if best_bid is not None and best_ask is not None else None
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread_bps": spread_bps,
        "bid_volume": sum(b["size"] for b in bids[:levels]),
        "ask_volume": sum(a["size"] for a in asks[:levels]),
    }


def render_ladder(book: dict, levels: int = 6, bar_width: int = 28) -> str:
    """
    Render top-of-book as a vertical ladder (asks top → bids bottom), with
    horizontal bars scaled to the max size across shown levels.

        ASK  98.50¢  ##########    1200sh
        ASK  98.00¢  #####           600sh
        ---- spread 0.50¢ ----
        BID  97.50¢  ###             400sh
        BID  97.00¢  ########        950sh
    """
    asks = (book.get("asks") or [])[:levels]
    bids = (book.get("bids") or [])[:levels]
    if not asks and not bids:
        return "  (empty book)"

    all_sizes = [r["size"] for r in asks] + [r["size"] for r in bids]
    max_size = max(all_sizes) if all_sizes else 1.0

    def _bar(size: float) -> str:
        n = max(1, int(round(bar_width * size / max_size))) if max_size > 0 else 0
        return "#" * n

    lines: list[str] = []
    # Asks printed high→low so the cheapest ask sits just above the spread.
    for row in reversed(asks):
        lines.append(
            f"  ASK  {row['price']*100:6.2f}¢  {_bar(row['size']):<{bar_width}}  {row['size']:8.0f}sh"
        )

    if asks and bids:
        spread = asks[0]["price"] - bids[0]["price"]
        lines.append(f"  ---- spread {spread*100:.2f}¢ ----")
    elif asks:
        lines.append("  ---- no bids ----")
    elif bids:
        lines.append("  ---- no asks ----")

    for row in bids:
        lines.append(
            f"  BID  {row['price']*100:6.2f}¢  {_bar(row['size']):<{bar_width}}  {row['size']:8.0f}sh"
        )

    return "\n".join(lines)
