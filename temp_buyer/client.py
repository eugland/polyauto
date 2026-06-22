from __future__ import annotations

import time

from py_clob_client_v2 import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    OpenOrderParams,
    OrderArgs,
    OrderPayload,
    OrderType,
)
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.order_builder.constants import BUY, SELL


def derive_api_credentials(host: str, private_key: str, funder: str | None = None, signature_type: int = 0) -> ApiCreds:
    """Derive CLOB API credentials from the private key."""
    import logging
    log = logging.getLogger("temp_buyer")
    client = ClobClient(host, key=private_key, chain_id=POLYGON, funder=funder, signature_type=signature_type)
    creds = client.derive_api_key()
    log.info("Derived API credentials OK (values redacted)")
    return creds


def build_client(
    host: str,
    private_key: str,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    funder: str | None = None,
    signature_type: int = 0,
) -> ClobClient:
    return ClobClient(
        host,
        key=private_key,
        chain_id=POLYGON,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ),
        funder=funder,
        signature_type=signature_type,
    )


def get_usdc_balance(client: ClobClient) -> float:
    """Return USDC balance from the proxy wallet via the CLOB. Raises on error."""
    data = client.get_balance_allowance(
        params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    # USDC has 6 decimals — API returns raw micro-USDC integer (e.g. 50890700 = $50.89)
    return int(data.get("balance", 0) or 0) / 1e6


def get_best_bid(host: str, token_id: str) -> float | None:
    """Fetch the live best bid price for a token. Returns None if no bids."""
    import requests
    try:
        resp = requests.get(f"{host}/book", params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        bids = resp.json().get("bids", [])
        return max(float(b["price"]) for b in bids) if bids else None
    except Exception:
        return None


def get_best_bid_ask(host: str, token_id: str) -> tuple[float | None, float | None]:
    """
    Fetch live best bid/ask for a token from the public order book.
    Returns (best_bid, best_ask); each side can be None if empty/unavailable.
    """
    import requests
    try:
        resp = requests.get(f"{host}/book", params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = max(float(b["price"]) for b in bids) if bids else None
        best_ask = min(float(a["price"]) for a in asks) if asks else None
        return best_bid, best_ask
    except Exception:
        return None, None


def _weighted_quantile_price(bid_pairs: list[tuple[float, float]], q: float) -> float | None:
    """Share-weighted price quantile from the top of the bid book.

    bid_pairs: list of (price, size), order-independent, all sizes > 0.
    q in (0,1]: the fraction of cumulative bid size, counted from the highest
    price down. q=0.25 returns the price at which the top 25% of bid shares sit
    at or above — i.e. "what is the top quarter of buyer interest willing to pay?"
    """
    if not bid_pairs:
        return None
    total = sum(s for _, s in bid_pairs)
    if total <= 0:
        return None
    target = q * total
    cum = 0.0
    for price, size in sorted(bid_pairs, key=lambda x: -x[0]):  # high → low
        cum += size
        if cum >= target:
            return price
    return bid_pairs[-1][0]


def get_book_depth(
    host: str, token_id: str,
    bid_conviction_threshold: float = 0.95,
    pct_floor: float = 0.10,
) -> dict:
    """Fetch bid/ask depth, a bid-conviction ratio, and shape-aware bid percentiles.

    Returns:
      bid              — best bid price
      ask              — best ask price
      ask_size         — shares available at the best-ask price level
      bid_size_above   — total shares bid at >= bid_conviction_threshold
      bid_size_below   — total shares bid at <  bid_conviction_threshold
      bid_pct_above    — bid_size_above / total_bid_size (0-100), or None if no bids
      bid_p25          — top-25% conviction price (within bids ≥ pct_floor)
      bid_p50          — top-50% conviction price (share-weighted median, within bids ≥ pct_floor)
      bid_p75          — top-75% conviction price (within bids ≥ pct_floor)

    bid_pct_above is a single binary cut at 95¢. bid_p25/p50/p75 capture
    the full shape of the *real* bid book — bids below pct_floor (default 10¢)
    are excluded because they're typically MM price-improvement floor / "free
    option" orders that drown the signal. A high bid_p25 (e.g. 0.90+) means
    the top quarter of buyer interest is priced near ask — strong conviction.
    A low bid_p25 (e.g. <0.30) means even the most-convicted bids are lowballing.
    """
    import requests
    empty = {"bid": None, "ask": None, "ask_size": None,
             "bid_size_above": None, "bid_size_below": None, "bid_pct_above": None,
             "bid_p25": None, "bid_p50": None, "bid_p75": None}
    try:
        resp = requests.get(f"{host}/book", params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])

        best_bid = max((float(b["price"]) for b in bids), default=None)
        best_ask = min((float(a["price"]) for a in asks), default=None)

        ask_size = None
        if best_ask is not None:
            ask_size = sum(float(a.get("size", 0)) for a in asks if float(a["price"]) == best_ask)

        bid_size_above = sum(float(b.get("size", 0)) for b in bids if float(b["price"]) >= bid_conviction_threshold)
        bid_size_below = sum(float(b.get("size", 0)) for b in bids if float(b["price"]) <  bid_conviction_threshold)
        total_bid = bid_size_above + bid_size_below
        bid_pct_above = (bid_size_above / total_bid * 100) if total_bid > 0 else None

        bid_pairs_real = [
            (float(b["price"]), float(b.get("size", 0)))
            for b in bids
            if float(b.get("size", 0)) > 0 and float(b["price"]) >= pct_floor
        ]
        bid_p25 = _weighted_quantile_price(bid_pairs_real, 0.25)
        bid_p50 = _weighted_quantile_price(bid_pairs_real, 0.50)
        bid_p75 = _weighted_quantile_price(bid_pairs_real, 0.75)

        return {
            "bid": best_bid,
            "ask": best_ask,
            "ask_size": ask_size,
            "bid_size_above": bid_size_above,
            "bid_size_below": bid_size_below,
            "bid_pct_above": bid_pct_above,
            "bid_p25": bid_p25,
            "bid_p50": bid_p50,
            "bid_p75": bid_p75,
        }
    except Exception:
        return empty


def get_best_books_bulk(host: str, token_ids: list[str], chunk_size: int = 200) -> dict[str, dict]:
    """
    Fetch live best bid and ask prices for multiple tokens via POST to /books.
    Returns {token_id: {"bid": float|None, "ask": float|None}}.
    """
    import requests
    result: dict[str, dict] = {}
    for i in range(0, len(token_ids), chunk_size):
        chunk = token_ids[i: i + chunk_size]
        try:
            resp = requests.post(
                f"{host}/books",
                json=[{"token_id": tid} for tid in chunk],
                timeout=10,
            )
            resp.raise_for_status()
            for book in resp.json():
                asset_id = book.get("asset_id") or book.get("token_id")
                if not asset_id:
                    continue
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                result[str(asset_id)] = {
                    "bid": max(float(b["price"]) for b in bids) if bids else None,
                    "ask": min(float(a["price"]) for a in asks) if asks else None,
                }
        except Exception:
            pass
    return result


def get_positions(funder: str) -> list[dict]:
    """
    Return open positions for the proxy wallet using Polymarket's data API.
    Returns list of dicts with {token_id, size, conditionId, outcome,
    negativeRisk, redeemable} when the upstream provides them.
    """
    import requests
    try:
        r = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": funder, "sizeThreshold": "0.01"},
            timeout=10,
        )
        r.raise_for_status()
        out = []
        for p in r.json():
            if float(p.get("size", 0)) <= 0:
                continue
            out.append({
                "token_id":     str(p["asset"]),
                "size":         float(p["size"]),
                "conditionId":  str(p.get("conditionId") or ""),
                "outcome":      str(p.get("outcome") or ""),
                "negativeRisk": bool(p.get("negativeRisk", False)),
                "redeemable":   bool(p.get("redeemable", False)),
            })
        return out
    except Exception as exc:
        import logging
        logging.getLogger("temp_buyer").warning("get_positions failed: %s", exc)
        return []


def get_positions_with_prices(funder: str, host: str = "https://clob.polymarket.com") -> dict:
    """
    Get account positions with current market prices and calculated P&L.
    Returns {
        'positions': [{'token_id', 'size', 'price', 'pnl', 'market_slug', 'outcome_label'}, ...],
        'total_pnl': float,
        'error': str (if any)
    }
    """
    import requests
    import logging
    log = logging.getLogger("temp_buyer")

    try:
        # Get positions
        pos_resp = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": funder, "sizeThreshold": "0.01"},
            timeout=10,
        )
        pos_resp.raise_for_status()
        positions = pos_resp.json()

        if not positions:
            return {"positions": [], "total_pnl": 0.0, "error": None}

        # Build result
        results = []
        total_pnl = 0.0

        for pos in positions:
            if float(pos.get("size", 0)) <= 0:
                continue

            token_id = str(pos.get("asset", ""))
            size = float(pos.get("size", 0))
            entry_price = float(pos.get("cost_basis", 0)) / max(1, size) if size > 0 else 0

            # Get current price
            bid, ask = get_best_bid_ask(host, token_id)
            mid_price = ((bid or 0) + (ask or 0)) / 2 if bid and ask else (bid or ask or 0)

            # Calculate P&L
            pnl = size * (mid_price - entry_price) if entry_price > 0 else 0
            total_pnl += pnl

            results.append({
                "token_id": token_id,
                "size": round(size, 2),
                "entry_price": round(entry_price, 4),
                "current_price": round(mid_price, 4),
                "pnl": round(pnl, 4),
                "market_slug": pos.get("market_slug", ""),
            })

        return {
            "positions": results,
            "total_pnl": round(total_pnl, 4),
            "error": None,
        }

    except Exception as exc:
        log.warning("get_positions_with_prices failed: %s", exc)
        return {
            "positions": [],
            "total_pnl": 0.0,
            "error": f"Failed to fetch positions: {str(exc)}",
        }


def get_open_orders(client: ClobClient, token_id: str) -> list[dict]:
    """Return all open orders for a given token_id."""
    try:
        raw = client.get_open_orders(OpenOrderParams(asset_id=token_id))
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def get_all_open_orders(client: ClobClient) -> list[dict]:
    """Return all open orders across all markets."""
    try:
        raw = client.get_open_orders(OpenOrderParams())
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def cancel_order(client: ClobClient, order_id: str) -> dict:
    """Cancel a single open order by id."""
    return client.cancel_order(OrderPayload(orderID=order_id))


def place_market_sell(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
    fee_rate_bps: int | None = None,
    post_only: bool = False,
    ttl_seconds: int | None = 60,
) -> dict:
    """
    Place an aggressive sell order up to the given price without using FOK.
    Always uses GTC.
    """
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size_shares,
        side=SELL,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)


def place_market_buy(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
    fee_rate_bps: int | None = None,
    ttl_seconds: int | None = 60,
) -> dict:
    """
    Place an aggressive buy order up to the given price without using FOK.
    Always uses GTC.
    """
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size_shares,
        side=BUY,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)


def place_sell_order(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
    fee_rate_bps: int | None = None,
) -> dict:
    """Place a GTC limit sell order on a token."""
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size_shares,
        side=SELL,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)


def place_no_order(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
    fee_rate_bps: int | None = None,
    post_only: bool = False,
    ttl_seconds: int | None = None,
) -> dict:
    """
    Place a limit buy order on the No token.
    size_shares: number of shares to buy.
    ttl_seconds: retained for call compatibility; orders are placed as GTC.
    """
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size_shares,
        side=BUY,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)
