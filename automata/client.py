from __future__ import annotations

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams, OrderArgs, OrderType
from py_clob_client.constants import POLYGON
from py_clob_client.order_builder.constants import BUY, SELL


def derive_api_credentials(host: str, private_key: str, funder: str | None = None, signature_type: int = 0) -> ApiCreds:
    """Derive CLOB API credentials from the private key."""
    import logging
    log = logging.getLogger("automata")
    client = ClobClient(host, key=private_key, chain_id=POLYGON, funder=funder, signature_type=signature_type)
    creds = client.derive_api_key()
    log.info("Derived API credentials — update your .env with these:")
    log.info("  CLOB_API_KEY=%s", creds.api_key)
    log.info("  CLOB_SECRET=%s", creds.api_secret)
    log.info("  CLOB_PASS=%s", creds.api_passphrase)
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


def get_best_ask(host: str, token_id: str) -> float | None:
    """
    Fetch the live best ask price for a single token from the public order book.
    Returns None if the book is empty or the call fails.
    """
    import requests
    try:
        resp = requests.get(f"{host}/book", params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        asks = resp.json().get("asks", [])
        return min(float(a["price"]) for a in asks) if asks else None
    except Exception:
        return None


def get_best_asks_bulk(host: str, token_ids: list[str], chunk_size: int = 200) -> dict[str, float]:
    """
    Fetch live best ask prices for multiple tokens via POST to /books.
    Splits into chunks to stay within the API limit.
    Returns {token_id: best_ask_price} for tokens that have an ask.
    """
    import requests
    result: dict[str, float] = {}
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
                asks = book.get("asks", [])
                if asset_id and asks:
                    result[str(asset_id)] = min(float(a["price"]) for a in asks)
        except Exception:
            pass
    return result




def get_positions(funder: str) -> list[dict]:
    """
    Return open positions for the proxy wallet using Polymarket's data API.
    Returns list of {token_id, size}.
    """
    import requests
    try:
        r = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": funder, "sizeThreshold": "0.01"},
            timeout=10,
        )
        r.raise_for_status()
        return [
            {"token_id": str(p["asset"]), "size": float(p["size"])}
            for p in r.json()
            if float(p.get("size", 0)) > 0
        ]
    except Exception as exc:
        import logging
        logging.getLogger("automata").warning("get_positions failed: %s", exc)
        return []


def get_open_orders(client: ClobClient, token_id: str) -> list[dict]:
    """Return all open orders for a given token_id."""
    try:
        from py_clob_client.clob_types import OpenOrderParams
        raw = client.get_orders(OpenOrderParams(asset_id=token_id))
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def get_all_open_orders(client: ClobClient) -> list[dict]:
    """Return all open orders across all markets."""
    try:
        from py_clob_client.clob_types import OpenOrderParams
        raw = client.get_orders(OpenOrderParams())
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def place_market_sell(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
) -> dict:
    """Place a FOK (Fill or Kill) sell order — acts as a market sell at the given price."""
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(size_shares, 2),
        side=SELL,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.FOK)


def place_sell_order(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
) -> dict:
    """Place a GTC limit sell order on a token."""
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(size_shares, 2),
        side=SELL,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)


def place_no_order(
    client: ClobClient,
    token_id: str,
    price: float,
    size_shares: float,
) -> dict:
    """
    Place a GTC limit buy order on the No token.
    size_shares: number of shares to buy.
    """
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(size_shares, 2),
        side=BUY,
    )
    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, OrderType.GTC)
