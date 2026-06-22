"""Authenticated bits for my own wallet — cash (USDC) balance via the CLOB.

Best-effort: if POLYMARKET_PRIVATE_KEY/HOST aren't set, cash is None and the
dashboard falls back to portfolio value only.
"""
from __future__ import annotations

import logging
import os
import time

from temp_buyer import config

log = logging.getLogger("dashboard.wallet")

_client = None  # built once, lazily
_cash_cache: tuple[float, float | None] = (0.0, None)  # (fetched_at, value)
_CASH_TTL = 30.0


def _build_client():
    global _client
    if _client is not None:
        return _client
    from temp_buyer.client import build_client, derive_api_credentials

    host = os.getenv("POLYMARKET_HOST")
    pk = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not (host and pk):
        return None

    if not (os.getenv("CLOB_API_KEY") and os.getenv("CLOB_SECRET") and os.getenv("CLOB_PASS")):
        creds = derive_api_credentials(
            host=host,
            private_key=pk,
            funder=os.getenv("POLYMARKET_FUNDER"),
            signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
        )
        os.environ["CLOB_API_KEY"] = creds.api_key
        os.environ["CLOB_SECRET"] = creds.api_secret
        os.environ["CLOB_PASS"] = creds.api_passphrase

    _client = build_client(
        host=host,
        private_key=pk,
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
    )
    return _client


def cash() -> float | None:
    """Free USDC in the proxy wallet, or None if credentials are unavailable.

    Cached ~30s — the underlying balance call is an on-chain/CLOB round-trip.
    """
    global _cash_cache
    now = time.time()
    if now - _cash_cache[0] < _CASH_TTL:
        return _cash_cache[1]
    try:
        client = _build_client()
        if client is None:
            _cash_cache = (now, None)
            return None
        from temp_buyer.client import get_usdc_balance

        val = round(get_usdc_balance(client), 2)
        _cash_cache = (now, val)
        return val
    except Exception as exc:  # noqa: BLE001
        log.warning("cash() failed: %s", exc)
        return _cash_cache[1]
