"""
CLOB client initialisation for the BTC bot.
Reuses automata/client.py — no duplication.
"""

import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

load_dotenv()


def build_client() -> ClobClient:
    host       = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    key        = os.getenv("POLYMARKET_PRIVATE_KEY")
    funder     = os.getenv("POLYMARKET_FUNDER")
    sig_type   = int(os.getenv("POLYMARKET_SIG_TYPE", "0"))

    # Level 1 client — derives API creds from private key
    l1 = ClobClient(host, key=key, chain_id=POLYGON, funder=funder, signature_type=sig_type)
    creds = l1.derive_api_key()

    # Level 2 client — full access
    from py_clob_client.clob_types import ApiCreds
    return ClobClient(
        host,
        key=key,
        chain_id=POLYGON,
        creds=ApiCreds(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
        ),
        funder=funder,
        signature_type=sig_type,
    )
