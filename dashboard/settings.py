"""Dashboard config accessors — thin wrappers over temp_buyer.config + .env."""
from __future__ import annotations

import os

from dotenv import load_dotenv

from temp_buyer import config

load_dotenv()

DATA_API = "https://data-api.polymarket.com"


def port() -> int:
    return config.get_int("DASHBOARD_PORT", "dashboard", "port", 8000)


def poll_interval_seconds() -> int:
    return config.get_int(
        "DASHBOARD_POLL_INTERVAL_SECONDS", "dashboard", "poll_interval_seconds", 300
    )


def my_address() -> str:
    """Wallet for the Positions tab: config override, else POLYMARKET_FUNDER."""
    addr = config.get_str("DASHBOARD_MY_ADDRESS", "dashboard", "my_address", "")
    return (addr or os.getenv("POLYMARKET_FUNDER") or "").strip()


def pros() -> list[dict[str, str]]:
    """Watched wallets as ``[{label, address}]`` from ``label:0xaddr`` entries."""
    out: list[dict[str, str]] = []
    for raw in config.get_list_str("DASHBOARD_PROS", "dashboard", "pros", []):
        label, _, addr = raw.partition(":")
        addr = addr.strip() or label.strip()
        if not addr:
            continue
        out.append({"label": (label.strip() if addr != label.strip() else addr[:8]), "address": addr})
    return out


def stock_watchlist() -> list[str]:
    return config.get_list_str(
        "DASHBOARD_STOCK_WATCHLIST", "dashboard", "stock_watchlist", ["SPY", "QQQ", "AAPL"]
    )


def crypto_watchlist() -> list[str]:
    return config.get_list_str(
        "DASHBOARD_CRYPTO_WATCHLIST", "dashboard", "crypto_watchlist", ["BTC-USD", "ETH-USD"]
    )
