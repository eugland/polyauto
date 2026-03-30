from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Market:
    market_id: str
    question: str
    event_slug: str
    no_token_id: str
    no_price: float          # 0-1


@dataclass
class ParsedMarket:
    market: Market
    location_key: str
    threshold_lo: float
    threshold_hi: float | None   # None = single-value threshold
    unit: str                    # "F" or "C"
    direction: str               # "higher", "below", "range"


@dataclass
class BetOrder:
    market: Market
    size_usdc: float
    reason: str
