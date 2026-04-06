"""
Polymarket XRP 1H Up/Down - Market URL builder and data fetcher.

URL pattern:
    https://polymarket.com/event/xrp-up-or-down-{month}-{day}-{year}-{hour}{ampm}-et

API:
    https://gamma-api.polymarket.com/events?slug={slug}

Resolution: XRP/USDT 1H candle
    - "Up"   if close >= open
    - "Down" if close <  open
"""

import json
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError

GAMMA_API = "https://gamma-api.polymarket.com/events?slug={slug}"
POLYMARKET_BASE = "https://polymarket.com/event/{slug}"
ET_OFFSET = timedelta(hours=-4)  # EDT (UTC-4); change to -5 for EST

MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]


def current_et() -> datetime:
    return datetime.now(timezone(ET_OFFSET))


def build_slug(dt: datetime) -> str:
    """Build the Polymarket slug for the 1H XRP market that starts at dt (ET)."""
    month = MONTH_NAMES[dt.month]
    day = dt.day
    year = dt.year
    hour_24 = dt.hour
    ampm = "am" if hour_24 < 12 else "pm"
    hour_12 = hour_24 % 12
    if hour_12 == 0:
        hour_12 = 12
    return f"xrp-up-or-down-{month}-{day}-{year}-{hour_12}{ampm}-et"


def build_url(slug: str) -> str:
    return POLYMARKET_BASE.format(slug=slug)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
}


def fetch_market(slug: str) -> dict | None:
    url = GAMMA_API.format(slug=slug)
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data[0] if data else None
    except (URLError, IndexError, json.JSONDecodeError) as e:
        print(f"  [error] {e}")
        return None


def parse_market(event: dict) -> dict:
    """Extract all useful fields from a Gamma API event response."""
    m = event["markets"][0]
    series = event.get("series", [{}])[0]

    outcomes = json.loads(m["outcomes"])
    prices = [float(p) for p in json.loads(m["outcomePrices"])]
    clob_ids = json.loads(m["clobTokenIds"])

    end_utc = datetime.fromisoformat(event["endDate"].replace("Z", "+00:00"))
    start_utc = datetime.fromisoformat(m["eventStartTime"].replace("Z", "+00:00")) if m.get("eventStartTime") else None
    now_utc = datetime.now(timezone.utc)
    time_remaining = (end_utc - now_utc) if not event["closed"] else timedelta(0)

    return {
        # Identity
        "event_id": event["id"],
        "market_id": m["id"],
        "slug": event["slug"],
        "title": event["title"],
        "url": build_url(event["slug"]),

        # Status
        "active": event["active"],
        "closed": event["closed"],
        "archived": event["archived"],

        # Timing
        "candle_start_utc": start_utc.isoformat() if start_utc else None,
        "candle_start_et": (start_utc.astimezone(timezone(ET_OFFSET)).isoformat() if start_utc else None),
        "resolves_utc": end_utc.isoformat(),
        "resolves_et": end_utc.astimezone(timezone(ET_OFFSET)).isoformat(),
        "time_remaining_sec": max(0, int(time_remaining.total_seconds())),
        "time_remaining_fmt": str(time_remaining).split(".")[0] if time_remaining.total_seconds() > 0 else "resolved",

        # Odds
        "outcomes": outcomes,
        "prices": {outcomes[i]: prices[i] for i in range(len(outcomes))},
        "best_bid": m.get("bestBid"),
        "best_ask": m.get("bestAsk"),
        "last_trade_price": m.get("lastTradePrice"),
        "spread": m.get("spread"),
        "one_hour_price_change": m.get("oneHourPriceChange"),
        "one_day_price_change": m.get("oneDayPriceChange"),

        # Volume & liquidity
        "volume": float(event.get("volume", 0)),
        "volume_24hr": float(event.get("volume24hr", 0)),
        "liquidity": float(event.get("liquidity", 0)),
        "open_interest": float(event.get("openInterest", 0)),
        "competitive_score": event.get("competitive"),

        # XRP price reference
        "price_to_beat": event.get("eventMetadata", {}).get("priceToBeat"),

        # Series-wide stats
        "series_volume_24hr": series.get("volume24hr"),
        "series_liquidity": series.get("liquidity"),
        "series_comments": series.get("commentCount"),

        # Technical / CLOB
        "condition_id": m["conditionId"],
        "question_id": m.get("questionID"),
        "clob_token_up": clob_ids[0],
        "clob_token_down": clob_ids[1],
        "min_order_size": m.get("orderMinSize"),
        "tick_size": m.get("orderPriceMinTickSize"),
        "fee_rate": m.get("feeSchedule", {}).get("rate"),
        "taker_only": m.get("feeSchedule", {}).get("takerOnly"),
        "accepting_orders": m.get("acceptingOrders"),
        "resolver_address": m.get("resolvedBy"),
    }
