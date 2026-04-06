"""
automata/btc_reach.py

BTC daily price-range probability analyzer.

Fetches the 'bitcoin-price-on-{date}' Polymarket event (range buckets like
'64,000-66,000', '<58,000', '>76,000'), estimates the probability that BTC
closes inside each bucket using a log-normal terminal model driven by Binance
1H momentum and volatility, then compares against live market YES prices.

Run:  python -m automata.btc_reach
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen

# ── Constants ─────────────────────────────────────────────────────────────────

GAMMA_API      = "https://gamma-api.polymarket.com/events?slug={slug}"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit={n}"
BINANCE_PRICE  = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}


# ── Slug builder & resolution time ───────────────────────────────────────────

def build_daily_slug(dt: datetime) -> str:
    """Build 'bitcoin-price-on-april-6' style slug for the given date."""
    return f"bitcoin-price-on-{MONTH_NAMES[dt.month]}-{dt.day}"


def slug_resolution_utc(slug: str) -> datetime | None:
    """
    Derive resolution time from slug ending in 'on-{month}-{day}'.
    Market resolves at 12:00 PM ET = 16:00 UTC (EDT, UTC-4).
    """
    m = re.search(r"on-([a-z]+)-(\d+)$", slug)
    if not m:
        return None
    month_idx = next((i for i, n in enumerate(MONTH_NAMES) if n == m.group(1)), None)
    if not month_idx:
        return None
    year = datetime.now(timezone.utc).year
    try:
        return datetime(year, month_idx, int(m.group(2)), 16, 0, 0, tzinfo=timezone.utc)
    except ValueError:
        return None


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


# ── Market fetch & parse ──────────────────────────────────────────────────────

def fetch_event(slug: str) -> dict | None:
    try:
        data = _get(GAMMA_API.format(slug=slug))
        return data[0] if data else None
    except (URLError, json.JSONDecodeError, IndexError) as e:
        print(f"  [btc_reach] fetch_event error: {e}")
        return None


def _parse_range_title(title: str) -> tuple[float | None, float | None] | None:
    """
    Parse groupItemTitle into (lo, hi) price bounds.
      '<58,000'        -> (None, 58000)   bottom bucket
      '58,000-60,000'  -> (58000, 60000)  range bucket
      '>76,000'        -> (76000, None)   top bucket
    Returns None if unrecognised.
    """
    t = title.strip().replace(",", "")
    # bottom: <58000
    m = re.fullmatch(r"<(\d+(?:\.\d+)?)", t)
    if m:
        return None, float(m.group(1))
    # top: >76000
    m = re.fullmatch(r">(\d+(?:\.\d+)?)", t)
    if m:
        return float(m.group(1)), None
    # range: 58000-60000
    m = re.fullmatch(r"(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)", t)
    if m:
        return float(m.group(1)), float(m.group(2))
    return None


def parse_btc_markets(event: dict) -> list[dict]:
    """
    Extract all sub-markets from the event.
    Each entry has: question, label, lo, hi, yes_token_id, no_token_id,
                    yes_price_mid, no_price_mid, end_utc, market_id.
    Sorted by lo ascending (None lo = bottom bucket, sorts first).
    """
    event_end_str = event.get("endDate") or ""
    event_end_utc: datetime | None = None
    if event_end_str:
        try:
            dt = datetime.fromisoformat(event_end_str.replace("Z", "+00:00"))
            event_end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    results = []
    for m in event.get("markets", []):
        if m.get("closed"):
            continue
        if m.get("active") is not None and not m.get("active"):
            continue

        title_raw = str(m.get("groupItemTitle") or "")
        bounds = _parse_range_title(title_raw)
        if bounds is None:
            continue
        lo, hi = bounds

        question = str(m.get("question") or title_raw)

        def _load(key: str) -> list:
            v = m.get(key, [])
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    return []
            return v if isinstance(v, list) else []

        outcomes   = _load("outcomes")
        token_ids  = _load("clobTokenIds")
        prices_raw = _load("outcomePrices")

        yes_token = no_token = None
        yes_price_mid = no_price_mid = None
        for i, name in enumerate(outcomes):
            name_l = str(name).strip().lower()
            if name_l == "yes" and i < len(token_ids):
                yes_token = str(token_ids[i])
                if i < len(prices_raw):
                    try:
                        yes_price_mid = float(prices_raw[i])
                    except (TypeError, ValueError):
                        pass
            elif name_l == "no" and i < len(token_ids):
                no_token = str(token_ids[i])
                if i < len(prices_raw):
                    try:
                        no_price_mid = float(prices_raw[i])
                    except (TypeError, ValueError):
                        pass

        if yes_token is None:
            continue

        m_end_utc = event_end_utc
        m_end_str = m.get("endDateIso") or m.get("endDate") or ""
        if m_end_str and m_end_str != event_end_str:
            try:
                dt2 = datetime.fromisoformat(m_end_str.replace("Z", "+00:00"))
                m_end_utc = dt2 if dt2.tzinfo else dt2.replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        results.append({
            "question":      question,
            "label":         title_raw,
            "lo":            lo,
            "hi":            hi,
            "yes_token_id":  yes_token,
            "no_token_id":   no_token,
            "yes_price_mid": yes_price_mid,
            "no_price_mid":  no_price_mid,
            "end_utc":       m_end_utc,
            "market_id":     m.get("id"),
        })

    # Sort: bottom bucket (lo=None) first, then by lo ascending, top bucket last
    results.sort(key=lambda x: (x["lo"] is None and x["hi"] is not None,
                                 x["lo"] if x["lo"] is not None else -1,
                                 x["hi"] is None))
    # Fix: bottom bucket should sort before all ranges
    results.sort(key=lambda x: x["lo"] if x["lo"] is not None else -float("inf"))
    return results


# ── Binance data ──────────────────────────────────────────────────────────────

def get_btc_spot() -> float:
    return float(_get(BINANCE_PRICE)["price"])


def get_btc_klines(n: int = 48) -> list[dict]:
    """Fetch last n 1H candles, oldest to newest."""
    raw = _get(BINANCE_KLINES.format(n=n))
    result = []
    for k in raw:
        o = float(k[1])
        c = float(k[4])
        result.append({
            "open_time": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            "open":  o,
            "high":  float(k[2]),
            "low":   float(k[3]),
            "close": c,
            "log_return": math.log(c / o) if o > 0 else 0.0,
        })
    return result


# ── Probability model ─────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def estimate_params(klines: list[dict]) -> tuple[float, float]:
    """
    Returns (hourly_drift, hourly_vol).
    drift = mean log-return of last 24 candles (momentum signal)
    vol   = std log-return of all candles (volatility estimate)
    """
    returns = [k["log_return"] for k in klines]
    n = len(returns)
    if n < 2:
        return 0.0, 0.001
    mean_all = sum(returns) / n
    var_all  = sum((r - mean_all) ** 2 for r in returns) / (n - 1)
    vol      = math.sqrt(var_all)
    recent   = returns[-24:] if n >= 24 else returns
    drift    = sum(recent) / len(recent)
    return drift, vol


def momentum_pick(markets: list[dict], spot: float, drift: float) -> dict | None:
    """
    Pick the best NO bet based on momentum direction.

    Going UP  (drift > 0): bet NO on buckets BELOW current price —
                           BTC is moving away from them.
    Going DOWN (drift < 0): bet NO on buckets ABOVE current price —
                           BTC is moving away from them.

    Among eligible buckets, pick the one with the highest NO edge
    that actually has a NO ask (liquid).
    """
    direction = "UP" if drift >= 0 else "DOWN"

    if direction == "UP":
        # buckets entirely below spot: hi <= spot
        eligible = [m for m in markets if m.get("hi") is not None and m["hi"] <= spot]
    else:
        # buckets entirely above spot: lo >= spot
        eligible = [m for m in markets if m.get("lo") is not None and m["lo"] >= spot]

    # must have a liquid NO ask and positive NO edge
    eligible = [m for m in eligible if m.get("no_ask") is not None and m.get("no_edge") is not None]

    if not eligible:
        return None

    return max(eligible, key=lambda m: m["no_edge"])


def _prob_above(current: float, target: float, T: float, drift: float, vol: float) -> float:
    """P(S_T >= target) — log-normal terminal probability."""
    if T <= 0:
        return 1.0 if current >= target else 0.0
    if vol < 1e-8:
        vol = 1e-8
    mu_star = drift - 0.5 * vol ** 2
    d = (-math.log(target / current) + mu_star * T) / (vol * math.sqrt(T))
    return _norm_cdf(d)


def bucket_probability(
    current: float,
    lo: float | None,
    hi: float | None,
    T_hours: float,
    drift: float,
    vol: float,
) -> float:
    """
    P(lo <= S_T < hi) at resolution time T_hours from now.
      lo=None  -> bottom bucket: P(S_T < hi)
      hi=None  -> top bucket:    P(S_T >= lo)
      both set -> range bucket:  P(lo <= S_T < hi) = P(above lo) - P(above hi)
    """
    if lo is None and hi is not None:
        return 1.0 - _prob_above(current, hi, T_hours, drift, vol)
    if hi is None and lo is not None:
        return _prob_above(current, lo, T_hours, drift, vol)
    if lo is not None and hi is not None:
        return max(0.0, _prob_above(current, lo, T_hours, drift, vol)
                      - _prob_above(current, hi, T_hours, drift, vol))
    return 0.0


# ── Live order book prices ────────────────────────────────────────────────────

def fetch_live_books(markets: list[dict], host: str) -> dict[str, dict]:
    """Bulk-fetch bid/ask for all YES and NO tokens."""
    import requests
    token_ids = [
        tid for m in markets
        for tid in [m.get("yes_token_id"), m.get("no_token_id")]
        if tid
    ]
    if not token_ids:
        return {}
    try:
        resp = requests.post(
            f"{host}/books",
            json=[{"token_id": tid} for tid in token_ids],
            timeout=10,
        )
        resp.raise_for_status()
        result: dict[str, dict] = {}
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
        return result
    except Exception as e:
        print(f"  [btc_reach] books fetch error: {e}")
        return {}


# ── Main analysis ─────────────────────────────────────────────────────────────

def analyze(
    slug: str | None = None,
    host: str = "https://clob.polymarket.com",
) -> list[dict]:
    """
    Fetch the BTC daily price-range event, compute bucket probabilities,
    print analysis table, and return enriched market list.
    """
    now_utc = datetime.now(timezone.utc)

    if slug:
        slugs_to_try = [slug]
    else:
        slugs_to_try = []
        for delta in [1, 0, 2]:
            s = build_daily_slug(now_utc + timedelta(days=delta))
            res = slug_resolution_utc(s)
            if res and now_utc < res:
                slugs_to_try.append(s)

    event = None
    used_slug = None
    for s in slugs_to_try:
        event = fetch_event(s)
        if event:
            used_slug = s
            break

    if not event:
        print(f"  [btc_reach] No active event found. Tried: {slugs_to_try}")
        return []

    markets = parse_btc_markets(event)
    if not markets:
        print(f"  [btc_reach] No parseable markets in event '{used_slug}'")
        return []

    # Resolution time — noon ET from slug, API endDate is unreliable
    end_utc = slug_resolution_utc(used_slug) or markets[0]["end_utc"]
    hours_remaining = 0.0
    if end_utc:
        hours_remaining = max(0.0, (end_utc - now_utc).total_seconds() / 3600.0)

    # Binance data
    try:
        spot   = get_btc_spot()
        klines = get_btc_klines(n=48)
        drift, vol = estimate_params(klines)
    except Exception as e:
        print(f"  [btc_reach] Binance error: {e}")
        return []

    live_books = fetch_live_books(markets, host)

    # Enrich each market
    for m in markets:
        m["_spot"]  = spot
        m["_drift"] = drift
        m["model_prob"] = bucket_probability(spot, m["lo"], m["hi"], hours_remaining, drift, vol)

        yes_book = live_books.get(m["yes_token_id"], {})
        m["yes_bid"] = yes_book.get("bid")
        m["yes_ask"] = yes_book.get("ask")
        market_yes   = m["yes_ask"] if m["yes_ask"] is not None else m.get("yes_price_mid")
        m["market_yes"] = market_yes
        m["yes_edge"] = (m["model_prob"] - market_yes) if market_yes is not None else None

        no_book = live_books.get(m["no_token_id"], {}) if m.get("no_token_id") else {}
        m["no_bid"] = no_book.get("bid")
        m["no_ask"] = no_book.get("ask")
        market_no    = m["no_ask"] if m["no_ask"] is not None else m.get("no_price_mid")
        m["market_no"] = market_no
        m["no_edge"] = ((1 - m["model_prob"]) - market_no) if market_no is not None else None

    # ── Print ─────────────────────────────────────────────────────────────────
    div  = "=" * 105
    div2 = "-" * 105

    print(f"\n{div}")
    print(f"  BTC DAILY PRICE RANGE ANALYSIS")
    print(f"  Event:    {event.get('title', used_slug)}")
    print(f"  Slug:     {used_slug}")
    print(f"  Now:      {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if end_utc:
        print(f"  Resolves: {end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}  ({hours_remaining:.1f}h remaining)")
    print(div2)

    daily_vol_pct = vol * math.sqrt(24) * 100
    print(f"\n  BINANCE BTC/USDT")
    print(f"    Spot price:     ${spot:>12,.2f}")
    if klines:
        p24 = klines[-24]["open"] if len(klines) >= 24 else klines[0]["open"]
        p6  = klines[-6]["open"]  if len(klines) >= 6  else klines[0]["open"]
        print(f"    24h change:     {(spot-p24)/p24*100:>+9.2f}%   (from ${p24:,.2f})")
        print(f"    6h change:      {(spot-p6)/p6*100:>+9.2f}%   (from ${p6:,.2f})")
    print(f"    Hourly drift:   {drift*100:>+9.4f}%   (mean log-return last 24h - momentum)")
    print(f"    Hourly vol:     {vol*100:>9.4f}%   (std log-return last 48h)")
    print(f"    Daily vol est:  {daily_vol_pct:>9.2f}%   (vol * sqrt(24))")

    print(f"\n  BUCKET PROBABILITIES vs MARKET")
    print(f"    {'':2}  {'Range':>15}  {'ModelP':>7}  {'YES ask':>7}  {'YES edge':>8}  {'YES bid':>7}  {'NO ask':>7}  {'NO edge':>8}  {'NO bid':>7}")
    print(f"    {'':2}  {'-'*15}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*7}")

    for m in markets:
        # range label
        if m["lo"] is None:
            rng = f"< ${m['hi']:>9,.0f}"
        elif m["hi"] is None:
            rng = f"> ${m['lo']:>9,.0f}"
        else:
            rng = f"${m['lo']:>7,.0f}-{m['hi']:>7,.0f}"

        model_str    = f"{m['model_prob']*100:6.1f}%"
        yes_ask_str  = f"{m['market_yes']*100:6.1f}%" if m["market_yes"] is not None else "   n/a"
        yes_edge_str = f"{m['yes_edge']*100:+7.1f}%"  if m["yes_edge"]  is not None else "    n/a"
        yes_bid_str  = f"{m['yes_bid']*100:6.2f}c"    if m["yes_bid"]   is not None else "   n/a"
        no_ask_str   = f"{m['market_no']*100:6.1f}%"  if m["market_no"] is not None else "   n/a"
        no_edge_str  = f"{m['no_edge']*100:+7.1f}%"   if m["no_edge"]   is not None else "    n/a"
        no_bid_str   = f"{m['no_bid']*100:6.2f}c"     if m["no_bid"]    is not None else "   n/a"

        yes_star = "*" if (m["yes_edge"] is not None and m["yes_edge"] >= 0.03) else " "
        no_star  = "*" if (m["no_edge"]  is not None and m["no_edge"]  >= 0.03) else " "

        # mark the bucket containing current spot
        in_range = (
            (m["lo"] is None or spot >= m["lo"]) and
            (m["hi"] is None or spot <  m["hi"])
        )
        spot_marker = "<<" if in_range else "  "

        print(
            f"    {yes_star}{no_star}  {rng:>15}  {model_str}  {yes_ask_str}  "
            f"{yes_edge_str}  {yes_bid_str}  {no_ask_str}  {no_edge_str}  {no_bid_str}  {spot_marker}"
        )

    print(f"\n  ModelP   = P(BTC closes inside this range at noon ET) - log-normal model")
    print(f"  YES edge = model - YES ask  (* >= 3%): buy YES if bucket is underpriced")
    print(f"  NO edge  = (1-model) - NO ask (* >= 3%): buy NO if bucket is overpriced")
    print(f"  <<       = BTC is currently in this range")

    # ── Momentum-based recommendation ─────────────────────────────────────────
    direction = "UP" if drift >= 0 else "DOWN"
    pick = momentum_pick(markets, spot, drift)
    print(f"\n  MOMENTUM BET")
    print(f"    Direction: {direction}  (hourly drift {drift*100:+.4f}%)")
    if pick:
        rng = f"${pick['lo']:,.0f}-{pick['hi']:,.0f}" if pick["lo"] and pick["hi"] else \
              (f"< ${pick['hi']:,.0f}" if pick["lo"] is None else f"> ${pick['lo']:,.0f}")
        print(f"    Bet:       NO on {rng}")
        print(f"    NO ask:    {pick['no_ask']*100:.1f}c   NO edge: {pick['no_edge']*100:+.1f}%")
        print(f"    Reason:    BTC trending {direction} -> {rng} is moving further away")
    else:
        print(f"    No liquid NO candidates on the {direction} side")

    print(f"{div}\n")

    return markets


if __name__ == "__main__":
    import os
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dotenv import load_dotenv
    load_dotenv()
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    analyze(host=host)
