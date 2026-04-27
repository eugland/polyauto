"""
View YES/NO prices for every market under a Polymarket event slug.

Usage:
  python -m experiment.view highest-temperature-in-london-on-april-27-2026
  python -m experiment.view https://polymarket.com/event/highest-temperature-in-london-on-april-27-2026
  python -m experiment.view                       # all temp events resolving within 24h
  python -m experiment.view <slug> --watch 5      # refresh every 5 seconds
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from experiment.temp_market_collector import _extract_icao, _resolve_coords_and_tz

_BUCKET_NUM_RE = re.compile(r"(-?\d+(?:\.\d+)?)")
# Matches "be 21°C", "be between 62-63°F", "be 25°C or higher" etc. Anchored on
# "be (between)?" so a range dash doesn't get parsed as a leading minus sign.
_BUCKET_UNIT_RE = re.compile(
    r"\bbe\s+(?:between\s+)?(-?\d+(?:\.\d+)?)\s*(?:°?\s*[CF]\s*)?(?:-\s*(-?\d+(?:\.\d+)?)\s*)?°?\s*([CF])\b",
    re.IGNORECASE,
)
_SLUG_RE = re.compile(
    r"^highest-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$",
    re.IGNORECASE,
)
_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june",
     "july", "august", "september", "october", "november", "december"], 1)}


def _bucket_sort_key(question: str) -> float:
    """Sort by the lower-bound number of the bucket (matched after 'be ')."""
    mu = _BUCKET_UNIT_RE.search(question or "")
    if mu:
        return float(mu.group(1))
    m = _BUCKET_NUM_RE.search(question or "")
    return float(m.group(1)) if m else float("inf")


def _bucket_label(question: str) -> str:
    """e.g. '21°C', '62°F', '62-66°F', '25+°F', '15-°C'."""
    mu = _BUCKET_UNIT_RE.search(question or "")
    if mu:
        lo, hi, unit = mu.group(1), mu.group(2), mu.group(3).upper()
    else:
        m = _BUCKET_NUM_RE.search(question or "")
        if not m:
            return question
        lo, hi, unit = m.group(1), None, "C"
    q = question.lower()
    if "or higher" in q:
        return f"{lo}+°{unit}"
    if "or below" in q or "or lower" in q:
        return f"{lo}-°{unit}"
    if hi:
        return f"{lo}-{hi}°{unit}"
    return f"{lo}°{unit}"


def _city_date_from_slug(slug: str) -> tuple[str | None, datetime | None, str | None]:
    """Return (city, naive event_date as datetime, formatted date string)."""
    m = _SLUG_RE.match(slug or "")
    if not m:
        return None, None, None
    city = m.group(1).replace("-", " ").title()
    month = _MONTHS.get(m.group(2).lower())
    day, year = int(m.group(3)), int(m.group(4))
    if not month:
        return city, None, None
    try:
        dt = datetime(year, month, day)
    except ValueError:
        return city, None, None
    date_str = dt.strftime("%B %d, %Y").replace(" 0", " ")
    return city, dt, date_str


_tz_cache: dict[tuple[str | None, str | None], str | None] = {}


def _cached_tz(icao: str | None, city: str | None) -> str | None:
    key = (icao, city)
    if key not in _tz_cache:
        _, _, tz = _resolve_coords_and_tz(icao, city)
        _tz_cache[key] = tz
    return _tz_cache[key]


def _resolution_dt(event: dict, city: str | None, event_date: datetime | None) -> datetime | None:
    """Resolution = local 3pm at event's location, returned as a tz-aware datetime."""
    if not event_date:
        return None
    icao = _extract_icao(event.get("description") or "")
    tz_name = _cached_tz(icao, city)
    if not tz_name:
        return None
    try:
        from zoneinfo import ZoneInfo
        local_tz = ZoneInfo(tz_name)
    except Exception:
        return None
    return event_date.replace(hour=15, minute=0, second=0, tzinfo=local_tz)


def _resolution_local(event: dict, slug: str, city: str | None, event_date: datetime | None) -> str | None:
    dt = _resolution_dt(event, city, event_date)
    if not dt:
        return None
    return dt.astimezone().strftime("%m-%d  %H:%M")

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"


def _parse_json_list(raw: Any) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return []


def _safe_float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def slug_from_arg(arg: str) -> str:
    """Accept either a bare slug or a polymarket.com/event/<slug> URL."""
    if arg.startswith("http://") or arg.startswith("https://"):
        path = urlparse(arg).path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        if "event" in parts:
            i = parts.index("event")
            if i + 1 < len(parts):
                return parts[i + 1]
        return parts[-1] if parts else arg
    return arg.strip()


def fetch_event(slug: str) -> dict | None:
    resp = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=15)
    resp.raise_for_status()
    events = resp.json()
    if isinstance(events, list) and events:
        return events[0]
    return None


def fetch_temperature_events(window_hours: int = 48) -> list[dict]:
    """Page through Gamma weather/highest-temperature events ending within the window."""
    end_max = (datetime.now(timezone.utc) + timedelta(hours=window_hours)).isoformat()
    out: list[dict] = []
    seen: set[str] = set()
    for tag in ("highest-temperature", "weather"):
        for page in range(30):
            resp = requests.get(
                GAMMA_EVENTS_URL,
                params={
                    "tag_slug": tag,
                    "closed": "false",
                    "limit": 20,
                    "offset": page * 20,
                    "end_date_max": end_max,
                },
                timeout=15,
            )
            resp.raise_for_status()
            events = resp.json()
            if not isinstance(events, list) or not events:
                break
            for ev in events:
                slug = str(ev.get("slug") or "")
                if not _SLUG_RE.match(slug):
                    continue
                eid = str(ev.get("id") or slug)
                if eid in seen:
                    continue
                seen.add(eid)
                out.append(ev)
            if len(events) < 20:
                break
    return out


def fetch_events_resolving_within(hours: int) -> list[dict]:
    """Return events whose local-3pm resolution falls in [now, now+hours]."""
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc + timedelta(hours=hours)
    keep: list[tuple[datetime, dict]] = []
    for ev in fetch_temperature_events(window_hours=max(hours + 24, 48)):
        slug = ev.get("slug") or ""
        city, ed, _ = _city_date_from_slug(slug)
        rdt = _resolution_dt(ev, city, ed)
        if rdt and now_utc <= rdt.astimezone(timezone.utc) <= cutoff:
            keep.append((rdt, ev))
    keep.sort(key=lambda x: x[0])
    return [ev for _, ev in keep]


def fetch_clob_price(token_id: str, side: str) -> float | None:
    """side = 'buy' returns best ask, 'sell' returns best bid."""
    try:
        r = requests.get(CLOB_PRICE_URL, params={"token_id": token_id, "side": side}, timeout=8)
        r.raise_for_status()
        return _safe_float(r.json().get("price"))
    except Exception:
        return None


def market_row(mkt: dict, *, live: bool = False) -> dict:
    """Extract YES/NO pricing from one Gamma market dict."""
    outcomes = _parse_json_list(mkt.get("outcomes"))
    token_ids = _parse_json_list(mkt.get("clobTokenIds"))
    outcome_prices = _parse_json_list(mkt.get("outcomePrices"))

    yes_tok = no_tok = None
    yes_last = no_last = None
    for i, name in enumerate(outcomes):
        s = str(name).strip().lower()
        tok = str(token_ids[i]) if i < len(token_ids) else None
        last = _safe_float(outcome_prices[i]) if i < len(outcome_prices) else None
        if s == "yes":
            yes_tok, yes_last = tok, last
        elif s == "no":
            no_tok, no_last = tok, last

    # Gamma embeds bestBid/bestAsk as the YES side
    yes_bid = _safe_float(mkt.get("bestBid"))
    yes_ask = _safe_float(mkt.get("bestAsk"))
    no_bid = (1 - yes_ask) if yes_ask is not None else None
    no_ask = (1 - yes_bid) if yes_bid is not None else None

    if live and yes_tok:
        live_yes_ask = fetch_clob_price(yes_tok, "buy")
        live_yes_bid = fetch_clob_price(yes_tok, "sell")
        if live_yes_ask is not None:
            yes_ask = live_yes_ask
            no_bid = 1 - live_yes_ask
        if live_yes_bid is not None:
            yes_bid = live_yes_bid
            no_ask = 1 - live_yes_bid

    return {
        "question": str(mkt.get("question") or ""),
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_last": yes_last,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "no_last": no_last,
        "volume": _safe_float(mkt.get("volume")),
        "closed": bool(mkt.get("closed")),
    }


def _fmt(p: float | None) -> str:
    return f"{p:>5.3f}" if p is not None else "  -  "


def render(
    event: dict,
    *,
    live: bool = False,
    max_no: float = 1.0,
    min_no_bid: float | None = None,
    min_delta: float = 0,
) -> str | None:
    """Return one-line summary, or None if no bucket passes the filters."""
    slug = event.get("slug") or ""
    url = f"https://polymarket.com/event/{slug}" if slug else ""
    city, event_date, _date_str = _city_date_from_slug(slug)
    date_short = event_date.strftime("%m-%d") if event_date else ""

    markets = _parse_json_list(event.get("markets")) or []
    rows = [market_row(m, live=live) for m in markets if isinstance(m, dict)]
    if not rows:
        return f"{city or slug} | (no markets)"

    fav = max(
        (r for r in rows if r["yes_bid"] is not None),
        key=lambda r: r["yes_bid"],
        default=None,
    )
    fav_str = (
        f"exp: {_bucket_label(fav['question'])} ({int(round(fav['yes_bid'] * 100))}c)"
        if fav else "exp: -"
    )
    resolves = _resolution_local(event, slug, city, event_date) or ""
    fav_temp = _bucket_sort_key(fav["question"]) if fav else None

    def _passes(r: dict) -> bool:
        if r["no_ask"] is None or r["no_ask"] >= max_no:
            return False
        if min_no_bid is not None and (r["no_bid"] is None or r["no_bid"] < min_no_bid):
            return False
        if min_delta > 0:
            if fav_temp is None:
                return False
            if abs(_bucket_sort_key(r["question"]) - fav_temp) < min_delta:
                return False
        return True

    tradable = [r for r in rows if _passes(r)]
    if not tradable:
        return None
    tradable.sort(key=lambda r: (r["closed"], _bucket_sort_key(r["question"])))

    r = tradable[0]
    ask_str = f"{r['no_ask'] * 100:>5.1f}c"
    bucket = f"{_bucket_label(r['question']):>5}  {ask_str}"
    if fav:
        delta = abs(_bucket_sort_key(r["question"]) - fav_temp)
        delta_s = f"{delta:.0f}" if delta == int(delta) else f"{delta:g}"
        exp_str = f"{fav_str} Δ: {delta_s}"
    else:
        exp_str = fav_str
    parts = [city or "", date_short, bucket, exp_str, resolves, url]
    return " | ".join(p for p in parts if p)


def main() -> int:
    ap = argparse.ArgumentParser(description="View YES/NO prices for a Polymarket event")
    ap.add_argument("slug", nargs="?", default=None,
                    help="Event slug or full polymarket.com/event/<slug> URL. Omit to list all temperature events resolving within --within hours.")
    ap.add_argument("--watch", type=float, default=0, help="Refresh every N seconds (0 = once)")
    ap.add_argument("--live", action="store_true",
                    help="Hit CLOB /price for fresher YES bid/ask (slower, one extra request per market)")
    ap.add_argument("--max-no", type=float, default=1.0,
                    help="Only show buckets with NO ask < this (default 1.0 = drop only fully-dead rows)")
    ap.add_argument("--within", type=int, default=24,
                    help="When no slug is given, list events resolving within this many hours (default 24)")
    ap.add_argument("--min-no-bid", type=float, default=0.95,
                    help="Bulk list only: drop buckets whose NO bid < this (default 0.95)")
    ap.add_argument("--min-delta", type=float, default=2,
                    help="Bulk list only: drop buckets whose temp gap to favorite < this (default 2)")
    args = ap.parse_args()

    while True:
        if args.watch > 0:
            print("\033[2J\033[H", end="")  # clear screen

        if args.slug:
            slug = slug_from_arg(args.slug)
            try:
                event = fetch_event(slug)
            except Exception as exc:
                print(f"Error fetching event: {exc}", file=sys.stderr)
                return 1
            if not event:
                print(f"No event found for slug: {slug}", file=sys.stderr)
                return 1
            line = render(event, live=args.live, max_no=args.max_no)
            if line:
                print(line)
            else:
                print(f"(no tradable buckets for {slug})")
        else:
            try:
                events = fetch_events_resolving_within(args.within)
            except Exception as exc:
                print(f"Error fetching events: {exc}", file=sys.stderr)
                return 1
            shown = 0
            for ev in events:
                line = render(
                    ev, live=args.live, max_no=args.max_no,
                    min_no_bid=args.min_no_bid, min_delta=args.min_delta,
                )
                if line:
                    print(line)
                    shown += 1
            if shown == 0:
                print(f"(no events match filters within {args.within}h)")

        if args.watch <= 0:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
