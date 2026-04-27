"""Pull aapang's full Polymarket trade + activity history and analyze weather-market behaviour.

Username:   aapang Husky-Golf
Wallet:     0x104171232971a6db8cf938f76fdbebbb81c5f452

Usage:
    python -m experiment.analyze_aapang                 # full report
    python -m experiment.analyze_aapang --raw out.json  # also dump raw activity to JSON
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

ADDR = "0x104171232971a6db8cf938f76fdbebbb81c5f452"
DATA_API = "https://data-api.polymarket.com"


def fetch_all(path: str, params: dict, page_size: int = 500, max_pages: int = 60) -> list[dict]:
    """Paginate via offset until exhausted or the API's offset cap (~3000) is reached."""
    out: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        p = dict(params)
        p["limit"] = page_size
        p["offset"] = offset
        r = requests.get(f"{DATA_API}/{path}", params=p, timeout=20)
        if r.status_code == 400 and "offset" in r.text.lower():
            # API caps historical offset (3000 at time of writing). Stop here.
            break
        r.raise_for_status()
        page = r.json()
        if not isinstance(page, list) or not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
        time.sleep(0.1)
    return out


THRESH_RE = re.compile(r"be (-?\d+)\s*[°�]?C", re.IGNORECASE)
ABOVE_RE = re.compile(r"above (-?\d+(?:\.\d+)?)\s*[°�]?C", re.IGNORECASE)


def parse_threshold(title: str, slug: str) -> str:
    """Extract 'XX°C' or '<X>C/above' label from a temperature market title or slug."""
    m = THRESH_RE.search(title or "")
    if m:
        return f"{m.group(1)}C"
    m = ABOVE_RE.search(title or "")
    if m:
        return f">{m.group(1)}C"
    # slug usually ends with '...-N-2026-19c' style
    s = slug or ""
    m2 = re.search(r"-(\d+(?:\.\d+)?)c$", s)
    if m2:
        return f"{m2.group(1)}C"
    m3 = re.search(r"above-(\d+(?:\.\d+)?)c", s)
    if m3:
        return f">{m3.group(1)}C"
    return "?"


def parse_event_key(slug: str) -> str:
    """e.g. highest-temperature-in-paris-on-april-25-2026"""
    s = slug or ""
    base = re.sub(r"-(\d+(?:\.\d+)?)c$", "", s)
    base = re.sub(r"-above-(\d+(?:\.\d+)?)c$", "", base)
    return base


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_temp_market(title: str) -> bool:
    return "temperature" in (title or "").lower()


def classify_outcome(t: dict) -> str:
    """YES or NO label given the trade record."""
    o = (t.get("outcome") or "").upper()
    if o in ("YES", "NO"):
        return o
    idx = t.get("outcomeIndex")
    return "YES" if idx == 0 else "NO" if idx == 1 else "?"


def fetch_trades(addr: str = ADDR) -> list[dict]:
    return fetch_all("trades", {"user": addr})


def fetch_activity(addr: str = ADDR) -> list[dict]:
    return fetch_all("activity", {"user": addr})


def fetch_positions(addr: str = ADDR) -> list[dict]:
    return fetch_all("positions", {"user": addr, "sizeThreshold": "0.01"}, page_size=500, max_pages=2)


def group_by_event(trades: list[dict]) -> dict[str, list[dict]]:
    g = defaultdict(list)
    for t in trades:
        if not is_temp_market(t.get("title", "")):
            continue
        ek = parse_event_key(t.get("eventSlug") or t.get("slug") or "")
        g[ek].append(t)
    for v in g.values():
        v.sort(key=lambda x: x["timestamp"])
    return g


def event_resolution_ts(slug: str) -> int | None:
    """Last fill on a market we can use as 'closed near here' fallback."""
    return None


def render_event(event_slug: str, trades: list[dict]) -> str:
    out: list[str] = []
    title_first = trades[0].get("title", "")
    # Header: city + date
    m = re.match(r"highest-temperature-in-(.+)-on-([a-z]+-\d{1,2}-\d{4})$", event_slug)
    city = m.group(1).replace("-", " ").title() if m else event_slug
    date = m.group(2).replace("-", " ").title() if m else ""
    out.append("=" * 96)
    out.append(f"EVENT: {city} on {date}    ({event_slug})")
    out.append(f"  fills: {len(trades)}  first: {fmt_ts(trades[0]['timestamp'])}  last: {fmt_ts(trades[-1]['timestamp'])}")
    out.append("")

    # Per-bucket breakdown: bucket = threshold label. Track buys + sells + net.
    by_bucket: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in trades:
        thresh = parse_threshold(t.get("title", ""), t.get("slug", ""))
        side = classify_outcome(t)  # YES or NO token
        by_bucket[(thresh, side)].append(t)

    # Aggregate position per (bucket, token)
    out.append(f"  Per-bucket summary  (BUY = +shares, SELL = -shares):")
    out.append(f"  {'Bucket':>8} {'Token':>4}  {'Buys':>8}  {'Sells':>8}  {'Net':>8}  {'Avg buy':>8}  {'Avg sell':>8}  {'Cash spent':>11}  {'Cash got':>10}")
    rows = []
    for (thresh, side), fills in sorted(by_bucket.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        buys = [f for f in fills if f["side"] == "BUY"]
        sells = [f for f in fills if f["side"] == "SELL"]
        nb = sum(f["size"] for f in buys)
        ns = sum(f["size"] for f in sells)
        cb = sum(f["size"] * f["price"] for f in buys)
        cs = sum(f["size"] * f["price"] for f in sells)
        avg_b = (cb / nb) if nb else 0.0
        avg_s = (cs / ns) if ns else 0.0
        net = nb - ns
        rows.append((thresh, side, nb, ns, net, avg_b, avg_s, cb, cs))
    for r in rows:
        out.append(f"  {r[0]:>8} {r[1]:>4}  {r[2]:>8.0f}  {r[3]:>8.0f}  {r[4]:>+8.0f}  {r[5]:>8.4f}  {r[6]:>8.4f}  {r[7]:>11.2f}  {r[8]:>10.2f}")
    out.append("")

    # Full timeline
    out.append(f"  Full fill timeline:")
    out.append(f"  {'Time (UTC)':>20}  {'Side':>4}  {'Tok':>3}  {'Bucket':>8}  {'Px':>7}  {'Size':>10}  {'$':>10}")
    for t in trades:
        thresh = parse_threshold(t.get("title", ""), t.get("slug", ""))
        side_token = classify_outcome(t)
        out.append(
            f"  {fmt_ts(t['timestamp']):>20}  {t['side']:>4}  {side_token:>3}  {thresh:>8}  "
            f"{t['price']:>7.4f}  {t['size']:>10.2f}  {t['size']*t['price']:>10.2f}"
        )
    out.append("")
    return "\n".join(out)


def overall_stats(trades: list[dict]) -> str:
    out = []
    weather = [t for t in trades if is_temp_market(t.get("title", ""))]
    other = [t for t in trades if not is_temp_market(t.get("title", ""))]
    out.append(f"Total trades:        {len(trades):,}")
    out.append(f"Weather (temp) trades: {len(weather):,}")
    out.append(f"Other markets:        {len(other):,}")
    if trades:
        out.append(f"First trade: {fmt_ts(min(t['timestamp'] for t in trades))}")
        out.append(f"Last trade:  {fmt_ts(max(t['timestamp'] for t in trades))}")
    # Volume by side
    buy_v = sum(t["size"] * t["price"] for t in weather if t["side"] == "BUY")
    sell_v = sum(t["size"] * t["price"] for t in weather if t["side"] == "SELL")
    out.append(f"Weather BUY volume:  ${buy_v:>10,.2f}")
    out.append(f"Weather SELL volume: ${sell_v:>10,.2f}")
    # Cities
    cities: dict[str, int] = defaultdict(int)
    for t in weather:
        m = re.match(r"highest-temperature-in-(.+)-on-", t.get("eventSlug") or "")
        if m:
            cities[m.group(1).replace("-", " ")] += 1
    out.append("Top cities by fill count:")
    for c, n in sorted(cities.items(), key=lambda kv: -kv[1])[:15]:
        out.append(f"  {n:>5}  {c}")
    return "\n".join(out)


def render_open_positions(positions: list[dict]) -> str:
    out = ["=" * 96, "OPEN POSITIONS (size > 0)"]
    weather = [p for p in positions if "temperature" in (p.get("title") or "").lower()]
    other = [p for p in positions if "temperature" not in (p.get("title") or "").lower()]
    out.append(f"  weather positions: {len(weather)}    other: {len(other)}")
    out.append("")
    out.append(f"  {'Bucket':>8}  {'Tok':>3}  {'Size':>10}  {'AvgPx':>7}  {'CurPx':>7}  {'Init$':>10}  {'Cur$':>10}  {'PnL$':>10}  {'Title'}")
    weather.sort(key=lambda p: (p.get("slug") or ""))
    for p in weather:
        thresh = parse_threshold(p.get("title", ""), p.get("slug", ""))
        side = "YES" if (p.get("outcomeIndex") == 0 or (p.get("outcome") or "").upper() == "YES") else "NO"
        out.append(
            f"  {thresh:>8}  {side:>3}  {p['size']:>10.2f}  {p['avgPrice']:>7.4f}  {p['curPrice']:>7.4f}  "
            f"{p['initialValue']:>10.2f}  {p['currentValue']:>10.2f}  {p['cashPnl']:>10.2f}  {p.get('title','')}"
        )
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", help="dump raw trades+activity+positions JSON to this path")
    ap.add_argument("--limit-events", type=int, default=20, help="how many recent events to show in detail")
    ap.add_argument("--all-events", action="store_true", help="show every event (overrides --limit-events)")
    args = ap.parse_args()

    print(f"Fetching trades for {ADDR}...", file=sys.stderr)
    trades = fetch_trades()
    print(f"  {len(trades)} trades", file=sys.stderr)

    print("Fetching activity...", file=sys.stderr)
    activity = fetch_activity()
    print(f"  {len(activity)} activity rows", file=sys.stderr)

    print("Fetching positions...", file=sys.stderr)
    positions = fetch_positions()
    print(f"  {len(positions)} positions", file=sys.stderr)

    # Account value
    rv = requests.get(f"{DATA_API}/value", params={"user": ADDR}, timeout=10).json()
    print(f"Account value: ${rv[0]['value']:,.2f}", file=sys.stderr)

    if args.raw:
        with open(args.raw, "w") as f:
            json.dump({"trades": trades, "activity": activity, "positions": positions, "value": rv}, f)

    print(f"\n# aapang Husky-Golf  ({ADDR})")
    print(f"# Account value: ${rv[0]['value']:,.2f}")
    print()
    print("OVERALL STATS")
    print("=" * 96)
    print(overall_stats(trades))
    print()

    grouped = group_by_event(trades)
    # Sort events by most recent fill desc
    events_sorted = sorted(grouped.items(), key=lambda kv: -max(t["timestamp"] for t in kv[1]))
    if not args.all_events:
        events_sorted = events_sorted[: args.limit_events]
    print(f"Detail for {len(events_sorted)} events (most recent first):\n")
    for slug, fills in events_sorted:
        print(render_event(slug, fills))

    print(render_open_positions(positions))

    # MERGE / REDEEM / SPLIT activity (non-trade)
    non_trade = [a for a in activity if (a.get("type") or "").upper() != "TRADE"]
    if non_trade:
        print()
        print("=" * 96)
        print(f"NON-TRADE ACTIVITY ({len(non_trade)} rows)")
        for a in sorted(non_trade, key=lambda x: -x["timestamp"])[:60]:
            print(f"  {fmt_ts(a['timestamp']):>20}  {a.get('type'):>8}  size={a.get('size','')}  $={a.get('usdcSize','')}  {a.get('title','')}")


if __name__ == "__main__":
    main()
