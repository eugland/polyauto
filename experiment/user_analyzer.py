#!/usr/bin/env python3
"""
Polymarket user behavior analyzer.

Fetches all activity for a wallet address and produces a behavioral report:
  - Overall P/L and ROI
  - Market-type breakdown (which slugs / categories they trade)
  - Bet-size distribution
  - Win/loss rate by market type
  - Timing patterns (hour of day, recency)
  - Position concentration (diversified vs. focused)

Usage:
  python -m experiment.user_analyzer 0x7da07b2a8b009a406198677debda46
  python -m experiment.user_analyzer 0x7da07b2a8b009a406198677debda46 --max-pages 50
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DATA_API = "https://data-api.polymarket.com/activity"
POSITIONS_API = "https://data-api.polymarket.com/positions"
PAGE_SIZE = 500


# ── data fetching ─────────────────────────────────────────────────────────────

def _get_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def fetch_all_activity(addr: str, max_pages: int) -> list[dict]:
    rows: list[dict] = []
    for page in range(max_pages):
        offset = page * PAGE_SIZE
        url = f"{DATA_API}?user={addr}&limit={PAGE_SIZE}&offset={offset}"
        try:
            batch = _get_json(url)
        except (HTTPError, URLError) as exc:
            print(f"[warn] page {page} failed: {exc}", file=sys.stderr)
            break
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        print(f"[page {page:>3}] offset={offset:>5}  fetched={len(batch):>3}  total={len(rows):>5}",
              file=sys.stderr)
        if len(batch) < PAGE_SIZE:
            break
    return rows


def fetch_positions(addr: str) -> list[dict]:
    url = f"{POSITIONS_API}?user={addr}&sizeThreshold=0.01"
    try:
        data = _get_json(url)
        return data if isinstance(data, list) else []
    except Exception as exc:
        print(f"[warn] positions fetch failed: {exc}", file=sys.stderr)
        return []


# ── categorization ────────────────────────────────────────────────────────────

def _market_category(slug: str) -> str:
    slug = slug.lower()
    if "btc" in slug and ("5m" in slug or "1h" in slug or "updown" in slug):
        return "BTC candle"
    if "eth" in slug and ("5m" in slug or "1h" in slug or "15m" in slug or "updown" in slug):
        return "ETH candle"
    if "btc" in slug:
        return "BTC other"
    if "eth" in slug:
        return "ETH other"
    if "temperature" in slug or "highest-temp" in slug or "celsius" in slug:
        return "Weather"
    if any(x in slug for x in ["trump", "biden", "harris", "election", "president", "vote", "poll"]):
        return "Politics/Elections"
    if any(x in slug for x in ["fed", "rate", "inflation", "gdp", "cpi", "fomc"]):
        return "Macro/Economics"
    if any(x in slug for x in ["nba", "nfl", "mlb", "soccer", "football", "basketball",
                                 "tennis", "mma", "ufc", "cricket", "golf"]):
        return "Sports"
    if any(x in slug for x in ["crypto", "sol", "bnb", "doge", "xrp", "ada", "avax", "link"]):
        return "Crypto other"
    return "Other"


# ── aggregation ───────────────────────────────────────────────────────────────

@dataclass
class MarketSummary:
    slug: str
    title: str
    category: str
    spent: float = 0.0
    payout: float = 0.0   # sell proceeds + redeems
    trades: int = 0
    redeems: int = 0
    timestamps: list[int] = field(default_factory=list)
    usdc_sizes: list[float] = field(default_factory=list)


def aggregate(rows: list[dict]) -> dict[str, MarketSummary]:
    mkts: dict[str, MarketSummary] = {}
    for a in rows:
        slug = a.get("slug") or a.get("market") or ""
        if not slug:
            continue
        if slug not in mkts:
            mkts[slug] = MarketSummary(
                slug=slug,
                title=a.get("title") or slug,
                category=_market_category(slug),
            )
        m = mkts[slug]
        atype = a.get("type", "")
        try:
            usdc = float(a.get("usdcSize") or 0)
        except (TypeError, ValueError):
            usdc = 0.0
        ts = a.get("timestamp")
        if isinstance(ts, (int, float)):
            m.timestamps.append(int(ts))

        if atype == "TRADE":
            m.trades += 1
            side = a.get("side", "")
            if side == "BUY":
                m.spent += usdc
                m.usdc_sizes.append(usdc)
            elif side == "SELL":
                m.payout += usdc
        elif atype == "REDEEM":
            m.redeems += 1
            m.payout += usdc
    return mkts


# ── stats helpers ─────────────────────────────────────────────────────────────

def _pct(n: float, d: float) -> str:
    if d == 0:
        return "  n/a"
    return f"{n / d * 100:>5.1f}%"


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2


def _fmt_ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ── report ────────────────────────────────────────────────────────────────────

def print_report(addr: str, rows: list[dict], mkts: dict[str, MarketSummary],
                 positions: list[dict]) -> None:
    W = 80
    sep = "=" * W
    thin = "-" * W

    def hdr(title: str) -> None:
        print(f"\n{sep}")
        print(f"  {title}")
        print(sep)

    print(f"\n{'=' * W}")
    print(f"  Polymarket User Behavior Report")
    print(f"  Wallet : {addr}")
    print(f"  As of  : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'=' * W}")

    # ── Overview ──────────────────────────────────────────────────────────────
    hdr("OVERVIEW")
    all_spent = sum(m.spent for m in mkts.values())
    all_payout = sum(m.payout for m in mkts.values())
    net = all_payout - all_spent
    roi = net / all_spent * 100 if all_spent else 0.0
    total_trades = sum(m.trades for m in mkts.values())
    total_redeems = sum(m.redeems for m in mkts.values())
    all_ts = [ts for m in mkts.values() for ts in m.timestamps]
    first_ts = min(all_ts) if all_ts else 0
    last_ts = max(all_ts) if all_ts else 0

    print(f"  Markets traded    : {len(mkts):>6}")
    print(f"  Total BUY trades  : {total_trades:>6}")
    print(f"  Total redeems     : {total_redeems:>6}")
    print(f"  Total activity    : {len(rows):>6}")
    print(f"  Total spent       : ${all_spent:>10.2f}")
    print(f"  Total payout      : ${all_payout:>10.2f}")
    print(f"  Net P/L           : ${net:>+10.2f}")
    print(f"  ROI               : {roi:>+9.2f}%")
    if first_ts:
        print(f"  First activity    : {_fmt_ts(first_ts)}")
        print(f"  Last activity     : {_fmt_ts(last_ts)}")
        days_active = (last_ts - first_ts) / 86400
        if days_active > 0:
            daily_vol = all_spent / days_active
            print(f"  Days active       : {days_active:>6.1f}")
            print(f"  Avg daily spend   : ${daily_vol:>10.2f}")

    # ── Category breakdown ────────────────────────────────────────────────────
    hdr("MARKET CATEGORY BREAKDOWN")
    cat_stats: dict[str, dict] = defaultdict(lambda: {
        "markets": 0, "trades": 0, "spent": 0.0, "payout": 0.0
    })
    for m in mkts.values():
        c = cat_stats[m.category]
        c["markets"] += 1
        c["trades"] += m.trades
        c["spent"] += m.spent
        c["payout"] += m.payout

    cats_sorted = sorted(cat_stats.items(), key=lambda x: x[1]["spent"], reverse=True)
    print(f"  {'Category':<22}  {'Mkts':>5}  {'Trades':>7}  {'Spent':>10}  {'Payout':>10}  {'Net P/L':>10}  {'ROI':>7}")
    print(f"  {thin}")
    for cat, c in cats_sorted:
        c_net = c["payout"] - c["spent"]
        c_roi = c_net / c["spent"] * 100 if c["spent"] else 0
        print(f"  {cat:<22}  {c['markets']:>5}  {c['trades']:>7}  "
              f"${c['spent']:>9.2f}  ${c['payout']:>9.2f}  ${c_net:>+9.2f}  {c_roi:>+6.1f}%")

    # ── Bet size distribution ─────────────────────────────────────────────────
    hdr("BET SIZE DISTRIBUTION (USDC per trade)")
    all_sizes = [sz for m in mkts.values() for sz in m.usdc_sizes]
    if all_sizes:
        buckets = [(0, 1), (1, 5), (5, 10), (10, 25), (25, 50), (50, 100), (100, 500), (500, 99999)]
        print(f"  {'Range':<18}  {'Count':>7}  {'% of trades':>12}")
        print(f"  {thin}")
        for lo, hi in buckets:
            cnt = sum(1 for s in all_sizes if lo <= s < hi)
            if cnt:
                label = f"${lo}-${hi}" if hi < 99999 else f"${lo}+"
                print(f"  {label:<18}  {cnt:>7}  {_pct(cnt, len(all_sizes)):>12}")
        print(f"  {thin}")
        print(f"  {'Median bet':<18}  ${_median(all_sizes):>9.2f}")
        print(f"  {'Mean bet':<18}  ${sum(all_sizes)/len(all_sizes):>9.2f}")
        print(f"  {'Max bet':<18}  ${max(all_sizes):>9.2f}")
        print(f"  {'Min bet':<18}  ${min(all_sizes):>9.2f}")

    # ── Hour-of-day pattern ───────────────────────────────────────────────────
    hdr("ACTIVITY BY HOUR OF DAY (UTC)")
    hour_counts: dict[int, int] = defaultdict(int)
    for ts in all_ts:
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        hour_counts[h] += 1
    if hour_counts:
        peak_hour = max(hour_counts, key=hour_counts.__getitem__)
        total_acts = sum(hour_counts.values())
        bar_scale = 40 / max(hour_counts.values())
        for h in range(24):
            cnt = hour_counts.get(h, 0)
            bar = "#" * int(cnt * bar_scale)
            pct = cnt / total_acts * 100 if total_acts else 0
            marker = " <-- peak" if h == peak_hour else ""
            print(f"  {h:>02}:00  {bar:<40} {cnt:>4} ({pct:4.1f}%){marker}")

    # ── Top markets by volume ─────────────────────────────────────────────────
    hdr("TOP 20 MARKETS BY SPEND")
    top = sorted(mkts.values(), key=lambda m: m.spent, reverse=True)[:20]
    print(f"  {'#':>2}  {'Category':<16}  {'Spent':>9}  {'Payout':>9}  {'P/L':>9}  Title")
    print(f"  {thin}")
    for i, m in enumerate(top, 1):
        pnl = m.payout - m.spent
        title = (m.title or m.slug)[:45]
        print(f"  {i:>2}  {m.category:<16}  ${m.spent:>8.2f}  ${m.payout:>8.2f}  "
              f"${pnl:>+8.2f}  {title}")

    # ── Win/loss by market (settled) ──────────────────────────────────────────
    # A market is "won" if payout > spent (profit), "lost" if payout < spent
    settled = [m for m in mkts.values() if m.payout > 0 or m.redeems > 0]
    if settled:
        hdr("WIN/LOSS SUMMARY (markets with any payout)")
        wins = [m for m in settled if m.payout >= m.spent]
        losses = [m for m in settled if m.payout < m.spent]
        print(f"  Settled markets   : {len(settled)}")
        print(f"  Profitable markets: {len(wins)}  ({_pct(len(wins), len(settled))})")
        print(f"  Loss markets      : {len(losses)}  ({_pct(len(losses), len(settled))})")

        # By category
        cat_win: dict[str, list] = defaultdict(list)
        cat_loss: dict[str, list] = defaultdict(list)
        for m in wins:
            cat_win[m.category].append(m)
        for m in losses:
            cat_loss[m.category].append(m)
        all_cats = sorted(set(list(cat_win) + list(cat_loss)))
        print()
        print(f"  {'Category':<22}  {'Wins':>5}  {'Losses':>7}  {'Win%':>6}")
        print(f"  {thin}")
        for cat in all_cats:
            w = len(cat_win.get(cat, []))
            l = len(cat_loss.get(cat, []))
            total = w + l
            print(f"  {cat:<22}  {w:>5}  {l:>7}  {_pct(w, total):>6}")

    # ── Open positions ────────────────────────────────────────────────────────
    if positions:
        hdr(f"OPEN POSITIONS ({len(positions)} tokens)")
        pos_sorted = sorted(positions, key=lambda p: float(p.get("currentValue") or 0), reverse=True)
        total_value = sum(float(p.get("currentValue") or 0) for p in pos_sorted)
        print(f"  Total current value: ${total_value:.2f}")
        print()
        print(f"  {'Value':>8}  {'Size':>8}  {'Avg$':>6}  Title")
        print(f"  {thin}")
        for p in pos_sorted[:20]:
            val = float(p.get("currentValue") or 0)
            size = float(p.get("size") or 0)
            avg = float(p.get("avgPrice") or 0)
            title = str(p.get("title") or p.get("market") or "")[:55]
            print(f"  ${val:>7.2f}  {size:>8.1f}  {avg:>6.3f}  {title}")

    # ── Behavioral summary ────────────────────────────────────────────────────
    hdr("BEHAVIORAL SUMMARY")
    if cats_sorted:
        top_cat = cats_sorted[0][0]
        top_cat_pct = cats_sorted[0][1]["spent"] / all_spent * 100 if all_spent else 0
        print(f"  Primary market type: {top_cat} ({top_cat_pct:.1f}% of spend)")
    if all_sizes:
        med = _median(all_sizes)
        if med < 2:
            style = "micro-bettor (median < $2)"
        elif med < 10:
            style = "small-stakes (median $2–$10)"
        elif med < 50:
            style = "mid-stakes (median $10–$50)"
        else:
            style = "high-stakes (median > $50)"
        print(f"  Betting style      : {style}")
    if len(mkts) > 0:
        concentration = all_spent / len(mkts) if mkts else 0
        print(f"  Avg spend/market   : ${concentration:.2f}")
    if hour_counts:
        peak_h = max(hour_counts, key=hour_counts.__getitem__)
        print(f"  Peak trading hour  : {peak_h:02d}:00 UTC ({hour_counts[peak_h]} events)")
    if net > 0:
        print(f"  P/L verdict        : PROFITABLE  +${net:.2f} ({roi:+.1f}% ROI)")
    elif all_spent > 0:
        print(f"  P/L verdict        : UNPROFITABLE  ${net:.2f} ({roi:+.1f}% ROI)")
    else:
        print(f"  P/L verdict        : No spend data")

    print(f"\n{'=' * W}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Analyze a Polymarket user's behavior.")
    p.add_argument("address", help="Proxy-wallet address (e.g. 0x7da07...)")
    p.add_argument("--max-pages", type=int, default=40,
                   help=f"Max activity pages to fetch ({PAGE_SIZE}/page, default: 40)")
    args = p.parse_args()

    addr = args.address
    print(f"Fetching activity for {addr} ...", file=sys.stderr)
    rows = fetch_all_activity(addr, max_pages=args.max_pages)
    print(f"Fetched {len(rows)} activity rows.", file=sys.stderr)

    mkts = aggregate(rows)
    print(f"Fetching open positions ...", file=sys.stderr)
    positions = fetch_positions(addr)

    print_report(addr, rows, mkts, positions)


if __name__ == "__main__":
    main()
