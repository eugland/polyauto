#!/usr/bin/env python3
"""
BTC 5m wallet history — per-market breakdown and P/L.

Fetches Polymarket `/activity` for a wallet, filters BTC 5m markets,
and prints, for each of the N most-recent markets:

  * Up shares bought / sold
  * Down shares bought / sold
  * USDC spent (BUY side)
  * USDC payout (SELL + REDEEM)
  * Profit / loss

Plus a grand total across all shown markets.

Usage:
  python -m experiment.btc_5m_wallet_history 0x030C04d83a7C2c31aA0Ea2356BF0a00f9a79537b
  python -m experiment.btc_5m_wallet_history 0x030C04d83a7C2c31aA0Ea2356BF0a00f9a79537b --markets 10
  python -m experiment.btc_5m_wallet_history 0x030C04d83a7C2c31aA0Ea2356BF0a00f9a79537b --max-pages 30
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DATA_API = "https://data-api.polymarket.com/activity"
PAGE_SIZE = 500
BTC_5M_PREFIX = "btc-updown-5m-"


@dataclass
class MarketAgg:
    slug: str
    title: str = ""
    end_epoch: int = 0

    up_bought: float = 0.0
    up_sold: float = 0.0
    down_bought: float = 0.0
    down_sold: float = 0.0

    spent: float = 0.0          # BUY usdc
    sell_proceeds: float = 0.0  # SELL usdc
    redeemed: float = 0.0       # REDEEM usdc

    trades: int = 0
    redeems: int = 0


# ── data fetching ──────────────────────────────────────────────────────

def _fetch_page(addr: str, offset: int) -> list[dict]:
    url = f"{DATA_API}?user={addr}&limit={PAGE_SIZE}&offset={offset}"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read())
    return data if isinstance(data, list) else []


def _slug_epoch(slug: str) -> int:
    try:
        return int(slug.rsplit("-", 1)[-1])
    except Exception:
        return 0


def _apply_activity(m: MarketAgg, a: dict) -> None:
    atype = a.get("type", "")
    try:
        usdc = float(a.get("usdcSize") or 0)
        size = float(a.get("size") or 0)
    except (TypeError, ValueError):
        return

    if atype == "TRADE":
        m.trades += 1
        side = a.get("side", "")
        outcome = a.get("outcome", "")
        if side == "BUY":
            m.spent += usdc
            if outcome == "Up":
                m.up_bought += size
            elif outcome == "Down":
                m.down_bought += size
        elif side == "SELL":
            m.sell_proceeds += usdc
            if outcome == "Up":
                m.up_sold += size
            elif outcome == "Down":
                m.down_sold += size
    elif atype == "REDEEM":
        m.redeems += 1
        m.redeemed += usdc


def fetch_btc5m(addr: str, max_pages: int, want_markets: int) -> dict[str, MarketAgg]:
    """
    Paginate `/activity` newest-first, aggregate per-market BTC 5m state.
    Stops early once we're confident no new activities for the top-N most-recent
    markets will appear (i.e. we've scrolled past their time window entirely).
    """
    markets: dict[str, MarketAgg] = {}

    for page in range(max_pages):
        offset = page * PAGE_SIZE
        try:
            batch = _fetch_page(addr, offset)
        except (HTTPError, URLError) as exc:
            print(f"[warn] page {page} (offset {offset}) failed: {exc}", file=sys.stderr)
            break
        if not batch:
            break

        oldest_ts_in_batch = None
        for a in batch:
            slug = a.get("slug", "")
            if BTC_5M_PREFIX not in slug:
                continue
            m = markets.get(slug)
            if m is None:
                m = MarketAgg(
                    slug=slug,
                    title=a.get("title", ""),
                    end_epoch=_slug_epoch(slug),
                )
                markets[slug] = m
            _apply_activity(m, a)

            ts = a.get("timestamp")
            if isinstance(ts, (int, float)):
                ts = int(ts)
                if oldest_ts_in_batch is None or ts < oldest_ts_in_batch:
                    oldest_ts_in_batch = ts

        print(
            f"[page {page:>3}] offset={offset:>5}  "
            f"fetched={len(batch):>3}  btc5m_markets={len(markets)}"
        )

        # Stop early: if we already have > want_markets and the newest batch is
        # entirely older than the want_markets-th most-recent market's candle
        # start, no new data can affect the top-N.
        if len(markets) >= want_markets and oldest_ts_in_batch is not None:
            top_n = sorted(markets.values(), key=lambda x: x.end_epoch, reverse=True)[:want_markets]
            cutoff = min(m.end_epoch for m in top_n) - 5 * 60  # minus one candle width
            if oldest_ts_in_batch < cutoff:
                print(f"[stop] page batch older than top-{want_markets} cutoff")
                break

        if len(batch) < PAGE_SIZE:
            break

    return markets


# ── reporting ──────────────────────────────────────────────────────────

def _fmt_ts(epoch: int) -> str:
    if not epoch:
        return "n/a"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def print_report(markets: dict[str, MarketAgg], limit: int, addr: str) -> None:
    ordered = sorted(markets.values(), key=lambda m: m.end_epoch, reverse=True)[:limit]
    if not ordered:
        print("No BTC 5m markets found for this wallet.")
        return

    print()
    print("=" * 118)
    print(f"  BTC 5m wallet history       wallet={addr}")
    print(f"  showing {len(ordered)} most-recent markets (of {len(markets)} total found)")
    print("=" * 118)
    header = (
        f"  {'#':>2}  {'Candle end UTC':<16}  {'Epoch':<12}  "
        f"{'Up buy':>8}  {'Up sell':>8}  {'Dn buy':>8}  {'Dn sell':>8}  "
        f"{'Spent $':>10}  {'Payout $':>10}  {'P/L $':>10}"
    )
    print(header)
    print("  " + "-" * 116)

    tot_spent = 0.0
    tot_payout = 0.0
    tot_up_buy = tot_up_sell = tot_dn_buy = tot_dn_sell = 0.0

    for idx, m in enumerate(ordered, start=1):
        payout = m.sell_proceeds + m.redeemed
        pnl = payout - m.spent
        tot_spent += m.spent
        tot_payout += payout
        tot_up_buy += m.up_bought
        tot_up_sell += m.up_sold
        tot_dn_buy += m.down_bought
        tot_dn_sell += m.down_sold
        print(
            f"  {idx:>2}  {_fmt_ts(m.end_epoch):<16}  {str(m.end_epoch):<12}  "
            f"{m.up_bought:>8.2f}  {m.up_sold:>8.2f}  "
            f"{m.down_bought:>8.2f}  {m.down_sold:>8.2f}  "
            f"{m.spent:>10.2f}  {payout:>10.2f}  {pnl:>+10.2f}"
        )

    net = tot_payout - tot_spent
    roi = (net / tot_spent * 100.0) if tot_spent > 0 else 0.0
    print("  " + "-" * 116)
    print(
        f"  {'':>2}  {'TOTALS':<16}  {'':<12}  "
        f"{tot_up_buy:>8.2f}  {tot_up_sell:>8.2f}  "
        f"{tot_dn_buy:>8.2f}  {tot_dn_sell:>8.2f}  "
        f"{tot_spent:>10.2f}  {tot_payout:>10.2f}  {net:>+10.2f}"
    )
    print(f"  {'':>2}  {'':<16}  {'':<12}  "
          f"{'':>8}  {'':>8}  {'':>8}  {'':>8}  "
          f"{'':>10}  {'ROI':>10}  {roi:>+9.2f}%")
    print("=" * 118)
    print()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Polymarket BTC 5m per-market history and P/L for a wallet."
    )
    p.add_argument("address", help="Proxy-wallet address, e.g. 0x030C04...")
    p.add_argument(
        "--markets", type=int, default=10,
        help="How many most-recent BTC 5m markets to show (default: 10)",
    )
    p.add_argument(
        "--max-pages", type=int, default=20,
        help=f"Max activity pages to scan ({PAGE_SIZE}/page, default: 20)",
    )
    args = p.parse_args()

    markets = fetch_btc5m(args.address, max_pages=args.max_pages, want_markets=args.markets)
    print_report(markets, limit=args.markets, addr=args.address)


if __name__ == "__main__":
    main()
