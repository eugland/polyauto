"""
XRP Polymarket 1H - Dry Run

Shows the current live market and the next upcoming market.
Prints all available data.
"""

import json
from .market import (
    current_et, build_slug, build_url, fetch_market, parse_market, ET_OFFSET
)
from datetime import timedelta, timezone


DIVIDER = "=" * 70


def print_raw(event: dict):
    print(f"\n  RAW API OBJECT")
    print(f"  " + "-" * 68)
    print(json.dumps(event, indent=4, default=str))
    print()


def print_market(label: str, info: dict, raw: dict = None):
    print(f"\n{DIVIDER}")
    print(f"  {label}")
    print(DIVIDER)

    print(f"\n  LINK:    {info['url']}")
    print(f"  TITLE:   {info['title']}")
    print(f"  STATUS:  {'CLOSED/RESOLVED' if info['closed'] else 'LIVE / OPEN FOR TRADING'}")
    print(f"  ACTIVE:  {info['active']}")

    print(f"\n  TIMING")
    print(f"    Candle start (ET): {info['candle_start_et']}")
    print(f"    Resolves   (UTC):  {info['resolves_utc']}")
    print(f"    Resolves    (ET):  {info['resolves_et']}")
    print(f"    Time remaining:    {info['time_remaining_fmt']}  ({info['time_remaining_sec']}s)")

    print(f"\n  ODDS")
    for outcome, price in info['prices'].items():
        pct = price * 100
        bar = "#" * int(pct / 2)
        print(f"    {outcome:5s}  {price:.3f}  ({pct:5.1f}%)  {bar}")
    print(f"    Best bid:          {info['best_bid']}")
    print(f"    Best ask:          {info['best_ask']}")
    print(f"    Last trade:        {info['last_trade_price']}")
    print(f"    Spread:            {info['spread']}")
    print(f"    1H price change:   {info['one_hour_price_change']}")
    print(f"    24H price change:  {info['one_day_price_change']}")

    print(f"\n  VOLUME & LIQUIDITY")
    print(f"    Volume:            ${info['volume']:,.2f}")
    print(f"    Volume 24hr:       ${info['volume_24hr']:,.2f}")
    print(f"    Liquidity:         ${info['liquidity']:,.2f}")
    print(f"    Open interest:     ${info['open_interest']:,.2f}")
    print(f"    Competitive score: {info['competitive_score']:.4f}")

    print(f"\n  XRP PRICE REFERENCE  (Binance XRP/USDT 1H candle open — resolves Up if close >= this)")
    print(f"    Price to beat:     ${info['price_to_beat']:,.2f}" if info['price_to_beat'] else "    Price to beat:     N/A (candle not started yet — open price set at candle start)")

    print(f"\n  SERIES (XRP Up or Down Hourly)")
    print(f"    Series vol 24hr:   ${(info['series_volume_24hr'] or 0):,.2f}")
    print(f"    Series liquidity:  ${(info['series_liquidity'] or 0):,.2f}")
    print(f"    Comments (total):  {info['series_comments']}")

    print(f"\n  TECHNICAL / CLOB")
    print(f"    Event ID:          {info['event_id']}")
    print(f"    Market ID:         {info['market_id']}")
    print(f"    Condition ID:      {info['condition_id']}")
    print(f"    Question ID:       {info['question_id']}")
    print(f"    CLOB token UP:     {info['clob_token_up']}")
    print(f"    CLOB token DOWN:   {info['clob_token_down']}")
    print(f"    Min order size:    {info['min_order_size']} shares")
    print(f"    Tick size:         ${info['tick_size']}")
    print(f"    Fee rate:          {info['fee_rate']} ({(info['fee_rate'] or 0)*100:.1f}%) taker-only={info['taker_only']}")
    print(f"    Accepting orders:  {info['accepting_orders']}")
    print(f"    Resolver address:  {info['resolver_address']}")

    if raw is not None:
        print_raw(raw)
    else:
        print()


def dry_run():
    now_et = current_et()
    print(f"\n{'='*70}")
    print(f"  BTC Polymarket 1H - DRY RUN")
    print(f"  Now (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'='*70}")

    # Current candle = the hour we are in right now
    current_hour_et = now_et.replace(minute=0, second=0, microsecond=0)
    next_hour_et = current_hour_et + timedelta(hours=1)

    current_slug = build_slug(current_hour_et)
    next_slug = build_slug(next_hour_et)

    print(f"\n  Current slug: {current_slug}")
    print(f"  Next slug:    {next_slug}")

    print(f"\n  Fetching current market...")
    current_event = fetch_market(current_slug)
    if current_event:
        print_market("CURRENT MARKET (live candle)", parse_market(current_event), raw=current_event)
    else:
        print(f"  [!] Could not fetch current market: {build_url(current_slug)}")

    print(f"\n  Fetching next market...")
    next_event = fetch_market(next_slug)
    if next_event:
        print_market("NEXT MARKET (upcoming candle)", parse_market(next_event), raw=next_event)
    else:
        print(f"  [!] Could not fetch next market: {build_url(next_slug)}")

    print(DIVIDER)
    print("  DRY RUN COMPLETE — no bets placed")
    print(DIVIDER)


if __name__ == "__main__":
    dry_run()
