"""
Weather temperature-market analyzer.

Ranks all active "highest-temperature" Polymarket markets by 3 PM local
decide time (soonest first), shows bid/ask/volume with an ASCII depth ladder,
computes P(high >= threshold) from an Open-Meteo ensemble, and optionally
places a NO bet on the least-likely threshold per market.

Usage:
  python -m weather                               # daemon dry-run, interval 60s
  python -m weather --once                        # single dry-run cycle, then exit
  python -m weather --city "Los Angeles"          # filter to one city
  python -m weather --top 5 --no-graph            # compact
  python -m weather --bet --shares 5 --max-balance 10
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from automata.client import build_client, get_best_books_bulk, get_usdc_balance
from automata.db import init_db
from automata.weather_bot import CITY_TZ
from weather.bet import pick_best_executable, place_bet, should_skip_city, suggested_bid
from weather.forecast import attach_probabilities
from weather.markets import Bracket, RankedMarket, fetch_ranked_markets
from weather.snapshots import record_snapshots

SIGNAL_LOG_PATH = Path("experiment") / "logs" / "weather_signals.log"


def _derive_creds_from_env() -> None:
    from automata.client import derive_api_credentials
    creds = derive_api_credentials(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def _attach_prices(ranked: list[RankedMarket], host: str) -> None:
    token_ids: list[str] = []
    for rm in ranked:
        for br in rm.brackets:
            token_ids.append(br.no_token_id)
            if br.yes_token_id:
                token_ids.append(br.yes_token_id)
    if not token_ids:
        return
    books = get_best_books_bulk(host, token_ids)
    for rm in ranked:
        for br in rm.brackets:
            book = books.get(br.no_token_id, {}) or {}
            br.bid = book.get("bid")
            br.ask = book.get("ask")
            if br.yes_token_id:
                ybook = books.get(br.yes_token_id, {}) or {}
                br.yes_bid = ybook.get("bid")
                br.yes_ask = ybook.get("ask")


def _fmt_money(v: float | None) -> str:
    if v is None:
        return "n/a"
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.2f}k"
    return f"{v:.2f}"


def _format_decide(rm: RankedMarket) -> str:
    if rm.decide_utc is None:
        return "decide=?"
    tz_name = CITY_TZ.get(rm.city, "UTC")
    from zoneinfo import ZoneInfo
    local = rm.decide_utc.astimezone(ZoneInfo(tz_name))
    mins = rm.minutes_until_decide or 0
    rel = (
        f"{mins/60:+.1f}h" if abs(mins) >= 60
        else f"{mins:+.0f}m"
    )
    return f"decide={local.strftime('%Y-%m-%d %H:%M %Z')} ({rel})"


def _print_market_block(rm: RankedMarket, picked_no_id: str | None) -> None:
    fmean = rm.brackets[0].forecast_mean if rm.brackets else None
    fstd = rm.brackets[0].forecast_std if rm.brackets else None
    forecast_str = (
        f"forecast={fmean:.1f}°{rm.unit} ±{fstd:.1f}"
        if fmean is not None and fstd is not None else "forecast=n/a"
    )

    # Model prediction = bracket with the highest YES bid (market-implied mode).
    with_yes_bid = [b for b in rm.brackets if b.yes_bid is not None]
    pred_bracket = max(with_yes_bid, key=lambda b: b.yes_bid) if with_yes_bid else None
    pred_unit = pred_bracket.unit if pred_bracket else rm.unit
    pred_str = (
        f"model={pred_bracket.threshold:g}°{pred_unit} (YES bid {pred_bracket.yes_bid*100:.2f}¢)"
        if pred_bracket else "model=n/a"
    )

    event_vol = sum(b.volume or 0.0 for b in rm.brackets)
    event_liq = sum(b.liquidity or 0.0 for b in rm.brackets)
    liq_str = f"vol=${_fmt_money(event_vol)}  liq=${_fmt_money(event_liq)}"

    print()
    print(
        f"━━━ {rm.city}  {rm.title_date}  [{rm.icao or '?'}]  {forecast_str}  "
        f"{_format_decide(rm)}  {pred_str}  {liq_str}"
    )

    print(
        f"    {'bracket':<30}  {'NO ask':>8}  {'NO bid':>8}  {'YES ask':>8}  {'YES bid':>8}  "
        f"{'P(yes)':>8}  {'P(no)':>8}  {'Δ°':>6}  {'$vol':>9}  {'$liq':>9}  {'my bid':>8}  pick"
    )
    print(
        f"    {'-'*30}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*6}  "
        f"{'-'*9}  {'-'*9}  {'-'*8}  {'-'*4}"
    )

    # Order brackets low→high threshold for readability.
    # Skip rows with no NO ask — they're not bettable on the NO side.
    brackets = sorted([b for b in rm.brackets if b.ask is not None], key=lambda b: b.threshold)
    for br in brackets:
        ask = f"{br.ask*100:6.2f}¢" if br.ask is not None else "   n/a"
        bid = f"{br.bid*100:6.2f}¢" if br.bid is not None else "   n/a"
        yask = f"{br.yes_ask*100:6.2f}¢" if br.yes_ask is not None else "   n/a"
        ybid = f"{br.yes_bid*100:6.2f}¢" if br.yes_bid is not None else "   n/a"
        py = f"{br.p_yes*100:6.2f}%" if br.p_yes is not None else "    n/a"
        pn = f"{(1-br.p_yes)*100:6.2f}%" if br.p_yes is not None else "    n/a"
        if pred_bracket is not None:
            delta = br.threshold - pred_bracket.threshold
            dstr = f"{delta:+g}°"
        else:
            dstr = "   n/a"
        mark = "  *" if picked_no_id and br.no_token_id == picked_no_id else ""
        vstr = f"${_fmt_money(br.volume)}" if br.volume is not None else "      n/a"
        lstr = f"${_fmt_money(br.liquidity)}" if br.liquidity is not None else "      n/a"
        sb = suggested_bid(br)
        mb = f"{sb*100:6.2f}¢" if sb is not None else "   n/a"
        print(
            f"    {br.question[:30]:<30}  {ask:>8}  {bid:>8}  {yask:>8}  {ybid:>8}  "
            f"{py:>8}  {pn:>8}  {dstr:>6}  {vstr:>9}  {lstr:>9}  {mb:>8}  {mark}"
        )


def _run_cycle(args: argparse.Namespace, log: logging.Logger) -> int:
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    min_no_price = float(os.getenv("MIN_NO_PRICE", "0.97"))
    max_no_price = float(os.getenv("MAX_NO_PRICE", "0.998"))
    shares = args.shares if args.shares is not None else float(os.getenv("BET_SIZE_SHARES", "20.0"))

    log.info("Fetching temperature markets...")
    ranked = fetch_ranked_markets()
    log.info("  %d events with parseable brackets", len(ranked))

    if args.city:
        ranked = [rm for rm in ranked if rm.city.lower() == args.city.lower()]
        log.info("  %d after --city filter", len(ranked))
    if args.top:
        ranked = ranked[: args.top]

    if not ranked:
        log.warning("No markets to display.")
        return 0

    log.info("Fetching live order books (bulk)...")
    _attach_prices(ranked, host)

    log.info("Fetching ensemble forecasts...")
    attach_probabilities(ranked)

    inserted = record_snapshots(ranked)
    log.info("Snapshot: %d new bracket rows recorded for this hour", inserted)

    # Pick one executable positive-EV bracket per city/date.
    picks: dict[str, tuple[RankedMarket, Bracket, float]] = {}
    for rm in ranked:
        if should_skip_city(rm.city):
            continue
        picked = pick_best_executable(rm, min_no_price=min_no_price, max_no_price=max_no_price)
        if picked is None:
            continue
        br, ev = picked
        key = rm.city + "|" + rm.event_date
        prev = picks.get(key)
        if prev is None or ev > prev[2]:
            picks[key] = (rm, br, ev)

    signal_log = logging.getLogger("weather.signal")
    for key in sorted(picks):
        rm, br, ev = picks[key]
        entry = suggested_bid(br)
        if entry is None:
            continue
        signal_log.info(
            "CANDIDATE city=%s date=%s question=%s ev=%.4f ask=%.4f bid=%.4f entry=%.4f",
            rm.city, rm.event_date, br.question, ev, br.ask or 0.0, br.bid or 0.0, entry,
        )

    for rm in ranked:
        key = rm.city + "|" + rm.event_date
        picked = picks.get(key)
        picked_id = picked[1].no_token_id if picked and picked[0] is rm else None
        _print_market_block(rm, picked_id)

    if not args.bet:
        print()
        log.info("[DRY RUN] %d pick(s) total. Pass --bet to place orders.", len(picks))
        return 0

    # ── Live betting ──
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        log.error("Missing env vars for --bet: %s", ", ".join(missing))
        return 2

    if not all(os.getenv(k) for k in ("CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS")):
        _derive_creds_from_env()

    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    balance = get_usdc_balance(client)
    if args.max_balance is not None:
        balance = min(balance, args.max_balance)
    if balance <= 0:
        log.warning("Account balance is $%.2f, skipping betting this round.", balance)
        return 0
    log.info("Spend budget: $%.2f", balance)

    placed = 0
    placed_city_dates: set[str] = set()
    for rm in ranked:
        key = rm.city + "|" + rm.event_date
        if key in placed_city_dates:
            continue
        picked = picks.get(key)
        if picked is None or picked[0] is not rm:
            continue
        br = picked[1]
        entry = suggested_bid(br)
        if entry is None:
            continue
        cost = shares * entry
        if cost > balance:
            log.warning("Budget exhausted before %s (need $%.2f, have $%.2f) — stopping", rm.city, cost, balance)
            signal_log.info(
                "SKIP_BUDGET city=%s date=%s question=%s need=%.2f have=%.2f",
                rm.city, rm.event_date, br.question, cost, balance,
            )
            break
        place_bet(client, rm, br, shares, dry_run=False)
        balance -= cost
        placed += 1
        placed_city_dates.add(key)
    log.info("Placed %d order(s). Remaining budget $%.2f", placed, balance)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket weather market analyzer")
    parser.add_argument("--bet", action="store_true", help="Place real NO orders (default: dry-run)")
    parser.add_argument("--city", default=None, help="Filter to a single city")
    parser.add_argument("--top", type=int, default=50, help="Max markets to show (default 50)")
    parser.add_argument("--shares", type=float, default=None, help="Shares per bet (overrides BET_SIZE_SHARES)")
    parser.add_argument("--max-balance", type=float, default=None, help="USDC spend cap for this run")
    parser.add_argument("--daemon", action="store_true", default=True, help="Run continuously (default)")
    parser.add_argument("--once", action="store_true", help="Run a single scan/bet cycle, then exit")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between daemon cycles (default 60)")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("weather")
    SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    signal_log = logging.getLogger("weather.signal")
    signal_log.handlers.clear()
    signal_log.setLevel(logging.INFO)
    signal_log.propagate = False
    fh = logging.FileHandler(SIGNAL_LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
    signal_log.addHandler(fh)

    init_db()

    if args.once:
        return _run_cycle(args, log)

    interval = max(1, args.interval)
    iteration = 0
    while True:
        iteration += 1
        log.info("=== Daemon cycle %d ===", iteration)
        try:
            _run_cycle(args, log)
        except Exception as exc:
            log.exception("Cycle failed: %s", exc)
        log.info("Sleeping %ds before next cycle...", interval)
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
