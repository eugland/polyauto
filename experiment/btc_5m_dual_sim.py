#!/usr/bin/env python3
"""
experiment/btc_5m_dual_sim.py

Dual-side bid simulation for BTC 5-minute Up/Down markets on Polymarket.

STRATEGY
--------
Each candle: place a limit BUY of $49 on Up (at Up bid price)
                               AND $49 on Down (at Down bid price).

Because one side ALWAYS resolves at $1.00:

  If BOTH bids fill:
    net = (49/up_bid + 49/dn_bid) - 98
        = 49 * (1/up_bid + 1/dn_bid - 2)
    PROFITABLE when up_bid + dn_bid < 1.00  (you paid under par for a $1 payout)

  If ONLY the winning side fills (most likely in a fast-moving candle):
    net = 49/winning_bid - 49  (win on one, lose the $49 staked)

  If ONLY the losing side fills:
    net = -$49  (you bought the side that resolved $0)

FILL MECHANICS
--------------
A limit buy at the BID price sits in the queue waiting for a seller.
In a 5-minute candle, as price resolves one direction:
  - The LOSING side's price drops -> sellers rush to exit -> your bid fills
  - The WINNING side's price rises -> no one sells at your old bid -> may NOT fill

So the fill scenario is NOT symmetric: you're more likely to fill on the LOSING side.
This script models this and shows EV across all scenarios.

USAGE
-----
  python experiment/btc_5m_dual_sim.py                          # starts from current candle
  python experiment/btc_5m_dual_sim.py btc-updown-5m-1775809500 # starts from specific slug
  python experiment/btc_5m_dual_sim.py btc-updown-5m-1775809500 20  # 20 candles
"""
from __future__ import annotations

import json
import math
import random
import sys
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import requests

# ── Config ────────────────────────────────────────────────────────────────────

STARTING_BALANCE = 1_000.0
BID_EACH_SIDE    = 49.0        # dollars to bid on each side
CANDLE_SECS      = 300         # 5-minute candles
N_CANDLES        = 10          # default number of candles to simulate

GAMMA_API = "https://gamma-api.polymarket.com/events?slug={slug}"
CLOB_HOST = "https://clob.polymarket.com"
HEADERS   = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

TOTAL_COST = BID_EACH_SIDE * 2   # $98 if both fill

# ── Slug utilities ─────────────────────────────────────────────────────────────

def ts_from_slug(slug: str) -> int:
    return int(slug.rsplit("-", 1)[-1])

def slug_from_ts(ts: int) -> str:
    return f"btc-updown-5m-{ts}"

def current_candle_ts() -> int:
    return (int(time.time()) // CANDLE_SECS) * CANDLE_SECS

# ── Network helpers ────────────────────────────────────────────────────────────

def _get(url: str):
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def fetch_market(slug: str) -> dict | None:
    try:
        data  = _get(GAMMA_API.format(slug=slug))
        event = data[0] if data else None
    except Exception:
        return None

    if not event or not event.get("markets"):
        return None

    m = event["markets"][0]
    if m.get("closed"):
        return None

    def _load(key):
        v = m.get(key, [])
        return json.loads(v) if isinstance(v, str) else v

    outcomes  = _load("outcomes")
    token_ids = _load("clobTokenIds")

    up_token = dn_token = None
    for i, name in enumerate(outcomes):
        nl = name.strip().lower()
        if nl == "up"   and i < len(token_ids): up_token = str(token_ids[i])
        if nl == "down" and i < len(token_ids): dn_token = str(token_ids[i])

    if not up_token or not dn_token:
        return None

    end_str = m.get("endDate") or event.get("endDate") or ""
    end_utc = None
    try:
        dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    price_to_beat = None
    try:
        ptb = (event.get("eventMetadata") or {}).get("priceToBeat")
        if ptb is not None:
            price_to_beat = float(ptb)
    except Exception:
        pass

    return {
        "slug":          slug,
        "up_token":      up_token,
        "dn_token":      dn_token,
        "end_utc":       end_utc,
        "price_to_beat": price_to_beat,
    }


def fetch_books(token_ids: list[str]) -> dict[str, dict]:
    try:
        resp = requests.post(
            f"{CLOB_HOST}/books",
            json=[{"token_id": tid} for tid in token_ids],
            timeout=8,
        )
        resp.raise_for_status()
        result = {}
        for book in resp.json():
            tid  = book.get("asset_id") or book.get("token_id")
            if not tid:
                continue
            bids = sorted(book.get("bids", []), key=lambda x: -float(x["price"]))
            asks = sorted(book.get("asks", []), key=lambda x:  float(x["price"]))

            # Depth: USDC available in top 5 levels on each side
            ask_depth = sum(float(a["price"]) * float(a["size"]) for a in asks[:5])
            bid_depth = sum(float(b["price"]) * float(b["size"]) for b in bids[:5])

            result[str(tid)] = {
                "bid":       float(bids[0]["price"]) if bids else None,
                "ask":       float(asks[0]["price"]) if asks else None,
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
            }
        return result
    except Exception:
        return {}

# ── Fill probability model ─────────────────────────────────────────────────────

def fill_prob_winning_side(ask_depth: float, needed: float) -> float:
    """
    Probability the WINNING side's bid fills.
    Winning side price is RISING — sellers ask more, so they won't hit our bid.
    Only fills if the market churns enough mid-candle before conviction sets in.
    Proxy: lower ask depth = thinner market = less churn = less likely to fill at old bid.
    """
    if ask_depth <= 0:
        return 0.05
    ratio = ask_depth / max(needed, 1.0)
    # Winning side rarely fills once direction is clear — cap at 50%
    return min(0.50, 0.10 + 0.25 * math.log1p(ratio))


def fill_prob_losing_side(ask_depth: float, needed: float) -> float:
    """
    Probability the LOSING side's bid fills.
    Losing side price is FALLING — sellers panic-exit and hit any bid.
    Very likely to fill in an active 5-minute candle.
    """
    if ask_depth <= 0:
        return 0.30
    ratio = ask_depth / max(needed, 1.0)
    # Losing side fills readily as sellers dump — floor at 60%, cap at 95%
    return min(0.95, 0.60 + 0.20 * math.log1p(ratio))

# ── Candle simulation ──────────────────────────────────────────────────────────

def simulate_candle(
    slug: str,
    candle_num: int,
    balance: float,
    rng: random.Random,
) -> dict:
    div  = "=" * 70
    div2 = "-" * 70

    print(f"\n{div}")
    print(f"  CANDLE #{candle_num:02d}  |  {slug}")
    print(f"  Balance entering: ${balance:,.2f}")
    print(div2)

    mkt = fetch_market(slug)
    if not mkt:
        candle_dt = datetime.fromtimestamp(ts_from_slug(slug), tz=timezone.utc)
        print(f"  Market not found (candle at {candle_dt:%Y-%m-%d %H:%M UTC}) — SKIP")
        print(div)
        return {"balance": balance, "pnl": 0.0, "status": "skip", "slug": slug}

    # Time info
    now_utc   = datetime.now(timezone.utc)
    secs_left = (mkt["end_utc"] - now_utc).total_seconds() if mkt["end_utc"] else None
    mins_left = secs_left / 60 if secs_left is not None else None

    books = fetch_books([mkt["up_token"], mkt["dn_token"]])
    ub    = books.get(mkt["up_token"], {})
    db    = books.get(mkt["dn_token"], {})

    up_bid = ub.get("bid");   up_ask = ub.get("ask")
    dn_bid = db.get("bid");   dn_ask = db.get("ask")
    up_ask_depth = ub.get("ask_depth", 0)
    dn_ask_depth = db.get("ask_depth", 0)
    up_bid_depth = ub.get("bid_depth", 0)
    dn_bid_depth = db.get("bid_depth", 0)

    ptb_str  = f"${mkt['price_to_beat']:>12,.2f}" if mkt["price_to_beat"] else "  n/a"
    time_str = f"{mins_left:+.1f} min" if mins_left is not None else "n/a"
    end_str  = mkt["end_utc"].strftime("%H:%M UTC") if mkt["end_utc"] else "n/a"

    print(f"  BTC price to beat: {ptb_str}   |   ends {end_str}  ({time_str})")
    print()
    print(f"  {'':16}  {'Up':>10}   {'Down':>10}")
    print(f"  ask               {up_ask*100 if up_ask else float('nan'):>9.2f}c  {dn_ask*100 if dn_ask else float('nan'):>9.2f}c")
    print(f"  bid               {up_bid*100 if up_bid else float('nan'):>9.2f}c  {dn_bid*100 if dn_bid else float('nan'):>9.2f}c")
    print(f"  spread            {((up_ask-up_bid)*100 if up_ask and up_bid else float('nan')):>9.2f}c  {((dn_ask-dn_bid)*100 if dn_ask and dn_bid else float('nan')):>9.2f}c")
    print(f"  ask-side depth   ${up_ask_depth:>9.2f}   ${dn_ask_depth:>9.2f}")

    if not up_bid or not dn_bid or not up_ask or not dn_ask:
        print(f"\n  Missing book data or candle already resolved -- SKIP")
        print(div)
        return {"balance": balance, "pnl": 0.0, "status": "skip", "slug": slug}

    # Skip resolved candles (one side at 1.00)
    if up_ask >= 0.999 or dn_ask >= 0.999:
        print(f"\n  Candle already resolved (up_ask={up_ask} dn_ask={dn_ask}) -- SKIP")
        print(div)
        return {"balance": balance, "pnl": 0.0, "status": "skip", "slug": slug}

    # ── Core math ─────────────────────────────────────────────────────────────
    bid_sum    = up_bid + dn_bid
    ask_sum    = up_ask + dn_ask
    spread_sum = ask_sum - bid_sum

    # If BOTH bids fill — return from each resolved side
    up_shares  = BID_EACH_SIDE / up_bid   # shares of Up we'd get at Up bid
    dn_shares  = BID_EACH_SIDE / dn_bid   # shares of Down we'd get at Down bid

    # Guaranteed return when BOTH fill (bid_sum < 1.0 = profit, > 1.0 = loss)
    net_if_both_up_wins = up_shares - TOTAL_COST    # Up wins, both filled
    net_if_both_dn_wins = dn_shares - TOTAL_COST    # Down wins, both filled
    net_if_only_win_wins  = up_shares - BID_EACH_SIDE   # only winning side filled, wins
    net_if_only_lose_side = -BID_EACH_SIDE               # only losing side filled

    arb_pct = (1.0 / bid_sum - 1.0) * 100   # guaranteed % if both fill

    print()
    print(f"  Bid sum:  {bid_sum:.4f}  {'[UNDER par -> arb exists]' if bid_sum < 1.0 else '[OVER par  -> no arb]'}")
    print(f"  Ask sum:  {ask_sum:.4f}  (buying at ask always costs {(ask_sum-1)*100:.2f}c over par)")
    print(f"  If BOTH bids fill -> guaranteed return: {arb_pct:+.2f}%  (no matter who wins)")

    # ── Fill probability model ─────────────────────────────────────────────────
    # Implied win probabilities (from mid-price)
    mid_up   = (up_bid + up_ask) / 2
    mid_dn   = (dn_bid + dn_ask) / 2
    prob_up  = mid_up / (mid_up + mid_dn)
    prob_dn  = 1.0 - prob_up

    # Fill probabilities are ASYMMETRIC:
    #   - WINNING side: price rises, sellers want more -> hard to fill at our bid
    #   - LOSING side:  price falls, sellers panic-exit -> easy fill at our bid
    fill_win_up = fill_prob_winning_side(up_ask_depth, BID_EACH_SIDE)   # Up fills when Up is winning
    fill_win_dn = fill_prob_winning_side(dn_ask_depth, BID_EACH_SIDE)   # Down fills when Down is winning
    fill_los_up = fill_prob_losing_side(up_ask_depth, BID_EACH_SIDE)    # Up fills when Up is LOSING
    fill_los_dn = fill_prob_losing_side(dn_ask_depth, BID_EACH_SIDE)    # Down fills when Down is LOSING

    print()
    print(f"  Implied probability:  Up={prob_up*100:.1f}%   Down={prob_dn*100:.1f}%")
    print()
    print(f"  Fill prob model (bid at bid-price, limit order):")
    print(f"    Up fills (Up wins  — price rising): {fill_win_up*100:.0f}%")
    print(f"    Up fills (Up loses — price falling): {fill_los_up*100:.0f}%")
    print(f"    Down fills (Down wins  — rising):   {fill_win_dn*100:.0f}%")
    print(f"    Down fills (Down loses — falling):  {fill_los_dn*100:.0f}%")

    # ── Scenario table ─────────────────────────────────────────────────────────
    # Joint scenarios: (Up outcome, Up fills, Down fills)
    # When Up wins:  Up price rises (hard fill), Down price falls (easy fill)
    # When Dn wins:  Dn price rises (hard fill), Up price falls (easy fill)

    # P(Up wins, Up fills, Down fills) = prob_up * fill_win_up * fill_los_dn
    # etc.

    scenarios = []

    # ── Up wins ──
    p_uw = prob_up
    for up_f, dn_f in [(True, True), (True, False), (False, True), (False, False)]:
        pf_up = fill_win_up if up_f else (1 - fill_win_up)
        pf_dn = fill_los_dn if dn_f else (1 - fill_los_dn)
        prob  = p_uw * pf_up * pf_dn
        spent = (BID_EACH_SIDE if up_f else 0) + (BID_EACH_SIDE if dn_f else 0)
        recv  = (up_shares if up_f else 0)   # Up wins -> Up shares pay, Down shares = 0
        net   = recv - spent
        label = f"Up wins | Up {'Y' if up_f else 'N'} Down {'Y' if dn_f else 'N'}"
        scenarios.append((label, prob, net, up_f, dn_f, True))

    # ── Down wins ──
    p_dw = prob_dn
    for up_f, dn_f in [(True, True), (True, False), (False, True), (False, False)]:
        pf_up = fill_los_up if up_f else (1 - fill_los_up)
        pf_dn = fill_win_dn if dn_f else (1 - fill_win_dn)
        prob  = p_dw * pf_up * pf_dn
        spent = (BID_EACH_SIDE if up_f else 0) + (BID_EACH_SIDE if dn_f else 0)
        recv  = (dn_shares if dn_f else 0)   # Down wins -> Down shares pay, Up shares = 0
        net   = recv - spent
        label = f"Dn wins | Up {'Y' if up_f else 'N'} Down {'Y' if dn_f else 'N'}"
        scenarios.append((label, prob, net, up_f, dn_f, False))

    ev = sum(p * n for _, p, n, *_ in scenarios)

    print()
    print(f"  {'Scenario':<38}  {'Prob':>6}  {'Net P&L':>10}  {'Note'}")
    print(f"  {div2}")
    for label, prob, net, up_f, dn_f, up_wins in sorted(scenarios, key=lambda x: -x[1]):
        note = ""
        if up_f and dn_f:
            note = "<- arb zone" if net > 0 else "<- over-par"
        elif not up_f and not dn_f:
            note = "no fill"
        print(f"  {label:<38}  {prob*100:>5.1f}%  ${net:>+9.2f}  {note}")

    # Summarise both-fill probability
    both_fill_pct = sum(
        p for _, p, _, up_f, dn_f, _ in scenarios if up_f and dn_f
    ) * 100

    print(f"  {div2}")
    print(f"  {'Expected value (per candle)':<38}  {'100%':>6}  ${ev:>+9.2f}")
    print(f"  Both-fill probability: {both_fill_pct:.1f}%")

    # ── Simulate this candle ───────────────────────────────────────────────────
    # Sample outcome
    up_wins_sim = rng.random() < prob_up

    # Fill logic: winning side harder, losing side easier
    if up_wins_sim:
        up_filled = rng.random() < fill_win_up
        dn_filled = rng.random() < fill_los_dn
    else:
        up_filled = rng.random() < fill_los_up
        dn_filled = rng.random() < fill_win_dn

    spent    = (BID_EACH_SIDE if up_filled else 0) + (BID_EACH_SIDE if dn_filled else 0)
    received = ((up_shares if up_wins_sim else 0) if up_filled else 0) + \
               ((dn_shares if not up_wins_sim else 0) if dn_filled else 0)
    actual_pnl  = received - spent
    new_balance = balance + actual_pnl

    fill_str = ("Up+Down" if up_filled and dn_filled
                else "Up only" if up_filled
                else "Down only" if dn_filled
                else "No fill")
    win_str  = "Up wins" if up_wins_sim else "Down wins"

    print()
    print(f"  -- Simulated result {'-'*49}")
    print(f"  Outcome: {win_str:<14}  Fills: {fill_str}")
    print(f"  Spent: ${spent:.2f}   Received: ${received:.2f}   Net: ${actual_pnl:+.2f}")
    print(f"  Balance: ${balance:,.2f}  ->  ${new_balance:,.2f}")
    print(div)

    return {
        "slug":            slug,
        "balance":         new_balance,
        "pnl":             actual_pnl,
        "ev":              ev,
        "bid_sum":         bid_sum,
        "ask_sum":         ask_sum,
        "arb_pct":         arb_pct,
        "both_fill_pct":   both_fill_pct,
        "up_bid":          up_bid,
        "dn_bid":          dn_bid,
        "prob_up":         prob_up,
        "fill_str":        fill_str,
        "win_str":         win_str,
        "status":          "ok",
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    argv      = sys.argv[1:]
    start_slug = argv[0] if argv else slug_from_ts(current_candle_ts())
    n_candles  = int(argv[1]) if len(argv) > 1 else N_CANDLES
    start_ts   = ts_from_slug(start_slug)

    hdr = "#" * 70
    print(f"\n{hdr}")
    print(f"  BTC 5M DUAL-BID SIMULATION")
    print(f"  Starting slug:    {start_slug}")
    print(f"  Candle start:     {datetime.fromtimestamp(start_ts, tz=timezone.utc):%Y-%m-%d %H:%M UTC}")
    print(f"  Starting balance: ${STARTING_BALANCE:,.2f}")
    print(f"  Per candle:       ${BID_EACH_SIDE:.0f} Up  +  ${BID_EACH_SIDE:.0f} Down  =  ${TOTAL_COST:.0f} at risk")
    print(f"  Candles to sim:   {n_candles}")
    print(hdr)

    rng     = random.Random(42)
    balance = STARTING_BALANCE
    results = []

    for i in range(n_candles):
        ts   = start_ts + i * CANDLE_SECS
        slug = slug_from_ts(ts)
        res  = simulate_candle(slug, i + 1, balance, rng)
        balance = res["balance"]
        results.append(res)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{hdr}")
    print(f"  FINAL SUMMARY  ({n_candles} candles × ${TOTAL_COST:.0f} at risk each)")
    print(f"  {'-'*68}")
    print(f"  {'#':>3}  {'Slug':<30}  {'BidSum':>7}  {'EV':>7}  {'Fill':>9}  {'P&L':>8}  {'Balance':>10}")
    print(f"  {'-'*68}")

    for i, r in enumerate(results):
        if r["status"] == "skip":
            print(f"  {i+1:>3}  {r['slug']:<30}  {'SKIP':>7}")
            continue
        print(
            f"  {i+1:>3}  {r['slug']:<30}"
            f"  {r['bid_sum']:.4f}"
            f"  ${r['ev']:>+6.2f}"
            f"  {r['fill_str']:>9}"
            f"  ${r['pnl']:>+7.2f}"
            f"  ${r['balance']:>9,.2f}"
        )

    ok = [r for r in results if r["status"] == "ok"]
    if ok:
        total_pnl      = sum(r["pnl"] for r in ok)
        total_ev       = sum(r["ev"] for r in ok)
        avg_bid_sum    = sum(r["bid_sum"] for r in ok) / len(ok)
        avg_ask_sum    = sum(r["ask_sum"] for r in ok) / len(ok)
        avg_both_fill  = sum(r["both_fill_pct"] for r in ok) / len(ok)
        avg_arb        = sum(r["arb_pct"] for r in ok) / len(ok)
        n_profitable   = sum(1 for r in ok if r["pnl"] > 0)
        n_arb_possible = sum(1 for r in ok if r["bid_sum"] < 1.0)

        print(f"  {'-'*68}")
        print(f"  Final balance:             ${balance:>10,.2f}  (started ${STARTING_BALANCE:,.2f})")
        print(f"  Total P&L (simulated):     ${total_pnl:>+10.2f}")
        print(f"  Total EV (model):          ${total_ev:>+10.2f}")
        print(f"  Return on capital:         {(balance-STARTING_BALANCE)/STARTING_BALANCE*100:>+9.2f}%")
        print()
        print(f"  Avg bid sum:               {avg_bid_sum:>10.4f}  (need <1.0 for arb)")
        print(f"  Avg ask sum:               {avg_ask_sum:>10.4f}  (always >1.0)")
        print(f"  Avg arb return if both fill:{avg_arb:>+9.2f}%")
        print(f"  Avg both-fill probability: {avg_both_fill:>9.1f}%")
        print(f"  Candles with arb (bid<1.0):{n_arb_possible:>3}/{len(ok)}")
        print(f"  Profitable candles:        {n_profitable:>3}/{len(ok)}")

    print(f"\n{hdr}\n")


if __name__ == "__main__":
    main()
