"""Build a narrative-style summary of aapang's strategy across recent weather events.

Reads the full activity dump produced by analyze_aapang.py --raw experiment/aapang_dump.json
and renders a clear per-event story:
  SPLIT/CONVERSION → fade losing buckets → directional bets on favoured bucket →
  hold winner till resolution → REDEEM (or sell at ~$0.997).

Usage:
    python -m experiment.aapang_summary [--top N]
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def fmt_dur(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h{m:02d}m"


def parse_threshold_label(slug: str) -> str:
    s = slug or ""
    m = re.search(r"-(\d+(?:\.\d+)?)corbelow$", s)
    if m:
        return f"<={m.group(1)}C"
    m = re.search(r"-(\d+(?:\.\d+)?)corhigher$", s)
    if m:
        return f">={m.group(1)}C"
    m = re.search(r"-above-(\d+(?:\.\d+)?)c$", s)
    if m:
        return f">{m.group(1)}C"
    m = re.search(r"-(\d+(?:\.\d+)?)c$", s)
    if m:
        return f"={m.group(1)}C"
    return "?"


def event_key(slug: str) -> str:
    s = slug or ""
    s = re.sub(r"-(\d+(?:\.\d+)?)c(?:orbelow|orhigher)?$", "", s)
    s = re.sub(r"-above-(\d+(?:\.\d+)?)c$", "", s)
    return s


def render_event(slug: str, acts: list[dict]) -> str:
    out: list[str] = []
    acts = sorted(acts, key=lambda a: a["timestamp"])
    t0 = acts[0]["timestamp"]
    t1 = acts[-1]["timestamp"]

    # Header
    m = re.match(r"(highest|lowest)-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$", slug)
    if m:
        kind, city, mo, day, yr = m.groups()
        header = f"{kind.title()} temperature in {city.replace('-', ' ').title()} on {mo.title()} {int(day)}, {yr}"
    else:
        header = slug

    # Aggregates
    by_type: dict[str, int] = defaultdict(int)
    usdc_in_split = 0.0
    usdc_in_conv = 0.0
    redeem_cash = 0.0
    for a in acts:
        t = a.get("type", "")
        by_type[t] += 1
        if t == "SPLIT":
            usdc_in_split += float(a.get("usdcSize") or 0)
        elif t == "CONVERSION":
            usdc_in_conv += float(a.get("usdcSize") or 0)
        elif t == "REDEEM":
            redeem_cash += float(a.get("usdcSize") or 0)

    trades = [a for a in acts if a.get("type") == "TRADE"]
    buys = [t for t in trades if t.get("side") == "BUY"]
    sells = [t for t in trades if t.get("side") == "SELL"]
    big_sells = [t for t in trades if t.get("side") == "SELL" and float(t.get("price", 0)) > 0.5]

    out.append("=" * 100)
    out.append(header)
    out.append(f"  span: {fmt_ts(t0)} → {fmt_ts(t1)}  ({fmt_dur(t1 - t0)})")
    out.append(
        f"  events: {by_type.get('SPLIT', 0)} SPLIT  {by_type.get('CONVERSION', 0)} CONVERSION  "
        f"{by_type.get('TRADE', 0)} TRADE ({len(buys)} buy / {len(sells)} sell)  "
        f"{by_type.get('REDEEM', 0)} REDEEM"
    )
    out.append(
        f"  cash flow:  split-in ${usdc_in_split:>9,.0f}  conv-in ${usdc_in_conv:>9,.0f}  "
        f"redeem ${redeem_cash:>9,.0f}"
    )

    # Directional buys (price > 0.10) — these are the conviction bets
    directional = [t for t in buys if float(t.get("price", 0)) > 0.10]
    if directional:
        out.append("")
        out.append("  Directional BUYS (price > 0.10) — conviction temperature bets:")
        for t in directional:
            lbl = parse_threshold_label(t.get("slug") or "")
            out.append(
                f"    {fmt_ts(t['timestamp'])}  BUY YES {lbl:>8}  "
                f"px={float(t['price']):.4f}  size={float(t['size']):>8.1f}  "
                f"$={float(t['size'])*float(t['price']):>7.2f}"
            )

    # Big sells (price > 0.5) — these are the resolution-time exits
    if big_sells:
        out.append("")
        out.append("  Resolution exits (SELL at price > 0.5) — winning bucket sold near $1:")
        for t in big_sells:
            lbl = parse_threshold_label(t.get("slug") or "")
            out.append(
                f"    {fmt_ts(t['timestamp'])}  SELL YES {lbl:>8}  "
                f"px={float(t['price']):.4f}  size={float(t['size']):>10.2f}  "
                f"$={float(t['size'])*float(t['price']):>10.2f}"
            )

    # Range of buckets touched
    buckets = sorted({parse_threshold_label(a.get("slug") or "") for a in acts if a.get("slug")})
    out.append("")
    out.append(f"  Buckets touched: {' '.join(buckets)}")

    # SPLIT/CONVERSION moments
    splits = [a for a in acts if a.get("type") in ("SPLIT", "CONVERSION")]
    if splits:
        out.append("")
        out.append("  SPLIT / CONVERSION events (capital injection):")
        for a in splits:
            lbl = parse_threshold_label(a.get("slug") or "")
            kind = a["type"]
            out.append(
                f"    {fmt_ts(a['timestamp'])}  {kind:>10}  "
                f"size={float(a.get('size') or 0):>10.0f} shares    ${float(a.get('usdcSize') or 0):>9,.0f}"
                + (f"  on {lbl}" if kind == "SPLIT" and lbl != "?" else "")
            )

    # REDEEMs
    redeems = [a for a in acts if a.get("type") == "REDEEM"]
    if redeems:
        out.append("")
        out.append("  REDEEM events (winning shares cashed at $1):")
        for a in redeems:
            lbl = parse_threshold_label(a.get("slug") or "")
            out.append(
                f"    {fmt_ts(a['timestamp'])}  REDEEM   "
                f"size={float(a.get('size') or 0):>10.0f}  ${float(a.get('usdcSize') or 0):>9,.0f}  "
                f"on {lbl}"
            )

    # Overall fade pattern: sells at < 0.05 grouped by bucket
    fade = [t for t in sells if float(t.get("price", 0)) < 0.05]
    if fade:
        by_bucket: dict[str, list[dict]] = defaultdict(list)
        for t in fade:
            by_bucket[parse_threshold_label(t.get("slug") or "")].append(t)
        out.append("")
        out.append("  Long-tail FADE sells (px < 0.05) — recovering dust from losing buckets:")
        for lbl, ts in sorted(by_bucket.items()):
            shares = sum(float(t["size"]) for t in ts)
            cash = sum(float(t["size"]) * float(t["price"]) for t in ts)
            avg_px = cash / shares if shares else 0.0
            out.append(
                f"    bucket {lbl:>8}: {len(ts):>3} sells, total {shares:>9.0f} shares "
                f"@ avg ${avg_px:.4f} → ${cash:>7.2f}"
            )

    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10, help="number of most recent events to render")
    ap.add_argument("--dump", default="experiment/aapang_dump.json")
    ap.add_argument("--event")
    args = ap.parse_args()

    data = json.load(open(args.dump))
    activity = data["activity"]

    grouped: dict[str, list[dict]] = defaultdict(list)
    for a in activity:
        es = a.get("eventSlug") or ""
        if "temperature" not in es.lower():
            continue
        grouped[es].append(a)

    if args.event:
        if args.event in grouped:
            print(render_event(args.event, grouped[args.event]))
        else:
            print(f"No activity found for {args.event}")
        return

    events_sorted = sorted(grouped.items(), key=lambda kv: -max(a["timestamp"] for a in kv[1]))[: args.top]

    # Top-level summary across all temperature events
    all_temp_acts = [a for a in activity if "temperature" in (a.get("eventSlug") or "").lower()]
    splits_total = sum(float(a.get("usdcSize") or 0) for a in all_temp_acts if a.get("type") == "SPLIT")
    conv_total = sum(float(a.get("usdcSize") or 0) for a in all_temp_acts if a.get("type") == "CONVERSION")
    redeem_total = sum(float(a.get("usdcSize") or 0) for a in all_temp_acts if a.get("type") == "REDEEM")
    n_events = len(grouped)

    print(f"AAPANG STRATEGY SUMMARY  ({n_events} weather events touched)")
    print("=" * 100)
    print(f"  Total USDC SPLIT in:        ${splits_total:>12,.0f}")
    print(f"  Total USDC CONVERSION in:   ${conv_total:>12,.0f}")
    print(f"  Total USDC REDEEM out:      ${redeem_total:>12,.0f}")
    print()
    print("  Strategy in one sentence:")
    print("    SPLIT/CONVERSION large USDC into balanced bucket-sets → fade losing buckets at $0.001-0.005")
    print("    → place small directional buys on the favoured bucket → hold winner to resolution and REDEEM at $1.")
    print()

    for slug, acts in events_sorted:
        print(render_event(slug, acts))
        print()


if __name__ == "__main__":
    main()
