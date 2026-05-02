"""Analyze aapang's shedding sequence within an event.

Question: as the resolution time (afternoon for daily high) approaches, does aapang
sell off the LOW-temperature (clearly-out) buckets first and hold the HIGH ones longer?
Or are sells evenly distributed across buckets vs time?

Approach: for each event with >= 5 fade sells, plot:
  - bucket numeric value (e.g. 18, 19, ..., 28)
  - timestamp of each fade sell
  - winning bucket (inferred from BUYs above $0.10 + REDEEM/big-sell)
Then compute: for each sell, distance-from-winner (signed) vs time-since-first-action.

Usage:
    python -m experiment.aapang_shed_timing --top 6
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%m-%d %H:%M")


def bucket_numeric(label: str) -> float | None:
    """Return numeric °C for a bucket label like '=23C', '<=18C', '>=28C'."""
    m = re.match(r"^[<>]?=?(-?\d+(?:\.\d+)?)C$", label)
    if m:
        return float(m.group(1))
    return None


def parse_threshold_label(slug: str) -> str:
    s = slug or ""
    m = re.search(r"-(\d+(?:\.\d+)?)corbelow$", s)
    if m: return f"<={m.group(1)}C"
    m = re.search(r"-(\d+(?:\.\d+)?)corhigher$", s)
    if m: return f">={m.group(1)}C"
    m = re.search(r"-above-(\d+(?:\.\d+)?)c$", s)
    if m: return f">{m.group(1)}C"
    m = re.search(r"-(\d+(?:\.\d+)?)c$", s)
    if m: return f"={m.group(1)}C"
    return "?"


def infer_winner_bucket(acts: list[dict]) -> str | None:
    """Find the resolved-winning bucket: bucket with REDEEM, or with SELL @ price > 0.5."""
    for a in acts:
        if a.get("type") == "REDEEM":
            return parse_threshold_label(a.get("slug") or "")
    big_sells = [a for a in acts if a.get("type") == "TRADE" and a.get("side") == "SELL" and float(a.get("price", 0)) > 0.5]
    if big_sells:
        return parse_threshold_label(big_sells[-1].get("slug") or "")
    return None


def event_label(slug: str) -> str:
    m = re.match(r"(highest|lowest)-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$", slug)
    if not m:
        return slug
    kind, city, mo, day, yr = m.groups()
    return f"{kind.title()} {city.replace('-', ' ').title()} {mo.title()} {int(day)}"


def analyze_event(slug: str, acts: list[dict]) -> str:
    acts = sorted(acts, key=lambda a: a["timestamp"])
    label = event_label(slug)
    winner = infer_winner_bucket(acts)
    winner_n = bucket_numeric(winner) if winner else None

    sells = [a for a in acts if a.get("type") == "TRADE" and a.get("side") == "SELL"]
    fade = [s for s in sells if float(s.get("price", 0)) < 0.05]
    if len(fade) < 3:
        return ""

    t_first = acts[0]["timestamp"]
    t_last = acts[-1]["timestamp"]
    span = t_last - t_first

    # Per-bucket fade chronology: median sell time, count, total shares, avg price
    by_bucket: dict[str, list[dict]] = defaultdict(list)
    for s in fade:
        by_bucket[parse_threshold_label(s.get("slug") or "")].append(s)

    rows = []
    for lbl, ss in by_bucket.items():
        n = bucket_numeric(lbl)
        median_ts = statistics.median(s["timestamp"] for s in ss)
        first_ts = min(s["timestamp"] for s in ss)
        last_ts = max(s["timestamp"] for s in ss)
        total_shares = sum(float(s["size"]) for s in ss)
        avg_px = sum(float(s["size"]) * float(s["price"]) for s in ss) / total_shares
        # distance from winner (signed: negative = below winner, positive = above)
        if winner_n is not None and n is not None:
            dist = n - winner_n
        else:
            dist = None
        # fraction of event-span where median sell occurred (0=start, 1=end)
        frac = (median_ts - t_first) / span if span > 0 else 0.0
        rows.append({
            "label": lbl,
            "n_celsius": n,
            "dist": dist,
            "n_sells": len(ss),
            "shares": total_shares,
            "avg_px": avg_px,
            "first_ts": first_ts,
            "median_ts": median_ts,
            "last_ts": last_ts,
            "median_frac": frac,
        })

    # Sort by bucket numeric (low → high). Buckets with no numeric (just "?") go last.
    rows.sort(key=lambda r: (r["n_celsius"] if r["n_celsius"] is not None else 9999))

    out: list[str] = []
    out.append("=" * 100)
    out.append(f"{label}    winner={winner}    span={int(span/3600)}h  fade_sells={len(fade)}")
    out.append(f"  event start: {fmt_ts(t_first)}    end: {fmt_ts(t_last)}")
    out.append("")
    out.append(f"  {'bucket':>8} {'°C':>5} {'dist':>5} {'#sells':>6} {'shares':>8} "
               f"{'avg_px':>7} {'first_sell':>13} {'median_sell':>13} {'last_sell':>13} {'med_frac':>9}")
    for r in rows:
        dist_s = f"{r['dist']:+.0f}" if r["dist"] is not None else "  -"
        n_s = f"{r['n_celsius']:.0f}" if r["n_celsius"] is not None else " -"
        out.append(
            f"  {r['label']:>8} {n_s:>5} {dist_s:>5} {r['n_sells']:>6} {r['shares']:>8.0f} "
            f"${r['avg_px']:>5.4f} {fmt_ts(r['first_ts']):>13} {fmt_ts(r['median_ts']):>13} "
            f"{fmt_ts(r['last_ts']):>13} {r['median_frac']:>9.2f}"
        )

    # Correlation: does |dist| correlate with median_frac? (i.e. far-from-winner sold earlier?)
    pts = [(abs(r["dist"]), r["median_frac"]) for r in rows if r["dist"] is not None]
    if len(pts) >= 4:
        # spearman-ish: rank both
        xs = sorted(set(p[0] for p in pts))
        ys = sorted(set(p[1] for p in pts))
        # simple pearson on raw values
        n = len(pts)
        mx = sum(p[0] for p in pts) / n
        my = sum(p[1] for p in pts) / n
        num = sum((p[0] - mx) * (p[1] - my) for p in pts)
        dx = (sum((p[0] - mx) ** 2 for p in pts)) ** 0.5
        dy = (sum((p[1] - my) ** 2 for p in pts)) ** 0.5
        rho = num / (dx * dy) if dx > 0 and dy > 0 else 0.0
        out.append("")
        out.append(f"  pearson corr( |bucket - winner| ,  median_sell_frac ) = {rho:+.3f}")
        out.append(f"    interpretation: negative => farther-from-winner sold EARLIER (fade losers first)")
        out.append(f"                    positive => farther-from-winner sold LATER  (fade losers last)")

        # Also: separate below-winner vs above-winner
        below = [(r["dist"], r["median_frac"]) for r in rows if r["dist"] is not None and r["dist"] < 0]
        above = [(r["dist"], r["median_frac"]) for r in rows if r["dist"] is not None and r["dist"] > 0]
        if below:
            mb = sum(p[1] for p in below) / len(below)
            out.append(f"  median_frac for buckets BELOW winner ({len(below)}): {mb:.2f}  "
                       f"(buckets {[r['label'] for r in rows if r['dist'] is not None and r['dist'] < 0]})")
        if above:
            ma = sum(p[1] for p in above) / len(above)
            out.append(f"  median_frac for buckets ABOVE winner ({len(above)}): {ma:.2f}  "
                       f"(buckets {[r['label'] for r in rows if r['dist'] is not None and r['dist'] > 0]})")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="experiment/aapang_dump.json")
    ap.add_argument("--top", type=int, default=8)
    ap.add_argument("--min-fades", type=int, default=5)
    args = ap.parse_args()

    data = json.load(open(args.dump))
    activity = data["activity"]

    grouped: dict[str, list[dict]] = defaultdict(list)
    for a in activity:
        es = a.get("eventSlug") or ""
        if "temperature" not in es.lower():
            continue
        grouped[es].append(a)

    # Rank events by number of fade sells (more sells → richer story)
    def n_fades(acts: list[dict]) -> int:
        return sum(
            1 for a in acts
            if a.get("type") == "TRADE" and a.get("side") == "SELL" and float(a.get("price", 0)) < 0.05
        )

    events_ranked = sorted(grouped.items(), key=lambda kv: -n_fades(kv[1]))
    events_ranked = [e for e in events_ranked if n_fades(e[1]) >= args.min_fades][: args.top]

    print(f"Analyzing shedding sequence in top {len(events_ranked)} events by fade-sell count.\n")
    print("KEY:")
    print("  dist        = bucket_C - winner_C  (negative = below winner, positive = above)")
    print("  med_frac    = median sell timestamp normalized to event span (0=start, 1=end)")
    print("                LOW med_frac => sold early   HIGH med_frac => sold late")
    print()

    for slug, acts in events_ranked:
        s = analyze_event(slug, acts)
        if s:
            print(s)
            print()


if __name__ == "__main__":
    main()
