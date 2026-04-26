"""
Auto-pick scoring for aapang-style SPLIT/CONVERT events.

Scores every open multi-bucket weather event on:
  • bucket count        — more buckets = more dust to harvest
  • resolution window   — sweet spot is 18–48h to expiry (median aapang hold = 65h)
  • peak YES bid        — clean favorite (0.40–0.80) preferred over too-tight or too-loose
  • city                — aapang's highest-redemption cities boosted (HK/Seoul/Chicago/etc)
  • liquidity           — sum of live YES bids across buckets (proxy for fade-fillability)
  • not blacklisted     — respects [weather].city_blacklist from config.toml

Standalone usage (logs only, no broadcast):
  python -m automata.picker                   # top 10 candidates with scores + reasons
  python -m automata.picker --top 3           # top 3
  python -m automata.picker --json            # machine-readable
  python -m automata.picker --min-score 50    # only events scoring >= 50

Used by `automata.splitter --action auto` to pick which event to SPLIT.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from automata import config

# Aapang's top 12 cities by REDEEM volume (from experiment/aapang_dump.json analysis).
HIGH_VOL_CITIES = {
    "hong kong", "seoul", "chicago", "tokyo", "austin", "shanghai",
    "denver", "nyc", "new york", "london", "toronto", "beijing",
}


def _setup_logging() -> logging.Logger:
    """Set up console logging + share with the stock UI's automata.log."""
    import os
    from pathlib import Path
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler()],
        )
    shared = Path(__file__).resolve().parent.parent / "logs" / "automata.log"
    shared.parent.mkdir(exist_ok=True)
    if not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(shared)
        for h in root.handlers
    ):
        fh = logging.FileHandler(shared, encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        fh.setLevel(logging.INFO)
        root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.picker")


def _score_event(ev: dict[str, Any], blacklist: set[str]) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    city_lower = (ev.get("city") or "").lower()

    # Hard veto: blacklisted city
    if city_lower in blacklist:
        return -1000.0, [f"VETO city={ev['city']} (blacklist)"]

    # Bucket count
    n = ev.get("n_buckets", 0)
    if n >= 8:
        b = 30
    elif n >= 6:
        b = 20
    elif n >= 4:
        b = 5
    else:
        b = -30
    score += b
    reasons.append(f"buckets={n} {b:+d}")

    # Resolution window (hours)
    hrs = ev.get("hours_to_resolution")
    if hrs is None:
        score -= 30
        reasons.append("no resolution time -30")
    elif hrs < 4:
        score -= 50
        reasons.append(f"hrs={hrs:.1f} -50 (too close)")
    elif hrs <= 8:
        score += 5
        reasons.append(f"hrs={hrs:.1f} +5 (tight)")
    elif hrs <= 18:
        score += 20
        reasons.append(f"hrs={hrs:.1f} +20 (good)")
    elif hrs <= 48:
        score += 30
        reasons.append(f"hrs={hrs:.1f} +30 (sweet)")
    elif hrs <= 72:
        score += 15
        reasons.append(f"hrs={hrs:.1f} +15 (long)")
    else:
        score -= 10
        reasons.append(f"hrs={hrs:.1f} -10 (too far)")

    # Peak YES bid — clean favorite preferred
    peak = ev.get("peak_yes_bid") or 0.0
    if peak >= 0.95:
        score -= 25
        reasons.append(f"peak_yes={peak:.2f} -25 (edge gone)")
    elif peak >= 0.80:
        score += 10
        reasons.append(f"peak_yes={peak:.2f} +10 (tight favorite)")
    elif peak >= 0.40:
        score += 25
        reasons.append(f"peak_yes={peak:.2f} +25 (clean favorite)")
    elif peak >= 0.20:
        score += 5
        reasons.append(f"peak_yes={peak:.2f} +5 (weak favorite)")
    else:
        score -= 15
        reasons.append(f"peak_yes={peak:.2f} -15 (no clear favorite)")

    # Liquidity proxy: sum of YES bids across buckets
    liq = ev.get("liquidity_yes_bid_sum") or 0.0
    if liq >= 1.0:
        score += 15
        reasons.append(f"liq={liq:.2f} +15")
    elif liq >= 0.3:
        score += 5
        reasons.append(f"liq={liq:.2f} +5")
    else:
        score -= 10
        reasons.append(f"liq={liq:.2f} -10 (illiquid)")

    # City boost
    if city_lower in HIGH_VOL_CITIES:
        score += 15
        reasons.append(f"city={ev['city']} +15 (aapang-vol)")

    return score, reasons


def get_held_event_slugs(raw_markets: list[dict] | None = None) -> set[str]:
    """
    Return the set of event_slugs where we currently hold any YES or NO token.
    Driven entirely by on-chain positions + Gamma — no separate state DB needed.

    If `raw_markets` is provided, uses it (avoids a duplicate Gamma fetch when
    the caller already has the payload).
    """
    import os
    funder = (os.getenv("POLYMARKET_FUNDER") or "").strip()
    if not funder:
        return set()

    from automata.client import get_positions
    held = get_positions(funder)
    held_tokens = {p["token_id"] for p in held if p.get("size", 0) > 0.01}
    if not held_tokens:
        return set()

    from automata.parser import _extract_yes_token_id, _extract_no_token_id
    if raw_markets is None:
        from automata.polymarket import fetch_temperature_markets_payload
        raw_markets = fetch_temperature_markets_payload()["markets"]

    held_slugs: set[str] = set()
    for raw in raw_markets:
        yes_tok = _extract_yes_token_id(raw)
        no_tok = _extract_no_token_id(raw)
        if (yes_tok and yes_tok in held_tokens) or (no_tok and no_tok in held_tokens):
            held_slugs.add(str(raw.get("event_slug") or ""))
    return held_slugs


def list_scored_events(exclude_held: bool = True) -> list[dict[str, Any]]:
    """
    Build the candidate event list with everything needed for scoring + downstream
    SPLIT/CONVERT (negRiskMarketID, on-chain question_indices, conditionIds).
    Returns events sorted by score desc.

    If `exclude_held=True` (default), events where we already hold any YES/NO
    are dropped — prevents the picker from re-picking the same event between
    auto-cycles. State of "what's already split" comes from on-chain positions.
    """
    log = logging.getLogger("automata.picker")

    # Late imports to avoid web3/CLOB import cost when this module is just queried.
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.parser import _parse_threshold, _extract_yes_token_id, _extract_no_token_id
    from automata.client import get_best_books_bulk
    from automata.weather_bot import _compute_resolution_dt, _extract_city, _extract_title_date
    from automata.splitter import resolve_question_indices

    blacklist = {c.strip().lower() for c in config.get_list_str(
        "CITY_BLACKLIST", "weather", "city_blacklist", []
    )}

    log.info("Fetching open weather events ...")
    payload = fetch_temperature_markets_payload()
    raw_markets = payload["markets"]
    log.info("  %d raw markets", len(raw_markets))

    held_slugs: set[str] = set()
    if exclude_held:
        held_slugs = get_held_event_slugs(raw_markets)
        if held_slugs:
            log.info("  %d events excluded (already holding positions): %s",
                     len(held_slugs), ", ".join(sorted(held_slugs))[:200])

    # Group by event
    events: dict[str, dict[str, Any]] = {}
    for raw in raw_markets:
        if raw.get("closed") or (raw.get("active") is not None and not raw.get("active")):
            continue
        slug = str(raw.get("event_slug") or "")
        if exclude_held and slug in held_slugs:
            continue
        title = str(raw.get("event_title") or slug)
        if slug not in events:
            events[slug] = {
                "slug": slug,
                "title": title,
                "city": _extract_city(title),
                "title_date": _extract_title_date(title),
                "end_date": (raw.get("endDateIso") or raw.get("endDate") or "")[:10],
                "negRiskMarketID": raw.get("negRiskMarketID") or raw.get("neg_risk_market_id"),
                "buckets": [],
            }
        question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
        parsed = _parse_threshold(question)
        if not parsed:
            continue
        threshold, threshold_hi, unit, direction = parsed
        events[slug]["buckets"].append({
            "question": question,
            "threshold": threshold,
            "threshold_hi": threshold_hi,
            "unit": unit,
            "direction": direction,
            "yes_token": _extract_yes_token_id(raw),
            "no_token": _extract_no_token_id(raw),
            "conditionId": raw.get("conditionId"),
        })

    # Drop events without negRiskMarketID or with too few buckets
    eligible = [
        ev for ev in events.values()
        if ev["negRiskMarketID"]
        and len(ev["buckets"]) >= 2
        and all(b["conditionId"] and b["yes_token"] for b in ev["buckets"])
    ]
    log.info("  %d events with valid metadata (negRiskMarketID + conditionIds)", len(eligible))

    # Fetch live YES books for liquidity / peak detection
    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")
    yes_tokens = [b["yes_token"] for ev in eligible for b in ev["buckets"]]
    log.info("  fetching YES books for %d tokens ...", len(yes_tokens))
    books = get_best_books_bulk(host, yes_tokens)

    now = datetime.now(timezone.utc)
    scored: list[dict[str, Any]] = []
    for ev in eligible:
        # Live book stats per bucket
        peak_yes_bid = 0.0
        peak_yes_ask = 0.0
        peak_idx = None
        liq_sum = 0.0
        for i, b in enumerate(ev["buckets"]):
            yb = books.get(b["yes_token"], {})
            bid = yb.get("bid")
            ask = yb.get("ask")
            b["yes_bid"] = bid
            b["yes_ask"] = ask
            score = max(bid or 0, ask or 0)
            if score > max(peak_yes_bid, peak_yes_ask):
                peak_yes_bid = bid or 0
                peak_yes_ask = ask or 0
                peak_idx = i
            if bid is not None:
                liq_sum += bid

        ev["peak_idx"] = peak_idx
        ev["peak_yes_bid"] = peak_yes_bid
        ev["peak_yes_ask"] = peak_yes_ask
        ev["liquidity_yes_bid_sum"] = round(liq_sum, 4)
        ev["n_buckets"] = len(ev["buckets"])

        # Resolution time
        res_dt = _compute_resolution_dt(ev["city"], ev["title_date"], ev["end_date"])
        ev["resolution_dt"] = res_dt
        if res_dt is not None:
            ev["hours_to_resolution"] = round((res_dt - now).total_seconds() / 3600.0, 2)
        else:
            ev["hours_to_resolution"] = None

        score, reasons = _score_event(ev, blacklist)
        ev["score"] = round(score, 1)
        ev["score_reasons"] = reasons
        scored.append(ev)

    scored.sort(key=lambda e: -e["score"])
    return scored


def pick_best_event(min_score: float = 30.0, exclude_held: bool = True) -> dict[str, Any] | None:
    """
    Return the single highest-scoring event, with on-chain question_indices
    probed and the favorite bucket marked. None if no event scores above
    `min_score` (or if probing fails).

    `exclude_held=True` (default) skips events where we already hold positions —
    prevents double-trading the same event when called in a loop.
    """
    log = logging.getLogger("automata.picker")
    scored = list_scored_events(exclude_held=exclude_held)
    if not scored:
        log.warning("No candidate events found")
        return None

    top = scored[0]
    if top["score"] < min_score:
        log.warning(
            "Best event '%s' scored %.1f < min_score %.1f — declining to pick",
            top["title"], top["score"], min_score,
        )
        return None

    # Probe authoritative on-chain question_indices for this event's buckets
    from automata.splitter import resolve_question_indices
    cid_to_idx = resolve_question_indices(
        top["negRiskMarketID"],
        [b["conditionId"] for b in top["buckets"]],
        max_probe=max(32, len(top["buckets"]) * 2),
    )
    for b in top["buckets"]:
        b["question_index"] = cid_to_idx.get(str(b["conditionId"]).lower())

    missing = [b for b in top["buckets"] if b["question_index"] is None]
    if missing:
        log.warning(
            "Could not resolve on-chain question_index for %d/%d buckets — skipping event",
            len(missing), len(top["buckets"]),
        )
        return None

    return top


def _format_event(ev: dict[str, Any]) -> str:
    parts = [
        f"score={ev['score']:>5.1f}",
        f"city={ev.get('city',''):<12}",
        f"date={ev.get('title_date',''):<10}",
        f"buckets={ev.get('n_buckets',0):>2}",
        f"peak_yes_bid={(ev.get('peak_yes_bid') or 0):.2f}",
        f"liq={ev.get('liquidity_yes_bid_sum',0):.2f}",
        f"hrs={ev.get('hours_to_resolution','?')}",
    ]
    return " | ".join(parts)


def main() -> int:
    p = argparse.ArgumentParser(description="Auto-pick scoring for SPLIT/CONVERT events")
    p.add_argument("--top", type=int, default=10, help="show top N events (default 10)")
    p.add_argument("--min-score", type=float, default=30.0, help="filter to score >= N (default 30)")
    p.add_argument("--json", action="store_true", help="machine-readable JSON output")
    p.add_argument("--show-reasons", action="store_true", help="print scoring reasons per event")
    p.add_argument("--include-held", action="store_true",
                   help="include events where we already hold positions (default: excluded)")
    args = p.parse_args()

    log = _setup_logging()

    scored = list_scored_events(exclude_held=not args.include_held)
    visible = [e for e in scored if e["score"] >= args.min_score][: args.top]

    if args.json:
        # Strip non-JSON-safe fields
        out = []
        for ev in visible:
            ev_copy = dict(ev)
            ev_copy.pop("resolution_dt", None)
            for b in ev_copy["buckets"]:
                b.pop("yes_token", None)
                b.pop("no_token", None)
            out.append(ev_copy)
        print(json.dumps(out, indent=2, default=str))
        return 0

    log.info("──────── PICKER — top %d events (score >= %.1f) ────────", len(visible), args.min_score)
    if not visible:
        log.info("No events meet threshold. (%d total scored, best=%.1f)",
                 len(scored), scored[0]["score"] if scored else 0)
        return 0

    for ev in visible:
        log.info("%s | %s", _format_event(ev), ev.get("title", ""))
        if args.show_reasons:
            log.info("    reasons: %s", " | ".join(ev["score_reasons"]))

    log.info("")
    log.info("Best pick: %s (score=%.1f)", visible[0].get("title", ""), visible[0]["score"])
    log.info("  slug:            %s", visible[0]["slug"])
    log.info("  negRiskMarketID: %s", visible[0]["negRiskMarketID"])
    if visible[0]["peak_idx"] is not None:
        peak = visible[0]["buckets"][visible[0]["peak_idx"]]
        log.info("  favorite bucket: %s (YES bid=%.4f, ask=%s)",
                 peak["question"], peak.get("yes_bid") or 0.0,
                 f"{peak.get('yes_ask'):.4f}" if peak.get("yes_ask") else "n/a")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
