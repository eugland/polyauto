"""
Polymarket weather "aapang-style" seller — STAGE 1: filter + log only.

Scans every open temperature event and identifies events that have at least
N qualifying markets. A market qualifies when:

  1. Its NO bid >= MIN_NO_PRICE (default 0.97), AND
  2. Its bucket is >= MIN_GAP positions away from the highest-YES bucket
     in the same event (i.e. it sits well outside the market's "favourite").

The intent (future stages, not yet implemented) is the aapang playbook:
buy NOs of the long-tail buckets at >= 0.97, SPLIT the matching set,
dump the faded YES legs, hold the surviving leg to redemption.

Usage:
  python -m automata.seller
  python -m automata.seller --min-no-price 0.97 --min-qualifying 2 --min-gap 2
  python -m automata.seller --loop --interval 60
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from automata import config

_LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "seller.log"


def _setup_logging() -> logging.Logger:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    fh = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handlers.append(fh)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=handlers,
        )
    else:
        root = logging.getLogger()
        if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(_LOG_FILE) for h in root.handlers):
            root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.seller")


def _format_bucket(c: dict[str, Any]) -> str:
    """Compact bucket label, e.g. '<=18C', '=23C', '>=29C'."""
    direction = c.get("direction") or ""
    thr = c.get("threshold")
    thr_hi = c.get("threshold_hi")
    unit = c.get("unit") or "C"
    if thr is None:
        return c.get("question", "?")
    if direction == "below":
        return f"<={thr:g}{unit}"
    if direction == "above":
        return f">={thr:g}{unit}"
    if direction == "range" and thr_hi is not None:
        return f"{thr:g}-{thr_hi:g}{unit}"
    return f"={thr:g}{unit}"


def scan(
    min_no_price: float = 0.97,
    min_qualifying: int = 2,
    min_gap: int = 2,
) -> list[dict[str, Any]]:
    """
    Return a list of qualifying events. Each entry:
      {
        "event_slug": str,
        "event_title": str,
        "city": str,
        "peak_question": str,
        "peak_yes_price": float,
        "qualifying": [ {question, bucket, no_bid, no_ask, yes_ask, gap, ...}, ... ],
      }
    """
    log = logging.getLogger("automata.seller")

    from automata.client import get_best_books_bulk
    from automata.parser import _extract_no_token_id, _extract_yes_token_id, _parse_threshold
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.weather_bot import _extract_city, _extract_title_date

    host = config.get_str("POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")

    log.info("Fetching Polymarket temperature markets...")
    payload = fetch_temperature_markets_payload()
    raw_markets = payload["markets"]
    log.info("  %d raw markets fetched", len(raw_markets))

    # ── Group by event, parse, gather token_ids ───────────────────────────
    events: dict[str, dict[str, Any]] = {}
    for raw in raw_markets:
        if raw.get("closed") or (raw.get("active") is not None and not raw.get("active")):
            continue
        event_slug = str(raw.get("event_slug") or "")
        event_title = str(raw.get("event_title") or event_slug)
        question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
        parsed = _parse_threshold(question)
        if not parsed:
            continue
        no_token = _extract_no_token_id(raw)
        if not no_token:
            continue
        yes_token = _extract_yes_token_id(raw)
        threshold, threshold_hi, unit, direction = parsed

        ev = events.setdefault(event_slug, {
            "title": event_title,
            "city": _extract_city(event_title),
            "title_date": _extract_title_date(event_title),
            "markets": [],
        })
        ev["markets"].append({
            "question": question,
            "no_token": no_token,
            "yes_token": yes_token,
            "threshold": threshold,
            "threshold_hi": threshold_hi,
            "unit": unit,
            "direction": direction,
        })

    log.info("  %d events grouped", len(events))

    # ── Bulk fetch books for every NO + YES token across all events ───────
    all_token_ids: list[str] = []
    for ev in events.values():
        for m in ev["markets"]:
            all_token_ids.append(m["no_token"])
            if m["yes_token"]:
                all_token_ids.append(m["yes_token"])
    if not all_token_ids:
        log.info("  no token ids to fetch — nothing to do")
        return []

    log.info("  fetching order books for %d tokens...", len(all_token_ids))
    books = get_best_books_bulk(host, all_token_ids)

    # ── Per-event: rank buckets by threshold, find peak-YES idx, apply gap+price filter ──
    qualifying_events: list[dict[str, Any]] = []
    total_candidates = 0
    total_qualifying = 0
    for slug, ev in events.items():
        peers = ev["markets"]
        if len(peers) < 2:
            continue

        peers_sorted = sorted(
            peers,
            key=lambda p: (p["threshold"], p.get("threshold_hi") or p["threshold"]),
        )

        # Attach live book + bucket index
        peak_idx = None
        peak_yes = -1.0
        for i, p in enumerate(peers_sorted):
            no_book = books.get(p["no_token"], {})
            p["no_bid"] = no_book.get("bid")
            p["no_ask"] = no_book.get("ask")
            yes_book = books.get(p["yes_token"], {}) if p["yes_token"] else {}
            p["yes_bid"] = yes_book.get("bid")
            p["yes_ask"] = yes_book.get("ask")
            p["bucket_idx"] = i
            yp = p["yes_ask"]
            if yp is not None and yp > peak_yes:
                peak_yes = yp
                peak_idx = i

        if peak_idx is None:
            continue

        peak = peers_sorted[peak_idx]
        qualifying: list[dict[str, Any]] = []
        for i, p in enumerate(peers_sorted):
            total_candidates += 1
            gap = abs(i - peak_idx)
            no_bid = p["no_bid"]
            if gap < min_gap:
                continue
            if no_bid is None or no_bid < min_no_price:
                continue
            qualifying.append({
                **p,
                "gap": gap,
            })
            total_qualifying += 1

        if len(qualifying) >= min_qualifying:
            qualifying_events.append({
                "event_slug": slug,
                "event_title": ev["title"],
                "city": ev["city"],
                "title_date": ev["title_date"],
                "peak_question": peak["question"],
                "peak_yes_ask": peak_yes,
                "peak_bucket": _format_bucket(peak),
                "n_buckets": len(peers_sorted),
                "qualifying": qualifying,
            })

    log.info(
        "  filter: total_markets=%d  qualifying=%d  events_with_>=%d=%d (of %d)",
        total_candidates, total_qualifying, min_qualifying,
        len(qualifying_events), len(events),
    )

    # ── Pretty log per qualifying event ───────────────────────────────────
    for qe in qualifying_events:
        log.info(
            "[QUALIFY] %s — peak %s YES_ask=%.1f¢ — %d/%d buckets qualify",
            qe["event_title"], qe["peak_bucket"], qe["peak_yes_ask"] * 100,
            len(qe["qualifying"]), qe["n_buckets"],
        )
        for q in qe["qualifying"]:
            no_bid_pct = q["no_bid"] * 100 if q["no_bid"] is not None else float("nan")
            no_ask_pct = q["no_ask"] * 100 if q["no_ask"] is not None else float("nan")
            log.info(
                "    %-10s gap=%d  NO bid=%.2f¢ ask=%.2f¢",
                _format_bucket(q), q["gap"], no_bid_pct, no_ask_pct,
            )

    return qualifying_events


def run(
    min_no_price: float | None = None,
    min_qualifying: int | None = None,
    min_gap: int | None = None,
    loop: bool = False,
    interval_seconds: int = 60,
) -> None:
    log = _setup_logging()

    mnp = min_no_price if min_no_price is not None else config.get_float(
        "MIN_NO_PRICE", "weather", "min_no_price", 0.97,
    )
    mq = min_qualifying if min_qualifying is not None else config.get_int(
        "SELLER_MIN_QUALIFYING", "seller", "min_qualifying", 2,
    )
    mg = min_gap if min_gap is not None else config.get_int(
        "SELLER_MIN_GAP", "seller", "min_gap", 2,
    )

    log.info(
        "Seller scan: min_no_price=%.3f  min_qualifying=%d  min_gap=%d  loop=%s",
        mnp, mq, mg, loop,
    )

    iteration = 0
    while True:
        iteration += 1
        log.info("── Iteration %d ──", iteration)
        try:
            scan(min_no_price=mnp, min_qualifying=mq, min_gap=mg)
        except Exception as exc:
            log.warning("scan failed: %s: %s", type(exc).__name__, exc)
        if not loop:
            break
        import time as _t
        _t.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Aapang-style weather seller — stage 1: filter + log")
    parser.add_argument("--min-no-price", type=float, default=None, help="Minimum NO bid (default 0.97 from config.weather.min_no_price)")
    parser.add_argument("--min-qualifying", type=int, default=None, help="Minimum qualifying markets per event (default 2)")
    parser.add_argument("--min-gap", type=int, default=None, help="Minimum bucket-index distance from peak-YES bucket (default 2 = at least 1 bucket of gap between)")
    parser.add_argument("--loop", action="store_true", help="Loop forever instead of running one cycle")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds (default 60)")
    args = parser.parse_args()

    run(
        min_no_price=args.min_no_price,
        min_qualifying=args.min_qualifying,
        min_gap=args.min_gap,
        loop=args.loop,
        interval_seconds=max(1, args.interval),
    )
