"""
Live checker for BTC 5m Up/Down market depth at a target buy price.

Example:
  python -m experiment.btc_5m_49c_check --url https://polymarket.com/event/btc-updown-5m-1775784900 --max-price 0.49 --shares 30
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen

import requests

GAMMA_API = "https://gamma-api.polymarket.com/events"
CLOB_HOST = "https://clob.polymarket.com"
ACTIVE_MAX_AHEAD_SECONDS = 20 * 60


def _extract_slug(value: str) -> str:
    value = (value or "").strip()
    if "/event/" in value:
        m = re.search(r"/event/([^/?#]+)", value)
        if not m:
            raise ValueError(f"Could not extract slug from URL: {value}")
        return m.group(1)
    return value


def _get_json(url: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_event(slug: str) -> dict[str, Any]:
    data = _get_json(f"{GAMMA_API}?slug={slug}")
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"No event found for slug '{slug}'")
    event = data[0]
    if not isinstance(event, dict):
        raise RuntimeError("Malformed event payload")
    return event


def _is_btc_5m_slug(slug: str) -> bool:
    return bool(re.match(r"^btc-updown-5m-\d+$", slug.strip().lower()))


def _extract_market_up_down_tokens(market: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = _load_field(market.get("outcomes")) or []
    token_ids = _load_field(market.get("clobTokenIds")) or []
    up_token = None
    down_token = None
    for i, name in enumerate(outcomes):
        label = str(name).strip().lower()
        if i >= len(token_ids):
            continue
        if label == "up":
            up_token = str(token_ids[i])
        elif label == "down":
            down_token = str(token_ids[i])
    if up_token and down_token:
        return up_token, down_token
    return None


def _event_to_candidate(event: dict[str, Any], now_utc: datetime) -> dict[str, Any] | None:
    slug = str(event.get("slug") or "")
    if not _is_btc_5m_slug(slug):
        return None
    markets = event.get("markets") or []
    if not markets:
        return None
    market = markets[0]
    if market.get("closed") or (market.get("active") is not None and not market.get("active")):
        return None
    tokens = _extract_market_up_down_tokens(market)
    if not tokens:
        return None

    end_str = market.get("endDate") or event.get("endDate") or ""
    try:
        end_dt = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        end_utc = end_dt if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    seconds_remaining = int((end_utc - now_utc).total_seconds())
    if seconds_remaining < 0 or seconds_remaining > ACTIVE_MAX_AHEAD_SECONDS:
        return None

    up_token, down_token = tokens
    return {
        "slug": slug,
        "title": str(event.get("title") or slug),
        "up_token": up_token,
        "down_token": down_token,
        "seconds_remaining": seconds_remaining,
        "end_utc": end_utc,
    }


def _build_btc_5m_slug_candidates(now_utc: datetime) -> list[str]:
    bucket_seconds = 5 * 60
    now_epoch = int(now_utc.timestamp())
    base = (now_epoch // bucket_seconds) * bucket_seconds
    slugs: list[str] = []
    # Probe nearby 5m buckets first (past and near-future).
    for delta in range(-6, 7):
        ts = base + delta * bucket_seconds
        slugs.append(f"btc-updown-5m-{ts}")
    return slugs


def _find_current_btc_5m_event() -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)

    # Fast path: deterministic slug checks around current 5m bucket.
    best_direct: dict[str, Any] | None = None
    for slug in _build_btc_5m_slug_candidates(now_utc):
        try:
            event = _fetch_event(slug)
        except Exception:
            continue
        candidate = _event_to_candidate(event, now_utc)
        if candidate is None:
            continue
        if best_direct is None or candidate["end_utc"] < best_direct["end_utc"]:
            best_direct = candidate
    if best_direct is not None:
        return best_direct

    # Fallback path: scan open events pages.
    best: dict[str, Any] | None = None
    for page in range(20):
        offset = page * 200
        data = _get_json(f"{GAMMA_API}?closed=false&limit=200&offset={offset}")
        if not isinstance(data, list) or not data:
            break
        for event in data:
            if not isinstance(event, dict):
                continue
            candidate = _event_to_candidate(event, now_utc)
            if candidate is None:
                continue
            if best is None or candidate["end_utc"] < best["end_utc"]:
                best = candidate
        if best is not None:
            break
    if best is None:
        raise RuntimeError("No active btc-updown-5m market found")
    return best


def _load_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value


def _extract_up_down_tokens(event: dict[str, Any]) -> tuple[str, str]:
    markets = event.get("markets") or []
    if not markets:
        raise RuntimeError("Event has no markets")
    market = markets[0]
    outcomes = _load_field(market.get("outcomes")) or []
    token_ids = _load_field(market.get("clobTokenIds")) or []

    up_token = None
    down_token = None
    for i, name in enumerate(outcomes):
        label = str(name).strip().lower()
        if i >= len(token_ids):
            continue
        if label == "up":
            up_token = str(token_ids[i])
        elif label == "down":
            down_token = str(token_ids[i])

    if not up_token or not down_token:
        raise RuntimeError("Could not resolve both Up/Down token IDs")
    return up_token, down_token


def _fetch_books(token_ids: list[str], host: str) -> dict[str, dict[str, Any]]:
    resp = requests.post(
        f"{host}/books",
        json=[{"token_id": tid} for tid in token_ids],
        timeout=10,
    )
    resp.raise_for_status()
    out: dict[str, dict[str, Any]] = {}
    for book in resp.json():
        tid = str(book.get("asset_id") or book.get("token_id") or "")
        if tid:
            out[tid] = book
    return out


def _available_at_or_below(book: dict[str, Any], max_price: float) -> tuple[float, float | None]:
    asks = book.get("asks") or []
    total = 0.0
    best: float | None = None
    for ask in asks:
        try:
            price = float(ask.get("price"))
            size = float(ask.get("size"))
        except (TypeError, ValueError):
            continue
        if best is None or price < best:
            best = price
        if price <= max_price:
            total += size
    return total, best


def _append_log(log_file: str, row: dict[str, Any]) -> None:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _run_once(slug_or_url: str | None, max_price: float, shares: float, host: str, log_file: str) -> None:
    if slug_or_url:
        slug = _extract_slug(slug_or_url)
        event = _fetch_event(slug)
        up_token, down_token = _extract_up_down_tokens(event)
        title = str(event.get("title") or slug)
    else:
        current = _find_current_btc_5m_event()
        slug = current["slug"]
        title = current["title"]
        up_token = current["up_token"]
        down_token = current["down_token"]

    books = _fetch_books([up_token, down_token], host)

    up_book = books.get(up_token, {})
    down_book = books.get(down_token, {})
    up_liq, up_best_ask = _available_at_or_below(up_book, max_price)
    down_liq, down_best_ask = _available_at_or_below(down_book, max_price)

    up_ok = up_liq >= shares
    down_ok = down_liq >= shares
    both_ok = up_ok and down_ok

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    now_iso = datetime.now(timezone.utc).isoformat()
    print("=" * 72)
    print(f"{ts}")
    print(f"Event: {title}")
    print(f"Slug:  {slug}")
    print(f"Target buy price: <= {max_price:.3f}")
    print(f"Target shares per side: {shares:.2f}")
    print("-" * 72)
    print(f"Up   best_ask={up_best_ask if up_best_ask is not None else 'n/a'}  "
          f"liquidity<=target={up_liq:.2f}  success={up_ok}")
    print(f"Down best_ask={down_best_ask if down_best_ask is not None else 'n/a'}  "
          f"liquidity<=target={down_liq:.2f}  success={down_ok}")
    print("-" * 72)
    print(f"both_sides_succeed={both_ok}")
    print(f"log_file={log_file}")

    _append_log(
        log_file,
        {
            "ts_utc": now_iso,
            "slug": slug,
            "title": title,
            "max_price": max_price,
            "shares_per_side": shares,
            "up_best_ask": up_best_ask,
            "up_liquidity_at_or_below": up_liq,
            "up_success": up_ok,
            "down_best_ask": down_best_ask,
            "down_liquidity_at_or_below": down_liq,
            "down_success": down_ok,
            "both_sides_succeed": both_ok,
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check if buying both BTC 5m sides at a max price can fully fill."
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Polymarket event URL or slug. If omitted, auto-resolves current btc-updown-5m market.",
    )
    parser.add_argument(
        "--max-price",
        type=float,
        default=0.49,
        help="Max buy price (decimal dollars; 0.49 = 49 cents)",
    )
    parser.add_argument(
        "--shares",
        type=float,
        default=30.0,
        help="Target shares to buy per side",
    )
    parser.add_argument(
        "--host",
        default=CLOB_HOST,
        help="CLOB host",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously re-check in real time and append logs",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between checks when --watch is set",
    )
    parser.add_argument(
        "--log-file",
        default=os.path.join("experiment", "logs", "btc_5m_49c_check.jsonl"),
        help="JSONL file path for appended logs",
    )
    args = parser.parse_args()

    while True:
        try:
            _run_once(
                slug_or_url=args.url,
                max_price=args.max_price,
                shares=args.shares,
                host=args.host,
                log_file=args.log_file,
            )
        except Exception as exc:
            print(f"ERROR: {exc}")
        if not args.watch:
            return
        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
