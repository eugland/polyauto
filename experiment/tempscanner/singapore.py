"""Live Singapore temperature probability data fetched directly from Polymarket APIs."""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

log = logging.getLogger("tempscanner.singapore")

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY = "https://clob.polymarket.com/prices-history"
SG_OFFSET_H = 8  # Asia/Singapore = UTC+8

RANGE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.IGNORECASE)
THRESHOLD_RE = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+(or\s+higher|or\s+below|or\s+lower)", re.IGNORECASE
)


def _parse_json_list(raw: Any) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except Exception:
            pass
    return []


def _parse_bucket(question: str) -> tuple[float | None, float | None, str]:
    m = RANGE_RE.search(question)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper()
    m = THRESHOLD_RE.search(question)
    if m:
        return float(m.group(1)), None, m.group(2).upper()
    return None, None, "C"


def _bucket_label(lo: float, hi: float | None, unit: str) -> str:
    return f"{lo:.0f}–{hi:.0f}°{unit}" if hi is not None else f"{lo:.0f}°{unit}"


def _date_to_event_slug(date_str: str) -> str:
    """'2026-04-16' → 'highest-temperature-in-singapore-on-april-16-2026'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"highest-temperature-in-singapore-on-{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"


def _sg_window_utc(date_str: str) -> tuple[int, int]:
    """Return (start_unix, end_unix) for midnight–noon in Singapore time on date_str."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    start_utc = dt.replace(tzinfo=timezone.utc) - timedelta(hours=SG_OFFSET_H)
    end_utc = dt.replace(hour=12, tzinfo=timezone.utc) - timedelta(hours=SG_OFFSET_H)
    return int(start_utc.timestamp()), int(end_utc.timestamp())


def _ts_to_sg_hour(ts: int) -> float:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=SG_OFFSET_H)
    return dt.hour + dt.minute / 60 + dt.second / 3600


def _fetch_event_markets(date_str: str) -> list[dict]:
    """Fetch and parse bucket markets for Singapore on date_str from Gamma API."""
    slug = _date_to_event_slug(date_str)
    log.info("[Gamma] GET events?slug=%s", slug)
    try:
        r = requests.get(GAMMA_EVENTS, params={"slug": slug}, timeout=12)
        r.raise_for_status()
        raw = r.json()
        log.info("[Gamma] Response: %s", json.dumps(raw, indent=2))
        events = raw if isinstance(raw, list) else raw.get("events", [])
        if not events:
            log.warning("[Gamma] No events found for slug=%s", slug)
            return []
        event = events[0]
    except Exception as exc:
        log.error("[Gamma] Request failed for slug=%s: %s", slug, exc)
        return []

    results: list[dict] = []
    for mkt in _parse_json_list(event.get("markets")):
        if not isinstance(mkt, dict):
            continue
        question = str(mkt.get("question") or "")
        lo, hi, unit = _parse_bucket(question)
        if lo is None:
            continue
        outcomes = _parse_json_list(mkt.get("outcomes"))
        token_ids = _parse_json_list(mkt.get("clobTokenIds"))
        yes_tok = None
        for i, name in enumerate(outcomes):
            if str(name).strip().lower() == "yes" and i < len(token_ids):
                yes_tok = str(token_ids[i])
        prices = _parse_json_list(mkt.get("outcomePrices"))
        best_bid = mkt.get("bestBid")
        best_ask = mkt.get("bestAsk")
        if best_bid is not None and best_ask is not None:
            mid = (float(best_bid) + float(best_ask)) / 2
        elif prices:
            mid = float(prices[0])
        else:
            mid = None
        results.append({"lo": lo, "hi": hi, "unit": unit, "yes_token_id": yes_tok, "mid": mid})

    log.info("[Gamma] Parsed %d buckets for %s", len(results), date_str)
    results.sort(key=lambda x: x["lo"])
    return results


def _fetch_clob_history(token_id: str, start_ts: int, end_ts: int, fidelity: int = 30) -> list[dict]:
    """Return [{x: sg_hour, y: probability_pct}, ...] from CLOB price history."""
    log.info("[CLOB] GET prices-history market=%s startTs=%d endTs=%d fidelity=%d",
             token_id, start_ts, end_ts, fidelity)
    try:
        r = requests.get(
            CLOB_HISTORY,
            params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": fidelity},
            timeout=12,
        )
        r.raise_for_status()
        raw = r.json()
        log.info("[CLOB] Response: %s", json.dumps(raw, indent=2))
        history = raw.get("history") or []
    except Exception as exc:
        log.error("[CLOB] Request failed for token=%s: %s", token_id, exc)
        return []

    pts = []
    for h in history:
        t, p = h.get("t"), h.get("p")
        if t is None or p is None:
            continue
        sg_hour = _ts_to_sg_hour(int(t))
        if 0 <= sg_hour <= 12:
            pts.append({"x": round(sg_hour, 3), "y": round(float(p) * 100, 2)})
    return pts


def fetch_singapore_chart(days: int = 4) -> dict:
    """Return probability chart data for Singapore's top-3 temp buckets, live from Polymarket.

    Each day finds its OWN highest-probable bucket (slot 0), so slot −1/+1 are relative
    to that day's peak — not pinned to today's temperature range.
    """
    now_utc = datetime.now(timezone.utc)
    now_sg = now_utc + timedelta(hours=SG_OFFSET_H)
    today_sg = now_sg.strftime("%Y-%m-%d")

    dates = [
        (datetime.strptime(today_sg, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]
    day_labels = ["Today", "Yesterday", "−2 days", "−3 days"][:days]

    # Fetch markets for every date
    date_markets: dict[str, list[dict]] = {}
    for date in dates:
        date_markets[date] = _fetch_event_markets(date)

    today_markets = date_markets.get(today_sg, [])
    if not today_markets:
        return {"error": f"No Singapore market found on Polymarket for {today_sg}."}

    # Initialise slot containers
    result: dict = {
        "city": "Singapore",
        "today": today_sg,
        "dates": dates,
        "day_labels": day_labels,
        "slots": {"-1": {"days": {}}, "0": {"days": {}}, "1": {"days": {}}},
    }

    # Set today's slot labels + current_prob from today's peak
    today_best = max(range(len(today_markets)), key=lambda i: today_markets[i]["mid"] or 0)
    for offset, slot_key in [(-1, "-1"), (0, "0"), (1, "1")]:
        idx = today_best + offset
        if 0 <= idx < len(today_markets):
            m = today_markets[idx]
            result["slots"][slot_key]["label"] = _bucket_label(m["lo"], m["hi"], m["unit"])
            result["slots"][slot_key]["current_prob"] = round((m["mid"] or 0) * 100, 2)
        else:
            result["slots"][slot_key]["label"] = "N/A"
            result["slots"][slot_key]["current_prob"] = 0

    # For each day: find that day's own peak, fetch history for its ±1/0 buckets
    for date, label in zip(dates, day_labels):
        mkts = date_markets.get(date, [])
        start_ts, end_ts = _sg_window_utc(date)
        if date == today_sg:
            end_ts = min(end_ts, int(now_utc.timestamp()))

        if not mkts:
            for slot_key in ["-1", "0", "1"]:
                result["slots"][slot_key]["days"][date] = {"points": [], "temp_label": "no data"}
            continue

        best = max(range(len(mkts)), key=lambda i: mkts[i]["mid"] or 0)
        log.info("[Chart] %s (%s): peak bucket = %s (mid=%.3f)",
                 label, date, _bucket_label(mkts[best]["lo"], mkts[best]["hi"], mkts[best]["unit"]),
                 mkts[best]["mid"] or 0)

        for offset, slot_key in [(-1, "-1"), (0, "0"), (1, "1")]:
            idx = best + offset
            if 0 <= idx < len(mkts):
                mkt = mkts[idx]
                temp_label = _bucket_label(mkt["lo"], mkt["hi"], mkt["unit"])
                pts = _fetch_clob_history(mkt["yes_token_id"], start_ts, end_ts) \
                    if mkt["yes_token_id"] else []
            else:
                temp_label = "N/A"
                pts = []
            result["slots"][slot_key]["days"][date] = {"points": pts, "temp_label": temp_label}

    return result
