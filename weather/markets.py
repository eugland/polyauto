"""
Fetch Polymarket highest-temperature markets, group by event, parse bracket
thresholds, and rank by real-world decide time (3 PM local, when the daily
high is typically set).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from automata.parser import (
    _extract_no_token_id,
    _extract_yes_token_id,
    _parse_threshold,
)
from automata.polymarket import fetch_temperature_markets_payload
from automata.weather import (
    extract_all_urls,
    extract_icao_from_wunderground_url,
    extract_station_name,
    extract_unit,
)
from automata.weather_bot import CITY_TZ, _extract_city, _extract_title_date


@dataclass
class Bracket:
    question: str
    threshold: float
    threshold_hi: float | None
    direction: str  # "higher" | "below" | "range" | "exact"
    unit: str       # "F" | "C"
    no_token_id: str
    yes_token_id: str | None
    bid: float | None = None
    ask: float | None = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    bid_volume: float | None = None
    ask_volume: float | None = None
    volume: float | None = None            # total $ ever traded
    volume_24h: float | None = None        # 24-hour $ volume
    liquidity: float | None = None         # $ resting on the book
    forecast_mean: float | None = None
    forecast_std: float | None = None
    p_yes: float | None = None

    @property
    def p_no(self) -> float | None:
        return None if self.p_yes is None else 1.0 - self.p_yes


@dataclass
class RankedMarket:
    city: str
    event_title: str
    event_date: str                    # YYYY-MM-DD
    title_date: str                    # e.g. "March 30"
    decide_utc: datetime | None
    minutes_until_decide: float | None
    icao: str | None
    station_name: str | None
    unit: str
    brackets: list[Bracket] = field(default_factory=list)


def _decide_utc(city: str, event_date: str) -> datetime | None:
    tz_name = CITY_TZ.get(city)
    if not tz_name:
        return None
    try:
        d = date.fromisoformat(event_date)
    except ValueError:
        return None
    local = datetime.combine(d, time(15, 0), ZoneInfo(tz_name))
    return local.astimezone(timezone.utc)


def _group_events(raw_markets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for raw in raw_markets:
        event_slug = str(raw.get("event_slug") or "")
        event_title = str(raw.get("event_title") or event_slug)
        if event_slug not in events:
            description = str(raw.get("event_description") or "")
            urls = extract_all_urls(description)
            icao = next(
                (extract_icao_from_wunderground_url(u) for u in urls if "wunderground" in u.lower()),
                None,
            )
            end_raw = raw.get("endDateIso") or raw.get("endDate") or ""
            event_date = end_raw[:10] if end_raw else ""
            events[event_slug] = {
                "title": event_title,
                "station_name": extract_station_name(description),
                "icao": icao,
                "unit": extract_unit(description),
                "date": event_date,
                "markets": [],
            }
        events[event_slug]["markets"].append(raw)
    return events


def fetch_ranked_markets() -> list[RankedMarket]:
    """
    Fetch open temperature markets, parse brackets, rank by decide_utc ascending.
    """
    payload = fetch_temperature_markets_payload()
    events = _group_events(payload["markets"])
    now = datetime.now(timezone.utc)

    ranked: list[RankedMarket] = []
    for event in events.values():
        city = _extract_city(event["title"])
        decide = _decide_utc(city, event["date"])
        mins = (decide - now).total_seconds() / 60 if decide else None

        rm = RankedMarket(
            city=city,
            event_title=event["title"],
            event_date=event["date"],
            title_date=_extract_title_date(event["title"]),
            decide_utc=decide,
            minutes_until_decide=mins,
            icao=event["icao"],
            station_name=event["station_name"],
            unit=event["unit"],
        )

        for raw in event["markets"]:
            if raw.get("closed"):
                continue
            if raw.get("active") is not None and not raw.get("active"):
                continue
            question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
            parsed = _parse_threshold(question)
            if not parsed:
                continue
            no_id = _extract_no_token_id(raw)
            if not no_id:
                continue
            yes_id = _extract_yes_token_id(raw)
            threshold, threshold_hi, unit, direction = parsed

            def _f(key: str) -> float | None:
                v = raw.get(key)
                try:
                    return float(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            rm.brackets.append(Bracket(
                question=question,
                threshold=threshold,
                threshold_hi=threshold_hi,
                direction=direction,
                unit=unit or event["unit"],
                no_token_id=no_id,
                yes_token_id=yes_id,
                volume=_f("volumeNum") or _f("volume"),
                volume_24h=_f("volume24hr") or _f("volume24hrNum"),
                liquidity=_f("liquidityNum") or _f("liquidity"),
            ))

        if rm.brackets:
            ranked.append(rm)

    def _rank_key(r: RankedMarket):
        # Ordering: future-decide first (soonest first), then past-decide
        # (most recent first), then unknowns. Past markets are already
        # determined but not yet settled — useful to see but not actionable.
        if r.decide_utc is None:
            return (2, 0.0)
        mins = r.minutes_until_decide or 0.0
        if mins >= 0:
            return (0, mins)       # upcoming: soonest first
        return (1, -mins)          # past: most recent first

    ranked.sort(key=_rank_key)
    return ranked
