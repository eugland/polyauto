#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from data.dto import ApiEvent

POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
OPENWEATHER_CURRENT_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

CONFIG_PATH = Path("config.json")
DEBUG_ENABLED = False
DEBUG_SHOW_LEVEL = True
EVENT_HORIZON_HOURS = 30

SLUG_RE = re.compile(r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$")


def debug_log(message: str) -> None:
    if not DEBUG_ENABLED:
        return
    if DEBUG_SHOW_LEVEL:
        print(f"[DEBUG] {message}")
    else:
        print(message)


def build_debug_url(url: str, params) -> str:
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    base_parts = urlsplit(url)
    base_pairs = parse_qsl(base_parts.query, keep_blank_values=True)
    if base_pairs:
        redacted_base_pairs = []
        for key, value in base_pairs:
            if key in {"appid", "apiKey"}:
                redacted_base_pairs.append((key, "***"))
            else:
                redacted_base_pairs.append((key, value))
        url = urlunsplit(
            (
                base_parts.scheme,
                base_parts.netloc,
                base_parts.path,
                urlencode(redacted_base_pairs),
                base_parts.fragment,
            )
        )

    if isinstance(params, dict):
        safe_params = dict(params)
        if "appid" in safe_params:
            safe_params["appid"] = "***"
        if "apiKey" in safe_params:
            safe_params["apiKey"] = "***"
    else:
        safe_params = []
        for key, value in params:
            if key in {"appid", "apiKey"}:
                safe_params.append((key, "***"))
            else:
                safe_params.append((key, value))
    req = requests.Request("GET", url, params=safe_params).prepare()
    return req.url or url


@dataclass
class ConfigItem:
    name: str
    key: str
    unit: str
    wunderground: str | None


def extract_wunderground_station_tag(text: str | None) -> str | None:
    if not text:
        return None
    from urllib.parse import urlsplit

    for match in re.finditer(r"https?://(?:www\.)?wunderground\.com/[^\s\"'<>]+", text, re.IGNORECASE):
        url = match.group(0).rstrip(".,);]")
        parts = urlsplit(url)
        segments = [segment for segment in parts.path.split("/") if segment]
        for segment in reversed(segments):
            candidate = re.sub(r"[^A-Za-z0-9]", "", segment).upper()
            if len(candidate) >= 4:
                return candidate
    return None


def openweather_units(unit: str) -> str:
    return "imperial" if unit.upper() == "F" else "metric"


def fetch_weather_now_by_unit(coordinates, api_key: str, unit: str) -> dict:
    params = {
        "lat": coordinates.lat,
        "lon": coordinates.lon,
        "appid": api_key,
        "units": openweather_units(unit),
    }
    debug_log(f"GET {build_debug_url(OPENWEATHER_CURRENT_WEATHER_URL, params)}")
    response = requests.get(
        OPENWEATHER_CURRENT_WEATHER_URL,
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    timezone_offset = int(payload.get("timezone") or 0)
    dt = int(payload.get("dt") or 0)
    local_time = None
    local_date = None
    if dt:
        from datetime import datetime, timezone as tz, timedelta as td

        location_tz = tz(td(seconds=timezone_offset))
        local_datetime = datetime.fromtimestamp(dt, tz=tz.utc).astimezone(location_tz)
        local_time = local_datetime.isoformat()
        local_date = local_datetime.date().isoformat()

    main = payload.get("main") or {}
    weather_items = payload.get("weather") or []
    description = None
    if weather_items and isinstance(weather_items[0], dict):
        description = weather_items[0].get("description")

    temperature = main.get("temp")
    return {
        "local_time_now": local_time,
        "local_date_now": local_date,
        "temperature": round(float(temperature), 2) if temperature is not None else None,
        "weather_description": description,
    }


def fetch_forecast_by_unit(coordinates, api_key: str, unit: str) -> dict:
    params = {
        "lat": coordinates.lat,
        "lon": coordinates.lon,
        "appid": api_key,
        "units": openweather_units(unit),
    }
    debug_log(f"GET {build_debug_url(OPENWEATHER_FORECAST_URL, params)}")
    response = requests.get(
        OPENWEATHER_FORECAST_URL,
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    city = payload.get("city") or {}
    timezone_offset = int(city.get("timezone") or 0)
    from datetime import datetime, timezone as tz, timedelta as td

    location_tz = tz(td(seconds=timezone_offset))
    points = []
    for item in payload.get("list") or []:
        if not isinstance(item, dict):
            continue
        dt = item.get("dt")
        if not isinstance(dt, (int, float)):
            continue
        local_datetime = datetime.fromtimestamp(int(dt), tz=tz.utc).astimezone(location_tz)
        main = item.get("main") or {}
        points.append(
            {
                "local_date": local_datetime.date().isoformat(),
                "temp_max": (
                    float(main["temp_max"])
                    if isinstance(main.get("temp_max"), (int, float))
                    else None
                ),
            }
        )
    return {"forecast_points": points}


def build_daily_max_temperature(
    weather_now: dict, forecast: dict, target_date: date
) -> float | None:
    target_date_iso = target_date.isoformat()
    max_temp: float | None = None

    forecast_points = forecast.get("forecast_points")
    if isinstance(forecast_points, list):
        for point in forecast_points:
            if not isinstance(point, dict):
                continue
            if point.get("local_date") != target_date_iso:
                continue
            raw_max = point.get("temp_max")
            if not isinstance(raw_max, (int, float)):
                continue
            value = float(raw_max)
            max_temp = value if max_temp is None else max(max_temp, value)

    local_date_now = weather_now.get("local_date_now")
    current_temp = weather_now.get("temperature")
    if local_date_now == target_date_iso and isinstance(current_temp, (int, float)):
        current_value = float(current_temp)
        max_temp = current_value if max_temp is None else max(max_temp, current_value)

    return max_temp


def parse_json_list(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    return []


def parse_price(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_locked_pair(yes_price: float | None, no_price: float | None) -> bool:
    if yes_price is None or no_price is None:
        return False
    return (
        abs(yes_price - 0.0005) < 1e-12 and abs(no_price - 0.9995) < 1e-12
    ) or (
        abs(yes_price - 0.9995) < 1e-12 and abs(no_price - 0.0005) < 1e-12
    )


def load_config(path: Path) -> list[ConfigItem]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items: list[ConfigItem] = []
    for name, value in payload.items():
        if not isinstance(value, dict):
            continue
        key = str(value.get("key") or name).strip().lower()
        unit = str(value.get("unit", "")).upper()
        if unit not in {"C", "F"}:
            continue
        wunderground = str(value.get("wunderground") or "").strip()
        items.append(
            ConfigItem(
                name=name,
                key=key,
                unit=unit,
                wunderground=wunderground or None,
            )
        )
    return items


def load_raw_config(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


def sync_config_wunderground_from_events(path: Path, events: list[ApiEvent]) -> int:
    raw_config = load_raw_config(path)

    station_by_key: dict[str, str] = {}
    event_by_key: dict[str, ApiEvent] = {}
    for event in events:
        location_key = extract_location_key_from_slug(event.slug)
        if not location_key:
            continue
        station = extract_wunderground_station_tag(event.description)
        if station is None:
            station = extract_wunderground_station_tag(event.resolutionSource)
        if station:
            station_by_key[location_key] = station
            event_by_key[location_key] = event

    updates = 0
    existing_keys: set[str] = set()
    for name, value in raw_config.items():
        if not isinstance(value, dict):
            continue
        key = str(value.get("key") or name).strip().lower()
        if not key:
            continue
        existing_keys.add(key)
        station = station_by_key.get(key)
        if not station:
            continue
        if str(value.get("wunderground") or "").strip().upper() == station:
            continue
        value["wunderground"] = station
        updates += 1

    for key, station in station_by_key.items():
        if key in existing_keys:
            continue
        event = event_by_key.get(key)
        unit = infer_unit_from_event(event) if event else "C"
        raw_config[key] = {
            "key": key,
            "unit": unit,
            "wunderground": station,
        }
        updates += 1

    if updates > 0:
        path.write_text(json.dumps(raw_config, indent=2) + "\n", encoding="utf-8")
    return updates


def wunderground_units(unit: str) -> str:
    return "e" if unit.upper() == "F" else "m"


def set_url_query_param(url: str, key: str, value: str) -> str:
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    updated = []
    replaced = False
    for k, v in pairs:
        if k == key:
            updated.append((k, value))
            replaced = True
        else:
            updated.append((k, v))
    if not replaced:
        updated.append((key, value))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(updated), parts.fragment))


def normalize_wunderground_target(target: str) -> tuple[str, str]:
    from urllib.parse import urlsplit

    raw = str(target or "").strip()
    if not raw:
        raise ValueError("Empty Wunderground station target.")

    if raw.lower().startswith(("http://", "https://")):
        parts = urlsplit(raw)
        if not parts.netloc.lower().endswith("wunderground.com"):
            raise ValueError(f"Unsupported Wunderground host in target: {target}")
        page_url = raw
        segments = [segment for segment in parts.path.split("/") if segment]
        station_tag = ""
        for segment in reversed(segments):
            candidate = re.sub(r"[^A-Za-z0-9]", "", segment).upper()
            if len(candidate) >= 4:
                station_tag = candidate
                break
        if not station_tag:
            raise ValueError(f"Could not parse station tag from Wunderground URL: {target}")
        return page_url, station_tag

    station_tag = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    if len(station_tag) < 4:
        raise ValueError(f"Invalid Wunderground station tag: {target}")
    page_url = f"https://www.wunderground.com/weather/{station_tag}"
    return page_url, station_tag


def fetch_wunderground_weather(station_tag: str, unit: str) -> dict:
    page_url, normalized_station_tag = normalize_wunderground_target(station_tag)
    response = requests.get(page_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    html = response.text

    current_match = re.search(
        r"https://api\.weather\.com/v2/pws/observations/current\?[^\"']+",
        html,
    )
    if not current_match:
        raise RuntimeError(f"Could not locate Wunderground current endpoint for {station_tag}.")
    current_url = set_url_query_param(current_match.group(0), "units", wunderground_units(unit))
    debug_log(f"GET {build_debug_url(current_url, {})}")
    current = requests.get(current_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).json()
    observations = current.get("observations") or []
    if not observations:
        raise RuntimeError(f"No Wunderground current observations for {station_tag}.")
    now = observations[0]

    station_id = str(now.get("stationID") or "")
    obs_local = str(now.get("obsTimeLocal") or "")
    local_date = obs_local.split(" ")[0]
    date_for_history = datetime.strptime(local_date, "%Y-%m-%d").strftime("%Y%m%d")

    api_key_match = re.search(r"(?:\?|&)apiKey=([^&]+)", current_url)
    if not api_key_match:
        raise RuntimeError(f"Could not parse Wunderground apiKey for {station_tag}.")
    api_key = requests.utils.unquote(api_key_match.group(1))
    history_params = {
        "apiKey": api_key,
        "stationId": station_id,
        "units": wunderground_units(unit),
        "format": "json",
        "date": date_for_history,
    }
    history_url = "https://api.weather.com/v2/pws/history/all"
    debug_log(f"GET {build_debug_url(history_url, history_params)}")
    history = requests.get(
        history_url,
        params=history_params,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0"},
    ).json()

    unit_key = "imperial" if unit.upper() == "F" else "metric"
    temp_history = []
    max_from_history = None
    for item in history.get("observations") or []:
        unit_payload = item.get(unit_key) or {}
        temp = unit_payload.get("tempAvg")
        temp_high = unit_payload.get("tempHigh")
        temp_history.append(
            {
                "obs_time_local": item.get("obsTimeLocal"),
                "temp": temp,
                "temp_high": temp_high,
            }
        )
        if isinstance(temp_high, (int, float)):
            max_from_history = (
                float(temp_high)
                if max_from_history is None
                else max(max_from_history, float(temp_high))
            )

    forecast_max = None
    forecast_match = re.search(
        rf"https://api\.weather\.com/v3/wx/forecast/daily/3day\?[^\"']*icaoCode={re.escape(normalized_station_tag)}[^\"']*",
        html,
    )
    if forecast_match:
        forecast_url = set_url_query_param(forecast_match.group(0), "units", wunderground_units(unit))
        debug_log(f"GET {build_debug_url(forecast_url, {})}")
        forecast = requests.get(forecast_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).json()
        candidates = forecast.get("temperatureMax")
        if isinstance(candidates, list) and candidates and isinstance(candidates[0], (int, float)):
            forecast_max = float(candidates[0])

    current_unit_payload = now.get(unit_key) or {}
    current_temp = current_unit_payload.get("temp")
    max_today = None
    for v in [current_temp, max_from_history, forecast_max]:
        if isinstance(v, (int, float)):
            max_today = float(v) if max_today is None else max(max_today, float(v))

    return {
        "local_time_now": obs_local,
        "local_date_now": local_date,
        "temperature": current_temp,
        "max_temperature": max_today,
        "unit": unit,
        "weather_description": "wunderground",
        "history_temperature_today": temp_history,
    }


def build_default_weather(unit: str) -> dict:
    return {
        "local_time_now": None,
        "local_date_now": None,
        "temperature": None,
        "max_temperature": None,
        "unit": unit,
        "weather_description": "disabled",
    }


def prefetch_wunderground_weather(
    events: list[ApiEvent], config_by_key: dict[str, ConfigItem]
) -> dict[str, dict]:
    tasks: dict[str, ConfigItem] = {}
    for event in events:
        key = extract_location_key_from_slug(event.slug) or (event.slug or "unknown")
        config_item = config_by_key.get(key)
        if not config_item or not config_item.wunderground:
            continue
        tasks[key] = config_item

    if not tasks:
        return {}

    weather_cache: dict[str, dict] = {}
    max_workers = min(12, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(
                fetch_wunderground_weather, config_item.wunderground, config_item.unit
            ): key
            for key, config_item in tasks.items()
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            config_item = tasks[key]
            try:
                weather_cache[key] = future.result()
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                weather = build_default_weather(config_item.unit)
                weather["weather_description"] = f"wunderground error: {exc}"
                weather_cache[key] = weather
    return weather_cache


def extract_location_key_from_slug(slug: str | None) -> str | None:
    if not slug:
        return None
    match = SLUG_RE.match(slug.strip().lower())
    if not match:
        return None
    return match.group(1)


def fetch_temperature_tag_events() -> list[ApiEvent]:
    horizon = datetime.now(timezone.utc) + timedelta(hours=EVENT_HORIZON_HOURS)
    params = {
        "tag_slug": "temperature",
        "closed": "false",
        "limit": 200,
        "end_date_max": horizon.isoformat(),
    }
    debug_log(f"GET {build_debug_url(POLYMARKET_EVENTS_URL, params)}")
    response = requests.get(POLYMARKET_EVENTS_URL, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    events = [ApiEvent.from_dict(raw) for raw in payload if isinstance(raw, dict)]
    events.sort(key=lambda e: str(e.endDate or ""))
    return events


def infer_unit_from_event(event: ApiEvent) -> str:
    for market in event.markets:
        text = f"{market.groupItemTitle or ''} {market.question or ''}"
        if "°F" in text:
            return "F"
        if "°C" in text:
            return "C"
    return "C"


def market_matches_unit(question: str | None, group_item_title: str | None, unit: str) -> bool:
    marker = f"\N{DEGREE SIGN}{unit.upper()}"
    text = f"{question or ''} {group_item_title or ''}"
    return marker in text


def build_markets(event: ApiEvent | None, unit: str):
    if not event:
        return []

    markets = []
    for market in event.markets:
        if not market.active or market.closed:
            continue
        if not market_matches_unit(market.question, market.groupItemTitle, unit):
            continue

        outcomes = parse_json_list(market.outcomes)
        prices = parse_json_list(market.outcomePrices)
        yes_price = None
        no_price = None
        for i, outcome in enumerate(outcomes):
            name = str(outcome).strip().lower()
            price = prices[i] if i < len(prices) else None
            parsed = parse_price(price)
            if name == "yes":
                yes_price = parsed
            elif name == "no":
                no_price = parsed
        if is_locked_pair(yes_price, no_price):
            continue

        options = []
        for i, outcome in enumerate(outcomes):
            price = prices[i] if i < len(prices) else None
            options.append({"name": outcome, "price": price})

        markets.append(
            {
                "id": market.id,
                "question": market.question,
                "group_item_title": market.groupItemTitle,
                "last_trade_price": market.lastTradePrice,
                "volume_num": market.volumeNum,
                "active": market.active,
                "closed": market.closed,
                "outcomes": options,
            }
        )
    return markets


def process_event(
    event: ApiEvent,
    config_by_key: dict[str, ConfigItem],
    weather_cache: dict[str, dict],
):
    key = extract_location_key_from_slug(event.slug) or (event.slug or "unknown")
    config_item = config_by_key.get(key)
    unit = config_item.unit if config_item else infer_unit_from_event(event)
    unit_markets = build_markets(event, unit)
    weather = build_default_weather(unit)
    if config_item and config_item.wunderground:
        if key in weather_cache:
            weather = weather_cache[key]
        else:
            try:
                weather = fetch_wunderground_weather(config_item.wunderground, unit)
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                weather["weather_description"] = f"wunderground error: {exc}"
            weather_cache[key] = weather

    return {
        "key": key,
        "weather": weather,
        "polymarket": {
            "slug": event.slug,
            "event": event.to_dict(),
            "market": [
                {
                    "group_item_title": market.get("group_item_title"),
                    "volume_num": market.get("volume_num"),
                    "outcomes": "\t".join(
                        [
                            f"{outcome.get('name')}: {outcome.get('price')}"
                            for outcome in market.get("outcomes", [])[:2]
                        ]
                    ),
                }
                for market in unit_markets
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket weather market viewer")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument("--debug", action="store_true", help="Print debug logs (including called URLs)")
    parser.add_argument(
        "--sync-wunderground",
        action="store_true",
        help="Update config.json wunderground station tags from event description links",
    )
    parser.add_argument(
        "--debug-no-level",
        action="store_true",
        help="Print debug logs without [DEBUG] prefix",
    )
    return parser.parse_args()


def parse_outcome_price(outcomes: str, side: str) -> float | None:
    target = f"{side.lower()}:"
    for part in outcomes.split("\t"):
        text = part.strip()
        if not text.lower().startswith(target):
            continue
        raw = text[len(target):].strip()
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


def format_cents(value: float | None) -> str:
    if value is None:
        return "   -  "
    cents = value * 100
    return f"{cents:>6.1f}"


def format_market_line(group_item_title: str, outcomes: str, width: int) -> str:
    yes_price = parse_outcome_price(outcomes, "yes")
    no_price = parse_outcome_price(outcomes, "no")
    return (
        f"  - {group_item_title.ljust(width)} | "
        f"Yes:{format_cents(yes_price)}c  No:{format_cents(no_price)}c"
    )


def format_volume(value: float | int | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "-"
    if isinstance(value, (int, float)):
        return f"{round(value):,}"
    return str(value)


def f_to_c(value: float | None) -> float | None:
    if value is None:
        return None
    return round((value - 32) * 5 / 9, 2)


def display_temp(value: float | None, unit: str) -> str:
    if value is None:
        return f"None°{unit}"
    if unit == "F":
        c_value = f_to_c(value)
        return f"{value}°F ({c_value}°C)"
    return f"{value}°{unit}"


def format_local_time_and_date(local_time_iso: str | None, local_date: str | None) -> tuple[str, str]:
    if not local_time_iso:
        return "Unknown", local_date or "Unknown"
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(local_time_iso)
        time_text = dt.strftime("%I:%M%p").lstrip("0")
        date_text = dt.date().isoformat()
        return time_text, date_text
    except ValueError:
        return local_time_iso, local_date or "Unknown"


def print_friendly(output: dict) -> None:
    print(f"Config: {output['config_path']}")
    print(f"Keys: {', '.join(output['request_lication'])}")
    print("")

    weather_map = {item["key"]: item for item in output["weather"]}
    polymarket_map = {item["key"]: item for item in output["polymarket"]}

    for key in output["request_lication"]:
        weather = weather_map[key]
        polymarket = polymarket_map[key]
        markets = polymarket["market"]
        local_time_text, local_date_text = format_local_time_and_date(
            weather.get("local_time_now"), weather.get("local_date_now")
        )
        width = 0
        vol_width = 1
        for market in markets:
            width = max(width, len(market["group_item_title"]))
            vol_width = max(vol_width, len(format_volume(market.get("volume_num"))))

        print(f"[{key}]")
        print(f"Local time: {local_time_text} | Local date: {local_date_text}")
        print(
            f"Weather: {display_temp(weather['temperature'], weather['unit'])} | "
            f"max: {display_temp(weather['max_temperature'], weather['unit'])} | "
            f"{weather['weather_description']}"
        )
        print(f"Slug: {polymarket['slug']}")
        event_volume = format_volume((polymarket.get("event") or {}).get("volume"))
        print(f"Event total vol: {event_volume}")
        if not markets:
            print("Markets: none")
            print("")
            continue

        print("Markets:")
        for market in markets:
            volume_text = format_volume(market.get("volume_num")).rjust(vol_width)
            line = format_market_line(
                market["group_item_title"],
                market["outcomes"],
                width,
            )
            print(
                f"{line} | vol: {volume_text}"
            )
        print("")


def main() -> int:
    args = parse_args()
    global DEBUG_ENABLED, DEBUG_SHOW_LEVEL
    DEBUG_ENABLED = args.debug
    DEBUG_SHOW_LEVEL = not args.debug_no_level

    items = load_config(CONFIG_PATH)
    config_by_key = {item.key: item for item in items}
    events = fetch_temperature_tag_events()
    weather_cache = prefetch_wunderground_weather(events, config_by_key)

    rows = []
    config_updates = 0
    if args.sync_wunderground:
        config_updates = sync_config_wunderground_from_events(CONFIG_PATH, events)
        if config_updates:
            items = load_config(CONFIG_PATH)
            config_by_key = {item.key: item for item in items}
    for event in events:
        rows.append(process_event(event, config_by_key, weather_cache))

    output = {
        "config_path": str(CONFIG_PATH.resolve()),
        "request_lication": [row["key"] for row in rows],
        "weather": [{"key": row["key"], **row["weather"]} for row in rows],
        "polymarket": [
            {"key": row["key"], **row["polymarket"]}
            for row in rows
        ],
        "config_updates": config_updates,
    }
    if args.json:
        print(json.dumps(output, indent=2))
    else:
        print_friendly(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
