from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

OPENWEATHER_GEOCODING_URL = "https://api.openweathermap.org/geo/1.0/direct"
OPENWEATHER_CURRENT_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
OPENWEATHER_API_KEY_ENV = "OPENWEATHER_API_KEY"
REQUEST_TIMEOUT_SECONDS = 20
OPENWEATHER_UNITS = "metric"

# Internal keyword -> OpenWeather geocoding query mapping.
LOCATION_QUERIES: dict[str, str] = {
    "shanghai": "Shanghai,CN",
}
DEFAULT_LOCATIONS_FILE = Path(__file__).resolve().parent.parent / "locations.json"
DEFAULT_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


@dataclass(frozen=True)
class LocationQuery:
    keyword: str
    query: str


@dataclass(frozen=True)
class Coordinates:
    lat: float
    lon: float


def normalize_location_key(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-").replace(" ", "-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized


def raise_for_openweather_error(response: requests.Response) -> None:
    if response.ok:
        return

    message = response.text
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        raw_message = payload.get("message")
        if isinstance(raw_message, str) and raw_message.strip():
            message = raw_message.strip()

    raise requests.HTTPError(
        f"OpenWeather API error {response.status_code}: {message}",
        response=response,
    )


def load_local_env(path: Path = DEFAULT_ENV_FILE) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def get_openweather_api_key() -> str:
    load_local_env()
    api_key = os.getenv(OPENWEATHER_API_KEY_ENV, "").strip()
    if not api_key:
        raise RuntimeError(
            f"Missing {OPENWEATHER_API_KEY_ENV}. Set it in the local .env file or environment."
        )
    return api_key


def resolve_location_query(keyword: str) -> str | None:
    return LOCATION_QUERIES.get(normalize_location_key(keyword))


def load_locations(path: Path = DEFAULT_LOCATIONS_FILE) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("locations.json must contain a JSON object at the top level.")
    return payload


def resolve_location_coordinates(
    location_key: str, path: Path = DEFAULT_LOCATIONS_FILE
) -> Coordinates:
    payload = load_locations(path)
    normalized_key = normalize_location_key(location_key)
    raw_location = payload.get(normalized_key)
    if not isinstance(raw_location, dict):
        raise ValueError(
            f"Location key '{normalized_key}' was not found in locations.json."
        )

    lat = raw_location.get("lat")
    lon = raw_location.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        raise ValueError(
            f"Location key '{normalized_key}' must define numeric 'lat' and 'lon' values."
        )
    return Coordinates(lat=float(lat), lon=float(lon))


def geocode_location(location: LocationQuery, api_key: str) -> dict[str, Any]:
    response = requests.get(
        OPENWEATHER_GEOCODING_URL,
        params={"q": location.query, "limit": 1, "appid": api_key},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    raise_for_openweather_error(response)

    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError(
            f"No OpenWeather geocoding result found for keyword '{location.keyword}'."
        )

    geocoded = payload[0]
    if not isinstance(geocoded, dict):
        raise ValueError(
            f"Unexpected geocoding response for keyword '{location.keyword}'."
        )
    return geocoded


def fetch_current_weather(location: LocationQuery, api_key: str) -> dict[str, Any]:
    geocoded = geocode_location(location, api_key)
    lat = geocoded.get("lat")
    lon = geocoded.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        raise ValueError(
            f"Missing coordinates in geocoding response for keyword '{location.keyword}'."
        )

    response = requests.get(
        OPENWEATHER_CURRENT_WEATHER_URL,
        params={"lat": lat, "lon": lon, "appid": api_key, "units": OPENWEATHER_UNITS},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    raise_for_openweather_error(response)
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(
            f"Unexpected current weather response for keyword '{location.keyword}'."
        )

    temperature_c = None
    main = payload.get("main")
    if isinstance(main, dict):
        raw_temp = main.get("temp")
        if isinstance(raw_temp, (int, float)):
            temperature_c = float(raw_temp)

    weather_description = None
    weather_items = payload.get("weather")
    if isinstance(weather_items, list) and weather_items:
        first = weather_items[0]
        if isinstance(first, dict):
            raw_description = first.get("description")
            if isinstance(raw_description, str):
                weather_description = raw_description

    return {
        "keyword": location.keyword,
        "query": location.query,
        "provider": "openweather",
        "resolved_name": geocoded.get("name"),
        "resolved_state": geocoded.get("state"),
        "resolved_country": geocoded.get("country"),
        "coordinates": {"lat": lat, "lon": lon},
        "units": OPENWEATHER_UNITS,
        "temperature_c": temperature_c,
        "weather_description": weather_description,
    }


def fetch_current_weather_by_coordinates(
    coordinates: Coordinates, api_key: str
) -> dict[str, Any]:
    response = requests.get(
        OPENWEATHER_CURRENT_WEATHER_URL,
        params={
            "lat": coordinates.lat,
            "lon": coordinates.lon,
            "appid": api_key,
            "units": OPENWEATHER_UNITS,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    raise_for_openweather_error(response)
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected current weather response from OpenWeather.")

    temperature_c = None
    main = payload.get("main")
    if isinstance(main, dict):
        raw_temp = main.get("temp")
        if isinstance(raw_temp, (int, float)):
            temperature_c = float(raw_temp)

    weather_description = None
    weather_items = payload.get("weather")
    if isinstance(weather_items, list) and weather_items:
        first = weather_items[0]
        if isinstance(first, dict):
            raw_description = first.get("description")
            if isinstance(raw_description, str):
                weather_description = raw_description

    coord_payload = payload.get("coord")
    resolved_lat = coordinates.lat
    resolved_lon = coordinates.lon
    if isinstance(coord_payload, dict):
        raw_lat = coord_payload.get("lat")
        raw_lon = coord_payload.get("lon")
        if isinstance(raw_lat, (int, float)):
            resolved_lat = float(raw_lat)
        if isinstance(raw_lon, (int, float)):
            resolved_lon = float(raw_lon)

    timezone_offset_seconds = 0
    raw_timezone = payload.get("timezone")
    if isinstance(raw_timezone, (int, float)):
        timezone_offset_seconds = int(raw_timezone)
    location_timezone = timezone(timedelta(seconds=timezone_offset_seconds))

    observed_at_utc = None
    local_time_now = None
    local_date_now = None
    raw_dt = payload.get("dt")
    if isinstance(raw_dt, (int, float)):
        observed_datetime_utc = datetime.fromtimestamp(int(raw_dt), tz=timezone.utc)
        observed_at_utc = observed_datetime_utc.isoformat()
        local_datetime = observed_datetime_utc.astimezone(location_timezone)
        local_time_now = local_datetime.isoformat()
        local_date_now = local_datetime.date().isoformat()

    return {
        "provider": "openweather",
        "units": OPENWEATHER_UNITS,
        "resolved_name": payload.get("name"),
        "resolved_country": (
            payload.get("sys", {}).get("country")
            if isinstance(payload.get("sys"), dict)
            else None
        ),
        "coordinates": {"lat": resolved_lat, "lon": resolved_lon},
        "timezone_offset_seconds": timezone_offset_seconds,
        "observed_at_utc": observed_at_utc,
        "local_time_now": local_time_now,
        "local_date_now": local_date_now,
        "temperature_c": temperature_c,
        "weather_description": weather_description,
    }


def fetch_intraday_forecast(coordinates: Coordinates, api_key: str) -> dict[str, Any]:
    response = requests.get(
        OPENWEATHER_FORECAST_URL,
        params={
            "lat": coordinates.lat,
            "lon": coordinates.lon,
            "appid": api_key,
            "units": OPENWEATHER_UNITS,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    raise_for_openweather_error(response)
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected intraday forecast response from OpenWeather.")

    city = payload.get("city")
    timezone_offset_seconds = 0
    if isinstance(city, dict) and isinstance(city.get("timezone"), (int, float)):
        timezone_offset_seconds = int(city["timezone"])
    location_timezone = timezone(timedelta(seconds=timezone_offset_seconds))

    forecast_points: list[dict[str, Any]] = []
    raw_list = payload.get("list")
    if isinstance(raw_list, list):
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            weather_description = None
            weather_items = item.get("weather")
            if isinstance(weather_items, list) and weather_items:
                first = weather_items[0]
                if isinstance(first, dict):
                    raw_description = first.get("description")
                    if isinstance(raw_description, str):
                        weather_description = raw_description
            main = item.get("main")
            temp_c = None
            feels_like_c = None
            humidity = None
            temp_max_c = None
            temp_min_c = None
            if isinstance(main, dict):
                if isinstance(main.get("temp"), (int, float)):
                    temp_c = float(main["temp"])
                if isinstance(main.get("feels_like"), (int, float)):
                    feels_like_c = float(main["feels_like"])
                if isinstance(main.get("humidity"), (int, float)):
                    humidity = float(main["humidity"])
                if isinstance(main.get("temp_max"), (int, float)):
                    temp_max_c = float(main["temp_max"])
                if isinstance(main.get("temp_min"), (int, float)):
                    temp_min_c = float(main["temp_min"])

            wind_speed = None
            wind = item.get("wind")
            if isinstance(wind, dict) and isinstance(wind.get("speed"), (int, float)):
                wind_speed = float(wind["speed"])

            local_dt = None
            local_date = None
            raw_dt = item.get("dt")
            if isinstance(raw_dt, (int, float)):
                local_datetime = datetime.fromtimestamp(
                    int(raw_dt), tz=timezone.utc
                ).astimezone(location_timezone)
                local_dt = local_datetime.isoformat()
                local_date = local_datetime.date().isoformat()

            forecast_points.append(
                {
                    "dt": raw_dt,
                    "dt_txt": item.get("dt_txt"),
                    "local_dt": local_dt,
                    "local_date": local_date,
                    "temp_c": temp_c,
                    "feels_like_c": feels_like_c,
                    "temp_max_c": temp_max_c,
                    "temp_min_c": temp_min_c,
                    "humidity": humidity,
                    "wind_speed": wind_speed,
                    "weather_description": weather_description,
                    "pop": item.get("pop"),
                }
            )

    daily_max_by_date: dict[str, float] = {}
    for item in forecast_points:
        local_date = item.get("local_date")
        temp_max_c = item.get("temp_max_c")
        if not isinstance(local_date, str):
            continue
        if not isinstance(temp_max_c, (int, float)):
            continue
        current = daily_max_by_date.get(local_date)
        if current is None or float(temp_max_c) > current:
            daily_max_by_date[local_date] = float(temp_max_c)

    return {
        "provider": "openweather",
        "units": OPENWEATHER_UNITS,
        "coordinates": {
            "lat": coordinates.lat,
            "lon": coordinates.lon,
        },
        "timezone_offset_seconds": timezone_offset_seconds,
        "forecast_points": forecast_points,
        "daily_max_by_date": daily_max_by_date,
        "city": city,
    }
