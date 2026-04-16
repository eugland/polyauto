"""
Open-Meteo ensemble forecast → mean / stdev of daily high → normal-CDF
probability that a threshold is hit.
"""
from __future__ import annotations

import logging
import math
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

_log = logging.getLogger(__name__)

from automata.weather import (
    fetch_city_coords,
    fetch_coords_for_stations,
    fetch_noaa_high,
    fetch_open_meteo_archive_high,
    fetch_open_meteo_high,
    fetch_station_coords,
)
from weather.markets import Bracket, RankedMarket

# Fallback std when ensemble unavailable or collapsed (same unit as market).
_FALLBACK_STD_F = 2.0
_FALLBACK_STD_C = 1.1  # ~equivalent in Celsius

# Floor to prevent absurdly confident probabilities from a tight ensemble.
_MIN_STD_F = 0.5
_MIN_STD_C = 0.28
_FORECAST_CACHE_TTL_SECONDS = 600.0

_FORECAST_CACHE: dict[tuple[str, str, str], tuple[float, tuple[float, float] | None]] = {}
_CITY_COORD_CACHE: dict[str, tuple[float, float] | None] = {}


def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# Ensemble models tried in order. gfs_seamless has global coverage but is US-biased;
# icon_seamless and ecmwf_ifs025 cover Europe/Asia better.
_ENSEMBLE_MODELS = ("gfs_seamless", "icon_seamless", "ecmwf_ifs025")


def fetch_ensemble_high(
    lat: float,
    lon: float,
    date_str: str,
    unit: str,
    timeout: float = 15.0,
) -> tuple[float, float] | None:
    """
    Query Open-Meteo ensemble API; compute per-member daily max over the
    06:00–18:00 local window on `date_str`; return (mean, stdev) across members.
    Tries multiple models — returns the first with usable data.
    """
    data = None
    for model in _ENSEMBLE_MODELS:
        try:
            resp = requests.get(
                "https://ensemble-api.open-meteo.com/v1/ensemble",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "hourly": "temperature_2m",
                    "models": model,
                    "temperature_unit": "fahrenheit" if unit == "F" else "celsius",
                    "timezone": "auto",
                    "start_date": date_str,
                    "end_date": date_str,
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            candidate = resp.json()
        except Exception:
            continue
        hourly = candidate.get("hourly") or {}
        if hourly.get("time") and any(
            k.startswith("temperature_2m_member") for k in hourly
        ):
            data = candidate
            break
    if data is None:
        return None

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return None

    # Indices for 06:00–18:00 local hours on the target date.
    daytime_idx = [i for i, t in enumerate(times) if t.startswith(date_str) and 6 <= int(t[11:13]) <= 18]
    if not daytime_idx:
        return None

    member_highs: list[float] = []
    # Default (deterministic) member.
    base = hourly.get("temperature_2m")
    if isinstance(base, list):
        vals = [base[i] for i in daytime_idx if i < len(base) and base[i] is not None]
        if vals:
            member_highs.append(max(vals))

    # Ensemble members temperature_2m_member01, _02, ...
    for key, series in hourly.items():
        if not key.startswith("temperature_2m_member"):
            continue
        if not isinstance(series, list):
            continue
        vals = [series[i] for i in daytime_idx if i < len(series) and series[i] is not None]
        if vals:
            member_highs.append(max(vals))

    if not member_highs:
        return None
    if len(member_highs) == 1:
        return member_highs[0], 0.0
    return statistics.mean(member_highs), statistics.pstdev(member_highs)


def _clamp_std(std: float, unit: str) -> float:
    return max(std, _MIN_STD_F if unit == "F" else _MIN_STD_C)


def _fallback_std(unit: str) -> float:
    return _FALLBACK_STD_F if unit == "F" else _FALLBACK_STD_C


def p_yes(bracket: Bracket) -> float | None:
    """Normal-CDF probability the actual high triggers YES on this bracket."""
    if bracket.forecast_mean is None or bracket.forecast_std is None:
        return None
    mu = bracket.forecast_mean
    sigma = _clamp_std(bracket.forecast_std, bracket.unit)
    lo = bracket.threshold
    hi = bracket.threshold_hi

    if bracket.direction == "higher":
        return 1.0 - _phi((lo - mu) / sigma)
    if bracket.direction == "below":
        return _phi((lo - mu) / sigma)
    if bracket.direction == "range" and hi is not None:
        return _phi((hi - mu) / sigma) - _phi((lo - mu) / sigma)
    if bracket.direction == "exact":
        # "exactly N° (rounded)" — approximate as ±0.5° band around the integer.
        return _phi((lo + 0.5 - mu) / sigma) - _phi((lo - 0.5 - mu) / sigma)
    return None


def _resolve_coords(ranked: list[RankedMarket]) -> dict[str, tuple[float, float] | None]:
    """
    Resolve each market's city to (lat, lon). Prefers Open-Meteo geocoding
    (reliable worldwide); falls back to aviationweather.gov ICAO lookup.
    Keyed by city name.
    """
    cities = sorted({rm.city for rm in ranked if rm.city})
    coords: dict[str, tuple[float, float] | None] = {c: _CITY_COORD_CACHE.get(c) for c in cities}
    unresolved = [c for c in cities if coords[c] is None]

    if unresolved:
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(fetch_city_coords, c): c for c in unresolved}
            for fut in as_completed(futures):
                city = futures[fut]
                resolved = fut.result()
                coords[city] = resolved
                _CITY_COORD_CACHE[city] = resolved

    # Fallback: ICAO lookup for any city that failed the geocoder.
    missing = [rm for rm in ranked if rm.city in coords and coords[rm.city] is None and rm.icao]
    if missing:
        icao_map = fetch_coords_for_stations(list({rm.icao for rm in missing}))
        for rm in missing:
            ic = icao_map.get(rm.icao)
            if ic is None:
                ic = fetch_station_coords(rm.icao)
            if ic is not None:
                coords[rm.city] = ic
                _CITY_COORD_CACHE[rm.city] = ic

    return coords


def attach_probabilities(ranked: list[RankedMarket]) -> None:
    """
    Fill forecast_mean/std and p_yes on every Bracket in `ranked`.
    One ensemble call per unique (city, date, unit).
    """
    coords = _resolve_coords(ranked)

    now = time.monotonic()
    jobs: dict[tuple[str, str, str], tuple[float, float] | None] = {}
    tasks: list[tuple[str, str, str]] = []
    for rm in ranked:
        if not (rm.city and rm.event_date and coords.get(rm.city)):
            continue
        key = (rm.city, rm.event_date, rm.unit)
        if key not in jobs:
            cached = _FORECAST_CACHE.get(key)
            if cached is not None and cached[0] > now:
                jobs[key] = cached[1]
            else:
                jobs[key] = None
                tasks.append(key)

    def _run(key: tuple[str, str, str]):
        city, date_str, unit = key
        lat, lon = coords[city]

        # 1. Ensemble (gives us a real std).
        result = fetch_ensemble_high(lat, lon, date_str, unit)
        if result is not None and result[1] > 0.0:
            return key, result

        ensemble_mean = result[0] if result is not None else None

        # 2. Point forecast from Open-Meteo (future/today up to 16 days).
        point = fetch_open_meteo_high(lat, lon, date_str, unit)

        # 3. Archive for past dates.
        if point is None:
            point = fetch_open_meteo_archive_high(lat, lon, date_str, unit)

        # 4. NOAA NWS as last resort (US only).
        if point is None:
            point = fetch_noaa_high(lat, lon, date_str, unit)

        mean = point if point is not None else ensemble_mean
        if mean is None:
            return key, None
        return key, (mean, _fallback_std(unit))

    if tasks:
        with ThreadPoolExecutor(max_workers=10) as pool:
            for fut in as_completed([pool.submit(_run, k) for k in tasks]):
                key, result = fut.result()
                jobs[key] = result
                _FORECAST_CACHE[key] = (time.monotonic() + _FORECAST_CACHE_TTL_SECONDS, result)

    no_forecast: list[str] = []
    for rm in ranked:
        key = (rm.city, rm.event_date, rm.unit) if rm.city else None
        forecast = jobs.get(key) if key else None
        if forecast is None:
            if coords.get(rm.city) is None:
                no_forecast.append(f"{rm.city} (no coords)")
            else:
                no_forecast.append(f"{rm.city} (forecast unavailable)")
            continue
        mean, std = forecast
        for br in rm.brackets:
            br.forecast_mean = mean
            br.forecast_std = std
            br.p_yes = p_yes(br)

    if no_forecast:
        _log.warning("No forecast for %d market(s): %s", len(no_forecast), ", ".join(no_forecast))
