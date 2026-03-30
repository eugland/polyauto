from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backendapp.domains.constants import DEFAULT_RUN_DEBUG, DEFAULT_RUN_HOST, DEFAULT_RUN_PORT
from backendapp.domains.models import LocationConfig, RuntimeSettings


def read_config_payload(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_int(raw: Any, default: int) -> int:
    return int(raw) if raw is not None else default


def load_runtime_settings(payload: dict[str, Any]) -> RuntimeSettings:
    settings = payload.get("settings", {})
    return RuntimeSettings(
        run_host=str(settings.get("run_host") or DEFAULT_RUN_HOST).strip() or DEFAULT_RUN_HOST,
        run_port=_parse_int(settings.get("run_port"), DEFAULT_RUN_PORT),
        run_debug=bool(settings.get("run_debug", DEFAULT_RUN_DEBUG)),
    )


def _normalize_source(item: dict[str, Any]) -> dict[str, str]:
    source_payload = item.get("source", {})
    source: dict[str, str] = {}
    for source_key, source_value in source_payload.items():
        source_name = str(source_key or "").strip().lower()
        source_url = str(source_value or "").strip()
        if source_name and source_url:
            source[source_name] = source_url
    return source


def _parse_offset_minutes(raw: Any) -> int | None:
    return int(raw) if raw is not None else None


def load_location_mapping(payload: dict[str, Any]) -> dict[str, LocationConfig]:
    out: dict[str, LocationConfig] = {}
    locations_payload = payload.get("locations", [])

    for value in locations_payload:
        key = str(value.get("key") or "").strip().lower()
        if not key:
            continue
        out[key] = LocationConfig(
            key=key,
            station=str(value.get("station") or "").strip(),
            source=_normalize_source(value),
            timezone=str(value.get("timezone") or "").strip(),
            utc_offset_minutes=_parse_offset_minutes(value.get("utc_offset_minutes")),
            enabled=bool(value.get("enabled", True)),
        )

    return out
