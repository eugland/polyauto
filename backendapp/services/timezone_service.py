from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, reset_tzpath

from backendapp.domains.models import LocationConfig


def configure_zoneinfo_tzpath() -> None:
    # Always prefer bundled tzdata for consistent behavior across platforms.
    try:
        import tzdata  # type: ignore
    except Exception:
        return
    tzdata_path = Path(tzdata.__file__).resolve().parent / "zoneinfo"
    if tzdata_path.exists():
        reset_tzpath([str(tzdata_path)])


def parse_local_time(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.strip())
    except ValueError:
        return None


def format_local_time(raw: object) -> str:
    dt = parse_local_time(raw)
    if dt is None:
        return "-"
    return dt.strftime("%m-%d %I:%M%p").replace(" 0", " ")


def local_offset_sort_value(raw: object) -> int | None:
    dt = parse_local_time(raw)
    if dt is None:
        return None
    offset = dt.utcoffset()
    if offset is None:
        return None
    return int(offset.total_seconds() // 60)


def build_local_time_now(location_key: str, mapping: dict[str, LocationConfig]) -> str | None:
    info = mapping.get(location_key)
    if info is None:
        print(f"[LOCALTIME] key={location_key} timezone=<missing> local_time=<none>")
        return None

    timezone_name = info.timezone
    offset_minutes = info.utc_offset_minutes

    if not timezone_name:
        if offset_minutes is None:
            print(f"[LOCALTIME] key={location_key} timezone=<missing> local_time=<none>")
            return None
        tz_offset = timezone(timedelta(minutes=offset_minutes))
        local_time = datetime.now(timezone.utc).astimezone(tz_offset).isoformat()
        print(
            f"[LOCALTIME] key={location_key} timezone=<missing> "
            f"fallback_offset_minutes={offset_minutes} local_time={local_time}"
        )
        return local_time

    try:
        local_time = datetime.now(ZoneInfo(timezone_name)).isoformat()
        return local_time
    except Exception as exc:
        if offset_minutes is None:
            print(
                f"[LOCALTIME] key={location_key} timezone={timezone_name} "
                f"error={exc} fallback_offset_minutes=<missing> local_time=<none>"
            )
            return None
        tz_offset = timezone(timedelta(minutes=offset_minutes))
        local_time = datetime.now(timezone.utc).astimezone(tz_offset).isoformat()
        print(
            f"[LOCALTIME] key={location_key} timezone={timezone_name} "
            f"error={exc} fallback_offset_minutes={offset_minutes} local_time={local_time}"
        )
        return local_time
