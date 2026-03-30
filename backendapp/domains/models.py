from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RuntimeSettings:
    run_host: str
    run_port: int
    run_debug: bool


@dataclass(frozen=True)
class LocationConfig:
    key: str
    station: str
    timezone: str
    utc_offset_minutes: int | None
    enabled: bool = True
    source: dict[str, str] = field(default_factory=dict)
