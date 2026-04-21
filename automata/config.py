"""Config loader for tunable settings.

Loads `config.toml` at the repository root once per process. Every getter
takes an env-var name, a TOML (section, key), and a code default — the
resolution order is:

    env var > config.toml > default

Secrets (POLYMARKET_PRIVATE_KEY, RELAYER_API_KEY, CLOB_*) stay in `.env`
and are read directly with `os.environ`. This module is only for tunables
that are safe to git-commit.
"""
from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _REPO_ROOT / "config.toml"

_CONFIG: dict[str, Any] = {}
if _CONFIG_PATH.exists():
    with open(_CONFIG_PATH, "rb") as _f:
        _CONFIG = tomllib.load(_f)


def _toml_get(section: str, key: str) -> Any:
    return _CONFIG.get(section, {}).get(key)


def _env_get(env_name: str) -> str | None:
    val = os.getenv(env_name)
    if val is None or val == "":
        return None
    return val


def get_str(env_name: str, section: str, key: str, default: str) -> str:
    env = _env_get(env_name)
    if env is not None:
        return env
    toml_val = _toml_get(section, key)
    if toml_val is not None:
        return str(toml_val)
    return default


def get_int(env_name: str, section: str, key: str, default: int) -> int:
    env = _env_get(env_name)
    if env is not None:
        return int(env)
    toml_val = _toml_get(section, key)
    if toml_val is not None:
        return int(toml_val)
    return default


def get_float(env_name: str, section: str, key: str, default: float) -> float:
    env = _env_get(env_name)
    if env is not None:
        return float(env)
    toml_val = _toml_get(section, key)
    if toml_val is not None:
        return float(toml_val)
    return default


def get_bool(env_name: str, section: str, key: str, default: bool) -> bool:
    env = _env_get(env_name)
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "on")
    toml_val = _toml_get(section, key)
    if toml_val is not None:
        if isinstance(toml_val, bool):
            return toml_val
        return str(toml_val).strip().lower() in ("1", "true", "yes", "on")
    return default


def get_list_str(
    env_name: str, section: str, key: str, default: list[str]
) -> list[str]:
    """Comma-separated env var, or TOML list/string."""
    env = _env_get(env_name)
    if env is not None:
        return [x.strip() for x in env.split(",") if x.strip()]
    toml_val = _toml_get(section, key)
    if toml_val is not None:
        if isinstance(toml_val, list):
            return [str(x).strip() for x in toml_val if str(x).strip()]
        return [x.strip() for x in str(toml_val).split(",") if x.strip()]
    return list(default)
