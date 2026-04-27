"""Config loader for the restream package.

Reads the repo-root `config.toml` at import time and exposes typed getters.
Precedence:  environment variable  >  [restream] section of config.toml  >  default.

Only secrets (TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET) live in restream/.env.
Everything else is a tunable — edit the TOML.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib  # Python 3.10 fallback (installed via tomli)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _REPO_ROOT / "config.toml"
_SECTION = "restream"

_CONFIG: dict[str, Any] = {}
if _CONFIG_PATH.exists():
    with open(_CONFIG_PATH, "rb") as _f:
        _CONFIG = tomllib.load(_f)


def _toml_get(key: str) -> Any:
    return _CONFIG.get(_SECTION, {}).get(key)


def _env_get(env_name: str) -> str | None:
    val = os.getenv(env_name)
    return val if val else None


def get_str(env_name: str, key: str, default: str) -> str:
    env = _env_get(env_name)
    if env is not None:
        return env
    t = _toml_get(key)
    if t is not None:
        return str(t)
    return default


def get_int(env_name: str, key: str, default: int) -> int:
    env = _env_get(env_name)
    if env is not None:
        return int(env)
    t = _toml_get(key)
    if t is not None:
        return int(t)
    return default


def get_bool(env_name: str, key: str, default: bool) -> bool:
    env = _env_get(env_name)
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "on")
    t = _toml_get(key)
    if t is not None:
        if isinstance(t, bool):
            return t
        return str(t).strip().lower() in ("1", "true", "yes", "on")
    return default
