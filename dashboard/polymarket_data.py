"""Thin wrappers over Polymarket's public data-api (no auth).

Used for both my own wallet and watched "pro" wallets.
  /value     -> portfolio market value
  /positions -> open holdings
  /activity  -> trade history
"""
from __future__ import annotations

import logging
import time

import requests

from dashboard.settings import DATA_API

log = logging.getLogger("dashboard.polymarket")

_TIMEOUT = 12
_PNL_API = "https://user-pnl-api.polymarket.com/user-pnl"

# days requested -> Polymarket (interval, fidelity). Valid intervals are only
# 1d/1w/1m/all (no 1y), so anything past a month pulls "all" and is trimmed.
_PNL_RANGE = [
    (1,  "1d", "1h"),
    (7,  "1w", "1h"),
    (31, "1m", "1d"),
]


def value_history(address: str, days: int = 30) -> list[dict]:
    """P&L curve for ``address`` over ~``days`` (Polymarket user-pnl).

    Returns ``[{ts, value}]`` where value is cumulative profit/loss (USDC).
    Polymarket exposes no portfolio-value time series, so this is the P&L line
    its own profile page charts.
    """
    if not address:
        return []
    interval, fidelity = "all", "1d"
    for lim, iv, fid in _PNL_RANGE:
        if days <= lim:
            interval, fidelity = iv, fid
            break
    try:
        r = requests.get(
            _PNL_API,
            params={"user_address": address, "interval": interval, "fidelity": fidelity},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        out = [
            {"ts": int(x.get("t", 0) or 0), "value": round(float(x.get("p", 0) or 0), 2)}
            for x in (r.json() or [])
        ]
        if interval == "all":  # trim "all" down to the requested window
            cutoff = int(time.time()) - days * 86400
            out = [p for p in out if p["ts"] >= cutoff]
        return out
    except Exception as exc:  # noqa: BLE001
        log.warning("value_history(%s) failed: %s", address[:10], exc)
        return []


_VALUE_CACHE: dict[str, tuple[float, float]] = {}  # address -> (fetched_at, value)
_VALUE_TTL = 60.0


def portfolio_value(address: str) -> float:
    """Market value of all open positions for ``address`` (USDC), cached ~60s."""
    if not address:
        return 0.0
    now = time.time()
    hit = _VALUE_CACHE.get(address)
    if hit and now - hit[0] < _VALUE_TTL:
        return hit[1]
    try:
        r = requests.get(f"{DATA_API}/value", params={"user": address}, timeout=_TIMEOUT)
        r.raise_for_status()
        rows = r.json() or []
        val = round(float(rows[0].get("value", 0.0)), 2) if rows else 0.0
        _VALUE_CACHE[address] = (now, val)
        return val
    except Exception as exc:  # noqa: BLE001
        log.warning("portfolio_value(%s) failed: %s", address[:10], exc)
        return hit[1] if hit else 0.0


_POS_CACHE: dict[str, tuple[float, list[dict]]] = {}  # address -> (fetched_at, rows)
_POS_TTL = 20.0


def positions(address: str) -> list[dict]:
    """Normalized open positions for ``address``, largest value first (cached ~20s)."""
    if not address:
        return []
    now = time.time()
    hit = _POS_CACHE.get(address)
    if hit and now - hit[0] < _POS_TTL:
        return hit[1]
    try:
        r = requests.get(
            f"{DATA_API}/positions",
            params={"user": address, "sizeThreshold": "0.01"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        out: list[dict] = []
        for p in r.json() or []:
            size = float(p.get("size", 0) or 0)
            if size <= 0:
                continue
            out.append({
                "title":       str(p.get("title") or p.get("slug") or "")[:120],
                "slug":        str(p.get("slug") or ""),
                "event_slug":  str(p.get("eventSlug") or ""),
                "outcome":     str(p.get("outcome") or ""),
                "size":        round(size, 2),
                "avg_price":   round(float(p.get("avgPrice", 0) or 0), 4),
                "cur_price":   round(float(p.get("curPrice", 0) or 0), 4),
                "value":       round(float(p.get("currentValue", 0) or 0), 2),
                "pnl":         round(float(p.get("cashPnl", 0) or 0), 2),
                "pnl_pct":     round(float(p.get("percentPnl", 0) or 0), 2),
                "redeemable":  bool(p.get("redeemable", False)),
            })
        out.sort(key=lambda x: x["value"], reverse=True)
        _POS_CACHE[address] = (now, out)
        return out
    except Exception as exc:  # noqa: BLE001
        log.warning("positions(%s) failed: %s", address[:10], exc)
        return hit[1] if hit else []


def activity(address: str, limit: int = 50) -> list[dict]:
    """Recent trade activity for ``address``, newest first."""
    if not address:
        return []
    try:
        r = requests.get(
            f"{DATA_API}/activity",
            params={"user": address, "limit": str(limit)},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        out: list[dict] = []
        for a in r.json() or []:
            out.append({
                "ts":        int(a.get("timestamp", 0) or 0),
                "type":      str(a.get("type") or ""),
                "title":     str(a.get("title") or a.get("slug") or "")[:120],
                "outcome":   str(a.get("outcome") or ""),
                "side":      str(a.get("side") or ""),
                "size":      round(float(a.get("size", 0) or 0), 2),
                "usdc_size": round(float(a.get("usdcSize", 0) or 0), 2),
                "price":     round(float(a.get("price", 0) or 0), 4),
                "tx":        str(a.get("transactionHash") or ""),
            })
        return out
    except Exception as exc:  # noqa: BLE001
        log.warning("activity(%s) failed: %s", address[:10], exc)
        return []
