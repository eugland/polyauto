"""Polymarket pro-trader position + activity collector.

Fetches positions (snapshot) and activity (deduped) for the watched wallets
and persists them into DuckDB so the weather-pro UI has 3 days of history
without hitting the live API on every page load.

Called from collector.collect_once — positions every 15 min, activity every 30 min.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

log = logging.getLogger("stock.pro_collector")

PRO_WALLETS: dict[str, str] = {
    "haerder":    "0x8dec027d883949a6bfe79842d0ae6b80347e46e0",
    "sin3000":    "0x8d71ff86701227bb479b2039edd92b08f73115d8",
    "aapang":     "0x104171232971a6db8cf938f76fdbebbb81c5f452",
    "auniwarper": "0x0ec451646092f877f80d2b53d5500e50dac05ed3",
}
_POS_LIMITS: dict[str, int] = {
    "haerder": 100, "sin3000": 100, "aapang": 500, "auniwarper": 500,
}
_ACT_LIMITS: dict[str, int] = {
    "haerder": 100, "sin3000": 100, "aapang": 500, "auniwarper": 500,
}
_DATA_API = "https://data-api.polymarket.com"
KEEP_DAYS = 3


def init_pro_schema(con) -> None:
    """Create pro_positions and pro_activity tables if they don't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS pro_positions (
            snapped_at    TIMESTAMP NOT NULL,
            trader        VARCHAR   NOT NULL,
            asset         VARCHAR   NOT NULL,
            condition_id  VARCHAR,
            title         VARCHAR,
            outcome       VARCHAR,
            size          DOUBLE,
            avg_price     DOUBLE,
            cur_price     DOUBLE,
            current_value DOUBLE,
            cash_pnl      DOUBLE,
            initial_value DOUBLE,
            redeemable    BOOLEAN,
            slug          VARCHAR,
            event_slug    VARCHAR,
            PRIMARY KEY(snapped_at, trader, asset)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pro_activity (
            trader        VARCHAR NOT NULL,
            tx_hash       VARCHAR NOT NULL,
            asset         VARCHAR,
            condition_id  VARCHAR,
            title         VARCHAR,
            outcome       VARCHAR,
            side          VARCHAR,
            type          VARCHAR,
            price         DOUBLE,
            size          DOUBLE,
            usdc_size     DOUBLE,
            timestamp     BIGINT,
            slug          VARCHAR,
            event_slug    VARCHAR,
            fetched_at    TIMESTAMP,
            PRIMARY KEY(trader, tx_hash)
        )
    """)


def _get_positions(wallet: str, limit: int) -> list:
    try:
        resp = requests.get(
            f"{_DATA_API}/positions",
            params={"user": wallet, "limit": limit},
            timeout=15,
        )
        data = resp.json() if resp.ok else []
        return data if isinstance(data, list) else []
    except Exception as exc:
        log.warning("positions fetch failed (%s): %s", wallet, exc)
        return []


def _get_activity(wallet: str, limit: int) -> list:
    all_items: list = []
    offset = 0
    page = 500
    while len(all_items) < limit:
        try:
            resp = requests.get(
                f"{_DATA_API}/activity",
                params={"user": wallet, "limit": page, "offset": offset},
                timeout=15,
            )
            if not resp.ok:
                break
            batch = resp.json()
            if not isinstance(batch, list) or not batch:
                break
            all_items.extend(batch)
            if len(batch) < page:
                break
            offset += page
        except Exception as exc:
            log.warning("activity fetch failed (%s): %s", wallet, exc)
            break
    return all_items[:limit]


def snapshot_positions(con) -> int:
    """Take a position snapshot for every watched wallet and insert into DB."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    total = 0
    for trader, wallet in PRO_WALLETS.items():
        positions = _get_positions(wallet, _POS_LIMITS.get(trader, 100))
        rows = []
        for p in positions:
            if not isinstance(p, dict):
                continue
            rows.append((
                now,
                trader,
                p.get("asset") or "",
                p.get("conditionId"),
                p.get("title"),
                p.get("outcome"),
                p.get("size"),
                p.get("avgPrice"),
                p.get("curPrice"),
                p.get("currentValue"),
                p.get("cashPnl"),
                p.get("initialValue"),
                bool(p.get("redeemable", False)),
                p.get("slug"),
                p.get("eventSlug"),
            ))
        if rows:
            con.executemany("""
                INSERT INTO pro_positions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (snapped_at, trader, asset) DO NOTHING
            """, rows)
            total += len(rows)
        log.info("positions snapshot: %s → %d rows", trader, len(rows))
    con.commit()
    return total


def upsert_activity(con) -> int:
    """Fetch recent activity for every wallet and upsert (dedup by tx_hash)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    total = 0
    for trader, wallet in PRO_WALLETS.items():
        items = _get_activity(wallet, _ACT_LIMITS.get(trader, 100))
        rows = []
        for a in items:
            if not isinstance(a, dict):
                continue
            tx = a.get("transactionHash") or ""
            if not tx:
                continue
            rows.append((
                trader,
                tx,
                a.get("asset"),
                a.get("conditionId"),
                a.get("title"),
                a.get("outcome"),
                a.get("side"),
                a.get("type"),
                a.get("price"),
                a.get("size"),
                a.get("usdcSize"),
                a.get("timestamp"),
                a.get("slug"),
                a.get("eventSlug"),
                now,
            ))
        if rows:
            con.executemany("""
                INSERT INTO pro_activity VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (trader, tx_hash) DO NOTHING
            """, rows)
            total += len(rows)
        log.info("activity upsert: %s → %d rows", trader, len(rows))
    con.commit()
    return total


def prune_pro_data(con, keep_days: int = KEEP_DAYS) -> None:
    """Delete activity records older than keep_days (by trade timestamp)."""
    cutoff_epoch = int(
        (datetime.now(timezone.utc).timestamp()) - keep_days * 86400
    )
    con.execute(
        "DELETE FROM pro_activity WHERE timestamp < ?",
        [cutoff_epoch],
    )
    con.commit()
