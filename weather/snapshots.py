"""
Hourly snapshot recorder for weather markets.

Writes one row per (bracket, hour) into bets.db so the historical evolution
of every market is preserved for post-mortem forecast calibration. Dedupes
within an hour — running the analyzer multiple times inside the same clock
hour is a no-op for already-snapshotted brackets.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from automata.db import DB_PATH
from weather.markets import RankedMarket


def init_snapshot_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_hour_utc    TEXT    NOT NULL,   -- ISO hour bucket, e.g. 2026-04-16T15
                snapshot_at_utc      TEXT    NOT NULL,   -- exact ISO timestamp
                city                 TEXT    NOT NULL,
                icao                 TEXT,
                event_date           TEXT    NOT NULL,
                unit                 TEXT,
                hours_to_decide      REAL,               -- hrs until 3pm local decide
                decide_utc           TEXT,
                forecast_mean        REAL,
                forecast_std         REAL,
                model_threshold      REAL,               -- bracket w/ highest YES bid
                model_yes_bid        REAL,
                bracket_question     TEXT    NOT NULL,
                bracket_threshold    REAL,
                bracket_threshold_hi REAL,
                bracket_direction    TEXT,
                no_ask               REAL,
                no_bid               REAL,
                yes_ask              REAL,
                yes_bid              REAL,
                p_yes                REAL,
                volume_usd           REAL,
                liquidity_usd        REAL,
                resolved_temp        REAL,               -- backfilled later
                outcome              TEXT,               -- backfilled later
                UNIQUE(snapshot_hour_utc, city, event_date, bracket_question)
            )
        """)


def record_snapshots(ranked: list[RankedMarket]) -> int:
    """
    Write one row per (bracket, current-hour) to market_snapshots. Returns
    count of newly inserted rows (duplicates from prior runs this hour are
    silently ignored via INSERT OR IGNORE).
    """
    init_snapshot_db()
    now = datetime.now(timezone.utc)
    hour_bucket = now.strftime("%Y-%m-%dT%H")
    now_iso = now.isoformat()

    rows = []
    for rm in ranked:
        with_yes_bid = [b for b in rm.brackets if b.yes_bid is not None]
        pred = max(with_yes_bid, key=lambda b: b.yes_bid) if with_yes_bid else None
        model_thr = pred.threshold if pred else None
        model_yb = pred.yes_bid if pred else None
        hrs = rm.minutes_until_decide / 60.0 if rm.minutes_until_decide is not None else None
        decide_iso = rm.decide_utc.isoformat() if rm.decide_utc else None
        fmean = rm.brackets[0].forecast_mean if rm.brackets else None
        fstd = rm.brackets[0].forecast_std if rm.brackets else None

        for br in rm.brackets:
            rows.append((
                hour_bucket, now_iso,
                rm.city, rm.icao, rm.event_date, br.unit,
                hrs, decide_iso,
                fmean, fstd,
                model_thr, model_yb,
                br.question, br.threshold, br.direction,
                br.ask, br.bid, br.yes_ask, br.yes_bid,
                br.p_yes,
                br.volume, br.liquidity,
            ))

    if not rows:
        return 0

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.executemany(
            """
            INSERT OR IGNORE INTO market_snapshots (
                snapshot_hour_utc, snapshot_at_utc,
                city, icao, event_date, unit,
                hours_to_decide, decide_utc,
                forecast_mean, forecast_std,
                model_threshold, model_yes_bid,
                bracket_question, bracket_threshold, bracket_direction,
                no_ask, no_bid, yes_ask, yes_bid,
                p_yes,
                volume_usd, liquidity_usd
            ) VALUES (?,?, ?,?,?,?, ?,?, ?,?, ?,?, ?,?,?, ?,?,?,?, ?, ?,?)
            """,
            rows,
        )
        return cur.rowcount
