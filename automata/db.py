from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "bets.db"


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS placed_bets (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Core identifiers
                city                     TEXT    NOT NULL,
                icao                     TEXT,
                event_date               TEXT    NOT NULL,   -- YYYY-MM-DD
                question                 TEXT    NOT NULL,
                option                   TEXT    NOT NULL,   -- "No" or "Yes"
                token_id                 TEXT,
                order_id                 TEXT,

                -- Bet sizing
                shares                   REAL    NOT NULL,
                no_price                 REAL,               -- price paid per share (0-1)
                yes_price                REAL,               -- yes-side ask at time of bet
                cost_usdc                REAL,               -- shares × price

                -- Temperature context
                unit                     TEXT,               -- "F" or "C"
                threshold                REAL,               -- lower (or only) temp value in question
                threshold_hi             REAL,               -- upper bound for range questions
                direction                TEXT,               -- "higher" | "below" | "range" | "exact"
                forecast_high            REAL,               -- Open-Meteo forecast high for event date
                forecast_minus_threshold REAL,               -- forecast_high - threshold (margin)

                -- Temporal features
                placed_at_utc            TEXT    NOT NULL,   -- ISO-8601 UTC timestamp
                days_until_event         INTEGER,            -- calendar days from placement to event
                month                    INTEGER,            -- 1-12, seasonality signal

                -- Outcome (filled in post-resolution)
                resolved_temp            REAL,               -- actual recorded temperature
                outcome                  TEXT,               -- "win" | "loss" | null

                UNIQUE(city, event_date, question)
            )
        """)


def already_bet(city: str, event_date: str, question: str) -> bool:
    """Return True if this city/date/question combo was already placed."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM placed_bets WHERE city = ? AND event_date = ? AND question = ?",
            (city, event_date, question),
        ).fetchone()
    return row is not None


def record_bet(
    *,
    city: str,
    icao: str | None,
    event_date: str,
    question: str,
    option: str,
    token_id: str,
    order_id: str,
    shares: float,
    no_price: float,
    yes_price: float | None,
    cost_usdc: float,
    unit: str | None,
    threshold: float | None,
    threshold_hi: float | None,
    direction: str | None,
    forecast_high: float | None,
) -> None:
    now = datetime.now(timezone.utc)
    forecast_minus_threshold = (
        round(forecast_high - threshold, 2)
        if forecast_high is not None and threshold is not None
        else None
    )
    try:
        event_dt = date.fromisoformat(event_date)
        days_until_event = (event_dt - now.date()).days
    except Exception:
        days_until_event = None

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO placed_bets (
                city, icao, event_date, question, option, token_id, order_id,
                shares, no_price, yes_price, cost_usdc,
                unit, threshold, threshold_hi, direction,
                forecast_high, forecast_minus_threshold,
                placed_at_utc, days_until_event, month
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?
            )
            """,
            (
                city, icao, event_date, question, option, token_id, order_id,
                shares, no_price, yes_price, cost_usdc,
                unit, threshold, threshold_hi, direction,
                forecast_high, forecast_minus_threshold,
                now.isoformat(), days_until_event, now.month,
            ),
        )
