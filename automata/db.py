from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "bets.db"


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS placed_bets (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                city       TEXT    NOT NULL,
                date       TEXT    NOT NULL,
                question   TEXT,
                token_id   TEXT,
                price      REAL,
                size_usdc  REAL,
                order_id   TEXT,
                placed_at  TEXT    NOT NULL,
                UNIQUE(city, date, question)
            )
        """)


def already_bet(city: str, date: str, question: str) -> bool:
    """Return True if this city/date/question combo was already placed."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM placed_bets WHERE city = ? AND date = ? AND question = ?",
            (city, date, question),
        ).fetchone()
    return row is not None


def record_bet(
    city: str,
    date: str,
    question: str,
    token_id: str,
    price: float,
    size_usdc: float,
    order_id: str,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO placed_bets
                (city, date, question, token_id, price, size_usdc, order_id, placed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (city, date, question, token_id, price, size_usdc, order_id,
             datetime.now(timezone.utc).isoformat()),
        )
