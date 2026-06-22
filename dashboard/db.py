"""SQLite store for the dashboard — balance snapshots over time.

DB at db/dashboard.db (gitignored via *.db).
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DB_DIR = _REPO_ROOT / "db"
DB_PATH = _DB_DIR / "dashboard.db"


def _connect() -> sqlite3.Connection:
    _DB_DIR.mkdir(exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balance_snapshots (
                ts        INTEGER PRIMARY KEY,   -- unix seconds
                portfolio REAL NOT NULL,
                cash      REAL,                  -- NULL if creds unavailable
                total     REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_watchlist (
                symbol   TEXT PRIMARY KEY,       -- e.g. NVDA, BTC-USD
                kind     TEXT NOT NULL,          -- 'stock' | 'crypto'
                added_ts INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_pros (
                address  TEXT PRIMARY KEY,       -- watched wallet (lowercased)
                label    TEXT NOT NULL,
                added_ts INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol   TEXT NOT NULL,
                amount   REAL NOT NULL,
                initial  REAL NOT NULL,
                freq     TEXT NOT NULL,
                start    TEXT NOT NULL,
                end      TEXT NOT NULL,
                color    TEXT NOT NULL,
                created_ts INTEGER NOT NULL
            )
            """
        )


def insert_snapshot(portfolio: float, cash: float | None) -> None:
    total = portfolio + (cash or 0.0)
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO balance_snapshots (ts, portfolio, cash, total) "
            "VALUES (?, ?, ?, ?)",
            (int(time.time()), round(portfolio, 2), cash, round(total, 2)),
        )


def add_watchlist(symbol: str) -> dict:
    """Persist a user-added symbol. Crypto if it looks like ``BASE-USD``."""
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("empty symbol")
    kind = "crypto" if symbol.endswith("-USD") else "stock"
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO custom_watchlist (symbol, kind, added_ts) VALUES (?, ?, ?)",
            (symbol, kind, int(time.time())),
        )
    return {"symbol": symbol, "kind": kind}


def remove_watchlist(symbol: str) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM custom_watchlist WHERE symbol = ?", (symbol.strip().upper(),))


def get_watchlist() -> list[dict]:
    """User-added symbols as ``[{symbol, kind}]``, oldest first."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT symbol, kind FROM custom_watchlist ORDER BY added_ts ASC"
        ).fetchall()
    return [{"symbol": s, "kind": k} for s, k in rows]


def add_pro(label: str, address: str) -> dict:
    """Persist a user-added watched wallet. Returns ``{address, label}``."""
    address = address.strip()
    if not address:
        raise ValueError("empty address")
    label = label.strip() or address[:8]
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO custom_pros (address, label, added_ts) VALUES (?, ?, ?)",
            (address.lower(), label, int(time.time())),
        )
    return {"address": address.lower(), "label": label}


def remove_pro(address: str) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM custom_pros WHERE address = ?", (address.strip().lower(),))


def list_pros() -> list[dict]:
    """User-added watched wallets as ``[{address, label}]``, oldest first."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT address, label FROM custom_pros ORDER BY added_ts ASC"
        ).fetchall()
    return [{"address": a, "label": l} for a, l in rows]


def add_backtest_run(d: dict) -> int:
    """Persist one backtest run definition; returns its new id."""
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO backtest_runs (symbol, amount, initial, freq, start, end, color, created_ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(d["symbol"]).upper(), float(d["amount"]), float(d["initial"]),
                str(d["freq"]), str(d["start"]), str(d["end"]), str(d["color"]),
                int(time.time()),
            ),
        )
        return int(cur.lastrowid)


def list_backtest_runs() -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT id, symbol, amount, initial, freq, start, end, color "
            "FROM backtest_runs ORDER BY created_ts ASC"
        ).fetchall()
    cols = ["id", "symbol", "amount", "initial", "freq", "start", "end", "color"]
    return [dict(zip(cols, r)) for r in rows]


def remove_backtest_run(run_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM backtest_runs WHERE id = ?", (run_id,))


def get_history(days: int = 30) -> list[dict]:
    cutoff = int(time.time()) - days * 86400
    with _connect() as conn:
        rows = conn.execute(
            "SELECT ts, portfolio, cash, total FROM balance_snapshots "
            "WHERE ts >= ? ORDER BY ts ASC",
            (cutoff,),
        ).fetchall()
    return [
        {"ts": ts, "portfolio": portfolio, "cash": cash, "total": total}
        for ts, portfolio, cash, total in rows
    ]
