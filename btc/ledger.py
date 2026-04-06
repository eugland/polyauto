"""
SQLite ledger for BTC monitor trades.

Schema:
    trades
        id              INTEGER PRIMARY KEY
        candle_slug     TEXT
        direction       TEXT       -- UP | DOWN
        entry_price     REAL
        exit_price      REAL       -- NULL until closed
        shares          REAL
        gross_pnl       REAL       -- NULL until closed
        fee             REAL       -- 7.2% taker fee on entry cost
        net_pnl         REAL       -- NULL until closed
        exit_reason     TEXT       -- NULL until closed
        status          TEXT       -- OPEN | CLOSED
        order_id        TEXT       -- buy order id
        entry_time      TEXT       -- ISO UTC
        exit_time       TEXT       -- NULL until closed
        mode            TEXT       -- DRY-RUN | LIVE
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

FEE_RATE = 0.072
DB_PATH  = Path(__file__).resolve().parent / "trades.db"


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            candle_slug TEXT,
            direction   TEXT,
            entry_price REAL,
            exit_price  REAL,
            shares      REAL,
            gross_pnl   REAL,
            fee         REAL,
            net_pnl     REAL,
            exit_reason TEXT,
            status      TEXT DEFAULT 'OPEN',
            order_id    TEXT,
            entry_time  TEXT,
            exit_time   TEXT,
            mode        TEXT
        )
    """)
    con.commit()
    # Migrations
    cols = {r[1] for r in con.execute("PRAGMA table_info(trades)")}
    if "status"   not in cols:
        con.execute("ALTER TABLE trades ADD COLUMN status TEXT DEFAULT 'CLOSED'")
    if "order_id" not in cols:
        con.execute("ALTER TABLE trades ADD COLUMN order_id TEXT")
    con.commit()
    return con


def record_entry(
    candle_slug: str,
    direction:   str,
    entry_price: float,
    shares:      float,
    order_id:    str,
    entry_time:  str,
    mode:        str = "DRY-RUN",
) -> int:
    """Record a new entry immediately. Returns the trade id."""
    fee = round(entry_price * shares * FEE_RATE, 6)
    con = _conn()
    cur = con.execute("""
        INSERT INTO trades
            (candle_slug, direction, entry_price, shares, fee,
             status, order_id, entry_time, mode)
        VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)
    """, (candle_slug, direction, entry_price, shares, fee,
          order_id, entry_time, mode))
    trade_id = cur.lastrowid
    con.commit()
    con.close()
    return trade_id


def record_exit(
    trade_id:    int,
    exit_price:  float,
    exit_reason: str,
) -> float:
    """Update an open trade with exit details. Returns net_pnl."""
    con = _conn()
    row = con.execute(
        "SELECT entry_price, shares, fee FROM trades WHERE id = ?", (trade_id,)
    ).fetchone()

    if not row:
        con.close()
        return 0.0

    entry_price, shares, fee = row
    gross_pnl = round((exit_price - entry_price) * shares, 6)
    net_pnl   = round(gross_pnl - fee, 6)
    exit_time = datetime.now(timezone.utc).isoformat()

    con.execute("""
        UPDATE trades
        SET exit_price=?, gross_pnl=?, net_pnl=?, exit_reason=?,
            status='CLOSED', exit_time=?
        WHERE id=?
    """, (exit_price, gross_pnl, net_pnl, exit_reason, exit_time, trade_id))
    con.commit()
    con.close()
    return net_pnl


def load_open_position() -> dict | None:
    """
    On startup, restore any OPEN trade from the DB.
    Returns a position dict compatible with monitor.py, or None.
    """
    con = _conn()
    row = con.execute("""
        SELECT id, candle_slug, direction, entry_price, shares,
               order_id, entry_time, mode
        FROM trades
        WHERE status = 'OPEN'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    con.close()

    if not row:
        return None

    tid, slug, direction, entry_price, shares, order_id, entry_time, mode = row
    return {
        "active":      True,
        "trade_id":    tid,
        "candle_slug": slug,
        "direction":   direction,
        "entry_price": entry_price,
        "shares":      shares,
        "order_id":    order_id,
        "entry_time":  entry_time,
        "token_id":    None,   # will be re-fetched from market on next cycle
    }


def print_summary():
    con = _conn()

    print("\n  TRADE LEDGER")
    print(f"  DB: {DB_PATH}")
    print(f"  {'-'*90}")

    # Open positions
    open_rows = con.execute("""
        SELECT id, candle_slug, direction, entry_price, shares, entry_time, mode
        FROM trades WHERE status = 'OPEN' ORDER BY id DESC
    """).fetchall()

    if open_rows:
        print(f"  OPEN POSITIONS:")
        for r in open_rows:
            tid, slug, direction, entry, shares, entry_t, mode = r
            print(f"    #{tid}  {slug}  {direction}  entry={entry:.3f}  shares={shares}  [{mode}]  entered={entry_t[:19]}")
        print()

    # Closed trades
    rows = con.execute("""
        SELECT id, candle_slug, direction, entry_price, exit_price,
               shares, net_pnl, exit_reason, exit_time, mode
        FROM trades WHERE status = 'CLOSED'
        ORDER BY id DESC LIMIT 30
    """).fetchall()

    if not rows and not open_rows:
        print("  No trades recorded yet.")
        con.close()
        return

    if rows:
        print(f"  {'#':>3}  {'CANDLE':<44}  {'DIR':<5}  {'ENTRY':>7}  {'EXIT':>7}  {'NET P&L':>9}  {'REASON':<15}  MODE")
        print(f"  {'-'*90}")
        for r in rows:
            tid, slug, direction, entry, exit_p, shares, net, reason, exit_t, mode = r
            print(
                f"  {tid:>3}  {slug:<44}  {direction:<5}  "
                f"{entry:>7.3f}  {(exit_p or 0):>7.3f}  "
                f"{(net or 0):>+9.4f}  {(reason or ''):<15}  {mode}"
            )

    totals = con.execute("""
        SELECT COUNT(*), SUM(net_pnl),
               SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END)
        FROM trades WHERE status = 'CLOSED'
    """).fetchone()
    con.close()

    count, total_net, wins, losses = totals
    if count:
        win_rate = (wins / count * 100) if count else 0
        print(f"  {'-'*90}")
        print(f"  Closed: {count}  |  Wins: {wins}  Losses: {losses}  |  Win rate: {win_rate:.1f}%")
        print(f"  Total net P&L: {(total_net or 0):+.4f} USDC")
    print()


if __name__ == "__main__":
    print_summary()
