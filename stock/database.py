"""Read-only DuckDB queries for the Flask UI.

Each function opens a short-lived connection, returns JSON-serialisable data,
and swallows errors by returning `{"error": "..."}`-shaped objects — the UI
renders empty panels rather than crashing when data is unavailable.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

log = logging.getLogger("stock.database")


def _connect(db_path: str):
    import duckdb
    return duckdb.connect(db_path, read_only=False)


def _missing(db_path: str) -> bool:
    return not os.path.exists(db_path)


# ── pair 5d / intraday ────────────────────────────────────────────────────────

def _normalize_series(points: list[tuple[Any, float]]) -> list[dict]:
    if not points:
        return []
    base = points[0][1]
    if base is None or base == 0:
        return [{"t": str(t), "value": v} for t, v in points]
    return [{"t": str(t), "value": (v / base) * 100.0 if v is not None else None}
            for t, v in points]


def query_pair(db_path: str, symbols: list[str], view: str) -> dict:
    if _missing(db_path):
        return {"series": [], "error": "DB not found — start the collector first."}
    try:
        con = _connect(db_path)
        series = []
        for sym in symbols:
            if view == "5d":
                rows = con.execute("""
                    SELECT date, close FROM daily_bars
                    WHERE symbol = ? AND date >= CURRENT_DATE - INTERVAL 10 DAY
                    ORDER BY date
                """, [sym]).fetchall()
            else:  # intraday
                rows = con.execute("""
                    SELECT ts, price FROM intraday_bars
                    WHERE symbol = ? AND ts >= CURRENT_DATE
                    ORDER BY ts
                """, [sym]).fetchall()
            pts = [(r[0], r[1]) for r in rows]
            series.append({"symbol": sym, "points": _normalize_series(pts),
                           "raw_points": [{"t": str(t), "value": v} for t, v in pts]})
        con.close()
        return {"series": series, "normalized": True, "view": view}
    except Exception as exc:
        log.exception("query_pair failed")
        return {"series": [], "error": str(exc)}


# ── bubble chart (S&P 500) ────────────────────────────────────────────────────

def query_bubble(db_path: str) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT q.symbol, u.name, u.sector, q.last, q.pct_change, q.volume
            FROM quotes q
            JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE AND q.last IS NOT NULL AND q.pct_change IS NOT NULL
        """).fetchall()
        con.close()
        return [{"symbol": r[0], "name": r[1], "sector": r[2],
                 "price": r[3], "pct": r[4], "volume": r[5] or 0}
                for r in rows]
    except Exception as exc:
        log.exception("query_bubble failed")
        return []


# ── sectors ───────────────────────────────────────────────────────────────────

def query_sectors(db_path: str) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            WITH sector AS (
                SELECT symbol, name
                FROM universe
                WHERE is_sector_etf = TRUE
            ),
            bars AS (
                SELECT d.symbol, d.date, d.close, d.volume,
                       row_number() OVER (PARTITION BY d.symbol ORDER BY d.date DESC) AS rn
                FROM daily_bars d
                JOIN sector s ON s.symbol = d.symbol
            ),
            latest AS (
                SELECT symbol, close AS last_close, volume AS last_volume
                FROM bars
                WHERE rn = 1
            ),
            prev AS (
                SELECT symbol, close AS prev_close
                FROM bars
                WHERE rn = 2
            )
            SELECT s.symbol, s.name,
                   COALESCE(q.last, l.last_close) AS price,
                   COALESCE(
                       q.pct_change,
                       CASE
                           WHEN p.prev_close > 0 AND l.last_close IS NOT NULL
                           THEN (l.last_close - p.prev_close) / p.prev_close
                       END
                   ) AS pct,
                   COALESCE(q.volume, l.last_volume, 0) AS volume
            FROM sector s
            LEFT JOIN quotes q ON q.symbol = s.symbol
            LEFT JOIN latest l ON l.symbol = s.symbol
            LEFT JOIN prev p ON p.symbol = s.symbol
            ORDER BY pct DESC NULLS LAST, s.symbol
        """).fetchall()
        con.close()
        return [{"etf": r[0], "name": r[1], "price": r[2],
                 "pct": r[3], "volume": r[4] or 0} for r in rows]
    except Exception:
        log.exception("query_sectors failed")
        return []


# ── movers ────────────────────────────────────────────────────────────────────

def query_movers(db_path: str, side: str, limit: int = 10) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        order = "DESC" if side == "up" else "ASC"
        rows = con.execute(f"""
            SELECT q.symbol, u.name, u.sector, q.last, q.pct_change, q.volume
            FROM quotes q
            JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE AND q.pct_change IS NOT NULL
            ORDER BY q.pct_change {order} NULLS LAST
            LIMIT ?
        """, [limit]).fetchall()
        con.close()
        return [{"symbol": r[0], "name": r[1], "sector": r[2],
                 "price": r[3], "pct": r[4], "volume": r[5] or 0} for r in rows]
    except Exception:
        log.exception("query_movers failed")
        return []


# ── macro ─────────────────────────────────────────────────────────────────────

def query_macro(db_path: str) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT q.symbol, u.name, q.last, q.pct_change
            FROM quotes q JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_macro = TRUE
            ORDER BY CASE u.symbol
                WHEN 'SPY' THEN 0 WHEN 'QQQ' THEN 1 WHEN 'IWM' THEN 2
                WHEN 'UUP' THEN 3 WHEN '^TNX' THEN 4 WHEN 'GLD' THEN 5
                WHEN 'USO' THEN 6 WHEN 'BTC-USD' THEN 7 ELSE 99 END
        """).fetchall()
        con.close()
        return [{"symbol": r[0], "label": r[1], "last": r[2], "pct": r[3]}
                for r in rows]
    except Exception:
        log.exception("query_macro failed")
        return []


# ── VIX term structure ────────────────────────────────────────────────────────

def query_vix_term(db_path: str) -> dict:
    if _missing(db_path):
        return {"series": [], "contango_ratio": None}
    try:
        con = _connect(db_path)
        symbols = ("^VIX", "^VIX3M", "^VIX6M")
        series = []
        for sym in symbols:
            rows = con.execute("""
                SELECT ts, last FROM vol_snapshots
                WHERE symbol = ? AND ts >= CURRENT_TIMESTAMP - INTERVAL 5 DAY
                ORDER BY ts
            """, [sym]).fetchall()
            series.append({"symbol": sym,
                           "points": [{"t": str(r[0]), "value": r[1]} for r in rows]})
        latest: list[tuple[str, float]] = []
        for sym in symbols:
            row = con.execute("""
                SELECT symbol, last
                FROM vol_snapshots
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT 1
            """, [sym]).fetchone()
            if row:
                latest.append((row[0], row[1]))
        con.close()
        by_sym = {r[0]: r[1] for r in latest}
        vix = by_sym.get("^VIX")
        vix3m = by_sym.get("^VIX3M")
        contango = (vix3m / vix) if (vix and vix3m and vix > 0) else None
        return {"series": series, "contango_ratio": contango,
                "latest": by_sym}
    except Exception as exc:
        log.exception("query_vix_term failed")
        return {"series": [], "contango_ratio": None, "error": str(exc)}


def query_vvix(db_path: str) -> dict:
    if _missing(db_path):
        return {"points": []}
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT ts, last FROM vol_snapshots
            WHERE symbol = '^VVIX' AND ts >= CURRENT_TIMESTAMP - INTERVAL 5 DAY
            ORDER BY ts
        """).fetchall()
        con.close()
        return {"points": [{"t": str(r[0]), "value": r[1]} for r in rows]}
    except Exception:
        return {"points": []}


# ── breadth ───────────────────────────────────────────────────────────────────

def query_breadth(db_path: str) -> dict:
    if _missing(db_path):
        return {"advancers": 0, "decliners": 0, "unchanged": 0,
                "new_highs": 0, "new_lows": 0}
    try:
        con = _connect(db_path)
        row = con.execute("""
            SELECT
              SUM(CASE WHEN q.pct_change > 0 THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN q.pct_change < 0 THEN 1 ELSE 0 END) AS down,
              SUM(CASE WHEN q.pct_change = 0 THEN 1 ELSE 0 END) AS flat,
              SUM(CASE WHEN q.pct_from_high >= -0.02 THEN 1 ELSE 0 END) AS hi,
              SUM(CASE WHEN q.pct_from_low  <=  0.02 THEN 1 ELSE 0 END) AS lo
            FROM quotes q
            JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE
        """).fetchone()
        con.close()
        return {"advancers": row[0] or 0, "decliners": row[1] or 0,
                "unchanged": row[2] or 0,
                "new_highs": row[3] or 0, "new_lows": row[4] or 0}
    except Exception:
        log.exception("query_breadth failed")
        return {"advancers": 0, "decliners": 0, "unchanged": 0,
                "new_highs": 0, "new_lows": 0}


# ── tracking error (leveraged ETF decay) ──────────────────────────────────────

def query_tracking_error(db_path: str, pair: str, base: str,
                         leverage: float, days: int = 10) -> dict:
    """Compare actual leveraged ETF path against a synthetic compounded version."""
    if _missing(db_path):
        return {"actual": [], "synthetic": []}
    try:
        con = _connect(db_path)
        pair_rows = con.execute("""
            SELECT date, close FROM daily_bars
            WHERE symbol = ? AND date >= CURRENT_DATE - (? || ' DAY')::INTERVAL
            ORDER BY date
        """, [pair, days + 2]).fetchall()
        base_rows = con.execute("""
            SELECT date, close FROM daily_bars
            WHERE symbol = ? AND date >= CURRENT_DATE - (? || ' DAY')::INTERVAL
            ORDER BY date
        """, [base, days + 2]).fetchall()
        con.close()
        if not pair_rows or not base_rows:
            return {"actual": [], "synthetic": []}

        base_by_date = {d: c for d, c in base_rows}
        # Build synthetic path from base daily returns compounded at `leverage`
        base_dates = sorted(base_by_date.keys())
        synth_map: dict = {}
        synth_val = 1.0
        prev_close = None
        for d in base_dates:
            c = base_by_date[d]
            if prev_close and prev_close > 0:
                r = (c - prev_close) / prev_close
                synth_val *= (1.0 + leverage * r)
            synth_map[d] = synth_val
            prev_close = c

        # Rebase both to 100 at the first pair_rows date that has base data
        aligned_dates = [d for d, _ in pair_rows if d in synth_map]
        if not aligned_dates:
            return {"actual": [], "synthetic": []}
        start = aligned_dates[0]
        base_pair = dict(pair_rows).get(start)
        if not base_pair:
            return {"actual": [], "synthetic": []}
        synth_start = synth_map[start]

        actual = []
        synthetic = []
        for d, c in pair_rows:
            if d not in synth_map:
                continue
            actual.append({"t": str(d), "value": (c / base_pair) * 100.0})
            synthetic.append({"t": str(d),
                              "value": (synth_map[d] / synth_start) * 100.0})
        tracking_error_pct = None
        if actual and synthetic:
            tracking_error_pct = actual[-1]["value"] - synthetic[-1]["value"]
        return {"actual": actual, "synthetic": synthetic,
                "tracking_error_pct": tracking_error_pct,
                "pair": pair, "base": base, "leverage": leverage}
    except Exception as exc:
        log.exception("query_tracking_error failed")
        return {"actual": [], "synthetic": [], "error": str(exc)}


# ── screeners ─────────────────────────────────────────────────────────────────

def query_screener(db_path: str, kind: str, limit: int = 20) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        base = """
            SELECT q.symbol, u.name, u.sector, q.last, q.pct_change, q.volume,
                   q.volume_ratio, q.gap_pct, q.pct_from_high, q.pct_from_low,
                   q.premarket_pct, q.postmarket_pct
            FROM quotes q JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE
        """
        if kind == "volume":
            sql = base + " AND q.volume_ratio >= 2.0 ORDER BY q.volume_ratio DESC NULLS LAST LIMIT ?"
            rows = con.execute(sql, [limit]).fetchall()
        elif kind == "gap-up":
            sql = base + " AND q.gap_pct >= 0.03 ORDER BY q.gap_pct DESC NULLS LAST LIMIT ?"
            rows = con.execute(sql, [limit]).fetchall()
        elif kind == "gap-down":
            sql = base + " AND q.gap_pct <= -0.03 ORDER BY q.gap_pct ASC NULLS LAST LIMIT ?"
            rows = con.execute(sql, [limit]).fetchall()
        elif kind == "near-high":
            sql = base + " AND q.pct_from_high IS NOT NULL ORDER BY q.pct_from_high DESC NULLS LAST LIMIT ?"
            rows = con.execute(sql, [limit]).fetchall()
        elif kind == "near-low":
            sql = base + " AND q.pct_from_low IS NOT NULL ORDER BY q.pct_from_low ASC NULLS LAST LIMIT ?"
            rows = con.execute(sql, [limit]).fetchall()
        elif kind == "prepost":
            sql = """
                SELECT q.symbol, u.name, u.sector, q.last, q.pct_change, q.volume,
                       q.volume_ratio, q.gap_pct, q.pct_from_high, q.pct_from_low,
                       q.premarket_pct, q.postmarket_pct
                FROM quotes q JOIN universe u ON u.symbol = q.symbol
                WHERE COALESCE(q.premarket_pct, q.postmarket_pct) IS NOT NULL
                ORDER BY ABS(COALESCE(q.postmarket_pct, q.premarket_pct)) DESC LIMIT ?
            """
            rows = con.execute(sql, [limit]).fetchall()
        else:
            con.close()
            return []
        con.close()
        return [{
            "symbol": r[0], "name": r[1], "sector": r[2], "price": r[3],
            "pct": r[4], "volume": r[5] or 0, "volume_ratio": r[6],
            "gap_pct": r[7], "pct_from_high": r[8], "pct_from_low": r[9],
            "premarket_pct": r[10], "postmarket_pct": r[11],
        } for r in rows]
    except Exception:
        log.exception("query_screener failed")
        return []


# ── correlation ───────────────────────────────────────────────────────────────

def query_correlation(db_path: str, window: int = 20) -> dict:
    if _missing(db_path):
        return {"symbols": [], "matrix": []}
    try:
        import numpy as np
        con = _connect(db_path)
        etfs = con.execute(
            "SELECT symbol FROM universe WHERE is_sector_etf = TRUE ORDER BY symbol"
        ).fetchall()
        symbols = [r[0] for r in etfs]
        series_by_date: dict[str, dict[Any, float]] = {}
        for sym in symbols:
            rows = con.execute("""
                SELECT date, close FROM daily_bars
                WHERE symbol = ? AND date >= CURRENT_DATE - (? || ' DAY')::INTERVAL
                ORDER BY date
            """, [sym, window + 5]).fetchall()
            returns: dict[Any, float] = {}
            for i in range(1, len(rows)):
                d_prev, c_prev = rows[i - 1]
                d_cur, c_cur = rows[i]
                if c_prev and c_prev > 0 and c_cur is not None:
                    returns[d_cur] = (c_cur - c_prev) / c_prev
            series_by_date[sym] = returns
        con.close()

        eligible = [s for s in symbols if len(series_by_date.get(s, {})) >= 3]
        if len(eligible) < 2:
            n = len(eligible)
            return {"symbols": eligible, "matrix": [[None] * n for _ in range(n)]}

        common_dates: set[Any] | None = None
        for s in eligible:
            ds = set(series_by_date[s].keys())
            common_dates = ds if common_dates is None else (common_dates & ds)
        aligned_dates = sorted(common_dates or [])
        if len(aligned_dates) < 3:
            n = len(eligible)
            return {"symbols": eligible, "matrix": [[None] * n for _ in range(n)]}
        aligned_dates = aligned_dates[-window:]

        arr = np.array([
            [series_by_date[s][d] for d in aligned_dates]
            for s in eligible
        ])
        corr = np.corrcoef(arr)
        return {"symbols": eligible, "matrix": corr.tolist(), "window": window,
                "points": len(aligned_dates)}
    except Exception as exc:
        log.exception("query_correlation failed")
        return {"symbols": [], "matrix": [], "error": str(exc)}


# ── vol scatter ───────────────────────────────────────────────────────────────

def query_vol_scatter(db_path: str) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT q.symbol, u.sector, q.volume_ratio, q.pct_change
            FROM quotes q JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE
              AND q.volume_ratio IS NOT NULL AND q.pct_change IS NOT NULL
        """).fetchall()
        con.close()
        return [{"symbol": r[0], "sector": r[1],
                 "volume_ratio": r[2], "pct_change": r[3]} for r in rows]
    except Exception:
        return []


# ── earnings + econ ───────────────────────────────────────────────────────────

def query_earnings(db_path: str, days: int = 7) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT e.symbol, u.name, e.report_date, e.when_reported, e.eps_estimate
            FROM earnings_calendar e
            LEFT JOIN universe u ON u.symbol = e.symbol
            WHERE e.report_date BETWEEN CURRENT_DATE AND CURRENT_DATE + (? || ' DAY')::INTERVAL
            ORDER BY e.report_date, e.symbol
        """, [days]).fetchall()
        con.close()
        return [{"symbol": r[0], "name": r[1], "report_date": str(r[2]),
                 "when_reported": r[3], "eps_estimate": r[4]} for r in rows]
    except Exception:
        log.exception("query_earnings failed")
        return []


def query_econ_events(db_path: str, days: int = 1) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        rows = con.execute("""
            SELECT event_date, event_time, name, importance, forecast, previous, actual
            FROM econ_events
            WHERE event_date BETWEEN CURRENT_DATE AND CURRENT_DATE + (? || ' DAY')::INTERVAL
            ORDER BY event_date, event_time
        """, [days]).fetchall()
        con.close()
        return [{"event_date": str(r[0]), "event_time": r[1], "name": r[2],
                 "importance": r[3], "forecast": r[4],
                 "previous": r[5], "actual": r[6]} for r in rows]
    except Exception:
        return []


def query_health(db_path: str) -> dict:
    """Sanity info for the UI header."""
    if _missing(db_path):
        return {"db": "missing", "quotes": 0, "last_poll": None}
    try:
        con = _connect(db_path)
        q = con.execute("SELECT COUNT(*), MAX(ts) FROM quotes").fetchone()
        d = con.execute("SELECT COUNT(*) FROM daily_bars").fetchone()
        con.close()
        return {"db": "ok", "quotes": q[0], "last_poll": str(q[1]) if q[1] else None,
                "daily_bars": d[0]}
    except Exception as exc:
        return {"db": "error", "error": str(exc)}
