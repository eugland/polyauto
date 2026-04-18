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


def _parse_date_param(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _apply_date_window(rows: list[tuple[Any, ...]], start: date | None,
                       end: date | None, ts_index: int = 0) -> list[tuple[Any, ...]]:
    if start is None and end is None:
        return rows
    out: list[tuple[Any, ...]] = []
    for r in rows:
        t = r[ts_index]
        td = t.date() if isinstance(t, datetime) else t
        if start and td < start:
            continue
        if end and td > end:
            continue
        out.append(r)
    return out


def _weekly_aggregate(rows: list[tuple[Any, float, float | None]]) -> list[tuple[date, float, float]]:
    buckets: dict[date, tuple[date, float, float]] = {}
    for d, close, vol in rows:
        if d is None or close is None:
            continue
        week_key = d - timedelta(days=d.weekday())
        prev = buckets.get(week_key)
        if prev is None:
            buckets[week_key] = (d, close, float(vol or 0))
            continue
        prev_d, _, prev_vol = prev
        last_d = d if d >= prev_d else prev_d
        last_close = close if d >= prev_d else prev[1]
        buckets[week_key] = (last_d, last_close, prev_vol + float(vol or 0))
    ordered = sorted(buckets.items(), key=lambda x: x[0])
    return [(k, v[1], v[2]) for k, v in ordered]


DEFAULT_DIVIDEND_PROFILE = [
    ("MRVL", 0.0037),
    ("MSFU", 310.0),
    ("NVHE", 65.0),
    ("TSLL", 231.0),
    ("TSLY", 36.0),
    ("VDY", 0.2168),
    ("VXUS", 0.0203),
    ("YCON", 10.0),
    ("YNVD", 10.0),
    ("DRAM", 10.0),
    ("FSYD", 20.0),
    ("FTN", 116.0),
    ("HHIS", 90.0),
    ("HODY", 23.0),
    ("HPYE", 10.062),
    ("MHYB", 6.0),
]


def _prev_business_day(d: date) -> date:
    out = d - timedelta(days=1)
    while out.weekday() >= 5:
        out -= timedelta(days=1)
    return out


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(value[:10], fmt).date()
            except Exception:
                continue
    return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _ensure_dividend_tables(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dividend_profiles (
            profile_name VARCHAR PRIMARY KEY,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dividend_holdings (
            profile_name VARCHAR,
            symbol       VARCHAR,
            quantity     DOUBLE,
            PRIMARY KEY(profile_name, symbol)
        )
    """)
    row = con.execute(
        "SELECT 1 FROM dividend_profiles WHERE profile_name='default' LIMIT 1"
    ).fetchone()
    if row:
        return
    now = datetime.utcnow()
    con.execute(
        "INSERT INTO dividend_profiles VALUES ('default', ?, ?)",
        [now, now],
    )
    con.executemany(
        "INSERT INTO dividend_holdings VALUES ('default', ?, ?)",
        DEFAULT_DIVIDEND_PROFILE,
    )
    con.commit()


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

def query_tracking_error(db_path: str, pair: str, base: str, leverage: float,
                         days: int = 10, view: str = "current",
                         start: str | None = None, end: str | None = None) -> dict:
    """Compare actual leveraged ETF path against a synthetic compounded version."""
    if _missing(db_path):
        return {"actual": [], "synthetic": []}
    try:
        con = _connect(db_path)
        view = (view or "current").lower()
        start_d = _parse_date_param(start)
        end_d = _parse_date_param(end)

        if view == "intraday":
            pair_rows_raw = con.execute("""
                SELECT ts, price FROM intraday_bars
                WHERE symbol = ? AND ts >= CURRENT_DATE - INTERVAL 7 DAY
                ORDER BY ts
            """, [pair]).fetchall()
            base_rows_raw = con.execute("""
                SELECT ts, price FROM intraday_bars
                WHERE symbol = ? AND ts >= CURRENT_DATE - INTERVAL 7 DAY
                ORDER BY ts
            """, [base]).fetchall()
            con.close()

            pair_rows_raw = _apply_date_window(pair_rows_raw, start_d, end_d)
            base_rows_raw = _apply_date_window(base_rows_raw, start_d, end_d)
            if start_d is None and end_d is None:
                pair_rows_raw = pair_rows_raw[-900:]
                base_rows_raw = base_rows_raw[-900:]

            pair_rows = [(r[0], r[1]) for r in pair_rows_raw if r[1] is not None]
            base_rows = [(r[0], r[1]) for r in base_rows_raw if r[1] is not None]
        else:
            pair_daily = con.execute("""
                SELECT date, close, volume FROM daily_bars
                WHERE symbol = ? AND date >= CURRENT_DATE - INTERVAL 420 DAY
                ORDER BY date
            """, [pair]).fetchall()
            base_daily = con.execute("""
                SELECT date, close, volume FROM daily_bars
                WHERE symbol = ? AND date >= CURRENT_DATE - INTERVAL 420 DAY
                ORDER BY date
            """, [base]).fetchall()
            con.close()

            pair_daily = _apply_date_window(pair_daily, start_d, end_d)
            base_daily = _apply_date_window(base_daily, start_d, end_d)

            if view == "weekly":
                pair_rows = [(r[0], r[1]) for r in pair_daily if r[1] is not None]
                base_rows = [(r[0], r[1]) for r in base_daily if r[1] is not None]
                if start_d is None and end_d is None:
                    pair_rows = pair_rows[-7:]
                    base_rows = base_rows[-7:]
            else:
                pair_rows = [(r[0], r[1]) for r in pair_daily if r[1] is not None]
                base_rows = [(r[0], r[1]) for r in base_daily if r[1] is not None]
                if start_d is None and end_d is None:
                    pair_rows = pair_rows[-(days + 3):]
                    base_rows = base_rows[-(days + 3):]

        if not pair_rows or not base_rows:
            return {"actual": [], "synthetic": [], "view": view}

        base_by_date = {d: c for d, c in base_rows}
        base_dates = sorted(base_by_date.keys())
        synth_map: dict[Any, float] = {}
        synth_val = 1.0
        prev_close = None
        for d in base_dates:
            c = base_by_date[d]
            if prev_close and prev_close > 0:
                r = (c - prev_close) / prev_close
                synth_val *= (1.0 + leverage * r)
            synth_map[d] = synth_val
            prev_close = c

        aligned_dates = [d for d, _ in pair_rows if d in synth_map]
        if not aligned_dates:
            return {"actual": [], "synthetic": [], "view": view}
        start_key = aligned_dates[0]
        base_pair = dict(pair_rows).get(start_key)
        if not base_pair:
            return {"actual": [], "synthetic": [], "view": view}
        synth_start = synth_map[start_key]

        actual = []
        synthetic = []
        for d, c in pair_rows:
            if d not in synth_map:
                continue
            actual.append({"t": str(d), "value": (c / base_pair) * 100.0})
            synthetic.append({"t": str(d), "value": (synth_map[d] / synth_start) * 100.0})

        tracking_error_pct = None
        if actual and synthetic:
            tracking_error_pct = actual[-1]["value"] - synthetic[-1]["value"]
        return {
            "actual": actual,
            "synthetic": synthetic,
            "tracking_error_pct": tracking_error_pct,
            "pair": pair,
            "base": base,
            "leverage": leverage,
            "view": view,
            "start": str(start_d) if start_d else None,
            "end": str(end_d) if end_d else None,
        }
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
            SELECT e.symbol, u.name, e.report_date, e.when_reported, e.eps_estimate,
                   e.implied_move_pct, e.iv_30d,
                   COALESCE(q.last, e.last_close) AS price,
                   q.pct_change,
                   e.last_surprise_pct, e.hist_avg_move_pct, e.market_cap
            FROM earnings_calendar e
            LEFT JOIN universe u ON u.symbol = e.symbol
            LEFT JOIN quotes q   ON q.symbol = e.symbol
            WHERE e.report_date BETWEEN CURRENT_DATE AND CURRENT_DATE + (? || ' DAY')::INTERVAL
            ORDER BY e.report_date, e.market_cap DESC NULLS LAST, e.symbol
        """, [days]).fetchall()
        con.close()
        return [{
            "symbol": r[0], "name": r[1], "report_date": str(r[2]),
            "when_reported": r[3], "eps_estimate": r[4],
            "implied_move_pct": r[5], "iv_30d": r[6],
            "price": r[7], "pct_change": r[8],
            "last_surprise_pct": r[9], "hist_avg_move_pct": r[10],
            "market_cap": r[11],
        } for r in rows]
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


# ── dividend profiles + schedule ───────────────────────────────────────────────

def query_dividend_profiles(db_path: str) -> list[dict]:
    if _missing(db_path):
        return []
    try:
        con = _connect(db_path)
        _ensure_dividend_tables(con)
        rows = con.execute("""
            SELECT p.profile_name, p.updated_at, COUNT(h.symbol) AS n
            FROM dividend_profiles p
            LEFT JOIN dividend_holdings h ON h.profile_name = p.profile_name
            GROUP BY p.profile_name, p.updated_at
            ORDER BY CASE WHEN p.profile_name = 'default' THEN 0 ELSE 1 END,
                     p.profile_name
        """).fetchall()
        con.close()
        return [{
            "name": r[0],
            "updated_at": str(r[1]) if r[1] else None,
            "holdings_count": r[2] or 0,
        } for r in rows]
    except Exception:
        log.exception("query_dividend_profiles failed")
        return []


def query_dividend_profile(db_path: str, name: str = "default") -> dict:
    if _missing(db_path):
        return {"name": name, "holdings": []}
    try:
        con = _connect(db_path)
        _ensure_dividend_tables(con)
        row = con.execute(
            "SELECT profile_name FROM dividend_profiles WHERE profile_name = ?",
            [name],
        ).fetchone()
        if row is None:
            row = con.execute(
                "SELECT profile_name FROM dividend_profiles ORDER BY profile_name LIMIT 1"
            ).fetchone()
            name = row[0] if row else "default"
        holdings = con.execute("""
            SELECT symbol, quantity
            FROM dividend_holdings
            WHERE profile_name = ?
            ORDER BY symbol
        """, [name]).fetchall()
        con.close()
        return {
            "name": name,
            "holdings": [{"symbol": r[0], "quantity": r[1]} for r in holdings],
        }
    except Exception:
        log.exception("query_dividend_profile failed")
        return {"name": name, "holdings": []}


def upsert_dividend_profile(db_path: str, name: str, holdings: list[dict]) -> dict:
    if _missing(db_path):
        return {"ok": False, "error": "DB missing"}
    name = (name or "").strip().lower() or "default"
    clean: list[tuple[str, float]] = []
    seen: set[str] = set()
    for h in holdings or []:
        sym = str((h or {}).get("symbol", "")).strip().upper()
        qty = _as_float((h or {}).get("quantity"))
        if not sym or qty is None or qty <= 0:
            continue
        if sym in seen:
            continue
        seen.add(sym)
        clean.append((sym, qty))
    try:
        con = _connect(db_path)
        _ensure_dividend_tables(con)
        now = datetime.utcnow()
        con.execute("""
            INSERT INTO dividend_profiles(profile_name, created_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(profile_name) DO UPDATE SET updated_at = excluded.updated_at
        """, [name, now, now])
        con.execute("DELETE FROM dividend_holdings WHERE profile_name = ?", [name])
        if clean:
            con.executemany(
                "INSERT INTO dividend_holdings VALUES (?, ?, ?)",
                [(name, s, q) for s, q in clean],
            )
        con.commit()
        con.close()
        return {"ok": True, "name": name, "count": len(clean)}
    except Exception as exc:
        log.exception("upsert_dividend_profile failed")
        return {"ok": False, "error": str(exc)}


def delete_dividend_profile(db_path: str, name: str) -> dict:
    if _missing(db_path):
        return {"ok": False, "error": "DB missing"}
    profile = (name or "").strip().lower()
    if not profile:
        return {"ok": False, "error": "profile name required"}
    if profile == "default":
        return {"ok": False, "error": "default profile cannot be deleted"}
    try:
        con = _connect(db_path)
        _ensure_dividend_tables(con)
        row = con.execute(
            "SELECT 1 FROM dividend_profiles WHERE profile_name = ? LIMIT 1",
            [profile],
        ).fetchone()
        if row is None:
            con.close()
            return {"ok": False, "error": "profile not found"}
        con.execute("DELETE FROM dividend_holdings WHERE profile_name = ?", [profile])
        con.execute("DELETE FROM dividend_profiles WHERE profile_name = ?", [profile])
        con.commit()
        con.close()
        return {"ok": True, "name": profile}
    except Exception as exc:
        log.exception("delete_dividend_profile failed")
        return {"ok": False, "error": str(exc)}


def _fetch_dividend_snapshot(symbol: str) -> dict:
    import yfinance as yf
    ticker = yf.Ticker(symbol)
    info = {}
    try:
        info = ticker.info or {}
    except Exception:
        info = {}
    div_rate = _as_float(info.get("dividendRate"))
    div_yield = _as_float(info.get("dividendYield"))
    payout = _as_float(info.get("payoutRatio"))
    events: list[dict] = []

    # Upcoming event hints (when available)
    try:
        cal = ticker.calendar
        ex_d = None
        pay_d = None
        if isinstance(cal, dict):
            ex_d = _to_date(cal.get("Ex-Dividend Date"))
            pay_d = _to_date(cal.get("Dividend Date"))
        else:
            if hasattr(cal, "columns"):
                if "Ex-Dividend Date" in cal.columns and len(cal.index):
                    ex_d = _to_date(cal.iloc[0]["Ex-Dividend Date"])
                if "Dividend Date" in cal.columns and len(cal.index):
                    pay_d = _to_date(cal.iloc[0]["Dividend Date"])
        if ex_d:
            events.append({
                "ex_date": ex_d,
                "pay_date": pay_d,
                "amount": None,
                "source": "calendar",
            })
    except Exception:
        pass

    # Historical ex-dividend list
    try:
        divs = ticker.dividends
        if divs is not None and not divs.empty:
            tail = divs.tail(12)
            for idx, amt in tail.items():
                d = _to_date(idx)
                a = _as_float(amt)
                if d and a is not None:
                    events.append({
                        "ex_date": d,
                        "pay_date": None,
                        "amount": a,
                        "source": "history",
                    })
    except Exception:
        pass

    # Deduplicate by ex-date + amount
    dedup: dict[str, dict] = {}
    for e in events:
        key = f"{e.get('ex_date')}|{e.get('amount')}"
        if key not in dedup:
            dedup[key] = e
    events_clean = list(dedup.values())
    events_clean.sort(key=lambda x: x.get("ex_date") or date.min, reverse=True)
    last_hist = next(
        (e for e in events_clean if e.get("source") == "history" and _as_float(e.get("amount")) is not None),
        None,
    )
    last_div_per_share = _as_float(last_hist.get("amount")) if last_hist else None

    return {
        "symbol": symbol,
        "dividend_rate": div_rate,
        "dividend_yield": div_yield,
        "payout_ratio": payout,
        "last_dividend_per_share": last_div_per_share,
        "events": events_clean,
    }


def query_dividend_report(db_path: str, profile: str = "default") -> dict:
    if _missing(db_path):
        return {"profile": profile, "rows": [], "events": []}
    try:
        p = query_dividend_profile(db_path, profile)
        holdings = p.get("holdings", [])
        rows: list[dict] = []
        events: list[dict] = []
        for h in holdings:
            sym = h.get("symbol")
            qty = _as_float(h.get("quantity")) or 0.0
            if not sym or qty <= 0:
                continue
            snap = _fetch_dividend_snapshot(sym)
            per_share = snap.get("last_dividend_per_share")
            if per_share is None:
                per_share = snap.get("dividend_rate")
            per_share = _as_float(per_share)
            est = (per_share * qty) if (per_share is not None) else None
            rows.append({
                "symbol": sym,
                "quantity": qty,
                "dividend_per_share": per_share,
                "est_payout": est,
            })
            for e in snap.get("events", [])[:12]:
                ex_evt = e.get("ex_date")
                amt_evt = _as_float(e.get("amount"))
                if amt_evt is None:
                    continue
                events.append({
                    "symbol": sym,
                    "quantity": qty,
                    "ex_date": str(ex_evt) if ex_evt else None,
                    "amount": amt_evt,
                    "est_payout": (amt_evt * qty) if amt_evt is not None else None,
                    "source": e.get("source"),
                })

        rows.sort(key=lambda r: r["symbol"])
        events.sort(key=lambda r: (r.get("ex_date") or "", r.get("symbol") or ""))
        return {
            "profile": p.get("name", profile),
            "as_of": datetime.utcnow().isoformat(timespec="seconds"),
            "rows": rows,
            "events": events,
        }
    except Exception as exc:
        log.exception("query_dividend_report failed")
        return {"profile": profile, "rows": [], "events": [], "error": str(exc)}


# ── fear & greed index ────────────────────────────────────────────────────────

def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def query_fear_greed(db_path: str) -> dict:
    """Composite 0–100 fear/greed score from 5 sub-indicators.

    0 = extreme fear, 50 = neutral, 100 = extreme greed.
    """
    if _missing(db_path):
        return {"score": None, "components": [], "error": "DB not found."}
    try:
        con = _connect(db_path)
        components: list[dict] = []

        # 1. Momentum — SPY vs 125-day moving average
        spy = con.execute("""
            SELECT date, close FROM daily_bars
            WHERE symbol = 'SPY'
            ORDER BY date DESC LIMIT 130
        """).fetchall()
        if len(spy) >= 20:
            closes = [r[1] for r in spy if r[1] is not None]
            last = closes[0]
            ma = sum(closes[:125]) / min(len(closes), 125)
            dev = (last - ma) / ma if ma else 0.0
            # ±8% above/below MA saturates at 0 or 100
            score = _clip(50.0 + (dev / 0.08) * 50.0)
            components.append({
                "key": "momentum",
                "label": "Momentum (SPY vs 125d MA)",
                "score": round(score, 1),
                "detail": f"{dev*100:+.2f}% vs MA",
            })
        else:
            components.append({"key": "momentum", "label": "Momentum (SPY vs 125d MA)",
                               "score": None, "detail": "no data"})

        # 2. Strength — new 52w highs vs lows among S&P 500
        row = con.execute("""
            SELECT
              SUM(CASE WHEN q.pct_from_high >= -0.02 THEN 1 ELSE 0 END) AS hi,
              SUM(CASE WHEN q.pct_from_low  <=  0.02 THEN 1 ELSE 0 END) AS lo
            FROM quotes q JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE
        """).fetchone()
        hi = row[0] or 0
        lo = row[1] or 0
        total = hi + lo
        if total > 0:
            score = (hi / total) * 100.0
            components.append({"key": "strength", "label": "52w Highs vs Lows",
                               "score": round(score, 1),
                               "detail": f"{hi} highs / {lo} lows"})
        else:
            components.append({"key": "strength", "label": "52w Highs vs Lows",
                               "score": 50.0, "detail": "no extremes"})

        # 3. Breadth — advancers / (adv + dec) among S&P 500
        row = con.execute("""
            SELECT
              SUM(CASE WHEN q.pct_change > 0 THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN q.pct_change < 0 THEN 1 ELSE 0 END) AS down
            FROM quotes q JOIN universe u ON u.symbol = q.symbol
            WHERE u.is_sp500 = TRUE
        """).fetchone()
        up = row[0] or 0
        dn = row[1] or 0
        if up + dn > 0:
            score = (up / (up + dn)) * 100.0
            components.append({"key": "breadth", "label": "Advance / Decline",
                               "score": round(score, 1),
                               "detail": f"{up} up / {dn} down"})
        else:
            components.append({"key": "breadth", "label": "Advance / Decline",
                               "score": 50.0, "detail": "no data"})

        # 4. Volatility — VIX vs 50d MA (inverted: higher VIX = more fear)
        vix = con.execute("""
            SELECT close FROM daily_bars
            WHERE symbol = '^VIX' ORDER BY date DESC LIMIT 50
        """).fetchall()
        if len(vix) >= 10:
            closes = [r[0] for r in vix if r[0] is not None]
            last_vix = closes[0]
            ma = sum(closes) / len(closes)
            dev = (last_vix - ma) / ma if ma else 0.0
            # VIX 30% above MA = max fear (0), 30% below = max greed (100)
            score = _clip(50.0 - (dev / 0.30) * 50.0)
            components.append({"key": "volatility",
                               "label": "VIX vs 50d MA (inverted)",
                               "score": round(score, 1),
                               "detail": f"VIX {last_vix:.2f}, MA {ma:.2f}"})
        else:
            components.append({"key": "volatility",
                               "label": "VIX vs 50d MA (inverted)",
                               "score": None, "detail": "no data"})

        # 5. Safe haven demand — SPY 20d total return
        if len(spy) >= 21:
            last = spy[0][1]
            prior = spy[20][1]
            if prior and prior > 0:
                ret = (last - prior) / prior
                # ±6% in 20 days saturates
                score = _clip(50.0 + (ret / 0.06) * 50.0)
                components.append({"key": "safe_haven",
                                   "label": "SPY 20d Return",
                                   "score": round(score, 1),
                                   "detail": f"{ret*100:+.2f}% 20d"})
            else:
                components.append({"key": "safe_haven", "label": "SPY 20d Return",
                                   "score": 50.0, "detail": "flat base"})
        else:
            components.append({"key": "safe_haven", "label": "SPY 20d Return",
                               "score": None, "detail": "no data"})

        con.close()

        scored = [c["score"] for c in components if c["score"] is not None]
        overall = round(sum(scored) / len(scored), 1) if scored else None

        def _label(s: float | None) -> str:
            if s is None:
                return "Unknown"
            if s < 25: return "Extreme Fear"
            if s < 45: return "Fear"
            if s <= 55: return "Neutral"
            if s <= 75: return "Greed"
            return "Extreme Greed"

        return {"score": overall, "label": _label(overall),
                "components": components}
    except Exception as exc:
        log.exception("query_fear_greed failed")
        return {"score": None, "components": [], "error": str(exc)}


# ── SPY forward volume signal (OBV + accumulation/distribution) ───────────────

def query_spy_volume_signal(db_path: str, view: str = "current",
                            start: str | None = None, end: str | None = None) -> dict:
    """OBV + volume-flow diagnostics on SPY. Returns 60d OBV series + summary.

    On-Balance Volume (OBV) accumulates signed volume: +V on up days, -V on down days.
    Rising OBV on flat/down price signals accumulation (bullish leading); falling OBV
    on flat/up price signals distribution (bearish leading).
    """
    if _missing(db_path):
        return {"error": "DB not found.", "points": []}
    try:
        con = _connect(db_path)
        view = (view or "current").lower()
        start_d = _parse_date_param(start)
        end_d = _parse_date_param(end)

        if view == "intraday":
            rows_raw = con.execute("""
                SELECT ts, price, volume FROM intraday_bars
                WHERE symbol = 'SPY' AND ts >= CURRENT_DATE - INTERVAL 7 DAY
                ORDER BY ts
            """).fetchall()
            con.close()
            rows_raw = _apply_date_window(rows_raw, start_d, end_d)
            if start_d is None and end_d is None:
                rows_raw = rows_raw[-900:]
            rows = [(r[0], r[1], r[2]) for r in rows_raw]
        else:
            daily_rows = con.execute("""
                SELECT date, close, volume FROM daily_bars
                WHERE symbol = 'SPY' ORDER BY date DESC LIMIT 420
            """).fetchall()
            con.close()
            daily_rows = list(reversed(daily_rows))
            daily_rows = _apply_date_window(daily_rows, start_d, end_d)
            if view == "weekly":
                rows = daily_rows
                if start_d is None and end_d is None:
                    rows = rows[-7:]
            else:
                rows = daily_rows
                if start_d is None and end_d is None:
                    rows = rows[-80:]

        if len(rows) < 2:
            return {"error": "not enough SPY history yet", "points": []}

        obv = 0
        obv_series: list[dict] = []
        price_series: list[dict] = []
        prev_close = None
        up_vol = 0
        dn_vol = 0
        up_days = 0
        dn_days = 0
        for d, c, v in rows:
            if prev_close is not None and c is not None and v is not None:
                if c > prev_close:
                    obv += v
                elif c < prev_close:
                    obv -= v
            obv_series.append({"t": str(d), "value": obv})
            price_series.append({"t": str(d), "value": c})
            prev_close = c

        # last 20 trading days flow
        for i in range(max(0, len(rows) - 20), len(rows)):
            if i == 0:
                continue
            prev_c = rows[i - 1][1]
            c = rows[i][1]
            v = rows[i][2] or 0
            if prev_c is None or c is None:
                continue
            if c > prev_c:
                up_vol += v
                up_days += 1
            elif c < prev_c:
                dn_vol += v
                dn_days += 1

        # price & OBV slope (linear fit, last 20)
        def _slope(series: list[dict]) -> float | None:
            pts = [p["value"] for p in series[-20:] if p["value"] is not None]
            if len(pts) < 5:
                return None
            n = len(pts)
            xs = list(range(n))
            mean_x = sum(xs) / n
            mean_y = sum(pts) / n
            num = sum((xs[i] - mean_x) * (pts[i] - mean_y) for i in range(n))
            den = sum((x - mean_x) ** 2 for x in xs)
            if den == 0:
                return None
            # normalize by mean so we can compare obv-slope vs price-slope
            return (num / den) / mean_y if mean_y else None

        obv_slope = _slope(obv_series)
        price_slope = _slope(price_series)

        # divergence signal
        signal = "Neutral"
        reason = ""
        if obv_slope is not None and price_slope is not None:
            # thresholds: slopes are per-day normalized returns
            p_up = price_slope > 0.0005
            p_dn = price_slope < -0.0005
            o_up = obv_slope > 0.002
            o_dn = obv_slope < -0.002
            if o_up and not p_up:
                signal = "Accumulation"
                reason = "OBV rising on flat/down price — buyers stepping in"
            elif o_dn and not p_dn:
                signal = "Distribution"
                reason = "OBV falling on flat/up price — selling into strength"
            elif o_up and p_up:
                signal = "Bullish Confirm"
                reason = "OBV + price both trending up"
            elif o_dn and p_dn:
                signal = "Bearish Confirm"
                reason = "OBV + price both trending down"
            else:
                signal = "Neutral"
                reason = "no clear volume-flow bias"

        # today's volume ratio — use latest available day
        last_v = rows[-1][2] or 0
        prior_vols = [r[2] for r in rows[-21:-1] if r[2] is not None]
        avg_vol_20 = sum(prior_vols) / len(prior_vols) if prior_vols else None
        vol_ratio = (last_v / avg_vol_20) if avg_vol_20 else None

        up_dn_ratio = None
        if dn_vol > 0:
            up_dn_ratio = up_vol / dn_vol
        elif up_vol > 0:
            up_dn_ratio = float("inf")

        if start_d is None and end_d is None:
            keep = 390 if view == "intraday" else 60
            out_obv = obv_series[-keep:]
            out_px = price_series[-keep:]
        else:
            out_obv = obv_series
            out_px = price_series

        return {
            "points": out_obv,
            "price_points": out_px,
            "signal": signal,
            "reason": reason,
            "obv_slope": obv_slope,
            "price_slope": price_slope,
            "volume_ratio": vol_ratio,
            "up_vol": up_vol,
            "down_vol": dn_vol,
            "up_days": up_days,
            "down_days": dn_days,
            "up_down_ratio": up_dn_ratio if up_dn_ratio != float("inf") else None,
            "view": view,
            "start": str(start_d) if start_d else None,
            "end": str(end_d) if end_d else None,
        }
    except Exception as exc:
        log.exception("query_spy_volume_signal failed")
        return {"error": str(exc), "points": []}


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
