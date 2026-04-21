"""yfinance-backed data collector for the stock dashboard.

Runs a blocking loop in a background thread: pulls batched daily + intraday bars,
computes screener metrics, upserts into DuckDB at the configured path.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Iterable

import duckdb

from . import universe

log = logging.getLogger("stock.collector")


# ── DB schema ─────────────────────────────────────────────────────────────────

def init_db(path: str | Path) -> duckdb.DuckDBPyConnection:
    from .pro_collector import init_pro_schema
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))

    con.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            symbol         VARCHAR PRIMARY KEY,
            name           VARCHAR,
            sector         VARCHAR,
            is_sp500       BOOLEAN,
            is_pair        BOOLEAN,
            is_sector_etf  BOOLEAN,
            is_macro       BOOLEAN,
            is_vol         BOOLEAN,
            updated_at     TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            symbol  VARCHAR,
            date    DATE,
            open    DOUBLE,
            high    DOUBLE,
            low     DOUBLE,
            close   DOUBLE,
            volume  BIGINT,
            PRIMARY KEY(symbol, date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS intraday_bars (
            symbol  VARCHAR,
            ts      TIMESTAMP,
            price   DOUBLE,
            volume  BIGINT,
            PRIMARY KEY(symbol, ts)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            symbol          VARCHAR PRIMARY KEY,
            ts              TIMESTAMP,
            last            DOUBLE,
            prev_close      DOUBLE,
            pct_change      DOUBLE,
            volume          BIGINT,
            avg_volume_20d  DOUBLE,
            volume_ratio    DOUBLE,
            gap_pct         DOUBLE,
            high_52w        DOUBLE,
            low_52w         DOUBLE,
            pct_from_high   DOUBLE,
            pct_from_low    DOUBLE,
            premarket_pct   DOUBLE,
            postmarket_pct  DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS vol_snapshots (
            ts     TIMESTAMP,
            symbol VARCHAR,
            last   DOUBLE,
            PRIMARY KEY(ts, symbol)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_calendar (
            symbol        VARCHAR,
            report_date   DATE,
            when_reported VARCHAR,
            eps_estimate  DOUBLE,
            eps_actual    DOUBLE,
            PRIMARY KEY(symbol, report_date)
        )
    """)
    # Additive schema migration — safe to re-run
    for col, typ in [
        ("implied_move_pct",  "DOUBLE"),
        ("iv_30d",            "DOUBLE"),
        ("last_close",        "DOUBLE"),
        ("last_surprise_pct", "DOUBLE"),
        ("hist_avg_move_pct", "DOUBLE"),
        ("market_cap",        "BIGINT"),
    ]:
        con.execute(
            f"ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS {col} {typ}"
        )
    con.execute("""
        CREATE TABLE IF NOT EXISTS econ_events (
            event_date DATE,
            event_time VARCHAR,
            name       VARCHAR,
            importance VARCHAR,
            forecast   VARCHAR,
            previous   VARCHAR,
            actual     VARCHAR,
            PRIMARY KEY(event_date, event_time, name)
        )
    """)
    init_pro_schema(con)
    return con


def seed_universe(con: duckdb.DuckDBPyConnection, sp500: list[tuple[str, str, str]]) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute("DELETE FROM universe")
    rows: list[tuple] = []
    for sym, name, sector in sp500:
        rows.append((sym, name, sector, True, False, False, False, False, now))
    for t in universe.all_static_tickers():
        rows.append((t.symbol, t.name, t.sector, False, t.is_pair,
                     t.is_sector_etf, t.is_macro, t.is_vol, now))
    # de-dupe by symbol (pair TSLA overlap with S&P)
    seen: dict[str, tuple] = {}
    for r in rows:
        prev = seen.get(r[0])
        if prev is None:
            seen[r[0]] = r
        else:
            # preserve flags: OR bools, take non-empty name/sector
            merged = (
                r[0],
                prev[1] or r[1],
                prev[2] or r[2],
                prev[3] or r[3],
                prev[4] or r[4],
                prev[5] or r[5],
                prev[6] or r[6],
                prev[7] or r[7],
                now,
            )
            seen[r[0]] = merged
    con.executemany(
        "INSERT INTO universe VALUES (?,?,?,?,?,?,?,?,?)",
        list(seen.values()),
    )
    con.commit()


# ── yfinance helpers ──────────────────────────────────────────────────────────

def _yf_download(symbols: list[str], **kwargs):
    """Thin wrapper over yfinance.download with sane defaults.

    Returns a pandas DataFrame — possibly with MultiIndex columns when len(symbols)>1.
    """
    import yfinance as yf
    kwargs.setdefault("progress", False)
    kwargs.setdefault("threads", False)  # Windows + curl_cffi can stall with threads=True
    kwargs.setdefault("auto_adjust", False)
    kwargs.setdefault("group_by", "ticker")
    kwargs.setdefault("timeout", 30)
    return yf.download(symbols, **kwargs)


def _iter_symbol_frames(df, symbols: list[str]):
    """Yield (symbol, sub_df) pairs from a yfinance.download result."""
    import pandas as pd
    if df is None or df.empty:
        return
    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            if sym in df.columns.get_level_values(0):
                sub = df[sym].dropna(how="all")
                if not sub.empty:
                    yield sym, sub
    else:
        # single-ticker fallback
        yield symbols[0], df.dropna(how="all")


# ── Writers ───────────────────────────────────────────────────────────────────

def upsert_daily_bars(con: duckdb.DuckDBPyConnection, symbol: str, sub) -> int:
    rows: list[tuple] = []
    for ts, row in sub.iterrows():
        try:
            d = ts.date() if hasattr(ts, "date") else ts
            o = float(row.get("Open")) if row.get("Open") is not None else None
            h = float(row.get("High")) if row.get("High") is not None else None
            lo = float(row.get("Low")) if row.get("Low") is not None else None
            c = float(row.get("Close")) if row.get("Close") is not None else None
            v = int(row.get("Volume")) if row.get("Volume") is not None else None
        except Exception:
            continue
        if c is None:
            continue
        rows.append((symbol, d, o, h, lo, c, v))
    if not rows:
        return 0
    con.executemany("""
        INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume
    """, rows)
    return len(rows)


def upsert_intraday_bars(con: duckdb.DuckDBPyConnection, symbol: str, sub) -> int:
    rows: list[tuple] = []
    for ts, row in sub.iterrows():
        try:
            ts_py = ts.to_pydatetime().replace(tzinfo=None) if hasattr(ts, "to_pydatetime") else ts
        except Exception:
            continue
        close = row.get("Close")
        if close is None:
            continue
        try:
            price = float(close)
        except Exception:
            continue
        vol = row.get("Volume")
        try:
            vol_i = int(vol) if vol is not None else 0
        except Exception:
            vol_i = 0
        rows.append((symbol, ts_py, price, vol_i))
    if not rows:
        return 0
    con.executemany("""
        INSERT INTO intraday_bars VALUES (?,?,?,?)
        ON CONFLICT (symbol, ts) DO UPDATE SET
            price=excluded.price, volume=excluded.volume
    """, rows)
    return len(rows)


def prune_intraday(con: duckdb.DuckDBPyConnection) -> None:
    # Keep 7 days so the UI's 5D view (intraday granularity) has full data.
    con.execute("DELETE FROM intraday_bars WHERE ts < CURRENT_DATE - INTERVAL 7 DAY")


def prune_daily(con: duckdb.DuckDBPyConnection, keep_days: int = 1850) -> None:
    # 5y + slack so the 5Y view in the UI is never front-edge truncated.
    con.execute(f"DELETE FROM daily_bars WHERE date < CURRENT_DATE - INTERVAL {keep_days} DAY")


def prune_vol(con: duckdb.DuckDBPyConnection, keep_days: int = 60) -> None:
    con.execute(f"DELETE FROM vol_snapshots WHERE ts < CURRENT_TIMESTAMP - INTERVAL {keep_days} DAY")


# ── Steps ─────────────────────────────────────────────────────────────────────

def refresh_daily_bars(con: duckdb.DuckDBPyConnection, symbols: list[str],
                       on_chunk=None, period: str = "300d") -> int:
    """Pull daily history for all symbols (batched).

    `period` is forwarded to yfinance: "300d" is enough for the 1Y dashboard
    view, pass "5y" for pair/SPY tickers that back the 5Y toggle.
    """
    chunk = 50
    total = 0
    n_chunks = (len(symbols) + chunk - 1) // chunk
    for ci, i in enumerate(range(0, len(symbols), chunk), start=1):
        batch = symbols[i:i + chunk]
        t0 = time.time()
        try:
            df = _yf_download(batch, period=period, interval="1d")
        except Exception as exc:
            log.warning("daily chunk %d/%d failed: %s", ci, n_chunks, exc)
            continue
        chunk_rows = 0
        for sym, sub in _iter_symbol_frames(df, batch):
            try:
                chunk_rows += upsert_daily_bars(con, sym, sub)
            except Exception as exc:
                log.warning("upsert_daily_bars(%s) failed: %s", sym, exc)
        con.commit()
        total += chunk_rows
        log.info("daily chunk %d/%d done: %d rows in %.1fs (total %d)",
                 ci, n_chunks, chunk_rows, time.time() - t0, total)
        if on_chunk is not None:
            try:
                on_chunk(ci, n_chunks)
            except Exception:
                log.exception("on_chunk callback failed")
    return total


def refresh_intraday_pairs(con: duckdb.DuckDBPyConnection, symbols: list[str]) -> int:
    # yfinance caps 1m data at 7d. Pull 7d so the UI's 5D toggle has a full
    # window even on Monday morning (weekends compress the available bars).
    try:
        df = _yf_download(symbols, period="7d", interval="1m", prepost=True)
    except Exception as exc:
        log.warning("intraday batch failed: %s", exc)
        return 0
    total = 0
    for sym, sub in _iter_symbol_frames(df, symbols):
        try:
            total += upsert_intraday_bars(con, sym, sub)
        except Exception as exc:
            log.warning("upsert_intraday_bars(%s) failed: %s", sym, exc)
    con.commit()
    return total


def refresh_vol_snapshot(con: duckdb.DuckDBPyConnection) -> int:
    symbols = [t.symbol for t in universe.VOL_TICKERS]
    try:
        df = _yf_download(symbols, period="2d", interval="1m")
    except Exception as exc:
        log.warning("vol fetch failed: %s", exc)
        return 0
    import pandas as pd
    ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows: list[tuple] = []
    for sym, sub in _iter_symbol_frames(df, symbols):
        close = sub["Close"].dropna()
        if close.empty:
            continue
        rows.append((ts, sym, float(close.iloc[-1])))
    if not rows:
        return 0
    con.executemany("""
        INSERT INTO vol_snapshots VALUES (?,?,?)
        ON CONFLICT (ts, symbol) DO UPDATE SET last=excluded.last
    """, rows)
    con.commit()
    return len(rows)


def refresh_quotes(con: duckdb.DuckDBPyConnection) -> int:
    """Compute per-symbol quote row from daily_bars + today's intraday (if pair)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    # Everything we need in a single SQL query: for each symbol, pull the last 260
    # trading days from daily_bars, plus compute 52w high/low, 20d avg volume, latest
    # close, prior close, gap, etc.
    rows = con.execute("""
        WITH last_bars AS (
            SELECT symbol, date, open, high, low, close, volume,
                   row_number() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
            FROM daily_bars
        ),
        latest AS (
            SELECT symbol, date AS last_date, open AS today_open, close AS last_close,
                   volume AS today_volume
            FROM last_bars WHERE rn = 1
        ),
        prev AS (
            SELECT symbol, close AS prev_close
            FROM last_bars WHERE rn = 2
        ),
        stats AS (
            SELECT symbol,
                   MAX(high) FILTER (WHERE rn <= 252) AS high_52w,
                   MIN(low)  FILTER (WHERE rn <= 252) AS low_52w,
                   AVG(CAST(volume AS DOUBLE)) FILTER (WHERE rn BETWEEN 2 AND 21) AS avg_vol_20d
            FROM last_bars
            GROUP BY symbol
        )
        SELECT l.symbol, l.today_open, l.last_close, l.today_volume,
               p.prev_close, s.high_52w, s.low_52w, s.avg_vol_20d
        FROM latest l
        LEFT JOIN prev p ON p.symbol = l.symbol
        LEFT JOIN stats s ON s.symbol = l.symbol
    """).fetchall()

    out: list[tuple] = []
    for (sym, today_open, last_close, today_volume,
         prev_close, high_52w, low_52w, avg_vol_20d) in rows:
        if last_close is None:
            continue
        pct_change = None
        if prev_close and prev_close > 0:
            pct_change = (last_close - prev_close) / prev_close
        volume_ratio = None
        if avg_vol_20d and avg_vol_20d > 0 and today_volume is not None:
            volume_ratio = today_volume / avg_vol_20d
        gap_pct = None
        if prev_close and prev_close > 0 and today_open is not None:
            gap_pct = (today_open - prev_close) / prev_close
        pct_from_high = None
        if high_52w and high_52w > 0:
            pct_from_high = (last_close - high_52w) / high_52w
        pct_from_low = None
        if low_52w and low_52w > 0:
            pct_from_low = (last_close - low_52w) / low_52w
        out.append((
            sym, now, last_close, prev_close, pct_change, today_volume,
            avg_vol_20d, volume_ratio, gap_pct, high_52w, low_52w,
            pct_from_high, pct_from_low, None, None,
        ))

    if not out:
        return 0
    con.execute("DELETE FROM quotes")
    con.executemany(
        "INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        out,
    )

    # Extended-hours % from pair intraday_bars (if we have them)
    try:
        _update_prepost_pct(con)
    except Exception as exc:
        log.warning("prepost pct update failed: %s", exc)

    con.commit()
    return len(out)


def _update_prepost_pct(con: duckdb.DuckDBPyConnection) -> None:
    """Compute premarket_pct / postmarket_pct for pair tickers from intraday_bars."""
    pairs = [t.symbol for t in universe.PAIR_TICKERS]
    if not pairs:
        return
    placeholders = ",".join("?" for _ in pairs)
    rows = con.execute(f"""
        WITH bars AS (
            SELECT ib.symbol, ib.ts, ib.price, q.prev_close
            FROM intraday_bars ib
            JOIN quotes q ON q.symbol = ib.symbol
            WHERE ib.symbol IN ({placeholders})
              AND ib.ts >= CURRENT_DATE - INTERVAL 1 DAY
        ),
        pre AS (
            SELECT symbol, LAST(price ORDER BY ts) AS px, ANY_VALUE(prev_close) AS prev_close
            FROM bars
            WHERE (EXTRACT(HOUR FROM ts) < 9 OR (EXTRACT(HOUR FROM ts) = 9 AND EXTRACT(MINUTE FROM ts) < 30))
            GROUP BY symbol
        ),
        post AS (
            SELECT symbol, LAST(price ORDER BY ts) AS px, ANY_VALUE(prev_close) AS prev_close
            FROM bars
            WHERE EXTRACT(HOUR FROM ts) >= 16
            GROUP BY symbol
        )
        SELECT symbol, (px - prev_close) / prev_close AS pct FROM pre WHERE prev_close > 0
        UNION ALL
        SELECT symbol, (px - prev_close) / prev_close AS pct FROM post WHERE prev_close > 0
    """, pairs).fetchall()
    # We lose which pct is which — redo split properly
    for sym in pairs:
        pre = con.execute("""
            SELECT ib.price, q.prev_close FROM intraday_bars ib
            JOIN quotes q ON q.symbol = ib.symbol
            WHERE ib.symbol = ? AND ib.ts >= CURRENT_DATE
              AND (EXTRACT(HOUR FROM ib.ts) < 9
                   OR (EXTRACT(HOUR FROM ib.ts) = 9 AND EXTRACT(MINUTE FROM ib.ts) < 30))
            ORDER BY ib.ts DESC LIMIT 1
        """, [sym]).fetchone()
        post = con.execute("""
            SELECT ib.price, q.prev_close FROM intraday_bars ib
            JOIN quotes q ON q.symbol = ib.symbol
            WHERE ib.symbol = ? AND ib.ts >= CURRENT_DATE
              AND EXTRACT(HOUR FROM ib.ts) >= 16
            ORDER BY ib.ts DESC LIMIT 1
        """, [sym]).fetchone()
        pre_pct = (pre[0] - pre[1]) / pre[1] if pre and pre[1] else None
        post_pct = (post[0] - post[1]) / post[1] if post and post[1] else None
        con.execute(
            "UPDATE quotes SET premarket_pct=?, postmarket_pct=? WHERE symbol=?",
            [pre_pct, post_pct, sym],
        )


# ── Earnings + Econ calendar ──────────────────────────────────────────────────

def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except Exception:
            return None
    return None


def _coerce_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _extract_calendar_entries(cal) -> list[tuple[date, float | None]]:
    out: list[tuple[date, float | None]] = []
    if cal is None:
        return out
    if isinstance(cal, dict):
        raw = cal.get("Earnings Date")
        eps_est = _coerce_float(cal.get("Earnings Average"))
        values = raw if isinstance(raw, (list, tuple)) else [raw]
        for v in values:
            d = _coerce_date(v)
            if d:
                out.append((d, eps_est))
        return out
    try:
        if getattr(cal, "empty", True):
            return out
        if "Earnings Date" in cal.columns:
            eps_col = None
            for c in ("Earnings Average", "EPS Estimate", "EPS Estimate Current Year"):
                if c in cal.columns:
                    eps_col = c
                    break
            for _, row in cal.iterrows():
                d = _coerce_date(row.get("Earnings Date"))
                if d:
                    out.append((d, _coerce_float(row.get(eps_col)) if eps_col else None))
            return out
        first = cal.iloc[0, 0] if cal.shape[0] else None
        d = _coerce_date(first)
        if d:
            out.append((d, None))
    except Exception:
        return out
    return out


def _extract_earnings_for_symbol(ticker, lookahead_days: int) -> list[tuple[date, float | None]]:
    today = date.today()
    cutoff = today + timedelta(days=lookahead_days)
    seen: set[date] = set()
    out: list[tuple[date, float | None]] = []

    try:
        edf = ticker.get_earnings_dates(limit=8)
        if edf is not None and not edf.empty:
            eps_col = None
            for c in ("EPS Estimate", "epsEstimate", "EPS Estimate Current Year"):
                if c in edf.columns:
                    eps_col = c
                    break
            for idx, row in edf.iterrows():
                d = _coerce_date(idx)
                if not d or d < today or d > cutoff or d in seen:
                    continue
                seen.add(d)
                out.append((d, _coerce_float(row.get(eps_col)) if eps_col else None))
    except Exception:
        pass

    try:
        for d, eps_est in _extract_calendar_entries(ticker.calendar):
            if d < today or d > cutoff or d in seen:
                continue
            seen.add(d)
            out.append((d, eps_est))
    except Exception:
        pass

    out.sort(key=lambda x: x[0])
    return out


def _fetch_implied_move_iv(ticker, report_date: date, last_close: float | None) -> dict:
    """Compute implied post-earnings move % + 30d ATM IV from options chain.

    Picks the earliest option expiry on or after `report_date`. ATM = strike
    closest to last_close. Implied move = (ATM call mid + ATM put mid) / spot.
    """
    out: dict = {}
    if not last_close or last_close <= 0:
        return out
    try:
        expiries = list(ticker.options or [])
    except Exception:
        return out
    if not expiries:
        return out
    exp = None
    for e in expiries:
        try:
            ed = datetime.strptime(e, "%Y-%m-%d").date()
        except Exception:
            continue
        if ed >= report_date:
            exp = e
            break
    if exp is None:
        exp = expiries[0]
    try:
        chain = ticker.option_chain(exp)
        calls = chain.calls
        puts = chain.puts
    except Exception:
        return out
    if calls is None or puts is None or calls.empty or puts.empty:
        return out
    try:
        calls = calls.assign(_d=(calls["strike"] - last_close).abs())
        puts = puts.assign(_d=(puts["strike"] - last_close).abs())
        atm_c = calls.loc[calls["_d"].idxmin()]
        atm_p = puts.loc[puts["_d"].idxmin()]

        def _mid(row) -> float:
            b = float(row.get("bid") or 0)
            a = float(row.get("ask") or 0)
            if b > 0 and a > 0 and a >= b:
                return (b + a) / 2.0
            lp = row.get("lastPrice")
            try:
                return float(lp) if lp else 0.0
            except Exception:
                return 0.0

        straddle = _mid(atm_c) + _mid(atm_p)
        if straddle > 0:
            out["implied_move_pct"] = straddle / last_close
        ivs = []
        for row in (atm_c, atm_p):
            iv = row.get("impliedVolatility")
            if iv is not None:
                try:
                    iv_f = float(iv)
                    if iv_f > 0:
                        ivs.append(iv_f)
                except Exception:
                    pass
        if ivs:
            out["iv_30d"] = sum(ivs) / len(ivs)
    except Exception:
        return out
    return out


def _historical_earnings_move_pct(con: duckdb.DuckDBPyConnection, symbol: str,
                                  edf, lookback: int = 4) -> float | None:
    """Avg |close-to-close %| move on the past `lookback` earnings dates."""
    if edf is None or getattr(edf, "empty", True):
        return None
    today = date.today()
    past: list[date] = []
    for idx in edf.index:
        d = _coerce_date(idx)
        if d and d < today:
            past.append(d)
    if not past:
        return None
    past = sorted(past, reverse=True)[:lookback]
    moves: list[float] = []
    for d in past:
        # find latest bar on-or-before d (AMC prints land next session),
        # plus the bar immediately before that one.
        rows = con.execute("""
            SELECT close FROM daily_bars
            WHERE symbol = ? AND date <= ?
            ORDER BY date DESC LIMIT 3
        """, [symbol, d]).fetchall()
        if len(rows) >= 2 and rows[0][0] and rows[1][0] and rows[1][0] > 0:
            moves.append(abs((rows[0][0] - rows[1][0]) / rows[1][0]))
    if not moves:
        return None
    return sum(moves) / len(moves)


def _last_surprise_pct(edf) -> float | None:
    if edf is None or getattr(edf, "empty", True):
        return None
    if "Surprise(%)" not in edf.columns:
        return None
    today = date.today()
    try:
        for idx, row in edf.iterrows():
            d = _coerce_date(idx)
            if d is None or d >= today:
                continue
            v = row.get("Surprise(%)")
            if v is not None:
                try:
                    return float(v) / 100.0
                except Exception:
                    continue
    except Exception:
        return None
    return None


def _fetch_ticker_extras(con: duckdb.DuckDBPyConnection, ticker, symbol: str,
                        first_report_date: date) -> dict:
    """All the per-symbol context needed for the enriched earnings row."""
    extras: dict = {}
    last_close: float | None = None
    try:
        fi = ticker.fast_info
        try:
            last_close = float(fi.last_price)
        except Exception:
            pass
        try:
            mc = fi.market_cap
            if mc:
                extras["market_cap"] = int(mc)
        except Exception:
            pass
    except Exception:
        pass
    if last_close is None:
        row = con.execute(
            "SELECT last FROM quotes WHERE symbol = ?", [symbol]
        ).fetchone()
        if row and row[0]:
            last_close = float(row[0])
    if last_close is not None:
        extras["last_close"] = last_close
    extras.update(_fetch_implied_move_iv(ticker, first_report_date, last_close))
    try:
        edf = ticker.get_earnings_dates(limit=8)
    except Exception:
        edf = None
    sp = _last_surprise_pct(edf)
    if sp is not None:
        extras["last_surprise_pct"] = sp
    hm = _historical_earnings_move_pct(con, symbol, edf)
    if hm is not None:
        extras["hist_avg_move_pct"] = hm
    return extras


def refresh_earnings_calendar(con: duckdb.DuckDBPyConnection, sp500: list[tuple[str, str, str]],
                              lookahead_days: int = 14, limit: int = 200) -> int:
    """Pull upcoming earnings from yfinance for a subset of S&P names.

    Also enriches each row with IV, implied move, historical earnings move,
    last-quarter surprise, last close, and market cap.
    """
    import yfinance as yf
    count = 0
    con.execute("DELETE FROM earnings_calendar WHERE report_date < CURRENT_DATE - INTERVAL 3 DAY")
    for sym, _, _ in sp500[:limit]:
        try:
            ticker = yf.Ticker(sym)
        except Exception:
            continue
        upcoming = _extract_earnings_for_symbol(ticker, lookahead_days)
        if not upcoming:
            time.sleep(0.05)
            continue
        try:
            extras = _fetch_ticker_extras(con, ticker, sym, upcoming[0][0])
        except Exception:
            log.exception("earnings extras(%s) failed", sym)
            extras = {}
        for rd, eps_est in upcoming:
            con.execute("""
                INSERT INTO earnings_calendar
                    (symbol, report_date, when_reported, eps_estimate, eps_actual,
                     implied_move_pct, iv_30d, last_close, last_surprise_pct,
                     hist_avg_move_pct, market_cap)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (symbol, report_date) DO UPDATE SET
                    when_reported=COALESCE(excluded.when_reported, earnings_calendar.when_reported),
                    eps_estimate=COALESCE(excluded.eps_estimate, earnings_calendar.eps_estimate),
                    implied_move_pct=COALESCE(excluded.implied_move_pct, earnings_calendar.implied_move_pct),
                    iv_30d=COALESCE(excluded.iv_30d, earnings_calendar.iv_30d),
                    last_close=COALESCE(excluded.last_close, earnings_calendar.last_close),
                    last_surprise_pct=COALESCE(excluded.last_surprise_pct, earnings_calendar.last_surprise_pct),
                    hist_avg_move_pct=COALESCE(excluded.hist_avg_move_pct, earnings_calendar.hist_avg_move_pct),
                    market_cap=COALESCE(excluded.market_cap, earnings_calendar.market_cap)
            """, [sym, rd, None, eps_est, None,
                  extras.get("implied_move_pct"),
                  extras.get("iv_30d"),
                  extras.get("last_close"),
                  extras.get("last_surprise_pct"),
                  extras.get("hist_avg_move_pct"),
                  extras.get("market_cap")])
            count += 1
        time.sleep(0.15)
    con.commit()
    return count


def refresh_econ_events(con: duckdb.DuckDBPyConnection) -> int:
    """Best-effort scrape of tradingeconomics.com US calendar. Silent failure is OK."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except Exception:
        return 0
    url = "https://tradingeconomics.com/united-states/calendar"
    try:
        resp = requests.get(
            url, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (stock-dashboard)"},
        )
        resp.raise_for_status()
    except Exception as exc:
        log.info("econ calendar scrape skipped: %s", exc)
        return 0
    try:
        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", id="calendar")
        if table is None:
            return 0
        count = 0
        current_date: date | None = None
        for tr in table.find_all("tr"):
            # date rows have thead-like content
            if tr.find("th"):
                dtxt = tr.get_text(strip=True)
                try:
                    current_date = datetime.strptime(dtxt, "%A %B %d %Y").date()
                except Exception:
                    current_date = None
                continue
            tds = tr.find_all("td")
            if len(tds) < 6 or current_date is None:
                continue
            event_time = tds[0].get_text(strip=True)
            name = tds[3].get_text(strip=True)
            actual = tds[5].get_text(strip=True) if len(tds) > 5 else ""
            previous = tds[6].get_text(strip=True) if len(tds) > 6 else ""
            forecast = tds[8].get_text(strip=True) if len(tds) > 8 else ""
            importance_el = tr.find(attrs={"data-importance": True})
            importance = importance_el.get("data-importance", "") if importance_el else ""
            if not name:
                continue
            con.execute("""
                INSERT INTO econ_events VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (event_date, event_time, name) DO UPDATE SET
                    importance=excluded.importance,
                    forecast=excluded.forecast,
                    previous=excluded.previous,
                    actual=excluded.actual
            """, [current_date, event_time, name, importance, forecast, previous, actual])
            count += 1
        con.commit()
        return count
    except Exception as exc:
        log.info("econ calendar parse failed: %s", exc)
        return 0


# ── Main loop ────────────────────────────────────────────────────────────────

def collect_once(con: duckdb.DuckDBPyConnection, state: dict,
                 sp500: list[tuple[str, str, str]]) -> None:
    now = datetime.now(timezone.utc)
    all_syms = universe.all_symbols(sp500)
    fast_syms = sorted({
        *(t.symbol for t in universe.SECTOR_ETFS),
        *(t.symbol for t in universe.VOL_TICKERS),
        *(t.symbol for t in universe.MACRO_TICKERS),
        *(t.symbol for t in universe.PAIR_TICKERS),
    })
    fast_set = set(fast_syms)
    # Keep SPY intraday around for OBV intraday mode in the dashboard.
    pair_syms = sorted({*(t.symbol for t in universe.PAIR_TICKERS), "SPY"})

    last_daily = state.get("last_daily")
    if last_daily is None or (now - last_daily).total_seconds() >= 600:
        log.info("Refreshing daily bars for %d symbols...", len(all_syms))
        try:
            # Pair + SPY back the 5Y chart toggle, so pull long history only
            # for that tiny set; the rest get the standard 300d window.
            long_hist_syms = sorted({*(t.symbol for t in universe.PAIR_TICKERS), "SPY"})
            n_long = refresh_daily_bars(con, long_hist_syms, period="5y")
            fast_rest = [s for s in fast_syms if s not in set(long_hist_syms)]
            n_fast = refresh_daily_bars(con, fast_rest)
            n_quotes_fast = refresh_quotes(con)
            log.info("Fast pass complete (%d long + %d rows, %d quote rows)",
                     n_long, n_fast, n_quotes_fast)
        except Exception:
            log.exception("fast daily refresh failed")

        def _after_chunk(done: int, total: int) -> None:
            # Progressive quote refresh so the UI fills in as chunks land
            try:
                n = refresh_quotes(con)
                log.info("  -> quotes refreshed incrementally (%d rows)", n)
            except Exception:
                log.exception("incremental refresh_quotes failed")

        rest_syms = [s for s in all_syms if s not in fast_set]
        n = refresh_daily_bars(con, rest_syms, on_chunk=_after_chunk)
        log.info("Daily bars refreshed (%d rows upserted)", n)
        state["last_daily"] = now
    else:
        try:
            n_quotes = refresh_quotes(con)
            log.info("Quotes refreshed (%d rows)", n_quotes)
        except Exception:
            log.exception("refresh_quotes failed")

    log.info("Refreshing intraday bars for %d pair tickers...", len(pair_syms))
    try:
        refresh_intraday_pairs(con, pair_syms)
    except Exception:
        log.exception("refresh_intraday_pairs failed")
    try:
        refresh_vol_snapshot(con)
    except Exception:
        log.exception("refresh_vol_snapshot failed")

    last_earn = state.get("last_earn")
    if last_earn is None or (now - last_earn).total_seconds() >= 24 * 3600:
        log.info("Refreshing earnings calendar...")
        try:
            refresh_earnings_calendar(con, sp500)
        except Exception:
            log.exception("earnings refresh failed")
        state["last_earn"] = now

    last_econ = state.get("last_econ")
    if last_econ is None or (now - last_econ).total_seconds() >= 3600:
        try:
            refresh_econ_events(con)
        except Exception:
            log.exception("econ refresh failed")
        state["last_econ"] = now

    from .pro_collector import upsert_activity, prune_pro_data

    last_pro_act = state.get("last_pro_act")
    if last_pro_act is None or (now - last_pro_act).total_seconds() >= 1800:
        try:
            n = upsert_activity(con)
            log.info("Pro activity upsert: %d rows", n)
        except Exception:
            log.exception("pro activity upsert failed")
        state["last_pro_act"] = now

    last_prune = state.get("last_prune")
    if last_prune is None or (now - last_prune).total_seconds() >= 3600:
        prune_intraday(con)
        prune_daily(con)
        prune_vol(con)
        try:
            prune_pro_data(con)
        except Exception:
            log.exception("prune_pro_data failed")
        state["last_prune"] = now

    con.commit()


def run_loop(db_path: str | Path, poll_seconds: int = 60) -> None:
    """Blocking collector loop — designed to run on a daemon thread."""
    con = init_db(db_path)
    log.info("Loading S&P 500 universe...")
    sp500 = universe.load_sp500(log)
    seed_universe(con, sp500)
    log.info("Universe seeded (%d S&P 500 + %d static)",
             len(sp500), len(universe.all_static_tickers()))

    state: dict = {}
    while True:
        try:
            collect_once(con, state, sp500)
        except Exception:
            log.exception("collect_once failed")
        time.sleep(poll_seconds)
