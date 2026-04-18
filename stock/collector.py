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
    con.execute("DELETE FROM intraday_bars WHERE ts < CURRENT_DATE - INTERVAL 2 DAY")


def prune_daily(con: duckdb.DuckDBPyConnection, keep_days: int = 300) -> None:
    con.execute(f"DELETE FROM daily_bars WHERE date < CURRENT_DATE - INTERVAL {keep_days} DAY")


def prune_vol(con: duckdb.DuckDBPyConnection, keep_days: int = 60) -> None:
    con.execute(f"DELETE FROM vol_snapshots WHERE ts < CURRENT_TIMESTAMP - INTERVAL {keep_days} DAY")


# ── Steps ─────────────────────────────────────────────────────────────────────

def refresh_daily_bars(con: duckdb.DuckDBPyConnection, symbols: list[str],
                       on_chunk=None) -> int:
    """Pull 300d of daily history for all symbols (batched).

    If `on_chunk` is provided it's called after each chunk commits with
    (chunks_done, total_chunks) so the caller can refresh derived tables.
    """
    chunk = 50
    total = 0
    n_chunks = (len(symbols) + chunk - 1) // chunk
    for ci, i in enumerate(range(0, len(symbols), chunk), start=1):
        batch = symbols[i:i + chunk]
        t0 = time.time()
        try:
            df = _yf_download(batch, period="300d", interval="1d")
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
    try:
        df = _yf_download(symbols, period="2d", interval="1m", prepost=True)
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


def refresh_earnings_calendar(con: duckdb.DuckDBPyConnection, sp500: list[tuple[str, str, str]],
                              lookahead_days: int = 14, limit: int = 200) -> int:
    """Pull upcoming earnings from yfinance for a subset of S&P names."""
    import yfinance as yf
    count = 0
    con.execute("DELETE FROM earnings_calendar WHERE report_date < CURRENT_DATE - INTERVAL 3 DAY")
    for sym, _, _ in sp500[:limit]:
        try:
            ticker = yf.Ticker(sym)
        except Exception:
            continue
        inserted_for_sym = 0
        for rd, eps_est in _extract_earnings_for_symbol(ticker, lookahead_days):
            con.execute("""
                INSERT INTO earnings_calendar VALUES (?,?,?,?,?)
                ON CONFLICT (symbol, report_date) DO UPDATE SET
                    when_reported=excluded.when_reported,
                    eps_estimate=excluded.eps_estimate
            """, [sym, rd, None, eps_est, None])
            inserted_for_sym += 1
            count += 1
        time.sleep(0.15 if inserted_for_sym > 0 else 0.05)
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
    pair_syms = [t.symbol for t in universe.PAIR_TICKERS]

    last_daily = state.get("last_daily")
    if last_daily is None or (now - last_daily).total_seconds() >= 600:
        log.info("Refreshing daily bars for %d symbols...", len(all_syms))
        try:
            n_fast = refresh_daily_bars(con, fast_syms)
            n_quotes_fast = refresh_quotes(con)
            log.info("Fast pass complete (%d rows, %d quote rows)", n_fast, n_quotes_fast)
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

    last_prune = state.get("last_prune")
    if last_prune is None or (now - last_prune).total_seconds() >= 3600:
        prune_intraday(con)
        prune_daily(con)
        prune_vol(con)
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
