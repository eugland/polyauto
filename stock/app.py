"""Flask app + JSON API for the stock decision dashboard."""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from . import database as db


# yfinance logs HTTP 404s and "possibly delisted" messages at ERROR level for
# every quoteSummary/calendar/dividends fetch that misses. Our dividend +
# overnight code already swallows these exceptions, so the log noise is
# purely cosmetic. Suppress it so the actual app log stays useful.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
# yfinance also prints to a child "py.warnings" logger via warnings.warn
logging.getLogger("py.warnings").setLevel(logging.CRITICAL)


# ── Pre-market & overnight live-fetch (yfinance, cached) ─────────────────────

_OVERNIGHT_TICKERS: list[tuple[str, str, str]] = [
    # symbol,    label,            group
    ("ES=F",     "S&P fut",        "futures"),
    ("NQ=F",     "Nasdaq fut",     "futures"),
    ("YM=F",     "Dow fut",        "futures"),
    ("RTY=F",    "Russell fut",    "futures"),
    ("BTC-USD",  "BTC",            "crypto"),
    ("ETH-USD",  "ETH",            "crypto"),
    ("DX=F",     "DXY",            "macro"),
    ("CL=F",     "WTI crude",      "macro"),
    ("GC=F",     "Gold",           "macro"),
    ("^TNX",     "10Y yield",      "macro"),
]

_overnight_cache: dict = {"data": None, "ts": 0.0}
_overnight_lock = threading.Lock()
_OVERNIGHT_TTL = 30  # seconds

_premkt_cache: dict = {"data": None, "ts": 0.0}
_premkt_lock = threading.Lock()
_PREMKT_TTL = 60  # seconds

# Symbols shown in the overnight-trend fallback chart (used when there's no
# pre-market data, e.g. weekends and outside the 04:00-09:30 ET window).
_OVERNIGHT_TREND_TICKERS: list[tuple[str, str]] = [
    ("ES=F",     "S&P fut"),
    ("NQ=F",     "Nasdaq fut"),
    ("BTC-USD",  "BTC"),
    ("ETH-USD",  "ETH"),
]
_overnight_trend_cache: dict = {"data": None, "ts": 0.0}
_overnight_trend_lock = threading.Lock()
_OVERNIGHT_TREND_TTL = 120  # seconds

# Maps each overnight futures contract to its underlying cash index for the
# "futures-implied open" card. Mechanical: implied_open = cash_prev_close *
# (1 + futures_pct). NQ futures track the Nasdaq 100 (^NDX), not the
# Composite (^IXIC).
_IMPLIED_OPEN_MAP: list[tuple[str, str, str]] = [
    ("ES=F",  "^GSPC", "S&P 500"),
    ("NQ=F",  "^NDX",  "Nasdaq 100"),
    ("YM=F",  "^DJI",  "Dow Jones"),
    ("RTY=F", "^RUT",  "Russell 2000"),
]
_implied_cache: dict = {"data": None, "ts": 0.0}
_implied_lock = threading.Lock()
_IMPLIED_TTL = 30  # seconds


def _fetch_overnight_one(item: tuple[str, str, str]) -> dict:
    sym, label, group = item
    try:
        import yfinance as yf
        t = yf.Ticker(sym)
        fi = t.fast_info
        last = fi.last_price
        prev = fi.previous_close
        last_f = float(last) if last is not None else None
        prev_f = float(prev) if prev is not None else None
        pct = (last_f - prev_f) / prev_f if (last_f is not None and prev_f) else None
        return {"symbol": sym, "label": label, "group": group,
                "last": last_f, "prev_close": prev_f, "pct": pct}
    except Exception as exc:
        return {"symbol": sym, "label": label, "group": group, "error": str(exc)}


def _fetch_overnight() -> list[dict]:
    with ThreadPoolExecutor(max_workers=10) as ex:
        return list(ex.map(_fetch_overnight_one, _OVERNIGHT_TICKERS))


def _fetch_premarket_gappers(db_path: str, top_n: int = 100, min_pct: float = 0.01,
                             limit: int = 25) -> list[dict]:
    """Pull 1-minute prepost bars for the top-N S&P names by avg volume,
    compute pre-market % vs prior close + pre-market volume."""
    import duckdb
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute("""
        SELECT q.symbol, u.name, u.sector, q.prev_close, q.avg_volume_20d
        FROM quotes q JOIN universe u ON u.symbol = q.symbol
        WHERE u.is_sp500 = TRUE AND q.prev_close > 0
        ORDER BY q.avg_volume_20d DESC NULLS LAST
        LIMIT ?
    """, [top_n]).fetchall()
    con.close()
    if not rows:
        return []
    meta = {r[0]: {"name": r[1], "sector": r[2], "prev_close": float(r[3])} for r in rows}
    syms = list(meta.keys())

    import yfinance as yf
    out: list[dict] = []
    chunk = 50
    for i in range(0, len(syms), chunk):
        batch = syms[i:i + chunk]
        try:
            df = yf.download(batch, period="1d", interval="1m", prepost=True,
                             progress=False, threads=False, group_by="ticker",
                             timeout=20, auto_adjust=False)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for sym in batch:
            try:
                sub = df[sym] if sym in df.columns.get_level_values(0) else None
            except Exception:
                sub = None
            if sub is None or sub.empty:
                continue
            sub = sub.dropna(subset=["Close"])
            if sub.empty:
                continue
            try:
                idx = sub.index
                if hasattr(idx, "tz") and idx.tz is not None:
                    sub = sub.tz_convert("America/New_York")
                else:
                    sub = sub.tz_localize("UTC").tz_convert("America/New_York")
            except Exception:
                pass
            mask = [(t.hour > 4 or (t.hour == 4 and t.minute >= 0))
                    and (t.hour < 9 or (t.hour == 9 and t.minute < 30))
                    for t in sub.index]
            pre = sub[mask]
            if pre.empty:
                continue
            try:
                last_pre = float(pre["Close"].iloc[-1])
                pre_vol = int(pre["Volume"].fillna(0).sum())
            except Exception:
                continue
            prev_close = meta[sym]["prev_close"]
            if not prev_close:
                continue
            pct = (last_pre - prev_close) / prev_close
            if abs(pct) < min_pct:
                continue
            out.append({
                "symbol": sym,
                "name": meta[sym]["name"],
                "sector": meta[sym]["sector"],
                "last": last_pre,
                "prev_close": prev_close,
                "premarket_pct": pct,
                "premarket_volume": pre_vol,
            })
    out.sort(key=lambda r: abs(r.get("premarket_pct") or 0), reverse=True)
    return out[:limit]


def _fetch_one_quote(sym: str) -> dict:
    try:
        import yfinance as yf
        fi = yf.Ticker(sym).fast_info
        last = fi.last_price
        prev = fi.previous_close
        return {
            "symbol": sym,
            "last": float(last) if last is not None else None,
            "prev_close": float(prev) if prev is not None else None,
        }
    except Exception as exc:
        return {"symbol": sym, "error": str(exc)}


def _fetch_implied_open() -> list[dict]:
    """For each futures→cash pair, compute the mechanical implied open:
    implied_open = cash_last_close * (1 + futures_pct).

    Uses cash `last_price` (= most-recent regular-session close while markets
    are closed) as the anchor rather than `previous_close`, because yfinance's
    `previous_close` on weekends/holidays returns the session-before-last
    close (e.g. Thursday's close on a Sunday) instead of Friday's.

    Ignores fair-value basis (dividends + financing) — same calc CNBC shows
    pre-open, accurate enough for a directional read."""
    syms: list[str] = []
    for fut, cash, _ in _IMPLIED_OPEN_MAP:
        syms.append(fut)
        syms.append(cash)
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_fetch_one_quote, syms))
    by_sym = {r["symbol"]: r for r in results}
    out: list[dict] = []
    for fut, cash, label in _IMPLIED_OPEN_MAP:
        f = by_sym.get(fut, {})
        c = by_sym.get(cash, {})
        f_last = f.get("last")
        f_prev = f.get("prev_close")
        c_anchor = c.get("last")  # last regular-session close for the cash index
        f_pct = ((f_last - f_prev) / f_prev) if (f_last is not None and f_prev) else None
        target = (c_anchor * (1 + f_pct)) if (c_anchor is not None and f_pct is not None) else None
        delta_pts = (target - c_anchor) if (target is not None and c_anchor is not None) else None
        out.append({
            "label": label,
            "futures": fut,
            "cash": cash,
            "futures_last": f_last,
            "futures_prev_close": f_prev,
            "futures_pct": f_pct,
            "cash_prev_close": c_anchor,
            "implied_open": target,
            "delta_pts": delta_pts,
            "delta_pct": f_pct,
        })
    return out


def _fetch_overnight_trend() -> list[dict]:
    """Pull 5-min bars over the last 2 days for futures + crypto.
    Returns each series normalized to % change from the first point so they
    can be overlaid on a single chart."""
    import yfinance as yf
    syms = [s for s, _ in _OVERNIGHT_TREND_TICKERS]
    try:
        df = yf.download(syms, period="2d", interval="5m", prepost=True,
                         progress=False, threads=False, group_by="ticker",
                         timeout=20, auto_adjust=False)
    except Exception:
        return []
    if df is None or df.empty:
        return []
    out: list[dict] = []
    for sym, label in _OVERNIGHT_TREND_TICKERS:
        try:
            sub = df[sym] if sym in df.columns.get_level_values(0) else None
        except Exception:
            sub = None
        if sub is None or sub.empty:
            continue
        sub = sub.dropna(subset=["Close"])
        if sub.empty:
            continue
        try:
            idx = sub.index
            if hasattr(idx, "tz") and idx.tz is not None:
                sub = sub.tz_convert("America/New_York")
            else:
                sub = sub.tz_localize("UTC").tz_convert("America/New_York")
        except Exception:
            pass
        try:
            base = float(sub["Close"].iloc[0])
        except Exception:
            continue
        if not base:
            continue
        points: list[dict] = []
        for ts, row in sub.iterrows():
            try:
                px = float(row["Close"])
            except Exception:
                continue
            label_ts = ts.strftime("%a %H:%M")
            points.append({"t": label_ts, "value": (px - base) / base})
        if points:
            out.append({"symbol": sym, "label": label, "points": points})
    return out


# Patterns for secrets that must never leave the server. Order matters:
# key=value patterns first (preserve the key for context), then bare hex
# wallet/key strings.
_REDACT_PATTERNS = [
    # CLOB_API_KEY=..., CLOB_SECRET=..., CLOB_PASS=..., RELAYER_API_KEY=...,
    # POLYMARKET_PRIVATE_KEY=..., api_key=..., secret=..., passphrase=...
    (re.compile(
        r"((?:CLOB_API_KEY|CLOB_SECRET|CLOB_PASS|RELAYER_API_KEY|"
        r"POLYMARKET_PRIVATE_KEY|api[_-]?key|api[_-]?secret|"
        r"passphrase|secret|private[_-]?key)\s*[:=]\s*)(\S+)",
        re.IGNORECASE,
    ), r"\1[REDACTED]"),
    # Bare 0x-prefixed private keys (64 hex chars)
    (re.compile(r"\b0x[0-9a-fA-F]{64}\b"), "[REDACTED_KEY]"),
    # Bearer tokens
    (re.compile(r"(Bearer\s+)\S+", re.IGNORECASE), r"\1[REDACTED]"),
]


def _redact(line: str) -> str:
    for pat, repl in _REDACT_PATTERNS:
        line = pat.sub(repl, line)
    return line


def create_app(db_path: str) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["DB_PATH"] = db_path

    def _db() -> str:
        return app.config["DB_PATH"]

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/weather-log")
    def weather_log():
        return render_template("weather_log.html")

    @app.route("/weather-pro")
    def weather_pro():
        return render_template("weather_pro.html")

    @app.route("/weather-model")
    def weather_model_index():
        return render_template("weather_model.html")

    @app.route("/api/weather-model/cities")
    def api_weather_model_cities():
        try:
            from automata import weather_model
            weather_model.init_db()
            cities = weather_model.list_cities()
            # Join with outcome counts + hit-rate so the overview is one call.
            import duckdb as _duck
            con = _duck.connect(str(weather_model.DB_PATH), read_only=True)
            try:
                rows = con.execute("""
                    SELECT s.city,
                           COUNT(DISTINCT s.event_date) AS n_events,
                           COUNT(DISTINCT CASE WHEN o.icao IS NOT NULL
                                               THEN s.event_date END) AS n_resolved
                    FROM scans s
                    LEFT JOIN outcomes o
                        ON o.icao = s.icao AND o.event_date = s.event_date
                    GROUP BY s.city
                """).fetchall()
            finally:
                con.close()
            resolved_by_city = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in rows}
            for c in cities:
                n_evt, n_res = resolved_by_city.get(c["city"], (0, 0))
                c["n_events"] = n_evt
                c["n_resolved"] = n_res
            return jsonify({"cities": cities})
        except Exception as exc:
            return jsonify({"cities": [], "error": str(exc)}), 500

    @app.route("/api/weather-model/scans")
    def api_weather_model_scans():
        try:
            from automata import weather_model
            weather_model.init_db()
            city = request.args.get("city") or None
            try:
                limit = int(request.args.get("limit", "500"))
            except ValueError:
                limit = 500
            scans = weather_model.list_scans(city=city, limit=limit)
            outcomes = weather_model.list_outcomes(city=city, limit=limit)
            return jsonify({"scans": scans, "outcomes": outcomes})
        except Exception as exc:
            return jsonify({"scans": [], "outcomes": [], "error": str(exc)}), 500

    @app.route("/api/weather-model/stations")
    def api_weather_model_stations():
        try:
            from automata import weather_model
            weather_model.init_db()
            return jsonify(weather_model.list_stations())
        except Exception as exc:
            return jsonify({"bias": [], "sigma": [], "error": str(exc)}), 500

    @app.route("/api/weather-model/calibration")
    def api_weather_model_calibration():
        try:
            from automata import weather_model
            weather_model.init_db()
            return jsonify(weather_model.calibration_summary())
        except Exception as exc:
            return jsonify({"buckets": [], "total_paired": 0, "error": str(exc)}), 500

    @app.route("/api/weather-model/live")
    def api_weather_model_live():
        """
        Most-recent scan per currently-open market (one row per bet item).
        Filters to scans taken in the last `window_min` minutes and whose
        resolution_dt is still in the future, so the output is the live
        scanner's working set.
        """
        try:
            from automata import weather_model
            weather_model.init_db()
            try:
                window_min = int(request.args.get("window_min", "10"))
            except ValueError:
                window_min = 10

            import duckdb as _duck
            con = _duck.connect(str(weather_model.DB_PATH), read_only=True)
            try:
                rows = con.execute(f"""
                    WITH ranked AS (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY city, event_date, question
                                   ORDER BY scanned_at DESC
                               ) AS rn
                        FROM scans
                        WHERE scanned_at > (CURRENT_TIMESTAMP - INTERVAL '{window_min} minutes')
                    )
                    SELECT scanned_at, city, icao, event_date,
                           question, token_id,
                           resolution_dt, hours_to_res, lead_bucket,
                           unit, threshold, threshold_hi, direction,
                           openmeteo_high, noaa_high, taf_high,
                           metar_current, metar_max_so_far,
                           source_used, bias_used, calibrated_mu, calibrated_sigma,
                           no_bid, no_ask, yes_bid, yes_ask,
                           fair_no_prob, edge_bps
                    FROM ranked
                    WHERE rn = 1
                      AND (resolution_dt IS NULL OR resolution_dt > CURRENT_TIMESTAMP)
                    ORDER BY ABS(COALESCE(edge_bps, 0)) DESC
                """).fetchall()
            finally:
                con.close()
            keys = [
                "scanned_at", "city", "icao", "event_date",
                "question", "token_id",
                "resolution_dt", "hours_to_res", "lead_bucket",
                "unit", "threshold", "threshold_hi", "direction",
                "openmeteo_high", "noaa_high", "taf_high",
                "metar_current", "metar_max_so_far",
                "source_used", "bias_used", "calibrated_mu", "calibrated_sigma",
                "no_bid", "no_ask", "yes_bid", "yes_ask",
                "fair_no_prob", "edge_bps",
            ]
            out = []
            for r in rows:
                d = dict(zip(keys, r))
                for k in ("scanned_at", "resolution_dt"):
                    if d[k] is not None:
                        d[k] = d[k].isoformat()
                out.append(d)
            return jsonify({"markets": out, "window_min": window_min})
        except Exception as exc:
            return jsonify({"markets": [], "error": str(exc)}), 500

    @app.route("/api/health")
    def api_health():
        return jsonify(db.query_health(_db()))

    @app.route("/api/pair")
    def api_pair():
        symbols = [s.strip().upper() for s in
                   (request.args.get("symbols") or "").split(",") if s.strip()]
        view = request.args.get("view", "5d")
        if len(symbols) < 1:
            return jsonify({"error": "symbols param required"}), 400
        return jsonify(db.query_pair(_db(), symbols, view))

    @app.route("/api/bubble")
    def api_bubble():
        return jsonify(db.query_bubble(_db()))

    @app.route("/api/sectors")
    def api_sectors():
        return jsonify(db.query_sectors(_db()))

    @app.route("/api/movers")
    def api_movers():
        side = request.args.get("side", "up")
        limit = int(request.args.get("limit", "10"))
        return jsonify(db.query_movers(_db(), side, limit))

    @app.route("/api/macro")
    def api_macro():
        return jsonify(db.query_macro(_db()))

    @app.route("/api/overnight")
    def api_overnight():
        now = time.time()
        with _overnight_lock:
            if (_overnight_cache["data"] is not None
                    and (now - _overnight_cache["ts"]) < _OVERNIGHT_TTL):
                return jsonify({"data": _overnight_cache["data"],
                                "as_of": _overnight_cache["ts"], "cached": True})
        try:
            data = _fetch_overnight()
        except Exception as exc:
            return jsonify({"error": str(exc), "data": []}), 500
        with _overnight_lock:
            _overnight_cache["data"] = data
            _overnight_cache["ts"] = now
        return jsonify({"data": data, "as_of": now, "cached": False})

    @app.route("/api/premarket-gappers")
    def api_premarket_gappers():
        now = time.time()
        with _premkt_lock:
            if (_premkt_cache["data"] is not None
                    and (now - _premkt_cache["ts"]) < _PREMKT_TTL):
                return jsonify({"data": _premkt_cache["data"],
                                "as_of": _premkt_cache["ts"], "cached": True})
        try:
            data = _fetch_premarket_gappers(_db())
        except Exception as exc:
            return jsonify({"error": str(exc), "data": []}), 500
        with _premkt_lock:
            _premkt_cache["data"] = data
            _premkt_cache["ts"] = now
        return jsonify({"data": data, "as_of": now, "cached": False})

    @app.route("/api/implied-open")
    def api_implied_open():
        now = time.time()
        with _implied_lock:
            if (_implied_cache["data"] is not None
                    and (now - _implied_cache["ts"]) < _IMPLIED_TTL):
                return jsonify({"data": _implied_cache["data"],
                                "as_of": _implied_cache["ts"], "cached": True})
        try:
            data = _fetch_implied_open()
        except Exception as exc:
            return jsonify({"error": str(exc), "data": []}), 500
        with _implied_lock:
            _implied_cache["data"] = data
            _implied_cache["ts"] = now
        return jsonify({"data": data, "as_of": now, "cached": False})

    @app.route("/api/overnight-trend")
    def api_overnight_trend():
        now = time.time()
        with _overnight_trend_lock:
            if (_overnight_trend_cache["data"] is not None
                    and (now - _overnight_trend_cache["ts"]) < _OVERNIGHT_TREND_TTL):
                return jsonify({"data": _overnight_trend_cache["data"],
                                "as_of": _overnight_trend_cache["ts"], "cached": True})
        try:
            data = _fetch_overnight_trend()
        except Exception as exc:
            return jsonify({"error": str(exc), "data": []}), 500
        with _overnight_trend_lock:
            _overnight_trend_cache["data"] = data
            _overnight_trend_cache["ts"] = now
        return jsonify({"data": data, "as_of": now, "cached": False})

    @app.route("/api/vix-term")
    def api_vix_term():
        return jsonify(db.query_vix_term(_db()))

    @app.route("/api/vvix")
    def api_vvix():
        return jsonify(db.query_vvix(_db()))

    @app.route("/api/breadth")
    def api_breadth():
        return jsonify(db.query_breadth(_db()))

    @app.route("/api/tracking-error")
    def api_tracking_error():
        pair = request.args.get("pair", "TSLL")
        base = request.args.get("base", "TSLA")
        view = request.args.get("view", "current")
        start = request.args.get("start")
        end = request.args.get("end")
        try:
            leverage = float(request.args.get("leverage", "2"))
        except ValueError:
            leverage = 2.0
        try:
            days = int(request.args.get("days", "10"))
        except ValueError:
            days = 10
        return jsonify(
            db.query_tracking_error(
                _db(), pair, base, leverage, days=days, view=view, start=start, end=end
            )
        )

    @app.route("/api/screener")
    def api_screener():
        kind = request.args.get("kind", "volume")
        try:
            limit = int(request.args.get("limit", "20"))
        except ValueError:
            limit = 20
        return jsonify(db.query_screener(_db(), kind, limit))

    @app.route("/api/correlation")
    def api_correlation():
        try:
            window = int(request.args.get("window", "20"))
        except ValueError:
            window = 20
        return jsonify(db.query_correlation(_db(), window))

    @app.route("/api/vol-scatter")
    def api_vol_scatter():
        return jsonify(db.query_vol_scatter(_db()))

    @app.route("/api/earnings")
    def api_earnings():
        try:
            days = int(request.args.get("days", "7"))
        except ValueError:
            days = 7
        return jsonify(db.query_earnings(_db(), days))

    @app.route("/api/fear-greed")
    def api_fear_greed():
        return jsonify(db.query_fear_greed(_db()))

    @app.route("/api/spy-volume-signal")
    def api_spy_volume_signal():
        view = request.args.get("view", "current")
        start = request.args.get("start")
        end = request.args.get("end")
        return jsonify(db.query_spy_volume_signal(_db(), view=view, start=start, end=end))

    @app.route("/api/econ-events")
    def api_econ_events():
        try:
            days = int(request.args.get("days", "1"))
        except ValueError:
            days = 1
        return jsonify(db.query_econ_events(_db(), days))

    @app.route("/api/dividend-profiles")
    def api_dividend_profiles():
        return jsonify(db.query_dividend_profiles(_db()))

    @app.route("/api/dividend-profile", methods=["GET", "POST"])
    def api_dividend_profile():
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            action = payload.get("action")
            if action == "delete":
                name = payload.get("name", "")
                return jsonify(db.delete_dividend_profile(_db(), name))
            name = payload.get("name", "default")
            holdings = payload.get("holdings", [])
            return jsonify(db.upsert_dividend_profile(_db(), name, holdings))
        name = request.args.get("name", "default")
        return jsonify(db.query_dividend_profile(_db(), name))

    @app.route("/api/dividends")
    def api_dividends():
        profile = request.args.get("profile", "default")
        return jsonify(db.query_dividend_report(_db(), profile))

    _LOG_FILE = Path(__file__).resolve().parent.parent / "logs" / "automata.log"

    @app.route("/api/weather-log")
    def api_weather_log():
        try:
            lines = int(request.args.get("lines", "200"))
        except ValueError:
            lines = 200
        path_str = str(_LOG_FILE)
        try:
            if not _LOG_FILE.exists():
                return jsonify({"lines": [], "exists": False, "path": path_str})
            with open(_LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
            return jsonify({
                "lines": [_redact(l.rstrip()) for l in all_lines[-lines:]],
                "exists": True,
                "path": path_str,
                "total_lines": len(all_lines),
            })
        except Exception as exc:
            return jsonify({"error": str(exc), "path": path_str}), 500

    import requests as _requests
    _PRO_WALLETS = {
        "haerder":    "0x8dec027d883949a6bfe79842d0ae6b80347e46e0",
        "sin3000":    "0x8d71ff86701227bb479b2039edd92b08f73115d8",
        "aapang":     "0x104171232971a6db8cf938f76fdbebbb81c5f452",
        "auniwarper": "0x0ec451646092f877f80d2b53d5500e50dac05ed3",
        "gopfan2":    "0xf2f6af4f27ec2dcf4072095ab804016e14cd5817",
        "aenews2":    "0x44c1dfe43260c94ed4f1d00de2e1f80fb113ebc1",
        "gopfan":     "0x6af75d4e4aaf700450efbac3708cce1665810ff1",
        "ColdMath":   "0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
        "Hans323":    "0x0f37cb80dee49d55b5f6d9e595d52591d6371410",
    }
    _POS_LIMITS = {
        "haerder": 100, "sin3000": 100, "aapang": 500, "auniwarper": 500,
        "gopfan2": 500, "aenews2": 100, "gopfan": 100, "ColdMath": 500, "Hans323": 100,
    }
    _DATA_API = "https://data-api.polymarket.com"

    @app.route("/api/weather-pro/positions")
    def api_weather_pro_positions():
        results = {}
        for name, wallet in _PRO_WALLETS.items():
            try:
                resp = _requests.get(
                    f"{_DATA_API}/positions",
                    params={"user": wallet, "limit": _POS_LIMITS.get(name, 100)},
                    timeout=15,
                )
                results[name] = resp.json() if resp.ok else []
            except Exception as exc:
                results[name] = {"error": str(exc)}
        return jsonify(results)

    @app.route("/api/weather-pro/activity")
    def api_weather_pro_activity():
        from .temp_lookup import annotate_activity
        results = db.query_pro_activity(_db())
        if isinstance(results, dict) and "error" not in results:
            for items in results.values():
                if isinstance(items, list):
                    try:
                        annotate_activity(items)
                    except Exception:
                        pass
        return jsonify(results)

    return app
