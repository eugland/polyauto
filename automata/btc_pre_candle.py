"""
automata/btc_pre_candle.py

Forward (paper) test for BTC 1H pre-candle strategies. Multiple strategies
run side-by-side; each fire is recorded independently in fires table.

Each cycle the daemon:
  1. Computes features once from live Binance 1H klines
  2. Looks up the upcoming Polymarket BTC 1H markets that fall in the
     entry window (10–40 min before candle opens = 70–100 min before
     market resolution)
  3. For each (target_hour × strategy), evaluates and inserts a fire row
     if the signal triggers, recording the live Polymarket ask
  4. Settles past fires whose target candle has now closed

The daemon NEVER places real orders. Output: db/btc_pre_candle.db (DuckDB).

Run:
    python -m automata.btc_pre_candle --interval 60
    python -m automata.btc_pre_candle --once
    python -m automata.btc_pre_candle --strategies P3_rsi_mr_70_30,P13_24h_extreme_0.2

Strategies map 1:1 to the backtest in experiment/btc_1h_backtest/REPORT_PRE_CANDLE.md.
P6_tod_OOS is omitted — its train/test split is meaningless live.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import statistics
import time as _time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from urllib.error import URLError
from zoneinfo import ZoneInfo

import duckdb

from automata import config
from automata.crypto_common import _get, build_slug

log = logging.getLogger("automata.btc_pre_candle")

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "db" / "btc_pre_candle.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

ET = ZoneInfo("America/New_York")
HOST = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com/events?slug={slug}"

FIRE_MIN_OFFSET = -40
FIRE_MAX_OFFSET = -10

# ── Live-bet knobs (only consulted when --bet is on) ──────────────────────────
BET_SHARES         = config.get_float("BTC_PRE_CANDLE_BET_SHARES",
                                      "btc_pre_candle", "bet_shares", 5.0)
MAX_ENTRY_ASK      = config.get_float("BTC_PRE_CANDLE_MAX_ENTRY_ASK",
                                      "btc_pre_candle", "max_entry_ask", 0.65)
DAILY_SPEND_CAP    = config.get_float("BTC_PRE_CANDLE_DAILY_SPEND_CAP_USDC",
                                      "btc_pre_candle", "daily_spend_cap_usdc", 20.0)
LIVE_STRATEGIES    = set(config.get_list_str(
                            "BTC_PRE_CANDLE_LIVE_STRATEGIES",
                            "btc_pre_candle", "live_strategies",
                            ["P13_24h_extreme_0.2"]))

# How many 1H klines to fetch per cycle. We need enough to compute MACD-hist
# z-score (30-day rolling std of macd_hist) ≈ 720 hours plus warmup. Binance
# allows up to 1000.
KLINES_LIMIT = 800

BINANCE_KLINES = "https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"


# ── Feature snapshot ──────────────────────────────────────────────────────────

@dataclass
class Features:
    spot:               float | None = None
    rsi_7:              float | None = None
    rsi_14:             float | None = None
    rsi_28:             float | None = None
    ret_1h:             float | None = None
    ret_2h:             float | None = None
    ret_3h:             float | None = None
    ret_4h:             float | None = None
    sigma_24h:          float | None = None
    sigma_24h_med_30d:  float | None = None
    taker_ratio_1h:     float | None = None
    taker_ratio_2h:     float | None = None
    taker_ratio_3h:     float | None = None
    bb_pct:             float | None = None    # (close - sma20) / (2*sd20); ±1 = touching ±2σ band
    macd_hist:          float | None = None
    macd_hist_z:        float | None = None    # macd_hist / 30d rolling std of macd_hist
    high_24h:           float | None = None
    low_24h:            float | None = None
    dist_to_24h_hi_pct: float | None = None
    dist_to_24h_lo_pct: float | None = None


# ── Feature math helpers ─────────────────────────────────────────────────────

def _rsi_wilder(closes: list[float], period: int) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l <= 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def _ema(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1.0 - alpha) * out[-1])
    return out


def _stdev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return statistics.stdev(values)


def fetch_recent_1h_klines(symbol: str = "BTCUSDT", limit: int = KLINES_LIMIT) -> list[list]:
    return _get(BINANCE_KLINES.format(symbol=symbol, interval="1h", limit=limit))


def compute_features() -> Features | None:
    """
    Pull last `KLINES_LIMIT` 1H klines, drop the in-progress hour, compute every
    feature any strategy might need. Mirrors the backtest's `bars_1h.shift(2)`
    alignment — features are observable at fire time t in [H-40m, H-10m] because
    the most-recent-completed 1H bar closed at H-1:00.
    """
    try:
        klines = fetch_recent_1h_klines(limit=KLINES_LIMIT)
    except (URLError, Exception) as exc:  # noqa: BLE001
        log.warning("binance klines fetch failed: %s", exc)
        return None

    if not isinstance(klines, list) or len(klines) < 50:
        return None

    completed = klines[:-1]
    closes:    list[float] = []
    volumes:   list[float] = []
    taker_buy: list[float] = []
    for k in completed:
        try:
            closes.append(float(k[4]))
            volumes.append(float(k[5]))
            taker_buy.append(float(k[9]))
        except (ValueError, IndexError):
            return None

    f = Features()
    f.spot   = closes[-1]
    f.rsi_7  = _rsi_wilder(closes, 7)
    f.rsi_14 = _rsi_wilder(closes, 14)
    f.rsi_28 = _rsi_wilder(closes, 28)

    if len(closes) >= 2: f.ret_1h = math.log(closes[-1] / closes[-2])
    if len(closes) >= 3: f.ret_2h = math.log(closes[-1] / closes[-3])
    if len(closes) >= 4: f.ret_3h = math.log(closes[-1] / closes[-4])
    if len(closes) >= 5: f.ret_4h = math.log(closes[-1] / closes[-5])

    # Rolling 1-hour log returns (used for sigma_24h and its 30-day median)
    rets_1h = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(rets_1h) >= 24:
        f.sigma_24h = _stdev(rets_1h[-24:])
        # 30-day rolling median of sigma_24h ≈ 720 trailing windows
        if len(rets_1h) >= 24 + 60:
            sigmas: list[float] = []
            n_windows = min(720, len(rets_1h) - 23)
            for end in range(len(rets_1h) - n_windows, len(rets_1h)):
                window = rets_1h[end - 24 + 1:end + 1]
                s = _stdev(window) if len(window) >= 24 else None
                if s is not None:
                    sigmas.append(s)
            if sigmas:
                f.sigma_24h_med_30d = statistics.median(sigmas)

    # Taker-buy ratios over N completed hours (volume-weighted)
    def _flow(n: int) -> float | None:
        if len(volumes) < n: return None
        v_tot = sum(volumes[-n:])
        if v_tot <= 0: return None
        return sum(taker_buy[-n:]) / v_tot
    f.taker_ratio_1h = _flow(1)
    f.taker_ratio_2h = _flow(2)
    f.taker_ratio_3h = _flow(3)

    # 24h high / low and distance to them
    if len(closes) >= 24:
        f.high_24h = max(closes[-24:])
        f.low_24h  = min(closes[-24:])
        if f.high_24h > 0: f.dist_to_24h_hi_pct = (f.spot - f.high_24h) / f.high_24h
        if f.low_24h  > 0: f.dist_to_24h_lo_pct = (f.spot - f.low_24h)  / f.low_24h

    # Bollinger Bands 20/2σ on 1H closes
    if len(closes) >= 20:
        win20 = closes[-20:]
        sma20 = sum(win20) / 20.0
        sd20 = _stdev(win20)
        if sd20 and sd20 > 0:
            f.bb_pct = (closes[-1] - sma20) / (2.0 * sd20)

    # MACD: 12/26 EMA + 9-EMA signal; histogram = macd - signal
    if len(closes) >= 26 + 9:
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd_line = [a - b for a, b in zip(ema12, ema26)]
        macd_sig  = _ema(macd_line, 9)
        macd_hist_series = [m - s for m, s in zip(macd_line, macd_sig)]
        f.macd_hist = macd_hist_series[-1]
        # 30-day rolling std of macd_hist for z-score
        tail = macd_hist_series[-min(720, len(macd_hist_series)):]
        std = _stdev(tail) if len(tail) >= 50 else None
        if std and std > 0:
            f.macd_hist_z = f.macd_hist / std

    return f


# ── Strategies (1:1 with backtest) ───────────────────────────────────────────

def _p1_trend_3h(f: Features, threshold: float) -> str | None:
    if f.ret_3h is None: return None
    if f.ret_3h >  threshold: return "up"
    if f.ret_3h < -threshold: return "down"
    return None


def _p2_reversion_1h(f: Features, threshold: float) -> str | None:
    """Bet AGAINST the 1H move when |ret_1h| > threshold."""
    if f.ret_1h is None: return None
    if f.ret_1h >  threshold: return "down"
    if f.ret_1h < -threshold: return "up"
    return None


def _p3_rsi_mr(f: Features, hi: float, lo: float) -> str | None:
    if f.rsi_14 is None: return None
    if f.rsi_14 > hi: return "down"
    if f.rsi_14 < lo: return "up"
    return None


def _p4_flow_2h(f: Features, hi: float, lo: float) -> str | None:
    """Continuation flow signal — taker_ratio_2h ≥ hi → up, ≤ lo → down."""
    if f.taker_ratio_2h is None: return None
    if f.taker_ratio_2h >= hi: return "up"
    if f.taker_ratio_2h <= lo: return "down"
    return None


def _p5_trend_flow(f: Features, ret_thr: float = 0.005,
                   flow_hi: float = 0.55, flow_lo: float = 0.45) -> str | None:
    if f.ret_3h is None or f.taker_ratio_2h is None: return None
    up = (f.ret_3h >  ret_thr) and (f.taker_ratio_2h >= flow_hi)
    dn = (f.ret_3h < -ret_thr) and (f.taker_ratio_2h <= flow_lo)
    if up: return "up"
    if dn: return "down"
    return None


def _p7_bb_mr(f: Features, threshold: float) -> str | None:
    if f.bb_pct is None: return None
    if f.bb_pct >=  threshold: return "down"
    if f.bb_pct <= -threshold: return "up"
    return None


def _p8_rsi_x_1h(f: Features, hi: float = 70.0, lo: float = 30.0,
                 ret1h_thr: float = 0.01) -> str | None:
    if f.rsi_14 is None or f.ret_1h is None: return None
    overbought = (f.rsi_14 > hi) and (f.ret_1h >  ret1h_thr)
    oversold   = (f.rsi_14 < lo) and (f.ret_1h < -ret1h_thr)
    if overbought: return "down"
    if oversold:   return "up"
    return None


def _p9_rsi_lowvol(f: Features, hi: float = 70.0, lo: float = 30.0) -> str | None:
    if f.rsi_14 is None or f.sigma_24h is None or f.sigma_24h_med_30d is None: return None
    if f.sigma_24h >= f.sigma_24h_med_30d: return None    # only fire in low-vol regime
    if f.rsi_14 > hi: return "down"
    if f.rsi_14 < lo: return "up"
    return None


def _p10_rsi_slow(f: Features, hi: float, lo: float) -> str | None:
    if f.rsi_28 is None: return None
    if f.rsi_28 > hi: return "down"
    if f.rsi_28 < lo: return "up"
    return None


def _p11_rsi_fast(f: Features, hi: float, lo: float) -> str | None:
    if f.rsi_7 is None: return None
    if f.rsi_7 > hi: return "down"
    if f.rsi_7 < lo: return "up"
    return None


def _p12_macd_revert(f: Features, z_threshold: float) -> str | None:
    if f.macd_hist_z is None: return None
    if f.macd_hist_z >=  z_threshold: return "down"   # MACD-hist extremely positive → revert down
    if f.macd_hist_z <= -z_threshold: return "up"
    return None


def _p13_24h_extreme(f: Features, threshold_pct: float) -> str | None:
    if f.dist_to_24h_hi_pct is None or f.dist_to_24h_lo_pct is None: return None
    at_high = f.dist_to_24h_hi_pct >= -threshold_pct
    at_low  = f.dist_to_24h_lo_pct <=  threshold_pct
    if at_high and at_low: return None     # tiny-range degenerate hour — skip
    if at_high: return "down"
    if at_low:  return "up"
    return None


def _p14_rsi_strict(f: Features) -> str | None:
    return _p3_rsi_mr(f, hi=80.0, lo=20.0)


def _p15_triple_conf(f: Features) -> str | None:
    if f.rsi_14 is None or f.bb_pct is None or f.ret_1h is None: return None
    overbought = (f.rsi_14 > 70) and (f.bb_pct >=  1.0) and (f.ret_1h >  0.01)
    oversold   = (f.rsi_14 < 30) and (f.bb_pct <= -1.0) and (f.ret_1h < -0.01)
    if overbought: return "down"
    if oversold:   return "up"
    return None


@dataclass
class Strategy:
    name: str
    evaluate: Callable[[Features], str | None]


STRATEGIES_ALL: list[Strategy] = [
    Strategy("P1_trend_3h_0.5%",     lambda f: _p1_trend_3h(f, 0.005)),
    Strategy("P1_trend_3h_1.0%",     lambda f: _p1_trend_3h(f, 0.010)),
    Strategy("P2_reversion_1h_1%",   lambda f: _p2_reversion_1h(f, 0.010)),
    Strategy("P2_reversion_1h_2%",   lambda f: _p2_reversion_1h(f, 0.020)),
    Strategy("P3_rsi_mr_70_30",      lambda f: _p3_rsi_mr(f, 70.0, 30.0)),
    Strategy("P3_rsi_mr_75_25",      lambda f: _p3_rsi_mr(f, 75.0, 25.0)),
    Strategy("P4_flow_2h_55_45",     lambda f: _p4_flow_2h(f, 0.55, 0.45)),
    Strategy("P4_flow_2h_52_48",     lambda f: _p4_flow_2h(f, 0.52, 0.48)),
    Strategy("P5_trend+flow",        lambda f: _p5_trend_flow(f)),
    Strategy("P7_BB_MR_1.0",         lambda f: _p7_bb_mr(f, 1.0)),
    Strategy("P7_BB_MR_1.5",         lambda f: _p7_bb_mr(f, 1.5)),
    Strategy("P8_RSI_x_1H_70_30",    lambda f: _p8_rsi_x_1h(f)),
    Strategy("P9_RSI_lowvol",        lambda f: _p9_rsi_lowvol(f)),
    Strategy("P10_RSI28_65_35",      lambda f: _p10_rsi_slow(f, 65.0, 35.0)),
    Strategy("P10_RSI28_70_30",      lambda f: _p10_rsi_slow(f, 70.0, 30.0)),
    Strategy("P11_RSI7_75_25",       lambda f: _p11_rsi_fast(f, 75.0, 25.0)),
    Strategy("P11_RSI7_80_20",       lambda f: _p11_rsi_fast(f, 80.0, 20.0)),
    Strategy("P12_MACD_z1.5",        lambda f: _p12_macd_revert(f, 1.5)),
    Strategy("P12_MACD_z2.0",        lambda f: _p12_macd_revert(f, 2.0)),
    Strategy("P13_24h_extreme_0",    lambda f: _p13_24h_extreme(f, 0.0)),
    Strategy("P13_24h_extreme_0.2",  lambda f: _p13_24h_extreme(f, 0.002)),
    Strategy("P14_RSI_strict_80_20", _p14_rsi_strict),
    Strategy("P15_triple_conf",      _p15_triple_conf),
]


def select_strategies(names_csv: str | None) -> list[Strategy]:
    if not names_csv or names_csv.lower() == "all":
        return STRATEGIES_ALL
    names = {n.strip() for n in names_csv.split(",")}
    out = [s for s in STRATEGIES_ALL if s.name in names]
    if not out:
        raise SystemExit("--strategies didn't match anything; known: "
                         + ", ".join(s.name for s in STRATEGIES_ALL))
    return out


# ── DB ────────────────────────────────────────────────────────────────────────

def _connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def init_db() -> None:
    """Create tables + sequences if absent. Idempotent — safe at every startup."""
    con = _connect()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS fires (
                id              BIGINT,
                recorded_at     TIMESTAMPTZ,
                target_hour_utc TIMESTAMPTZ,
                end_utc         TIMESTAMPTZ,
                fire_offset_min INTEGER,
                strategy        VARCHAR,
                side            VARCHAR,
                rsi_14          DOUBLE,
                ret_1h          DOUBLE,
                ret_3h          DOUBLE,
                slug            VARCHAR,
                condition_id    VARCHAR,
                up_token        VARCHAR,
                down_token      VARCHAR,
                up_bid          DOUBLE,
                up_ask          DOUBLE,
                down_bid        DOUBLE,
                down_ask        DOUBLE,
                entry_price     DOUBLE,
                resolved_at     TIMESTAMPTZ,
                candle_open     DOUBLE,
                candle_close    DOUBLE,
                winner          VARCHAR,
                won             INTEGER,
                pnl_per_share   DOUBLE
            )
        """)
        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_btc_pre_candle START 1")
        # Idempotent feature-column additions for new strategies (P4, P5, P7-P15).
        new_cols = [
            ("rsi_7",              "DOUBLE"),
            ("rsi_28",             "DOUBLE"),
            ("ret_2h",             "DOUBLE"),
            ("ret_4h",             "DOUBLE"),
            ("sigma_24h",          "DOUBLE"),
            ("sigma_24h_med_30d",  "DOUBLE"),
            ("taker_ratio_1h",     "DOUBLE"),
            ("taker_ratio_2h",     "DOUBLE"),
            ("taker_ratio_3h",     "DOUBLE"),
            ("bb_pct",             "DOUBLE"),
            ("macd_hist",          "DOUBLE"),
            ("macd_hist_z",        "DOUBLE"),
            ("high_24h",           "DOUBLE"),
            ("low_24h",            "DOUBLE"),
            ("dist_to_24h_hi_pct", "DOUBLE"),
            ("dist_to_24h_lo_pct", "DOUBLE"),
            ("spot_at_fire",       "DOUBLE"),
        ]
        for col, ddl in new_cols:
            try:
                con.execute(f"ALTER TABLE fires ADD COLUMN IF NOT EXISTS {col} {ddl}")
            except Exception:
                cols = {r[1] for r in con.execute("PRAGMA table_info('fires')").fetchall()}
                if col not in cols:
                    con.execute(f"ALTER TABLE fires ADD COLUMN {col} {ddl}")

        con.execute("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id              BIGINT,
                placed_at       TIMESTAMPTZ,
                target_hour_utc TIMESTAMPTZ,
                strategy        VARCHAR,
                side            VARCHAR,
                slug            VARCHAR,
                condition_id    VARCHAR,
                token_id        VARCHAR,
                shares          DOUBLE,
                entry_price     DOUBLE,
                cost_usdc       DOUBLE,
                order_id        VARCHAR,
                fire_id         BIGINT,
                resolved_at     TIMESTAMPTZ,
                outcome         VARCHAR,
                pnl_usdc        DOUBLE
            )
        """)
        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_btc_live_trades START 1")
    finally:
        con.close()


def _live_dedup_hit(con: duckdb.DuckDBPyConnection, strategy: str,
                    target_hour_utc: datetime) -> bool:
    return con.execute(
        "SELECT 1 FROM live_trades WHERE strategy = ? AND target_hour_utc = ? LIMIT 1",
        [strategy, target_hour_utc],
    ).fetchone() is not None


def _live_spent_today(con: duckdb.DuckDBPyConnection, now_utc: datetime) -> float:
    """Sum of cost_usdc for live trades placed in the current UTC day."""
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    row = con.execute(
        "SELECT COALESCE(SUM(cost_usdc), 0.0) FROM live_trades WHERE placed_at >= ?",
        [day_start],
    ).fetchone()
    return float(row[0] or 0.0)


def already_fired(con: duckdb.DuckDBPyConnection, target_hour_utc: datetime, strategy: str) -> bool:
    return con.execute(
        "SELECT 1 FROM fires WHERE target_hour_utc = ? AND strategy = ? LIMIT 1",
        [target_hour_utc, strategy],
    ).fetchone() is not None


# ── Polymarket lookup ─────────────────────────────────────────────────────────

def slug_for_target(target_hour_utc: datetime) -> str:
    return build_slug(target_hour_utc.astimezone(ET), asset="bitcoin")


def fetch_market(slug: str) -> dict | None:
    try:
        data = _get(GAMMA.format(slug=slug))
    except (URLError, Exception):  # noqa: BLE001
        return None
    if not data:
        return None
    event = data[0]
    markets = event.get("markets") or []
    if not markets:
        return None
    m = markets[0]
    if m.get("closed") or (m.get("active") is not None and not m.get("active")):
        return None

    def _load(key: str):
        v = m.get(key, [])
        return json.loads(v) if isinstance(v, str) else v

    outcomes  = _load("outcomes") or []
    token_ids = _load("clobTokenIds") or []
    up_token = down_token = None
    for i, name in enumerate(outcomes):
        nl = (name or "").strip().lower()
        if nl == "up"   and i < len(token_ids): up_token   = str(token_ids[i])
        if nl == "down" and i < len(token_ids): down_token = str(token_ids[i])
    if not up_token or not down_token:
        return None

    end_str = m.get("endDate") or event.get("endDate") or ""
    end_utc = None
    try:
        dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    return {
        "slug":         event.get("slug", slug),
        "up_token":     up_token,
        "down_token":   down_token,
        "condition_id": str(m.get("conditionId") or ""),
        "end_utc":      end_utc,
    }


def fetch_books(up_token: str, down_token: str) -> dict[str, dict]:
    from automata.client import get_best_books_bulk
    return get_best_books_bulk(HOST, [up_token, down_token])


# ── Settlement ────────────────────────────────────────────────────────────────

def fetch_candle_for_hour(target_hour_utc: datetime, symbol: str = "BTCUSDT") -> tuple[float, float] | None:
    start_ms = int(target_hour_utc.timestamp() * 1000)
    url = (f"https://api.binance.com/api/v3/klines"
           f"?symbol={symbol}&interval=1h&startTime={start_ms}&limit=1")
    try:
        klines = _get(url)
    except (URLError, Exception):  # noqa: BLE001
        return None
    if not isinstance(klines, list) or not klines:
        return None
    row = klines[0]
    try:
        if int(row[0]) != start_ms:
            return None
        return float(row[1]), float(row[4])
    except (ValueError, IndexError):
        return None


def settle_resolved(con: duckdb.DuckDBPyConnection, now_utc: datetime) -> int:
    rows = con.execute("""
        SELECT id, target_hour_utc, side, entry_price
          FROM fires
         WHERE resolved_at IS NULL
           AND end_utc <= ?
    """, [now_utc]).fetchall()
    n = 0
    cache: dict = {}
    for fid, tgt, side, entry in rows:
        if entry is None:
            continue
        candle = cache.get(tgt)
        if candle is None:
            candle = fetch_candle_for_hour(tgt)
            cache[tgt] = candle
        if candle is None:
            continue
        c_open, c_close = candle
        winner = "up" if c_close >= c_open else "down"
        won = 1 if side == winner else 0
        pnl = (1.0 - entry) if won else (-entry)
        con.execute("""
            UPDATE fires
               SET resolved_at = ?, candle_open = ?, candle_close = ?,
                   winner = ?, won = ?, pnl_per_share = ?
             WHERE id = ?
        """, [now_utc, c_open, c_close, winner, won, pnl, fid])
        # Mirror outcome onto any live_trade that opened on this fire. Actual
        # USDC redeem is handled by automata/eth.py's _settle_resolved_trades
        # (which scans positions by funder address); we just update bookkeeping.
        con.execute("""
            UPDATE live_trades
               SET resolved_at = ?,
                   outcome     = ?,
                   pnl_usdc    = shares * ?
             WHERE fire_id = ?
               AND resolved_at IS NULL
        """, [now_utc, ("win" if won else "loss"), pnl, fid])
        log.info("settled id=%s tgt=%s side=%s winner=%s pnl=%+.4f",
                 fid, tgt.isoformat(), side, winner, pnl)
        n += 1
    return n


# ── Main loop ─────────────────────────────────────────────────────────────────

def candidate_target_hours(now_utc: datetime) -> list[datetime]:
    earliest = now_utc + timedelta(minutes=-FIRE_MAX_OFFSET)  # +10 min
    latest   = now_utc + timedelta(minutes=-FIRE_MIN_OFFSET)  # +40 min
    h = earliest.replace(minute=0, second=0, microsecond=0)
    if h < earliest:
        h += timedelta(hours=1)
    out: list[datetime] = []
    while h <= latest:
        out.append(h)
        h += timedelta(hours=1)
    return out


def _build_clob_client():
    """Build a Polymarket CLOB client from .env credentials. Returns None on failure.

    CLOB_API_KEY/SECRET/PASS are derived on demand from POLYMARKET_PRIVATE_KEY,
    mirroring automata/eth.py — only the private key + host need to be in .env.
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()

    host = os.getenv("POLYMARKET_HOST") or config.get_str(
        "POLYMARKET_HOST", "polymarket", "host", "https://clob.polymarket.com")
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        log.error("[btc_pre_candle] cannot --bet: POLYMARKET_PRIVATE_KEY not set in .env")
        return None

    from automata.client import build_client, derive_api_credentials
    sig_type = config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0)
    funder = os.getenv("POLYMARKET_FUNDER") or None
    try:
        creds = derive_api_credentials(
            host=host,
            private_key=private_key,
            funder=funder,
            signature_type=sig_type,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("[btc_pre_candle] cannot --bet: failed to derive CLOB creds: %s", exc)
        return None

    return build_client(
        host=host,
        private_key=private_key,
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        api_passphrase=creds.api_passphrase,
        funder=funder,
        signature_type=sig_type,
    )


def _try_place_live(con: duckdb.DuckDBPyConnection, client, *,
                    fire_id: int, strategy: str, side: str,
                    target_hour_utc: datetime, mkt: dict,
                    chosen_ask: float, now_utc: datetime) -> str:
    """
    Attempt to place a real 5-share buy on the chosen-side token.
    Returns one of: 'placed', 'not_allowed', 'ask_too_high', 'cap_hit',
    'dedup', 'order_failed'.
    """
    if strategy not in LIVE_STRATEGIES:
        return "not_allowed"
    if chosen_ask > MAX_ENTRY_ASK:
        log.info("[btc_pre_candle] LIVE skip strat=%s ask=%.4f > %.2f cap",
                 strategy, chosen_ask, MAX_ENTRY_ASK)
        return "ask_too_high"
    if _live_dedup_hit(con, strategy, target_hour_utc):
        return "dedup"
    cost_estimate = float(BET_SHARES) * float(chosen_ask)
    spent_today = _live_spent_today(con, now_utc)
    if spent_today + cost_estimate > DAILY_SPEND_CAP:
        log.warning("[btc_pre_candle] LIVE skip: daily cap hit "
                    "(spent=$%.2f + this=$%.2f > $%.2f)",
                    spent_today, cost_estimate, DAILY_SPEND_CAP)
        return "cap_hit"

    token = mkt["up_token"] if side == "up" else mkt["down_token"]
    from automata.client import place_no_order
    try:
        resp = place_no_order(client, token, float(chosen_ask), float(BET_SHARES))
        order_id = str(resp.get("orderID") or resp.get("id") or "?")
    except Exception as exc:
        log.error("[btc_pre_candle] LIVE order failed strat=%s slug=%s: %s",
                  strategy, mkt["slug"], exc)
        return "order_failed"

    cost_actual = round(float(BET_SHARES) * float(chosen_ask), 4)
    next_id = con.execute("SELECT nextval('seq_btc_live_trades')").fetchone()[0]
    con.execute("""
        INSERT INTO live_trades
            (id, placed_at, target_hour_utc, strategy, side,
             slug, condition_id, token_id, shares, entry_price, cost_usdc,
             order_id, fire_id, resolved_at, outcome, pnl_usdc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
    """, [
        int(next_id), now_utc, target_hour_utc, strategy, side,
        mkt["slug"], mkt["condition_id"], token,
        float(BET_SHARES), float(chosen_ask), cost_actual,
        order_id, int(fire_id),
    ])
    log.info("[btc_pre_candle] LIVE BUY strat=%s slug=%s side=%s ask=%.4f "
             "shares=%.2f cost=$%.2f order_id=%s",
             strategy, mkt["slug"], side, chosen_ask,
             float(BET_SHARES), cost_actual, order_id)
    return "placed"


def run_once(con: duckdb.DuckDBPyConnection, now_utc: datetime,
             strategies: list[Strategy], *, clob_client=None) -> dict:
    summary = {"fires_recorded": 0, "settled": 0,
               "skipped_already_fired": 0, "skipped_no_signal": 0,
               "skipped_no_market": 0, "by_strategy": {},
               "live_placed": 0, "live_skipped": 0}

    targets = candidate_target_hours(now_utc)
    if targets:
        feats = compute_features()
        if feats is None:
            log.warning("could not compute features; skipping cycle")
            summary["settled"] = settle_resolved(con, now_utc)
            return summary

        market_cache: dict[datetime, dict | None] = {}
        book_cache: dict[datetime, dict] = {}

        for tgt in targets:
            for strat in strategies:
                summary["by_strategy"].setdefault(strat.name, 0)
                if already_fired(con, tgt, strat.name):
                    summary["skipped_already_fired"] += 1
                    continue
                side = strat.evaluate(feats)
                if side is None:
                    summary["skipped_no_signal"] += 1
                    continue

                if tgt not in market_cache:
                    market_cache[tgt] = fetch_market(slug_for_target(tgt))
                mkt = market_cache[tgt]
                if not mkt:
                    summary["skipped_no_market"] += 1
                    continue
                if tgt not in book_cache:
                    book_cache[tgt] = fetch_books(mkt["up_token"], mkt["down_token"])
                books = book_cache[tgt]

                up = books.get(mkt["up_token"], {}) or {}
                dn = books.get(mkt["down_token"], {}) or {}
                chosen_ask = up.get("ask") if side == "up" else dn.get("ask")
                if chosen_ask is None or chosen_ask <= 0 or chosen_ask >= 1.0:
                    summary["skipped_no_market"] += 1
                    continue

                fire_offset = int((now_utc - tgt).total_seconds() / 60)
                next_id = con.execute("SELECT nextval('seq_btc_pre_candle')").fetchone()[0]
                con.execute("""
                    INSERT INTO fires (
                        id, recorded_at, target_hour_utc, end_utc, fire_offset_min,
                        strategy, side,
                        rsi_7, rsi_14, rsi_28,
                        ret_1h, ret_2h, ret_3h, ret_4h,
                        sigma_24h, sigma_24h_med_30d,
                        taker_ratio_1h, taker_ratio_2h, taker_ratio_3h,
                        bb_pct, macd_hist, macd_hist_z,
                        spot_at_fire, high_24h, low_24h,
                        dist_to_24h_hi_pct, dist_to_24h_lo_pct,
                        slug, condition_id, up_token, down_token,
                        up_bid, up_ask, down_bid, down_ask, entry_price
                    ) VALUES (
                        ?,?,?,?,?, ?,?,
                        ?,?,?,
                        ?,?,?,?,
                        ?,?,
                        ?,?,?,
                        ?,?,?,
                        ?,?,?,
                        ?,?,
                        ?,?,?,?,
                        ?,?,?,?, ?
                    )
                """, [
                    int(next_id), now_utc, tgt, mkt["end_utc"], fire_offset,
                    strat.name, side,
                    feats.rsi_7, feats.rsi_14, feats.rsi_28,
                    feats.ret_1h, feats.ret_2h, feats.ret_3h, feats.ret_4h,
                    feats.sigma_24h, feats.sigma_24h_med_30d,
                    feats.taker_ratio_1h, feats.taker_ratio_2h, feats.taker_ratio_3h,
                    feats.bb_pct, feats.macd_hist, feats.macd_hist_z,
                    feats.spot, feats.high_24h, feats.low_24h,
                    feats.dist_to_24h_hi_pct, feats.dist_to_24h_lo_pct,
                    mkt["slug"], mkt["condition_id"], mkt["up_token"], mkt["down_token"],
                    up.get("bid"), up.get("ask"), dn.get("bid"), dn.get("ask"),
                    float(chosen_ask),
                ])
                log.info(
                    "FIRE id=%s strat=%s slug=%s side=%s ask=%.4f "
                    "rsi14=%.1f rsi7=%.1f bb=%+.2f macd_z=%+.2f offset=%dm",
                    next_id, strat.name, mkt["slug"], side, chosen_ask,
                    feats.rsi_14 or float("nan"), feats.rsi_7 or float("nan"),
                    feats.bb_pct or float("nan"), feats.macd_hist_z or float("nan"),
                    fire_offset,
                )
                summary["fires_recorded"] += 1
                summary["by_strategy"][strat.name] += 1

                if clob_client is not None and strat.name in LIVE_STRATEGIES:
                    outcome = _try_place_live(
                        con, clob_client,
                        fire_id=int(next_id), strategy=strat.name, side=side,
                        target_hour_utc=tgt, mkt=mkt,
                        chosen_ask=float(chosen_ask), now_utc=now_utc,
                    )
                    if outcome == "placed":
                        summary["live_placed"] += 1
                    else:
                        summary["live_skipped"] += 1

    summary["settled"] = settle_resolved(con, now_utc)
    return summary


def run_daemon(interval: int = 60, once: bool = False, strategies_csv: str | None = None,
               bet: bool = False) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s  %(levelname)-7s  %(message)s")
    init_db()
    strategies = select_strategies(strategies_csv)

    clob_client = None
    if bet:
        clob_client = _build_clob_client()
        if clob_client is None:
            log.error("[btc_pre_candle] --bet requested but CLOB client failed; "
                      "running in shadow-only mode this session.")
        else:
            log.info("[btc_pre_candle] LIVE betting ENABLED. allowlist=%s "
                     "shares=%.2f max_ask=%.2f daily_cap=$%.2f",
                     sorted(LIVE_STRATEGIES), BET_SHARES, MAX_ENTRY_ASK, DAILY_SPEND_CAP)

    log.info("btc_pre_candle daemon starting; db=%s, interval=%ds, %d strategies, bet=%s",
             DB_PATH, interval, len(strategies), bool(clob_client))

    while True:
        now_utc = datetime.now(timezone.utc)
        con = _connect()
        try:
            summary = run_once(con, now_utc, strategies, clob_client=clob_client)
            log.info("cycle %s", summary)
        except Exception:  # noqa: BLE001
            log.exception("cycle failed")
        finally:
            con.close()

        if once:
            return
        _time.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--strategies", default="all",
                    help='comma-separated strategy names, or "all" (default)')
    ap.add_argument("--bet", action="store_true",
                    help="Place real Polymarket orders on allowlisted strategies. "
                         "Defaults to shadow-only. Requires .env CLOB credentials. "
                         "Run `python -m automata.eth` alongside for redeem.")
    args = ap.parse_args()
    run_daemon(interval=args.interval, once=args.once,
               strategies_csv=args.strategies, bet=args.bet)


# Backwards-compat constant for any caller that imported the original single-strategy name.
STRATEGY_NAME = STRATEGIES_ALL[0].name


if __name__ == "__main__":
    main()
