#!/usr/bin/env python3
"""
Black-Scholes edge backtest for crypto 5m Up/Down markets.

Uses Binance historical 1m candles to replay intra-candle price movement.
At each 1-minute mark within a 5m candle, computes the BS binary-option
probability (fair price) for each side.  Records a simulated trade whenever:

    edge = fair_price - hypothetical_ask  >=  --min-edge

Results are written to the `bs_backtest` table in experiment/crypto_5m.db.
Also prints a real-time tick log showing spot / strike / fair / edge.

Usage:
  python -m experiment.crypto_5m_bs_backtest
  python -m experiment.crypto_5m_bs_backtest --asset ETH --candles 300 --min-edge 0.05
  python -m experiment.crypto_5m_bs_backtest --asset BTC --candles 500 --tiers 0.01 0.02 0.03
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

# ── constants ──────────────────────────────────────────────────────────────────
BINANCE_BASE   = "https://api.binance.com/api/v3"
DB_PATH        = os.path.join("experiment", "crypto_5m.db")
VOL_WINDOW     = 100      # trailing 5m candles for rolling vol estimation
SECS_PER_YEAR  = 365 * 24 * 3600

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto5m.bs_backtest")


# ── DB ─────────────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS bs_backtest (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    asset          TEXT    NOT NULL,
    slug           TEXT    NOT NULL,
    candle_start   INTEGER NOT NULL,
    entry_ts       INTEGER NOT NULL,    -- epoch when signal fires (1m bar close)
    secs_remaining INTEGER NOT NULL,    -- seconds left in the 5m candle
    side           TEXT    NOT NULL,    -- 'Up' / 'Down'
    ask_price      REAL    NOT NULL,    -- hypothetical ask (tier being tested)
    fair_price     REAL    NOT NULL,    -- BS binary-option probability
    edge           REAL    NOT NULL,    -- fair_price - ask_price
    sigma          REAL    NOT NULL,    -- annualised vol used
    spot           REAL    NOT NULL,    -- S: intra-candle price at entry
    strike         REAL    NOT NULL,    -- K: 5m candle open price
    winner         TEXT    NOT NULL,    -- 'Up' / 'Down'
    won            INTEGER NOT NULL,    -- 1 / 0
    pnl            REAL    NOT NULL,    -- per-share P/L (1-ask if won, -ask if lost)
    UNIQUE(asset, candle_start, secs_remaining, side, ask_price)
);
CREATE INDEX IF NOT EXISTS idx_bsbt_asset ON bs_backtest(asset, candle_start);
"""


def _init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL)
    conn.commit()
    return conn


# ── Black-Scholes binary option ────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF — implemented via math.erfc (no scipy needed)."""
    return 0.5 * math.erfc(-x / math.sqrt(2))


def bs_prob_up(S: float, K: float, T_secs: float, sigma_5m: float) -> float:
    """
    Risk-neutral probability that asset price ends *above* K.

    S        — current spot price
    K        — strike (5m candle open price)
    T_secs   — seconds remaining until expiry
    sigma_5m — per-bar std dev of 5m log returns (NOT annualised)

    Time is converted to T_bars = T_secs / 300 so that sigma and T
    are in the same native 5-minute units.
    """
    T_bars = T_secs / 300.0
    if T_bars <= 0 or sigma_5m <= 1e-9 or S <= 0 or K <= 0:
        return 1.0 if S > K else (0.5 if S == K else 0.0)
    try:
        d2 = (math.log(S / K) - 0.5 * sigma_5m ** 2 * T_bars) / (sigma_5m * math.sqrt(T_bars))
        return _norm_cdf(d2)
    except (ValueError, ZeroDivisionError):
        return 0.5


def estimate_vol(closes: list[float]) -> float:
    """
    Per-bar (5m) log-return std dev — raw, NOT annualised.
    E.g. 0.0007 ≈ 0.07% typical move per 5m candle.
    """
    if len(closes) < 2:
        return 0.001
    log_rets = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    if len(log_rets) < 1:
        return 0.001
    mean = sum(log_rets) / len(log_rets)
    var  = sum((r - mean) ** 2 for r in log_rets) / max(1, len(log_rets) - 1)
    return math.sqrt(var)            # pure per-bar std dev


# ── Binance helpers ────────────────────────────────────────────────────────────

def _get_klines(
    symbol: str,
    interval: str,
    limit: int = 500,
    start_ms: int | None = None,
    end_ms:   int | None = None,
) -> list:
    params: dict = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms
    r = requests.get(
        f"{BINANCE_BASE}/klines",
        params=params,
        timeout=15,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    r.raise_for_status()
    return r.json()


# ── DB write ───────────────────────────────────────────────────────────────────

def _insert_trade(conn: sqlite3.Connection, t: dict) -> bool:
    """Insert one backtest trade.  Returns True if newly inserted, False if duplicate."""
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO bs_backtest
                (asset, slug, candle_start, entry_ts, secs_remaining, side, ask_price,
                 fair_price, edge, sigma, spot, strike, winner, won, pnl)
            VALUES
                (:asset, :slug, :candle_start, :entry_ts, :secs_remaining, :side, :ask_price,
                 :fair_price, :edge, :sigma, :spot, :strike, :winner, :won, :pnl)
            """,
            t,
        )
        conn.commit()
        return bool(conn.execute("SELECT changes()").fetchone()[0])
    except Exception as exc:
        log.debug("DB insert: %s", exc)
        return False


# ── summary ────────────────────────────────────────────────────────────────────

def _print_summary(trades: list[dict], shares: float) -> None:
    if not trades:
        print("\nNo trades generated — try lowering --min-edge or increasing --candles.\n")
        return

    by_tier: dict[float, list] = {}
    for t in trades:
        by_tier.setdefault(t["ask_price"], []).append(t)

    total_n   = len(trades)
    total_pnl = sum(t["pnl"] for t in trades)
    avg_edge  = sum(t["edge"] for t in trades) / total_n

    print()
    print("=" * 76)
    print("  BLACK-SCHOLES BACKTEST RESULTS")
    print("-" * 76)
    print(f"  Total trades : {total_n}")
    print(f"  Total P/L    : ${total_pnl:+.4f}  (shares={shares:.1f})")
    print(f"  Avg BS edge  : {avg_edge:+.4f}")
    print("-" * 76)
    print(f"  {'Tier':>6}  {'N':>5}  {'Wins':>5}  {'WR':>7}  "
          f"{'P/L':>10}  {'ROI':>8}  {'AvgEdge':>9}")
    print("-" * 76)
    for tier in sorted(by_tier.keys()):
        ts    = by_tier[tier]
        n     = len(ts)
        wins  = sum(1 for t in ts if t["won"])
        wr    = wins / n * 100 if n else 0
        pnl   = sum(t["pnl"] for t in ts)
        cost  = tier * shares * n
        roi   = pnl / cost * 100 if cost else 0
        ae    = sum(t["edge"] for t in ts) / n if n else 0
        print(f"  ${tier:.2f}  {n:>5}  {wins:>5}  {wr:>6.1f}%  "
              f"${pnl:>+9.4f}  {roi:>+7.1f}%  {ae:>+9.4f}")
    print("=" * 76)
    print()


# ── main backtest logic ────────────────────────────────────────────────────────

def run(
    asset:     str,
    candles_n: int,
    min_edge:  float,
    tiers:     list[float],
    shares:    float,
    db_path:   str,
) -> None:
    symbol = f"{asset.upper()}USDT"
    conn   = _init_db(db_path)

    # ── fetch historical 5m candles ──────────────────────────────────────────
    fetch_n = min(candles_n + VOL_WINDOW + 10, 1000)
    log.info("Fetching %d 5m candles for %s …", fetch_n, symbol)
    try:
        klines_5m = _get_klines(symbol, "5m", limit=fetch_n)
    except Exception as exc:
        log.error("Could not fetch 5m candles: %s", exc)
        return

    total = len(klines_5m)
    if total < VOL_WINDOW + 5:
        log.error("Only %d candles available — need %d+.  Aborting.", total, VOL_WINDOW)
        return

    log.info("Got %d 5m candles  (backtesting last %d)", total, min(candles_n, total - VOL_WINDOW))
    all_closes = [float(k[4]) for k in klines_5m]

    test_start = max(0, total - candles_n)
    trades: list[dict] = []

    for idx in range(test_start, total):
        k5       = klines_5m[idx]
        open_ts  = int(k5[0]) // 1000     # epoch seconds
        open_px  = float(k5[1])
        close_px = float(k5[4])
        close_ts = open_ts + 300
        winner   = "Up" if close_px > open_px else "Down"
        slug     = f"{asset.lower()}-updown-5m-{open_ts}"

        # Rolling vol from VOL_WINDOW preceding closes
        preceding = all_closes[max(0, idx - VOL_WINDOW): idx]
        sigma     = estimate_vol(preceding)

        # ── fetch 1m bars inside this 5m candle ─────────────────────────────
        try:
            bars_1m = _get_klines(
                symbol, "1m",
                limit    = 5,
                start_ms = open_ts  * 1000,
                end_ms   = (close_ts - 1) * 1000,
            )
            time.sleep(0.08)   # ~12 req/s — well within Binance's 1 200/min limit
        except Exception as exc:
            log.warning("1m fetch failed for %s: %s", slug, exc)
            continue

        candle_dt = datetime.fromtimestamp(open_ts, tz=timezone.utc)
        log.info(
            "CANDLE  %-35s  %s  K=%.4f→%.4f  σ_5m=%.5f(%.4f%%/bar)  winner=%s",
            slug, candle_dt.strftime("%m/%d %H:%M"),
            open_px, close_px, sigma, sigma * 100, winner,
        )

        for bar in bars_1m:
            bar_open_ts  = int(bar[0]) // 1000
            bar_close_px = float(bar[4])
            bar_close_ts = bar_open_ts + 60
            secs_rem     = close_ts - bar_close_ts

            if secs_rem <= 0 or secs_rem > 295:
                continue

            p_up   = bs_prob_up(bar_close_px, open_px, secs_rem, sigma)
            p_down = 1.0 - p_up

            # ── tick log ────────────────────────────────────────────────────
            log.info(
                "  secs=%3d  S=%-10.4f  K=%-10.4f  σ_5m=%.5f  T_bars=%.3f  "
                "P(Up)=%.4f  P(Down)=%.4f",
                secs_rem, bar_close_px, open_px, sigma,
                secs_rem / 300.0, p_up, p_down,
            )

            for tier in tiers:
                for side, fair in (("Up", p_up), ("Down", p_down)):
                    edge = fair - tier
                    log.info(
                        "    %-4s  ask=$%.2f  fair=%.4f  edge=%+.4f",
                        side, tier, fair, edge,
                    )
                    if edge < min_edge:
                        continue

                    won = 1 if side == winner else 0
                    pnl = round(shares * ((1.0 - tier) if won else -tier), 6)
                    t = {
                        "asset":         asset.upper(),
                        "slug":          slug,
                        "candle_start":  open_ts,
                        "entry_ts":      bar_close_ts,
                        "secs_remaining": secs_rem,
                        "side":          side,
                        "ask_price":     tier,
                        "fair_price":    round(fair, 6),
                        "edge":          round(edge, 6),
                        "sigma":         round(sigma, 6),
                        "spot":          bar_close_px,
                        "strike":        open_px,
                        "winner":        winner,
                        "won":           won,
                        "pnl":           pnl,
                    }
                    inserted = _insert_trade(conn, t)
                    if inserted or True:   # always show in log
                        trades.append(t)
                    marker = "★ SIGNAL" if inserted else "· DUP   "
                    log.info(
                        "  %s  %-4s  tier=$%.2f  fair=%.4f  edge=%+.4f  → %s",
                        marker, side, tier, fair, edge,
                        "WIN" if won else "LOSS",
                    )

    _print_summary(trades, shares)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="BS-edge backtest for crypto 5m Up/Down markets"
    )
    p.add_argument("--asset",    default="BTC",
                   help="Asset symbol (default BTC)")
    p.add_argument("--candles",  type=int, default=200,
                   help="Number of 5m candles to backtest (default 200)")
    p.add_argument("--min-edge", type=float, default=0.03,
                   help="Minimum BS edge to trigger a trade (default 0.03)")
    p.add_argument("--tiers",    type=float, nargs="+", default=[0.01, 0.02, 0.03],
                   help="Hypothetical ask prices to test (default 0.01 0.02 0.03)")
    p.add_argument("--shares",   type=float, default=1.0,
                   help="Shares per simulated trade (default 1.0)")
    p.add_argument("--db",       default=DB_PATH,
                   help="SQLite DB path")
    args = p.parse_args()
    try:
        run(
            asset     = args.asset,
            candles_n = args.candles,
            min_edge  = args.min_edge,
            tiers     = args.tiers,
            shares    = args.shares,
            db_path   = args.db,
        )
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
