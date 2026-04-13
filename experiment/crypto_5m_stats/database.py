"""Database queries for the trading dashboard."""
from __future__ import annotations

import os
import sqlite3
from collections import deque
from datetime import datetime, timezone

TIERS = [("1c", 0.01), ("2c", 0.02), ("3c", 0.03)]
LOG_TAIL = 200

_WIN_OUTCOMES = {"win"}
_LOSS_OUTCOMES = {"loss", "stop_loss", "expired"}


def query_crypto_stats(crypto_db_path: str) -> dict:
    """Query crypto 5m signal stats from crypto_5m.db."""
    if not os.path.exists(crypto_db_path):
        return {"error": "DB not found -- start crypto_5m_scanner first.", "assets": []}

    conn = sqlite3.connect(crypto_db_path)
    conn.row_factory = sqlite3.Row
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    if "tier" not in cols:
        conn.close()
        return {
            "error": "Tier view is disabled in edge mode. Use the 'BS Forward Test' tab.",
            "assets": [],
        }

    rows = conn.execute("""
        SELECT asset, side, tier, entry_price, candle_start, won, pnl
        FROM signals WHERE won IS NOT NULL
        ORDER BY asset, candle_start
    """).fetchall()
    all_rows = conn.execute("SELECT asset, side, tier, won FROM signals").fetchall()
    total_candles = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    total_resolved = conn.execute(
        "SELECT COUNT(*) FROM candles WHERE winner IS NOT NULL AND winner != '?'"
    ).fetchone()[0]
    last_signal = conn.execute("SELECT MAX(signal_ts) FROM signals").fetchone()[0]
    conn.close()

    by_asset: dict[str, list] = {}
    for row in rows:
        by_asset.setdefault(row["asset"], []).append(dict(row))

    all_by_asset: dict[str, list] = {}
    for row in all_rows:
        all_by_asset.setdefault(row["asset"], []).append(dict(row))

    all_assets = sorted(set(by_asset.keys()) | set(all_by_asset.keys()))
    assets_data: dict[str, dict] = {}

    for asset in all_assets:
        asset_rows = by_asset.get(asset, [])
        all_asset_rows = all_by_asset.get(asset, [])
        candle_starts = sorted(set(r["candle_start"] for r in asset_rows))

        series: dict[str, list[float]] = {"1c": [], "2c": [], "3c": []}
        labels: list[str] = []
        running: dict[str, float] = {"1c": 0.0, "2c": 0.0, "3c": 0.0}

        for cs in candle_starts:
            candle_rows = [r for r in asset_rows if r["candle_start"] == cs]
            dt = datetime.fromtimestamp(cs, tz=timezone.utc)
            labels.append(dt.strftime("%m/%d %H:%M"))
            for tier_key, threshold in TIERS:
                tier_rows = [r for r in candle_rows if round(r["tier"], 2) <= threshold]
                running[tier_key] += sum(r["pnl"] for r in tier_rows)
                series[tier_key].append(round(running[tier_key], 4))

        stats: dict[str, dict] = {}
        for tier_key, threshold in TIERS:
            resolved = [r for r in asset_rows if round(r["tier"], 2) <= threshold]
            pending = [
                r for r in all_asset_rows
                if round(r["tier"], 2) <= threshold and r["won"] is None
            ]
            wins = [r for r in resolved if r["won"] == 1]
            total_pnl = sum(r["pnl"] for r in resolved)
            total_cost = sum(r["entry_price"] for r in resolved)
            stats[tier_key] = {
                "signals": len(resolved) + len(pending),
                "resolved": len(resolved),
                "pending": len(pending),
                "wins": len(wins),
                "losses": len(resolved) - len(wins),
                "win_rate": round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
                "total_pnl": round(total_pnl, 4),
                "roi": round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
            }

        assets_data[asset] = {
            "chart": {
                "labels": labels,
                "series_1c": series["1c"],
                "series_2c": series["2c"],
                "series_3c": series["3c"],
            },
            "stats": stats,
        }

    last_update = (
        datetime.fromtimestamp(last_signal, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if last_signal
        else "--"
    )
    return {
        "assets": all_assets,
        "data": assets_data,
        "total_candles": total_candles,
        "total_resolved": total_resolved,
        "last_update": last_update,
    }


def query_eth_stats(bets_db_path: str) -> dict:
    """Query ETH 1H trade stats from bets.db."""
    if not os.path.exists(bets_db_path):
        return {"error": "bets.db not found -- start automata.eth first."}

    conn = sqlite3.connect(bets_db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, slug, direction, shares, entry_price, cost_usdc,
                   placed_at, mins_remaining, outcome, dry_run,
                   redeem_tx_hash, redeemed_at
            FROM eth_1h_trades
            ORDER BY placed_at ASC
        """
        ).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return {"error": "eth_1h_trades table not found -- start automata.eth first."}
    conn.close()

    trades = [dict(r) for r in rows]
    labels, cum_series = [], []
    running = 0.0

    for t in trades:
        if t["outcome"] in _WIN_OUTCOMES:
            t["pnl"] = round(t["shares"] * (1.0 - t["entry_price"]), 4)
        elif t["outcome"] in _LOSS_OUTCOMES:
            t["pnl"] = round(-t["shares"] * t["entry_price"], 4)
        else:
            t["pnl"] = None

        if t["pnl"] is not None:
            running += t["pnl"]
            try:
                dt = datetime.fromisoformat(t["placed_at"].replace("Z", "+00:00"))
                labels.append(dt.strftime("%m/%d %H:%M"))
            except Exception:
                labels.append((t["placed_at"] or "?")[:16])
            cum_series.append(round(running, 4))

    resolved = [t for t in trades if t["outcome"] in (_WIN_OUTCOMES | _LOSS_OUTCOMES)]
    wins = [t for t in resolved if t["outcome"] in _WIN_OUTCOMES]
    pending = [t for t in trades if t["outcome"] is None]
    total_pnl = sum(t["pnl"] for t in resolved)
    total_cost = sum(t["cost_usdc"] or 0 for t in trades)

    stats = {
        "total": len(trades),
        "resolved": len(resolved),
        "pending": len(pending),
        "wins": len(wins),
        "losses": len(resolved) - len(wins),
        "win_rate": round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
        "total_pnl": round(total_pnl, 4),
        "total_cost": round(total_cost, 2),
        "roi": round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
    }
    return {
        "stats": stats,
        "chart": {"labels": labels, "series": cum_series},
        "trades": list(reversed(trades)),  # newest first for table
    }


def read_eth_log_tail(eth_log_path: str) -> list[str]:
    """Read the last N lines from ETH log."""
    try:
        with open(eth_log_path, "r", encoding="utf-8", errors="replace") as f:
            return list(deque(f, maxlen=LOG_TAIL))
    except FileNotFoundError:
        return [f"Log file not found: {eth_log_path}"]


def read_bs_log_tail(bs_log_path: str) -> list[str]:
    """Read the last N lines from BS log."""
    try:
        with open(bs_log_path, "r", encoding="utf-8", errors="replace") as f:
            return list(deque(f, maxlen=LOG_TAIL))
    except FileNotFoundError:
        return [f"Log file not found: {bs_log_path}"]


def query_weather_stats(bets_db_path: str) -> dict:
    """Query weather bet stats from bets.db."""
    if not os.path.exists(bets_db_path):
        return {"error": "bets.db not found.", "bets": []}

    conn = sqlite3.connect(bets_db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, placed_at_utc, city, event_date, question, option,
                   shares, no_price, cost_usdc, forecast_high,
                   forecast_minus_threshold, outcome, resolved_temp
            FROM placed_bets
            ORDER BY placed_at_utc DESC
        """
        ).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return {"error": "placed_bets table not found.", "bets": []}
    conn.close()

    bets = [dict(r) for r in rows]
    wins = [b for b in bets if b["outcome"] == "win"]
    losses = [b for b in bets if b["outcome"] == "loss"]
    pending = [b for b in bets if b["outcome"] is None]
    resolved = wins + losses
    total_pnl = sum(
        (b["shares"] * (1.0 - (b["no_price"] or 0)))
        if b["outcome"] == "win"
        else (-b["shares"] * (b["no_price"] or 0))
        for b in resolved
    )
    total_cost = sum(b["cost_usdc"] or 0 for b in bets)

    stats = {
        "total": len(bets),
        "resolved": len(resolved),
        "pending": len(pending),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(resolved) * 100, 2) if resolved else 0,
        "total_pnl": round(total_pnl, 4),
        "total_cost": round(total_cost, 2),
    }
    return {"stats": stats, "bets": bets}


def query_bs_forward(crypto_db_path: str, min_edge_filter: float = 0.0) -> dict:
    """Query BS forward test signals from crypto_5m.db."""
    if not os.path.exists(crypto_db_path):
        return {"error": "DB not found -- start crypto_5m_scanner first.", "assets": []}

    conn = sqlite3.connect(crypto_db_path)
    conn.row_factory = sqlite3.Row

    # Check columns exist (older DBs may not have fair_price/edge yet)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)")}
    has_bs = "fair_price" in cols and "edge" in cols

    if has_bs:
        rows = conn.execute(
            """
            SELECT slug, asset, candle_start, signal_ts, secs_remaining,
                   side, entry_price, shares,
                   fair_price, edge, winner, won, pnl
            FROM signals
            WHERE fair_price IS NOT NULL
              AND asset = 'BTC'
              AND edge >= ?
            ORDER BY signal_ts ASC
        """,
            (min_edge_filter,),
        ).fetchall()
    else:
        rows = []

    all_rows_count = (
        conn.execute(
            "SELECT COUNT(*) FROM signals WHERE fair_price IS NOT NULL AND asset = 'BTC'"
        ).fetchone()[0]
        if has_bs
        else 0
    )
    conn.close()

    if not rows:
        msg = (
            "No BTC BS signals yet -- run:  "
            "python -m experiment.crypto_5m_scanner --min-edge 0.05"
            if has_bs or all_rows_count == 0
            else f"No signals with edge >= {min_edge_filter:.3f}"
        )
        return {"error": msg, "assets": [], "total": 0, "total_pnl": 0}

    by_asset: dict[str, list] = {}
    for r in rows:
        by_asset.setdefault(r["asset"], []).append(dict(r))

    all_assets = sorted(by_asset.keys())
    assets_data: dict[str, dict] = {}

    for asset in all_assets:
        asset_rows = by_asset[asset]

        # cumulative P&L series (time-ordered, resolved only)
        resolved_rows = [r for r in asset_rows if r["won"] is not None]
        labels: list[str] = []
        timestamps: list[int] = []
        cum_series: list[float] = []
        running = 0.0
        for r in resolved_rows:
            running += r["pnl"] or 0
            dt = datetime.fromtimestamp(r["signal_ts"], tz=timezone.utc)
            labels.append(dt.strftime("%m/%d %H:%M"))
            timestamps.append(r["signal_ts"])
            cum_series.append(round(running, 4))

        wins = [r for r in resolved_rows if r["won"] == 1]
        pending = [r for r in asset_rows if r["won"] is None]
        total_pnl = sum(r["pnl"] or 0 for r in resolved_rows)
        total_cost = sum(r["entry_price"] * r["shares"] for r in asset_rows)
        avg_edge = (
            sum(r["edge"] for r in asset_rows if r["edge"] is not None)
            / max(1, len([r for r in asset_rows if r["edge"] is not None]))
        )

        stats = {
            "signals": len(asset_rows),
            "resolved": len(resolved_rows),
            "pending": len(pending),
            "wins": len(wins),
            "losses": len(resolved_rows) - len(wins),
            "win_rate": round(len(wins) / len(resolved_rows) * 100, 2)
            if resolved_rows
            else 0,
            "total_pnl": round(total_pnl, 4),
            "total_cost": round(total_cost, 4),
            "roi": round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
            "avg_edge": round(avg_edge, 4),
        }

        assets_data[asset] = {
            "chart": {"labels": labels, "series": cum_series, "timestamps": timestamps},
            "stats": stats,
            "trades": list(reversed(asset_rows)),  # newest first
        }

    total_pnl = sum(r["pnl"] or 0 for r in rows if r["won"] is not None)
    return {
        "assets": all_assets,
        "data": assets_data,
        "total": len(rows),
        "total_pnl": round(total_pnl, 4),
    }


def update_weather_bet(
    bets_db_path: str, bet_id: int, resolved_temp: float | None, outcome: str | None
) -> None:
    """Update weather bet outcome and resolved temperature."""
    with sqlite3.connect(bets_db_path) as conn:
        conn.execute(
            "UPDATE placed_bets SET resolved_temp = ?, outcome = ? WHERE id = ?",
            (resolved_temp, outcome, bet_id),
        )
