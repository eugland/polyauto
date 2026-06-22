"""Flask app factory + JSON API for the dashboard.

Run via ``python -m dashboard``. Serves the SPA shell at ``/`` and a small
JSON API under ``/api/*``. Every handler is wrapped so one bad upstream
returns ``{error}`` instead of blanking the page.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from dashboard import backtest, db, polymarket_data, settings, stocks, wallet

log = logging.getLogger("dashboard.app")

_STATIC = Path(__file__).resolve().parent / "static"


def _api(fn):
    """Wrap a handler: exceptions -> {error} + 500."""
    @wraps(fn)
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s failed: %s", fn.__name__, exc)
            return jsonify({"error": str(exc)}), 500
    return inner


def _short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if len(addr) > 12 else addr


def create_app(start_poller: bool = True) -> Flask:
    app = Flask(__name__, static_folder=None)
    db.init_db()

    if start_poller:
        from dashboard.poller import start_thread
        start_thread()

    # ── Static shell ────────────────────────────────────────────────────
    @app.route("/")
    def index():
        return send_from_directory(_STATIC, "index.html")

    @app.route("/static/<path:fname>")
    def static_files(fname: str):
        return send_from_directory(_STATIC, fname)

    # ── Positions tab ───────────────────────────────────────────────────
    @app.route("/api/me")
    @_api
    def api_me():
        addr = settings.my_address()
        pv = polymarket_data.portfolio_value(addr)
        cash = wallet.cash()
        total = pv + (cash or 0.0)
        return jsonify({
            "address": addr,
            "username": _short(addr) if addr else "—",
            "portfolio_value": round(pv, 2),
            "cash": cash,
            "total": round(total, 2),
        })

    @app.route("/api/positions")
    @_api
    def api_positions():
        return jsonify({"positions": polymarket_data.positions(settings.my_address())})

    @app.route("/api/balance-history")
    @_api
    def api_balance_history():
        # P&L curve for my wallet via Polymarket user-pnl (same source/granularity
        # as the Pros tab), rather than locally-recorded snapshots.
        days = int(request.args.get("days", "30"))
        return jsonify({"history": polymarket_data.value_history(settings.my_address(), days)})

    # ── Pros tab ────────────────────────────────────────────────────────
    def _all_pros() -> list[dict]:
        """Config pros (fixed) + user-added pros (removable), deduped by address."""
        rows, seen = [], set()
        for p in settings.pros():
            rows.append({"label": p["label"], "address": p["address"], "custom": False})
            seen.add(p["address"].lower())
        for cp in db.list_pros():
            if cp["address"].lower() in seen:
                continue
            rows.append({"label": cp["label"], "address": cp["address"], "custom": True})
            seen.add(cp["address"].lower())
        return rows

    @app.route("/api/pros")
    @_api
    def api_pros():
        # Instant: just labels/addresses, no network. Portfolio values load
        # separately via /api/pros/values so the list paints first.
        return jsonify({"pros": _all_pros()})

    @app.route("/api/pros", methods=["POST"])
    @_api
    def api_pros_add():
        body = request.get_json(silent=True) or {}
        added = db.add_pro(body.get("label", ""), body.get("address", ""))
        return jsonify({"ok": True, **added})

    @app.route("/api/pros/<addr>", methods=["DELETE"])
    @_api
    def api_pros_remove(addr: str):
        db.remove_pro(addr)
        return jsonify({"ok": True})

    @app.route("/api/pros/values")
    @_api
    def api_pro_values():
        pros = _all_pros()
        # Independent network calls — fetch concurrently (cached for ~60s).
        with ThreadPoolExecutor(max_workers=min(16, len(pros) or 1)) as ex:
            values = list(ex.map(lambda p: polymarket_data.portfolio_value(p["address"]), pros))
        return jsonify({"values": {p["address"]: v for p, v in zip(pros, values)}})

    @app.route("/api/pros/<addr>/positions")
    @_api
    def api_pro_positions(addr: str):
        return jsonify({"positions": polymarket_data.positions(addr)})

    @app.route("/api/pros/<addr>/activity")
    @_api
    def api_pro_activity(addr: str):
        limit = int(request.args.get("limit", "50"))
        return jsonify({"activity": polymarket_data.activity(addr, limit)})

    @app.route("/api/pros/<addr>/value-history")
    @_api
    def api_pro_value_history(addr: str):
        days = int(request.args.get("days", "30"))
        return jsonify({"history": polymarket_data.value_history(addr, days)})

    # ── Markets tab ─────────────────────────────────────────────────────
    def _watchlist() -> dict:
        """Config defaults merged with user-added symbols (dedup, order-stable)."""
        custom = db.get_watchlist()
        stocks_l = list(settings.stock_watchlist())
        crypto_l = list(settings.crypto_watchlist())
        for c in custom:
            bucket = crypto_l if c["kind"] == "crypto" else stocks_l
            if c["symbol"] not in bucket:
                bucket.append(c["symbol"])
        return {"stocks": stocks_l, "crypto": crypto_l, "custom": [c["symbol"] for c in custom]}

    @app.route("/api/stocks/watchlist")
    @_api
    def api_watchlist():
        return jsonify(_watchlist())

    @app.route("/api/stocks/watchlist", methods=["POST"])
    @_api
    def api_watchlist_add():
        symbol = (request.get_json(silent=True) or {}).get("symbol", "")
        added = db.add_watchlist(symbol)
        return jsonify({"ok": True, **added})

    @app.route("/api/stocks/watchlist/<symbol>", methods=["DELETE"])
    @_api
    def api_watchlist_remove(symbol: str):
        db.remove_watchlist(symbol)
        return jsonify({"ok": True})

    @app.route("/api/stocks/quotes")
    @_api
    def api_quotes():
        syms = [s for s in request.args.get("symbols", "").split(",") if s.strip()]
        if not syms:
            wl = _watchlist()
            syms = wl["stocks"] + wl["crypto"]
        return jsonify({"quotes": stocks.quotes(syms)})

    @app.route("/api/stocks/history")
    @_api
    def api_history():
        symbol = request.args.get("symbol", "SPY")
        rng = request.args.get("range", "1y")
        return jsonify(stocks.history(symbol, rng))

    # ── Backtester tab ──────────────────────────────────────────────────
    @app.route("/api/backtest")
    @_api
    def api_backtest():
        symbol = request.args.get("symbol", "SPY")
        strategy = request.args.get("strategy", "compare")  # dca|lumpsum|compare
        amount = float(request.args.get("amount", "100"))
        initial = float(request.args.get("initial", "0"))
        freq = request.args.get("freq", "monthly")
        start = request.args.get("start", "2020-01-01")
        end = request.args.get("end", "2025-01-01")
        return jsonify(backtest.run(symbol, strategy, amount, freq, start, end, initial))

    @app.route("/api/backtest/runs")
    @_api
    def api_backtest_runs():
        return jsonify({"runs": db.list_backtest_runs()})

    @app.route("/api/backtest/runs", methods=["POST"])
    @_api
    def api_backtest_run_add():
        body = request.get_json(silent=True) or {}
        return jsonify({"ok": True, "id": db.add_backtest_run(body)})

    @app.route("/api/backtest/runs/<int:run_id>", methods=["DELETE"])
    @_api
    def api_backtest_run_remove(run_id: int):
        db.remove_backtest_run(run_id)
        return jsonify({"ok": True})

    @app.route("/api/health")
    def api_health():
        return jsonify({"ok": True})

    return app
