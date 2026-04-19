"""Flask app + JSON API for the stock decision dashboard."""
from __future__ import annotations

import os
from pathlib import Path

import requests as _requests
from flask import Flask, jsonify, render_template, request

from . import database as db


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
                "lines": [l.rstrip() for l in all_lines[-lines:]],
                "exists": True,
                "path": path_str,
                "total_lines": len(all_lines),
            })
        except Exception as exc:
            return jsonify({"error": str(exc), "path": path_str}), 500

    _PRO_WALLETS = {
        "haerder": "0x8dec027d883949a6bfe79842d0ae6b80347e46e0",
        "sin3000": "0x8d71ff86701227bb479b2039edd92b08f73115d8",
        "aapang":  "0x104171232971a6db8cf938f76fdbebbb81c5f452",
    }
    _POLYMARKET_DATA_API = "https://data-api.polymarket.com"

    @app.route("/api/weather-pro/positions")
    def api_weather_pro_positions():
        results = {}
        for name, wallet in _PRO_WALLETS.items():
            try:
                resp = _requests.get(
                    f"{_POLYMARKET_DATA_API}/positions",
                    params={"user": wallet, "limit": 50},
                    timeout=10,
                )
                results[name] = resp.json() if resp.ok else []
            except Exception as exc:
                results[name] = {"error": str(exc)}
        return jsonify(results)

    def _fetch_activity(wallet: str, limit: int = 100) -> list:
        """Fetch activity, paginating until limit reached or no more data."""
        all_items: list = []
        offset = 0
        page = 500
        while len(all_items) < limit:
            resp = _requests.get(
                f"{_POLYMARKET_DATA_API}/activity",
                params={"user": wallet, "limit": page, "offset": offset},
                timeout=15,
            )
            if not resp.ok:
                break
            batch = resp.json()
            if not isinstance(batch, list) or not batch:
                break
            all_items.extend(batch)
            if len(batch) < page:
                break
            offset += page
        return all_items[:limit]

    @app.route("/api/weather-pro/activity")
    def api_weather_pro_activity():
        from .temp_lookup import annotate_activity
        results = {}
        limits = {"haerder": 100, "sin3000": 100, "aapang": 500}
        for name, wallet in _PRO_WALLETS.items():
            try:
                items = _fetch_activity(wallet, limit=limits.get(name, 100))
                try:
                    annotate_activity(items)
                except Exception:
                    pass  # never fail the response on a temp-cache hiccup
                results[name] = items
            except Exception as exc:
                results[name] = {"error": str(exc)}
        return jsonify(results)

    return app
