"""Flask app + JSON API for the stock decision dashboard."""
from __future__ import annotations

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
        try:
            leverage = float(request.args.get("leverage", "2"))
        except ValueError:
            leverage = 2.0
        try:
            days = int(request.args.get("days", "10"))
        except ValueError:
            days = 10
        return jsonify(db.query_tracking_error(_db(), pair, base, leverage, days))

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

    @app.route("/api/econ-events")
    def api_econ_events():
        try:
            days = int(request.args.get("days", "1"))
        except ValueError:
            days = 1
        return jsonify(db.query_econ_events(_db(), days))

    return app
