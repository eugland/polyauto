"""Flask app and routes for the trading dashboard."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .database import (
    query_bs_forward,
    query_crypto_stats,
    query_eth_stats,
    query_eth_live_positions,
    query_weather_stats,
    read_bs_log_tail,
    read_eth_log_tail,
    update_weather_bet,
)


def create_app(crypto_db_path: str, bets_db_path: str, eth_log_path: str, bs_log_path: str) -> Flask:
    """Create and configure the Flask app."""
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # Store config
    app.config["CRYPTO_DB_PATH"] = crypto_db_path
    app.config["BETS_DB_PATH"] = bets_db_path
    app.config["ETH_LOG_PATH"] = eth_log_path
    app.config["BS_LOG_PATH"] = bs_log_path

    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/api/data")
    def api_data():
        return jsonify(query_crypto_stats(app.config["CRYPTO_DB_PATH"]))

    @app.route("/api/eth")
    def api_eth():
        # Try live positions first, fall back to bets.db
        live = query_eth_live_positions()
        if live.get("error"):
            return jsonify(query_eth_stats(app.config["BETS_DB_PATH"]))
        return jsonify(live)

    @app.route("/api/log")
    def api_log():
        return jsonify({"lines": read_eth_log_tail(app.config["ETH_LOG_PATH"])})

    @app.route("/api/bs-log")
    def api_bs_log():
        return jsonify({"lines": read_bs_log_tail(app.config["BS_LOG_PATH"])})

    @app.route("/api/weather")
    def api_weather():
        return jsonify(query_weather_stats(app.config["BETS_DB_PATH"]))

    @app.route("/api/bs-forward")
    def api_bs_forward():
        try:
            min_edge = float(request.args.get("min_edge", 0))
        except (TypeError, ValueError):
            min_edge = 0.0
        return jsonify(query_bs_forward(app.config["CRYPTO_DB_PATH"], min_edge))

    @app.route("/api/weather/update/<int:bet_id>", methods=["POST"])
    def api_weather_update(bet_id: int):
        data = request.get_json(force=True) or {}
        resolved_temp = data.get("resolved_temp")
        outcome = data.get("outcome") or None
        update_weather_bet(app.config["BETS_DB_PATH"], bet_id, resolved_temp, outcome)
        return jsonify({"ok": True})

    return app
