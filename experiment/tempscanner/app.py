"""Flask app for the temp-market scanner UI."""
from __future__ import annotations

from flask import Flask, jsonify, render_template, request

from .database import query_cities, query_session


def create_app(db_path: str) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["DB_PATH"] = db_path

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/api/cities")
    def api_cities():
        return jsonify(query_cities(app.config["DB_PATH"]))

    @app.route("/api/session")
    def api_session():
        city = request.args.get("city", "")
        date = request.args.get("date", "")
        if not city or not date:
            return jsonify({"error": "city and date params required"}), 400
        return jsonify(query_session(app.config["DB_PATH"], city, date))

    return app
