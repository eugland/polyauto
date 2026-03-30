#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from flask import Flask

from backendapp.controllers.home_controller import load_runtime_from_config, register_home_routes
from backendapp.services.timezone_service import configure_zoneinfo_tzpath

CONFIG_PATH = Path(__file__).resolve().parent.parent / "webapp.json"


def create_app() -> Flask:
    configure_zoneinfo_tzpath()
    app = Flask(__name__)
    register_home_routes(app, CONFIG_PATH)
    return app


app = create_app()


if __name__ == "__main__":
    runtime = load_runtime_from_config(CONFIG_PATH)
    app.run(
        host=runtime.run_host,
        port=runtime.run_port,
        debug=runtime.run_debug,
    )
  