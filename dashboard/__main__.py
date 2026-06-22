"""Entry point: ``python -m dashboard [--port 8000] [--no-poller]``."""
from __future__ import annotations

import argparse
import logging

from dashboard import settings
from dashboard.app import create_app


def main() -> int:
    p = argparse.ArgumentParser(description="Polymarket-style dashboard web app.")
    p.add_argument("--port", type=int, default=None, help="HTTP port (default: [dashboard].port = 8000)")
    p.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p.add_argument("--no-poller", action="store_true", help="Don't start the balance-history poller")
    p.add_argument("--debug", action="store_true", help="Flask debug/reloader")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(name)s  %(message)s")
    port = args.port or settings.port()
    # With the reloader the module is imported twice; only poll in the main run.
    app = create_app(start_poller=not args.no_poller and not args.debug)
    logging.getLogger("dashboard").info("dashboard on http://%s:%d", args.host, port)
    app.run(host=args.host, port=port, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
