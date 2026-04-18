"""
Overnight highest-temperature market scanner + web UI.

Starts the collector in a background thread, then serves the Flask UI.

Usage:
  python -m experiment.tempscanner                         # poll every 2 min, UI on :5055
  python -m experiment.tempscanner --poll 5 --port 5055
  python -m experiment.tempscanner --no-collect            # UI only (read existing DB)
"""
from __future__ import annotations

import argparse
import logging
import socket
import threading
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DB_DEFAULT = str(_REPO_ROOT / "db" / "tempscanner.db")


def main() -> None:
    parser = argparse.ArgumentParser(description="Temp-market scanner + web UI")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--poll", type=int, default=2, metavar="MIN",
                        help="Collector poll interval in minutes (default: 2)")
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to DuckDB data file")
    parser.add_argument("--no-collect", action="store_true",
                        help="Skip collector — serve UI against existing DB only")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log = logging.getLogger("tempscanner")

    if not args.no_collect:
        from .collector import run_loop
        t = threading.Thread(target=run_loop, args=(args.db, args.poll), daemon=True, name="collector")
        t.start()
        log.info("Collector thread started (poll every %d min)", args.poll)

    from .app import create_app
    app = create_app(db_path=args.db)

    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  Temp Scanner UI:  http://localhost:{args.port}")
    print(f"  On LAN:           http://{local_ip}:{args.port}")
    print(f"  DB:               {args.db}")
    if not args.no_collect:
        print(f"  Collector:        every {args.poll} min")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
