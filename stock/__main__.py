"""
Stock Decision Dashboard — local web UI with yfinance-backed collector.

Usage:
  python -m stock                        # poll 60s, UI on :5060
  python -m stock --poll 30 --port 5060
  python -m stock --no-collect           # UI only, read existing DB
"""
from __future__ import annotations

import argparse
import logging
import socket
import sys
import threading
from pathlib import Path

# Windows console defaults to cp1252, which can't encode '→' / '↗' / '°' that
# appear in log messages (e.g. stock.pro_collector "activity upsert: %s → %d").
# Force UTF-8 on stdout/stderr so log handlers don't blow up on these chars.
for _stream in (sys.stdout, sys.stderr):
    if _stream is not None and hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_DEFAULT = str(_REPO_ROOT / "db" / "stock.db")
LOG_DEFAULT = str(_REPO_ROOT / "logs" / "stock.log")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock decision dashboard")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--poll", type=int, default=60, metavar="SEC",
                        help="Collector poll interval in seconds (default: 60)")
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to DuckDB data file")
    parser.add_argument("--log", default=LOG_DEFAULT, help="Path to log file")
    parser.add_argument("--no-collect", action="store_true",
                        help="Skip collector — serve UI against existing DB only")
    args = parser.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    Path(args.log).parent.mkdir(parents=True, exist_ok=True)

    handlers = [logging.StreamHandler(), logging.FileHandler(args.log, encoding="utf-8")]
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
                        handlers=handlers)
    log = logging.getLogger("stock")

    if not args.no_collect:
        from .collector import run_loop
        t = threading.Thread(target=run_loop, args=(args.db, args.poll),
                             daemon=True, name="stock-collector")
        t.start()
        log.info("Collector thread started (poll every %d s)", args.poll)

    from .app import create_app
    app = create_app(db_path=args.db)

    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  Stock Dashboard UI:  http://localhost:{args.port}")
    print(f"  On LAN:              http://{local_ip}:{args.port}")
    print(f"  DB:                  {args.db}")
    if not args.no_collect:
        print(f"  Collector:           every {args.poll} s")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
