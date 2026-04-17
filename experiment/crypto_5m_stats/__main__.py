"""Entry point for the crypto 5m stats web app."""
import argparse
import os
import socket
from pathlib import Path

from .app import create_app

CRYPTO_DB_PATH    = os.path.join("experiment", "crypto_5m.db")
BETS_DB_PATH      = str(Path(__file__).resolve().parent.parent.parent / "bets.db")
ETH_LOG_PATH      = os.path.join("experiment", "logs", "eth_1h.log")
BS_LOG_PATH       = os.path.join("experiment", "logs", "crypto_5m_scanner.log")
ETH5MIN_DB_PATH   = os.path.join("experiment", "eth_5min.db")
ETH5MIN_LOG_PATH  = os.path.join("experiment", "logs", "eth_5min.log")
TEMP_MARKET_DB_PATH = os.path.join("experiment", "temp_market.db")


def main() -> None:
    """Run the trading dashboard web app."""
    parser = argparse.ArgumentParser(description="Unified trading stats web UI")
    parser.add_argument("--port", type=int, default=5051)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument(
        "--db",
        default=CRYPTO_DB_PATH,
        dest="crypto_db",
        help="Path to crypto_5m.db",
    )
    parser.add_argument(
        "--bets-db",
        default=BETS_DB_PATH,
        help="Path to bets.db (ETH 1H + Weather)",
    )
    parser.add_argument(
        "--eth5min-db",
        default=ETH5MIN_DB_PATH,
        help="Path to eth_5min.db",
    )
    parser.add_argument(
        "--temp-market-db",
        default=TEMP_MARKET_DB_PATH,
        help="Path to temp_market.db",
    )
    args = parser.parse_args()

    # Get local IP for LAN access
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "127.0.0.1"

    print(f"  Stats UI:  http://localhost:{args.port}")
    print(f"  On LAN:    http://{local_ip}:{args.port}")
    print(f"  Tabs:      ETH 1H Bot | Weather Bets | BTC 5m BS Test | ETH 5m Bot | Highest Temp")

    app = create_app(
        crypto_db_path=args.crypto_db,
        bets_db_path=args.bets_db,
        eth_log_path=ETH_LOG_PATH,
        bs_log_path=BS_LOG_PATH,
        eth5min_db_path=args.eth5min_db,
        eth5min_log_path=ETH5MIN_LOG_PATH,
        temp_market_db_path=args.temp_market_db,
    )
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
