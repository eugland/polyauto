"""
Combined automata runner.

Usage:
  python -m automata
  python -m automata --bet
  python -m automata --bet --weather-max-balance 40 --eth-max-balance 20

Runs both daemons together:
  - weather daemon in the main thread
  - ETH daemon in a background thread
"""

from __future__ import annotations

import argparse
import logging
import threading


def main() -> None:
    parser = argparse.ArgumentParser(description="Run weather and ETH daemons together")
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    parser.add_argument("--weather-interval", type=int, default=60, help="Weather loop interval in seconds")
    parser.add_argument("--eth-interval", type=int, default=10, help="ETH loop interval in seconds")
    parser.add_argument("--weather-max-balance", type=float, default=None, help="Max USDC weather daemon can spend")
    parser.add_argument("--eth-max-balance", type=float, default=None, help="Max USDC ETH daemon can spend")
    args = parser.parse_args()

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log = logging.getLogger("automata")

    from automata.eth import run_eth_daemon
    from automata.weather import run_weather_daemon

    eth_thread = threading.Thread(
        target=run_eth_daemon,
        kwargs={
            "bet": args.bet,
            "interval_seconds": max(1, args.eth_interval),
            "once": False,
            "max_balance_usdc": args.eth_max_balance,
        },
        daemon=True,
        name="eth-daemon",
    )
    eth_thread.start()
    log.info("[eth] Background daemon started (%ss interval)", max(1, args.eth_interval))

    run_weather_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.weather_interval),
        once=False,
        max_balance_usdc=args.weather_max_balance,
    )


if __name__ == "__main__":
    main()
