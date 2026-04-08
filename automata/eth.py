"""
ETH 1H betting daemon.

Usage:
  python -m automata.eth
  python -m automata.eth --bet
  python -m automata.eth --bet --once --max-balance 30

Flags:
  --bet          Place real orders (default is dry-run).
  --interval     Loop interval in seconds (default 10).
  --once         Run one cycle, then exit.
  --max-balance  Max USDC this process can spend.
"""

from __future__ import annotations

import logging
import os
import time

from dotenv import load_dotenv


def _derive_clob_credentials() -> None:
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    from automata.client import derive_api_credentials

    creds = derive_api_credentials(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def run_eth_daemon(
    bet: bool = False,
    interval_seconds: int = 10,
    once: bool = False,
    max_balance_usdc: float | None = None,
) -> None:
    load_dotenv()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log = logging.getLogger("automata.eth")

    if bet:
        _derive_clob_credentials()

    from automata.eth_1h import run_eth_1h

    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    iteration = 0
    while True:
        iteration += 1
        try:
            log.info("[eth] Iteration %d", iteration)
            run_eth_1h(
                dry_run=not bet,
                host=host,
                max_spend_usdc=max_balance_usdc if bet else None,
            )
        except Exception as exc:
            log.error("[eth] Unhandled error: %s", exc)
        if once:
            log.info("[eth] --once set, exiting after single cycle")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETH 1H betting daemon")
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    parser.add_argument("--interval", type=int, default=10, help="Loop interval in seconds (default: 10)")
    parser.add_argument("--once", action="store_true", help="Run one cycle, then exit")
    parser.add_argument("--max-balance", type=float, default=None, help="Max USDC this process is allowed to spend")
    args = parser.parse_args()

    run_eth_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.interval),
        once=args.once,
        max_balance_usdc=args.max_balance,
    )
