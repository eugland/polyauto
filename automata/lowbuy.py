"""
Low-bracket safe-NO buyer daemon.

Mirrors `automata.weather` but bets the OPPOSITE side: NO on the LOWEST
brackets of the closest-resolving "Highest Temperature in <city>" markets,
gated on today's METAR max-so-far having already climbed past the bracket
upper bound by a configurable safety margin.

Usage:
  python -m automata.lowbuy
  python -m automata.lowbuy --bet
  python -m automata.lowbuy --bet --once --max-balance 30

Flags:
  --bet          Place real orders (default is dry-run).
  --interval     Loop interval in seconds (default 60).
  --once         Run one cycle, place at most one order, then exit.
  --max-balance  Max USDC this process can spend.
"""
from __future__ import annotations

import io
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from automata import config

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv()

_LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "lowbuy.log"
_file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
_file_handler.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), _file_handler],
)
_root = logging.getLogger()
if _file_handler not in _root.handlers:
    _root.addHandler(_file_handler)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("automata.lowbuy")


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
        signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def run_lowbuy_daemon(
    bet: bool = False,
    interval_seconds: int = 60,
    once: bool = False,
    max_balance_usdc: float | None = None,
) -> None:
    from automata.db import init_db
    from automata.lowbuy_bot import run
    from automata.weather_bot import _scan_positions

    if bet:
        _derive_clob_credentials()
    init_db()

    iteration = 0
    while True:
        iteration += 1
        log.info("[lowbuy] Iteration %d", iteration)

        if bet:
            # Reuse weather_bot's position scanner — it places TP sells on
            # any held NO position, regardless of which bot opened it.
            _scan_positions(dry_run=False)
            try:
                from automata.client import build_client, get_usdc_balance
                client = build_client(
                    host=os.environ["POLYMARKET_HOST"],
                    private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                    api_key=os.environ["CLOB_API_KEY"],
                    api_secret=os.environ["CLOB_SECRET"],
                    api_passphrase=os.environ["CLOB_PASS"],
                    funder=os.getenv("POLYMARKET_FUNDER") or None,
                    signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
                )
                balance = get_usdc_balance(client)
                effective = min(balance, max_balance_usdc) if max_balance_usdc is not None else balance
                log.info("[lowbuy] USDC balance: $%.2f  effective: $%.2f", balance, effective)
            except Exception as exc:
                log.warning("[lowbuy] Balance check failed: %s — skipping cycle", exc)
                if once:
                    break
                time.sleep(interval_seconds)
                continue
            capped = min(balance, max_balance_usdc) if max_balance_usdc is not None else balance
            bet_this_cycle = capped >= 10.0
            if not bet_this_cycle:
                log.warning("[lowbuy] Balance $%.2f < $10.00 — running shadow-mode scan", capped)
                if once:
                    break
        else:
            bet_this_cycle = False

        try:
            run(
                dry_run=not bet_this_cycle,
                max_spend_usdc=max_balance_usdc if bet_this_cycle else None,
                max_orders=1 if (bet_this_cycle and once) else None,
            )
        except Exception as exc:
            log.warning("[lowbuy] Cycle failed: %s: %s — continuing", type(exc).__name__, exc)
            if once:
                break

        if once:
            log.info("[lowbuy] --once set, exiting after single cycle")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Lowest-bracket safe-NO buyer daemon")
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds (default: 60)")
    parser.add_argument("--once", action="store_true", help="Run one cycle, place at most one order, then exit")
    parser.add_argument("--max-balance", type=float, default=None, help="Max USDC this process is allowed to spend")
    args = parser.parse_args()

    run_lowbuy_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.interval),
        once=args.once,
        max_balance_usdc=args.max_balance,
    )
