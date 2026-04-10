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
  --redeem/--no-redeem
                 Enable/disable independent redeem settlement loop (live mode).
  --redeem-interval
                 Seconds between redeem settlement scans.
  --polygon-rpc  Polygon RPC URL for onchain redeem mode.
  --redeem-mode  Redeem mode: relayer (default) or onchain.
  --redeem-cmd   Optional external redeem command when outside entry window.
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
    bet_size_shares: float = 20.0,
    show_stats: bool = True,
    redeem: bool = True,
    redeem_interval_seconds: int = 20,
    polygon_rpc: str | None = None,
    redeem_mode: str = "relayer",
    redeem_cmd: str | None = None,
) -> None:
    load_dotenv()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log = logging.getLogger("automata.eth")

    if bet:
        _derive_clob_credentials()

    from automata.eth_1h import (
        MAX_MINUTES,
        MIN_MINUTES,
        _settle_resolved_trades,
        analyze,
        build_slug,
        current_et,
        fetch_and_parse,
        run_eth_1h,
    )
    from automata.eth_15m import _maybe_run_replayer_redeem

    last_redeem_cmd_at = 0.0
    next_redeem_due = 0.0

    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    iteration = 0
    while True:
        iteration += 1
        try:
            log.info("[eth] Iteration %d", iteration)
            if show_stats:
                analyze(host=host)

            # Match eth_15m redeem flow: independent redeem settlement cadence.
            now_mono = time.monotonic()
            if bet and redeem and now_mono >= next_redeem_due:
                log.info("[eth] Redeem scan (mode=%s)", redeem_mode)
                _settle_resolved_trades(
                    private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                    rpc_url=polygon_rpc or os.getenv("POLYGON_RPC_URL"),
                    redeem_mode=redeem_mode,
                )
                next_redeem_due = now_mono + max(1, redeem_interval_seconds)

            # Outside entry window: optional external redeem hook.
            now_et = current_et()
            slug = build_slug(now_et.replace(minute=0, second=0, microsecond=0))
            mkt = fetch_and_parse(slug)
            mins = int(mkt["minutes_remaining"]) if mkt and mkt.get("minutes_remaining") is not None else None
            in_window = mins is not None and MIN_MINUTES <= mins <= MAX_MINUTES
            if not in_window and redeem_cmd:
                if now_mono - last_redeem_cmd_at >= 60:
                    last_redeem_cmd_at = now_mono
                    _maybe_run_replayer_redeem(redeem_cmd, min_interval_seconds=60)

            run_eth_1h(
                dry_run=not bet,
                host=host,
                max_spend_usdc=max_balance_usdc if bet else None,
                bet_shares=bet_size_shares,
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
    parser.add_argument("--size", type=float, default=20.0, help="Shares per bet for ETH 1H (default: 20)")
    parser.add_argument("--stats", dest="show_stats", action="store_true", default=True, help="Print ETH 1H market/fair/volume stats each cycle (default: enabled)")
    parser.add_argument("--no-stats", dest="show_stats", action="store_false", help="Disable ETH 1H stats printout")
    parser.add_argument("--max-balance", type=float, default=None, help="Max USDC this process is allowed to spend")
    parser.add_argument("--redeem", dest="redeem", action="store_true", default=True, help="Run redeem/settle loop (default: enabled)")
    parser.add_argument("--no-redeem", dest="redeem", action="store_false", help="Disable redeem/settle loop")
    parser.add_argument("--redeem-interval", type=int, default=20, help="Seconds between redeem loop runs")
    parser.add_argument("--polygon-rpc", type=str, default=None, help="Polygon RPC URL for onchain redeem")
    parser.add_argument("--redeem-mode", choices=["relayer", "onchain"], default="relayer", help="Redeem flow to use when redeem is enabled")
    parser.add_argument("--redeem-cmd", type=str, default=None, help="External redeem command to run outside entry window")
    args = parser.parse_args()

    run_eth_daemon(
        bet=args.bet,
        interval_seconds=max(1, args.interval),
        once=args.once,
        max_balance_usdc=args.max_balance,
        bet_size_shares=max(0.01, float(args.size)),
        show_stats=args.show_stats,
        redeem=args.redeem,
        redeem_interval_seconds=max(1, args.redeem_interval),
        polygon_rpc=args.polygon_rpc,
        redeem_mode=args.redeem_mode,
        redeem_cmd=args.redeem_cmd,
    )
