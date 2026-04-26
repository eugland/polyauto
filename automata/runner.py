"""
Autonomous aapang-style runner — single-process orchestrator.

Wraps the three subsystems built in:
  * automata.picker          — score events, find next un-held pick
  * automata.splitter        — SPLIT + CONVERT + winner-sell + redeem
  * automata.seller_bot      — three-tier sell maintenance (winner / dust / placeholder)

…and adds budget gating + resolution-based auto-redeem so it can run unattended.

Three concurrent loops in one process (single thread, time-multiplexed):

  1. PICK loop          — every --pick-interval (default 30min):
                          if (held_events < max_events) AND
                             (committed < budget)       AND
                             (free_usdc >= per_trade + buffer)
                          then run splitter.action_auto(usdc=per_trade)

  2. MAINTAIN loop      — every --maintenance-interval (default 60s):
                          run seller_bot.scan() — refreshes winner-sells,
                          dust-fades, placeholders across every held event.

  3. REDEEM loop        — every --redeem-interval (default 300s = 5min):
                          for each held event, query CTF.payoutDenominator;
                          if resolved, call splitter.action_redeem on the
                          winning conditionId (recovers $1 if winner-sell
                          didn't fill).

Budget model
============
* `--usdc 5`        — per-event SPLIT amount (= shares per bucket = our "lot size")
* `--budget 50`     — max $ committed in held YES positions at any time
* `--max-events 10` — max concurrent distinct events in play

Committed value is approximated as `held_event_count × per_trade_usdc`.
After resolution + redemption, $ comes back to the wallet and is available
for the next pick.

SAFETY
======
* Defaults to --dry-run. The --live flag is required for any broadcast.
* All sub-actions inherit the dry_run flag.
* Per-call USDC capped at MAX_USDC_HARD ($5) by splitter.py.
* Each cycle catches and logs exceptions — never crashes the loop.

Usage:
  python -m automata.runner                                       # dry-run
  python -m automata.runner --live                                # broadcast
  python -m automata.runner --live --usdc 5 --budget 50 --max-events 10
  python -m automata.runner --live --pick-interval 1800 --maintenance-interval 60
  python -m automata.runner --once                                # one cycle, then exit
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from automata import config

load_dotenv()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = _REPO_ROOT / "logs"
_LOGS_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOGS_DIR / "runner.log"
# Shared log file the stock UI reads (stock/app.py /api/weather-log).
_SHARED_LOG_FILE = _LOGS_DIR / "automata.log"


def _attach_file_handler(root: logging.Logger, path: Path) -> None:
    """Add a FileHandler for `path` to root if one isn't already attached."""
    if any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(path)
        for h in root.handlers
    ):
        return
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    fh.setLevel(logging.INFO)
    root.addHandler(fh)


def _setup_logging() -> logging.Logger:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler()],
        )
    _attach_file_handler(root, _LOG_FILE)
    _attach_file_handler(root, _SHARED_LOG_FILE)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.runner")


# ───────────── Budget helpers ─────────────────────────────────────────────────
def _free_usdc(funder: str) -> float:
    """Cash USDC.e in the proxy wallet."""
    from automata.splitter import preflight
    pre = preflight(funder)
    return pre["usdc_balance"]


def _held_event_slugs() -> set[str]:
    """Set of distinct weather event slugs where we hold any YES/NO position."""
    from automata.picker import get_held_event_slugs
    return get_held_event_slugs()


# ───────────── Resolution + auto-redeem ───────────────────────────────────────
_PAYOUT_ABI = [
    {"inputs": [{"name": "", "type": "bytes32"}],
     "name": "payoutDenominator",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "", "type": "bytes32"}, {"name": "", "type": "uint256"}],
     "name": "payoutNumerators",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]


def _bucket_resolution(condition_id: str) -> tuple[bool, bool] | None:
    """
    Return (is_resolved, yes_won) for a single neg-risk bucket conditionId.
    None if RPC fails. yes_won=True means YES of this bucket pays $1 (this is
    the winning bucket of its event); yes_won=False means NO pays $1.
    """
    try:
        from web3 import Web3
        from automata.splitter import _w3, CTF_ADDRESS
        w3 = _w3()
        ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=_PAYOUT_ABI)
        cb = bytes.fromhex(condition_id.lower().replace("0x", ""))
        denom = int(ctf.functions.payoutDenominator(cb).call())
        if denom <= 0:
            return (False, False)
        n_yes = int(ctf.functions.payoutNumerators(cb, 0).call())
        n_no = int(ctf.functions.payoutNumerators(cb, 1).call())
        return (True, n_yes > n_no)
    except Exception:
        return None


def _redeem_resolved_held_events(dry_run: bool, log: logging.Logger) -> dict:
    """
    For each held event, query each bucket's on-chain payout. Redeem the YES of
    any bucket whose YES is the winning side (we never hold NO post-CONVERT).
    Idempotent at the action level — re-redeeming a zero balance is a no-op.
    """
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.splitter import action_redeem_neg_risk
    from automata.parser import _extract_yes_token_id, _extract_no_token_id
    from automata.client import get_positions

    funder = os.getenv("POLYMARKET_FUNDER") or ""
    if not funder:
        return {"redeemed": 0, "error": "no funder"}

    held = get_positions(funder)
    held_tokens = {p["token_id"] for p in held if p.get("size", 0) > 0.01}
    if not held_tokens:
        return {"redeemed": 0, "skipped": "no held tokens"}

    payload = fetch_temperature_markets_payload()
    # Build (conditionId → metadata) for every weather market we hold YES of
    conds_to_check: list[dict] = []
    for raw in payload["markets"]:
        yes_tok = _extract_yes_token_id(raw)
        no_tok = _extract_no_token_id(raw)
        if not (yes_tok in held_tokens or no_tok in held_tokens):
            continue
        cid = raw.get("conditionId")
        if not cid:
            continue
        conds_to_check.append({
            "conditionId": cid,
            "event_slug": raw.get("event_slug"),
            "question": raw.get("groupItemTitle") or raw.get("question"),
        })

    redeemed = 0
    for c in conds_to_check:
        status = _bucket_resolution(c["conditionId"])
        if status is None or not status[0]:
            continue
        is_resolved, yes_won = status
        if not yes_won:
            continue  # YES of this bucket is the loser side — redemption gives $0, skip
        log.info("[REDEEM] %s | %s | conditionId=%s",
                 c.get("event_slug", ""), c.get("question", ""), c["conditionId"][:18])
        try:
            res = action_redeem_neg_risk(c["conditionId"], dry_run, log)
            if res.get("ok") and not res.get("dry_run"):
                redeemed += 1
        except Exception as exc:
            log.warning("redeem failed on %s: %s", c["conditionId"][:14], exc)
    return {"redeemed": redeemed, "checked": len(conds_to_check)}


# ───────────── Cancel-on-exit: cancel all open CLOB sells before stopping ────
def _cancel_all_open_sells(log: logging.Logger) -> int:
    """Cancel every open SELL order on the CLOB. Returns count cancelled.
    Used by --cancel-on-exit at shutdown so resting fades don't outlive the bot."""
    try:
        from automata.client import (
            build_client, derive_api_credentials,
            get_all_open_orders, cancel_order,
        )
    except Exception as exc:
        log.warning("[cancel-on-exit] client import failed: %s", exc)
        return 0
    if not (os.getenv("POLYMARKET_PRIVATE_KEY") and os.getenv("POLYMARKET_HOST")):
        log.warning("[cancel-on-exit] missing private_key/host env — skipping")
        return 0
    if not (os.getenv("CLOB_API_KEY") and os.getenv("CLOB_SECRET") and os.getenv("CLOB_PASS")):
        try:
            creds = derive_api_credentials(
                host=os.environ["POLYMARKET_HOST"],
                private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                funder=os.getenv("POLYMARKET_FUNDER"),
                signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
            )
            os.environ["CLOB_API_KEY"] = creds.api_key
            os.environ["CLOB_SECRET"] = creds.api_secret
            os.environ["CLOB_PASS"] = creds.api_passphrase
        except Exception as exc:
            log.warning("[cancel-on-exit] derive_api_credentials failed: %s", exc)
            return 0
    try:
        client = build_client(
            host=os.environ["POLYMARKET_HOST"],
            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
            api_key=os.environ["CLOB_API_KEY"],
            api_secret=os.environ["CLOB_SECRET"],
            api_passphrase=os.environ["CLOB_PASS"],
            funder=os.getenv("POLYMARKET_FUNDER"),
            signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
        )
        orders = get_all_open_orders(client)
    except Exception as exc:
        log.warning("[cancel-on-exit] get_all_open_orders failed: %s", exc)
        return 0
    cancelled = 0
    for o in orders:
        if str(o.get("side", "")).upper() != "SELL":
            continue
        oid = str(o.get("id") or o.get("orderID") or "")
        if not oid:
            continue
        try:
            cancel_order(client, oid)
            cancelled += 1
            log.info("[cancel-on-exit] cancelled %s  px=%s  asset=%s",
                     oid[:14], o.get("price"), str(o.get("asset_id", ""))[:14])
        except Exception as exc:
            log.warning("[cancel-on-exit] cancel %s failed: %s", oid[:14], exc)
    return cancelled


# ───────────── Daily summary — periodic state snapshot for the UI log ────────
def _daily_summary(funder: str, log: logging.Logger,
                   cycles: int, picks_done: int, redeems_done: int,
                   start_usdc: float | None) -> None:
    """One log line summarizing current bot state. Visible in the stock UI's
    weather-log viewer for at-a-glance health."""
    try:
        free = _free_usdc(funder)
    except Exception:
        free = float("nan")
    try:
        held = _held_event_slugs()
    except Exception:
        held = set()
    delta_str = ""
    if start_usdc is not None and not math.isnan(free):
        delta = free - start_usdc
        delta_str = f"  Δusdc={delta:+.2f}"
    log.info(
        "[summary] cycles=%d picks_done=%d redeems_done=%d  held_events=%d  free_usdc=%.4f%s",
        cycles, picks_done, redeems_done, len(held), free, delta_str,
    )


# ───────────── The main loop ──────────────────────────────────────────────────
def runner(
    *,
    dry_run: bool,
    usdc_per_trade: float,
    budget: float,
    max_events: int,
    min_score: float,
    pick_interval: int,
    maintenance_interval: int,
    redeem_interval: int,
    free_usdc_buffer: float,
    once: bool,
    min_shares: float,
    min_gap: int,
    cancel_on_exit: bool = False,
    summary_interval: int = 3600,
) -> None:
    log = _setup_logging()
    funder = os.getenv("POLYMARKET_FUNDER") or ""
    if not funder:
        log.error("POLYMARKET_FUNDER not set in .env"); return

    log.info("──────── RUNNER START ────────")
    log.info("  dry_run=%s  per-trade=$%.2f  budget=$%.2f  max-events=%d  min-score=%.0f",
             dry_run, usdc_per_trade, budget, max_events, min_score)
    log.info("  intervals: pick=%ds  maintain=%ds  redeem=%ds  summary=%ds",
             pick_interval, maintenance_interval, redeem_interval, summary_interval)
    log.info("  cancel_on_exit=%s  funder=%s", cancel_on_exit, funder)
    log.info("──────────────────────────────")

    try:
        start_usdc = _free_usdc(funder)
        log.info("[summary] start  free_usdc=%.4f", start_usdc)
    except Exception:
        start_usdc = None

    last_pick_at = 0.0
    last_maintenance_at = 0.0
    last_redeem_at = 0.0
    last_summary_at = 0.0
    iteration = 0
    picks_done = 0
    redeems_done = 0

    try:
        while True:
            iteration += 1
            now = time.monotonic()
            log.info("── tick %d  utc=%s ──", iteration, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

            # ─── MAINTENANCE: seller_bot scan (winners + dust + placeholders) ─
            if now - last_maintenance_at >= maintenance_interval:
                try:
                    from automata.seller_bot import scan as seller_scan
                    from automata.seller_bot import _init_state_db
                    _init_state_db()
                    stats = seller_scan(
                        min_gap=min_gap,
                        edge_px=0.002,    # legacy arg (unused under new classifier)
                        closure_px=0.001, # legacy arg (unused under new classifier)
                        closure_minutes=60.0,
                        min_remaining_shares=min_shares,
                        dry_run=dry_run,
                    )
                    log.info("[maintain] considered=%d posted=%d",
                             stats.get("considered", 0), stats.get("posted", 0))
                except Exception as exc:
                    log.exception("[maintain] failed: %s", exc)
                last_maintenance_at = now

            # ─── REDEEM: cash out resolved events ─────────────────────────────
            if now - last_redeem_at >= redeem_interval:
                try:
                    stats = _redeem_resolved_held_events(dry_run, log)
                    log.info("[redeem] checked=%d redeemed=%d",
                             stats.get("checked", 0), stats.get("redeemed", 0))
                    redeems_done += int(stats.get("redeemed", 0) or 0)
                except Exception as exc:
                    log.exception("[redeem] failed: %s", exc)
                last_redeem_at = now

            # ─── PICK: open a new position if budget allows ───────────────────
            if now - last_pick_at >= pick_interval:
                try:
                    held_slugs = _held_event_slugs()
                    committed_est = len(held_slugs) * usdc_per_trade
                    free = _free_usdc(funder)
                    log.info("[budget] held_events=%d  committed≈$%.2f / $%.2f  free_usdc=$%.2f",
                             len(held_slugs), committed_est, budget, free)

                    if len(held_slugs) >= max_events:
                        log.info("[pick] at max-events (%d) — skipping", max_events)
                    elif committed_est + usdc_per_trade > budget:
                        log.info("[pick] would exceed budget ($%.2f + $%.2f > $%.2f) — skipping",
                                 committed_est, usdc_per_trade, budget)
                    elif free < usdc_per_trade + free_usdc_buffer:
                        log.info("[pick] insufficient free USDC ($%.2f < $%.2f + buffer $%.2f) — skipping",
                                 free, usdc_per_trade, free_usdc_buffer)
                    else:
                        from automata.splitter import action_auto
                        log.info("[pick] running auto chain ($%.2f)", usdc_per_trade)
                        res = action_auto(usdc_per_trade, dry_run, log, min_score=min_score)
                        if res.get("ok"):
                            log.info("[pick] auto chain OK")
                            picks_done += 1
                        else:
                            log.warning("[pick] auto chain ended at stage=%s ok=%s",
                                        res.get("stage"), res.get("ok"))
                except Exception as exc:
                    log.exception("[pick] failed: %s", exc)
                last_pick_at = now

            # ─── SUMMARY: periodic state snapshot for the UI log ──────────────
            if now - last_summary_at >= summary_interval:
                try:
                    _daily_summary(funder, log, iteration, picks_done, redeems_done, start_usdc)
                except Exception as exc:
                    log.warning("[summary] failed: %s", exc)
                last_summary_at = now

            if once:
                log.info("--once set, exiting after one cycle")
                return

            # Sleep until the next loop event (smallest interval). Catches signals.
            sleep_for = min(maintenance_interval, redeem_interval, pick_interval, 60)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        log.info("──── RUNNER STOPPING (KeyboardInterrupt) ────")
    except Exception as exc:
        log.exception("──── RUNNER CRASH: %s ────", exc)
    finally:
        try:
            _daily_summary(funder, log, iteration, picks_done, redeems_done, start_usdc)
        except Exception:
            pass
        if cancel_on_exit and not dry_run:
            log.info("──── cancel-on-exit: cancelling all open SELLs ────")
            try:
                n = _cancel_all_open_sells(log)
                log.info("──── cancel-on-exit: %d SELL order(s) cancelled ────", n)
            except Exception as exc:
                log.warning("cancel-on-exit failed: %s", exc)
        log.info("──── RUNNER STOPPED ────")


def main() -> int:
    p = argparse.ArgumentParser(description="Autonomous aapang-style trading runner")
    p.add_argument("--usdc", type=float, default=5.0,
                   help="Per-event SPLIT amount in USDC (= shares per bucket). Default $5.")
    p.add_argument("--budget", type=float, default=50.0,
                   help="Max $ committed across all held events at once. Default $50.")
    p.add_argument("--max-events", type=int, default=10,
                   help="Max concurrent distinct events in play. Default 10.")
    p.add_argument("--min-score", type=float, default=30.0,
                   help="Picker minimum score to take an event. Default 30.")
    p.add_argument("--pick-interval", type=int, default=1800,
                   help="Seconds between pick scans. Default 1800 (30min).")
    p.add_argument("--maintenance-interval", type=int, default=60,
                   help="Seconds between seller_bot scans. Default 60.")
    p.add_argument("--redeem-interval", type=int, default=300,
                   help="Seconds between redeem scans. Default 300 (5min).")
    p.add_argument("--free-usdc-buffer", type=float, default=1.0,
                   help="Reserve this much extra USDC above per-trade for slack. Default $1.")
    p.add_argument("--min-gap", type=int, default=2,
                   help="Seller_bot min bucket distance from peak to fade. Default 2.")
    p.add_argument("--min-shares", type=float, default=5.0,
                   help="Skip buckets with fewer than this many shares (Polymarket order-min). Default 5.")
    p.add_argument("--once", action="store_true",
                   help="Run one cycle of all three loops, then exit.")
    p.add_argument("--cancel-on-exit", action="store_true",
                   help="On Ctrl-C / shutdown, cancel every open SELL order on the CLOB. "
                        "Useful so resting fades don't outlive the runner.")
    p.add_argument("--summary-interval", type=int, default=3600,
                   help="Seconds between [summary] log lines (default 3600 = 1h). "
                        "These show in the stock UI weather-log viewer.")
    p.add_argument("--live", action="store_true",
                   help="Broadcast for real. Default: dry-run.")
    args = p.parse_args()

    runner(
        dry_run=not args.live,
        usdc_per_trade=args.usdc,
        budget=args.budget,
        max_events=args.max_events,
        min_score=args.min_score,
        pick_interval=max(60, args.pick_interval),
        maintenance_interval=max(15, args.maintenance_interval),
        redeem_interval=max(60, args.redeem_interval),
        free_usdc_buffer=args.free_usdc_buffer,
        once=args.once,
        min_shares=args.min_shares,
        min_gap=args.min_gap,
        cancel_on_exit=args.cancel_on_exit,
        summary_interval=max(60, args.summary_interval),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
