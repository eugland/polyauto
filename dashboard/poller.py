"""Balance-history poller: snapshots (portfolio value + cash) on an interval.

Runs as a daemon thread inside the Flask app, or standalone:
    python -m dashboard.poller
"""
from __future__ import annotations

import logging
import threading
import time

from dashboard import db, polymarket_data, settings, wallet

log = logging.getLogger("dashboard.poller")


def snapshot_once() -> dict:
    """Take one balance snapshot and persist it. Returns the recorded values."""
    addr = settings.my_address()
    portfolio = polymarket_data.portfolio_value(addr)
    cash = wallet.cash()
    db.init_db()
    db.insert_snapshot(portfolio, cash)
    total = portfolio + (cash or 0.0)
    log.info("snapshot: portfolio=%.2f cash=%s total=%.2f", portfolio, cash, total)
    return {"portfolio": portfolio, "cash": cash, "total": round(total, 2)}


def run_forever(stop: threading.Event | None = None) -> None:
    interval = settings.poll_interval_seconds()
    log.info("balance poller started (every %ds)", interval)
    while True:
        try:
            snapshot_once()
        except Exception as exc:  # noqa: BLE001
            log.warning("snapshot failed: %s", exc)
        if stop is not None and stop.wait(interval):
            return
        if stop is None:
            time.sleep(interval)


def start_thread() -> threading.Thread:
    """Start the poller as a daemon thread (used by the Flask app)."""
    t = threading.Thread(target=run_forever, name="balance-poller", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    run_forever()
