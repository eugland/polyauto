#!/usr/bin/env python3
"""
BTC 5-minute penny scanner — paper-trade when ask hits 1 cent.

Continuously scans the active btc-updown-5m market on Polymarket.
When either side's best ask drops to $0.01, records a simulated buy.
After the candle closes, polls Polymarket until the market resolves
(outcomePrices shows a "1"), then updates the running profit/loss tally.

Console log shows every slug switch, every bet, every resolution,
and a running P/L summary.

Usage:
  python experiment/btc_5m_penny_scanner.py
  python experiment/btc_5m_penny_scanner.py --shares 10 --max-ask 0.02
"""
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from experiment import btc_5m_49c_check as checker

GAMMA_API = "https://gamma-api.polymarket.com/events"
LOG_DIR = os.path.join("experiment", "logs")
DEFAULT_LOG = os.path.join(LOG_DIR, "btc_5m_penny_scanner.jsonl")


# ── helpers ──────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug_epoch(slug: str) -> int | None:
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except Exception:
        return None


def _append_jsonl(path: str, row: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _resolve_winner_polymarket(slug: str) -> str | None:
    """Poll Gamma API for the resolved outcome.

    Returns "Up" or "Down" if the market has resolved, None if still pending.
    Uses outcomePrices — the winning side gets "1", the loser gets "0".
    """
    try:
        data = checker._get_json(f"{GAMMA_API}?slug={slug}")
        if not isinstance(data, list) or not data:
            return None
        event = data[0]
        markets = event.get("markets") or []
        if not markets:
            return None
        market = markets[0]
        if not market.get("closed"):
            return None
        outcomes = market.get("outcomes") or []
        prices_raw = market.get("outcomePrices") or []
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices_raw, str):
            prices_raw = json.loads(prices_raw)
        for i, price_str in enumerate(prices_raw):
            if i < len(outcomes) and str(price_str) == "1":
                return str(outcomes[i])  # "Up" or "Down"
        return None
    except Exception:
        return None


# ── data ─────────────────────────────────────────────────────────────

@dataclass
class PaperBet:
    slug: str
    side: str          # "Up" or "Down"
    ask: float
    shares: float
    entry_time: str    # ISO-8601
    secs_remaining: int
    outcome: str | None = None    # "win" / "loss" / None
    pnl: float = 0.0


@dataclass
class Scanner:
    max_ask: float
    shares: float
    interval: float
    resolve_delay: int
    log_file: str
    mode: str

    current_slug: str = ""
    bets: list[PaperBet] = field(default_factory=list)
    total_cost: float = 0.0
    total_pnl: float = 0.0
    wins: int = 0
    losses: int = 0

    def _log(self, tag: str, msg: str) -> None:
        print(f"[{_ts()}] [{tag}] {msg}")

    # ── slug lifecycle ───────────────────────────────────────────────

    def _on_slug_switch(self, new_slug: str, secs: int) -> None:
        if self.current_slug and self.current_slug != new_slug:
            self._log("SLUG-END", f"{self.current_slug} finished")
            self._resolve_pending(self.current_slug)
        self._log("SLUG-NEW", f"Switched to {new_slug}  ({secs}s remaining)")
        _append_jsonl(self.log_file, {
            "ts": _now_iso(), "event": "slug_switch",
            "from": self.current_slug or "(start)",
            "to": new_slug, "secs_remaining": secs,
        })
        self.current_slug = new_slug

    # ── entry ────────────────────────────────────────────────────────

    def _already_bet(self, slug: str) -> bool:
        return any(b.slug == slug for b in self.bets)

    def _place_bet(self, slug: str, side: str, ask: float, secs: int) -> None:
        cost = ask * self.shares
        bet = PaperBet(
            slug=slug, side=side, ask=ask, shares=self.shares,
            entry_time=_now_iso(), secs_remaining=secs,
        )
        self.bets.append(bet)
        self.total_cost += cost
        self._log("BET", (
            f"Paper buy {side} @ ${ask:.4f} x {self.shares:.0f} shares  "
            f"(cost ${cost:.4f})  slug={slug}  {secs}s left"
        ))
        _append_jsonl(self.log_file, {
            "ts": _now_iso(), "event": "bet",
            "slug": slug, "side": side, "ask": ask,
            "shares": self.shares, "cost": cost,
            "secs_remaining": secs,
        })

    # ── resolution ───────────────────────────────────────────────────

    def _resolve_pending(self, slug: str) -> None:
        pending = [b for b in self.bets if b.slug == slug and b.outcome is None]
        if not pending:
            return
        winner = _resolve_winner_polymarket(slug)
        if winner is None:
            # Not resolved yet on Polymarket — leave pending, will retry later.
            self._log("RESOLVE", f"{slug} not yet resolved on Polymarket — will retry")
            return

        for b in pending:
            won = b.side == winner
            b.outcome = "win" if won else "loss"
            b.pnl = ((1.0 - b.ask) if won else -b.ask) * b.shares
            self.total_pnl += b.pnl
            if won:
                self.wins += 1
            else:
                self.losses += 1
            self._log("RESOLVE", (
                f"{slug}  side={b.side}  winner={winner}  "
                f"{'WIN' if won else 'LOSS'}  pnl={b.pnl:+.4f}"
            ))
            _append_jsonl(self.log_file, {
                "ts": _now_iso(), "event": "resolve",
                "slug": slug, "side": b.side, "winner": winner,
                "won": won, "ask": b.ask, "shares": b.shares,
                "pnl": round(b.pnl, 6),
            })
        self._print_summary()

    def _try_resolve_expired(self, now_epoch: int) -> None:
        pending_slugs = {
            b.slug for b in self.bets
            if b.outcome is None and b.slug != self.current_slug
        }
        for slug in pending_slugs:
            ep = _slug_epoch(slug)
            if ep is None:
                continue
            # Only start checking after the candle has ended (epoch + 300s).
            if now_epoch >= ep + 300:
                self._resolve_pending(slug)

    # ── summary ──────────────────────────────────────────────────────

    def _print_summary(self) -> None:
        resolved = self.wins + self.losses
        wr = (self.wins / resolved * 100) if resolved else 0
        print()
        print(f"  ╔══════════════ P/L REPORT ══════════════╗")
        print(f"  ║  Bets placed  : {len(self.bets):>6}                ║")
        print(f"  ║  Resolved     : {resolved:>6}                ║")
        print(f"  ║  Wins / Losses: {self.wins:>4} / {self.losses:<4}            ║")
        print(f"  ║  Win rate     : {wr:>6.1f}%               ║")
        print(f"  ║  Total cost   : ${self.total_cost:>10.4f}         ║")
        print(f"  ║  Total P/L    : ${self.total_pnl:>+10.4f}         ║")
        roi = (self.total_pnl / self.total_cost * 100) if self.total_cost else 0
        print(f"  ║  ROI          : {roi:>+7.1f}%              ║")
        print(f"  ╚═════════════════════════════════════════╝")
        print()

    # ── main loop ────────────────────────────────────────────────────

    def run(self) -> None:
        print("=" * 60)
        print("  BTC 5m Penny Scanner  (paper trading)")
        print(f"  max_ask=${self.max_ask:.2f}  shares={self.shares:.0f}  "
              f"mode={self.mode}  interval={self.interval}s")
        print(f"  log: {self.log_file}")
        print("=" * 60)
        print()

        while True:
            now_epoch = int(datetime.now(timezone.utc).timestamp())

            # Try resolving any expired bets first.
            self._try_resolve_expired(now_epoch)

            # Find current market.
            try:
                mkt = checker._find_current_btc_5m_event()
            except Exception as exc:
                self._log("SCAN", f"Market lookup failed: {exc}")
                time.sleep(self.interval)
                continue

            slug = str(mkt["slug"])
            secs = int(mkt.get("seconds_remaining") or -1)
            up_token = str(mkt["up_token"])
            down_token = str(mkt["down_token"])

            # Detect slug switch.
            if slug != self.current_slug:
                self._on_slug_switch(slug, secs)

            # Already bet on this slug — just wait.
            if self._already_bet(slug):
                time.sleep(self.interval)
                continue

            # Fetch order books.
            try:
                books = checker._fetch_books([up_token, down_token], checker.CLOB_HOST)
            except Exception as exc:
                self._log("SCAN", f"Book fetch failed: {exc}")
                time.sleep(self.interval)
                continue

            up_book = books.get(up_token, {})
            down_book = books.get(down_token, {})
            _, up_ask = checker._available_at_or_below(up_book, 1.0)
            _, down_ask = checker._available_at_or_below(down_book, 1.0)

            # Check if either side is at or below our penny threshold.
            up_hit = up_ask is not None and up_ask <= self.max_ask
            down_hit = down_ask is not None and down_ask <= self.max_ask

            if not up_hit and not down_hit:
                # Log scan heartbeat every so often (quiet).
                self._log("SCAN", (
                    f"{slug}  {secs}s left  "
                    f"up_ask={up_ask if up_ask is not None else 'n/a'}  "
                    f"down_ask={down_ask if down_ask is not None else 'n/a'}"
                ))
                time.sleep(self.interval)
                continue

            # Pick side(s) to buy.
            if self.mode == "both" and up_hit and down_hit:
                self._place_bet(slug, "Up", up_ask, secs)
                self._place_bet(slug, "Down", down_ask, secs)
            elif self.mode == "cheapest":
                if up_hit and down_hit:
                    if up_ask <= down_ask:
                        self._place_bet(slug, "Up", up_ask, secs)
                    else:
                        self._place_bet(slug, "Down", down_ask, secs)
                elif up_hit:
                    self._place_bet(slug, "Up", up_ask, secs)
                else:
                    self._place_bet(slug, "Down", down_ask, secs)
            else:
                # Default: buy whichever is at penny (prefer up if tie).
                if up_hit:
                    self._place_bet(slug, "Up", up_ask, secs)
                elif down_hit:
                    self._place_bet(slug, "Down", down_ask, secs)

            time.sleep(self.interval)


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Paper-trade BTC 5m markets when ask hits a penny")
    p.add_argument("--max-ask", type=float, default=0.01,
                   help="Buy threshold (default $0.01)")
    p.add_argument("--shares", type=float, default=1.0,
                   help="Paper shares per trade")
    p.add_argument("--interval", type=float, default=2.0,
                   help="Seconds between scans")
    p.add_argument("--resolve-delay", type=int, default=5,
                   help="Seconds after candle end before resolving")
    p.add_argument("--mode", choices=["first", "cheapest", "both"],
                   default="first",
                   help="Side selection when both qualify")
    p.add_argument("--log-file", default=DEFAULT_LOG,
                   help="JSONL log path")
    args = p.parse_args()

    scanner = Scanner(
        max_ask=args.max_ask,
        shares=args.shares,
        interval=args.interval,
        resolve_delay=args.resolve_delay,
        log_file=args.log_file,
        mode=args.mode,
    )
    scanner.run()


if __name__ == "__main__":
    main()
