#!/usr/bin/env python3
"""
Forward paper test for a 1-cent BTC 5m strategy.

No backtest/replay. This script only evaluates candles while it is running.

Flow:
1) Poll active btc-updown-5m market and books.
2) Near expiry window, if ask <= max_ask, record a paper entry (once per slug).
3) After candle resolves, determine winner from Binance 5m open/close.
4) Log realized PnL and rolling stats.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen

from experiment import btc_5m_49c_check as checker


BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


@dataclass
class OpenTrade:
    slug: str
    side: str
    ask: float
    shares: float
    entered_at: str
    seconds_remaining: int


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _append_jsonl(path: str, row: dict[str, Any]) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _slug_start_ts(slug: str) -> int | None:
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except Exception:
        return None


def _resolve_winner(slug: str) -> str | None:
    ts = _slug_start_ts(slug)
    if ts is None:
        return None
    url = f"{BINANCE_KLINES}?symbol=BTCUSDT&interval=5m&startTime={ts * 1000}&limit=1"
    try:
        k = _get_json(url)
        if not isinstance(k, list) or not k:
            return None
        o = float(k[0][1])
        c = float(k[0][4])
        return "up" if c > o else "down"
    except Exception:
        return None


def _choose_entry_side(
    up_ask: float | None,
    down_ask: float | None,
    up_edge: float | None,
    down_edge: float | None,
    max_ask: float,
    mode: str,
) -> tuple[str, float] | None:
    cands: list[tuple[str, float, float | None]] = []
    if up_ask is not None and up_ask <= max_ask:
        cands.append(("up", up_ask, up_edge))
    if down_ask is not None and down_ask <= max_ask:
        cands.append(("down", down_ask, down_edge))
    if not cands:
        return None

    if mode == "up":
        for s, a, _ in cands:
            if s == "up":
                return s, a
        return None
    if mode == "down":
        for s, a, _ in cands:
            if s == "down":
                return s, a
        return None
    if mode == "cheapest":
        s, a, _ = min(cands, key=lambda x: x[1])
        return s, a
    # best-edge default
    s, a, _ = max(cands, key=lambda x: (-999.0 if x[2] is None else x[2]))
    return s, a


def run(
    interval: float,
    max_ask: float,
    min_seconds: int,
    max_seconds: int,
    shares: float,
    mode: str,
    log_file: str,
    resolve_delay: int,
) -> None:
    open_trades: dict[str, OpenTrade] = {}
    seen_slugs: set[str] = set()
    completed = 0
    wins = 0
    total_pnl = 0.0
    total_cost = 0.0

    print("=" * 72)
    print("BTC 5m one-cent FORWARD paper test")
    print(
        f"mode={mode} max_ask={max_ask:.4f} window=[{min_seconds},{max_seconds}]s "
        f"shares={shares:.2f} interval={interval:.2f}s"
    )
    print(f"log_file={log_file}")
    print("=" * 72)

    while True:
        now = datetime.now(timezone.utc)
        now_epoch = int(now.timestamp())

        # Resolve open trades when the candle has finished.
        for slug in list(open_trades.keys()):
            start_ts = _slug_start_ts(slug)
            if start_ts is None:
                open_trades.pop(slug, None)
                continue
            end_ts = start_ts + 5 * 60
            if now_epoch < end_ts + max(resolve_delay, 0):
                continue
            t = open_trades.pop(slug)
            winner = _resolve_winner(slug)
            if winner is None:
                # Keep it simple: skip unresolved rows, do not count.
                _append_jsonl(
                    log_file,
                    {
                        "ts_utc": _now_iso(),
                        "kind": "resolve_miss",
                        "slug": slug,
                        "side": t.side,
                        "ask": t.ask,
                        "shares": t.shares,
                        "entered_at": t.entered_at,
                    },
                )
                continue
            won = t.side == winner
            pnl_per_share = (1.0 - t.ask) if won else (-t.ask)
            pnl = pnl_per_share * t.shares
            cost = t.ask * t.shares
            completed += 1
            wins += 1 if won else 0
            total_pnl += pnl
            total_cost += cost
            win_rate = (wins / completed) if completed else 0.0
            roi_on_cost = (total_pnl / total_cost) if total_cost > 0 else None

            row = {
                "ts_utc": _now_iso(),
                "kind": "resolved_trade",
                "slug": slug,
                "side": t.side,
                "winner": winner,
                "won": won,
                "ask": t.ask,
                "shares": t.shares,
                "seconds_remaining_at_entry": t.seconds_remaining,
                "entered_at": t.entered_at,
                "pnl_per_share": pnl_per_share,
                "pnl": pnl,
                "running_trades": completed,
                "running_wins": wins,
                "running_win_rate": win_rate,
                "running_total_pnl": total_pnl,
                "running_total_cost": total_cost,
                "roi_on_cost": roi_on_cost,
            }
            _append_jsonl(log_file, row)
            print(
                f"[RESOLVE] slug={slug} side={t.side} winner={winner} "
                f"ask={t.ask:.4f} pnl={pnl:+.4f}  "
                f"running: trades={completed} wr={win_rate*100:.2f}% pnl={total_pnl:+.4f}"
            )

        # Find current market and decide on entry.
        try:
            mkt = checker._find_current_btc_5m_event()
            slug = str(mkt["slug"])
            secs = int(mkt.get("seconds_remaining") or -1)
            end_utc = mkt.get("end_utc")
            strike = mkt.get("price_to_beat")
            up_token = str(mkt["up_token"])
            down_token = str(mkt["down_token"])
        except Exception as exc:
            print(f"[WARN] market lookup failed: {exc}")
            time.sleep(max(interval, 0.2))
            continue

        if slug in seen_slugs or slug in open_trades:
            time.sleep(max(interval, 0.2))
            continue

        if secs < min_seconds or secs > max_seconds:
            time.sleep(max(interval, 0.2))
            continue

        try:
            books = checker._fetch_books([up_token, down_token], checker.CLOB_HOST)
        except Exception as exc:
            print(f"[WARN] books fetch failed: {exc}")
            time.sleep(max(interval, 0.2))
            continue

        up_book = books.get(up_token, {})
        down_book = books.get(down_token, {})
        _, up_ask = checker._available_at_or_below(up_book, 1.0)
        _, down_ask = checker._available_at_or_below(down_book, 1.0)
        fair_up, fair_down, _meta = checker._fetch_btc_bs_fair(
            seconds_remaining=secs,
            strike=strike,
            end_utc=end_utc,
        )
        up_edge = (fair_up - up_ask) if fair_up is not None and up_ask is not None else None
        down_edge = (fair_down - down_ask) if fair_down is not None and down_ask is not None else None

        pick = _choose_entry_side(
            up_ask=up_ask,
            down_ask=down_ask,
            up_edge=up_edge,
            down_edge=down_edge,
            max_ask=max_ask,
            mode=mode,
        )
        if pick is None:
            time.sleep(max(interval, 0.2))
            continue

        side, ask = pick
        trade = OpenTrade(
            slug=slug,
            side=side,
            ask=ask,
            shares=shares,
            entered_at=_now_iso(),
            seconds_remaining=secs,
        )
        open_trades[slug] = trade
        seen_slugs.add(slug)

        _append_jsonl(
            log_file,
            {
                "ts_utc": _now_iso(),
                "kind": "entry",
                "slug": slug,
                "side": side,
                "ask": ask,
                "shares": shares,
                "seconds_remaining": secs,
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_edge": up_edge,
                "down_edge": down_edge,
                "max_ask": max_ask,
                "mode": mode,
            },
        )
        print(
            f"[ENTRY] slug={slug} sec={secs} side={side} ask={ask:.4f} "
            f"(up_ask={up_ask if up_ask is not None else 'n/a'} "
            f"down_ask={down_ask if down_ask is not None else 'n/a'})"
        )

        time.sleep(max(interval, 0.2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward-only paper test for 1-cent BTC 5m entries")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval seconds")
    parser.add_argument("--max-ask", type=float, default=0.01, help="Entry cap per side")
    parser.add_argument("--min-seconds", type=int, default=1, help="Min seconds remaining to enter")
    parser.add_argument("--max-seconds", type=int, default=30, help="Max seconds remaining to enter")
    parser.add_argument("--shares", type=float, default=1.0, help="Paper shares per trade")
    parser.add_argument(
        "--mode",
        choices=["best-edge", "cheapest", "up", "down"],
        default="best-edge",
        help="Side selection mode when one or both asks qualify",
    )
    parser.add_argument(
        "--log-file",
        default=os.path.join("experiment", "logs", "btc_5m_one_cent_forward.jsonl"),
        help="Forward test JSONL log path",
    )
    parser.add_argument(
        "--resolve-delay",
        type=int,
        default=3,
        help="Seconds after candle end before resolving outcome",
    )
    args = parser.parse_args()
    run(
        interval=args.interval,
        max_ask=args.max_ask,
        min_seconds=args.min_seconds,
        max_seconds=args.max_seconds,
        shares=args.shares,
        mode=args.mode,
        log_file=args.log_file,
        resolve_delay=args.resolve_delay,
    )


if __name__ == "__main__":
    main()
