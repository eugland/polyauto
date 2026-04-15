#!/usr/bin/env python3
"""
Forward test: simulate posting BTC + ETH buy-limit orders, then cancel after a fixed window.

Strategy:
- At cycle start, "submit" one buy limit for BTC and one for ETH at --limit-price.
- Watch both sides (Up/Down) of each asset for --window-sec seconds.
- If either side ask touches <= --limit-price during the window, mark that asset as hit.
- At window end, cancel both simulated orders.

This measures touch probability, not guaranteed fill probability.

Usage:
  python -m experiment.crypto_dual_limit_forward
  python -m experiment.crypto_dual_limit_forward --limit-price 0.03 --window-sec 150 --poll 1
  python -m experiment.crypto_dual_limit_forward --cycles 20
"""
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

GAMMA_API = "https://gamma-api.polymarket.com/events"
CLOB_HOST = "https://clob.polymarket.com"

LOG_DIR = os.path.join("experiment", "logs")
DEFAULT_LOG = os.path.join(LOG_DIR, "crypto_dual_limit_forward.jsonl")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _get_json(url: str, timeout: int = 12) -> Any:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _post_json(url: str, payload: Any, timeout: int = 15) -> Any:
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _load_field(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return []
    return v


def _append_jsonl(path: str, row: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=True) + "\n")


def _best_ask(book: dict) -> float | None:
    best = None
    for level in book.get("asks") or []:
        try:
            p = float(level.get("price"))
        except (TypeError, ValueError):
            continue
        if best is None or p < best:
            best = p
    return best


def _fetch_books(token_ids: list[str]) -> dict[str, dict]:
    if not token_ids:
        return {}
    try:
        books = _post_json(f"{CLOB_HOST}/books", [{"token_id": t} for t in token_ids], timeout=15)
    except Exception:
        return {}
    out: dict[str, dict] = {}
    for b in books:
        token_id = str(b.get("asset_id") or b.get("token_id") or "")
        if token_id:
            out[token_id] = b
    return out


def _get_current_market(asset: str) -> dict[str, str] | None:
    now_ts = int(_now_utc().timestamp())
    bucket = (now_ts // 300) * 300
    for delta in (0, -1, 1):
        slug = f"{asset.lower()}-updown-5m-{bucket + delta * 300}"
        try:
            data = _get_json(f"{GAMMA_API}?slug={slug}")
        except Exception:
            continue
        if not isinstance(data, list) or not data:
            continue
        event = data[0]
        markets = event.get("markets") or []
        if not markets:
            continue
        market = markets[0]
        if market.get("closed"):
            continue
        outcomes = _load_field(market.get("outcomes")) or []
        token_ids = _load_field(market.get("clobTokenIds")) or []
        up_token = None
        down_token = None
        for i, name in enumerate(outcomes):
            if i >= len(token_ids):
                continue
            label = str(name).strip().lower()
            if label == "up":
                up_token = str(token_ids[i])
            elif label == "down":
                down_token = str(token_ids[i])
        if up_token and down_token:
            return {
                "asset": asset,
                "slug": slug,
                "up_token": up_token,
                "down_token": down_token,
            }
    return None


@dataclass
class AssetOrderState:
    asset: str
    slug: str
    up_token: str
    down_token: str
    submitted_at: str
    hit: bool = False
    hit_side: str | None = None
    hit_price: float | None = None
    hit_at: str | None = None


class DualLimitForwardTest:
    def __init__(
        self,
        limit_price: float,
        window_sec: int,
        poll_sec: float,
        log_file: str,
        cycles: int,
    ) -> None:
        self.limit_price = limit_price
        self.window_sec = window_sec
        self.poll_sec = poll_sec
        self.log_file = log_file
        self.cycles = cycles

        self.total_cycles = 0
        self.btc_hits = 0
        self.eth_hits = 0
        self.any_hits = 0

    def _log(self, msg: str) -> None:
        print(f"[{_now_utc().strftime('%H:%M:%S')}] {msg}")

    def _print_summary(self) -> None:
        if self.total_cycles == 0:
            return
        btc_wr = (100.0 * self.btc_hits / self.total_cycles)
        eth_wr = (100.0 * self.eth_hits / self.total_cycles)
        any_wr = (100.0 * self.any_hits / self.total_cycles)
        print()
        print("=" * 68)
        print(
            f"cycles={self.total_cycles}  "
            f"BTC hit={self.btc_hits} ({btc_wr:.1f}%)  "
            f"ETH hit={self.eth_hits} ({eth_wr:.1f}%)  "
            f"either hit={self.any_hits} ({any_wr:.1f}%)"
        )
        print("=" * 68)
        print()

    def _build_cycle_state(self) -> dict[str, AssetOrderState] | None:
        btc = _get_current_market("BTC")
        eth = _get_current_market("ETH")
        if not btc or not eth:
            self._log("Cycle skipped: could not load active BTC and ETH 5m markets.")
            return None

        submitted_at = _now_iso()
        state = {
            "BTC": AssetOrderState(
                asset="BTC",
                slug=btc["slug"],
                up_token=btc["up_token"],
                down_token=btc["down_token"],
                submitted_at=submitted_at,
            ),
            "ETH": AssetOrderState(
                asset="ETH",
                slug=eth["slug"],
                up_token=eth["up_token"],
                down_token=eth["down_token"],
                submitted_at=submitted_at,
            ),
        }
        _append_jsonl(
            self.log_file,
            {
                "ts_utc": submitted_at,
                "kind": "submit",
                "limit_price": self.limit_price,
                "window_sec": self.window_sec,
                "orders": {
                    "BTC": {"slug": state["BTC"].slug},
                    "ETH": {"slug": state["ETH"].slug},
                },
            },
        )
        self._log(
            f"Submit BTC+ETH buy limits @ ${self.limit_price:.4f} "
            f"(window={self.window_sec}s)  "
            f"btc={state['BTC'].slug}  eth={state['ETH'].slug}"
        )
        return state

    def _poll_for_hits(self, state: dict[str, AssetOrderState], end_ts: float) -> None:
        all_tokens = [
            state["BTC"].up_token,
            state["BTC"].down_token,
            state["ETH"].up_token,
            state["ETH"].down_token,
        ]

        while time.time() < end_ts:
            books = _fetch_books(all_tokens)
            now_iso = _now_iso()

            for asset in ("BTC", "ETH"):
                st = state[asset]
                if st.hit:
                    continue
                up_ask = _best_ask(books.get(st.up_token, {}))
                down_ask = _best_ask(books.get(st.down_token, {}))

                side = None
                price = None
                if up_ask is not None and up_ask <= self.limit_price:
                    side = "Up"
                    price = up_ask
                if down_ask is not None and down_ask <= self.limit_price:
                    if price is None or down_ask < price:
                        side = "Down"
                        price = down_ask

                if side is None:
                    continue

                st.hit = True
                st.hit_side = side
                st.hit_price = price
                st.hit_at = now_iso
                _append_jsonl(
                    self.log_file,
                    {
                        "ts_utc": now_iso,
                        "kind": "hit",
                        "asset": st.asset,
                        "slug": st.slug,
                        "side": side,
                        "ask": price,
                        "limit_price": self.limit_price,
                    },
                )
                self._log(f"HIT {st.asset}: {side} ask touched ${price:.4f} <= ${self.limit_price:.4f}")

            if state["BTC"].hit and state["ETH"].hit:
                break
            time.sleep(self.poll_sec)

    def _cancel_and_score(self, state: dict[str, AssetOrderState]) -> None:
        now_iso = _now_iso()
        btc_hit = state["BTC"].hit
        eth_hit = state["ETH"].hit

        self.total_cycles += 1
        self.btc_hits += int(btc_hit)
        self.eth_hits += int(eth_hit)
        self.any_hits += int(btc_hit or eth_hit)

        _append_jsonl(
            self.log_file,
            {
                "ts_utc": now_iso,
                "kind": "cancel",
                "limit_price": self.limit_price,
                "window_sec": self.window_sec,
                "results": {
                    "BTC": {
                        "slug": state["BTC"].slug,
                        "hit": btc_hit,
                        "hit_side": state["BTC"].hit_side,
                        "hit_price": state["BTC"].hit_price,
                        "hit_at": state["BTC"].hit_at,
                    },
                    "ETH": {
                        "slug": state["ETH"].slug,
                        "hit": eth_hit,
                        "hit_side": state["ETH"].hit_side,
                        "hit_price": state["ETH"].hit_price,
                        "hit_at": state["ETH"].hit_at,
                    },
                },
                "running": {
                    "cycles": self.total_cycles,
                    "btc_hits": self.btc_hits,
                    "eth_hits": self.eth_hits,
                    "either_hits": self.any_hits,
                },
            },
        )

        self._log(
            f"Cancel after {self.window_sec}s | "
            f"BTC={'HIT' if btc_hit else 'MISS'} "
            f"ETH={'HIT' if eth_hit else 'MISS'} "
            f"either={'HIT' if (btc_hit or eth_hit) else 'MISS'}"
        )
        self._print_summary()

    def run(self) -> None:
        print("=" * 68)
        print("Dual-Asset Limit Touch Forward Test (BTC + ETH)")
        print(
            f"limit=${self.limit_price:.4f}  window={self.window_sec}s  "
            f"poll={self.poll_sec:.2f}s  cycles={'infinite' if self.cycles == 0 else self.cycles}"
        )
        print(f"log={self.log_file}")
        print("=" * 68)
        print()

        target_cycles = self.cycles if self.cycles > 0 else None

        while target_cycles is None or self.total_cycles < target_cycles:
            state = self._build_cycle_state()
            if state is None:
                time.sleep(max(self.poll_sec, 1.0))
                continue
            end_ts = time.time() + self.window_sec
            self._poll_for_hits(state, end_ts)
            # Enforce full window before cancel emulation.
            remaining = end_ts - time.time()
            if remaining > 0:
                time.sleep(remaining)
            self._cancel_and_score(state)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Forward test: submit BTC+ETH buy limits, watch for touch, cancel after fixed window."
    )
    p.add_argument("--limit-price", type=float, default=0.03, help="Buy limit price (default: 0.03)")
    p.add_argument("--window-sec", type=int, default=150, help="Order lifetime in seconds (default: 150)")
    p.add_argument("--poll", type=float, default=1.0, help="Book polling interval in seconds (default: 1.0)")
    p.add_argument("--cycles", type=int, default=0, help="Number of cycles; 0 means run forever (default: 0)")
    p.add_argument("--log-file", default=DEFAULT_LOG, help="JSONL output path")
    args = p.parse_args()

    if args.limit_price <= 0:
        raise SystemExit("--limit-price must be > 0")
    if args.window_sec <= 0:
        raise SystemExit("--window-sec must be > 0")
    if args.poll <= 0:
        raise SystemExit("--poll must be > 0")
    if args.cycles < 0:
        raise SystemExit("--cycles cannot be negative")

    DualLimitForwardTest(
        limit_price=args.limit_price,
        window_sec=args.window_sec,
        poll_sec=args.poll,
        log_file=args.log_file,
        cycles=args.cycles,
    ).run()


if __name__ == "__main__":
    main()
