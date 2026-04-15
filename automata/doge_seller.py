"""
automata/doge_seller.py — Live DOGE 5m sell-side strategy for Polymarket.

Strategy: Adaptive Urgency-Weighted Execution (AUWE)
-----------------------------------------------------
At candle start:
  • Split $5 USDC → $2.50 per side (Up / Down).
  • Evaluate a composite signal (Binance dual-EMA momentum + order-book
    imbalance) to choose WHICH side to sell first.
  • Place real GTC limit sell orders via the Polymarket CLOB.

Execution:
  T > 20 s  →  best_ask − 1 tick  (undercut queue, front of line immediately)
  T ≤ 20 s  →  best_bid           (cross to taker, guaranteed exit)

No minimum price floor — if the market is skewed 80/20, selling the 20-cent
side at $0.19 is the trade. The signal decides whether to trade, not the price.

Reprice whenever market best_ask drifts ≥ 1 tick below our order.

Usage:
    python -m automata.doge_seller                    # dry-run
    python -m automata.doge_seller --bet              # live orders
    python -m automata.doge_seller --bet --budget 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import websockets
from dotenv import load_dotenv

from automata.client import (
    build_client,
    cancel_order,
    derive_api_credentials,
    get_positions,
    get_usdc_balance,
    place_sell_order,
)

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────

log = logging.getLogger("automata.doge_seller")

LOG_PATH = Path("experiment") / "logs" / "doge_seller.log"


def _init_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
        )
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    log.addHandler(fh)


# ── Constants ─────────────────────────────────────────────────────────────────

GAMMA_API       = "https://gamma-api.polymarket.com/events"
WS_POLY         = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_BINANCE      = "wss://stream.binance.com:9443/stream?streams=dogeusdt@aggTrade"

WS_RECONNECT    = 3    # seconds between reconnect attempts
WS_PING         = 20   # WebSocket keepalive interval

# Binance dual-EMA signal
SHORT_ALPHA     = 0.30    # fast EMA (reacts in ~3 ticks)
LONG_ALPHA      = 0.03    # slow EMA (baseline trend)
MIN_SIGNAL_STR  = 0.0003  # 0.03% momentum divergence to trigger

# Order-book imbalance
DEPTH_LEVELS    = 5       # top N levels used for OBI computation
OBI_MIN         = 0.15    # |OBI| must exceed this to count as confirming signal
TOB_RATE_WINDOW = 10      # seconds window for ToB event-rate calculation

TICK            = 0.01    # Polymarket price tick

TAKER_SECS      = 20      # T ≤ this → cross to bid (last-chance taker)
REPRICE_TICKS   = 1       # reprice when market ask drops ≥ this many ticks below ours
STRATEGY_TICK   = 3.0     # main loop cadence (seconds)
DISPLAY_TICK    = 1.0     # terminal refresh (seconds)
MARKET_REFRESH  = 30.0    # how often to rediscover the active candle (seconds)
AGGRESSIVE_FROM_START_DEFAULT = True
CANDLE_SECONDS = 300.0

DEFAULT_BUDGET      = 5.0   # USDC total per candle
DEFAULT_MAX_BALANCE = 50.0  # safety cap on USDC balance consumed
CTF_ADDRESS         = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E_ADDRESS      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID    = 137
POLYGON_RPC_FALLBACKS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]
SPLIT_INVENTORY_SYNC_ATTEMPTS = 5
SPLIT_INVENTORY_SYNC_DELAY_SECONDS = 0.8


# ── Shared streaming state ────────────────────────────────────────────────────

_book_cache: dict[str, dict] = {}       # token_id → {"bids": [...], "asks": [...]}
_event_ts:   dict[str, deque] = {}      # token_id → deque[wall_ts] for ToB rate
_doge_price: float = 0.0               # latest Binance DOGE/USDT price
_doge_signal_ready: bool = False
_short_ema: float = 0.0
_long_ema:  float = 0.0

# WS token subscription (updated when candle rolls)
_ws_subscribed_tokens: set[str] = set()
_last_split_attempt_at: float = 0.0
_last_split_candle_slug: str | None = None


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class Candle:
    slug:       str
    up_token:   str
    down_token: str
    condition_id: str
    end_ts:     float   # unix timestamp


@dataclass
class LiveOrder:
    """A real Polymarket CLOB sell order."""
    order_id:    str          # CLOB order ID returned by API
    token_side:  str          # "Up" or "Down"
    token_id:    str
    ask_price:   float        # price we posted
    shares:      float        # number of shares
    posted_ts:   float
    candle_end:  float
    status:      str = "open"  # open | filled | cancelled | expired
    fill_ts:     float | None = None

    @property
    def usdc_received(self) -> float:
        return self.ask_price * self.shares

    @property
    def max_loss(self) -> float:
        return (1.0 - self.ask_price) * self.shares

    @property
    def age_s(self) -> float:
        return time.time() - self.posted_ts

    @property
    def secs_remaining(self) -> float:
        return max(0.0, self.candle_end - time.time())


@dataclass
class BotState:
    """Mutable strategy state for the current candle."""
    candle:          Candle | None = None
    candle_open_px:  float | None = None  # Binance DOGE price at candle start
    # active CLOB orders, keyed by token_side
    open_orders:     dict[str, LiveOrder] = field(default_factory=dict)
    # history of completed orders this session
    history:         list[LiveOrder] = field(default_factory=list)
    # metrics
    total_usdc_recv: float = 0.0
    fills:           int   = 0
    cancels:         int   = 0
    candles_seen:    int   = 0
    last_market_refresh: float = 0.0


_state = BotState()


# ── Signal helpers ────────────────────────────────────────────────────────────

def _update_binance_price(price: float) -> None:
    global _doge_price, _short_ema, _long_ema, _doge_signal_ready
    if not _doge_signal_ready:
        _short_ema = _long_ema = price
        _doge_signal_ready = True
    else:
        _short_ema = SHORT_ALPHA * price + (1 - SHORT_ALPHA) * _short_ema
        _long_ema  = LONG_ALPHA  * price + (1 - LONG_ALPHA)  * _long_ema
    _doge_price = price


def _momentum_direction() -> str:
    """BULLISH / BEARISH / NEUTRAL from dual-EMA crossover."""
    if not _doge_signal_ready or _long_ema == 0:
        return "NEUTRAL"
    diff = (_short_ema - _long_ema) / _long_ema
    if   diff >  MIN_SIGNAL_STR: return "BULLISH"
    elif diff < -MIN_SIGNAL_STR: return "BEARISH"
    return "NEUTRAL"


def _momentum_strength() -> float:
    if not _doge_signal_ready or _long_ema == 0:
        return 0.0
    return abs(_short_ema - _long_ema) / _long_ema * 100


def _order_book_imbalance(token_id: str) -> float:
    """
    OBI ∈ [-1, 1].  +1 = all bids, -1 = all asks.
    Measures buying vs selling pressure at the top of book.
    """
    book = _book_cache.get(token_id, {})
    bids = sorted(book.get("bids", []), key=lambda x: -float(x.get("price", 0)))
    asks = sorted(book.get("asks", []), key=lambda x:  float(x.get("price", 0)))
    bid_vol = sum(float(b.get("size", 0)) for b in bids[:DEPTH_LEVELS])
    ask_vol = sum(float(a.get("size", 0)) for a in asks[:DEPTH_LEVELS])
    total = bid_vol + ask_vol
    return (bid_vol - ask_vol) / total if total > 0 else 0.0


def _tob_rate(token_id: str) -> float:
    """Events per second at top-of-book (proxy for market activity)."""
    dq = _event_ts.get(token_id)
    return len(dq) / TOB_RATE_WINDOW if dq else 0.0


def _composite_signal(candle: Candle) -> tuple[str, float]:
    """
    Fuse momentum + OBI on the target token into a single sell recommendation.

    Returns (token_side_to_sell, confidence_0_to_1).

    BULLISH Binance → sell Down (Down resolves $0 when price rises → keep premium)
    BEARISH Binance → sell Up   (Up resolves $0 when price falls → keep premium)
    """
    direction = _momentum_direction()
    mom_str   = _momentum_strength()

    if direction == "NEUTRAL":
        return "NEUTRAL", 0.0

    # Which side do we want to sell?
    sell_side = "Down" if direction == "BULLISH" else "Up"
    sell_tok  = candle.down_token if sell_side == "Down" else candle.up_token

    # Order book imbalance on the target token
    obi = _order_book_imbalance(sell_tok)

    # OBI confirmation: positive OBI on Down → more buyers of Down → our sell finds a match
    # OBI confirmation: negative OBI on Up  → more sellers of Up → competition (weak signal)
    obi_confirms = (sell_side == "Down" and obi > OBI_MIN) or \
                   (sell_side == "Up"   and obi < -OBI_MIN)

    # Composite confidence: momentum normalised + OBI bonus
    confidence = min(1.0, mom_str / 0.05 * 0.7 + (0.3 if obi_confirms else 0.0))

    return sell_side, confidence


# ── Book helpers ──────────────────────────────────────────────────────────────

def _best_bid_ask(token_id: str) -> tuple[float | None, float | None]:
    book = _book_cache.get(token_id, {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
    best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None
    return best_bid, best_ask


def _vwap_ask(token_id: str, max_levels: int = DEPTH_LEVELS) -> float | None:
    """Volume-weighted average price of top ask levels (our fair reference)."""
    book = _book_cache.get(token_id, {})
    asks = sorted(book.get("asks", []), key=lambda x: float(x.get("price", 0)))[:max_levels]
    if not asks:
        return None
    total_vol = sum(float(a.get("size", 0)) for a in asks)
    if total_vol == 0:
        return None
    return sum(float(a["price"]) * float(a.get("size", 0)) for a in asks) / total_vol


def _apply_price_change(book: dict, changes: list[dict]) -> list[tuple[float, str]]:
    """Merge incremental level updates; return list of removed (price, side) pairs."""
    removed: list[tuple[float, str]] = []
    for ch in changes or []:
        try:
            p  = float(ch["price"])
            s  = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key    = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        existed = any(abs(float(lv.get("price", 0) or 0) - p) < 1e-9 for lv in levels)
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})
        elif existed:
            removed.append((p, key))
    return removed


# ── Market discovery ──────────────────────────────────────────────────────────

def _load_json_field(v: Any) -> list:
    if isinstance(v, str):
        try:
            r = json.loads(v)
            return r if isinstance(r, list) else []
        except Exception:
            return []
    return v if isinstance(v, list) else []


def _fetch_doge_candle() -> Candle | None:
    """Find the active DOGE 5m candle from Gamma API. Tries current bucket ±1."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300
    for delta in (0, -1, 1):
        slug = f"doge-updown-5m-{bucket + delta * 300}"
        try:
            data = requests.get(
                f"{GAMMA_API}?slug={slug}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            ).json()
            if not isinstance(data, list) or not data:
                continue
            mkt = (data[0].get("markets") or [None])[0]
            if not mkt or mkt.get("closed"):
                continue
            outcomes  = _load_json_field(mkt.get("outcomes"))
            token_ids = _load_json_field(mkt.get("clobTokenIds"))
            up_tok = down_tok = None
            for i, name in enumerate(outcomes):
                if i >= len(token_ids):
                    continue
                label = str(name).strip().lower()
                if   label == "up":   up_tok   = str(token_ids[i])
                elif label == "down": down_tok = str(token_ids[i])
            if up_tok and down_tok:
                candle_ts = int(slug.rsplit("-", 1)[-1])
                condition_id = str(mkt.get("conditionId") or mkt.get("condition_id") or "")
                return Candle(
                    slug=slug,
                    up_token=up_tok,
                    down_token=down_tok,
                    condition_id=condition_id,
                    end_ts=float(candle_ts + 300),
                )
        except Exception as exc:
            log.debug("Candle fetch error %s: %s", slug, exc)
    return None


# ── Execution engine ──────────────────────────────────────────────────────────

def _target_ask_price(
    token_id: str,
    secs_remaining: float,
    aggressive_from_start: bool = AGGRESSIVE_FROM_START_DEFAULT,
) -> float | None:
    """
    Two-phase execution:
      T > 20 s  →  best_ask − 1 tick  (undercut queue, front of line)
      T ≤ 20 s  →  best_bid           (cross to taker, guaranteed exit)
    """
    best_bid, best_ask = _best_bid_ask(token_id)

    if aggressive_from_start:
        if best_ask is None or best_ask <= 0:
            return None
        if best_bid is None or best_bid <= 0:
            return round(best_ask, 2)

        spread = max(0.0, float(best_ask - best_bid))
        maker_start = float(best_ask - TICK) if spread >= 2 * TICK else float(best_ask)

        # Dynamic urgency: gradually lean from maker-start toward bid as candle decays.
        urgency = (CANDLE_SECONDS - max(0.0, secs_remaining)) / max(1.0, CANDLE_SECONDS - TAKER_SECS)
        urgency = max(0.0, min(1.0, urgency))
        price = maker_start - urgency * max(0.0, maker_start - float(best_bid))
        price = max(float(best_bid), price)
        price = round(price, 2)
    elif secs_remaining <= TAKER_SECS:
        if best_bid is not None and best_bid > 0:
            price = round(best_bid, 2)
        elif best_ask is not None and best_ask > 0:
            price = round(best_ask, 2)
        else:
            return None
    else:
        if best_ask is None:
            return None
        price = round(best_ask - TICK, 2)
        if price <= 0:
            return None
    return price if price > 0 else None


def _needs_reprice(
    order: LiveOrder,
    secs_remaining: float,
    aggressive_from_start: bool = AGGRESSIVE_FROM_START_DEFAULT,
) -> bool:
    """
    Reprice when market ask drifts ≥ REPRICE_TICKS below our order (we've
    fallen behind the queue), or when taker phase opens and we're still above bid.
    """
    best_bid, best_ask = _best_bid_ask(order.token_id)
    desired = _target_ask_price(
        order.token_id,
        secs_remaining,
        aggressive_from_start=aggressive_from_start,
    )
    if desired is not None and abs(desired - order.ask_price) >= REPRICE_TICKS * TICK:
        return True
    if order.secs_remaining <= TAKER_SECS:
        if best_bid is not None and order.ask_price > best_bid:
            return True
    return False


# ── Strategy actions ──────────────────────────────────────────────────────────
def _place_order(
    clob,
    token_side: str,
    token_id: str,
    condition_id: str,
    price: float,
    budget_half: float,
    candle_end: float,
    bet: bool,
    split_mode: str = "relayer",
    split_cmd: str | None = None,
) -> LiveOrder | None:
    target_shares = round(budget_half / price, 4)
    if target_shares < 0.01:
        log.warning("Shares too small (%.4f) for %s - skip", target_shares, token_side)
        return None

    order_id = "DRY"
    if bet:
        held = _held_shares_for_token(token_id)
        shares = min(target_shares, round(held, 4))
        if shares < 0.01:
            log.warning(
                "Skipping sell %s: no inventory (held=%.4f target=%.4f)",
                token_side, held, target_shares,
            )
            return None
        try:
            resp = place_sell_order(clob, token_id, price, shares)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            log.info("PLACED sell %s  id=%s  price=%.4f  shares=%.4f  recv=$%.4f",
                     token_side, order_id, price, shares, price * shares)
        except Exception as exc:
            log.error("place_sell_order failed for %s: %s", token_side, exc)
            return None
    else:
        shares = target_shares
        log.info("[DRY] sell %s  price=%.4f  shares=%.4f  recv=$%.4f  max_loss=$%.4f",
                 token_side, price, shares, price * shares, (1 - price) * shares)

    return LiveOrder(
        order_id   = order_id,
        token_side = token_side,
        token_id   = token_id,
        ask_price  = price,
        shares     = shares,
        posted_ts  = time.time(),
        candle_end = candle_end,
    )


def _cancel_order_safe(clob, order: LiveOrder, bet: bool) -> None:
    if not bet or order.order_id in ("DRY", "?"):
        return
    try:
        cancel_order(clob, order.order_id)
        log.info("CANCELLED %s  id=%s", order.token_side, order.order_id)
    except Exception as exc:
        log.warning("Cancel failed for %s id=%s: %s", order.token_side, order.order_id, exc)


def _is_insufficient_balance_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "not enough balance / allowance" in msg or "balance is not enough" in msg


def _as_bytes32(hex_value: str) -> bytes:
    h = (hex_value or "").lower().replace("0x", "")
    if len(h) != 64:
        raise ValueError(f"Invalid bytes32 hex length: {hex_value}")
    return bytes.fromhex(h)


def _get_web3_and_ctf(rpc_url: str | None = None):
    from web3 import Web3

    ctf_abi = [
        {
            "inputs": [
                {"internalType": "address", "name": "collateralToken", "type": "address"},
                {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
                {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
                {"internalType": "uint256[]", "name": "partition", "type": "uint256[]"},
                {"internalType": "uint256", "name": "amount", "type": "uint256"},
            ],
            "name": "splitPosition",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }
    ]

    rpc_candidates = [rpc_url] if rpc_url else []
    rpc_candidates.extend(POLYGON_RPC_FALLBACKS)
    last_err = None
    for url in [u for u in rpc_candidates if u]:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
            _ = w3.eth.block_number
            ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=ctf_abi)
            return w3, ctf
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"No working Polygon RPC for split: {last_err}")


def _split_amount_units(amount_usdc: float) -> int:
    return max(1, int(math.ceil(max(0.0, amount_usdc) * 1_000_000)))


def _split_condition_positions_onchain(
    private_key: str,
    condition_id: str,
    amount_usdc: float,
    rpc_url: str | None = None,
) -> str | None:
    from web3 import Web3

    w3, ctf = _get_web3_and_ctf(rpc_url)
    account = w3.eth.account.from_key(private_key)
    try:
        tx = ctf.functions.splitPosition(
            Web3.to_checksum_address(USDC_E_ADDRESS),
            b"\x00" * 32,
            _as_bytes32(condition_id),
            [1, 2],
            _split_amount_units(amount_usdc),
        ).build_transaction(
            {
                "from": account.address,
                "nonce": w3.eth.get_transaction_count(account.address),
                "chainId": POLYGON_CHAIN_ID,
                "gas": 350000,
                "gasPrice": w3.eth.gas_price,
            }
        )
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        return tx_hash.hex()
    except Exception as exc:
        log.warning("onchain split failed for condition %s: %s", condition_id, exc)
        return None


def _split_condition_positions_relayer(
    private_key: str,
    condition_id: str,
    amount_usdc: float,
    relayer_url: str | None = None,
) -> str | None:
    relayer_base = relayer_url or os.getenv("POLYMARKET_RELAYER_URL") or "https://relayer-v2.polymarket.com"
    relayer_key = os.getenv("RELAYER_API_KEY")
    relayer_addr = os.getenv("RELAYER_API_KEY_ADDRESS")

    if relayer_key and relayer_addr:
        try:
            from py_builder_relayer_client.signer import Signer
            from py_builder_relayer_client.config import get_contract_config
            from py_builder_relayer_client.builder.safe import build_safe_transaction_request
            from py_builder_relayer_client.models import SafeTransaction, OperationType, SafeTransactionArgs, TransactionType
            from web3 import Web3

            headers = {
                "RELAYER_API_KEY": relayer_key,
                "RELAYER_API_KEY_ADDRESS": relayer_addr,
                "Content-Type": "application/json",
            }
            signer = Signer(private_key, POLYGON_CHAIN_ID)
            from_address = signer.address()
            nonce_resp = requests.get(
                f"{relayer_base}/nonce",
                params={"address": from_address, "type": TransactionType.SAFE.value},
                headers=headers,
                timeout=20,
            )
            if nonce_resp.status_code != 200:
                raise RuntimeError(f"nonce failed: {nonce_resp.status_code} {nonce_resp.text[:200]}")
            nonce_payload = nonce_resp.json() or {}
            nonce = nonce_payload.get("nonce")
            if nonce is None:
                raise RuntimeError(f"invalid nonce payload: {nonce_payload}")

            _, ctf = _get_web3_and_ctf(os.getenv("POLYGON_RPC_URL"))
            data = ctf.encode_abi(
                "splitPosition",
                args=[
                    Web3.to_checksum_address(USDC_E_ADDRESS),
                    b"\x00" * 32,
                    _as_bytes32(condition_id),
                    [1, 2],
                    _split_amount_units(amount_usdc),
                ],
            )
            tx = SafeTransaction(
                to=Web3.to_checksum_address(CTF_ADDRESS),
                operation=OperationType.Call,
                data=data,
                value="0",
            )
            req = build_safe_transaction_request(
                signer=signer,
                args=SafeTransactionArgs(
                    from_address=from_address,
                    nonce=nonce,
                    chain_id=POLYGON_CHAIN_ID,
                    transactions=[tx],
                ),
                config=get_contract_config(POLYGON_CHAIN_ID),
                metadata="split positions",
            ).to_dict()
            submit_resp = requests.post(f"{relayer_base}/submit", headers=headers, json=req, timeout=20)
            if submit_resp.status_code != 200:
                raise RuntimeError(f"submit failed: {submit_resp.status_code} {submit_resp.text[:300]}")
            body = submit_resp.json() or {}
            tx_hash = body.get("transactionHash")
            tx_id = body.get("transactionID")
            if tx_hash:
                return tx_hash
            if tx_id:
                for _ in range(30):
                    q = requests.get(f"{relayer_base}/transaction", params={"id": tx_id}, headers=headers, timeout=20)
                    if q.status_code != 200:
                        break
                    arr = q.json() if q.text else []
                    if isinstance(arr, list) and arr:
                        state = arr[0].get("state")
                        txh = arr[0].get("transactionHash")
                        if txh:
                            return txh
                        if state in ("STATE_FAILED", "STATE_INVALID"):
                            break
            return None
        except Exception as exc:
            log.warning("relayer split (api-key) failed for condition %s: %s", condition_id, exc)

    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
        from web3 import Web3
    except Exception as exc:
        log.warning("relayer SDK unavailable for split: %s", exc)
        return None

    key = os.getenv("POLY_BUILDER_API_KEY")
    secret = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not key or not secret or not passphrase:
        log.warning("Missing builder creds for relayer split (POLY_BUILDER_API_KEY/SECRET/PASSPHRASE)")
        return None

    try:
        builder_cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))
        client = RelayClient(relayer_base, POLYGON_CHAIN_ID, private_key=private_key, builder_config=builder_cfg)
        _, ctf = _get_web3_and_ctf(os.getenv("POLYGON_RPC_URL"))
        data = ctf.encode_abi(
            "splitPosition",
            args=[
                Web3.to_checksum_address(USDC_E_ADDRESS),
                b"\x00" * 32,
                _as_bytes32(condition_id),
                [1, 2],
                _split_amount_units(amount_usdc),
            ],
        )
        tx = SafeTransaction(
            to=Web3.to_checksum_address(CTF_ADDRESS),
            operation=OperationType.Call,
            data=data,
            value="0",
        )
        resp = client.execute([tx], "split positions")
        waited = resp.wait()
        tx_hash = waited.get("transactionHash") if isinstance(waited, dict) else None
        if not tx_hash:
            tx_hash = getattr(resp, "transaction_hash", None)
        return tx_hash
    except Exception as exc:
        log.warning("relayer split failed for condition %s: %s", condition_id, exc)
        return None


def _run_split_cmd(split_cmd: str | None, min_interval_seconds: int = 2) -> bool:
    """
    Optional hook to run an external split command (e.g., replayer script)
    that mints/splits collateral into market outcome tokens.
    """
    global _last_split_attempt_at
    if not split_cmd:
        return False
    now = time.time()
    if now - _last_split_attempt_at < min_interval_seconds:
        return False
    _last_split_attempt_at = now
    try:
        proc = subprocess.run(split_cmd, shell=True, capture_output=True, text=True, timeout=90)
        if proc.returncode == 0:
            _log_inventory_snapshot()
            log.info("Split command succeeded")
            return True
        err = (proc.stderr or proc.stdout or "").strip()
        log.warning("Split command failed (code=%s): %s", proc.returncode, err[:300])
    except Exception as split_exc:
        log.warning("Split command error: %s", split_exc)
    return False


def _held_shares_for_token(token_id: str) -> float:
    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        return 0.0
    try:
        positions = get_positions(funder)
        for p in positions:
            if str(p.get("token_id")) == str(token_id):
                return float(p.get("size", 0) or 0.0)
    except Exception:
        pass
    return 0.0


def _split_condition_positions(
    condition_id: str,
    amount_usdc: float,
    split_mode: str,
    split_cmd: str | None = None,
) -> bool:
    if amount_usdc <= 0:
        return True
    if not condition_id:
        log.warning("Cannot split: missing condition_id")
        return False

    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    tx_hash = None
    if split_mode != "none" and private_key:
        if split_mode == "relayer":
            tx_hash = _split_condition_positions_relayer(private_key, condition_id, amount_usdc)
        elif split_mode == "onchain":
            tx_hash = _split_condition_positions_onchain(
                private_key,
                condition_id,
                amount_usdc,
                rpc_url=os.getenv("POLYGON_RPC_URL"),
            )
    if tx_hash:
        log.info("Split submitted: amount=$%.4f condition=%s tx=%s", amount_usdc, condition_id[:10], tx_hash)
        _log_inventory_snapshot()
        return True
    if split_cmd:
        return _run_split_cmd(split_cmd)
    return False


def _ensure_inventory_for_sell(
    token_id: str,
    target_shares: float,
    condition_id: str,
    split_mode: str,
    split_cmd: str | None = None,
) -> bool:
    held = _held_shares_for_token(token_id)
    if held + 0.01 >= target_shares:
        return True
    shortfall = max(0.0, target_shares - held)
    if not _split_condition_positions(condition_id, shortfall, split_mode, split_cmd=split_cmd):
        return False
    for _ in range(SPLIT_INVENTORY_SYNC_ATTEMPTS):
        time.sleep(SPLIT_INVENTORY_SYNC_DELAY_SECONDS)
        held = _held_shares_for_token(token_id)
        if held + 0.01 >= target_shares:
            return True
    log.warning("Inventory still short for token %s: held=%.4f target=%.4f", token_id, held, target_shares)
    return held + 0.01 >= target_shares


def _split_for_candle(
    candle: Candle,
    budget: float,
    split_mode: str,
    split_cmd: str | None = None,
) -> bool:
    """
    Run split once per candle to seed inventory before sells.
    """
    global _last_split_candle_slug
    if _last_split_candle_slug == candle.slug:
        return True
    ok = _split_condition_positions(candle.condition_id, max(0.0, budget), split_mode, split_cmd=split_cmd)
    if ok:
        _last_split_candle_slug = candle.slug
    return ok


def _log_inventory_snapshot() -> None:
    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        return
    try:
        positions = get_positions(funder)
        total = sum(float(p.get("size", 0) or 0) for p in positions)
        log.info("Inventory snapshot: %d token positions, total shares=%.4f", len(positions), total)
    except Exception:
        pass


def _expire_open_orders(clob, bet: bool) -> None:
    """Cancel and archive all open orders (called at candle close)."""
    for side, order in list(_state.open_orders.items()):
        _cancel_order_safe(clob, order, bet)
        order.status = "expired"
        _state.history.append(order)
        _state.cancels += 1
        log.info("EXPIRED %s  age=%.1fs  unfilled", side, order.age_s)
    _state.open_orders.clear()


def _check_filled_via_book() -> None:
    """
    Heuristic fill detection: if best_bid ≥ our ask, the order was crossed.
    (Real fill confirmation comes from CLOB polling or WS trade events.)
    """
    for side, order in list(_state.open_orders.items()):
        best_bid, _ = _best_bid_ask(order.token_id)
        if best_bid is not None and best_bid >= order.ask_price:
            order.status  = "filled"
            order.fill_ts = time.time()
            _state.fills += 1
            _state.total_usdc_recv += order.usdc_received
            _state.history.append(order)
            del _state.open_orders[side]
            log.info("FILLED (bid-overtook) %s @ $%.4f  recv=$%.4f",
                     side, order.ask_price, order.usdc_received)


# ── Main strategy loop ────────────────────────────────────────────────────────

async def _strategy_loop(
    clob,
    budget: float,
    bet: bool,
    split_mode: str = "relayer",
    aggressive_from_start: bool = AGGRESSIVE_FROM_START_DEFAULT,
    split_cmd: str | None = None,
) -> None:
    budget_half = budget / 2.0

    while True:
        now = time.time()

        # ── Refresh candle ─────────────────────────────────────────────────
        need_refresh = (
            _state.candle is None
            or now >= _state.candle.end_ts
            or now - _state.last_market_refresh > MARKET_REFRESH
        )
        if need_refresh:
            new_candle = await asyncio.to_thread(_fetch_doge_candle)
            _state.last_market_refresh = now

            if new_candle is None:
                log.warning("No active DOGE 5m candle found — retrying…")
                await asyncio.sleep(STRATEGY_TICK)
                continue

            if _state.candle is None or new_candle.slug != _state.candle.slug:
                # Candle rolled — expire old orders, start fresh
                if _state.open_orders:
                    _expire_open_orders(clob, bet)

                _state.candle         = new_candle
                _state.candle_open_px = _doge_price or None
                _state.candles_seen  += 1

                # Subscribe new tokens on the shared WS (handled by poly_ws reconnect
                # or dynamic subscription — we update the shared set here)
                _ws_subscribed_tokens.update({new_candle.up_token, new_candle.down_token})

                log.info("New candle: %s  ends=%s  open_px=%.6f",
                         new_candle.slug,
                         datetime.fromtimestamp(new_candle.end_ts, tz=timezone.utc)
                                 .strftime("%H:%M:%S UTC"),
                         _state.candle_open_px or 0)
                if bet:
                    _split_for_candle(new_candle, budget, split_mode, split_cmd=split_cmd)

        candle = _state.candle
        secs   = max(0.0, candle.end_ts - now)

        # ── Candle expiry ──────────────────────────────────────────────────
        if secs == 0:
            _expire_open_orders(clob, bet)
            await asyncio.sleep(STRATEGY_TICK)
            continue

        # ── Fill detection ─────────────────────────────────────────────────
        _check_filled_via_book()

        # ── Reprice existing orders ────────────────────────────────────────
        for side, order in list(_state.open_orders.items()):
            if not _needs_reprice(order, secs, aggressive_from_start=aggressive_from_start):
                continue
            token_id = order.token_id
            new_price = _target_ask_price(
                token_id,
                secs,
                aggressive_from_start=aggressive_from_start,
            )
            if new_price is None or abs(new_price - order.ask_price) < 1e-9:
                continue
            # Cancel old, place new
            _cancel_order_safe(clob, order, bet)
            order.status = "cancelled"
            _state.history.append(order)
            _state.cancels += 1
            del _state.open_orders[side]

            new_order = _place_order(
                clob, side, token_id, candle.condition_id, new_price, budget_half, candle.end_ts, bet,
                split_mode=split_mode, split_cmd=split_cmd
            )
            if new_order:
                _state.open_orders[side] = new_order

        # ── Open new orders for missing sides ─────────────────────────────
        for side in ("Up", "Down"):
            if side in _state.open_orders:
                continue  # already posted

            # Already filled this side this candle?
            already_filled = any(
                o.token_side == side and o.status == "filled"
                for o in _state.history
                if o.candle_end == candle.end_ts
            )
            if already_filled:
                continue

            token_id = candle.up_token if side == "Up" else candle.down_token

            # Composite signal: prefer momentum-favoured side
            sell_side, confidence = _composite_signal(candle)

            # Skip the non-favoured side unless signal is NEUTRAL
            if sell_side != "NEUTRAL" and sell_side != side and confidence > 0.5:
                continue

            price = _target_ask_price(
                token_id,
                secs,
                aggressive_from_start=aggressive_from_start,
            )
            if price is None:
                continue

            new_order = _place_order(
                clob, side, token_id, candle.condition_id, price, budget_half, candle.end_ts, bet,
                split_mode=split_mode, split_cmd=split_cmd
            )
            if new_order:
                _state.open_orders[side] = new_order

        await asyncio.sleep(STRATEGY_TICK)


# ── WebSocket coroutines ──────────────────────────────────────────────────────

async def _binance_ws() -> None:
    """Stream DOGE/USDT aggTrades from Binance; update EMA signal."""
    while True:
        try:
            async with websockets.connect(
                WS_BINANCE, ping_interval=WS_PING, max_size=2 ** 20
            ) as ws:
                log.info("Binance WS connected (dogeusdt@aggTrade)")
                async for raw in ws:
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        if data.get("e") != "aggTrade":
                            continue
                        _update_binance_price(float(data["p"]))
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Binance WS error: %s — retry in %ds", exc, WS_RECONNECT)
            await asyncio.sleep(WS_RECONNECT)


async def _poly_ws() -> None:
    """
    Stream the Polymarket CLOB order book for the active DOGE candle tokens.
    Re-subscribes whenever _ws_subscribed_tokens changes.
    """
    while True:
        tokens = list(_ws_subscribed_tokens)
        if not tokens:
            await asyncio.sleep(1)
            continue

        sub = json.dumps({
            "auth":       {},
            "type":       "market",
            "assets_ids": tokens,
        })

        try:
            async with websockets.connect(
                WS_POLY,
                ping_interval=WS_PING,
                ping_timeout=WS_PING,
                max_size=2 ** 22,
            ) as ws:
                await ws.send(sub)
                log.info("Poly WS connected — %d tokens", len(tokens))

                async for raw in ws:
                    # Re-subscribe if the candle rolled and new tokens appeared
                    current = list(_ws_subscribed_tokens)
                    if set(current) != set(tokens):
                        log.info("Token set changed — reconnecting Poly WS")
                        break  # exit inner loop → reconnect with new tokens

                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue

                    events = payload if isinstance(payload, list) else [payload]
                    now = time.time()
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        et  = ev.get("event_type")
                        tid = str(ev.get("asset_id") or "")
                        if not tid:
                            continue
                        if et == "book":
                            _book_cache[tid] = {
                                "bids": list(ev.get("bids") or []),
                                "asks": list(ev.get("asks") or []),
                            }
                        elif et == "price_change":
                            book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
                            _apply_price_change(book, ev.get("changes") or [])
                            dq = _event_ts.setdefault(tid, deque())
                            dq.append(now)
                            while dq and dq[0] < now - TOB_RATE_WINDOW:
                                dq.popleft()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Poly WS error: %s — retry in %ds", exc, WS_RECONNECT)
            await asyncio.sleep(WS_RECONNECT)


# ── Terminal display ──────────────────────────────────────────────────────────

CLEAR   = "\033[2J\033[H"
HIDE_C  = "\033[?25l"
SHOW_C  = "\033[?25h"
BOLD    = "\033[1m"
RESET   = "\033[0m"
GREEN   = "\033[92m"
RED     = "\033[91m"
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
DIM     = "\033[2m"
MAGENTA = "\033[95m"
SEP     = "  " + "─" * 100


def _sig_col(d: str) -> str:
    return GREEN if d == "BULLISH" else (RED if d == "BEARISH" else DIM)


def _pnl_col(v: float) -> str:
    return GREEN if v >= 0 else RED


def _fmt_order(order: LiveOrder) -> str:
    secs = int(order.secs_remaining)
    if order.status == "open":
        st = f"{YELLOW}OPEN  {order.age_s:4.0f}s age  {secs:3d}s left{RESET}"
    elif order.status == "filled":
        st = f"{GREEN}FILLED{RESET}"
    elif order.status == "cancelled":
        st = f"{DIM}CANCELLED{RESET}"
    else:
        st = f"{DIM}{order.status.upper()}{RESET}"
    return (
        f"    #{order.order_id[:8]}  sell {CYAN}{order.token_side:4s}{RESET}"
        f"  @${order.ask_price:.4f}  shrs={order.shares:.3f}"
        f"  recv={GREEN}${order.usdc_received:.4f}{RESET}"
        f"  maxloss={RED}${order.max_loss:.4f}{RESET}  {st}"
    )


def _render(budget: float, bet: bool) -> str:
    now    = time.time()
    ts     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    candle = _state.candle
    mode   = f"{GREEN}LIVE{RESET}" if bet else f"{YELLOW}DRY-RUN{RESET}"

    lines  = ["", f"  {BOLD}[{ts}]  DOGE 5m Sell Bot — AUWE  [{mode}{BOLD}]{RESET}", SEP]

    # Binance signal row
    d   = _momentum_direction()
    sc  = _sig_col(d)
    str_pct = _momentum_strength()
    obi_up   = _order_book_imbalance(candle.up_token   if candle else "") if candle else 0.0
    obi_down = _order_book_imbalance(candle.down_token if candle else "") if candle else 0.0

    lines.append(
        f"  Binance DOGE: {CYAN}${_doge_price:.6f}{RESET}"
        f"   Signal: {sc}{BOLD}{d}{RESET}  ({sc}{str_pct:.4f}%{RESET})"
        f"   OBI Up={obi_up:+.3f}  OBI Down={obi_down:+.3f}"
    )

    if candle:
        secs = max(0, int(candle.end_ts - now))
        phase = (
            "undercut" if secs > TAKER_SECS else
            f"{RED}taker{RESET}"
        )
        up_bid, up_ask   = _best_bid_ask(candle.up_token)
        dn_bid, dn_ask   = _best_bid_ask(candle.down_token)
        up_obi = _order_book_imbalance(candle.up_token)
        dn_obi = _order_book_imbalance(candle.down_token)

        def _p(v: float | None) -> str:
            return f"${v:.4f}" if v is not None else "   n/a"

        lines.append(
            f"  Candle: {candle.slug}  {BOLD}{secs:3d}s left{RESET}  [{phase}]"
        )
        lines.append(
            f"    Up   bid={CYAN}{_p(up_bid)}{RESET}  ask={CYAN}{_p(up_ask)}{RESET}"
            f"  OBI={up_obi:+.3f}"
            f"    Down bid={CYAN}{_p(dn_bid)}{RESET}  ask={CYAN}{_p(dn_ask)}{RESET}"
            f"  OBI={dn_obi:+.3f}"
        )
    else:
        lines.append(f"  {DIM}Waiting for active DOGE candle…{RESET}")

    lines.append(SEP)

    # Open orders
    if _state.open_orders:
        lines.append(f"  {YELLOW}Active orders:{RESET}")
        for order in _state.open_orders.values():
            lines.append(_fmt_order(order))
    else:
        lines.append(f"  {DIM}No active orders{RESET}")

    # Recent history (last 8)
    recent = list(reversed(_state.history))[:8]
    if recent:
        lines.append(f"  {DIM}Recent history:{RESET}")
        for order in recent:
            lines.append(_fmt_order(order))

    lines.append(SEP)

    lines.append(
        f"  {BOLD}Session:{RESET}"
        f"  candles={_state.candles_seen}"
        f"  fills={GREEN}{_state.fills}{RESET}"
        f"  cancels={DIM}{_state.cancels}{RESET}"
        f"  recv={GREEN}${_state.total_usdc_recv:.4f}{RESET}"
        f"   budget=${budget:.2f}"
    )
    lines.append(f"  {DIM}Press Ctrl+C to stop{RESET}")
    return "\n".join(lines)


async def _display_loop(budget: float, bet: bool) -> None:
    sys.stdout.write(HIDE_C)
    sys.stdout.flush()
    try:
        while True:
            sys.stdout.write(CLEAR)
            sys.stdout.write(_render(budget, bet) + "\n")
            sys.stdout.flush()
            await asyncio.sleep(DISPLAY_TICK)
    finally:
        sys.stdout.write(SHOW_C)
        sys.stdout.flush()


# ── Client setup ──────────────────────────────────────────────────────────────

def _build_clob_client():
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    if not os.getenv("CLOB_API_KEY"):
        log.info("Deriving CLOB API credentials…")
        creds = derive_api_credentials(
            host           = os.environ["POLYMARKET_HOST"],
            private_key    = os.environ["POLYMARKET_PRIVATE_KEY"],
            funder         = os.getenv("POLYMARKET_FUNDER") or None,
            signature_type = int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
        )
        os.environ["CLOB_API_KEY"] = creds.api_key
        os.environ["CLOB_SECRET"]  = creds.api_secret
        os.environ["CLOB_PASS"]    = creds.api_passphrase

    return build_client(
        host           = os.environ["POLYMARKET_HOST"],
        private_key    = os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key        = os.environ["CLOB_API_KEY"],
        api_secret     = os.environ["CLOB_SECRET"],
        api_passphrase = os.environ["CLOB_PASS"],
        funder         = os.getenv("POLYMARKET_FUNDER") or None,
        signature_type = int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

async def run(
    budget: float,
    max_balance: float,
    bet: bool,
    split_mode: str = "relayer",
    aggressive_from_start: bool = AGGRESSIVE_FROM_START_DEFAULT,
    split_cmd: str | None = None,
) -> None:
    _init_logging()

    clob = None
    if bet:
        clob = _build_clob_client()
        balance = get_usdc_balance(clob)
        log.info("USDC balance: $%.2f", balance)
        if balance < budget:
            log.error("Insufficient balance $%.2f < budget $%.2f", balance, budget)
            sys.exit(1)
        if balance > max_balance:
            log.warning(
                "Balance $%.2f exceeds --max-balance $%.2f — continuing but be aware",
                balance, max_balance,
            )

    log.info(
        "DOGE Seller starting  budget=$%.2f  mode=%s",
        budget, "LIVE" if bet else "DRY-RUN",
    )

    # Pre-warm: discover first candle so WS has tokens immediately
    first_candle = await asyncio.to_thread(_fetch_doge_candle)
    if first_candle is None:
        log.error("No active DOGE 5m candle — cannot start")
        sys.exit(1)

    _state.candle         = first_candle
    _state.candle_open_px = _doge_price or None
    _state.candles_seen   = 1
    _ws_subscribed_tokens.update({first_candle.up_token, first_candle.down_token})
    log.info("First candle: %s", first_candle.slug)
    if bet:
        _split_for_candle(first_candle, budget, split_mode, split_cmd=split_cmd)

    await asyncio.gather(
        _binance_ws(),
        _poly_ws(),
        _strategy_loop(
            clob,
            budget,
            bet,
            split_mode=split_mode,
            aggressive_from_start=aggressive_from_start,
            split_cmd=split_cmd,
        ),
        _display_loop(budget, bet),
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="DOGE 5m sell-side bot — Adaptive Urgency-Weighted Execution"
    )
    p.add_argument("--bet", action="store_true",
                   help="Place real CLOB orders (default: dry-run)")
    p.add_argument("--budget", type=float, default=DEFAULT_BUDGET,
                   help=f"USDC per candle, split $budget/2 per side (default: {DEFAULT_BUDGET})")
    p.add_argument("--max-balance", type=float, default=DEFAULT_MAX_BALANCE,
                   help=f"Safety cap: warn if USDC balance exceeds this (default: {DEFAULT_MAX_BALANCE})")
    p.add_argument(
        "--split-mode",
        choices=["relayer", "onchain", "none"],
        default="relayer",
        help="How to split USDC.e into outcome tokens in live mode (default: relayer)",
    )
    p.add_argument(
        "--aggressive-from-start",
        action="store_true",
        default=AGGRESSIVE_FROM_START_DEFAULT,
        help="Price at best bid from candle start (default: enabled)",
    )
    p.add_argument(
        "--no-aggressive-from-start",
        dest="aggressive_from_start",
        action="store_false",
        help="Disable start-of-candle bid crossing and use legacy two-phase pricing",
    )
    p.add_argument(
        "--split-cmd",
        type=str,
        default=None,
        help="Optional fallback external split command (used only if native split path fails)",
    )
    args = p.parse_args()

    try:
        asyncio.run(run(
            budget      = args.budget,
            max_balance = args.max_balance,
            bet         = args.bet,
            split_mode  = args.split_mode,
            aggressive_from_start = args.aggressive_from_start,
            split_cmd   = args.split_cmd,
        ))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()

