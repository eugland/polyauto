"""
automata/btc_seller.py — BTC 5m dual-side sell-side bot for Polymarket.

Strategy: Pre-Candle Pair-Sum Maker → Adjust → Hail Mary
---------------------------------------------------------
Find NEXT un-started BTC 5m candle, split $10 USDC → 10 Up + 10 Down,
post GTC sells before the candle starts. Three phases keyed off candle
start time:

  PRE_CANDLE (now < candle.start_ts):
     Joint pair-sum maker. Try undercut both asks by 1 tick; else undercut
     only the more expensive side; else match both asks. Each candidate
     must have (up_price + down_price) ≥ MIN_PAIR_SUM ($1.01). Else hold.

  ADJUST (0 ≤ secs_since_start < ADJUST_GRACE, default 20s):
     Candle has started. Drop the pair-sum floor. Linearly interpolate
     each side's price from (best_ask − 1 tick) toward best_bid as
     urgency rises 0 → 1 over the wind-down window.

  HAIL_MARY (secs_since_start ≥ ADJUST_GRACE):
     Cross to bid on each side at any price. Get out before resolution.

Profit targets (PRE_CANDLE phase) — skewed markets are fine:
  0.51 + 0.51 → $10.20 on $10 cost = 2¢ net
  0.50 + 0.51 → $10.10 = 1¢ net   ✓
  0.48 + 0.53 → $10.10 = 1¢ net   ✓
  0.47 + 0.53 → $10.00 = 0¢       ✗ (rejected, sum < $1.01)

Usage:
    python -m automata.btc_seller                       # dry-run
    python -m automata.btc_seller --bet                 # live, $10 budget
    python -m automata.btc_seller --bet --budget 20
    python -m automata.btc_seller --bet --split-mode onchain
    python -m automata.btc_seller --min-pair-sum 1.02   # tighter (2¢) floor
    python -m automata.btc_seller --adjust-grace 10     # tighter (10s) wind-down
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

# ── Logging ───────────────────────────────────────────────────────────────────

log = logging.getLogger("automata.btc_seller")

LOG_PATH = Path("experiment") / "logs" / "btc_seller.log"


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

GAMMA_API    = "https://gamma-api.polymarket.com/events"
WS_POLY      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

WS_RECONNECT = 3    # seconds between reconnect attempts
WS_PING      = 20   # WebSocket keepalive interval

# Price discipline — constraint is on the PAIR SUM, not per-side.
# Cost per (Up + Down) pair = $1.00 from split. Min sum = $1.01 → 1¢ profit.
# Skewed markets are fine (e.g. 0.48 + 0.53 = $1.01 ✓).
MIN_PAIR_SUM = 1.02   # minimum (up_price + down_price) in PRE-CANDLE phase
TICK         = 0.01   # Polymarket price tick
FLOOR_DROP_AFTER_START = 20.0  # keep pair-sum floor for first N seconds after start

# Candle-relative phases (now − candle.start_ts):
#   < 0           → PRE_CANDLE  : maker, joint pair-sum ≥ MIN_PAIR_SUM
#   0 .. ADJUST   → ADJUST      : drop floor, lean toward bid (interp ask→bid)
#   > ADJUST      → HAIL_MARY   : cross to bid each tick (urgent exit)
ADJUST_GRACE  = 20.0  # seconds after candle start to wind down toward bid
REPRICE_TICKS = 1     # reprice when market ask drops ≥ this many ticks below ours
STRATEGY_TICK = 3.0   # main loop cadence (seconds)
DISPLAY_TICK  = 1.0   # terminal refresh (seconds)
MARKET_REFRESH = 30.0  # how often to rediscover the active candle (seconds)
CANDLE_SECONDS = 300.0

DEFAULT_BUDGET      = 10.0   # $10 USDC per candle → 10 Up + 10 Down tokens
DEFAULT_MAX_BALANCE = 100.0
CTF_ADDRESS         = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E_ADDRESS      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID    = 137
POLYGON_RPC_FALLBACKS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]
SPLIT_INVENTORY_SYNC_ATTEMPTS      = 5
SPLIT_INVENTORY_SYNC_DELAY_SECONDS = 0.8


# ── Shared streaming state ────────────────────────────────────────────────────

_book_cache: dict[str, dict] = {}    # token_id → {"bids": [...], "asks": [...]}
_ws_subscribed_tokens: set[str] = set()
_last_split_attempt_at: float = 0.0
_last_split_candle_slug: str | None = None


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class Candle:
    slug:         str
    up_token:     str
    down_token:   str
    condition_id: str
    start_ts:     float   # unix timestamp — candle opens / market starts pricing
    end_ts:       float   # unix timestamp — candle resolves


@dataclass
class LiveOrder:
    """A real (or simulated) Polymarket CLOB sell order."""
    order_id:   str
    token_side: str     # "Up" or "Down"
    token_id:   str
    ask_price:  float
    shares:     float
    posted_ts:  float
    candle_end: float
    status:     str = "open"   # open | filled | cancelled | expired
    fill_ts:    float | None = None

    @property
    def usdc_received(self) -> float:
        return self.ask_price * self.shares

    @property
    def age_s(self) -> float:
        return time.time() - self.posted_ts

    @property
    def secs_remaining(self) -> float:
        return max(0.0, self.candle_end - time.time())


@dataclass
class BotState:
    candle:          Candle | None = None
    open_orders:     dict[str, LiveOrder] = field(default_factory=dict)
    history:         list[LiveOrder] = field(default_factory=list)
    total_usdc_recv: float = 0.0
    fills:           int   = 0
    cancels:         int   = 0
    candles_seen:    int   = 0
    last_market_refresh: float = 0.0


_state = BotState()


# ── Book helpers ──────────────────────────────────────────────────────────────

def _best_bid_ask(token_id: str) -> tuple[float | None, float | None]:
    book = _book_cache.get(token_id, {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
    best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None
    return best_bid, best_ask


def _apply_price_change(book: dict, changes: list[dict]) -> None:
    for ch in changes or []:
        try:
            p  = float(ch["price"])
            s  = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key    = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


# ── Market discovery ──────────────────────────────────────────────────────────

def _load_json_field(v: Any) -> list:
    if isinstance(v, str):
        try:
            r = json.loads(v)
            return r if isinstance(r, list) else []
        except Exception:
            return []
    return v if isinstance(v, list) else []


def _fetch_btc_candle() -> Candle | None:
    """
    Find an BTC 5m candle from Gamma API.
    Strongly prefers the NEXT un-started candle (start_ts > now) so we have
    time to split + post sells before the candle starts. Falls back to the
    currently-running candle only if no upcoming one is listed yet.
    """
    now_ts = int(datetime.now(timezone.utc).timestamp())
    bucket = (now_ts // 300) * 300
    best: Candle | None = None

    request_errors = 0

    for delta in (0, 1, 2, 3, -1):
        slug = f"btc-updown-5m-{bucket + delta * 300}"
        try:
            data = requests.get(
                f"{GAMMA_API}?slug={slug}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            ).json()
            if not isinstance(data, list) or not data:
                continue
            markets = data[0].get("markets") or []
            if not isinstance(markets, list):
                continue

            up_tok = down_tok = condition_id = None
            for mkt in markets:
                if not isinstance(mkt, dict) or mkt.get("closed"):
                    continue
                outcomes  = _load_json_field(mkt.get("outcomes"))
                token_ids = _load_json_field(mkt.get("clobTokenIds"))
                cand_up = cand_down = None
                for i, name in enumerate(outcomes):
                    if i >= len(token_ids):
                        continue
                    label = str(name).strip().lower()
                    if   label == "up":   cand_up   = str(token_ids[i])
                    elif label == "down": cand_down = str(token_ids[i])
                cand_condition_id = str(mkt.get("conditionId") or mkt.get("condition_id") or "")
                if cand_up and cand_down and cand_condition_id:
                    up_tok = cand_up
                    down_tok = cand_down
                    condition_id = cand_condition_id
                    break

            if not up_tok or not down_tok or not condition_id:
                continue
            candle_ts = int(slug.rsplit("-", 1)[-1])
            start_ts  = float(candle_ts)
            end_ts    = float(candle_ts + 300)
            # Skip already-resolved candles
            if end_ts <= now_ts:
                continue
            # Don't reach too far into the future
            if start_ts - now_ts > 1500:
                continue

            candle = Candle(
                slug=slug,
                up_token=up_tok,
                down_token=down_tok,
                condition_id=condition_id,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            # Preference: un-started > running; among same status, earlier start_ts wins
            if best is None:
                best = candle
                continue
            cand_unstarted = candle.start_ts > now_ts
            best_unstarted = best.start_ts > now_ts
            if cand_unstarted and not best_unstarted:
                best = candle
            elif cand_unstarted == best_unstarted and candle.start_ts < best.start_ts:
                best = candle
        except Exception as exc:
            request_errors += 1
            log.debug("Candle fetch error %s: %s", slug, exc)

    if best is None and request_errors:
        log.warning("Gamma fetch failed for all BTC candle slugs (%d request errors)", request_errors)

    return best


# ── Execution engine ──────────────────────────────────────────────────────────

def _phase(candle: Candle, now: float) -> str:
    """PRE_CANDLE | ADJUST | HAIL_MARY based on now vs. candle.start_ts."""
    secs_since_start = now - candle.start_ts
    if secs_since_start < 0:
        return "PRE_CANDLE"
    if secs_since_start < ADJUST_GRACE:
        return "ADJUST"
    return "HAIL_MARY"


def _adjust_price(bid: float | None, ask: float | None, urgency: float) -> float | None:
    """
    Linear interpolation from (best_ask − 1 tick) → best_bid as urgency rises 0 → 1.
    urgency=0 → maker-style top of ask;  urgency=1 → cross to bid.
    """
    if bid and bid > 0 and (not ask or ask <= 0):
        return round(bid, 2)
    if ask and ask > 0 and (not bid or bid <= 0):
        return round(max(TICK, ask - TICK), 2)
    if not bid or not ask or bid <= 0 or ask <= 0:
        return None
    spread_top = max(bid, ask - TICK)
    price = spread_top - urgency * (spread_top - bid)
    return round(max(TICK, price), 2)


def _target_pair_prices(
    candle: Candle, now: float
) -> tuple[float | None, float | None]:
    """
    Compute joint (up_price, down_price) for the current phase.

    PRE_CANDLE (now < candle.start_ts):
      Maker — try undercut both, then undercut higher side, then match asks.
      Each candidate must have sum ≥ MIN_PAIR_SUM. Else hold (None, None).

    ADJUST (0 ≤ secs_since_start < ADJUST_GRACE):
      Linearly interpolate price from (best_ask − 1 tick) toward best_bid as
      urgency 0 → 1. Keep pair-sum floor active for the first
      FLOOR_DROP_AFTER_START seconds after candle start.

    HAIL_MARY (secs_since_start ≥ ADJUST_GRACE):
      Cross to bid on each side at any price (urgent exit).
    """
    up_bid, up_ask = _best_bid_ask(candle.up_token)
    dn_bid, dn_ask = _best_bid_ask(candle.down_token)
    phase = _phase(candle, now)

    if phase == "HAIL_MARY":
        up_p = round(up_bid, 2) if up_bid and up_bid > 0 else (
               round(up_ask, 2) if up_ask and up_ask > 0 else None)
        dn_p = round(dn_bid, 2) if dn_bid and dn_bid > 0 else (
               round(dn_ask, 2) if dn_ask and dn_ask > 0 else None)
        return up_p, dn_p

    if phase == "ADJUST":
        secs_since_start = max(0.0, now - candle.start_ts)
        urgency = (now - candle.start_ts) / max(1.0, ADJUST_GRACE)
        urgency = max(0.0, min(1.0, urgency))
        up_p = _adjust_price(up_bid, up_ask, urgency)
        dn_p = _adjust_price(dn_bid, dn_ask, urgency)
        if (
            secs_since_start < FLOOR_DROP_AFTER_START
            and up_p is not None
            and dn_p is not None
            and (up_p + dn_p + 1e-9) < MIN_PAIR_SUM
        ):
            return None, None
        return up_p, dn_p

    # PRE_CANDLE — pair-sum maker
    if not up_ask or up_ask <= 0 or not dn_ask or dn_ask <= 0:
        return None, None

    candidates = [
        (round(up_ask - TICK, 2), round(dn_ask - TICK, 2)),  # both undercut
    ]
    if up_ask >= dn_ask:
        candidates.append((round(up_ask - TICK, 2), round(dn_ask, 2)))
    else:
        candidates.append((round(up_ask, 2), round(dn_ask - TICK, 2)))
    candidates.append((round(up_ask, 2), round(dn_ask, 2)))

    for up_p, dn_p in candidates:
        if up_p <= 0 or dn_p <= 0:
            continue
        if up_p + dn_p + 1e-9 >= MIN_PAIR_SUM:
            return up_p, dn_p

    return None, None


def _target_ask_price(token_id: str, now: float) -> float | None:
    """Per-side wrapper around _target_pair_prices using current candle context."""
    candle = _state.candle
    if candle is None:
        return None
    up_p, dn_p = _target_pair_prices(candle, now)
    if token_id == candle.up_token:
        return up_p
    if token_id == candle.down_token:
        return dn_p
    return None


def _needs_reprice(order: LiveOrder, now: float) -> bool:
    """
    Reprice when:
    - The joint pair-pricing produces a desired price that differs by ≥ REPRICE_TICKS, OR
    - We're past candle start and still priced above the bid (force convergence to bid).
    """
    desired = _target_ask_price(order.token_id, now)
    if desired is not None and abs(desired - order.ask_price) >= REPRICE_TICKS * TICK:
        return True
    candle = _state.candle
    if candle is not None and now >= candle.start_ts:
        best_bid, _ = _best_bid_ask(order.token_id)
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
    budget: float,
    candle_end: float,
    bet: bool,
    split_mode: str = "relayer",
    split_cmd: str | None = None,
) -> LiveOrder | None:
    # splitPosition mints `budget` shares per side (1:1 with USDC).
    # In live mode we sell the FULL held inventory (drains any leftover from
    # prior candles too). In dry-run we use `budget` as the simulated qty.
    order_id = "DRY"
    if bet:
        held   = _held_shares_for_token(token_id)
        shares = round(held, 4)
        if shares < 0.01:
            log.warning("Skipping sell %s: no inventory (held=%.4f)", token_side, held)
            return None
        try:
            resp     = place_sell_order(clob, token_id, price, shares)
            order_id = str(resp.get("orderID") or resp.get("id") or "?")
            log.info("PLACED sell %s  id=%s  price=%.4f  shares=%.4f  recv=$%.4f",
                     token_side, order_id, price, shares, price * shares)
        except Exception as exc:
            log.error("place_sell_order failed for %s: %s", token_side, exc)
            return None
    else:
        shares = round(budget, 4)
        if shares < 0.01:
            log.warning("Budget too small (%.4f) for %s - skip", shares, token_side)
            return None
        log.info("[DRY] sell %s  price=%.4f  shares=%.4f  recv=$%.4f",
                 token_side, price, shares, price * shares)

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


# ── Split infrastructure (adapted from doge_seller) ───────────────────────────

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
                {"internalType": "address",   "name": "collateralToken",    "type": "address"},
                {"internalType": "bytes32",   "name": "parentCollectionId", "type": "bytes32"},
                {"internalType": "bytes32",   "name": "conditionId",        "type": "bytes32"},
                {"internalType": "uint256[]", "name": "partition",          "type": "uint256[]"},
                {"internalType": "uint256",   "name": "amount",             "type": "uint256"},
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
            w3  = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
            _   = w3.eth.block_number
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
        ).build_transaction({
            "from":     account.address,
            "nonce":    w3.eth.get_transaction_count(account.address),
            "chainId":  POLYGON_CHAIN_ID,
            "gas":      350000,
            "gasPrice": w3.eth.gas_price,
        })
        signed  = w3.eth.account.sign_transaction(tx, private_key=private_key)
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
    relayer_key  = os.getenv("RELAYER_API_KEY")
    relayer_addr = os.getenv("RELAYER_API_KEY_ADDRESS")

    if relayer_key and relayer_addr:
        try:
            from py_builder_relayer_client.signer import Signer
            from py_builder_relayer_client.config import get_contract_config
            from py_builder_relayer_client.builder.safe import build_safe_transaction_request
            from py_builder_relayer_client.models import (
                SafeTransaction, OperationType, SafeTransactionArgs, TransactionType,
            )
            from web3 import Web3

            headers = {
                "RELAYER_API_KEY":         relayer_key,
                "RELAYER_API_KEY_ADDRESS": relayer_addr,
                "Content-Type":            "application/json",
            }
            signer       = Signer(private_key, POLYGON_CHAIN_ID)
            from_address = signer.address()
            nonce_resp   = requests.get(
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
            tx  = SafeTransaction(
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
            body    = submit_resp.json() or {}
            tx_hash = body.get("transactionHash")
            tx_id   = body.get("transactionID")
            if tx_hash:
                return tx_hash
            if tx_id:
                for _ in range(30):
                    q = requests.get(f"{relayer_base}/transaction", params={"id": tx_id},
                                     headers=headers, timeout=20)
                    if q.status_code != 200:
                        break
                    arr = q.json() if q.text else []
                    if isinstance(arr, list) and arr:
                        state = arr[0].get("state")
                        txh   = arr[0].get("transactionHash")
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

    key        = os.getenv("POLY_BUILDER_API_KEY")
    secret     = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not key or not secret or not passphrase:
        log.warning("Missing builder creds for relayer split (POLY_BUILDER_API_KEY/SECRET/PASSPHRASE)")
        return None

    try:
        builder_cfg = BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase)
        )
        client = RelayClient(
            relayer_base, POLYGON_CHAIN_ID, private_key=private_key, builder_config=builder_cfg
        )
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
        tx     = SafeTransaction(to=Web3.to_checksum_address(CTF_ADDRESS),
                                 operation=OperationType.Call, data=data, value="0")
        resp   = client.execute([tx], "split positions")
        waited = resp.wait()
        tx_hash = waited.get("transactionHash") if isinstance(waited, dict) else None
        if not tx_hash:
            tx_hash = getattr(resp, "transaction_hash", None)
        return tx_hash
    except Exception as exc:
        log.warning("relayer split failed for condition %s: %s", condition_id, exc)
        return None


def _run_split_cmd(split_cmd: str | None, min_interval_seconds: int = 2) -> bool:
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
                private_key, condition_id, amount_usdc,
                rpc_url=os.getenv("POLYGON_RPC_URL"),
            )
    if tx_hash:
        log.info("Split submitted: amount=$%.4f  condition=%s  tx=%s",
                 amount_usdc, condition_id[:10], tx_hash)
        _log_inventory_snapshot()
        return True
    if split_cmd:
        return _run_split_cmd(split_cmd)
    return False


def _log_inventory_snapshot() -> None:
    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        return
    try:
        positions = get_positions(funder)
        total = sum(float(p.get("size", 0) or 0) for p in positions)
        log.info("Inventory: %d positions  total_shares=%.4f", len(positions), total)
    except Exception:
        pass


def _split_for_candle(
    candle: Candle,
    budget: float,
    split_mode: str,
    split_cmd: str | None = None,
) -> bool:
    """Split once per candle slug to seed Up + Down inventory before posting sells."""
    global _last_split_candle_slug
    if _last_split_candle_slug == candle.slug:
        return True

    # Skip split if we already hold ≥ budget on both sides (leftover inventory
    # from previous candles that never filled). Avoids piling up stranded shares.
    held_up   = _held_shares_for_token(candle.up_token)
    held_down = _held_shares_for_token(candle.down_token)
    if held_up >= budget and held_down >= budget:
        log.info("Skip split %s: already hold Up=%.4f Down=%.4f (budget=%.2f)",
                 candle.slug, held_up, held_down, budget)
        _last_split_candle_slug = candle.slug
        return True

    ok = _split_condition_positions(candle.condition_id, max(0.0, budget), split_mode,
                                    split_cmd=split_cmd)
    if ok:
        _last_split_candle_slug = candle.slug
    return ok


# ── Order lifecycle ───────────────────────────────────────────────────────────

def _expire_open_orders(clob, bet: bool) -> None:
    """Cancel and archive all open orders (called at candle close or roll)."""
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
    split_cmd: str | None = None,
) -> None:
    while True:
        now = time.time()

        # ── Refresh candle ─────────────────────────────────────────────────
        need_refresh = (
            _state.candle is None
            or now >= _state.candle.end_ts
            or now - _state.last_market_refresh > MARKET_REFRESH
        )
        if need_refresh:
            new_candle = await asyncio.to_thread(_fetch_btc_candle)
            _state.last_market_refresh = now

            if new_candle is None:
                log.warning("No active BTC 5m candle found — retrying…")
                await asyncio.sleep(STRATEGY_TICK)
                continue

            if _state.candle is None or new_candle.slug != _state.candle.slug:
                # Candle rolled — expire old orders, start fresh
                if _state.open_orders:
                    _expire_open_orders(clob, bet)

                _state.candle       = new_candle
                _state.candles_seen += 1
                _ws_subscribed_tokens.update({new_candle.up_token, new_candle.down_token})

                log.info("New candle: %s  ends=%s",
                         new_candle.slug,
                         datetime.fromtimestamp(new_candle.end_ts, tz=timezone.utc)
                                 .strftime("%H:%M:%S UTC"))

                if bet:
                    _split_for_candle(new_candle, budget, split_mode, split_cmd=split_cmd)

        candle = _state.candle
        secs   = max(0.0, candle.end_ts - now)

        # ── Candle expired — clean up and wait for next ────────────────────
        if secs == 0:
            _expire_open_orders(clob, bet)
            await asyncio.sleep(STRATEGY_TICK)
            continue

        # ── Fill detection ─────────────────────────────────────────────────
        _check_filled_via_book()

        # ── Reprice existing orders ────────────────────────────────────────
        for side, order in list(_state.open_orders.items()):
            if not _needs_reprice(order, now):
                continue
            new_price = _target_ask_price(order.token_id, now)
            if new_price is None or abs(new_price - order.ask_price) < 1e-9:
                continue
            _cancel_order_safe(clob, order, bet)
            order.status = "cancelled"
            _state.history.append(order)
            _state.cancels += 1
            del _state.open_orders[side]

            new_order = _place_order(
                clob, side, order.token_id, candle.condition_id,
                new_price, budget, candle.end_ts, bet,
                split_mode=split_mode, split_cmd=split_cmd,
            )
            if new_order:
                _state.open_orders[side] = new_order

        # ── Open orders for unfilled sides ─────────────────────────────────
        for side in ("Up", "Down"):
            if side in _state.open_orders:
                continue

            already_filled = any(
                o.token_side == side and o.status == "filled"
                for o in _state.history
                if o.candle_end == candle.end_ts
            )
            if already_filled:
                continue

            token_id = candle.up_token if side == "Up" else candle.down_token
            price    = _target_ask_price(token_id, now)
            if price is None:
                log.debug("Holding %s: pair-sum below floor or book empty", side)
                continue

            new_order = _place_order(
                clob, side, token_id, candle.condition_id,
                price, budget, candle.end_ts, bet,
                split_mode=split_mode, split_cmd=split_cmd,
            )
            if new_order:
                _state.open_orders[side] = new_order

        await asyncio.sleep(STRATEGY_TICK)


# ── WebSocket ─────────────────────────────────────────────────────────────────

async def _poly_ws() -> None:
    """Stream the Polymarket CLOB order book for the active BTC candle tokens."""
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
                    current = list(_ws_subscribed_tokens)
                    if set(current) != set(tokens):
                        log.info("Token set changed — reconnecting Poly WS")
                        break

                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue

                    events = payload if isinstance(payload, list) else [payload]
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
SEP     = "  " + "─" * 90


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
        f"  recv={GREEN}${order.usdc_received:.4f}{RESET}  {st}"
    )


def _render(budget: float, bet: bool) -> str:
    now    = time.time()
    ts     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    candle = _state.candle
    mode   = f"{GREEN}LIVE{RESET}" if bet else f"{YELLOW}DRY-RUN{RESET}"

    lines = ["", f"  {BOLD}[{ts}]  BTC 5m Sell Bot — Pre-Candle Pair Maker  [{mode}{BOLD}]{RESET}", SEP]

    if candle:
        secs_left   = max(0, int(candle.end_ts - now))
        secs_to_start = int(candle.start_ts - now)
        phase_name  = _phase(candle, now)
        if phase_name == "PRE_CANDLE":
            phase_lbl = f"{CYAN}PRE-CANDLE  starts in {-secs_to_start if secs_to_start < 0 else secs_to_start:3d}s{RESET}"
        elif phase_name == "ADJUST":
            phase_lbl = f"{YELLOW}ADJUST  +{int(now - candle.start_ts):2d}s into candle{RESET}"
        else:
            phase_lbl = f"{RED}HAIL-MARY  +{int(now - candle.start_ts):3d}s in{RESET}"
        up_bid, up_ask = _best_bid_ask(candle.up_token)
        dn_bid, dn_ask = _best_bid_ask(candle.down_token)

        def _p(v: float | None) -> str:
            return f"${v:.4f}" if v is not None else "   n/a"

        lines.append(f"  Candle: {candle.slug}  {BOLD}{secs_left:3d}s left{RESET}  [{phase_lbl}]")
        lines.append(
            f"    Up   bid={CYAN}{_p(up_bid)}{RESET}  ask={CYAN}{_p(up_ask)}{RESET}"
            f"    Down bid={CYAN}{_p(dn_bid)}{RESET}  ask={CYAN}{_p(dn_ask)}{RESET}"
        )
        combined_ask = (up_ask or 0) + (dn_ask or 0)
        sum_col = GREEN if combined_ask >= MIN_PAIR_SUM else RED
        lines.append(
            f"    pair_min=${MIN_PAIR_SUM:.2f}  market_sum={sum_col}${combined_ask:.4f}{RESET}"
            f"  budget/side=${budget/2:.2f}"
        )
    else:
        lines.append(f"  {DIM}Waiting for active BTC 5m candle…{RESET}")

    lines.append(SEP)

    if _state.open_orders:
        lines.append(f"  {YELLOW}Active orders:{RESET}")
        for order in _state.open_orders.values():
            lines.append(_fmt_order(order))
    else:
        lines.append(f"  {DIM}No active orders{RESET}")

    recent = list(reversed(_state.history))[:8]
    if recent:
        lines.append(f"  {DIM}Recent history:{RESET}")
        for order in recent:
            lines.append(_fmt_order(order))

    lines.append(SEP)

    cost = budget * _state.candles_seen
    net  = _state.total_usdc_recv - cost
    lines.append(
        f"  {BOLD}Session:{RESET}"
        f"  candles={_state.candles_seen}"
        f"  fills={GREEN}{_state.fills}{RESET}"
        f"  cancels={DIM}{_state.cancels}{RESET}"
        f"  recv={GREEN}${_state.total_usdc_recv:.4f}{RESET}"
        f"  cost=${cost:.2f}"
        f"  net={_pnl_col(net)}${net:+.4f}{RESET}"
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
    split_cmd: str | None = None,
) -> None:
    _init_logging()

    clob = None
    if bet:
        clob    = _build_clob_client()
        balance = get_usdc_balance(clob)
        log.info("USDC balance: $%.2f", balance)
        if balance < budget:
            log.error("Insufficient balance $%.2f < budget $%.2f", balance, budget)
            sys.exit(1)
        if balance > max_balance:
            log.warning("Balance $%.2f exceeds --max-balance $%.2f — continuing",
                        balance, max_balance)

    log.info("BTC Seller starting  budget=$%.2f  mode=%s",
             budget, "LIVE" if bet else "DRY-RUN")

    # Pre-warm: discover first candle so WS has tokens immediately
    first_candle = await asyncio.to_thread(_fetch_btc_candle)
    if first_candle is None:
        log.warning("No active BTC 5m candle at startup — will keep retrying")
    else:
        _state.candle       = first_candle
        _state.candles_seen = 1
        _ws_subscribed_tokens.update({first_candle.up_token, first_candle.down_token})
        log.info("First candle: %s  ends=%s",
                 first_candle.slug,
                 datetime.fromtimestamp(first_candle.end_ts, tz=timezone.utc).strftime("%H:%M:%S UTC"))

        if bet:
            _split_for_candle(first_candle, budget, split_mode, split_cmd=split_cmd)

    await asyncio.gather(
        _poly_ws(),
        _strategy_loop(clob, budget, bet, split_mode=split_mode, split_cmd=split_cmd),
        _display_loop(budget, bet),
    )


def main() -> None:
    global MIN_PAIR_SUM, ADJUST_GRACE

    # Avoid Windows cp1252 crashes from Unicode UI glyphs.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    p = argparse.ArgumentParser(
        description="BTC 5m sell-side bot — pre-candle pair-sum maker → adjust → hail mary"
    )
    p.add_argument("--bet", action="store_true",
                   help="Place real CLOB orders (default: dry-run)")
    p.add_argument("--budget", type=float, default=DEFAULT_BUDGET,
                   help=f"Total USDC per candle; split evenly Up/Down (default: {DEFAULT_BUDGET})")
    p.add_argument("--max-balance", type=float, default=DEFAULT_MAX_BALANCE,
                   help=f"Safety cap: warn if USDC balance exceeds this (default: {DEFAULT_MAX_BALANCE})")
    p.add_argument("--min-pair-sum", type=float, default=MIN_PAIR_SUM,
                   help=f"Minimum (up+down) sell-price sum (default: {MIN_PAIR_SUM} = 1¢ profit/pair)")
    p.add_argument("--adjust-grace", type=float, default=ADJUST_GRACE,
                   help=f"Seconds after candle start to wind down toward bid before hail mary (default: {ADJUST_GRACE})")
    p.add_argument("--split-mode", choices=["relayer", "onchain", "none"], default="relayer",
                   help="How to split USDC.e into outcome tokens (default: relayer)")
    p.add_argument("--split-cmd", type=str, default=None,
                   help="Optional fallback external split command")
    args = p.parse_args()

    if args.min_pair_sum != MIN_PAIR_SUM:
        MIN_PAIR_SUM = args.min_pair_sum
    if args.adjust_grace != ADJUST_GRACE:
        ADJUST_GRACE = args.adjust_grace

    try:
        asyncio.run(run(
            budget      = args.budget,
            max_balance = args.max_balance,
            bet         = args.bet,
            split_mode  = args.split_mode,
            split_cmd   = args.split_cmd,
        ))
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
