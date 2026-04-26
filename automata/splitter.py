"""
Aapang-style SPLIT/CONVERT/MERGE/REDEEM via Polymarket's NegRiskAdapter (CTF wrapper).

Walks the same flow proven in `automata/eth_1h.py::_redeem_condition_positions_relayer`
but for the multi-bucket weather (negative-risk) markets:

  STEP 1 (split):    NegRiskAdapter.splitPosition(conditionId, amountUSDC)
                       -> mints amountUSDC YES + amountUSDC NO of that bucket.
  STEP 2 (convert):  NegRiskAdapter.convertPositions(marketId, indexSet, amountUSDC)
                       -> burns amountUSDC NO of bucket k, returns amountUSDC
                          YES of every OTHER bucket in the event.
  STEP 3 (merge):    NegRiskAdapter equivalent — typically only used to undo
                       a split before resolution (rare in aapang's flow).
  STEP 4 (redeem):   CTF.redeemPositions on the winning bucket once the event
                       has resolved (Polymarket Safe via the relayer).

The typical aapang trip = SPLIT $X then CONVERT $X (both with the same X). Net
USDC in = X, you end up with X YES of every bucket in the event, redeemed at $1
when one wins (so X back, plus whatever dust the seller_bot harvested fading
the losing legs).

SAFETY
======
* Defaults to --dry-run. The --live flag is required to broadcast.
* Hard cap MAX_USDC_HARD = $5 per call enforced in code (no env override).
* USDC.approve() amount is set to the EXACT split amount, not infinite.
* Live mode prints full tx params and sleeps 5s before submission.

Usage:
  python -m automata.splitter --info
  python -m automata.splitter --action split --condition-id 0x... --usdc 1
  python -m automata.splitter --action convert --market-id 0x... --question-index 4 --usdc 1
  python -m automata.splitter --action redeem --condition-id 0x... --neg-risk
  python -m automata.splitter --pick-event                # list candidates
  # add --live to actually broadcast
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from automata import config

load_dotenv()

# ───────────── Constants — verified against scripts/debug_neg_risk.py & eth_1h.py
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER_ADDRESS = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
POLYGON_CHAIN_ID = 137
POLYGON_RPC_FALLBACKS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

# Hard caps — cannot be bypassed via env or flags.
MAX_USDC_HARD = 40.0          # per-call ceiling
LIVE_PRECONFIRM_SECONDS = 5  # operator window to Ctrl-C before broadcast

# ───────────── ABIs (minimal — only the functions we call) ────────────────────
ERC20_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]

CTF_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "id", "type": "uint256"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"}],
     "name": "redeemPositions", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
]

# NegRiskAdapter — split takes (conditionId, amount) only;
# convert takes (marketId, indexSet, amount). This matches scripts/debug_neg_risk.py.
NEG_ADAPTER_ABI = [
    {"inputs": [{"name": "_conditionId", "type": "bytes32"}, {"name": "_amount", "type": "uint256"}],
     "name": "splitPosition", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "_marketId", "type": "bytes32"}, {"name": "_indexSet", "type": "uint256"}, {"name": "_amount", "type": "uint256"}],
     "name": "convertPositions", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "_questionId", "type": "bytes32"}, {"name": "_outcome", "type": "bool"}],
     "name": "getPositionId", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]


def _setup_logging() -> logging.Logger:
    """Set up console logging + share with the stock UI's automata.log."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler()],
        )
    # Shared log the stock UI reads (stock/app.py /api/weather-log).
    shared = Path(__file__).resolve().parent.parent / "logs" / "automata.log"
    shared.parent.mkdir(exist_ok=True)
    if not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(shared)
        for h in root.handlers
    ):
        fh = logging.FileHandler(shared, encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        fh.setLevel(logging.INFO)
        root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("automata.splitter")


def _w3():
    """Return a connected Web3 instance using POLYGON_RPC_URL or known fallbacks."""
    from web3 import Web3
    candidates = [(os.getenv("POLYGON_RPC_URL") or "").strip()] + POLYGON_RPC_FALLBACKS
    last_err: Exception | None = None
    for rpc in [u for u in candidates if u]:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 20}))
            _ = w3.eth.block_number
            return w3
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"No working Polygon RPC: {last_err}")


def _as_bytes32(hex_value: str) -> bytes:
    h = (hex_value or "").lower().replace("0x", "")
    if len(h) != 64:
        raise ValueError(f"Invalid bytes32 hex length ({len(h)} chars, expected 64): {hex_value}")
    return bytes.fromhex(h)


def _question_id_bytes(market_id_hex: str, question_index: int) -> bytes:
    """NegRiskIdLib: lower 8 bits of marketId are replaced with the question index."""
    raw = bytes.fromhex(market_id_hex.lower().replace("0x", ""))
    if len(raw) != 32:
        raise ValueError(f"Invalid marketId length: {market_id_hex}")
    return raw[:-1] + bytes([question_index & 0xff])


# ───────────── Read-only preflight ────────────────────────────────────────────
def preflight(funder: str) -> dict:
    """Return wallet balances + relevant allowances for a Polymarket Safe."""
    from web3 import Web3
    w3 = _w3()
    funder_cs = Web3.to_checksum_address(funder)

    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_E_ADDRESS), abi=ERC20_ABI)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)

    bal = int(usdc.functions.balanceOf(funder_cs).call())
    allow_ctf = int(usdc.functions.allowance(funder_cs, Web3.to_checksum_address(CTF_ADDRESS)).call())
    allow_neg = int(usdc.functions.allowance(funder_cs, Web3.to_checksum_address(NEG_RISK_ADAPTER_ADDRESS)).call())
    ctf_op_neg = bool(ctf.functions.isApprovedForAll(funder_cs, Web3.to_checksum_address(NEG_RISK_ADAPTER_ADDRESS)).call())

    matic_bal = int(w3.eth.get_balance(funder_cs))

    return {
        "funder": funder_cs,
        "usdc_balance_micro": bal,
        "usdc_balance": bal / 1e6,
        "matic_balance_wei": matic_bal,
        "matic_balance": matic_bal / 1e18,
        "allowance_to_ctf_micro": allow_ctf,
        "allowance_to_neg_adapter_micro": allow_neg,
        "ctf_approved_for_neg_adapter": ctf_op_neg,
    }


def _print_preflight(funder: str, log: logging.Logger) -> dict:
    pre = preflight(funder)
    log.info("──────── WALLET PREFLIGHT ────────")
    log.info("  funder:              %s", pre["funder"])
    log.info("  USDC.e balance:      %.6f  (raw %d)", pre["usdc_balance"], pre["usdc_balance_micro"])
    log.info("  MATIC balance:       %.6f  (relayer txs cost ~0)", pre["matic_balance"])
    log.info("  USDC -> CTF allow:   %s", _human_allow(pre["allowance_to_ctf_micro"]))
    log.info("  USDC -> NEG allow:   %s", _human_allow(pre["allowance_to_neg_adapter_micro"]))
    log.info("  CTF setApprovalForAll(NEG_ADAPTER):  %s", pre["ctf_approved_for_neg_adapter"])
    log.info("──────────────────────────────────")
    return pre


def _human_allow(micro: int) -> str:
    if micro == 0:
        return "0 (NOT SET — split will fail)"
    if micro >= 2 ** 200:
        return "MAX (∞ — already approved)"
    return f"{micro / 1e6:.2f} USDC"


# ───────────── Encode contract calldata (for relayer-style submission) ────────
def _encode_call(contract_addr: str, abi: list, fn_name: str, args: list) -> str:
    from web3 import Web3
    w3 = _w3()
    c = w3.eth.contract(address=Web3.to_checksum_address(contract_addr), abi=abi)
    return c.encode_abi(fn_name, args=args)


def _decode_addr(addr: str) -> str:
    from web3 import Web3
    return Web3.to_checksum_address(addr)


# ───────────── Relayer submission (mirrors eth_1h.py) ─────────────────────────
def _submit_via_relayer(to_addr: str, calldata_hex: str, metadata: str, log: logging.Logger) -> str | None:
    """
    Submit a SafeTransaction via Polymarket's relayer (RELAYER_API_KEY auth path).
    Returns transaction hash on success, None on failure.
    """
    import requests
    from py_builder_relayer_client.builder.safe import build_safe_transaction_request
    from py_builder_relayer_client.config import get_contract_config
    from py_builder_relayer_client.models import (
        SafeTransaction, OperationType, SafeTransactionArgs, TransactionType,
    )
    from py_builder_relayer_client.signer import Signer
    from web3 import Web3

    relayer_base = config.get_str("POLYMARKET_RELAYER_URL", "polymarket", "relayer_url", "https://relayer-v2.polymarket.com")
    relayer_key = os.getenv("RELAYER_API_KEY")
    relayer_addr_key = os.getenv("RELAYER_API_KEY_ADDRESS")
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not all((relayer_key, relayer_addr_key, private_key)):
        log.error("Missing RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS / POLYMARKET_PRIVATE_KEY")
        return None

    headers = {
        "RELAYER_API_KEY": relayer_key,
        "RELAYER_API_KEY_ADDRESS": relayer_addr_key,
        "Content-Type": "application/json",
    }

    signer = Signer(private_key, POLYGON_CHAIN_ID)
    from_address = signer.address()

    nonce_resp = requests.get(
        f"{relayer_base}/nonce",
        params={"address": from_address, "type": TransactionType.SAFE.value},
        headers=headers, timeout=20,
    )
    if nonce_resp.status_code != 200:
        log.error("nonce failed: %s %s", nonce_resp.status_code, nonce_resp.text[:300])
        return None
    nonce = (nonce_resp.json() or {}).get("nonce")
    if nonce is None:
        log.error("invalid nonce payload: %s", nonce_resp.text[:300])
        return None

    tx = SafeTransaction(
        to=Web3.to_checksum_address(to_addr),
        operation=OperationType.Call,
        data=calldata_hex,
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
        metadata=metadata,
    ).to_dict()

    log.info("Submitting tx via relayer (%s) ...", relayer_base)
    submit_resp = requests.post(f"{relayer_base}/submit", headers=headers, json=req, timeout=30)
    if submit_resp.status_code != 200:
        log.error("relayer submit failed: %s %s", submit_resp.status_code, submit_resp.text[:500])
        return None
    body = submit_resp.json() or {}
    tx_hash = body.get("transactionHash")
    tx_id = body.get("transactionID")
    if tx_hash:
        return tx_hash
    if tx_id:
        log.info("relayer accepted tx, polling /transaction id=%s ...", tx_id)
        for _ in range(60):
            q = requests.get(f"{relayer_base}/transaction", params={"id": tx_id}, headers=headers, timeout=20)
            if q.status_code != 200:
                log.warning("poll failed: %s", q.text[:200])
                break
            arr = q.json() if q.text else []
            if isinstance(arr, list) and arr:
                row = arr[0]
                state = row.get("state")
                h = row.get("transactionHash")
                if h:
                    return h
                if state in ("STATE_FAILED", "STATE_INVALID"):
                    log.error("relayer reports state=%s body=%s", state, row)
                    break
            time.sleep(2)
    return None


# ───────────── Public actions ─────────────────────────────────────────────────
def _validate_amount(usdc: float) -> int:
    if usdc <= 0:
        raise ValueError("amount must be > 0")
    if usdc > MAX_USDC_HARD:
        raise ValueError(f"amount ${usdc:.2f} exceeds hard cap ${MAX_USDC_HARD:.2f} — edit MAX_USDC_HARD if you really want more")
    return int(round(usdc * 1_000_000))  # USDC has 6 decimals


def _confirm_live(action: str, params: dict, log: logging.Logger) -> bool:
    log.warning("**** LIVE BROADCAST IMMINENT  action=%s ****", action)
    for k, v in params.items():
        log.warning("    %-20s %s", k, v)
    log.warning("Sleeping %ds — Ctrl-C to abort.", LIVE_PRECONFIRM_SECONDS)
    try:
        for i in range(LIVE_PRECONFIRM_SECONDS, 0, -1):
            log.warning("    %d ...", i)
            time.sleep(1)
    except KeyboardInterrupt:
        log.warning("aborted by operator")
        return False
    return True


def action_approve_usdc_to_neg_adapter(amount_usdc: float, dry_run: bool, log: logging.Logger) -> dict:
    """Approve USDC.e to NegRiskAdapter for `amount_usdc` (exact, not infinite)."""
    amount_micro = _validate_amount(amount_usdc)
    calldata = _encode_call(USDC_E_ADDRESS, ERC20_ABI, "approve", [
        _decode_addr(NEG_RISK_ADAPTER_ADDRESS), amount_micro,
    ])
    params = {
        "to":             USDC_E_ADDRESS,
        "function":       "approve(spender, amount)",
        "spender":        NEG_RISK_ADAPTER_ADDRESS,
        "amount_micro":   amount_micro,
        "amount_usdc":    f"${amount_usdc:.6f}",
        "calldata":       calldata,
        "calldata_bytes": len(bytes.fromhex(calldata.replace("0x", ""))),
    }
    log.info("APPROVE plan: %s", json.dumps({k: str(v) for k, v in params.items()}, indent=2))
    if dry_run:
        log.info("[DRY RUN] approve calldata above would be sent. No broadcast.")
        return {"ok": True, "dry_run": True, "params": params}
    if not _confirm_live("approve", params, log):
        return {"ok": False, "aborted": True}
    tx_hash = _submit_via_relayer(USDC_E_ADDRESS, calldata, "approve usdc->neg_adapter", log)
    return {"ok": tx_hash is not None, "tx_hash": tx_hash, "params": params}


def action_split_neg_risk(condition_id: str, amount_usdc: float, dry_run: bool, log: logging.Logger) -> dict:
    """NegRiskAdapter.splitPosition(conditionId, amount). Mints YES+NO of one bucket."""
    amount_micro = _validate_amount(amount_usdc)
    cond_bytes = _as_bytes32(condition_id)
    calldata = _encode_call(NEG_RISK_ADAPTER_ADDRESS, NEG_ADAPTER_ABI, "splitPosition", [
        cond_bytes, amount_micro,
    ])
    params = {
        "to":             NEG_RISK_ADAPTER_ADDRESS,
        "function":       "splitPosition(bytes32 conditionId, uint256 amount)",
        "conditionId":    "0x" + cond_bytes.hex(),
        "amount_micro":   amount_micro,
        "amount_usdc":    f"${amount_usdc:.6f}",
        "calldata":       calldata,
        "calldata_bytes": len(bytes.fromhex(calldata.replace("0x", ""))),
    }
    log.info("SPLIT (neg-risk) plan: %s", json.dumps({k: str(v) for k, v in params.items()}, indent=2))
    if dry_run:
        log.info("[DRY RUN] splitPosition calldata above would be sent. No broadcast.")
        return {"ok": True, "dry_run": True, "params": params}
    if not _confirm_live("split", params, log):
        return {"ok": False, "aborted": True}
    tx_hash = _submit_via_relayer(NEG_RISK_ADAPTER_ADDRESS, calldata, f"split {condition_id}", log)
    return {"ok": tx_hash is not None, "tx_hash": tx_hash, "params": params}


def action_convert_neg_risk(market_id: str, index_set: int, amount_usdc: float, dry_run: bool, log: logging.Logger) -> dict:
    """NegRiskAdapter.convertPositions(marketId, indexSet, amount).
       Burns NO of bucket(s) in indexSet → YES of every other bucket."""
    amount_micro = _validate_amount(amount_usdc)
    mid_bytes = _as_bytes32(market_id)
    calldata = _encode_call(NEG_RISK_ADAPTER_ADDRESS, NEG_ADAPTER_ABI, "convertPositions", [
        mid_bytes, index_set, amount_micro,
    ])
    params = {
        "to":             NEG_RISK_ADAPTER_ADDRESS,
        "function":       "convertPositions(bytes32 marketId, uint256 indexSet, uint256 amount)",
        "marketId":       "0x" + mid_bytes.hex(),
        "indexSet":       index_set,
        "indexSet_binary": bin(index_set),
        "amount_micro":   amount_micro,
        "amount_usdc":    f"${amount_usdc:.6f}",
        "calldata":       calldata,
        "calldata_bytes": len(bytes.fromhex(calldata.replace("0x", ""))),
    }
    log.info("CONVERT (neg-risk) plan: %s", json.dumps({k: str(v) for k, v in params.items()}, indent=2))
    if dry_run:
        log.info("[DRY RUN] convertPositions calldata above would be sent. No broadcast.")
        return {"ok": True, "dry_run": True, "params": params}
    if not _confirm_live("convert", params, log):
        return {"ok": False, "aborted": True}
    tx_hash = _submit_via_relayer(NEG_RISK_ADAPTER_ADDRESS, calldata, f"convert {market_id} idx={index_set}", log)
    return {"ok": tx_hash is not None, "tx_hash": tx_hash, "params": params}


# ───────────── Winner-sell — post a CLOB limit sell at $0.999 on the favorite ─
# Some Polymarket markets cap at 0.99 and reject 0.999 (saw HK Apr 27 do this).
# We keep the default at 0.999 anyway — slippage matters at scale, and markets
# that allow 0.999 give us +0.9% per share. If the order fails on a 0.99-capped
# market the seller_bot logs and moves on; the dust-fade and placeholder orders
# still post. Auto-redeem still recovers the position at $1.00 at resolution.
WINNER_SELL_PRICE = 0.999


def action_winner_sell(
    yes_token_id: str,
    shares: float,
    dry_run: bool,
    log: logging.Logger,
    price: float = WINNER_SELL_PRICE,
) -> dict:
    """
    Post a GTC limit sell at `price` for `shares` of the given YES token via CLOB.
    Used post-CONVERT to monetize the favorite bucket without waiting for redeem.
    Polymarket's per-market min_order_size (typically 5 shares) applies.
    """
    params = {
        "token_id":      yes_token_id,
        "price":         price,
        "shares":        round(shares, 4),
        "notional_usd":  round(shares * price, 4),
        "order_type":    "GTC limit sell",
    }
    log.info("WINNER-SELL plan: %s", json.dumps(params, indent=2))

    if dry_run:
        log.info("[DRY RUN] would post sell @ $%.4f for %.4f sh on token %s",
                 price, shares, yes_token_id[:14] + "...")
        return {"ok": True, "dry_run": True, "params": params}

    if shares < 5:
        msg = (f"shares={shares:.4f} < 5 (Polymarket min_order_size) — "
               f"not posting. Run a SPLIT >= $5 USDC to enable winner-sell.")
        log.error(msg)
        return {"ok": False, "error": msg, "params": params}

    # Derive CLOB credentials and post
    from automata.client import (
        build_client, derive_api_credentials, place_sell_order,
    )
    if not (os.getenv("CLOB_API_KEY") and os.getenv("CLOB_SECRET") and os.getenv("CLOB_PASS")):
        creds = derive_api_credentials(
            host=os.environ["POLYMARKET_HOST"],
            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
            funder=os.getenv("POLYMARKET_FUNDER"),
            signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
        )
        os.environ["CLOB_API_KEY"] = creds.api_key
        os.environ["CLOB_SECRET"] = creds.api_secret
        os.environ["CLOB_PASS"] = creds.api_passphrase
    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER"),
        signature_type=config.get_int("POLYMARKET_SIG_TYPE", "polymarket", "signature_type", 0),
    )
    try:
        resp = place_sell_order(client, yes_token_id, price, shares)
        order_id = resp.get("orderID") or resp.get("id") or "?"
        log.info("WINNER-SELL placed  id=%s  %.4f sh @ $%.4f", order_id, shares, price)
        return {"ok": True, "order_id": str(order_id), "raw": resp, "params": params}
    except Exception as exc:
        log.error("WINNER-SELL failed: %s", exc)
        return {"ok": False, "error": str(exc), "params": params}


# ───────────── Auto-chain — pick best event, SPLIT, CONVERT, post winner-sell ─
def action_auto(
    amount_usdc: float,
    dry_run: bool,
    log: logging.Logger,
    min_score: float = 30.0,
    settle_seconds: int = 18,
    allow_topup: bool = False,
) -> dict:
    """
    End-to-end aapang trip:
      1. picker.pick_best_event()         — choose top-scoring event
      2. action_split_neg_risk(favorite)  — SPLIT on the favorite bucket
      3. wait for tx                       — relayer takes ~10–15s on Polygon
      4. action_convert_neg_risk(idx)     — CONVERT with favorite's question_index
      5. wait for tx
      6. action_winner_sell(favorite)     — post limit sell @ $0.999 on favorite YES

    Top-up mode (`allow_topup=True`):
      `amount_usdc` is interpreted as the TARGET stake per event. If the picker
      returns an event we already partially hold, the SPLIT/CONVERT/winner-sell
      amount is the DELTA (`amount_usdc - committed`), not the full target.
      Top-ups < 5 USDC are skipped (Polymarket order minimum). Useful for
      raising your per-event lot size mid-flight without abandoning open
      positions.

    All four sub-actions inherit `dry_run`. In dry-run nothing is broadcast.
    """
    from automata.picker import pick_best_event

    log.info("──────── AUTO-CHAIN — picking best event ────────")
    ev = pick_best_event(
        min_score=min_score,
        topup_target_usdc=amount_usdc if allow_topup else None,
    )
    if ev is None:
        return {"ok": False, "error": "no event meets min_score"}

    fav_bucket = ev["buckets"][ev["peak_idx"]]
    fav_qidx = fav_bucket.get("question_index")
    fav_cid = fav_bucket["conditionId"]
    fav_token = fav_bucket["yes_token"]
    if fav_qidx is None:
        return {"ok": False, "error": "favorite bucket has no on-chain question_index"}

    # Determine actual SPLIT amount: full target for fresh events, delta for top-ups.
    committed = float(ev.get("committed_usdc") or 0.0)
    is_topup = allow_topup and committed > 0.0
    deploy_usdc = (amount_usdc - committed) if is_topup else amount_usdc
    deploy_usdc = round(deploy_usdc, 6)
    _validate_amount(deploy_usdc)  # raises if > MAX_USDC_HARD or <= 0

    if is_topup:
        log.info("TOP-UP mode: target=$%.4f committed=$%.4f → SPLIT delta=$%.4f",
                 amount_usdc, committed, deploy_usdc)
    log.info("Picked event: %s (score=%.1f)", ev.get("title"), ev.get("score"))
    log.info("  favorite bucket: %s  yes_bid=%.4f  question_index=%d",
             fav_bucket["question"], fav_bucket.get("yes_bid") or 0.0, fav_qidx)
    log.info("  conditionId:     %s", fav_cid)
    log.info("  negRiskMarketID: %s", ev["negRiskMarketID"])

    results: dict[str, Any] = {"event": {
        "title": ev.get("title"), "slug": ev.get("slug"),
        "score": ev.get("score"),
        "is_topup": is_topup,
        "committed_before": committed,
        "deploy_usdc": deploy_usdc,
        "favorite": {
            "question": fav_bucket["question"],
            "conditionId": fav_cid,
            "question_index": fav_qidx,
            "yes_token": fav_token,
            "yes_bid": fav_bucket.get("yes_bid"),
            "yes_ask": fav_bucket.get("yes_ask"),
        },
    }}

    log.info("")
    log.info("──────── STEP 1/3 — SPLIT $%.4f on favorite bucket ────────", deploy_usdc)
    split_res = action_split_neg_risk(fav_cid, deploy_usdc, dry_run, log)
    results["split"] = split_res
    if not split_res.get("ok"):
        log.error("SPLIT failed — aborting chain")
        return {"ok": False, "stage": "split", **results}

    if not dry_run:
        log.info("Sleeping %ds for SPLIT tx to mine ...", settle_seconds)
        time.sleep(settle_seconds)

    log.info("")
    log.info("──────── STEP 2/3 — CONVERT $%.4f (indexSet=1<<%d=%d) ────────",
             deploy_usdc, fav_qidx, 1 << fav_qidx)
    convert_res = action_convert_neg_risk(
        ev["negRiskMarketID"], 1 << fav_qidx, deploy_usdc, dry_run, log,
    )
    results["convert"] = convert_res
    if not convert_res.get("ok"):
        log.error("CONVERT failed — STOP. The SPLIT shares are still in the wallet "
                  "as YES+NO of the favorite bucket. Use --action winner-sell or "
                  "--action redeem after resolution to recover.")
        return {"ok": False, "stage": "convert", **results}

    if not dry_run:
        log.info("Sleeping %ds for CONVERT tx to mine ...", settle_seconds)
        time.sleep(settle_seconds)

    log.info("")
    # Winner-sell post-condition: total holding of the favorite is now
    # `committed + deploy_usdc` shares (since SPLIT+CONVERT minted `deploy`
    # YES on every bucket including the favorite, on top of any prior holding).
    final_shares = committed + deploy_usdc
    log.info("──────── STEP 3/3 — Post winner-sell @ $%.4f for %.4f sh ────────",
             WINNER_SELL_PRICE, final_shares)
    sell_res = action_winner_sell(fav_token, final_shares, dry_run, log)
    results["winner_sell"] = sell_res
    if not sell_res.get("ok"):
        log.warning("WINNER-SELL didn't land. Position is still good — manual fallback: "
                    "redeem after resolution, or repost with --action winner-sell.")

    results["ok"] = bool(sell_res.get("ok"))
    return results


def action_redeem_neg_risk(condition_id: str, dry_run: bool, log: logging.Logger) -> dict:
    """CTF.redeemPositions for a binary YES/NO bucket. Standard partition [1, 2]."""
    cond_bytes = _as_bytes32(condition_id)
    calldata = _encode_call(CTF_ADDRESS, CTF_ABI, "redeemPositions", [
        _decode_addr(USDC_E_ADDRESS),
        b"\x00" * 32,
        cond_bytes,
        [1, 2],
    ])
    params = {
        "to":             CTF_ADDRESS,
        "function":       "redeemPositions(collateral, parentCollectionId=0, conditionId, indexSets=[1,2])",
        "conditionId":    "0x" + cond_bytes.hex(),
        "calldata":       calldata,
        "calldata_bytes": len(bytes.fromhex(calldata.replace("0x", ""))),
    }
    log.info("REDEEM plan: %s", json.dumps({k: str(v) for k, v in params.items()}, indent=2))
    if dry_run:
        log.info("[DRY RUN] redeemPositions calldata above would be sent. No broadcast.")
        return {"ok": True, "dry_run": True, "params": params}
    if not _confirm_live("redeem", params, log):
        return {"ok": False, "aborted": True}
    tx_hash = _submit_via_relayer(CTF_ADDRESS, calldata, f"redeem {condition_id}", log)
    return {"ok": tx_hash is not None, "tx_hash": tx_hash, "params": params}


# ───────────── Pick-event helper — show real candidate condition ids ──────────
_ADAPTER_CONDITION_ABI = [
    {"inputs": [{"name": "_questionId", "type": "bytes32"}],
     "name": "getConditionId", "outputs": [{"name": "", "type": "bytes32"}],
     "stateMutability": "view", "type": "function"},
]


def resolve_question_indices(market_id: str, condition_ids: list[str], max_probe: int = 32) -> dict[str, int]:
    """
    Probe NegRiskAdapter.getConditionId(questionId(marketId, idx)) for idx 0..max_probe-1
    and return {conditionId_lower: question_index} for every conditionId provided.

    The on-chain question_index is the lower 8 bits of the questionId, which is
    derived as marketId[:31] || idx. This is the AUTHORITATIVE index for use in
    convertPositions's indexSet bitmask — Gamma's market list order is unreliable.
    """
    from web3 import Web3
    w3 = _w3()
    adapter = w3.eth.contract(
        address=Web3.to_checksum_address(NEG_RISK_ADAPTER_ADDRESS),
        abi=_ADAPTER_CONDITION_ABI,
    )
    target = {cid.lower(): True for cid in condition_ids if cid}
    out: dict[str, int] = {}
    for idx in range(max_probe):
        qid = _question_id_bytes(market_id, idx)
        try:
            cid_b = adapter.functions.getConditionId(qid).call()
        except Exception:
            break
        cid_hex = "0x" + cid_b.hex()
        if cid_hex.lower() in target:
            out[cid_hex.lower()] = idx
        if len(out) == len(target):
            break
    return out


def list_candidate_events(limit: int = 6, probe_indices: bool = True) -> list[dict]:
    """
    Return a small list of currently-open weather events with the data needed to
    SPLIT/CONVERT one bucket: (event_slug, negRiskMarketId, conditionId, on-chain question_index).

    Buckets are returned in Gamma's natural order, but `question_index` is the
    AUTHORITATIVE on-chain index obtained by probing NegRiskAdapter.getConditionId
    (when `probe_indices=True`). Polymarket's Gamma list order does NOT always
    match on-chain question_index — see the test on Atlanta =82°F which is on
    chain at idx=6 but appears earlier in Gamma's list.
    """
    from automata.polymarket import fetch_temperature_markets_payload
    from automata.parser import _parse_threshold

    payload = fetch_temperature_markets_payload()
    events: dict[str, dict] = {}
    for raw in payload["markets"]:
        if raw.get("closed") or (raw.get("active") is not None and not raw.get("active")):
            continue
        slug = str(raw.get("event_slug") or "")
        if slug not in events:
            events[slug] = {
                "slug": slug,
                "title": raw.get("event_title"),
                "negRiskMarketID": raw.get("negRiskMarketID") or raw.get("neg_risk_market_id"),
                "buckets": [],
            }
        question = str(raw.get("groupItemTitle") or raw.get("question") or "-")
        parsed = _parse_threshold(question)
        if not parsed:
            continue
        threshold, threshold_hi, unit, direction = parsed
        events[slug]["buckets"].append({
            "question": question,
            "threshold": threshold,
            "threshold_hi": threshold_hi,
            "unit": unit,
            "direction": direction,
            "conditionId": raw.get("conditionId"),
            "questionID": raw.get("questionID") or raw.get("question_id"),
        })

    out = [e for e in events.values()
           if e["negRiskMarketID"] and e["buckets"] and all(b["conditionId"] for b in e["buckets"])]
    out = out[:limit]

    if probe_indices:
        for ev in out:
            cid_to_idx = resolve_question_indices(
                ev["negRiskMarketID"],
                [b["conditionId"] for b in ev["buckets"]],
                max_probe=max(32, len(ev["buckets"]) * 2),
            )
            for b in ev["buckets"]:
                b["question_index"] = cid_to_idx.get(str(b["conditionId"]).lower())
        # Sort by chain question_index so the display reflects the bitmask layout.
        for ev in out:
            ev["buckets"].sort(key=lambda b: (b.get("question_index") if b.get("question_index") is not None else 999))
    else:
        for ev in out:
            for i, b in enumerate(ev["buckets"]):
                b["question_index"] = i
    return out


def _print_pick_event(log: logging.Logger) -> None:
    cands = list_candidate_events(limit=6)
    log.info("──────── PICK-EVENT (top %d open weather events) ────────", len(cands))
    for ev in cands:
        log.info("Event: %s", ev["title"])
        log.info("  slug:            %s", ev["slug"])
        log.info("  negRiskMarketID: %s", ev["negRiskMarketID"])
        log.info("  %d buckets:", len(ev["buckets"]))
        for b in ev["buckets"]:
            unit = b.get("unit") or "C"
            label = (
                f"<={b['threshold']:g}{unit}" if b["direction"] == "below"
                else f">={b['threshold']:g}{unit}" if b["direction"] == "higher"
                else f"={b['threshold']:g}{unit}"
            )
            log.info(
                "    [idx=%d] %-10s  conditionId=%s",
                b["question_index"], label, b["conditionId"],
            )
        log.info("")


# ───────────── CLI ────────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(description="Aapang-style SPLIT/CONVERT/MERGE/REDEEM via Polymarket NegRiskAdapter")
    p.add_argument("--info", action="store_true", help="Print wallet preflight (USDC, MATIC, allowances)")
    p.add_argument("--pick-event", action="store_true", help="List candidate open weather events with conditionIds")
    p.add_argument("--action", choices=["approve", "split", "convert", "redeem", "winner-sell", "auto"],
                   help="What to do. 'auto' = pick + split + convert + winner-sell.")
    p.add_argument("--condition-id", help="bytes32 conditionId (for split/redeem)")
    p.add_argument("--market-id", help="bytes32 negRiskMarketID (for convert)")
    p.add_argument("--question-index", type=int, help="Bucket index in event for convert (used to compute indexSet=1<<index)")
    p.add_argument("--index-set", type=int, help="Override indexSet directly for convert (bitmask)")
    p.add_argument("--token-id", help="YES token id (for winner-sell)")
    p.add_argument("--shares", type=float, help="Shares to sell (for winner-sell)")
    p.add_argument("--sell-price", type=float, default=WINNER_SELL_PRICE,
                   help=f"Limit sell price for winner-sell (default {WINNER_SELL_PRICE})")
    p.add_argument("--min-score", type=float, default=30.0,
                   help="Minimum picker score to take an event (auto action only). Default 30.")
    p.add_argument("--allow-topup", action="store_true",
                   help="auto action: treat --usdc as TARGET stake per event. "
                        "Top up held events with committed < target (delta-sized SPLIT).")
    p.add_argument("--usdc", type=float, help=f"USDC amount (HARD CAP ${MAX_USDC_HARD:.2f})")
    p.add_argument("--live", action="store_true", help="Broadcast for real. Default: dry-run.")
    args = p.parse_args()

    log = _setup_logging()
    funder = (os.getenv("POLYMARKET_FUNDER") or "").strip()
    if not funder:
        log.error("POLYMARKET_FUNDER not set in .env"); return 2

    if args.info:
        _print_preflight(funder, log)
        return 0

    if args.pick_event:
        _print_pick_event(log)
        return 0

    if not args.action:
        p.print_help(); return 0

    dry_run = not args.live

    # Always show preflight before any action
    _print_preflight(funder, log)
    log.info("dry_run=%s   action=%s", dry_run, args.action)

    if args.action == "approve":
        if args.usdc is None:
            log.error("--usdc required for approve"); return 2
        result = action_approve_usdc_to_neg_adapter(args.usdc, dry_run, log)
    elif args.action == "split":
        if not args.condition_id or args.usdc is None:
            log.error("--condition-id and --usdc required for split"); return 2
        result = action_split_neg_risk(args.condition_id, args.usdc, dry_run, log)
    elif args.action == "convert":
        if not args.market_id or args.usdc is None:
            log.error("--market-id and --usdc required for convert"); return 2
        if args.index_set is not None:
            idx_set = args.index_set
        elif args.question_index is not None:
            idx_set = 1 << args.question_index
        else:
            log.error("provide --question-index or --index-set"); return 2
        result = action_convert_neg_risk(args.market_id, idx_set, args.usdc, dry_run, log)
    elif args.action == "redeem":
        if not args.condition_id:
            log.error("--condition-id required for redeem"); return 2
        result = action_redeem_neg_risk(args.condition_id, dry_run, log)
    elif args.action == "winner-sell":
        if not args.token_id or args.shares is None:
            log.error("--token-id and --shares required for winner-sell"); return 2
        result = action_winner_sell(args.token_id, args.shares, dry_run, log, price=args.sell_price)
    elif args.action == "auto":
        if args.usdc is None:
            log.error("--usdc required for auto"); return 2
        result = action_auto(args.usdc, dry_run, log,
                             min_score=args.min_score,
                             allow_topup=args.allow_topup)
    else:
        log.error("unknown action"); return 2

    log.info("RESULT: %s", json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
