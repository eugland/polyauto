"""
Debug split + convert on the Polymarket NegRiskAdapter using a real 1 USDC.

Flow:
  1. Read Safe (funder) USDC balance, NO-token balance, allowances.
  2. NegRiskAdapter.splitPosition(conditionId, 1e6)  -> +1 YES + 1 NO on that bucket
  3. NegRiskAdapter.convertPositions(marketId, 1 << index, 1e6)
       -> burns 1 NO of the bucket, returns 1 YES of each OTHER question

Run:
  python -m scripts.debug_neg_risk
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

from web3 import Web3

from stock import weather_api

# --- Constants for the target market ---
CONDITION_ID = "0x756cdfd2d2384d32873c9fd8633f85783c3ffbc4bec11ee32800401cce14b171"
NEG_RISK_MARKET_ID = "0x45ad9926b942a8faddc2907f21a543eb0c690fc2aa45ac90a623dcc9210bd000"
QUESTION_INDEX = 4           # Tokyo 20°C bucket
AMOUNT_WEI = 1_000_000       # 1 USDC (6 decimals)
INDEX_SET = 1 << QUESTION_INDEX  # 16

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ABIs for read-only preflight
ERC20_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]
CTF_READ_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "id", "type": "uint256"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "collateralToken", "type": "address"}, {"name": "collectionId", "type": "bytes32"}],
     "name": "getPositionId", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "pure", "type": "function"},
    {"inputs": [{"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSet", "type": "uint256"}],
     "name": "getCollectionId", "outputs": [{"name": "", "type": "bytes32"}],
     "stateMutability": "view", "type": "function"},
]
ADAPTER_READ_ABI = [
    {"inputs": [], "name": "wcol", "outputs": [{"name": "", "type": "address"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "_questionId", "type": "bytes32"}, {"name": "_outcome", "type": "bool"}],
     "name": "getPositionId", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "_questionId", "type": "bytes32"}],
     "name": "getConditionId", "outputs": [{"name": "", "type": "bytes32"}],
     "stateMutability": "view", "type": "function"},
]


def _w3() -> Web3:
    candidates = [
        (os.getenv("POLYGON_RPC_URL") or "").strip(),
        "https://polygon-bor-rpc.publicnode.com",
        "https://1rpc.io/matic",
        "https://polygon-rpc.com",
    ]
    last_err: Exception | None = None
    for rpc in [u for u in candidates if u]:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 20}))
            _ = w3.eth.block_number
            return w3
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"no working Polygon RPC: {last_err}")


def _question_id(market_id_hex: str, index: int) -> bytes:
    """NegRiskIdLib: lower 8 bits of marketId are replaced with the index byte."""
    raw = bytes.fromhex(market_id_hex.lower().replace("0x", ""))
    return raw[:-1] + bytes([index & 0xff])


def preflight(funder: str) -> dict:
    w3 = _w3()
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_E), abi=ERC20_ABI)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF), abi=CTF_READ_ABI)
    adapter = w3.eth.contract(address=Web3.to_checksum_address(NEG_ADAPTER), abi=ADAPTER_READ_ABI)

    funder = Web3.to_checksum_address(funder)
    bal = int(usdc.functions.balanceOf(funder).call())
    allow_ctf = int(usdc.functions.allowance(funder, Web3.to_checksum_address(CTF)).call())
    allow_adapter = int(usdc.functions.allowance(funder, Web3.to_checksum_address(NEG_ADAPTER)).call())
    ctf_approved = bool(ctf.functions.isApprovedForAll(funder, Web3.to_checksum_address(NEG_ADAPTER)).call())

    qid = _question_id(NEG_RISK_MARKET_ID, QUESTION_INDEX)
    no_pos_id = int(adapter.functions.getPositionId(qid, False).call())
    yes_pos_id = int(adapter.functions.getPositionId(qid, True).call())
    no_bal = int(ctf.functions.balanceOf(funder, no_pos_id).call())
    yes_bal = int(ctf.functions.balanceOf(funder, yes_pos_id).call())

    return {
        "funder": funder,
        "usdc_balance": bal,
        "usdc_balance_human": bal / 1e6,
        "usdc_allowance_to_ctf": allow_ctf,
        "usdc_allowance_to_neg_adapter": allow_adapter,
        "ctf_setApprovalForAll_to_neg_adapter": ctf_approved,
        "no_position_id": no_pos_id,
        "yes_position_id": yes_pos_id,
        "no_token_balance": no_bal,
        "yes_token_balance": yes_bal,
    }


def main() -> int:
    funder = (os.getenv("POLYMARKET_FUNDER") or "").strip()
    if not funder:
        print("POLYMARKET_FUNDER is not set", file=sys.stderr)
        return 2
    transport = (os.getenv("WEATHER_TX_TRANSPORT") or "relayer").strip().lower()

    print("=" * 72)
    print("PREFLIGHT")
    print("=" * 72)
    pre = preflight(funder)
    print(json.dumps(pre, indent=2))

    if pre["usdc_balance"] < AMOUNT_WEI:
        print(f"\nInsufficient USDC on Safe. Need >= {AMOUNT_WEI / 1e6} USDC.", file=sys.stderr)
        return 3

    # --- STEP 1: SPLIT 1 USDC on the 20C bucket ---
    print("\n" + "=" * 72)
    print(f"STEP 1 — NegRiskAdapter.splitPosition(condition=20C, amount=1 USDC) via {transport}")
    print("=" * 72)
    split_result = weather_api.execute_negative_split(
        condition_id=CONDITION_ID,
        amount_wei=AMOUNT_WEI,
        execute=True,
        mode="simple",
        transport=transport,
    )
    print(json.dumps(split_result, indent=2, default=str))
    if not split_result.get("ok") or not split_result.get("executed"):
        print("Split failed — stopping before convert.", file=sys.stderr)
        return 4

    # Give the chain a moment to index the new YES/NO balances.
    print("\nSleeping 20s to let balances settle...")
    time.sleep(20)
    mid = preflight(funder)
    print(json.dumps({
        "no_token_balance_after_split": mid["no_token_balance"],
        "yes_token_balance_after_split": mid["yes_token_balance"],
    }, indent=2))

    if mid["no_token_balance"] < AMOUNT_WEI:
        print("Split did not land yet — aborting convert. Re-run the script later.", file=sys.stderr)
        return 5

    # --- STEP 2: CONVERT the 1 NO back to YES of all other buckets ---
    print("\n" + "=" * 72)
    print(f"STEP 2 — NegRiskAdapter.convertPositions(marketId, indexSet={INDEX_SET}, amount=1 USDC) via {transport}")
    print("=" * 72)
    convert_result = weather_api.execute_negative_convert(
        market_id=NEG_RISK_MARKET_ID,
        index_set=INDEX_SET,
        amount_wei=AMOUNT_WEI,
        execute=True,
        transport=transport,
    )
    print(json.dumps(convert_result, indent=2, default=str))

    print("\nSleeping 20s then re-reading balances...")
    time.sleep(20)
    post = preflight(funder)
    print(json.dumps({
        "no_token_balance_after_convert": post["no_token_balance"],
        "yes_token_balance_after_convert": post["yes_token_balance"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
