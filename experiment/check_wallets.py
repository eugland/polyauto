#!/usr/bin/env python3
"""
Check balances side-by-side for:
  1) wallet derived from POLYMARKET_PRIVATE_KEY
  2) POLYMARKET_FUNDER (if set)

Examples:
  python experiment/check_wallets.py
  python experiment/check_wallets.py --network polygon
  python experiment/check_wallets.py --token 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
"""

from __future__ import annotations

import argparse
import os
from decimal import Decimal

from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3


DEFAULT_RPC_URLS = {
    "polygon": "https://polygon-bor-rpc.publicnode.com",
    "ethereum": "https://ethereum.publicnode.com",
    "base": "https://mainnet.base.org",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
    "optimism": "https://mainnet.optimism.io",
}

DEFAULT_USDC_BY_NETWORK = {
    "polygon": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    "ethereum": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "base": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "arbitrum": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "optimism": "0x0b2C639c533813f4Aa9D7837CaF62653d097FF85",
}

ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check EOA and funder balances side-by-side")
    parser.add_argument(
        "--network",
        choices=sorted(DEFAULT_RPC_URLS.keys()),
        default=os.getenv("NETWORK", "polygon"),
        help="Network preset (default: polygon)",
    )
    parser.add_argument("--rpc-url", default=os.getenv("RPC_URL", ""), help="Optional RPC URL override")
    parser.add_argument("--token", default="", help="Optional token contract address (defaults to USDC for network)")
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY", help="Env var name for private key")
    return parser.parse_args()


def _get_private_key_address(env_name: str) -> str:
    key = os.getenv(env_name, "").strip()
    if not key:
        raise RuntimeError(f"Missing env var: {env_name}")
    return Account.from_key(key).address


def _native_balance(w3: Web3, address: str) -> Decimal:
    wei = w3.eth.get_balance(Web3.to_checksum_address(address))
    return Decimal(wei) / Decimal(10**18)


def _token_balance(w3: Web3, address: str, token_addr: str) -> tuple[str, Decimal]:
    token = w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=ERC20_ABI)
    decimals = int(token.functions.decimals().call())
    try:
        symbol = str(token.functions.symbol().call())
    except Exception:
        symbol = "TOKEN"
    raw = int(token.functions.balanceOf(Web3.to_checksum_address(address)).call())
    human = Decimal(raw) / (Decimal(10) ** decimals)
    return symbol, human


def main() -> None:
    load_dotenv()
    args = _parse_args()

    rpc_url = args.rpc_url or DEFAULT_RPC_URLS[args.network]
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError(f"Could not connect to RPC URL: {rpc_url}")

    eoa = _get_private_key_address(args.private_key_env)
    funder = os.getenv("POLYMARKET_FUNDER", "").strip()
    funder = Web3.to_checksum_address(funder) if funder else ""
    eoa = Web3.to_checksum_address(eoa)

    token_addr = args.token.strip() or DEFAULT_USDC_BY_NETWORK.get(args.network, "")
    token_addr = Web3.to_checksum_address(token_addr) if token_addr else ""

    print(f"Network: {args.network}")
    print(f"RPC: {rpc_url}")
    print(f"Chain ID: {w3.eth.chain_id}")
    if token_addr:
        print(f"Token: {token_addr}")
    print()

    rows: list[tuple[str, str]] = [("EOA (private key)", eoa)]
    if funder:
        rows.append(("Funder", funder))
    else:
        rows.append(("Funder", "(not set)"))

    for label, addr in rows:
        print(f"{label}: {addr}")
        if not addr.startswith("0x"):
            print("  native: n/a")
            print("  token:  n/a")
            print()
            continue
        try:
            native = _native_balance(w3, addr)
            print(f"  native: {native}")
        except Exception as exc:
            print(f"  native: error ({exc})")
        if token_addr:
            try:
                symbol, bal = _token_balance(w3, addr, token_addr)
                print(f"  {symbol}: {bal}")
            except Exception as exc:
                print(f"  token: error ({exc})")
        print()


if __name__ == "__main__":
    main()
