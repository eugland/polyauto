#!/usr/bin/env python3
"""
Transfer native coin or ERC-20 tokens from a private key loaded from .env.

Examples:
  python experiment/transfer.py --scan-networks
  python experiment/transfer.py --to 0xRecipient --amount 0.01
  python experiment/transfer.py --to 0xRecipient --amount 5 --token 0xToken
  python experiment/transfer.py --to 0xRecipient --amount 0.01 --network ethereum --send --yes
"""

from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from dotenv import load_dotenv

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
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

DEFAULT_RPC_URLS = {
    "polygon": "https://polygon-rpc.com",
    "ethereum": "https://ethereum.publicnode.com",
    "base": "https://mainnet.base.org",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
    "optimism": "https://mainnet.optimism.io",
}


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def _to_base_units(amount: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int((amount * scale).to_integral_value(rounding=ROUND_DOWN))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transfer native coin or ERC-20 tokens")
    parser.add_argument("--to", default="", help="Recipient address (0x...)")
    parser.add_argument("--amount", default="", help="Amount in human units, e.g. 0.01 or 5")
    parser.add_argument(
        "--network",
        choices=sorted(DEFAULT_RPC_URLS.keys()),
        default=os.getenv("NETWORK", "polygon"),
        help="Network preset used when --rpc-url is omitted (default: polygon)",
    )
    parser.add_argument("--rpc-url", default=os.getenv("RPC_URL", ""), help="RPC URL (optional)")
    parser.add_argument("--scan-networks", action="store_true", help="Show native balances on all preset networks and exit")
    parser.add_argument("--token", default="", help="ERC-20 token contract address. Omit for native transfer")
    parser.add_argument("--decimals", type=int, default=None, help="Token decimals override (optional)")
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY", help="Env var name for private key")
    parser.add_argument("--gas-limit", type=int, default=None, help="Optional gas limit override")
    parser.add_argument("--nonce", type=int, default=None, help="Optional nonce override")
    parser.add_argument("--send", action="store_true", help="Broadcast signed tx. Default is preview only")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = _parse_args()

    try:
        from web3 import Web3
    except Exception as exc:  # pragma: no cover
        print("Missing dependency: web3")
        print("Install with: pip install web3")
        print(f"Import error: {exc}")
        raise SystemExit(1)

    private_key = _require_env(args.private_key_env)
    from eth_account import Account

    account = Account.from_key(private_key)
    sender = account.address

    if args.scan_networks:
        print(f"Wallet: {sender}")
        print("Network balances:")
        for net, preset_url in DEFAULT_RPC_URLS.items():
            try:
                net_w3 = Web3(Web3.HTTPProvider(preset_url))
                if not net_w3.is_connected():
                    print(f"  - {net:<9} unavailable")
                    continue
                cid = int(net_w3.eth.chain_id)
                bal = net_w3.eth.get_balance(sender)
                human = Decimal(bal) / Decimal(10**18)
                print(f"  - {net:<9} chain_id={cid:<6} native_balance={human}")
            except Exception as exc:
                print(f"  - {net:<9} error: {exc}")
        return

    rpc_url = args.rpc_url or DEFAULT_RPC_URLS.get(args.network, "")
    if not rpc_url:
        raise RuntimeError("No RPC URL resolved. Provide --rpc-url or set RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Could not connect to RPC URL")

    if not args.to or not args.amount:
        raise RuntimeError("--to and --amount are required unless --scan-networks is used")
    try:
        amount = Decimal(args.amount)
    except InvalidOperation:
        raise RuntimeError("--amount must be a numeric value")
    if amount <= 0:
        raise RuntimeError("--amount must be > 0")

    recipient = Web3.to_checksum_address(args.to)
    chain_id = int(w3.eth.chain_id)
    native_balance_wei = w3.eth.get_balance(sender)
    native_balance = Decimal(native_balance_wei) / Decimal(10**18)

    nonce = args.nonce if args.nonce is not None else w3.eth.get_transaction_count(sender, "pending")
    tx: dict
    transfer_label: str

    if args.token:
        token_addr = Web3.to_checksum_address(args.token)
        token = w3.eth.contract(address=token_addr, abi=ERC20_ABI)
        decimals = args.decimals if args.decimals is not None else int(token.functions.decimals().call())
        token_balance_base = int(token.functions.balanceOf(sender).call())
        token_balance = Decimal(token_balance_base) / (Decimal(10) ** decimals)
        amount_base = _to_base_units(amount, decimals)
        if amount_base <= 0:
            raise RuntimeError("Amount is too small after token decimals conversion")
        tx = token.functions.transfer(recipient, amount_base).build_transaction(
            {
                "from": sender,
                "nonce": nonce,
                "chainId": chain_id,
            }
        )
        if args.gas_limit is not None:
            tx["gas"] = args.gas_limit
        elif "gas" not in tx:
            tx["gas"] = w3.eth.estimate_gas({**tx, "from": sender})
        transfer_label = f"ERC20 {token_addr} amount={amount} (base={amount_base}, decimals={decimals})"
    else:
        value_wei = _to_base_units(amount, 18)
        tx = {
            "from": sender,
            "to": recipient,
            "value": value_wei,
            "nonce": nonce,
            "chainId": chain_id,
        }
        if args.gas_limit is not None:
            tx["gas"] = args.gas_limit
        else:
            tx["gas"] = w3.eth.estimate_gas(tx)
        transfer_label = f"NATIVE amount={amount} (wei={value_wei})"

    gas_price = w3.eth.gas_price
    tx["gasPrice"] = gas_price
    fee_wei = int(tx["gas"]) * int(tx["gasPrice"])

    print("Transfer preview")
    print(f"  from: {sender}")
    print(f"  to:   {recipient}")
    print(f"  network: {args.network}")
    print(f"  kind: {transfer_label}")
    print(f"  rpc_url: {rpc_url}")
    print(f"  chain_id: {chain_id}")
    print(f"  native_balance: {native_balance}")
    if args.token:
        print(f"  token_balance: {token_balance}")
    print(f"  nonce: {nonce}")
    print(f"  gas: {tx['gas']}")
    print(f"  gas_price_wei: {tx['gasPrice']}")
    print(f"  max_fee_native: {Decimal(fee_wei) / Decimal(10**18)}")

    if not args.send:
        print("\nDry run only. Add --send to broadcast.")
        return

    if not args.yes:
        confirm = input("Type YES to broadcast this transaction: ").strip()
        if confirm != "YES":
            print("Cancelled.")
            return

    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"Broadcasted tx hash: {tx_hash.hex()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
