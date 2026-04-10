from __future__ import annotations

"""
Live ETH price dashboard using Chainlink feeds on Polygon.

Plots:
1) ETH/USD and ETH/USDT in real time.
2) Difference (ETH/USD - ETH/USDT) in real time.

Feed addresses (Polygon mainnet):
- ETH/USD:  0xF9680D99D6C9589e2a93a78A04A279e509205945
- USDT/ETH: 0xf9d5AAC6E5572AEFa6bd64108ff86a222F69B64d
"""

import argparse
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime


ETH_USD_FEED = "0xF9680D99D6C9589e2a93a78A04A279e509205945"
USDT_ETH_FEED = "0xf9d5AAC6E5572AEFa6bd64108ff86a222F69B64d"
DEFAULT_RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass
class FeedClient:
    contract: any
    decimals: int

    def latest_price(self) -> tuple[float, int]:
        _, answer, _, updated_at, _ = self.contract.functions.latestRoundData().call()
        if answer <= 0:
            raise RuntimeError("Feed returned non-positive answer")
        return float(answer) / (10 ** self.decimals), int(updated_at)


def fmt_feed_time(ts: int) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")


def make_feed_client(w3: any, address: str) -> FeedClient:
    from web3 import Web3

    contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=AGGREGATOR_V3_ABI)
    decimals = int(contract.functions.decimals().call())
    return FeedClient(contract=contract, decimals=decimals)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live ETH/USD vs ETH/USDT dashboard from Chainlink on Polygon")
    parser.add_argument(
        "--rpc-url",
        action="append",
        default=[],
        help="Polygon RPC URL (can be passed multiple times for fallback order)",
    )
    parser.add_argument("--interval", type=float, default=2.0, help="Polling interval in seconds")
    parser.add_argument("--history", type=int, default=300, help="Max points kept in memory for charts")
    parser.add_argument("--no-plot", action="store_true", help="Print streaming values only (no matplotlib window)")
    parser.add_argument("--samples", type=int, default=0, help="Stop after N samples in --no-plot mode (0=run forever)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        from web3 import Web3
    except ImportError as exc:
        raise SystemExit("Missing web3 dependency. Install with: pip install web3") from exc

    rpc_urls = args.rpc_url or DEFAULT_RPC_URLS
    w3 = None
    connected_rpc = None
    errors: list[str] = []
    for rpc_url in rpc_urls:
        try:
            candidate = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
            _ = candidate.eth.block_number
            w3 = candidate
            connected_rpc = rpc_url
            break
        except Exception as exc:
            errors.append(f"{rpc_url} -> {exc}")

    if w3 is None or connected_rpc is None:
        detail = "\n".join(errors) if errors else "No RPC URLs provided."
        raise SystemExit(f"Could not connect to any Polygon RPC endpoint:\n{detail}")
    print(f"Connected RPC: {connected_rpc}")

    eth_usd = make_feed_client(w3, ETH_USD_FEED)
    usdt_eth = make_feed_client(w3, USDT_ETH_FEED)

    if args.no_plot:
        count = 0
        while True:
            eth_usd_price, eth_usd_updated = eth_usd.latest_price()
            usdt_per_eth, usdt_eth_updated = usdt_eth.latest_price()
            eth_usdt_price = 1.0 / usdt_per_eth
            spread = eth_usd_price - eth_usdt_price
            ts = datetime.now()
            print(
                f"{ts.strftime('%H:%M:%S')}  ETH/USD={eth_usd_price:.4f}  "
                f"USDT/ETH={usdt_per_eth:.8f}  ETH/USDT={eth_usdt_price:.4f}  DIFF={spread:.6f}"
            )
            print(
                f"  src1(ETH/USD) update: {fmt_feed_time(eth_usd_updated)} | "
                f"src2(USDT/ETH) update: {fmt_feed_time(usdt_eth_updated)}"
            )
            count += 1
            if args.samples > 0 and count >= args.samples:
                break
            time.sleep(max(0.2, args.interval))
        return

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        from matplotlib.animation import FuncAnimation
    except ImportError as exc:
        raise SystemExit(
            "Missing plotting dependency. Install with: pip install matplotlib"
        ) from exc

    timestamps: deque[datetime] = deque(maxlen=args.history)
    eth_usd_vals: deque[float] = deque(maxlen=args.history)
    eth_usdt_vals: deque[float] = deque(maxlen=args.history)
    spread_vals: deque[float] = deque(maxlen=args.history)

    fig, (ax_prices, ax_spread) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle("Chainlink Polygon: ETH/USD vs ETH/USDT")

    (line_eth_usd,) = ax_prices.plot([], [], label="ETH/USD", linewidth=1.8)
    (line_eth_usdt,) = ax_prices.plot([], [], label="ETH/USDT (from USDT/ETH)", linewidth=1.8)
    ax_prices.set_ylabel("Price")
    ax_prices.grid(True, alpha=0.3)
    ax_prices.legend(loc="upper left")

    (line_spread,) = ax_spread.plot([], [], label="Difference (ETH/USD - ETH/USDT)", linewidth=1.8)
    ax_spread.axhline(0.0, linestyle="--", linewidth=1.0, alpha=0.6)
    ax_spread.set_ylabel("USD")
    ax_spread.set_xlabel("Time")
    ax_spread.grid(True, alpha=0.3)
    ax_spread.legend(loc="upper left")

    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    ax_spread.xaxis.set_major_locator(locator)
    ax_spread.xaxis.set_major_formatter(formatter)

    last_poll = 0.0

    def update(_frame: int):
        nonlocal last_poll
        now = time.time()
        if now - last_poll < args.interval:
            return line_eth_usd, line_eth_usdt, line_spread

        try:
            eth_usd_price, eth_usd_updated = eth_usd.latest_price()
            usdt_per_eth, usdt_eth_updated = usdt_eth.latest_price()
            eth_usdt_price = 1.0 / usdt_per_eth
        except Exception as exc:
            print(f"[{datetime.utcnow().isoformat()}Z] Fetch error: {exc}")
            return line_eth_usd, line_eth_usdt, line_spread

        ts = datetime.now()
        spread = eth_usd_price - eth_usdt_price

        timestamps.append(ts)
        eth_usd_vals.append(eth_usd_price)
        eth_usdt_vals.append(eth_usdt_price)
        spread_vals.append(spread)

        line_eth_usd.set_data(timestamps, eth_usd_vals)
        line_eth_usdt.set_data(timestamps, eth_usdt_vals)
        line_spread.set_data(timestamps, spread_vals)

        if timestamps:
            ax_prices.set_xlim(timestamps[0], timestamps[-1])
            min_price = min(min(eth_usd_vals), min(eth_usdt_vals))
            max_price = max(max(eth_usd_vals), max(eth_usdt_vals))
            pad_price = max((max_price - min_price) * 0.05, 0.5)
            ax_prices.set_ylim(min_price - pad_price, max_price + pad_price)

            min_spread = min(spread_vals)
            max_spread = max(spread_vals)
            pad_spread = max((max_spread - min_spread) * 0.1, 0.01)
            ax_spread.set_ylim(min_spread - pad_spread, max_spread + pad_spread)

        ax_prices.set_title(
            f"ETH/USD={eth_usd_price:.4f} | ETH/USDT={eth_usdt_price:.4f} | Diff={spread:.6f}"
        )

        print(
            f"{ts.strftime('%H:%M:%S')}  ETH/USD={eth_usd_price:.4f}  "
            f"USDT/ETH={usdt_per_eth:.8f}  ETH/USDT={eth_usdt_price:.4f}  DIFF={spread:.6f}"
        )
        print(
            f"  src1(ETH/USD) update: {fmt_feed_time(eth_usd_updated)} | "
            f"src2(USDT/ETH) update: {fmt_feed_time(usdt_eth_updated)}"
        )
        last_poll = now

        return line_eth_usd, line_eth_usdt, line_spread

    FuncAnimation(fig, update, interval=250, blit=False, cache_frame_data=False)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
