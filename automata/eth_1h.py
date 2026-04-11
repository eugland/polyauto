"""
automata/eth_1h.py

ETH 1H Up/Down tail-capture for Polymarket.

Strategy:
  When one outcome (Up or Down) is priced in the buy zone
  with MIN_MINUTES to MAX_MINUTES remaining in the candle:
    1. Buy BET_SHARES at market ask
    2. Hold position to market resolution (redeem flow)

  Entry threshold uses a Brownian Bridge-inspired formula:
    min_bid = 1 - 0.09 / sqrt(mins_remaining)
    T-20: 0.980+   T-10: 0.972+   T-3: 0.948+
  k=0.09 is self-calibrated from trade history when >= 10 outcomes exist.

  Stop-loss only applies when redeem-only mode is disabled.

  The probability naturally decays toward 1.0 as the candle closes.
  Either the sell fills before resolution, or it resolves at $1.00.

  One position per candle.  Sizing: 20 shares or balance * 0.9 if short.

Run:  python -m automata.eth_1h
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger("automata.eth_1h")


def _init_file_logging() -> None:
    global _file_log_initialized
    if _file_log_initialized:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    log.addHandler(fh)
    _file_log_initialized = True


# ── Parameters ─────────────────────────────────────────────────────────────────

SELL_TARGET  = 0.99    # used only when redeem-only mode is disabled
STOP_LOSS    = 0.60    # exit immediately if position bid drops below this
BET_SHARES   = 20      # target shares per trade
K_DEFAULT    = 0.09   # Brownian Bridge k — auto-calibrated when >= 10 outcomes exist
MIN_MINUTES  = 0       # allow entry from expiry up to MAX_MINUTES remaining
MAX_MINUTES  = 7       # only enter in the last 7 minutes before expiry
REDEEM_ONLY_MODE = True  # hold to resolution/redeem; no TP sell placement

DB_PATH  = Path(__file__).resolve().parent.parent / "bets.db"
LOG_DIR  = Path(__file__).resolve().parent.parent / "experiment" / "logs"
LOG_FILE = LOG_DIR / "eth_1h.log"

_file_log_initialized = False

MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
ET_OFFSET = timedelta(hours=-4)   # EDT (UTC-4)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

GAMMA_API = "https://gamma-api.polymarket.com/events?slug={slug}"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 137
POLYGON_RPC_FALLBACKS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

# ── In-memory position state ───────────────────────────────────────────────────

_position: dict = {
    "active":        False,
    "slug":          None,
    "direction":     None,
    "token_id":      None,
    "sell_order_id": None,
    "shares":        0,
    "entry_price":   None,
    "fee_rate_bps":  0,
}

CTF_ABI = [
    {
        "inputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "bytes32", "name": "", "type": "bytes32"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


def _reset_position() -> None:
    global _position
    _position = {k: None for k in _position}
    _position["active"] = False
    _position["shares"] = 0
    _position["fee_rate_bps"] = 0


def _fetch_event_by_slug(slug: str) -> dict | None:
    try:
        data = _get(GAMMA_API.format(slug=slug))
        return data[0] if isinstance(data, list) and data else None
    except Exception:
        return None


def _get_web3_and_ctf(rpc_url: str | None = None):
    from web3 import Web3

    rpc_candidates = [rpc_url] if rpc_url else []
    rpc_candidates.extend(POLYGON_RPC_FALLBACKS)
    last_err = None
    for url in [u for u in rpc_candidates if u]:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
            _ = w3.eth.block_number
            ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
            return w3, ctf
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"No working Polygon RPC for redeem: {last_err}")


def _as_bytes32(hex_value: str) -> bytes:
    h = (hex_value or "").lower().replace("0x", "")
    if len(h) != 64:
        raise ValueError(f"Invalid bytes32 hex length: {hex_value}")
    return bytes.fromhex(h)


def _resolved_side_from_chain(ctf, condition_id: str) -> str | None:
    cid = _as_bytes32(condition_id)
    denom = int(ctf.functions.payoutDenominator(cid).call())
    if denom <= 0:
        return None
    n0 = int(ctf.functions.payoutNumerators(cid, 0).call())
    n1 = int(ctf.functions.payoutNumerators(cid, 1).call())
    if n0 > n1:
        return "up"
    if n1 > n0:
        return "down"
    return "invalid"


def _redeem_condition_positions(private_key: str, condition_id: str, rpc_url: str | None = None) -> str | None:
    from web3 import Web3

    w3, ctf = _get_web3_and_ctf(rpc_url)
    account = w3.eth.account.from_key(private_key)
    cid = _as_bytes32(condition_id)
    try:
        tx = ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_E_ADDRESS),
            b"\x00" * 32,
            cid,
            [1, 2],
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
        log.warning("[eth_1h] redeemPositions failed for %s: %s", condition_id, exc)
        return None


def _redeem_condition_positions_relayer(private_key: str, condition_id: str, relayer_url: str | None = None) -> str | None:
    relayer_base = relayer_url or os.getenv("POLYMARKET_RELAYER_URL") or "https://relayer-v2.polymarket.com"
    relayer_key = os.getenv("RELAYER_API_KEY")
    relayer_addr = os.getenv("RELAYER_API_KEY_ADDRESS")

    if relayer_key and relayer_addr:
        try:
            import requests
            from py_builder_relayer_client.builder.safe import build_safe_transaction_request
            from py_builder_relayer_client.config import get_contract_config
            from py_builder_relayer_client.models import SafeTransaction, OperationType, SafeTransactionArgs, TransactionType
            from py_builder_relayer_client.signer import Signer
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
                "redeemPositions",
                args=[
                    Web3.to_checksum_address(USDC_E_ADDRESS),
                    b"\x00" * 32,
                    _as_bytes32(condition_id),
                    [1, 2],
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
                metadata="redeem positions",
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
                        st = arr[0].get("state")
                        h = arr[0].get("transactionHash")
                        if h:
                            return h
                        if st in ("STATE_FAILED", "STATE_INVALID"):
                            break
            return None
        except Exception as exc:
            log.warning("[eth_1h] relayer API-key redeem failed for %s: %s", condition_id, exc)

    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
        from web3 import Web3
    except Exception as exc:
        log.warning("[eth_1h] relayer SDK unavailable: %s", exc)
        return None

    key = os.getenv("POLY_BUILDER_API_KEY")
    secret = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not key or not secret or not passphrase:
        log.warning("[eth_1h] Missing builder creds for relayer redeem (POLY_BUILDER_API_KEY/SECRET/PASSPHRASE)")
        return None

    try:
        builder_cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))
        client = RelayClient(relayer_base, POLYGON_CHAIN_ID, private_key=private_key, builder_config=builder_cfg)
        _, ctf = _get_web3_and_ctf(os.getenv("POLYGON_RPC_URL"))
        data = ctf.encode_abi(
            "redeemPositions",
            args=[
                Web3.to_checksum_address(USDC_E_ADDRESS),
                b"\x00" * 32,
                _as_bytes32(condition_id),
                [1, 2],
            ],
        )
        tx = SafeTransaction(
            to=Web3.to_checksum_address(CTF_ADDRESS),
            operation=OperationType.Call,
            data=data,
            value="0",
        )
        resp = client.execute([tx], "redeem positions")
        waited = resp.wait()
        tx_hash = waited.get("transactionHash") if isinstance(waited, dict) else None
        if not tx_hash:
            tx_hash = getattr(resp, "transaction_hash", None)
        return tx_hash
    except Exception as exc:
        log.warning("[eth_1h] relayer redeem failed for %s: %s", condition_id, exc)
        return None


# ── Slug builder ───────────────────────────────────────────────────────────────

def current_et() -> datetime:
    return datetime.now(timezone(ET_OFFSET))


def build_slug(dt: datetime) -> str:
    """ethereum-up-or-down-april-5-2026-3pm-et"""
    month = MONTH_NAMES[dt.month]
    h24   = dt.hour
    h12   = h24 % 12 or 12
    return f"ethereum-up-or-down-{month}-{dt.day}-{dt.year}-{h12}{'am' if h24 < 12 else 'pm'}-et"


# ── Market fetch ───────────────────────────────────────────────────────────────

def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _realized_annual_vol(symbol: str = "ETHUSDT", lookback_hours: int = 168) -> float | None:
    """
    Estimate annualized volatility from hourly log returns.
    """
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={lookback_hours}"
        )
    except Exception:
        return None
    if not isinstance(klines, list) or len(klines) < 3:
        return None
    closes: list[float] = []
    for row in klines:
        try:
            closes.append(float(row[4]))
        except Exception:
            continue
    if len(closes) < 3:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    hourly_vol = math.sqrt(max(var, 0.0))
    return hourly_vol * math.sqrt(365.0 * 24.0)


def _fetch_eth_volume_stats(symbol: str = "ETHUSDT", lookback_hours: int = 24) -> tuple[float | None, float | None, float | None]:
    """
    Returns (current_hour_base_volume, current_hour_quote_volume, avg_quote_volume_lookback).
    """
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={max(2, lookback_hours + 1)}"
        )
    except Exception:
        return None, None, None
    if not isinstance(klines, list) or len(klines) < 2:
        return None, None, None
    try:
        current = klines[-1]
        current_base = float(current[5])  # base asset volume (ETH)
        current_quote = float(current[7])  # quote asset volume (USDT)
    except Exception:
        return None, None, None
    prev = klines[:-1][-lookback_hours:]
    qvs: list[float] = []
    for row in prev:
        try:
            qvs.append(float(row[7]))
        except Exception:
            continue
    avg_quote = (sum(qvs) / len(qvs)) if qvs else None
    return current_base, current_quote, avg_quote


def _black_scholes_digital_up_prob(spot: float, strike: float, years_to_expiry: float, sigma: float, r: float = 0.0) -> float | None:
    """
    Risk-neutral probability P(S_T >= K) for a cash-or-nothing digital call.
    """
    if spot <= 0 or strike <= 0 or years_to_expiry <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(years_to_expiry)
    d2 = (math.log(spot / strike) + (r - 0.5 * sigma * sigma) * years_to_expiry) / (sigma * sqrt_t)
    return min(1.0, max(0.0, _normal_cdf(d2)))


def _fetch_eth_bs_fair(
    end_utc: datetime | None,
    mins_remaining: int | None,
    strike: float | None = None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """
    Returns (fair_up, fair_down, spot, strike) from Black-Scholes,
    or (None, None, None, None) if unavailable.

    strike – candle open price.  When provided (from eventMetadata.priceToBeat)
             the redundant Binance kline fetch is skipped entirely.
    """
    if not end_utc or mins_remaining is None:
        return None, None, None, None
    try:
        ticker = _get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
        spot = float(ticker["price"])
    except Exception:
        return None, None, None, None

    if strike is None:
        # Fallback: derive from Binance kline open (less reliable than metadata)
        try:
            start_utc = end_utc - timedelta(hours=1)
            start_ms  = int(start_utc.timestamp() * 1000)
            kline     = _get(
                f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1h&startTime={start_ms}&limit=1"
            )
            strike = float(kline[0][1]) if isinstance(kline, list) and kline else None
        except Exception:
            strike = None
    if strike is None:
        return None, None, None, None

    sigma = _realized_annual_vol("ETHUSDT", 168)
    if sigma is None or sigma <= 0:
        return None, None, None, None

    # Keep fair probabilistic even at/after 0 min by flooring T to a tiny value.
    effective_mins = max(float(mins_remaining), 1.0 / 60.0)  # 1 second
    years = effective_mins / (365.0 * 24.0 * 60.0)
    fair_up = _black_scholes_digital_up_prob(spot=spot, strike=strike, years_to_expiry=years, sigma=sigma, r=0.0)
    if fair_up is None:
        return None, None, None, None
    fair_down = 1.0 - fair_up
    return fair_up, fair_down, spot, strike


def _fetch_1m_momentum(symbol: str = "ETHUSDT", lookback: int = 7) -> dict | None:
    """
    Fetch the last `lookback+1` 1-minute klines and compute momentum metrics.

    Returns dict:
      taker_ratio    – taker buy vol / total vol for last completed candle
                       (0 = all sellers, 1 = all buyers)
      consecutive_dir – +N consecutive up candles, -N consecutive down candles
      vol_accel      – current-minute projected vol rate vs prior N-minute average
      trade_count    – number of trades in last completed 1m candle
      sigma_1m       – realized 1m return std-dev as a fraction (e.g. 0.001 = 0.1%)
    or None on any fetch failure.
    """
    try:
        klines = _get(
            f"https://api.binance.com/api/v3/klines"
            f"?symbol={symbol}&interval=1m&limit={lookback + 1}"
        )
    except Exception:
        return None

    if not isinstance(klines, list) or len(klines) < 3:
        return None

    completed = klines[:-1]   # fully closed 1m candles
    current   = klines[-1]    # current open (incomplete) candle

    # ── Taker ratio (last completed candle) ───────────────────────────────────
    try:
        last       = completed[-1]
        base_vol   = float(last[5])
        taker_buy  = float(last[9])
        taker_ratio = taker_buy / base_vol if base_vol > 0 else 0.5
    except Exception:
        taker_ratio = 0.5

    # ── Trade count (last completed candle) ───────────────────────────────────
    try:
        trade_count = int(completed[-1][8])
    except Exception:
        trade_count = None

    # ── Consecutive direction ─────────────────────────────────────────────────
    # Walks backwards through completed candles; stops at first direction change or doji.
    consecutive = 0
    try:
        for k in reversed(completed):
            o, c = float(k[1]), float(k[4])
            if c > o:
                if consecutive >= 0:
                    consecutive += 1
                else:
                    break
            elif c < o:
                if consecutive <= 0:
                    consecutive -= 1
                else:
                    break
            else:
                break   # doji — stop streak
    except Exception:
        consecutive = 0

    # ── Volume acceleration ───────────────────────────────────────────────────
    # Projects the current (incomplete) minute's volume to a full minute
    # and compares against the average of the last 5 completed minutes.
    vol_accel = None
    try:
        open_ms    = int(current[0])
        now_ms     = int(time.time() * 1000)
        elapsed_ms = max(now_ms - open_ms, 1_000)          # floor at 1 s
        projected  = float(current[5]) * (60_000 / elapsed_ms)
        prior_vols = [float(k[5]) for k in completed[-min(5, len(completed)):]]
        avg_prior  = sum(prior_vols) / len(prior_vols) if prior_vols else None
        if avg_prior and avg_prior > 0:
            vol_accel = projected / avg_prior
    except Exception:
        pass

    # ── 1-minute realized sigma ───────────────────────────────────────────────
    sigma_1m = None
    try:
        closes = [float(k[4]) for k in completed]
        if len(closes) >= 3:
            rets = [
                math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i - 1] > 0 and closes[i] > 0
            ]
            if len(rets) >= 2:
                mean     = sum(rets) / len(rets)
                var      = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
                sigma_1m = math.sqrt(max(var, 0.0))
    except Exception:
        pass

    return {
        "taker_ratio":     taker_ratio,
        "consecutive_dir": consecutive,
        "vol_accel":       vol_accel,
        "trade_count":     trade_count,
        "sigma_1m":        sigma_1m,
    }


def _assess_reversal_risk(
    gap_pct: float,
    mins_remaining: int,
    direction: str,
    taker_ratio: float,
    consecutive_dir: int,
    sigma_1m: float | None,
    vol_accel: float | None,
) -> dict:
    """
    Assess whether adverse momentum can realistically overturn the current price gap.

    gap_pct         – |spot - candle_open| / candle_open (always positive fraction)
    direction       – "Up" or "Down" (bet direction)
    taker_ratio     – 0=all sellers active, 1=all buyers active (last completed 1m)
    consecutive_dir – +N = N consecutive up candles, -N = N consecutive down candles
    sigma_1m        – realized 1m return std-dev as fraction; None → use fallback
    vol_accel       – projected current-min vol / prior-min average; None → ignored

    Returns dict with:
      adverse_taker       – taker pressure in the adverse direction (0–1)
      consecutive_adverse – consecutive candles moving against the bet
      adverse_multiplier  – total scaling factor applied to sigma (1.0–5.0)
      expected_adverse_pct – expected adverse move in % over remaining time
      gap_pct             – input gap (as fraction)
      gap_safety          – gap_pct / expected_adverse_move  (>2.0 = safe, <1.5 = skip)
      safe                – True only when no skip reasons triggered
      skip_reasons        – list of reason strings; empty when safe
    """
    # Adverse direction: buyers fight a Down bet; sellers fight an Up bet.
    if direction == "Down":
        adverse_taker       = taker_ratio
        consecutive_adverse = max(0, consecutive_dir)   # up candles are adverse
    else:
        adverse_taker       = 1.0 - taker_ratio
        consecutive_adverse = max(0, -consecutive_dir)  # down candles are adverse

    # ── Adverse multiplier components ─────────────────────────────────────────
    # Taker pressure: neutral=0.5 → 0 extra; fully adverse=1.0 → +2.0
    pressure  = max(0.0, adverse_taker - 0.5) * 4.0        # 0 – 2.0

    # Consecutive adverse candles: each adds 0.5, capped at 1.5
    momentum  = min(consecutive_adverse * 0.5, 1.5)        # 0 – 1.5

    # Volume surge on the adverse side
    vol_extra = 0.0
    if vol_accel is not None and vol_accel > 1.5:
        vol_extra = min((vol_accel - 1.5) * 0.3, 0.5)     # 0 – 0.5

    adverse_multiplier = 1.0 + pressure + momentum + vol_extra   # 1.0 – 5.0

    # ── Gap safety ────────────────────────────────────────────────────────────
    SIGMA_FALLBACK = 0.001   # 0.1% per minute — conservative ETH default
    sigma = sigma_1m if (sigma_1m and sigma_1m > 0) else SIGMA_FALLBACK
    expected_adverse_move = sigma * math.sqrt(max(mins_remaining, 1)) * adverse_multiplier
    gap_safety = (
        gap_pct / expected_adverse_move
        if expected_adverse_move > 0 else float("inf")
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    SKIP_GAP_SAFETY        = 1.2   # loosened: allow entries unless reversal is within ~1.2 sigma reach
    SKIP_ADVERSE_STREAK    = 5     # loosened: require a longer adverse streak before skipping
    SKIP_ADVERSE_TAKER_MAX = 0.75  # loosened: tolerate stronger adverse taker pressure

    skip_reasons = []
    if gap_safety < SKIP_GAP_SAFETY:
        skip_reasons.append(f"gap_safety={gap_safety:.2f}<{SKIP_GAP_SAFETY}")
    if consecutive_adverse >= SKIP_ADVERSE_STREAK:
        skip_reasons.append(f"adverse_streak={consecutive_adverse}>={SKIP_ADVERSE_STREAK}")
    if adverse_taker > SKIP_ADVERSE_TAKER_MAX:
        skip_reasons.append(f"adverse_taker={adverse_taker:.2f}>{SKIP_ADVERSE_TAKER_MAX}")

    return {
        "adverse_taker":        adverse_taker,
        "consecutive_adverse":  consecutive_adverse,
        "adverse_multiplier":   adverse_multiplier,
        "expected_adverse_pct": expected_adverse_move * 100,
        "gap_pct":              gap_pct,
        "gap_safety":           gap_safety,
        "safe":                 len(skip_reasons) == 0,
        "skip_reasons":         skip_reasons,
    }


def _fmt_matrix_cell(v: float | None) -> str:
    return f"{v * 100:.1f}" if v is not None else "n/a"


def _print_price_matrix(up_ask: float | None, down_ask: float | None, up_bid: float | None, down_bid: float | None, fair_up: float | None, fair_down: float | None) -> None:
    print("              Up     Down")
    print(f"  ask       {_fmt_matrix_cell(up_ask):>6}  {_fmt_matrix_cell(down_ask):>6}")
    print(f"  bid       {_fmt_matrix_cell(up_bid):>6}  {_fmt_matrix_cell(down_bid):>6}")
    print(f"  fair      {_fmt_matrix_cell(fair_up):>6}  {_fmt_matrix_cell(fair_down):>6}")


def _settle_resolved_trades(private_key: str | None, rpc_url: str | None = None, redeem_mode: str = "relayer") -> None:
    from automata.eth_15m import (
        _get_web3_and_ctf as _get_web3_and_ctf_15m,
        _redeem_condition_positions as _redeem_condition_positions_15m,
        _redeem_condition_positions_relayer as _redeem_condition_positions_relayer_15m,
        _resolved_side_from_chain as _resolved_side_from_chain_15m,
    )

    _init_table()
    if not private_key:
        log.warning("[eth_1h] POLYMARKET_PRIVATE_KEY missing; cannot redeem")
        return

    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        log.warning("[eth_1h] POLYMARKET_FUNDER missing; cannot scan positions for redeem")
        return

    from automata.client import get_positions
    positions = get_positions(funder)
    if not positions:
        return

    open_token_ids = {str(p.get("token_id")) for p in positions if p.get("token_id")}
    if not open_token_ids:
        return

    placeholders = ",".join("?" for _ in open_token_ids)
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT token_id, slug, condition_id
            FROM eth_1h_trades
            WHERE COALESCE(dry_run, 0) = 0
              AND token_id IN ({placeholders})
              AND redeemed_at IS NULL
            """,
            tuple(open_token_ids),
        ).fetchall()

    if not rows:
        return

    enriched: list[tuple[str, str, str]] = []
    for token_id, slug, condition_id in rows:
        cid = str(condition_id or "")
        if not cid:
            event = _fetch_event_by_slug(str(slug))
            if event and event.get("markets"):
                cid = str(event["markets"][0].get("conditionId") or "")
                if cid:
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute(
                            "UPDATE eth_1h_trades SET condition_id = ? WHERE slug = ?",
                            (cid, slug),
                        )
        if cid:
            enriched.append((str(token_id), str(slug), cid))

    if not enriched:
        return

    _, ctf = _get_web3_and_ctf_15m(rpc_url)

    condition_to_slugs: dict[str, set[str]] = {}
    for _, slug, cid in enriched:
        condition_to_slugs.setdefault(cid, set()).add(slug)

    for cid, slugs in condition_to_slugs.items():
        side = _resolved_side_from_chain_15m(ctf, str(cid))
        if side is None:
            continue
        if redeem_mode == "relayer":
            tx_hash = _redeem_condition_positions_relayer_15m(private_key=private_key, condition_id=str(cid))
        else:
            tx_hash = _redeem_condition_positions_15m(private_key=private_key, condition_id=str(cid), rpc_url=rpc_url)
        if not tx_hash:
            log.warning("[eth_1h] Redeem submit failed for condition=%s side=%s", cid, side)
            continue
        now_iso = datetime.now(timezone.utc).isoformat()
        for slug in slugs:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE eth_1h_trades
                    SET redeemed_at = ?, redeem_tx_hash = ?
                    WHERE slug = ? AND COALESCE(dry_run, 0) = 0 AND redeemed_at IS NULL
                    """,
                    (now_iso, tx_hash, slug),
                )
                if side in ("up", "down"):
                    conn.execute(
                        """
                        UPDATE eth_1h_trades
                        SET outcome = CASE
                            WHEN lower(direction) = ? THEN 'win'
                            ELSE 'loss'
                        END
                        WHERE slug = ? AND COALESCE(dry_run, 0) = 0 AND outcome IS NULL
                        """,
                        (side, slug),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE eth_1h_trades
                        SET outcome = 'invalid'
                        WHERE slug = ? AND COALESCE(dry_run, 0) = 0 AND outcome IS NULL
                        """,
                        (slug,),
                    )
            log.info("[eth_1h] Settled slug=%s side=%s redeem_tx=%s", slug, side, tx_hash or "n/a")


def fetch_and_parse(slug: str) -> dict | None:
    """
    Returns dict: slug, title, up_token, down_token, minutes_remaining, end_utc,
    condition_id, maker_fee_bps
    or None if market is closed / not found.
    """
    try:
        data = _get(GAMMA_API.format(slug=slug))
        event = data[0] if data else None
    except (URLError, json.JSONDecodeError, IndexError):
        return None

    if not event or not event.get("markets"):
        return None
    m = event["markets"][0]
    if m.get("closed") or (m.get("active") is not None and not m.get("active")):
        return None

    def _load(key):
        v = m.get(key, [])
        return json.loads(v) if isinstance(v, str) else v

    outcomes  = _load("outcomes")
    token_ids = _load("clobTokenIds")

    up_token = down_token = None
    for i, name in enumerate(outcomes):
        nl = name.strip().lower()
        if nl == "up"   and i < len(token_ids): up_token   = str(token_ids[i])
        if nl == "down" and i < len(token_ids): down_token = str(token_ids[i])

    if not up_token or not down_token:
        return None

    end_str = m.get("endDate") or event.get("endDate") or ""
    end_utc = None
    try:
        dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    now_utc = datetime.now(timezone.utc)
    minutes_remaining = int((end_utc - now_utc).total_seconds() / 60) if end_utc else 0

    # Official candle-open price used by Polymarket for resolution
    price_to_beat: float | None = None
    try:
        ptb = (event.get("eventMetadata") or {}).get("priceToBeat")
        if ptb is not None:
            price_to_beat = float(ptb)
    except (TypeError, ValueError):
        pass

    maker_fee_bps = 0
    try:
        maker_fee_bps = max(0, int(m.get("makerBaseFee") or 0))
    except (TypeError, ValueError):
        maker_fee_bps = 0

    return {
        "slug":              event.get("slug", slug),
        "title":             event.get("title", ""),
        "up_token":          up_token,
        "down_token":        down_token,
        "minutes_remaining": minutes_remaining,
        "end_utc":           end_utc,
        "condition_id":      str(m.get("conditionId") or ""),
        "price_to_beat":     price_to_beat,
        "maker_fee_bps":     maker_fee_bps,
    }


def get_books(host: str, token_ids: list[str]) -> dict[str, dict]:
    """Returns {token_id: {bid, ask}} via bulk /books."""
    import requests
    try:
        resp = requests.post(
            f"{host}/books",
            json=[{"token_id": tid} for tid in token_ids],
            timeout=8,
        )
        resp.raise_for_status()
        result = {}
        for book in resp.json():
            tid = book.get("asset_id") or book.get("token_id")
            if not tid:
                continue
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            result[str(tid)] = {
                "bid": max(float(b["price"]) for b in bids) if bids else None,
                "ask": min(float(a["price"]) for a in asks) if asks else None,
            }
        return result
    except Exception:
        return {}


# ── SQLite trade tracking ──────────────────────────────────────────────────────

def _init_table() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eth_1h_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                slug           TEXT NOT NULL,
                direction      TEXT NOT NULL,
                token_id       TEXT NOT NULL,
                buy_order      TEXT,
                sell_order     TEXT,
                shares         REAL NOT NULL,
                entry_price    REAL NOT NULL,
                sell_target    REAL NOT NULL,
                cost_usdc      REAL NOT NULL,
                placed_at      TEXT NOT NULL,
                mins_remaining REAL,
                condition_id   TEXT,
                redeem_tx_hash TEXT,
                redeemed_at    TEXT,
                dry_run        INTEGER NOT NULL DEFAULT 0,
                outcome        TEXT
            )
        """)
        # Migrate existing rows that may lack the new columns
        existing = {r[1] for r in conn.execute("PRAGMA table_info(eth_1h_trades)")}
        if "mins_remaining" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN mins_remaining REAL")
        if "condition_id" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN condition_id TEXT")
        if "redeem_tx_hash" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN redeem_tx_hash TEXT")
        if "redeemed_at" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN redeemed_at TEXT")
        if "dry_run" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0")
        if "outcome" not in existing:
            conn.execute("ALTER TABLE eth_1h_trades ADD COLUMN outcome TEXT")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eth_1h_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO eth_1h_settings (key, value) VALUES ('k', ?)",
            (str(K_DEFAULT),)
        )


def _get_k() -> float:
    _init_table()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT value FROM eth_1h_settings WHERE key = 'k'"
        ).fetchone()
    return float(row[0]) if row else K_DEFAULT


def _set_k(k: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO eth_1h_settings (key, value) VALUES ('k', ?)",
            (str(round(k, 2)),)
        )
    log.info("[eth_1h] k updated to %.2f in DB", k)


def _has_trade(slug: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM eth_1h_trades WHERE slug = ? AND COALESCE(dry_run, 0) = 0", (slug,)
        ).fetchone()
    return row is not None


def _record_trade(slug, direction, token_id, buy_order, sell_order,
                  shares, entry_price, sell_target, cost_usdc,
                  mins_remaining: float | None = None,
                  condition_id: str | None = None,
                  dry_run: bool = False) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO eth_1h_trades
               (slug, direction, token_id, buy_order, sell_order,
                shares, entry_price, sell_target, cost_usdc, placed_at,
                mins_remaining, condition_id, dry_run, outcome)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (slug, direction, token_id, buy_order, sell_order,
             shares, entry_price, sell_target, cost_usdc,
             datetime.now(timezone.utc).isoformat(),
             mins_remaining, condition_id, 1 if dry_run else 0),
        )


def _update_outcome(slug: str, outcome: str) -> None:
    """Set outcome for the trade with this slug (win / stop_loss / expired)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE eth_1h_trades SET outcome = ? WHERE slug = ? AND outcome IS NULL AND COALESCE(dry_run, 0) = 0",
            (outcome, slug),
        )
    log.info("[eth_1h] outcome recorded: %s → %s", slug[-24:], outcome)


def _calibrate_k() -> float:
    """
    Grid-search the best k for min_bid = 1 - k / sqrt(mins).
    Uses resolved trades (outcome = 'win' or 'stop_loss') from the DB.
    Auto-saves the best k to the DB when >= 10 outcomes exist.
    Returns the current k (updated or unchanged).
    """
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT entry_price, mins_remaining, outcome
               FROM eth_1h_trades
               WHERE outcome IN ('win', 'stop_loss')
               AND COALESCE(dry_run, 0) = 0
               AND mins_remaining IS NOT NULL"""
        ).fetchall()

    current_k = _get_k()

    if len(rows) < 10:
        log.info("[eth_1h] calibration: only %d resolved trades — need 10, using k=%.2f",
                 len(rows), current_k)
        return current_k

    best_k, best_score = current_k, -1.0
    for k in [round(0.06 + 0.01 * i, 2) for i in range(15)]:  # 0.06 .. 0.20
        taken = [(ep, out) for ep, mins, out in rows
                 if mins and ep >= 1 - k / math.sqrt(mins)]
        if not taken:
            continue
        win_rate = sum(1 for _, out in taken if out == "win") / len(taken)
        score = win_rate * math.log1p(len(taken))
        if score > best_score:
            best_score, best_k = score, k

    wins   = sum(1 for _, _, o in rows if o == "win")
    losses = sum(1 for _, _, o in rows if o == "stop_loss")
    log.info(
        "[eth_1h] calibration: %d resolved trades (%d W / %d L) — "
        "best k=%.2f (score=%.3f)  previous k=%.2f",
        len(rows), wins, losses, best_k, best_score, current_k,
    )
    if best_k != current_k:
        _set_k(best_k)
    return best_k


# ── Main entry point ───────────────────────────────────────────────────────────

def run_eth_1h(
    dry_run: bool = True,
    host: str = "https://clob.polymarket.com",
    max_spend_usdc: float | None = None,
    bet_shares: float = BET_SHARES,
) -> None:
    """
    Called every 10 s.  Monitors open position for stop-loss,
    then looks for a new entry if none is active.
    """
    _init_table()
    _init_file_logging()

    now_et       = current_et()
    candle_start = now_et.replace(minute=0, second=0, microsecond=0)
    slug         = build_slug(candle_start)

    # ── Monitor open position ──────────────────────────────────────────────────
    if _position["active"]:
        if _position["slug"] != slug:
            # New candle crossed; outcome for redeem mode is settled by chain-based redeem flow.
            prev_slug = _position["slug"] or ""
            log.info("[eth_1h] New candle — position on %s resolved, resetting",
                     prev_slug[-24:])
            if not REDEEM_ONLY_MODE:
                _update_outcome(prev_slug, "expired")
            _reset_position()
            if not REDEEM_ONLY_MODE:
                _calibrate_k()
        else:
            # Same candle — check stop-loss and win detection
            books    = get_books(host, [_position["token_id"]])
            cur_bid  = books.get(_position["token_id"], {}).get("bid")

            if not REDEEM_ONLY_MODE and cur_bid is not None and cur_bid >= SELL_TARGET:
                log.info("[eth_1h] WIN detected — bid=%.3f >= sell target %.2f",
                         cur_bid, SELL_TARGET)
                _update_outcome(_position["slug"], "win")
                _reset_position()
                _calibrate_k()
                return

            if cur_bid is not None and cur_bid < STOP_LOSS:
                log.warning("[eth_1h] STOP-LOSS  bid=%.3f < %.2f — exiting %s",
                            cur_bid, STOP_LOSS, _position["direction"])
                if not dry_run:
                    try:
                        from automata.client import build_client, cancel_order, place_market_sell
                        client = build_client(
                            host=os.environ["POLYMARKET_HOST"],
                            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
                            api_key=os.environ["CLOB_API_KEY"],
                            api_secret=os.environ["CLOB_SECRET"],
                            api_passphrase=os.environ["CLOB_PASS"],
                            funder=os.getenv("POLYMARKET_FUNDER") or None,
                            signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
                        )
                        if _position["sell_order_id"] and _position["sell_order_id"] != "?":
                            cancel_order(client, _position["sell_order_id"])
                        exit_price = max(round(cur_bid - 0.01, 2), 0.01)
                        place_market_sell(
                            client,
                            _position["token_id"],
                            exit_price,
                            _position["shares"],
                            fee_rate_bps=int(_position.get("fee_rate_bps") or 0),
                        )
                        log.info("[eth_1h] Stop-loss sell placed @ %.3f", exit_price)
                    except Exception as exc:
                        log.error("[eth_1h] Stop-loss exit failed: %s", exc)
                else:
                    log.info("[eth_1h] [DRY RUN] Would stop-loss exit @ ~%.3f", cur_bid)
                _update_outcome(_position["slug"], "stop_loss")
                _reset_position()
                _calibrate_k()
            else:
                pnl_pct = ((cur_bid - _position["entry_price"]) / _position["entry_price"] * 100) if cur_bid and _position["entry_price"] else None
                log.info(
                    "[eth_1h] HOLD  direction=%s  bid=%s  entry=%.3f  pnl=%s  shares=%.2f  slug=%s",
                    _position["direction"],
                    f"{cur_bid:.3f}" if cur_bid else "n/a",
                    _position["entry_price"],
                    f"{pnl_pct:+.2f}%" if pnl_pct is not None else "n/a",
                    _position["shares"],
                    (_position["slug"] or "")[-28:],
                )
            return

    # ── Entry logic ────────────────────────────────────────────────────────────

    # Already traded this candle (from a previous process run)?
    if _has_trade(slug):
        log.info("[eth_1h] Already traded %s — skip", slug[-24:])
        return

    target_shares = bet_shares if bet_shares and bet_shares > 0 else BET_SHARES

    mkt = fetch_and_parse(slug)
    if not mkt:
        log.info("[eth_1h] Market not found or closed: %s", slug)
        return

    mins = mkt["minutes_remaining"]
    if not (MIN_MINUTES <= mins <= MAX_MINUTES):
        log.info("[eth_1h] %d min remaining — outside entry window [%d-%d]",
                 mins, MIN_MINUTES, MAX_MINUTES)
        return

    k = _calibrate_k()

    # Time-adjusted minimum bid (Brownian Bridge): stricter earlier in the window
    mins_for_bid = max(mins, 1)
    min_bid = round(1 - k / mins_for_bid ** 0.5, 3)

    # Live order book
    books     = get_books(host, [mkt["up_token"], mkt["down_token"]])
    up_book   = books.get(mkt["up_token"],   {})
    down_book = books.get(mkt["down_token"], {})

    up_ask   = up_book.get("ask")
    down_ask = down_book.get("ask")
    up_bid   = up_book.get("bid")
    down_bid = down_book.get("bid")

    # ── Print current state ────────────────────────────────────────────────────
    div = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 1H TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Time remaining: {mins} min  (window {MIN_MINUTES}-{MAX_MINUTES}min)")
    fair_up, fair_down, spot, strike = _fetch_eth_bs_fair(mkt.get("end_utc"), mins, strike=mkt.get("price_to_beat"))
    vol_base, vol_quote, vol_quote_avg = _fetch_eth_volume_stats("ETHUSDT", 24)
    _print_price_matrix(up_ask, down_ask, up_bid, down_bid, fair_up, fair_down)
    print(f"  Min bid (T-{mins}m): {min_bid:.3f}  |  sell: {SELL_TARGET:.2f}")
    vq = f"{vol_quote:,.0f}" if vol_quote is not None else "n/a"
    vqa = f"{vol_quote_avg:,.0f}" if vol_quote_avg is not None else "n/a"
    vb = f"{vol_base:,.2f}" if vol_base is not None else "n/a"
    print(f"  Binance 1h volume: {vb} ETH / {vq} USDT  (24h avg quote vol: {vqa} USDT)")
    if spot is not None and strike is not None:
        pct = (spot - strike) / strike * 100
        src = "metadata" if mkt.get("price_to_beat") else "binance-kline"
        print(f"  ETH spot: ${spot:,.2f}  |  price to beat: ${strike:,.2f} ({src})  |  Δopen: {pct:+.3f}%")

    # ── Compute per-direction edge ─────────────────────────────────────────────
    edge_up   = (fair_up   - up_ask)   if fair_up   is not None and up_ask   is not None else None
    edge_down = (fair_down - down_ask) if fair_down is not None and down_ask is not None else None

    # Best edge direction (for logging, regardless of whether it meets threshold)
    best_edge_dir = None
    if edge_up is not None and edge_down is not None:
        best_edge_dir = "Up" if edge_up >= edge_down else "Down"
    elif edge_up is not None:
        best_edge_dir = "Up"
    elif edge_down is not None:
        best_edge_dir = "Down"

    pct_from_open = ((spot - strike) / strike) if spot is not None and strike is not None else None

    log.info(
        "[eth_1h] SCAN  slug=%s  mins=%d  "
        "spot=%s  open=%s  pct_open=%s  "
        "up_ask=%s  up_bid=%s  fair_up=%s  edge_up=%s  "
        "down_ask=%s  down_bid=%s  fair_down=%s  edge_down=%s  "
        "min_bid=%.3f  best_edge=%s  vol_ratio=%s",
        slug[-28:], mins,
        f"{spot:.2f}"         if spot          is not None else "n/a",
        f"{strike:.2f}"       if strike        is not None else "n/a",
        f"{pct_from_open:+.4f}" if pct_from_open is not None else "n/a",
        f"{up_ask:.4f}"       if up_ask        is not None else "n/a",
        f"{up_bid:.4f}"       if up_bid        is not None else "n/a",
        f"{fair_up:.4f}"      if fair_up       is not None else "n/a",
        f"{edge_up:+.4f}"     if edge_up       is not None else "n/a",
        f"{down_ask:.4f}"     if down_ask      is not None else "n/a",
        f"{down_bid:.4f}"     if down_bid      is not None else "n/a",
        f"{fair_down:.4f}"    if fair_down     is not None else "n/a",
        f"{edge_down:+.4f}"   if edge_down     is not None else "n/a",
        min_bid,
        f"{best_edge_dir}({(edge_up if best_edge_dir == 'Up' else edge_down):+.4f})" if best_edge_dir else "n/a",
        f"{vol_quote / vol_quote_avg:.2f}x" if vol_quote and vol_quote_avg else "n/a",
    )

    # ── Find entry ─────────────────────────────────────────────────────────────
    candidate = None
    for direction, ask, bid, token in [
        ("Up",   up_ask,   up_bid,   mkt["up_token"]),
        ("Down", down_ask, down_bid, mkt["down_token"]),
    ]:
        if ask is None or bid is None:
            continue
        if bid >= min_bid:
            candidate = {"direction": direction, "ask": ask, "bid": bid, "token": token}
            break

    if not candidate:
        up_str   = f"{up_ask:.3f}"   if up_ask   else "n/a"
        down_str = f"{down_ask:.3f}" if down_ask else "n/a"
        print(f"  --> No entry: Up={up_str}  Down={down_str}  "
              f"(need bid>={min_bid:.3f})")
        log.info("[eth_1h] DECISION  slug=%s  → NO-SIGNAL  up_ask=%s  down_ask=%s  need_bid>=%.3f",
                 slug[-28:], up_str, down_str, min_bid)
        print(f"{div}\n")
        return

    direction  = candidate["direction"]
    ask        = candidate["ask"]
    token      = candidate["token"]
    fee_rate_bps = int(mkt.get("maker_fee_bps") or 0)
    entry_edge = edge_down if direction == "Down" else edge_up
    entry_fair = fair_down if direction == "Down" else fair_up

    # ── 1m momentum + reversal risk ───────────────────────────────────────────
    momentum = _fetch_1m_momentum("ETHUSDT", lookback=7)
    gap_pct  = abs(spot - strike) / strike if spot is not None and strike is not None else None

    risk = None
    if momentum is not None and gap_pct is not None:
        risk = _assess_reversal_risk(
            gap_pct         = gap_pct,
            mins_remaining  = mins,
            direction       = direction,
            taker_ratio     = momentum["taker_ratio"],
            consecutive_dir = momentum["consecutive_dir"],
            sigma_1m        = momentum["sigma_1m"],
            vol_accel       = momentum["vol_accel"],
        )

    if momentum:
        log.info(
            "[eth_1h] MOMENTUM  direction=%s  taker_ratio=%s  consecutive_dir=%+d"
            "  vol_accel=%s  trade_count=%s  sigma_1m=%s",
            direction,
            f"{momentum['taker_ratio']:.3f}",
            momentum["consecutive_dir"],
            f"{momentum['vol_accel']:.2f}x"          if momentum["vol_accel"]  is not None else "n/a",
            str(momentum["trade_count"])              if momentum["trade_count"] is not None else "n/a",
            f"{momentum['sigma_1m'] * 100:.4f}%"     if momentum["sigma_1m"]   is not None else "n/a",
        )
    else:
        log.warning("[eth_1h] MOMENTUM  unavailable — proceeding without momentum filter")

    if risk:
        log.info(
            "[eth_1h] RISK  direction=%s  gap_pct=%.4f%%  gap_safety=%.2f"
            "  adverse_taker=%.3f  adverse_streak=%d  adverse_mult=%.2f"
            "  expected_move=%.4f%%  safe=%s  reasons=%s",
            direction,
            risk["gap_pct"] * 100,
            risk["gap_safety"],
            risk["adverse_taker"],
            risk["consecutive_adverse"],
            risk["adverse_multiplier"],
            risk["expected_adverse_pct"],
            risk["safe"],
            (",".join(risk["skip_reasons"]) or "none"),
        )

    # ── Apply momentum/risk gate ───────────────────────────────────────────────
    if risk is not None and not risk["safe"]:
        skip_str = "  |  ".join(risk["skip_reasons"])
        print(f"  --> SKIP (momentum risk): {skip_str}")
        print(f"{div}\n")
        log.info(
            "[eth_1h] DECISION  slug=%s  → SKIP-RISK  direction=%s  "
            "gap_safety=%s  reasons=%s",
            slug[-28:], direction,
            f"{risk['gap_safety']:.2f}" if risk else "n/a",
            skip_str,
        )
        return

    # ── All filters passed — log entry decision ────────────────────────────────
    safety_str = f"{risk['gap_safety']:.2f}" if risk else "n/a"
    log.info(
        "[eth_1h] DECISION  slug=%s  → ENTER-%s  ask=%.4f  fair=%s  edge=%s"
        "  gap_pct=%s  gap_safety=%s  min_bid=%.3f  mins=%d  spot=%s  open=%s",
        slug[-28:], direction, ask,
        f"{entry_fair:.4f}"    if entry_fair is not None else "n/a",
        f"{entry_edge:+.4f}"   if entry_edge is not None else "n/a",
        f"{gap_pct * 100:.3f}%" if gap_pct   is not None else "n/a",
        safety_str,
        min_bid, mins,
        f"{spot:.2f}"   if spot   is not None else "n/a",
        f"{strike:.2f}" if strike is not None else "n/a",
    )
    log.info("[eth_1h] FEE  maker_fee_bps=%d", fee_rate_bps)

    if dry_run:
        shares = target_shares
        cost   = round(shares * ask, 2)
        edge_str   = f"  edge={entry_edge:+.4f}" if entry_edge is not None else ""
        safety_tag = f"  gap_safety={safety_str}" if risk else ""
        print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
              f"{shares}sh  ${cost:.2f}{edge_str}{safety_tag}"
              f"  mode={'redeem-only' if REDEEM_ONLY_MODE else 'tp-sell'}")
        print(f"{div}\n")
        log.info("[eth_1h] [DRY RUN] Would buy %d %s @ %.3f  $%.2f  mode=%s",
                 shares, direction, ask, cost, "redeem-only" if REDEEM_ONLY_MODE else "tp-sell")
        _record_trade(
            slug, direction, token, "DRY_RUN", "DRY_RUN",
            shares, ask, SELL_TARGET, cost,
            mins_remaining=mins,
            condition_id=str(mkt.get("condition_id") or ""),
            dry_run=True,
        )
        return

    # ── Live: buy then immediately post sell ───────────────────────────────────
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET",
                "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        log.error("[eth_1h] Missing .env keys")
        return

    from automata.client import build_client, get_usdc_balance, place_no_order

    client = build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )

    # Determine shares based on available balance
    try:
        balance = get_usdc_balance(client)
    except Exception as exc:
        log.warning("[eth_1h] Could not fetch balance, defaulting to %.2f shares: %s", target_shares, exc)
        balance = target_shares * ask  # assume enough
    if max_spend_usdc is not None:
        capped = min(balance, max_spend_usdc)
        log.info("[eth_1h] Balance cap enabled: available $%.2f, capped spend $%.2f", balance, capped)
        balance = capped

    if balance >= target_shares:
        shares = target_shares
    else:
        shares = round(balance * 0.9, 2)

    cost = round(shares * ask, 2)
    log.info("[eth_1h] Balance $%.2f — using %.2f shares (target %.2f)", balance, shares, target_shares)

    edge_str   = f"  edge={entry_edge:+.4f}" if entry_edge is not None else ""
    safety_tag = f"  gap_safety={safety_str}" if risk else ""
    print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
          f"{shares}sh  ${cost:.2f}{edge_str}{safety_tag}"
          f"  mode={'redeem-only' if REDEEM_ONLY_MODE else 'tp-sell'}")
    print(f"{div}\n")

    if shares <= 0:
        log.error("[eth_1h] Insufficient balance (%.2f), skipping", balance)
        return

    # Step 1 — buy
    try:
        buy_resp = place_no_order(
            client,
            token,
            ask,
            shares,
            fee_rate_bps=fee_rate_bps,
        )
        buy_id   = buy_resp.get("orderID") or buy_resp.get("id") or "?"
        log.info("[eth_1h] Bought %.2f %s @ %.3f  $%.2f  id=%s",
                 shares, direction, ask, cost, buy_id)
    except Exception as exc:
        log.error("[eth_1h] Buy failed: %s", exc)
        return

    # Step 2 — redeem-only mode: hold position to resolution.
    sell_id = "REDEEM_ONLY"
    if not REDEEM_ONLY_MODE:
        sell_id = "?"

    _record_trade(slug, direction, token, buy_id, sell_id,
                  shares, ask, SELL_TARGET, cost,
                  mins_remaining=mins,
                  condition_id=str(mkt.get("condition_id") or ""),
                  dry_run=False)

    # Track in memory for stop-loss monitoring
    _position.update({
        "active":        True,
        "slug":          slug,
        "direction":     direction,
        "token_id":      token,
        "sell_order_id": sell_id,
        "shares":        shares,
        "entry_price":   ask,
        "fee_rate_bps":  fee_rate_bps,
    })

    print(f"  [eth_1h] BUY {direction} {shares}sh @ {ask:.3f}  ${cost:.2f}"
          f"  buy={buy_id}  sell={sell_id}")


# ── Standalone display ─────────────────────────────────────────────────────────

def analyze(host: str = "https://clob.polymarket.com") -> None:
    _init_table()
    now_et       = current_et()
    candle_start = now_et.replace(minute=0, second=0, microsecond=0)

    # Try current and next hour
    for dt in [candle_start, candle_start + timedelta(hours=1)]:
        slug = build_slug(dt)
        mkt  = fetch_and_parse(slug)
        if mkt:
            break
    else:
        print("No active ETH 1H market found")
        return

    books     = get_books(host, [mkt["up_token"], mkt["down_token"]])
    up_book   = books.get(mkt["up_token"],   {})
    down_book = books.get(mkt["down_token"], {})

    mins = mkt["minutes_remaining"]
    mins_for_bid = max(mins, 1)
    min_bid = round(1 - _get_k() / mins_for_bid ** 0.5, 3)
    div     = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 1H TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Slug:           {mkt['slug']}")
    print(f"  Time remaining: {mins} min")
    fair_up, fair_down, spot, strike = _fetch_eth_bs_fair(mkt.get("end_utc"), mins, strike=mkt.get("price_to_beat"))
    vol_base, vol_quote, vol_quote_avg = _fetch_eth_volume_stats("ETHUSDT", 24)
    _print_price_matrix(
        up_book.get("ask"),
        down_book.get("ask"),
        up_book.get("bid"),
        down_book.get("bid"),
        fair_up,
        fair_down,
    )
    vq = f"{vol_quote:,.0f}" if vol_quote is not None else "n/a"
    vqa = f"{vol_quote_avg:,.0f}" if vol_quote_avg is not None else "n/a"
    vb = f"{vol_base:,.2f}" if vol_base is not None else "n/a"
    print(f"  Binance 1h volume: {vb} ETH / {vq} USDT  (24h avg quote vol: {vqa} USDT)")
    if spot is not None and strike is not None:
        pct = (spot - strike) / strike * 100
        src = "metadata" if mkt.get("price_to_beat") else "binance-kline"
        print(f"  ETH spot: ${spot:,.2f}  |  price to beat: ${strike:,.2f} ({src})  |  Δopen: {pct:+.3f}%")
    min_bid_str = f"{min_bid:.3f}" if min_bid is not None else "n/a"
    print(f"  Min bid (T-{mins}m): {min_bid_str}  |  mode: {'redeem-only' if REDEEM_ONLY_MODE else 'tp-sell'}")
    if not REDEEM_ONLY_MODE:
        print(f"  Stop-loss: {STOP_LOSS:.2f}  |  Entry window: {MIN_MINUTES}-{MAX_MINUTES} min remaining")
    else:
        print(f"  Entry window: {MIN_MINUTES}-{MAX_MINUTES} min remaining")

    in_window = MIN_MINUTES <= mins <= MAX_MINUTES
    print(f"  Window: {'OPEN' if in_window else f'CLOSED ({mins}min)'}")

    for label, book, fair in [("Up", up_book, fair_up), ("Down", down_book, fair_down)]:
        ask = book.get("ask")
        bid = book.get("bid")
        if ask and bid and min_bid is not None and bid >= min_bid and in_window:
            cost  = round(BET_SHARES * ask, 2)
            edge  = (fair - ask) if fair is not None else None
            edge_str = f"  edge={edge:+.4f}" if edge is not None else ""
            print(f"  --> SIGNAL: buy {label} @ {ask:.3f}  "
                  f"{BET_SHARES}sh (balance-adjusted at trade time)  ${cost:.2f}{edge_str}  mode={'redeem-only' if REDEEM_ONLY_MODE else 'tp-sell'}")
    print(f"{div}\n")


if __name__ == "__main__":
    import os, sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    analyze(host=host)
