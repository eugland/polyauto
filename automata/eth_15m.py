"""
automata/eth_15m.py

ETH 15M Up/Down tail-capture for Polymarket.

Strategy:
  When one outcome (Up or Down) is priced in the buy zone
  with MIN_SECONDS to MAX_SECONDS remaining in the candle:
    1. Buy BET_SHARES at market ask
    2. Immediately post a GTC limit sell at SELL_TARGET (0.999)

  Entry threshold uses a Brownian Bridge-inspired formula:
    min_bid = 1 - 0.12 / sqrt(seconds_remaining / 60)
    T-20: 0.973+   T-10: 0.962+   T-3: 0.931+
  k=0.12 is self-calibrated from trade history when >= 10 outcomes exist.

  Stop-loss: if held position bid drops below STOP_LOSS (0.75),
  cancel the sell and exit immediately at market.

  The probability naturally decays toward 1.0 as the candle closes.
  Either the sell fills before resolution, or it resolves at $1.00.

  One position per candle.  Sizing: 20 shares or balance * 0.9 if short.

Run:  python -m automata.eth_15m
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger("automata.eth_15m")

# ── Parameters ─────────────────────────────────────────────────────────────────

SELL_TARGET  = 0.999   # desired TP; effective TP is capped by venue limits
SELL_TARGET_HARD_MAX = 0.99  # CLOB reject threshold for this market family
BUY_MAX      = 0.99    # never enter above this ask price (99.0c)
FORCE_ENTRY_BID = 0.99   # if ask is missing but bid is this high, enter at BUY_MAX
BUY_ORDER_TTL_SECONDS = 5 * 60  # auto-expire buy orders after 5 minutes
BUY_CROSS_TICK = 0.01   # cross one cent above latest ask/bid to improve fill odds
STOP_LOSS    = 0.75    # exit immediately if position bid drops below this
BET_SHARES   = 30      # default target shares per trade
K_DEFAULT    = 0.12   # Brownian Bridge k — auto-calibrated when >= 10 outcomes exist
MIN_SECONDS  = 2       # no-entry zone at the very end
MAX_SECONDS  = 120     # enter from T-120s onward
MAX_SECONDS_1H = 4 * 60  # when using hourly slug source, allow last 4 minutes
CANDLE_MINUTES = 15
ACTIVE_MARKET_MAX_AHEAD_SECONDS = 120 * 60
VOL_LOOKBACK_15M = 672          # 7 days of 15m candles
MOMENTUM_FAST_15M = 2           # 30m
MOMENTUM_SLOW_15M = 8           # 2h
MOMENTUM_MAX_ADJUST = 0.06      # max +/- 6 percentage points on fair_up
MOMENTUM_GAIN = 4.0             # scales (fast/slow - 1) into fair adjustment
INVENTORY_SYNC_ATTEMPTS = 6         # retries for syncing TP sell after a buy
INVENTORY_SYNC_DELAY_SECONDS = 0.7  # base backoff while waiting for inventory
REDEEM_ONLY_MODE = True             # do not place TP/stop-loss sells; hold for redeem

DB_PATH = Path(__file__).resolve().parent.parent / "bets.db"

MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
ET_OFFSET = timedelta(hours=-4)   # EDT (UTC-4)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

GAMMA_API = "https://gamma-api.polymarket.com/events"
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
}

_last_market_pick: tuple[str, str] | None = None  # (source, slug)
_last_redeem_attempt_at: datetime | None = None

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


def _maybe_run_replayer_redeem(redeem_cmd: str | None, min_interval_seconds: int = 60) -> None:
    """
    Optional hook to run an external redeem command (e.g., a relayer/replayer script)
    outside the entry window.
    """
    global _last_redeem_attempt_at
    if not redeem_cmd:
        return
    now = datetime.now(timezone.utc)
    if _last_redeem_attempt_at is not None:
        dt = (now - _last_redeem_attempt_at).total_seconds()
        if dt < min_interval_seconds:
            return
    _last_redeem_attempt_at = now
    try:
        proc = subprocess.run(redeem_cmd, shell=True, capture_output=True, text=True, timeout=60)
        if proc.returncode == 0:
            log.info("[eth_15m] Replayer redeem command succeeded")
        else:
            err = (proc.stderr or proc.stdout or "").strip()
            log.warning("[eth_15m] Replayer redeem command failed (code=%s): %s", proc.returncode, err[:300])
    except Exception as exc:
        log.warning("[eth_15m] Replayer redeem command error: %s", exc)


def _fetch_event_by_slug(slug: str) -> dict | None:
    try:
        data = _get(f"{GAMMA_API}?slug={slug}")
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
        log.warning("[eth_15m] redeemPositions failed for %s: %s", condition_id, exc)
        return None


def _redeem_condition_positions_relayer(private_key: str, condition_id: str, relayer_url: str | None = None) -> str | None:
    """
    Redeem via Polymarket relayer flow (gasless).
    Primary path: RELAYER_API_KEY auth.
    Fallback path: Builder creds via SDK.
    """
    relayer_base = relayer_url or os.getenv("POLYMARKET_RELAYER_URL") or "https://relayer-v2.polymarket.com"
    relayer_key = os.getenv("RELAYER_API_KEY")
    relayer_addr = os.getenv("RELAYER_API_KEY_ADDRESS")

    # Path A: direct relayer API key auth (no builder creds required).
    if relayer_key and relayer_addr:
        try:
            import requests
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

            # Poll for mined/confirmed hash when submit returns only transactionID.
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
            log.warning("[eth_15m] relayer API-key redeem failed for %s: %s", condition_id, exc)

    # Path B: builder creds via SDK.
    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig
        from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
        from web3 import Web3
    except Exception as exc:
        log.warning("[eth_15m] relayer SDK unavailable: %s", exc)
        return None

    key = os.getenv("POLY_BUILDER_API_KEY")
    secret = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not key or not secret or not passphrase:
        log.warning("[eth_15m] Missing builder creds for relayer redeem (POLY_BUILDER_API_KEY/SECRET/PASSPHRASE)")
        return None

    try:
        builder_cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))
        client = RelayClient(
            relayer_base,
            POLYGON_CHAIN_ID,
            private_key=private_key,
            builder_config=builder_cfg,
        )

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
        tx_hash = None
        if isinstance(waited, dict):
            tx_hash = waited.get("transactionHash")
        if not tx_hash:
            tx_hash = getattr(resp, "transaction_hash", None)
        return tx_hash
    except Exception as exc:
        log.warning("[eth_15m] relayer redeem failed for %s: %s", condition_id, exc)
        return None


def _settle_resolved_trades(private_key: str | None, rpc_url: str | None = None, redeem_mode: str = "relayer") -> None:
    _init_table()
    if not private_key:
        log.warning("[eth_15m] POLYMARKET_PRIVATE_KEY missing; cannot run native redeem")
        return

    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        log.warning("[eth_15m] POLYMARKET_FUNDER missing; cannot scan positions for redeem")
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
            FROM eth_15m_trades
            WHERE COALESCE(dry_run, 0) = 0
              AND token_id IN ({placeholders})
            """,
            tuple(open_token_ids),
        ).fetchall()

    if not rows:
        log.info("[eth_15m] Redeem scan: no tracked eth_15m DB rows for current open positions")
        return

    # Ensure condition ids are populated where missing.
    enriched: list[tuple[str, str, str]] = []
    unknown_tokens = set(open_token_ids)
    for token_id, slug, condition_id in rows:
        tid = str(token_id or "")
        unknown_tokens.discard(tid)
        cid = str(condition_id or "")
        if not cid:
            event = _fetch_event_by_slug(str(slug))
            if event and event.get("markets"):
                cid = str(event["markets"][0].get("conditionId") or "")
                if cid:
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute(
                            "UPDATE eth_15m_trades SET condition_id = ? WHERE slug = ?",
                            (cid, slug),
                        )
        if cid:
            enriched.append((tid, str(slug), cid))

    if unknown_tokens:
        log.info("[eth_15m] Redeem scan: %d open token(s) not in eth_15m_trades DB mapping", len(unknown_tokens))

    if not enriched:
        return

    # Resolve by unique condition id, but keep DB slugs to update.
    condition_to_slugs: dict[str, set[str]] = {}
    for _, slug, cid in enriched:
        condition_to_slugs.setdefault(cid, set()).add(slug)

    w3, ctf = _get_web3_and_ctf(rpc_url)
    _ = w3  # keep for connectivity side-effects

    for cid, slugs in condition_to_slugs.items():
        side = _resolved_side_from_chain(ctf, str(cid))
        if side is None:
            continue

        if redeem_mode == "relayer":
            tx_hash = _redeem_condition_positions_relayer(private_key=private_key, condition_id=str(cid))
        else:
            tx_hash = _redeem_condition_positions(private_key=private_key, condition_id=str(cid), rpc_url=rpc_url)
        now_iso = datetime.now(timezone.utc).isoformat()
        for slug in slugs:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE eth_15m_trades
                    SET redeemed_at = ?, redeem_tx_hash = ?
                    WHERE slug = ? AND COALESCE(dry_run, 0) = 0 AND redeemed_at IS NULL
                    """,
                    (now_iso, tx_hash, slug),
                )
                if side in ("up", "down"):
                    conn.execute(
                        """
                        UPDATE eth_15m_trades
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
                        UPDATE eth_15m_trades
                        SET outcome = 'invalid'
                        WHERE slug = ? AND COALESCE(dry_run, 0) = 0 AND outcome IS NULL
                        """,
                        (slug,),
                    )
            log.info("[eth_15m] Settled slug=%s side=%s redeem_tx=%s", slug, side, tx_hash or "n/a")


# ── Slug builder ───────────────────────────────────────────────────────────────

def current_et() -> datetime:
    return datetime.now(timezone(ET_OFFSET))


def _event_matches_15m_eth(event_slug: str) -> bool:
    return event_slug.lower().startswith(f"eth-updown-{CANDLE_MINUTES}m-")


def _event_matches_1h_eth(event_slug: str) -> bool:
    return event_slug.lower().startswith("ethereum-up-or-down-")


def _build_15m_slug_candidates(now_utc: datetime) -> list[str]:
    period = CANDLE_MINUTES * 60
    base = (int(now_utc.timestamp()) // period) * period
    starts = [base - 2 * period, base - period, base, base + period, base + 2 * period, base + 3 * period]
    seen: set[int] = set()
    out: list[str] = []
    for ts in starts:
        if ts in seen:
            continue
        seen.add(ts)
        out.append(f"eth-updown-{CANDLE_MINUTES}m-{ts}")
    return out


def _build_1h_slug_for_current_hour(now_et: datetime) -> str:
    month = MONTH_NAMES[now_et.month]
    hour_24 = now_et.hour
    hour_12 = hour_24 % 12 or 12
    ampm = "am" if hour_24 < 12 else "pm"
    return f"ethereum-up-or-down-{month}-{now_et.day}-{now_et.year}-{hour_12}{ampm}-et"


def _prefer_1h_slug_window(now_et: datetime) -> bool:
    # Last 15 minutes until top-of-hour: xx:45:00 to xx:59:59
    return now_et.minute >= 45


def _entry_window_for_source(source: str | None) -> tuple[int, int]:
    if source == "eth_1h":
        return MIN_SECONDS, MAX_SECONDS_1H
    return MIN_SECONDS, MAX_SECONDS


# ── Market fetch ───────────────────────────────────────────────────────────────

def _get(url: str) -> any:
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _time_units_from_seconds(seconds_remaining: float | int) -> float:
    # Keep the historical minute-scaled curve, but evaluate it with second precision.
    return max(float(seconds_remaining) / 60.0, 1.0 / 60.0)


def _min_bid_from_seconds(k: float, seconds_remaining: float | int) -> float:
    return 1.0 - k / math.sqrt(_time_units_from_seconds(seconds_remaining))


def _fetch_klines_closes(symbol: str, interval: str, limit: int) -> list[float]:
    try:
        klines = _get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}")
    except Exception:
        return []
    if not isinstance(klines, list) or len(klines) < 3:
        return []
    closes: list[float] = []
    for row in klines:
        try:
            closes.append(float(row[4]))
        except Exception:
            continue
    return closes


def _realized_annual_vol_from_closes(closes: list[float], periods_per_year: float) -> float | None:
    if len(closes) < 3:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    period_vol = math.sqrt(max(var, 0.0))
    return period_vol * math.sqrt(periods_per_year)


def _momentum_adjust_from_closes(closes_15m: list[float]) -> float:
    if len(closes_15m) < MOMENTUM_SLOW_15M:
        return 0.0
    fast = sum(closes_15m[-MOMENTUM_FAST_15M:]) / MOMENTUM_FAST_15M
    slow = sum(closes_15m[-MOMENTUM_SLOW_15M:]) / MOMENTUM_SLOW_15M
    if fast <= 0 or slow <= 0:
        return 0.0
    raw = (fast / slow - 1.0) * MOMENTUM_GAIN
    return max(-MOMENTUM_MAX_ADJUST, min(MOMENTUM_MAX_ADJUST, raw))


def _black_scholes_digital_up_prob(spot: float, strike: float, years_to_expiry: float, sigma: float, r: float = 0.0) -> float | None:
    """
    Risk-neutral probability P(S_T >= K) for a cash-or-nothing digital call.
    """
    if spot <= 0 or strike <= 0 or years_to_expiry <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(years_to_expiry)
    d2 = (math.log(spot / strike) + (r - 0.5 * sigma * sigma) * years_to_expiry) / (sigma * sqrt_t)
    return min(1.0, max(0.0, _normal_cdf(d2)))


def _fetch_eth_bs_fair(end_utc: datetime | None, seconds_remaining: int | None) -> tuple[float | None, float | None, dict[str, float | None]]:
    """
    Returns (fair_up, fair_down, meta) from Black-Scholes.
    Strike is the ETH/USDT open of the target 15m candle; spot is current ETH/USDT.
    """
    meta: dict[str, float | None] = {"sigma_annual": None, "momentum_adj": None, "spot": None, "strike": None}
    if not end_utc or seconds_remaining is None:
        return None, None, meta
    try:
        ticker = _get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
        spot = float(ticker["price"])
        meta["spot"] = spot
    except Exception:
        return None, None, meta

    try:
        start_utc = end_utc - timedelta(minutes=CANDLE_MINUTES)
        start_ms = int(start_utc.timestamp() * 1000)
        kline = _get(
            f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=15m&startTime={start_ms}&limit=1"
        )
        strike = float(kline[0][1]) if isinstance(kline, list) and kline else None
    except Exception:
        strike = None
    if strike is None:
        return None, None, meta
    meta["strike"] = strike

    closes_15m = _fetch_klines_closes("ETHUSDT", "15m", VOL_LOOKBACK_15M)
    sigma = _realized_annual_vol_from_closes(closes_15m, periods_per_year=365.0 * 24.0 * 4.0)
    if sigma is None or sigma <= 0:
        return None, None, meta
    meta["sigma_annual"] = sigma

    # Keep fair probabilistic even at/after 0 min by flooring T to a tiny value.
    effective_seconds = max(float(seconds_remaining), 1.0)  # 1 second
    years = effective_seconds / (365.0 * 24.0 * 60.0 * 60.0)
    fair_up = _black_scholes_digital_up_prob(spot=spot, strike=strike, years_to_expiry=years, sigma=sigma, r=0.0)
    if fair_up is None:
        return None, None, meta

    momentum_adj = _momentum_adjust_from_closes(closes_15m)
    meta["momentum_adj"] = momentum_adj
    fair_up = max(0.001, min(0.999, fair_up + momentum_adj))
    fair_down = 1.0 - fair_up
    return fair_up, fair_down, meta


def _fmt_matrix_cell(v: float | None) -> str:
    return f"{v * 100:.1f}" if v is not None else "n/a"


def _print_price_matrix(up_ask: float | None, down_ask: float | None, up_bid: float | None, down_bid: float | None, fair_up: float | None, fair_down: float | None) -> None:
    print("              Up     Down")
    print(f"  ask       {_fmt_matrix_cell(up_ask):>6}  {_fmt_matrix_cell(down_ask):>6}")
    print(f"  bid       {_fmt_matrix_cell(up_bid):>6}  {_fmt_matrix_cell(down_bid):>6}")
    print(f"  fair      {_fmt_matrix_cell(fair_up):>6}  {_fmt_matrix_cell(fair_down):>6}")


def _event_to_market(event: dict, now_utc: datetime, slug_matcher=_event_matches_15m_eth) -> dict | None:
    if not event or not event.get("markets"):
        return None
    slug = str(event.get("slug") or "")
    if not slug_matcher(slug):
        return None
    m = event["markets"][0]
    if m.get("closed") or (m.get("active") is not None and not m.get("active")):
        return None

    def _load(key):
        v = m.get(key, [])
        return json.loads(v) if isinstance(v, str) else v

    outcomes = _load("outcomes")
    token_ids = _load("clobTokenIds")
    up_token = down_token = None
    for i, name in enumerate(outcomes):
        nl = str(name).strip().lower()
        if nl == "up" and i < len(token_ids):
            up_token = str(token_ids[i])
        if nl == "down" and i < len(token_ids):
            down_token = str(token_ids[i])
    if not up_token or not down_token:
        return None

    end_str = m.get("endDate") or event.get("endDate") or ""
    try:
        dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        end_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    seconds_remaining = int((end_utc - now_utc).total_seconds())
    if seconds_remaining < 0 or seconds_remaining > ACTIVE_MARKET_MAX_AHEAD_SECONDS:
        return None

    return {
        "slug": event.get("slug", slug),
        "title": event.get("title", ""),
        "up_token": up_token,
        "down_token": down_token,
        "condition_id": m.get("conditionId"),
        "seconds_remaining": seconds_remaining,
        "end_utc": end_utc,
    }


def _event_to_market_from_slug(
    event: dict, now_utc: datetime, slug_matcher
) -> dict | None:
    return _event_to_market(event, now_utc, slug_matcher=slug_matcher)


def _fetch_market_by_slug(now_utc: datetime, slug: str, slug_matcher) -> dict | None:
    data = _get(f"{GAMMA_API}?slug={slug}")
    if isinstance(data, list) and data:
        return _event_to_market_from_slug(data[0], now_utc, slug_matcher)
    return None


def fetch_and_parse_active() -> dict | None:
    """
    Returns nearest active ETH market for eth_15m strategy:
    - During last 15m of each hour, prefer ETH 1H slug market.
    - Otherwise (or on 1H miss), use ETH 15m market.
    payload: slug, title, up_token, down_token, seconds_remaining, end_utc, market_source
    """
    try:
        now_utc = datetime.now(timezone.utc)
        now_et = current_et()

        # In the last 15m to top-of-hour, try ETH 1H market first.
        if _prefer_1h_slug_window(now_et):
            slug_1h = _build_1h_slug_for_current_hour(now_et)
            mkt_1h = _fetch_market_by_slug(now_utc, slug_1h, _event_matches_1h_eth)
            if mkt_1h is not None:
                mkt_1h["market_source"] = "eth_1h"
                return mkt_1h

        # Fast path: direct timestamp-based slug lookup around current bucket.
        for slug in _build_15m_slug_candidates(now_utc):
            candidate = _fetch_market_by_slug(now_utc, slug, _event_matches_15m_eth)
            if candidate is not None:
                candidate["market_source"] = "eth_15m"
                return candidate

        # Fallback: scan open events pages.
        best: dict | None = None
        for page in range(20):
            offset = page * 200
            data = _get(f"{GAMMA_API}?closed=false&limit=200&offset={offset}")
            if not isinstance(data, list) or not data:
                break
            for event in data:
                candidate = _event_to_market_from_slug(event, now_utc, _event_matches_15m_eth)
                if candidate is None:
                    continue
                if best is None or candidate["end_utc"] < best["end_utc"]:
                    best = candidate
            if best is not None:
                break
        if best is not None:
            best["market_source"] = "eth_15m"
        return best
    except (URLError, json.JSONDecodeError, IndexError):
        return None


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
            CREATE TABLE IF NOT EXISTS eth_15m_trades (
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
                secs_remaining REAL,
                mins_remaining REAL,
                market_up_ask  REAL,
                market_down_ask REAL,
                market_up_bid  REAL,
                market_down_bid REAL,
                fair_up        REAL,
                fair_down      REAL,
                fair_edge      REAL,
                sigma_annual   REAL,
                momentum_adj   REAL,
                model_spot     REAL,
                model_strike   REAL,
                condition_id   TEXT,
                redeem_tx_hash TEXT,
                redeemed_at    TEXT,
                dry_run        INTEGER NOT NULL DEFAULT 0,
                outcome        TEXT
            )
        """)
        # Migrate existing rows that may lack the new columns
        existing = {r[1] for r in conn.execute("PRAGMA table_info(eth_15m_trades)")}
        if "secs_remaining" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN secs_remaining REAL")
        if "mins_remaining" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN mins_remaining REAL")
        if "market_up_ask" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN market_up_ask REAL")
        if "market_down_ask" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN market_down_ask REAL")
        if "market_up_bid" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN market_up_bid REAL")
        if "market_down_bid" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN market_down_bid REAL")
        if "fair_up" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN fair_up REAL")
        if "fair_down" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN fair_down REAL")
        if "fair_edge" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN fair_edge REAL")
        if "sigma_annual" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN sigma_annual REAL")
        if "momentum_adj" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN momentum_adj REAL")
        if "model_spot" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN model_spot REAL")
        if "model_strike" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN model_strike REAL")
        if "condition_id" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN condition_id TEXT")
        if "redeem_tx_hash" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN redeem_tx_hash TEXT")
        if "redeemed_at" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN redeemed_at TEXT")
        if "dry_run" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0")
        if "outcome" not in existing:
            conn.execute("ALTER TABLE eth_15m_trades ADD COLUMN outcome TEXT")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eth_15m_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO eth_15m_settings (key, value) VALUES ('k', ?)",
            (str(K_DEFAULT),)
        )


def _get_k() -> float:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT value FROM eth_15m_settings WHERE key = 'k'"
        ).fetchone()
    return float(row[0]) if row else K_DEFAULT


def _set_k(k: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO eth_15m_settings (key, value) VALUES ('k', ?)",
            (str(round(k, 2)),)
        )
    log.info("[eth_15m] k updated to %.2f in DB", k)


def _has_trade(slug: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM eth_15m_trades WHERE slug = ? AND COALESCE(dry_run, 0) = 0", (slug,)
        ).fetchone()
    return row is not None


def _record_trade(slug, direction, token_id, buy_order, sell_order,
                  shares, entry_price, sell_target, cost_usdc,
                  secs_remaining: float | None = None,
                  market_up_ask: float | None = None,
                  market_down_ask: float | None = None,
                  market_up_bid: float | None = None,
                  market_down_bid: float | None = None,
                  fair_up: float | None = None,
                  fair_down: float | None = None,
                  fair_edge: float | None = None,
                  sigma_annual: float | None = None,
                  momentum_adj: float | None = None,
                  model_spot: float | None = None,
                  model_strike: float | None = None,
                  condition_id: str | None = None,
                  dry_run: bool = False) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO eth_15m_trades
                (slug, direction, token_id, buy_order, sell_order,
                shares, entry_price, sell_target, cost_usdc, placed_at,
                secs_remaining, mins_remaining, market_up_ask, market_down_ask,
                market_up_bid, market_down_bid, fair_up, fair_down, fair_edge,
                sigma_annual, momentum_adj, model_spot, model_strike, condition_id, dry_run, outcome)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (slug, direction, token_id, buy_order, sell_order,
             shares, entry_price, sell_target, cost_usdc,
             datetime.now(timezone.utc).isoformat(),
             secs_remaining,
             (secs_remaining / 60.0) if secs_remaining is not None else None,
             market_up_ask, market_down_ask,
             market_up_bid, market_down_bid,
             fair_up, fair_down, fair_edge,
             sigma_annual, momentum_adj, model_spot, model_strike, condition_id, 1 if dry_run else 0),
        )


def _update_outcome(slug: str, outcome: str) -> None:
    """Set outcome for the trade with this slug (win / stop_loss / expired)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE eth_15m_trades SET outcome = ? WHERE slug = ? AND outcome IS NULL AND COALESCE(dry_run, 0) = 0",
            (outcome, slug),
        )
    log.info("[eth_15m] outcome recorded: %s → %s", slug[-24:], outcome)


def _calibrate_k() -> float:
    """
    Grid-search the best k for min_bid = 1 - k / sqrt(seconds / 60).
    Uses resolved trades (outcome = 'win' or 'stop_loss') from the DB.
    Prefers second-level timestamps and falls back to legacy minute rows.
    Auto-saves the best k to the DB when >= 10 outcomes exist.
    Returns the current k (updated or unchanged).
    """
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT entry_price, secs_remaining, mins_remaining, outcome
               FROM eth_15m_trades
               WHERE outcome IN ('win', 'stop_loss')
               AND COALESCE(dry_run, 0) = 0
               AND (secs_remaining IS NOT NULL OR mins_remaining IS NOT NULL)"""
        ).fetchall()

    current_k = _get_k()

    if len(rows) < 10:
        log.info("[eth_15m] calibration: only %d resolved trades — need 10, using k=%.2f",
                 len(rows), current_k)
        return current_k

    best_k, best_score = current_k, -1.0
    for k in [round(0.06 + 0.01 * i, 2) for i in range(15)]:  # 0.06 .. 0.20
        taken: list[tuple[float, str]] = []
        for ep, secs, mins, out in rows:
            seconds_remaining = secs if secs is not None else ((mins or 0.0) * 60.0)
            if seconds_remaining <= 0:
                continue
            if ep >= _min_bid_from_seconds(k, seconds_remaining):
                taken.append((ep, out))
        if not taken:
            continue
        win_rate = sum(1 for _, out in taken if out == "win") / len(taken)
        score = win_rate * math.log1p(len(taken))
        if score > best_score:
            best_score, best_k = score, k

    wins   = sum(1 for *_, o in rows if o == "win")
    losses = sum(1 for *_, o in rows if o == "stop_loss")
    log.info(
        "[eth_15m] calibration: %d resolved trades (%d W / %d L) — "
        "best k=%.2f (score=%.3f)  previous k=%.2f",
        len(rows), wins, losses, best_k, best_score, current_k,
    )
    if best_k != current_k:
        _set_k(best_k)
    return best_k


def _ensure_clob_credentials() -> bool:
    """
    Ensure CLOB credentials are available for live trading.
    If missing, derive them from the private key and inject into process env.
    """
    required_clob = ["CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS"]
    if all(os.getenv(k) for k in required_clob):
        return True

    required_base = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing_base = [k for k in required_base if not os.getenv(k)]
    if missing_base:
        log.error("[eth_15m] Missing .env keys: %s", ", ".join(missing_base))
        return False

    try:
        from automata.client import derive_api_credentials

        creds = derive_api_credentials(
            host=os.environ["POLYMARKET_HOST"],
            private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
            funder=os.getenv("POLYMARKET_FUNDER") or None,
            signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
        )
        os.environ["CLOB_API_KEY"] = creds.api_key
        os.environ["CLOB_SECRET"] = creds.api_secret
        os.environ["CLOB_PASS"] = creds.api_passphrase
        log.info("[eth_15m] Derived missing CLOB credentials from POLYMARKET_PRIVATE_KEY")
        return True
    except Exception as exc:
        log.error("[eth_15m] Could not derive CLOB credentials: %s", exc)
        return False


def _as_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _order_open_shares(order: dict) -> float:
    for key in ("remaining_size", "size_left", "sizeLeft", "open_size"):
        v = _as_float(order.get(key))
        if v is not None and v >= 0:
            return v
    size = _as_float(order.get("size")) or _as_float(order.get("original_size")) or 0.0
    filled = _as_float(order.get("matched_size")) or _as_float(order.get("filled_size")) or 0.0
    return max(0.0, size - filled)


def _held_shares_for_token(token_id: str) -> float:
    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        return 0.0
    try:
        from automata.client import get_positions

        positions = get_positions(funder)
    except Exception:
        return 0.0
    return round(
        sum(float(p.get("size") or 0.0) for p in positions if str(p.get("token_id")) == str(token_id)),
        2,
    )


def _build_live_client():
    from automata.client import build_client

    return build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )


def _effective_sell_target() -> float:
    return min(SELL_TARGET, SELL_TARGET_HARD_MAX)


def _sync_take_profit_sell_from_inventory(token_id: str, attempts: int = 1) -> tuple[float, str]:
    """
    Ensure a SELL_TARGET order exists for currently held shares on this token.
    Returns (held_shares, any_tp_order_id).
    """
    from automata.client import get_open_orders, get_positions, place_sell_order

    funder = os.getenv("POLYMARKET_FUNDER")
    if not funder:
        log.warning("[eth_15m] POLYMARKET_FUNDER missing; cannot sync TP sell from inventory")
        return 0.0, "?"

    client = _build_live_client()
    tp_price = _effective_sell_target()
    tp_order_id = "?"
    held_shares = 0.0

    for attempt in range(1, max(1, attempts) + 1):
        positions = get_positions(funder)
        held_shares = round(
            sum(float(p.get("size") or 0.0) for p in positions if str(p.get("token_id")) == str(token_id)),
            2,
        )

        open_orders = get_open_orders(client, str(token_id))
        tp_open_shares = 0.0
        for order in open_orders:
            side = str(order.get("side") or "").upper()
            if side and side != "SELL":
                continue
            px = _as_float(order.get("price"))
            if px is None or abs(px - tp_price) > 0.0005:
                continue
            rem = _order_open_shares(order)
            if rem <= 0:
                continue
            tp_open_shares += rem
            if tp_order_id == "?":
                tp_order_id = str(order.get("id") or order.get("orderID") or "?")

        missing = round(max(0.0, held_shares - tp_open_shares), 2)
        if missing <= 0:
            return held_shares, tp_order_id

        try:
            resp = place_sell_order(client, str(token_id), tp_price, missing)
            new_id = str(resp.get("orderID") or resp.get("id") or "?")
            if new_id != "?":
                tp_order_id = new_id
            log.info(
                "[eth_15m] TP sync: posted %.2f shares @ %.3f (held %.2f, existing TP %.2f) id=%s",
                missing, tp_price, held_shares, tp_open_shares, tp_order_id,
            )
            return held_shares, tp_order_id
        except Exception as exc:
            if attempt >= attempts:
                log.warning(
                    "[eth_15m] TP sync failed after %d attempt(s): %s (held %.2f, existing TP %.2f)",
                    attempt, exc, held_shares, tp_open_shares,
                )
                return held_shares, tp_order_id
            delay = round(INVENTORY_SYNC_DELAY_SECONDS * attempt, 2)
            log.warning(
                "[eth_15m] TP sync attempt %d/%d failed: %s; retrying in %.2fs",
                attempt, attempts, exc, delay,
            )
            time.sleep(delay)

    return held_shares, tp_order_id


# ── Main entry point ───────────────────────────────────────────────────────────

def run_eth_15m(
    dry_run: bool = True,
    host: str = "https://clob.polymarket.com",
    max_spend_usdc: float | None = None,
    bet_shares: float = BET_SHARES,
) -> None:
    """
    Called every 10 s.  Monitors open position for stop-loss,
    then looks for a new entry if none is active.
    """
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
    if not dry_run and not _ensure_clob_credentials():
        return

    target_shares = max(0.0, float(bet_shares))
    _init_table()

    mkt = fetch_and_parse_active()
    slug = mkt["slug"] if mkt else None
    source = str(mkt.get("market_source", "eth_15m")) if mkt else "eth_15m"
    window_min, window_max = _entry_window_for_source(source)

    global _last_market_pick
    if mkt and slug:
        marker = (source, slug)
        if _last_market_pick != marker:
            secs_dbg = int(mkt.get("seconds_remaining") or -1)
            log.info(
                "[eth_15m] Market switch -> source=%s slug=%s secs=%d entry_window=[%d-%d]",
                source, slug, secs_dbg, window_min, window_max,
            )
            _last_market_pick = marker

    # ── Monitor open position ──────────────────────────────────────────────────
    if _position["active"]:
        if not dry_run and _position.get("token_id"):
            held_now = _held_shares_for_token(str(_position["token_id"]))
            if held_now <= 0:
                log.info(
                    "[eth_15m] No scanned inventory for tracked token %s; clearing local position state",
                    _position["token_id"],
                )
                _reset_position()
            else:
                _position["shares"] = held_now

        if not _position["active"]:
            pass
        elif _position["slug"] != slug:
            # New candle — position resolved, record outcome and calibrate
            prev_slug = _position["slug"] or ""
            log.info("[eth_15m] New candle — position on %s resolved, resetting",
                     prev_slug[-24:])
            _update_outcome(prev_slug, "expired")
            _reset_position()
            _calibrate_k()
        else:
            # Same candle — check stop-loss and win detection
            books    = get_books(host, [_position["token_id"]])
            cur_bid  = books.get(_position["token_id"], {}).get("bid")
            if not dry_run and not REDEEM_ONLY_MODE:
                try:
                    held_now, tp_id = _sync_take_profit_sell_from_inventory(
                        str(_position["token_id"]), attempts=1
                    )
                    if held_now > 0:
                        _position["shares"] = held_now
                    if tp_id and tp_id != "?":
                        _position["sell_order_id"] = tp_id
                except Exception as exc:
                    log.warning("[eth_15m] TP inventory sync error: %s", exc)

            if not REDEEM_ONLY_MODE and cur_bid is not None and cur_bid < STOP_LOSS:
                log.warning("[eth_15m] STOP-LOSS  bid=%.3f < %.2f — exiting %s",
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
                        place_market_sell(client, _position["token_id"],
                                          exit_price, _position["shares"])
                        log.info("[eth_15m] Stop-loss sell placed @ %.3f", exit_price)
                    except Exception as exc:
                        log.error("[eth_15m] Stop-loss exit failed: %s", exc)
                else:
                    log.info("[eth_15m] [DRY RUN] Would stop-loss exit @ ~%.3f", cur_bid)
                _update_outcome(_position["slug"], "stop_loss")
                _reset_position()
                _calibrate_k()
                raise SystemExit("[eth_15m] Stop-loss triggered; terminating process by policy")
            else:
                log.info("[eth_15m] Holding %s — bid=%s  entry=%.3f",
                         _position["direction"],
                         f"{cur_bid:.3f}" if cur_bid else "n/a",
                         _position["entry_price"])
            return

    # ── Entry logic ────────────────────────────────────────────────────────────

    # Already traded this candle (from a previous process run)?
    if _has_trade(slug):
        log.info("[eth_15m] Already traded %s — skip", slug[-24:])
        return

    if not mkt or not slug:
        log.info("[eth_15m] No active ETH market found (checked 1h preference window + 15m fallback)")
        return

    secs = mkt["seconds_remaining"]
    in_window = window_min <= secs <= window_max
    if not in_window and not dry_run:
        log.info(
            "[eth_15m] source=%s %d sec remaining - outside entry window [%d-%d]",
            source, secs, window_min, window_max,
        )
        return
    if not in_window and dry_run:
        log.info(
            "[eth_15m] [DRY RUN] source=%s %d sec remaining - bypassing entry window [%d-%d]",
            source, secs, window_min, window_max,
        )


    k = _calibrate_k()

    # Time-adjusted minimum bid (Brownian Bridge): stricter earlier in the window
    min_bid = round(_min_bid_from_seconds(k, secs), 3)

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
    print(f"  ETH 15M TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Market source: {mkt.get('market_source', 'eth_15m')}")
    print(f"  Time remaining: {secs} sec  (window {window_min}-{window_max}s)")
    fair_up, fair_down, fair_meta = _fetch_eth_bs_fair(mkt.get("end_utc"), secs)
    _print_price_matrix(up_ask, down_ask, up_bid, down_bid, fair_up, fair_down)
    sigma_txt = f"{fair_meta['sigma_annual']:.3f}" if fair_meta.get("sigma_annual") is not None else "n/a"
    mom_txt = f"{(fair_meta['momentum_adj'] or 0.0) * 100:.2f}pp" if fair_meta.get("momentum_adj") is not None else "n/a"
    spot_txt = f"{fair_meta['spot']:.2f}" if fair_meta.get("spot") is not None else "n/a"
    strike_txt = f"{fair_meta['strike']:.2f}" if fair_meta.get("strike") is not None else "n/a"
    tp_price = _effective_sell_target()
    print(f"  Min bid (T-{secs}s): {min_bid:.3f}  |  sell: {tp_price:.3f}")
    print(f"  Current price: {spot_txt}  |  Price to beat: {strike_txt}")
    print(f"  Fair model: sigma_15m_annual={sigma_txt}  momentum_adj={mom_txt}")

    # ── Find entry ─────────────────────────────────────────────────────────────
    candidates: list[dict[str, float | str | bool | None]] = []
    for idx, (direction, ask, bid, token, fair_side) in enumerate([
        ("Up",   up_ask,   up_bid,   mkt["up_token"],   fair_up),
        ("Down", down_ask, down_bid, mkt["down_token"], fair_down),
    ]):
        if bid is None:
            continue
        effective_ask = ask
        ask_fallback = False
        if effective_ask is None and bid >= FORCE_ENTRY_BID:
            # One-sided book fallback: still attempt entry at BUY_MAX when bid is near-certain.
            effective_ask = BUY_MAX
            ask_fallback = True
        if effective_ask is None:
            continue
        if effective_ask > BUY_MAX:
            continue
        if bid < min_bid:
            continue
        fair_edge = (fair_side - effective_ask) if fair_side is not None else None
        candidates.append(
            {
                "direction": direction,
                "ask": effective_ask,
                "bid": bid,
                "token": token,
                "fair_edge": fair_edge,
                "idx": float(idx),
                "ask_fallback": ask_fallback,
            }
        )

    candidate = None
    priced = [c for c in candidates if c.get("fair_edge") is not None]
    if priced:
        candidate = max(priced, key=lambda c: float(c["fair_edge"]))  # type: ignore[arg-type]
    elif candidates:
        candidate = min(candidates, key=lambda c: float(c["idx"]))  # preserve Up->Down fallback

    if not candidate:
        up_str   = f"{up_ask:.3f}"   if up_ask   else "n/a"
        down_str = f"{down_ask:.3f}" if down_ask else "n/a"
        print(f"  --> No entry: Up={up_str}  Down={down_str}  "
              f"(need bid>={min_bid:.3f})")
        print(f"{div}\n")
        return

    direction = str(candidate["direction"])
    ask       = float(candidate["ask"])
    token     = str(candidate["token"])
    fair_edge = candidate.get("fair_edge")
    fair_edge_txt = f"{float(fair_edge) * 100:.2f}pp" if fair_edge is not None else "n/a"
    ask_fallback = bool(candidate.get("ask_fallback"))

    if dry_run:
        shares = target_shares
        cost   = round(shares * ask, 2)
        print(f"  --> ENTRY: {direction} @ {ask:.3f}  "
              f"{shares}sh  ${cost:.2f}  sell target {tp_price:.3f}  fair edge {fair_edge_txt}")
        if ask_fallback:
            print(f"  Note: ask missing; fallback buy at {BUY_MAX:.3f} due to bid >= {FORCE_ENTRY_BID:.3f}")
        print(f"{div}\n")
        log.info("[eth_15m] [DRY RUN] Would buy %d %s @ %.3f  $%.2f  then sell @ %.3f  fair_edge=%s",
                 shares, direction, ask, cost, tp_price, fair_edge_txt)
        _record_trade(
            slug, direction, token, "DRY_RUN", "DRY_RUN",
            shares, ask, tp_price, cost,
            secs_remaining=secs,
            market_up_ask=up_ask, market_down_ask=down_ask,
            market_up_bid=up_bid, market_down_bid=down_bid,
            fair_up=fair_up, fair_down=fair_down,
            fair_edge=float(fair_edge) if fair_edge is not None else None,
            sigma_annual=fair_meta.get("sigma_annual"),
            momentum_adj=fair_meta.get("momentum_adj"),
            model_spot=fair_meta.get("spot"),
            model_strike=fair_meta.get("strike"),
            condition_id=str(mkt.get("condition_id") or ""),
            dry_run=True,
        )
        return

    # ── Live: buy then immediately post sell ───────────────────────────────────
    required = ["POLYMARKET_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET",
                "CLOB_PASS", "POLYMARKET_HOST"]
    if any(not os.getenv(k) for k in required):
        log.error("[eth_15m] Missing .env keys")
        return

    from automata.client import build_client, get_usdc_balance, place_market_buy, place_no_order

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
        log.warning("[eth_15m] Could not fetch balance, defaulting to %.2f shares: %s", target_shares, exc)
        balance = target_shares * ask  # assume enough
    if max_spend_usdc is not None:
        capped = min(balance, max_spend_usdc)
        log.info("[eth_15m] Balance cap enabled: available $%.2f, capped spend $%.2f", balance, capped)
        balance = capped

    if balance >= target_shares:
        shares = target_shares
    else:
        shares = round(balance * 0.9, 2)

    submit_ask = ask
    try:
        latest = get_books(host, [token]).get(token, {})
        latest_ask = latest.get("ask")
        latest_bid = latest.get("bid")
        if latest_ask is not None:
            submit_ask = min(BUY_MAX, max(submit_ask, float(latest_ask) + BUY_CROSS_TICK))
        elif latest_bid is not None:
            submit_ask = min(BUY_MAX, max(submit_ask, float(latest_bid) + BUY_CROSS_TICK))
        submit_ask = round(submit_ask, 3)
    except Exception:
        submit_ask = ask

    cost = round(shares * submit_ask, 2)
    log.info("[eth_15m] Balance $%.2f - using %.2f shares (target %.2f)", balance, shares, target_shares)

    print(f"  --> ENTRY: {direction} @ {submit_ask:.3f}  "
          f"{shares}sh  ${cost:.2f}  sell target {tp_price:.3f}  fair edge {fair_edge_txt}")
    if ask_fallback:
        print(f"  Note: ask missing; fallback buy at {BUY_MAX:.3f} due to bid >= {FORCE_ENTRY_BID:.3f}")
    print(f"{div}\n")

    if shares <= 0:
        log.error("[eth_15m] Insufficient balance (%.2f), skipping", balance)
        return

    # Step 1 — buy
    try:
        market_shares = round(shares * 0.5, 2)
        limit_shares = round(shares - market_shares, 2)
        if market_shares <= 0 and shares > 0:
            market_shares = shares
            limit_shares = 0.0
        buy_ids: list[str] = []
        market_submitted = 0.0
        limit_submitted = 0.0
        if market_shares > 0:
            try:
                buy_resp_mkt = place_market_buy(
                    client, token, submit_ask, market_shares, ttl_seconds=BUY_ORDER_TTL_SECONDS
                )
                buy_id_mkt = str(buy_resp_mkt.get("orderID") or buy_resp_mkt.get("id") or "?")
                buy_ids.append(f"mkt:{buy_id_mkt}")
                market_submitted = market_shares
                log.info("[eth_15m] Market buy submitted %.2f %s @ <=%.3f id=%s",
                         market_shares, direction, submit_ask, buy_id_mkt)
            except Exception as mkt_exc:
                msg = str(mkt_exc)
                msg_l = msg.lower()
                if "fok" in msg_l or "fully filled or killed" in msg_l:
                    # FOK can fail in thin/one-sided books; degrade to limit-only rather than aborting the cycle.
                    limit_shares = round(limit_shares + market_shares, 2)
                    market_shares = 0.0
                    log.warning(
                        "[eth_15m] Market buy FOK not fillable at <=%.3f; rolling %.2f shares into limit order",
                        submit_ask,
                        limit_shares,
                    )
                else:
                    raise
        if limit_shares > 0:
            buy_resp_lmt = place_no_order(client, token, submit_ask, limit_shares, ttl_seconds=BUY_ORDER_TTL_SECONDS)
            buy_id_lmt = str(buy_resp_lmt.get("orderID") or buy_resp_lmt.get("id") or "?")
            buy_ids.append(f"lmt:{buy_id_lmt}")
            limit_submitted = limit_shares
            log.info("[eth_15m] Limit buy submitted %.2f %s @ %.3f ttl=%ds id=%s",
                     limit_shares, direction, submit_ask, BUY_ORDER_TTL_SECONDS, buy_id_lmt)
        buy_id = ",".join(buy_ids) if buy_ids else "?"
        log.info("[eth_15m] Buy split: market=%.2f limit=%.2f total=%.2f",
                 market_submitted, limit_submitted, shares)
        if abs(submit_ask - ask) > 1e-9:
            log.info("[eth_15m] Aggressive reprice: signal ask %.3f -> submit %.3f", ask, submit_ask)
        if ask_fallback:
            log.info("[eth_15m] Entry used ask-fallback (bid>=%.3f, buy@%.3f)", FORCE_ENTRY_BID, ask)
    except Exception as exc:
        log.error("[eth_15m] Buy failed: %s", exc)
        return

    # Step 2 — redeem-only mode: do not post TP sells; hold inventory to resolution.
    sell_id = "REDEEM_ONLY"
    held_after_buy = 0.0 if dry_run else _held_shares_for_token(str(token))
    if held_after_buy > 0:
        shares = held_after_buy
    if held_after_buy <= 0:
        log.info(
            "[eth_15m] Buy accepted but no scanned position yet for token %s; not marking as holding",
            token,
        )
    elif not REDEEM_ONLY_MODE:
        held_after_buy, sell_id = _sync_take_profit_sell_from_inventory(
            str(token), attempts=INVENTORY_SYNC_ATTEMPTS
        )
        if held_after_buy > 0:
            shares = held_after_buy

    _record_trade(
        slug, direction, token, buy_id, sell_id,
        shares, submit_ask, tp_price, cost,
        secs_remaining=secs,
        market_up_ask=up_ask, market_down_ask=down_ask,
        market_up_bid=up_bid, market_down_bid=down_bid,
        fair_up=fair_up, fair_down=fair_down,
        fair_edge=float(fair_edge) if fair_edge is not None else None,
        sigma_annual=fair_meta.get("sigma_annual"),
        momentum_adj=fair_meta.get("momentum_adj"),
        model_spot=fair_meta.get("spot"),
        model_strike=fair_meta.get("strike"),
        condition_id=str(mkt.get("condition_id") or ""),
        dry_run=False,
    )

    # Track in memory for stop-loss monitoring only when inventory confirms a fill.
    if held_after_buy > 0:
        _position.update({
            "active":        True,
            "slug":          slug,
            "direction":     direction,
            "token_id":      token,
            "sell_order_id": sell_id,
            "shares":        shares,
            "entry_price":   submit_ask,
        })
    else:
        _reset_position()

    if held_after_buy > 0:
        print(f"  [eth_15m] BUY {direction} {shares}sh @ {submit_ask:.3f}  ${cost:.2f}"
              f"  buy={buy_id}  sell@{tp_price}={sell_id}")
    else:
        print(f"  [eth_15m] BUY ORDER {direction} {shares}sh @ {submit_ask:.3f}  ${cost:.2f}"
              f"  id={buy_id}  status=pending-fill ttl={BUY_ORDER_TTL_SECONDS}s")


# ── Standalone display ─────────────────────────────────────────────────────────

def analyze(host: str = "https://clob.polymarket.com") -> None:
    _init_table()
    mkt = fetch_and_parse_active()
    if not mkt:
        print("No active ETH 15M market found")
        return

    books     = get_books(host, [mkt["up_token"], mkt["down_token"]])
    up_book   = books.get(mkt["up_token"],   {})
    down_book = books.get(mkt["down_token"], {})

    secs = mkt["seconds_remaining"]
    source = str(mkt.get("market_source", "eth_15m"))
    window_min, window_max = _entry_window_for_source(source)
    k = _get_k()
    min_bid = round(_min_bid_from_seconds(k, secs), 3) if secs > 0 else None
    div     = "=" * 65
    print(f"\n{div}")
    print(f"  ETH 15M TAIL CAPTURE  {mkt['title']}")
    print(f"  {'-'*63}")
    print(f"  Market source: {source}")
    print(f"  Slug:           {mkt['slug']}")
    print(f"  Time remaining: {secs} sec")
    fair_up, fair_down, fair_meta = _fetch_eth_bs_fair(mkt.get("end_utc"), secs)
    _print_price_matrix(
        up_book.get("ask"),
        down_book.get("ask"),
        up_book.get("bid"),
        down_book.get("bid"),
        fair_up,
        fair_down,
    )
    sigma_txt = f"{fair_meta['sigma_annual']:.3f}" if fair_meta.get("sigma_annual") is not None else "n/a"
    mom_txt = f"{(fair_meta['momentum_adj'] or 0.0) * 100:.2f}pp" if fair_meta.get("momentum_adj") is not None else "n/a"
    spot_txt = f"{fair_meta['spot']:.2f}" if fair_meta.get("spot") is not None else "n/a"
    strike_txt = f"{fair_meta['strike']:.2f}" if fair_meta.get("strike") is not None else "n/a"
    min_bid_str = f"{min_bid:.3f}" if min_bid is not None else "n/a"
    tp_price = _effective_sell_target()
    print(f"  Min bid (T-{secs}s): {min_bid_str}  |  sell: {tp_price:.3f}  |  k={k:.2f}")
    print(f"  Current price: {spot_txt}  |  Price to beat: {strike_txt}")
    print(f"  Fair model: sigma_15m_annual={sigma_txt}  momentum_adj={mom_txt}")
    print(f"  Stop-loss: {STOP_LOSS:.2f}  |  Entry window: {window_min}-{window_max} sec remaining")

    in_window = window_min <= secs <= window_max
    print(f"  Window: {'OPEN' if in_window else f'CLOSED ({secs}s)'}")

    for label, book in [("Up", up_book), ("Down", down_book)]:
        ask = book.get("ask")
        bid = book.get("bid")
        if ask and bid and min_bid is not None and bid >= min_bid and in_window:
            cost = round(BET_SHARES * ask, 2)
            print(f"  --> SIGNAL: buy {label} @ {ask:.3f}  "
                  f"{BET_SHARES}sh (balance-adjusted at trade time)  ${cost:.2f}  then sell @ {tp_price:.3f}")
    print(f"{div}\n")


if __name__ == "__main__":
    import argparse
    import os
    import sys
    import time
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    parser = argparse.ArgumentParser(description="ETH 15m Up/Down analyzer/runner")
    parser.add_argument("--run", action="store_true", help="Run strategy cycle(s) instead of display-only analyze")
    parser.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    parser.add_argument("--interval", type=int, default=10, help="Legacy fixed interval fallback seconds for --run mode")
    parser.add_argument("--hot-interval", type=int, default=2, help="Polling seconds when inside entry window")
    parser.add_argument("--idle-interval", type=int, default=20, help="Polling seconds when outside entry window")
    parser.add_argument("--once", action="store_true", help="Run a single cycle in --run mode")
    parser.add_argument("--bet-size", type=float, default=BET_SHARES, help="Target shares per trade (default: 30)")
    parser.add_argument("--max-balance", type=float, default=None, help="Max USDC this process can spend")
    parser.add_argument("--redeem", dest="redeem", action="store_true", default=True, help="Run redeem/settle outside entry window (default: enabled)")
    parser.add_argument("--no-redeem", dest="redeem", action="store_false", help="Disable redeem/settle outside entry window")
    parser.add_argument("--redeem-interval", type=int, default=20, help="Seconds between redeem loop runs")
    parser.add_argument("--polygon-rpc", type=str, default=None, help="Polygon RPC URL for native redeem")
    parser.add_argument("--redeem-mode", choices=["relayer", "onchain"], default="relayer", help="Redeem flow to use when redeem is enabled")
    parser.add_argument("--redeem-cmd", type=str, default=None, help="External replayer redeem command to run outside entry window")
    args = parser.parse_args()
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    if not args.run:
        analyze(host=host)
    else:
        next_redeem_due = 0.0
        while True:
            next_sleep = max(1, args.interval)
            try:
                active = fetch_and_parse_active()
                secs = int(active["seconds_remaining"]) if active and active.get("seconds_remaining") is not None else None
                source = str(active.get("market_source", "eth_15m")) if active else "eth_15m"
                wmin, wmax = _entry_window_for_source(source)
                in_window = secs is not None and wmin <= secs <= wmax

                # Independent redeem loop (position-driven), not coupled to entry scan cadence.
                now_mono = time.monotonic()
                if args.bet and args.redeem and now_mono >= next_redeem_due:
                    _settle_resolved_trades(
                        private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                        rpc_url=args.polygon_rpc or os.getenv("POLYGON_RPC_URL"),
                        redeem_mode=args.redeem_mode,
                    )
                    next_redeem_due = now_mono + max(1, args.redeem_interval)

                # Outside window: optional external redeem command hook.
                if not in_window:
                    _maybe_run_replayer_redeem(args.redeem_cmd)

                run_eth_15m(
                    dry_run=not args.bet,
                    host=host,
                    max_spend_usdc=args.max_balance if args.bet else None,
                    bet_shares=args.bet_size,
                )

                if secs is None:
                    next_sleep = max(1, args.idle_interval)
                else:
                    next_sleep = max(1, args.hot_interval if in_window else args.idle_interval)
            except Exception as exc:
                log.error("[eth_15m] Unhandled error: %s", exc)
            if args.once:
                break
            time.sleep(next_sleep)


