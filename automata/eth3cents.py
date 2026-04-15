from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from automata.client import (
    build_client,
    cancel_order,
    derive_api_credentials,
    get_open_orders,
    get_positions,
    get_usdc_balance,
)

log = logging.getLogger("automata.eth3cents")

GAMMA_API = "https://gamma-api.polymarket.com/events"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
CLOB_HOST_DEFAULT = "https://clob.polymarket.com"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ETH_5M_RE = re.compile(r"^eth-updown-5m-(\d+)$", re.IGNORECASE)

DEFAULT_POLL = 0.5
DEFAULT_SHARES = 5.0
DEFAULT_MIN_PRICE = 0.03
WS_RECONNECT_SEC = 3
WS_PING_INTERVAL = 10
WS_TOKEN_CHECK_SEC = 1.0
BINANCE_WS_RECONNECT_SEC = 3
SPOT_STALE_SEC = 3.0

DB_PATH_DEFAULT = Path("experiment") / "eth3cents.db"
LOG_PATH_DEFAULT = Path("experiment") / "logs" / "eth3cents.log"

_book_cache: dict[str, dict] = {}
_ws_tokens: list[str] = []
_ws_lock = threading.Lock()

_spot_cache: float | None = None
_spot_updated_at: float = 0.0


@dataclass
class Market:
    slug: str
    candle_start: int
    candle_end: int
    up_token: str
    down_token: str


@dataclass
class CandleOrders:
    slug: str
    up_token: str
    down_token: str
    up_order_id: str | None = None
    down_order_id: str | None = None
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_sell_order_id: str | None = None
    down_sell_order_id: str | None = None
    up_exit_submitted: bool = False
    down_exit_submitted: bool = False
    placed_at: float = 0.0
    canceled: bool = False


def _init_logging(log_path: Path) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    log.addHandler(fh)


def _get_json(url: str, *, params: dict | None = None, timeout: int = 10) -> Any:
    r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.json()


def _post_json(url: str, payload: Any, timeout: int = 10) -> Any:
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _load_json_list(v: Any) -> list:
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return v if isinstance(v, list) else []


def _event_to_market(event: dict, now_ts: int) -> Market | None:
    slug = str(event.get("slug") or "")
    m = ETH_5M_RE.match(slug)
    if not m:
        return None

    markets = event.get("markets") or []
    if not markets or not isinstance(markets[0], dict):
        return None
    mk = markets[0]

    outcomes = _load_json_list(mk.get("outcomes"))
    token_ids = _load_json_list(mk.get("clobTokenIds"))
    if not outcomes or not token_ids or len(outcomes) != len(token_ids):
        return None

    up_token = down_token = None
    for i, outcome in enumerate(outcomes):
        label = str(outcome).strip().lower()
        if label == "up":
            up_token = str(token_ids[i])
        elif label == "down":
            down_token = str(token_ids[i])

    if not up_token or not down_token:
        return None

    try:
        end_str = mk.get("endDate") or event.get("endDate") or ""
        dt = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        end_ts = int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())
    except Exception:
        return None

    if end_ts <= now_ts or end_ts - now_ts > 1200:
        return None

    candle_start = int(m.group(1))
    return Market(
        slug=slug,
        candle_start=candle_start,
        candle_end=end_ts,
        up_token=up_token,
        down_token=down_token,
    )


def _candidate_slugs(now_ts: int) -> list[str]:
    bucket = (now_ts // 300) * 300
    return [f"eth-updown-5m-{bucket + d * 300}" for d in (-1, 0, 1, 2)]


def _fetch_active_eth_market() -> Market | None:
    now_ts = int(time.time())
    best: Market | None = None

    for slug in _candidate_slugs(now_ts):
        try:
            data = _get_json(GAMMA_API, params={"slug": slug})
            if isinstance(data, list) and data:
                m = _event_to_market(data[0], now_ts)
                if m and (best is None or m.candle_end < best.candle_end):
                    best = m
        except Exception:
            continue

    return best


def _fetch_books(host: str, token_ids: list[str]) -> dict[str, dict]:
    if not token_ids:
        return {}
    payload = [{"token_id": tid} for tid in token_ids]
    books = _post_json(f"{host}/books", payload, timeout=10)
    out: dict[str, dict] = {}
    for book in books:
        tid = str(book.get("asset_id") or book.get("token_id") or "")
        if not tid:
            continue
        out[tid] = {
            "bids": list(book.get("bids") or []),
            "asks": list(book.get("asks") or []),
        }
    return out


def _best_ask(book: dict) -> float | None:
    best = None
    for a in book.get("asks") or []:
        try:
            p = float(a.get("price"))
        except Exception:
            continue
        if best is None or p < best:
            best = p
    return best


def _ws_apply_price_change(book: dict, changes: list[dict]) -> None:
    for ch in changes or []:
        try:
            p = float(ch["price"])
            s = float(ch["size"])
            sd = str(ch["side"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
        key = "asks" if sd == "SELL" else "bids"
        levels = book.setdefault(key, [])
        levels[:] = [lv for lv in levels if abs(float(lv.get("price", 0) or 0) - p) > 1e-9]
        if s > 0:
            levels.append({"price": str(p), "size": str(s)})


def _ws_handle_event(ev: dict) -> None:
    tid = str(ev.get("asset_id") or "")
    if not tid:
        return
    et = ev.get("event_type")
    if et == "book":
        _book_cache[tid] = {
            "bids": [dict(b) for b in (ev.get("bids") or [])],
            "asks": [dict(a) for a in (ev.get("asks") or [])],
        }
    elif et == "price_change":
        book = _book_cache.setdefault(tid, {"bids": [], "asks": []})
        _ws_apply_price_change(book, ev.get("changes") or [])


async def _poly_ws_loop_async() -> None:
    import websockets

    subscribed: list[str] = []
    while True:
        with _ws_lock:
            want = list(_ws_tokens)

        if not want:
            await asyncio.sleep(0.5)
            continue

        if want != subscribed:
            subscribed = want
            log.info("Polymarket WS subscribing to %d tokens", len(subscribed))

        msg = json.dumps({"auth": {}, "type": "market", "assets_ids": subscribed})
        try:
            async with websockets.connect(
                POLY_WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_INTERVAL,
                max_size=2 ** 22,
            ) as ws:
                await ws.send(msg)
                log.info("Polymarket WS connected")
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_TOKEN_CHECK_SEC)
                    except asyncio.TimeoutError:
                        with _ws_lock:
                            new_want = list(_ws_tokens)
                        if new_want != subscribed:
                            log.info("Polymarket WS token list changed; reconnecting")
                            break
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    events = payload if isinstance(payload, list) else [payload]
                    for ev in events:
                        if isinstance(ev, dict):
                            _ws_handle_event(ev)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Polymarket WS disconnected (%s: %s); reconnecting in %ds",
                type(exc).__name__,
                exc,
                WS_RECONNECT_SEC,
            )
            await asyncio.sleep(WS_RECONNECT_SEC)


def _poly_ws_thread_main() -> None:
    asyncio.run(_poly_ws_loop_async())


def _ws_subscribe(token_ids: list[str]) -> None:
    with _ws_lock:
        _ws_tokens[:] = token_ids


def _get_cached_books(token_ids: list[str]) -> dict[str, dict]:
    return {tid: _book_cache.get(tid, {"bids": [], "asks": []}) for tid in token_ids}


async def _binance_ws_loop_async() -> None:
    import websockets

    global _spot_cache, _spot_updated_at
    url = f"{BINANCE_WS_URL}?streams=ethusdt@trade"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2 ** 20) as ws:
                log.info("Binance WS connected (ethusdt@trade)")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    data = msg.get("data") or msg
                    if data.get("e") != "trade":
                        continue
                    try:
                        price = float(data.get("p"))
                    except Exception:
                        continue
                    if price > 0:
                        _spot_cache = price
                        _spot_updated_at = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Binance WS disconnected (%s: %s); reconnecting in %ds",
                type(exc).__name__,
                exc,
                BINANCE_WS_RECONNECT_SEC,
            )
            await asyncio.sleep(BINANCE_WS_RECONNECT_SEC)


def _binance_ws_thread_main() -> None:
    asyncio.run(_binance_ws_loop_async())


def _parse_winner(event: dict) -> str | None:
    markets = event.get("markets") or []
    if not markets or not isinstance(markets[0], dict):
        return None
    mk = markets[0]
    outcomes = _load_json_list(mk.get("outcomes"))
    prices_raw = mk.get("outcomePrices") or "[]"
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except Exception:
        return None
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None
    for outcome, price in zip(outcomes, prices):
        try:
            if float(price) >= 0.99:
                return str(outcome).strip()
        except Exception:
            continue
    return None


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS eth3cents_orders (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            placed_at             INTEGER NOT NULL,
            slug                  TEXT NOT NULL,
            side                  TEXT NOT NULL,
            token_id              TEXT NOT NULL,
            trigger_ask           REAL NOT NULL,
            entry_ask             REAL NOT NULL,
            submit_price          REAL NOT NULL,
            trigger_spot          REAL NOT NULL,
            confirm_spot          REAL NOT NULL,
            move_pct              REAL NOT NULL,
            rebound_move_pct      REAL NOT NULL,
            secs_remaining        INTEGER NOT NULL,
            shares                REAL NOT NULL,
            order_id              TEXT,
            order_status          TEXT,
            dry_run               INTEGER NOT NULL DEFAULT 0,
            response_json         TEXT,
            error                 TEXT,
            won                   INTEGER,
            pnl                   REAL,
            winner                TEXT,
            resolved_at           INTEGER,
            UNIQUE(slug, side)
        )
        """
    )
    conn.commit()
    return conn


def _order_already_attempted(conn: sqlite3.Connection, slug: str, side: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM eth3cents_orders WHERE slug=? AND side=? LIMIT 1",
        (slug, side),
    ).fetchone()
    return row is not None


def _record_attempt(
    conn: sqlite3.Connection,
    *,
    slug: str,
    side: str,
    token_id: str,
    trigger_ask: float,
    entry_ask: float,
    submit_price: float,
    trigger_spot: float,
    confirm_spot: float,
    move_pct: float,
    rebound_move_pct: float,
    secs_remaining: int,
    shares: float,
    dry_run: bool,
    order_id: str | None,
    order_status: str | None,
    response: dict | None,
    error: str | None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO eth3cents_orders (
            placed_at, slug, side, token_id,
            trigger_ask, entry_ask, submit_price,
            trigger_spot, confirm_spot, move_pct, rebound_move_pct,
            secs_remaining, shares,
            order_id, order_status, dry_run, response_json, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            slug,
            side,
            token_id,
            trigger_ask,
            entry_ask,
            submit_price,
            trigger_spot,
            confirm_spot,
            move_pct,
            rebound_move_pct,
            secs_remaining,
            shares,
            order_id,
            order_status,
            1 if dry_run else 0,
            json.dumps(response or {}, ensure_ascii=True),
            error,
        ),
    )
    conn.commit()


def _settle_orders(conn: sqlite3.Connection) -> None:
    now = int(time.time())
    rows = conn.execute(
        """
        SELECT slug, side, shares, submit_price
        FROM eth3cents_orders
        WHERE won IS NULL AND error IS NULL
        """
    ).fetchall()

    for row in rows:
        slug = row["slug"]
        m = ETH_5M_RE.match(slug)
        if not m:
            continue
        candle_end = int(m.group(1)) + 300
        if now < candle_end + 30:
            continue
        try:
            data = _get_json(GAMMA_API, params={"slug": slug})
            if not isinstance(data, list) or not data:
                continue
            winner_side = _parse_winner(data[0])
            if winner_side is None:
                continue
            won = 1 if winner_side == row["side"] else 0
            pnl = round(
                row["shares"] * (1.0 - row["submit_price"]) if won
                else -row["shares"] * row["submit_price"],
                4,
            )
            conn.execute(
                """
                UPDATE eth3cents_orders
                SET won=?, pnl=?, winner=?, resolved_at=?
                WHERE slug=? AND side=?
                """,
                (won, pnl, winner_side, now, slug, row["side"]),
            )
            conn.commit()
            log.info("SETTLED %s %s won=%d pnl=%+.4f winner=%s", slug, row["side"], won, pnl, winner_side)
        except Exception as exc:
            log.debug("Settlement check failed for %s: %s", slug, exc)


def _derive_clob_credentials() -> None:
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    creds = derive_api_credentials(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )
    os.environ["CLOB_API_KEY"] = creds.api_key
    os.environ["CLOB_SECRET"] = creds.api_secret
    os.environ["CLOB_PASS"] = creds.api_passphrase


def _build_client_from_env():
    required = ["POLYMARKET_PRIVATE_KEY", "POLYMARKET_HOST", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    return build_client(
        host=os.environ["POLYMARKET_HOST"],
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        api_key=os.environ["CLOB_API_KEY"],
        api_secret=os.environ["CLOB_SECRET"],
        api_passphrase=os.environ["CLOB_PASS"],
        funder=os.getenv("POLYMARKET_FUNDER") or None,
        signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "0")),
    )


def _submit_limit_buy(client, token_id: str, submit_price: float, shares: float) -> dict:
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY

    args = OrderArgs(token_id=token_id, price=submit_price, size=shares, side=BUY, fee_rate_bps=0)
    signed = client.create_order(args)
    return client.post_order(signed, OrderType.GTC)


def _submit_limit_sell(client, token_id: str, submit_price: float, shares: float) -> dict:
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import SELL

    args = OrderArgs(token_id=token_id, price=submit_price, size=shares, side=SELL, fee_rate_bps=0)
    signed = client.create_order(args)
    return client.post_order(signed, OrderType.GTC)


def _cancel_if_open(client, token_id: str, order_id: str | None) -> bool:
    if not order_id:
        return False
    try:
        open_orders = get_open_orders(client, token_id)
    except Exception as exc:
        log.warning("Open-order check failed for token %s: %s", token_id, exc)
        return False
    is_open = any(str(o.get("id") or o.get("orderID") or "") == str(order_id) for o in open_orders)
    if not is_open:
        return False
    try:
        cancel_order(client, str(order_id))
        return True
    except Exception as exc:
        log.warning("Cancel failed for %s: %s", order_id, exc)
        return False


def _is_order_open(client, token_id: str, order_id: str | None) -> bool:
    if not order_id:
        return False
    try:
        open_orders = get_open_orders(client, token_id)
    except Exception as exc:
        log.warning("Open-order check failed for token %s: %s", token_id, exc)
        return False
    return any(str(o.get("id") or o.get("orderID") or "") == str(order_id) for o in open_orders)


def _calc_shares(desired: float, max_spend_usdc: float | None, price: float, balance: float | None) -> float:
    cap = balance if balance is not None else float("inf")
    if max_spend_usdc is not None:
        cap = min(cap, max_spend_usdc)
    affordable = cap / max(price, 1e-9)
    use = min(desired, affordable)
    return round(max(0.0, use), 2)


def _fetch_spot_eth() -> float | None:
    try:
        data = _get_json(BINANCE_TICKER, params={"symbol": "ETHUSDT"}, timeout=5)
        price = float(data.get("price"))
        return price if price > 0 else None
    except Exception as exc:
        log.debug("Spot fetch failed: %s", exc)
        return None


def _get_spot_eth() -> float | None:
    if _spot_cache is not None and (time.time() - _spot_updated_at) <= SPOT_STALE_SEC:
        return _spot_cache
    return _fetch_spot_eth()


def _move_pct_from_tape(tape: deque[tuple[float, float]], lookback_sec: float) -> float | None:
    if len(tape) < 2:
        return None
    now_ts, now_price = tape[-1]
    start_price = None
    for ts, price in reversed(tape):
        if now_ts - ts >= lookback_sec:
            start_price = price
            break
    if start_price is None or start_price <= 0:
        return None
    return (now_price / start_price - 1.0) * 100.0


def run_eth3cents(
    *,
    bet: bool,
    poll: float,
    shares: float,
    order_price: float,
    cancel_before_sec: int,
    take_profit_price: float,
    db_path: Path,
    log_path: Path,
    host: str,
    max_spend_usdc: float | None,
    once: bool,
) -> None:
    _init_logging(log_path)
    conn = _init_db(db_path)

    if bet:
        _derive_clob_credentials()
        client = _build_client_from_env()
    else:
        client = None

    poly_ws_thread = threading.Thread(target=_poly_ws_thread_main, daemon=True, name="eth3cents-poly-ws")
    poly_ws_thread.start()
    binance_ws_thread = threading.Thread(target=_binance_ws_thread_main, daemon=True, name="eth3cents-binance-ws")
    binance_ws_thread.start()

    candle_orders: dict[str, CandleOrders] = {}
    market: Market | None = None
    last_market_refresh = 0

    log.info(
        "Started eth3cents bet=%s poll=%.2fs order_price=%.3f cancel_before=%ss tp=%.3f",
        bet,
        poll,
        order_price,
        cancel_before_sec,
        take_profit_price,
    )

    while True:
        now = int(time.time())

        if market is None or now >= market.candle_end or (now - last_market_refresh) >= 10:
            market = _fetch_active_eth_market()
            last_market_refresh = now
            if market:
                log.info("MARKET %s ends_in=%ds", market.slug, max(0, market.candle_end - now))
                _ws_subscribe([market.up_token, market.down_token])
                if market.slug not in candle_orders:
                    candle_orders[market.slug] = CandleOrders(
                        slug=market.slug,
                        up_token=market.up_token,
                        down_token=market.down_token,
                    )
            else:
                log.warning("No active ETH 5m market found")

        if market:
            secs_remaining = max(0, market.candle_end - int(time.time()))
            state = candle_orders.get(market.slug)
            if state and not state.up_order_id and not _order_already_attempted(conn, market.slug, "Up"):
                bal = None
                if bet and client is not None:
                    try:
                        bal = get_usdc_balance(client)
                    except Exception as exc:
                        log.warning("Balance fetch failed: %s", exc)
                use_shares = _calc_shares(shares, max_spend_usdc=max_spend_usdc, price=order_price, balance=bal)
                if use_shares >= 0.01:
                    spot = _get_spot_eth() or 0.0
                    if not bet or client is None:
                        state.up_order_id = "DRY_RUN"
                        state.up_shares = use_shares
                        _record_attempt(
                            conn,
                            slug=market.slug,
                            side="Up",
                            token_id=market.up_token,
                            trigger_ask=order_price,
                            entry_ask=order_price,
                            submit_price=order_price,
                            trigger_spot=spot,
                            confirm_spot=spot,
                            move_pct=0.0,
                            rebound_move_pct=0.0,
                            secs_remaining=secs_remaining,
                            shares=use_shares,
                            dry_run=True,
                            order_id="DRY_RUN",
                            order_status="dry_run",
                            response={"slug": market.slug, "side": "Up", "price": order_price, "shares": use_shares},
                            error=None,
                        )
                        log.info("DRY ORDER %s Up @ %.3f shares=%.2f", market.slug, order_price, use_shares)
                    else:
                        try:
                            resp = _submit_limit_buy(client, market.up_token, order_price, use_shares)
                            state.up_order_id = str(resp.get("orderID") or resp.get("id") or "?")
                            state.up_shares = use_shares
                            _record_attempt(
                                conn,
                                slug=market.slug,
                                side="Up",
                                token_id=market.up_token,
                                trigger_ask=order_price,
                                entry_ask=order_price,
                                submit_price=order_price,
                                trigger_spot=spot,
                                confirm_spot=spot,
                                move_pct=0.0,
                                rebound_move_pct=0.0,
                                secs_remaining=secs_remaining,
                                shares=use_shares,
                                dry_run=False,
                                order_id=state.up_order_id,
                                order_status=str(resp.get("status") or "submitted"),
                                response=resp,
                                error=None,
                            )
                            log.info("ORDER %s Up id=%s @ %.3f shares=%.2f", market.slug, state.up_order_id, order_price, use_shares)
                        except Exception as exc:
                            _record_attempt(
                                conn,
                                slug=market.slug,
                                side="Up",
                                token_id=market.up_token,
                                trigger_ask=order_price,
                                entry_ask=order_price,
                                submit_price=order_price,
                                trigger_spot=spot,
                                confirm_spot=spot,
                                move_pct=0.0,
                                rebound_move_pct=0.0,
                                secs_remaining=secs_remaining,
                                shares=use_shares,
                                dry_run=False,
                                order_id=None,
                                order_status="error",
                                response=None,
                                error=str(exc),
                            )
                            log.error("ORDER FAILED %s Up: %s", market.slug, exc)

            if state and not state.down_order_id and not _order_already_attempted(conn, market.slug, "Down"):
                bal = None
                if bet and client is not None:
                    try:
                        bal = get_usdc_balance(client)
                    except Exception as exc:
                        log.warning("Balance fetch failed: %s", exc)
                use_shares = _calc_shares(shares, max_spend_usdc=max_spend_usdc, price=order_price, balance=bal)
                if use_shares >= 0.01:
                    spot = _get_spot_eth() or 0.0
                    if not bet or client is None:
                        state.down_order_id = "DRY_RUN"
                        state.down_shares = use_shares
                        _record_attempt(
                            conn,
                            slug=market.slug,
                            side="Down",
                            token_id=market.down_token,
                            trigger_ask=order_price,
                            entry_ask=order_price,
                            submit_price=order_price,
                            trigger_spot=spot,
                            confirm_spot=spot,
                            move_pct=0.0,
                            rebound_move_pct=0.0,
                            secs_remaining=secs_remaining,
                            shares=use_shares,
                            dry_run=True,
                            order_id="DRY_RUN",
                            order_status="dry_run",
                            response={"slug": market.slug, "side": "Down", "price": order_price, "shares": use_shares},
                            error=None,
                        )
                        log.info("DRY ORDER %s Down @ %.3f shares=%.2f", market.slug, order_price, use_shares)
                    else:
                        try:
                            resp = _submit_limit_buy(client, market.down_token, order_price, use_shares)
                            state.down_order_id = str(resp.get("orderID") or resp.get("id") or "?")
                            state.down_shares = use_shares
                            _record_attempt(
                                conn,
                                slug=market.slug,
                                side="Down",
                                token_id=market.down_token,
                                trigger_ask=order_price,
                                entry_ask=order_price,
                                submit_price=order_price,
                                trigger_spot=spot,
                                confirm_spot=spot,
                                move_pct=0.0,
                                rebound_move_pct=0.0,
                                secs_remaining=secs_remaining,
                                shares=use_shares,
                                dry_run=False,
                                order_id=state.down_order_id,
                                order_status=str(resp.get("status") or "submitted"),
                                response=resp,
                                error=None,
                            )
                            log.info("ORDER %s Down id=%s @ %.3f shares=%.2f", market.slug, state.down_order_id, order_price, use_shares)
                        except Exception as exc:
                            _record_attempt(
                                conn,
                                slug=market.slug,
                                side="Down",
                                token_id=market.down_token,
                                trigger_ask=order_price,
                                entry_ask=order_price,
                                submit_price=order_price,
                                trigger_spot=spot,
                                confirm_spot=spot,
                                move_pct=0.0,
                                rebound_move_pct=0.0,
                                secs_remaining=secs_remaining,
                                shares=use_shares,
                                dry_run=False,
                                order_id=None,
                                order_status="error",
                                response=None,
                                error=str(exc),
                            )
                            log.error("ORDER FAILED %s Down: %s", market.slug, exc)

            if state and bet and client is not None:
                funder = os.getenv("POLYMARKET_FUNDER")
                position_sizes: dict[str, float] = {}
                if funder:
                    try:
                        pos = get_positions(funder)
                        position_sizes = {str(p.get("token_id")): float(p.get("size") or 0.0) for p in pos}
                    except Exception as exc:
                        log.warning("Position fetch failed: %s", exc)
                else:
                    log.warning("POLYMARKET_FUNDER missing; auto TP sell may be less accurate")

                if state.up_order_id and not state.up_exit_submitted and state.up_order_id != "DRY_RUN":
                    if not _is_order_open(client, state.up_token, state.up_order_id):
                        held = float(position_sizes.get(state.up_token, 0.0))
                        sell_size = round(held if held > 0 else state.up_shares, 2)
                        if sell_size >= 0.01:
                            try:
                                resp = _submit_limit_sell(client, state.up_token, take_profit_price, sell_size)
                                state.up_sell_order_id = str(resp.get("orderID") or resp.get("id") or "?")
                                state.up_exit_submitted = True
                                log.info(
                                    "TP SELL %s Up token=%s size=%.2f price=%.3f id=%s",
                                    state.slug,
                                    state.up_token,
                                    sell_size,
                                    take_profit_price,
                                    state.up_sell_order_id,
                                )
                            except Exception as exc:
                                log.warning("TP sell failed %s Up: %s", state.slug, exc)
                        else:
                            state.up_exit_submitted = True
                            log.info("No Up fill detected for %s after buy left book", state.slug)

                if state.down_order_id and not state.down_exit_submitted and state.down_order_id != "DRY_RUN":
                    if not _is_order_open(client, state.down_token, state.down_order_id):
                        held = float(position_sizes.get(state.down_token, 0.0))
                        sell_size = round(held if held > 0 else state.down_shares, 2)
                        if sell_size >= 0.01:
                            try:
                                resp = _submit_limit_sell(client, state.down_token, take_profit_price, sell_size)
                                state.down_sell_order_id = str(resp.get("orderID") or resp.get("id") or "?")
                                state.down_exit_submitted = True
                                log.info(
                                    "TP SELL %s Down token=%s size=%.2f price=%.3f id=%s",
                                    state.slug,
                                    state.down_token,
                                    sell_size,
                                    take_profit_price,
                                    state.down_sell_order_id,
                                )
                            except Exception as exc:
                                log.warning("TP sell failed %s Down: %s", state.slug, exc)
                        else:
                            state.down_exit_submitted = True
                            log.info("No Down fill detected for %s after buy left book", state.slug)

            if state and bet and client is not None and not state.canceled and secs_remaining <= cancel_before_sec:
                cancelled_any = False
                if _cancel_if_open(client, state.up_token, state.up_order_id):
                    cancelled_any = True
                    log.info("CANCEL %s Up order=%s", state.slug, state.up_order_id)
                if _cancel_if_open(client, state.down_token, state.down_order_id):
                    cancelled_any = True
                    log.info("CANCEL %s Down order=%s", state.slug, state.down_order_id)
                state.canceled = True
                if not cancelled_any:
                    log.info("CANCEL window reached for %s, no open orders left", state.slug)

        _settle_orders(conn)

        if once:
            break
        time.sleep(max(0.2, poll))


def main() -> None:
    load_dotenv()
    p = argparse.ArgumentParser(description="ETH 5m dual-order strategy: place both sides at 3c, cancel near expiry")
    p.add_argument("--bet", action="store_true", help="Place real orders (default: dry-run)")
    p.add_argument("--once", action="store_true", help="Run one loop and exit")
    p.add_argument("--poll", type=float, default=DEFAULT_POLL, help="Seconds between loops")
    p.add_argument("--shares", type=float, default=DEFAULT_SHARES, help="Target shares per entry")
    p.add_argument("--price", type=float, default=DEFAULT_MIN_PRICE, help="Limit buy price for both Up and Down (default 0.03)")
    p.add_argument("--cancel-before-sec", type=int, default=12, help="Cancel open orders this many seconds before close")
    p.add_argument("--take-profit-price", type=float, default=0.05, help="Post-fill limit sell price (e.g. 0.04 or 0.05)")
    p.add_argument("--max-balance", type=float, default=None, help="Max USDC this process may spend")
    p.add_argument("--db", type=Path, default=DB_PATH_DEFAULT, help="SQLite DB path")
    p.add_argument("--log", type=Path, default=LOG_PATH_DEFAULT, help="Log file path")
    p.add_argument("--host", default=os.getenv("POLYMARKET_HOST", CLOB_HOST_DEFAULT), help="Polymarket CLOB host")
    args = p.parse_args()

    run_eth3cents(
        bet=args.bet,
        poll=max(0.2, args.poll),
        shares=max(0.01, args.shares),
        order_price=max(0.01, args.price),
        cancel_before_sec=max(1, args.cancel_before_sec),
        take_profit_price=max(0.01, args.take_profit_price),
        db_path=args.db,
        log_path=args.log,
        host=args.host,
        max_spend_usdc=args.max_balance,
        once=args.once,
    )


if __name__ == "__main__":
    main()
