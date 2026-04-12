"""
Position tracker — maintains real-time position and margin state via WebSocket.

On startup: reconciles DB positions with live exchange state (REST).
On WS reconnect: reconciles again before signaling 'ready' to strategies.
During operation: updates in-memory position/margin state from WS topics.

Instrument/funding metadata (funding rates, mark prices) is handled by
MarketDataCache (engine/market_data.py), which this tracker populates from
the WS feed but does not own conceptually. Strategies read instrument data
from tracker.market_data, not from the tracker itself.

State recovery rule: strategies are stateless. On startup, position_tracker
loads all non-IDLE positions from DB and cross-checks them against the
live exchange. Any discrepancy (DB has it, exchange doesn't) is flagged
as an orphan and triggers an alert.

Subscriptions used:
  - position: position changes (fills, funding, liquidation warnings)
  - execution: fill confirmations
  - funding: funding rate payments received/paid → forwarded to MarketDataCache
  - instrument: mark price, funding rate → forwarded to MarketDataCache
  - margin: available balance updates
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import datetime

import structlog
import websockets

from engine.db import repository
from engine.exchange.base import ExchangeBase
from engine.market_data import MarketDataCache
from engine.risk_guard import RiskGuard

logger = structlog.get_logger(__name__)

# WebSocket topics to subscribe to
WS_TOPICS = [
    "position",
    "execution",
    "funding",
    "instrument",
    "margin",
    "order",
]


class PositionTracker:
    def __init__(
        self,
        exchange: ExchangeBase,
        risk_guard: RiskGuard,
        ws_url: str,
        api_key: str,
        api_secret: str,
        on_funding_payment: Callable | None = None,
    ):
        self._exchange = exchange
        self._risk_guard = risk_guard
        self._ws_url = ws_url
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_funding_payment = on_funding_payment

        # In-memory cache of live position and margin state (keyed by symbol)
        self._live_positions: dict[str, dict] = {}
        self._live_margin: dict = {}

        # Instrument/funding metadata — owned by MarketDataCache, populated here from WS
        self.market_data = MarketDataCache()

        self._ready = asyncio.Event()  # set once reconciliation completes
        self._ws_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Startup reconciliation
    # ------------------------------------------------------------------

    async def reconcile_with_exchange(self) -> None:
        """
        Reconcile DB positions against live exchange state.
        Called on startup AND on every WS reconnect before strategies resume.

        Orphan detection:
          - DB has ACTIVE/ENTERING position, exchange has no matching position
            → flag as orphan, alert operator
          - Exchange has position, DB has no record
            → flag as unknown position, alert operator
        """
        logger.info("reconciliation_started")

        # Fetch live positions from exchange (REST)
        live = await self._exchange.get_open_positions()
        live_by_symbol = {p["symbol"]: p for p in live}

        # Update in-memory cache
        self._live_positions = live_by_symbol

        # Fetch DB positions that should be open
        db_positions = await repository.get_open_positions()

        for db_pos in db_positions:
            leg_a_live = live_by_symbol.get(db_pos.leg_a_symbol)
            leg_b_live = live_by_symbol.get(db_pos.leg_b_symbol)

            if not leg_a_live and not leg_b_live:
                logger.error(
                    "orphan_position_detected",
                    position_id=db_pos.id,
                    strategy=db_pos.strategy,
                    leg_a=db_pos.leg_a_symbol,
                    leg_b=db_pos.leg_b_symbol,
                    action="operator_review_required",
                )
                continue

            if not leg_a_live or not leg_b_live:
                missing = db_pos.leg_a_symbol if not leg_a_live else db_pos.leg_b_symbol
                logger.warning(
                    "partial_orphan_detected",
                    position_id=db_pos.id,
                    missing_leg=missing,
                    action="operator_review_required",
                )

        # Flag exchange positions with no DB record
        db_symbols = set()
        for p in db_positions:
            if p.leg_a_symbol:
                db_symbols.add(p.leg_a_symbol)
            if p.leg_b_symbol:
                db_symbols.add(p.leg_b_symbol)

        for symbol, live_pos in live_by_symbol.items():
            if symbol not in db_symbols:
                logger.warning(
                    "unknown_exchange_position",
                    symbol=symbol,
                    size=live_pos.get("contracts"),
                    action="operator_review_required",
                )

        logger.info("reconciliation_complete", db_positions=len(db_positions), live=len(live))

    # ------------------------------------------------------------------
    # WebSocket feed
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Reconcile then start the WS feed."""
        await self.reconcile_with_exchange()
        self._ready.set()
        self._ws_task = asyncio.create_task(self._ws_loop())

    async def wait_ready(self) -> None:
        """Strategies call this before acting. Blocks during reconnect."""
        await self._ready.wait()

    async def _ws_loop(self) -> None:
        """WS connection loop with reconnect and reconciliation."""
        while True:
            try:
                await self._connect_and_stream()
            except (websockets.ConnectionClosed, OSError) as e:
                logger.warning("ws_disconnected", error=str(e))
                self._ready.clear()
                self._risk_guard.set_reconnecting(True)
                await asyncio.sleep(2)

                reconciled = False
                for attempt in range(3):
                    try:
                        await self.reconcile_with_exchange()
                        reconciled = True
                        break
                    except Exception as rec_err:
                        logger.error(
                            "reconciliation_failed",
                            attempt=attempt + 1,
                            error=str(rec_err),
                        )
                        if attempt < 2:
                            await asyncio.sleep(5)

                self._risk_guard.set_reconnecting(False)
                if reconciled:
                    self._ready.set()
                    logger.info("ws_reconnected_and_reconciled")
                else:
                    logger.critical(
                        "reconciliation_exhausted_retries",
                        note="strategies remain paused — manual intervention required",
                    )

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("ws_unexpected_error", error=str(e))
                await asyncio.sleep(5)

    async def _connect_and_stream(self) -> None:
        auth = self._build_auth()
        async with websockets.connect(self._ws_url, max_size=None) as ws:
            # Authenticate
            await ws.send(json.dumps(auth))
            # Subscribe to topics
            await ws.send(json.dumps({"op": "subscribe", "args": WS_TOPICS}))
            logger.info("ws_connected", url=self._ws_url)

            async for raw in ws:
                # Offload to a task so the WS read loop isn't blocked by
                # large snapshots (e.g. instrument partial with 300+ items).
                asyncio.create_task(self._safe_handle(raw))

    async def _safe_handle(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
            await self._handle_message(msg)
        except Exception as e:
            logger.error("ws_message_handler_error", error=str(e))

    def _build_auth(self) -> dict:
        """Build BitMEX WebSocket auth message."""
        import hashlib
        import hmac
        import time

        expires = int(time.time()) + 60
        sig = hmac.new(
            self._api_secret.encode(),
            f"GET/realtime{expires}".encode(),
            hashlib.sha256,
        ).hexdigest()
        return {"op": "authKeyExpires", "args": [self._api_key, expires, sig]}

    async def _handle_message(self, msg: dict) -> None:
        table = msg.get("table")
        data = msg.get("data", [])

        if not table or not data:
            return

        if table == "position":
            for item in data:
                symbol = item.get("symbol")
                if symbol:
                    if symbol not in self._live_positions:
                        self._live_positions[symbol] = {}
                    self._live_positions[symbol].update(item)

        elif table == "funding":
            for item in data:
                symbol = item.get("symbol")
                if symbol:
                    self.market_data.update_funding(symbol, item)
                    if self._on_funding_payment:
                        await self._on_funding_payment(item)
                    await self._record_funding_rate(item)

        elif table == "instrument":
            for item in data:
                symbol = item.get("symbol")
                if symbol:
                    # Forward to MarketDataCache. We do NOT write to the DB on every
                    # WS tick — the instrument snapshot can contain 300+ rows and
                    # sequential DB commits would block the loop.
                    self.market_data.update_instrument(symbol, item)

        elif table == "margin":
            if data:
                self._live_margin = data[0]

    async def _record_funding_rate(self, item: dict) -> None:
        symbol = item.get("symbol")
        rate = item.get("fundingRate")
        ts_raw = item.get("timestamp") or item.get("fundingTimestamp")
        if symbol and rate is not None and ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                await repository.insert_funding_rate(symbol, ts, rate)
            except Exception as e:
                logger.warning("funding_rate_record_failed", error=str(e))

    # ------------------------------------------------------------------
    # Accessors for strategies and risk guard
    # ------------------------------------------------------------------

    def get_live_position(self, symbol: str) -> dict | None:
        return self._live_positions.get(symbol)

    def get_nav_usd(self, btc_price: float) -> float:
        """Approximate NAV in USD from wallet balance * BTC price."""
        btc_balance = self._live_margin.get("walletBalance", 0) / 1e8  # satoshis → BTC
        return btc_balance * btc_price

    def get_net_delta_usd(self) -> float:
        """
        Sum of all open position deltas in USD.

        BitMEX contract settlement types have different currentQty semantics:

        - Inverse (XBTUSD, XBTEUR, XBTETH and quarterly futures like XBTUSDTZ25):
            BTC-settled. currentQty is the USD notional (number of $1 contracts).
            delta_usd = currentQty

        - Quanto (ETHUSD, SOLUSD, etc.):
            BTC-settled at a fixed XBT multiplier (0.000001 XBT/contract).
            NOT inverse. currentQty is in contracts (USD-quoted, BTC-settled).
            delta_usd ≈ currentQty × markPrice  (first-order; ignores quanto
            convexity adjustment, acceptable for delta-neutral threshold checks)

        - Linear / Spot (XBTUSDT, XBT_USDT, ETH_USDT):
            USDT-settled or spot. currentQty is in the base currency (BTC/ETH).
            delta_usd = currentQty × markPrice

        Contract type is resolved via market_data.is_inverse_contract() which
        prefers the WS instrument cache (isInverse flag) over symbol pattern
        matching. Mark price for non-inverse positions is taken from the position
        record first, then from market_data.get_mark_price().
        """
        total = 0.0
        for symbol, pos in self._live_positions.items():
            qty = pos.get("currentQty", 0)
            if not qty:
                continue
            if self.market_data.is_inverse_contract(symbol):
                total += qty
            else:
                mark_price = pos.get("markPrice") or self.market_data.get_mark_price(symbol) or 0.0
                if mark_price > 0:
                    total += qty * mark_price
                else:
                    logger.warning(
                        "delta_price_unavailable",
                        symbol=symbol,
                        qty=qty,
                        action="position_excluded_from_delta",
                    )
        return total

    def stop(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
