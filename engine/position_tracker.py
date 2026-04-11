"""
Position tracker — maintains real-time position state via WebSocket.

On startup: reconciles DB positions with live exchange state (REST).
On WS reconnect: reconciles again before signaling 'ready' to strategies.
During operation: updates in-memory state from WS 'position' and
'execution' topics.

State recovery rule: strategies are stateless. On startup, position_tracker
loads all non-IDLE positions from DB and cross-checks them against the
live exchange. Any discrepancy (DB has it, exchange doesn't) is flagged
as an orphan and triggers an alert.

Subscriptions used:
  - position: position changes (fills, funding, liquidation warnings)
  - execution: fill confirmations
  - funding: funding rate payments received/paid
  - instrument: mark price, funding rate, indicative rate
  - margin: available balance updates
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Callable, Optional

import websockets
import structlog

from engine.db import repository
from engine.db.models import PositionState
from engine.exchange.base import ExchangeBase
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
        on_funding_payment: Optional[Callable] = None,
    ):
        self._exchange = exchange
        self._risk_guard = risk_guard
        self._ws_url = ws_url
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_funding_payment = on_funding_payment

        # In-memory cache of live position data (keyed by symbol)
        self._live_positions: dict[str, dict] = {}
        self._live_margin: dict = {}
        self._live_funding: dict[str, dict] = {}       # symbol → settlement payment event
        self._live_instruments: dict[str, dict] = {}   # symbol → current instrument data (fundingRate, markPrice, etc.)

        self._ready = asyncio.Event()  # set once reconciliation completes
        self._ws_task: Optional[asyncio.Task] = None

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

                try:
                    await self.reconcile_with_exchange()
                except Exception as rec_err:
                    logger.error("reconciliation_failed", error=str(rec_err))

                self._risk_guard.set_reconnecting(False)
                self._ready.set()
                logger.info("ws_reconnected_and_reconciled")

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

    async def _safe_handle(self, raw: str) -> None:
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
        action = msg.get("action")
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
                    self._live_funding[symbol] = item
                    if self._on_funding_payment:
                        await self._on_funding_payment(item)
                    await self._record_funding_rate(item)

        elif table == "instrument":
            for item in data:
                symbol = item.get("symbol")
                if symbol:
                    # Update in-memory cache only. Strategies read current funding rate
                    # from here without waiting for the 8h settlement `funding` event.
                    # We do NOT write to the DB on every WS tick — the instrument snapshot
                    # can contain 300+ rows and sequential DB commits would block the loop.
                    if symbol not in self._live_instruments:
                        self._live_instruments[symbol] = {}
                    self._live_instruments[symbol].update(item)

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

    def get_live_position(self, symbol: str) -> Optional[dict]:
        return self._live_positions.get(symbol)

    def get_nav_usd(self, btc_price: float) -> float:
        """Approximate NAV in USD from wallet balance * BTC price."""
        btc_balance = self._live_margin.get("walletBalance", 0) / 1e8  # satoshis → BTC
        return btc_balance * btc_price

    def get_net_delta_usd(self) -> float:
        """
        Sum of all open position deltas in USD.
        For inverse contracts: delta = currentQty (in USD contracts).
        For spot: delta = qty * price.
        """
        total = 0.0
        for pos in self._live_positions.items():
            qty = pos.get("currentQty", 0)
            total += qty  # For inverse: currentQty is already in USD
        return total

    def get_latest_funding_rate(self, symbol: str) -> Optional[float]:
        """
        Return the current funding rate for a symbol.

        Priority:
          1. `funding` topic (settlement event) — most recent confirmed payment rate
          2. `instrument` topic (continuous) — current indicative rate, updated every few seconds

        The `funding` topic only fires at 04:00/12:00/20:00 UTC. Between settlements
        the current rate is only available via `instrument.fundingRate`, which is what
        will be paid at the next settlement. Both are used for entry/exit decisions.
        """
        # Prefer settlement event if we have one
        rate = self._live_funding.get(symbol, {}).get("fundingRate")
        if rate is not None:
            return rate
        # Fall back to current indicative rate from instrument stream
        return self._live_instruments.get(symbol, {}).get("fundingRate")

    def stop(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
