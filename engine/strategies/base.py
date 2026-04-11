"""
Abstract strategy interface.

Strategies are STATELESS in memory. All persistent state (entry price,
leg quantities, cumulative funding, position ID) lives in the DB.
On startup, strategies recover state by querying repository.get_open_positions().

State machine for two-leg strategies:

  IDLE ──► ENTERING ──► ACTIVE ──► EXITING ──► IDLE
             │    │          │                   ▲
             │    └─ leg B   │                   │
             │    fail ──►   │                   │
             │    UNWIND_A   │ exit triggered     │
             │       │       │ (circuit breaker,  │
             │       └──►────┤  expiry, funding   │
             │           IDLE│  flip, manual)     │
             │                                    │
             └──────────────────────────────────── ┘
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from engine.db.models import Position
from engine.exchange.base import ExchangeBase
from engine.order_manager import OrderManager
from engine.position_tracker import PositionTracker
from engine.risk_guard import RiskGuard


class Strategy(ABC):
    name: str = "base"

    def __init__(
        self,
        exchange: ExchangeBase,
        order_manager: OrderManager,
        position_tracker: PositionTracker,
        risk_guard: RiskGuard,
        config: dict,
    ):
        self._exchange = exchange
        self._order_mgr = order_manager
        self._tracker = position_tracker
        self._risk = risk_guard
        self._config = config

    @abstractmethod
    async def should_enter(self) -> bool:
        """Return True if conditions are met to open a new position."""
        ...

    @abstractmethod
    async def should_exit(self, position: Position) -> bool:
        """Return True if the given position should be closed."""
        ...

    @abstractmethod
    async def enter(self) -> Optional[int]:
        """Open a new position. Returns position_id or None on failure."""
        ...

    @abstractmethod
    async def exit(self, position: Position) -> bool:
        """Close the given position. Returns True on success."""
        ...

    async def on_funding_payment(self, event: dict) -> None:
        """Called when a funding payment is received or paid. Override to update state."""
        pass

    async def continue_entry(self, position: Position) -> None:
        """
        Called each tick while a position is in ENTERING state.
        TwoLegStrategy overrides this to attempt the next orderbook-sized slice.
        """
        pass

    async def run_once(self) -> None:
        """
        Single strategy iteration. Called by the engine's main loop.
        Strategies should not implement long loops here — keep it a single
        check-and-act cycle so the event loop stays responsive.
        """
        from engine.db import repository
        from engine.db.models import PositionState

        # Wait for position tracker to be ready (handles WS reconnect)
        await self._tracker.wait_ready()

        # Check risk guard before any action
        margin_result = await self._risk.check_margin()
        from engine.risk_guard import RiskAction

        # Always process exits and in-flight entries — even on HARD_STOP.
        # Only block new entries when margin is stressed.
        open_positions = await repository.get_open_positions(strategy=self.name)
        for pos in open_positions:
            if pos.state == PositionState.ENTERING:
                if margin_result.action != RiskAction.HARD_STOP:
                    await self.continue_entry(pos)
            elif await self.should_exit(pos):
                await self.exit(pos)

        # Try to enter only when risk is acceptable
        if margin_result.action == RiskAction.HARD_STOP:
            return
        if not open_positions and await self.should_enter():
            if margin_result.action != RiskAction.WARNING:
                await self.enter()
