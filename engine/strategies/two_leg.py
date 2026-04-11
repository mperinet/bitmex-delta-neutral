"""
TwoLegStrategy — base class for all delta-neutral two-leg trades.

Shared across Strategy 1 (cash-and-carry) and Strategy 2 (funding harvest).
Both strategies have:
  - Leg A: short derivative (future or perp)
  - Leg B: long hedge (perp or spot)
  - Progressive entry (N slices, both legs simultaneously)
  - Shared exit logic (market orders on both legs)

Subclasses override:
  - compute_legs(): return (leg_a_symbol, leg_a_side, leg_b_symbol, leg_b_side,
                            price_a, price_b, qty_a, qty_b, metadata)
  - should_enter(): strategy-specific signal
  - should_exit(): strategy-specific exit conditions
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import structlog

from engine.db import repository
from engine.db.models import Position, PositionState
from engine.strategies.base import Strategy

logger = structlog.get_logger(__name__)


@dataclass
class LegSpec:
    symbol: str
    side: str      # "buy" | "sell"
    qty: float
    price: float


@dataclass
class EntrySpec:
    leg_a: LegSpec
    leg_b: LegSpec
    n_slices: int
    fill_timeout_s: float
    metadata: dict  # strategy-specific data to store on the position


class TwoLegStrategy(Strategy):
    """
    Intermediate base class for two-leg delta-neutral strategies.
    Handles the mechanics of entry, exit, and state transitions.
    Subclasses provide the signal logic.
    """

    @property
    def n_slices(self) -> int:
        return self._config.get("entry_slices", 5)

    @property
    def fill_timeout_s(self) -> float:
        return self._config.get("slice_fill_timeout_s", 30)

    async def compute_entry_spec(self) -> Optional[EntrySpec]:
        """
        Compute the legs and quantities for a new entry.
        Subclasses must implement this.
        Returns None if entry is not possible (e.g., insufficient data).
        """
        raise NotImplementedError

    async def enter(self) -> Optional[int]:
        spec = await self.compute_entry_spec()
        if spec is None:
            return None

        # Create DB record before placing orders (stateless strategy requirement)
        position = await repository.create_position(
            strategy=self.name,
            leg_a_symbol=spec.leg_a.symbol,
            leg_a_side=spec.leg_a.side,
            leg_a_target_qty=spec.leg_a.qty,
            leg_b_symbol=spec.leg_b.symbol,
            leg_b_side=spec.leg_b.side,
            leg_b_target_qty=spec.leg_b.qty,
            entry_slices_total=spec.n_slices,
            **spec.metadata,
        )

        logger.info(
            "strategy_entering",
            strategy=self.name,
            position_id=position.id,
            leg_a=f"{spec.leg_a.side} {spec.leg_a.qty} {spec.leg_a.symbol} @ {spec.leg_a.price}",
            leg_b=f"{spec.leg_b.side} {spec.leg_b.qty} {spec.leg_b.symbol} @ {spec.leg_b.price}",
        )

        success = await self._order_mgr.progressive_entry(
            position_id=position.id,
            strategy=self.name,
            leg_a_symbol=spec.leg_a.symbol,
            leg_a_side=spec.leg_a.side,
            leg_b_symbol=spec.leg_b.symbol,
            leg_b_side=spec.leg_b.side,
            total_qty_a=spec.leg_a.qty,
            total_qty_b=spec.leg_b.qty,
            leg_a_price=spec.leg_a.price,
            leg_b_price=spec.leg_b.price,
            n_slices=spec.n_slices,
            fill_timeout_s=spec.fill_timeout_s,
        )

        if not success:
            # Progressive entry failed partway through — position may be partially filled.
            # Mark as IDLE so it doesn't get re-entered automatically.
            # Operator should review the position.
            await repository.update_position(position.id, state=PositionState.IDLE)
            logger.error(
                "strategy_entry_failed",
                strategy=self.name,
                position_id=position.id,
                action="position_marked_idle_for_review",
            )
            return None

        logger.info(
            "strategy_entered",
            strategy=self.name,
            position_id=position.id,
        )
        return position.id

    async def exit(self, position: Position) -> bool:
        """
        Close both legs with market orders.
        Records trades and marks position as IDLE.
        """
        logger.info(
            "strategy_exiting",
            strategy=self.name,
            position_id=position.id,
            leg_a=position.leg_a_symbol,
            leg_b=position.leg_b_symbol,
        )

        await repository.update_position(position.id, state=PositionState.EXITING)

        # Close leg A (reverse the side)
        if position.leg_a_qty and position.leg_a_qty > 0:
            exit_side_a = "buy" if position.leg_a_side == "short" else "sell"
            order_a = await self._order_mgr.place_market(
                position.leg_a_symbol, exit_side_a, position.leg_a_qty, emergency=True
            )
            await repository.record_trade(
                position.id, self.name, "a", order_a.order_id,
                position.leg_a_symbol, exit_side_a, order_a.filled_qty,
                order_a.avg_price, order_a.fee, is_entry=False,
            )

        # Close leg B
        if position.leg_b_qty and position.leg_b_qty > 0:
            exit_side_b = "buy" if position.leg_b_side == "short" else "sell"
            order_b = await self._order_mgr.place_market(
                position.leg_b_symbol, exit_side_b, position.leg_b_qty, emergency=True
            )
            await repository.record_trade(
                position.id, self.name, "b", order_b.order_id,
                position.leg_b_symbol, exit_side_b, order_b.filled_qty,
                order_b.avg_price, order_b.fee, is_entry=False,
            )

        realised_pnl = (position.unrealised_pnl or 0.0) + (position.realised_pnl or 0.0)
        await repository.close_position(position.id, realised_pnl)

        logger.info(
            "strategy_exited",
            strategy=self.name,
            position_id=position.id,
            realised_pnl=realised_pnl,
        )
        return True
