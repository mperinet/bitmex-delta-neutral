"""
TwoLegStrategy — base class for all delta-neutral two-leg trades.

Shared across Strategy 1 (cash-and-carry) and Strategy 2 (funding harvest).
Both strategies have:
  - Leg A: short derivative (future or perp)
  - Leg B: long hedge (perp or spot)
  - Orderbook-aware progressive entry (market orders, one slice per loop tick)
  - Shared exit logic (market orders on both legs)

Subclasses override:
  - compute_entry_spec(): return (leg_a_symbol, leg_a_side, leg_b_symbol, leg_b_side,
                                  qty_a, qty_b, metadata)
  - should_enter(): strategy-specific signal
  - should_exit(): strategy-specific exit conditions

Entry flow across loop ticks:
  Tick N   — enter() creates DB record + attempts first slice
  Tick N+1 — continue_entry() attempts next slice (if still ENTERING)
  ...
  Tick N+k — final slice fills, position transitions to ACTIVE
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
    side: str   # "buy" | "sell"
    qty: float  # total target quantity (USD for inverse, base ccy for spot)


@dataclass
class EntrySpec:
    leg_a: LegSpec
    leg_b: LegSpec
    metadata: dict  # strategy-specific fields stored on the Position row


class TwoLegStrategy(Strategy):
    """
    Intermediate base class for two-leg delta-neutral strategies.
    Handles entry mechanics (orderbook-sized market slices) and exit.
    Subclasses provide signal logic and compute_entry_spec().
    """

    async def compute_entry_spec(self) -> Optional[EntrySpec]:
        """
        Compute leg symbols, sides, and total target quantities.
        Subclasses must implement this.
        Returns None if entry is not possible (e.g. insufficient balance).
        """
        raise NotImplementedError

    async def enter(self) -> Optional[int]:
        spec = await self.compute_entry_spec()
        if spec is None:
            return None

        # Create DB record before placing any orders (stateless recovery requirement)
        position = await repository.create_position(
            strategy=self.name,
            leg_a_symbol=spec.leg_a.symbol,
            leg_a_side=spec.leg_a.side,
            leg_a_target_qty=spec.leg_a.qty,
            leg_b_symbol=spec.leg_b.symbol,
            leg_b_side=spec.leg_b.side,
            leg_b_target_qty=spec.leg_b.qty,
            entry_slices_total=0,   # dynamic — no fixed slice count
            **spec.metadata,
        )

        logger.info(
            "strategy_entering",
            strategy=self.name,
            position_id=position.id,
            leg_a=f"{spec.leg_a.side} {spec.leg_a.qty} {spec.leg_a.symbol}",
            leg_b=f"{spec.leg_b.side} {spec.leg_b.qty} {spec.leg_b.symbol}",
        )

        # Attempt first slice immediately; remaining slices come on subsequent ticks
        await self.continue_entry(position)
        return position.id

    async def continue_entry(self, position: Position) -> None:
        """
        Attempt one orderbook-sized market slice on both legs.
        Called each loop tick while the position is in ENTERING state.
        Skips silently if orderbook depth is insufficient (tries again next tick).
        """
        remaining_a = (position.leg_a_target_qty or 0) - (position.leg_a_qty or 0)
        remaining_b = (position.leg_b_target_qty or 0) - (position.leg_b_qty or 0)

        filled = await self._order_mgr.fill_next_slice(
            position_id=position.id,
            strategy=self.name,
            leg_a_symbol=position.leg_a_symbol,
            leg_a_side=position.leg_a_side,
            leg_a_remaining=remaining_a,
            leg_b_symbol=position.leg_b_symbol,
            leg_b_side=position.leg_b_side,
            leg_b_remaining=remaining_b,
        )

        if not filled:
            logger.info(
                "continue_entry_skipped",
                strategy=self.name,
                position_id=position.id,
                remaining_a=remaining_a,
                remaining_b=remaining_b,
                reason="insufficient_depth",
            )

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
