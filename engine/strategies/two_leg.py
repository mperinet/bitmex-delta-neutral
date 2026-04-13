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

import structlog

from engine.db import repository
from engine.db.models import Position, PositionState
from engine.strategies.base import Strategy

logger = structlog.get_logger(__name__)


@dataclass
class LegSpec:
    symbol: str
    side: str  # "buy" | "sell"
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

    async def compute_entry_spec(self) -> EntrySpec | None:
        """
        Compute leg symbols, sides, and total target quantities.
        Subclasses must implement this.
        Returns None if entry is not possible (e.g. insufficient balance).
        """
        raise NotImplementedError

    async def _qty_for_usd_notional(
        self, symbol: str, usd_notional: float, mark_price: float | None = None
    ) -> float:
        """
        Convert a USD notional to the correct native contract quantity for any symbol.

        Resolves mark price from the WS instrument cache first (zero REST calls if
        the cache is warm), falling back to a REST ticker call. Delegates contract
        type detection and lot-size rounding to OrderManager.usd_to_contract_qty().

        For quanto contracts (e.g. ETH/USD:BTC) also fetches the BTC/USD price,
        since quanto contract value in USD depends on both the underlying price and
        the BTC settlement rate.

        Pass mark_price explicitly to reuse a price already fetched in the caller
        and avoid the redundant REST call.
        """
        if mark_price is None:
            assert self._tracker is not None
            mark_price = self._tracker.market_data.get_mark_price(symbol)
        if mark_price is None:
            ticker = await self._exchange.get_ticker(symbol)
            mark_price = ticker.mark_price

        btc_price: float | None = None
        if self._order_mgr._ccxt_contract_type(symbol) == "quanto":
            # Quanto contracts settle in BTC; sizing requires the BTC/USD rate.
            assert self._tracker is not None
            btc_price = self._tracker.market_data.get_mark_price("XBTUSD")
            if btc_price is None:
                ticker = await self._exchange.get_ticker("BTC/USD:BTC")
                btc_price = ticker.mark_price

        return self._order_mgr.usd_to_contract_qty(symbol, usd_notional, mark_price, btc_price)

    async def enter(self) -> int | None:
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
            entry_slices_total=0,  # dynamic — no fixed slice count
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
        assert position.leg_a_symbol is not None
        assert position.leg_a_side is not None
        assert position.leg_b_symbol is not None
        assert position.leg_b_side is not None

        remaining_a = (position.leg_a_target_qty or 0.0) - position.leg_a_qty
        remaining_b = (position.leg_b_target_qty or 0.0) - position.leg_b_qty

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

    async def force_abort(self) -> None:
        """
        Forcefully unwind any in-flight positions for this strategy.

        Called when an operator queues an abort control signal from the dashboard.
        Works on any position state (ENTERING, ACTIVE, EXITING). Unwinds whatever
        has been filled so far on both legs using emergency market orders, closes
        the DB record, and marks the strategy instance as done so it won't re-enter.

        Uses exit() directly — exit() already unwinds only what's been filled
        (leg_a_qty / leg_b_qty) rather than the target quantity, so it's correct
        for partially-filled ENTERING positions too.
        """
        open_positions = await repository.get_open_positions(strategy=self.name)
        if not open_positions:
            logger.info("force_abort_no_open_positions", strategy=self.name)
            self._done = True
            return

        for pos in open_positions:
            logger.warning(
                "force_abort_unwinding",
                strategy=self.name,
                position_id=pos.id,
                state=pos.state,
                leg_a_filled=pos.leg_a_qty,
                leg_b_filled=pos.leg_b_qty,
            )
            try:
                await self.exit(pos)
            except Exception as e:
                logger.error(
                    "force_abort_exit_failed",
                    strategy=self.name,
                    position_id=pos.id,
                    error=str(e),
                )
                # Mark closed in DB even if unwind failed — operator must check exchange
                await repository.close_position(pos.id, realised_pnl=0.0)

        self._done = True
        logger.info("force_abort_complete", strategy=self.name)

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

        assert position.leg_a_symbol is not None
        assert position.leg_a_side is not None
        assert position.leg_b_symbol is not None
        assert position.leg_b_side is not None

        await repository.update_position(position.id, state=PositionState.EXITING)

        # Close leg A (reverse the side: entry was "sell" → exit is "buy", and vice versa)
        if position.leg_a_qty > 0:
            exit_side_a = "buy" if position.leg_a_side == "sell" else "sell"
            order_a = await self._order_mgr.place_market(
                position.leg_a_symbol, exit_side_a, position.leg_a_qty, emergency=True
            )
            await repository.record_trade(
                position.id,
                self.name,
                "a",
                order_a.order_id,
                position.leg_a_symbol,
                exit_side_a,
                order_a.filled_qty,
                order_a.avg_price,
                order_a.fee,
                is_entry=False,
            )

        # Close leg B
        if position.leg_b_qty > 0:
            exit_side_b = "buy" if position.leg_b_side == "sell" else "sell"
            order_b = await self._order_mgr.place_market(
                position.leg_b_symbol, exit_side_b, position.leg_b_qty, emergency=True
            )
            await repository.record_trade(
                position.id,
                self.name,
                "b",
                order_b.order_id,
                position.leg_b_symbol,
                exit_side_b,
                order_b.filled_qty,
                order_b.avg_price,
                order_b.fee,
                is_entry=False,
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
