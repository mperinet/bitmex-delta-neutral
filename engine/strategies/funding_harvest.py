"""
Strategy 2: Funding Rate Harvest

Short the XBTUSD perpetual, buy equivalent spot (XBT_USDT).
Collect funding every 8h (04:00, 12:00, 20:00 UTC) while staying delta-neutral.

Entry signal: funding_rate > threshold (default: 3x baseline = 0.03%/8h)
Exit signals:
  - Funding rate falls below baseline (0.01%/8h)
  - Funding flips negative (you'd be paying as a short; cumulative cost rises)
  - Risk guard: HARD_STOP (margin), REBALANCE (delta drift)
  - Manual / operator trigger

cumulative_funding_paid sign convention (both strategies):
  positive = net cost (paid more than received)
  negative = net income (received more than paid)

Funding rate baseline: 0.01%/8h = ~10.95% APR
Entry threshold (3x): 0.03%/8h = ~32.85% APR
"""

from __future__ import annotations

from typing import Optional

import structlog

from engine.db.models import Position
from engine.exchange.bitmex import BitMEXExchange
from engine.strategies.two_leg import EntrySpec, LegSpec, TwoLegStrategy

logger = structlog.get_logger(__name__)

PERP_SYMBOL = "BTC/USD:BTC"    # ccxt symbol — used for order placement and REST calls
PERP_WS_SYMBOL = "XBTUSD"     # BitMEX native symbol — used for WS instrument/funding lookups
SPOT_SYMBOL = "BTC/USDT"       # XBT_USDT spot


class FundingHarvestStrategy(TwoLegStrategy):
    name = "funding_harvest"

    @property
    def _min_funding_rate(self) -> float:
        baseline = self._config.get("min_funding_rate", 0.0001)
        multiplier = self._config.get("entry_threshold_multiplier", 3)
        return baseline * multiplier

    @property
    def _exit_funding_rate(self) -> float:
        """Exit when rate falls below this."""
        return self._config.get("min_funding_rate", 0.0001)

    @property
    def _max_position_usd(self) -> float:
        return self._config.get("max_position_usd", 10000.0)

    async def should_enter(self) -> bool:
        rate = self._tracker.market_data.get_predictive_funding_rate(PERP_WS_SYMBOL)
        if rate is None:
            logger.debug("funding_harvest_no_rate_data")
            return False
        signal = rate >= self._min_funding_rate
        logger.debug(
            "funding_harvest_entry_check",
            rate=rate,
            threshold=self._min_funding_rate,
            signal=signal,
        )
        return signal

    @property
    def _max_cumulative_funding_cost(self) -> float:
        """Exit if cumulative net cost (positive = paid) exceeds this fraction.
        Default 0.002 = 20bps, roughly 20 adverse 8h periods at baseline."""
        return self._config.get("max_cumulative_funding_cost", 0.002)

    async def should_exit(self, position: Position) -> bool:
        rate = self._tracker.market_data.get_predictive_funding_rate(PERP_WS_SYMBOL)

        # No rate data — don't exit, but log a warning
        if rate is None:
            logger.warning("funding_harvest_no_rate_for_exit_check", position_id=position.id)
            return False

        # Funding flipped negative: we're now paying as a short
        if rate < 0:
            logger.info(
                "funding_harvest_exit_negative_funding",
                rate=rate,
                position_id=position.id,
            )
            return True

        # Funding normalised below entry threshold
        if rate < self._exit_funding_rate:
            logger.info(
                "funding_harvest_exit_rate_normalised",
                rate=rate,
                threshold=self._exit_funding_rate,
                position_id=position.id,
            )
            return True

        # Circuit breaker: cumulative net cost exceeds threshold.
        # positive cumulative_funding_paid = net paid (bad for this strategy).
        cumulative = position.cumulative_funding_paid or 0.0
        if cumulative > self._max_cumulative_funding_cost:
            logger.warning(
                "funding_harvest_exit_cumulative_cost_circuit_breaker",
                cumulative_funding_paid=cumulative,
                threshold=self._max_cumulative_funding_cost,
                position_id=position.id,
            )
            return True

        return False

    async def compute_entry_spec(self) -> Optional[EntrySpec]:
        perp_ticker = await self._exchange.get_ticker(PERP_SYMBOL)
        spot_ticker = await self._exchange.get_ticker(SPOT_SYMBOL)
        balance = await self._exchange.get_balance()

        # Size: use configured max, but don't exceed available margin
        # For inverse contracts, notional in USD = qty (contracts)
        # For spot, qty in BTC = usd_notional / spot_price
        usd_notional = min(
            self._max_position_usd,
            balance.available * perp_ticker.mark_price * 0.40,  # 40% of available
        )

        if usd_notional < 100:
            logger.warning("funding_harvest_insufficient_balance", available=balance.available)
            return None

        perp_qty = usd_notional                             # USD contracts (inverse)
        spot_qty = usd_notional / spot_ticker.ask           # BTC to buy

        return EntrySpec(
            leg_a=LegSpec(symbol=PERP_SYMBOL, side="sell", qty=perp_qty),
            leg_b=LegSpec(symbol=SPOT_SYMBOL, side="buy", qty=spot_qty),
            metadata={},
        )

    async def on_funding_payment(self, event: dict) -> None:
        """
        Called by position_tracker when a funding payment is exchanged.
        Updates cumulative_funding_paid on all active positions.

        We are SHORT the perp:
          - Positive rate: we RECEIVE funding (income → reduces cumulative cost).
          - Negative rate: we PAY funding (cost → increases cumulative cost).

        cumulative_funding_paid tracks the NET cost (positive = net paid, negative = net
        received). A large positive value signals that the trade has turned costly.
        """
        from engine.db import repository

        symbol = event.get("symbol")
        rate = event.get("fundingRate", 0.0)

        if symbol != PERP_WS_SYMBOL:
            return

        positions = await repository.get_open_positions(strategy=self.name)
        for pos in positions:
            # Shorts receive when rate > 0 (income, negative cost contribution).
            # Shorts pay when rate < 0 (cost, positive cost contribution).
            funding_this_period = -rate
            new_cumulative = (pos.cumulative_funding_paid or 0.0) + funding_this_period
            await repository.update_position(pos.id, cumulative_funding_paid=new_cumulative)
            logger.info(
                "funding_payment_recorded",
                strategy=self.name,
                position_id=pos.id,
                rate=rate,
                funding_this_period=funding_this_period,
                cumulative=new_cumulative,
            )
