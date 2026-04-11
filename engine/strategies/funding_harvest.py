"""
Strategy 2: Funding Rate Harvest

Short the XBTUSD perpetual, buy equivalent spot (XBT_USDT).
Collect funding every 8h (04:00, 12:00, 20:00 UTC) while staying delta-neutral.

Entry signal: funding_rate > threshold (default: 3x baseline = 0.03%/8h)
Exit signals:
  - Funding rate falls below baseline (0.01%/8h)
  - Funding flips negative (you'd be paying as a short)
  - Risk guard: HARD_STOP (margin), REBALANCE (delta drift)
  - Manual / operator trigger

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

PERP_SYMBOL = "BTC/USD:BTC"    # XBTUSD on ccxt
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
        rate = self._tracker.get_latest_funding_rate(PERP_SYMBOL)
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

    async def should_exit(self, position: Position) -> bool:
        rate = self._tracker.get_latest_funding_rate(PERP_SYMBOL)

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
            leg_a=LegSpec(
                symbol=PERP_SYMBOL,
                side="sell",                                # short the perp
                qty=perp_qty,
                price=perp_ticker.bid * 0.9999,            # just below bid (post-only)
            ),
            leg_b=LegSpec(
                symbol=SPOT_SYMBOL,
                side="buy",                                 # buy spot
                qty=spot_qty,
                price=spot_ticker.ask * 1.0001,            # just above ask (post-only)
            ),
            n_slices=self.n_slices,
            fill_timeout_s=self.fill_timeout_s,
            metadata={},
        )

    async def on_funding_payment(self, event: dict) -> None:
        """
        Called by position_tracker when a funding payment is exchanged.
        Updates cumulative_funding_paid on all active positions.
        For this strategy: we're SHORT the perp, so positive funding = we RECEIVE.
        """
        from engine.db import repository

        symbol = event.get("symbol")
        rate = event.get("fundingRate", 0.0)

        if symbol != PERP_SYMBOL:
            return

        positions = await repository.get_open_positions(strategy=self.name)
        for pos in positions:
            # Positive rate: shorts receive → we receive, not pay
            # We only track funding PAID for the circuit breaker (applies to S1)
            funding_paid = -rate * (pos.leg_a_qty or 0) if rate > 0 else abs(rate) * (pos.leg_a_qty or 0)
            new_cumulative = (pos.cumulative_funding_paid or 0.0) + max(0, funding_paid)
            await repository.update_position(pos.id, cumulative_funding_paid=new_cumulative)
            logger.info(
                "funding_payment_recorded",
                strategy=self.name,
                position_id=pos.id,
                rate=rate,
                funding_paid=funding_paid,
                cumulative=new_cumulative,
            )
