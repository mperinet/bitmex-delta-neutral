"""
Strategy 1: Cash-and-Carry (Futures Basis Trade)

Short the nearest quarterly future, long the XBTUSD perpetual.
At settlement, the future converges to spot. Pocket the locked-in basis.

Entry signal: annualised_basis > min_basis_annualised (default: 10% APR)
Exit signals:
  - Within 24h of future expiry (close before settlement window)
  - Funding circuit breaker: cumulative funding paid > 50% of locked basis
  - Risk guard: HARD_STOP or REBALANCE
  - Manual / operator trigger

Key considerations:
  - Settlement fee: 0.05% on the future leg at expiry
  - The perp long pays funding (positive funding ~92% of the time)
  - Locked basis is only known at entry — the circuit breaker uses this value
  - Expiry is tracked in the Position.expiry field
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import structlog

from engine.db.models import Position
from engine.exchange.bitmex import BitMEXExchange
from engine.risk_guard import RiskAction
from engine.strategies.two_leg import EntrySpec, LegSpec, TwoLegStrategy

logger = structlog.get_logger(__name__)

PERP_SYMBOL = "BTC/USD:BTC"    # ccxt symbol — used for order placement and REST calls
PERP_WS_SYMBOL = "XBTUSD"     # BitMEX native symbol — used for WS instrument/funding lookups
SPOT_INDEX = ".BXBT"           # spot index for basis calculation


class CashAndCarryStrategy(TwoLegStrategy):
    name = "cash_and_carry"

    @property
    def _min_basis(self) -> float:
        return self._config.get("min_basis_annualised", 0.10)

    @property
    def _circuit_breaker_ratio(self) -> float:
        return self._config.get("funding_circuit_breaker", 0.50)

    @property
    def _max_position_usd(self) -> float:
        return self._config.get("max_position_usd", 10000.0)

    @property
    def _hours_before_expiry_exit(self) -> int:
        return 24

    async def _get_nearest_future(self) -> Optional[tuple[str, float, datetime]]:
        """
        Find the nearest active quarterly future for BTC.
        Returns (ccxt_symbol, mark_price, expiry_datetime) or None.
        """
        futures = await self._exchange.get_active_futures()
        btc_futures = [
            f for f in futures
            if "BTC" in f.get("base", "") and f.get("expiry")
        ]
        if not btc_futures:
            return None
        # Sort by expiry ascending, pick nearest
        btc_futures.sort(key=lambda f: f["expiry"])
        nearest = btc_futures[0]
        expiry_dt = datetime.fromtimestamp(nearest["expiry"] / 1000, tz=timezone.utc)
        ticker = await self._exchange.get_ticker(nearest["symbol"])
        return nearest["symbol"], ticker.mark_price, expiry_dt

    async def should_enter(self) -> bool:
        result = await self._get_nearest_future()
        if result is None:
            return False
        future_symbol, future_price, expiry = result

        # Don't enter if expiry is too close
        days_to_expiry = (expiry - datetime.now(tz=timezone.utc)).days
        if days_to_expiry < 7:
            logger.debug("cash_and_carry_expiry_too_close", days=days_to_expiry)
            return False

        # Get spot price via perp mark price (proxy)
        perp_ticker = await self._exchange.get_ticker(PERP_SYMBOL)
        spot_price = perp_ticker.mark_price

        basis = BitMEXExchange.compute_annualised_basis(future_price, spot_price, days_to_expiry)
        signal = basis >= self._min_basis
        logger.debug(
            "cash_and_carry_entry_check",
            future=future_symbol,
            basis=basis,
            threshold=self._min_basis,
            days_to_expiry=days_to_expiry,
            signal=signal,
        )
        return signal

    async def should_exit(self, position: Position) -> bool:
        # Expiry check: exit 24h before settlement
        if position.expiry:
            hours_remaining = (
                position.expiry.replace(tzinfo=timezone.utc) - datetime.now(tz=timezone.utc)
            ).total_seconds() / 3600
            if hours_remaining < self._hours_before_expiry_exit:
                logger.info(
                    "cash_and_carry_exit_near_expiry",
                    hours_remaining=hours_remaining,
                    position_id=position.id,
                )
                return True

        # Funding circuit breaker
        cb = self._risk.check_funding_circuit_breaker(
            cumulative_funding_paid=position.cumulative_funding_paid or 0.0,
            locked_basis=position.locked_basis or 0.0,
        )
        if cb.action == RiskAction.EXIT_STRATEGY:
            logger.info(
                "cash_and_carry_circuit_breaker_fired",
                reason=cb.reason,
                position_id=position.id,
            )
            return True

        return False

    async def compute_entry_spec(self) -> Optional[EntrySpec]:
        result = await self._get_nearest_future()
        if result is None:
            return None
        future_symbol, future_price, expiry = result

        perp_ticker = await self._exchange.get_ticker(PERP_SYMBOL)
        spot_price = perp_ticker.mark_price
        days_to_expiry = (expiry - datetime.now(tz=timezone.utc)).days
        locked_basis = BitMEXExchange.compute_annualised_basis(
            future_price, spot_price, days_to_expiry
        )

        balance = await self._exchange.get_balance()
        usd_notional = min(
            self._max_position_usd,
            balance.available * spot_price * 0.40,
        )

        if usd_notional < 100:
            logger.warning("cash_and_carry_insufficient_balance")
            return None

        future_qty = usd_notional     # USD contracts (inverse) on the future
        perp_qty = usd_notional       # USD contracts on the perp

        return EntrySpec(
            leg_a=LegSpec(
                symbol=future_symbol,
                side="sell",                              # short the future
                qty=future_qty,
                price=future_price * 0.9999,
            ),
            leg_b=LegSpec(
                symbol=PERP_SYMBOL,
                side="buy",                               # long the perp
                qty=perp_qty,
                price=perp_ticker.ask * 1.0001,
            ),
            n_slices=self.n_slices,
            fill_timeout_s=self.fill_timeout_s,
            metadata={
                "locked_basis": locked_basis,
                "expiry": expiry,
            },
        )

    async def on_funding_payment(self, event: dict) -> None:
        """
        The perp LONG pays funding when rates are positive.
        Track cumulative funding paid for the circuit breaker.
        """
        from engine.db import repository

        symbol = event.get("symbol")
        rate = event.get("fundingRate", 0.0)

        if symbol != PERP_WS_SYMBOL:
            return

        positions = await repository.get_open_positions(strategy=self.name)
        for pos in positions:
            # Positive rate: longs PAY — this is a cost to track
            funding_paid_this_period = max(0.0, rate)
            new_cumulative = (pos.cumulative_funding_paid or 0.0) + funding_paid_this_period
            await repository.update_position(pos.id, cumulative_funding_paid=new_cumulative)
            logger.info(
                "funding_paid_recorded",
                strategy=self.name,
                position_id=pos.id,
                rate=rate,
                funding_paid=funding_paid_this_period,
                cumulative=new_cumulative,
                locked_basis=pos.locked_basis,
                circuit_breaker_ratio=new_cumulative / (pos.locked_basis or 1),
            )
