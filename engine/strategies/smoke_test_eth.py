"""
ETH Smoke Test Strategy — one-shot integration test using ETH instruments.

Same pattern as the BTC smoke test but with:
  - Leg A: short ETH/USD:BTC (ETHUSD quanto perpetual, settled in BTC)
  - Leg B: long  ETH/USDT:USDT (ETHUSDT linear perpetual, settled in USDT)

Triggered by a "smoke_test_eth" control signal. Enters, observes one active
tick, then exits.

Tick sequence (assuming enough depth each tick):
  Tick N   — enter() → position: ENTERING, first slice placed
  Tick N+1 — continue_entry() slices until ACTIVE (or stays ENTERING)
  ...
  Tick N+k — position ACTIVE, _seen_active=False → skip exit, set flag
  Tick N+k+1 — position ACTIVE, _seen_active=True → exit()
"""

from __future__ import annotations

import structlog

from engine.db.models import Position, PositionState
from engine.strategies.two_leg import EntrySpec, LegSpec, TwoLegStrategy

logger = structlog.get_logger(__name__)

QUANTO_SYMBOL = "ETH/USD:BTC"        # quanto perp (settled in BTC)
QUANTO_WS_SYMBOL = "ETHUSD"          # BitMEX native symbol for WS data
LINEAR_SYMBOL = "ETH/USDT:USDT"      # linear perp (settled in USDT)
BTC_PERP_SYMBOL = "BTC/USD:BTC"      # used to convert BTC balance → USD
BTC_PERP_WS_SYMBOL = "XBTUSD"


class SmokeTestEthStrategy(TwoLegStrategy):
    name = "smoke_test_eth"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._done = False
        self._seen_active = False

    async def should_enter(self) -> bool:
        return not self._done

    async def should_exit(self, position: Position) -> bool:
        if position.state != PositionState.ACTIVE:
            return False
        if not self._seen_active:
            self._seen_active = True
            logger.info(
                "smoke_test_eth_active_observed",
                position_id=position.id,
                note="will exit on next tick",
            )
            return False
        return True

    async def exit(self, position: Position) -> bool:
        result = await super().exit(position)
        if result:
            self._done = True
            logger.info("smoke_test_eth_complete", position_id=position.id)
        return result

    async def compute_entry_spec(self) -> EntrySpec | None:
        usd_notional = self._config.get("target_notional_usd", 1000.0)

        # Balance is in BTC — convert to USD using BTC/USD mark price.
        balance = await self._exchange.get_balance()
        assert self._tracker is not None
        btc_price = self._tracker.market_data.get_mark_price(BTC_PERP_WS_SYMBOL)
        if btc_price is None:
            ticker = await self._exchange.get_ticker(BTC_PERP_SYMBOL)
            btc_price = ticker.mark_price

        available_usd = balance.available * btc_price
        if available_usd < usd_notional:
            logger.warning(
                "smoke_test_eth_insufficient_balance",
                available_usd=round(available_usd, 2),
                target_notional_usd=usd_notional,
            )
            return None

        # Let _qty_for_usd_notional resolve each instrument's mark price
        # and (for the quanto leg) the BTC settlement rate.
        quanto_qty = await self._qty_for_usd_notional(QUANTO_SYMBOL, usd_notional)
        linear_qty = await self._qty_for_usd_notional(LINEAR_SYMBOL, usd_notional)

        logger.info(
            "smoke_test_eth_entry_spec",
            target_notional_usd=usd_notional,
            quanto_qty=quanto_qty,
            linear_qty=linear_qty,
        )

        return EntrySpec(
            leg_a=LegSpec(symbol=QUANTO_SYMBOL, side="sell", qty=quanto_qty),
            leg_b=LegSpec(symbol=LINEAR_SYMBOL, side="buy", qty=linear_qty),
            metadata={},
        )
