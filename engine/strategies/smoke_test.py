"""
Smoke Test Strategy — one-shot integration test for the execution pipeline.

Triggered by a "smoke_test" control signal written to the DB (e.g. from the
dashboard button). The engine instantiates this strategy on demand and runs it
until completion; it is NOT in the regular strategy list.

Behaviour:
  - Enter: unconditional. Shorts the nearest BTC quarterly future, longs the
    XBTUSD perp. Notional = 40% of available balance (same formula as
    production strategies, no extra cap). Entry uses the shared orderbook-
    aware progressive entry: one market-order slice per loop tick, sized by
    orderbook depth within max_slippage_pct.
  - Exit: after observing the position in ACTIVE state for one full loop tick
    (Tick N+2 relative to first fill). Uses market orders on both legs.
  - After exit: sets _done=True so run_once() does not re-enter.

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

PERP_SYMBOL = "BTC/USD:BTC"
PERP_WS_SYMBOL = "XBTUSD"


class SmokeTestStrategy(TwoLegStrategy):
    name = "smoke_test"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._done = False  # set True after first successful exit
        self._seen_active = False  # True once we've observed ACTIVE for one tick

    async def should_enter(self) -> bool:
        return not self._done

    async def should_exit(self, position: Position) -> bool:
        if position.state != PositionState.ACTIVE:
            return False
        if not self._seen_active:
            # First tick we see ACTIVE — observe, don't exit yet
            self._seen_active = True
            logger.info(
                "smoke_test_active_observed",
                position_id=position.id,
                note="will exit on next tick",
            )
            return False
        return True

    async def exit(self, position: Position) -> bool:
        result = await super().exit(position)
        if result:
            self._done = True
            logger.info("smoke_test_complete", position_id=position.id)
        return result

    async def compute_entry_spec(self) -> EntrySpec | None:
        # Find nearest BTC quarterly future
        futures = await self._exchange.get_active_futures()
        btc_futures = [f for f in futures if "BTC" in f.get("base", "") and f.get("expiry")]
        if not btc_futures:
            logger.error("smoke_test_no_btc_futures_found")
            return None
        btc_futures.sort(key=lambda f: f["expiry"])
        future_symbol = btc_futures[0]["symbol"]

        usd_notional = self._config.get("target_notional_usd", 1000.0)

        # Safety guard: skip if the account can't support the configured notional.
        balance = await self._exchange.get_balance()
        assert self._tracker is not None
        mark_price = self._tracker.market_data.get_mark_price(PERP_WS_SYMBOL)
        if mark_price is None:
            ticker = await self._exchange.get_ticker(PERP_SYMBOL)
            mark_price = ticker.mark_price
        available_usd = balance.available * mark_price
        if available_usd < usd_notional:
            logger.warning(
                "smoke_test_insufficient_balance",
                available_usd=round(available_usd, 2),
                target_notional_usd=usd_notional,
            )
            return None

        future_qty = await self._qty_for_usd_notional(future_symbol, usd_notional, mark_price)
        perp_qty = await self._qty_for_usd_notional(PERP_SYMBOL, usd_notional, mark_price)

        logger.info(
            "smoke_test_entry_spec",
            future=future_symbol,
            target_notional_usd=usd_notional,
            future_qty=future_qty,
            perp_qty=perp_qty,
        )

        return EntrySpec(
            leg_a=LegSpec(symbol=future_symbol, side="sell", qty=future_qty),
            leg_b=LegSpec(symbol=PERP_SYMBOL, side="buy", qty=perp_qty),
            metadata={},
        )
