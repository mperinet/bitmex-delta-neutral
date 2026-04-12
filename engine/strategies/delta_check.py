"""
Delta Balance Check Strategy — one-shot manual integration test.

Triggered by a "delta_check" control signal from the dashboard (same
mechanism as the smoke test). Enters a minimal short XBTUSD perp + long
XBT_USDT spot position, lets the position tracker read the live delta
from both legs, logs whether exposure is balanced, then exits.

Purpose: verify that get_net_delta_usd() correctly converts the spot leg
from BTC to USD (using lastPrice from the instrument stream) and that the
delta guard actually sees a near-zero net delta for a hedged book.

Tick sequence:
  Tick N   — enter() → ENTERING, first slice placed
  Tick N+1 — continue_entry() slices until ACTIVE (or stays ENTERING)
  ...
  Tick N+k — position ACTIVE, _seen_active=False → read delta, log result, set flag
  Tick N+k+1 — position ACTIVE, _seen_active=True → exit()

The observed delta is stored on the instance and surfaced in the
structured log as delta_check_result for the dashboard to display.
"""

from __future__ import annotations

from typing import Optional

import structlog

from engine.db.models import Position, PositionState
from engine.strategies.two_leg import EntrySpec, LegSpec, TwoLegStrategy

logger = structlog.get_logger(__name__)

PERP_SYMBOL = "BTC/USD:BTC"       # ccxt symbol for order placement
PERP_WS_SYMBOL = "XBTUSD"        # BitMEX native symbol in _live_positions
SPOT_SYMBOL = "BTC/USDT"          # XBT_USDT spot
MIN_NOTIONAL_USD = 100.0
DELTA_BALANCE_THRESHOLD = 0.02    # 2% imbalance triggers a warning in the log


class DeltaCheckStrategy(TwoLegStrategy):
    name = "delta_check"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._done = False           # set True after first successful exit
        self._seen_active = False    # True once we've observed ACTIVE for one tick
        self.observed_delta_usd: Optional[float] = None   # recorded while ACTIVE
        self.observed_nav_usd: Optional[float] = None
        self.delta_balanced: Optional[bool] = None        # True if |delta/nav| < threshold

    async def should_enter(self) -> bool:
        return not self._done

    async def should_exit(self, position: Position) -> bool:
        if position.state != PositionState.ACTIVE:
            return False

        if not self._seen_active:
            # First tick in ACTIVE — read and record the net delta
            await self._record_delta(position)
            self._seen_active = True
            return False

        return True

    async def exit(self, position: Position) -> bool:
        result = await super().exit(position)
        if result:
            self._done = True
            logger.info(
                "delta_check_complete",
                position_id=position.id,
                observed_delta_usd=self.observed_delta_usd,
                observed_nav_usd=self.observed_nav_usd,
                delta_balanced=self.delta_balanced,
            )
        return result

    async def _record_delta(self, position: Position) -> None:
        """Read net delta and NAV from the tracker and log the balance verdict."""
        try:
            ticker = await self._exchange.get_ticker(PERP_SYMBOL)
            btc_price = ticker.mark_price
            nav = self._tracker.get_nav_usd(btc_price)
            delta = self._tracker.get_net_delta_usd()

            self.observed_delta_usd = delta
            self.observed_nav_usd = nav

            if nav > 0:
                delta_pct = abs(delta) / nav
                self.delta_balanced = delta_pct < DELTA_BALANCE_THRESHOLD
                logger.info(
                    "delta_check_observation",
                    position_id=position.id,
                    net_delta_usd=round(delta, 2),
                    nav_usd=round(nav, 2),
                    delta_pct_nav=round(delta_pct * 100, 3),
                    balanced=self.delta_balanced,
                    threshold_pct=DELTA_BALANCE_THRESHOLD * 100,
                )
            else:
                logger.warning("delta_check_nav_zero", position_id=position.id)
        except Exception as e:
            logger.error("delta_check_observation_failed", error=str(e))

    async def compute_entry_spec(self) -> Optional[EntrySpec]:
        perp_ticker = await self._exchange.get_ticker(PERP_SYMBOL)
        spot_ticker = await self._exchange.get_ticker(SPOT_SYMBOL)
        balance = await self._exchange.get_balance()

        # Use the same 40% formula as production strategies but cap at MIN_NOTIONAL
        usd_notional = max(
            MIN_NOTIONAL_USD,
            min(balance.available * perp_ticker.mark_price * 0.40, 1000.0),
        )

        if balance.available * perp_ticker.mark_price < MIN_NOTIONAL_USD:
            logger.warning(
                "delta_check_insufficient_balance",
                available_btc=balance.available,
                mark_price=perp_ticker.mark_price,
            )
            return None

        perp_qty = usd_notional                           # USD contracts (inverse)
        spot_qty = usd_notional / spot_ticker.ask         # BTC to buy

        logger.info(
            "delta_check_entry_spec",
            notional_usd=usd_notional,
            perp_qty=perp_qty,
            spot_qty=round(spot_qty, 6),
        )

        return EntrySpec(
            leg_a=LegSpec(symbol=PERP_SYMBOL, side="sell", qty=perp_qty),
            leg_b=LegSpec(symbol=SPOT_SYMBOL, side="buy", qty=spot_qty),
            metadata={},
        )
