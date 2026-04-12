"""
Delta Balance Check Strategy — one-shot manual integration test.

Triggered by a "delta_check" control signal from the dashboard (same
mechanism as the smoke test). Enters a minimal short XBTUSD perp + long
BTCUSDT linear perpetual position, lets the position tracker read the live
delta from both legs, logs whether exposure is balanced, then exits.

Purpose: verify that get_net_delta_usd() correctly converts the linear perp
leg from BTC to USD (using markPrice from the instrument stream) and that the
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

import structlog

from engine.db.models import Position, PositionState
from engine.strategies.two_leg import EntrySpec, LegSpec, TwoLegStrategy

logger = structlog.get_logger(__name__)

PERP_SYMBOL = "BTC/USD:BTC"  # ccxt symbol for order placement
PERP_WS_SYMBOL = "XBTUSD"  # BitMEX native symbol in _live_positions
LINEAR_PERP_SYMBOL = "BTC/USDT:USDT"  # BTCUSDT linear perpetual
# underlyingToPositionMultiplier from BitMEX contract spec:
# 1 contract = 0.000001 XBT (micro-XBT), so contracts = XBT_amount × 1_000_000
LINEAR_UNDERLYING_TO_POSITION_MULT = 1_000_000
PERP_LOT_SIZE = 100    # XBTUSD minimum order increment in USD contracts
LINEAR_LOT_SIZE = 100  # XBTUSDT minimum order increment in micro-XBT contracts
MIN_NOTIONAL_USD = 100.0
DELTA_BALANCE_THRESHOLD = 0.02  # 2% imbalance triggers a warning in the log


class DeltaCheckStrategy(TwoLegStrategy):
    name = "delta_check"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._done = False  # set True after first successful exit
        self._seen_active = False  # True once we've observed ACTIVE for one tick
        self.observed_delta_usd: float | None = None  # recorded while ACTIVE
        self.observed_nav_usd: float | None = None
        self.delta_balanced: bool | None = None  # True if |delta/nav| < threshold

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
            assert self._tracker is not None
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

    async def compute_entry_spec(self) -> EntrySpec | None:
        perp_ticker = await self._exchange.get_ticker(PERP_SYMBOL)
        linear_ticker = await self._exchange.get_ticker(LINEAR_PERP_SYMBOL)
        balance = await self._exchange.get_balance()

        # Use the same 40% formula as production strategies (floor at MIN_NOTIONAL_USD)
        usd_notional = max(
            MIN_NOTIONAL_USD,
            balance.available * perp_ticker.mark_price * 0.40,
        )

        if balance.available * perp_ticker.mark_price < MIN_NOTIONAL_USD:
            logger.warning(
                "delta_check_insufficient_balance",
                available_btc=balance.available,
                mark_price=perp_ticker.mark_price,
            )
            return None

        # XBTUSD qty is in USD contracts (1 contract = $1 notional); must be a
        # multiple of PERP_LOT_SIZE (100) or the exchange rounds down on fill,
        # leaving a sub-lot remainder that can never be filled.
        perp_qty = max(
            float(PERP_LOT_SIZE),
            round(usd_notional / PERP_LOT_SIZE) * PERP_LOT_SIZE,
        )
        # XBTUSDT qty is in micro-XBT contracts (1 contract = 0.000001 XBT).
        # Convert: contracts = (usd_notional / ask) × underlyingToPositionMultiplier
        # then round to the nearest lot (100 contracts).
        linear_qty_raw = (usd_notional / linear_ticker.ask) * LINEAR_UNDERLYING_TO_POSITION_MULT
        linear_qty = max(
            float(LINEAR_LOT_SIZE),
            round(linear_qty_raw / LINEAR_LOT_SIZE) * LINEAR_LOT_SIZE,
        )

        logger.info(
            "delta_check_entry_spec",
            notional_usd=usd_notional,
            perp_qty=perp_qty,
            linear_qty=linear_qty,
        )

        return EntrySpec(
            leg_a=LegSpec(symbol=PERP_SYMBOL,        side="sell", qty=perp_qty),
            leg_b=LegSpec(symbol=LINEAR_PERP_SYMBOL, side="buy",  qty=linear_qty),
            metadata={},
        )
