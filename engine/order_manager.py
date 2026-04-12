"""
Order manager with rate-limit token bucket and orderbook-aware progressive entry.

Rate limit: 300 requests per 5-minute window (BitMEX).
  - Token bucket initialized from X-RateLimit-Remaining on startup
    (prevents 429s after a crash-loop restart).
  - 20-token emergency reserve strategies cannot drain.
  - Normal strategy orders draw from the shared bucket.
  - Emergency operations (cancel, dead-man's switch) draw from reserve.

Progressive entry (two-leg, market orders):
  Each call to fill_next_slice() places ONE slice on both legs simultaneously.
  Slice size is determined by orderbook depth within max_slippage_pct:

    1. Fetch orderbook for both legs
    2. Compute available qty within slippage on each leg's consuming side
    3. If either leg has insufficient depth → skip this tick (return False)
    4. Size the slice to the most constrained leg, keeping both proportional
    5. Place market orders on leg A then leg B
    6. If leg B fails: immediately unwind leg A (market order, emergency)
    7. Record fills and update position DB state
    8. If target qty reached → transition position to ACTIVE

  fill_next_slice() is called once per loop tick per ENTERING position,
  by TwoLegStrategy.continue_entry() which is called from Strategy.run_once().

Orphan risk: at most one slice (one market order on leg A) if leg B API call fails.
Market orders fill in milliseconds, so the orphan window is ~100ms.
"""

from __future__ import annotations

import asyncio

import structlog

from engine.db import repository
from engine.db.models import PositionState
from engine.exchange.base import ExchangeBase, OrderBook, OrderResult

logger = structlog.get_logger(__name__)

_REFILL_INTERVAL_S = 300  # 5 minutes
_BUCKET_CAPACITY = 300
_EMERGENCY_RESERVE = 20  # strategies cannot drain below this


class RateLimitBucket:
    """
    Asyncio token bucket for BitMEX rate limiting.

    Initialized from the exchange's X-RateLimit-Remaining header on startup
    so crash-loop restarts don't start with a stale full bucket.
    """

    def __init__(self, initial_tokens: int = _BUCKET_CAPACITY):
        self._tokens = min(initial_tokens, _BUCKET_CAPACITY)
        self._lock = asyncio.Lock()
        self._refill_task: asyncio.Task | None = None

    def start(self) -> None:
        self._refill_task = asyncio.create_task(self._refill_loop())

    def stop(self) -> None:
        if self._refill_task:
            self._refill_task.cancel()

    async def acquire(self, emergency: bool = False) -> None:
        """
        Consume one token.
        emergency=True: may draw from the reserve (for cancel/dead-man's switch).
        emergency=False: blocked if tokens <= EMERGENCY_RESERVE.
        """
        while True:
            async with self._lock:
                threshold = 0 if emergency else _EMERGENCY_RESERVE
                if self._tokens > threshold:
                    self._tokens -= 1
                    return
            await asyncio.sleep(1.0)

    async def _refill_loop(self) -> None:
        while True:
            await asyncio.sleep(_REFILL_INTERVAL_S)
            async with self._lock:
                self._tokens = _BUCKET_CAPACITY
                logger.debug("rate_limit_bucket_refilled", tokens=_BUCKET_CAPACITY)

    @property
    def tokens_remaining(self) -> int:
        return self._tokens


class OrderManager:
    def __init__(
        self,
        exchange: ExchangeBase,
        bucket: RateLimitBucket,
        max_slippage: float = 0.001,
    ):
        self._exchange = exchange
        self._bucket = bucket
        self._max_slippage = max_slippage
        # Minimum order sizes per symbol — populated lazily from ccxt market info.
        # Prevents placing orders below exchange minimums (e.g. 100 contracts for XBTUSD).
        self._min_order_size: dict[str, float] = {}

    def _get_min_order_size(self, symbol: str) -> float:
        """Return the minimum order size for a symbol, queried from ccxt market data.
        Returns 0.0 if the information is unavailable (skips the check)."""
        if symbol in self._min_order_size:
            return self._min_order_size[symbol]
        try:
            ccxt = getattr(self._exchange, "_ccxt", None)
            if ccxt is None:
                return 0.0
            markets = getattr(ccxt, "markets", None)
            if not isinstance(markets, dict):
                return 0.0
            market = markets.get(symbol, {})
            min_amt = (market.get("limits") or {}).get("amount", {}).get("min") or 0
            result = float(min_amt)
            self._min_order_size[symbol] = result
            return result
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Single order placement
    # ------------------------------------------------------------------

    async def place_limit(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        post_only: bool = True,
        emergency: bool = False,
    ) -> OrderResult:
        await self._bucket.acquire(emergency=emergency)
        result = await self._exchange.place_limit_order(symbol, side, qty, price, post_only)
        logger.info(
            "order_placed",
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_id=result.order_id,
        )
        return result

    async def place_market(
        self, symbol: str, side: str, qty: float, emergency: bool = False
    ) -> OrderResult:
        await self._bucket.acquire(emergency=emergency)
        result = await self._exchange.place_market_order(symbol, side, qty)
        logger.info(
            "market_order_placed",
            symbol=symbol,
            side=side,
            qty=qty,
            order_id=result.order_id,
            filled=result.filled_qty,
            avg_price=result.avg_price,
        )
        return result

    async def cancel(self, order_id: str, symbol: str, emergency: bool = True) -> bool:
        await self._bucket.acquire(emergency=emergency)
        ok = await self._exchange.cancel_order(order_id, symbol)
        logger.info("order_cancelled", order_id=order_id, symbol=symbol, ok=ok)
        return ok

    async def cancel_all(self, symbol: str | None = None) -> int:
        await self._bucket.acquire(emergency=True)
        count = await self._exchange.cancel_all_orders(symbol)
        logger.info("all_orders_cancelled", symbol=symbol, count=count)
        return count

    # ------------------------------------------------------------------
    # Orderbook-aware progressive entry
    # ------------------------------------------------------------------

    async def fill_next_slice(
        self,
        position_id: int,
        strategy: str,
        leg_a_symbol: str,
        leg_a_side: str,
        leg_a_remaining: float,
        leg_b_symbol: str,
        leg_b_side: str,
        leg_b_remaining: float,
    ) -> bool:
        """
        Attempt one market-order slice on both legs, sized by orderbook depth.

        Returns True  — slice placed (or position already fully filled).
        Returns False — insufficient depth on at least one leg; try next tick.

        On leg B failure, leg A is immediately unwound (market order, emergency).
        """
        # Already fully filled — just ensure state is ACTIVE
        if leg_a_remaining <= 0 and leg_b_remaining <= 0:
            await repository.update_position(position_id, state=PositionState.ACTIVE)
            return True

        # Fetch orderbooks for both legs (market data, not counted against trade bucket)
        ob_a = await self._exchange.fetch_orderbook(leg_a_symbol)
        ob_b = await self._exchange.fetch_orderbook(leg_b_symbol)

        mid_a = self._mid(ob_a)
        mid_b = self._mid(ob_b)

        depth_a = self._available_qty(ob_a, leg_a_side, mid_a, self._max_slippage)
        depth_b = self._available_qty(ob_b, leg_b_side, mid_b, self._max_slippage)

        if depth_a <= 0 or depth_b <= 0:
            logger.info(
                "fill_next_slice_insufficient_depth",
                position_id=position_id,
                leg_a=leg_a_symbol,
                depth_a=depth_a,
                leg_b=leg_b_symbol,
                depth_b=depth_b,
                max_slippage=self._max_slippage,
            )
            return False

        # Size each leg: capped by available depth, then aligned to the more
        # constrained leg so both legs always move by the same fraction of remaining.
        fill_a = min(leg_a_remaining, depth_a)
        fill_b = min(leg_b_remaining, depth_b)
        ratio = min(fill_a / leg_a_remaining, fill_b / leg_b_remaining)
        fill_a = leg_a_remaining * ratio
        fill_b = leg_b_remaining * ratio

        # Reject the slice if either leg would be below the exchange's minimum order size.
        # This prevents a "minimum amount precision" rejection when one leg has thin depth.
        min_a = self._get_min_order_size(leg_a_symbol)
        min_b = self._get_min_order_size(leg_b_symbol)
        if (min_a and fill_a < min_a) or (min_b and fill_b < min_b):
            logger.info(
                "fill_next_slice_below_minimum",
                position_id=position_id,
                leg_a=f"{fill_a:.4f} {leg_a_symbol} (min {min_a})",
                leg_b=f"{fill_b:.6f} {leg_b_symbol} (min {min_b})",
                ratio=ratio,
                reason="slice_below_exchange_minimum",
            )
            return False

        logger.info(
            "fill_next_slice_placing",
            position_id=position_id,
            leg_a=f"{leg_a_side} {fill_a:.2f} {leg_a_symbol}",
            leg_b=f"{leg_b_side} {fill_b:.4f} {leg_b_symbol}",
            depth_a=depth_a,
            depth_b=depth_b,
            ratio=ratio,
        )

        # Place leg A
        order_a = await self.place_market(leg_a_symbol, leg_a_side, fill_a)

        # Place leg B — unwind A immediately on failure
        try:
            order_b = await self.place_market(leg_b_symbol, leg_b_side, fill_b)
        except Exception as exc:
            logger.error(
                "fill_next_slice_leg_b_failed_unwinding_a",
                position_id=position_id,
                error=str(exc),
            )
            unwind_side = "sell" if leg_a_side == "buy" else "buy"
            try:
                await self.place_market(
                    leg_a_symbol, unwind_side, order_a.filled_qty, emergency=True
                )
            except Exception as unwind_exc:
                logger.critical(
                    "exchange_orphan_created",
                    position_id=position_id,
                    symbol=leg_a_symbol,
                    side=leg_a_side,
                    qty=order_a.filled_qty,
                    unwind_error=str(unwind_exc),
                    note="leg A filled but unwind failed — manual close required on exchange",
                )
            raise

        # Record fills
        await repository.record_trade(
            position_id,
            strategy,
            "a",
            order_a.order_id,
            leg_a_symbol,
            leg_a_side,
            order_a.filled_qty,
            order_a.avg_price,
            order_a.fee,
            is_entry=True,
        )
        await repository.record_trade(
            position_id,
            strategy,
            "b",
            order_b.order_id,
            leg_b_symbol,
            leg_b_side,
            order_b.filled_qty,
            order_b.avg_price,
            order_b.fee,
            is_entry=True,
        )

        # Update position quantities and state
        pos = await repository.get_position(position_id)
        if pos:
            new_qty_a = pos.leg_a_qty + order_a.filled_qty
            new_qty_b = pos.leg_b_qty + order_b.filled_qty
            new_avg_a = self._running_avg(
                pos.leg_a_avg_entry,
                pos.leg_a_qty,
                order_a.avg_price,
                order_a.filled_qty,
            )
            new_avg_b = self._running_avg(
                pos.leg_b_avg_entry,
                pos.leg_b_qty,
                order_b.avg_price,
                order_b.filled_qty,
            )
            slices_done = pos.entry_slices_done + 1

            new_remaining_a = (pos.leg_a_target_qty or 0.0) - new_qty_a
            new_remaining_b = (pos.leg_b_target_qty or 0.0) - new_qty_b
            fully_filled = new_remaining_a <= 0 and new_remaining_b <= 0

            await repository.update_position(
                position_id,
                leg_a_qty=new_qty_a,
                leg_b_qty=new_qty_b,
                leg_a_avg_entry=new_avg_a,
                leg_b_avg_entry=new_avg_b,
                entry_slices_done=slices_done,
                state=PositionState.ACTIVE if fully_filled else PositionState.ENTERING,
            )

            logger.info(
                "fill_next_slice_done",
                position_id=position_id,
                slices_done=slices_done,
                remaining_a=new_remaining_a,
                remaining_b=new_remaining_b,
                state="ACTIVE" if fully_filled else "ENTERING",
            )

        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _mid(ob: OrderBook) -> float:
        best_bid = ob.bids[0][0] if ob.bids else 0.0
        best_ask = ob.asks[0][0] if ob.asks else 0.0
        return (best_bid + best_ask) / 2

    @staticmethod
    def _available_qty(
        ob: OrderBook,
        side: str,
        mid_price: float,
        max_slippage: float,
    ) -> float:
        """
        Cumulative quantity available within slippage of mid price.

        Buy  → consuming asks: sum qty where ask_price <= mid * (1 + slippage)
        Sell → consuming bids: sum qty where bid_price >= mid * (1 - slippage)

        Qty units match the instrument (USD for inverse contracts, base ccy for spot).
        """
        if mid_price <= 0:
            return 0.0
        if side == "buy":
            limit = mid_price * (1 + max_slippage)
            return sum(qty for price, qty in ob.asks if price <= limit)
        else:
            limit = mid_price * (1 - max_slippage)
            return sum(qty for price, qty in ob.bids if price >= limit)

    @staticmethod
    def _running_avg(
        old_avg: float | None,
        old_qty: float,
        new_price: float,
        new_qty: float,
    ) -> float:
        if not old_avg or old_qty == 0:
            return new_price
        return (old_avg * old_qty + new_price * new_qty) / (old_qty + new_qty)
