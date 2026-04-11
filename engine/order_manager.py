"""
Order manager with rate-limit token bucket and progressive entry.

Rate limit: 300 requests per 5-minute window (BitMEX).
  - Token bucket initialized from X-RateLimit-Remaining on startup
    (prevents 429s after a crash-loop restart).
  - 20-token emergency reserve strategies cannot drain.
  - Normal strategy orders draw from the shared bucket.
  - Emergency operations (cancel, dead-man's switch) draw from reserve.

Progressive entry (two-leg):
  Entry is divided into N slices. For each slice:
    1. Place leg A slice
    2. Wait for fill (timeout = slice_fill_timeout_s)
    3. Place leg B slice
    4. Wait for fill
    5. If either times out: unwind the filled half, stop entry
    6. If both fill: update DB state, proceed to next slice

  Max orphan exposure = 1/N of total notional (1 slice worst case).

ASCII flow:

  for slice in range(N):
      place leg_a slice ──► fill? ──► NO ──► abort (no leg_b needed)
                │
                YES
                │
      place leg_b slice ──► fill? ──► NO ──► cancel/close leg_a slice, abort
                │
                YES
                │
      record_trade (both legs)
      update DB (entry_slices_done += 1)
      ├── all N done? → position.state = ACTIVE
      └── continue
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import structlog

from engine.db import repository
from engine.exchange.base import ExchangeBase, OrderResult

logger = structlog.get_logger(__name__)

_REFILL_INTERVAL_S = 300      # 5 minutes
_BUCKET_CAPACITY = 300
_EMERGENCY_RESERVE = 20       # strategies cannot drain below this


class RateLimitBucket:
    """
    Asyncio token bucket for BitMEX rate limiting.

    Initialized from the exchange's X-RateLimit-Remaining header on startup
    so crash-loop restarts don't start with a stale full bucket.
    """

    def __init__(self, initial_tokens: int = _BUCKET_CAPACITY):
        self._tokens = min(initial_tokens, _BUCKET_CAPACITY)
        self._lock = asyncio.Lock()
        self._refill_task: Optional[asyncio.Task] = None

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


@dataclass
class SliceFillTimeout(Exception):
    leg: str
    symbol: str
    order_id: str


class OrderManager:
    def __init__(self, exchange: ExchangeBase, bucket: RateLimitBucket):
        self._exchange = exchange
        self._bucket = bucket

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
        )
        return result

    async def cancel(self, order_id: str, symbol: str, emergency: bool = True) -> bool:
        await self._bucket.acquire(emergency=emergency)
        ok = await self._exchange.cancel_order(order_id, symbol)
        logger.info("order_cancelled", order_id=order_id, symbol=symbol, ok=ok)
        return ok

    async def cancel_all(self, symbol: Optional[str] = None) -> int:
        await self._bucket.acquire(emergency=True)
        count = await self._exchange.cancel_all_orders(symbol)
        logger.info("all_orders_cancelled", symbol=symbol, count=count)
        return count

    # ------------------------------------------------------------------
    # Progressive entry (two-leg, N slices)
    # ------------------------------------------------------------------

    async def progressive_entry(
        self,
        position_id: int,
        strategy: str,
        leg_a_symbol: str,
        leg_a_side: str,          # "buy" | "sell"
        leg_b_symbol: str,
        leg_b_side: str,
        total_qty_a: float,
        total_qty_b: float,
        leg_a_price: float,
        leg_b_price: float,
        n_slices: int,
        fill_timeout_s: float,
    ) -> bool:
        """
        Enter both legs simultaneously in N equal slices.

        Returns True if all slices filled successfully.
        Returns False if any slice failed (orphan unwound automatically).

        On failure, the position state is NOT updated to ACTIVE —
        caller should handle the partial state.
        """
        slice_qty_a = total_qty_a / n_slices
        slice_qty_b = total_qty_b / n_slices

        for slice_idx in range(n_slices):
            success = await self._enter_one_slice(
                position_id=position_id,
                strategy=strategy,
                slice_idx=slice_idx,
                leg_a_symbol=leg_a_symbol,
                leg_a_side=leg_a_side,
                leg_b_symbol=leg_b_symbol,
                leg_b_side=leg_b_side,
                qty_a=slice_qty_a,
                qty_b=slice_qty_b,
                price_a=leg_a_price,
                price_b=leg_b_price,
                fill_timeout_s=fill_timeout_s,
            )
            if not success:
                logger.warning(
                    "progressive_entry_aborted",
                    position_id=position_id,
                    strategy=strategy,
                    slices_completed=slice_idx,
                )
                return False

        return True

    async def _enter_one_slice(
        self,
        position_id: int,
        strategy: str,
        slice_idx: int,
        leg_a_symbol: str,
        leg_a_side: str,
        leg_b_symbol: str,
        leg_b_side: str,
        qty_a: float,
        qty_b: float,
        price_a: float,
        price_b: float,
        fill_timeout_s: float,
    ) -> bool:
        # Place leg A
        order_a = await self.place_limit(leg_a_symbol, leg_a_side, qty_a, price_a)
        filled_a = await self._wait_for_fill(order_a.order_id, leg_a_symbol, fill_timeout_s)
        if not filled_a:
            logger.warning(
                "slice_leg_a_timeout",
                slice=slice_idx,
                order_id=order_a.order_id,
                symbol=leg_a_symbol,
            )
            # No leg B placed yet — just cancel leg A (may already be dead)
            await self.cancel(order_a.order_id, leg_a_symbol)
            return False

        # Place leg B
        order_b = await self.place_limit(leg_b_symbol, leg_b_side, qty_b, price_b)
        filled_b = await self._wait_for_fill(order_b.order_id, leg_b_symbol, fill_timeout_s)
        if not filled_b:
            logger.warning(
                "slice_leg_b_timeout",
                slice=slice_idx,
                order_id=order_b.order_id,
                symbol=leg_b_symbol,
            )
            # Unwind leg A: place opposing market order
            unwind_side = "sell" if leg_a_side == "buy" else "buy"
            await self.place_market(leg_a_symbol, unwind_side, qty_a, emergency=True)
            await self.cancel(order_b.order_id, leg_b_symbol)
            logger.info(
                "slice_leg_a_unwound",
                slice=slice_idx,
                symbol=leg_a_symbol,
                qty=qty_a,
            )
            return False

        # Both legs filled — record trades and update DB
        final_a = await self._exchange.get_order(order_a.order_id, leg_a_symbol)
        final_b = await self._exchange.get_order(order_b.order_id, leg_b_symbol)

        await repository.record_trade(
            position_id, strategy, "a", final_a.order_id,
            leg_a_symbol, leg_a_side, final_a.filled_qty, final_a.avg_price,
            final_a.fee, is_entry=True,
        )
        await repository.record_trade(
            position_id, strategy, "b", final_b.order_id,
            leg_b_symbol, leg_b_side, final_b.filled_qty, final_b.avg_price,
            final_b.fee, is_entry=True,
        )

        pos = await repository.get_position(position_id)
        if pos:
            new_qty_a = (pos.leg_a_qty or 0) + final_a.filled_qty
            new_qty_b = (pos.leg_b_qty or 0) + final_b.filled_qty
            slices_done = (pos.entry_slices_done or 0) + 1
            new_avg_a = self._running_avg(
                pos.leg_a_avg_entry, pos.leg_a_qty or 0,
                final_a.avg_price, final_a.filled_qty
            )
            new_avg_b = self._running_avg(
                pos.leg_b_avg_entry, pos.leg_b_qty or 0,
                final_b.avg_price, final_b.filled_qty
            )
            from engine.db.models import PositionState
            await repository.update_position(
                position_id,
                leg_a_qty=new_qty_a,
                leg_b_qty=new_qty_b,
                leg_a_avg_entry=new_avg_a,
                leg_b_avg_entry=new_avg_b,
                entry_slices_done=slices_done,
                state=PositionState.ACTIVE if slices_done >= pos.entry_slices_total
                      else PositionState.ENTERING,
            )

        return True

    async def _wait_for_fill(
        self, order_id: str, symbol: str, timeout_s: float, poll_interval: float = 1.0
    ) -> bool:
        """Poll order status until filled or timeout."""
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(poll_interval)
            await self._bucket.acquire()
            order = await self._exchange.get_order(order_id, symbol)
            if order.status == "closed":
                return True
            if order.status == "canceled":
                return False
        return False

    @staticmethod
    def _running_avg(
        old_avg: Optional[float],
        old_qty: float,
        new_price: float,
        new_qty: float,
    ) -> float:
        if not old_avg or old_qty == 0:
            return new_price
        return (old_avg * old_qty + new_price * new_qty) / (old_qty + new_qty)
