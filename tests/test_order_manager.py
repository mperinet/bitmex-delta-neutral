"""Tests for order_manager.py — rate limit bucket and progressive entry."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio

from engine.exchange.base import Balance, OrderResult
from engine.order_manager import OrderManager, RateLimitBucket


def make_order(order_id="o1", symbol="BTC/USD:BTC", side="sell", qty=1000.0,
               filled=1000.0, price=50000.0, status="closed"):
    return OrderResult(
        order_id=order_id,
        symbol=symbol,
        side=side,
        qty=qty,
        filled_qty=filled,
        avg_price=price,
        status=status,
        fee=0.075,
    )


@pytest.fixture
def mock_exchange():
    ex = MagicMock()
    ex.place_limit_order = AsyncMock(return_value=make_order())
    ex.place_market_order = AsyncMock(return_value=make_order())
    ex.cancel_order = AsyncMock(return_value=True)
    ex.cancel_all_orders = AsyncMock(return_value=3)
    ex.get_order = AsyncMock(return_value=make_order(status="closed"))
    ex.get_balance = AsyncMock(return_value=Balance(available=1.0, total=2.0, currency="BTC"))
    return ex


@pytest.fixture
def bucket():
    b = RateLimitBucket(initial_tokens=300)
    return b


@pytest.fixture
def order_mgr(mock_exchange, bucket):
    return OrderManager(exchange=mock_exchange, bucket=bucket)


# ------------------------------------------------------------------ #
# Rate limit bucket                                                    #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_bucket_initialized_with_tokens(bucket):
    assert bucket.tokens_remaining == 300


@pytest.mark.asyncio
async def test_bucket_depletes(bucket):
    for _ in range(5):
        await bucket.acquire()
    assert bucket.tokens_remaining == 295


@pytest.mark.asyncio
async def test_bucket_emergency_reserve_blocks_normal(bucket):
    # Drain to just above emergency reserve (20)
    for _ in range(280):
        await bucket.acquire()
    assert bucket.tokens_remaining == 20

    # Normal acquire should block (timeout expected)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bucket.acquire(emergency=False), timeout=0.1)


@pytest.mark.asyncio
async def test_bucket_emergency_can_go_below_reserve(bucket):
    # Drain to emergency reserve
    for _ in range(280):
        await bucket.acquire()
    # Emergency acquire should succeed even below reserve
    await bucket.acquire(emergency=True)
    assert bucket.tokens_remaining == 19


# ------------------------------------------------------------------ #
# Order placement                                                      #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_place_limit_calls_exchange(order_mgr, mock_exchange):
    result = await order_mgr.place_limit("BTC/USD:BTC", "sell", 1000, 50000)
    mock_exchange.place_limit_order.assert_called_once_with(
        "BTC/USD:BTC", "sell", 1000, 50000, True
    )
    assert result.order_id == "o1"


@pytest.mark.asyncio
async def test_cancel_calls_exchange(order_mgr, mock_exchange):
    ok = await order_mgr.cancel("o1", "BTC/USD:BTC")
    mock_exchange.cancel_order.assert_called_once_with("o1", "BTC/USD:BTC")
    assert ok is True


# ------------------------------------------------------------------ #
# Progressive entry                                                    #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_progressive_entry_all_slices_fill(order_mgr, mock_exchange):
    """Happy path: all 5 slices fill for both legs."""
    # Patch DB calls
    with patch("engine.order_manager.repository") as mock_repo:
        mock_pos = MagicMock()
        mock_pos.leg_a_qty = 0
        mock_pos.leg_b_qty = 0
        mock_pos.leg_a_avg_entry = None
        mock_pos.leg_b_avg_entry = None
        mock_pos.entry_slices_done = 0
        mock_pos.entry_slices_total = 5
        mock_repo.get_position = AsyncMock(return_value=mock_pos)
        mock_repo.update_position = AsyncMock()
        mock_repo.record_trade = AsyncMock()

        success = await order_mgr.progressive_entry(
            position_id=1,
            strategy="test",
            leg_a_symbol="BTC/USD:BTC",
            leg_a_side="sell",
            leg_b_symbol="BTC/USDT",
            leg_b_side="buy",
            total_qty_a=5000.0,
            total_qty_b=0.1,
            leg_a_price=50000.0,
            leg_b_price=50010.0,
            n_slices=5,
            fill_timeout_s=5.0,
        )
        assert success is True
        assert mock_exchange.place_limit_order.call_count == 10  # 5 slices × 2 legs


@pytest.mark.asyncio
async def test_progressive_entry_leg_b_fails_on_slice_3(order_mgr, mock_exchange):
    """
    Leg B fails on slice 3: leg A slice 3 should be unwound via market order.
    No further slices should be placed.
    """
    call_count = {"leg_a": 0, "leg_b": 0}

    async def place_limit_side_effect(symbol, side, qty, price, post_only=True):
        if symbol == "BTC/USD:BTC":
            call_count["leg_a"] += 1
            return make_order(order_id=f"a{call_count['leg_a']}", symbol=symbol, side=side)
        else:
            call_count["leg_b"] += 1
            return make_order(order_id=f"b{call_count['leg_b']}", symbol=symbol, side=side)

    get_order_call = {"count": 0}

    async def get_order_side_effect(order_id, symbol):
        get_order_call["count"] += 1
        # Leg B slice 3 never fills (return "open" status → timeout)
        if order_id.startswith("b") and int(order_id[1:]) == 3:
            return make_order(order_id=order_id, status="open")
        return make_order(order_id=order_id, status="closed")

    mock_exchange.place_limit_order = AsyncMock(side_effect=place_limit_side_effect)
    mock_exchange.get_order = AsyncMock(side_effect=get_order_side_effect)

    with patch("engine.order_manager.repository") as mock_repo:
        mock_pos = MagicMock()
        mock_pos.leg_a_qty = 0
        mock_pos.leg_b_qty = 0
        mock_pos.leg_a_avg_entry = None
        mock_pos.leg_b_avg_entry = None
        mock_pos.entry_slices_done = 0
        mock_pos.entry_slices_total = 5
        mock_repo.get_position = AsyncMock(return_value=mock_pos)
        mock_repo.update_position = AsyncMock()
        mock_repo.record_trade = AsyncMock()

        success = await order_mgr.progressive_entry(
            position_id=1,
            strategy="test",
            leg_a_symbol="BTC/USD:BTC",
            leg_a_side="sell",
            leg_b_symbol="BTC/USDT",
            leg_b_side="buy",
            total_qty_a=5000.0,
            total_qty_b=0.1,
            leg_a_price=50000.0,
            leg_b_price=50010.0,
            n_slices=5,
            fill_timeout_s=0.1,  # short timeout for test speed
        )

    assert success is False
    # After slice 3 fails: leg A slice 3 should be unwound (market order)
    mock_exchange.place_market_order.assert_called_once()
    unwind_call = mock_exchange.place_market_order.call_args
    assert unwind_call[0][0] == "BTC/USD:BTC"   # correct symbol
    assert unwind_call[0][1] == "buy"            # reverse of "sell"
