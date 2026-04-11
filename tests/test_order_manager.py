"""Tests for order_manager.py — rate limit bucket, orderbook helpers, fill_next_slice."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.exchange.base import Balance, OrderBook, OrderResult
from engine.order_manager import OrderManager, RateLimitBucket


def make_order(order_id="o1", symbol="BTC/USD:BTC", side="sell", qty=1000.0,
               filled=1000.0, price=50000.0, status="closed", fee=0.075):
    return OrderResult(
        order_id=order_id, symbol=symbol, side=side, qty=qty,
        filled_qty=filled, avg_price=price, status=status, fee=fee,
    )


def make_orderbook(bids=None, asks=None, symbol="BTC/USD:BTC"):
    return OrderBook(
        symbol=symbol,
        bids=bids or [[49990, 500], [49980, 1000]],
        asks=asks or [[50010, 500], [50020, 1000]],
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
    ex.fetch_orderbook = AsyncMock(return_value=make_orderbook())
    return ex


@pytest.fixture
def bucket():
    return RateLimitBucket(initial_tokens=300)


@pytest.fixture
def order_mgr(mock_exchange, bucket):
    return OrderManager(exchange=mock_exchange, bucket=bucket, max_slippage=0.001)


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
    for _ in range(280):
        await bucket.acquire()
    assert bucket.tokens_remaining == 20

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bucket.acquire(emergency=False), timeout=0.1)


@pytest.mark.asyncio
async def test_bucket_emergency_can_go_below_reserve(bucket):
    for _ in range(280):
        await bucket.acquire()
    await bucket.acquire(emergency=True)
    assert bucket.tokens_remaining == 19


# ------------------------------------------------------------------ #
# Order placement                                                      #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_place_limit_calls_exchange(order_mgr, mock_exchange):
    result = await order_mgr.place_limit("BTC/USD:BTC", "sell", 1000, 50000)
    mock_exchange.place_limit_order.assert_called_once_with("BTC/USD:BTC", "sell", 1000, 50000, True)
    assert result.order_id == "o1"


@pytest.mark.asyncio
async def test_cancel_calls_exchange(order_mgr, mock_exchange):
    ok = await order_mgr.cancel("o1", "BTC/USD:BTC")
    mock_exchange.cancel_order.assert_called_once_with("o1", "BTC/USD:BTC")
    assert ok is True


# ------------------------------------------------------------------ #
# _available_qty — static method unit tests                            #
# ------------------------------------------------------------------ #

def test_available_qty_buy_sums_asks_within_slippage():
    # mid=50000, limit=50050 (0.1% slippage)
    # asks: 50010 → in, 50030 → in, 50100 → out
    ob = make_orderbook(asks=[[50010, 100], [50030, 200], [50100, 500]], bids=[])
    qty = OrderManager._available_qty(ob, "buy", 50000, 0.001)
    assert qty == 300  # 100 + 200


def test_available_qty_sell_sums_bids_within_slippage():
    # mid=50000, limit=49950 (0.1% slippage)
    # bids: 49990 → in, 49960 → in, 49800 → out
    ob = make_orderbook(bids=[[49990, 150], [49960, 250], [49800, 500]], asks=[])
    qty = OrderManager._available_qty(ob, "sell", 50000, 0.001)
    assert qty == 400  # 150 + 250


def test_available_qty_no_depth_within_slippage():
    # Only ask is outside the 0.1% slippage band
    ob = make_orderbook(asks=[[50100, 500]], bids=[])
    qty = OrderManager._available_qty(ob, "buy", 50000, 0.001)
    assert qty == 0


def test_available_qty_zero_mid_price():
    ob = make_orderbook()
    qty = OrderManager._available_qty(ob, "buy", 0, 0.001)
    assert qty == 0


def test_available_qty_wider_slippage_captures_more():
    ob = make_orderbook(asks=[[50010, 100], [50030, 200], [50100, 500]], bids=[])
    qty_tight = OrderManager._available_qty(ob, "buy", 50000, 0.001)   # 0.1%
    qty_wide  = OrderManager._available_qty(ob, "buy", 50000, 0.002)   # 0.2%
    assert qty_wide >= qty_tight


# ------------------------------------------------------------------ #
# fill_next_slice                                                      #
# ------------------------------------------------------------------ #

def _make_pos(leg_a_qty=0.0, leg_b_qty=0.0, leg_a_target=1000.0, leg_b_target=0.02,
              slices_done=0):
    pos = MagicMock()
    pos.leg_a_qty = leg_a_qty
    pos.leg_b_qty = leg_b_qty
    pos.leg_a_target_qty = leg_a_target
    pos.leg_b_target_qty = leg_b_target
    pos.leg_a_avg_entry = None
    pos.leg_b_avg_entry = None
    pos.entry_slices_done = slices_done
    pos.state = "entering"
    return pos


@pytest.mark.asyncio
async def test_fill_next_slice_places_market_orders(order_mgr, mock_exchange):
    """Sufficient depth on both legs → two market orders placed, returns True."""
    # Orderbook has 500 USD depth within slippage (more than the 1000 USD target in one shot)
    mock_exchange.fetch_orderbook = AsyncMock(return_value=make_orderbook(
        asks=[[50010, 2000]], bids=[[49990, 2000]]
    ))
    mock_exchange.place_market_order = AsyncMock(return_value=make_order(filled=1000.0))

    with patch("engine.order_manager.repository") as repo:
        repo.get_position = AsyncMock(return_value=_make_pos())
        repo.update_position = AsyncMock()
        repo.record_trade = AsyncMock()

        result = await order_mgr.fill_next_slice(
            position_id=1, strategy="test",
            leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
            leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
        )

    assert result is True
    assert mock_exchange.place_market_order.call_count == 2


@pytest.mark.asyncio
async def test_fill_next_slice_skips_when_leg_a_has_no_depth(order_mgr, mock_exchange):
    """Leg A orderbook has no depth within slippage → skip, return False."""
    mock_exchange.fetch_orderbook = AsyncMock(side_effect=[
        make_orderbook(asks=[[50200, 500]], bids=[[49990, 500]]),  # leg A: ask outside slippage
        make_orderbook(asks=[[50010, 500]], bids=[[49990, 500]]),  # leg B: fine
    ])

    result = await order_mgr.fill_next_slice(
        position_id=1, strategy="test",
        leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
        leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
    )

    assert result is False
    mock_exchange.place_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_fill_next_slice_skips_when_leg_b_has_no_depth(order_mgr, mock_exchange):
    """Leg B has no depth within slippage → skip, return False."""
    mock_exchange.fetch_orderbook = AsyncMock(side_effect=[
        make_orderbook(asks=[[50010, 500]], bids=[[49990, 500]]),  # leg A: fine
        make_orderbook(asks=[[50200, 500]], bids=[[49990, 500]]),  # leg B: ask outside slippage
    ])

    result = await order_mgr.fill_next_slice(
        position_id=1, strategy="test",
        leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
        leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
    )

    assert result is False
    mock_exchange.place_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_fill_next_slice_unwinds_leg_a_on_leg_b_failure(order_mgr, mock_exchange):
    """Leg B market order raises → leg A immediately unwound."""
    mock_exchange.fetch_orderbook = AsyncMock(return_value=make_orderbook(
        asks=[[50010, 2000]], bids=[[49990, 2000]]
    ))

    call_count = {"n": 0}

    async def place_market_side_effect(symbol, side, qty, emergency=False):
        call_count["n"] += 1
        if call_count["n"] == 2:  # leg B
            raise RuntimeError("exchange error")
        return make_order(symbol=symbol, side=side, filled=qty)

    mock_exchange.place_market_order = AsyncMock(side_effect=place_market_side_effect)

    with patch("engine.order_manager.repository"):
        with pytest.raises(RuntimeError):
            await order_mgr.fill_next_slice(
                position_id=1, strategy="test",
                leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
                leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
            )

    # 3 calls total: leg A, leg B (raises), unwind leg A
    assert mock_exchange.place_market_order.call_count == 3
    unwind = mock_exchange.place_market_order.call_args_list[2]
    assert unwind[0][0] == "BTC/USD:BTC"
    assert unwind[0][1] == "buy"  # reverse of "sell"


@pytest.mark.asyncio
async def test_fill_next_slice_marks_active_when_fully_filled(order_mgr, mock_exchange):
    """When the slice fills the remaining qty, position transitions to ACTIVE."""
    from engine.db.models import PositionState

    mock_exchange.fetch_orderbook = AsyncMock(return_value=make_orderbook(
        asks=[[50010, 2000]], bids=[[49990, 2000]]
    ))
    mock_exchange.place_market_order = AsyncMock(return_value=make_order(filled=1000.0))

    # Position already has 0 filled; target is 1000 → this slice fills it completely
    with patch("engine.order_manager.repository") as repo:
        repo.get_position = AsyncMock(return_value=_make_pos(
            leg_a_qty=0.0, leg_b_qty=0.0,
            leg_a_target=1000.0, leg_b_target=0.02,
        ))
        repo.update_position = AsyncMock()
        repo.record_trade = AsyncMock()

        mock_exchange.place_market_order = AsyncMock(side_effect=[
            make_order(filled=1000.0, symbol="BTC/USD:BTC"),  # leg A fills target
            make_order(filled=0.02,   symbol="BTC/USDT"),      # leg B fills target
        ])

        await order_mgr.fill_next_slice(
            position_id=1, strategy="test",
            leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
            leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
        )

        update_call = repo.update_position.call_args
        assert update_call[1]["state"] == PositionState.ACTIVE


@pytest.mark.asyncio
async def test_fill_next_slice_already_filled_marks_active(order_mgr, mock_exchange):
    """If remaining is 0 on both legs, mark ACTIVE without placing orders."""
    with patch("engine.order_manager.repository") as repo:
        repo.update_position = AsyncMock()

        result = await order_mgr.fill_next_slice(
            position_id=1, strategy="test",
            leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=0.0,
            leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.0,
        )

    assert result is True
    mock_exchange.place_market_order.assert_not_called()
    mock_exchange.fetch_orderbook.assert_not_called()


@pytest.mark.asyncio
async def test_fill_next_slice_proportional_sizing(order_mgr, mock_exchange):
    """
    Leg B depth constrains the slice. Both legs should be filled by the same
    fraction of their remaining quantities.
    """
    # Leg A (sell): bids deep — 5000 USD available within slippage
    # Leg B (buy):  asks shallow — only 0.005 BTC available within slippage
    leg_a_ob = make_orderbook(bids=[[49990, 5000], [49980, 5000]], asks=[[50010, 1]])
    leg_b_ob = make_orderbook(asks=[[50010, 0.005]], bids=[[49990, 5]])

    mock_exchange.fetch_orderbook = AsyncMock(side_effect=[leg_a_ob, leg_b_ob])

    placed_qtys = []

    async def capture_market(symbol, side, qty, emergency=False):
        placed_qtys.append((symbol, qty))
        return make_order(symbol=symbol, side=side, filled=qty)

    mock_exchange.place_market_order = AsyncMock(side_effect=capture_market)

    with patch("engine.order_manager.repository") as repo:
        repo.get_position = AsyncMock(return_value=_make_pos(
            leg_a_target=1000.0, leg_b_target=0.02,
        ))
        repo.update_position = AsyncMock()
        repo.record_trade = AsyncMock()

        await order_mgr.fill_next_slice(
            position_id=1, strategy="test",
            leg_a_symbol="BTC/USD:BTC", leg_a_side="sell", leg_a_remaining=1000.0,
            leg_b_symbol="BTC/USDT",   leg_b_side="buy",  leg_b_remaining=0.02,
        )

    # Leg B was the constraint (0.005 / 0.02 = 25% of remaining)
    # So leg A should also be 25%: 1000 * 0.25 = 250
    leg_a_qty = placed_qtys[0][1]
    leg_b_qty = placed_qtys[1][1]
    assert leg_b_qty == pytest.approx(0.005, rel=1e-6)
    assert leg_a_qty == pytest.approx(250.0, rel=1e-6)
