"""Tests for risk_guard.py — delta checks, margin checks, circuit breaker."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from engine.exchange.base import Balance
from engine.risk_guard import RiskAction, RiskGuard


@pytest.fixture
def mock_exchange():
    exchange = MagicMock()
    exchange.get_balance = AsyncMock(return_value=Balance(
        available=1.0, total=2.0, currency="BTC"
    ))
    exchange.cancel_all_after = AsyncMock()
    return exchange


@pytest.fixture
def risk_guard(mock_exchange):
    return RiskGuard(
        exchange=mock_exchange,
        max_delta_pct_nav=0.005,     # 0.5%
        max_margin_utilization=0.50,
        margin_warning_level=0.40,
        liquidation_buffer_pct=0.10,
        dms_interval_s=15,
        dms_timeout_s=60,
        dms_reconnect_timeout_s=120,
    )


# ------------------------------------------------------------------ #
# Delta check                                                          #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_delta_within_bounds(risk_guard):
    result = await risk_guard.check_delta(nav_usd=100_000, net_delta_usd=400)
    assert result.action == RiskAction.OK
    assert result.delta_pct_nav == pytest.approx(0.004)


@pytest.mark.asyncio
async def test_delta_breached(risk_guard):
    result = await risk_guard.check_delta(nav_usd=100_000, net_delta_usd=600)
    assert result.action == RiskAction.REBALANCE
    assert result.delta_pct_nav == pytest.approx(0.006)


@pytest.mark.asyncio
async def test_delta_zero_nav(risk_guard):
    result = await risk_guard.check_delta(nav_usd=0, net_delta_usd=100)
    assert result.action == RiskAction.WARNING


# ------------------------------------------------------------------ #
# Margin check                                                         #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_margin_ok(risk_guard, mock_exchange):
    # available=1.0, total=2.0 → utilization=50% — exactly at hard cap
    # Let's give more room
    mock_exchange.get_balance.return_value = Balance(available=1.5, total=2.0, currency="BTC")
    result = await risk_guard.check_margin()
    # used=0.5, util=0.25 < 0.40 → OK
    assert result.action == RiskAction.OK


@pytest.mark.asyncio
async def test_margin_warning(risk_guard, mock_exchange):
    # utilization = 0.45 → warning
    mock_exchange.get_balance.return_value = Balance(available=0.55, total=1.0, currency="BTC")
    result = await risk_guard.check_margin()
    assert result.action == RiskAction.WARNING
    assert result.margin_utilization == pytest.approx(0.45)


@pytest.mark.asyncio
async def test_margin_hard_stop(risk_guard, mock_exchange):
    # utilization = 0.51 → hard stop
    mock_exchange.get_balance.return_value = Balance(available=0.49, total=1.0, currency="BTC")
    result = await risk_guard.check_margin()
    assert result.action == RiskAction.HARD_STOP
    assert result.margin_utilization >= 0.50


# ------------------------------------------------------------------ #
# Funding circuit breaker (Strategy 1)                                #
# ------------------------------------------------------------------ #

def test_funding_circuit_breaker_not_triggered(risk_guard):
    # Paid 49% of locked basis → should NOT exit
    result = risk_guard.check_funding_circuit_breaker(
        cumulative_funding_paid=0.049,
        locked_basis=0.10,
    )
    assert result.action == RiskAction.OK


def test_funding_circuit_breaker_triggered(risk_guard):
    # Paid exactly 50% of locked basis → exit
    result = risk_guard.check_funding_circuit_breaker(
        cumulative_funding_paid=0.05,
        locked_basis=0.10,
    )
    assert result.action == RiskAction.EXIT_STRATEGY


def test_funding_circuit_breaker_exceeded(risk_guard):
    # Paid 80% → definitely exit
    result = risk_guard.check_funding_circuit_breaker(
        cumulative_funding_paid=0.08,
        locked_basis=0.10,
    )
    assert result.action == RiskAction.EXIT_STRATEGY


def test_funding_circuit_breaker_no_locked_basis(risk_guard):
    # No locked basis → no exit (not a carry trade)
    result = risk_guard.check_funding_circuit_breaker(
        cumulative_funding_paid=0.05,
        locked_basis=0.0,
    )
    assert result.action == RiskAction.OK


# ------------------------------------------------------------------ #
# Liquidation buffer                                                   #
# ------------------------------------------------------------------ #

def test_liquidation_buffer_healthy_long(risk_guard):
    result = risk_guard.check_liquidation_buffer(
        current_price=50000, liquidation_price=40000, side="long"
    )
    assert result.action == RiskAction.OK


def test_liquidation_buffer_warning_long(risk_guard):
    # Within 10% → warning
    result = risk_guard.check_liquidation_buffer(
        current_price=50000, liquidation_price=46000, side="long"
    )
    assert result.action == RiskAction.WARNING


def test_liquidation_buffer_warning_short(risk_guard):
    result = risk_guard.check_liquidation_buffer(
        current_price=50000, liquidation_price=54000, side="short"
    )
    assert result.action == RiskAction.WARNING


# ------------------------------------------------------------------ #
# Dead-man's switch                                                    #
# ------------------------------------------------------------------ #

@pytest.mark.asyncio
async def test_dms_called_on_start(risk_guard, mock_exchange):
    risk_guard.start_dead_mans_switch()
    await asyncio.sleep(0.05)  # let the task schedule
    risk_guard.stop_dead_mans_switch()
    mock_exchange.cancel_all_after.assert_called()


@pytest.mark.asyncio
async def test_dms_reconnect_extends_timeout(risk_guard, mock_exchange):
    # During reconnect, should use 120s timeout
    risk_guard.set_reconnecting(True)
    risk_guard.start_dead_mans_switch()
    await asyncio.sleep(0.05)
    risk_guard.stop_dead_mans_switch()
    calls = mock_exchange.cancel_all_after.call_args_list
    if calls:
        # Should be called with 120_000ms (120s * 1000)
        assert calls[0][0][0] == 120_000
    risk_guard.set_reconnecting(False)


# ------------------------------------------------------------------ #
# CRITICAL: get_net_delta_usd iterates .items() correctly              #
# Regression test for the bug where pos.get("currentQty") was called  #
# on a (str, dict) tuple instead of the dict value, silencing the     #
# delta guard.                                                          #
# ------------------------------------------------------------------ #

def test_position_tracker_delta_reads_dict_not_tuple():
    """
    get_net_delta_usd() must iterate _live_positions.items() and read
    from the value dict, not from the (key, value) tuple. If this is
    wrong, it returns 0 regardless of actual position sizes and the
    REBALANCE guard never fires.
    """
    from engine.position_tracker import PositionTracker

    tracker = PositionTracker.__new__(PositionTracker)
    tracker._live_instruments = {}  # no live price data in this unit test

    # Single inverse position: short $10k XBTUSD — currentQty is already in USD
    tracker._live_positions = {
        "XBTUSD": {"currentQty": -10000},
    }
    delta = tracker.get_net_delta_usd()
    assert delta == pytest.approx(-10000.0), (
        f"Expected -10000 but got {delta}. "
        "If 0, the bug is back: .get() is being called on the tuple not the dict."
    )

    # Two inverse legs that cancel (long + short same notional)
    tracker._live_positions = {
        "XBTUSD": {"currentQty": -10000},
        "ETHUSD": {"currentQty":  10000},
    }
    delta_net = tracker.get_net_delta_usd()
    assert delta_net == pytest.approx(0.0)  # hedged → zero net delta
