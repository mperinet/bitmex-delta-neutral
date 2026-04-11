"""
Tests for strategies — entry/exit signals, circuit breakers, inverse math.

Inverse contract math tests are marked as CRITICAL and must pass
before any live deployment. PnL = notional * (1/entry - 1/exit).
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from engine.exchange.bitmex import BitMEXExchange
from engine.exchange.base import Balance, Ticker
from engine.db.models import Position, PositionState
from engine.risk_guard import RiskAction


# ================================================================== #
# CRITICAL: Inverse contract math                                      #
# These tests verify the non-linear PnL formula for BTC-settled        #
# contracts. A wrong division order inverts delta neutrality.          #
# ================================================================== #

class TestInverseContractMath:
    def test_long_profit(self):
        """Long 1000 USD, price goes up: should profit in BTC."""
        pnl = BitMEXExchange.inverse_pnl(1000, entry_price=50000, exit_price=60000)
        assert pnl > 0
        assert pnl == pytest.approx(1000 * (1/50000 - 1/60000))

    def test_long_loss(self):
        """Long 1000 USD, price goes down: should lose in BTC."""
        pnl = BitMEXExchange.inverse_pnl(1000, entry_price=50000, exit_price=40000)
        assert pnl < 0

    def test_short_profit(self):
        """Short 1000 USD (negate notional), price goes down: should profit."""
        pnl = BitMEXExchange.inverse_pnl(-1000, entry_price=50000, exit_price=40000)
        assert pnl > 0

    def test_pnl_at_same_price(self):
        """No price movement = no PnL."""
        pnl = BitMEXExchange.inverse_pnl(1000, entry_price=50000, exit_price=50000)
        assert pnl == pytest.approx(0.0)

    def test_zero_entry_price_raises(self):
        with pytest.raises(ValueError):
            BitMEXExchange.inverse_pnl(1000, entry_price=0, exit_price=50000)

    def test_zero_exit_price_raises(self):
        with pytest.raises(ValueError):
            BitMEXExchange.inverse_pnl(1000, entry_price=50000, exit_price=0)

    def test_hedge_ratio_basic(self):
        """At $50k spot, $10k notional needs 0.2 BTC to hedge."""
        ratio = BitMEXExchange.compute_hedge_ratio(10000, 50000)
        assert ratio == pytest.approx(0.2)

    def test_hedge_ratio_drifts_with_price(self):
        """Hedge ratio changes as spot moves — this is the delta drift."""
        ratio_at_50k = BitMEXExchange.compute_hedge_ratio(10000, 50000)
        ratio_at_40k = BitMEXExchange.compute_hedge_ratio(10000, 40000)
        assert ratio_at_40k > ratio_at_50k  # need more BTC at lower price

    def test_hedge_ratio_zero_price_raises(self):
        with pytest.raises(ValueError):
            BitMEXExchange.compute_hedge_ratio(10000, 0)

    def test_annualised_basis(self):
        """Basic basis calculation."""
        basis = BitMEXExchange.compute_annualised_basis(
            future_price=51000, spot_price=50000, days_to_expiry=90
        )
        expected = (51000/50000 - 1) * (365/90)
        assert basis == pytest.approx(expected)

    def test_annualised_basis_zero_days_raises(self):
        with pytest.raises(ValueError):
            BitMEXExchange.compute_annualised_basis(51000, 50000, days_to_expiry=0)


# ================================================================== #
# Strategy 2: Funding Harvest entry/exit signals                       #
# ================================================================== #

def make_position(**kwargs):
    defaults = dict(
        id=1,
        strategy="funding_harvest",
        state=PositionState.ACTIVE,
        leg_a_symbol="BTC/USD:BTC",
        leg_a_side="short",
        leg_a_qty=10000.0,
        leg_b_symbol="BTC/USDT",
        leg_b_side="long",
        leg_b_qty=0.2,
        cumulative_funding_paid=0.0,
        locked_basis=None,
        entry_slices_total=5,
        entry_slices_done=5,
    )
    defaults.update(kwargs)
    pos = MagicMock(spec=Position)
    for k, v in defaults.items():
        setattr(pos, k, v)
    return pos


class TestFundingHarvestStrategy:
    def _make_strategy(self, funding_rate=0.0003):
        """Create a FundingHarvestStrategy with mocked dependencies."""
        from engine.strategies.funding_harvest import FundingHarvestStrategy

        tracker = MagicMock()
        tracker.get_latest_funding_rate = MagicMock(return_value=funding_rate)
        tracker.wait_ready = AsyncMock()

        exchange = MagicMock()
        exchange.get_ticker = AsyncMock(return_value=Ticker(
            symbol="BTC/USD:BTC", bid=49990, ask=50010, last=50000, mark_price=50000
        ))
        exchange.get_balance = AsyncMock(return_value=Balance(
            available=1.0, total=2.0, currency="BTC"
        ))

        risk = MagicMock()
        risk.check_margin = AsyncMock(return_value=MagicMock(action=RiskAction.OK))

        return FundingHarvestStrategy(
            exchange=exchange,
            order_manager=MagicMock(),
            position_tracker=tracker,
            risk_guard=risk,
            config={
                "min_funding_rate": 0.0001,
                "entry_threshold_multiplier": 3,
                "max_position_usd": 10000,
                "entry_slices": 5,
                "slice_fill_timeout_s": 30,
            },
        )

    @pytest.mark.asyncio
    async def test_should_enter_above_threshold(self):
        s = self._make_strategy(funding_rate=0.00031)  # above 3x baseline threshold
        assert await s.should_enter() is True

    @pytest.mark.asyncio
    async def test_should_not_enter_below_threshold(self):
        s = self._make_strategy(funding_rate=0.00005)  # below baseline
        assert await s.should_enter() is False

    @pytest.mark.asyncio
    async def test_should_not_enter_at_baseline(self):
        s = self._make_strategy(funding_rate=0.0001)  # exactly baseline, < 3x
        assert await s.should_enter() is False

    @pytest.mark.asyncio
    async def test_should_exit_negative_funding(self):
        s = self._make_strategy(funding_rate=-0.0001)  # negative → exit
        pos = make_position()
        assert await s.should_exit(pos) is True

    @pytest.mark.asyncio
    async def test_should_exit_normalised_funding(self):
        s = self._make_strategy(funding_rate=0.00005)  # below exit threshold
        pos = make_position()
        assert await s.should_exit(pos) is True

    @pytest.mark.asyncio
    async def test_should_not_exit_elevated_funding(self):
        s = self._make_strategy(funding_rate=0.0005)  # still elevated → hold
        pos = make_position()
        assert await s.should_exit(pos) is False

    @pytest.mark.asyncio
    async def test_should_not_exit_no_rate_data(self):
        s = self._make_strategy(funding_rate=None)
        pos = make_position()
        # No data → don't exit (conservative)
        assert await s.should_exit(pos) is False


# ================================================================== #
# Strategy 1: Cash-and-Carry circuit breaker (via risk guard)          #
# ================================================================== #

class TestCashAndCarryCircuitBreaker:
    """
    These tests use the risk_guard.check_funding_circuit_breaker() directly
    since that's where the logic lives. See test_risk_guard.py for full coverage.
    """

    def test_60_periods_at_baseline_does_not_trigger(self):
        from engine.risk_guard import RiskGuard
        rg = RiskGuard(exchange=MagicMock(), max_delta_pct_nav=0.005)

        # 60 × 0.01%/8h = 0.6% cumulative; locked basis = 15%
        # ratio = 0.006/0.15 = 4% → well under 50%
        result = rg.check_funding_circuit_breaker(
            cumulative_funding_paid=0.006,
            locked_basis=0.15,
        )
        assert result.action == RiskAction.OK

    def test_circuit_breaker_fires_when_funding_erodes_basis(self):
        from engine.risk_guard import RiskGuard
        rg = RiskGuard(exchange=MagicMock(), max_delta_pct_nav=0.005)

        # Basis = 10%, funding eaten = 5.1% → ratio = 51% → exit
        result = rg.check_funding_circuit_breaker(
            cumulative_funding_paid=0.051,
            locked_basis=0.10,
        )
        assert result.action == RiskAction.EXIT_STRATEGY

    def test_exactly_50_percent_triggers(self):
        from engine.risk_guard import RiskGuard
        rg = RiskGuard(exchange=MagicMock(), max_delta_pct_nav=0.005)
        result = rg.check_funding_circuit_breaker(0.05, locked_basis=0.10)
        assert result.action == RiskAction.EXIT_STRATEGY

    def test_49_percent_does_not_trigger(self):
        from engine.risk_guard import RiskGuard
        rg = RiskGuard(exchange=MagicMock(), max_delta_pct_nav=0.005)
        result = rg.check_funding_circuit_breaker(0.0499, locked_basis=0.10)
        assert result.action == RiskAction.OK
