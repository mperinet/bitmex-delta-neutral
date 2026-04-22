"""
Unit tests for funding_analysis.simulator — exercised against a real SQLite
database seeded with known-input series so the math is deterministic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from funding_analysis.db import repository
from funding_analysis.db.models import init_db
from funding_analysis.simulator import simulate_payout


@pytest.fixture
async def seeded_db(tmp_path):
    """Build a fresh in-file SQLite DB for the simulator to read."""
    # Reset the module-level engine so init_db attaches to this fresh file.
    import funding_analysis.db.models as models

    models._engine = None  # type: ignore[attr-defined]
    models._session_factory = None  # type: ignore[attr-defined]

    db_path = tmp_path / "sim.db"
    await init_db(f"sqlite+aiosqlite:///{db_path}")

    # Seed universe row (not strictly required by the simulator, but matches
    # production data shape).
    await repository.upsert_asset(
        asset="BTC",
        bitmex_symbol="XBTUSD",
        bitmex_contract_type="inverse_perp",
        hyperliquid_name="BTC",
        binance_spot_symbol="BTCUSDT",
        discovered_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    # Seed 24 hourly HL funding points of 0.00005 (0.005%/h) on Jan 2 2026.
    day = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    for h in range(24):
        ts = day + timedelta(hours=h)
        await repository.upsert_funding_rate(
            exchange="hyperliquid",
            asset="BTC",
            venue_symbol="BTC",
            timestamp=ts.replace(tzinfo=None),
            funding_rate=0.00005,
            interval_hours=1,
        )

    # Seed 3 BitMEX funding points of 0.0001 (0.01%/8h) on the same day.
    for h in (4, 12, 20):
        ts = day.replace(hour=h)
        await repository.upsert_funding_rate(
            exchange="bitmex",
            asset="BTC",
            venue_symbol="XBTUSD",
            timestamp=ts.replace(tzinfo=None),
            funding_rate=0.0001,
            interval_hours=8,
        )

    # Seed a single flat Binance USDT borrow rate of 0.0002/day.
    await repository.upsert_binance_margin_rate(
        asset="USDT",
        timestamp=day.replace(tzinfo=None) - timedelta(days=1),  # before window
        daily_interest_rate=0.0002,
        vip_level=0,
    )
    yield
    models._engine = None  # type: ignore[attr-defined]
    models._session_factory = None  # type: ignore[attr-defined]


class TestSimulator:
    async def test_short_hl_btc_one_day(self, seeded_db):
        start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        result = await simulate_payout(
            asset="BTC",
            venue="hyperliquid",
            side="short",
            notional_usd=10_000,
            start=start,
            end=end,
            hedge_currency="USDT",
        )
        # Short BTC on HL with +0.005%/h for 24h on $10k:
        #   24 periods × 10000 × 0.00005 × sign(+1) = +12.0 USD
        assert result.total_funding == pytest.approx(12.0, rel=1e-6)

        # Borrow cost: 10000 × 0.0002/day = 2.0/day; simulator steps hourly
        # so total ~2.0 over 24h (sign negative).
        assert result.total_borrow == pytest.approx(-2.0, rel=1e-3)

        # Net ~ +10.0
        assert result.net == pytest.approx(10.0, rel=1e-3)

        # Annualized APR = net / notional * (365 / days) = 10/10000 * 365 = 0.365
        assert result.annualized_net_apr == pytest.approx(0.365, rel=1e-3)

        assert result.funding_periods == 24
        assert result.borrow_hours == 24

    async def test_long_flips_sign(self, seeded_db):
        start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        short = await simulate_payout(
            asset="BTC",
            venue="hyperliquid",
            side="short",
            notional_usd=10_000,
            start=start,
            end=end,
        )
        long = await simulate_payout(
            asset="BTC",
            venue="hyperliquid",
            side="long",
            notional_usd=10_000,
            start=start,
            end=end,
        )
        assert long.total_funding == pytest.approx(-short.total_funding, rel=1e-6)
        assert long.sign == -1
        assert short.sign == +1

    async def test_bitmex_short_uses_8h_interval(self, seeded_db):
        start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        result = await simulate_payout(
            asset="BTC",
            venue="bitmex",
            side="short",
            notional_usd=10_000,
            start=start,
            end=end,
        )
        # 3 periods × 10000 × 0.0001 = +3.0 USD funding.
        assert result.total_funding == pytest.approx(3.0, rel=1e-6)
        assert result.funding_periods == 3

    async def test_invalid_notional_raises(self, seeded_db):
        start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        with pytest.raises(ValueError):
            await simulate_payout(
                asset="BTC",
                venue="hyperliquid",
                side="short",
                notional_usd=0,
                start=start,
                end=end,
            )

    async def test_invalid_window_raises(self, seeded_db):
        start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError):
            await simulate_payout(
                asset="BTC",
                venue="hyperliquid",
                side="short",
                notional_usd=10_000,
                start=start,
                end=start,
            )
