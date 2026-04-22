"""
Unit tests for funding_analysis.normalize — the APR math is load-bearing for
every chart, so exercise the boundaries explicitly.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from funding_analysis.normalize import (
    binance_hourly_from_daily,
    downsample_to_bucket,
    geometric_annualized,
    to_annualized_apr,
    to_daily,
)


class TestAnnualizedAPR:
    def test_bitmex_8h_baseline(self):
        # 0.01%/8h = 3 periods/day * 365 days = 10.95% APR.
        assert to_annualized_apr(0.0001, 8) == pytest.approx(0.0001 * 3 * 365)

    def test_hyperliquid_1h(self):
        # 0.001%/h = 24 * 365 = 8760 periods/y.
        assert to_annualized_apr(0.00001, 1) == pytest.approx(0.00001 * 24 * 365)

    def test_negative_rate_stays_negative(self):
        assert to_annualized_apr(-0.0002, 8) < 0

    def test_zero_rate_is_zero(self):
        assert to_annualized_apr(0.0, 8) == 0.0

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError):
            to_annualized_apr(0.0001, 0)
        with pytest.raises(ValueError):
            to_annualized_apr(0.0001, -1)


class TestDaily:
    def test_bitmex_8h_to_daily(self):
        # 0.01%/8h * 3 = 0.03%/day.
        assert to_daily(0.0001, 8) == pytest.approx(0.0003)

    def test_hourly_to_daily(self):
        assert to_daily(0.0001, 1) == pytest.approx(0.0024)


class TestBinanceHourly:
    def test_divide_by_24(self):
        assert binance_hourly_from_daily(0.00024) == pytest.approx(0.00001)


class TestDownsample:
    def _ts(self, hour: int) -> datetime:
        return datetime(2026, 1, 1, hour, 0, 0, tzinfo=timezone.utc)

    def test_sum_mode_8h_bucket(self):
        # 8 hourly rates of 0.0001 → one 8h bucket of 0.0008.
        points = [(self._ts(h), 0.0001) for h in range(8)]
        buckets = downsample_to_bucket(points, bucket_hours=8, mode="sum")
        assert len(buckets) == 1
        assert buckets[0][0] == self._ts(0)
        assert buckets[0][1] == pytest.approx(0.0008)

    def test_mean_mode_8h_bucket(self):
        points = [(self._ts(h), 0.0001 + h * 1e-6) for h in range(8)]
        buckets = downsample_to_bucket(points, bucket_hours=8, mode="mean")
        assert len(buckets) == 1
        expected = sum(0.0001 + h * 1e-6 for h in range(8)) / 8
        assert buckets[0][1] == pytest.approx(expected)

    def test_multiple_buckets(self):
        # 16 hourly points → two 8h buckets.
        points = [(self._ts(h), 0.0001) for h in range(16)]
        buckets = downsample_to_bucket(points, bucket_hours=8, mode="sum")
        assert len(buckets) == 2
        assert buckets[0][0] == self._ts(0)
        assert buckets[1][0] == self._ts(8)

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            downsample_to_bucket([], bucket_hours=8, mode="median")  # type: ignore

    def test_invalid_bucket_raises(self):
        with pytest.raises(ValueError):
            downsample_to_bucket([], bucket_hours=0, mode="sum")


class TestGeometricAnnualized:
    def test_small_rate_close_to_simple(self):
        # For tiny rates, compounded ≈ simple.
        rate = 0.00001  # per hour
        simple = to_annualized_apr(rate, 1)
        compounded = geometric_annualized(rate, 1)
        assert compounded == pytest.approx(simple, rel=0.05)
