"""
Pure rate-math helpers — no I/O, no DB.

All charts and tables pass funding rates through these functions so that
HyperLiquid (hourly) and BitMEX (8h) rates can be compared on the same axis.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timedelta

HOURS_PER_YEAR = 24 * 365
HOURS_PER_DAY = 24


def to_annualized_apr(rate: float, interval_hours: int) -> float:
    """
    Convert a per-period funding rate to annualized APR (simple interest).

    Example: 0.0001 per 8h → 0.0001 * 3 * 365 = 0.10950 → 10.95% APR.
    """
    if interval_hours <= 0:
        raise ValueError("interval_hours must be positive")
    periods_per_year = HOURS_PER_YEAR / interval_hours
    return rate * periods_per_year


def to_daily(rate: float, interval_hours: int) -> float:
    """Convert a per-period rate to per-day equivalent (simple, not compounded)."""
    if interval_hours <= 0:
        raise ValueError("interval_hours must be positive")
    return rate * (HOURS_PER_DAY / interval_hours)


def binance_hourly_from_daily(daily_rate: float) -> float:
    """Binance publishes margin interest as dailyInterestRate; hourly = daily / 24."""
    return daily_rate / HOURS_PER_DAY


def downsample_to_bucket(
    points: Iterable[tuple[datetime, float]],
    bucket_hours: int,
    mode: str = "sum",
) -> list[tuple[datetime, float]]:
    """
    Downsample (timestamp, rate) points into `bucket_hours` buckets.

    `mode`:
      - "sum":  bucket value = sum of per-period rates inside the bucket
                (use when converting hourly HL rates to an 8h-equivalent).
      - "mean": bucket value = arithmetic mean
                (use when comparing two rate series as averages).

    Buckets are aligned to UTC midnight; the label is the bucket-start time.
    """
    if bucket_hours <= 0:
        raise ValueError("bucket_hours must be positive")
    if mode not in ("sum", "mean"):
        raise ValueError(f"unknown mode: {mode}")

    buckets: dict[datetime, list[float]] = {}
    for ts, rate in points:
        bucket = _align_bucket(ts, bucket_hours)
        buckets.setdefault(bucket, []).append(rate)

    out: list[tuple[datetime, float]] = []
    for bucket in sorted(buckets.keys()):
        values = buckets[bucket]
        if mode == "sum":
            out.append((bucket, sum(values)))
        else:
            out.append((bucket, sum(values) / len(values)))
    return out


def _align_bucket(ts: datetime, bucket_hours: int) -> datetime:
    """Align `ts` down to the start of its `bucket_hours` bucket (UTC midnight-anchored)."""
    day_start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    hours_since_midnight = ts.hour + ts.minute / 60 + ts.second / 3600
    bucket_index = int(hours_since_midnight // bucket_hours)
    return day_start + timedelta(hours=bucket_index * bucket_hours)


def mean_rate(rates: list[float]) -> float:
    """Safe mean returning 0.0 on empty input."""
    if not rates:
        return 0.0
    return sum(rates) / len(rates)


def geometric_annualized(rate: float, interval_hours: int) -> float:
    """
    Compounded annualized rate. Used sparingly — the dashboard primarily shows
    simple-APR numbers, but compounding matters for long-hold simulations.
    """
    if interval_hours <= 0:
        raise ValueError("interval_hours must be positive")
    periods_per_year = HOURS_PER_YEAR / interval_hours
    try:
        return math.pow(1.0 + rate, periods_per_year) - 1.0
    except (OverflowError, ValueError):
        return float("nan")
