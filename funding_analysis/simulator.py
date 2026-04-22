"""
Historical delta-neutral payout simulator.

Given a historical window, a venue (HL/BitMEX), an asset, a side, and a
USD notional, compute:
- Cumulative funding received/paid on the perp leg.
- Cumulative Binance USD margin borrow cost for funding the hedge leg.
- Net = funding - borrow cost.
- A time series for plotting.

Caveats (surfaced in the UI):
- Constant notional (no rebalancing, no mark-to-market drift).
- Hedge leg is treated as perfectly delta-neutral — spot price drift ignored.
- Execution costs on HL/BitMEX are not subtracted.
- Binance margin rates before the earliest stored timestamp fall back to the
  earliest known rate (flat extrapolation).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from funding_analysis.db import repository
from funding_analysis.db.models import FundingRatePoint

Side = Literal["short", "long"]
Venue = Literal["bitmex", "hyperliquid"]


@dataclass
class SimPoint:
    timestamp: datetime
    funding_cashflow: float       # USD, at this period boundary (+ received, - paid)
    borrow_cashflow: float        # USD (always <= 0: it's a cost)
    cum_funding: float
    cum_borrow: float
    cum_net: float


@dataclass
class SimResult:
    points: list[SimPoint]
    total_funding: float
    total_borrow: float
    net: float
    annualized_net_apr: float
    duration_days: float
    sign: int                     # +1 short (receives positive funding), -1 long
    funding_periods: int
    borrow_hours: int

    def to_dict(self) -> dict:
        return {
            "total_funding": self.total_funding,
            "total_borrow": self.total_borrow,
            "net": self.net,
            "annualized_net_apr": self.annualized_net_apr,
            "duration_days": self.duration_days,
            "funding_periods": self.funding_periods,
            "borrow_hours": self.borrow_hours,
        }


def _sign_for_side(side: Side) -> int:
    # Short receives positive funding → sign +1.
    # Long pays positive funding → sign -1.
    return 1 if side == "short" else -1


async def simulate_payout(
    asset: str,
    venue: Venue,
    side: Side,
    notional_usd: float,
    start: datetime,
    end: datetime,
    hedge_currency: str = "USDT",
) -> SimResult:
    if notional_usd <= 0:
        raise ValueError("notional_usd must be positive")
    if end <= start:
        raise ValueError("end must be after start")

    funding_rows = await repository.get_funding_rates(
        exchange=venue,
        asset=asset,
        since=start,
        until=end,
    )
    # Binance publishes margin rates only when they change, so we may see 0
    # updates inside a short window even though a rate is in force. Widen the
    # borrow-rate query to include the most recent prior rate; the simulator
    # uses it as a flat-extrapolated starting value.
    borrow_rows = await repository.get_binance_margin_rates(
        asset=hedge_currency,
        since=start - timedelta(days=365),
        until=end,
    )

    sign = _sign_for_side(side)

    # ---------- Build funding cashflows keyed by timestamp ----------
    funding_events: list[tuple[datetime, float]] = []
    for r in funding_rows:
        ts = _aware(r.timestamp)
        funding_events.append((ts, notional_usd * r.funding_rate * sign))

    # ---------- Build hourly borrow cost series over the window ----------
    # Binance publishes dailyInterestRate; hourly cost = notional * daily / 24.
    borrow_hourly = _hourly_borrow_cost_series(borrow_rows, start, end, notional_usd)

    # ---------- Interleave into one monotonic timeline ----------
    timeline: list[tuple[datetime, float, float]] = []
    # (ts, funding_cashflow, borrow_cashflow)
    for ts, cf in funding_events:
        timeline.append((ts, cf, 0.0))
    for ts, cf in borrow_hourly:
        timeline.append((ts, 0.0, cf))
    timeline.sort(key=lambda t: t[0])

    points: list[SimPoint] = []
    cum_f, cum_b = 0.0, 0.0
    for ts, f_cf, b_cf in timeline:
        cum_f += f_cf
        cum_b += b_cf
        points.append(
            SimPoint(
                timestamp=ts,
                funding_cashflow=f_cf,
                borrow_cashflow=b_cf,
                cum_funding=cum_f,
                cum_borrow=cum_b,
                cum_net=cum_f + cum_b,
            )
        )

    net = cum_f + cum_b
    duration_days = (end - start).total_seconds() / 86400.0
    if duration_days > 0 and notional_usd > 0:
        annualized_apr = net / notional_usd * (365.0 / duration_days)
    else:
        annualized_apr = 0.0

    return SimResult(
        points=points,
        total_funding=cum_f,
        total_borrow=cum_b,
        net=net,
        annualized_net_apr=annualized_apr,
        duration_days=duration_days,
        sign=sign,
        funding_periods=len(funding_events),
        borrow_hours=len(borrow_hourly),
    )


def _aware(ts: datetime) -> datetime:
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def _hourly_borrow_cost_series(
    borrow_rows: list,
    start: datetime,
    end: datetime,
    notional_usd: float,
) -> list[tuple[datetime, float]]:
    """
    Return an (hour_boundary, borrow_cost_usd) series across [start, end].

    Binance publishes a daily rate with (roughly) irregular timestamps. We
    step hour-by-hour and use the most recent rate-point ≤ current hour.
    Each hour is charged `notional * dailyRate / 24` as a negative cashflow.
    """
    if notional_usd <= 0 or not borrow_rows:
        return []

    # Sort rows ascending by timestamp (they should already be).
    sorted_rows = sorted(borrow_rows, key=lambda r: _aware(r.timestamp))

    start_aware = _aware(start)
    end_aware = _aware(end)
    out: list[tuple[datetime, float]] = []

    # Pointer into sorted_rows; advance as time moves forward.
    idx = 0
    # If the first row is after `start`, use it as a flat extrapolation
    # for the initial hours. If there are rows ≤ start, pick the latest of those.
    current_daily_rate: float | None = None
    for i, r in enumerate(sorted_rows):
        if _aware(r.timestamp) <= start_aware:
            current_daily_rate = float(r.daily_interest_rate)
            idx = i + 1
        else:
            break
    if current_daily_rate is None:
        current_daily_rate = float(sorted_rows[0].daily_interest_rate)

    t = start_aware.replace(minute=0, second=0, microsecond=0)
    if t < start_aware:
        t = t + timedelta(hours=1)

    while t < end_aware:
        # Advance rate pointer up to t.
        while idx < len(sorted_rows) and _aware(sorted_rows[idx].timestamp) <= t:
            current_daily_rate = float(sorted_rows[idx].daily_interest_rate)
            idx += 1
        hourly_cost = -notional_usd * current_daily_rate / 24.0
        out.append((t, hourly_cost))
        t = t + timedelta(hours=1)

    return out


def build_funding_apr_series(
    rows: list[FundingRatePoint],
) -> list[tuple[datetime, float]]:
    """Convert DB rows into (timestamp, annualized_apr) tuples."""
    from funding_analysis.normalize import to_annualized_apr

    return [
        (_aware(r.timestamp), to_annualized_apr(r.funding_rate, r.interval_hours))
        for r in rows
    ]
