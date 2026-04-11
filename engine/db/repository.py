"""
Data access layer. All DB reads/writes go through here.

Strategies are stateless in memory — they call repository methods to
read and write their state. On startup, position_tracker calls
get_open_positions() to recover state from the DB before strategies run.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from engine.db.models import (
    FundingRate,
    Instrument,
    Position,
    PositionState,
    RiskSnapshot,
    Trade,
    get_session,
)


# ---------------------------------------------------------------------------
# Instruments
# ---------------------------------------------------------------------------

async def upsert_instrument(data: dict) -> None:
    async with get_session() as session:
        result = await session.execute(
            select(Instrument).where(Instrument.symbol == data["symbol"])
        )
        inst = result.scalar_one_or_none()
        if inst is None:
            inst = Instrument(**data)
            session.add(inst)
        else:
            for k, v in data.items():
                setattr(inst, k, v)
        await session.commit()


async def get_instrument(symbol: str) -> Optional[Instrument]:
    async with get_session() as session:
        result = await session.execute(
            select(Instrument).where(Instrument.symbol == symbol)
        )
        return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Funding rates
# ---------------------------------------------------------------------------

async def insert_funding_rate(symbol: str, timestamp: datetime, rate: float) -> None:
    async with get_session() as session:
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=rate,
            funding_rate_daily=rate * 3,
            funding_rate_annual=rate * 3 * 365,
        )
        session.add(fr)
        try:
            await session.commit()
        except Exception:
            await session.rollback()


async def get_recent_funding(symbol: str, limit: int = 90) -> List[FundingRate]:
    """Return the last `limit` funding rate records (most recent first)."""
    async with get_session() as session:
        result = await session.execute(
            select(FundingRate)
            .where(FundingRate.symbol == symbol)
            .order_by(FundingRate.timestamp.desc())
            .limit(limit)
        )
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

async def create_position(strategy: str, **kwargs) -> Position:
    async with get_session() as session:
        pos = Position(strategy=strategy, state=PositionState.ENTERING, **kwargs)
        session.add(pos)
        await session.commit()
        await session.refresh(pos)
        return pos


async def update_position(position_id: int, **kwargs) -> None:
    kwargs["updated_at"] = datetime.utcnow()
    async with get_session() as session:
        await session.execute(
            update(Position)
            .where(Position.id == position_id)
            .values(**kwargs)
        )
        await session.commit()


async def get_open_positions(strategy: Optional[str] = None) -> List[Position]:
    """Return all positions that are not IDLE (entering, active, exiting)."""
    async with get_session() as session:
        q = select(Position).where(Position.state != PositionState.IDLE)
        if strategy:
            q = q.where(Position.strategy == strategy)
        result = await session.execute(q)
        return list(result.scalars().all())


async def get_position(position_id: int) -> Optional[Position]:
    async with get_session() as session:
        result = await session.execute(
            select(Position).where(Position.id == position_id)
        )
        return result.scalar_one_or_none()


async def close_position(position_id: int, realised_pnl: float) -> None:
    await update_position(
        position_id,
        state=PositionState.IDLE,
        closed_at=datetime.utcnow(),
        realised_pnl=realised_pnl,
    )


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

async def record_trade(
    position_id: int,
    strategy: str,
    leg: str,
    order_id: str,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    fee: float = 0.0,
    is_entry: bool = True,
) -> None:
    async with get_session() as session:
        trade = Trade(
            position_id=position_id,
            strategy=strategy,
            leg=leg,
            order_id=order_id,
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            fee=fee,
            is_entry=1 if is_entry else 0,
            timestamp=datetime.utcnow(),
        )
        session.add(trade)
        await session.commit()


# ---------------------------------------------------------------------------
# Risk snapshots
# ---------------------------------------------------------------------------

async def save_risk_snapshot(
    net_delta_usd: float,
    net_delta_pct_nav: float,
    margin_balance: float,
    margin_used: float,
    margin_utilization: float,
    nav: float,
    open_positions: int,
    notes: str = "",
) -> None:
    async with get_session() as session:
        snap = RiskSnapshot(
            net_delta_usd=net_delta_usd,
            net_delta_pct_nav=net_delta_pct_nav,
            margin_balance=margin_balance,
            margin_used=margin_used,
            margin_utilization=margin_utilization,
            nav=nav,
            open_positions=open_positions,
            notes=notes,
        )
        session.add(snap)
        await session.commit()
