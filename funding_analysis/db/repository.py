"""
Data access layer for funding_analysis.db.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from funding_analysis.db.models import ExecutionFee, FundingPayment, SyncCursor, get_session


# ---------------------------------------------------------------------------
# Funding payments
# ---------------------------------------------------------------------------


async def upsert_funding_payment(
    exec_id: str,
    symbol: str,
    timestamp: datetime,
    fee_amount: int,
    fee_currency: str,
    last_qty: float,
) -> bool:
    """Insert a funding payment. Returns True if a new row was inserted."""
    stmt = (
        sqlite_insert(FundingPayment)
        .values(
            exec_id=exec_id,
            symbol=symbol,
            timestamp=timestamp,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            last_qty=last_qty,
        )
        .on_conflict_do_nothing(index_elements=["exec_id"])
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_funding_payments(
    symbol: str | None = None,
    since: datetime | None = None,
    limit: int = 5000,
) -> list[FundingPayment]:
    query = select(FundingPayment).order_by(FundingPayment.timestamp.desc()).limit(limit)
    if symbol:
        query = query.where(FundingPayment.symbol == symbol)
    if since:
        query = query.where(FundingPayment.timestamp >= since)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_funding_symbols() -> list[str]:
    query = select(FundingPayment.symbol).distinct()
    async with get_session() as session:
        result = await session.execute(query)
        return sorted(result.scalars().all())


async def get_funding_totals() -> dict:
    """Return {symbol: {received_xbt, paid_xbt, net_xbt}} for XBt-denominated rows."""
    query = select(
        FundingPayment.symbol,
        FundingPayment.fee_currency,
        func.sum(FundingPayment.fee_amount).label("total"),
    ).group_by(FundingPayment.symbol, FundingPayment.fee_currency)
    async with get_session() as session:
        result = await session.execute(query)
        rows = result.all()

    totals: dict[str, dict] = {}
    for symbol, currency, total in rows:
        key = f"{symbol} ({currency})"
        divisor = 1e8 if currency == "XBt" else 1e6
        totals[key] = {
            "symbol": symbol,
            "currency": currency,
            "net": total / divisor,
            "received": max(0.0, total / divisor),
            "paid": min(0.0, total / divisor),
        }
    return totals


# ---------------------------------------------------------------------------
# Execution fees
# ---------------------------------------------------------------------------


async def upsert_execution_fee(
    exec_id: str,
    order_id: str | None,
    symbol: str,
    side: str | None,
    last_qty: float,
    last_px: float,
    fee_amount: int,
    fee_currency: str,
    timestamp: datetime,
) -> bool:
    """Insert an execution fee. Returns True if a new row was inserted."""
    stmt = (
        sqlite_insert(ExecutionFee)
        .values(
            exec_id=exec_id,
            order_id=order_id,
            symbol=symbol,
            side=side,
            last_qty=last_qty,
            last_px=last_px,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            timestamp=timestamp,
        )
        .on_conflict_do_nothing(index_elements=["exec_id"])
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_execution_fees(
    symbol: str | None = None,
    since: datetime | None = None,
    limit: int = 5000,
) -> list[ExecutionFee]:
    query = select(ExecutionFee).order_by(ExecutionFee.timestamp.desc()).limit(limit)
    if symbol:
        query = query.where(ExecutionFee.symbol == symbol)
    if since:
        query = query.where(ExecutionFee.timestamp >= since)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_execution_symbols() -> list[str]:
    query = select(ExecutionFee.symbol).distinct()
    async with get_session() as session:
        result = await session.execute(query)
        return sorted(result.scalars().all())


async def get_fee_totals() -> dict:
    """Return aggregated fees by symbol+currency."""
    query = select(
        ExecutionFee.symbol,
        ExecutionFee.fee_currency,
        func.sum(ExecutionFee.fee_amount).label("total"),
        func.count(ExecutionFee.id).label("count"),
    ).group_by(ExecutionFee.symbol, ExecutionFee.fee_currency)
    async with get_session() as session:
        result = await session.execute(query)
        rows = result.all()

    totals: dict[str, dict] = {}
    for symbol, currency, total, count in rows:
        key = f"{symbol} ({currency})"
        divisor = 1e8 if currency == "XBt" else 1e6
        totals[key] = {
            "symbol": symbol,
            "currency": currency,
            "net": total / divisor,
            "trade_count": count,
        }
    return totals


# ---------------------------------------------------------------------------
# Sync cursors
# ---------------------------------------------------------------------------


async def get_sync_cursor(data_type: str) -> SyncCursor | None:
    async with get_session() as session:
        result = await session.execute(
            select(SyncCursor).where(SyncCursor.data_type == data_type)
        )
        return result.scalar_one_or_none()


async def set_sync_cursor(data_type: str, last_synced_at: datetime, total_rows: int) -> None:
    stmt = (
        sqlite_insert(SyncCursor)
        .values(data_type=data_type, last_synced_at=last_synced_at, total_rows=total_rows)
        .on_conflict_do_update(
            index_elements=["data_type"],
            set_={"last_synced_at": last_synced_at, "total_rows": total_rows},
        )
    )
    async with get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def get_all_cursors() -> list[SyncCursor]:
    async with get_session() as session:
        result = await session.execute(select(SyncCursor))
        return list(result.scalars().all())
