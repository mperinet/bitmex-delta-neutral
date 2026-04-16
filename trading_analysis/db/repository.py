"""
Data access layer for trading_analysis.db.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trading_analysis.db.models import ExecutionFee, FundingPayment, SyncCursor, WalletTransaction, get_session


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
    funding_rate: float | None = None,
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
            funding_rate=funding_rate,
        )
        .on_conflict_do_update(
            index_elements=["exec_id"],
            set_={"fee_amount": fee_amount, "fee_currency": fee_currency, "last_qty": last_qty, "funding_rate": funding_rate},
        )
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_funding_payments(
    symbol: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> list[FundingPayment]:
    query = select(FundingPayment).order_by(FundingPayment.timestamp.desc())
    if limit is not None:
        query = query.limit(limit)
    if symbol:
        query = query.where(FundingPayment.symbol == symbol)
    if since:
        # Strip tz info so SQLite string comparison works on naive UTC values
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(FundingPayment.timestamp >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(FundingPayment.timestamp <= _until)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_earliest_zero_funding_timestamp() -> datetime | None:
    """Return the timestamp of the oldest funding_payments row with fee_amount=0."""
    query = (
        select(func.min(FundingPayment.timestamp))
        .where(FundingPayment.fee_amount == 0)
    )
    async with get_session() as session:
        result = await session.execute(query)
        val = result.scalar_one_or_none()
        if val is None:
            return None
        return val if isinstance(val, datetime) else datetime.fromisoformat(str(val))


async def patch_zero_funding_amounts(
    updates: list[tuple[str, datetime, int, str]],
) -> int:
    """
    Bulk-patch funding_payments rows whose fee_amount is still 0.

    `updates` is a list of (symbol, timestamp, new_fee_amount, fee_currency).
    The timestamp match uses a ±30-second window to handle sub-second
    differences between the /execution and /user/walletHistory timestamps.

    Returns the number of rows actually updated.
    """
    from datetime import timedelta
    from sqlalchemy import update as sql_update

    updated = 0
    async with get_session() as session:
        for symbol, ts, amount, currency in updates:
            if amount == 0:
                continue
            ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
            low = ts_naive - timedelta(seconds=30)
            high = ts_naive + timedelta(seconds=30)
            stmt = (
                sql_update(FundingPayment)
                .where(FundingPayment.symbol == symbol)
                .where(FundingPayment.timestamp >= low)
                .where(FundingPayment.timestamp <= high)
                .where(FundingPayment.fee_amount == 0)
                .values(fee_amount=amount, fee_currency=currency)
            )
            result = await session.execute(stmt)
            updated += result.rowcount
        await session.commit()
    return updated


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
    realised_pnl: int = 0,
) -> bool:
    """Insert or update an execution fee. Returns True if a row was inserted or updated."""
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
            realised_pnl=realised_pnl,
        )
        .on_conflict_do_update(
            index_elements=["exec_id"],
            set_={"fee_amount": fee_amount, "realised_pnl": realised_pnl},
        )
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_execution_fees(
    symbol: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> list[ExecutionFee]:
    query = select(ExecutionFee).order_by(ExecutionFee.timestamp.desc())
    if limit is not None:
        query = query.limit(limit)
    if symbol:
        query = query.where(ExecutionFee.symbol == symbol)
    if since:
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(ExecutionFee.timestamp >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(ExecutionFee.timestamp <= _until)
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
# Wallet transactions (deposits / withdrawals)
# ---------------------------------------------------------------------------

_WALLET_TX_TYPES = {"Deposit", "Withdrawal", "Conversion", "Transfer"}


async def upsert_wallet_transaction(
    transact_id: str,
    transact_type: str,
    currency: str,
    amount: int,
    fee: int,
    address: str | None,
    tx_hash: str | None,
    transact_time: datetime,
    wallet_balance: int | None,
) -> bool:
    """Insert or ignore a wallet transaction. Returns True if newly inserted."""
    stmt = (
        sqlite_insert(WalletTransaction)
        .values(
            transact_id=transact_id,
            transact_type=transact_type,
            currency=currency,
            amount=amount,
            fee=fee,
            address=address,
            tx_hash=tx_hash,
            transact_time=transact_time,
            wallet_balance=wallet_balance,
        )
        .on_conflict_do_nothing(index_elements=["transact_id"])
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_wallet_transactions(
    since: datetime | None = None,
    until: datetime | None = None,
    types: set[str] | None = None,
) -> list[WalletTransaction]:
    query = select(WalletTransaction).order_by(WalletTransaction.transact_time.asc())
    if types:
        query = query.where(WalletTransaction.transact_type.in_(types))
    if since:
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(WalletTransaction.transact_time >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(WalletTransaction.transact_time <= _until)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_latest_wallet_transact_id(currency: str) -> str | None:
    """Return the transact_id of the most recent wallet transaction for a currency."""
    query = (
        select(WalletTransaction.transact_id)
        .where(WalletTransaction.currency == currency)
        .order_by(WalletTransaction.transact_time.desc())
        .limit(1)
    )
    async with get_session() as session:
        result = await session.execute(query)
        return result.scalar_one_or_none()


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


async def delete_sync_cursor(data_type: str) -> None:
    """Delete a sync cursor so the next sync starts from the beginning (epoch)."""
    from sqlalchemy import delete as sql_delete
    async with get_session() as session:
        await session.execute(
            sql_delete(SyncCursor).where(SyncCursor.data_type == data_type)
        )
        await session.commit()
