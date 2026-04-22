"""Data access layer for funding_analysis.db."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select
from sqlalchemy import update as sql_update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from funding_analysis.db.models import (
    AssetUniverse,
    BinanceBorrowHistory,
    BinanceMarginRatePoint,
    FundingRatePoint,
    PredictiveFundingSnapshot,
    SyncCursor,
    get_session,
)

# ---------------------------------------------------------------------------
# Asset universe
# ---------------------------------------------------------------------------


async def upsert_asset(
    asset: str,
    bitmex_symbol: str | None,
    bitmex_contract_type: str | None,
    hyperliquid_name: str | None,
    binance_spot_symbol: str | None,
    discovered_at: datetime,
    active: bool = True,
) -> bool:
    stmt = (
        sqlite_insert(AssetUniverse)
        .values(
            asset=asset,
            bitmex_symbol=bitmex_symbol,
            bitmex_contract_type=bitmex_contract_type,
            hyperliquid_name=hyperliquid_name,
            binance_spot_symbol=binance_spot_symbol,
            discovered_at=discovered_at,
            active=active,
        )
        .on_conflict_do_update(
            index_elements=["asset"],
            set_={
                "bitmex_symbol": bitmex_symbol,
                "bitmex_contract_type": bitmex_contract_type,
                "hyperliquid_name": hyperliquid_name,
                "binance_spot_symbol": binance_spot_symbol,
                "active": active,
            },
        )
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def set_asset_inactive(asset: str) -> None:
    async with get_session() as session:
        await session.execute(
            sql_update(AssetUniverse).where(AssetUniverse.asset == asset).values(active=False)
        )
        await session.commit()


async def get_active_assets() -> list[AssetUniverse]:
    async with get_session() as session:
        result = await session.execute(
            select(AssetUniverse).where(AssetUniverse.active == True).order_by(AssetUniverse.asset)  # noqa: E712
        )
        return list(result.scalars().all())


async def get_all_assets() -> list[AssetUniverse]:
    async with get_session() as session:
        result = await session.execute(select(AssetUniverse).order_by(AssetUniverse.asset))
        return list(result.scalars().all())


async def get_asset(asset: str) -> AssetUniverse | None:
    async with get_session() as session:
        result = await session.execute(
            select(AssetUniverse).where(AssetUniverse.asset == asset)
        )
        return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Funding rate points
# ---------------------------------------------------------------------------


async def upsert_funding_rate(
    exchange: str,
    asset: str,
    venue_symbol: str,
    timestamp: datetime,
    funding_rate: float,
    interval_hours: int,
) -> bool:
    stmt = (
        sqlite_insert(FundingRatePoint)
        .values(
            exchange=exchange,
            asset=asset,
            venue_symbol=venue_symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            interval_hours=interval_hours,
        )
        .on_conflict_do_update(
            index_elements=["exchange", "venue_symbol", "timestamp"],
            set_={"funding_rate": funding_rate, "interval_hours": interval_hours, "asset": asset},
        )
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def bulk_upsert_funding_rates(rows: list[dict]) -> int:
    """Bulk upsert. `rows` each contain the same keys as upsert_funding_rate."""
    if not rows:
        return 0
    inserted = 0
    async with get_session() as session:
        for r in rows:
            stmt = (
                sqlite_insert(FundingRatePoint)
                .values(**r)
                .on_conflict_do_update(
                    index_elements=["exchange", "venue_symbol", "timestamp"],
                    set_={
                        "funding_rate": r["funding_rate"],
                        "interval_hours": r["interval_hours"],
                        "asset": r["asset"],
                    },
                )
            )
            result = await session.execute(stmt)
            if result.rowcount > 0:
                inserted += 1
        await session.commit()
    return inserted


async def get_funding_rates(
    exchange: str | None = None,
    asset: str | None = None,
    venue_symbol: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> list[FundingRatePoint]:
    query = select(FundingRatePoint).order_by(FundingRatePoint.timestamp.asc())
    if exchange:
        query = query.where(FundingRatePoint.exchange == exchange)
    if asset:
        query = query.where(FundingRatePoint.asset == asset)
    if venue_symbol:
        query = query.where(FundingRatePoint.venue_symbol == venue_symbol)
    if since:
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(FundingRatePoint.timestamp >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(FundingRatePoint.timestamp <= _until)
    if limit:
        query = query.limit(limit)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_latest_funding_timestamp(exchange: str, venue_symbol: str) -> datetime | None:
    query = (
        select(func.max(FundingRatePoint.timestamp))
        .where(FundingRatePoint.exchange == exchange)
        .where(FundingRatePoint.venue_symbol == venue_symbol)
    )
    async with get_session() as session:
        result = await session.execute(query)
        val = result.scalar_one_or_none()
        if val is None:
            return None
        return val if isinstance(val, datetime) else datetime.fromisoformat(str(val))


# ---------------------------------------------------------------------------
# Predictive funding
# ---------------------------------------------------------------------------


async def upsert_predictive_snapshot(
    exchange: str,
    asset: str,
    venue_symbol: str,
    predicted_rate: float,
    next_settlement_time: datetime | None,
    captured_at: datetime,
) -> None:
    stmt = (
        sqlite_insert(PredictiveFundingSnapshot)
        .values(
            exchange=exchange,
            asset=asset,
            venue_symbol=venue_symbol,
            predicted_rate=predicted_rate,
            next_settlement_time=next_settlement_time,
            captured_at=captured_at,
        )
        .on_conflict_do_nothing(
            index_elements=["exchange", "venue_symbol", "captured_at"]
        )
    )
    async with get_session() as session:
        await session.execute(stmt)
        await session.commit()


async def get_latest_predictive(asset: str | None = None) -> list[PredictiveFundingSnapshot]:
    """Most recent predictive snapshot per (exchange, venue_symbol)."""
    # SQLite-friendly: subquery for max captured_at per (exchange, venue_symbol).
    sub = (
        select(
            PredictiveFundingSnapshot.exchange,
            PredictiveFundingSnapshot.venue_symbol,
            func.max(PredictiveFundingSnapshot.captured_at).label("max_cap"),
        )
        .group_by(PredictiveFundingSnapshot.exchange, PredictiveFundingSnapshot.venue_symbol)
        .subquery()
    )
    query = select(PredictiveFundingSnapshot).join(
        sub,
        (PredictiveFundingSnapshot.exchange == sub.c.exchange)
        & (PredictiveFundingSnapshot.venue_symbol == sub.c.venue_symbol)
        & (PredictiveFundingSnapshot.captured_at == sub.c.max_cap),
    )
    if asset:
        query = query.where(PredictiveFundingSnapshot.asset == asset)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Binance margin rates
# ---------------------------------------------------------------------------


async def upsert_binance_margin_rate(
    asset: str,
    timestamp: datetime,
    daily_interest_rate: float,
    vip_level: int = 0,
) -> bool:
    stmt = (
        sqlite_insert(BinanceMarginRatePoint)
        .values(
            asset=asset,
            timestamp=timestamp,
            daily_interest_rate=daily_interest_rate,
            vip_level=vip_level,
        )
        .on_conflict_do_update(
            index_elements=["asset", "vip_level", "timestamp"],
            set_={"daily_interest_rate": daily_interest_rate},
        )
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_binance_margin_rates(
    asset: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    vip_level: int = 0,
) -> list[BinanceMarginRatePoint]:
    query = (
        select(BinanceMarginRatePoint)
        .where(BinanceMarginRatePoint.vip_level == vip_level)
        .order_by(BinanceMarginRatePoint.timestamp.asc())
    )
    if asset:
        query = query.where(BinanceMarginRatePoint.asset == asset)
    if since:
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(BinanceMarginRatePoint.timestamp >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(BinanceMarginRatePoint.timestamp <= _until)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_latest_binance_margin_timestamp(asset: str, vip_level: int = 0) -> datetime | None:
    query = (
        select(func.max(BinanceMarginRatePoint.timestamp))
        .where(BinanceMarginRatePoint.asset == asset)
        .where(BinanceMarginRatePoint.vip_level == vip_level)
    )
    async with get_session() as session:
        result = await session.execute(query)
        val = result.scalar_one_or_none()
        if val is None:
            return None
        return val if isinstance(val, datetime) else datetime.fromisoformat(str(val))


# ---------------------------------------------------------------------------
# Binance borrow history (authed)
# ---------------------------------------------------------------------------


async def upsert_borrow_event(
    tx_id: str,
    timestamp: datetime,
    asset: str,
    principal: float,
    interest: float,
    status: str,
) -> bool:
    stmt = (
        sqlite_insert(BinanceBorrowHistory)
        .values(
            tx_id=tx_id,
            timestamp=timestamp,
            asset=asset,
            principal=principal,
            interest=interest,
            status=status,
        )
        .on_conflict_do_nothing(index_elements=["tx_id"])
    )
    async with get_session() as session:
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


async def get_borrow_history(
    asset: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[BinanceBorrowHistory]:
    query = select(BinanceBorrowHistory).order_by(BinanceBorrowHistory.timestamp.desc())
    if asset:
        query = query.where(BinanceBorrowHistory.asset == asset)
    if since:
        _since = since.replace(tzinfo=None) if since.tzinfo else since
        query = query.where(BinanceBorrowHistory.timestamp >= _since)
    if until:
        _until = until.replace(tzinfo=None) if until.tzinfo else until
        query = query.where(BinanceBorrowHistory.timestamp <= _until)
    async with get_session() as session:
        result = await session.execute(query)
        return list(result.scalars().all())


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
    async with get_session() as session:
        await session.execute(sql_delete(SyncCursor).where(SyncCursor.data_type == data_type))
        await session.commit()
