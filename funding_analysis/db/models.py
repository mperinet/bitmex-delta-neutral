"""
SQLAlchemy models for the funding analysis market-data database.

Market-scoped (funding rates + Binance borrow cost), not account-scoped:
- `asset_universe`            canonical asset ↔ per-venue symbol mapping
- `funding_rate_point`        one row per (exchange, venue_symbol, settlement)
- `predictive_funding_snapshot` live snapshots of current predicted rate
- `binance_margin_rate_point` hourly USD borrow rates on Binance margin
- `binance_borrow_history`    user's own borrow/interest events (authed)
- `sync_cursor`               high-water mark per data_type

Completely isolated from engine/ and trading_analysis/ — separate DB file,
own ccxt instances, own readonly keys (Binance only; HL and BitMEX public).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AssetUniverse(Base):
    """
    Canonical asset → per-venue symbol mapping.

    Discovered by `symbols.refresh_universe()`. A row is only created when
    the asset is listed on ≥ 2 venues (otherwise no cross-venue comparison
    is possible).
    """

    __tablename__ = "asset_universe"

    asset: Mapped[str] = mapped_column(String(16), primary_key=True)  # canonical, e.g. "BTC"
    bitmex_symbol: Mapped[str | None] = mapped_column(String(32))
    bitmex_contract_type: Mapped[str | None] = mapped_column(String(32))  # inverse_perp / linear_perp / quanto_future
    hyperliquid_name: Mapped[str | None] = mapped_column(String(32))
    binance_spot_symbol: Mapped[str | None] = mapped_column(String(32))
    discovered_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class FundingRatePoint(Base):
    """
    One settled funding rate for a (exchange, venue_symbol, timestamp).

    `funding_rate` is the per-period fraction as reported by the exchange
    (e.g. 0.0001 = 1bp/period). `interval_hours` is 1 for HyperLiquid,
    8 for BitMEX perps and Binance perps. APR is computed on read via
    `normalize.to_annualized_apr`.
    """

    __tablename__ = "funding_rate_point"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False)  # bitmex / hyperliquid / binance
    asset: Mapped[str] = mapped_column(String(16), nullable=False)
    venue_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    funding_rate: Mapped[float] = mapped_column(Float, nullable=False)
    interval_hours: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("exchange", "venue_symbol", "timestamp", name="uq_frp_exch_sym_ts"),
        Index("ix_frp_asset_ts", "asset", "timestamp"),
        Index("ix_frp_exch_ts", "exchange", "timestamp"),
    )


class PredictiveFundingSnapshot(Base):
    """Current-period predicted funding rate at a given capture time."""

    __tablename__ = "predictive_funding_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False)
    asset: Mapped[str] = mapped_column(String(16), nullable=False)
    venue_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    predicted_rate: Mapped[float] = mapped_column(Float, nullable=False)
    next_settlement_time: Mapped[datetime | None] = mapped_column(DateTime)
    captured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("exchange", "venue_symbol", "captured_at", name="uq_pfs_exch_sym_cap"),
        Index("ix_pfs_asset_cap", "asset", "captured_at"),
    )


class BinanceMarginRatePoint(Base):
    """
    Historical hourly USD borrow rate on Binance cross margin.

    Binance publishes a `dailyInterestRate` on `/sapi/v1/margin/interestRateHistory`;
    hourly = daily / 24. Kept as the daily figure in storage to match the API.
    """

    __tablename__ = "binance_margin_rate_point"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset: Mapped[str] = mapped_column(String(16), nullable=False)  # "USDT" / "USDC"
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    daily_interest_rate: Mapped[float] = mapped_column(Float, nullable=False)
    vip_level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("asset", "vip_level", "timestamp", name="uq_bmr_asset_vip_ts"),
        Index("ix_bmr_asset_ts", "asset", "timestamp"),
    )


class BinanceBorrowHistory(Base):
    """User's own margin borrow / repay / interest events (authed only)."""

    __tablename__ = "binance_borrow_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tx_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    asset: Mapped[str] = mapped_column(String(16), nullable=False)
    principal: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    interest: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    status: Mapped[str] = mapped_column(String(32), nullable=False)  # BORROWING / REPAYING / CONFIRMED / INTEREST

    __table_args__ = (
        Index("ix_bbh_asset_ts", "asset", "timestamp"),
    )


class SyncCursor(Base):
    """Tracks the high-water mark for each incremental sync data_type."""

    __tablename__ = "sync_cursor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    data_type: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    total_rows: Mapped[int] = mapped_column(Integer, default=0)


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def init_db(url: str) -> None:
    global _engine, _session_factory

    if _engine is not None:
        return  # already initialised

    _engine = create_async_engine(url, echo=False)

    async with _engine.begin() as conn:
        if "sqlite" in url:
            from sqlalchemy import text

            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA synchronous=NORMAL"))
        await conn.run_sync(Base.metadata.create_all)

    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


def get_session() -> AsyncSession:
    if _session_factory is None:
        raise RuntimeError("DB not initialised — call init_db() first")
    return _session_factory()
