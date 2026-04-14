"""
SQLAlchemy models for the funding analysis database.

Separate from the trading engine's trading.db — uses a dedicated
readonly API key pair and stores account-level funding/fee history.

Tables:
  funding_payments  — account-level funding settlements (execType=Funding)
  execution_fees    — trade execution fees (execType=Trade)
  sync_cursors      — tracks last sync timestamp per data type
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class FundingPayment(Base):
    """
    Account-level funding payment at each 8h settlement.

    Sourced from BitMEX /execution?filter={"execType":"Funding"}.
    fee_amount is in the smallest unit of fee_currency:
      XBt  → satoshis (÷ 1e8 = XBT)
      USDt → micro-USD (÷ 1e6 = USD)
    Positive = received, negative = paid.
    """

    __tablename__ = "funding_payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exec_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    fee_amount: Mapped[int] = mapped_column(Integer, nullable=False)  # raw satoshis
    fee_currency: Mapped[str] = mapped_column(String(8), nullable=False, default="XBt")
    last_qty: Mapped[float] = mapped_column(Float, default=0.0)  # position size at settlement

    __table_args__ = (
        Index("ix_fp_symbol_ts", "symbol", "timestamp"),
    )


class ExecutionFee(Base):
    """
    Fee charged (or rebate earned) on each trade execution.

    Sourced from BitMEX /execution?filter={"execType":"Trade"}.
    fee_amount semantics mirror FundingPayment: negative = fee paid, positive = maker rebate.
    """

    __tablename__ = "execution_fees"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exec_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    order_id: Mapped[str | None] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8))  # "Buy" | "Sell"
    last_qty: Mapped[float] = mapped_column(Float, default=0.0)
    last_px: Mapped[float] = mapped_column(Float, default=0.0)
    fee_amount: Mapped[int] = mapped_column(Integer, nullable=False)  # raw satoshis
    fee_currency: Mapped[str] = mapped_column(String(8), nullable=False, default="XBt")
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        Index("ix_ef_symbol_ts", "symbol", "timestamp"),
    )


class SyncCursor(Base):
    """Tracks the high-water mark for each incremental sync."""

    __tablename__ = "sync_cursors"

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
