"""
SQLAlchemy models for the trading engine.

Database: SQLite (dev) — PostgreSQL via config change:
  url = "postgresql+asyncpg://user:pass@host/db"

WAL mode is enabled at connect time to prevent "database locked"
errors under asyncio concurrent writes from order_manager,
position_tracker, and risk_guard.

Table overview:
  instruments     — live instrument metadata (mark price, funding rate, expiry)
  funding_rates   — historical funding rate snapshots (8h intervals)
  positions       — current open positions, one row per strategy+leg
  trades          — fill history (entry/exit, both legs)
  risk_snapshots  — periodic delta/margin snapshots for audit trail
  control_signals — one-shot commands written by the dashboard, consumed by the engine
"""

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class LegSide(StrEnum):
    LONG = "long"
    SHORT = "short"


class PositionState(StrEnum):
    IDLE = "idle"
    ENTERING = "entering"
    ACTIVE = "active"
    EXITING = "exiting"


class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    typ: Mapped[str | None] = mapped_column(String(16))
    mark_price: Mapped[float | None] = mapped_column(Float)
    fair_price: Mapped[float | None] = mapped_column(Float)
    funding_rate: Mapped[float | None] = mapped_column(Float)  # current 8h rate
    indicative_funding_rate: Mapped[float | None] = mapped_column(Float)
    next_funding_time: Mapped[datetime | None] = mapped_column(DateTime)
    expiry: Mapped[datetime | None] = mapped_column(DateTime)  # null for perps
    underlying: Mapped[str | None] = mapped_column(String(16))  # XBT, ETH ...
    quote: Mapped[str | None] = mapped_column(String(16))  # USD, USDT ...
    is_inverse: Mapped[int] = mapped_column(Integer, default=0)  # 1 for BTC-settled
    lot_size: Mapped[float | None] = mapped_column(Float)
    tick_size: Mapped[float | None] = mapped_column(Float)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class FundingRate(Base):
    __tablename__ = "funding_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    funding_rate: Mapped[float] = mapped_column(Float, nullable=False)
    funding_rate_daily: Mapped[float | None] = mapped_column(Float)  # rate * 3
    funding_rate_annual: Mapped[float | None] = mapped_column(Float)  # rate * 3 * 365

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp"),
        Index("ix_funding_symbol_ts", "symbol", "timestamp"),
    )


class Position(Base):
    """
    One row per open strategy position (both legs combined).

    State machine:
      IDLE → ENTERING → ACTIVE → EXITING → IDLE

    Progressive entry: entry_slices_done tracks how many slices have
    been successfully placed. Both legs grow simultaneously per slice.
    """

    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    strategy: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default=PositionState.IDLE)

    # Leg A (typically the derivative: future or perp short)
    leg_a_symbol: Mapped[str | None] = mapped_column(String(32))
    leg_a_side: Mapped[str | None] = mapped_column(String(8))  # long | short
    leg_a_qty: Mapped[float] = mapped_column(Float, default=0.0)  # filled so far
    leg_a_target_qty: Mapped[float | None] = mapped_column(Float)  # total intended
    leg_a_avg_entry: Mapped[float | None] = mapped_column(Float)

    # Leg B (hedge leg: perp long or spot long)
    leg_b_symbol: Mapped[str | None] = mapped_column(String(32))
    leg_b_side: Mapped[str | None] = mapped_column(String(8))
    leg_b_qty: Mapped[float] = mapped_column(Float, default=0.0)
    leg_b_target_qty: Mapped[float | None] = mapped_column(Float)
    leg_b_avg_entry: Mapped[float | None] = mapped_column(Float)

    # Carry trade specific
    locked_basis: Mapped[float | None] = mapped_column(Float)  # annualised basis at entry
    cumulative_funding_paid: Mapped[float] = mapped_column(Float, default=0.0)
    expiry: Mapped[datetime | None] = mapped_column(DateTime)  # S1: future expiry

    # Progressive entry tracking
    entry_slices_total: Mapped[int] = mapped_column(Integer, default=5)
    entry_slices_done: Mapped[int] = mapped_column(Integer, default=0)

    # PnL
    unrealised_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    realised_pnl: Mapped[float] = mapped_column(Float, default=0.0)

    opened_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (Index("ix_positions_strategy_state", "strategy", "state"),)


class Trade(Base):
    """Fill record for every order that executes."""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[int | None] = mapped_column(Integer)  # FK to positions.id
    strategy: Mapped[str] = mapped_column(String(32), nullable=False)
    leg: Mapped[str | None] = mapped_column(String(8))  # a | b
    order_id: Mapped[str | None] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8))  # buy | sell
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    is_entry: Mapped[int] = mapped_column(Integer, default=1)  # 1=entry, 0=exit
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_trades_position", "position_id"),
        Index("ix_trades_strategy_ts", "strategy", "timestamp"),
    )


class RiskSnapshot(Base):
    """Periodic snapshot of risk metrics for audit trail."""

    __tablename__ = "risk_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    net_delta_usd: Mapped[float | None] = mapped_column(Float)
    net_delta_pct_nav: Mapped[float | None] = mapped_column(Float)
    margin_balance: Mapped[float | None] = mapped_column(Float)
    margin_used: Mapped[float | None] = mapped_column(Float)
    margin_utilization: Mapped[float | None] = mapped_column(Float)
    nav: Mapped[float | None] = mapped_column(Float)
    open_positions: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (Index("ix_risk_ts", "timestamp"),)


class ControlSignal(Base):
    """
    One-shot command written by the dashboard, consumed by the engine.

    Workflow:
      1. Dashboard writes a row with consumed_at=None.
      2. Engine checks for pending rows each loop tick.
      3. Engine sets consumed_at when it acts on the signal.

    Currently supported signals: "smoke_test"
    """

    __tablename__ = "control_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    signal: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=datetime.utcnow)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime)

    __table_args__ = (Index("ix_control_signals_pending", "signal", "consumed_at"),)


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
        # WAL mode prevents "database locked" under concurrent asyncio writers
        # (order_manager + position_tracker + risk_guard). Must run before DDL.
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
