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
"""

from datetime import datetime
from enum import Enum

from sqlalchemy import (
    Column, DateTime, Float, Index, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


class LegSide(str, Enum):
    LONG = "long"
    SHORT = "short"


class PositionState(str, Enum):
    IDLE = "idle"
    ENTERING = "entering"
    ACTIVE = "active"
    EXITING = "exiting"


class Instrument(Base):
    __tablename__ = "instruments"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(32), nullable=False, unique=True)
    typ = Column(String(16))              # FFWCSX, FFCCSX, IFXXXP ...
    mark_price = Column(Float)
    fair_price = Column(Float)
    funding_rate = Column(Float)          # current 8h rate
    indicative_funding_rate = Column(Float)
    next_funding_time = Column(DateTime)
    expiry = Column(DateTime)             # null for perps
    underlying = Column(String(16))       # XBT, ETH ...
    quote = Column(String(16))            # USD, USDT ...
    is_inverse = Column(Integer, default=0)  # 1 for BTC-settled inverse contracts
    lot_size = Column(Float)
    tick_size = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FundingRate(Base):
    __tablename__ = "funding_rates"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(32), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    funding_rate = Column(Float, nullable=False)
    funding_rate_daily = Column(Float)     # rate * 3 (3 payments per day)
    funding_rate_annual = Column(Float)    # rate * 3 * 365

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

    id = Column(Integer, primary_key=True)
    strategy = Column(String(32), nullable=False)    # cash_and_carry | funding_harvest
    state = Column(String(16), nullable=False, default=PositionState.IDLE)

    # Leg A (typically the derivative: future or perp short)
    leg_a_symbol = Column(String(32))
    leg_a_side = Column(String(8))                   # long | short
    leg_a_qty = Column(Float, default=0.0)           # filled quantity so far
    leg_a_target_qty = Column(Float)                 # total intended quantity
    leg_a_avg_entry = Column(Float)                  # average entry price

    # Leg B (hedge leg: perp long or spot long)
    leg_b_symbol = Column(String(32))
    leg_b_side = Column(String(8))
    leg_b_qty = Column(Float, default=0.0)
    leg_b_target_qty = Column(Float)
    leg_b_avg_entry = Column(Float)

    # Carry trade specific
    locked_basis = Column(Float)                     # annualised basis at entry (S1)
    cumulative_funding_paid = Column(Float, default=0.0)
    expiry = Column(DateTime)                        # for S1: future expiry date

    # Progressive entry tracking
    entry_slices_total = Column(Integer, default=5)
    entry_slices_done = Column(Integer, default=0)

    # PnL
    unrealised_pnl = Column(Float, default=0.0)
    realised_pnl = Column(Float, default=0.0)

    opened_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_positions_strategy_state", "strategy", "state"),
    )


class Trade(Base):
    """Fill record for every order that executes."""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    position_id = Column(Integer)           # FK to positions.id
    strategy = Column(String(32), nullable=False)
    leg = Column(String(8))                 # a | b
    order_id = Column(String(64))
    symbol = Column(String(32), nullable=False)
    side = Column(String(8))                # buy | sell
    qty = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    is_entry = Column(Integer, default=1)   # 1=entry, 0=exit
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_trades_position", "position_id"),
        Index("ix_trades_strategy_ts", "strategy", "timestamp"),
    )


class RiskSnapshot(Base):
    """Periodic snapshot of risk metrics for audit trail."""
    __tablename__ = "risk_snapshots"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    net_delta_usd = Column(Float)
    net_delta_pct_nav = Column(Float)
    margin_balance = Column(Float)
    margin_used = Column(Float)
    margin_utilization = Column(Float)
    nav = Column(Float)
    open_positions = Column(Integer)
    notes = Column(Text)

    __table_args__ = (
        Index("ix_risk_ts", "timestamp"),
    )


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_session_factory: sessionmaker | None = None


async def init_db(url: str) -> AsyncSession:
    global _engine, _session_factory

    if _engine is not None:
        return _session_factory  # already initialised

    _engine = create_async_engine(url, echo=False)

    async with _engine.begin() as conn:
        # WAL mode prevents "database locked" under concurrent asyncio writers
        # (order_manager + position_tracker + risk_guard). Must run before DDL.
        if "sqlite" in url:
            from sqlalchemy import text
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA synchronous=NORMAL"))
        await conn.run_sync(Base.metadata.create_all)

    _session_factory = sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False
    )
    return _session_factory


def get_session() -> AsyncSession:
    if _session_factory is None:
        raise RuntimeError("DB not initialised — call init_db() first")
    return _session_factory()
