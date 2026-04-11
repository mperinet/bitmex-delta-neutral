"""
Streamlit monitoring dashboard — read-only.

Polls the DB every 5 seconds. No order execution.

Pages:
  - Live Positions: open positions, leg details, unrealised PnL
  - Funding Rates: current and historical funding for tracked perps
  - Strategy PnL: closed trade performance by strategy
  - Risk: delta, margin, recent snapshots
"""

import asyncio
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Make engine importable from dashboard/
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.db.models import init_db

st.set_page_config(
    page_title="BitMEX Delta-Neutral Engine",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------ #
# DB init (async → sync bridge for Streamlit)                         #
# ------------------------------------------------------------------ #

import concurrent.futures

_db_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def run_async(coro):
    """Run an async coroutine safely regardless of Streamlit's own event loop."""
    future = _db_pool.submit(asyncio.run, coro)
    return future.result()


def _load_db_url() -> str:
    try:
        import tomli as tomllib
    except ImportError:
        import tomllib
    config_path = Path(__file__).parent.parent / "config" / "settings.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    return config["database"]["url"]


@st.cache_resource
def _init_db():
    run_async(init_db(_load_db_url()))


_init_db()


# ------------------------------------------------------------------ #
# Data loaders                                                         #
# ------------------------------------------------------------------ #

@st.cache_data(ttl=5)
def load_positions():
    from engine.db import repository
    positions = run_async(repository.get_open_positions())
    if not positions:
        return pd.DataFrame()
    return pd.DataFrame([{
        "id": p.id,
        "strategy": p.strategy,
        "state": p.state,
        "leg_a": f"{p.leg_a_side} {p.leg_a_qty or 0:.0f} {p.leg_a_symbol or ''}",
        "leg_b": f"{p.leg_b_side} {p.leg_b_qty or 0:.4f} {p.leg_b_symbol or ''}",
        "locked_basis": f"{(p.locked_basis or 0) * 100:.2f}%" if p.locked_basis else "—",
        "funding_paid": f"{(p.cumulative_funding_paid or 0) * 100:.4f}%",
        "unrealised_pnl": p.unrealised_pnl or 0,
        "slices": f"{p.entry_slices_done or 0}/{p.entry_slices_total or 5}",
        "opened": p.opened_at,
    } for p in positions])


@st.cache_data(ttl=5)
def load_funding_rates(symbol: str, limit: int = 90):
    from engine.db import repository
    rates = run_async(repository.get_recent_funding(symbol, limit=limit))
    if not rates:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "timestamp": r.timestamp,
        "rate": r.funding_rate * 100,        # convert to %
        "annual_rate": r.funding_rate_annual * 100,
    } for r in rates])
    return df.sort_values("timestamp")


@st.cache_data(ttl=30)
def load_risk_snapshots(limit: int = 50):
    from sqlalchemy import select
    from engine.db.models import RiskSnapshot, get_session

    async def _fetch():
        async with get_session() as session:
            result = await session.execute(
                select(RiskSnapshot).order_by(RiskSnapshot.timestamp.desc()).limit(limit)
            )
            return list(result.scalars().all())

    snaps = run_async(_fetch())
    if not snaps:
        return pd.DataFrame()
    return pd.DataFrame([{
        "timestamp": s.timestamp,
        "delta_pct_nav": (s.net_delta_pct_nav or 0) * 100,
        "margin_utilization": (s.margin_utilization or 0) * 100,
        "nav_usd": s.nav or 0,
    } for s in snaps]).sort_values("timestamp")


# ------------------------------------------------------------------ #
# Sidebar                                                              #
# ------------------------------------------------------------------ #

st.sidebar.title("BitMEX Delta-Neutral")
st.sidebar.caption("Read-only monitoring dashboard")

page = st.sidebar.radio("View", [
    "Live Positions",
    "Funding Rates",
    "Risk",
    "Smoke Test",
])

auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
if auto_refresh:
    import time
    st.sidebar.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")

# ------------------------------------------------------------------ #
# Pages                                                                #
# ------------------------------------------------------------------ #

if page == "Live Positions":
    st.title("Live Positions")
    positions_df = load_positions()
    if positions_df.empty:
        st.info("No open positions. Engine may not be running or no signals yet.")
    else:
        st.dataframe(positions_df, use_container_width=True)

        # PnL summary
        total_pnl = positions_df["unrealised_pnl"].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Open Positions", len(positions_df))
        col2.metric("Total Unrealised PnL (BTC)", f"{total_pnl:.6f}")
        col3.metric(
            "Strategies Active",
            ", ".join(positions_df["strategy"].unique()),
        )

elif page == "Funding Rates":
    st.title("Funding Rates")
    symbol = st.selectbox("Symbol", ["BTC/USD:BTC", "ETH/USD:BTC"])
    df = load_funding_rates(symbol)

    if df.empty:
        st.info(f"No funding rate data for {symbol}. Run the backfill script or wait for the engine to collect data.")
    else:
        latest_rate = df["rate"].iloc[-1]
        avg_rate = df["rate"].mean()
        col1, col2, col3 = st.columns(3)
        col1.metric("Latest Rate", f"{latest_rate:.4f}%/8h")
        col2.metric("90-period Average", f"{avg_rate:.4f}%/8h")
        col3.metric("Annualised (avg)", f"{avg_rate * 3 * 365:.1f}%")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["timestamp"], y=df["rate"],
            name="Funding Rate %/8h",
            line=dict(color="#00cc96"),
        ))
        fig.add_hline(y=0.01, line_dash="dash", line_color="orange", annotation_text="Baseline (0.01%)")
        fig.add_hline(y=0.03, line_dash="dash", line_color="red", annotation_text="Entry threshold (0.03%)")
        fig.update_layout(title=f"{symbol} Funding Rate History", xaxis_title="Time", yaxis_title="Rate (%/8h)")
        st.plotly_chart(fig, use_container_width=True)

elif page == "Risk":
    st.title("Risk")
    snaps = load_risk_snapshots()

    if snaps.empty:
        st.info("No risk snapshots yet. Engine saves one every 5 minutes.")
    else:
        latest = snaps.iloc[-1]
        col1, col2, col3 = st.columns(3)
        col1.metric("Net Delta % NAV", f"{latest['delta_pct_nav']:.3f}%",
                    delta_color="inverse" if abs(latest["delta_pct_nav"]) > 0.3 else "normal")
        col2.metric("Margin Utilization", f"{latest['margin_utilization']:.1f}%",
                    delta_color="inverse" if latest["margin_utilization"] > 40 else "normal")
        col3.metric("NAV (USD)", f"${latest['nav_usd']:,.0f}")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=snaps["timestamp"], y=snaps["margin_utilization"],
            name="Margin Utilization %",
            fill="tozeroy",
            line=dict(color="#636efa"),
        ))
        fig.add_hline(y=40, line_dash="dash", line_color="orange", annotation_text="Warning (40%)")
        fig.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="Hard stop (50%)")
        fig.update_layout(title="Margin Utilization Over Time", yaxis_range=[0, 60])
        st.plotly_chart(fig, use_container_width=True)

elif page == "Smoke Test":
    st.title("Smoke Test")
    st.caption(
        "Queues a one-shot integration test: enters a minimal 2-leg position "
        "(short nearest BTC future + long XBTUSD perp, 100 USD) on testnet, "
        "then exits on the next engine loop tick (~30s). "
        "Results appear under Recent runs below and in Live Positions while active."
    )

    from engine.db import repository

    # Load recent signals (no cache — we want live status)
    signals = run_async(repository.get_recent_control_signals("smoke_test", limit=5))
    pending = [s for s in signals if s.consumed_at is None]

    if pending:
        st.warning(
            "Smoke test pending — the engine will pick it up on its next loop tick (~30s). "
            "You can queue another once the engine consumes this one."
        )
        st.button("Run Smoke Test", disabled=True)
    else:
        if st.button("Run Smoke Test"):
            run_async(repository.create_control_signal("smoke_test"))
            st.success("Signal queued. Engine will pick it up within 30s.")
            st.rerun()

    # Recent smoke test positions (all states, newest first)
    positions = run_async(repository.get_positions_by_strategy("smoke_test", limit=5))
    if positions:
        st.subheader("Recent runs")
        st.dataframe(
            pd.DataFrame([{
                "id": p.id,
                "state": p.state,
                "leg_a": f"{p.leg_a_side} {p.leg_a_qty or 0:.0f} {p.leg_a_symbol or ''}",
                "leg_b": f"{p.leg_b_side} {p.leg_b_qty or 0:.0f} {p.leg_b_symbol or ''}",
                "slices": f"{p.entry_slices_done or 0}/{p.entry_slices_total or 1}",
                "realised_pnl": p.realised_pnl or 0,
                "opened": p.opened_at,
                "closed": p.closed_at or "—",
            } for p in positions]),
            use_container_width=True,
        )

    # Signal history
    if signals:
        st.subheader("Signal history")
        st.dataframe(
            pd.DataFrame([{
                "id": s.id,
                "created_at": s.created_at,
                "consumed_at": s.consumed_at,
                "status": "pending" if s.consumed_at is None else "consumed",
            } for s in signals]),
            use_container_width=True,
        )

# Auto-refresh
if auto_refresh:
    import time
    time.sleep(5)
    st.rerun()
