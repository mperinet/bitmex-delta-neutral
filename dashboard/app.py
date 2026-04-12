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

import ccxt.async_support as ccxt
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


@st.cache_data(ttl=300)
def load_indicative_rates() -> dict:
    """
    Fetch predicted next funding rates from live BitMEX (public endpoint, no auth).
    Returns a dict keyed by both ccxt symbol and native BitMEX symbol so callers
    can look up regardless of which format the DB uses.
    """
    async def _fetch():
        exchange = ccxt.bitmex({"enableRateLimit": True})
        try:
            rates = await exchange.fetch_funding_rates()
        finally:
            await exchange.close()
        lookup = {}
        for ccxt_sym, data in rates.items():
            rate = data.get("nextFundingRate")
            lookup[ccxt_sym] = rate
            native = (data.get("info") or {}).get("symbol")
            if native:
                lookup[native] = rate
        return lookup

    return run_async(_fetch())


@st.cache_data(ttl=30)
def load_funding_summary():
    from engine.db import repository
    rows = run_async(repository.get_funding_summary())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for col in ("last_rate", "avg_10d"):
        df[col] = df[col] * 100  # → %
    df.drop(columns=["predicted_rate"], inplace=True)
    return df


@st.cache_data(ttl=60)
def load_funding_symbols() -> list:
    from engine.db import repository
    symbols = run_async(repository.get_funding_symbols())
    return symbols or ["BTC/USD:BTC", "ETH/USD:BTC"]


@st.cache_data(ttl=60)
def load_daily_funding_avg(symbol: str, days: int = 90):
    from engine.db import repository
    # Fetch enough 8h records to cover `days` of history (3 records/day)
    rates = run_async(repository.get_recent_funding(symbol, limit=days * 3))
    if not rates:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "date": r.timestamp.date(),
        "rate": r.funding_rate * 100,  # %/8h
    } for r in rates])
    daily = df.groupby("date")["rate"].mean().reset_index()
    daily.columns = ["date", "avg_rate_pct"]
    return daily.sort_values("date")


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
    "Delta Check",
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
        st.dataframe(positions_df, width="stretch")

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

    summary = load_funding_summary()
    indicative = load_indicative_rates()  # live from BitMEX public API

    if not summary.empty:
        st.subheader("All symbols")

        def _fmt(v):
            return f"{v:.4f}%" if pd.notna(v) and v is not None else "—"

        def _predicted(sym):
            rate = indicative.get(sym)
            if rate is None:
                return "—"
            pct = rate * 100
            ann = rate * 3 * 365 * 100
            return f"{pct:.4f}% ({ann:.0f}% ann.)"

        display = pd.DataFrame({
            "Symbol": summary["symbol"],
            "Last rate (/8h)": summary["last_rate"].map(_fmt),
            "Predicted next (/8h)": summary["symbol"].map(_predicted),
            "10d daily avg (/8h)": summary["avg_10d"].map(_fmt),
        })
        st.dataframe(display, width="stretch", hide_index=True)

    st.subheader("Daily average")
    symbol = st.selectbox("Symbol", load_funding_symbols())
    days = st.slider("Days of history", min_value=7, max_value=180, value=90, step=7)
    daily = load_daily_funding_avg(symbol, days=days)

    if daily.empty:
        st.info(f"No funding rate data for {symbol}. Run the backfill script or wait for the engine to collect data.")
    else:
        overall_avg = daily["avg_rate_pct"].mean()
        col1, col2, col3 = st.columns(3)
        col1.metric("Days shown", len(daily))
        col2.metric("Overall avg rate", f"{overall_avg:.4f}%/8h")
        col3.metric("Annualised (avg)", f"{overall_avg * 3 * 365:.1f}%")

        daily["annualised"] = daily["avg_rate_pct"] * 3 * 365

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=daily["date"],
            y=daily["avg_rate_pct"],
            name="Daily avg funding %/8h",
            marker_color=["#ef553b" if v < 0 else "#00cc96" for v in daily["avg_rate_pct"]],
            customdata=daily["annualised"],
            hovertemplate="%{x}<br>Annualised: %{customdata:.1f}%<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="solid", line_color="white", line_width=0.5)
        fig.add_hline(y=0.01, line_dash="dash", line_color="orange", annotation_text="Baseline (0.01%)")
        fig.add_hline(y=0.03, line_dash="dash", line_color="red", annotation_text="Entry threshold (0.03%)")
        fig.update_layout(
            title=f"{symbol} — Daily Average Funding Rate",
            xaxis_title="Date",
            yaxis_title="Avg rate (%/8h)",
            yaxis_range=[daily["avg_rate_pct"].min() * 1.1 if daily["avg_rate_pct"].min() < 0 else -0.05, 0.5],
            bargap=0.2,
        )
        st.plotly_chart(fig, width="stretch")

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
        st.plotly_chart(fig, width="stretch")

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
            width="stretch",
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
            width="stretch",
        )

elif page == "Delta Check":
    st.title("Delta Balance Check")
    st.caption(
        "Enters a minimal short XBTUSD perp + long XBT_USDT spot position "
        "(inverse + spot legs), reads the net delta from the position tracker "
        "while ACTIVE, then exits. Verifies that the hedge ratio is balanced "
        "and that the delta guard correctly converts the spot leg from BTC to USD."
    )

    from engine.db import repository

    signals = run_async(repository.get_recent_control_signals("delta_check", limit=5))
    pending = [s for s in signals if s.consumed_at is None]

    if pending:
        st.warning(
            "Delta check pending — the engine will pick it up on its next loop tick (~30s)."
        )
        st.button("Run Delta Check", disabled=True)
    else:
        if st.button("Run Delta Check"):
            run_async(repository.create_control_signal("delta_check"))
            st.success("Signal queued. Engine will pick it up within 30s.")
            st.rerun()

    # Recent delta check positions
    positions = run_async(repository.get_positions_by_strategy("delta_check", limit=5))
    if positions:
        st.subheader("Recent runs")

        rows = []
        for p in positions:
            rows.append({
                "id": p.id,
                "state": p.state,
                "leg_a (perp)": f"{p.leg_a_side} {p.leg_a_qty or 0:.0f} {p.leg_a_symbol or ''}",
                "leg_b (spot)": f"{p.leg_b_side} {p.leg_b_qty or 0:.6f} {p.leg_b_symbol or ''}",
                "slices": f"{p.entry_slices_done or 0}/{p.entry_slices_total or 3}",
                "realised_pnl": p.realised_pnl or 0,
                "opened": p.opened_at,
                "closed": p.closed_at or "—",
            })

        st.dataframe(pd.DataFrame(rows), width="stretch")

        # Delta balance result — pulled from the most recently closed position's
        # structured log is not queryable here, so we show what we can from DB.
        latest = positions[0]
        if latest.state == "idle" and latest.closed_at:
            st.info(
                "Check engine logs for `delta_check_observation` to see the recorded "
                "net delta and balance verdict for the completed run."
            )

    if signals:
        st.subheader("Signal history")
        st.dataframe(
            pd.DataFrame([{
                "id": s.id,
                "created_at": s.created_at,
                "consumed_at": s.consumed_at,
                "status": "pending" if s.consumed_at is None else "consumed",
            } for s in signals]),
            width="stretch",
        )

# Auto-refresh
if auto_refresh:
    import time
    time.sleep(5)
    st.rerun()
