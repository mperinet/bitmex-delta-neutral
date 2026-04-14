"""
Funding Analysis Dashboard — read-only.

Fetches account-level funding payments and execution fees from BitMEX
using dedicated readonly API keys, stored in a separate SQLite database.

Startup: reconciles against BitMEX (incremental sync) before rendering.
Port:    8502 (run via `make funding-analysis`)
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import sys
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from funding_analysis.db.models import init_db
from funding_analysis.exchange import FundingAnalysisClient

# ------------------------------------------------------------------ #
# Config + env                                                         #
# ------------------------------------------------------------------ #

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "settings.toml"
_ENV_PATH = Path(__file__).parent.parent / "config" / ".env"

load_dotenv(_ENV_PATH)


def _load_config() -> dict:
    with open(_CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


_config = _load_config()
_fa_config = _config.get("funding_analysis", {})
_DB_URL = _fa_config.get("db_url", "sqlite+aiosqlite:///data/funding_analysis.db")
_TESTNET = _fa_config.get("testnet", True)

_API_KEY = os.environ.get("BITMEX_READONLY_API_KEY", "")
_API_SECRET = os.environ.get("BITMEX_READONLY_API_SECRET", "")

# ------------------------------------------------------------------ #
# Async bridge                                                         #
# ------------------------------------------------------------------ #

_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def run_async(coro):
    future = _pool.submit(asyncio.run, coro)
    return future.result()


# ------------------------------------------------------------------ #
# DB init (once per session)                                           #
# ------------------------------------------------------------------ #


@st.cache_resource
def _init_db():
    run_async(init_db(_DB_URL))


_init_db()

# ------------------------------------------------------------------ #
# Page config                                                          #
# ------------------------------------------------------------------ #

st.set_page_config(
    page_title="BitMEX Funding Analysis",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------ #
# Startup sync                                                         #
# ------------------------------------------------------------------ #

if not _API_KEY or not _API_SECRET:
    st.error(
        "Missing API credentials. Set **BITMEX_READONLY_API_KEY** and "
        "**BITMEX_READONLY_API_SECRET** in `config/.env`."
    )
    st.stop()

if "synced" not in st.session_state:
    with st.spinner("Syncing with BitMEX… (first run may take a moment)"):
        try:
            client = FundingAnalysisClient(
                api_key=_API_KEY,
                api_secret=_API_SECRET,
                testnet=_TESTNET,
            )
            from funding_analysis import sync

            stats = run_async(sync.run_sync(client))
            run_async(client.close())
            st.session_state["synced"] = True
            st.session_state["sync_stats"] = stats
        except Exception as e:
            st.error(f"Sync failed: {e}")
            st.session_state["synced"] = False
            st.session_state["sync_stats"] = {}

# ------------------------------------------------------------------ #
# Sidebar                                                              #
# ------------------------------------------------------------------ #

with st.sidebar:
    env_label = ":red[TESTNET]" if _TESTNET else ":green[LIVE]"
    st.markdown(f"**Environment:** {env_label}")
    st.divider()

    # Date range filter
    st.subheader("Filters")
    from datetime import date as _date

    _today = _date.today()
    _default_start = _today - timedelta(days=30)
    _date_range = st.date_input(
        "Date range",
        value=(_default_start, _today),
        max_value=_today,
    )
    if isinstance(_date_range, (list, tuple)) and len(_date_range) == 2:
        since = datetime(_date_range[0].year, _date_range[0].month, _date_range[0].day, tzinfo=timezone.utc)
        until = datetime(_date_range[1].year, _date_range[1].month, _date_range[1].day, 23, 59, 59, tzinfo=timezone.utc)
    else:
        since = None
        until = None

    # Symbol filter (populated from DB after first sync)
    from funding_analysis.db import repository

    all_symbols = run_async(repository.get_funding_symbols()) + run_async(
        repository.get_execution_symbols()
    )
    all_symbols = sorted(set(all_symbols))
    selected_symbols = st.multiselect("Symbols", all_symbols, default=all_symbols)

    st.divider()
    if st.button("Re-sync now"):
        del st.session_state["synced"]
        if "sync_stats" in st.session_state:
            del st.session_state["sync_stats"]
        st.rerun()

# ------------------------------------------------------------------ #
# Data loaders                                                         #
# ------------------------------------------------------------------ #


@st.cache_data(ttl=60)
def load_funding_df(since_iso: str | None, until_iso: str | None, symbols: tuple) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    rows = run_async(repository.get_funding_payments(since=since_dt, limit=10_000))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "symbol": r.symbol,
                "amount_xbt": r.fee_amount / (1e8 if r.fee_currency == "XBt" else 1e6),
                "currency": r.fee_currency,
                "position_qty": r.last_qty,
            }
            for r in rows
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if until_iso:
        until_dt = pd.Timestamp(datetime.fromisoformat(until_iso))
        df = df[df["timestamp"] <= until_dt]
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    return df.sort_values("timestamp")


@st.cache_data(ttl=60)
def load_fees_df(since_iso: str | None, symbols: tuple) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    rows = run_async(repository.get_execution_fees(since=since_dt, limit=10_000))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "symbol": r.symbol,
                "side": r.side,
                "qty": r.last_qty,
                "price": r.last_px,
                "fee_xbt": r.fee_amount / (1e8 if r.fee_currency == "XBt" else 1e6),
                "currency": r.fee_currency,
            }
            for r in rows
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    return df.sort_values("timestamp")


# Build cache key for filters
_since_iso = since.isoformat() if since else None
_until_iso = until.isoformat() if until else None
_sym_tuple = tuple(sorted(selected_symbols))

funding_df = load_funding_df(_since_iso, _until_iso, _sym_tuple)
fees_df = load_fees_df(_since_iso, _sym_tuple)

# ------------------------------------------------------------------ #
# Tabs                                                                 #
# ------------------------------------------------------------------ #

tab_overview, tab_funding, tab_fees, tab_sync = st.tabs(
    ["Overview", "Funding Payments", "Execution Fees", "Sync Status"]
)

# ================================================================== #
# TAB 1: Overview                                                      #
# ================================================================== #

with tab_overview:
    st.header("Overview")

    if funding_df.empty and fees_df.empty:
        st.info("No data yet. Check Sync Status tab.")
    else:
        # KPI row
        total_funding_received = funding_df[funding_df["amount_xbt"] > 0]["amount_xbt"].sum() if not funding_df.empty else 0.0
        total_funding_paid = funding_df[funding_df["amount_xbt"] < 0]["amount_xbt"].sum() if not funding_df.empty else 0.0
        net_funding = funding_df["amount_xbt"].sum() if not funding_df.empty else 0.0
        total_fees = fees_df["fee_xbt"].sum() if not fees_df.empty else 0.0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Funding Received (XBT)", f"{total_funding_received:+.6f}")
        with col2:
            st.metric("Funding Paid (XBT)", f"{total_funding_paid:+.6f}")
        with col3:
            st.metric("Net Funding (XBT)", f"{net_funding:+.6f}")
        with col4:
            st.metric("Net Exec Fees (XBT)", f"{total_fees:+.6f}")

        st.divider()

        # Per-symbol funding breakdown
        if not funding_df.empty:
            st.subheader("Funding by Symbol")
            breakdown = (
                funding_df.groupby("symbol")["amount_xbt"]
                .agg(
                    received=lambda x: x[x > 0].sum(),
                    paid=lambda x: x[x < 0].sum(),
                    net="sum",
                    settlements="count",
                )
                .reset_index()
                .sort_values("net", ascending=False)
            )
            breakdown.columns = ["Symbol", "Received (XBT)", "Paid (XBT)", "Net (XBT)", "Settlements"]
            st.dataframe(
                breakdown.style.format(
                    {
                        "Received (XBT)": "{:+.6f}",
                        "Paid (XBT)": "{:+.6f}",
                        "Net (XBT)": "{:+.6f}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        # Per-symbol fee breakdown
        if not fees_df.empty:
            st.subheader("Fees by Symbol")
            fee_breakdown = (
                fees_df.groupby("symbol")["fee_xbt"]
                .agg(net="sum", trades="count")
                .reset_index()
                .sort_values("net")
            )
            fee_breakdown.columns = ["Symbol", "Net Fee (XBT)", "Trades"]
            st.dataframe(
                fee_breakdown.style.format({"Net Fee (XBT)": "{:+.6f}"}),
                use_container_width=True,
                hide_index=True,
            )

# ================================================================== #
# TAB 2: Funding Payments                                              #
# ================================================================== #

with tab_funding:
    st.header("Funding Payments")

    if funding_df.empty:
        st.info("No funding payment records in the selected range.")
    else:
        symbols_in_data = sorted(funding_df["symbol"].unique())

        # One sub-tab per symbol
        sym_tabs = st.tabs(symbols_in_data)

        for sym, sym_tab in zip(symbols_in_data, sym_tabs):
            with sym_tab:
                sym_df = funding_df[funding_df["symbol"] == sym].copy().sort_values("timestamp")
                sym_df["cumulative"] = sym_df["amount_xbt"].cumsum()

                net = sym_df["amount_xbt"].sum()
                received = sym_df[sym_df["amount_xbt"] > 0]["amount_xbt"].sum()
                paid = sym_df[sym_df["amount_xbt"] < 0]["amount_xbt"].sum()
                settlements = len(sym_df)

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Net (XBT)", f"{net:+.6f}")
                c2.metric("Received (XBT)", f"{received:+.6f}")
                c3.metric("Paid (XBT)", f"{paid:+.6f}")
                c4.metric("Settlements", settlements)

                # Cumulative line
                fig_cum = go.Figure(
                    go.Scatter(
                        x=sym_df["timestamp"],
                        y=sym_df["cumulative"],
                        mode="lines+markers",
                        marker=dict(size=4),
                        name="Cumulative",
                        line=dict(color="royalblue"),
                    )
                )
                fig_cum.update_layout(
                    title="Cumulative Funding (XBT)",
                    xaxis_title="Date",
                    yaxis_title="XBT",
                    hovermode="x unified",
                    height=350,
                )
                st.plotly_chart(fig_cum, use_container_width=True)

                # Per-settlement bars (green=received, red=paid)
                colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in sym_df["amount_xbt"]]
                fig_bar = go.Figure(
                    go.Bar(
                        x=sym_df["timestamp"],
                        y=sym_df["amount_xbt"],
                        marker_color=colors,
                        name="Per settlement",
                    )
                )
                fig_bar.update_layout(
                    title="Funding per Settlement",
                    xaxis_title="Date",
                    yaxis_title="XBT",
                    height=300,
                )
                st.plotly_chart(fig_bar, use_container_width=True)

                # Raw table for this symbol
                display_df = sym_df[["timestamp", "amount_xbt", "position_qty"]].copy()
                display_df.columns = ["Timestamp", "Amount (XBT)", "Position Qty"]
                st.dataframe(
                    display_df.style.format({"Amount (XBT)": "{:+.8f}"}),
                    use_container_width=True,
                    hide_index=True,
                )

# ================================================================== #
# TAB 3: Execution Fees                                                #
# ================================================================== #

with tab_fees:
    st.header("Execution Fees")

    if fees_df.empty:
        st.info("No execution fee records in the selected range.")
    else:
        # Cumulative fees line chart
        st.subheader("Cumulative Fees (XBT)")
        fig = go.Figure()
        for sym in sorted(fees_df["symbol"].unique()):
            sym_df = fees_df[fees_df["symbol"] == sym].copy()
            sym_df = sym_df.sort_values("timestamp")
            sym_df["cumulative"] = sym_df["fee_xbt"].cumsum()
            fig.add_trace(
                go.Scatter(
                    x=sym_df["timestamp"],
                    y=sym_df["cumulative"],
                    mode="lines",
                    name=sym,
                )
            )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Cumulative XBT",
            hovermode="x unified",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Fee by symbol bar
        st.subheader("Total Fees by Symbol")
        fee_by_sym = fees_df.groupby("symbol")["fee_xbt"].sum().reset_index()
        fig2 = go.Figure(
            go.Bar(x=fee_by_sym["symbol"], y=fee_by_sym["fee_xbt"], name="Net fee")
        )
        fig2.update_layout(xaxis_title="Symbol", yaxis_title="XBT", height=300)
        st.plotly_chart(fig2, use_container_width=True)

        # Buy vs Sell fee split
        if "side" in fees_df.columns and fees_df["side"].notna().any():
            st.subheader("Fees by Side")
            side_df = (
                fees_df.groupby(["symbol", "side"])["fee_xbt"]
                .sum()
                .reset_index()
            )
            fig3 = go.Figure()
            for side in sorted(side_df["side"].unique()):
                s = side_df[side_df["side"] == side]
                fig3.add_trace(go.Bar(x=s["symbol"], y=s["fee_xbt"], name=side))
            fig3.update_layout(barmode="group", xaxis_title="Symbol", yaxis_title="XBT", height=300)
            st.plotly_chart(fig3, use_container_width=True)

        # Raw table
        st.subheader("Raw Data")
        display_df = fees_df[["timestamp", "symbol", "side", "qty", "price", "fee_xbt"]].copy()
        display_df.columns = ["Timestamp", "Symbol", "Side", "Qty", "Price", "Fee (XBT)"]
        st.dataframe(
            display_df.style.format({"Fee (XBT)": "{:+.8f}", "Price": "{:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )

# ================================================================== #
# TAB 4: Sync Status                                                   #
# ================================================================== #

with tab_sync:
    st.header("Sync Status")

    cursors = run_async(repository.get_all_cursors())

    if cursors:
        cursor_data = [
            {
                "Data Type": c.data_type,
                "Last Synced At (UTC)": c.last_synced_at,
                "Total Rows": c.total_rows,
            }
            for c in cursors
        ]
        st.dataframe(pd.DataFrame(cursor_data), use_container_width=True, hide_index=True)
    else:
        st.info("No sync cursors found — run a sync first.")

    sync_stats = st.session_state.get("sync_stats", {})
    if sync_stats:
        st.subheader("Last Sync Results")
        for data_type, stats in sync_stats.items():
            col1, col2 = st.columns(2)
            with col1:
                st.metric(f"{data_type.capitalize()} — New rows", stats.get("new_rows", 0))
            with col2:
                st.metric(f"{data_type.capitalize()} — Total rows", stats.get("total_rows", 0))

    st.divider()
    st.caption(f"Database: `{_DB_URL}`")
    st.caption(f"Environment: {'Testnet' if _TESTNET else 'Live'}")
