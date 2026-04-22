"""
Funding Analysis Dashboard — read-only market analysis.

Shows BitMEX + HyperLiquid funding rates side by side and compares them to
the Binance USD margin borrow cost (the funding leg used to buy spot as a
hedge). Purely a market-analysis tool: no orders, no engine integration.

Runs on port 8503  (8501 = engine dashboard, 8502 = trading_analysis).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import sys
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from funding_analysis import sync as sync_mod
from funding_analysis.db import repository
from funding_analysis.db.models import init_db
from funding_analysis.exchanges.binance import BinanceClient
from funding_analysis.exchanges.bitmex import BitmexFundingClient
from funding_analysis.exchanges.hyperliquid import HyperliquidFundingClient
from funding_analysis.normalize import downsample_to_bucket, to_annualized_apr
from funding_analysis.simulator import simulate_payout

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
_fa_cfg = _config.get("funding_analysis", {})
_DB_URL = _fa_cfg.get("db_url", "sqlite+aiosqlite:///data/funding_analysis.db")
_HL_URL = _fa_cfg.get("hyperliquid_api_url", "https://api.hyperliquid.xyz")
_MARGIN_ASSETS = _fa_cfg.get("binance_margin_assets", ["USDC"])
_INITIAL_BACKFILL_DAYS = int(_fa_cfg.get("initial_backfill_days", 90))
_UNIVERSE_STALE_DAYS = int(_fa_cfg.get("asset_universe_refresh_days", 7))
_VIP_LEVEL = int(_fa_cfg.get("binance_vip_level", 0))
_SYNC_DELAY_S = float(_fa_cfg.get("sync_interval_delay_ms", 250)) / 1000.0

_BINANCE_KEY = os.environ.get("BINANCE_READONLY_API_KEY", "")
_BINANCE_SECRET = os.environ.get("BINANCE_READONLY_API_SECRET", "")
_HAS_BINANCE_KEY = bool(_BINANCE_KEY and _BINANCE_SECRET)

# ------------------------------------------------------------------ #
# Async bridge                                                         #
# ------------------------------------------------------------------ #

_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def run_async(coro):
    future = _pool.submit(asyncio.run, coro)
    return future.result()


# ------------------------------------------------------------------ #
# DB init                                                              #
# ------------------------------------------------------------------ #

run_async(init_db(_DB_URL))

st.set_page_config(
    page_title="Funding Analysis",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------ #
# Sync (first page load only; explicit re-sync via sidebar button)    #
# ------------------------------------------------------------------ #


async def _do_sync(assets_filter: set[str] | None = None) -> dict:
    bitmex = BitmexFundingClient(testnet=False)
    hl = HyperliquidFundingClient(base_url=_HL_URL)
    binance = BinanceClient(api_key=_BINANCE_KEY, api_secret=_BINANCE_SECRET)
    try:
        return await sync_mod.run_sync(
            bitmex=bitmex,
            hyperliquid=hl,
            binance=binance,
            backfill_days=_INITIAL_BACKFILL_DAYS,
            margin_assets=_MARGIN_ASSETS,
            universe_stale_days=_UNIVERSE_STALE_DAYS,
            vip_level=_VIP_LEVEL,
            delay_s=_SYNC_DELAY_S,
            assets_filter=assets_filter,
        )
    finally:
        await bitmex.close()
        await hl.close()
        await binance.close()


if "synced" not in st.session_state:
    with st.spinner("Syncing funding rates (first run may take several minutes)…"):
        try:
            st.session_state["sync_stats"] = run_async(_do_sync())
            st.session_state["synced"] = True
        except Exception as e:
            st.error(f"Sync failed: {e}")
            st.session_state["synced"] = False
            st.session_state["sync_stats"] = {}

# ------------------------------------------------------------------ #
# Sidebar                                                              #
# ------------------------------------------------------------------ #


with st.sidebar:
    st.markdown("### Funding Analysis")
    st.caption("Market-wide funding — read-only")
    if _HAS_BINANCE_KEY:
        st.success("Binance readonly key: ✓")
    else:
        st.warning(
            "No Binance readonly API key — margin rate history + borrow history disabled. "
            "Set `BINANCE_READONLY_API_KEY` / `_SECRET` in `config/.env`."
        )
    st.divider()

    # Date range filter
    st.subheader("Date range")
    _today = datetime.now(UTC)
    _today_end = _today.replace(hour=23, minute=59, second=59, microsecond=0)

    _options = ["Last 7 days", "Last 30 days", "Last 90 days", "Last 180 days", "Last year"]
    _selected = st.selectbox("Window", _options, index=1)
    _days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 90 days": 90,
        "Last 180 days": 180,
        "Last year": 365,
    }
    _days = _days_map[_selected]
    since = _today_end - timedelta(days=_days)
    until = _today_end

    st.divider()
    if st.button("Re-sync now"):
        st.cache_data.clear()
        st.session_state.pop("synced", None)
        st.session_state.pop("sync_stats", None)
        st.rerun()

    if st.button("Full re-sync (wipe cursors)", type="secondary"):
        st.session_state["confirm_full_resync"] = True
    if st.session_state.get("confirm_full_resync"):
        st.warning(
            f"Wipes all sync cursors and re-fetches the last {_INITIAL_BACKFILL_DAYS} days "
            "of funding + borrow data. May take several minutes."
        )
        c1, c2 = st.columns(2)
        if c1.button("Yes, proceed", type="primary"):
            for dt in (
                "bitmex_funding",
                "hl_funding",
                "binance_margin_rate",
                "binance_borrow",
                "universe",
            ):
                run_async(repository.delete_sync_cursor(dt))
            st.session_state.pop("confirm_full_resync", None)
            st.session_state.pop("synced", None)
            st.session_state.pop("sync_stats", None)
            st.cache_data.clear()
            st.rerun()
        if c2.button("Cancel"):
            st.session_state.pop("confirm_full_resync", None)
            st.rerun()

    st.divider()
    stats = st.session_state.get("sync_stats") or {}
    if stats:
        st.caption("Last sync:")
        st.json(stats, expanded=False)


# ------------------------------------------------------------------ #
# Data loaders (cached)                                                #
# ------------------------------------------------------------------ #


@st.cache_data(ttl=60)
def load_assets() -> pd.DataFrame:
    rows = run_async(repository.get_active_assets())
    return pd.DataFrame(
        [
            {
                "asset": r.asset,
                "bitmex_symbol": r.bitmex_symbol,
                "bitmex_contract_type": r.bitmex_contract_type,
                "hyperliquid_name": r.hyperliquid_name,
                "binance_spot_symbol": r.binance_spot_symbol,
            }
            for r in rows
        ]
    )


@st.cache_data(ttl=60)
def load_funding_df(
    exchange: str | None,
    asset: str | None,
    venue_symbol: str | None,
    since_iso: str,
    until_iso: str,
) -> pd.DataFrame:
    _since = datetime.fromisoformat(since_iso)
    _until = datetime.fromisoformat(until_iso)
    rows = run_async(
        repository.get_funding_rates(
            exchange=exchange,
            asset=asset,
            venue_symbol=venue_symbol,
            since=_since,
            until=_until,
        )
    )
    return pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "exchange": r.exchange,
                "asset": r.asset,
                "venue_symbol": r.venue_symbol,
                "funding_rate": r.funding_rate,
                "interval_hours": r.interval_hours,
                "apr": to_annualized_apr(r.funding_rate, r.interval_hours),
            }
            for r in rows
        ]
    )


@st.cache_data(ttl=60)
def load_binance_margin_df(asset: str, since_iso: str, until_iso: str) -> pd.DataFrame:
    _since = datetime.fromisoformat(since_iso)
    _until = datetime.fromisoformat(until_iso)
    rows = run_async(
        repository.get_binance_margin_rates(asset=asset, since=_since, until=_until, vip_level=_VIP_LEVEL)
    )
    return pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "asset": r.asset,
                "daily_rate": r.daily_interest_rate,
                "apr": r.daily_interest_rate * 365,
            }
            for r in rows
        ]
    )


@st.cache_data(ttl=300)
def load_cursors() -> pd.DataFrame:
    rows = run_async(repository.get_all_cursors())
    return pd.DataFrame(
        [
            {"data_type": r.data_type, "last_synced_at": r.last_synced_at, "total_rows": r.total_rows}
            for r in rows
        ]
    )


@st.cache_data(ttl=60)
def load_borrow_history(since_iso: str, until_iso: str) -> pd.DataFrame:
    _since = datetime.fromisoformat(since_iso)
    _until = datetime.fromisoformat(until_iso)
    rows = run_async(repository.get_borrow_history(since=_since, until=_until))
    return pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "asset": r.asset,
                "principal": r.principal,
                "interest": r.interest,
                "status": r.status,
            }
            for r in rows
        ]
    )


# ------------------------------------------------------------------ #
# Tabs                                                                 #
# ------------------------------------------------------------------ #

assets_df = load_assets()
_since_iso = since.isoformat()
_until_iso = until.isoformat()

st.title("Funding Analysis")
st.caption(
    f"{len(assets_df)} assets tracked · BitMEX funding settles every 8h · "
    f"HyperLiquid every 1h · Binance margin charges hourly"
)

tab_overview, tab_spread, tab_net, tab_sim, tab_binance, tab_sync = st.tabs(
    [
        "Overview",
        "Cross-exchange spread",
        "Funding − borrow",
        "Payout simulator",
        "Binance margin",
        "Sync status",
    ]
)

# ------------------------------------------------------------------ #
# Overview                                                             #
# ------------------------------------------------------------------ #

with tab_overview:
    if assets_df.empty:
        st.info("No assets yet — run a sync from the sidebar.")
    else:
        # Build a per-asset summary: last bitmex apr, mean hl apr over window, borrow apr.
        all_funding = load_funding_df(None, None, None, _since_iso, _until_iso)
        if all_funding.empty:
            st.info("No funding history in the selected window yet.")
        else:
            rows = []
            usdc_df = load_binance_margin_df("USDC", _since_iso, _until_iso)
            usdc_borrow_apr = usdc_df["apr"].iloc[-1] if not usdc_df.empty else None

            for _, asset_row in assets_df.iterrows():
                asset = asset_row["asset"]
                bitmex_part = all_funding[
                    (all_funding["exchange"] == "bitmex") & (all_funding["asset"] == asset)
                ]
                hl_part = all_funding[
                    (all_funding["exchange"] == "hyperliquid") & (all_funding["asset"] == asset)
                ]
                bm_last = bitmex_part["apr"].iloc[-1] if not bitmex_part.empty else None
                hl_mean = hl_part["apr"].mean() if not hl_part.empty else None

                best_venue = None
                best_apr = None
                for venue, value in (("BitMEX", bm_last), ("HL", hl_mean)):
                    if value is None:
                        continue
                    if best_apr is None or value > best_apr:
                        best_apr = value
                        best_venue = venue
                net_yield = (best_apr - usdc_borrow_apr) if (best_apr is not None and usdc_borrow_apr is not None) else None

                rows.append(
                    {
                        "asset": asset,
                        "bitmex_apr": bm_last,
                        "hl_mean_apr": hl_mean,
                        "usdc_borrow_apr": usdc_borrow_apr,
                        "best_short_venue": best_venue,
                        "net_yield_vs_borrow": net_yield,
                    }
                )
            df = pd.DataFrame(rows).sort_values("net_yield_vs_borrow", ascending=False, na_position="last")

            def _pct(col):
                return df[col].map(lambda v: f"{v*100:+.2f}%" if pd.notna(v) else "—")

            display = pd.DataFrame(
                {
                    "asset": df["asset"],
                    "BitMEX APR": _pct("bitmex_apr"),
                    "HL mean APR": _pct("hl_mean_apr"),
                    "USDC borrow APR": _pct("usdc_borrow_apr"),
                    "Best short venue": df["best_short_venue"].fillna("—"),
                    "Net yield vs borrow": _pct("net_yield_vs_borrow"),
                }
            )
            st.dataframe(display, width="stretch", hide_index=True)

# ------------------------------------------------------------------ #
# Cross-exchange spread                                                #
# ------------------------------------------------------------------ #

with tab_spread:
    if assets_df.empty:
        st.info("No assets loaded.")
    else:
        options = [
            a
            for a in assets_df["asset"].tolist()
            if pd.notna(assets_df.loc[assets_df.asset == a, "bitmex_symbol"].iloc[0])
            and pd.notna(assets_df.loc[assets_df.asset == a, "hyperliquid_name"].iloc[0])
        ]
        if not options:
            st.info("No asset is listed on both BitMEX and HyperLiquid yet.")
        else:
            default_idx = options.index("BTC") if "BTC" in options else 0
            asset = st.selectbox("Asset", options, index=default_idx, key="spread_asset")
            bm_df = load_funding_df("bitmex", asset, None, _since_iso, _until_iso)
            hl_df = load_funding_df("hyperliquid", asset, None, _since_iso, _until_iso)

            if bm_df.empty and hl_df.empty:
                st.info("No history for this asset in the selected window.")
            else:
                # Downsample HL to 8h buckets for direct spread comparison.
                hl_points = list(zip(hl_df["timestamp"], hl_df["funding_rate"]))
                hl_8h = downsample_to_bucket(hl_points, bucket_hours=8, mode="sum")
                hl_8h_df = pd.DataFrame(
                    [
                        {"timestamp": ts, "hl_apr": to_annualized_apr(rate, 8)}
                        for ts, rate in hl_8h
                    ]
                )
                bm_simple = pd.DataFrame(
                    {"timestamp": bm_df["timestamp"], "bitmex_apr": bm_df["apr"]}
                )
                merged = pd.merge(bm_simple, hl_8h_df, on="timestamp", how="outer").sort_values("timestamp")
                merged["spread_bitmex_minus_hl"] = merged["bitmex_apr"] - merged["hl_apr"]

                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=merged["timestamp"],
                        y=merged["bitmex_apr"],
                        name="BitMEX APR",
                        mode="lines",
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=merged["timestamp"],
                        y=merged["hl_apr"],
                        name="HL APR (8h-bucket sum)",
                        mode="lines",
                    )
                )
                fig.update_layout(
                    title=f"{asset} funding APR — BitMEX vs HyperLiquid",
                    yaxis_title="APR",
                    yaxis_tickformat=".1%",
                    hovermode="x unified",
                    height=450,
                )
                st.plotly_chart(fig, width="stretch")

                fig2 = go.Figure()
                fig2.add_trace(
                    go.Scatter(
                        x=merged["timestamp"],
                        y=merged["spread_bitmex_minus_hl"],
                        name="Spread (BitMEX − HL)",
                        fill="tozeroy",
                    )
                )
                fig2.update_layout(
                    title="Spread (positive ⇒ short BitMEX / long HL is the harvest direction)",
                    yaxis_title="APR spread",
                    yaxis_tickformat=".1%",
                    hovermode="x unified",
                    height=320,
                )
                st.plotly_chart(fig2, width="stretch")

                col1, col2, col3 = st.columns(3)
                col1.metric("Mean spread", f"{merged['spread_bitmex_minus_hl'].mean()*100:+.2f}%")
                col2.metric("Median spread", f"{merged['spread_bitmex_minus_hl'].median()*100:+.2f}%")
                col3.metric("P90 |spread|", f"{merged['spread_bitmex_minus_hl'].abs().quantile(0.9)*100:.2f}%")

# ------------------------------------------------------------------ #
# Funding minus borrow                                                 #
# ------------------------------------------------------------------ #

with tab_net:
    if assets_df.empty:
        st.info("No assets loaded.")
    else:
        col_a, col_b, col_c = st.columns(3)
        asset = col_a.selectbox("Asset", assets_df["asset"].tolist(), key="net_asset")
        venue = col_b.radio("Funding venue", ["BitMEX", "HyperLiquid"], horizontal=True, key="net_venue")
        borrow_asset = col_c.radio("Borrow asset", _MARGIN_ASSETS, horizontal=True, key="net_borrow")

        venue_key = "bitmex" if venue == "BitMEX" else "hyperliquid"
        f_df = load_funding_df(venue_key, asset, None, _since_iso, _until_iso)
        m_df = load_binance_margin_df(borrow_asset, _since_iso, _until_iso)

        if f_df.empty:
            st.info("No funding history for this (asset, venue) in the window.")
        elif m_df.empty and _HAS_BINANCE_KEY:
            st.info(
                f"No Binance {borrow_asset} margin-rate history in the window yet "
                "— re-sync or widen the window."
            )
        elif m_df.empty:
            st.warning("Binance readonly key missing — cannot show net yield.")
        else:
            # Align both series to an 8h grid for a common axis.
            f_points = list(zip(f_df["timestamp"], f_df["funding_rate"]))
            bucket_hours = 8
            if venue_key == "hyperliquid":
                f_bucket = downsample_to_bucket(f_points, bucket_hours=bucket_hours, mode="sum")
                f_apr = [(ts, to_annualized_apr(r, bucket_hours)) for ts, r in f_bucket]
            else:
                f_apr = [(ts, to_annualized_apr(r, bucket_hours)) for ts, r in f_points]
            borrow_apr_by_day = dict(zip(m_df["timestamp"], m_df["apr"]))

            # For each funding point, find the most recent borrow-APR value ≤ its timestamp.
            sorted_m = m_df.sort_values("timestamp").reset_index(drop=True)
            sorted_m_ts = sorted_m["timestamp"].tolist()
            sorted_m_apr = sorted_m["apr"].tolist()

            def _last_apr(ts):
                import bisect

                i = bisect.bisect_right(sorted_m_ts, ts) - 1
                if i < 0:
                    return sorted_m_apr[0]
                return sorted_m_apr[i]

            merged_rows = [
                {"timestamp": ts, "funding_apr": apr, "borrow_apr": _last_apr(ts), "net_apr": apr - _last_apr(ts)}
                for ts, apr in f_apr
            ]
            merged = pd.DataFrame(merged_rows)

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=merged["timestamp"], y=merged["funding_apr"], name=f"{venue} funding APR"))
            fig.add_trace(go.Scatter(x=merged["timestamp"], y=merged["borrow_apr"], name=f"Binance {borrow_asset} borrow APR"))
            fig.add_trace(go.Scatter(x=merged["timestamp"], y=merged["net_apr"], name="Net APR", line={"width": 3}))
            fig.update_layout(
                title=f"{asset} — funding on {venue} minus Binance {borrow_asset} borrow cost",
                yaxis_tickformat=".1%",
                hovermode="x unified",
                height=500,
            )
            st.plotly_chart(fig, width="stretch")

            c1, c2, c3 = st.columns(3)
            c1.metric("Mean net APR", f"{merged['net_apr'].mean()*100:+.2f}%")
            c2.metric("% periods positive", f"{(merged['net_apr'] > 0).mean()*100:.1f}%")
            c3.metric("Max net APR", f"{merged['net_apr'].max()*100:+.2f}%")

# ------------------------------------------------------------------ #
# Payout simulator                                                     #
# ------------------------------------------------------------------ #

with tab_sim:
    if assets_df.empty:
        st.info("No assets loaded.")
    else:
        col1, col2, col3 = st.columns(3)
        asset = col1.selectbox("Asset", assets_df["asset"].tolist(), key="sim_asset")
        venue_label = col2.radio("Venue", ["BitMEX", "HyperLiquid"], horizontal=True, key="sim_venue")
        side = col3.radio("Side", ["short", "long"], horizontal=True, key="sim_side")

        col4, col5, col6 = st.columns(3)
        notional = col4.number_input("Notional (USD)", min_value=100.0, value=10_000.0, step=1_000.0)
        lookback_days = col5.slider("Lookback (days)", min_value=7, max_value=_INITIAL_BACKFILL_DAYS, value=30)
        borrow_asset = col6.radio("Borrow asset", _MARGIN_ASSETS, horizontal=True, key="sim_borrow")

        sim_until = datetime.now(UTC)
        sim_since = sim_until - timedelta(days=lookback_days)
        venue_key = "bitmex" if venue_label == "BitMEX" else "hyperliquid"

        if st.button("Run simulation", type="primary"):
            try:
                result = run_async(
                    simulate_payout(
                        asset=asset,
                        venue=venue_key,
                        side=side,
                        notional_usd=float(notional),
                        start=sim_since,
                        end=sim_until,
                        hedge_currency=borrow_asset,
                    )
                )
            except Exception as e:
                st.error(f"Simulation failed: {e}")
                result = None

            if result is not None:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Funding", f"${result.total_funding:+,.2f}")
                c2.metric("Borrow cost", f"${result.total_borrow:+,.2f}")
                c3.metric("Net", f"${result.net:+,.2f}")
                c4.metric("Annualized net APR", f"{result.annualized_net_apr*100:+.2f}%")

                points_df = pd.DataFrame(
                    [
                        {
                            "timestamp": p.timestamp,
                            "cum_funding": p.cum_funding,
                            "cum_borrow": p.cum_borrow,
                            "cum_net": p.cum_net,
                        }
                        for p in result.points
                    ]
                )
                if not points_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=points_df["timestamp"], y=points_df["cum_funding"], name="Cumulative funding"))
                    fig.add_trace(go.Scatter(x=points_df["timestamp"], y=points_df["cum_borrow"], name="Cumulative borrow"))
                    fig.add_trace(go.Scatter(x=points_df["timestamp"], y=points_df["cum_net"], name="Cumulative net", line={"width": 3}))
                    fig.update_layout(
                        title=f"{side.upper()} {venue_label} {asset} · ${notional:,.0f} · {lookback_days}d",
                        yaxis_title="USD",
                        hovermode="x unified",
                        height=500,
                    )
                    st.plotly_chart(fig, width="stretch")
                    st.download_button(
                        "Download CSV",
                        points_df.to_csv(index=False),
                        file_name=f"sim_{asset}_{venue_key}_{side}.csv",
                    )
                st.caption(
                    f"{result.funding_periods} funding periods · {result.borrow_hours} borrow hours · "
                    "Constant notional, no mark-to-market drift, no execution costs."
                )

# ------------------------------------------------------------------ #
# Binance margin                                                       #
# ------------------------------------------------------------------ #

with tab_binance:
    st.subheader("USD borrow rate (cross margin)")
    if not _HAS_BINANCE_KEY:
        st.warning(
            "Binance readonly API key missing — only public snapshot rates are available. "
            "Add `BINANCE_READONLY_API_KEY` / `_SECRET` to `config/.env` for historical series."
        )

    if _HAS_BINANCE_KEY:
        fig = go.Figure()
        any_data = False
        for asset in _MARGIN_ASSETS:
            df = load_binance_margin_df(asset, _since_iso, _until_iso)
            if df.empty:
                continue
            any_data = True
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["apr"], name=f"{asset} APR"))
        if any_data:
            fig.update_layout(
                title="Binance cross-margin USD borrow APR",
                yaxis_tickformat=".1%",
                hovermode="x unified",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No margin-rate history yet — run a sync.")

        st.subheader("Borrow history (authed)")
        bh = load_borrow_history(_since_iso, _until_iso)
        if bh.empty:
            st.info("No borrow events in the window.")
        else:
            st.dataframe(bh, width="stretch", hide_index=True)

# ------------------------------------------------------------------ #
# Sync status                                                          #
# ------------------------------------------------------------------ #

with tab_sync:
    cursors = load_cursors()
    if cursors.empty:
        st.info("No sync cursors yet — run a sync.")
    else:
        st.dataframe(cursors, width="stretch", hide_index=True)

    st.subheader("Asset universe")
    if not assets_df.empty:
        st.dataframe(assets_df, width="stretch", hide_index=True)
    else:
        st.info("Asset universe is empty — run a sync.")
