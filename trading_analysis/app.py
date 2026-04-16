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

from trading_analysis.db import repository
from trading_analysis.db.models import init_db
from trading_analysis.exchange import FundingAnalysisClient

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
_fa_config = _config.get("trading_analysis", {})
_DB_URL = _fa_config.get("db_url", "sqlite+aiosqlite:///data/trading_analysis.db")
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
# DB init — runs every script execution; init_db() is idempotent      #
# ------------------------------------------------------------------ #

run_async(init_db(_DB_URL))

# ------------------------------------------------------------------ #
# Page config                                                          #
# ------------------------------------------------------------------ #

st.set_page_config(
    page_title="BitMEX Trading Analysis",
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
        client = FundingAnalysisClient(
            api_key=_API_KEY,
            api_secret=_API_SECRET,
            testnet=_TESTNET,
        )
        from trading_analysis import sync
        try:
            stats = run_async(sync.run_sync(client))
            st.session_state["synced"] = True
            st.session_state["sync_stats"] = stats
        except Exception as e:
            st.error(f"Sync failed: {e}")
            st.session_state["synced"] = False
            st.session_state["sync_stats"] = {}
        finally:
            run_async(client.close())

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
    _current_year = _today.year
    _today_end = datetime(_today.year, _today.month, _today.day, 23, 59, 59, tzinfo=timezone.utc)

    _relative_options = ["All", "Last month", "3 months", "6 months", "YTD", "1 year"]
    _year_options = [str(y) for y in range(_current_year, 2013, -1)]
    _selected_period = st.selectbox("Date range", _relative_options + _year_options, index=0)

    if _selected_period == "All":
        since = None
        until = None
    elif _selected_period == "Last month":
        since = datetime(_today.year, _today.month, _today.day, tzinfo=timezone.utc) - timedelta(days=30)
        until = _today_end
    elif _selected_period == "3 months":
        since = datetime(_today.year, _today.month, _today.day, tzinfo=timezone.utc) - timedelta(days=90)
        until = _today_end
    elif _selected_period == "6 months":
        since = datetime(_today.year, _today.month, _today.day, tzinfo=timezone.utc) - timedelta(days=180)
        until = _today_end
    elif _selected_period == "YTD":
        since = datetime(_current_year, 1, 1, tzinfo=timezone.utc)
        until = _today_end
    elif _selected_period == "1 year":
        since = datetime(_today.year, _today.month, _today.day, tzinfo=timezone.utc) - timedelta(days=365)
        until = _today_end
    else:
        _y = int(_selected_period)
        since = datetime(_y, 1, 1, tzinfo=timezone.utc)
        until = _today_end if _y == _current_year else datetime(_y, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    st.divider()
    if st.button("Re-sync now"):
        st.cache_data.clear()
        del st.session_state["synced"]
        if "sync_stats" in st.session_state:
            del st.session_state["sync_stats"]
        st.rerun()

    if st.button("Full re-sync", type="secondary"):
        st.session_state["confirm_full_resync"] = True

    if st.session_state.get("confirm_full_resync"):
        st.warning(
            "This will replay all funding **and** execution history from the BitMEX "
            "genesis date and may take several minutes. Proceed?"
        )
        col_yes, col_no = st.columns(2)
        if col_yes.button("Yes, proceed", type="primary"):
            run_async(repository.delete_sync_cursor("funding"))
            run_async(repository.delete_sync_cursor("execution"))
            del st.session_state["confirm_full_resync"]
            del st.session_state["synced"]
            if "sync_stats" in st.session_state:
                del st.session_state["sync_stats"]
            st.rerun()
        if col_no.button("Cancel"):
            del st.session_state["confirm_full_resync"]
            st.rerun()

    if st.button("Backfill wallet history", type="secondary"):
        st.session_state["confirm_wallet_backfill"] = True

    if st.session_state.get("confirm_wallet_backfill"):
        st.warning(
            "Fetches `/user/walletHistory` from the BitMEX genesis date to patch "
            "funding records whose amount is currently 0. This is the fix for "
            "pre-Nov-2024 entries where `/execution` returns null realisedPnl. "
            "May take a few minutes. Proceed?"
        )
        col_w1, col_w2 = st.columns(2)
        if col_w1.button("Yes, proceed", type="primary", key="wallet_backfill_yes"):
            del st.session_state["confirm_wallet_backfill"]
            with st.spinner("Fetching wallet history from BitMEX…"):
                _wc = FundingAnalysisClient(
                    api_key=_API_KEY,
                    api_secret=_API_SECRET,
                    testnet=_TESTNET,
                )
                from trading_analysis import sync as _sync
                try:
                    _wstats = run_async(_sync.backfill_funding_from_wallet(_wc))
                    st.success(
                        f"Wallet backfill complete: patched **{_wstats['patched']}** rows "
                        f"from {_wstats['total_wallet_rows']} wallet entries."
                    )
                    st.cache_data.clear()
                    st.rerun()
                except Exception as _e:
                    st.error(f"Wallet backfill failed: {_e}")
                finally:
                    run_async(_wc.close())
        if col_w2.button("Cancel", key="wallet_backfill_cancel"):
            del st.session_state["confirm_wallet_backfill"]
            st.rerun()

    if st.button("Full re-sync (executions)", type="secondary"):
        st.session_state["confirm_exec_resync"] = True

    if st.session_state.get("confirm_exec_resync"):
        st.warning(
            "This will replay all trade execution history from the BitMEX genesis "
            "date to populate `realised_pnl`. May take several minutes. Proceed?"
        )
        col_yes2, col_no2 = st.columns(2)
        if col_yes2.button("Yes, proceed", type="primary", key="exec_resync_yes"):
            run_async(repository.delete_sync_cursor("execution"))
            del st.session_state["confirm_exec_resync"]
            del st.session_state["synced"]
            if "sync_stats" in st.session_state:
                del st.session_state["sync_stats"]
            st.rerun()
        if col_no2.button("Cancel", key="exec_resync_cancel"):
            del st.session_state["confirm_exec_resync"]
            st.rerun()

# ------------------------------------------------------------------ #
# Data loaders                                                         #
# ------------------------------------------------------------------ #


@st.cache_data(ttl=300)
def fetch_btc_usd_price() -> float:
    """Fetch current BTC/USD price from BitMEX public instrument endpoint."""
    import urllib.request, json as _json
    base = "https://testnet.bitmex.com" if _TESTNET else "https://www.bitmex.com"
    url = f"{base}/api/v1/instrument?symbol=XBTUSD&columns=lastPrice&count=1"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = _json.loads(r.read())
        return float(data[0]["lastPrice"])
    except Exception:
        return 0.0


@st.cache_data(ttl=60)
def load_funding_df(since_iso: str | None, until_iso: str | None) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    until_dt = datetime.fromisoformat(until_iso) if until_iso else None
    rows = run_async(repository.get_funding_payments(since=since_dt, until=until_dt))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "symbol": r.symbol,
                "amount_xbt": r.fee_amount / (1e8 if r.fee_currency.lower() == "xbt" else 1e6),
                "currency": r.fee_currency.lower(),
                "position_qty": r.last_qty if r.fee_currency.lower() == "xbt" else r.last_qty / 100,
                "funding_rate": r.funding_rate,
            }
            for r in rows
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp")


@st.cache_data(ttl=60)
def load_fees_df(since_iso: str | None, until_iso: str | None) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    until_dt = datetime.fromisoformat(until_iso) if until_iso else None
    rows = run_async(repository.get_execution_fees(since=since_dt, until=until_dt))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "symbol": r.symbol,
                "side": r.side,
                "qty": r.last_qty if r.fee_currency.lower() in ("xbt", "usd") else r.last_qty / 100,
                "price": r.last_px,
                "fee_xbt": -r.fee_amount / (1e8 if r.fee_currency.lower() in ("xbt", "usd") else 1e6),
                "realised_pnl": r.realised_pnl / (1e8 if r.fee_currency.lower() in ("xbt", "usd") else 1e6),
                "net_position_pnl": (r.realised_pnl + r.fee_amount) / (1e8 if r.fee_currency.lower() in ("xbt", "usd") else 1e6),
                "currency": "xbt" if r.fee_currency.lower() in ("xbt", "usd") else r.fee_currency.lower(),
            }
            for r in rows
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp")


@st.cache_data(ttl=60)
def load_wallet_tx_df(since_iso: str | None, until_iso: str | None) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    until_dt = datetime.fromisoformat(until_iso) if until_iso else None
    rows = run_async(repository.get_wallet_transactions(
        since=since_dt,
        until=until_dt,
        types={"Deposit", "Withdrawal", "Conversion", "Transfer"},
    ))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([
        {
            "timestamp": r.transact_time,
            "type": r.transact_type,
            "currency": r.currency.lower(),
            "amount": r.amount / (1e8 if r.currency.lower() == "xbt" else 1e6),
            "fee": r.fee / (1e8 if r.currency.lower() == "xbt" else 1e6),
            "address": r.address or "",
            "tx_hash": r.tx_hash or "",
            "wallet_balance": (r.wallet_balance or 0) / (1e8 if r.currency.lower() == "xbt" else 1e6),
        }
        for r in rows
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# Build cache key for filters
_since_iso = since.isoformat() if since else None
_until_iso = until.isoformat() if until else None

funding_df = load_funding_df(_since_iso, _until_iso)
fees_df = load_fees_df(_since_iso, _until_iso)
wallet_tx_df = load_wallet_tx_df(_since_iso, _until_iso)
_btc_usd_price = fetch_btc_usd_price()

# ------------------------------------------------------------------ #
# Tabs                                                                 #
# ------------------------------------------------------------------ #

tab_overview, tab_funding, tab_fees, tab_pnl, tab_deposits, tab_sync = st.tabs(
    ["Overview", "Funding Payments", "Execution Fees", "PNL", "Deposits & Withdrawals", "Sync Status"]
)

# ================================================================== #
# TAB 1: Overview                                                      #
# ================================================================== #

with tab_overview:
    st.header("Overview")

    if funding_df.empty and fees_df.empty and wallet_tx_df.empty:
        st.info("No data yet. Check Sync Status tab.")
    else:
        def _ccy_label(ccy: str) -> str:
            return {"xbt": "XBT", "usdt": "USDT"}.get(ccy, ccy.upper())

        _CCY_ORDER = {"xbt": 0, "usdt": 1}
        funding_currencies = set(funding_df["currency"].unique()) if not funding_df.empty else set()
        fee_currencies = set(fees_df["currency"].unique()) if not fees_df.empty else set()
        wallet_currencies = set(wallet_tx_df["currency"].unique()) if not wallet_tx_df.empty else set()
        all_currencies = sorted(
            funding_currencies | fee_currencies | wallet_currencies,
            key=lambda c: (_CCY_ORDER.get(c, 2), c),
        )

        for idx, ccy in enumerate(all_currencies):
            label = _ccy_label(ccy)
            st.subheader(label)

            ccy_funding = funding_df[funding_df["currency"] == ccy] if not funding_df.empty else pd.DataFrame()
            ccy_fees    = fees_df[fees_df["currency"] == ccy]       if not fees_df.empty    else pd.DataFrame()
            ccy_wallet  = wallet_tx_df[wallet_tx_df["currency"] == ccy] if not wallet_tx_df.empty else pd.DataFrame()

            net_funding   = ccy_funding["amount_xbt"].sum()         if not ccy_funding.empty else 0.0
            net_trade_pnl = ccy_fees["net_position_pnl"].sum()     if not ccy_fees.empty    else 0.0
            net_exec_fees = ccy_fees["fee_xbt"].sum()               if not ccy_fees.empty    else 0.0
            # net_position_pnl = gross position gain; fee_xbt = cost (negative) — no double-count
            total_pnl     = net_funding + net_trade_pnl + net_exec_fees

            if not ccy_wallet.empty:
                deposits    = ccy_wallet[ccy_wallet["type"] == "Deposit"]["amount"].sum()
                withdrawals = ccy_wallet[ccy_wallet["type"] == "Withdrawal"]["amount"].sum()
                w_fees      = ccy_wallet[ccy_wallet["type"] == "Withdrawal"]["fee"].sum()
                net_flow    = deposits + withdrawals - w_fees  # withdrawals already negative
            else:
                net_flow = 0.0

            summary = pd.DataFrame([{
                f"Net Funding ({label})":     net_funding,
                f"Net Trading PnL ({label})": net_trade_pnl,
                f"Net Exec Fees ({label})":   net_exec_fees,
                f"Total PnL ({label})":       total_pnl,
                f"Net Flow ({label})":        net_flow,
            }])
            st.dataframe(
                summary.style.format("{:+.6f}"),
                width="stretch",
                hide_index=True,
            )

            # Funding by Symbol
            if not ccy_funding.empty:
                st.markdown("**Funding by Symbol**")
                breakdown = (
                    ccy_funding.groupby("symbol")["amount_xbt"]
                    .agg(
                        received=lambda x: x[x > 0].sum(),
                        paid=lambda x: x[x < 0].sum(),
                        net="sum",
                        settlements="count",
                    )
                    .reset_index()
                    .sort_values("net", ascending=False)
                )
                breakdown.columns = [
                    "Symbol",
                    f"Received ({label})",
                    f"Paid ({label})",
                    f"Net ({label})",
                    "Settlements",
                ]
                st.dataframe(
                    breakdown.style.format({
                        f"Received ({label})": "{:+.6f}",
                        f"Paid ({label})":     "{:+.6f}",
                        f"Net ({label})":      "{:+.6f}",
                    }),
                    width="stretch",
                    hide_index=True,
                )



            if idx < len(all_currencies) - 1:
                st.divider()

# ================================================================== #
# TAB 2: Funding Payments                                              #
# ================================================================== #

with tab_funding:
    st.header("Funding Payments")

    if funding_df.empty:
        st.info("No funding payment records in the selected range.")
    else:
        # Map each symbol to its settlement currency (mode in case of mixed rows)
        _sym_ccy = (
            funding_df.groupby("symbol")["currency"]
            .agg(lambda x: x.mode().iloc[0])
            .to_dict()
        )
        _CCY_ORDER = {"xbt": 0, "usdt": 1}
        symbols_in_data = sorted(
            funding_df["symbol"].unique(),
            key=lambda s: (_CCY_ORDER.get(_sym_ccy.get(s, ""), 2), s),
        )

        # One sub-tab per symbol
        sym_tabs = st.tabs(symbols_in_data)

        for sym, sym_tab in zip(symbols_in_data, sym_tabs):
            with sym_tab:
                sym_df = funding_df[funding_df["symbol"] == sym].copy().sort_values("timestamp")
                sym_df["cumulative"] = sym_df["amount_xbt"].cumsum()

                _sym_label = {"xbt": "XBT", "usdt": "USDT"}.get(_sym_ccy.get(sym, "xbt"), _sym_ccy.get(sym, "").upper())

                net = sym_df["amount_xbt"].sum()
                received = sym_df[sym_df["amount_xbt"] > 0]["amount_xbt"].sum()
                paid = sym_df[sym_df["amount_xbt"] < 0]["amount_xbt"].sum()
                settlements = len(sym_df)

                c1, c2, c3, c4 = st.columns(4)
                if _sym_label == "XBT" and _btc_usd_price:
                    _usd = lambda v: f"≈ ${v * _btc_usd_price:,.0f} USD  (@ ${_btc_usd_price:,.0f}/BTC)"
                    c1.metric(f"Net ({_sym_label})", f"{net:+.6f}", help=_usd(net))
                    c2.metric(f"Received ({_sym_label})", f"{received:+.6f}", help=_usd(received))
                    c3.metric(f"Paid ({_sym_label})", f"{paid:+.6f}", help=_usd(paid))
                else:
                    c1.metric(f"Net ({_sym_label})", f"{net:+.6f}")
                    c2.metric(f"Received ({_sym_label})", f"{received:+.6f}")
                    c3.metric(f"Paid ({_sym_label})", f"{paid:+.6f}")
                c4.metric("Settlements", settlements)

                # Cumulative line
                _xrange = [since, until] if since and until else None
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
                    title=f"Cumulative Funding ({_sym_label})",
                    xaxis_title="Date",
                    yaxis_title=_sym_label,
                    yaxis_tickformat=".6f",
                    hovermode="x unified",
                    height=350,
                    xaxis=dict(range=_xrange) if _xrange else {},
                )
                st.plotly_chart(fig_cum, width='stretch', key=f"cum_{sym}")

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
                    yaxis_title=_sym_label,
                    yaxis_tickformat=".6f",
                    height=300,
                    xaxis=dict(range=_xrange) if _xrange else {},
                )
                st.plotly_chart(fig_bar, width='stretch', key=f"bar_{sym}")

                # Funding rate per settlement (commission field from API, converted to %)
                rate_df = sym_df[sym_df["funding_rate"].notna()].copy()
                if not rate_df.empty:
                    _ANNUALISE = 3 * 365  # 3 settlements/day × 365 days
                    rate_df["rate_pct"] = rate_df["funding_rate"] * 100
                    rate_colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in rate_df["rate_pct"]]
                    fig_rate = go.Figure(
                        go.Bar(
                            x=rate_df["timestamp"],
                            y=rate_df["rate_pct"],
                            marker_color=rate_colors,
                            name="8h rate",
                        )
                    )
                    _r_min = rate_df["rate_pct"].min()
                    _r_max = rate_df["rate_pct"].max()
                    _pad = max(abs(_r_min), abs(_r_max)) * 0.1 or 0.001
                    fig_rate.update_layout(
                        title="Funding Rate per Settlement",
                        xaxis_title="Date",
                        height=300,
                        xaxis=dict(range=_xrange) if _xrange else {},
                        yaxis=dict(
                            title="8h rate (%)",
                            range=[_r_min - _pad, _r_max + _pad],
                            tickformat=".4f",
                        ),
                        yaxis2=dict(
                            title="Annualized (%)",
                            overlaying="y",
                            side="right",
                            range=[(_r_min - _pad) * _ANNUALISE, (_r_max + _pad) * _ANNUALISE],
                            tickformat=".2f",
                            showgrid=False,
                        ),
                    )
                    st.plotly_chart(fig_rate, width='stretch', key=f"rate_{sym}")

                # Raw table for this symbol
                display_df = sym_df[["timestamp", "amount_xbt", "position_qty"]].copy()
                display_df.columns = ["Timestamp", f"Amount ({_sym_label})", "Position Qty"]
                st.dataframe(
                    display_df.style.format({f"Amount ({_sym_label})": "{:+.8f}"}),
                    width='stretch',
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
            yaxis_tickformat=".6f",
            hovermode="x unified",
            height=400,
        )
        st.plotly_chart(fig, width='stretch')

        # Fee by symbol bar
        st.subheader("Total Fees by Symbol")
        fee_by_sym = fees_df.groupby("symbol")["fee_xbt"].sum().reset_index()
        fig2 = go.Figure(
            go.Bar(x=fee_by_sym["symbol"], y=fee_by_sym["fee_xbt"], name="Net fee")
        )
        fig2.update_layout(xaxis_title="Symbol", yaxis_title="XBT", yaxis_tickformat=".6f", height=300)
        st.plotly_chart(fig2, width='stretch')

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
            fig3.update_layout(barmode="group", xaxis_title="Symbol", yaxis_title="XBT", yaxis_tickformat=".6f", height=300)
            st.plotly_chart(fig3, width='stretch')

        # Raw table
        st.subheader("Raw Data")
        display_df = fees_df[["timestamp", "symbol", "side", "qty", "price", "fee_xbt"]].copy()
        display_df.columns = ["Timestamp", "Symbol", "Side", "Qty", "Price", "Fee (XBT)"]
        st.dataframe(
            display_df.style.format({"Fee (XBT)": "{:+.8f}", "Price": "{:,.2f}"}),
            width='stretch',
            hide_index=True,
        )

# ================================================================== #
# TAB 4: PNL                                                           #
# ================================================================== #

with tab_pnl:
    st.header("PNL")

    if funding_df.empty and fees_df.empty:
        st.info("No data yet. Check Sync Status tab.")
    else:
        _ccy_label_pnl = lambda c: {"xbt": "XBT", "usdt": "USDT"}.get(c, c.upper())
        _CCY_ORDER_PNL = {"xbt": 0, "usdt": 1}

        # Position closes: net_position_pnl = realisedPnl + execComm (adds back the fee to get
        # gross position gain). For opening/non-closing trades realisedPnl = -execComm, so
        # net_position_pnl = 0. Only genuine closes have net_position_pnl != 0.
        _fees_with_pnl = fees_df[fees_df["net_position_pnl"] != 0].copy() if not fees_df.empty else pd.DataFrame()

        # Only show symbols that have at least one trade with nonzero realised_pnl
        _all_pnl_syms = sorted(
            set(_fees_with_pnl["symbol"].unique()) if not _fees_with_pnl.empty else set()
        )

        if not _all_pnl_syms:
            st.info(
                "No trading PnL records found. BitMEX populates `realisedPnl` from "
                "~Nov 2024 onwards; run **Full re-sync (executions)** to backfill."
            )
        else:
            st.caption(
                "Trading PnL reflects executed position closes from ~Nov 2024 onwards. "
                "Earlier records are unavailable from the BitMEX API."
            )

            # Currency map: prefer fees_df (execution currency), fall back to funding
            _pnl_sym_ccy: dict[str, str] = {}
            for _src in [fees_df, funding_df]:
                if not _src.empty and "currency" in _src.columns:
                    _pnl_sym_ccy.update(
                        _src.groupby("symbol")["currency"]
                        .agg(lambda x: x.mode().iloc[0])
                        .to_dict()
                    )

            _all_pnl_syms_sorted = sorted(
                _all_pnl_syms,
                key=lambda s: (_CCY_ORDER_PNL.get(_pnl_sym_ccy.get(s, ""), 2), s),
            )

            _xrange = [since, until] if since and until else None

            sym_pnl_tabs = st.tabs(_all_pnl_syms_sorted)

            for sym, sym_tab in zip(_all_pnl_syms_sorted, sym_pnl_tabs):
                with sym_tab:
                    ccy = _pnl_sym_ccy.get(sym, "xbt")
                    label = _ccy_label_pnl(ccy)

                    sym_trade = _fees_with_pnl[_fees_with_pnl["symbol"] == sym].copy()
                    sym_fund  = funding_df[(funding_df["symbol"] == sym) & (funding_df["amount_xbt"] != 0)].copy() if not funding_df.empty else pd.DataFrame()
                    sym_fee   = fees_df[fees_df["symbol"] == sym].copy() if not fees_df.empty else pd.DataFrame()

                    # net_position_pnl = gross gain from closes; fee_xbt = all fees (negative)
                    trade_pnl_val = sym_trade["net_position_pnl"].sum() if not sym_trade.empty else 0.0
                    net_fund_val  = sym_fund["amount_xbt"].sum()        if not sym_fund.empty  else 0.0
                    net_fees_val  = sym_fee["fee_xbt"].sum()            if not sym_fee.empty   else 0.0
                    total_val     = trade_pnl_val + net_fund_val + net_fees_val

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric(f"Trading PNL ({label})", f"{trade_pnl_val:+.6f}")
                    c2.metric(f"Net Funding ({label})", f"{net_fund_val:+.6f}")
                    c3.metric(f"Exec Fees ({label})",   f"{net_fees_val:+.6f}")
                    c4.metric(f"Total PNL ({label})",   f"{total_val:+.6f}")

                    # ── Cumulative PnL chart — one trace per component ─────
                    fig_sym = go.Figure()

                    _td = sym_trade.sort_values("timestamp")
                    _td = _td.assign(cumulative=_td["net_position_pnl"].cumsum())
                    fig_sym.add_trace(go.Scatter(
                        x=_td["timestamp"], y=_td["cumulative"],
                        mode="lines", name="Trading PNL",
                        line=dict(color="#2ecc71"),
                    ))

                    if not sym_fund.empty:
                        _fd = sym_fund.sort_values("timestamp")
                        _fd = _fd.assign(cumulative=_fd["amount_xbt"].cumsum())
                        fig_sym.add_trace(go.Scatter(
                            x=_fd["timestamp"], y=_fd["cumulative"],
                            mode="lines", name="Funding",
                            line=dict(color="royalblue", dash="solid"),
                        ))

                    if not sym_fee.empty:
                        _ed = sym_fee.sort_values("timestamp")
                        _ed = _ed.assign(cumulative=_ed["fee_xbt"].cumsum())
                        fig_sym.add_trace(go.Scatter(
                            x=_ed["timestamp"], y=_ed["cumulative"],
                            mode="lines", name="Exec Fees",
                            line=dict(color="#e67e22", dash="dot"),
                        ))

                    # Total: gross position gains + funding + fees (no double-count)
                    _all_events = [sym_trade[["timestamp", "net_position_pnl"]].rename(columns={"net_position_pnl": "delta"})]
                    if not sym_fund.empty:
                        _all_events.append(sym_fund[["timestamp", "amount_xbt"]].rename(columns={"amount_xbt": "delta"}))
                    if not sym_fee.empty:
                        _all_events.append(sym_fee[["timestamp", "fee_xbt"]].rename(columns={"fee_xbt": "delta"}))
                    _tot_d = pd.concat(_all_events, ignore_index=True).sort_values("timestamp")
                    _tot_d = _tot_d.assign(cumulative=_tot_d["delta"].cumsum())
                    fig_sym.add_trace(go.Scatter(
                        x=_tot_d["timestamp"], y=_tot_d["cumulative"],
                        mode="lines", name="Total",
                        line=dict(color="#aaaaaa", width=1.5),
                    ))

                    fig_sym.update_layout(
                        title=f"Cumulative PNL — {sym} ({label})",
                        xaxis_title="Date", yaxis_title=label,
                        yaxis_tickformat=".6f",
                        hovermode="x unified", height=400,
                        xaxis=dict(range=_xrange) if _xrange else {},
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_sym, width="stretch", key=f"pnl_sym_{sym}")

                    # ── Trade history table ────────────────────────────────
                    st.subheader("Trade History")
                    tbl_df = sym_trade[["timestamp", "side", "qty", "price", "realised_pnl", "fee_xbt"]].copy()
                    tbl_df = tbl_df.sort_values("timestamp", ascending=False)
                    tbl_df.columns = ["Timestamp", "Side", "Qty", "Price", f"PNL ({label})", f"Fee ({label})"]
                    st.dataframe(
                        tbl_df.style.format({
                            f"PNL ({label})": "{:+.8f}",
                            f"Fee ({label})": "{:+.8f}",
                            "Price": "{:,.2f}",
                        }),
                        width="stretch",
                        hide_index=True,
                    )

                    # ── Funding history table ──────────────────────────────
                    if not sym_fund.empty:
                        st.subheader("Funding History")
                        fund_tbl = sym_fund[["timestamp", "amount_xbt", "position_qty"]].copy()
                        fund_tbl = fund_tbl.sort_values("timestamp", ascending=False)
                        fund_tbl.columns = ["Timestamp", f"Amount ({label})", "Position Qty"]
                        st.dataframe(
                            fund_tbl.style.format({f"Amount ({label})": "{:+.8f}"}),
                            width="stretch",
                            hide_index=True,
                        )

# ================================================================== #
# TAB 5: Deposits & Withdrawals                                        #
# ================================================================== #

with tab_deposits:
    st.header("Deposits & Withdrawals")

    if wallet_tx_df.empty:
        st.info("No deposit/withdrawal records found. Run a sync to fetch them.")
    else:
        for ccy in sorted(wallet_tx_df["currency"].unique(),
                          key=lambda c: {"xbt": 0, "usdt": 1}.get(c, 2)):
            label = {"xbt": "XBT", "usdt": "USDT"}.get(ccy, ccy.upper())
            ccy_df = wallet_tx_df[wallet_tx_df["currency"] == ccy]

            deposits    = ccy_df[ccy_df["type"] == "Deposit"]["amount"].sum()
            withdrawals = ccy_df[ccy_df["type"] == "Withdrawal"]["amount"].sum()
            w_fees      = ccy_df[ccy_df["type"] == "Withdrawal"]["fee"].sum()
            net         = deposits + withdrawals  # withdrawals already negative

            st.subheader(label)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric(f"Total Deposited ({label})",   f"{deposits:+.6f}")
            c2.metric(f"Total Withdrawn ({label})",   f"{withdrawals:+.6f}")
            c3.metric(f"Withdrawal Fees ({label})",   f"{-w_fees:+.6f}")
            c4.metric(f"Net Flow ({label})",          f"{net:+.6f}")

            display = ccy_df[["timestamp", "type", "amount", "fee", "address", "tx_hash"]].copy()
            display.columns = ["Timestamp", "Type", f"Amount ({label})", f"Fee ({label})", "Address", "TX Hash"]
            st.dataframe(
                display.style.format({
                    f"Amount ({label})": "{:+.8f}",
                    f"Fee ({label})":    "{:.8f}",
                }),
                width="stretch",
                hide_index=True,
            )
            st.divider()


# ================================================================== #
# TAB 6: Sync Status                                                   #
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
        st.dataframe(pd.DataFrame(cursor_data), width='stretch', hide_index=True)
    else:
        st.info("No sync cursors found — run a sync first.")

    sync_stats = st.session_state.get("sync_stats", {})
    if sync_stats:
        st.subheader("Last Sync Results")
        for data_type, stats in sync_stats.items():
            if stats is None:
                continue
            col1, col2 = st.columns(2)
            with col1:
                st.metric(f"{data_type.capitalize()} — New rows", stats.get("new_rows", 0))
            with col2:
                st.metric(f"{data_type.capitalize()} — Total rows", stats.get("total_rows", 0))

    st.divider()
    st.caption(f"Database: `{_DB_URL}`")
    st.caption(f"Environment: {'Testnet' if _TESTNET else 'Live'}")
