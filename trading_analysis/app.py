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
                "amount_xbt": r.fee_amount / (1e8 if r.fee_currency.lower() in ("xbt", "usd") else 1e6),
                "currency": r.fee_currency.lower(),
                "position_qty": _display_qty(r.symbol, r.fee_currency, r.timestamp, r.last_qty),
                "funding_rate": r.funding_rate,
            }
            for r in rows
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp")



# Per-symbol multipliers for USD quanto (XBT-settled) contracts.
# Formula: underlying_qty = raw_lastQty × multiplier / 1e8
# (same formula as USDT linear; derived empirically from fee-rate analysis)
# Symbols not listed here use the default 0.01 (= 1/100 — equivalent to the old
# raw/100 display, i.e. DOTUSD/LINKUSD/LUNAUSD/SUIUSD which need no extra scaling).
_USD_QUANTO_CORRECTIONS: dict[str, float] = {
    # multiplier = 1e5 (= 0.001 underlying per contract):
    "ETHUSD": 0.001, "ETHUSDH21": 0.001, "ETHUSDH22": 0.001, "ETHUSDH24": 0.001,
    "ETHUSDH25": 0.001, "ETHUSDM25": 0.001, "ETHUSDZ24": 0.001,
    "SOLUSD": 0.001, "AAVEUSD": 0.001, "AXSUSD": 0.001, "BNBUSD": 0.001,
    # multiplier = 1e7 (= 0.1 underlying per contract):
    "ADAUSD": 0.1, "GMTUSD": 0.1,
    # Larger multipliers:
    "XRPUSD": 0.2,     # multiplier = 2e7; confirmed across 2020-2025
    "DOGEUSD": 1.0,    # multiplier = 1e8; confirmed 2024-2025 bulk
}
_USD_QUANTO_DEFAULT = 0.01  # = multiplier 1e6; covers DOTUSD/LINKUSD/LUNAUSD/SUIUSD etc.


def _usd_quanto_qty(symbol: str, db_qty: float) -> float:
    """Convert raw lastQty to underlying asset quantity for a USD quanto XBT-settled contract."""
    return db_qty * _USD_QUANTO_CORRECTIONS.get(symbol.upper(), _USD_QUANTO_DEFAULT)


def _display_qty(symbol: str, fee_currency: str, timestamp: datetime, db_qty: float) -> float:
    """Return the correct display quantity for any contract type."""
    fc = fee_currency.lower()
    if _usd_symbol_is_inverse(symbol):
        return db_qty  # XBT inverse (XBTUSD/futures): raw contracts, no correction
    if fc in ("xbt", "usd"):
        return _usd_quanto_qty(symbol, db_qty)  # USD quanto: convert contracts → underlying
    return _usdt_qty(symbol, timestamp, db_qty)  # USDT linear


# BitMEX instrument multiplier per USDT symbol (verified empirically from fee rates).
# Formula: underlying_qty = raw_lastQty × multiplier / 1e8
# multiplier is in units of 1e-8 underlying per contract (BitMEX instrument field).
_USDT_MULTIPLIERS: dict[str, int] = {
    "MATICUSDT": 10_000,
    "DOGEUSDT":  10_000,
    "DOTUSDT":    1_000,
    "SOLUSDT":   10_000,   # current era (post-2026-03-01)
    "ETHUSDT":       10,
    "XRPUSDT":   10_000,
    "ADAUSDT":   10_000,
    "BNBUSDT":      100,
    "LTCUSDT":      100,
    "AVAXUSDT":     100,
    "BCHUSDT":       10,
    "SUIUSDT":    1_000,
}

# Symbols that changed contract size mid-history.
# Format: {symbol: [(cutoff_utc, multiplier_before_cutoff), ...]}
# Records with timestamp < cutoff use multiplier_before_cutoff.
_HISTORICAL_MULTIPLIERS: dict[str, list[tuple[datetime, int]]] = {
    # SOLUSDT multiplier was 100 until ~2026-03: confirmed by fee-rate analysis of DB data.
    # Last old-era record: 2026-02-21. First new-era record: 2026-04-16.
    "SOLUSDT": [(datetime(2026, 3, 1, tzinfo=timezone.utc), 100)],
}


def _usdt_qty(symbol: str, timestamp: datetime, db_qty: float) -> float:
    """Convert raw lastQty to underlying asset quantity for a USDT linear contract.

    Formula: raw_lastQty × multiplier / 1e8
    where multiplier is the BitMEX instrument field in units of 1e-8 underlying/contract.
    """
    multiplier = _USDT_MULTIPLIERS.get(symbol.upper(), 10_000)

    for cutoff, old_mult in _HISTORICAL_MULTIPLIERS.get(symbol.upper(), []):
        ts = timestamp if isinstance(timestamp, datetime) else pd.Timestamp(timestamp).to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            multiplier = old_mult
            break

    return db_qty * multiplier / 1e8


def _fmt_price(v: float) -> str:
    """Format a price value with enough decimal places for micro-priced tokens."""
    if v == 0:
        return "0"
    if v >= 0.01:
        return f"{v:,.4f}"
    if v >= 0.000001:
        return f"{v:.8f}"
    return f"{v:.10f}"


def _usd_symbol_is_inverse(symbol: str) -> bool:
    """Return True only for XBTUSD and its dated futures (e.g. XBTH18, XBTZ18).

    All other USD-labelled contracts (ETHUSD, LINKUSD, DOGEUSD, SOLUSD …) are
    USD quanto perpetuals whose execComm/realisedPnl are stored as satoshis
    (divide by 1e8 to get XBT, same as inverse).
    """
    return symbol.upper().startswith("XBT")


def _fee_divisor_and_currency(symbol: str, fee_currency: str) -> tuple[float, str]:
    """Return (divisor, display_currency) for a symbol's raw fee/pnl integers.

    XBT*   → satoshis ÷ 1e8 → XBT  (inverse, fee_currency="XBt")
    *USD quanto → satoshis ÷ 1e8 → XBT  (fee_currency="USD" legacy or "XBt" after sync fix)
    *USDT  → micro-USDT ÷ 1e6 → USDT
    """
    fc = fee_currency.lower()
    if fc == "xbt":
        return 1e8, "xbt"
    if fc == "usd":
        return 1e8, "xbt"  # inverse and quanto both store satoshis; display in XBT
    # usdt / usdт
    return 1e6, "usdt"


@st.cache_data(ttl=60)
def load_fees_df(since_iso: str | None, until_iso: str | None) -> pd.DataFrame:
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    until_dt = datetime.fromisoformat(until_iso) if until_iso else None
    rows = run_async(repository.get_execution_fees(since=since_dt, until=until_dt))
    if not rows:
        return pd.DataFrame()
    def _row_dict(r):
        div, ccy = _fee_divisor_and_currency(r.symbol, r.fee_currency)
        return {
            "exec_id": r.exec_id,
            "timestamp": r.timestamp,
            "symbol": r.symbol,
            "side": r.side,
            "qty": _display_qty(r.symbol, r.fee_currency, r.timestamp, r.last_qty),
            "price": r.last_px,
            "fee_xbt": -r.fee_amount / div,
            "realised_pnl": r.realised_pnl / div,
            "net_position_pnl": (r.realised_pnl + r.fee_amount) / div,
            "currency": ccy,
            "fee_currency": r.fee_currency.lower(),
        }

    df = pd.DataFrame([_row_dict(r) for r in rows])
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

        # ── Shared helpers ─────────────────────────────────────────────
        _now = pd.Timestamp.now(tz="UTC")
        _30d_ago = _now - pd.Timedelta(days=30)
        _1y_ago = _now - pd.Timedelta(days=365)
        _overview_df = funding_df[funding_df["timestamp"] >= _30d_ago].copy()
        _1y_df = funding_df[funding_df["timestamp"] >= _1y_ago].copy()

        _xbt_syms = [s for s in symbols_in_data if _sym_ccy.get(s) == "xbt"]
        _usdt_syms = [s for s in symbols_in_data if _sym_ccy.get(s) == "usdt"]

        _PALETTE = [
            "#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
            "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
        ]

        def _build_overview_fig(
            syms: list[str], df: pd.DataFrame, ccy_label: str,
            x_start: pd.Timestamp = None, x_end: pd.Timestamp = None,
        ) -> go.Figure:
            fig = go.Figure()
            for i, sym in enumerate(syms):
                color = _PALETTE[i % len(_PALETTE)]
                sdf = df[df["symbol"] == sym].sort_values("timestamp")
                if sdf.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=sdf["timestamp"],
                    y=sdf["amount_xbt"],
                    mode="lines+markers",
                    marker=dict(size=3),
                    name=sym,
                    line=dict(color=color),
                    legendgroup=sym,
                    yaxis="y1",
                ))
                rate_sdf = sdf[sdf["funding_rate"].notna()].set_index("timestamp")["funding_rate"]
                sdf["rate_pct"] = sdf["timestamp"].map(rate_sdf).apply(
                    lambda v: f"{v * 100:.4f}%" if pd.notna(v) else "n/a"
                )
                fig.data[-1].update(
                    customdata=sdf["rate_pct"].values,
                    hovertemplate=(
                        f"<b>{sym}</b><br>"
                        "Amount: %{y:.6f}<br>"
                        "Rate: %{customdata}<extra></extra>"
                    ),
                )
            x_range = [x_start, x_end] if x_start and x_end else None
            fig.update_layout(
                xaxis=dict(title="Date", **({"range": x_range} if x_range else {})),
                yaxis=dict(title=f"Amount ({ccy_label})", tickformat=".6f"),
                hovermode="x unified",
                height=360,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=40, b=40),
            )
            return fig

        def _build_daily_agg_fig(
            syms: list[str], df: pd.DataFrame, ccy_label: str,
            x_start: pd.Timestamp, x_end: pd.Timestamp,
            btc_usd: float = 0.0,
        ) -> go.Figure:
            sdf = df[df["symbol"].isin(syms)].copy()
            if sdf.empty:
                return go.Figure()
            sdf["date"] = sdf["timestamp"].dt.normalize()
            daily = sdf.groupby("date")["amount_xbt"].sum().reset_index()
            daily["cumulative"] = daily["amount_xbt"].cumsum()
            daily["color"] = daily["amount_xbt"].apply(lambda v: "#2ecc71" if v >= 0 else "#e74c3c")
            daily["group"] = (daily["color"] != daily["color"].shift()).cumsum()
            daily = daily.reset_index(drop=True)
            fig = go.Figure()
            for _, grp in daily.groupby("group", sort=True):
                color = grp["color"].iloc[0]
                first_idx = grp.index[0]
                if first_idx > 0:
                    prev = daily.loc[first_idx - 1]
                    xs = [prev["date"]] + list(grp["date"])
                    ys = [prev["cumulative"]] + list(grp["cumulative"])
                else:
                    xs = list(grp["date"])
                    ys = list(grp["cumulative"])
                if ccy_label == "XBT" and btc_usd:
                    usd_vals = [v * btc_usd for v in ys]
                    hover = "Cumulative: %{y:.6f} XBT<br>≈ %{customdata:,.2f}$<extra></extra>"
                else:
                    usd_vals = None
                    hover = "Cumulative: %{y:.6f}<extra></extra>"
                fig.add_trace(go.Scatter(
                    x=xs, y=ys,
                    mode="lines",
                    line=dict(color=color, width=2),
                    showlegend=False,
                    customdata=usd_vals,
                    hovertemplate=hover,
                ))
            fig.update_layout(
                xaxis=dict(title="Date", range=[x_start, x_end], hoverformat=" "),
                yaxis=dict(title=f"Cumulative ({ccy_label})", tickformat=".6f"),
                hovermode="x unified",
                height=360,
                margin=dict(t=40, b=40),
            )
            return fig

        # ── 1-year overview charts ─────────────────────────────────────
        if not _1y_df.empty:
            _1y_col_xbt, _1y_col_usdt = st.columns(2)
            with _1y_col_xbt:
                st.subheader("XBT-settled — last year")
                if _xbt_syms:
                    _1y_xbt_syms = [s for s in _xbt_syms if s in _1y_df["symbol"].values]
                    st.plotly_chart(
                        _build_daily_agg_fig(_1y_xbt_syms, _1y_df, "XBT", _1y_ago, _now, _btc_usd_price),
                        width="stretch", key="overview_1y_xbt",
                    )
                else:
                    st.caption("No XBT-settled symbols in data.")
            with _1y_col_usdt:
                st.subheader("USDT-settled — last year")
                if _usdt_syms:
                    _1y_usdt_syms = [s for s in _usdt_syms if s in _1y_df["symbol"].values]
                    st.plotly_chart(
                        _build_daily_agg_fig(_1y_usdt_syms, _1y_df, "USDT", _1y_ago, _now),
                        use_container_width=True, key="overview_1y_usdt",
                    )
                else:
                    st.caption("No USDT-settled symbols in data.")

        # ── 30-day overview charts ─────────────────────────────────────
        if _overview_df.empty:
            st.info("No funding data in the last 30 days for the overview charts.")
        else:
            _ov_col_xbt, _ov_col_usdt = st.columns(2)
            with _ov_col_xbt:
                st.subheader("XBT-settled — last 30 days")
                if _xbt_syms:
                    _fig_xbt = _build_overview_fig(
                        [s for s in _xbt_syms if s in _overview_df["symbol"].values],
                        _overview_df, "XBT", _30d_ago, _now,
                    )
                    st.plotly_chart(_fig_xbt, use_container_width=True, key="overview_xbt")
                else:
                    st.caption("No XBT-settled symbols in data.")
            with _ov_col_usdt:
                st.subheader("USDT-settled — last 30 days")
                if _usdt_syms:
                    _fig_usdt = _build_overview_fig(
                        [s for s in _usdt_syms if s in _overview_df["symbol"].values],
                        _overview_df,
                        "USDT",
                    )
                    st.plotly_chart(_fig_usdt, use_container_width=True, key="overview_usdt")
                else:
                    st.caption("No USDT-settled symbols in data.")

        # ── Daily aggregate bar charts ─────────────────────────────────
        if not _overview_df.empty:
            _daily_col_xbt, _daily_col_usdt = st.columns(2)

            def _build_daily_bar(syms: list[str], df: pd.DataFrame, ccy_label: str, btc_usd: float = 0.0) -> go.Figure:
                sdf = df[df["symbol"].isin(syms)].copy()
                if sdf.empty:
                    return go.Figure()
                sdf["date"] = sdf["timestamp"].dt.normalize()
                daily = sdf.groupby("date")["amount_xbt"].sum().reset_index()
                colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in daily["amount_xbt"]]
                if ccy_label == "XBT" and btc_usd:
                    usd_labels = (daily["amount_xbt"] * btc_usd).apply(lambda v: f"{v:,.2f}$")
                    hovertemplate = "Date: %{x|%Y-%m-%d}<br>Total: %{y:.6f} XBT<br>≈ %{customdata}<extra></extra>"
                else:
                    usd_labels = daily["amount_xbt"].apply(lambda v: f"{v:,.2f}$")
                    hovertemplate = "Date: %{x|%Y-%m-%d}<br>Total: %{y:.6f}<extra></extra>"
                fig = go.Figure(go.Bar(
                    x=daily["date"],
                    y=daily["amount_xbt"],
                    marker_color=colors,
                    customdata=usd_labels,
                    hovertemplate=hovertemplate,
                    text=usd_labels,
                    textposition="inside",
                    textfont=dict(size=10),
                ))
                fig.update_layout(
                    xaxis=dict(title="Date", range=[_30d_ago, _now]),
                    yaxis=dict(title=f"Amount ({ccy_label})", tickformat=".6f"),
                    height=300,
                    margin=dict(t=20, b=40),
                )
                return fig

            with _daily_col_xbt:
                if _xbt_syms:
                    _xbt_syms_present = [s for s in _xbt_syms if s in _overview_df["symbol"].values]
                    _xbt_total = _overview_df[_overview_df["symbol"].isin(_xbt_syms_present)]["amount_xbt"].sum()
                    _xbt_usd_total = _xbt_total * _btc_usd_price if _btc_usd_price else None
                    _xbt_usd_str = f"≈ {_xbt_usd_total:,.2f}$" if _xbt_usd_total is not None else ""
                    st.subheader(
                        "XBT-settled daily total",
                        anchor=False,
                    )
                    st.markdown(
                        f"<span style='font-size:0.8em; color:gray;'>{_xbt_total:+.6f} XBT{_xbt_usd_str} — (last 30 days)</span>",
                        unsafe_allow_html=True,
                    )
                    st.plotly_chart(_build_daily_bar(_xbt_syms_present, _overview_df, "XBT", _btc_usd_price), use_container_width=True, key="daily_xbt")
                else:
                    st.subheader("XBT-settled daily total")
                    st.caption("No XBT-settled symbols in data.")
            with _daily_col_usdt:
                if _usdt_syms:
                    _usdt_syms_present = [s for s in _usdt_syms if s in _overview_df["symbol"].values]
                    _usdt_total = _overview_df[_overview_df["symbol"].isin(_usdt_syms_present)]["amount_xbt"].sum()
                    st.subheader(
                        "USDT-settled daily total",
                        anchor=False,
                    )
                    st.markdown(
                        f"<span style='font-size:0.8em; color:gray;'>{_usdt_total:+,.2f} USDT — (last 30 days)</span>",
                        unsafe_allow_html=True,
                    )
                    st.plotly_chart(_build_daily_bar(_usdt_syms_present, _overview_df, "USDT"), use_container_width=True, key="daily_usdt")
                else:
                    st.subheader("USDT-settled daily total")
                    st.caption("No USDT-settled symbols in data.")

        st.divider()

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
            display_df.style.format({"Fee (XBT)": "{:+.8f}", "Price": _fmt_price}),
            width='stretch',
            hide_index=True,
        )

def _compute_fifo_pnl_series(df: pd.DataFrame, is_inverse: bool = False) -> "pd.Series":
    """Average-cost FIFO PNL for one symbol's fills.

    Returns a Series indexed like df where each value is the realised PNL
    of that fill: positive for a profitable close, 0 for an opening fill.

    is_inverse: True only for XBTUSD and XBT futures.
      Uses PNL = close_qty × (1/avg_cost − 1/price) instead of the linear formula.
      All other USD contracts (ETHUSD, LINKUSD, DOGEUSD …) are quanto — pass False.
    """
    df = df.sort_values(["timestamp", "exec_id"])
    position = 0.0
    avg_cost = 0.0
    pnls: list[float] = []
    for _, row in df.iterrows():
        qty = float(row["qty"])
        price = float(row["price"])
        signed = qty if str(row["side"]).lower() == "buy" else -qty
        pnl = 0.0
        if position == 0.0:
            position = signed
            avg_cost = price
        elif (position > 0) == (signed > 0):
            new_pos = position + signed
            avg_cost = (abs(position) * avg_cost + qty * price) / abs(new_pos)
            position = new_pos
        else:
            close_qty = min(qty, abs(position))
            if is_inverse:
                pnl = close_qty * (1/avg_cost - 1/price) if position > 0 else close_qty * (1/price - 1/avg_cost)
            else:
                pnl = close_qty * (price - avg_cost) if position > 0 else close_qty * (avg_cost - price)
            new_pos = position + signed
            if abs(new_pos) < 1e-9:
                position = 0.0
                avg_cost = 0.0
            elif (new_pos > 0) != (position > 0):
                # True reversal: position changed sign → new avg_cost at fill price
                position = new_pos
                avg_cost = price
            else:
                # Partial close: same sign remains → avg_cost unchanged
                position = new_pos
        pnls.append(pnl)
    return pd.Series(pnls, index=df.index)


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

        # Augment fees_df: for any fill where BitMEX realisedPnl is 0 (pre-Nov 2024),
        # compute PNL using average-cost FIFO. FIFO runs over ALL fills per symbol (to
        # maintain correct position state across the pre/post-Nov 2024 boundary) but is
        # only written back to rows that have realised_pnl == 0. Post-Nov 2024 rows keep
        # BitMEX's net_position_pnl unchanged.
        _fees_aug = fees_df.copy() if not fees_df.empty else pd.DataFrame()
        if not _fees_aug.empty:
            _syms_with_zero = set(
                _fees_aug.loc[_fees_aug["realised_pnl"] == 0, "symbol"].unique()
            )
            for _zsym in _syms_with_zero:
                _sym_mask = _fees_aug["symbol"] == _zsym
                _sym_rows = _fees_aug[_sym_mask]
                _fc = _sym_rows["fee_currency"].iloc[0]
                _is_inverse = _usd_symbol_is_inverse(_zsym)
                # USD quanto: FIFO returns qty_underlying × Δprice_usd; scale by 1e-3 → XBT
                # (contractSize / qty_correction ≈ 1e5 sat/$1 per underlying unit → × 1e5/1e8 = × 1e-3)
                _is_quanto = not _is_inverse and _fc in ("xbt", "usd")
                _fifo = _compute_fifo_pnl_series(_sym_rows, is_inverse=_is_inverse)
                if _is_quanto:
                    _fifo = _fifo * 1e-3
                _zero_rows = _sym_mask & (_fees_aug["realised_pnl"] == 0)
                _fees_aug.loc[_zero_rows, "net_position_pnl"] = _fifo[_fees_aug.index[_zero_rows]]

        _fees_with_pnl = _fees_aug[_fees_aug["net_position_pnl"] != 0].copy() if not _fees_aug.empty else pd.DataFrame()

        # Only show symbols that have at least one trade with nonzero PNL
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
                "From ~Nov 2024 onwards BitMEX provides `realisedPnl` directly. "
                "For earlier records, PnL is estimated using the average-cost method from raw fill prices."
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

            sym = st.selectbox("Symbol", _all_pnl_syms_sorted, key="pnl_sym_select")

            if sym:
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

                _pnl_last_ts = _tot_d["timestamp"].max()
                _pnl_first_ts = _tot_d["timestamp"].min()
                _pnl_pad = max((_pnl_last_ts - _pnl_first_ts) * 0.05, pd.Timedelta(days=2))
                _pnl_xrange = [_xrange[0], max(_xrange[1], _pnl_last_ts + _pnl_pad)] if _xrange else [_pnl_first_ts, _pnl_last_ts + _pnl_pad]

                fig_sym.update_layout(
                    title=f"Cumulative PNL — {sym} ({label})",
                    xaxis_title="Date", yaxis_title=label,
                    yaxis_tickformat=".6f",
                    hovermode="x unified", height=400,
                    xaxis=dict(range=_pnl_xrange),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig_sym, width="stretch", key=f"pnl_sym_{sym}")

                # ── Open position size chart ───────────────────────────
                if not sym_fee.empty:
                    _exec_qty = sym_fee[["timestamp", "side", "qty"]].copy()
                    _exec_qty["signed_qty"] = _exec_qty.apply(
                        lambda r: r["qty"] if str(r["side"]).lower() == "buy" else -r["qty"], axis=1
                    )
                    _exec_qty = _exec_qty.sort_values("timestamp").reset_index(drop=True)
                    _exec_qty["open_qty"] = _exec_qty["signed_qty"].cumsum()
                    _exec_qty["color"] = _exec_qty["open_qty"].apply(
                        lambda v: "#2ecc71" if v > 0 else ("#e74c3c" if v < 0 else "#aaaaaa")
                    )
                    _exec_qty["group"] = (_exec_qty["color"] != _exec_qty["color"].shift()).cumsum()
                    fig_qty = go.Figure()
                    for _, _seg in _exec_qty.groupby("group", sort=True):
                        _color = _seg["color"].iloc[0]
                        _first_idx = _seg.index[0]
                        if _first_idx > 0:
                            _prev = _exec_qty.loc[_first_idx - 1]
                            _xs = [_prev["timestamp"]] + list(_seg["timestamp"])
                            _ys = [_prev["open_qty"]] + list(_seg["open_qty"])
                        else:
                            _xs = list(_seg["timestamp"])
                            _ys = list(_seg["open_qty"])
                        fig_qty.add_trace(go.Scatter(
                            x=_xs, y=_ys,
                            mode="lines",
                            line=dict(color=_color, shape="vh"),
                            showlegend=False,
                            name="",
                            hovertemplate="%{y:,.0f}<extra></extra>",
                        ))
                    _qty_abs_max = _exec_qty["open_qty"].abs().max() or 1
                    _qty_last_ts = _exec_qty["timestamp"].max()
                    _qty_first_ts = _exec_qty["timestamp"].min()
                    _qty_pad = max((_qty_last_ts - _qty_first_ts) * 0.05, pd.Timedelta(days=2))
                    _qty_xrange = [_xrange[0], max(_xrange[1], _qty_last_ts + _qty_pad)] if _xrange else [_qty_first_ts, _qty_last_ts + _qty_pad]

                    fig_qty.update_layout(
                        title=f"Open Position Size — {sym}",
                        xaxis_title="Date",
                        yaxis=dict(
                            title="Qty (contracts)",
                            range=[-_qty_abs_max, _qty_abs_max],
                        ),
                        hovermode="x unified",
                        height=300,
                        xaxis=dict(range=_qty_xrange),
                        margin=dict(t=40, b=40),
                    )
                    st.plotly_chart(fig_qty, width="stretch", key=f"qty_{sym}")

                # ── Trade history table ────────────────────────────────
                st.subheader("Trade History")
                # Use _fees_aug so the PNL column shows either BitMEX realisedPnl
                # (post-Nov 2024) or FIFO-computed PNL (earlier records).
                _all_sym_fills = _fees_aug[_fees_aug["symbol"] == sym].copy() if not _fees_aug.empty else pd.DataFrame()
                tbl_df = _all_sym_fills[["timestamp", "side", "qty", "price", "net_position_pnl", "fee_xbt"]].copy() if not _all_sym_fills.empty else pd.DataFrame()
                tbl_df = tbl_df.sort_values("timestamp", ascending=False)
                tbl_df.columns = ["Timestamp", "Side", "Qty", "Price", f"PNL ({label})", f"Fee ({label})"]
                st.dataframe(
                    tbl_df.style.format({
                        f"PNL ({label})": "{:+.8f}",
                        f"Fee ({label})": "{:+.8f}",
                        "Price": _fmt_price,
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
