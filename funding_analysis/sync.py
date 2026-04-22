"""
Incremental sync orchestrator for funding_analysis.db.

Pulls funding rate history from BitMEX + HyperLiquid and Binance USD margin
rates (optional: user borrow history if keys are configured). Each source
owns its own row in `sync_cursor` so runs are resumable and fast.

Run via `make funding-analysis` (Streamlit drives sync on page load) or as
a CLI:  `python -m funding_analysis.sync --backfill-days 90`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog
from dotenv import load_dotenv

from funding_analysis import symbols as symbol_discovery
from funding_analysis.db import repository
from funding_analysis.db.models import init_db
from funding_analysis.exchanges.binance import BinanceClient
from funding_analysis.exchanges.bitmex import BitmexFundingClient
from funding_analysis.exchanges.hyperliquid import HyperliquidFundingClient

logger = structlog.get_logger(__name__)

_PAGE_SIZE = 500
_BITMEX_FUNDING_INTERVAL_HOURS = 8  # all BitMEX perps settle every 8h
_HL_FUNDING_INTERVAL_HOURS = 1
_ASSET_CONCURRENCY = 5  # max parallel per-asset fetches per exchange


def _parse_iso(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(UTC)
    raw = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _from_ms(ms: int | float) -> datetime:
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=UTC)


async def _start_time_for(data_type: str, backfill_days: int) -> datetime:
    cursor = await repository.get_sync_cursor(data_type)
    if cursor is not None:
        ts = cursor.last_synced_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts
    return datetime.now(UTC) - timedelta(days=backfill_days)


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------


async def _refresh_universe_if_stale(
    bitmex: BitmexFundingClient,
    hyperliquid: HyperliquidFundingClient,
    binance: BinanceClient,
    stale_after_days: int,
) -> dict:
    cursor = await repository.get_sync_cursor("universe")
    now = datetime.now(UTC)
    stale = True
    if cursor is not None:
        last = cursor.last_synced_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        stale = (now - last) > timedelta(days=stale_after_days)
    if not stale:
        return {"skipped": True}
    result = await symbol_discovery.refresh_universe(bitmex, hyperliquid, binance)
    await repository.set_sync_cursor("universe", now, result["total_active"])
    return result


# ---------------------------------------------------------------------------
# BitMEX funding
# ---------------------------------------------------------------------------


async def _sync_bitmex_one_asset(
    bitmex: BitmexFundingClient,
    asset_row,
    backfill_days: int,
    delay_s: float,
    sem: asyncio.Semaphore,
) -> tuple[int, datetime]:
    symbol = asset_row.bitmex_symbol
    existing_latest = await repository.get_latest_funding_timestamp("bitmex", symbol)
    if existing_latest is not None:
        start_time = existing_latest + timedelta(seconds=1)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
    else:
        start_time = datetime.now(UTC) - timedelta(days=backfill_days)

    asset_new = 0
    last_ts = datetime(1970, 1, 1, tzinfo=UTC)
    page = 0
    async with sem:
        while True:
            batch = await bitmex.fetch_funding_history(
                symbol=symbol, start_time=start_time, start=page * _PAGE_SIZE
            )
            if not batch:
                break
            rows = []
            for rec in batch:
                rate = rec.get("fundingRate")
                ts_raw = rec.get("timestamp")
                if rate is None or ts_raw is None:
                    continue
                ts = _parse_iso(ts_raw)
                rows.append(
                    {
                        "exchange": "bitmex",
                        "asset": asset_row.asset,
                        "venue_symbol": symbol,
                        "timestamp": ts.replace(tzinfo=None),
                        "funding_rate": float(rate),
                        "interval_hours": _BITMEX_FUNDING_INTERVAL_HOURS,
                    }
                )
                if ts > last_ts:
                    last_ts = ts
            inserted = await repository.bulk_upsert_funding_rates(rows)
            asset_new += inserted
            if len(batch) < _PAGE_SIZE:
                break
            page += 1
            await asyncio.sleep(delay_s)
    return asset_new, last_ts


async def _sync_bitmex_funding(
    bitmex: BitmexFundingClient,
    assets_filter: set[str] | None,
    backfill_days: int,
    delay_s: float,
) -> dict:
    assets = await repository.get_active_assets()
    filtered = [
        a
        for a in assets
        if a.bitmex_symbol and (not assets_filter or a.asset in assets_filter)
    ]

    sem = asyncio.Semaphore(_ASSET_CONCURRENCY)
    results = await asyncio.gather(
        *[_sync_bitmex_one_asset(bitmex, a, backfill_days, delay_s, sem) for a in filtered]
    )

    total_new = sum(r[0] for r in results)
    last_ts = max((r[1] for r in results), default=datetime(1970, 1, 1, tzinfo=UTC))

    cursor = await repository.get_sync_cursor("bitmex_funding")
    total_rows = (cursor.total_rows if cursor else 0) + total_new
    await repository.set_sync_cursor("bitmex_funding", last_ts, total_rows)
    return {"new_rows": total_new, "total_rows": total_rows}


# ---------------------------------------------------------------------------
# HyperLiquid funding
# ---------------------------------------------------------------------------


async def _sync_hl_one_asset(
    hl: HyperliquidFundingClient,
    asset_row,
    backfill_days: int,
    delay_s: float,
    sem: asyncio.Semaphore,
) -> tuple[int, datetime]:
    coin = asset_row.hyperliquid_name
    existing_latest = await repository.get_latest_funding_timestamp("hyperliquid", coin)
    if existing_latest is not None:
        start_time = existing_latest + timedelta(seconds=1)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
    else:
        start_time = datetime.now(UTC) - timedelta(days=backfill_days)

    asset_new = 0
    last_ts = datetime(1970, 1, 1, tzinfo=UTC)
    async with sem:
        while True:
            batch = await hl.fetch_funding_history(coin=coin, start_time=start_time)
            if not batch:
                break
            rows = []
            max_ts_this_batch = start_time
            for rec in batch:
                rate = rec.get("fundingRate")
                time_ms = rec.get("time")
                if rate is None or time_ms is None:
                    continue
                ts = _from_ms(time_ms)
                rows.append(
                    {
                        "exchange": "hyperliquid",
                        "asset": asset_row.asset,
                        "venue_symbol": coin,
                        "timestamp": ts.replace(tzinfo=None),
                        "funding_rate": float(rate),
                        "interval_hours": _HL_FUNDING_INTERVAL_HOURS,
                    }
                )
                if ts > last_ts:
                    last_ts = ts
                if ts > max_ts_this_batch:
                    max_ts_this_batch = ts
            inserted = await repository.bulk_upsert_funding_rates(rows)
            asset_new += inserted
            if len(batch) < _PAGE_SIZE:
                break
            # Page by advancing start_time past the last batch's last record.
            if max_ts_this_batch <= start_time:
                break
            start_time = max_ts_this_batch + timedelta(seconds=1)
            await asyncio.sleep(delay_s)
    return asset_new, last_ts


async def _sync_hl_funding(
    hl: HyperliquidFundingClient,
    assets_filter: set[str] | None,
    backfill_days: int,
    delay_s: float,
) -> dict:
    assets = await repository.get_active_assets()
    filtered = [
        a
        for a in assets
        if a.hyperliquid_name and (not assets_filter or a.asset in assets_filter)
    ]

    sem = asyncio.Semaphore(_ASSET_CONCURRENCY)
    results = await asyncio.gather(
        *[_sync_hl_one_asset(hl, a, backfill_days, delay_s, sem) for a in filtered]
    )

    total_new = sum(r[0] for r in results)
    last_ts = max((r[1] for r in results), default=datetime(1970, 1, 1, tzinfo=UTC))

    cursor = await repository.get_sync_cursor("hl_funding")
    total_rows = (cursor.total_rows if cursor else 0) + total_new
    await repository.set_sync_cursor("hl_funding", last_ts, total_rows)
    return {"new_rows": total_new, "total_rows": total_rows}


# ---------------------------------------------------------------------------
# Binance margin USD borrow rates (30-day windows per call)
# ---------------------------------------------------------------------------


async def _sync_binance_margin_rates(
    binance: BinanceClient,
    margin_assets: list[str],
    backfill_days: int,
    vip_level: int,
    delay_s: float,
) -> dict:
    if not binance.authed:
        logger.info("binance margin-rate sync skipped — no API key")
        return {"new_rows": 0, "total_rows": 0, "skipped": True}

    total_new = 0
    last_ts = datetime(1970, 1, 1, tzinfo=UTC)

    for asset in margin_assets:
        existing_latest = await repository.get_latest_binance_margin_timestamp(asset, vip_level)
        if existing_latest is not None:
            start = existing_latest + timedelta(seconds=1)
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
        else:
            start = datetime.now(UTC) - timedelta(days=backfill_days)

        # Binance caps the endpoint to 30-day windows.
        window_end = datetime.now(UTC)
        cur = start
        while cur < window_end:
            chunk_end = min(cur + timedelta(days=29), window_end)
            batch = await binance.fetch_margin_interest_rate_history(
                asset=asset,
                start_time=cur,
                end_time=chunk_end,
                vip_level=vip_level,
            )
            for rec in batch:
                ts_ms = rec.get("timestamp")
                rate = rec.get("dailyInterestRate")
                if ts_ms is None or rate is None:
                    continue
                ts = _from_ms(ts_ms)
                inserted = await repository.upsert_binance_margin_rate(
                    asset=asset,
                    timestamp=ts.replace(tzinfo=None),
                    daily_interest_rate=float(rate),
                    vip_level=vip_level,
                )
                if inserted:
                    total_new += 1
                if ts > last_ts:
                    last_ts = ts
            cur = chunk_end + timedelta(seconds=1)
            await asyncio.sleep(delay_s)

    cursor = await repository.get_sync_cursor("binance_margin_rate")
    total_rows = (cursor.total_rows if cursor else 0) + total_new
    await repository.set_sync_cursor("binance_margin_rate", last_ts, total_rows)
    return {"new_rows": total_new, "total_rows": total_rows}


# ---------------------------------------------------------------------------
# Binance borrow history (authed-only; user-scoped)
# ---------------------------------------------------------------------------


async def _sync_binance_borrow_history(
    binance: BinanceClient,
    margin_assets: list[str],
    backfill_days: int,
    delay_s: float,
) -> dict:
    if not binance.authed:
        return {"new_rows": 0, "skipped": True}

    new_rows = 0
    last_ts = datetime(1970, 1, 1, tzinfo=UTC)

    cursor = await repository.get_sync_cursor("binance_borrow")
    if cursor is not None:
        start = cursor.last_synced_at
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
    else:
        start = datetime.now(UTC) - timedelta(days=backfill_days)

    for asset in margin_assets:
        for kind in ("BORROW", "REPAY"):
            batch = await binance.fetch_cross_margin_borrow_repay_history(
                asset=asset, start_time=start, type_=kind
            )
            for rec in batch:
                tx_id = str(rec.get("txId") or rec.get("id") or "")
                ts_ms = rec.get("timestamp") or rec.get("time")
                principal = rec.get("principal") or rec.get("amount") or 0
                if not tx_id or ts_ms is None:
                    continue
                ts = _from_ms(ts_ms)
                signed_principal = float(principal) if kind == "BORROW" else -float(principal)
                inserted = await repository.upsert_borrow_event(
                    tx_id=tx_id,
                    timestamp=ts.replace(tzinfo=None),
                    asset=asset,
                    principal=signed_principal,
                    interest=0.0,
                    status=str(rec.get("status") or kind),
                )
                if inserted:
                    new_rows += 1
                if ts > last_ts:
                    last_ts = ts
            await asyncio.sleep(delay_s)

        interest_batch = await binance.fetch_margin_interest_history(asset=asset, start_time=start)
        for rec in interest_batch:
            tx_id = f"interest_{rec.get('txId') or rec.get('id') or rec.get('interestAccruedTime') or ''}"
            ts_ms = rec.get("interestAccruedTime") or rec.get("timestamp")
            interest = rec.get("interest") or 0
            if not tx_id or ts_ms is None:
                continue
            ts = _from_ms(ts_ms)
            inserted = await repository.upsert_borrow_event(
                tx_id=tx_id,
                timestamp=ts.replace(tzinfo=None),
                asset=asset,
                principal=0.0,
                interest=float(interest),
                status="INTEREST",
            )
            if inserted:
                new_rows += 1
            if ts > last_ts:
                last_ts = ts
        await asyncio.sleep(delay_s)

    await repository.set_sync_cursor(
        "binance_borrow",
        last_ts if last_ts.year > 1970 else datetime.now(UTC),
        new_rows + (cursor.total_rows if cursor else 0),
    )
    return {"new_rows": new_rows}


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


async def run_sync(
    bitmex: BitmexFundingClient,
    hyperliquid: HyperliquidFundingClient,
    binance: BinanceClient,
    backfill_days: int,
    margin_assets: list[str],
    universe_stale_days: int,
    vip_level: int,
    delay_s: float,
    assets_filter: set[str] | None = None,
) -> dict:
    """Run all sync stages. Returns a stats dict per source including elapsed times."""
    t0 = time.perf_counter()

    universe_stats = await _refresh_universe_if_stale(
        bitmex, hyperliquid, binance, stale_after_days=universe_stale_days
    )

    t1 = time.perf_counter()
    bitmex_stats, hl_stats = await asyncio.gather(
        _sync_bitmex_funding(bitmex, assets_filter, backfill_days, delay_s),
        _sync_hl_funding(hyperliquid, assets_filter, backfill_days, delay_s),
    )
    t2 = time.perf_counter()

    binance_rate_stats = await _sync_binance_margin_rates(
        binance, margin_assets, backfill_days, vip_level, delay_s
    )
    borrow_stats = await _sync_binance_borrow_history(binance, margin_assets, backfill_days, delay_s)

    t3 = time.perf_counter()
    logger.info(
        "sync complete",
        universe_s=round(t1 - t0, 2),
        funding_s=round(t2 - t1, 2),
        binance_s=round(t3 - t2, 2),
        total_s=round(t3 - t0, 2),
    )

    return {
        "universe": universe_stats,
        "bitmex_funding": bitmex_stats,
        "hl_funding": hl_stats,
        "binance_margin_rate": binance_rate_stats,
        "binance_borrow": borrow_stats,
        "elapsed_s": {
            "universe": round(t1 - t0, 2),
            "funding": round(t2 - t1, 2),
            "binance": round(t3 - t2, 2),
            "total": round(t3 - t0, 2),
        },
    }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def _load_config() -> dict:
    path = Path(__file__).parent.parent / "config" / "settings.toml"
    with open(path, "rb") as f:
        return tomllib.load(f)


async def _cli_main(args: argparse.Namespace) -> None:
    load_dotenv(Path(__file__).parent.parent / "config" / ".env")
    cfg = _load_config().get("funding_analysis", {})

    db_url = cfg.get("db_url", "sqlite+aiosqlite:///data/funding_analysis.db")
    margin_assets = cfg.get("binance_margin_assets", ["USDT", "USDC"])
    universe_stale_days = int(cfg.get("asset_universe_refresh_days", 7))
    hl_url = cfg.get("hyperliquid_api_url", "https://api.hyperliquid.xyz")
    vip_level = int(cfg.get("binance_vip_level", 0))
    delay_s = float(cfg.get("sync_interval_delay_ms", 250)) / 1000.0

    await init_db(db_url)

    bitmex = BitmexFundingClient(testnet=False)
    hyperliquid = HyperliquidFundingClient(base_url=hl_url)
    binance = BinanceClient(
        api_key=os.environ.get("BINANCE_READONLY_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_READONLY_API_SECRET", ""),
    )

    assets_filter = None
    if args.assets:
        assets_filter = {a.strip().upper() for a in args.assets.split(",") if a.strip()}

    try:
        stats = await run_sync(
            bitmex=bitmex,
            hyperliquid=hyperliquid,
            binance=binance,
            backfill_days=args.backfill_days,
            margin_assets=margin_assets,
            universe_stale_days=universe_stale_days,
            vip_level=vip_level,
            delay_s=delay_s,
            assets_filter=assets_filter,
        )
        print("sync complete:")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    finally:
        await bitmex.close()
        await hyperliquid.close()
        await binance.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an incremental funding_analysis sync")
    parser.add_argument("--backfill-days", type=int, default=90, help="initial backfill window")
    parser.add_argument(
        "--assets",
        type=str,
        default="",
        help="Comma-separated canonical assets to restrict sync to (e.g. BTC,ETH)",
    )
    args = parser.parse_args()
    try:
        asyncio.run(_cli_main(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
