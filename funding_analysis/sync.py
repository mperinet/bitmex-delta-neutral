"""
Incremental sync: fetch new funding payments and execution fees from
BitMEX and store them in funding_analysis.db.

On first run: fetches all history from the BitMEX genesis date.
On subsequent runs: fetches only records since the last sync cursor.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from funding_analysis.db import repository
from funding_analysis.exchange import FundingAnalysisClient

logger = structlog.get_logger(__name__)

# BitMEX mainnet launched 2014-11-22; use as epoch for first-run full backfill.
_EPOCH = datetime(2014, 11, 22, tzinfo=timezone.utc)

_PAGE_SIZE = 500


def _parse_ts(raw: str | None) -> datetime:
    """Parse a BitMEX timestamp string to an aware UTC datetime."""
    if not raw:
        return datetime.now(timezone.utc)
    raw = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def _sync_funding(client: FundingAnalysisClient) -> dict:
    cursor = await repository.get_sync_cursor("funding")
    start_time = cursor.last_synced_at if cursor else _EPOCH
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    new_rows = 0
    last_ts = start_time
    page = 0

    logger.info("syncing funding payments", start_time=start_time.isoformat())

    while True:
        batch = await client.fetch_funding_executions(start_time=start_time, start=page * _PAGE_SIZE)
        if not batch:
            break

        for row in batch:
            ts = _parse_ts(row.get("timestamp"))
            inserted = await repository.upsert_funding_payment(
                exec_id=row["execID"],
                symbol=row.get("symbol", ""),
                timestamp=ts,
                fee_amount=int(row.get("realisedPnl") or 0),
                fee_currency=row.get("settlCurrency", "XBt"),
                last_qty=float(row.get("lastQty") or 0),
            )
            if inserted:
                new_rows += 1
            if ts > last_ts:
                last_ts = ts

        if len(batch) < _PAGE_SIZE:
            break
        page += 1

    total = (cursor.total_rows if cursor else 0) + new_rows
    await repository.set_sync_cursor("funding", last_ts, total)

    logger.info("funding sync complete", new_rows=new_rows, total_rows=total)
    return {"new_rows": new_rows, "total_rows": total, "last_synced_at": last_ts}


async def _sync_fees(client: FundingAnalysisClient) -> dict:
    cursor = await repository.get_sync_cursor("execution")
    start_time = cursor.last_synced_at if cursor else _EPOCH
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    new_rows = 0
    last_ts = start_time
    page = 0

    logger.info("syncing execution fees", start_time=start_time.isoformat())

    while True:
        batch = await client.fetch_trade_executions(start_time=start_time, start=page * _PAGE_SIZE)
        if not batch:
            break

        for row in batch:
            ts = _parse_ts(row.get("timestamp"))
            inserted = await repository.upsert_execution_fee(
                exec_id=row["execID"],
                order_id=row.get("orderID"),
                symbol=row.get("symbol", ""),
                side=row.get("side"),
                last_qty=float(row.get("lastQty") or 0),
                last_px=float(row.get("lastPx") or 0),
                fee_amount=int(row.get("execFee") or 0),
                fee_currency=row.get("currency", "XBt"),
                timestamp=ts,
            )
            if inserted:
                new_rows += 1
            if ts > last_ts:
                last_ts = ts

        if len(batch) < _PAGE_SIZE:
            break
        page += 1

    total = (cursor.total_rows if cursor else 0) + new_rows
    await repository.set_sync_cursor("execution", last_ts, total)

    logger.info("execution fee sync complete", new_rows=new_rows, total_rows=total)
    return {"new_rows": new_rows, "total_rows": total, "last_synced_at": last_ts}


async def run_sync(client: FundingAnalysisClient) -> dict:
    """
    Run incremental sync for all data types.

    Returns a stats dict:
      {
        "funding":   {"new_rows": int, "total_rows": int, "last_synced_at": datetime},
        "execution": {"new_rows": int, "total_rows": int, "last_synced_at": datetime},
      }
    """
    funding_stats = await _sync_funding(client)
    fee_stats = await _sync_fees(client)
    return {"funding": funding_stats, "execution": fee_stats}
