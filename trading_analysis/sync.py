"""
Incremental sync: fetch new funding payments and execution fees from
BitMEX and store them in trading_analysis.db.

On first run: fetches all history from the BitMEX genesis date.
On subsequent runs: fetches only records since the last sync cursor.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from trading_analysis.db import repository
from trading_analysis.exchange import FundingAnalysisClient

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
            # realisedPnl is the authoritative field for recent records (post-~Nov 2024).
            # For older records realisedPnl is null, but execComm is always populated
            # and holds the funding payment in the smallest currency unit.
            # Sign convention: realisedPnl follows P&L sign (negative=paid, positive=received).
            #                  execComm follows commission sign (positive=paid, negative=received).
            # So fee_amount = realisedPnl  OR  -execComm  (negate to align signs).
            realised = row.get("realisedPnl")
            if realised is not None:
                fee_amount = int(realised)
            else:
                exec_comm = row.get("execComm")
                fee_amount = -int(exec_comm) if exec_comm is not None else 0
            raw_rate = row.get("commission")
            inserted = await repository.upsert_funding_payment(
                exec_id=row["execID"],
                symbol=row.get("symbol", ""),
                timestamp=ts,
                fee_amount=fee_amount,
                fee_currency=row.get("settlCurrency", "XBt"),
                last_qty=float(row.get("lastQty") or 0),
                funding_rate=float(raw_rate) if raw_rate is not None else None,
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
                fee_amount=int(row.get("execComm") or 0),
                fee_currency=row.get("settlCurrency") or "XBt",
                timestamp=ts,
                realised_pnl=int(row.get("realisedPnl") or 0),
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


async def backfill_funding_from_wallet(client: FundingAnalysisClient) -> dict:
    """
    Patch funding_payments rows that still have fee_amount=0 by cross-referencing
    /user/walletHistory (transactType=RealisedPNL).

    BitMEX /user/walletHistory records every funding settlement with the correct
    `amount` field going back to account genesis, unlike /execution which omits
    realisedPnl for records older than ~Nov 2024.

    NOTE: /user/walletHistory does NOT support startTime or reverse parameters —
    only currency, count, and start. We paginate forward through all records.
    BitMEX returns them newest-first by default, so we stop once we've gone past
    the oldest zero-amount funding record in the DB.

    BitMEX renamed the funding wallet entry type from "RealisedPNL" (pre-mid-2024)
    to "Funding" (post-mid-2024). We accept both.

    The wallet history `address` field contains the contract symbol (e.g. "XBTUSD").
    We match wallet entries to existing funding_payments rows by (symbol, timestamp)
    within a ±30-second window and update fee_amount where it is currently 0.

    Returns {"patched": int, "total_wallet_rows": int}.
    """
    # Find the oldest zero-amount record so we know when to stop paginating.
    earliest_zero = await repository.get_earliest_zero_funding_timestamp()
    if earliest_zero is None:
        logger.info("no zero-amount funding records found — wallet backfill skipped")
        return {"patched": 0, "total_wallet_rows": 0}

    # Make naive for comparison (DB stores naive UTC).
    if earliest_zero.tzinfo is not None:
        earliest_zero = earliest_zero.replace(tzinfo=None)

    logger.info("wallet backfill starting", earliest_zero=earliest_zero.isoformat())

    # BitMEX settles funding in two currencies; fetch both.
    currencies = [("XBt", "XBt"), ("USDt", "USDt")]

    updates: list[tuple[str, datetime, int, str]] = []
    total_wallet_rows = 0

    for api_currency, store_currency in currencies:
        page = 0
        done = False
        while not done:
            batch = await client.fetch_wallet_history(
                currency=api_currency,
                start=page * _PAGE_SIZE,
            )
            if not batch:
                break

            total_wallet_rows += len(batch)

            for row in batch:
                # BitMEX renamed this type: "RealisedPNL" before ~mid-2024,
                # "Funding" after. Accept both — but for "RealisedPNL" we must
                # filter to genuine funding settlements only, since that type
                # also covers position-close PnL.
                #
                # Reliable discriminator: BitMEX funding always settles at
                # exactly 04:00:00, 12:00:00, or 20:00:00 UTC (minute=0,
                # second=0). Trading PnL can land at any arbitrary second.
                txtype = row.get("transactType")
                if txtype == "Funding":
                    pass  # always a funding entry — no extra check needed
                elif txtype == "RealisedPNL":
                    ts_str = row.get("transactTime") or row.get("timestamp") or ""
                    # ts_str format: "2023-05-17T12:00:00.000Z"
                    try:
                        hh = int(ts_str[11:13])
                        mm = int(ts_str[14:16])
                        ss = int(ts_str[17:19])
                    except (ValueError, IndexError):
                        continue
                    if mm != 0 or ss != 0 or hh not in (4, 12, 20):
                        continue  # trading PnL — skip
                else:
                    continue

                amount = int(row.get("amount") or 0)
                if amount == 0:
                    continue
                # symbol is in the `address` field
                symbol = (row.get("address") or "").strip()
                if not symbol:
                    continue
                ts = _parse_ts(row.get("transactTime") or row.get("timestamp"))
                ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
                # walletHistory is newest-first; stop once past our target range.
                if ts_naive < earliest_zero:
                    done = True
                    break
                updates.append((symbol, ts, amount, store_currency))

            if len(batch) < _PAGE_SIZE:
                break
            page += 1

        logger.info(
            "wallet history fetched",
            currency=api_currency,
            entries=sum(1 for u in updates if u[3] == store_currency),
        )

    patched = await repository.patch_zero_funding_amounts(updates)
    logger.info("wallet backfill complete", patched=patched, total_wallet_rows=total_wallet_rows)
    return {"patched": patched, "total_wallet_rows": total_wallet_rows}


_WALLET_TX_TYPES = {"Deposit", "Withdrawal", "Conversion", "Transfer"}


async def _sync_wallet_transactions(client: FundingAnalysisClient) -> dict:
    """
    Sync deposits, withdrawals, conversions and transfers from /user/walletHistory.

    Fetches newest-first (no startTime support on that endpoint) and stops as
    soon as it encounters a transact_id already in the DB — so incremental runs
    are fast once the initial full scan is done.
    """
    new_rows = 0

    for currency in ("XBt", "USDt"):
        known_id = await repository.get_latest_wallet_transact_id(currency)
        page = 0
        done = False

        while not done:
            batch = await client.fetch_wallet_history(currency=currency, start=page * _PAGE_SIZE)
            if not batch:
                break

            for row in batch:
                tid = row.get("transactID") or row.get("transactId") or ""
                if not tid:
                    continue
                # Stop once we reach a record we've already stored.
                if tid == known_id:
                    done = True
                    break
                if row.get("transactType") not in _WALLET_TX_TYPES:
                    continue
                if row.get("transactStatus") != "Completed":
                    continue
                ts = _parse_ts(row.get("transactTime") or row.get("timestamp"))
                inserted = await repository.upsert_wallet_transaction(
                    transact_id=tid,
                    transact_type=row.get("transactType", ""),
                    currency=row.get("currency", currency),
                    amount=int(row.get("amount") or 0),
                    fee=int(row.get("fee") or 0),
                    address=row.get("address"),
                    tx_hash=row.get("tx"),
                    transact_time=ts,
                    wallet_balance=int(row.get("walletBalance") or 0) if row.get("walletBalance") else None,
                )
                if inserted:
                    new_rows += 1

            if len(batch) < _PAGE_SIZE:
                break
            page += 1

    logger.info("wallet transaction sync complete", new_rows=new_rows)
    return {"new_rows": new_rows}


async def run_sync(client: FundingAnalysisClient) -> dict:
    """
    Run incremental sync for all data types.

    Returns a stats dict:
      {
        "funding":   {"new_rows": int, "total_rows": int, "last_synced_at": datetime},
        "execution": {"new_rows": int, "total_rows": int, "last_synced_at": datetime},
        "wallet":    {"new_rows": int},
      }
    """
    funding_stats = await _sync_funding(client)
    fee_stats = await _sync_fees(client)
    wallet_stats = await _sync_wallet_transactions(client)
    return {"funding": funding_stats, "execution": fee_stats, "wallet": wallet_stats}
