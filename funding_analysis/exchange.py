"""
Thin BitMEX API client for account-level data fetching.

Uses only private GET endpoints — no order placement, no WebSocket.
Designed for readonly API keys.

Endpoints used:
  GET /execution  (execType=Funding)  — account funding settlements
  GET /execution  (execType=Trade)    — trade executions with fees
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import structlog

logger = structlog.get_logger(__name__)

_PAGE_SIZE = 500


class FundingAnalysisClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self._ccxt = ccxt.bitmex(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,  # let ccxt throttle — read-only, no urgency
                "options": {"defaultType": "swap"},
            }
        )
        if testnet:
            self._ccxt.set_sandbox_mode(True)
        self.testnet = testnet

    async def close(self) -> None:
        await self._ccxt.close()

    async def fetch_funding_executions(
        self,
        start_time: datetime,
        start: int = 0,
    ) -> list[dict]:
        """
        Fetch funding payment executions (execType=Funding).

        Returns up to _PAGE_SIZE records starting from start_time at the
        given row offset. Oldest first (reverse=False).
        """
        params = {
            "filter": json.dumps({"execType": "Funding"}),
            "count": _PAGE_SIZE,
            "start": start,
            "reverse": False,
            "startTime": _fmt_ts(start_time),
        }
        try:
            raw = await self._ccxt.private_get_execution(params)
        except Exception as e:
            logger.error("fetch_funding_executions failed", error=str(e))
            raise
        return raw if isinstance(raw, list) else []

    async def fetch_trade_executions(
        self,
        start_time: datetime,
        start: int = 0,
    ) -> list[dict]:
        """
        Fetch trade executions (execType=Trade) with their fees.

        Returns up to _PAGE_SIZE records starting from start_time at the
        given row offset. Oldest first (reverse=False).
        """
        params = {
            "filter": json.dumps({"execType": "Trade"}),
            "count": _PAGE_SIZE,
            "start": start,
            "reverse": False,
            "startTime": _fmt_ts(start_time),
        }
        try:
            raw = await self._ccxt.private_get_execution(params)
        except Exception as e:
            logger.error("fetch_trade_executions failed", error=str(e))
            raise
        return raw if isinstance(raw, list) else []


def _fmt_ts(dt: datetime) -> str:
    """Format a datetime as BitMEX startTime string (ISO 8601 UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
