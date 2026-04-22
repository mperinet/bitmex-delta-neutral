"""
Public BitMEX client for funding analysis.

Uses only public endpoints — no API key required. Completely separate ccxt
instance from `engine/exchange/bitmex.py` to keep funding_analysis isolated.

Endpoints used:
  GET /instrument/active         — list of live perps + quarterlies
  GET /funding?symbol=...        — historical 8h funding rates
  GET /instrument?symbol=...     — current settled + indicative funding rate
"""

from __future__ import annotations

from datetime import UTC, datetime

import ccxt.async_support as ccxt
import structlog

logger = structlog.get_logger(__name__)

_PAGE_SIZE = 500


class BitmexFundingClient:
    def __init__(self, testnet: bool = False):
        self._ccxt = ccxt.bitmex(
            {
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        if testnet:
            self._ccxt.set_sandbox_mode(True)
        self.testnet = testnet

    async def close(self) -> None:
        await self._ccxt.close()

    async def list_active_contracts(self) -> list[dict]:
        """
        Return all currently-live BitMEX contracts (perps + quarterly futures).

        Each item: {symbol, typ, rootSymbol, underlying, quoteCurrency, expiry,
                    foreignNotional24h, fundingInterval}.
        """
        try:
            raw = await self._ccxt.public_get_instrument_active()
        except Exception as e:
            logger.error("bitmex list_active_contracts failed", error=str(e))
            raise
        return raw if isinstance(raw, list) else []

    async def fetch_funding_history(
        self,
        symbol: str,
        start_time: datetime,
        start: int = 0,
    ) -> list[dict]:
        """
        Fetch up to _PAGE_SIZE historical funding records for `symbol`, oldest first.

        Each record: {timestamp, symbol, fundingInterval, fundingRate,
                       fundingRateDaily}.
        """
        params = {
            "symbol": symbol,
            "count": _PAGE_SIZE,
            "start": start,
            "reverse": False,
            "startTime": _fmt_ts(start_time),
        }
        try:
            raw = await self._ccxt.public_get_funding(params)
        except Exception as e:
            logger.error("bitmex fetch_funding_history failed", symbol=symbol, error=str(e))
            raise
        return raw if isinstance(raw, list) else []

    async def fetch_current_funding(self, symbol: str) -> dict | None:
        """Return {fundingRate, indicativeFundingRate, fundingTimestamp} from /instrument."""
        try:
            raw = await self._ccxt.public_get_instrument({"symbol": symbol})
        except Exception as e:
            logger.error("bitmex fetch_current_funding failed", symbol=symbol, error=str(e))
            return None
        if not isinstance(raw, list) or not raw:
            return None
        return raw[0]


def _fmt_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
