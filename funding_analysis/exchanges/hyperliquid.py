"""
HyperLiquid public info client.

HL exposes a single POST endpoint (`/info`) for all public market data.
ccxt's HL support is thin for funding history, so we call the API directly
via aiohttp. No auth required for any of these endpoints.

Endpoints used (all POST with JSON body):
  {"type": "meta"}              — perp universe (names, szDecimals, maxLeverage)
  {"type": "fundingHistory",
   "coin": "<name>",
   "startTime": ms,
   "endTime": ms}               — hourly funding rates (max 500 per response)
  {"type": "predictedFundings"} — current predicted funding per coin per venue
"""

from __future__ import annotations

from datetime import UTC, datetime

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

_DEFAULT_URL = "https://api.hyperliquid.xyz"
_PAGE_SIZE = 500  # HL caps fundingHistory at 500 records per response


class HyperliquidFundingClient:
    def __init__(self, base_url: str = _DEFAULT_URL, timeout_s: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _post_info(self, body: dict) -> object:
        session = await self._ensure_session()
        url = f"{self.base_url}/info"
        async with session.post(url, json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def list_perp_universe(self) -> list[dict]:
        """Return HL's perp universe: list of {name, szDecimals, maxLeverage, ...}."""
        try:
            raw = await self._post_info({"type": "meta"})
        except Exception as e:
            logger.error("hyperliquid list_perp_universe failed", error=str(e))
            raise
        if isinstance(raw, dict):
            universe = raw.get("universe", [])
            return universe if isinstance(universe, list) else []
        return []

    async def fetch_funding_history(
        self,
        coin: str,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> list[dict]:
        """
        Fetch hourly funding history for `coin` in the [start_time, end_time] range.

        HL caps responses at 500 records (~20d at hourly). Caller iterates by
        moving start_time forward past the last returned timestamp when the
        response is full.

        Each record: {coin, fundingRate, premium, time}  (time is ms epoch).
        """
        start_ms = _to_ms(start_time)
        body: dict = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
        if end_time is not None:
            body["endTime"] = _to_ms(end_time)
        try:
            raw = await self._post_info(body)
        except Exception as e:
            logger.error("hyperliquid fetch_funding_history failed", coin=coin, error=str(e))
            raise
        return raw if isinstance(raw, list) else []

    async def fetch_predicted_fundings(self) -> list[list]:
        """
        Return HL's current predicted funding per coin per venue.

        Payload shape: [[coin, [[venue, {fundingRate, nextFundingTime, ...}], ...]], ...]
        """
        try:
            raw = await self._post_info({"type": "predictedFundings"})
        except Exception as e:
            logger.error("hyperliquid fetch_predicted_fundings failed", error=str(e))
            return []
        return raw if isinstance(raw, list) else []


def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)
