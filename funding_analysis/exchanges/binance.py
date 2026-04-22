"""
Binance client for funding analysis.

Two roles:
1. Public spot symbol discovery — list all Binance spot USDT / USDC pairs
   so the symbol-mapping layer can pick which assets are hedgeable on Binance.
2. Authed (readonly margin scope): historical USD borrow rates on cross
   margin + the user's own borrow/interest events. Without keys, the dashboard
   degrades gracefully (no borrow history tab, no rate history — only the
   current snapshot from ccxt's public `fetchBorrowRate`).

Note: Binance does not pay funding via "margin" — margin charges hourly
interest on borrowed assets. In this dashboard Binance is the **cost side**
of a hedge (borrow USDT → buy spot), never the funding provider.
"""

from __future__ import annotations

from datetime import UTC, datetime

import ccxt.async_support as ccxt
import structlog

logger = structlog.get_logger(__name__)


class BinanceClient:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self._authed = bool(api_key and api_secret)
        self._ccxt = ccxt.binance(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
        )

    @property
    def authed(self) -> bool:
        return self._authed

    async def close(self) -> None:
        await self._ccxt.close()

    async def list_spot_symbols(self, quotes: tuple[str, ...] = ("USDT", "USDC")) -> list[dict]:
        """
        Return Binance spot markets with quote in `quotes`.

        Each item: {symbol, base, quote, active}.
        """
        try:
            markets = await self._ccxt.load_markets()
        except Exception as e:
            logger.error("binance load_markets failed", error=str(e))
            raise
        out: list[dict] = []
        for m in markets.values():
            if not m.get("spot"):
                continue
            if not m.get("active"):
                continue
            if m.get("quote") not in quotes:
                continue
            out.append(
                {
                    "symbol": m.get("id"),
                    "base": m.get("base"),
                    "quote": m.get("quote"),
                    "active": True,
                }
            )
        return out

    async def fetch_margin_interest_rate_history(
        self,
        asset: str,
        start_time: datetime,
        end_time: datetime | None = None,
        vip_level: int = 0,
    ) -> list[dict]:
        """
        Hourly margin interest rate history. Requires authed key (readonly OK).

        Calls `GET /sapi/v1/margin/interestRateHistory` (max 30-day window per
        call per Binance docs). Each record:
            {asset, dailyInterestRate, timestamp, vipLevel}
        """
        if asset == "USDT":
            logger.warning(
                "binance margin rate history skipped — USDT not supported by interestRateHistory endpoint",
                asset=asset,
            )
            return []
        if not self._authed:
            logger.info(
                "binance margin rate history skipped — no API key",
                asset=asset,
            )
            return []
        params = {
            "asset": asset,
            "vipLevel": vip_level,
            "startTime": _to_ms(start_time),
        }
        if end_time is not None:
            params["endTime"] = _to_ms(end_time)
        try:
            raw = await self._ccxt.sapi_get_margin_interestratehistory(params)
        except Exception as e:
            logger.error("binance margin rate history failed", asset=asset, error=str(e))
            return []
        return raw if isinstance(raw, list) else []

    async def fetch_cross_margin_borrow_repay_history(
        self,
        asset: str | None,
        start_time: datetime,
        type_: str = "BORROW",  # "BORROW" or "REPAY"
    ) -> list[dict]:
        """
        User's borrow/repay history on cross margin. Requires authed key.

        Calls `GET /sapi/v1/margin/borrow-repay` (authed). Fields: {txId, asset,
        principal, timestamp, status, type}.
        """
        if not self._authed:
            return []
        params: dict = {
            "type": type_,
            "startTime": _to_ms(start_time),
            "size": 100,
        }
        if asset:
            params["asset"] = asset
        try:
            raw = await self._ccxt.sapi_get_margin_borrow_repay(params)
        except Exception as e:
            logger.warning(
                "binance borrow-repay history failed", asset=asset, type=type_, error=str(e)
            )
            return []
        if isinstance(raw, dict) and "rows" in raw:
            rows = raw.get("rows") or []
            return rows if isinstance(rows, list) else []
        return raw if isinstance(raw, list) else []

    async def fetch_margin_interest_history(
        self,
        asset: str | None,
        start_time: datetime,
    ) -> list[dict]:
        """User's margin interest accrual history. Requires authed key."""
        if not self._authed:
            return []
        params: dict = {
            "startTime": _to_ms(start_time),
            "size": 100,
        }
        if asset:
            params["asset"] = asset
        try:
            raw = await self._ccxt.sapi_get_margin_interesthistory(params)
        except Exception as e:
            logger.warning("binance margin interest history failed", asset=asset, error=str(e))
            return []
        if isinstance(raw, dict) and "rows" in raw:
            rows = raw.get("rows") or []
            return rows if isinstance(rows, list) else []
        return raw if isinstance(raw, list) else []

    async def fetch_current_margin_rate(self, asset: str) -> float | None:
        """
        Current daily borrow rate for `asset` via public ccxt `fetchBorrowRate`.

        Works without auth. Returns `dailyRate` (Binance reports daily for
        cross margin). Caller divides by 24 for hourly.
        """
        try:
            result = await self._ccxt.fetch_borrow_rate(asset)
        except Exception as e:
            logger.warning("binance fetch_current_margin_rate failed", asset=asset, error=str(e))
            return None
        # ccxt shape: {info, currency, rate, period, timestamp}
        # rate is the per-period rate matching period (usually ms=3600000 → hourly).
        period_ms = result.get("period")
        rate = result.get("rate")
        if rate is None:
            return None
        if period_ms == 3600000:
            # hourly -> convert to daily
            return float(rate) * 24
        return float(rate)


def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)
