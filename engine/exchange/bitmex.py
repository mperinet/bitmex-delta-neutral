"""
BitMEX exchange implementation.

Uses ccxt for all order placement, ticker data, and funding endpoints.
Uses ccxt's private_post_* dynamic methods for BitMEX-specific endpoints
(e.g. cancelAllAfter dead-man's switch) that aren't in the unified API.

Inverse contract math (XBTUSD and similar):
  PnL (BTC) = notional_usd * (1/entry_price - 1/exit_price)
  Hedge ratio: to hedge $N USD notional on the perp, hold $N / spot_price BTC on spot.
  This ratio drifts as spot moves — position_tracker triggers rebalancing.

Rate limit headers:
  X-RateLimit-Remaining: remaining requests in current 5-min window.
  Consumed by order_manager on startup to initialize the token bucket.
"""

from __future__ import annotations

import logging
from datetime import datetime

import ccxt.async_support as ccxt

from engine.exchange.base import Balance, ExchangeBase, OrderBook, OrderResult, Ticker

logger = logging.getLogger(__name__)

TESTNET_REST = "https://testnet.bitmex.com"
LIVE_REST = "https://www.bitmex.com"
TESTNET_WS = "wss://ws.testnet.bitmex.com/realtime"
LIVE_WS = "wss://ws.bitmex.com/realtime"


class BitMEXExchange(ExchangeBase):
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.testnet = testnet
        self._base_url = TESTNET_REST if testnet else LIVE_REST
        self.ws_url = TESTNET_WS if testnet else LIVE_WS
        self._last_rate_limit_remaining: int = 300

        self._ccxt = ccxt.bitmex(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": False,  # we manage our own token bucket
                "options": {"defaultType": "swap"},
            }
        )
        if testnet:
            self._ccxt.set_sandbox_mode(True)

    # ------------------------------------------------------------------
    # ExchangeBase: order placement (ccxt)
    # ------------------------------------------------------------------

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        post_only: bool = True,
    ) -> OrderResult:
        params = {"execInst": "ParticipateDoNotInitiate"} if post_only else {}
        raw = await self._ccxt.create_order(symbol, "limit", side, qty, price, params)
        self._update_rate_limit(raw)
        return self._parse_order(raw)

    async def place_market_order(self, symbol: str, side: str, qty: float) -> OrderResult:
        raw = await self._ccxt.create_order(symbol, "market", side, qty)
        self._update_rate_limit(raw)
        return self._parse_order(raw)

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        try:
            await self._ccxt.cancel_order(order_id, symbol)
            return True
        except ccxt.OrderNotFound:
            logger.warning("cancel_order: %s not found (already filled?)", order_id)
            return False

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        params = {"symbol": symbol} if symbol else {}
        result = await self._ccxt.cancel_all_orders(symbol, params)
        return len(result) if isinstance(result, list) else 0

    async def get_order(self, order_id: str, symbol: str) -> OrderResult:
        raw = await self._ccxt.fetch_order(order_id, symbol)
        return self._parse_order(raw)

    async def get_ticker(self, symbol: str) -> Ticker:
        raw = await self._ccxt.fetch_ticker(symbol)
        return Ticker(
            symbol=symbol,
            bid=raw["bid"],
            ask=raw["ask"],
            last=raw["last"],
            mark_price=float(raw.get("info", {}).get("markPrice") or raw["last"]),
        )

    async def get_balance(self) -> Balance:
        raw = await self._ccxt.fetch_balance()
        # BitMEX testnet wallet is in XBt (satoshis); ccxt converts to BTC
        btc = raw.get("BTC", {})
        return Balance(
            available=btc.get("free", 0.0),
            total=btc.get("total", 0.0),
            currency="BTC",
        )

    async def get_open_positions(self) -> list[dict]:
        """Return raw position data for WS reconnect reconciliation."""
        positions = await self._ccxt.fetch_positions()
        return [p for p in positions if p.get("contracts", 0) != 0]

    async def fetch_orderbook(self, symbol: str, depth: int = 25) -> OrderBook:
        raw = await self._ccxt.fetch_order_book(symbol, limit=depth)
        return OrderBook(symbol=symbol, bids=raw["bids"], asks=raw["asks"])

    # ------------------------------------------------------------------
    # BitMEX-specific: funding rates and settlement
    # ------------------------------------------------------------------

    async def get_funding_rate(self, symbol: str) -> dict:
        """
        Return current funding rate data for a perpetual contract.
        Uses ccxt fetch_funding_rate which maps to GET /instrument.
        Returns: {rate, indicative_rate, next_funding_time}
        """
        raw = await self._ccxt.fetch_funding_rate(symbol)
        return {
            "rate": raw["fundingRate"],
            "indicative_rate": raw.get("estimatedSettlePrice"),
            "next_funding_time": raw.get("fundingDatetime"),
        }

    async def get_historical_funding(self, symbol: str, limit: int = 100) -> list[dict]:
        """Fetch historical funding rates. Used by backfill script."""
        raw = await self._ccxt.fetch_funding_rate_history(symbol, limit=limit)
        return [
            {
                "symbol": symbol,
                "timestamp": r["datetime"],
                "rate": r["fundingRate"],
            }
            for r in raw
        ]

    async def get_active_futures(self) -> list[dict]:
        """Return all active futures (FFCCSX type) for the given underlying."""
        markets = await self._ccxt.load_markets()
        return [
            m for m in markets.values() if m.get("future") and m.get("active") and not m.get("swap")
        ]

    async def get_settlement_date(self, symbol: str) -> datetime | None:
        """Return the expiry datetime for a futures contract, or None for perps."""
        markets = await self._ccxt.load_markets()
        market = markets.get(symbol)
        if market and market.get("expiry"):
            return datetime.fromtimestamp(market["expiry"] / 1000)
        return None

    async def get_rate_limit_remaining(self) -> int:
        """
        Make a cheap REST call to fetch X-RateLimit-Remaining header.
        Called on startup to initialize the order_manager token bucket
        with the actual remaining tokens (not a stale 300 after crash).
        """
        try:
            await self._ccxt.fetch_ticker("BTC/USD:BTC")
            remaining = self._ccxt.last_response_headers.get("x-ratelimit-remaining", 300)
            self._last_rate_limit_remaining = int(remaining)
        except Exception as e:
            logger.warning("Could not fetch rate limit remaining: %s", e)
        return self._last_rate_limit_remaining

    # ------------------------------------------------------------------
    # Inverse contract math
    # ------------------------------------------------------------------

    @staticmethod
    def inverse_pnl(notional_usd: float, entry_price: float, exit_price: float) -> float:
        """
        PnL in BTC for an inverse contract (e.g. XBTUSD).

        PnL = notional * (1/entry - 1/exit)

        Positive for longs when exit > entry.
        Negative for longs when exit < entry.
        """
        if entry_price <= 0 or exit_price <= 0:
            raise ValueError("Prices must be positive")
        return notional_usd * (1.0 / entry_price - 1.0 / exit_price)

    @staticmethod
    def compute_hedge_ratio(perp_usd_notional: float, spot_price: float) -> float:
        """
        BTC quantity needed to hedge a given USD notional on the inverse perp.

        Since XBTUSD is inverse (BTC-settled), 1 contract = $1 USD notional.
        To be delta-neutral: hold (notional_usd / spot_price) BTC on spot.

        This ratio drifts as spot moves — caller is responsible for
        triggering rebalancing when drift exceeds threshold.
        """
        if spot_price <= 0:
            raise ValueError("Spot price must be positive")
        return perp_usd_notional / spot_price

    @staticmethod
    def compute_annualised_basis(
        future_price: float, spot_price: float, days_to_expiry: int
    ) -> float:
        """
        Annualised basis for a cash-and-carry trade.

        basis = (future / spot - 1) * (365 / days_to_expiry)
        """
        if spot_price <= 0 or days_to_expiry <= 0:
            raise ValueError("Spot price and days to expiry must be positive")
        return (future_price / spot_price - 1) * (365.0 / days_to_expiry)

    # ------------------------------------------------------------------
    # Dead-man's switch
    # ------------------------------------------------------------------

    async def cancel_all_after(self, timeout_ms: int) -> None:
        """
        POST /order/cancelAllAfter — BitMEX dead-man's switch.
        All open orders are cancelled if this is not refreshed within timeout_ms.
        timeout_ms=0 disables (cancels all orders immediately).
        """
        await self._ccxt.private_post_order_cancelallafter({"timeout": timeout_ms})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_order(self, raw: dict) -> OrderResult:
        return OrderResult(
            order_id=raw["id"],
            symbol=raw["symbol"],
            side=raw["side"],
            qty=raw["amount"] or 0.0,
            filled_qty=raw["filled"] or 0.0,
            avg_price=raw.get("average") or raw.get("price") or 0.0,
            status=raw["status"],
            fee=raw.get("fee", {}).get("cost", 0.0) if raw.get("fee") else 0.0,
        )

    def _update_rate_limit(self, response: dict) -> None:
        """Track remaining rate limit from response headers."""
        headers = getattr(self._ccxt, "last_response_headers", {}) or {}
        remaining = headers.get("x-ratelimit-remaining")
        if remaining is not None:
            self._last_rate_limit_remaining = int(remaining)

    async def close(self) -> None:
        await self._ccxt.close()
