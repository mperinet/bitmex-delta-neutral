"""
Abstract exchange interface.

ccxt normalises the ORDER PLACEMENT API (place, cancel, fetch) and
ticker data. All funding/margin/settlement semantics live in
exchange-specific subclasses (exchange/bitmex.py).

This keeps the exchange abstraction thin — we're not trying to abstract
funding mechanics across BitMEX and Binance, just the order API surface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderResult:
    order_id: str
    symbol: str
    side: str           # "buy" | "sell"
    qty: float
    filled_qty: float
    avg_price: float
    status: str         # "open" | "closed" | "canceled"
    fee: float


@dataclass
class Ticker:
    symbol: str
    bid: float
    ask: float
    last: float
    mark_price: float


@dataclass
class Balance:
    available: float    # available margin (in BTC for inverse, USDT for linear)
    total: float        # total wallet balance
    currency: str


@dataclass
class OrderBook:
    symbol: str
    bids: list  # [[price, qty], ...] descending — qty in instrument's native units
    asks: list  # [[price, qty], ...] ascending


class ExchangeBase(ABC):
    """
    Thin ccxt wrapper for order placement and ticker data.

    Subclasses implement exchange-specific semantics:
    funding rates, margin calculations, settlement dates, hedge ratios.
    """

    @abstractmethod
    async def place_limit_order(
        self,
        symbol: str,
        side: str,          # "buy" | "sell"
        qty: float,
        price: float,
        post_only: bool = True,
    ) -> OrderResult:
        ...

    @abstractmethod
    async def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
    ) -> OrderResult:
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        ...

    @abstractmethod
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """Cancel all open orders. Returns count cancelled."""
        ...

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> OrderResult:
        ...

    @abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        ...

    @abstractmethod
    async def get_balance(self) -> Balance:
        ...

    @abstractmethod
    async def get_open_positions(self) -> list[dict]:
        """Return raw exchange position data for reconciliation."""
        ...

    @abstractmethod
    async def fetch_orderbook(self, symbol: str, depth: int = 25) -> OrderBook:
        """Return order book for slippage-aware slice sizing."""
        ...
