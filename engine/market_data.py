"""
Live market data cache — populated from the WebSocket feed.

Separated from position_tracker because instrument metadata (funding rates,
mark prices) is symbol-level information, not position state.

The cache is written by PositionTracker's WS handler and read by strategies
and any component that needs live instrument data without going to REST.

Two sources, priority order for funding rate:
  1. `funding` topic — settlement event (definitive, fires at 04:00/12:00/20:00 UTC)
  2. `instrument` topic — current indicative rate, updated every few seconds
"""

from __future__ import annotations

from typing import Optional


class MarketDataCache:
    """
    In-memory store for live instrument and funding data from the WS feed.

    Thread-safety: single-threaded asyncio; no locks needed.
    """

    def __init__(self) -> None:
        self._funding: dict[str, dict] = {}      # symbol → latest settlement event
        self._instruments: dict[str, dict] = {}  # symbol → latest instrument snapshot

    # ------------------------------------------------------------------
    # Write side — called by PositionTracker WS handler
    # ------------------------------------------------------------------

    def update_funding(self, symbol: str, item: dict) -> None:
        """Record a funding settlement event for a symbol."""
        self._funding[symbol] = item

    def update_instrument(self, symbol: str, item: dict) -> None:
        """Merge a partial instrument update (WS sends diffs, not full rows)."""
        if symbol not in self._instruments:
            self._instruments[symbol] = {}
        self._instruments[symbol].update(item)

    # ------------------------------------------------------------------
    # Read side — used by strategies and delta calculation
    # ------------------------------------------------------------------

    def get_latest_funding_rate(self, symbol: str) -> Optional[float]:
        """
        Return the current funding rate for a symbol.

        Priority:
          1. `funding` topic (settlement event) — most recent confirmed payment rate
          2. `instrument` topic (continuous) — current indicative rate

        The `funding` topic only fires at 04:00/12:00/20:00 UTC. Between
        settlements the current rate is only available via instrument.fundingRate.
        Both are used for entry/exit decisions.
        """
        rate = self._funding.get(symbol, {}).get("fundingRate")
        if rate is not None:
            return rate
        return self._instruments.get(symbol, {}).get("fundingRate")

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Return the last traded price for a symbol from the instrument stream."""
        return self._instruments.get(symbol, {}).get("lastPrice")
