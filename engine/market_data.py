"""
Live market data cache — populated from the WebSocket feed.

Separated from position_tracker because instrument metadata (funding rates,
mark prices) is symbol-level information, not position state.

The cache is written by PositionTracker's WS handler and read by strategies
and any component that needs live instrument data without going to REST.

Two distinct funding rate sources:
  - `funding` topic  → settlement events (04:00/12:00/20:00 UTC) — what was actually paid
  - `instrument` topic → indicative rate, updated every few seconds — what will be paid next

Use get_latest_funding_rate() when you need confirmed settlement data (accounting,
circuit breakers on realised cost). Use get_predictive_funding_rate() when you need
a forward-looking signal for entry/exit decisions.
"""

from __future__ import annotations


class MarketDataCache:
    """
    In-memory store for live instrument and funding data from the WS feed.

    Thread-safety: single-threaded asyncio; no locks needed.
    """

    def __init__(self) -> None:
        self._funding: dict[str, dict] = {}  # symbol → latest settlement event
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

    def get_latest_funding_rate(self, symbol: str) -> float | None:
        """
        Return the most recent confirmed funding rate from a settlement event.

        Source: `funding` WS topic only — fires at 04:00/12:00/20:00 UTC.
        Returns None between settlement windows (no event received yet this period).

        Use for: accounting, circuit breakers on realised cost, post-settlement checks.
        Do NOT use for entry/exit signals — stale between settlements.
        """
        return self._funding.get(symbol, {}).get("fundingRate")

    def get_predictive_funding_rate(self, symbol: str) -> float | None:
        """
        Return the current indicative funding rate from the instrument stream.

        Source: `instrument` WS topic — updated every few seconds by BitMEX.
        This is `instrument.fundingRate`: the rate that will be charged at the
        next settlement if market conditions hold. It is the primary signal for
        entry/exit decisions.

        Returns None until the first instrument update is received for this symbol.

        Use for: entry signals, exit signals, real-time strategy logic.
        """
        return self._instruments.get(symbol, {}).get("fundingRate")

    def get_mark_price(self, symbol: str) -> float | None:
        """Return the current mark price for a symbol from the instrument stream."""
        return self._instruments.get(symbol, {}).get("markPrice")

    def get_last_price(self, symbol: str) -> float | None:
        """Return the last traded price for a symbol from the instrument stream."""
        return self._instruments.get(symbol, {}).get("lastPrice")

    def get_underlying_to_position_multiplier(self, symbol: str) -> float:
        """
        Return the factor that converts currentQty (WS position contracts) to base
        currency units.

        BitMEX linear perps like XBTUSDT report currentQty in micro-XBT contracts
        (underlyingToPositionMultiplier = 1_000_000 → 1 contract = 0.000001 XBT).
        Spot products and inverse contracts have no such multiplier (return 1.0).

        Sourced from the WS instrument snapshot; falls back to 1.0 (identity) so
        spot/linear symbols without this field in their instrument data are unaffected.
        """
        mult = self._instruments.get(symbol, {}).get("underlyingToPositionMultiplier")
        return float(mult) if mult else 1.0

    def is_inverse_contract(self, symbol: str) -> bool:
        """
        Return True if symbol is an inverse (BTC-settled, USD-qty) contract.

        Resolution order:
        1. WS instrument cache — authoritative once the instrument snapshot arrives.
        2. '_' in symbol → spot / linear (XBT_USDT, ETH_USDT) — not inverse.
        3. Default True — covers XBTUSD, XBTEUR, XBTETH and quarterly futures
           (XBTUSDTZ25, etc.) whose symbols contain no '_'.

        Edge case: XBTUSDT (linear perpetual) has no '_' and isInverse=False.
        It falls into bucket 3 until the WS instrument snapshot populates the
        cache.  This is a brief startup window; live delta checks run after the
        WS is established so the cache is populated before any action is taken.
        """
        instrument = self._instruments.get(symbol, {})
        if "isInverse" in instrument:
            return bool(instrument["isInverse"])
        if "_" in symbol:
            return False
        return True  # default: XBT-prefixed inverse contracts and their futures
