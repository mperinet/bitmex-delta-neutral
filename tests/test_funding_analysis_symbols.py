"""
Unit tests for funding_analysis.symbols — BTC↔XBT alias, contract classification,
BitMEX symbol preference, and two-venue intersection.

These are pure in-memory tests with fake discovery payloads; no DB is touched.
"""

from __future__ import annotations

import pytest

from funding_analysis.symbols import (
    canonical_from_bitmex_base,
    classify_bitmex_contract,
    discover_universe,
    prefer_bitmex_symbol,
)


class TestCanonicalBase:
    def test_xbt_aliased_to_btc(self):
        assert canonical_from_bitmex_base("XBT") == "BTC"

    def test_eth_passes_through(self):
        assert canonical_from_bitmex_base("ETH") == "ETH"

    def test_lowercase_normalized(self):
        assert canonical_from_bitmex_base("xbt") == "BTC"
        assert canonical_from_bitmex_base("sol") == "SOL"


class TestClassify:
    def test_inverse_perp(self):
        inst = {"typ": "FFWCSX", "isInverse": True, "isQuanto": False}
        assert classify_bitmex_contract(inst) == "inverse_perp"

    def test_linear_perp(self):
        inst = {"typ": "FFWCSX", "isInverse": False, "isQuanto": False}
        assert classify_bitmex_contract(inst) == "linear_perp"

    def test_quanto_future(self):
        inst = {"typ": "FFCCSF", "isInverse": False, "isQuanto": True}
        assert classify_bitmex_contract(inst) == "quanto_future"

    def test_other(self):
        assert classify_bitmex_contract({"typ": "IFXXXR"}) == "other"


class TestPreferBitmexSymbol:
    def test_prefers_inverse_over_linear(self):
        sym, typ = prefer_bitmex_symbol([("XBTUSDT", "linear_perp"), ("XBTUSD", "inverse_perp")])
        assert sym == "XBTUSD"
        assert typ == "inverse_perp"

    def test_falls_back_to_linear(self):
        sym, typ = prefer_bitmex_symbol([("SOLUSDT", "linear_perp")])
        assert sym == "SOLUSDT"
        assert typ == "linear_perp"

    def test_empty_returns_none(self):
        assert prefer_bitmex_symbol([]) == (None, None)


# ------------------------------------------------------------------ #
# discover_universe — exercised with fakes                             #
# ------------------------------------------------------------------ #


class _FakeBitmex:
    def __init__(self, payload):
        self._payload = payload

    async def list_active_contracts(self):
        return self._payload


class _FakeHL:
    def __init__(self, payload):
        self._payload = payload

    async def list_perp_universe(self):
        return self._payload


class _FakeBinance:
    def __init__(self, payload):
        self._payload = payload

    async def list_spot_symbols(self, quotes=("USDT", "USDC")):
        return [m for m in self._payload if m.get("quote") in quotes]


class TestDiscoverUniverse:
    async def test_btc_present_on_all_three(self):
        bitmex = _FakeBitmex(
            [
                {
                    "symbol": "XBTUSD",
                    "rootSymbol": "XBT",
                    "typ": "FFWCSX",
                    "isInverse": True,
                    "isQuanto": False,
                    "state": "Open",
                },
                {
                    "symbol": "XBTUSDT",
                    "rootSymbol": "XBT",
                    "typ": "FFWCSX",
                    "isInverse": False,
                    "isQuanto": False,
                    "state": "Open",
                },
            ]
        )
        hl = _FakeHL([{"name": "BTC", "szDecimals": 4, "maxLeverage": 50}])
        binance = _FakeBinance(
            [{"symbol": "BTCUSDT", "base": "BTC", "quote": "USDT", "active": True}]
        )
        universe = await discover_universe(bitmex, hl, binance)
        by_asset = {v.asset: v for v in universe}
        assert "BTC" in by_asset
        assert by_asset["BTC"].hyperliquid_name == "BTC"
        assert by_asset["BTC"].binance_spot_symbol == "BTCUSDT"
        # Both BitMEX symbols surface; preference resolves to inverse.
        assert ("XBTUSD", "inverse_perp") in by_asset["BTC"].bitmex_symbols

    async def test_single_venue_excluded(self):
        # HL-only asset (no BitMEX, no Binance) → dropped.
        bitmex = _FakeBitmex([])
        hl = _FakeHL([{"name": "FART", "szDecimals": 2, "maxLeverage": 5}])
        binance = _FakeBinance([])
        universe = await discover_universe(bitmex, hl, binance)
        assert universe == []

    async def test_two_venue_asset_included(self):
        # HL + Binance, no BitMEX → still included.
        bitmex = _FakeBitmex([])
        hl = _FakeHL([{"name": "PEPE", "szDecimals": 0, "maxLeverage": 5}])
        binance = _FakeBinance(
            [{"symbol": "PEPEUSDT", "base": "PEPE", "quote": "USDT", "active": True}]
        )
        universe = await discover_universe(bitmex, hl, binance)
        assert len(universe) == 1
        assert universe[0].asset == "PEPE"
        assert universe[0].bitmex_symbols == []

    async def test_closed_bitmex_contracts_ignored(self):
        bitmex = _FakeBitmex(
            [
                {
                    "symbol": "XBTUSD_OLD",
                    "rootSymbol": "XBT",
                    "typ": "FFWCSX",
                    "isInverse": True,
                    "state": "Closed",
                }
            ]
        )
        hl = _FakeHL([{"name": "BTC"}])
        binance = _FakeBinance(
            [{"symbol": "BTCUSDT", "base": "BTC", "quote": "USDT", "active": True}]
        )
        universe = await discover_universe(bitmex, hl, binance)
        btc = next(v for v in universe if v.asset == "BTC")
        assert btc.bitmex_symbols == []

    async def test_delisted_hl_excluded(self):
        bitmex = _FakeBitmex([])
        hl = _FakeHL([{"name": "DEAD", "isDelisted": True}])
        binance = _FakeBinance(
            [{"symbol": "DEADUSDT", "base": "DEAD", "quote": "USDT", "active": True}]
        )
        universe = await discover_universe(bitmex, hl, binance)
        # Only Binance has it → dropped.
        assert universe == []
