"""
Tests for PositionTracker delta calculation and MarketDataCache contract-type logic.

Covers:
  - MarketDataCache.is_inverse_contract() resolution order (cache → pattern → default)
  - PositionTracker.get_net_delta_usd() for each BitMEX contract type
  - Iteration bug regression: _live_positions.items() must unpack (symbol, pos) correctly
"""

import pytest
from unittest.mock import MagicMock

from engine.market_data import MarketDataCache
from engine.position_tracker import PositionTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_cache(instruments=None):
    """Return a MarketDataCache pre-populated with instrument snapshots."""
    cache = MarketDataCache()
    for symbol, data in (instruments or {}).items():
        cache.update_instrument(symbol, data)
    return cache


def make_tracker(positions=None, instruments=None):
    """Return a PositionTracker with preset in-memory state, no live connections."""
    tracker = PositionTracker(
        exchange=MagicMock(),
        risk_guard=MagicMock(),
        ws_url="wss://test",
        api_key="key",
        api_secret="secret",
    )
    tracker._live_positions = positions or {}
    for symbol, data in (instruments or {}).items():
        tracker.market_data.update_instrument(symbol, data)
    return tracker


# ---------------------------------------------------------------------------
# MarketDataCache.is_inverse_contract
# ---------------------------------------------------------------------------

class TestIsInverseContract:
    def test_inverse_from_instrument_cache(self):
        c = make_cache({"XBTUSD": {"isInverse": True}})
        assert c.is_inverse_contract("XBTUSD") is True

    def test_quanto_from_instrument_cache(self):
        """ETHUSD is Quanto, not Inverse — isInverse=False in cache."""
        c = make_cache({"ETHUSD": {"isInverse": False}})
        assert c.is_inverse_contract("ETHUSD") is False

    def test_spot_from_instrument_cache(self):
        c = make_cache({"XBT_USDT": {"isInverse": False}})
        assert c.is_inverse_contract("XBT_USDT") is False

    def test_spot_underscore_pattern_fallback(self):
        """'_' in symbol → not inverse, even without instrument cache."""
        c = make_cache()
        assert c.is_inverse_contract("XBT_USDT") is False

    def test_eth_usdt_spot_underscore_pattern(self):
        c = make_cache()
        assert c.is_inverse_contract("ETH_USDT") is False

    def test_xbt_symbol_defaults_to_inverse(self):
        """No cache, no '_' → default True (covers XBTUSD, XBTEUR, etc.)."""
        c = make_cache()
        assert c.is_inverse_contract("XBTUSD") is True

    def test_quarterly_future_defaults_to_inverse(self):
        """XBTUSDTZ25 has no '_' → treated as inverse by default."""
        c = make_cache()
        assert c.is_inverse_contract("XBTUSDTZ25") is True

    def test_cache_overrides_default(self):
        """Instrument cache takes priority over the default True fallback."""
        c = make_cache({"XBTUSD": {"isInverse": False}})
        assert c.is_inverse_contract("XBTUSD") is False

    def test_cache_overrides_underscore_pattern(self):
        """Instrument cache takes priority over the '_' pattern."""
        c = make_cache({"XBT_USDT": {"isInverse": True}})
        assert c.is_inverse_contract("XBT_USDT") is True


# ---------------------------------------------------------------------------
# PositionTracker.get_net_delta_usd — contract-type correctness
# ---------------------------------------------------------------------------

class TestGetNetDeltaUsd:
    # -- baseline --

    def test_empty_positions_returns_zero(self):
        t = make_tracker()
        assert t.get_net_delta_usd() == 0.0

    def test_zero_qty_skipped(self):
        t = make_tracker(
            positions={"XBTUSD": {"currentQty": 0, "markPrice": 50_000}},
            instruments={"XBTUSD": {"isInverse": True}},
        )
        assert t.get_net_delta_usd() == 0.0

    # -- inverse (XBTUSD) --

    def test_inverse_short_delta(self):
        """Short 10 000 USD XBTUSD: delta = -10 000 USD (markPrice irrelevant)."""
        t = make_tracker(
            positions={"XBTUSD": {"currentQty": -10_000, "markPrice": 50_000}},
            instruments={"XBTUSD": {"isInverse": True}},
        )
        assert t.get_net_delta_usd() == pytest.approx(-10_000.0)

    def test_inverse_long_delta(self):
        """Long 10 000 USD XBTUSD: delta = +10 000 USD."""
        t = make_tracker(
            positions={"XBTUSD": {"currentQty": 10_000, "markPrice": 50_000}},
            instruments={"XBTUSD": {"isInverse": True}},
        )
        assert t.get_net_delta_usd() == pytest.approx(10_000.0)

    def test_inverse_delta_independent_of_mark_price(self):
        """For inverse, mark price has NO effect on delta_usd."""
        base = make_tracker(
            positions={"XBTUSD": {"currentQty": 5_000, "markPrice": 50_000}},
            instruments={"XBTUSD": {"isInverse": True}},
        )
        changed = make_tracker(
            positions={"XBTUSD": {"currentQty": 5_000, "markPrice": 100_000}},
            instruments={"XBTUSD": {"isInverse": True}},
        )
        assert base.get_net_delta_usd() == changed.get_net_delta_usd()

    # -- quarterly future (also inverse) --

    def test_quarterly_future_treated_as_inverse(self):
        """XBTUSDTZ25 — no '_', no cache → default inverse path."""
        t = make_tracker(
            positions={"XBTUSDTZ25": {"currentQty": -5_000}},
        )
        assert t.get_net_delta_usd() == pytest.approx(-5_000.0)

    # -- linear / spot (XBT_USDT) --

    def test_spot_long_delta(self):
        """Long 0.2 BTC at $50 000: delta = +10 000 USD."""
        t = make_tracker(
            positions={"XBT_USDT": {"currentQty": 0.2, "markPrice": 50_000}},
            instruments={"XBT_USDT": {"isInverse": False}},
        )
        assert t.get_net_delta_usd() == pytest.approx(10_000.0)

    def test_spot_mark_price_from_instrument_cache(self):
        """Mark price falls back to MarketDataCache when absent from position."""
        t = make_tracker(
            positions={"XBT_USDT": {"currentQty": 0.1}},  # no markPrice key
            instruments={"XBT_USDT": {"isInverse": False, "markPrice": 40_000}},
        )
        assert t.get_net_delta_usd() == pytest.approx(4_000.0)

    def test_spot_missing_price_excluded(self):
        """Non-inverse position with no price available contributes 0 (with warning)."""
        t = make_tracker(
            positions={"XBT_USDT": {"currentQty": 0.1}},
            instruments={"XBT_USDT": {"isInverse": False}},
        )
        assert t.get_net_delta_usd() == pytest.approx(0.0)

    # -- quanto (ETHUSD) --

    def test_quanto_ethusd_long(self):
        """ETHUSD (quanto, isInverse=False): delta ≈ qty × markPrice."""
        t = make_tracker(
            positions={"ETHUSD": {"currentQty": 1_000, "markPrice": 3_000}},
            instruments={"ETHUSD": {"isInverse": False, "isQuanto": True}},
        )
        assert t.get_net_delta_usd() == pytest.approx(3_000_000.0)

    def test_quanto_ethusd_short(self):
        t = make_tracker(
            positions={"ETHUSD": {"currentQty": -500, "markPrice": 3_000}},
            instruments={"ETHUSD": {"isInverse": False, "isQuanto": True}},
        )
        assert t.get_net_delta_usd() == pytest.approx(-1_500_000.0)

    # -- delta-neutral composite --

    def test_delta_neutral_inverse_plus_spot(self):
        """
        Funding-harvest position: short 10 000 USD XBTUSD + long 0.2 BTC spot
        at $50 000 → net delta = 0 (perfectly hedged).
        """
        t = make_tracker(
            positions={
                "XBTUSD": {"currentQty": -10_000, "markPrice": 50_000},
                "XBT_USDT": {"currentQty": 0.2, "markPrice": 50_000},
            },
            instruments={
                "XBTUSD": {"isInverse": True},
                "XBT_USDT": {"isInverse": False},
            },
        )
        assert t.get_net_delta_usd() == pytest.approx(0.0)

    def test_partial_hedge_residual_delta(self):
        """Under-hedged: residual delta surfaces correctly."""
        t = make_tracker(
            positions={
                "XBTUSD": {"currentQty": -10_000, "markPrice": 50_000},
                "XBT_USDT": {"currentQty": 0.15, "markPrice": 50_000},  # only 0.15 BTC
            },
            instruments={
                "XBTUSD": {"isInverse": True},
                "XBT_USDT": {"isInverse": False},
            },
        )
        # -10 000 + 0.15 × 50 000 = -10 000 + 7 500 = -2 500
        assert t.get_net_delta_usd() == pytest.approx(-2_500.0)

    def test_multiple_inverse_legs_summed(self):
        """Two inverse positions are summed directly."""
        t = make_tracker(
            positions={
                "XBTUSD": {"currentQty": -8_000},
                "XBTUSDTZ25": {"currentQty": -2_000},
            },
        )
        assert t.get_net_delta_usd() == pytest.approx(-10_000.0)

    # -- regression: iteration bug --

    def test_items_iteration_does_not_raise(self):
        """
        Regression: the original code did `for pos in .items()` then called
        pos.get(...) on the (symbol, dict) tuple — raises AttributeError.
        Verify no exception is raised with multiple positions.
        """
        t = make_tracker(
            positions={
                "XBTUSD": {"currentQty": -5_000, "markPrice": 50_000},
                "XBT_USDT": {"currentQty": 0.1, "markPrice": 50_000},
            },
            instruments={
                "XBTUSD": {"isInverse": True},
                "XBT_USDT": {"isInverse": False},
            },
        )
        result = t.get_net_delta_usd()  # must not raise AttributeError
        assert isinstance(result, float)
