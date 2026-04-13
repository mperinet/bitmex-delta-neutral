"""
Contract quantity calculation tests grounded in real ccxt market data.

All market dicts below are derived from actual BitMEX testnet responses
fetched on 2026-04-12 (abridged to fields relevant to sizing):

  BTC/USD:BTC:      inverse=True,  quanto=False, linear=False,
                    contractSize=100_000_000, precision.amount=100,
                    limits.amount.min=None

  ETH/USD:BTC:      inverse=False, quanto=True,  linear=False,
                    contractSize=100,            precision.amount=1,
                    limits.amount.min=None

  BTC/USDT:USDT:    inverse=False, quanto=False, linear=True,
                    contractSize=1e-06,           precision.amount=100,
                    limits.amount.min=None

  BTC/USDT (spot):  spot=True, contractSize=None, precision.amount=1000
                    (satoshi sub-units — must NOT be used as lot size)

  BTC/USD:BTC-260424: inverse=True, contractSize=100_000_000,
                      precision.amount=100, limits.amount.min=None

Key formulas:
  inverse  → qty = usd_notional                     (1 contract = $1 notional)
  quanto   → qty = usd_notional / (mark × cSize × 1e-8 × btc)
  linear   → qty = usd_notional / (mark × cSize)
  spot     → qty = usd_notional / mark
"""

import pytest
from unittest.mock import MagicMock

from engine.order_manager import OrderManager, RateLimitBucket


# ---------------------------------------------------------------------------
# Fixtures: real ccxt market dicts (abridged)
# ---------------------------------------------------------------------------

MARKETS = {
    "BTC/USD:BTC": {
        "inverse": True,
        "quanto": False,
        "linear": False,
        "spot": False,
        "contractSize": 100_000_000,
        "precision": {"amount": 100},
        "limits": {"amount": {"min": None}},
    },
    "ETH/USD:BTC": {
        "inverse": False,
        "quanto": True,
        "linear": False,
        "spot": False,
        "contractSize": 100,
        "precision": {"amount": 1},
        "limits": {"amount": {"min": None}},
    },
    "BTC/USDT:USDT": {
        "inverse": False,
        "quanto": False,
        "linear": True,
        "spot": False,
        "contractSize": 1e-6,
        "precision": {"amount": 100},
        "limits": {"amount": {"min": None}},
    },
    "BTC/USDT": {
        "inverse": False,
        "quanto": False,
        "linear": False,
        "spot": True,
        "contractSize": None,
        # precision.amount=1000 is in satoshi sub-units — must NOT be used as lot size
        "precision": {"amount": 1000},
        "limits": {"amount": {"min": None}},
    },
    "BTC/USD:BTC-260424": {
        "inverse": True,
        "quanto": False,
        "linear": False,
        "spot": False,
        "contractSize": 100_000_000,
        "precision": {"amount": 100},
        "limits": {"amount": {"min": None}},
    },
}


def make_mgr(extra_min_sizes: dict | None = None) -> OrderManager:
    ex = MagicMock()
    ex._ccxt = MagicMock()
    ex._ccxt.markets = MARKETS
    mgr = OrderManager(exchange=ex, bucket=RateLimitBucket(300))
    if extra_min_sizes:
        mgr._min_order_size.update(extra_min_sizes)
    return mgr


# ---------------------------------------------------------------------------
# Lot-size resolution (_get_min_order_size)
# ---------------------------------------------------------------------------


def test_lot_size_inverse_perp_from_precision_amount():
    """BTC/USD:BTC: limits.amount.min=None → falls back to precision.amount=100."""
    mgr = make_mgr()
    assert mgr._get_min_order_size("BTC/USD:BTC") == pytest.approx(100.0)


def test_lot_size_quanto_from_precision_amount():
    """ETH/USD:BTC: limits.amount.min=None → precision.amount=1."""
    mgr = make_mgr()
    assert mgr._get_min_order_size("ETH/USD:BTC") == pytest.approx(1.0)


def test_lot_size_linear_from_precision_amount():
    """BTC/USDT:USDT: limits.amount.min=None → precision.amount=100."""
    mgr = make_mgr()
    assert mgr._get_min_order_size("BTC/USDT:USDT") == pytest.approx(100.0)


def test_lot_size_quarterly_future_from_precision_amount():
    """BTC/USD:BTC-260424: limits.amount.min=None → precision.amount=100."""
    mgr = make_mgr()
    assert mgr._get_min_order_size("BTC/USD:BTC-260424") == pytest.approx(100.0)


def test_lot_size_spot_does_not_use_precision_amount():
    """BTC/USDT (spot): precision.amount=1000 is satoshi units — must NOT be lot size.
    Falls back to _BITMEX_KNOWN_MINIMUMS (absent for spot → 0.0)."""
    mgr = make_mgr()
    # Spot is not in _BITMEX_KNOWN_MINIMUMS → should return 0.0, not 1000
    result = mgr._get_min_order_size("BTC/USDT")
    assert result == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# USD → contract qty
# ---------------------------------------------------------------------------


def test_inverse_perp_xbtusd():
    """BTC/USD:BTC (XBTUSD inverse perp): qty = usd_notional, lot=100.

    contractSize=100_000_000 (raw satoshi mult) is ignored.
    mark_price is irrelevant for inverse contracts.
    """
    mgr = make_mgr()
    qty = mgr.usd_to_contract_qty("BTC/USD:BTC", 10_000.0, mark_price=84_000.0)
    assert qty == pytest.approx(10_000.0)


def test_inverse_perp_xbtusd_lot_rounding():
    """BTC/USD:BTC: notional not a multiple of 100 is rounded to nearest lot."""
    mgr = make_mgr()
    # 10_080 / 100 = 100.8 → rounds to 101 → 10_100
    qty = mgr.usd_to_contract_qty("BTC/USD:BTC", 10_080.0, mark_price=84_000.0)
    assert qty == pytest.approx(10_100.0)


def test_quarterly_future():
    """BTC/USD:BTC-260424 (inverse quarterly): same formula as perp, lot=100."""
    mgr = make_mgr()
    qty = mgr.usd_to_contract_qty("BTC/USD:BTC-260424", 5_000.0, mark_price=84_000.0)
    assert qty == pytest.approx(5_000.0)


def test_quanto_ethusd():
    """ETH/USD:BTC (ETHUSD quanto): qty = notional / (eth_price × 100 × 1e-8 × btc_price).

    contractSize=100 is 100 satoshis per $1 per contract (quanto multiplier).
    Both ETH/USD price and BTC/USD price are needed.

    At ETH=3000, BTC=84000:
      contract_value_usd = 3000 × 100 × 1e-8 × 84000 = 0.252 USD/contract
      qty = 1000 / 0.252 ≈ 3968.25 → 3968 (lot=1)
    """
    mgr = make_mgr()
    eth_price = 3_000.0
    btc_price = 84_000.0
    usd_notional = 1_000.0
    contract_value = eth_price * 100 * 1e-8 * btc_price
    expected = round(usd_notional / contract_value)

    qty = mgr.usd_to_contract_qty(
        "ETH/USD:BTC", usd_notional, mark_price=eth_price, btc_price=btc_price
    )
    assert qty == pytest.approx(float(expected))


def test_quanto_requires_btc_price():
    """Omitting btc_price for a quanto contract raises ValueError."""
    mgr = make_mgr()
    with pytest.raises(ValueError, match="btc_price required"):
        mgr.usd_to_contract_qty("ETH/USD:BTC", 1_000.0, mark_price=3_000.0)


def test_linear_xbtusdt():
    """BTC/USDT:USDT (XBTUSDT linear perp): qty = notional / (mark × 1e-6), lot=100.

    At mark=84000, notional=84:
      raw = 84 / (84000 × 1e-6) = 84 / 0.084 = 1000
    """
    mgr = make_mgr()
    qty = mgr.usd_to_contract_qty("BTC/USDT:USDT", 84.0, mark_price=84_000.0)
    assert qty == pytest.approx(1_000.0)


def test_spot_btcusdt():
    """BTC/USDT (spot): qty = notional / mark_price, no lot rounding (lot=0)."""
    mgr = make_mgr()
    qty = mgr.usd_to_contract_qty("BTC/USDT", 84_000.0, mark_price=84_000.0)
    assert qty == pytest.approx(1.0)
