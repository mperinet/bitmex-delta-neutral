"""
Unit tests for _compute_fifo_pnl_series — average-cost FIFO PNL.

Prices are chosen to be small, round numbers so expected PNL values are
easy to verify by hand. qty units are contracts (USD-face on BitMEX inverse,
or USD-equivalent on linear/quanto).

Test matrix
───────────
Linear (is_inverse=False):
  open      : 0→+100, 0→-100
  grow      : +100→+300 (three buys), -100→-300 (three sells)
  close     : +100→0, -100→0, +300→0 after three-buy pyramid, -300→0
  partial   : +200→+100, -200→-100
  reversal  : +100→-50 (long→short), -100→+50 (short→long)
  reopen    : close then open again (avg_cost must reset)
  break-even: close at entry price

Inverse (is_inverse=True):
  open, close (profit/loss), reversal — same shape but BTC-settled formula
"""

from __future__ import annotations

import pytest
import pandas as pd

from trading_analysis.fifo import _compute_fifo_pnl_series


# ------------------------------------------------------------------ #
# Helper                                                               #
# ------------------------------------------------------------------ #

def make_fills(*fills: tuple[str, float, float]) -> pd.DataFrame:
    """Build a minimal fills DataFrame from (side, qty, price) tuples.

    Timestamps are sequential seconds from 2024-01-01 so sort order is
    deterministic and matches insertion order.
    """
    base = pd.Timestamp("2024-01-01")
    rows = [
        {
            "timestamp": base + pd.Timedelta(seconds=i),
            "exec_id": f"e{i}",
            "side": side,
            "qty": float(qty),
            "price": float(price),
        }
        for i, (side, qty, price) in enumerate(fills)
    ]
    return pd.DataFrame(rows)


def pnl(df: pd.DataFrame, is_inverse: bool = False) -> list[float]:
    return list(_compute_fifo_pnl_series(df, is_inverse=is_inverse))


# ================================================================== #
# LINEAR / QUANTO contracts  (is_inverse=False)                        #
# ================================================================== #


class TestLinearOpen:
    def test_open_long_from_zero(self):
        """Single buy — no position to close, PNL must be 0."""
        df = make_fills(("Buy", 100, 100))
        assert pnl(df) == [0.0]

    def test_open_short_from_zero(self):
        """Single sell — no position to close, PNL must be 0."""
        df = make_fills(("Sell", 100, 100))
        assert pnl(df) == [0.0]


class TestLinearGrow:
    def test_grow_long_three_buys(self):
        """Buy 100@100, Buy 100@200, Buy 100@300 — avg_cost = 200.
        No closes, all PNL = 0.
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Buy", 100, 200),
            ("Buy", 100, 300),
        )
        result = pnl(df)
        assert result == [0.0, 0.0, 0.0]

    def test_grow_short_three_sells(self):
        """Sell 100@300, Sell 100@200, Sell 100@100 — avg_cost = 200.
        No closes, all PNL = 0.
        """
        df = make_fills(
            ("Sell", 100, 300),
            ("Sell", 100, 200),
            ("Sell", 100, 100),
        )
        result = pnl(df)
        assert result == [0.0, 0.0, 0.0]

    def test_avg_cost_weighted_correctly(self):
        """Buy 100@100 + Buy 300@200 → avg_cost = (100*100 + 300*200)/400 = 175.
        Sell 400@275 → PNL = 400*(275-175) = 40 000.
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Buy", 300, 200),
            ("Sell", 400, 275),
        )
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(0.0)
        assert result[2] == pytest.approx(40_000.0)


class TestLinearClose:
    def test_close_long_profit(self):
        """Buy 100@100, Sell 100@120 → PNL = 100*(120-100) = 2 000."""
        df = make_fills(("Buy", 100, 100), ("Sell", 100, 120))
        assert pnl(df) == pytest.approx([0.0, 2_000.0])

    def test_close_long_loss(self):
        """Buy 100@100, Sell 100@80 → PNL = 100*(80-100) = -2 000."""
        df = make_fills(("Buy", 100, 100), ("Sell", 100, 80))
        assert pnl(df) == pytest.approx([0.0, -2_000.0])

    def test_close_short_profit(self):
        """Sell 100@100, Buy 100@80 → PNL = 100*(100-80) = 2 000."""
        df = make_fills(("Sell", 100, 100), ("Buy", 100, 80))
        assert pnl(df) == pytest.approx([0.0, 2_000.0])

    def test_close_short_loss(self):
        """Sell 100@100, Buy 100@120 → PNL = 100*(100-120) = -2 000."""
        df = make_fills(("Sell", 100, 100), ("Buy", 100, 120))
        assert pnl(df) == pytest.approx([0.0, -2_000.0])

    def test_close_at_entry_price_zero_pnl(self):
        """Close at exact entry price → PNL = 0."""
        df = make_fills(("Buy", 100, 100), ("Sell", 100, 100))
        assert pnl(df) == pytest.approx([0.0, 0.0])

    def test_close_grown_long_three_buys(self):
        """Buy 100@100, Buy 100@200, Buy 100@300 → avg_cost = 200.
        Sell 300@350 → PNL = 300*(350-200) = 45 000.
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Buy", 100, 200),
            ("Buy", 100, 300),
            ("Sell", 300, 350),
        )
        result = pnl(df)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert result[3] == pytest.approx(45_000.0)

    def test_close_grown_short_three_sells(self):
        """Sell 100@300, Sell 100@200, Sell 100@100 → avg_cost = 200.
        Buy 300@50 → PNL = 300*(200-50) = 45 000.
        """
        df = make_fills(
            ("Sell", 100, 300),
            ("Sell", 100, 200),
            ("Sell", 100, 100),
            ("Buy", 300, 50),
        )
        result = pnl(df)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert result[3] == pytest.approx(45_000.0)

    def test_multiple_partial_closes_long(self):
        """Buy 100@100, Buy 100@100, Buy 100@100 → avg_cost = 100.
        Sell 100@150 → PNL = 5 000 (100 remain).
        Sell 100@200 → PNL = 10 000 (100 remain).
        Sell 100@250 → PNL = 15 000 (position = 0).
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Buy", 100, 100),
            ("Buy", 100, 100),
            ("Sell", 100, 150),
            ("Sell", 100, 200),
            ("Sell", 100, 250),
        )
        result = pnl(df)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert result[3] == pytest.approx(5_000.0)
        assert result[4] == pytest.approx(10_000.0)
        assert result[5] == pytest.approx(15_000.0)

    def test_multiple_partial_closes_short(self):
        """Sell 100@200, Sell 100@200, Sell 100@200 → avg_cost = 200.
        Buy 100@150 → PNL = 5 000.
        Buy 100@100 → PNL = 10 000.
        Buy 100@50  → PNL = 15 000.
        """
        df = make_fills(
            ("Sell", 100, 200),
            ("Sell", 100, 200),
            ("Sell", 100, 200),
            ("Buy", 100, 150),
            ("Buy", 100, 100),
            ("Buy", 100, 50),
        )
        result = pnl(df)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert result[3] == pytest.approx(5_000.0)
        assert result[4] == pytest.approx(10_000.0)
        assert result[5] == pytest.approx(15_000.0)


class TestLinearPartialClose:
    def test_partial_close_long(self):
        """Buy 200@100, Sell 100@120 → close half, PNL = 100*(120-100) = 2 000.
        Remaining 100 long still at avg_cost = 100.
        """
        df = make_fills(("Buy", 200, 100), ("Sell", 100, 120))
        assert pnl(df) == pytest.approx([0.0, 2_000.0])

    def test_partial_close_short(self):
        """Sell 200@100, Buy 100@80 → close half, PNL = 100*(100-80) = 2 000."""
        df = make_fills(("Sell", 200, 100), ("Buy", 100, 80))
        assert pnl(df) == pytest.approx([0.0, 2_000.0])

    def test_partial_close_avg_cost_unchanged(self):
        """After a partial close the remaining position keeps the original avg_cost.

        Buy 200@100 (avg_cost=100).
        Sell 100@120 → PNL = 2 000. Remaining 100 @ avg_cost = 100.
        Sell 100@140 → PNL = 100*(140-100) = 4 000.
        """
        df = make_fills(
            ("Buy", 200, 100),
            ("Sell", 100, 120),
            ("Sell", 100, 140),
        )
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(2_000.0)
        assert result[2] == pytest.approx(4_000.0)


class TestLinearReversal:
    def test_reversal_long_to_short(self):
        """Buy 100@100, Sell 150@120:
          - closes 100 → PNL = 100*(120-100) = 2 000
          - opens new short -50 at avg_cost = 120
        """
        df = make_fills(("Buy", 100, 100), ("Sell", 150, 120))
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(2_000.0)

    def test_reversal_short_to_long(self):
        """Sell 100@100, Buy 150@80:
          - closes 100 → PNL = 100*(100-80) = 2 000
          - opens new long +50 at avg_cost = 80
        """
        df = make_fills(("Sell", 100, 100), ("Buy", 150, 80))
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(2_000.0)

    def test_reversal_new_avg_cost_is_reversal_price(self):
        """After a reversal the new position uses the reversal fill's price as avg_cost.

        Buy 100@100, Sell 150@120 → new short -50 @ avg_cost 120.
        Close that short: Buy 50@100 → PNL = 50*(120-100) = 1 000  (cost=120, exit=100).
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Sell", 150, 120),
            ("Buy", 50, 100),
        )
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(2_000.0)   # close long
        assert result[2] == pytest.approx(1_000.0)   # close new short

    def test_grow_then_reversal(self):
        """Build up then reverse in one fill.

        Buy 100@100, Buy 100@300 → avg_cost = 200.
        Sell 250@400 → closes 200 @ avg_cost 200 → PNL = 200*(400-200) = 40 000.
                       opens new short -50 @ avg_cost 400.
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Buy", 100, 300),
            ("Sell", 250, 400),
        )
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(0.0)
        assert result[2] == pytest.approx(40_000.0)


class TestLinearReopen:
    def test_reopen_after_full_close(self):
        """Close to zero then open a new position — third fill PNL = 0."""
        df = make_fills(
            ("Buy", 100, 100),
            ("Sell", 100, 120),   # PNL = 2 000, position = 0
            ("Buy", 100, 110),    # new open, PNL = 0
        )
        result = pnl(df)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(2_000.0)
        assert result[2] == pytest.approx(0.0)

    def test_reopen_avg_cost_reset(self):
        """After full close, avg_cost resets to the new entry price.

        Buy 100@100, Sell 100@120 (close), Buy 100@110 (reopen), Sell 100@130.
        Final PNL = 100*(130-110) = 2 000, NOT 100*(130-100).
        """
        df = make_fills(
            ("Buy", 100, 100),
            ("Sell", 100, 120),
            ("Buy", 100, 110),
            ("Sell", 100, 130),
        )
        result = pnl(df)
        assert result[3] == pytest.approx(2_000.0)


# ================================================================== #
# INVERSE contracts  (is_inverse=True)                                 #
# PNL formula:                                                         #
#   long close:  qty × (1/avg_cost − 1/price)   [BTC profit]          #
#   short close: qty × (1/price − 1/avg_cost)   [BTC profit]          #
# ================================================================== #


class TestInverseOpen:
    def test_inverse_open_long(self):
        """Single inverse buy — PNL = 0."""
        df = make_fills(("Buy", 1000, 50_000))
        assert pnl(df, is_inverse=True) == [0.0]

    def test_inverse_open_short(self):
        """Single inverse sell — PNL = 0."""
        df = make_fills(("Sell", 1000, 50_000))
        assert pnl(df, is_inverse=True) == [0.0]


class TestInverseClose:
    def test_inverse_close_long_profit(self):
        """Buy 1000@50 000, Sell 1000@60 000 → profit in BTC.
        PNL = 1000*(1/50000 − 1/60000) ≈ 0.003333 BTC.
        """
        df = make_fills(("Buy", 1000, 50_000), ("Sell", 1000, 60_000))
        result = pnl(df, is_inverse=True)
        expected = 1000 * (1/50_000 - 1/60_000)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(expected)
        assert result[1] > 0

    def test_inverse_close_long_loss(self):
        """Buy 1000@50 000, Sell 1000@40 000 → loss in BTC."""
        df = make_fills(("Buy", 1000, 50_000), ("Sell", 1000, 40_000))
        result = pnl(df, is_inverse=True)
        expected = 1000 * (1/50_000 - 1/40_000)
        assert result[1] == pytest.approx(expected)
        assert result[1] < 0

    def test_inverse_close_short_profit(self):
        """Sell 1000@50 000, Buy 1000@40 000 → profit in BTC.
        PNL = 1000*(1/40000 − 1/50000) = 0.005 BTC.
        """
        df = make_fills(("Sell", 1000, 50_000), ("Buy", 1000, 40_000))
        result = pnl(df, is_inverse=True)
        expected = 1000 * (1/40_000 - 1/50_000)
        assert result[1] == pytest.approx(expected)
        assert result[1] > 0

    def test_inverse_close_short_loss(self):
        """Sell 1000@50 000, Buy 1000@60 000 → loss in BTC."""
        df = make_fills(("Sell", 1000, 50_000), ("Buy", 1000, 60_000))
        result = pnl(df, is_inverse=True)
        assert result[1] < 0

    def test_inverse_close_at_entry_zero_pnl(self):
        """Close at exact entry price → PNL = 0."""
        df = make_fills(("Buy", 1000, 50_000), ("Sell", 1000, 50_000))
        result = pnl(df, is_inverse=True)
        assert result[1] == pytest.approx(0.0)

    def test_inverse_grow_three_buys_then_close(self):
        """Buy 1000@40000, Buy 1000@50000, Buy 1000@60000 → avg_cost = 50000.
        Sell 3000@70000 → PNL = 3000*(1/50000 − 1/70000).
        """
        df = make_fills(
            ("Buy", 1000, 40_000),
            ("Buy", 1000, 50_000),
            ("Buy", 1000, 60_000),
            ("Sell", 3000, 70_000),
        )
        result = pnl(df, is_inverse=True)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        avg = 50_000.0
        expected = 3000 * (1/avg - 1/70_000)
        assert result[3] == pytest.approx(expected)

    def test_inverse_multiple_partial_closes(self):
        """Buy 1000@50000, Buy 1000@50000, Buy 1000@50000 → avg_cost = 50000.
        Sell 1000@60000 → PNL = 1000*(1/50000−1/60000).
        Sell 1000@70000 → PNL = 1000*(1/50000−1/70000).
        Sell 1000@80000 → PNL = 1000*(1/50000−1/80000).
        """
        df = make_fills(
            ("Buy", 1000, 50_000),
            ("Buy", 1000, 50_000),
            ("Buy", 1000, 50_000),
            ("Sell", 1000, 60_000),
            ("Sell", 1000, 70_000),
            ("Sell", 1000, 80_000),
        )
        result = pnl(df, is_inverse=True)
        assert result[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert result[3] == pytest.approx(1000 * (1/50_000 - 1/60_000))
        assert result[4] == pytest.approx(1000 * (1/50_000 - 1/70_000))
        assert result[5] == pytest.approx(1000 * (1/50_000 - 1/80_000))


class TestInverseReversal:
    def test_inverse_reversal_long_to_short(self):
        """Buy 1000@50000, Sell 1500@60000:
          - closes 1000 → PNL = 1000*(1/50000−1/60000)
          - opens new short −500 @ avg_cost 60000
        """
        df = make_fills(("Buy", 1000, 50_000), ("Sell", 1500, 60_000))
        result = pnl(df, is_inverse=True)
        expected_close = 1000 * (1/50_000 - 1/60_000)
        assert result[0] == pytest.approx(0.0)
        assert result[1] == pytest.approx(expected_close)

    def test_inverse_reversal_new_avg_cost(self):
        """After long→short reversal, new short avg_cost = reversal price.

        Buy 1000@50000, Sell 1500@60000 → short -500 @ 60000.
        Buy 500@50000 → PNL = 500*(1/50000−1/60000)  [short close].
        """
        df = make_fills(
            ("Buy", 1000, 50_000),
            ("Sell", 1500, 60_000),
            ("Buy", 500, 50_000),
        )
        result = pnl(df, is_inverse=True)
        expected_close1 = 1000 * (1/50_000 - 1/60_000)
        expected_close2 = 500 * (1/50_000 - 1/60_000)   # short cost=60000, exit=50000
        assert result[1] == pytest.approx(expected_close1)
        assert result[2] == pytest.approx(expected_close2)
