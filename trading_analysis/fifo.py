from __future__ import annotations

import pandas as pd


def _compute_fifo_pnl_series(df: pd.DataFrame, is_inverse: bool = False) -> pd.Series:
    """Average-cost FIFO PNL for one symbol's fills.

    Returns a Series indexed like df where each value is the realised PNL
    of that fill: positive for a profitable close, 0 for an opening fill.

    is_inverse: True only for XBTUSD and XBT futures.
      Uses PNL = close_qty × (1/avg_cost − 1/price) instead of the linear formula.
      All other USD contracts (ETHUSD, LINKUSD, DOGEUSD …) are quanto — pass False.
    """
    df = df.sort_values(["timestamp", "exec_id"])
    position = 0.0
    avg_cost = 0.0
    pnls: list[float] = []
    for _, row in df.iterrows():
        qty = float(row["qty"])
        price = float(row["price"])
        signed = qty if str(row["side"]).lower() == "buy" else -qty
        pnl = 0.0
        if position == 0.0:
            position = signed
            avg_cost = price
        elif (position > 0) == (signed > 0):
            new_pos = position + signed
            avg_cost = (abs(position) * avg_cost + qty * price) / abs(new_pos)
            position = new_pos
        else:
            close_qty = min(qty, abs(position))
            if is_inverse:
                pnl = close_qty * (1/avg_cost - 1/price) if position > 0 else close_qty * (1/price - 1/avg_cost)
            else:
                pnl = close_qty * (price - avg_cost) if position > 0 else close_qty * (avg_cost - price)
            new_pos = position + signed
            if abs(new_pos) < 1e-9:
                position = 0.0
                avg_cost = 0.0
            elif (new_pos > 0) != (position > 0):
                # True reversal: position changed sign → new avg_cost at fill price
                position = new_pos
                avg_cost = price
            else:
                # Partial close: same sign remains → avg_cost unchanged
                position = new_pos
        pnls.append(pnl)
    return pd.Series(pnls, index=df.index)
