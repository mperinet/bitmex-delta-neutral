# TODOS

## Regime-Aware Capital Rotation

**What:** Compute funding rate z-scores across instruments every 8h. Auto-rebalance capital allocation between Strategy 1 (cash-and-carry) and Strategy 2 (funding harvest) based on market regime. Add vol regime filter: reduce all allocations 50% when realized vol > 30-day median.

**Why:** This is the "portable alpha engine" version of the bot — it doesn't just harvest, it hunts. The difference between a fixed-allocation carry bot and a regime-aware one is significant in sideways/low-volatility markets.

**Pros:** Higher risk-adjusted returns. Automatically reduces exposure in dangerous regimes. Makes the bot genuinely autonomous.

**Cons:** Requires 30-day historical funding and vol data to compute z-scores. Adds complexity to the capital allocation layer. Risk of over-optimization if z-score thresholds are fit to historical data.

**Context:** Identified in office-hours session on 2026-04-11 as the "coolest version." Build after Strategy 1+2 are both live and profitable for at least 30 days. The regime logic belongs in a new `capital_allocator.py` module that the strategy runner queries before sizing positions.

**Depends on:** Strategy 1 and 2 both live, 30-day historical funding data in DB.

---

## Multi-Exchange: Binance Spot (Second Venue)

**What:** Add Binance spot as the hedge leg for Strategy 2 (funding harvest). Short XBTUSD perp on BitMEX, buy BTC spot on Binance.

**Why:** BitMEX spot (XBT_USDT) has lower liquidity and higher fees than Binance spot. Better fills and tighter spreads on the hedge leg improve net yield on Strategy 2.

**Pros:** Better execution quality on the spot leg. Opens up cross-exchange funding rate arbitrage (Strategy 4 in the original plan).

**Cons:** Cross-exchange capital management (funds split across two venues). Withdrawal risk and timing. Two sets of API keys and error handling. Binance's contract specs and fee structure need separate `exchange/binance.py` implementation.

**Context:** Decision made 2026-04-11: use ccxt for the ORDER PLACEMENT API layer only. All funding calculations, margin logic, and position sizing are BitMEX-specific. When adding Binance: create `exchange/binance.py` that implements `ExchangeBase` for order placement. Funding/settlement semantics stay exchange-specific — do NOT try to abstract them across exchanges.

**Depends on:** Strategy 2 profitable on BitMEX-only for 30+ days. Cross-exchange capital allocated intentionally.
