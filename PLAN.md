
<!-- /autoplan restore point: /Users/mperinet/.gstack/projects/mperinet-bitmex-delta-neutral/main-autoplan-restore-20260411-234135.md -->
/
I want to build an automated trading system. Your research should use BitMEX official documentation and API. You should pick the best software foundation to run the system locally. It should consist of a core trading engine and an isolated market opportunity analysis dashboard that save and expose the available trading strategies. Only the engine actually run them. Core of the system is arbitrage and carry trading : delta neutral exposure, no directional positioning. It could be a selling a future contract at a premium compare to the spot and buying the same amount of the perpetual contract because the expected funding rates paid should lower on the long run that the future contract premium at expiry. This only an example you should find other credible strategies. I want you to explore that and produce a plan for the system and insights on what can be done.

10 Apr

Charted BitMEX API research for delta-neutral trading system architecture
Charted BitMEX API research for delta-neutral trading system architecture
I'll research BitMEX's API documentation thoroughly to build you a solid plan. Let me start by exploring their official docs.


Synthesized requirements and architected comprehensive trading system blueprint
Synthesized requirements and architected comprehensive trading system blueprint
Now let me read the frontend design skill and build this as a comprehensive document.


Architected comprehensive markdown framework for trading system documentation
Architected comprehensive markdown framework for trading system documentation
I have enough research. Let me now produce the comprehensive plan document.


Architected comprehensive automated trading system leveraging arbitrage strategies
Architected comprehensive automated trading system leveraging arbitrage strategies
Here's your complete system plan covering 7 delta-neutral strategies, the full architecture, and BitMEX API specifics.

Key highlights from the research:

Strategies identified — beyond your futures-vs-perp carry example, I found 6 additional credible approaches: funding rate harvesting (perp vs spot, now possible entirely on-exchange since BitMEX added spot), calendar spreads between quarterly futures, cross-quote arbitrage (inverse XBTUSD vs linear XBTUSDT), funding spike mean-reversion, multi-asset funding rotation, and delta-neutral market making.

Critical BitMEX insight — funding rates have stabilized around a 0.01%/8h baseline (~10.95% APR) with institutional capital like Ethena acting as a ceiling on spikes 
BitMEX
. This means your carry trade's biggest cost (funding paid as a perp long) is predictable but real — the system must compare locked-in basis against expected cumulative funding.

Tech choice — Python is the clear winner. BitMEX's official market-maker bot is Python-based and uses WebSocket to minimize REST calls against the 300-request/5-minute rate limit 
PyPI
. The architecture isolates the dashboard (read-only analysis) from the engine (execution) via a shared PostgreSQL database.

Safety — the plan includes BitMEX's cancelAllAfter dead-man's switch, delta exposure guards, and margin circuit breakers. Everything validates on testnet first.


Bitmex delta neutral system plan
Document · MD 





Claude is AI and can make mistakes. Please double-check cited sources.
Bitmex delta neutral system plan · MD
Copy

# BitMEX Delta-Neutral Automated Trading System
## Architecture Plan & Strategy Research
 
---
 
## 1. Executive Summary
 
This document lays out the complete architecture for a locally-run, delta-neutral automated trading system on BitMEX. The system is split into two isolated components: a **Strategy Dashboard** (analysis & opportunity discovery) and a **Trading Engine** (execution). All strategies are non-directional — they harvest structural inefficiencies (basis, funding, cross-instrument spreads) rather than betting on price.
 
---
 
## 2. Technology Stack Recommendation
 
### Why Python (with Rust hot-paths later)
 
Python is the clear choice for the foundation. BitMEX officially maintains a Python REST client (`pip install bitmex`) and a reference WebSocket client. The official market-maker bot (`bitmex-market-maker`) is Python-based, meaning the entire order lifecycle is proven in Python against their engine. The ecosystem for quantitative work (numpy, pandas, scipy) is unmatched.
 
| Layer | Technology | Rationale |
|---|---|---|
| **Runtime** | Python 3.11+ | Official BitMEX SDK, quant ecosystem, async support |
| **Async framework** | `asyncio` + `aiohttp` / `websockets` | Non-blocking WS streams + REST calls |
| **Database** | SQLite (dev) → PostgreSQL (prod) | Time-series for funding/basis history, strategy state |
| **Dashboard** | Separate process: FastAPI + React (or Streamlit for MVP) | Isolated from engine, read-only DB access |
| **Task scheduling** | APScheduler or custom asyncio loop | Funding snapshots every 8h, basis scans every minute |
| **Configuration** | TOML/YAML files + env vars for secrets | API keys never in code |
| **Containerisation** | Docker Compose (engine + dashboard + db) | Process isolation, reproducible setup |
 
### Key Python Libraries
 
- `bitmex` — official REST client (Bravado-based, exposes every endpoint)
- `websockets` / `aiohttp` — for the real-time WebSocket feed
- `ccxt` — alternative unified client if you later add cross-exchange arb
- `pandas` / `numpy` — data manipulation and analytics
- `scipy.optimize` — for optimal hedge ratio calculation
 
---
 
## 3. BitMEX API Surface — What Matters for Delta-Neutral
 
### 3.1 REST API (Base: `https://www.bitmex.com/api/v1`)
 
| Endpoint | Use in System |
|---|---|
| `GET /instrument/active` | Discover all live perps (`typ=FFWCSX`), futures (`typ=FFCCSX`), spot (`typ=IFXXXP`) |
| `GET /instrument?symbol=XBT:quarterly` | Get the nearest quarterly future for a series |
| `GET /funding?symbol=XBTUSD` | Historical funding rate payments |
| `GET /trade?symbol=.XBTUSDTPI` | Premium index (minute-by-minute) |
| `GET /trade?symbol=.BXBT` | Spot index (composite price) |
| `GET /orderBook/L2?symbol=...&depth=25` | Order book for spread/slippage estimation |
| `POST /order` | Place orders (limit, market, bulk) |
| `POST /order/cancelAllAfter` | Dead-man's switch (critical safety) |
| `GET /position` | Current positions and unrealised PnL |
| `GET /user/wallet` | Balance and margin available |
| `GET /settlement` | Settlement prices for expired futures |
 
**Rate limits:** 300 requests per 5-minute window (authenticated). Maker rebate of **-0.025%**, taker fee of **0.075%**. Futures settlement fee: **0.05%**.
 
### 3.2 WebSocket API (`wss://ws.bitmex.com/realtime`)
 
Real-time data is **not rate-limited** once connected. Critical subscriptions:
 
| Topic | Use |
|---|---|
| `funding` | Real-time funding rate updates for all perps |
| `instrument` | Live mark price, fair price, premium, indicative funding |
| `orderBookL2_25:SYMBOL` | Top-of-book for spread monitoring |
| `position` | Position changes (fills, liquidation warnings) |
| `execution` | Fill confirmations |
| `order` | Order state changes |
| `margin` | Available balance updates |
 
The WS supports **multiplexing** via `/realtimemd` — use one connection for market data and separate authenticated streams for each subaccount. Max 20 subscription args per message (use multiple subscribe calls).
 
### 3.3 Instrument Taxonomy
 
BitMEX uses `typ` codes on the `/instrument` endpoint:
 
- **FFWCSX** — Perpetual contracts (crypto underliers): XBTUSD, ETHUSD, SOLUSD, etc.
- **FFWCSF** — Perpetual contracts (FX underliers)
- **FFCCSX** — Futures (quarterly, bi-quarterly): XBTUSDTZ25, XBTUSDTH26, etc.
- **IFXXXP** — Spot pairs: XBT_USDT, ETH_USDT
- **MRCXXX** — Crypto indices (.BXBT, .BETH)
- **MRRXXX** — Lending/Premium indices (.XBTUSDPI, .XBTUSDPI8H)
 
The system must poll `/instrument/active` to dynamically discover which futures are live and their expiry dates.
 
### 3.4 Testnet
 
BitMEX provides a full testnet at `testnet.bitmex.com` — identical engine, free test BTC. The Python client defaults to testnet. **All strategies must be validated on testnet before live deployment.**
 
---
 
## 4. System Architecture
 
```
┌─────────────────────────────────────────────────────┐
│                  DOCKER COMPOSE                      │
│                                                      │
│  ┌──────────────────────┐  ┌──────────────────────┐  │
│  │   TRADING ENGINE     │  │   STRATEGY DASHBOARD │  │
│  │                      │  │                      │  │
│  │  ┌────────────────┐  │  │  ┌────────────────┐  │  │
│  │  │ WS Feed Handler│  │  │  │  FastAPI Server │  │  │
│  │  │ (market data)  │  │  │  │  (REST API)     │  │  │
│  │  └───────┬────────┘  │  │  └───────┬────────┘  │  │
│  │          │           │  │          │           │  │
│  │  ┌───────▼────────┐  │  │  ┌───────▼────────┐  │  │
│  │  │ Strategy Runner│  │  │  │  Opportunity    │  │  │
│  │  │ (executes)     │  │  │  │  Scanner        │  │  │
│  │  └───────┬────────┘  │  │  │  (analysis only)│  │  │
│  │          │           │  │  └───────┬────────┘  │  │
│  │  ┌───────▼────────┐  │  │          │           │  │
│  │  │ Order Manager  │  │  │  ┌───────▼────────┐  │  │
│  │  │ + Risk Guard   │  │  │  │  React Frontend│  │  │
│  │  └───────┬────────┘  │  │  │  (read-only UI)│  │  │
│  │          │           │  │  └────────────────┘  │  │
│  │  ┌───────▼────────┐  │  │                      │  │
│  │  │ BitMEX REST +  │  │  │                      │  │
│  │  │ Dead Man Switch│  │  │                      │  │
│  │  └────────────────┘  │  │                      │  │
│  └──────────┬───────────┘  └──────────┬───────────┘  │
│             │                         │              │
│  ┌──────────▼─────────────────────────▼───────────┐  │
│  │              PostgreSQL Database                │  │
│  │  - funding_rates    - positions                 │  │
│  │  - basis_history    - trades                    │  │
│  │  - strategies       - opportunities             │  │
│  │  - risk_snapshots   - pnl_ledger                │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```
 
### Isolation Principle
 
The Dashboard **never** places orders. It reads from the shared database and exposes a REST API + UI. The Engine writes opportunities it discovers to the DB, and the Dashboard visualises them. The Engine is the only process with BitMEX API keys that have order permissions.
 
---
 
## 5. Delta-Neutral Strategies
 
All strategies below maintain zero or near-zero net market exposure. The profit comes from structural premiums, funding flows, or temporal mispricings — not from the direction of the underlying.
 
---
 
### Strategy 1: Cash-and-Carry (Futures Basis Trade)
 
**Concept:** When a quarterly future trades at a premium to spot (contango), sell the future and buy the equivalent notional on the perpetual (or spot). At settlement, the future converges to spot, and you pocket the basis.
 
**Mechanics on BitMEX:**
 
1. Monitor `XBTUSDTZ26` (quarterly future) vs `.BXBT` (spot index) or `XBTUSD` (perpetual mark price).
2. Compute the annualised basis: `basis_annualised = (future_price / spot_price - 1) * (365 / days_to_expiry)`.
3. When annualised basis exceeds a threshold (e.g., >15% APR after fees), enter:
   - **Short** the quarterly future (e.g., XBTUSDTZ26)
   - **Long** the perpetual (XBTUSD) for the same notional
4. Hold until expiry. The future settles at the index price. Close the perp leg.
 
**Revenue:** The locked-in basis, minus trading fees (4 legs × ~0.02-0.075%) and funding payments on the perp leg.
 
**Risk factors:**
- Funding on the perp leg can be negative (you pay as a long when rates are positive). Historically, funding is positive ~92% of the time at the 0.01%/8h baseline (~10.95% APR). This eats into your carry if you're long perp.
- The trade is most attractive when basis >> expected cumulative funding cost over the holding period.
- Liquidation risk if margin is insufficient (use low leverage, 1-2x max).
- Settlement fee on futures: 0.05%.
 
**BitMEX data needed:** `/instrument` for future price + expiry, `/trade?symbol=.BXBT` for spot, `/funding` for funding history.
 
---
 
### Strategy 2: Funding Rate Harvesting (Perp-Spot)
 
**Concept:** When funding rates are persistently positive (longs pay shorts), go short on the perpetual and buy the equivalent on spot. You earn funding every 8 hours as a short while holding the underlying on spot, remaining delta-neutral.
 
**Mechanics on BitMEX:**
 
1. BitMEX now offers spot pairs (XBT_USDT, ETH_USDT) — you can hedge entirely on-exchange.
2. Monitor the current and predicted funding rate via the `instrument` WS topic (field: `fundingRate`, `indicativeFundingRate`).
3. When predicted funding is significantly positive (e.g., >0.03%/8h = ~40% APR):
   - **Short** the perpetual (XBTUSD)
   - **Buy** equivalent on spot (XBT_USDT)
4. Collect funding every 8h (04:00, 12:00, 20:00 UTC).
5. Exit when funding normalises below a threshold.
 
**Revenue:** Net funding received minus spot/perp spread slippage and trading fees.
 
**Why it works:** The funding rate formula has a structural positive bias (floor at ~0.01%/8h due to the interest rate component). Institutional capital (e.g., Ethena) aggressively arbs spikes above this level, pushing rates back down — but the baseline yield of ~10.95% APR remains attractive for patient capital.
 
**Risk factors:**
- Spot fees on BitMEX are 0.05% maker/taker (post-October 2025 reduction). Entry + exit = ~0.20% round trip for both legs combined.
- Funding can go negative temporarily during extreme bearish events (you'd pay as a short). The system should have a circuit breaker.
- Margin on the perp leg requires monitoring.
 
---
 
### Strategy 3: Cross-Tenor Basis Spread (Calendar Spread)
 
**Concept:** Exploit relative mispricing between two futures of different maturities on the same underlying. For example, XBTUSDTH26 (March) vs XBTUSDTM26 (June).
 
**Mechanics:**
 
1. Calculate the implied forward rate between two futures:
   `spread = (far_future / near_future - 1) * (365 / day_difference)`
2. If this spread deviates significantly from the historical mean or from the funding-implied forward rate:
   - **Short** the relatively expensive leg
   - **Long** the relatively cheap leg
3. Profit as the spread mean-reverts or as the near-dated contract settles.
 
**Revenue:** Spread compression/convergence.
 
**Advantage:** No spot or funding exposure at all — purely a relative value trade between two BitMEX futures. Very capital efficient.
 
**Risk:** The spread can widen before it narrows. Requires careful position sizing and margin management. Both legs settle at different dates, so there's roll risk.
 
**BitMEX data:** `/instrument?symbol=XBT:quarterly` and `/instrument?symbol=XBT:biquarterly` to get both tenors.
 
---
 
### Strategy 4: Cross-Quote Basis (XBTUSD vs XBTUSDT)
 
**Concept:** BitMEX lists the same underlying with different quote currencies — inverse contracts (XBTUSD, settled in BTC) and linear contracts (XBTUSDT, settled in USDT). These can diverge in premium and funding.
 
**Mechanics:**
 
1. Monitor the price and funding rate differential between XBTUSD (inverse) and XBTUSDT (linear).
2. When XBTUSD is at a premium relative to XBTUSDT (or vice versa):
   - **Short** the premium contract
   - **Long** the discount contract
3. Also harvest any funding rate differential between the two.
 
**Revenue:** Premium convergence + funding differential.
 
**Nuance:** The inverse contract's PnL is non-linear (denominated in BTC), so hedging requires dynamic sizing as price moves. The system must recalculate the BTC-equivalent notional continuously.
 
---
 
### Strategy 5: Funding Rate Mean-Reversion (Tactical)
 
**Concept:** Funding rates spike during high-volatility events (liquidation cascades, news) but structurally revert to the 0.01% baseline. Trade the perp directionally *against* extreme funding, hedged with spot or a future.
 
**Mechanics:**
 
1. Monitor funding rate z-scores across all perps (XBTUSD, ETHUSD, SOLUSD, etc.).
2. When funding spikes to extreme levels (>0.1%/8h or <-0.05%/8h):
   - If funding is extremely positive: **short** the perp, **buy** spot → earn outsized funding.
   - If funding is extremely negative: **long** the perp, **sell** spot → earn outsized funding as a long.
3. Close when funding normalises.
 
**Revenue:** The spike-to-baseline funding differential, often 5-20 funding periods of elevated rates.
 
**Edge:** The formula's clamp function limits how far funding can deviate from the interest rate (I), bounded by ±0.05% per interval around the premium index. But during extreme premiums, the cumulative effect is still substantial.
 
---
 
### Strategy 6: Multi-Asset Funding Rotation
 
**Concept:** Different perpetual contracts (BTC, ETH, SOL, DOGE, etc.) have different funding rate dynamics. ETH tends to have higher funding rate volatility (higher beta) than BTC. Rotate capital to whichever pair offers the best risk-adjusted funding yield at any given time.
 
**Mechanics:**
 
1. Every funding interval (or more frequently), rank all perps by:
   - Current funding rate
   - Predicted next funding rate (from `indicativeFundingRate`)
   - Liquidity (order book depth)
   - Recent volatility of funding (variance)
2. Allocate the short-perp + buy-spot trade to the top N pairs by expected yield.
3. Rebalance at each funding timestamp.
 
**Revenue:** Optimised funding yield across the portfolio.
 
**Data:** `/instrument/active` filtered by `typ=FFWCSX`, streaming `funding` and `instrument` WS topics for all pairs.
 
---
 
### Strategy 7: Spot-Perp Market Making (Advanced)
 
**Concept:** Simultaneously provide liquidity on spot and the perpetual, maintaining a net-zero position while earning the bid-ask spread on both sides plus the maker rebate.
 
**Mechanics:**
 
1. Quote both sides on XBTUSD (perp) and XBT_USDT (spot).
2. When filled on one leg, immediately hedge on the other.
3. Target: earn the spread + maker rebate (-0.025% on derivatives, up to -0.015% on spot at high volume).
 
**Revenue:** Spread capture + rebates.
 
**Risk:** Adverse selection, latency, inventory buildup during fast moves. Requires the `cancelAllAfter` dead-man's switch as a safety net.
 
---
 
## 6. Dashboard Design
 
The dashboard is a **read-only analytical tool** that does not execute trades. It serves as the opportunity discovery and monitoring layer.
 
### Pages / Views
 
| View | Content |
|---|---|
| **Basis Monitor** | Live annualised basis for every future vs spot/perp, with historical chart |
| **Funding Scanner** | Current + predicted funding for all perps, z-score ranking, historical heatmap |
| **Strategy Catalogue** | All available strategies with parameters, expected yield, and risk metrics |
| **Active Positions** | Engine's current positions, delta exposure, unrealised PnL (read from DB) |
| **PnL Ledger** | Historical trade-by-trade and strategy-level performance |
| **Risk Dashboard** | Net delta, margin utilisation, max drawdown, correlation matrix |
| **Alerts** | Configurable alerts for basis threshold, funding spike, margin warning |
 
### Data Flow
 
```
BitMEX WS → Engine (writes to DB) → Dashboard (reads from DB) → Browser
```
 
The Dashboard polls the database or uses PostgreSQL LISTEN/NOTIFY for near-real-time updates.
 
---
 
## 7. Risk Management Framework
 
Since all strategies are delta-neutral *in theory*, the risk framework focuses on ensuring neutrality is maintained in practice.
 
### Hard Rules (enforced in engine)
 
1. **Max net delta:** Absolute net delta exposure across all legs must stay below a configurable threshold (e.g., 0.5% of NAV). If breached, the engine stops opening new positions and hedges the residual.
2. **Dead-man's switch:** The engine calls `POST /order/cancelAllAfter` every 15 seconds with a 60-second timeout. If the engine crashes, all open orders auto-cancel within 60s.
3. **Max position size per strategy:** Configurable per-strategy caps.
4. **Max margin utilisation:** Never exceed 50% of available margin. Leave room for adverse moves.
5. **Funding cost circuit breaker:** If cumulative funding paid on a carry trade exceeds 50% of the locked-in basis, exit.
6. **Liquidation buffer:** Monitor `liquidationPrice` from the position endpoint. If current price is within 10% of liquidation, reduce position.
 
### Monitoring
 
- Log every order, fill, funding payment, and rebalance event to the database.
- Calculate real-time Greeks-equivalent: delta, gamma-equivalent (for inverse contracts), and vega-equivalent (for funding rate sensitivity).
- Daily mark-to-market reconciliation against BitMEX wallet balance.
 
---
 
## 8. Implementation Roadmap
 
### Phase 1 — Foundation (Weeks 1-3)
- Set up Python project with async architecture
- Implement BitMEX REST + WebSocket clients (wrapper around official SDK)
- Build database schema (funding_rates, instruments, candles, positions, trades)
- Historical data ingestion: backfill funding rates and settlement data
- Testnet connectivity verified
 
### Phase 2 — Strategy Dashboard MVP (Weeks 3-5)
- Build basis calculator and funding scanner
- Implement FastAPI backend with endpoints for all views
- Simple Streamlit or React frontend for visualisation
- Strategy catalogue with parameter definitions (read-only)
- Alerting system (email/Telegram for threshold breaches)
 
### Phase 3 — Trading Engine Core (Weeks 5-8)
- Order manager with retry logic, rate-limit awareness, and dead-man's switch
- Implement Strategy 1 (Cash-and-Carry) and Strategy 2 (Funding Harvest) first
- Position tracker synced with BitMEX via WS
- Risk guard module (delta check, margin check, circuit breakers)
- Full testnet paper trading
 
### Phase 4 — Expansion (Weeks 8-12)
- Add Strategies 3-6
- Portfolio-level risk aggregation
- PnL attribution by strategy
- Performance analytics (Sharpe, max drawdown, win rate)
- Gradual live deployment with minimal capital
 
### Phase 5 — Hardening (Ongoing)
- Monitoring and alerting (Prometheus/Grafana or similar)
- Automated restart and state recovery
- Cross-exchange expansion via ccxt (Binance, Bybit) for cross-venue arb
- Strategy 7 (market making) if latency requirements are met
 
---
 
## 9. Key BitMEX-Specific Considerations
 
**Funding timing:** Funding is exchanged at exactly 04:00, 12:00, 20:00 UTC. You only pay/receive if you hold a position at that exact moment. Positions opened 1 minute before and closed 1 minute after are treated identically to positions held for 8 hours. This creates a tactical opportunity: you can enter a funding-harvesting position just before the timestamp and exit right after.
 
**Inverse vs Linear contracts:** XBTUSD is inverse (settled in BTC). Your PnL in BTC terms is non-linear: a $1 move has different BTC impact at $100k vs $50k. The system must account for this when computing hedge ratios.
 
**Settlement mechanics:** Futures settle at the time-weighted average of the spot index over a window before expiry (not a single print). This reduces manipulation risk but means the exact settlement price isn't known until the window closes.
 
**Withdrawal batching:** BitMEX processes withdrawals in daily batches with manual review. This is relevant for capital management — funds cannot be moved instantly.
 
**Bulk orders:** The API supports bulk order placement via `POST /order/bulk`, which is more rate-limit efficient for multi-leg strategies.
 
---
 
## 10. Expected Return Profile
 
| Strategy | Expected APR (gross) | Key Cost | Net APR Estimate |
|---|---|---|---|
| Cash-and-Carry | 8-25% (basis dependent) | Funding on perp leg (~10.95% APR) + fees | 5-15% |
| Funding Harvest | 10-40% (during spikes) | Spread + spot fees (~0.20% round trip) | 8-30% |
| Calendar Spread | 5-15% | Fees only (~0.15% round trip) | 4-14% |
| Cross-Quote Basis | 3-10% | Fees + dynamic hedging cost | 2-8% |
| Funding Rotation | 12-35% | Rebalancing fees | 10-25% |
 
These are rough estimates. Actual returns depend heavily on market conditions (volatility regime, open interest, institutional flow). The system should track realised returns per strategy and dynamically allocate capital to the best performers.
 
---
 
## 11. File/Module Structure
 
```
bitmex-delta-neutral/
├── docker-compose.yml
├── config/
│   ├── settings.toml          # Strategy params, thresholds
│   └── .env                   # API keys (gitignored)
├── engine/
│   ├── __init__.py
│   ├── main.py                # Entry point, event loop
│   ├── ws_feed.py             # WebSocket connection manager
│   ├── rest_client.py         # REST API wrapper
│   ├── order_manager.py       # Order placement, cancellation, dead-man's switch
│   ├── position_tracker.py    # Real-time position sync
│   ├── risk_guard.py          # Delta checks, margin checks, circuit breakers
│   ├── strategies/
│   │   ├── base.py            # Abstract strategy interface
│   │   ├── cash_and_carry.py
│   │   ├── funding_harvest.py
│   │   ├── calendar_spread.py
│   │   ├── cross_quote.py
│   │   ├── funding_rotation.py
│   │   └── market_making.py
│   └── db/
│       ├── models.py          # SQLAlchemy models
│       └── repository.py      # Data access layer
├── dashboard/
│   ├── api/
│   │   ├── main.py            # FastAPI app
│   │   └── routes/
│   │       ├── basis.py
│   │       ├── funding.py
│   │       ├── strategies.py
│   │       ├── positions.py
│   │       └── risk.py
│   └── frontend/              # React app (or Streamlit)
│       └── ...
├── scripts/
│   ├── backfill_funding.py    # Historical data ingestion
│   └── backfill_settlements.py
└── tests/
    ├── test_strategies.py
    ├── test_risk_guard.py
    └── test_order_manager.py
```
 
---
 
*This plan is based on research from BitMEX's official API documentation, their perpetual contracts guide, futures guide, WebSocket API reference, fee schedules, and their Q3 2025 derivatives report on funding rate structure. All strategies should be validated on BitMEX testnet before any live capital is deployed.*

---

## /autoplan Review — Phase 1: CEO Review

**Mode:** SELECTIVE EXPANSION | **Score:** 6.5/10 | **Voices:** [subagent-only] (Codex sandbox failure)

### Premises

| Premise | Status |
|---|---|
| BitMEX as primary venue | ACCEPTED |
| Delta-neutral strategies structurally sound | ACCEPTED |
| Python/asyncio/SQLite stack | ACCEPTED — right call |
| Net yield 5-15% APR (Strategy 1) | UNVALIDATED — no backtest |
| Funding positive 92% = edge | WEAK — also the cost of perp-long leg |
| Delta-neutral = zero risk | IMPRECISE — BTC exposure drifts in inverse contracts |

### Dream State
```
CURRENT: S1+S2 testnet, monitoring dashboard, no backtest, no live criteria
THIS PLAN: S3 + backtest + live criteria + opportunity scanner + attribution
12-MONTH: 5 viable strategies live, regime-aware allocation, 30+ days PnL record
```

### Added to Scope
1. Backtest harness for S1+S2 on historical DB data
2. Live deployment criteria document (explicit go/no-go thresholds)
3. Strategy 3 (Calendar Spread)
4. Dashboard opportunity scanner (basis monitor + funding z-score ranking)
5. Performance attribution queries by strategy
6. `cancelAllAfter` retry handler (open safety item)
7. S2 negative funding circuit breaker unit test (open safety item)
8. Delta stress test (30% BTC price move scenario)

### Deferred to TODOS.md
- Strategy 6 (Multi-Asset Rotation) — after S3 stable
- Docker/systemd process management
- Cross-exchange Binance spot (already in TODOS)
- Strategy 7 (Market Making) — architecture incompatible with 30s loop

### Error & Rescue Registry

| Error | Catcher | Tested |
|---|---|---|
| `cancelAllAfter` call fails | NO retry handler | NO |
| WS disconnect during entry | 120s dead-man extended | PARTIAL |
| S2 negative funding | Circuit breaker | NO unit test |
| HARD_STOP during slicing | `unwind()` | PARTIAL |
| Rate limit exhaustion | Token bucket (20-reserve) | YES |

### Failure Modes Registry

| Mode | Impact | Mitigation Status |
|---|---|---|
| Engine crash mid-entry | Stuck ENTERING position | Dead-man cancels orders; manual reconcile |
| Funding flips negative (S2) | Losses on hedge leg | Circuit breaker exits — test MISSING |
| Basis collapses early (S1) | Missed early exit | No trigger; expires at 24h-before-expiry |
| SQLite corruption | DB loss | No backup documented |
| BitMEX downtime | No trading | Acceptable; positions hedged |

### NOT In Scope
- Strategy 7 (market making) — incompatible architecture
- Cross-exchange execution — TODOS.md
- Prometheus/Grafana — TODOS.md
- Docker Compose — TODOS.md

### Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|-------|----------|----------------|-----------|-----------|----------|
| 1 | CEO | Mode = SELECTIVE EXPANSION | Mechanical | P3 | System built; baseline is bulletproof; audit gaps + surface next | HOLD SCOPE |
| 2 | CEO | Approach B (validate-first) before A (expand) | Mechanical | P1+P6 | Backtest before expanding strategies | Approach A (expand first) |
| 3 | CEO | Add backtest harness to scope | Mechanical | P1 | Validates investment thesis; lake to boil | Defer |
| 4 | CEO | Add live criteria doc to scope | Mechanical | P6 | Action bias; 1 day CC | Defer |
| 5 | CEO | Add Strategy 3 to scope | Mechanical | P1 | Lowest infra delta next strategy | Defer |
| 6 | CEO | Defer Strategy 6 | Mechanical | P3 | Depends on S3; TODOS.md already has it | Add |
| 7 | CEO | Add dashboard opportunity scanner | Mechanical | P1 | Original plan intent; currently underperforms spec | Defer |
| 8 | CEO | Add performance attribution | Mechanical | P1 | Needed before live; data already in DB | Defer |
| 9 | CEO | Add cancelAllAfter retry | Mechanical | P1 | Safety-critical open item | Defer |
| 10 | CEO | Add S2 circuit breaker test | Mechanical | P1 | Safety-critical open item | Defer |
| 11 | CEO | Add delta stress test | Mechanical | P1 | Unmodeled safety risk; inverse contract nonlinearity | Defer |

---

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 1 | COMPLETE | 8 scope items, 4 deferred |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | — | — |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 2 | COMPLETE | 2 critical + 5 pre-live blockers fixed; 61 tests |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | — |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | — |

**Architecture changes accepted:** Delta rebalance trigger, WS reconnect reconciliation, systemd auto-restart, stateless strategies, no custom REST wrapper, TwoLegStrategy base class, progressive entry (N-slice both-legs-simultaneous), WS-only for live funding, shared rate-limit token bucket with emergency reserve, dead-man's switch extended to 120s during reconnect, SQLite WAL mode, token bucket initialized from X-RateLimit-Remaining on startup.

**Implementation architecture:** Lean Structured (Approach C) — single asyncio process, SQLite+SQLAlchemy, Streamlit dashboard, ccxt order layer only, BitMEX-specific funding/margin semantics.

---

## /autoplan Phase 3 — Eng Review (2026-04-11)

### Critical Bugs Fixed

**BUG-1 (CRITICAL): `exit()` side-reversal used wrong string comparison**
- File: `engine/strategies/two_leg.py` lines 144, 156
- Root cause: `position.leg_a_side == "short"` — but sides are stored as `"sell"`/`"buy"` not `"short"`/`"long"`. Every exit doubled the short instead of closing it.
- Fix: Changed to `== "sell"` to produce the correct reverse side.
- Tests added: `TestExitSideReversal::test_short_leg_exits_with_buy`, `test_long_leg_exits_with_sell`

**BUG-2 (CRITICAL): `get_net_delta_usd()` called `.get()` on tuple not dict**
- File: `engine/position_tracker.py` line 293
- Root cause: `for pos in self._live_positions.items()` yields `(key, value)` tuples. `pos.get("currentQty")` on a tuple → always 0. Delta guard silently neutered.
- Fix: Changed to `for _symbol, pos in self._live_positions.items()`
- Tests added: `test_position_tracker_delta_reads_dict_not_tuple`

**BUG-3 (HIGH): HARD_STOP blocked exits, not just entries**
- File: `engine/strategies/base.py` lines 97-111
- Root cause: Early return on `HARD_STOP` prevented `should_exit()` from being called. Positions that need to close during a margin crisis would stay open.
- Fix: Restructured `run_once()` — exits always checked, entries blocked on `HARD_STOP`.

### Medium Severity Items

| # | Issue | File | Disposition |
|---|-------|------|-------------|
| M1 | WS reconnect sets `_ready` even on failed reconciliation | `position_tracker.py:176` | Open (tracked below) |
| M2 | `cancelAllAfter` no retry on failure — 15s window gap | `risk_guard.py:114` | Open (tracked below) |
| M3 | `should_enter()` and `compute_entry_spec()` both call `_get_nearest_future()` | `cash_and_carry.py` | Open (low risk at $10k size) |
| M4 | Partial fill on leg A unwind not checked | `order_manager.py:241` | Open (pre-live blocker) |
| M5 | Funding circuit breaker missing for Strategy 2 cumulative cost | `funding_harvest.py` | Open (pre-live blocker) |
| M6 | `get_rate_limit_remaining()` swallows exception → stale 300 | `exchange/bitmex.py:168` | Open |
| M7 | `get_net_delta_usd()` doesn't multiply spot qty by price (dimensionally inconsistent) | `position_tracker.py:289` | Open (pre-live blocker) |

### Test Coverage Added (58 total, was 55)
- `TestExitSideReversal::test_short_leg_exits_with_buy` — regression for BUG-1
- `TestExitSideReversal::test_long_leg_exits_with_sell` — regression for BUG-1
- `test_position_tracker_delta_reads_dict_not_tuple` — regression for BUG-2

**Tests: 61 passing** (+3 cumulative cost circuit breaker)

**OPEN ITEMS (resolve before live capital):** ~~all 5 resolved~~ **CLOSED 2026-04-12**
1. ~~`cancelAllAfter` retry handler~~ — immediate retry + critical log if retry fails
2. ~~WS reconciliation `_ready` guard~~ — 3× retry with 5s backoff; strategies pause on exhaustion
3. ~~Spot delta dimensionally wrong~~ — `get_net_delta_usd()` now multiplies spot qty by lastPrice
4. ~~Unwind orphan no log~~ — `CRITICAL exchange_orphan_created` log with full context
5. ~~Strategy 2 circuit breaker missing~~ — `max_cumulative_funding_cost` threshold in `should_exit()`

**VERDICT:** All pre-live engineering blockers resolved. 61 tests passing. Codebase is testnet-ready.

---

## /autoplan Phase 3.5 — DX Review (2026-04-11)

### DX Scorecard

| Dimension | Score (before) | Score (after) | Notes |
|-----------|---------------|---------------|-------|
| Onboarding (TTHW) | 5/10 | 7/10 | `make install` added; README still missing |
| Configuration safety | 7/10 | 9/10 | Startup credential check + live mode warning added |
| Test ergonomics | 8/10 | 8/10 | Already good |
| Local iteration loop | 6/10 | 6/10 | 30s loop still requires patience; `--once` flag deferred |
| Error messages | 5/10 | 8/10 | Clear `sys.exit()` message replacing raw `KeyError` |
| Observability | 7/10 | 7/10 | structlog + dashboard — adequate |
| Scripts and automation | 5/10 | 7/10 | `make install`, `make lint`, non-destructive `make setup` |
| Documentation | 4/10 | 4/10 | No README — open item |
| **Overall** | **5.9/10** | **7.0/10** | |

### Fixes Applied

1. **Startup credential check** (`engine/main.py`): `sys.exit()` with human-readable message + testnet URL instead of raw `KeyError`
2. **Live mode warning** (`engine/main.py`): logs `WARNING` when `testnet = false`
3. **Non-destructive `make setup`** (`Makefile`): only restores files if missing, never overwrites local edits to `settings.toml`
4. **`make install`** (`Makefile`): creates `.venv` and installs deps — documented entry point for new developers
5. **`make lint`** (`Makefile`): ruff check (soft fail) for code quality

### Open DX Items

1. No README at project root — new developers have no starting point
2. Python version requirement not documented (3.11+ required for `tomllib`)
3. No `--once` flag on engine to force a single `run_once()` cycle during strategy development
4. Funding Rates dashboard page silently empty until backfill is run — dependency not documented

**VERDICT:** DX score improved from 5.9 to 7.0 with 5 targeted fixes under 1 hour total. Core friction points (credential error UX, destructive setup, missing install target) resolved.
 
