# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make setup          # Restore tracked config files, verify .env exists
make test           # Run full test suite (pytest -v)
make engine         # Start trading engine
make dashboard      # Run Streamlit dashboard on port 8501
make backfill-btc   # Ingest 500 historical BTC funding rate records
make backfill-eth   # Ingest 500 historical ETH funding rate records
```

Run a single test file:
```bash
PYTHONPATH=. pytest tests/test_strategies.py -v
```

Run a single test by name:
```bash
PYTHONPATH=. pytest tests/test_risk_guard.py::test_delta_rebalance -v
```

All tests use `asyncio_mode = auto` (configured in `pytest.ini`) — no special decorator needed.

## Architecture

This is an async, delta-neutral automated trading engine for BitMEX. The engine and dashboard are **separate processes** sharing a single SQLite database (WAL mode for concurrent access).

### Startup sequence (`engine/main.py`)

1. Load `config/settings.toml` + `config/.env`
2. Init SQLite DB (creates `data/trading.db` on first run)
3. Connect to BitMEX via ccxt (REST) + seed the rate-limit token bucket from `X-RateLimit-Remaining`
4. Start risk guard and dead-man's switch (`cancelAllAfter` every 15s)
5. Start position tracker → reconcile DB positions with live exchange state → open WebSocket
6. Wait for position tracker `ready` signal before executing any strategy
7. Main loop (30s interval): `strategy.run_once()` for each enabled strategy; risk snapshot every 5 min

### Key design invariants

- **Stateless strategies**: Strategies hold no in-memory state. All positions live in the DB and are loaded on each `run_once()`. On restart, the engine re-reads open positions from DB and reconciles with the exchange.
- **Dead-man's switch**: `cancelAllAfter` is sent every 15s (extended to 120s during WS reconnect). If the engine crashes, all open orders cancel automatically.
- **Progressive entry**: Both legs of a trade are placed in N slices (`entry_slices` in config). If one leg times out (default 30s), the other is unwound before aborting.
- **Rate limit token bucket** (`engine/order_manager.py`): Seeded from the exchange header at startup. An emergency reserve of 20 tokens is held for risk operations (rebalance, unwind). Strategies draw from the remainder.

### Component map

| Component | File | Responsibility |
|---|---|---|
| Entry point | `engine/main.py` | Startup, main loop, orchestration |
| Strategies | `engine/strategies/` | Entry/exit signal logic |
| Order execution | `engine/order_manager.py` | Rate-limited order placement, progressive entry/unwind |
| Risk enforcement | `engine/risk_guard.py` | Delta/margin constraints, dead-man's switch |
| Real-time state | `engine/position_tracker.py` | WS subscriptions, reconciliation on startup/reconnect |
| Exchange I/O | `engine/exchange/bitmex.py` | ccxt wrapper, inverse contract math |
| DB layer | `engine/db/` | SQLAlchemy models + async repository |
| Dashboard | `dashboard/app.py` | Read-only Streamlit UI, polls DB every 5s |

### Strategies

Both live strategies extend `TwoLegStrategy` (`engine/strategies/two_leg.py`):

- **Cash-and-Carry** (`cash_and_carry.py`): Short nearest quarterly BTC future + long XBTUSD perpetual. Entry when annualised basis > 10% APR. Exit on: 24h before expiry, funding circuit breaker (paid funding > 50% of locked basis), or risk signals.
- **Funding Harvest** (`funding_harvest.py`): Short XBTUSD perp + buy XBT_USDT spot. Entry when 8h funding rate > 3× baseline (0.03%/8h). Exit when rate falls below baseline (0.01%/8h), flips negative, or risk signals.

### Risk guard levels

`engine/risk_guard.py` returns one of: `OK`, `WARNING`, `REBALANCE`, `HARD_STOP`.
- `REBALANCE`: net delta has drifted > 0.5% of NAV → rebalance before next entry
- `HARD_STOP`: margin utilization > 50% → no new entries, begin unwind

### Configuration

`config/settings.toml` controls all strategy parameters, risk thresholds, and DB URL. Key sections: `[exchange]`, `[database]`, `[risk]`, `[strategy.cash_and_carry]`, `[strategy.funding_harvest]`.

Secrets (`BITMEX_API_KEY`, `BITMEX_API_SECRET`, optional Telegram creds) go in `config/.env` (not committed). `config/.env.example` is the template.

Toggle testnet vs live: set `testnet = true/false` under `[exchange]` in `settings.toml`.

### Exchange abstraction boundary

ccxt is used **only** for order placement and basic account queries. Funding rate semantics, inverse contract math (BTC-settled contracts), and margin calculations are BitMEX-specific and live in `engine/exchange/bitmex.py` — do not try to abstract these across exchanges.
