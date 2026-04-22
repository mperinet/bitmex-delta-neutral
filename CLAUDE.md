# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make install        # Create .venv and install dependencies (run once after clone)
make setup          # Restore tracked config files, verify .env exists
make test           # Run full test suite (.venv/bin/python -m pytest tests/ -v)
make engine         # Start trading engine
make dashboard      # Run Streamlit dashboard on port 8501
make backfill-btc   # Ingest 500 historical BTC funding rate records
make backfill-eth   # Ingest 500 historical ETH funding rate records
make lint           # Lint engine/, dashboard/, tests/, scripts/ with ruff
make format         # Auto-fix + format with ruff
make typecheck      # Run mypy on engine/
make trading-analysis  # Run trading analysis dashboard on port 8502
```

Control CLI (engine must be running first):
```bash
make smoke-test     # trigger one-shot smoke test
make smoke-abort    # abort smoke test in progress
make smoke-test-eth # trigger one-shot ETH smoke test
make smoke-abort-eth # abort ETH smoke test in progress
make delta-check    # trigger delta balance check
make delta-abort    # abort delta check in progress
make ctl-status     # check if engine control server is reachable
```

Run a single test file:
```bash
PYTHONPATH=. .venv/bin/python -m pytest tests/test_strategies.py -v
```

Run a single test by name:
```bash
PYTHONPATH=. .venv/bin/python -m pytest tests/test_risk_guard.py::test_delta_within_bounds -v
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
7. Start HTTP control server (`engine/control/server.py`) on `127.0.0.1:8552`
8. Main loop: `strategy.run_once()` for each enabled strategy; risk snapshot every 5 min. Loop sleeps with `asyncio.wait_for(control_queue.get(), timeout=30s)` — wakes immediately on any control command

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
| Market data cache | `engine/market_data.py` | In-memory funding rates + instrument metadata from WS |
| Exchange I/O | `engine/exchange/bitmex.py` | ccxt wrapper, inverse contract math |
| DB layer | `engine/db/` | SQLAlchemy models + async repository |
| Control server | `engine/control/server.py` | aiohttp HTTP server on :8552 — accepts operator commands, logs to DB, wakes main loop via asyncio.Queue |
| Control CLI | `scripts/ctl.py` | Operator CLI — sends commands to the control server |
| Dashboard | `dashboard/app.py` | Read-only Streamlit UI, polls DB every 5s |
| Trading Analysis | `trading_analysis/app.py` | Read-only analytics dashboard on port 8502; fetches account-wide funding payments + execution fees from BitMEX via dedicated readonly API keys; owns its own SQLite DB (`data/trading_analysis.db`) |
| Funding Analysis | `funding_analysis/app.py` | Read-only market analysis dashboard on port 8503; aggregates BitMEX + HyperLiquid funding rates and Binance USD margin borrow cost to identify delta-neutral opportunities; owns its own SQLite DB (`data/funding_analysis.db`). Public APIs for HL/BitMEX funding; Binance readonly key required for margin-rate history and user borrow history |

### Strategies

All strategies extend `TwoLegStrategy` (`engine/strategies/two_leg.py`).

Live strategies (always running):
- **Cash-and-Carry** (`cash_and_carry.py`): Short nearest quarterly BTC future + long XBTUSD perpetual. Entry when annualised basis > 10% APR. Exit on: 24h before expiry, funding circuit breaker (paid funding > 50% of locked basis), or risk signals.
- **Funding Harvest** (`funding_harvest.py`): Short XBTUSD perp + buy XBT_USDT spot. Entry when 8h funding rate > 3× baseline (0.03%/8h). Exit when rate falls below baseline (0.01%/8h), flips negative, or risk signals. Has `max_cumulative_funding_cost` circuit breaker (default 20bps).

One-shot strategies (triggered on demand via `scripts/ctl.py`):
- **Smoke Test** (`smoke_test.py`): Short nearest BTC future + long XBTUSD perp. Enters, observes one active tick, exits. Validates the full execution pipeline.
- **Delta Check** (`delta_check.py`): Short XBTUSD perp + long XBT_USDT spot. Reads `get_net_delta_usd()` while ACTIVE, logs balance verdict, exits. Validates the inverse+spot delta calculation.

Both funding rate concepts are distinct:
- `market_data.get_latest_funding_rate(symbol)` — last confirmed settlement event (04:00/12:00/20:00 UTC)
- `market_data.get_predictive_funding_rate(symbol)` — live indicative rate from instrument stream; use for entry/exit signals

### Risk guard levels

`engine/risk_guard.py` returns one of: `OK`, `WARNING`, `REBALANCE`, `HARD_STOP`.
- `REBALANCE`: net delta has drifted > 0.5% of NAV → rebalance before next entry
- `HARD_STOP`: margin utilization > 50% → no new entries, begin unwind

### Control signal flow

Commands are sent via `scripts/ctl.py` → HTTP POST to the engine's control server (`engine/control/server.py` on `:8552`) → logged to `ControlSignal` table (audit trail) → pushed onto an `asyncio.Queue`. The main loop wakes immediately from `asyncio.wait_for(queue.get(), ...)` instead of waiting for the next 30s tick.

The dashboard is **read-only**: it shows status and signal history but has no Run/Abort buttons. All operator actions go through `scripts/ctl.py`.

### Configuration

`config/settings.toml` controls all strategy parameters, risk thresholds, and DB URL. Key sections: `[exchange]`, `[database]`, `[risk]`, `[strategy.cash_and_carry]`, `[strategy.funding_harvest]`, `[control]`.

Secrets (`BITMEX_API_KEY`, `BITMEX_API_SECRET`, optional Telegram creds) go in `config/.env` (not committed). `config/.env.example` is the template.

Toggle testnet vs live: set `testnet = true/false` under `[exchange]` in `settings.toml`.

### Exchange abstraction boundary

ccxt is used **only** for order placement and basic account queries. Funding rate semantics, inverse contract math (BTC-settled contracts), and margin calculations are BitMEX-specific and live in `engine/exchange/bitmex.py` — do not try to abstract these across exchanges.

## Skill routing

When the user's request matches an available skill, ALWAYS invoke it using the Skill
tool as your FIRST action. Do NOT answer directly, do NOT use other tools first.
The skill has specialized workflows that produce better results than ad-hoc answers.

Key routing rules:
- Product ideas, "is this worth building", brainstorming → invoke office-hours
- Bugs, errors, "why is this broken", 500 errors → invoke investigate
- Ship, deploy, push, create PR → invoke ship
- QA, test the site, find bugs → invoke qa
- Code review, check my diff → invoke review
- Update docs after shipping → invoke document-release
- Weekly retro → invoke retro
- Design system, brand → invoke design-consultation
- Visual audit, design polish → invoke design-review
- Architecture review → invoke plan-eng-review
- Save progress, checkpoint, resume → invoke checkpoint
- Code quality, health check → invoke health
