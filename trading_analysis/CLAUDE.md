# trading_analysis

Read-only analytics dashboard for account-wide funding payments, execution fees, and wallet transactions fetched from BitMEX.

## Commands

```bash
make trading-analysis   # Run on port 8502
# or directly:
uv run streamlit run trading_analysis/app.py --server.port 8502
```

## Config requirements

`config/settings.toml`:
```toml
[trading_analysis]
db_url = "sqlite+aiosqlite:///data/trading_analysis.db"
testnet = false
```

`config/.env` — **separate readonly-only keys** (distinct from the main engine's `BITMEX_API_KEY`):
```
BITMEX_READONLY_API_KEY=...
BITMEX_READONLY_API_SECRET=...
```

## Components

| File | Responsibility |
|------|---------------|
| `app.py` | Streamlit UI, data loaders (`@st.cache_data(ttl=60)`), sync trigger buttons |
| `exchange.py` | Thin ccxt async wrapper — fetch funding executions, trade executions, wallet history (no order placement) |
| `sync.py` | Three incremental sync functions (funding, execution fees, wallet); each tracks its own cursor in `sync_cursors` table |
| `db/models.py` | SQLAlchemy models: `funding_payments`, `execution_fees`, `wallet_transactions`, `sync_cursors` |
| `db/repository.py` | All upsert + query functions; `set_sync_cursor` / `get_sync_cursor` |

Database: `data/trading_analysis.db` (SQLite WAL, separate from main `trading.db`)

## Non-obvious patterns

**Async-to-sync bridge** — Streamlit is sync; all async DB/exchange calls go through:
```python
_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
run_async(coro)  # single-threaded to avoid SQLite contention
```

**Schema migration** — `init_db()` runs on every page load; `ALTER TABLE` calls are wrapped in try/except (column-already-exists → silent pass). Safe to redeploy without migration scripts.

**Timestamps** — BitMEX returns ISO 8601 with `Z`; stored as naive UTC strings in SQLite. Strip tzinfo before any DB filter or comparison.

**Fee sign conventions:**
- `funding_payments.fee_amount`: positive = received, negative = paid
- `execution_fees.fee_amount`: negative = fee paid, positive = maker rebate
- All amounts in smallest units (satoshis for XBt, micro-USD for USDt); divide by 1e8 / 1e6 for display

**Pre-Nov-2024 data gap** — older funding records have `fee_amount = 0` because `execComm` was unreliable. The "Backfill wallet history" button in the UI patches these by cross-matching wallet history entries (±30s timestamp window, genuine funding settled at 04:00/12:00/20:00 UTC only).

**Wallet sync is newest-first** — `/user/walletHistory` has no `startTime` param; the sync stops as soon as it hits a known `transact_id`.
