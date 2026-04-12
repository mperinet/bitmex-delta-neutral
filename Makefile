.PHONY: setup install test lint format typecheck backfill engine dashboard smoke-test smoke-abort delta-check delta-abort ctl-status

# Sync venv from pyproject.toml + uv.lock (run once after clone, or after dep changes)
install:
	uv sync --group dev

# Restore tracked config files only if missing (does NOT overwrite local edits)
setup:
	@test -f config/settings.toml || git checkout HEAD -- config/settings.toml
	@test -f config/.env.example  || git checkout HEAD -- config/.env.example
	@if [ ! -f config/.env ]; then \
		echo ""; \
		echo "WARNING: config/.env is missing. Create it with your testnet credentials:"; \
		echo "  printf 'BITMEX_API_KEY=<key>\\nBITMEX_API_SECRET=<secret>\\n' > config/.env"; \
		echo ""; \
	fi

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check engine/ dashboard/ tests/ scripts/

format:
	uv run ruff check --fix engine/ dashboard/ tests/ scripts/
	uv run ruff format engine/ dashboard/ tests/ scripts/

typecheck:
	uv run mypy engine/

backfill-btc:
	PYTHONPATH=. uv run python scripts/backfill_funding.py --symbols "BTC/USD:BTC" --limit 500

backfill-eth:
	PYTHONPATH=. uv run python scripts/backfill_funding.py --symbols "ETH/USD:BTC" --limit 500

backfill:
	PYTHONPATH=. uv run python scripts/backfill_funding.py --symbols "$(SYMBOL)" --limit $(or $(LIMIT),500)

engine: setup
	PYTHONPATH=. uv run python -m engine.main

dashboard:
	uv run streamlit run dashboard/app.py --server.port 8501

# Control CLI — engine must be running (make engine) before using these
smoke-test:
	PYTHONPATH=. uv run python scripts/ctl.py smoke_test

smoke-abort:
	PYTHONPATH=. uv run python scripts/ctl.py smoke_test_abort

delta-check:
	PYTHONPATH=. uv run python scripts/ctl.py delta_check

delta-abort:
	PYTHONPATH=. uv run python scripts/ctl.py delta_check_abort

ctl-status:
	PYTHONPATH=. uv run python scripts/ctl.py status
