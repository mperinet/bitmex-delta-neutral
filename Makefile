.PHONY: setup install test lint backfill engine dashboard

# Create venv and install dependencies (run once after clone)
install:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

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
	PYTHONPATH=. .venv/bin/python -m pytest tests/ -v

lint:
	.venv/bin/python -m ruff check engine/ dashboard/ tests/ || true

backfill-btc:
	PYTHONPATH=. .venv/bin/python scripts/backfill_funding.py --symbols "BTC/USD:BTC" --limit 500

backfill-eth:
	PYTHONPATH=. .venv/bin/python scripts/backfill_funding.py --symbols "ETH/USD:BTC" --limit 500

backfill:
	PYTHONPATH=. .venv/bin/python scripts/backfill_funding.py --symbols "$(SYMBOL)" --limit $(or $(LIMIT),500)

engine: setup
	PYTHONPATH=. .venv/bin/python -m engine.main

dashboard:
	.venv/bin/streamlit run dashboard/app.py --server.port 8501
