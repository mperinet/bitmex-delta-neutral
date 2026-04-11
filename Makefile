.PHONY: setup test backfill engine dashboard

# Restore tracked config files that get wiped between sessions
setup:
	git checkout HEAD -- config/settings.toml config/.env.example
	@if [ ! -f config/.env ]; then \
		echo ""; \
		echo "WARNING: config/.env is missing. Create it with your testnet credentials:"; \
		echo "  printf 'BITMEX_API_KEY=<key>\\nBITMEX_API_SECRET=<secret>\\n' > config/.env"; \
		echo ""; \
	fi

test:
	PYTHONPATH=. .venv/bin/python -m pytest tests/ -v

backfill-btc:
	PYTHONPATH=. .venv/bin/python scripts/backfill_funding.py --symbols "BTC/USD:BTC" --limit 500

backfill-eth:
	PYTHONPATH=. .venv/bin/python scripts/backfill_funding.py --symbols "ETH/USD:BTC" --limit 500

engine: setup
	PYTHONPATH=. .venv/bin/python -m engine.main

dashboard:
	.venv/bin/streamlit run dashboard/app.py --server.port 8501
