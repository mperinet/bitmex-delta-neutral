"""
Backfill historical funding rates from BitMEX.

Usage:
  python scripts/backfill_funding.py --symbols XBTUSD ETHUSD --limit 500
"""

import asyncio
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / "config" / ".env")

try:
    import tomli as tomllib
except ImportError:
    import tomllib


async def backfill(symbols: list[str], limit: int) -> None:
    from engine.db.models import init_db
    from engine.exchange.bitmex import BitMEXExchange
    from engine.db import repository

    config_path = Path(__file__).parent.parent / "config" / "settings.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    # Ensure data dir exists
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)

    await init_db(config["database"]["url"])

    exchange = BitMEXExchange(
        api_key=os.environ.get("BITMEX_API_KEY", ""),
        api_secret=os.environ.get("BITMEX_API_SECRET", ""),
        testnet=config["exchange"].get("testnet", True),
    )

    for symbol in symbols:
        print(f"Backfilling {symbol} (limit={limit})...")
        try:
            rates = await exchange.get_historical_funding(symbol, limit=limit)
            for r in rates:
                from datetime import datetime
                ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
                await repository.insert_funding_rate(symbol, ts, r["rate"])
            print(f"  → {len(rates)} records inserted for {symbol}")
        except Exception as e:
            print(f"  ERROR for {symbol}: {e}")

    await exchange.close()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill funding rate history")
    parser.add_argument("--symbols", nargs="+", default=["BTC/USD:BTC"], help="ccxt symbols")
    parser.add_argument("--limit", type=int, default=500, help="number of records per symbol")
    args = parser.parse_args()
    asyncio.run(backfill(args.symbols, args.limit))
