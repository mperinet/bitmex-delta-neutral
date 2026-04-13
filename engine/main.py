"""
Trading engine entry point.

Startup sequence:
  1. Load config + env
  2. Init DB (SQLite + WAL mode)
  3. Init exchange (ccxt, read rate-limit remaining from headers)
  4. Init order manager with token bucket (seeded from rate-limit headers)
  5. Init risk guard + start dead-man's switch
  6. Init position tracker + reconcile with exchange (REST)
  7. Wait for position tracker ready signal
  8. Start control server (aiohttp on 127.0.0.1:8552)
  9. Start strategies
 10. Run main loop

The main loop calls strategy.run_once() every LOOP_INTERVAL_S seconds.
Strategies are stateless — run_once() is a single check-and-act cycle.

Control commands arrive via HTTP POST to the control server and are put on
an asyncio.Queue. The main loop uses asyncio.wait_for(queue.get(), timeout=30)
instead of asyncio.sleep(), so commands take effect immediately rather than
waiting up to 30 seconds for the next tick.
"""

import asyncio
import os
import sys
import tomllib
from pathlib import Path

import structlog
from dotenv import load_dotenv

# Load .env before importing anything that reads it
load_dotenv(Path(__file__).parent.parent / "config" / ".env")

logger = structlog.get_logger(__name__)

LOOP_INTERVAL_S = 30  # seconds between strategy run_once() calls
RISK_SNAPSHOT_INTERVAL_S = 300  # save risk snapshot every 5 minutes


def load_config() -> dict:
    config_path = Path(__file__).parent.parent / "config" / "settings.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def _dispatch_command(
    cmd: dict,
    smoke_strategy,
    delta_check_strategy,
    *,
    exchange,
    order_mgr,
    tracker,
    risk_guard,
    strategy_config: dict,
):
    """
    Dispatch a control command from the queue. Instantiates one-shot strategies
    or schedules force_abort() as a background task. Returns updated strategy refs.
    """
    from engine.strategies.delta_check import DeltaCheckStrategy
    from engine.strategies.smoke_test import SmokeTestStrategy

    action = cmd["action"]

    if action == "smoke_test":
        if smoke_strategy is None or smoke_strategy._done:
            smoke_strategy = SmokeTestStrategy(
                exchange=exchange,
                order_manager=order_mgr,
                position_tracker=tracker,
                risk_guard=risk_guard,
                config=strategy_config.get("smoke_test", {}),
            )
            logger.info("smoke_test_dispatched")
        else:
            logger.warning("smoke_test_already_running")

    elif action == "smoke_test_abort":
        if smoke_strategy is not None and not smoke_strategy._done:
            asyncio.create_task(smoke_strategy.force_abort())
            logger.warning("smoke_test_abort_dispatched")
        else:
            logger.info("smoke_test_abort_noop", reason="no active smoke test")

    elif action == "delta_check":
        if delta_check_strategy is None or delta_check_strategy._done:
            delta_check_strategy = DeltaCheckStrategy(
                exchange=exchange,
                order_manager=order_mgr,
                position_tracker=tracker,
                risk_guard=risk_guard,
                config=strategy_config.get("delta_check", {}),
            )
            logger.info("delta_check_dispatched")
        else:
            logger.warning("delta_check_already_running")

    elif action == "delta_check_abort":
        if delta_check_strategy is not None and not delta_check_strategy._done:
            asyncio.create_task(delta_check_strategy.force_abort())
            logger.warning("delta_check_abort_dispatched")
        else:
            logger.info("delta_check_abort_noop", reason="no active delta check")

    return smoke_strategy, delta_check_strategy


async def run(config: dict) -> None:
    # -- Database --
    from engine.db.models import init_db

    db_url = config["database"]["url"]
    # Ensure data/ directory exists
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    await init_db(db_url)
    logger.info("database_initialised", url=db_url)

    # -- Credentials check --
    api_key = os.environ.get("BITMEX_API_KEY")
    api_secret = os.environ.get("BITMEX_API_SECRET")
    if not api_key or not api_secret:
        sys.exit(
            "ERROR: BITMEX_API_KEY / BITMEX_API_SECRET not set.\n"
            "  Copy config/.env.example → config/.env and fill in your keys.\n"
            "  Testnet keys: https://testnet.bitmex.com/app/apiKeys"
        )

    testnet = config["exchange"].get("testnet", True)
    if not testnet:
        logger.warning("LIVE_TRADING_MODE — not testnet. Real capital at risk.")

    # -- Exchange --
    from engine.exchange.bitmex import BitMEXExchange

    exchange = BitMEXExchange(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
    )

    # -- Rate limit bucket (seeded from exchange headers on startup) --
    from engine.order_manager import OrderManager, RateLimitBucket

    initial_tokens = await exchange.get_rate_limit_remaining()
    bucket = RateLimitBucket(initial_tokens=initial_tokens)
    bucket.start()
    logger.info("rate_limit_bucket_started", initial_tokens=initial_tokens)

    # -- Risk guard + dead-man's switch --
    from engine.risk_guard import RiskGuard

    risk_cfg = config["risk"]

    # -- Order manager --
    order_mgr = OrderManager(
        exchange=exchange,
        bucket=bucket,
        max_slippage=risk_cfg.get("max_slippage_pct", 0.001),
    )
    risk_guard = RiskGuard(
        exchange=exchange,
        max_delta_pct_nav=risk_cfg["max_delta_pct_nav"],
        max_margin_utilization=risk_cfg["max_margin_utilization"],
        liquidation_buffer_pct=risk_cfg["liquidation_buffer_pct"],
        dms_interval_s=risk_cfg["dead_mans_switch_interval_s"],
        dms_timeout_s=risk_cfg["dead_mans_switch_timeout_s"],
        dms_reconnect_timeout_s=risk_cfg["dead_mans_switch_reconnect_timeout_s"],
    )
    risk_guard.start_dead_mans_switch()
    logger.info("dead_mans_switch_started")

    # -- Strategies (instantiate before position tracker so callbacks can be registered) --
    from engine.strategies.base import Strategy
    from engine.strategies.cash_and_carry import CashAndCarryStrategy
    from engine.strategies.funding_harvest import FundingHarvestStrategy

    strategies: list[Strategy] = []

    if config["strategy"]["cash_and_carry"].get("enabled", True):
        s1 = CashAndCarryStrategy(
            exchange=exchange,
            order_manager=order_mgr,
            position_tracker=None,  # set below
            risk_guard=risk_guard,
            config=config["strategy"]["cash_and_carry"],
        )
        strategies.append(s1)

    if config["strategy"]["funding_harvest"].get("enabled", True):
        s2 = FundingHarvestStrategy(
            exchange=exchange,
            order_manager=order_mgr,
            position_tracker=None,  # set below
            risk_guard=risk_guard,
            config=config["strategy"]["funding_harvest"],
        )
        strategies.append(s2)

    # -- Position tracker --
    from engine.position_tracker import PositionTracker

    async def on_funding_payment(event: dict) -> None:
        for s in strategies:
            await s.on_funding_payment(event)

    tracker = PositionTracker(
        exchange=exchange,
        risk_guard=risk_guard,
        ws_url=exchange.ws_url,
        api_key=os.environ["BITMEX_API_KEY"],
        api_secret=os.environ["BITMEX_API_SECRET"],
        on_funding_payment=on_funding_payment,
    )

    # Wire tracker into strategies
    for s in strategies:
        s._tracker = tracker

    # Start tracker (reconcile + WS connection)
    await tracker.start()
    await tracker.wait_ready()
    logger.info("position_tracker_ready")

    # -- Control command queue + HTTP server --
    control_queue: asyncio.Queue = asyncio.Queue(maxsize=16)
    from engine.control.server import ControlServer

    ctrl_cfg = config.get("control", {})
    control_server = ControlServer(
        queue=control_queue,
        host=ctrl_cfg.get("host", "127.0.0.1"),
        port=int(ctrl_cfg.get("port", 8552)),
    )
    await control_server.start()

    # -- Main loop --
    from engine.db import repository

    logger.info("engine_started", strategies=[s.name for s in strategies])
    last_snapshot = asyncio.get_event_loop().time()
    smoke_strategy = None
    delta_check_strategy = None

    dispatch_kwargs = dict(
        exchange=exchange,
        order_mgr=order_mgr,
        tracker=tracker,
        risk_guard=risk_guard,
        strategy_config=config.get("strategy", {}),
    )

    try:
        while True:
            # -- Drain any commands that arrived while we were running strategies --
            while not control_queue.empty():
                cmd = control_queue.get_nowait()
                smoke_strategy, delta_check_strategy = _dispatch_command(
                    cmd, smoke_strategy, delta_check_strategy, **dispatch_kwargs
                )

            # -- One-shot strategies --
            if smoke_strategy is not None and not smoke_strategy._done:
                try:
                    await smoke_strategy.run_once()
                except Exception as e:
                    logger.error("smoke_test_run_once_error", error=str(e))

            if delta_check_strategy is not None and not delta_check_strategy._done:
                try:
                    await delta_check_strategy.run_once()
                except Exception as e:
                    logger.error("delta_check_run_once_error", error=str(e))

            # -- Regular strategies --
            for strategy in strategies:
                try:
                    await strategy.run_once()
                except Exception as e:
                    logger.error(
                        "strategy_run_once_error",
                        strategy=strategy.name,
                        error=str(e),
                    )

            # -- Periodic risk snapshot --
            now = asyncio.get_event_loop().time()
            if now - last_snapshot > RISK_SNAPSHOT_INTERVAL_S:
                try:
                    btc_price = (await exchange.get_ticker("BTC/USD:BTC")).mark_price
                    nav = tracker.get_nav_usd(btc_price)
                    delta = tracker.get_net_delta_usd()
                    open_count = len(await repository.get_open_positions())
                    await risk_guard.save_snapshot(delta, nav, open_count)
                    last_snapshot = now
                except Exception as e:
                    logger.error("risk_snapshot_failed", error=str(e))

            # -- Wait for next tick OR wake immediately on a new control command --
            try:
                cmd = await asyncio.wait_for(control_queue.get(), timeout=LOOP_INTERVAL_S)
                smoke_strategy, delta_check_strategy = _dispatch_command(
                    cmd, smoke_strategy, delta_check_strategy, **dispatch_kwargs
                )
            except TimeoutError:
                pass  # normal 30s tick

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("engine_shutting_down")
    finally:
        tracker.stop()
        risk_guard.stop_dead_mans_switch()
        bucket.stop()
        await control_server.stop()
        await exchange.close()
        logger.info("engine_stopped")


def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )
    config = load_config()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass  # second Ctrl+C during asyncio cleanup — already shut down cleanly


if __name__ == "__main__":
    main()
