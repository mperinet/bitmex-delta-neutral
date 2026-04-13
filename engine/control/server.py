"""
Engine control server — accepts operator commands over HTTP.

Runs inside the engine's asyncio event loop on 127.0.0.1:8552 (configurable).
Commands are logged to DB immediately (audit trail) then put on the control Queue.
The main loop wakes from asyncio.wait_for(queue.get(), ...) instantly.

Never expose this port externally — it runs on loopback only.

Routes:
  POST /control  {"action": "<action>"}  →  queues command, returns 200
  GET  /status                           →  returns {"ok": true}

Valid actions: smoke_test, smoke_test_abort, smoke_test_eth, smoke_test_eth_abort,
               delta_check, delta_check_abort
"""

from __future__ import annotations

import asyncio

import structlog
from aiohttp import web

from engine.db import repository

logger = structlog.get_logger(__name__)

VALID_ACTIONS = frozenset(
    {
        "smoke_test",
        "smoke_test_abort",
        "smoke_test_eth",
        "smoke_test_eth_abort",
        "delta_check",
        "delta_check_abort",
    }
)


class ControlServer:
    def __init__(self, queue: asyncio.Queue, host: str = "127.0.0.1", port: int = 8552):
        self._queue = queue
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None

        app = web.Application()
        app.router.add_post("/control", self._handle_control)
        app.router.add_get("/status", self._handle_status)
        self._app = app

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        logger.info("control_server_started", host=self._host, port=self._port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            logger.info("control_server_stopped")

    async def _handle_control(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        action = body.get("action")
        if action not in VALID_ACTIONS:
            return web.json_response(
                {"error": f"unknown action; valid: {sorted(VALID_ACTIONS)}"},
                status=400,
            )

        # Log to DB for audit trail
        sig = await repository.create_control_signal(action)

        # Put on queue — wakes the main loop immediately
        try:
            self._queue.put_nowait({"action": action})
        except asyncio.QueueFull:
            logger.warning("control_queue_full", action=action)
            return web.json_response({"error": "command queue full, try again"}, status=429)

        # Mark consumed immediately — it was accepted into the queue
        await repository.consume_control_signal(sig.id)

        logger.info("control_command_accepted", action=action, signal_id=sig.id)
        return web.json_response({"status": "queued", "action": action, "signal_id": sig.id})

    async def _handle_status(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "queue_size": self._queue.qsize()})
