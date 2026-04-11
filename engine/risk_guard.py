"""
Risk guard — enforces delta-neutral constraints at all times.

Hard rules:
  1. Net delta < max_delta_pct_nav of NAV → trigger rebalance
  2. Margin utilization < 50% → hard stop on new positions
  3. Dead-man's switch: cancelAllAfter every 15s (120s during WS reconnect)
  4. Funding circuit breaker: exit if cumulative funding paid > 50% of locked basis
  5. Liquidation buffer: reduce position if price within 10% of liquidation

Decision tree (check_delta):

  compute net delta (USD)
        │
        ▼
  |delta_pct_nav| > threshold?
        │
        ├── YES → return RiskAction.REBALANCE, delta_error
        └── NO  → return RiskAction.OK

Decision tree (check_margin):

  margin_utilization = margin_used / margin_balance
        │
        ├── > 0.50 → return RiskAction.HARD_STOP
        ├── > 0.40 → return RiskAction.WARNING
        └── else   → return RiskAction.OK
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import structlog

from engine.db import repository
from engine.exchange.base import ExchangeBase

logger = structlog.get_logger(__name__)


class RiskAction(str, Enum):
    OK = "ok"
    WARNING = "warning"
    REBALANCE = "rebalance"
    HARD_STOP = "hard_stop"
    EXIT_STRATEGY = "exit_strategy"


@dataclass
class RiskResult:
    action: RiskAction
    reason: str
    delta_pct_nav: float = 0.0
    margin_utilization: float = 0.0


class RiskGuard:
    def __init__(
        self,
        exchange: ExchangeBase,
        max_delta_pct_nav: float = 0.005,
        max_margin_utilization: float = 0.50,
        margin_warning_level: float = 0.40,
        liquidation_buffer_pct: float = 0.10,
        dms_interval_s: float = 15,
        dms_timeout_s: int = 60,
        dms_reconnect_timeout_s: int = 120,
    ):
        self._exchange = exchange
        self._max_delta = max_delta_pct_nav
        self._max_margin = max_margin_utilization
        self._margin_warning = margin_warning_level
        self._liq_buffer = liquidation_buffer_pct
        self._dms_interval = dms_interval_s
        self._dms_timeout = dms_timeout_s
        self._dms_reconnect_timeout = dms_reconnect_timeout_s
        self._dms_task: Optional[asyncio.Task] = None
        self._in_reconnect: bool = False

    # ------------------------------------------------------------------
    # Dead-man's switch
    # ------------------------------------------------------------------

    def start_dead_mans_switch(self) -> None:
        self._dms_task = asyncio.create_task(self._dms_loop())
        logger.info("dead_mans_switch_started", interval_s=self._dms_interval)

    def stop_dead_mans_switch(self) -> None:
        if self._dms_task:
            self._dms_task.cancel()

    def set_reconnecting(self, reconnecting: bool) -> None:
        """
        Call with reconnecting=True when WS disconnects.
        Extends cancelAllAfter timeout to 120s to avoid race condition
        where REST reconciliation takes longer than the 60s timer.
        Resume normal 60s timeout when reconnection completes.
        """
        self._in_reconnect = reconnecting
        timeout = self._dms_reconnect_timeout if reconnecting else self._dms_timeout
        logger.info("dead_mans_switch_mode_changed", reconnecting=reconnecting, timeout_s=timeout)

    async def _dms_loop(self) -> None:
        while True:
            timeout = self._dms_reconnect_timeout if self._in_reconnect else self._dms_timeout
            try:
                await self._exchange.cancel_all_after(timeout * 1000)
                logger.debug("dead_mans_switch_refreshed", timeout_s=timeout)
            except Exception as e:
                logger.error(
                    "dead_mans_switch_failed",
                    error=str(e),
                    retrying_in_s=self._dms_interval,
                )
                # Retry immediately on the next cycle — do NOT sleep longer
                # A failed refresh means orders could cancel unexpectedly
            await asyncio.sleep(self._dms_interval)

    # ------------------------------------------------------------------
    # Delta check
    # ------------------------------------------------------------------

    async def check_delta(
        self, nav_usd: float, net_delta_usd: float
    ) -> RiskResult:
        """
        net_delta_usd: positive = net long USD exposure, negative = net short.
        nav_usd: total portfolio value in USD for threshold calculation.
        """
        if nav_usd <= 0:
            return RiskResult(action=RiskAction.WARNING, reason="NAV is zero or negative")

        delta_pct = abs(net_delta_usd) / nav_usd

        if delta_pct > self._max_delta:
            return RiskResult(
                action=RiskAction.REBALANCE,
                reason=f"Net delta {delta_pct:.3%} exceeds limit {self._max_delta:.3%}",
                delta_pct_nav=delta_pct,
            )
        return RiskResult(
            action=RiskAction.OK,
            reason="Delta within bounds",
            delta_pct_nav=delta_pct,
        )

    # ------------------------------------------------------------------
    # Margin check
    # ------------------------------------------------------------------

    async def check_margin(self) -> RiskResult:
        balance = await self._exchange.get_balance()
        if balance.total <= 0:
            return RiskResult(
                action=RiskAction.WARNING,
                reason="Balance is zero",
                margin_utilization=1.0,
            )

        # margin_used = total - available (i.e. what's locked in positions)
        margin_used = balance.total - balance.available
        utilization = margin_used / balance.total

        if utilization >= self._max_margin:
            return RiskResult(
                action=RiskAction.HARD_STOP,
                reason=f"Margin utilization {utilization:.1%} >= {self._max_margin:.1%} hard cap",
                margin_utilization=utilization,
            )
        if utilization >= self._margin_warning:
            return RiskResult(
                action=RiskAction.WARNING,
                reason=f"Margin utilization {utilization:.1%} approaching hard cap",
                margin_utilization=utilization,
            )
        return RiskResult(
            action=RiskAction.OK,
            reason="Margin within bounds",
            margin_utilization=utilization,
        )

    # ------------------------------------------------------------------
    # Funding circuit breaker (Strategy 1: cash-and-carry)
    # ------------------------------------------------------------------

    def check_funding_circuit_breaker(
        self, cumulative_funding_paid: float, locked_basis: float
    ) -> RiskResult:
        """
        Exit when cumulative funding paid on the perp long leg equals
        50% of the locked-in basis. This prevents the carry trade from
        becoming a loss if funding persists higher than expected.

        locked_basis: the annualised basis captured at entry (e.g. 0.15 = 15%)
        cumulative_funding_paid: sum of all funding payments made so far,
                                  expressed as a fraction of notional.
        """
        if locked_basis <= 0:
            return RiskResult(action=RiskAction.OK, reason="No locked basis")

        ratio = cumulative_funding_paid / locked_basis
        if ratio >= 0.50:
            return RiskResult(
                action=RiskAction.EXIT_STRATEGY,
                reason=(
                    f"Funding circuit breaker: paid {cumulative_funding_paid:.4%} "
                    f"= {ratio:.1%} of locked basis {locked_basis:.4%}"
                ),
            )
        return RiskResult(
            action=RiskAction.OK,
            reason=f"Funding paid {ratio:.1%} of locked basis — within threshold",
        )

    # ------------------------------------------------------------------
    # Liquidation buffer
    # ------------------------------------------------------------------

    def check_liquidation_buffer(
        self, current_price: float, liquidation_price: float, side: str
    ) -> RiskResult:
        """
        Reduce position if current price is within liquidation_buffer_pct
        of the liquidation price.

        side: "long" or "short" (determines which direction is dangerous)
        """
        if liquidation_price <= 0:
            return RiskResult(action=RiskAction.OK, reason="No liquidation price")

        if side == "long":
            buffer = (current_price - liquidation_price) / current_price
        else:
            buffer = (liquidation_price - current_price) / current_price

        if buffer < self._liq_buffer:
            return RiskResult(
                action=RiskAction.WARNING,
                reason=(
                    f"Liquidation buffer {buffer:.1%} < {self._liq_buffer:.1%} threshold. "
                    f"Current: {current_price}, Liq: {liquidation_price}"
                ),
            )
        return RiskResult(action=RiskAction.OK, reason="Liquidation buffer healthy")

    # ------------------------------------------------------------------
    # Periodic snapshot
    # ------------------------------------------------------------------

    async def save_snapshot(
        self,
        net_delta_usd: float,
        nav_usd: float,
        open_positions: int,
        notes: str = "",
    ) -> None:
        balance = await self._exchange.get_balance()
        margin_used = balance.total - balance.available
        utilization = margin_used / balance.total if balance.total > 0 else 0.0
        delta_pct = abs(net_delta_usd) / nav_usd if nav_usd > 0 else 0.0

        await repository.save_risk_snapshot(
            net_delta_usd=net_delta_usd,
            net_delta_pct_nav=delta_pct,
            margin_balance=balance.total,
            margin_used=margin_used,
            margin_utilization=utilization,
            nav=nav_usd,
            open_positions=open_positions,
            notes=notes,
        )
