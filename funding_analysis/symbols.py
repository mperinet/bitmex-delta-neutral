"""
Canonical asset ↔ per-venue symbol mapping and universe discovery.

Anchors on HyperLiquid perp names (the narrowest set). A canonical asset
row is only materialized when the asset exists on **at least two** of the
three venues (otherwise no cross-venue comparison is possible).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from funding_analysis.db import repository
from funding_analysis.exchanges.binance import BinanceClient
from funding_analysis.exchanges.bitmex import BitmexFundingClient
from funding_analysis.exchanges.hyperliquid import HyperliquidFundingClient

logger = structlog.get_logger(__name__)

# BitMEX uses "XBT" for Bitcoin in contract names; canonical is "BTC".
_BITMEX_BASE_ALIAS: dict[str, str] = {
    "XBT": "BTC",
}


@dataclass
class VenueSymbols:
    """All per-venue symbols seen for a canonical asset during one discovery pass."""

    asset: str
    bitmex_symbols: list[tuple[str, str]]  # (symbol, contract_type)
    hyperliquid_name: str | None
    binance_spot_symbol: str | None


def canonical_from_bitmex_base(base: str) -> str:
    """Normalize a BitMEX root/base (e.g. 'XBT') to the canonical asset ('BTC')."""
    return _BITMEX_BASE_ALIAS.get(base.upper(), base.upper())


def classify_bitmex_contract(instrument: dict) -> str:
    """
    Return one of: "inverse_perp", "linear_perp", "quanto_future", "other".

    Uses BitMEX instrument fields: `typ`, `isInverse`, `isQuanto`.
    """
    typ = (instrument.get("typ") or "").upper()
    is_inverse = bool(instrument.get("isInverse"))
    is_quanto = bool(instrument.get("isQuanto"))
    # FFWCSX = perp (futures forward contract, symbol-XBT-settled)
    # FFCCSX = linear perp
    # FFCCSF = quarterly future
    if typ.endswith("SX"):
        # perpetual
        if is_quanto:
            return "quanto_perp"
        return "inverse_perp" if is_inverse else "linear_perp"
    if typ.endswith("SF"):
        if is_quanto:
            return "quanto_future"
        return "inverse_future" if is_inverse else "linear_future"
    return "other"


def prefer_bitmex_symbol(candidates: list[tuple[str, str]]) -> tuple[str | None, str | None]:
    """
    Pick the best BitMEX symbol for an asset from discovered candidates.

    Preference order:
      1. inverse_perp (XBTUSD-style, BTC-margined)
      2. linear_perp (XBTUSDT-style)
      3. anything else
    """
    if not candidates:
        return None, None
    order = {"inverse_perp": 0, "linear_perp": 1, "quanto_perp": 2}
    sorted_c = sorted(candidates, key=lambda c: order.get(c[1], 99))
    sym, typ = sorted_c[0]
    return sym, typ


async def discover_universe(
    bitmex: BitmexFundingClient,
    hyperliquid: HyperliquidFundingClient,
    binance: BinanceClient,
) -> list[VenueSymbols]:
    """
    Discover the full per-venue symbol set for each canonical asset.

    Runs the three API calls concurrently, then builds the intersection.
    """
    hl_universe_raw, bitmex_contracts, binance_spot = await asyncio.gather(
        hyperliquid.list_perp_universe(),
        bitmex.list_active_contracts(),
        binance.list_spot_symbols(),
    )

    # HL: {name: name}
    hl_names: dict[str, str] = {
        u["name"].upper(): u["name"]
        for u in hl_universe_raw
        if isinstance(u, dict) and u.get("name") and not u.get("isDelisted")
    }

    # BitMEX: {canonical_asset: [(symbol, contract_type), ...]}
    bitmex_by_asset: dict[str, list[tuple[str, str]]] = {}
    for inst in bitmex_contracts:
        if not isinstance(inst, dict):
            continue
        if (inst.get("state") or "").lower() != "open":
            continue
        symbol = inst.get("symbol")
        root = inst.get("rootSymbol") or inst.get("underlying")
        if not symbol or not root:
            continue
        canonical = canonical_from_bitmex_base(str(root))
        contract_type = classify_bitmex_contract(inst)
        bitmex_by_asset.setdefault(canonical, []).append((symbol, contract_type))

    # Binance: {base: symbol} — prefer USDT over USDC when both exist.
    binance_by_asset: dict[str, str] = {}
    for m in binance_spot:
        base = (m.get("base") or "").upper()
        quote = (m.get("quote") or "").upper()
        if not base or not m.get("symbol"):
            continue
        existing = binance_by_asset.get(base)
        if existing is None:
            binance_by_asset[base] = m["symbol"]
        elif quote == "USDT" and not existing.endswith("USDT"):
            binance_by_asset[base] = m["symbol"]

    # Union of all candidate assets.
    candidate_assets: set[str] = set()
    candidate_assets.update(hl_names.keys())
    candidate_assets.update(bitmex_by_asset.keys())
    candidate_assets.update(binance_by_asset.keys())

    out: list[VenueSymbols] = []
    for asset in sorted(candidate_assets):
        hl_name = hl_names.get(asset)
        bitmex_candidates = bitmex_by_asset.get(asset, [])
        bx_symbol, bx_type = prefer_bitmex_symbol(bitmex_candidates)
        bn_symbol = binance_by_asset.get(asset)

        present_count = sum(x is not None for x in (hl_name, bx_symbol, bn_symbol))
        if present_count < 2:
            continue
        out.append(
            VenueSymbols(
                asset=asset,
                bitmex_symbols=bitmex_candidates,
                hyperliquid_name=hl_name,
                binance_spot_symbol=bn_symbol,
            )
        )
    return out


async def refresh_universe(
    bitmex: BitmexFundingClient,
    hyperliquid: HyperliquidFundingClient,
    binance: BinanceClient,
) -> dict:
    """
    Run discovery and upsert into the asset_universe table.

    Returns {added, updated, deactivated, total_active}.
    """
    discovered = await discover_universe(bitmex, hyperliquid, binance)
    discovered_assets = {d.asset for d in discovered}
    now = datetime.now(UTC)

    added = 0
    updated = 0
    for d in discovered:
        bx_symbol, bx_type = prefer_bitmex_symbol(d.bitmex_symbols)
        existing = await repository.get_asset(d.asset)
        was_new = await repository.upsert_asset(
            asset=d.asset,
            bitmex_symbol=bx_symbol,
            bitmex_contract_type=bx_type,
            hyperliquid_name=d.hyperliquid_name,
            binance_spot_symbol=d.binance_spot_symbol,
            discovered_at=existing.discovered_at if existing else now,
            active=True,
        )
        if existing is None:
            added += 1
        else:
            updated += 1
        del was_new  # upsert returns rowcount; we track by existing presence

    # Deactivate assets that dropped out of all three venues.
    deactivated = 0
    for row in await repository.get_all_assets():
        if row.asset not in discovered_assets and row.active:
            await repository.set_asset_inactive(row.asset)
            deactivated += 1

    active = await repository.get_active_assets()
    result = {
        "added": added,
        "updated": updated,
        "deactivated": deactivated,
        "total_active": len(active),
    }
    logger.info("universe refreshed", **result)
    return result
