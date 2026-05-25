"""
Startup Reconciler — reconcile local portfolio state with Binance exchange state.

Called once on startup BEFORE any coroutines run (master arch §9, Step 3).
Every step is wrapped in try/except so a single exchange error never blocks startup.
"""
import asyncio
import json
import logging
import sqlite3
from datetime import date, datetime, timezone
from typing import Any

from config import settings
from models import PortfolioState

logger = logging.getLogger(__name__)


async def reconcile_on_startup(
    client: Any,
    portfolio: PortfolioState,
    risk_engine: Any,
    symbol: str = "BTCUSDT",
) -> dict:
    """
    Reconcile local state with Binance and restore today's realised PnL.

    Steps:
      S1 — query open orders
      S2 — query BTC account balance
      S3 — reconcile local positions vs exchange
      S4 — restore risk_engine._budget.realised_pnl from registry DB
      S5 — write STARTUP_RECONCILIATION event to system_events

    Returns a result dict; never raises.
    """
    result: dict = {
        "open_orders": [],
        "btc_balance": 0.0,
        "reconciled_positions": 0,
        "restored_pnl": 0.0,
        "errors": [],
    }

    if settings.DRY_RUN:
        logger.info("[Reconcile] DRY_RUN — skipping exchange queries")
        _write_event("STARTUP_RECONCILIATION", {"dry_run": True, **result})
        return result

    # S1 — open orders on exchange
    try:
        open_orders = await asyncio.wait_for(
            client.get_open_orders(symbol=symbol), timeout=30.0
        )
        result["open_orders"] = open_orders
        logger.info("[Reconcile] %d open order(s) found on exchange", len(open_orders))
    except Exception as exc:
        result["errors"].append(f"get_open_orders: {exc}")
        logger.warning("[Reconcile] Could not fetch open orders: %s", exc)

    # S2 — BTC balance (free + locked)
    try:
        account = await asyncio.wait_for(client.get_account(), timeout=30.0)
        for bal in account.get("balances", []):
            if bal["asset"] == "BTC":
                result["btc_balance"] = float(bal["free"]) + float(bal["locked"])
                break
        logger.info("[Reconcile] BTC balance: %.8f", result["btc_balance"])
    except Exception as exc:
        result["errors"].append(f"get_account: {exc}")
        logger.warning("[Reconcile] Could not fetch account: %s", exc)

    # S3 — reconcile: clear stale local positions if exchange shows none
    if not result["open_orders"] and portfolio.positions:
        count = len(portfolio.positions)
        logger.warning(
            "[Reconcile] Clearing %d stale local position(s) — no open orders on exchange",
            count,
        )
        portfolio.positions.clear()
        result["reconciled_positions"] = count
    elif result["open_orders"]:
        for order in result["open_orders"]:
            logger.info(
                "[Reconcile] Exchange order: %s %s qty=%s price=%s",
                order.get("side"), order.get("symbol"),
                order.get("origQty"), order.get("price"),
            )

    # S4 — restore today's realised_pnl from registry DB
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(settings.REGISTRY_DB)
        rows = conn.execute(
            "SELECT pnl FROM signal_records WHERE date(timestamp) = ? AND outcome != ''",
            (today,),
        ).fetchall()
        conn.close()
        today_pnl = sum(float(r[0]) for r in rows)
        risk_engine._budget.realised_pnl = today_pnl
        portfolio.daily_pnl = today_pnl
        result["restored_pnl"] = today_pnl
        logger.info(
            "[Reconcile] Restored realised_pnl=%.4f from %d trade(s) today",
            today_pnl, len(rows),
        )
    except Exception as exc:
        result["errors"].append(f"restore_pnl: {exc}")
        logger.warning("[Reconcile] Could not restore PnL: %s", exc)

    # S5 — write STARTUP_RECONCILIATION event
    _write_event("STARTUP_RECONCILIATION", result)

    if result["errors"]:
        logger.warning("[Reconcile] Completed with %d error(s): %s", len(result["errors"]), result["errors"])
    else:
        logger.info("[Reconcile] Startup reconciliation complete")

    return result


def _write_event(event_type: str, payload: dict) -> None:
    """Write a lifecycle event directly to system_events via sqlite3."""
    try:
        conn = sqlite3.connect(settings.REGISTRY_DB)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS system_events (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type   TEXT NOT NULL,
                    occurred_at  TEXT NOT NULL,
                    payload_json TEXT
                )"""
            )
            conn.execute(
                "INSERT INTO system_events (event_type, occurred_at, payload_json) VALUES (?, ?, ?)",
                (event_type, datetime.now(timezone.utc).isoformat(), json.dumps(payload, default=str)),
            )
        conn.close()
        logger.info("[Reconcile] %s event written to system_events", event_type)
    except Exception as exc:
        logger.warning("[Reconcile] Could not write %s event: %s", event_type, exc)
