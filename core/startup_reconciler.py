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
      S2 — query USDT + BTC account balance + ticker price → set portfolio equity
      S4 — restore risk_engine._budget.realised_pnl from registry DB
      set equity — apply actual_equity to portfolio fields
      S3 — reconcile local positions vs exchange
      S5 — write STARTUP_RECONCILIATION event to system_events

    Returns a result dict; never raises.
    Balance is always fetched (even in DRY_RUN) so equity reflects the real account.
    Only order submission is skipped in DRY_RUN (controlled by order_manager.py).
    """
    result: dict = {
        "open_orders":          [],
        "btc_balance":          0.0,
        "usdt_free":            0.0,
        "btc_free":             0.0,
        "actual_equity":        0.0,
        "reconciled_positions": 0,
        "restored_pnl":         0.0,
        "errors":               [],
    }

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

    # S2 — USDT + BTC balances + ticker price
    try:
        account   = await asyncio.wait_for(client.get_account(), timeout=30.0)
        usdt_free = 0.0
        btc_free  = 0.0
        for bal in account.get("balances", []):
            if bal["asset"] == "USDT":
                usdt_free = float(bal["free"])
            if bal["asset"] == "BTC":
                btc_free = float(bal["free"]) + float(bal["locked"])

        try:
            ticker    = await asyncio.wait_for(
                client.get_symbol_ticker(symbol=symbol), timeout=10.0
            )
            btc_price = float(ticker["price"])
        except Exception as exc:
            result["errors"].append(f"get_symbol_ticker: {exc}")
            logger.warning("[Reconcile] Could not fetch ticker: %s", exc)
            btc_price = 0.0

        result["btc_balance"]   = btc_free        # keep existing key for backwards compat
        result["usdt_free"]     = usdt_free
        result["btc_free"]      = btc_free
        result["actual_equity"] = usdt_free + btc_free * btc_price
        logger.info("[Reconcile] BTC balance: %.8f", btc_free)
    except Exception as exc:
        result["errors"].append(f"get_account: {exc}")
        logger.warning("[Reconcile] Could not fetch account: %s", exc)

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

    # Set equity from real account.
    # result["actual_equity"] defaults to 0.0 if S2 failed, so this never raises.
    # result["restored_pnl"] defaults to 0.0 if S4 failed.
    actual_equity = result["actual_equity"]
    if actual_equity > 0:
        portfolio.equity          = actual_equity
        portfolio.starting_equity = actual_equity - result["restored_pnl"]
        portfolio.peak_equity     = max(actual_equity, portfolio.starting_equity)
        logger.info(
            "[Reconcile] Equity set from exchange: USDT=%.2f BTC=%.6f total=%.2f",
            result["usdt_free"], result["btc_free"], actual_equity,
        )
    else:
        logger.warning("[Reconcile] Could not determine actual equity — keeping initialised value")

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
