"""
Startup Reconciler — synchronises local state with exchange on restart.

Called BEFORE starting any coroutines so the system has a consistent
view of open orders, balances, and yesterday's realised PnL.
"""

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


async def reconcile_on_startup(
    client,
    portfolio,
    risk_engine,
    db_path: str,
    symbol: str,
) -> dict:
    """
    Steps:
      1. Query open orders from exchange
      2. Query account balance
      3. Reconcile local portfolio vs exchange
      4. Restore today's realised PnL from registry.db
      5. Write STARTUP_RECONCILIATION to system_events

    Returns a summary dict for logging.
    """
    summary: dict = {
        "open_orders":    0,
        "btc_balance":    0.0,
        "pnl_restored":   0.0,
        "positions_synced": False,
    }

    if client is None:
        logger.warning("[Reconciler] No client — skipping exchange queries.")
        _write_event(db_path, "STARTUP_RECONCILIATION", {"skipped": True})
        return summary

    try:
        # Step 1: open orders
        open_orders = await client.get_open_orders(symbol=symbol)
        summary["open_orders"] = len(open_orders)
        if open_orders:
            logger.info("[Reconciler] %d open order(s) found on exchange.", len(open_orders))

        # Step 2: account balance
        account = await client.get_account()
        base_asset = symbol.replace("USDT", "")
        for bal in account.get("balances", []):
            if bal["asset"] == base_asset:
                summary["btc_balance"] = float(bal["free"]) + float(bal["locked"])
                break

        # Step 3: reconcile positions
        if portfolio is not None:
            if not open_orders and not portfolio.positions:
                summary["positions_synced"] = True
            elif open_orders:
                logger.warning(
                    "[Reconciler] %d open orders detected — positions may need manual review.",
                    len(open_orders),
                )

        # Step 4: restore today's realised PnL from registry
        today_pnl = _query_today_pnl(db_path)
        summary["pnl_restored"] = today_pnl
        if risk_engine is not None and today_pnl != 0.0:
            risk_engine.portfolio.daily_pnl = today_pnl
            logger.info("[Reconciler] Restored daily_pnl=%.2f from registry.", today_pnl)

    except Exception as exc:
        logger.error("[Reconciler] Error during reconciliation: %s", exc, exc_info=True)

    # Step 5: write event
    _write_event(db_path, "STARTUP_RECONCILIATION", summary)
    logger.info("[Reconciler] Reconciliation complete: %s", summary)
    return summary


def _query_today_pnl(db_path: str) -> float:
    """Sum today's closed trade PnL from signal_records."""
    try:
        conn = sqlite3.connect(db_path)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0.0) FROM signal_records "
            "WHERE gate_passed='APPROVED' AND timestamp LIKE ? AND outcome != ''",
            (f"{today}%",),
        ).fetchone()
        conn.close()
        return float(row[0]) if row else 0.0
    except Exception:
        return 0.0


def _write_event(db_path: str, event_type: str, payload: dict) -> None:
    import json
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS system_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "event_type TEXT NOT NULL, "
            "occurred_at TEXT NOT NULL, "
            "payload_json TEXT"
            ")"
        )
        conn.execute(
            "INSERT INTO system_events (event_type, occurred_at, payload_json) VALUES (?,?,?)",
            (event_type, datetime.now(timezone.utc).isoformat(), json.dumps(payload)),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("[Reconciler] Could not write system event: %s", exc)
