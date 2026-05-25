"""Tests for core/startup_reconciler.py"""
import asyncio
import json
import os
import sqlite3
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.startup_reconciler import _write_event, reconcile_on_startup
from models import Direction, PortfolioState, Position


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_portfolio(equity: float = 10_000.0) -> PortfolioState:
    return PortfolioState(
        equity=equity,
        starting_equity=equity,
        peak_equity=equity,
    )


def _make_risk_engine():
    budget = MagicMock()
    budget.realised_pnl = 0.0
    engine = MagicMock()
    engine._budget = budget
    return engine


def _make_client(open_orders=None, account_balances=None):
    client = AsyncMock()
    client.get_open_orders = AsyncMock(return_value=open_orders or [])
    client.get_account = AsyncMock(return_value={
        "balances": account_balances or [
            {"asset": "BTC", "free": "0.1", "locked": "0.0"},
            {"asset": "USDT", "free": "9900.0", "locked": "0.0"},
        ]
    })
    return client


# ── _write_event ──────────────────────────────────────────────────────────────

def test_write_event_creates_table_and_row(tmp_path):
    db_path = str(tmp_path / "registry.db")
    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.REGISTRY_DB = db_path
        mock_settings.DRY_RUN = False
        _write_event("TEST_EVENT", {"key": "value"})

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT event_type, payload_json FROM system_events").fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "TEST_EVENT"
    payload = json.loads(rows[0][1])
    assert payload["key"] == "value"


def test_write_event_idempotent_table_creation(tmp_path):
    db_path = str(tmp_path / "registry.db")
    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.REGISTRY_DB = db_path
        mock_settings.DRY_RUN = False
        _write_event("E1", {})
        _write_event("E2", {})

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM system_events").fetchone()[0]
    conn.close()
    assert count == 2


# ── reconcile_on_startup — DRY_RUN ───────────────────────────────────────────

def test_dry_run_skips_exchange_queries(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = AsyncMock()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = True
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(
            reconcile_on_startup(client, portfolio, risk_engine)
        )

    client.get_open_orders.assert_not_called()
    client.get_account.assert_not_called()
    assert result["open_orders"] == []
    assert result["errors"] == []


# ── reconcile_on_startup — S1: open orders ───────────────────────────────────

def test_s1_open_orders_logged(tmp_path):
    db_path = str(tmp_path / "registry.db")
    orders = [{"orderId": 123, "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.001", "price": "50000"}]
    client = _make_client(open_orders=orders)
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["open_orders"] == orders


def test_s1_exchange_error_captured(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = AsyncMock()
    client.get_open_orders = AsyncMock(side_effect=Exception("network timeout"))
    client.get_account = AsyncMock(return_value={"balances": []})
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert any("get_open_orders" in e for e in result["errors"])


# ── reconcile_on_startup — S2: BTC balance ───────────────────────────────────

def test_s2_btc_balance_extracted(tmp_path):
    db_path = str(tmp_path / "registry.db")
    balances = [
        {"asset": "BTC", "free": "0.05", "locked": "0.025"},
        {"asset": "USDT", "free": "9000.0", "locked": "0.0"},
    ]
    client = _make_client(account_balances=balances)
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert abs(result["btc_balance"] - 0.075) < 1e-9


def test_s2_missing_btc_asset_returns_zero(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = _make_client(account_balances=[{"asset": "USDT", "free": "1000.0", "locked": "0.0"}])
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["btc_balance"] == 0.0


# ── reconcile_on_startup — S3: position reconciliation ───────────────────────

def test_s3_clears_stale_positions_when_no_exchange_orders(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = _make_client(open_orders=[])
    portfolio = _make_portfolio()
    portfolio.positions = [
        Position(symbol="BTCUSDT", side=Direction.LONG, entry_price=50000.0,
                 quantity=0.001, stop_loss=49000.0, take_profit=52000.0)
    ]
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert portfolio.positions == []
    assert result["reconciled_positions"] == 1


def test_s3_keeps_positions_when_exchange_has_orders(tmp_path):
    db_path = str(tmp_path / "registry.db")
    orders = [{"orderId": 1, "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.001", "price": "50000"}]
    client = _make_client(open_orders=orders)
    portfolio = _make_portfolio()
    portfolio.positions = [
        Position(symbol="BTCUSDT", side=Direction.LONG, entry_price=50000.0,
                 quantity=0.001, stop_loss=49000.0, take_profit=52000.0)
    ]
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert len(portfolio.positions) == 1


def test_s3_no_positions_and_no_orders_is_noop(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = _make_client(open_orders=[])
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert portfolio.positions == []
    assert result["reconciled_positions"] == 0


# ── reconcile_on_startup — S4: restore realised_pnl ─────────────────────────

def test_s4_restores_pnl_from_today_trades(tmp_path):
    db_path = str(tmp_path / "registry.db")

    # Pre-populate signal_records with today's closed trades
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_records (
        signal_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        micro_signal TEXT, gate_passed TEXT, rejection_reason TEXT,
        lob_status TEXT, heartbeat_status TEXT,
        obi_zscore REAL, cvd_delta REAL, spread_bps REAL,
        confidence REAL, direction TEXT,
        outcome TEXT DEFAULT '', pnl REAL DEFAULT 0.0,
        pnl_pct REAL DEFAULT 0.0, duration_min REAL DEFAULT 0.0
    )""")
    from datetime import date
    today = date.today().isoformat()
    conn.execute("INSERT INTO signal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("sig-1", "v3.0", f"{today}T10:00:00", "", "APPROVED", "",
                  "SYNCED", "HEALTHY", 0.0, 0.0, 0.0, 0.8, "LONG", "WIN", 12.5, 0.01, 15.0))
    conn.execute("INSERT INTO signal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("sig-2", "v3.0", f"{today}T11:00:00", "", "APPROVED", "",
                  "SYNCED", "HEALTHY", 0.0, 0.0, 0.0, 0.7, "SHORT", "LOSS", -5.0, -0.005, 8.0))
    conn.commit()
    conn.close()

    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert abs(result["restored_pnl"] - 7.5) < 1e-9
    assert abs(risk_engine._budget.realised_pnl - 7.5) < 1e-9


def test_s4_missing_signal_records_table_does_not_crash(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    # signal_records doesn't exist yet → error captured, pnl stays 0
    assert result["restored_pnl"] == 0.0
    assert any("restore_pnl" in e for e in result["errors"])


# ── reconcile_on_startup — S5: system_events ─────────────────────────────────

def test_s5_startup_reconciliation_event_written(tmp_path):
    db_path = str(tmp_path / "registry.db")
    # Pre-create signal_records so S4 succeeds (no error)
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_records (
        signal_id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL,
        timestamp TEXT NOT NULL, micro_signal TEXT, gate_passed TEXT,
        rejection_reason TEXT, lob_status TEXT, heartbeat_status TEXT,
        obi_zscore REAL, cvd_delta REAL, spread_bps REAL,
        confidence REAL, direction TEXT,
        outcome TEXT DEFAULT '', pnl REAL DEFAULT 0.0,
        pnl_pct REAL DEFAULT 0.0, duration_min REAL DEFAULT 0.0
    )""")
    conn.commit()
    conn.close()

    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT event_type FROM system_events WHERE event_type = 'STARTUP_RECONCILIATION'"
    ).fetchall()
    conn.close()

    assert len(rows) == 1


def test_s5_dry_run_event_written(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = AsyncMock()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = True
        mock_settings.REGISTRY_DB = db_path
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT payload_json FROM system_events WHERE event_type = 'STARTUP_RECONCILIATION'"
    ).fetchall()
    conn.close()

    assert len(rows) == 1
    payload = json.loads(rows[0][0])
    assert payload.get("dry_run") is True
