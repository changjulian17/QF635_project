"""Tests for core/startup_reconciler.py"""
import asyncio
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import engine.db_writer as db_writer_module
from core.startup_reconciler import _write_event, reconcile_on_startup
from models import Direction, PortfolioState, Position
from risk.engine import RiskEngine


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


def _make_client(open_orders=None, usdt_balance: str = "10000.0"):
    """Return a mock AsyncClient wired for futures reconciler calls."""
    client = AsyncMock()
    client.futures_get_open_orders = AsyncMock(return_value=open_orders or [])
    client.futures_account_balance = AsyncMock(return_value=[
        {"asset": "USDT", "balance": usdt_balance},
    ])
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

def test_dry_run_fetches_balance_skips_order_submission(tmp_path):
    """Balance is always fetched so equity is real, even in DRY_RUN mode."""
    db_path = str(tmp_path / "registry.db")
    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = True
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(
            reconcile_on_startup(client, portfolio, risk_engine)
        )

    client.futures_get_open_orders.assert_called_once()
    client.futures_account_balance.assert_called_once()
    assert result["open_orders"] == []
    assert "futures_account_balance" not in " ".join(result["errors"])


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
    client.futures_get_open_orders = AsyncMock(side_effect=Exception("network timeout"))
    client.futures_account_balance = AsyncMock(return_value=[])
    portfolio = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert any("get_open_orders" in e for e in result["errors"])


# ── reconcile_on_startup — S2: futures USDT balance ──────────────────────────

def test_s2_usdt_wallet_balance_sets_equity(tmp_path):
    """futures_account_balance() USDT wallet balance sets portfolio equity."""
    db_path = str(tmp_path / "registry.db")
    client = _make_client(usdt_balance="12345.67")
    portfolio = _make_portfolio(equity=10_000.0)
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert abs(result["actual_equity"] - 12345.67) < 1e-6
    assert abs(portfolio.equity - 12345.67) < 1e-6
    assert result["btc_balance"] == 0.0


def test_s2_equity_set_from_futures_usdt_balance(tmp_path):
    """All portfolio equity fields are updated from USDT wallet balance."""
    db_path = str(tmp_path / "registry.db")
    client = _make_client(usdt_balance="18000.0")
    portfolio = _make_portfolio(equity=10_000.0)
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert abs(portfolio.equity - 18_000.0) < 1e-6
    assert abs(portfolio.starting_equity - 18_000.0) < 1e-6
    assert abs(portfolio.peak_equity - 18_000.0) < 1e-6
    assert abs(portfolio.usdt_balance - 18_000.0) < 1e-6
    assert abs(portfolio.btc_balance) < 1e-9


def test_s2_missing_usdt_asset_returns_zero(tmp_path):
    """If USDT is absent from futures_account_balance, equity defaults to 0."""
    db_path = str(tmp_path / "registry.db")
    client = AsyncMock()
    client.futures_get_open_orders = AsyncMock(return_value=[])
    # Return a list with no USDT entry
    client.futures_account_balance = AsyncMock(return_value=[
        {"asset": "BNB", "balance": "0.5"},
    ])
    portfolio = _make_portfolio(equity=10_000.0)
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["btc_balance"] == 0.0
    # actual_equity == 0 → portfolio equity stays at initialised value
    assert abs(portfolio.equity - 10_000.0) < 1e-6


def test_s2_account_failure_keeps_initialised_equity(tmp_path):
    db_path = str(tmp_path / "registry.db")
    client = AsyncMock()
    client.futures_get_open_orders = AsyncMock(return_value=[])
    client.futures_account_balance = AsyncMock(side_effect=Exception("network error"))
    portfolio = _make_portfolio(equity=10_000.0)
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = db_path
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert abs(portfolio.equity - 10_000.0) < 1e-6
    assert any("futures_account_balance" in e for e in result["errors"])


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

    assert result["restored_pnl"] == 0.0
    assert any("restore_pnl" in e for e in result["errors"])


# ── reconcile_on_startup — S5: system_events ─────────────────────────────────

def test_s5_startup_reconciliation_event_written(tmp_path):
    db_path = str(tmp_path / "registry.db")
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
    client = _make_client()
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
    assert "errors" in payload


# ── reconcile_on_startup — S6: restore consecutive-loss cooldown ────────────

def _seed_engine_health(db_path, consecutive_losses, cooldown_until_ms):
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS engine_health (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        lob_status TEXT, hb_status TEXT, risk_tier TEXT, ks_active INTEGER,
        updated_ms INTEGER, consecutive_losses INTEGER DEFAULT 0,
        cooldown_until_ms INTEGER
    )""")
    conn.execute(
        "INSERT INTO engine_health VALUES (1, 'SYNCED', 'OK', 'PASSIVE', 0, 0, ?, ?)",
        (consecutive_losses, cooldown_until_ms),
    )
    conn.commit()
    conn.close()


def test_s6_restores_active_cooldown_blocks_gate3(tmp_path, monkeypatch):
    registry_db = str(tmp_path / "registry.db")
    health_db = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)

    cooldown_until_ms = int((datetime.now(timezone.utc) + timedelta(seconds=60)).timestamp() * 1000)
    _seed_engine_health(health_db, consecutive_losses=3, cooldown_until_ms=cooldown_until_ms)

    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = RiskEngine(portfolio)

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = registry_db
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert portfolio.consecutive_losses == 3
    assert risk_engine.sync_tier() == "PASSIVE"


def test_s6_restores_expired_cooldown_still_paused_until_a_win(tmp_path, monkeypatch):
    registry_db = str(tmp_path / "registry.db")
    health_db = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)

    cooldown_until_ms = int((datetime.now(timezone.utc) - timedelta(seconds=60)).timestamp() * 1000)
    _seed_engine_health(health_db, consecutive_losses=3, cooldown_until_ms=cooldown_until_ms)

    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = RiskEngine(portfolio)

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = registry_db
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    # Expired cooldown does not forgive an active loss streak — re-enters PASSIVE
    # with a freshly-extended cooldown (mirrors risk/engine.py:101-106).
    assert risk_engine.sync_tier() == "PASSIVE"

    risk_engine.record_trade_result(1.0)  # win resets consecutive_losses to 0
    risk_engine._cooldown_until = datetime.now(timezone.utc) - timedelta(seconds=1)  # let cooldown lapse
    assert risk_engine.sync_tier() == "FULL"


def test_s6_missing_engine_health_row_defaults_to_full(tmp_path, monkeypatch):
    registry_db = str(tmp_path / "registry.db")
    health_db = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)
    # Fresh DB — no engine_health row at all.

    client = _make_client()
    portfolio = _make_portfolio()
    risk_engine = RiskEngine(portfolio)

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN = False
        mock_settings.REGISTRY_DB = registry_db
        asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert portfolio.consecutive_losses == 0
    assert risk_engine.cooldown_until is None
    assert risk_engine.sync_tier() == "FULL"


# ── reconcile_on_startup — S7: orphan position detection ─────────────────────

def test_s7_detects_orphan_position(tmp_path, monkeypatch):
    """S7 calls futures_account() and sets has_orphan_position when positionAmt != 0."""
    registry_db = str(tmp_path / "registry.db")
    health_db   = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)

    client = _make_client()
    client.futures_account = AsyncMock(return_value={
        "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.012"}]
    })
    portfolio   = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN      = False
        mock_settings.REGISTRY_DB  = registry_db
        mock_settings.QTY_STEP_SIZE = 0.001
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["has_orphan_position"] is True
    assert abs(result["orphan_position_qty"] - 0.012) < 1e-9


def test_s7_no_orphan_when_flat(tmp_path, monkeypatch):
    """S7 sets has_orphan_position=False when positionAmt is zero."""
    registry_db = str(tmp_path / "registry.db")
    health_db   = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)

    client = _make_client()
    client.futures_account = AsyncMock(return_value={
        "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.000"}]
    })
    portfolio   = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN       = False
        mock_settings.REGISTRY_DB   = registry_db
        mock_settings.QTY_STEP_SIZE = 0.001
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["has_orphan_position"] is False
    assert "orphan_position_qty" not in result


def test_s7_api_failure_does_not_block_startup(tmp_path, monkeypatch):
    """S7 API failure is captured in errors; reconcile completes with has_orphan_position=False."""
    registry_db = str(tmp_path / "registry.db")
    health_db   = str(tmp_path / "cryptosentinel.db")
    monkeypatch.setattr(db_writer_module, "DB_PATH", health_db)

    client = _make_client()
    client.futures_account = AsyncMock(side_effect=RuntimeError("network error"))
    portfolio   = _make_portfolio()
    risk_engine = _make_risk_engine()

    with patch("core.startup_reconciler.settings") as mock_settings:
        mock_settings.DRY_RUN       = False
        mock_settings.REGISTRY_DB   = registry_db
        mock_settings.QTY_STEP_SIZE = 0.001
        result = asyncio.run(reconcile_on_startup(client, portfolio, risk_engine))

    assert result["has_orphan_position"] is False
    assert any("orphan_position_check" in e for e in result["errors"])
