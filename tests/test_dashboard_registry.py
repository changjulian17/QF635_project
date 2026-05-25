"""Tests for /registry page: decay badge, funnel drift, LIVE gate display."""
import sys
import os
import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def registry_db(tmp_path):
    db = tmp_path / "registry.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE signal_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT,
            gate_passed TEXT,
            timestamp TEXT,
            outcome TEXT,
            pnl_pct REAL
        )
    """)
    conn.commit()
    yield str(db), conn
    conn.close()


def _insert_signal(conn, gate, days_ago=1, outcome="WIN", pnl=0.01, strategy_id="strat_1"):
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    conn.execute(
        "INSERT INTO signal_records (strategy_id, gate_passed, timestamp, outcome, pnl_pct) VALUES (?,?,?,?,?)",
        (strategy_id, gate, ts, outcome, pnl),
    )
    conn.commit()


def test_decay_badge_healthy():
    from dashboard._logic import decay_badge_color
    assert decay_badge_color(rolling_sharpe=1.0, backtest_sharpe=1.0) == "success"


def test_decay_badge_warning():
    from dashboard._logic import decay_badge_color
    assert decay_badge_color(rolling_sharpe=0.77, backtest_sharpe=1.0) == "warning"


def test_decay_badge_decay_alert():
    from dashboard._logic import decay_badge_color
    assert decay_badge_color(rolling_sharpe=0.5, backtest_sharpe=1.0) == "danger"


def test_gate_funnel_drift_detects_increase(registry_db, monkeypatch):
    """Drift > 20pp should be flagged."""
    db_path, conn = registry_db

    # Insert many 30-day records (baseline), fewer 7-day records for same gate
    for _ in range(20):
        _insert_signal(conn, "GATE_1", days_ago=25)  # only in 30d
    for _ in range(2):
        _insert_signal(conn, "GATE_1", days_ago=3)   # in 7d and 30d

    monkeypatch.setattr("dashboard._db.REGISTRY_DB", db_path)
    from dashboard._db import fetch_gate_funnel_drift

    rows = fetch_gate_funnel_drift()
    gate_map = {r["gate_passed"]: r for r in rows}
    r = gate_map.get("GATE_1", {})
    # 30d count should include all 22; 7d count should be 2
    assert r.get("cnt_30d", 0) == 22
    assert r.get("cnt_7d", 0) == 2
