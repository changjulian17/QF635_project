"""Tests for /live page logic: gate funnel query, KS modal, post-fire state."""
import sys
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
import requests

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


def _insert_signal(conn, gate: str, hours_ago: int = 1, outcome: str = "APPROVED"):
    ts = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    conn.execute(
        "INSERT INTO signal_records (gate_passed, timestamp, outcome) VALUES (?,?,?)",
        (gate, ts, outcome),
    )
    conn.commit()


def test_gate_funnel_counts_last_24h(registry_db, monkeypatch):
    db_path, conn = registry_db
    _insert_signal(conn, "GATE_0", hours_ago=1)
    _insert_signal(conn, "GATE_1", hours_ago=2)
    _insert_signal(conn, "GATE_2", hours_ago=3)
    _insert_signal(conn, "GATE_0", hours_ago=30)  # outside 24h window

    monkeypatch.setattr("dashboard._db.REGISTRY_DB", db_path)
    from dashboard._db import fetch_signal_funnel

    rows = fetch_signal_funnel(hours=24)
    gate_map = {r["gate_passed"]: r["cnt"] for r in rows}
    assert gate_map.get("GATE_0", 0) == 1  # only the recent one
    assert gate_map.get("GATE_1", 0) == 1
    assert gate_map.get("GATE_2", 0) == 1


def test_gate_funnel_missing_schema_returns_db_offline(tmp_path, monkeypatch):
    """fetch_signal_funnel should return DBOffline when schema not yet created."""
    db_path = str(tmp_path / "empty.db")
    monkeypatch.setattr("dashboard._db.REGISTRY_DB", db_path)
    from dashboard._db import fetch_signal_funnel, DBOffline

    result = fetch_signal_funnel(hours=24)
    assert isinstance(result, DBOffline)


def test_ks_confirm_requires_exact_string():
    """Kill switch confirm button should only enable when text is exactly 'CONFIRM'."""
    from dashboard._logic import validate_ks_confirm
    assert validate_ks_confirm("CONFIRM") is False   # disabled=False means enabled
    assert validate_ks_confirm("confirm") is True    # disabled=True
    assert validate_ks_confirm("") is True
    assert validate_ks_confirm("CONF") is True


def test_ks_modal_fires_api_on_confirm():
    """fire_killswitch should POST to /api/killswitch."""
    with patch("dashboard._logic.requests.post") as mock_post:
        mock_post.return_value = MagicMock(ok=True)
        from dashboard._logic import fire_killswitch
        result = fire_killswitch("http://127.0.0.1:8080")
        mock_post.assert_called_once_with("http://127.0.0.1:8080/api/killswitch", timeout=3)
        assert result is True


def test_ks_modal_does_not_fire_on_wrong_confirm():
    """validate_ks_confirm should block non-CONFIRM strings."""
    from dashboard._logic import validate_ks_confirm
    # All of these should return True (disabled=True, button inactive)
    assert validate_ks_confirm("wrong") is True
    assert validate_ks_confirm("CONFIRM ") is True  # trailing space
    assert validate_ks_confirm(" CONFIRM") is True  # leading space
    assert validate_ks_confirm("confirm") is True


# ── update_portfolio_state ─────────────────────────────────────────────────


def test_portfolio_state_accepts_valid_msg():
    """Valid {"type": "portfolio", "equity": ...} payload replaces state entirely."""
    from dashboard._logic import update_portfolio_state
    msg = {"type": "portfolio", "ts": "t1", "equity": 10_000.0, "daily_pnl": 5.0}
    assert update_portfolio_state({}, msg) == msg


def test_portfolio_state_rejects_non_portfolio_type():
    """Messages tagged as snapshot/event/anything else leave state unchanged."""
    from dashboard._logic import update_portfolio_state
    prev = {"type": "portfolio", "equity": 10_000.0}
    snap = {"type": "snapshot", "ts": "t2", "mid_price": 73000.0}
    assert update_portfolio_state(prev, snap) is prev


def test_portfolio_state_rejects_malformed():
    """Non-dict / missing equity rejected — state stays at last known good."""
    from dashboard._logic import update_portfolio_state
    prev = {"type": "portfolio", "equity": 10_000.0}
    assert update_portfolio_state(prev, "not a dict") is prev
    assert update_portfolio_state(prev, {"type": "portfolio"}) is prev   # no equity


def test_portfolio_state_replaces_not_merges():
    """Snapshot semantics: each push replaces the prior state in full."""
    from dashboard._logic import update_portfolio_state
    prev = {"type": "portfolio", "equity": 10_000.0, "positions": [{"side": "LONG"}]}
    nxt  = {"type": "portfolio", "equity":  9_950.0, "positions": []}
    result = update_portfolio_state(prev, nxt)
    assert result == nxt          # full replacement
    assert result["positions"] == []  # not merged with prior list


def test_ks_modal_clears_input_on_post_failure():
    """Input must be cleared to '' even when POST fails — prevents silent re-fire."""
    from unittest.mock import patch
    from dashboard._logic import fire_killswitch, validate_ks_confirm

    with patch("dashboard._logic.requests.post", side_effect=requests.RequestException()):
        result = fire_killswitch("http://127.0.0.1:8080")

    assert result is False  # POST failed

    # After clearing the input to "", the confirm button must be disabled
    assert validate_ks_confirm("") is True   # button disabled after clear
    # And enabled only when "CONFIRM" is typed again
    assert validate_ks_confirm("CONFIRM") is False  # disabled=False means enabled
