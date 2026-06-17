"""Phase 3 tests: window→hours + DOV budget colour (pure) and the retention-aware
downsampled equity fetcher fetch_portfolio_window."""
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard._logic import window_to_hours, dov_budget_color, WINDOW_HOURS  # noqa: E402


# ── Pure helpers ───────────────────────────────────────────────────────────────

def test_window_to_hours_mapping_and_default():
    assert window_to_hours("1h") == 1
    assert window_to_hours("24h") == 24
    assert window_to_hours("7d") == 168
    assert window_to_hours("30d") == 720
    assert window_to_hours(None) == 24          # default
    assert window_to_hours("bogus") == 24       # unknown → default
    assert set(WINDOW_HOURS) == {"1h", "24h", "7d", "30d"}


def test_dov_budget_color_thresholds():
    # args: (loss_pct, reduced_pct=0.5%, passive_pct=0.9%)
    assert dov_budget_color(0.0,    0.005, 0.009) == "success"
    assert dov_budget_color(0.0049, 0.005, 0.009) == "success"
    assert dov_budget_color(0.005,  0.005, 0.009) == "warning"   # at REDUCED
    assert dov_budget_color(0.008,  0.005, 0.009) == "warning"
    assert dov_budget_color(0.009,  0.005, 0.009) == "danger"    # at PASSIVE
    assert dov_budget_color(0.02,   0.005, 0.009) == "danger"


# ── fetch_portfolio_window: filter, downsample, retention cap ──────────────────

def _seed_portfolio(tmp_path, rows):
    db = tmp_path / "cryptosentinel.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE portfolio (ts TEXT PRIMARY KEY, equity REAL, daily_pnl REAL, "
        "drawdown_pct REAL, circuit_breaker TEXT)"
    )
    now = datetime.now(timezone.utc)
    for i, mins_ago in enumerate(rows):
        ts = (now - timedelta(minutes=mins_ago)).isoformat()
        conn.execute(
            "INSERT INTO portfolio (ts, equity, daily_pnl, drawdown_pct) VALUES (?,?,?,?)",
            (ts, 1_000_000 + i, 0.0, 0.0),
        )
    conn.commit()
    conn.close()
    return str(db)


def test_window_filter_excludes_out_of_window(tmp_path, monkeypatch):
    # 10 rows in the last hour, 5 rows ~2 days old
    db = _seed_portfolio(tmp_path, [i for i in range(10)] + [2880 + i for i in range(5)])
    monkeypatch.setattr("dashboard._db.MAIN_DB", db)
    from dashboard._db import fetch_portfolio_window, DBOffline

    rows = fetch_portfolio_window(hours=1)
    assert not isinstance(rows, DBOffline)
    assert len(rows) == 10                       # only the in-window rows
    assert [r["ts"] for r in rows] == sorted(r["ts"] for r in rows)  # ascending


def test_downsample_caps_points(tmp_path, monkeypatch):
    db = _seed_portfolio(tmp_path, list(range(0, 600, 1)))  # 600 rows in last 10h
    monkeypatch.setattr("dashboard._db.MAIN_DB", db)
    from dashboard._db import fetch_portfolio_window

    rows = fetch_portfolio_window(hours=24, max_points=50)
    # stride = 600 // 50 = 12 → ~50 rows, never the full 600
    assert 1 < len(rows) <= 60


def test_retention_cap_limits_to_7d(tmp_path, monkeypatch):
    # one row 200h old (outside 7d=168h), one 100h old (inside)
    db = _seed_portfolio(tmp_path, [200 * 60, 100 * 60])
    monkeypatch.setattr("dashboard._db.MAIN_DB", db)
    from dashboard._db import fetch_portfolio_window

    rows = fetch_portfolio_window(hours=720)      # 30d requested → capped to 168h
    assert len(rows) == 1                          # only the 100h-old row survives the cap
