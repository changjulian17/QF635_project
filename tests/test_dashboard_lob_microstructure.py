"""Phase 2 tests: microstructure_bars adapter + fetcher (surfaced on the LOB page)."""
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard._logic import microstructure_bars_to_events  # noqa: E402


# ── Adapter: flag columns → event dicts ────────────────────────────────────────

def test_adapter_maps_sweep_and_reload_to_marker_categories():
    rows = [{"ts": "2026-06-16T04:00:00+00:00", "mid_price": 65000.0,
             "sweep_up": 1, "reload_bid": 1}]
    evs = microstructure_bars_to_events(rows)
    kinds = {e["kind"]: e for e in evs}
    assert kinds["sweep_up"]["event"] == "sweep"
    assert kinds["sweep_up"]["side"] == "ask"
    assert kinds["sweep_up"]["direction"] == "up"
    assert kinds["reload_bid"]["event"] == "absorption"
    assert kinds["reload_bid"]["side"] == "bid"
    # price defaults to the bar mid_price (flags are per-bar, not per-level)
    assert all(e["price"] == 65000.0 for e in evs)


def test_adapter_emits_other_category_for_non_marker_flags():
    rows = [{"ts": "t", "mid_price": 1.0, "iceberg_bid": 1, "book_flip_ask": 1,
             "liq_flip_to_sup": 1, "break_protect_short": 1}]
    evs = microstructure_bars_to_events(rows)
    assert {e["kind"] for e in evs} == {
        "iceberg_bid", "book_flip_ask", "liq_flip_to_sup", "break_protect_short"
    }
    assert all(e["event"] == "other" for e in evs)


def test_adapter_skips_zero_and_missing_flags():
    rows = [{"ts": "t", "mid_price": 1.0, "sweep_up": 0, "sweep_down": None}]
    assert microstructure_bars_to_events(rows) == []
    assert microstructure_bars_to_events([]) == []


# ── Fetcher: window filter + ordering ──────────────────────────────────────────

_MS_COLS = [
    "mid_price", "obi", "delta", "cvd", "buy_volume", "sell_volume",
    "reload_bid", "reload_ask", "iceberg_bid", "iceberg_ask",
    "sweep_up", "sweep_down", "book_flip_bid", "book_flip_ask",
    "liq_flip_to_res", "liq_flip_to_sup", "break_protect_long", "break_protect_short",
]


def _seed_main_db(tmp_path):
    db = tmp_path / "cryptosentinel.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE microstructure_bars (ts TEXT, "
        + ", ".join(f"{c} REAL" for c in _MS_COLS) + ")"
    )
    now = datetime.now(timezone.utc)
    # one recent row (in window), one old row (outside a 20-min window)
    for mins, sweep in ((1, 1), (45, 1)):
        ts = (now - timedelta(minutes=mins)).isoformat()
        conn.execute(
            f"INSERT INTO microstructure_bars (ts, mid_price, sweep_up) VALUES (?, ?, ?)",
            (ts, 65000.0, sweep),
        )
    conn.commit()
    conn.close()
    return str(db)


def test_fetch_microstructure_bars_window_and_order(tmp_path, monkeypatch):
    db_path = _seed_main_db(tmp_path)
    monkeypatch.setattr("dashboard._db.MAIN_DB", db_path)
    from dashboard._db import fetch_microstructure_bars, DBOffline

    rows = fetch_microstructure_bars(minutes=20, limit=100)
    assert not isinstance(rows, DBOffline)
    # only the 1-minute-old row is inside the 20-minute window
    assert len(rows) == 1
    assert rows[0]["sweep_up"] == 1
    # the selected columns are present for the adapter
    for col in ("ts", "mid_price", "buy_volume", "sweep_up", "break_protect_short"):
        assert col in rows[0]
