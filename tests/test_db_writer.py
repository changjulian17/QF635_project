"""Unit tests for DBWriter — synchronous write/read/purge logic using a temp DB."""
import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

import engine.db_writer as db_mod
from engine.db_writer import DBWriter
from models import (
    Candle, MicrostructureBar, PortfolioState,
)


@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    """Redirect all DB writes to a per-test temp file."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    db_mod.init_db()
    return db_path


def make_portfolio(**kw) -> PortfolioState:
    defaults = dict(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    return PortfolioState(**{**defaults, **kw})


def make_candle(**kw) -> Candle:
    defaults = dict(
        open_time=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        open=80000.0, high=80100.0, low=79900.0, close=80050.0,
        volume=1.23, is_closed=True,
    )
    return Candle(**{**defaults, **kw})


def make_ms_bar(**kw) -> MicrostructureBar:
    defaults = dict(
        timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        mid_price=80050.0, spread=1.0, obi=0.1,
        delta=0.5, cvd=1.5, buy_volume=2.0, sell_volume=1.5,
        reload_bid=False, reload_ask=False,
        iceberg_bid=False, iceberg_ask=False,
        sweep_up=False, sweep_down=False,
        book_flip_bid=False, book_flip_ask=False,
        liq_flip_to_res=False, liq_flip_to_sup=False,
        break_protect_long=False, break_protect_short=False,
    )
    return MicrostructureBar(**{**defaults, **kw})


# ── init_db ───────────────────────────────────────────────────────────────────

def test_init_db_creates_all_tables(temp_db):
    conn = sqlite3.connect(temp_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"candles", "portfolio", "microstructure_bars", "lob_snapshots"} <= tables


def test_init_db_is_idempotent(temp_db):
    db_mod.init_db()  # second call must not raise
    db_mod.init_db()


# ── _write_candle ─────────────────────────────────────────────────────────────

def test_write_candle_persists(temp_db):
    candle = make_candle()
    DBWriter._write_candle(candle)
    conn = sqlite3.connect(temp_db)
    row = conn.execute("SELECT open, high, low, close, volume FROM candles").fetchone()
    conn.close()
    assert row == (80000.0, 80100.0, 79900.0, 80050.0, 1.23)


def test_write_candle_upserts_on_duplicate_time(temp_db):
    candle1 = make_candle(close=80000.0)
    candle2 = make_candle(close=81000.0)  # same open_time, different close
    DBWriter._write_candle(candle1)
    DBWriter._write_candle(candle2)
    conn = sqlite3.connect(temp_db)
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    close = conn.execute("SELECT close FROM candles").fetchone()[0]
    conn.close()
    assert count == 1
    assert close == 81000.0


# ── _write_portfolio ──────────────────────────────────────────────────────────

def test_write_portfolio_persists(temp_db):
    pf = make_portfolio(equity=9500.0, daily_pnl=-500.0)
    DBWriter._write_portfolio(pf)
    conn = sqlite3.connect(temp_db)
    row = conn.execute("SELECT equity, daily_pnl, circuit_breaker FROM portfolio").fetchone()
    conn.close()
    assert row[0] == 9500.0
    assert row[1] == -500.0
    assert row[2] == "ACTIVE"


# ── _purge_old_records ────────────────────────────────────────────────────────

def test_purge_removes_old_candles(temp_db, monkeypatch):
    writer = DBWriter(None, make_portfolio(), None)
    monkeypatch.setattr(writer, "RETENTION_DAYS", 7)

    old_time = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    recent_time = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO candles VALUES (?,100,101,99,100,1)", (old_time,))
    conn.execute("INSERT INTO candles VALUES (?,100,101,99,100,1)", (recent_time,))
    conn.commit()
    conn.close()

    writer._purge_old_records()

    conn = sqlite3.connect(temp_db)
    rows = conn.execute("SELECT open_time FROM candles").fetchall()
    conn.close()
    assert len(rows) == 1
    assert recent_time in rows[0][0]


# ── _write_ms_bar ─────────────────────────────────────────────────────────────

def test_write_ms_bar_persists(temp_db):
    bar = make_ms_bar(mid_price=80050.0, obi=0.25, sweep_up=True)
    DBWriter._write_ms_bar(bar)
    conn = sqlite3.connect(temp_db)
    row = conn.execute("SELECT mid_price, obi, sweep_up FROM microstructure_bars").fetchone()
    conn.close()
    assert row == (80050.0, 0.25, 1)


def test_write_ms_bar_rolling_retention(temp_db, monkeypatch):
    import engine.db_writer as db_mod
    monkeypatch.setattr(db_mod.settings, "LOB_HISTORY", 3)
    for i in range(5):
        DBWriter._write_ms_bar(make_ms_bar(mid_price=float(i)))
    conn = sqlite3.connect(temp_db)
    count = conn.execute("SELECT COUNT(*) FROM microstructure_bars").fetchone()[0]
    conn.close()
    assert count == 3


def test_init_db_enables_wal(temp_db):
    conn = sqlite3.connect(temp_db)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode == "wal"
