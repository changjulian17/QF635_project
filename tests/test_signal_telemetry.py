"""Tests for SignalTelemetry in core/signal_telemetry.py."""
import asyncio
import sqlite3
from unittest.mock import MagicMock

import pytest

from core.signal_telemetry import SignalRecord, SignalTelemetry, _FLUSH_BATCH, _MAX_BUF


def _telemetry(tmp_path: str) -> tuple[SignalTelemetry, asyncio.Queue]:
    q = asyncio.Queue()
    t = SignalTelemetry(q, db_path=tmp_path)
    t._conn = t._open_db()
    return t, q


def _record(**kwargs) -> SignalRecord:
    return SignalRecord(gate_passed="APPROVED", micro_signal="SWEEP_WITH_PROTECTION", **kwargs)


# ── Schema ────────────────────────────────────────────────────────────────────

def test_db_creates_tables(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    conn = sqlite3.connect(tmp)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "signal_records" in tables
    assert "system_events" in tables
    conn.close()


def test_wal_mode(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    mode = t._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"
    t._conn.close()


# ── Flush ─────────────────────────────────────────────────────────────────────

def test_flush_writes_records(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._buf = [_record(), _record(), _record()]
    asyncio.run(t._flush())
    count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    assert count == 3
    t._conn.close()


def test_flush_clears_buffer(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._buf = [_record()]
    asyncio.run(t._flush())
    assert len(t._buf) == 0
    t._conn.close()


def test_flush_on_batch_size(tmp_path):
    """Drain loop should flush once _FLUSH_BATCH records accumulate."""
    tmp = str(tmp_path / "test.db")

    async def _run():
        t, q = _telemetry(tmp)
        for _ in range(_FLUSH_BATCH):
            await q.put(_record())
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        return t

    t = asyncio.run(_run())
    count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    assert count >= _FLUSH_BATCH
    t._conn.close()


def test_flush_on_timeout(tmp_path):
    """Drain loop should flush buffered records when explicitly triggered after timeout."""
    tmp = str(tmp_path / "test.db")

    async def _run():
        t, q = _telemetry(tmp)
        await q.put(_record())
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.1)
        except asyncio.TimeoutError:
            pass
        await t._flush()
        return t

    t = asyncio.run(_run())
    count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    assert count >= 1
    t._conn.close()


# ── Outcome update ────────────────────────────────────────────────────────────

def test_outcome_update(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record()
    t._buf = [rec]
    asyncio.run(t._flush())
    t.update_outcome(rec.signal_id, "WIN", pnl=50.0, pnl_pct=0.005, duration_min=12.5)
    row = t._conn.execute(
        "SELECT outcome, pnl, pnl_pct, duration_min FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row[0] == "WIN"
    assert abs(row[1] - 50.0) < 1e-9
    assert abs(row[2] - 0.005) < 1e-9
    assert abs(row[3] - 12.5) < 1e-9
    t._conn.close()


def test_duplicate_signal_id_ignored(tmp_path):
    """INSERT OR IGNORE should silently drop duplicate signal_ids."""
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record()
    t._buf = [rec, rec]   # same signal_id twice
    asyncio.run(t._flush())
    count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    assert count == 1
    t._conn.close()


# ── Graceful shutdown ─────────────────────────────────────────────────────────

async def test_graceful_shutdown_flushes_remaining_records(tmp_path):
    """CancelledError must flush buffered + queued records before propagating."""
    tmp = str(tmp_path / "test.db")
    q = asyncio.Queue()
    t = SignalTelemetry(q, db_path=tmp)
    for _ in range(3):
        await q.put(_record())
    task = asyncio.create_task(t.run())
    await asyncio.sleep(0.05)   # let run() open DB and enter drain loop
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    conn = sqlite3.connect(tmp)
    count = conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    conn.close()
    assert count == 3


# ── Buffer cap ────────────────────────────────────────────────────────────────

def test_buffer_cap_on_persistent_db_error(tmp_path):
    """Buffer must not exceed _MAX_BUF when flushes repeatedly fail."""
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._conn = MagicMock()
    t._conn.executemany.side_effect = sqlite3.Error("disk full")
    t._buf = [_record() for _ in range(_MAX_BUF + 20)]
    asyncio.run(t._flush())
    assert len(t._buf) <= _MAX_BUF
