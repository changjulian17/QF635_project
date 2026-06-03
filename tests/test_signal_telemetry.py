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
    asyncio.run(t.update_outcome(rec.signal_id, "WIN", pnl=50.0, pnl_pct=0.005, duration_min=12.5))
    row = t._conn.execute(
        "SELECT outcome, pnl, pnl_pct, duration_min FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row[0] == "WIN"
    assert abs(row[1] - 50.0) < 1e-9
    assert abs(row[2] - 0.005) < 1e-9
    assert abs(row[3] - 12.5) < 1e-9
    t._conn.close()


def test_approved_record_flushes_immediately_for_outcome_update(tmp_path):
    """APPROVED flush is immediate so a racing update_outcome always finds the row.

    This test would FAIL without the fix in _drain_loop that triggers an immediate
    flush when gate_passed == 'APPROVED' — the row would not be committed within 0.3s.
    """
    tmp = str(tmp_path / "test.db")

    async def _run():
        t, q = _telemetry(tmp)
        rec = _record()  # gate_passed="APPROVED" by default
        await q.put(rec)
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.3)
        except asyncio.TimeoutError:
            pass
        # Row must already be in DB from the immediate flush triggered by APPROVED
        await t.update_outcome(rec.signal_id, "WIN", pnl=50.0, pnl_pct=0.005, duration_min=10.0)
        return t, rec.signal_id

    t, sid = asyncio.run(_run())
    row = t._conn.execute(
        "SELECT outcome FROM signal_records WHERE signal_id=?", (sid,)
    ).fetchone()
    assert row is not None, "APPROVED record must be in DB before update_outcome is called"
    assert row[0] == "WIN"
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


def test_flush_preserves_buffer_when_conn_is_none(tmp_path):
    """_flush must not clear the buffer when _conn is None — records must survive."""
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._buf = [_record(), _record()]
    t._conn = None
    asyncio.run(t._flush())
    assert len(t._buf) == 2


# ── update_fill ───────────────────────────────────────────────────────────────

def test_update_fill_writes_slippage(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record()
    t._buf = [rec]
    asyncio.run(t._flush())
    asyncio.run(t.update_fill(rec.signal_id, slippage_bps=2.5))
    row = t._conn.execute(
        "SELECT entry_slippage_bps FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row is not None
    assert abs(row[0] - 2.5) < 1e-9
    t._conn.close()


def test_update_fill_no_op_when_conn_is_none(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._conn = None
    asyncio.run(t.update_fill("nonexistent-id", slippage_bps=1.0))  # must not raise


# ── update_outcome guards ─────────────────────────────────────────────────────

def test_update_outcome_no_op_when_conn_is_none(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._conn = None
    asyncio.run(
        t.update_outcome("nonexistent-id", "WIN", pnl=1.0, pnl_pct=0.01, duration_min=1.0)
    )  # must not raise


def test_update_outcome_stores_r_multiple(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record()
    t._buf = [rec]
    asyncio.run(t._flush())
    asyncio.run(
        t.update_outcome(rec.signal_id, "WIN", pnl=100.0, pnl_pct=0.01,
                         duration_min=5.0, r_multiple=2.5)
    )
    row = t._conn.execute(
        "SELECT outcome, r_multiple_achieved FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row[0] == "WIN"
    assert abs(row[1] - 2.5) < 1e-9
    t._conn.close()


# ── write_system_event ────────────────────────────────────────────────────────

def test_write_system_event_persists(tmp_path):
    import json
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t.write_system_event("ENGINE_START", {"version": "3.0"})
    row = t._conn.execute(
        "SELECT event_type, payload_json FROM system_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "ENGINE_START"
    assert json.loads(row[1]) == {"version": "3.0"}
    t._conn.close()


def test_write_system_event_null_payload(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t.write_system_event("ENGINE_STOP")  # no payload
    row = t._conn.execute(
        "SELECT payload_json FROM system_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row[0] is None
    t._conn.close()


def test_write_system_event_no_op_when_conn_is_none(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._conn = None
    t.write_system_event("ENGINE_START", {"k": "v"})  # must not raise


# ── Non-APPROVED records must not flush immediately ───────────────────────────

def test_non_approved_record_does_not_flush_immediately(tmp_path):
    """Regression: only APPROVED triggers immediate flush; gate-fail records still batch."""
    tmp = str(tmp_path / "test.db")

    async def _run():
        t, q = _telemetry(tmp)
        from core.signal_telemetry import SignalRecord
        await q.put(SignalRecord(gate_passed="GATE_2_FAIL", micro_signal="SWEEP_WITH_PROTECTION"))
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.2)
        except asyncio.TimeoutError:
            pass
        return t

    t = asyncio.run(_run())
    count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    assert count == 0, "GATE_2_FAIL record must not flush before batch size or interval"
    t._conn.close()


# ── close() ───────────────────────────────────────────────────────────────────

def test_close_flushes_and_disconnects(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    t._buf = [_record(), _record()]
    asyncio.run(t.close())
    assert t._conn is None
    conn = sqlite3.connect(tmp)
    count = conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
    conn.close()
    assert count == 2


# ── features_json handling ────────────────────────────────────────────────────

def test_features_json_empty_string_stored_as_null(tmp_path):
    """Empty features_json must be coerced to SQL NULL, not stored as ''."""
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record(features_json="")
    t._buf = [rec]
    asyncio.run(t._flush())
    row = t._conn.execute(
        "SELECT features_json FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row[0] is None
    t._conn.close()


def test_features_json_populated_stored_verbatim(tmp_path):
    tmp = str(tmp_path / "test.db")
    t, _ = _telemetry(tmp)
    rec = _record(features_json="[1.0,2.0,3.0]")
    t._buf = [rec]
    asyncio.run(t._flush())
    row = t._conn.execute(
        "SELECT features_json FROM signal_records WHERE signal_id=?",
        (rec.signal_id,),
    ).fetchone()
    assert row[0] == "[1.0,2.0,3.0]"
    t._conn.close()
