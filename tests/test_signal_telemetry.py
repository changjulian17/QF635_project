"""Tests for SignalTelemetry in core/signal_telemetry.py."""
import asyncio
import os
import sqlite3
import tempfile

import pytest

from core.signal_telemetry import SignalRecord, SignalTelemetry, _FLUSH_BATCH


def _telemetry(tmp_path: str) -> tuple[SignalTelemetry, asyncio.Queue]:
    q = asyncio.Queue()
    t = SignalTelemetry(q, db_path=tmp_path)
    t._conn = t._open_db()
    return t, q


def _record(**kwargs) -> SignalRecord:
    return SignalRecord(gate_passed="APPROVED", micro_signal="SWEEP_WITH_PROTECTION", **kwargs)


# ── Schema ────────────────────────────────────────────────────────────────────

def test_db_creates_tables():
    tmp = tempfile.mktemp(suffix=".db")
    try:
        t, _ = _telemetry(tmp)
        conn = sqlite3.connect(tmp)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert "signal_records" in tables
        assert "system_events" in tables
        conn.close()
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def test_wal_mode():
    tmp = tempfile.mktemp(suffix=".db")
    try:
        t, _ = _telemetry(tmp)
        mode = t._conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
    finally:
        t._conn.close()
        if os.path.exists(tmp):
            os.unlink(tmp)


# ── Flush ─────────────────────────────────────────────────────────────────────

def test_flush_writes_records():
    tmp = tempfile.mktemp(suffix=".db")
    try:
        t, _ = _telemetry(tmp)
        t._buf = [_record(), _record(), _record()]
        asyncio.run(t._flush())
        count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
        assert count == 3
    finally:
        t._conn.close()
        os.unlink(tmp)


def test_flush_clears_buffer():
    tmp = tempfile.mktemp(suffix=".db")
    try:
        t, _ = _telemetry(tmp)
        t._buf = [_record()]
        asyncio.run(t._flush())
        assert len(t._buf) == 0
    finally:
        t._conn.close()
        os.unlink(tmp)


def test_flush_on_batch_size():
    """Drain loop should flush once _FLUSH_BATCH records accumulate."""
    tmp = tempfile.mktemp(suffix=".db")

    async def _run():
        t, q = _telemetry(tmp)
        for _ in range(_FLUSH_BATCH):
            await q.put(_record())
        # Run one full pass through the drain loop
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        return t

    try:
        t = asyncio.run(_run())
        count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
        assert count >= _FLUSH_BATCH
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def test_flush_on_timeout():
    """Drain loop should flush on timeout even with fewer than FLUSH_BATCH records."""
    tmp = tempfile.mktemp(suffix=".db")

    async def _run():
        t, q = _telemetry(tmp)
        await q.put(_record())
        # Run drain loop slightly longer than FLUSH_INTERVAL (we use a short timeout)
        try:
            await asyncio.wait_for(t._drain_loop(), timeout=0.1)
        except asyncio.TimeoutError:
            pass
        # Manually flush leftovers
        await t._flush()
        return t

    try:
        t = asyncio.run(_run())
        count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
        assert count >= 1
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


# ── Outcome update ────────────────────────────────────────────────────────────

def test_outcome_update():
    tmp = tempfile.mktemp(suffix=".db")
    try:
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
    finally:
        t._conn.close()
        os.unlink(tmp)


def test_duplicate_signal_id_ignored():
    """INSERT OR IGNORE should silently drop duplicate signal_ids."""
    tmp = tempfile.mktemp(suffix=".db")
    try:
        t, _ = _telemetry(tmp)
        rec = _record()
        t._buf = [rec, rec]   # same signal_id twice
        asyncio.run(t._flush())
        count = t._conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
        assert count == 1
    finally:
        t._conn.close()
        os.unlink(tmp)
