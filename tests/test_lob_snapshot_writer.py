"""Tests for engine.lob_snapshot_writer and DBWriter.write_lob_snapshot."""
import asyncio
import json
import sqlite3
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import LOBSnapshot, LOBLevel
from engine.db_writer import DBWriter
from engine.lob_snapshot_writer import lob_snapshot_writer


def _make_snapshot(n_levels: int = 5) -> LOBSnapshot:
    bids = [LOBLevel(price=50_000.0 - i * 10, qty=float(i + 1)) for i in range(n_levels)]
    asks = [LOBLevel(price=50_010.0 + i * 10, qty=float(i + 1)) for i in range(n_levels)]
    return LOBSnapshot(
        timestamp=datetime.now(timezone.utc),
        bids=bids,
        asks=asks,
        last_update_id=1234,
    )


@pytest.fixture
def db_conn(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test.db")
    monkeypatch.setattr("engine.db_writer.DB_PATH", db_file)
    from engine.db_writer import init_db
    init_db()
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    yield conn, db_file
    conn.close()


def _make_writer(db_file, monkeypatch):
    monkeypatch.setattr("engine.db_writer.DB_PATH", db_file)
    q = asyncio.Queue()
    return DBWriter(
        candle_queue=q,
        signal_queue=q,
        portfolio=MagicMock(
            equity=10_000.0, daily_pnl=0.0, drawdown_pct=0.0,
            circuit_breaker=MagicMock(name="ACTIVE"),
        ),
        ms_bar_queue=q,
    )


@pytest.mark.asyncio
async def test_write_lob_snapshot_inserts_row(db_conn, monkeypatch):
    conn, db_file = db_conn
    writer = _make_writer(db_file, monkeypatch)
    snapshot = _make_snapshot()
    await writer.write_lob_snapshot(snapshot, obi=0.3, spread=10.0, mid_price=50_005.0, cvd_delta=1.5)
    rows = conn.execute("SELECT * FROM lob_snapshots").fetchall()
    assert len(rows) == 1
    assert rows[0]["mid_price"] == 50_005.0
    assert rows[0]["spread"] == 10.0
    assert rows[0]["obi"] == 0.3
    assert rows[0]["cvd_delta"] == 1.5


@pytest.mark.asyncio
async def test_bid_ask_json_format(db_conn, monkeypatch):
    conn, db_file = db_conn
    writer = _make_writer(db_file, monkeypatch)
    snapshot = _make_snapshot(n_levels=3)
    await writer.write_lob_snapshot(snapshot, obi=0.0, spread=10.0, mid_price=50_005.0, cvd_delta=0.0)
    row = conn.execute("SELECT bid_levels_json, ask_levels_json FROM lob_snapshots").fetchone()
    bids = json.loads(row["bid_levels_json"])
    asks = json.loads(row["ask_levels_json"])
    assert isinstance(bids, list)
    assert isinstance(bids[0], list)
    assert len(bids[0]) == 2  # [price, qty]
    assert len(asks) == 3


@pytest.mark.asyncio
async def test_rolling_retention_enforced(db_conn, monkeypatch):
    conn, db_file = db_conn
    writer = _make_writer(db_file, monkeypatch)
    from config import settings
    limit = settings.LOB_HISTORY
    for i in range(limit + 5):
        snapshot = _make_snapshot()
        snapshot.timestamp = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + i, tz=timezone.utc
        )
        await writer.write_lob_snapshot(
            snapshot, obi=0.0, spread=0.0, mid_price=float(i), cvd_delta=0.0
        )
    count = conn.execute("SELECT COUNT(*) FROM lob_snapshots").fetchone()[0]
    assert count == limit


@pytest.mark.asyncio
async def test_lob_snapshot_writer_skips_when_not_synced():
    """Writer should not write when lob_status != SYNCED."""
    lob_engine = MagicMock()
    lob_engine.lob_status = "UNINITIALISED"
    lob_engine.get_snapshot = AsyncMock()
    db_writer = MagicMock()
    db_writer.write_lob_snapshot = AsyncMock()
    cvd = MagicMock()

    call_count = 0

    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise asyncio.CancelledError()

    with patch("engine.lob_snapshot_writer.asyncio.sleep", fake_sleep):
        try:
            await lob_snapshot_writer(lob_engine, cvd, db_writer, interval=0)
        except asyncio.CancelledError:
            pass

    lob_engine.get_snapshot.assert_not_called()
    db_writer.write_lob_snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_lob_snapshot_writer_skips_when_snapshot_none():
    """Writer should handle None snapshot gracefully."""
    lob_engine = MagicMock()
    lob_engine.lob_status = "SYNCED"
    lob_engine.get_snapshot = AsyncMock(return_value=None)
    db_writer = MagicMock()
    db_writer.write_lob_snapshot = AsyncMock()
    cvd = MagicMock()

    call_count = 0

    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise asyncio.CancelledError()

    with patch("engine.lob_snapshot_writer.asyncio.sleep", fake_sleep):
        try:
            await lob_snapshot_writer(lob_engine, cvd, db_writer, interval=0)
        except asyncio.CancelledError:
            pass

    db_writer.write_lob_snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_lob_snapshot_writer_writes_when_synced():
    """Writer should call write_lob_snapshot when LOB is SYNCED, with OBI in [-1, 1]."""
    snapshot = _make_snapshot()
    lob_engine = MagicMock()
    lob_engine.lob_status = "SYNCED"
    lob_engine.get_snapshot = AsyncMock(return_value=snapshot)
    db_writer = MagicMock()
    db_writer.write_lob_snapshot = AsyncMock()
    cvd = MagicMock()
    cvd.get_cvd_delta.return_value = 2.5

    call_count = 0

    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise asyncio.CancelledError()

    with patch("engine.lob_snapshot_writer.asyncio.sleep", fake_sleep):
        try:
            await lob_snapshot_writer(lob_engine, cvd, db_writer, interval=0)
        except asyncio.CancelledError:
            pass

    db_writer.write_lob_snapshot.assert_called_once()
    args = db_writer.write_lob_snapshot.call_args[0]
    assert args[0] is snapshot
    obi = args[1]
    assert -1.0 <= obi <= 1.0


@pytest.mark.asyncio
async def test_lob_snapshot_writer_tolerates_exception():
    """A transient exception should be logged and the loop should continue, not crash."""
    lob_engine = MagicMock()
    lob_engine.lob_status = "SYNCED"
    lob_engine.get_snapshot = AsyncMock(side_effect=RuntimeError("transient"))
    db_writer = MagicMock()
    db_writer.write_lob_snapshot = AsyncMock()
    cvd = MagicMock()

    call_count = 0

    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            raise asyncio.CancelledError()

    with patch("engine.lob_snapshot_writer.asyncio.sleep", fake_sleep):
        try:
            await lob_snapshot_writer(lob_engine, cvd, db_writer, interval=0)
        except asyncio.CancelledError:
            pass

    # Should have been called twice (2 iterations before cancel), not crashed
    assert lob_engine.get_snapshot.call_count == 2
    db_writer.write_lob_snapshot.assert_not_called()
