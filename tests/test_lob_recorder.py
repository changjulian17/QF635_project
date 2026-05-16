"""
Tests for LOBRecorder in core/lob_recorder.py.
All tests are synchronous or use asyncio.run(); no live network required.
"""
import asyncio
import json
import os
import sqlite3
import tempfile
import time
from unittest.mock import patch

import pytest

from core.lob_recorder import LOBRecorder, _MAX_BUFFER_SIZE, _RETENTION_DAYS


def _make_depth_msg(last_id: int = 1000, exchange_ts: int | None = None) -> str:
    data: dict = {
        "lastUpdateId": last_id,
        "bids": [["30000.00", "1.5"], ["29999.00", "0.8"]],
        "asks": [["30001.00", "1.2"], ["30002.00", "0.5"]],
    }
    if exchange_ts is not None:
        data["T"] = exchange_ts
    return json.dumps({"stream": "btcusdt@depth20@100ms", "data": data})


def _make_trade_msg(price: float = 30000.5, qty: float = 0.1, buyer_maker: bool = False) -> str:
    return json.dumps({
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "T": int(time.time() * 1000),
            "p": str(price),
            "q": str(qty),
            "m": buyer_maker,
        },
    })


def _recorder_with_tmpdb() -> tuple[LOBRecorder, str]:
    tmp = tempfile.mktemp(suffix=".db")
    rec = LOBRecorder(db_path=tmp)
    rec._conn = rec._open_db()
    return rec, tmp


def _fake_ws(msgs: list[str], rec: LOBRecorder):
    class FakeWS:
        def __aiter__(self):
            return self

        _msgs = msgs
        _idx = 0

        async def __anext__(self):
            if self._idx >= len(self._msgs):
                rec._running = False
                raise StopAsyncIteration
            msg = self._msgs[self._idx]
            self._idx += 1
            return msg

    return FakeWS()


# ── Database schema ───────────────────────────────────────────────────────────

def test_db_creates_tables():
    rec, tmp = _recorder_with_tmpdb()
    try:
        conn = sqlite3.connect(tmp)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "depth_snapshots" in tables
        assert "agg_trades" in tables
        conn.close()
    finally:
        os.unlink(tmp)


def test_wal_mode_enabled():
    rec, tmp = _recorder_with_tmpdb()
    try:
        mode = rec._conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Buffer + flush ────────────────────────────────────────────────────────────

def test_flush_writes_depth_rows():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._depth_buf.append((int(time.time() * 1000), "[]", "[]"))
        rec._depth_buf.append((int(time.time() * 1000), "[]", "[]"))
        asyncio.run(rec._flush())
        count = rec._conn.execute("SELECT COUNT(*) FROM depth_snapshots").fetchone()[0]
        assert count == 2
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_flush_writes_trade_rows():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._trade_buf.append((int(time.time() * 1000), 30000.0, 0.5, 0))
        asyncio.run(rec._flush())
        count = rec._conn.execute("SELECT COUNT(*) FROM agg_trades").fetchone()[0]
        assert count == 1
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_flush_clears_buffers():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._depth_buf.append((int(time.time() * 1000), "[]", "[]"))
        rec._trade_buf.append((int(time.time() * 1000), 30000.0, 0.1, 1))
        asyncio.run(rec._flush())
        assert len(rec._depth_buf) == 0
        assert len(rec._trade_buf) == 0
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_buffer_capped_on_db_failure():
    """On sustained DB failures, re-queued rows must not exceed _MAX_BUFFER_SIZE."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        overflow = _MAX_BUFFER_SIZE + 500
        rec._depth_buf = [(int(time.time() * 1000), "[]", "[]")] * overflow

        with patch.object(rec, "_flush_sync", return_value=False):
            asyncio.run(rec._flush())

        assert len(rec._depth_buf) == _MAX_BUFFER_SIZE
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Message parsing ───────────────────────────────────────────────────────────

def test_receive_loop_buffers_depth_snapshot():
    """Feed a depth20 message through the receive loop; verify it lands in _depth_buf."""
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True
        await rec._receive_loop(_fake_ws([_make_depth_msg(1001)], rec))

    try:
        asyncio.run(_run())
        assert len(rec._depth_buf) > 0 or rec._conn.execute(
            "SELECT COUNT(*) FROM depth_snapshots"
        ).fetchone()[0] > 0
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_receive_loop_buffers_agg_trade():
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True
        await rec._receive_loop(_fake_ws([_make_trade_msg(29999.0, 0.25, False)], rec))

    try:
        asyncio.run(_run())
        rows = rec._conn.execute("SELECT COUNT(*) FROM agg_trades").fetchone()[0]
        assert len(rec._trade_buf) > 0 or rows > 0
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_depth_uses_exchange_timestamp():
    """Depth snapshots must store the exchange timestamp from msg["T"], not local wall clock."""
    rec, tmp = _recorder_with_tmpdb()
    exchange_ts = 1_700_000_000_000  # fixed, far from any local time.time()

    async def _run():
        rec._running = True
        await rec._receive_loop(_fake_ws([_make_depth_msg(1001, exchange_ts=exchange_ts)], rec))

    try:
        asyncio.run(_run())
        if rec._depth_buf:
            stored_ts = rec._depth_buf[0][0]
        else:
            stored_ts = rec._conn.execute(
                "SELECT ts_event FROM depth_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()[0]
        assert stored_ts == exchange_ts
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_depth_falls_back_to_event_time():
    """When msg["T"] is absent, fall back to msg["E"]."""
    rec, tmp = _recorder_with_tmpdb()
    event_ts = 1_600_000_000_000

    msg_str = json.dumps({
        "stream": "btcusdt@depth20@100ms",
        "data": {"lastUpdateId": 1002, "E": event_ts, "bids": [], "asks": []},
    })

    async def _run():
        rec._running = True
        await rec._receive_loop(_fake_ws([msg_str], rec))

    try:
        asyncio.run(_run())
        if rec._depth_buf:
            stored_ts = rec._depth_buf[0][0]
        else:
            stored_ts = rec._conn.execute(
                "SELECT ts_event FROM depth_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()[0]
        assert stored_ts == event_ts
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Stats ─────────────────────────────────────────────────────────────────────

def test_get_stats_returns_row_counts():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._depth_buf.append((int(time.time() * 1000), "[]", "[]"))
        rec._trade_buf.append((int(time.time() * 1000), 30000.0, 0.1, 0))
        asyncio.run(rec._flush())
        stats = rec.get_stats()
        assert stats["depth_rows"] == 1
        assert stats["trade_rows"] == 1
        assert stats["db_size_bytes"] > 0
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_get_stats_includes_buffer():
    """get_stats() must count unflushed buffer rows on top of DB rows."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._depth_buf.append((int(time.time() * 1000), "[]", "[]"))
        rec._trade_buf.append((int(time.time() * 1000), 30000.0, 0.1, 0))
        asyncio.run(rec._flush())  # 1 depth + 1 trade in DB, buffers now empty

        rec._depth_buf.extend([(int(time.time() * 1000), "[]", "[]")] * 3)
        rec._trade_buf.extend([(int(time.time() * 1000), 30000.0, 0.1, 0)] * 2)

        stats = rec.get_stats()
        assert stats["depth_rows"] == 1 + 3
        assert stats["trade_rows"] == 1 + 2
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_get_stats_before_connect_returns_zeros():
    rec = LOBRecorder(db_path="/tmp/nonexistent_never_opened.db")
    stats = rec.get_stats()
    assert stats["depth_rows"] == 0
    assert stats["trade_rows"] == 0
    assert stats["db_size_bytes"] == 0


# ── Retention / cleanup ───────────────────────────────────────────────────────

def test_cleanup_purges_old_records():
    """_purge_old_records() removes rows older than _RETENTION_DAYS and keeps recent ones."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        old_ts = int((time.time() - (_RETENTION_DAYS + 1) * 86_400) * 1000)
        recent_ts = int(time.time() * 1000)

        rec._conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (old_ts, "[]", "[]"),
        )
        rec._conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (recent_ts, "[]", "[]"),
        )
        rec._conn.execute(
            "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
            (old_ts, 30000.0, 0.1, 0),
        )
        rec._conn.commit()

        rec._purge_old_records()

        depth_count = rec._conn.execute("SELECT COUNT(*) FROM depth_snapshots").fetchone()[0]
        trade_count = rec._conn.execute("SELECT COUNT(*) FROM agg_trades").fetchone()[0]
        assert depth_count == 1   # only the recent row survives
        assert trade_count == 0   # old trade purged
    finally:
        rec._conn.close()
        os.unlink(tmp)
