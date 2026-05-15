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

import pytest

from core.lob_recorder import LOBRecorder


def _make_depth_msg(last_id: int = 1000) -> str:
    return json.dumps({
        "stream": "btcusdt@depth20@100ms",
        "data": {
            "lastUpdateId": last_id,
            "bids": [["30000.00", "1.5"], ["29999.00", "0.8"]],
            "asks": [["30001.00", "1.2"], ["30002.00", "0.5"]],
        },
    })


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


# ── Message parsing ───────────────────────────────────────────────────────────

def test_receive_loop_buffers_depth_snapshot():
    """Feed a depth20 message through the receive loop; verify it lands in _depth_buf."""
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True

        class FakeWS:
            def __aiter__(self):
                return self

            _msgs = [_make_depth_msg(1001)]
            _idx = 0

            async def __anext__(self):
                if self._idx >= len(self._msgs):
                    rec._running = False
                    raise StopAsyncIteration
                msg = self._msgs[self._idx]
                self._idx += 1
                return msg

        await rec._receive_loop(FakeWS())

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

        class FakeWS:
            def __aiter__(self):
                return self

            _msgs = [_make_trade_msg(29999.0, 0.25, False)]
            _idx = 0

            async def __anext__(self):
                if self._idx >= len(self._msgs):
                    rec._running = False
                    raise StopAsyncIteration
                msg = self._msgs[self._idx]
                self._idx += 1
                return msg

        await rec._receive_loop(FakeWS())

    try:
        asyncio.run(_run())
        # Either still in buffer or already flushed
        rows = rec._conn.execute("SELECT COUNT(*) FROM agg_trades").fetchone()[0]
        assert len(rec._trade_buf) > 0 or rows > 0
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


def test_get_stats_before_connect_returns_zeros():
    rec = LOBRecorder(db_path="/tmp/nonexistent_never_opened.db")
    stats = rec.get_stats()
    assert stats["depth_rows"] == 0
    assert stats["trade_rows"] == 0
    assert stats["db_size_bytes"] == 0
