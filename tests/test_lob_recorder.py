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

from core.lob_recorder import (
    LOBRecorder,
    _BUCKET_WIDTH,
    _DEPTH_LEVELS,
    _MAX_BUFFER_SIZE,
    _RETENTION_DAYS,
)


def _make_depth_msg(exchange_ts: int | None = None) -> str:
    """Build a combined-stream depthUpdate message."""
    ts = exchange_ts or int(time.time() * 1000)
    data = {
        "e": "depthUpdate",
        "E": ts,
        "U": 1000,
        "u": 1001,
        "b": [["30000.00", "1.5"], ["29999.00", "0.8"]],
        "a": [["30001.00", "1.2"], ["30002.00", "0.5"]],
    }
    return json.dumps({"stream": "btcusdt@depth@100ms", "data": data})


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


# ── Local order book — _apply_diff ───────────────────────────────────────────

def test_apply_diff_adds_levels():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._apply_diff({
            "b": [["30000.00", "1.5"], ["29999.00", "0.8"]],
            "a": [["30001.00", "1.2"]],
            "u": 1,
        })
        assert rec._bid_book[30000.0] == 1.5
        assert rec._bid_book[29999.0] == 0.8
        assert rec._ask_book[30001.0] == 1.2
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_apply_diff_updates_existing_level():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._bid_book[30000.0] = 1.0
        rec._apply_diff({"b": [["30000.00", "3.5"]], "a": [], "u": 2})
        assert rec._bid_book[30000.0] == 3.5
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_apply_diff_removes_zero_qty():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._bid_book[30000.0] = 1.5
        rec._ask_book[30001.0] = 1.2
        rec._apply_diff({
            "b": [["30000.00", "0"]],
            "a": [["30001.00", "0.0"]],
            "u": 3,
        })
        assert 30000.0 not in rec._bid_book
        assert 30001.0 not in rec._ask_book
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_apply_diff_updates_last_update_id():
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._apply_diff({"b": [], "a": [], "u": 999})
        assert rec._last_update_id == 999
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Local order book — _bucket_levels ────────────────────────────────────────

def test_bucket_levels_aggregates_same_bucket():
    """Two levels that fall in the same price bucket must sum their qty."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        # Two prices guaranteed to share one bucket, regardless of _BUCKET_WIDTH
        base = (30000.0 // _BUCKET_WIDTH) * _BUCKET_WIDTH
        levels = [(base + _BUCKET_WIDTH * 0.1, 1.0), (base + _BUCKET_WIDTH * 0.6, 2.0)]
        result = rec._bucket_levels(levels)
        assert len(result) == 1
        bucket_price = float(result[0][0])
        bucket_qty   = float(result[0][1])
        assert bucket_price == base
        assert abs(bucket_qty - 3.0) < 1e-9
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_bucket_levels_respects_boundaries():
    """Levels in different $25 buckets produce separate entries."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        # 30000 → bucket 30000; 30025 → bucket 30025
        levels = [(30000.0, 1.0), (30025.0, 2.0)]
        result = rec._bucket_levels(levels)
        assert len(result) == 2
        prices = [float(r[0]) for r in result]
        assert 30000.0 in prices
        assert 30025.0 in prices
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_bucket_levels_empty_input():
    rec, tmp = _recorder_with_tmpdb()
    try:
        assert rec._bucket_levels([]) == []
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_bucket_levels_output_sorted():
    """Output must be sorted by bucket price ascending."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        levels = [(30050.0, 1.0), (30000.0, 2.0), (30100.0, 0.5)]
        result = rec._bucket_levels(levels)
        prices = [float(r[0]) for r in result]
        assert prices == sorted(prices)
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Local order book — _snapshot_levels ──────────────────────────────────────

def test_snapshot_levels_returns_top_n():
    """Book with more than _DEPTH_LEVELS entries must be capped at _DEPTH_LEVELS."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        # Populate 150 bid levels and 150 ask levels
        for i in range(150):
            rec._bid_book[30000.0 - i * 0.01] = 1.0
            rec._ask_book[30001.0 + i * 0.01] = 1.0
        bids, asks = rec._snapshot_levels()
        # After bucketing a $0.01-spaced book, many levels collapse into few buckets.
        # The pre-bucket selection must cap at _DEPTH_LEVELS before bucketing.
        # Check: total qty in bids ≤ _DEPTH_LEVELS (each level is qty=1.0)
        total_bid_qty = sum(float(r[1]) for r in bids)
        total_ask_qty = sum(float(r[1]) for r in asks)
        assert total_bid_qty <= _DEPTH_LEVELS
        assert total_ask_qty <= _DEPTH_LEVELS
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_snapshot_levels_bids_highest_first_selection():
    """Snapshot must select the highest bid levels (closest to mid), not lowest."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._bid_book = {float(p): 1.0 for p in range(29000, 29200)}  # 200 levels
        bids, _ = rec._snapshot_levels()
        total_qty = sum(float(r[1]) for r in bids)
        # Top 100 bid levels are 29100–29199; their bucket is floor(29100/25)*25=29100
        # All 100 levels fall in at most 4 buckets; total qty = 100
        assert abs(total_qty - _DEPTH_LEVELS) < 1e-9
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_snapshot_levels_asks_lowest_first_selection():
    """Snapshot must select the lowest ask levels (closest to mid), not highest."""
    rec, tmp = _recorder_with_tmpdb()
    try:
        rec._ask_book = {float(p): 1.0 for p in range(30001, 30201)}  # 200 levels
        _, asks = rec._snapshot_levels()
        total_qty = sum(float(r[1]) for r in asks)
        assert abs(total_qty - _DEPTH_LEVELS) < 1e-9
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Message parsing ───────────────────────────────────────────────────────────

def test_receive_loop_buffers_depth_snapshot():
    """Feed a depthUpdate message through the receive loop; verify it lands in _depth_buf."""
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True
        rec._synced = True
        # Seed a minimal book so _snapshot_levels returns non-empty
        rec._bid_book = {30000.0: 1.5, 29999.0: 0.8}
        rec._ask_book = {30001.0: 1.2, 30002.0: 0.5}
        await rec._receive_loop(_fake_ws([_make_depth_msg()], rec))

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
    """Depth snapshots must store the exchange timestamp from msg['E']."""
    rec, tmp = _recorder_with_tmpdb()
    exchange_ts = 1_700_000_000_000  # fixed, far from any local time.time()

    async def _run():
        rec._running = True
        rec._synced = True
        rec._bid_book = {30000.0: 1.5}
        rec._ask_book = {30001.0: 1.2}
        await rec._receive_loop(_fake_ws([_make_depth_msg(exchange_ts=exchange_ts)], rec))

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
    """When msg['E'] is absent, fall back to wall-clock time (not crash)."""
    rec, tmp = _recorder_with_tmpdb()

    msg_str = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate",
            # 'E' deliberately omitted
            "U": 1002, "u": 1003,
            "b": [["30000.00", "1.0"]],
            "a": [["30001.00", "1.0"]],
        },
    })

    before_ms = int(time.time() * 1000)

    async def _run():
        rec._running = True
        rec._synced = True
        rec._bid_book = {30000.0: 1.0}
        rec._ask_book = {30001.0: 1.0}
        await rec._receive_loop(_fake_ws([msg_str], rec))

    try:
        asyncio.run(_run())
        after_ms = int(time.time() * 1000) + 1000
        if rec._depth_buf:
            stored_ts = rec._depth_buf[0][0]
        else:
            stored_ts = rec._conn.execute(
                "SELECT ts_event FROM depth_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()[0]
        assert before_ms <= stored_ts <= after_ms
    finally:
        rec._conn.close()
        os.unlink(tmp)


# ── Sync gating ───────────────────────────────────────────────────────────────

def test_unsynced_events_buffered_not_written():
    """While _synced=False, depthUpdate events go to _pending_diffs, not _depth_buf."""
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True
        rec._synced = False
        await rec._receive_loop(_fake_ws([_make_depth_msg()], rec))

    try:
        asyncio.run(_run())
        assert len(rec._depth_buf) == 0
        assert len(rec._pending_diffs) == 1
    finally:
        rec._conn.close()
        os.unlink(tmp)


def test_synced_events_write_bucketed_snapshot():
    """After _synced=True, a depthUpdate writes a bucketed snapshot to _depth_buf."""
    rec, tmp = _recorder_with_tmpdb()

    async def _run():
        rec._running = True
        rec._synced = True
        rec._bid_book = {30000.0: 1.5, 29999.0: 0.8}
        rec._ask_book = {30001.0: 1.2, 30002.0: 0.5}
        await rec._receive_loop(_fake_ws([_make_depth_msg()], rec))

    try:
        asyncio.run(_run())
        assert len(rec._depth_buf) == 1
        ts, bids_json, asks_json = rec._depth_buf[0]
        bids = json.loads(bids_json)
        asks = json.loads(asks_json)
        # Verify bucket prices are multiples of _BUCKET_WIDTH
        for price_str, _ in bids:
            assert float(price_str) % _BUCKET_WIDTH == 0.0
        for price_str, _ in asks:
            assert float(price_str) % _BUCKET_WIDTH == 0.0
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


def test_bucketed_snapshot_retains_resolution_for_wall_detection():
    """A realistic dense BTC book, once bucketed for storage, must keep enough price
    resolution for identify_walls to work — otherwise recorded data is useless for
    backtesting (regression: $25 buckets collapsed the book to ~2 levels → 0 walls)."""
    from strategy.microstructure import identify_walls

    rec = LOBRecorder()
    # 100 bid levels spaced $0.5 apart (span ~$50, typical near-top BTC density).
    # Non-periodic background (so adjacent levels don't sum to a constant per bucket →
    # rolling std stays > 0) with a ~10x liquidity wall planted at level 20.
    top = 62000.0
    bg = lambda i: 1.0 + ((i * 37) % 13) * 0.1   # varied 1.0..2.2, period 13
    rec._bid_book = {round(top - i * 0.5, 2): (12.0 if i == 20 else bg(i)) for i in range(100)}
    rec._ask_book = {round(top + 1 + i * 0.5, 2): bg(i) for i in range(100)}

    bids, _asks = rec._snapshot_levels()
    assert len(bids) >= 10, f"bucketed snapshot too coarse for wall detection: {len(bids)} levels"

    levels = [(float(p), float(q)) for p, q in bids]
    walls = identify_walls(levels, "bid", sigma_threshold=2.5, window=5)
    assert len(walls) >= 1, "planted wall not detectable in bucketed snapshot"


def test_seed_failure_does_not_mark_synced():
    """REST seed failure must leave _synced False (no recording on a partial book)."""
    import aiohttp
    rec, tmp = _recorder_with_tmpdb()

    async def _no_sleep(*_a, **_k):
        return None

    try:
        with patch("core.lob_recorder.aiohttp.ClientSession",
                   side_effect=aiohttp.ClientError("seed boom")), \
             patch("core.lob_recorder.asyncio.sleep", new=_no_sleep):
            asyncio.run(rec._sync_snapshot())
        assert rec._synced is False, "recorder marked synced despite failed REST seed"
    finally:
        rec._conn.close()
        os.unlink(tmp)
