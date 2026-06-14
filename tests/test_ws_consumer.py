"""
Tests for HeartbeatMonitor in core/ws_consumer.py.
All tests are synchronous — HeartbeatMonitor.record() is a pure function.
"""
import time
from unittest.mock import patch

import pytest

from config import settings
from core.ws_consumer import HeartbeatMonitor, BinanceWebSocketConsumer
from models import SharedState

def _event_ms(delta_ms: float) -> int:
    """Return a Binance event timestamp that is delta_ms in the past."""
    return int(time.time() * 1000) - int(delta_ms)


def _fill_window(hb: HeartbeatMonitor, bad: int, good: int) -> str:
    """Feed (bad) degraded + (good) healthy packets to fill the 10-message window."""
    assert bad + good == hb._deltas.maxlen
    status = "HEALTHY"
    for _ in range(bad):
        status = hb.record(_event_ms(250))   # above WARN_MS (200)
    for _ in range(good):
        status = hb.record(_event_ms(50))    # below WARN_MS (200)
    return status


# ── Existing tests (updated for rate-gate behaviour) ─────────────────────────

def test_heartbeat_healthy():
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    status = hb.record(_event_ms(50))   # single packet — window not full, stays HEALTHY
    assert status == "HEALTHY"
    assert hb.last_delta_ms < hb.WARN_MS


def test_heartbeat_degraded():
    """Rate threshold (5/10) must be reached before DEGRADED is entered."""
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    # 5 bad + 5 good = exactly 50% — crosses entry threshold
    status = _fill_window(hb, bad=5, good=5)
    assert status == "DEGRADED"


def test_heartbeat_critical_consecutive():
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    for _ in range(hb.CONSEC_LIMIT - 1):
        status = hb.record(_event_ms(600))
        assert status != "CRITICAL"
    status = hb.record(_event_ms(600))
    assert status == "CRITICAL"


def test_heartbeat_resets_on_healthy_packet():
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    for _ in range(hb.CONSEC_LIMIT - 1):
        hb.record(_event_ms(600))
    hb.record(_event_ms(50))
    assert hb._critical_count == 0
    for _ in range(hb.CONSEC_LIMIT - 1):
        hb.record(_event_ms(600))
    assert hb.status != "CRITICAL"


def test_heartbeat_avg_delta():
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    hb.record(_event_ms(100))
    hb.record(_event_ms(200))
    assert 100 < hb.avg_delta_ms < 250


def test_heartbeat_shared_state_updated():
    """SharedState reflects DEGRADED only after rate threshold is crossed."""
    import asyncio
    from models import SharedState
    from core.ws_consumer import BinanceWebSocketConsumer

    state    = SharedState()
    q        = asyncio.Queue()
    # Use small thresholds for test
    consumer = BinanceWebSocketConsumer(
        streams=["btcusdt@bookTicker"], 
        shared_state=state, 
        candle_queue=q,
        warn_ms=200,
        critical_ms=500,
        consec_limit=3
    )

    # Fill window with 5 bad + 5 good → DEGRADED
    for _ in range(5):
        consumer.heartbeat.record(_event_ms(250))
    for _ in range(5):
        consumer.heartbeat.record(_event_ms(50))

    state.heartbeat_status = consumer.heartbeat.status
    state.last_delta_ms    = consumer.heartbeat.last_delta_ms

    assert state.heartbeat_status == "DEGRADED"
    assert state.last_delta_ms >= 0


# ── New tests ─────────────────────────────────────────────────────────────────

def test_heartbeat_below_rate_stays_healthy():
    """4/10 bad messages (< 50%) — window full but rate below threshold → HEALTHY."""
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    status = _fill_window(hb, bad=4, good=6)
    assert status == "HEALTHY"


def test_heartbeat_hysteresis_no_recovery():
    """4/10 bad (between 30–50%) while in DEGRADED → stays DEGRADED (hysteresis)."""
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    _fill_window(hb, bad=5, good=5)       # enter DEGRADED
    assert hb.status == "DEGRADED"

    hb.record(_event_ms(50))              # now 4/10 bad (40%)
    assert hb.status == "DEGRADED"        # should NOT recover yet (< 30% required)


def test_heartbeat_rate_recovery():
    """Rate dropping below 30% from DEGRADED transitions to HEALTHY."""
    hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
    _fill_window(hb, bad=5, good=5)       # enter DEGRADED
    assert hb.status == "DEGRADED"

    for _ in range(3):                    # now 2/10 bad (20%)
        hb.record(_event_ms(50))
    assert hb.status == "HEALTHY"


def test_heartbeat_sustained_degraded():
    """DEGRADED held for > HEARTBEAT_SUSTAINED_MS transitions to SUSTAINED_DEGRADED."""
    base_time = 1_000_000.0   # arbitrary fixed epoch in seconds

    with patch("core.ws_consumer.time") as mock_time:
        mock_time.time.return_value = base_time
        hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
        # Fill window with 10 bad packets at base_time → DEGRADED
        for _ in range(10):
            event_ms = int(base_time * 1000) - 250
            hb.record(event_ms)
        assert hb.status == "DEGRADED"

        # Advance time by 11 seconds (greater than default 10s SUSTAINED_MS)
        mock_time.time.return_value = base_time + 11.0
        hb.record(int((base_time + 11.0) * 1000) - 250)
        assert hb.status == "SUSTAINED_DEGRADED"


def test_heartbeat_recovery_from_sustained():
    """Rate dropping below 30% from SUSTAINED_DEGRADED transitions to HEALTHY."""
    base_time = 1_000_000.0

    with patch("core.ws_consumer.time") as mock_time:
        mock_time.time.return_value = base_time
        hb = HeartbeatMonitor(warn_ms=200, critical_ms=500, consec_limit=3)
        for _ in range(10):
            hb.record(int(base_time * 1000) - 250)
        assert hb.status == "DEGRADED"

        mock_time.time.return_value = base_time + 11.0
        hb.record(int((base_time + 11.0) * 1000) - 250)
        assert hb.status == "SUSTAINED_DEGRADED"

        # Drop rate to 2/10 bad
        for _ in range(8):
            hb.record(int((base_time + 11.0) * 1000) - 50)
        assert hb.status == "HEALTHY"


def test_heartbeat_monitor_default_critical_ms():
    # This test is now dependent on settings
    hb = HeartbeatMonitor()
    assert hb.CRITICAL_MS == settings.HEARTBEAT_CRITICAL_MS


def test_depth_update_replaces_oldest_snapshot_when_queue_full():
    import asyncio

    from core.ws_consumer import BinanceWebSocketConsumer

    async def _run() -> None:
        candle_q = asyncio.Queue()
        depth_q = asyncio.Queue(maxsize=1)
        # Pass required args
        consumer = BinanceWebSocketConsumer(streams=["btcusdt@depth@100ms"], shared_state=SharedState(), candle_queue=candle_q, depth_queue=depth_q)
        consumer._lob_synced = True
        consumer._bid_book = {100.0: 1.0, 99.5: 2.0}
        consumer._ask_book = {100.5: 1.5, 101.0: 2.0}
        consumer._lob_update_id = 41

        await depth_q.put({"sentinel": True})

        msg = {
            "e": "depthUpdate",
            "E": 1234567890000,
            "u": 42,
            "U": 42, # Added U
            "b": [["100.0", "3.0"]],
            "a": [],
        }

        await asyncio.wait_for(consumer._dispatch("btcusdt@depth@100ms", msg), timeout=0.2)

        assert depth_q.qsize() == 1
        snapshot = depth_q.get_nowait()
        assert snapshot["lastUpdateId"] == 42
        assert snapshot["bids"][0] == ["100.0", "3.0"]

    asyncio.run(_run())


def test_critical_triggers_immediate_break_in_receive_loop():
    """_receive_loop must return quickly on CRITICAL — not wait for 10s recv timeout."""
    import asyncio, json
    from models import SharedState
    from core.ws_consumer import BinanceWebSocketConsumer

    now_ms = int(time.time() * 1000)
    stale_msg = json.dumps({
        "stream": "btcusdt@bookTicker",
        "data": {"E": now_ms - 50000, "s": "BTCUSDT",
                 "b": "1", "B": "1", "a": "1", "A": "1"},
    })
    recv_calls = 0
    _consec = 3 # Use fixed value for test stability

    class _FakeWS:
        async def recv(self_):
            nonlocal recv_calls
            recv_calls += 1
            if recv_calls <= _consec:
                return stale_msg   # all stale → CRITICAL fires on recv_calls == _consec
            await asyncio.sleep(60)   # would stall here without CRITICAL break

        async def close(self_): pass

    consumer = BinanceWebSocketConsumer(
        shared_state=SharedState(),
        streams=["btcusdt@bookTicker"],
        heartbeat_key="heartbeat_status",
        warn_ms=200,
        critical_ms=500,
        consec_limit=_consec,
    )
    consumer._running = True   # normally set by start(); required for _receive_loop to enter

    async def _run() -> None:
        start = time.monotonic()
        await consumer._receive_loop(_FakeWS())
        elapsed = time.monotonic() - start
        assert recv_calls == _consec, f"expected {_consec} recv calls, got {recv_calls}"
        assert consumer.heartbeat.status == "CRITICAL"
        assert elapsed < 5.0, f"_receive_loop took {elapsed:.1f}s — CRITICAL break not firing"

    asyncio.run(_run())


def test_recv_timeout_value_is_expected():
    """_receive_loop passes timeout from settings to asyncio.wait_for."""
    import asyncio
    from unittest.mock import patch
    from models import SharedState
    from core.ws_consumer import BinanceWebSocketConsumer

    captured: list[float] = []

    async def _spy_wait_for(coro, timeout=None, **kw):
        captured.append(timeout)
        coro.close()
        raise asyncio.TimeoutError()

    consumer = BinanceWebSocketConsumer(
        shared_state=SharedState(),
        streams=["btcusdt@depth@500ms"],
        heartbeat_key="heartbeat_status",
    )
    consumer._running = True

    class _FakeWS:
        async def recv(self_): await asyncio.sleep(9999)
        async def close(self_): pass

    async def _run():
        with patch.object(asyncio, "wait_for", new=_spy_wait_for):
            await consumer._receive_loop(_FakeWS())

    asyncio.run(_run())

    assert captured, "asyncio.wait_for was never called by _receive_loop"
    assert captured[0] == settings.WS_RECV_TIMEOUT_S
