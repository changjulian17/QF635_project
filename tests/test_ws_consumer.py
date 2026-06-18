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

    assert hb.status == "HEALTHY"
    assert hb._degraded_since_ms is None


def test_ws_seed_failure_stays_unsynced():
    """REST seed failure must leave _lob_synced False so Gate 0 won't see SYNCED."""
    import asyncio, aiohttp
    from core.ws_consumer import BinanceWebSocketConsumer

    consumer = BinanceWebSocketConsumer(candle_queue=asyncio.Queue())

    async def _no_sleep(*_a, **_k):
        return None

    with patch("core.ws_consumer.aiohttp.ClientSession",
               side_effect=aiohttp.ClientError("seed boom")), \
         patch("core.ws_consumer.asyncio.sleep", new=_no_sleep):
        asyncio.run(consumer._sync_lob_snapshot())

    assert consumer._lob_synced is False


def test_ws_seed_rejects_gapful_buffer():
    """Buffered diffs that don't bridge the snapshot (gap) → seed fails → stays UNSYNCED."""
    import asyncio
    from core.ws_consumer import BinanceWebSocketConsumer
    consumer = BinanceWebSocketConsumer(candle_queue=asyncio.Queue())
    snap = {"bids": [["100.0", "1"]], "asks": [["101.0", "1"]], "lastUpdateId": 100}
    consumer._lob_pending = [{"U": 105, "u": 106, "b": [], "a": []}]  # 101–104 missing

    async def _fetch():
        return snap
    async def _no_sleep(*_a, **_k):
        return None

    with patch.object(consumer, "_fetch_rest_snapshot", new=_fetch), \
         patch("core.ws_consumer.asyncio.sleep", new=_no_sleep):
        asyncio.run(consumer._sync_lob_snapshot())
    assert consumer._lob_synced is False


def test_ws_seed_accepts_bridging_buffer():
    """First diff straddles lastUpdateId and each later diff's pu == prev u → SYNCED (futures)."""
    import asyncio
    from core.ws_consumer import BinanceWebSocketConsumer
    consumer = BinanceWebSocketConsumer(candle_queue=asyncio.Queue())
    snap = {"bids": [["100.0", "1"]], "asks": [["101.0", "1"]], "lastUpdateId": 100}
    consumer._lob_pending = [
        {"U": 100, "u": 101, "b": [], "a": []},               # futures bridge: 100 <= 100 <= 101
        {"U": 102, "u": 103, "pu": 101, "b": [], "a": []},    # contiguous: pu == prev u (101)
    ]

    async def _fetch():
        return snap

    with patch.object(consumer, "_fetch_rest_snapshot", new=_fetch):
        asyncio.run(consumer._sync_lob_snapshot())
    assert consumer._lob_synced is True
    assert consumer._lob_update_id == 103


def test_ws_seed_failure_sets_reseed_flag():
    """Exhausted REST seed sets _seed_failed so the receive loop reconnects to reseed."""
    import asyncio, aiohttp
    from core.ws_consumer import BinanceWebSocketConsumer
    consumer = BinanceWebSocketConsumer(candle_queue=asyncio.Queue())

    async def _fail():
        raise aiohttp.ClientError("boom")
    async def _no_sleep(*_a, **_k):
        return None

    with patch.object(consumer, "_fetch_rest_snapshot", new=_fail), \
         patch("core.ws_consumer.asyncio.sleep", new=_no_sleep):
        asyncio.run(consumer._sync_lob_snapshot())
    assert consumer._lob_synced is False
    assert consumer._seed_failed is True
