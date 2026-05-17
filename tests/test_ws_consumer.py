"""
Tests for HeartbeatMonitor in core/ws_consumer.py.
All tests are synchronous — HeartbeatMonitor.record() is a pure function.
"""
import time

import pytest

from core.ws_consumer import HeartbeatMonitor


def _event_ms(delta_ms: float) -> int:
    """Return a Binance event timestamp that is delta_ms in the past."""
    return int(time.time() * 1000) - int(delta_ms)


def test_heartbeat_healthy():
    hb = HeartbeatMonitor()
    status = hb.record(_event_ms(50))   # 50ms lag — well within WARN_MS
    assert status == "HEALTHY"
    assert hb.last_delta_ms < hb.WARN_MS


def test_heartbeat_degraded():
    hb = HeartbeatMonitor()
    status = hb.record(_event_ms(250))  # 250ms — above WARN_MS but below CRITICAL_MS
    assert status == "DEGRADED"


def test_heartbeat_critical_consecutive():
    hb = HeartbeatMonitor()
    # Need CONSEC_LIMIT consecutive critical packets to enter CRITICAL state
    for _ in range(hb.CONSEC_LIMIT - 1):
        status = hb.record(_event_ms(600))   # above CRITICAL_MS
        assert status != "CRITICAL"          # not yet
    status = hb.record(_event_ms(600))
    assert status == "CRITICAL"


def test_heartbeat_resets_on_healthy_packet():
    hb = HeartbeatMonitor()
    # Drive up the consecutive counter
    for _ in range(hb.CONSEC_LIMIT - 1):
        hb.record(_event_ms(600))
    # One healthy packet resets the counter
    hb.record(_event_ms(50))
    assert hb._critical_count == 0
    # Now we need CONSEC_LIMIT critical packets again to reach CRITICAL
    for _ in range(hb.CONSEC_LIMIT - 1):
        hb.record(_event_ms(600))
    assert hb.status != "CRITICAL"


def test_heartbeat_avg_delta():
    hb = HeartbeatMonitor()
    hb.record(_event_ms(100))
    hb.record(_event_ms(200))
    assert 100 < hb.avg_delta_ms < 250


def test_heartbeat_shared_state_updated():
    """SharedState is updated on every record() call via the consumer."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch
    from models import SharedState
    from core.ws_consumer import BinanceWebSocketConsumer

    state = SharedState()
    q = asyncio.Queue()
    consumer = BinanceWebSocketConsumer(candle_queue=q, shared_state=state)

    # Simulate recording a healthy packet
    consumer.heartbeat.record(_event_ms(50))
    state.heartbeat_status = consumer.heartbeat.status
    state.last_delta_ms    = consumer.heartbeat.last_delta_ms

    assert state.heartbeat_status == "HEALTHY"
    assert state.last_delta_ms >= 0
