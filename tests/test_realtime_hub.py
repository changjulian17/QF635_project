"""Tests for engine.realtime_hub.RealtimeHub and the /ws/lob endpoint plumbing."""
import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.realtime_hub import RealtimeHub


# ── Unit tests: hub fan-out logic with fake connections ───────────────────────

@pytest.mark.asyncio
async def test_broadcast_sends_json_to_all_connections():
    hub = RealtimeHub()
    a, b = AsyncMock(), AsyncMock()
    hub.register(a)
    hub.register(b)

    await hub.broadcast({"obi": 0.5, "ts": "t1"})

    expected = json.dumps({"obi": 0.5, "ts": "t1"})
    a.send_str.assert_awaited_once_with(expected)
    b.send_str.assert_awaited_once_with(expected)


@pytest.mark.asyncio
async def test_broadcast_with_no_subscribers_is_noop():
    hub = RealtimeHub()
    # Must not raise with zero connections
    await hub.broadcast({"obi": 0.1})
    assert hub.connection_count == 0


@pytest.mark.asyncio
async def test_failed_connection_is_dropped():
    hub = RealtimeHub()
    good = AsyncMock()
    bad = AsyncMock()
    bad.send_str.side_effect = ConnectionResetError("client gone")
    hub.register(good)
    hub.register(bad)

    await hub.broadcast({"x": 1})

    assert hub.connection_count == 1   # bad one removed
    good.send_str.assert_awaited_once()


def test_register_unregister_counts():
    hub = RealtimeHub()
    ws = object()
    hub.register(ws)
    assert hub.connection_count == 1
    hub.unregister(ws)
    assert hub.connection_count == 0
    hub.unregister(ws)  # idempotent — no error on double unregister
    assert hub.connection_count == 0


# ── Integration test: real WebSocket via the same handler shape as main.py ────

async def _build_ws_app(hub):
    """Mirror the /ws/lob handler in main._api_server."""
    async def _handle_ws_lob(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        hub.register(ws)
        try:
            async for _msg in ws:
                pass
        finally:
            hub.unregister(ws)
        return ws

    app = web.Application()
    app.router.add_get("/ws/lob", _handle_ws_lob)
    return app


async def _wait_for_count(hub, target, attempts=100):
    """Poll briefly until the hub reaches the expected connection count."""
    for _ in range(attempts):
        if hub.connection_count == target:
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_ws_endpoint_delivers_broadcast(aiohttp_client):
    hub = RealtimeHub()
    client = await aiohttp_client(await _build_ws_app(hub))
    ws = await client.ws_connect("/ws/lob")

    await _wait_for_count(hub, 1)
    assert hub.connection_count == 1

    payload = {"ts": "t1", "obi": 0.42, "bid_levels": [[100.0, 1.0]]}
    await hub.broadcast(payload)

    msg = await ws.receive()
    assert json.loads(msg.data) == payload

    await ws.close()


@pytest.mark.asyncio
async def test_ws_endpoint_unregisters_on_disconnect(aiohttp_client):
    hub = RealtimeHub()
    client = await aiohttp_client(await _build_ws_app(hub))
    ws = await client.ws_connect("/ws/lob")

    await _wait_for_count(hub, 1)
    assert hub.connection_count == 1

    await ws.close()

    # Server unregisters once it observes the close
    await _wait_for_count(hub, 0)
    assert hub.connection_count == 0
