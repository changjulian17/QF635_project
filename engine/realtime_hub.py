"""
Real-time broadcast hub — pushes JSON payloads to all subscribed WebSocket clients.

Used by the LOB snapshot writer to stream live metrics to the dashboard over the
``/ws/lob`` endpoint (see ``main.py``). Kept separate from the engine so it can be
unit-tested without a running event loop or aiohttp server.

The hub holds zero or more ``web.WebSocketResponse`` connections. ``broadcast()``
serialises the payload once and sends it to every connection; any connection that
fails to receive is dropped. With no subscribers, ``broadcast()`` is a cheap no-op,
so the engine runs normally whether or not the dashboard is open.
"""
import json
import logging

logger = logging.getLogger(__name__)


class RealtimeHub:
    """Fan-out hub for pushing JSON payloads to subscribed WebSocket clients."""

    def __init__(self) -> None:
        self._conns: set = set()

    def register(self, ws) -> None:
        """Add a WebSocket connection to the broadcast set."""
        self._conns.add(ws)

    def unregister(self, ws) -> None:
        """Remove a WebSocket connection (idempotent)."""
        self._conns.discard(ws)

    @property
    def connection_count(self) -> int:
        return len(self._conns)

    async def broadcast(self, payload: dict) -> None:
        """Send ``payload`` as JSON to every connection; drop any that fail."""
        if not self._conns:
            return
        message = json.dumps(payload)
        dead = []
        for ws in self._conns:
            try:
                await ws.send_str(message)
            except Exception:
                logger.debug("[Hub] Dropping dead WebSocket connection")
                dead.append(ws)
        for ws in dead:
            self._conns.discard(ws)
