"""
Binance user data stream — private WebSocket for executionReport events.

Manages the listenKey lifecycle (obtain, keepalive every 50 min, delete on stop)
and dispatches executionReport / outboundAccountPosition events to registered
callbacks.  Runs independently of the public market-data stream in ws_consumer.py
because it uses a different URL scheme (/ws/{listenKey}), a different keepalive
mechanism, and re-obtains a fresh listenKey on every reconnect.

Reconnect strategy mirrors ws_consumer.py:207-214: exponential backoff up to 60 s.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings

logger = logging.getLogger(__name__)

_KEEPALIVE_INTERVAL_S = 50 * 60   # refresh every 50 min (Binance TTL is 60 min)
_MAX_RECONNECT_DELAY  = 60.0

class _Fatal:
    """Sentinel returned by _obtain_listen_key() for non-retriable errors."""

_FATAL = _Fatal()


def _extract_listen_key(resp: object) -> str | None:
    """Normalize listen-key responses that may be a raw string or a dict."""
    if isinstance(resp, str):
        return resp or None
    if isinstance(resp, dict):
        value = resp.get("listenKey")
        return value or None
    return None


class UserDataStreamConsumer:
    """Manages listenKey lifecycle + dispatches executionReport / balanceUpdate."""

    def __init__(self, client) -> None:
        self._client              = client
        self._running             = False
        self._listen_key: str | None = None
        self._reconnect_delay     = 1.0
        self._execution_report_cb: Callable[[dict], Awaitable[None]] | None = None
        self._balance_update_cb:   Callable[[dict], Awaitable[None]] | None = None
        self._keepalive_failed    = asyncio.Event()

    async def start(
        self,
        execution_report_cb: Callable[[dict], Awaitable[None]] | None = None,
        balance_update_cb:   Callable[[dict], Awaitable[None]] | None = None,
    ) -> None:
        self._execution_report_cb = execution_report_cb
        self._balance_update_cb   = balance_update_cb
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False
        if self._listen_key:
            try:
                await self._client.futures_stream_close(listenKey=self._listen_key)
            except Exception as exc:
                logger.warning("[UserData] Failed to delete listenKey on stop: %s", exc)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _obtain_listen_key(self) -> "str | None | _Fatal":
        try:
            resp = await self._client.futures_stream_get_listen_key()
            key = _extract_listen_key(resp)
            if key:
                logger.info("[UserData] listenKey obtained: %s…", key[:8])
                return key
            logger.error("[UserData] listenKey response missing listenKey: %r", resp)
            return None
        except Exception as exc:
            exc_str = str(exc)
            # 410 Gone = endpoint permanently removed (e.g. testnet limitation).
            # Retrying is pointless; surface as a warning and signal the loop to stop.
            if "410" in exc_str or "Gone" in exc_str:
                logger.warning(
                    "[UserData] listenKey endpoint returned 410 Gone — "
                    "user data stream not supported in this environment; "
                    "falling back to REST polling only. Error: %s", exc
                )
                return _FATAL
            logger.error("[UserData] Failed to obtain listenKey: %s", exc)
            return None

    async def _keepalive_loop(self) -> None:
        consec_failures = 0
        while self._running:
            await asyncio.sleep(_KEEPALIVE_INTERVAL_S)
            if not self._running or not self._listen_key:
                return
            try:
                await self._client.futures_stream_keepalive(listenKey=self._listen_key)
                logger.debug("[UserData] listenKey keepalive sent")
                consec_failures = 0
            except Exception as exc:
                consec_failures += 1
                logger.warning(
                    "[UserData] listenKey keepalive failed (%d): %s", consec_failures, exc
                )
                if consec_failures >= 2:
                    logger.critical(
                        "[UserData] %d consecutive keepalive failures — forcing reconnect",
                        consec_failures,
                    )
                    self._keepalive_failed.set()

    async def _connect_loop(self) -> None:
        attempt = 0

        while self._running:
            result = await self._obtain_listen_key()
            if isinstance(result, _Fatal):
                self._running = False
                return
            self._listen_key = result
            if not self._listen_key:
                if not self._running:
                    break
                logger.info("[UserData] Retrying listenKey in %.1fs…", self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, _MAX_RECONNECT_DELAY)
                attempt += 1
                continue

            uri = f"{settings.WS_BASE}/ws/{self._listen_key}"
            try:
                logger.info("[UserData] Connecting (attempt %d): %s…", attempt + 1, uri[:60])
                async with websockets.connect(
                    uri,
                    ping_interval=10,
                    ping_timeout=15,
                    close_timeout=10,
                ) as ws:
                    logger.info("[UserData] User data stream connected")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    self._keepalive_failed.clear()
                    keepalive_task = asyncio.create_task(
                        self._keepalive_loop(), name="user_data_keepalive"
                    )
                    try:
                        await self._receive_loop(ws)
                    finally:
                        keepalive_task.cancel()
                        try:
                            await keepalive_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("[UserData] Connection closed: %s", exc)
            except Exception as exc:
                logger.error("[UserData] Unexpected error: %s", exc, exc_info=True)

            if not self._running:
                break

            attempt += 1
            logger.info("[UserData] Reconnecting in %.1fs…", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, _MAX_RECONNECT_DELAY)

    async def _receive_loop(self, ws) -> None:
        while True:
            if not self._running:
                break
            if self._keepalive_failed.is_set():
                logger.warning("[UserData] Keepalive failed — forcing reconnect to renew listenKey")
                break

            try:
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=90.0)
            except asyncio.TimeoutError:
                logger.warning("[UserData] No message for 90 s — forcing reconnect")
                break

            try:
                msg        = json.loads(raw_msg)
                event_type = msg.get("e")

                if event_type == "ORDER_TRADE_UPDATE":
                    if self._execution_report_cb is not None:
                        t = asyncio.create_task(
                            self._execution_report_cb(msg),
                            name="user_data_exec_report",
                        )
                        t.add_done_callback(
                            lambda t: logger.error(
                                "[UserData] ORDER_TRADE_UPDATE callback crashed: %s", t.exception()
                            ) if not t.cancelled() and t.exception() is not None else None
                        )

                elif event_type == "ACCOUNT_UPDATE":
                    if self._balance_update_cb is not None:
                        t = asyncio.create_task(
                            self._balance_update_cb(msg),
                            name="user_data_balance_update",
                        )
                        t.add_done_callback(
                            lambda t: logger.error(
                                "[UserData] ACCOUNT_UPDATE callback crashed: %s", t.exception()
                            ) if not t.cancelled() and t.exception() is not None else None
                        )

            except (ValueError, json.JSONDecodeError) as exc:
                logger.warning("[UserData] Malformed message: %s", exc)
