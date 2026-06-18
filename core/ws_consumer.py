import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any, List, Optional

import aiohttp
import numpy as np
import websockets

from config import settings
from core.lob_sync import SeedDiscontinuity, is_contiguous, seed_bridge_ok
from models import AggTrade, LOBLevel, LOBSnapshot, SharedState

logger = logging.getLogger(__name__)

# Max connection duration before forced reconnect (24h)
_MAX_CONNECTION_SECONDS = 24 * 3600
_SEED_MAX_ATTEMPTS      = 3        # REST seed retries before staying UNSYNCED


class HeartbeatMonitor:
    """
    Monitors WebSocket latency by comparing event_time_ms (E) to local time.
    
    Status transitions (rate computed over rolling 10-message window):
      HEALTHY -> DEGRADED when >=50% of window exceeds WARN_MS (single log on entry)
      DEGRADED -> SUSTAINED_DEGRADED after HEARTBEAT_SUSTAINED_MS elapsed at >=50% rate
      any -> HEALTHY when <30% of window exceeds WARN_MS (hysteresis, single log on exit)
    """

    def __init__(self, warn_ms: int = None, critical_ms: int | None = None, consec_limit: int = None) -> None:
        self.WARN_MS = warn_ms if warn_ms is not None else settings.HEARTBEAT_WARN_MS
        self.CONSEC_LIMIT = consec_limit if consec_limit is not None else settings.HEARTBEAT_CONSEC_LIMIT
        self.CRITICAL_MS = (
            critical_ms if critical_ms is not None else settings.HEARTBEAT_CRITICAL_MS
        )
        self._deltas: deque[float] = deque(maxlen=10)
        self._critical_count: int = 0
        self._degraded_since_ms: float | None = None
        self.status: str = "HEALTHY"
        self.last_delta_ms: float = 0.0
        self._last_critical_log_ts: float = 0.0

    @property
    def avg_delta_ms(self) -> float:
        if not self._deltas:
            return 0.0
        return sum(self._deltas) / len(self._deltas)

    def record(self, event_time_ms: int | None) -> str:
        """Update latency stats and return current status."""
        if not event_time_ms:
            return self.status

        now_ms = time.time() * 1000
        delta_ms = now_ms - event_time_ms
        self.last_delta_ms = delta_ms
        self._deltas.append(delta_ms)

        # -- CRITICAL: consecutive check --
        if delta_ms > self.CRITICAL_MS:
            self._critical_count += 1
            if self._critical_count >= self.CONSEC_LIMIT:
                if time.time() - self._last_critical_log_ts > 10:
                    logger.critical(
                        "[Heartbeat] CRITICAL: %d consecutive >%dms. Last=%.0fms",
                        self._critical_count,
                        self.CRITICAL_MS,
                        delta_ms,
                    )
                    self._last_critical_log_ts = time.time()
                self.status = "CRITICAL"
                return self.status
        else:
            self._critical_count = 0

        # -- DEGRADED: rate-based check --
        degraded_count = sum(1 for d in self._deltas if d > self.WARN_MS)
        degraded_rate = degraded_count / len(self._deltas) if len(self._deltas) > 0 else 0

        if self.status == "HEALTHY":
            # Only enter DEGRADED if we have a full window or meet the rate threshold
            if len(self._deltas) == self._deltas.maxlen and degraded_rate >= settings.HEARTBEAT_DEGRADED_RATE_THRESH:
                self.status = "DEGRADED"
                self._degraded_since_ms = now_ms
                logger.warning(
                    "[Heartbeat] DEGRADED: %d/10 messages >%dms avg=%.0fms",
                    degraded_count,
                    self.WARN_MS,
                    self.avg_delta_ms,
                )
        elif self.status == "DEGRADED":
            if (
                now_ms - (self._degraded_since_ms or now_ms)
                > settings.HEARTBEAT_SUSTAINED_MS
            ):
                self.status = "SUSTAINED_DEGRADED"
                logger.warning(
                    "[Heartbeat] SUSTAINED_DEGRADED: >%ds above %.0f%% degraded rate",
                    settings.HEARTBEAT_SUSTAINED_MS // 1000,
                    settings.HEARTBEAT_DEGRADED_RATE_THRESH * 100,
                )
            elif (
                degraded_rate < settings.HEARTBEAT_DEGRADED_RECOVERY_THRESH
            ):
                self._recover("DEGRADED")
        elif self.status == "SUSTAINED_DEGRADED":
            if degraded_rate < settings.HEARTBEAT_DEGRADED_RECOVERY_THRESH:
                self._recover("SUSTAINED_DEGRADED")
        elif self.status == "CRITICAL":
            # Exit CRITICAL if we see even one fast message
            if delta_ms < self.WARN_MS:
                self._recover("CRITICAL")

        return self.status

    def _recover(self, from_status: str) -> None:
        logger.info(
            "[Heartbeat] RECOVERED: status normal from %s avg=%.0fms",
            from_status, self.avg_delta_ms
        )
        self.status = "HEALTHY"
        self._degraded_since_ms = None


class BinanceWebSocketConsumer:
    """
    Manages a connection to multiple Binance streams.
    Reconstructs LOB if a depth queue is provided.
    Monitors heartbeat/latency.
    """
    def __init__(
        self,
        streams: list[str] = None,
        shared_state: SharedState = None,
        trade_queue: asyncio.Queue | None = None,
        candle_queue: asyncio.Queue | None = None,
        candle_db_queue: asyncio.Queue | None = None,
        depth_queue: asyncio.Queue | None = None,
        heartbeat_cb: Callable[[str, float], Awaitable[None]] | None = None,
        heartbeat_key: str = "heartbeat_status",
        warn_ms: int | None = None,
        critical_ms: int | None = None,
        consec_limit: int | None = None,
    ) -> None:
        self.streams          = streams or []
        self.heartbeat_key    = heartbeat_key
        self.heartbeat        = HeartbeatMonitor(
            warn_ms=warn_ms if warn_ms is not None else settings.HEARTBEAT_WARN_MS, 
            critical_ms=critical_ms if critical_ms is not None else settings.HEARTBEAT_CRITICAL_MS, 
            consec_limit=consec_limit if consec_limit is not None else settings.HEARTBEAT_CONSEC_LIMIT
        )
        self.heartbeat_cb     = heartbeat_cb
        self._shared_state    = shared_state or SharedState()
        self._trade_queue     = trade_queue
        self._candle_queue    = candle_queue
        self._candle_db_queue = candle_db_queue
        self._depth_queue     = depth_queue
        self._running         = False

        # LOB Reconstruction State
        self._bid_book: dict[float, float] = {}
        self._ask_book: dict[float, float] = {}
        self._lob_update_id: int = 0
        self._lob_synced = False
        self._seed_failed:    bool = False
        self._lob_pending: list[dict] = []
        self._consecutive_lob_gaps: int = 0

    async def start(self) -> None:
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False

    async def _connect_loop(self) -> None:
        attempt = 0
        while self._running:
            uri = f"{settings.WS_BASE}/stream?streams={'/'.join(self.streams)}"
            try:
                logger.info("[WS] Connecting (attempt %d): %s", attempt + 1, uri)
                async with websockets.connect(
                    uri,
                    ping_interval=settings.WS_PING_INTERVAL_S,
                    ping_timeout=settings.WS_PING_TIMEOUT_S,
                    close_timeout=0.1,
                ) as ws:
                    logger.info("[WS] Connected")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    if self._depth_queue is not None:
                        self._shared_state.lob_status = "UNINITIALISED"

                    # Reset local book and start async REST seed concurrently
                    # with the receive loop so diffs are buffered during the fetch.
                    self._bid_book.clear()
                    self._ask_book.clear()
                    self._lob_update_id       = 0
                    self._lob_synced          = False
                    self._seed_failed   = False
                    self._lob_pending         = []
                    self._consecutive_lob_gaps = 0

                    sync_task = None
                    if self._depth_queue is not None:
                        sync_task = asyncio.create_task(
                            self._sync_lob_snapshot(), name="ws_lob_sync"
                        )

                    try:
                        await self._receive_loop(ws)
                    finally:
                        if sync_task is not None:
                            sync_task.cancel()

            except (ConnectionRefusedError, OSError, websockets.exceptions.WebSocketException) as exc:
                if self._depth_queue is not None:
                    self._shared_state.lob_status = "DISCONNECTED"

                delay = min(2 ** attempt, 60)
                logger.error("[WS] Connection failed: %s. Retrying in %ds...", exc, delay)
                await asyncio.sleep(delay)
                attempt += 1

    async def _receive_loop(self, ws: Any) -> None:
        conn_start = time.monotonic()

        while self._running:
            if time.monotonic() - conn_start > _MAX_CONNECTION_SECONDS:
                logger.info("[WS] Approaching 24 h limit — reconnecting proactively.")
                await ws.close()
                break

            try:
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=settings.WS_RECV_TIMEOUT_S)
            except asyncio.TimeoutError:
                logger.warning(
                    "[WS][%s] No message for %ds — forcing reconnect",
                    self.heartbeat_key,
                    settings.WS_RECV_TIMEOUT_S
                )
                break
            except websockets.exceptions.ConnectionClosed:
                logger.warning("[WS] Connection closed by server")
                break

            try:
                data = json.loads(raw_msg)
                if "stream" in data and "data" in data:
                    stream = data["stream"]
                    msg = data["data"]
                else:
                    stream = ""
                    msg = data
                await self._dispatch(stream, msg)
            except json.JSONDecodeError as exc:
                logger.error("[WS] Failed to decode JSON: %s", exc)
            
            # Check for heartbeat-driven reconnect
            if self.heartbeat.status == "CRITICAL":
                break

    async def _dispatch(self, stream: str, msg: dict) -> None:
        # Update heartbeat
        status = self.heartbeat.record(msg.get("E"))
        setattr(self._shared_state, self.heartbeat_key, status)
        self._shared_state.last_delta_ms = self.heartbeat.last_delta_ms

        if self.heartbeat_cb:
            await self.heartbeat_cb(status, self.heartbeat.last_delta_ms)

        if self.heartbeat.status == "CRITICAL":
            # Exit to trigger reconnect
            return

        # Route message
        event_type = msg.get("e")

        if "depth" in stream or event_type == "depthUpdate":
            if not self._lob_synced:
                self._lob_pending.append(msg)
                if len(self._lob_pending) > 500:
                    self._lob_pending.pop(0)
            else:
                if self._apply_depth_diff(msg):
                    self._consecutive_lob_gaps = 0
                    self._enqueue_latest_depth_snapshot(msg.get("E", int(time.time() * 1000)))
                else:
                    self._consecutive_lob_gaps += 1
                    if self._consecutive_lob_gaps >= settings.LOB_GAP_RECONNECT_MIN_CONSECUTIVE:
                        logger.critical(
                            "[WS] Forcing reconnect after %d consecutive LOB gaps.",
                            self._consecutive_lob_gaps,
                        )
                        self._consecutive_lob_gaps = 0
                        self.heartbeat.status = "CRITICAL"
                    else:
                        logger.warning(
                            "[WS] LOB gap %d/%d — skipping diff.",
                            self._consecutive_lob_gaps,
                            settings.LOB_GAP_RECONNECT_MIN_CONSECUTIVE,
                        )

        elif event_type == "kline":
            if self._candle_queue is not None:
                await self._candle_queue.put(msg)
            if self._candle_db_queue is not None:
                await self._candle_db_queue.put(msg)

        elif event_type == "aggTrade":
            if self._trade_queue is not None:
                await self._trade_queue.put(msg)

        elif event_type == "bookTicker":
            # Best bid/ask for spread calculation; route alongside trades
            if self._trade_queue is not None:
                await self._trade_queue.put(msg)

    def _apply_depth_diff(self, diff: dict) -> bool:
        """
        Update local book with incremental diffs.
        Returns True if successful, False if a gap was detected.
        """
        # 1. verify U <= last_update_id + 1 <= u
        first_id = int(diff.get("U", 0))
        last_id  = int(diff.get("u", 0))

        if not last_id:
            return True

        if self._lob_update_id > 0 and first_id > 0 and first_id > self._lob_update_id + 1:
            gap = first_id - (self._lob_update_id + 1)
            if gap < settings.LOB_GAP_TOLERANCE_UPDATEIDS:
                logger.debug("[WS] LOB small gap (%d IDs) — applying and advancing", gap)
                # fall through — diff is applied below, _lob_update_id advances normally
            else:
                logger.warning("[WS] LOB GAP DETECTED: expected %d, got %d (gap=%d)",
                               self._lob_update_id + 1, first_id, gap)
                return False

        if last_id <= self._lob_update_id:
            return True

        for side, book in [("b", self._bid_book), ("a", self._ask_book)]:
            for price_str, qty_str in diff.get(side, []):
                price, qty = float(price_str), float(qty_str)
                if qty == 0:
                    book.pop(price, None)
                else:
                    book[price] = qty
        
        self._lob_update_id = last_id
        return True

    def _enqueue_latest_depth_snapshot(self, event_ms: int) -> None:
        if self._depth_queue is not None:
            # If queue is full, drop the oldest one to make room for the latest
            if self._depth_queue.full():
                try:
                    self._depth_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            
            msg = self._reconstruct_depth_msg(event_ms)
            try:
                self._depth_queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass

    def _reconstruct_depth_msg(self, event_ms: int) -> dict:
        """Create a full snapshot dict from local book."""
        bids = sorted([[str(p), str(q)] for p, q in self._bid_book.items()], 
                      key=lambda x: float(x[0]), reverse=True)
        asks = sorted([[str(p), str(q)] for p, q in self._ask_book.items()], 
                      key=lambda x: float(x[0]))
        
        return {
            "lastUpdateId": self._lob_update_id,
            "bids": bids[:1000],
            "asks": asks[:1000],
            "E": event_ms
        }

    async def _sync_lob_snapshot(self) -> None:
        """Fetch full snapshot from REST and align with buffered diffs."""
        url = f"{settings.REST_BASE}/fapi/v1/depth?symbol={settings.SYMBOL}&limit=1000"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    snap = await resp.json()
            
            self._bid_book = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
            self._ask_book = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
            last_uid = int(snap["lastUpdateId"])

            # Process buffered diffs; stop on first internal gap to avoid
            # leaving _lob_update_id below the gap and triggering an immediate
            # reconnect on the next live diff.
            for diff in self._lob_pending:
                u_last = int(diff.get("u", 0))
                if u_last <= last_uid:
                    continue
                if not self._apply_depth_diff(diff):
                    break
            
            self._lob_update_id = max(self._lob_update_id, last_uid)
            logger.info(
                "[WS] LOB seeded at updateId=%d  bids=%d  asks=%d",
                last_uid, len(self._bid_book), len(self._ask_book),
            )
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.error("[WS] LOB REST seed failed (%s) — continuing with buffered diffs.", exc)
        finally:
            self._lob_pending.clear()
            self._lob_synced = True
            if self._bid_book and self._ask_book:
                self._enqueue_latest_depth_snapshot(int(time.time() * 1000))
