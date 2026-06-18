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
from core.lob_sync import SeedDiscontinuity, futures_is_contiguous, futures_seed_bridge_ok
from models import AggTrade, Candle, SharedState

logger = logging.getLogger(__name__)

_MAX_CONNECTION_SECONDS = 86_000  # reconnect 400 s before Binance's 24 h limit
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
        self._reconnect_delay = 1.0
        self._max_delay       = 60.0
        self.heartbeat        = HeartbeatMonitor()
        self._frame_counts: dict[str, int] = {}
        self._frame_log_ts: float = 0.0

        # Local order book for diff-depth reconstruction
        self._bid_book:       dict[float, float] = {}
        self._ask_book:       dict[float, float] = {}
        self._lob_update_id:  int  = 0
        self._lob_synced:     bool = False
        self._seed_failed:    bool = False
        self._lob_pending:    list[dict] = []

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
                    self._lob_update_id = 0
                    self._lob_synced    = False
                    self._seed_failed   = False
                    self._lob_pending   = []
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

        while True:
            if not self._running:
                break

            if self._seed_failed:   # final seed attempt failed → reconnect to reseed
                logger.warning("[WS] seed failed — reconnecting to reseed")
                await ws.close()
                break

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

    async def _fetch_rest_snapshot(self) -> dict:
        """Fetch a REST depth snapshot (extracted for testability)."""
        url = f"{settings.REST_BASE}/fapi/v1/depth"
        params = {"symbol": settings.SYMBOL.upper(), "limit": 1000}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _sync_lob_snapshot(self) -> None:
        """Seed the local book from a REST snapshot, retrying on failure.

        Only sets _lob_synced=True after a successful seed, so Gate 0 never sees
        SYNCED on a partial book reconstructed from diffs alone.
        """
        for attempt in range(1, _SEED_MAX_ATTEMPTS + 1):
            try:
                snap = await self._fetch_rest_snapshot()

                self._bid_book = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
                self._ask_book = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
                last_uid = int(snap["lastUpdateId"])
                # Apply buffered diffs only if they form a gapless bridge from the
                # snapshot. USD-M Futures rules: first event must straddle lastUpdateId
                # (U <= lastUpdateId <= u); every subsequent event's pu must equal the
                # previous event's u.
                prev_u, first = last_uid, True
                for event in self._lob_pending:
                    u = int(event.get("u", 0))
                    if u <= last_uid:
                        continue
                    U = int(event.get("U", 0))
                    if first:
                        if not futures_seed_bridge_ok(U, u, last_uid):
                            raise SeedDiscontinuity(f"bridge fail U={U} u={u} lastUpdateId={last_uid}")
                        first = False
                    else:
                        pu = int(event.get("pu", 0))
                        if not futures_is_contiguous(prev_u, pu):
                            raise SeedDiscontinuity(f"gap pu={pu} != prev_u={prev_u}")
                    self._apply_depth_diff(event)
                    prev_u = u
                self._lob_pending.clear()
                self._lob_update_id = prev_u
                self._lob_synced = True
                logger.info(
                    "[WS] LOB seeded at updateId=%d  bids=%d  asks=%d",
                    prev_u, len(self._bid_book), len(self._ask_book),
                )
                return
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError,
                    KeyError, ValueError, SeedDiscontinuity) as exc:
                logger.error(
                    "[WS] LOB seed attempt %d/%d failed: %s", attempt, _SEED_MAX_ATTEMPTS, exc,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 5))

        # All attempts failed — flag for reconnect/reseed; stay unsynced meanwhile
        # so Gate 0 won't trade on a partial book.
        self._lob_pending.clear()
        self._lob_synced = False
        self._seed_failed = True
        logger.critical(
            "[WS] LOB seed failed after %d attempts — reconnecting to reseed (Gate 0 blocks meanwhile)",
            _SEED_MAX_ATTEMPTS,
        )

    def _apply_depth_diff(self, event: dict) -> None:
        for p, q in event.get("b", []):
            price, qty = float(p), float(q)
            if qty == 0.0:
                self._bid_book.pop(price, None)
            else:
                self._bid_book[price] = qty
        for p, q in event.get("a", []):
            price, qty = float(p), float(q)
            if qty == 0.0:
                self._ask_book.pop(price, None)
            else:
                self._ask_book[price] = qty
        self._lob_update_id = int(event.get("u", self._lob_update_id))

    def _reconstruct_depth_msg(self, event_ms: int) -> dict:
        """
        Build a full-snapshot-style dict from the local book (top _DEPTH_LEVELS
        per side) so downstream consumers (LOBEngine, MicrostructureDetector)
        receive the same message format as the old depth20 stream, but with
        100 levels instead of 20.
        """
        bids = sorted(self._bid_book.items(), reverse=True)[: self._DEPTH_LEVELS]
        asks = sorted(self._ask_book.items())[: self._DEPTH_LEVELS]
        return {
            "lastUpdateId": self._lob_update_id,
            "E": event_ms,
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        }

    async def _dispatch(self, stream: str, msg: dict) -> None:
        event_type = msg.get("e")

        if event_type == "aggTrade":
            trade = AggTrade(
                timestamp=datetime.fromtimestamp(msg["T"] / 1000, tz=timezone.utc),
                price=float(msg["p"]),
                qty=float(msg["q"]),
                is_buyer_maker=bool(msg["m"]),
            )
            if self._trade_queue is not None:
                await self._trade_queue.put(trade)

        elif event_type == "kline":
            kline = msg["k"]
            if kline.get("x"):   # closed candle only
                candle = Candle(
                    open_time=datetime.fromtimestamp(kline["t"] / 1000, tz=timezone.utc),
                    open=float(kline["o"]),
                    high=float(kline["h"]),
                    low=float(kline["l"]),
                    close=float(kline["c"]),
                    volume=float(kline["v"]),
                    is_closed=True,
                )
                await self._candle_queue.put(candle)
                if self._candle_db_queue is not None:
                    await self._candle_db_queue.put(candle)
                logger.info(
                    "[WS] Candle close — O=%.2f H=%.2f L=%.2f C=%.2f V=%.3f",
                    candle.open, candle.high, candle.low, candle.close, candle.volume,
                )

        elif event_type == "depthUpdate":
            # Incremental diff — buffer during REST seed, then apply and reconstruct.
            event_ms = int(msg.get("E") or time.time() * 1000)
            if not self._lob_synced:
                if len(self._lob_pending) < self._MAX_PENDING_DIFFS:
                    self._lob_pending.append(msg)
            else:
                self._apply_depth_diff(msg)
                if self._depth_queue is not None:
                    await self._depth_queue.put(self._reconstruct_depth_msg(event_ms))

        elif event_type == "bookTicker":
            # Best bid/ask for spread calculation; route alongside trades
            if self._trade_queue is not None:
                await self._trade_queue.put(msg)
