import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import aiohttp
import numpy as np
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings
from models import AggTrade, Candle, SharedState

logger = logging.getLogger(__name__)

_MAX_CONNECTION_SECONDS = 86_000  # reconnect 400 s before Binance's 24 h limit


class HeartbeatMonitor:
    """
    Tracks delta between Binance event time (field E) and local system time
    on every WebSocket message — never sampled, never batched (Rule 11).

    Testnet note: 150–400ms latency is normal on testnet. WARN will trigger
    frequently — this is expected. CRITICAL (500ms × 3 consecutive) is the
    genuine killswitch condition.
    """
    WARN_MS      = settings.HEARTBEAT_WARN_MS
    CRITICAL_MS  = settings.HEARTBEAT_CRITICAL_MS
    CONSEC_LIMIT = settings.HEARTBEAT_CONSEC_LIMIT

    def __init__(self) -> None:
        self._deltas: deque[float] = deque(maxlen=10)
        self._critical_count: int  = 0
        self.status: str           = "HEALTHY"
        self.last_delta_ms: float  = 0.0

    def record(self, event_time_ms: int) -> str:
        delta_ms           = (time.time() * 1000) - event_time_ms
        self.last_delta_ms = delta_ms
        self._deltas.append(delta_ms)

        if delta_ms > self.CRITICAL_MS:
            self._critical_count += 1
        else:
            self._critical_count = 0   # reset on any healthy packet

        if self._critical_count >= self.CONSEC_LIMIT:
            self.status = "CRITICAL"
            logger.critical(
                "[Heartbeat] CRITICAL: %d consecutive >%dms packets. Last=%.0fms",
                self._critical_count, self.CRITICAL_MS, delta_ms,
            )
        elif delta_ms > self.WARN_MS:
            self.status = "DEGRADED"
            logger.warning("[Heartbeat] DEGRADED: delta=%.0fms", delta_ms)
        else:
            self.status = "HEALTHY"

        return self.status

    @property
    def avg_delta_ms(self) -> float:
        return float(np.mean(self._deltas)) if self._deltas else 0.0

    @property
    def max_delta_ms(self) -> float:
        return float(max(self._deltas)) if self._deltas else 0.0


class BinanceWebSocketConsumer:
    # Incremental diff-depth stream: 100 levels per side vs the old 20-level snapshot.
    # At BTC prices ~$77k, 100 levels spans ~$50–200 from mid — sufficient range for
    # deep-book institutional wall detection above the transaction cost floor.
    _DEPTH_LEVELS = 100
    _MAX_PENDING_DIFFS = 500   # cap pending diff buffer during REST snapshot fetch

    STREAMS = [
        f"{settings.SYMBOL.lower()}@aggTrade",
        f"{settings.SYMBOL.lower()}@kline_{settings.TIMEFRAME}",
        f"{settings.SYMBOL.lower()}@depth@100ms",   # incremental diff — seeded by REST snapshot
        f"{settings.SYMBOL.lower()}@bookTicker",
    ]

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        candle_db_queue: asyncio.Queue | None = None,
        trade_queue: asyncio.Queue | None = None,
        depth_queue: asyncio.Queue | None = None,
        shared_state: SharedState | None = None,
        heartbeat_cb: Callable[[str, float], Awaitable[None]] | None = None,
    ) -> None:
        self._candle_queue    = candle_queue
        self._candle_db_queue = candle_db_queue
        self._trade_queue     = trade_queue
        self._depth_queue     = depth_queue
        self._shared_state    = shared_state or SharedState()
        self._heartbeat_cb    = heartbeat_cb
        self._running         = False
        self._reconnect_delay = 1.0
        self._max_delay       = 60.0
        self.heartbeat        = HeartbeatMonitor()

        # Local order book for diff-depth reconstruction
        self._bid_book:       dict[float, float] = {}
        self._ask_book:       dict[float, float] = {}
        self._lob_update_id:  int  = 0
        self._lob_synced:     bool = False
        self._lob_pending:    list[dict] = []

    async def start(self) -> None:
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False

    async def _connect_loop(self) -> None:
        stream_path = "/".join(self.STREAMS)
        uri = f"{settings.WS_BASE}/stream?streams={stream_path}"
        attempt = 0

        while self._running:
            try:
                logger.info("[WS] Connecting (attempt %d): %s", attempt + 1, uri)
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info("[WS] Connected — seeding LOB from REST snapshot…")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    self._shared_state.lob_status = "UNINITIALISED"

                    # Reset local book and start async REST seed concurrently
                    # with the receive loop so diffs are buffered during the fetch.
                    self._bid_book.clear()
                    self._ask_book.clear()
                    self._lob_update_id = 0
                    self._lob_synced    = False
                    self._lob_pending   = []
                    sync_task = asyncio.create_task(
                        self._sync_lob_snapshot(), name="ws_lob_sync"
                    )
                    try:
                        await self._receive_loop(ws)
                    finally:
                        sync_task.cancel()
                        try:
                            await sync_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("[WS] Connection closed: %s", exc)
            except Exception as exc:
                logger.error("[WS] Unexpected error: %s", exc, exc_info=True)

            if not self._running:
                break

            self._shared_state.heartbeat_status = "CRITICAL"
            self._shared_state.lob_status       = "DISCONNECTED"
            attempt += 1
            logger.info("[WS] Reconnecting in %.1fs…", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_delay)

    async def _receive_loop(self, ws) -> None:
        conn_start = asyncio.get_event_loop().time()

        while True:
            if not self._running:
                break

            if asyncio.get_event_loop().time() - conn_start > _MAX_CONNECTION_SECONDS:
                logger.info("[WS] Approaching 24 h limit — reconnecting proactively.")
                await ws.close()
                break

            try:
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[WS] No message for 30s — forcing reconnect")
                break

            try:
                outer      = json.loads(raw_msg)
                stream     = outer.get("stream", "")
                msg        = outer.get("data", outer)

                # Heartbeat — called on EVERY message (Rule 11)
                event_ms = msg.get("E", int(time.time() * 1000))
                status   = self.heartbeat.record(event_ms)
                self._shared_state.heartbeat_status = status
                self._shared_state.last_delta_ms    = self.heartbeat.last_delta_ms
                if self._heartbeat_cb is not None:
                    _task = asyncio.create_task(
                        self._heartbeat_cb(status, self.heartbeat.last_delta_ms),
                        name="heartbeat_cb",
                    )
                    _task.add_done_callback(
                        lambda t: logger.critical("[WS] heartbeat_cb crashed: %s", t.exception())
                        if not t.cancelled() and t.exception() is not None else None
                    )

                await self._dispatch(stream, msg)

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("[WS] Malformed message: %s", exc)

    async def _sync_lob_snapshot(self) -> None:
        """
        Fetch a REST depth snapshot to seed the local book, then apply any
        diffs that arrived during the fetch. Mirrors the LOBRecorder pattern.
        Falls back to marking synced anyway so data continues to flow on failure.
        """
        url = f"{settings.REST_BASE}/api/v3/depth"
        params = {"symbol": settings.SYMBOL.upper(), "limit": 1000}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    resp.raise_for_status()
                    snap = await resp.json()

            self._bid_book = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
            self._ask_book = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
            last_uid = int(snap["lastUpdateId"])

            for event in self._lob_pending:
                if int(event.get("u", 0)) <= last_uid:
                    continue
                self._apply_depth_diff(event)

            self._lob_update_id = last_uid
            logger.info(
                "[WS] LOB seeded at updateId=%d  bids=%d  asks=%d",
                last_uid, len(self._bid_book), len(self._ask_book),
            )
        except Exception as exc:
            logger.error("[WS] LOB REST seed failed (%s) — continuing with buffered diffs.", exc)
        finally:
            self._lob_pending.clear()
            self._lob_synced = True

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
