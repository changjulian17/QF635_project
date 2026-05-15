import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone

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
    # v3.0 stream set: snapshot depth (not diff), 5m candles, bookTicker for spread
    STREAMS = [
        f"{settings.SYMBOL.lower()}@aggTrade",
        f"{settings.SYMBOL.lower()}@kline_{settings.TIMEFRAME}",
        f"{settings.SYMBOL.lower()}@depth20@100ms",
        f"{settings.SYMBOL.lower()}@bookTicker",
    ]

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        candle_db_queue: asyncio.Queue | None = None,
        trade_queue: asyncio.Queue | None = None,
        depth_queue: asyncio.Queue | None = None,
        shared_state: SharedState | None = None,
    ) -> None:
        self._candle_queue    = candle_queue
        self._candle_db_queue = candle_db_queue
        self._trade_queue     = trade_queue
        self._depth_queue     = depth_queue
        self._shared_state    = shared_state or SharedState()
        self._running         = False
        self._reconnect_delay = 1.0
        self._max_delay       = 60.0
        self.heartbeat        = HeartbeatMonitor()

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
                    logger.info("[WS] Connected.")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    self._shared_state.lob_status = "UNINITIALISED"
                    await self._receive_loop(ws)

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

        async for raw_msg in ws:
            if not self._running:
                break

            if asyncio.get_event_loop().time() - conn_start > _MAX_CONNECTION_SECONDS:
                logger.info("[WS] Approaching 24 h limit — reconnecting proactively.")
                await ws.close()
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

                await self._dispatch(stream, msg)

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("[WS] Malformed message: %s", exc)

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

        elif "depth20" in stream:
            # depth20@100ms delivers full snapshots (not diffs)
            # msg format: {"lastUpdateId": int, "bids": [[price, qty]...], "asks": [...]}
            if self._depth_queue is not None:
                await self._depth_queue.put(msg)

        elif event_type == "bookTicker":
            # Best bid/ask for spread calculation; route alongside trades
            if self._trade_queue is not None:
                await self._trade_queue.put(msg)
