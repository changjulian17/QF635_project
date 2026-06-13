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

    Status transitions (rate computed over rolling 10-message window):
      HEALTHY → DEGRADED when ≥50% of window exceeds WARN_MS (single log on entry)
      DEGRADED → SUSTAINED_DEGRADED after HEARTBEAT_SUSTAINED_MS elapsed at ≥50% rate
      any → HEALTHY when <30% of window exceeds WARN_MS (hysteresis, single log on exit)
      any → CRITICAL after CONSEC_LIMIT consecutive packets exceed CRITICAL_MS
    """
    WARN_MS      = settings.HEARTBEAT_WARN_MS
    CONSEC_LIMIT = settings.HEARTBEAT_CONSEC_LIMIT

    def __init__(self, critical_ms: int | None = None) -> None:
        self.CRITICAL_MS              = critical_ms if critical_ms is not None else settings.HEARTBEAT_CRITICAL_MS
        self._deltas: deque[float]    = deque(maxlen=10)
        self._critical_count: int     = 0
        self._degraded_since_ms: float | None = None
        self.status: str              = "HEALTHY"
        self.last_delta_ms: float     = 0.0
        self._last_critical_log_ts: float = 0.0

    def record(self, event_time_ms: int) -> str:
        delta_ms           = (time.time() * 1000) - event_time_ms
        self.last_delta_ms = delta_ms
        self._deltas.append(delta_ms)

        # ── CRITICAL: consecutive check (unchanged) ──────────────────────────
        if delta_ms > self.CRITICAL_MS:
            self._critical_count += 1
        else:
            self._critical_count = 0

        if self._critical_count >= self.CONSEC_LIMIT:
            entering = self.status != "CRITICAL"
            self.status = "CRITICAL"
            self._degraded_since_ms = None
            now = time.time()
            if entering or (now - self._last_critical_log_ts) >= 60.0:
                logger.critical(
                    "[Heartbeat] CRITICAL: Binance WS latency %d consecutive >%dms. Last=%.0fms",
                    self._critical_count, self.CRITICAL_MS, delta_ms,
                )
                self._last_critical_log_ts = now
            return self.status

        # ── RATE-GATE: wait for full window before evaluating ─────────────────
        if len(self._deltas) < self._deltas.maxlen:
            return self.status

        degraded_in_window = sum(1 for d in self._deltas if d > self.WARN_MS)
        rate   = degraded_in_window / len(self._deltas)
        now_ms = time.time() * 1000

        if rate >= settings.HEARTBEAT_DEGRADED_RATE_THRESH:
            if self.status == "HEALTHY":
                self.status = "DEGRADED"
                self._degraded_since_ms = now_ms
                logger.warning(
                    "[Heartbeat] DEGRADED: Binance WS latency %d/%d messages >%dms avg=%.0fms",
                    degraded_in_window, len(self._deltas), self.WARN_MS, self.avg_delta_ms,
                )
            elif self.status == "DEGRADED" and self._degraded_since_ms is not None:
                if (now_ms - self._degraded_since_ms) >= settings.HEARTBEAT_SUSTAINED_MS:
                    self.status = "SUSTAINED_DEGRADED"
                    logger.warning(
                        "[Heartbeat] SUSTAINED_DEGRADED: Binance WS latency >%.0fs above %.0f%% degraded rate avg=%.0fms",
                        settings.HEARTBEAT_SUSTAINED_MS / 1000,
                        settings.HEARTBEAT_DEGRADED_RATE_THRESH * 100,
                        self.avg_delta_ms,
                    )
            # elif already SUSTAINED_DEGRADED: remain, no repeated log

        elif rate < settings.HEARTBEAT_DEGRADED_RECOVERY_THRESH:
            if self.status in ("DEGRADED", "SUSTAINED_DEGRADED", "CRITICAL"):
                logger.info(
                    "[Heartbeat] RECOVERED: Binance WS latency normal from %s avg=%.0fms",
                    self.status, self.avg_delta_ms,
                )
            self.status = "HEALTHY"
            self._degraded_since_ms = None

        # else: rate in hysteresis zone — no transition

        return self.status

    @property
    def avg_delta_ms(self) -> float:
        return float(np.mean(self._deltas)) if self._deltas else 0.0

    @property
    def max_delta_ms(self) -> float:
        return float(max(self._deltas)) if self._deltas else 0.0


class BinanceWebSocketConsumer:
    # Incremental diff-depth stream seeded by a 1000-level REST snapshot.
    # All levels from the internal book are forwarded to LOBEngine on each tick
    # — no artificial depth cap — so the full available book is visible downstream.
    _MAX_PENDING_DIFFS = 500   # cap pending diff buffer during REST snapshot fetch

    STREAMS = [
        f"{settings.SYMBOL.lower()}@aggTrade",
        f"{settings.SYMBOL.lower()}@kline_{settings.TIMEFRAME}",
        f"{settings.SYMBOL.lower()}@depth@100ms",   # incremental diff — seeded by REST snapshot
        f"{settings.SYMBOL.lower()}@bookTicker",
    ]

    def __init__(
        self,
        candle_queue: asyncio.Queue | None = None,
        candle_db_queue: asyncio.Queue | None = None,
        trade_queue: asyncio.Queue | None = None,
        depth_queue: asyncio.Queue | None = None,
        shared_state: SharedState | None = None,
        heartbeat_cb: Callable[[str, float], Awaitable[None]] | None = None,
        streams: list[str] | None = None,
        heartbeat_key: str = "heartbeat_status",
        critical_ms: int | None = None,
    ) -> None:
        self._candle_queue    = candle_queue
        self._candle_db_queue = candle_db_queue
        self._trade_queue     = trade_queue
        self._depth_queue     = depth_queue
        self._shared_state    = shared_state or SharedState()
        self._heartbeat_cb    = heartbeat_cb
        self._streams         = list(streams) if streams is not None else list(self.STREAMS)
        self._heartbeat_key   = heartbeat_key
        self._running         = False
        self._reconnect_delay = 1.0
        self._max_delay       = 10.0
        self.heartbeat        = HeartbeatMonitor(critical_ms=critical_ms)
        self._frame_counts: dict[str, int] = {}
        self._frame_log_ts: float = 0.0

        # Local order book for diff-depth reconstruction
        self._bid_book:       dict[float, float] = {}
        self._ask_book:       dict[float, float] = {}
        self._lob_update_id:  int  = 0
        self._lob_synced:     bool = False
        self._lob_pending:    list[dict] = []
        self._depth_drop_count: int = 0

    async def start(self) -> None:
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False

    async def _connect_loop(self) -> None:
        stream_path = "/".join(self._streams)
        uri = f"{settings.WS_BASE}/stream?streams={stream_path}"
        attempt = 0

        while self._running:
            try:
                logger.info("[WS] Connecting (attempt %d): %s", attempt + 1, uri)
                async with websockets.connect(
                    uri,
                    ping_interval=10,
                    ping_timeout=15,
                    close_timeout=5,
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
                    self._lob_pending   = []
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

            setattr(self._shared_state, self._heartbeat_key, "CRITICAL")
            if self._depth_queue is not None:
                self._shared_state.lob_status = "DISCONNECTED"
            attempt += 1
            logger.info("[WS] Reconnecting in %.1fs…", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_delay)

    async def _receive_loop(self, ws) -> None:
        conn_start = time.monotonic()

        while True:
            if not self._running:
                break

            if time.monotonic() - conn_start > _MAX_CONNECTION_SECONDS:
                logger.info("[WS] Approaching 24 h limit — reconnecting proactively.")
                await ws.close()
                break

            try:
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("[WS] No message for 5s — forcing reconnect")
                break

            try:
                outer      = json.loads(raw_msg)
                stream     = outer.get("stream", "")
                msg        = outer.get("data", outer)

                # Heartbeat — called on EVERY message (Rule 11)
                event_ms = msg.get("E", int(time.time() * 1000))
                status   = self.heartbeat.record(event_ms)
                setattr(self._shared_state, self._heartbeat_key, status)
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

                if self.heartbeat._critical_count >= self.heartbeat.CONSEC_LIMIT:
                    logger.warning(
                        "[WS] Heartbeat CRITICAL (delta=%.0fms) — forcing reconnect for fresh connection",
                        self.heartbeat.last_delta_ms,
                    )
                    break

                await self._dispatch(stream, msg)

                label = stream.split("@", 1)[-1] if "@" in stream else stream
                self._frame_counts[label] = self._frame_counts.get(label, 0) + 1
                now = time.monotonic()
                if now - self._frame_log_ts >= 10.0:
                    total  = sum(self._frame_counts.values())
                    detail = "  ".join(f"{k}:{v}" for k, v in sorted(self._frame_counts.items()))
                    logger.info("[WS] %d frames/10s  (%s)", total, detail)
                    self._frame_counts.clear()
                    self._frame_log_ts = now

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("[WS] Malformed message: %s", exc)

    async def _sync_lob_snapshot(self) -> None:
        """
        Fetch a REST depth snapshot to seed the local book, then apply any
        diffs that arrived during the fetch. Mirrors the LOBRecorder pattern.
        Falls back to marking synced anyway so data continues to flow on failure.
        """
        url = f"{settings.REST_BASE}/fapi/v1/depth"
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
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.error("[WS] LOB REST seed failed (%s) — continuing with buffered diffs.", exc)
        finally:
            self._lob_pending.clear()
            self._lob_synced = True
            if self._bid_book and self._ask_book:
                self._enqueue_latest_depth_snapshot(
                    self._reconstruct_depth_msg(int(time.time() * 1000))
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
        Build a full-snapshot-style dict from the local book so downstream
        consumers (LOBEngine, MicrostructureDetector) receive the complete
        available depth on every tick.
        """
        bids = sorted(self._bid_book.items(), reverse=True)
        asks = sorted(self._ask_book.items())
        return {
            "lastUpdateId": self._lob_update_id,
            "E": event_ms,
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        }

    def _enqueue_latest_depth_snapshot(self, snapshot: dict) -> None:
        """Keep the newest depth snapshot moving without blocking the websocket reader."""
        if self._depth_queue is None:
            return

        try:
            self._depth_queue.put_nowait(snapshot)
            return
        except asyncio.QueueFull:
            try:
                self._depth_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

            try:
                self._depth_queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                self._depth_drop_count += 1
                if self._depth_drop_count == 1 or self._depth_drop_count % 100 == 0:
                    logger.warning(
                        "[WS] depth_queue saturated — dropped %d latest snapshots",
                        self._depth_drop_count,
                    )
                return

            self._depth_drop_count += 1
            if self._depth_drop_count == 1 or self._depth_drop_count % 100 == 0:
                logger.warning(
                    "[WS] depth_queue saturated — replaced oldest snapshot (%d drops)",
                    self._depth_drop_count,
                )

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
                if self._candle_queue is not None:
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
                self._enqueue_latest_depth_snapshot(self._reconstruct_depth_msg(event_ms))

        elif event_type == "bookTicker":
            # Best bid/ask for spread calculation; route alongside trades
            if self._trade_queue is not None:
                await self._trade_queue.put(msg)
