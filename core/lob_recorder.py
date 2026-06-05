"""
LOB Recorder — always-on collector that writes order book snapshots and
aggTrades to a local SQLite database for later analysis and backtesting.

Connects to the real Binance public WebSocket diff-depth stream (no API key
required) and maintains a local order book up to _DEPTH_LEVELS levels per
side.  Before writing, levels are aggregated into _BUCKET_WIDTH USD price
buckets to reduce storage by 60–80% while preserving the LOB shape at the
distances relevant for wall detection ($50–500 from mid).

Usage:
    python -m core.lob_recorder
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import time

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings

logger = logging.getLogger(__name__)

_DEPTH_LEVELS        = 100     # levels per side kept in local book before bucketing
# USD price-bucket width for storage compression. Must stay fine enough to preserve the
# LOB shape for wall detection: BTCUSDT's top-100 levels span only ~$15, so the old $25
# width collapsed each snapshot to 1-2 buckets → identify_walls found nothing → 0 backtest
# trades. $1 keeps ~15-25 buckets/side — enough resolution while still compressing.
_BUCKET_WIDTH        = 1.0     # USD
_FLUSH_RECORDS       = 100
_FLUSH_SECONDS       = 5.0
_MAX_RECONNECT_DELAY = 60.0
_MAX_BUFFER_SIZE     = 10_000
_RETENTION_DAYS      = 7
_CLEANUP_INTERVAL    = 86_400.0


class LOBRecorder:
    """
    Buffers depth snapshots (derived from the incremental diff stream) and
    aggTrades, then flushes them to SQLite in batches.
    WAL mode ensures the trading engine can read concurrently without blocking.

    LOB maintenance
    ---------------
    On each WebSocket connect the recorder fetches a REST depth snapshot to
    seed the local book, buffers any diffs that arrive during the fetch, then
    merges them in order.  Subsequent diff events update the book in-place;
    zero-qty entries are removed.  The top _DEPTH_LEVELS bid/ask levels are
    bucketed and written to depth_snapshots after each diff event.
    """

    def __init__(self, db_path: str = settings.LOB_TICK_DB) -> None:
        self._db_path = db_path
        self._depth_buf: list[tuple] = []
        self._trade_buf: list[tuple] = []
        self._last_flush: float = time.monotonic()
        self._reconnect_delay: float = 1.0
        self._running: bool = False
        self._conn: sqlite3.Connection | None = None
        self._flush_lock = asyncio.Lock()

        # Local order book
        self._bid_book:       dict[float, float] = {}
        self._ask_book:       dict[float, float] = {}
        self._last_update_id: int                = 0
        self._synced:         bool               = False
        self._pending_diffs:  list[dict]         = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = self._open_db()
        logger.info("[LOBRec] Recording to %s", self._db_path)
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        try:
            await self._connect_loop()
        finally:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass

    async def stop(self) -> None:
        self._running = False
        if self._conn:
            await self._flush()
            self._conn.close()

    # ── WebSocket ─────────────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        symbol = settings.SYMBOL.lower()
        uri = (
            f"{settings.LOB_RECORDER_WS}/stream"
            f"?streams={symbol}@depth@100ms/{symbol}@aggTrade"
        )
        attempt = 0
        while self._running:
            try:
                logger.info("[LOBRec] Connecting (attempt %d)…", attempt + 1)
                self._bid_book.clear()
                self._ask_book.clear()
                self._last_update_id = 0
                self._synced = False
                self._pending_diffs = []

                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info("[LOBRec] Connected, syncing LOB snapshot…")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    sync_task = asyncio.create_task(self._sync_snapshot())
                    try:
                        await self._receive_loop(ws)
                    finally:
                        sync_task.cancel()
                        try:
                            await sync_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("[LOBRec] Connection closed: %s", exc)
            except Exception as exc:
                logger.error("[LOBRec] Unexpected error: %s", exc, exc_info=True)

            if not self._running:
                break

            attempt += 1
            logger.info("[LOBRec] Reconnecting in %.1fs…", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, _MAX_RECONNECT_DELAY)

    async def _sync_snapshot(self) -> None:
        """Fetch a REST depth snapshot to seed the local LOB, then apply any buffered diffs."""
        url = "https://fapi.binance.com/fapi/v1/depth"
        params = {"symbol": settings.SYMBOL.upper(), "limit": 1000}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    resp.raise_for_status()
                    snap = await resp.json()

            self._bid_book = {
                float(p): float(q) for p, q in snap["bids"] if float(q) > 0
            }
            self._ask_book = {
                float(p): float(q) for p, q in snap["asks"] if float(q) > 0
            }
            last_uid = snap["lastUpdateId"]

            # Apply buffered diffs that arrived during the REST call
            for event in self._pending_diffs:
                if event["u"] <= last_uid:
                    continue  # stale — discard
                self._apply_diff(event)

            self._pending_diffs.clear()
            self._last_update_id = last_uid
            self._synced = True
            logger.info(
                "[LOBRec] LOB synced at updateId=%d  bids=%d  asks=%d",
                last_uid, len(self._bid_book), len(self._ask_book),
            )
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.error("[LOBRec] Snapshot sync failed (%s); continuing unsynced.", exc)
            self._pending_diffs.clear()
            self._synced = True  # allow data to flow even if REST call failed

    async def _receive_loop(self, ws) -> None:
        async for raw in ws:
            if not self._running:
                break
            try:
                outer = json.loads(raw)
                stream_name = outer.get("stream", "")
                msg = outer.get("data", outer)
                event_type = msg.get("e") or (
                    "aggTrade"    if "@aggTrade" in stream_name else
                    "depthUpdate" if "@depth"   in stream_name else
                    None
                )

                if event_type == "depthUpdate":
                    if not self._synced:
                        self._pending_diffs.append(msg)
                    else:
                        self._apply_diff(msg)
                        ts = int(msg.get("E") or time.time() * 1000)
                        bids, asks = self._snapshot_levels()
                        if bids and asks:
                            self._depth_buf.append((
                                ts,
                                json.dumps(bids),
                                json.dumps(asks),
                            ))

                elif event_type == "aggTrade":
                    self._trade_buf.append((
                        int(msg["T"]),
                        float(msg["p"]),
                        float(msg["q"]),
                        1 if msg["m"] else 0,
                    ))

                else:
                    logger.debug(
                        "[LOBRec] Unrecognised event: stream=%s e=%s", stream_name, event_type
                    )

                await self._maybe_flush()

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("[LOBRec] Malformed message: %s", exc)

    # ── Local order book ──────────────────────────────────────────────────────

    def _apply_diff(self, event: dict) -> None:
        """Apply a depthUpdate diff event to the local bid/ask books."""
        for price_str, qty_str in event.get("b", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0.0:
                self._bid_book.pop(price, None)
            else:
                self._bid_book[price] = qty
        for price_str, qty_str in event.get("a", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0.0:
                self._ask_book.pop(price, None)
            else:
                self._ask_book[price] = qty
        self._last_update_id = event.get("u", self._last_update_id)

    def _snapshot_levels(self) -> tuple[list, list]:
        """Return top _DEPTH_LEVELS bid/ask levels bucketed into _BUCKET_WIDTH USD buckets."""
        bids = sorted(self._bid_book.items(), reverse=True)[:_DEPTH_LEVELS]
        asks = sorted(self._ask_book.items())[:_DEPTH_LEVELS]
        return self._bucket_levels(bids), self._bucket_levels(asks)

    def _bucket_levels(self, levels: list[tuple[float, float]]) -> list[list]:
        """Aggregate (price, qty) pairs into fixed-width USD price buckets."""
        buckets: dict[float, float] = {}
        for price, qty in levels:
            key = math.floor(price / _BUCKET_WIDTH) * _BUCKET_WIDTH
            buckets[key] = buckets.get(key, 0.0) + qty
        return [[str(p), str(q)] for p, q in sorted(buckets.items())]

    # ── Persistence ───────────────────────────────────────────────────────────

    async def _maybe_flush(self) -> None:
        total = len(self._depth_buf) + len(self._trade_buf)
        elapsed = time.monotonic() - self._last_flush
        if total >= _FLUSH_RECORDS or elapsed >= _FLUSH_SECONDS:
            await self._flush()

    async def _flush(self) -> None:
        if not self._conn:
            return
        async with self._flush_lock:
            depth_rows = self._depth_buf[:]
            trade_rows = self._trade_buf[:]
            self._depth_buf.clear()
            self._trade_buf.clear()
            self._last_flush = time.monotonic()

            success = await asyncio.to_thread(self._flush_sync, depth_rows, trade_rows)

            if not success:
                combined_depth = depth_rows + self._depth_buf
                combined_trade = trade_rows + self._trade_buf
                if len(combined_depth) > _MAX_BUFFER_SIZE:
                    logger.warning(
                        "[LOBRec] Buffer cap hit; dropping %d depth rows.",
                        len(combined_depth) - _MAX_BUFFER_SIZE,
                    )
                if len(combined_trade) > _MAX_BUFFER_SIZE:
                    logger.warning(
                        "[LOBRec] Buffer cap hit; dropping %d trade rows.",
                        len(combined_trade) - _MAX_BUFFER_SIZE,
                    )
                self._depth_buf = combined_depth[:_MAX_BUFFER_SIZE]
                self._trade_buf = combined_trade[:_MAX_BUFFER_SIZE]

    def _flush_sync(self, depth_rows: list, trade_rows: list) -> bool:
        """Synchronous DB write — runs in a thread pool via asyncio.to_thread."""
        try:
            cur = self._conn.cursor()
            if depth_rows:
                cur.executemany(
                    "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                    depth_rows,
                )
            if trade_rows:
                cur.executemany(
                    "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                    trade_rows,
                )
            self._conn.commit()
            logger.debug(
                "[LOBRec] Flushed %d snapshots, %d trades.",
                len(depth_rows), len(trade_rows),
            )
            return True
        except sqlite3.Error as exc:
            logger.error("[LOBRec] DB write failed: %s", exc)
            return False

    async def _cleanup_loop(self) -> None:
        while self._running:
            await asyncio.sleep(_CLEANUP_INTERVAL)
            if self._conn:
                await asyncio.to_thread(self._purge_old_records)

    def _purge_old_records(self) -> None:
        cutoff_ms = int((time.time() - _RETENTION_DAYS * 86_400) * 1000)
        try:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM depth_snapshots WHERE ts_event < ?", (cutoff_ms,))
            cur.execute("DELETE FROM agg_trades WHERE ts_event < ?", (cutoff_ms,))
            self._conn.commit()
            logger.info("[LOBRec] Purged records older than %d days.", _RETENTION_DAYS)
        except sqlite3.Error as exc:
            logger.error("[LOBRec] Cleanup failed: %s", exc)

    def _open_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS depth_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_event  INTEGER NOT NULL,
                bids_json TEXT    NOT NULL,
                asks_json TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agg_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_event       INTEGER NOT NULL,
                price          REAL    NOT NULL,
                qty            REAL    NOT NULL,
                is_buyer_maker INTEGER NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_depth_ts ON depth_snapshots(ts_event)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON agg_trades(ts_event)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_cov "
            "ON agg_trades(ts_event, qty, is_buyer_maker)"
        )
        conn.commit()
        return conn

    # ── Health ────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return row counts (DB + unflushed buffer) and DB file size."""
        if not self._conn:
            return {"depth_rows": 0, "trade_rows": 0, "db_size_bytes": 0}
        try:
            depth_rows = self._conn.execute(
                "SELECT COUNT(*) FROM depth_snapshots"
            ).fetchone()[0]
            trade_rows = self._conn.execute(
                "SELECT COUNT(*) FROM agg_trades"
            ).fetchone()[0]
            db_size = os.path.getsize(self._db_path) if os.path.exists(self._db_path) else 0
            return {
                "depth_rows": depth_rows + len(self._depth_buf),
                "trade_rows": trade_rows + len(self._trade_buf),
                "db_size_bytes": db_size,
            }
        except sqlite3.Error:
            return {"depth_rows": 0, "trade_rows": 0, "db_size_bytes": 0}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(LOBRecorder().start())
