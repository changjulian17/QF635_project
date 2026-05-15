"""
LOB Recorder — always-on collector that writes raw order book snapshots
and aggTrades to a local SQLite database for later analysis and backtesting.

Connects to the real Binance public WebSocket stream (no API key required).
Runs as a standalone process alongside the main trading engine.

Usage:
    python -m core.lob_recorder
"""

import asyncio
import json
import logging
import os
import sqlite3
import time

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings

logger = logging.getLogger(__name__)

_STREAMS = (
    f"{settings.SYMBOL.lower()}@depth20@100ms",
    f"{settings.SYMBOL.lower()}@aggTrade",
)
_FLUSH_RECORDS = 500
_FLUSH_SECONDS = 5.0
_MAX_RECONNECT_DELAY = 60.0


class LOBRecorder:
    """
    Buffers depth snapshots and aggTrades then flushes them to SQLite in batches.
    WAL mode ensures the trading engine can read concurrently without blocking.
    """

    def __init__(self, db_path: str = settings.LOB_TICK_DB) -> None:
        self._db_path = db_path
        self._depth_buf: list[tuple] = []
        self._trade_buf: list[tuple] = []
        self._last_flush: float = time.monotonic()
        self._reconnect_delay: float = 1.0
        self._running: bool = False
        self._conn: sqlite3.Connection | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = self._open_db()
        logger.info("[LOBRec] Recording to %s", self._db_path)
        await self._connect_loop()

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
            f"?streams={symbol}@depth20@100ms/{symbol}@aggTrade"
        )
        attempt = 0
        while self._running:
            try:
                logger.info("[LOBRec] Connecting (attempt %d)…", attempt + 1)
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info("[LOBRec] Connected.")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    await self._receive_loop(ws)

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

    async def _receive_loop(self, ws) -> None:
        async for raw in ws:
            if not self._running:
                break
            try:
                outer = json.loads(raw)
                stream = outer.get("stream", "")
                msg = outer.get("data", outer)
                event_type = msg.get("e")

                if "depth20" in stream:
                    ts = int(time.time() * 1000)
                    self._depth_buf.append((
                        ts,
                        json.dumps(msg.get("bids", [])),
                        json.dumps(msg.get("asks", [])),
                    ))

                elif event_type == "aggTrade":
                    self._trade_buf.append((
                        int(msg["T"]),
                        float(msg["p"]),
                        float(msg["q"]),
                        1 if msg["m"] else 0,
                    ))

                await self._maybe_flush()

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("[LOBRec] Malformed message: %s", exc)

    # ── Persistence ───────────────────────────────────────────────────────────

    async def _maybe_flush(self) -> None:
        total = len(self._depth_buf) + len(self._trade_buf)
        elapsed = time.monotonic() - self._last_flush
        if total >= _FLUSH_RECORDS or elapsed >= _FLUSH_SECONDS:
            await self._flush()

    async def _flush(self) -> None:
        if not self._conn:
            return
        depth_rows = self._depth_buf[:]
        trade_rows = self._trade_buf[:]
        self._depth_buf.clear()
        self._trade_buf.clear()
        self._last_flush = time.monotonic()

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
        except sqlite3.Error as exc:
            logger.error("[LOBRec] DB write failed: %s", exc)
            # Re-queue rows so they aren't lost on transient errors
            self._depth_buf = depth_rows + self._depth_buf
            self._trade_buf = trade_rows + self._trade_buf

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
        conn.commit()
        return conn

    # ── Health ────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return row counts and DB file size. Used by the health dashboard."""
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
                "depth_rows": depth_rows,
                "trade_rows": trade_rows,
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
