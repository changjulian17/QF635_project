"""
Signal Telemetry — async writer that records every gate evaluation to registry.db.

Every signal (approved AND rejected) is persisted so the 7-gate funnel
can be analysed and the confidence scorer can be trained offline.
"""

import asyncio
import datetime
import json
import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

_FLUSH_BATCH    = 50
_FLUSH_INTERVAL = 10.0   # seconds
_MAX_BUF        = 500    # hard cap — ~10× _FLUSH_BATCH; prevents OOM on persistent DB error


@dataclass
class SignalRecord:
    """One row in signal_records — covers every gate evaluation."""
    signal_id:       str   = field(default_factory=lambda: str(uuid.uuid4()))
    strategy_id:     str   = "v3.0"
    timestamp:       str   = field(default_factory=lambda: _now_iso())
    micro_signal:    str   = ""     # MicroSignal.signal_type or ""
    gate_passed:     str   = ""     # "GATE_0_FAIL" … "APPROVED"
    rejection_reason: str  = ""

    # Feature snapshot (subset — extend as needed)
    lob_status:       str   = ""
    heartbeat_status: str   = ""
    obi_zscore:       float = 0.0
    cvd_delta:        float = 0.0
    spread_bps:       float = 0.0
    confidence:       float = 0.0
    direction:        str   = ""
    features_json:    str   = ""   # JSON-encoded list from fv.to_ml_array(); "" if FV not available

    # Trade outcome — filled in after position closes
    outcome:          str   = ""    # "WIN" | "LOSS" | "FLAT"
    pnl:              float = 0.0
    pnl_pct:          float = 0.0
    duration_min:     float = 0.0


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _record_to_event_payload(record: SignalRecord) -> dict:
    """Serialise a SignalRecord for the /ws/signals stream.

    Subset of the full record — only the fields the live tape and funnel need.
    Kept narrow on purpose (no PII, no feature_json blob) so the WS frame stays small.
    """
    return {
        "type":             "signal_event",
        "ts":               record.timestamp,
        "signal_id":        record.signal_id,
        "micro_signal":     record.micro_signal,
        "gate_passed":      record.gate_passed,
        "rejection_reason": record.rejection_reason,
        "direction":        record.direction,
        "confidence":       record.confidence,
    }


class SignalTelemetry:
    """
    Drains a telemetry_queue of SignalRecord objects and writes them to
    registry.db in batches.  Flushed on FLUSH_BATCH records OR FLUSH_INTERVAL
    seconds, whichever comes first.

    When ``hub`` is provided, each record is also broadcast on dequeue (before
    buffering) so dashboards can react to every gate decision in real time.
    """

    def __init__(
        self,
        telemetry_queue: asyncio.Queue,
        db_path: str = settings.REGISTRY_DB,
        hub=None,
    ) -> None:
        self._queue   = telemetry_queue
        self._db_path = db_path
        self._hub     = hub
        self._conn: Optional[sqlite3.Connection] = None
        self._buf: list[SignalRecord] = []
        self._last_flush: float = time.monotonic()
        self._db_lock = asyncio.Lock()
        # Serialises event-loop writes (write_system_event) with thread-pool writes
        # (update_outcome via asyncio.to_thread). asyncio.Lock alone cannot protect
        # across OS-thread boundaries.
        self._write_lock = threading.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._open_db()
        logger.info("[Telemetry] Recording signals to %s", self._db_path)
        try:
            await self._drain_loop()
        except asyncio.CancelledError:
            await self._flush_remaining()
            raise
        finally:
            if self._conn:
                self._conn.close()
                self._conn = None

    async def close(self) -> None:
        """Flush remaining records and close the DB connection."""
        await self._flush_remaining()
        if self._conn:
            self._conn.close()
            self._conn = None

    async def _drain_loop(self) -> None:
        while True:
            try:
                record = await asyncio.wait_for(self._queue.get(), timeout=_FLUSH_INTERVAL)
                if self._hub is not None:
                    try:
                        await self._hub.broadcast(_record_to_event_payload(record))
                    except (TypeError, RuntimeError):
                        logger.exception("[Telemetry] hub broadcast failed — continuing")
                self._buf.append(record)
                if len(self._buf) >= _FLUSH_BATCH or record.gate_passed == "APPROVED":
                    await self._flush()
            except asyncio.TimeoutError:
                if self._buf:
                    await self._flush()

    async def _flush_remaining(self) -> None:
        """Drain the queue into the buffer and flush — called on shutdown."""
        while not self._queue.empty():
            try:
                self._buf.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        await self._flush()

    # ── Flush ─────────────────────────────────────────────────────────────────

    async def _flush(self) -> None:
        async with self._db_lock:
            if not self._buf or not self._conn:
                return
            rows = self._buf[:]
            self._buf.clear()
            self._last_flush = time.monotonic()
            try:
                self._conn.executemany(
                    """
                    INSERT OR IGNORE INTO signal_records (
                        signal_id, strategy_id, timestamp,
                        micro_signal, gate_passed, rejection_reason,
                        lob_status, heartbeat_status,
                        obi_zscore, cvd_delta, spread_bps, confidence, direction,
                        outcome, pnl, pnl_pct, duration_min, features_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            r.signal_id, r.strategy_id, r.timestamp,
                            r.micro_signal, r.gate_passed, r.rejection_reason,
                            r.lob_status, r.heartbeat_status,
                            r.obi_zscore, r.cvd_delta, r.spread_bps,
                            r.confidence, r.direction,
                            r.outcome, r.pnl, r.pnl_pct, r.duration_min,
                            r.features_json or None,
                        )
                        for r in rows
                    ],
                )
                self._conn.commit()
                logger.debug("[Telemetry] Flushed %d records.", len(rows))
            except sqlite3.Error as exc:
                logger.error("[Telemetry] Flush failed: %s", exc)
                combined = rows + self._buf
                if len(combined) > _MAX_BUF:
                    logger.warning(
                        "[Telemetry] Buffer overflow — dropping %d newest records (cap=%d).",
                        len(combined) - _MAX_BUF, _MAX_BUF,
                    )
                    combined = combined[:_MAX_BUF]   # keep oldest for audit trail
                self._buf = combined

    def write_system_event(self, event_type: str, payload: dict | None = None) -> None:
        """Write a lifecycle event to system_events while telemetry is running."""
        if self._conn is None:
            return
        try:
            with self._write_lock:
                with self._conn:
                    self._conn.execute(
                        "INSERT INTO system_events (event_type, occurred_at, payload_json) VALUES (?, ?, ?)",
                        (
                            event_type,
                            datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            json.dumps(payload) if payload is not None else None,
                        ),
                    )
        except sqlite3.Error as exc:
            logger.warning("[Telemetry] system_event write failed %s: %s", event_type, exc)

    async def update_outcome(
        self,
        signal_id: str,
        outcome: str,
        pnl: float,
        pnl_pct: float,
        duration_min: float,
        r_multiple: float | None = None,
    ) -> None:
        """Called by OrderManager after a position closes."""
        if not self._conn:
            return

        def _do_update() -> None:
            with self._write_lock:
                self._conn.execute(
                    """
                    UPDATE signal_records
                    SET outcome=?, pnl=?, pnl_pct=?, duration_min=?, r_multiple_achieved=?
                    WHERE signal_id=?
                    """,
                    (outcome, pnl, pnl_pct, duration_min, r_multiple, signal_id),
                )
                self._conn.commit()

        try:
            async with self._db_lock:
                await asyncio.to_thread(_do_update)
        except sqlite3.Error as exc:
            logger.error("[Telemetry] Outcome update failed: %s", exc)

    async def update_fill(self, signal_id: str, slippage_bps: float) -> None:
        """Called by _process_fills after a confirmed entry fill."""
        if not self._conn:
            return

        def _do() -> None:
            with self._write_lock:
                self._conn.execute(
                    "UPDATE signal_records SET entry_slippage_bps=? WHERE signal_id=?",
                    (slippage_bps, signal_id),
                )
                self._conn.commit()

        try:
            async with self._db_lock:
                await asyncio.to_thread(_do)
        except sqlite3.Error as exc:
            logger.error("[Telemetry] Fill update failed: %s", exc)

    # ── Schema ────────────────────────────────────────────────────────────────

    def _open_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _ensure_signal_records_schema(conn)
        return conn


def _ensure_signal_records_schema(conn: sqlite3.Connection) -> None:
    """Create signal_records and system_events tables and apply column migrations.

    Idempotent — safe to call on an existing database. Called by both
    SignalTelemetry._open_db() and StrategyRegistry._init_db() so either class
    can query signal_records without depending on the other having started first.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS signal_records (
            signal_id        TEXT PRIMARY KEY,
            strategy_id      TEXT NOT NULL,
            timestamp        TEXT NOT NULL,
            micro_signal     TEXT,
            gate_passed      TEXT,
            rejection_reason TEXT,
            lob_status       TEXT,
            heartbeat_status TEXT,
            obi_zscore       REAL,
            cvd_delta        REAL,
            spread_bps       REAL,
            confidence       REAL,
            direction        TEXT,
            outcome          TEXT DEFAULT '',
            pnl              REAL DEFAULT 0.0,
            pnl_pct          REAL DEFAULT 0.0,
            duration_min     REAL DEFAULT 0.0,
            features_json    TEXT DEFAULT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sigrecords_ts
            ON signal_records(strategy_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_sigrecords_gate
            ON signal_records(gate_passed, micro_signal);

        CREATE TABLE IF NOT EXISTS system_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type   TEXT NOT NULL,
            occurred_at  TEXT NOT NULL,
            payload_json TEXT
        );
    """)
    conn.commit()
    for col, typedef in [
        ("features_json",       "TEXT DEFAULT NULL"),
        ("entry_slippage_bps",  "REAL DEFAULT NULL"),
        ("r_multiple_achieved", "REAL DEFAULT NULL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE signal_records ADD COLUMN {col} {typedef}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
