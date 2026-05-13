import asyncio
import logging
import sqlite3
from datetime import datetime, timezone

from models import Candle, PatternSignal, PortfolioState

logger = logging.getLogger(__name__)

DB_PATH = "cryptosentinel.db"


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                open_time TEXT PRIMARY KEY,
                open REAL, high REAL, low REAL, close REAL, volume REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at TEXT,
                pattern TEXT, direction TEXT,
                confidence REAL, entry_price REAL,
                stop_loss REAL, take_profit REAL,
                r2 REAL, volume_ratio REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                ts TEXT PRIMARY KEY,
                equity REAL, daily_pnl REAL,
                drawdown_pct REAL, circuit_breaker TEXT
            )
        """)
        conn.commit()
    logger.info("[DB] Database initialized.")


class DBWriter:
    PORTFOLIO_INTERVAL = 5.0
    CLEANUP_INTERVAL = 86_400.0   # run once per day
    RETENTION_DAYS = 7

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        portfolio: PortfolioState,
    ) -> None:
        self._candle_queue = candle_queue
        self._signal_queue = signal_queue
        self._portfolio = portfolio

    async def run(self) -> None:
        logger.info("[DB] Writer started.")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._candle_loop())
            tg.create_task(self._signal_loop())
            tg.create_task(self._portfolio_loop())
            tg.create_task(self._cleanup_loop())

    async def _candle_loop(self) -> None:
        while True:
            candle: Candle = await self._candle_queue.get()
            await asyncio.to_thread(self._write_candle, candle)

    async def _signal_loop(self) -> None:
        while True:
            signal: PatternSignal = await self._signal_queue.get()
            await asyncio.to_thread(self._write_signal, signal)

    async def _portfolio_loop(self) -> None:
        while True:
            await asyncio.sleep(self.PORTFOLIO_INTERVAL)
            await asyncio.to_thread(self._write_portfolio, self._portfolio)

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.CLEANUP_INTERVAL)
            await asyncio.to_thread(self._purge_old_records)

    def _purge_old_records(self) -> None:
        cutoff = f"-{self.RETENTION_DAYS} days"
        with sqlite3.connect(DB_PATH) as conn:
            for table, col in [("candles", "open_time"), ("signals", "detected_at"), ("portfolio", "ts")]:
                deleted = conn.execute(
                    f"DELETE FROM {table} WHERE {col} < datetime('now', ?)", (cutoff,)
                ).rowcount
                if deleted:
                    logger.info(f"[DB] Purged {deleted} rows from {table} older than {self.RETENTION_DAYS}d")
            conn.commit()

    @staticmethod
    def _write_candle(candle: Candle) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?)",
                (candle.open_time.isoformat(), candle.open, candle.high,
                 candle.low, candle.close, candle.volume),
            )
            conn.commit()

    @staticmethod
    def _write_signal(signal: PatternSignal) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """INSERT INTO signals
                   (detected_at, pattern, direction, confidence, entry_price,
                    stop_loss, take_profit, r2, volume_ratio)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (signal.detected_at.isoformat(), signal.pattern.name,
                 signal.direction.name, signal.confidence, signal.entry_price,
                 signal.stop_loss, signal.take_profit, signal.r2, signal.volume_ratio),
            )
            conn.commit()

    @staticmethod
    def _write_portfolio(pf: PortfolioState) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO portfolio VALUES (?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), pf.equity,
                 pf.daily_pnl, pf.drawdown_pct, pf.circuit_breaker.name),
            )
            conn.commit()
