import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone

from config import settings
from models import Candle, LOBSnapshot, MicrostructureBar, PatternSignal, PortfolioState

logger = logging.getLogger(__name__)

DB_PATH = "cryptosentinel.db"


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
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
                drawdown_pct REAL, circuit_breaker TEXT,
                num_trades INTEGER DEFAULT 0,
                num_wins INTEGER DEFAULT 0,
                budget_loss_pct REAL DEFAULT 0.0,
                avg_slippage_bps REAL DEFAULT 0.0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS microstructure_bars (
                ts TEXT, mid_price REAL, spread REAL, obi REAL,
                delta REAL, cvd REAL, buy_volume REAL, sell_volume REAL,
                reload_bid INTEGER, reload_ask INTEGER,
                iceberg_bid INTEGER, iceberg_ask INTEGER,
                sweep_up INTEGER, sweep_down INTEGER,
                book_flip_bid INTEGER, book_flip_ask INTEGER,
                liq_flip_to_res INTEGER, liq_flip_to_sup INTEGER,
                break_protect_long INTEGER, break_protect_short INTEGER,
                bid_levels TEXT, ask_levels TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lob_snapshots (
                ts              TEXT PRIMARY KEY,
                mid_price       REAL,
                spread          REAL,
                obi             REAL,
                cvd_delta       REAL,
                bid_levels_json TEXT,
                ask_levels_json TEXT
            )
        """)
        for col, typedef in [
            ("num_trades",       "INTEGER DEFAULT 0"),
            ("num_wins",         "INTEGER DEFAULT 0"),
            ("budget_loss_pct",  "REAL DEFAULT 0.0"),
            ("avg_slippage_bps", "REAL DEFAULT 0.0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE portfolio ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # column already exists
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
        ms_bar_queue: asyncio.Queue,
    ) -> None:
        self._candle_queue = candle_queue
        self._signal_queue = signal_queue
        self._portfolio = portfolio
        self._ms_bar_queue = ms_bar_queue

    async def run(self) -> None:
        logger.info("[DB] Writer started.")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._candle_loop())
            tg.create_task(self._signal_loop())
            tg.create_task(self._portfolio_loop())
            tg.create_task(self._ms_bar_loop())
            tg.create_task(self._cleanup_loop())

    async def _candle_loop(self) -> None:
        while True:
            candle: Candle = await self._candle_queue.get()
            try:
                await asyncio.to_thread(self._write_candle, candle)
            except Exception as e:
                logger.warning("[DB] Write error (%s) — continuing", e)

    async def _signal_loop(self) -> None:
        while True:
            signal: PatternSignal = await self._signal_queue.get()
            try:
                await asyncio.to_thread(self._write_signal, signal)
            except Exception as e:
                logger.warning("[DB] Write error (%s) — continuing", e)

    async def _ms_bar_loop(self) -> None:
        while True:
            bar: MicrostructureBar = await self._ms_bar_queue.get()
            try:
                await asyncio.to_thread(self._write_ms_bar, bar)
            except Exception as e:
                logger.warning("[DB] Write error (%s) — continuing", e)

    async def _portfolio_loop(self) -> None:
        while True:
            await asyncio.sleep(self.PORTFOLIO_INTERVAL)
            try:
                await asyncio.to_thread(self._write_portfolio, self._portfolio)
            except Exception as e:
                logger.warning("[DB] Write error (%s) — continuing", e)

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.CLEANUP_INTERVAL)
            try:
                await asyncio.to_thread(self._purge_old_records)
            except Exception as e:
                logger.warning("[DB] Write error (%s) — continuing", e)

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
                "INSERT OR REPLACE INTO portfolio VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    pf.equity, pf.daily_pnl, pf.drawdown_pct,
                    pf.circuit_breaker.name,
                    pf.num_trades, pf.num_wins,
                    pf.budget_loss_pct, pf.avg_slippage_bps,
                ),
            )
            conn.commit()

    async def write_lob_snapshot(
        self,
        snapshot: LOBSnapshot,
        obi: float,
        spread: float,
        mid_price: float,
        cvd_delta: float,
    ) -> None:
        await asyncio.to_thread(
            self._write_lob_snapshot_sync, snapshot, obi, spread, mid_price, cvd_delta
        )

    @staticmethod
    def _write_lob_snapshot_sync(
        snapshot: LOBSnapshot,
        obi: float,
        spread: float,
        mid_price: float,
        cvd_delta: float,
    ) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO lob_snapshots
                   (ts, mid_price, spread, obi, cvd_delta, bid_levels_json, ask_levels_json)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    snapshot.timestamp.isoformat(),
                    mid_price, spread, obi, cvd_delta,
                    json.dumps([[l.price, l.qty] for l in snapshot.bids]),
                    json.dumps([[l.price, l.qty] for l in snapshot.asks]),
                ),
            )
            conn.execute(
                """DELETE FROM lob_snapshots
                   WHERE rowid NOT IN (
                       SELECT rowid FROM lob_snapshots
                       ORDER BY ts DESC LIMIT ?
                   )""",
                (settings.LOB_HISTORY,),
            )
            conn.commit()

    @staticmethod
    def _write_ms_bar(bar: MicrostructureBar) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO microstructure_bars VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    bar.timestamp.isoformat(),
                    bar.mid_price, bar.spread, bar.obi,
                    bar.delta, bar.cvd, bar.buy_volume, bar.sell_volume,
                    int(bar.reload_bid), int(bar.reload_ask),
                    int(bar.iceberg_bid), int(bar.iceberg_ask),
                    int(bar.sweep_up), int(bar.sweep_down),
                    int(bar.book_flip_bid), int(bar.book_flip_ask),
                    int(bar.liq_flip_to_res), int(bar.liq_flip_to_sup),
                    int(bar.break_protect_long), int(bar.break_protect_short),
                    json.dumps([[l.price, l.qty] for l in bar.bid_levels]),
                    json.dumps([[l.price, l.qty] for l in bar.ask_levels]),
                ),
            )
            conn.execute(
                """DELETE FROM microstructure_bars
                   WHERE rowid NOT IN (
                       SELECT rowid FROM microstructure_bars
                       ORDER BY ts DESC LIMIT ?
                   )""",
                (settings.LOB_HISTORY,),
            )
            conn.commit()
