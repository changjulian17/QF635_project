import logging
import os
import sqlite3
import time as _time
from contextlib import contextmanager
from typing import Generator, Union

from config import settings

logger = logging.getLogger(__name__)

MAIN_DB = "cryptosentinel.db"
REGISTRY_DB = settings.REGISTRY_DB
BACKTEST_DB = settings.BACKTEST_RESULTS_DB
LOB_TICK_DB = settings.LOB_TICK_DB


class DBOffline(Exception):
    """Raised when a registry/backtest DB query fails due to schema not yet created."""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def main_db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect(MAIN_DB)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def registry_db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect(REGISTRY_DB)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def backtest_db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect(BACKTEST_DB)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def lob_tick_db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect(LOB_TICK_DB)
    try:
        yield conn
    finally:
        conn.close()


def fetch_engine_health() -> dict | None:
    try:
        with main_db() as conn:
            row = conn.execute(
                "SELECT lob_status, hb_status, risk_tier, ks_active, updated_ms "
                "FROM engine_health WHERE id = 1"
            ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    stale = (_time.time() * 1000 - (row["updated_ms"] or 0)) > 15_000
    return {
        "lob_status":        row["lob_status"],
        "heartbeat_status":  row["hb_status"],
        "risk_tier":         row["risk_tier"],
        "killswitch_active": bool(row["ks_active"]),
        "stale":             stale,
    }


def fetch_agg_trades(
    since_ts_ms: int,
    until_ts_ms: int | None = None,
    limit: int | None = 20000,
) -> Union[list[dict], DBOffline]:
    if not os.path.exists(LOB_TICK_DB):
        return []
    with lob_tick_db() as conn:
        try:
            if until_ts_ms is not None and limit is None:
                # Both time bounds provided — let the WHERE clause cap the set; no LIMIT needed.
                rows = conn.execute(
                    "SELECT ts_event, price, qty, is_buyer_maker FROM agg_trades "
                    "WHERE ts_event >= ? AND ts_event <= ? ORDER BY ts_event ASC",
                    (since_ts_ms, until_ts_ms),
                ).fetchall()
                return [dict(r) for r in rows]
            elif until_ts_ms is not None:
                rows = conn.execute(
                    "SELECT ts_event, price, qty, is_buyer_maker FROM agg_trades "
                    "WHERE ts_event >= ? AND ts_event <= ? ORDER BY ts_event DESC LIMIT ?",
                    (since_ts_ms, until_ts_ms, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT ts_event, price, qty, is_buyer_maker FROM agg_trades "
                    "WHERE ts_event >= ? ORDER BY ts_event DESC LIMIT ?",
                    (since_ts_ms, limit if limit is not None else 20000),
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] lob_tick offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in reversed(rows)]


def fetch_agg_trade_bin_qtys(
    since_ts_ms: int,
    bucket_size: float,
    limit: int | None = 800_000,
) -> Union[list[float], DBOffline]:
    """Return per-second/per-price-bucket trade quantities for baseline percentiles."""
    if not os.path.exists(LOB_TICK_DB):
        return []
    with lob_tick_db() as conn:
        try:
            if limit is None:
                rows = conn.execute(
                    """
                    SELECT SUM(qty) AS qty
                    FROM agg_trades
                    WHERE ts_event >= ?
                    GROUP BY (ts_event / 1000),
                             ROUND(price / ?) * ?,
                             is_buyer_maker
                    """,
                    (since_ts_ms, bucket_size, bucket_size),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT SUM(qty) AS qty
                    FROM (
                        SELECT ts_event, price, qty, is_buyer_maker
                        FROM agg_trades
                        WHERE ts_event >= ?
                        ORDER BY ts_event DESC
                        LIMIT ?
                    )
                    GROUP BY (ts_event / 1000),
                             ROUND(price / ?) * ?,
                             is_buyer_maker
                    """,
                    (since_ts_ms, limit, bucket_size, bucket_size),
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] lob_tick offline: %s", e)
            return DBOffline(str(e))
    return [float(r["qty"]) for r in rows if r["qty"] is not None]


def fetch_cvd_series_24h(since_ms: int) -> Union[list[dict], DBOffline]:
    """Per-second CVD delta from agg_trades, aggregated from since_ms to now."""
    if not os.path.exists(LOB_TICK_DB):
        return []
    with lob_tick_db() as conn:
        try:
            rows = conn.execute(
                """
                SELECT
                    (ts_event / 1000) * 1000  AS ts_sec_ms,
                    SUM(CASE WHEN is_buyer_maker = 0 THEN qty ELSE -qty END) AS delta
                FROM agg_trades
                WHERE ts_event >= ?
                GROUP BY ts_sec_ms
                ORDER BY ts_sec_ms ASC
                """,
                (since_ms,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] lob_tick offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_portfolio_history(limit: int = 300) -> Union[list[dict], DBOffline]:
    with main_db() as conn:
        try:
            rows = conn.execute(
                "SELECT ts, equity, daily_pnl, drawdown_pct, circuit_breaker "
                "FROM portfolio ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] main offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in reversed(rows)]


def fetch_portfolio_window(hours: int, max_points: int = 1500) -> Union[list[dict], DBOffline]:
    """Equity / PnL / drawdown rows within the last ``hours``, downsampled to ~max_points.

    The ``portfolio`` table is purged at 7-day retention, so ``hours`` is capped at 168
    (callers should label longer requests accordingly). Downsampling happens in SQL via a
    stride on ROW_NUMBER so at most ~``max_points`` rows cross the wire even for multi-day
    windows. Returns rows ascending by ts.
    """
    hours = min(int(hours), 24 * 7)
    with main_db() as conn:
        try:
            (cnt,) = conn.execute(
                "SELECT COUNT(*) FROM portfolio WHERE datetime(ts) >= datetime('now', ?)",
                (f"-{hours} hours",),
            ).fetchone()
            stride = max(1, (cnt or 0) // max_points)
            rows = conn.execute(
                """SELECT ts, equity, daily_pnl, drawdown_pct FROM (
                       SELECT ts, equity, daily_pnl, drawdown_pct,
                              ROW_NUMBER() OVER (ORDER BY ts) AS rn
                       FROM portfolio WHERE datetime(ts) >= datetime('now', ?)
                   ) WHERE rn % ? = 0 ORDER BY ts ASC""",
                (f"-{hours} hours", stride),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] main offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_lob_snapshots(limit: int = 3600) -> Union[list[dict], DBOffline]:
    with main_db() as conn:
        try:
            rows = conn.execute(
                "SELECT ts, mid_price, spread, obi, cvd_delta, bid_levels_json, ask_levels_json "
                "FROM lob_snapshots ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] main offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in reversed(rows)]


def fetch_microstructure_bars(minutes: int = 20, limit: int = 2000) -> Union[list[dict], DBOffline]:
    """Recent ``microstructure_bars`` rows (oldest-first) for the LOB events panel.

    ``ts`` is stored as a TEXT ISO timestamp, so the time window uses the same
    ``datetime('now', ?)`` idiom as the other registry queries (not epoch ms).
    The newest ``limit`` rows inside the window are returned, reversed to ascending.
    """
    with main_db() as conn:
        try:
            rows = conn.execute(
                """SELECT ts, mid_price, obi, delta, cvd, buy_volume, sell_volume,
                          reload_bid, reload_ask, iceberg_bid, iceberg_ask,
                          sweep_up, sweep_down, book_flip_bid, book_flip_ask,
                          liq_flip_to_res, liq_flip_to_sup,
                          break_protect_long, break_protect_short
                   FROM microstructure_bars
                   WHERE datetime(ts) >= datetime('now', ?)
                   ORDER BY ts DESC LIMIT ?""",
                (f"-{minutes} minutes", limit),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] main offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in reversed(rows)]


def fetch_signal_funnel(hours: int = 24) -> Union[list[dict], DBOffline]:
    with registry_db() as conn:
        try:
            rows = conn.execute(
                "SELECT gate_passed, COUNT(*) as cnt FROM signal_records "
                "WHERE datetime(timestamp) >= datetime('now', ?) GROUP BY gate_passed",
                (f"-{hours} hours",),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_recent_signals(limit: int = 50) -> Union[list[dict], DBOffline]:
    """Return the most recent ``limit`` signal records (newest first) for tape hydration."""
    with registry_db() as conn:
        try:
            rows = conn.execute(
                """SELECT timestamp, signal_id, micro_signal, gate_passed,
                          rejection_reason, direction, confidence
                   FROM signal_records
                   ORDER BY timestamp DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return [
        {
            "ts":               r["timestamp"],
            "signal_id":        r["signal_id"],
            "micro_signal":     r["micro_signal"],
            "gate_passed":      r["gate_passed"],
            "rejection_reason": r["rejection_reason"],
            "direction":        r["direction"],
            "confidence":       r["confidence"],
        }
        for r in rows
    ]


def fetch_gate_funnel_drift() -> Union[list[dict], DBOffline]:
    with registry_db() as conn:
        try:
            rows = conn.execute(
                """SELECT gate_passed,
                       COUNT(*) FILTER (WHERE datetime(timestamp) >= datetime('now','-7 day')) as cnt_7d,
                       COUNT(*) FILTER (WHERE datetime(timestamp) >= datetime('now','-30 day')) as cnt_30d
                   FROM signal_records
                   GROUP BY gate_passed""",
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_strategies() -> Union[list[dict], DBOffline]:
    with registry_db() as conn:
        try:
            rows = conn.execute(
                "SELECT * FROM strategies ORDER BY created_at DESC"
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_session_stats(hours: int = 24) -> Union[dict, DBOffline]:
    with registry_db() as conn:
        try:
            row = conn.execute(
                """
                SELECT
                  COUNT(*)                                                        AS total_trades,
                  SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END)                 AS wins,
                  SUM(pnl)                                                        AS total_pnl,
                  SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) /
                    NULLIF(ABS(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END)), 0)  AS profit_factor,
                  AVG(r_multiple_achieved)                                        AS avg_r_multiple,
                  AVG(duration_min)                                               AS avg_duration_min,
                  AVG(entry_slippage_bps)                                         AS avg_slippage_bps
                FROM signal_records
                WHERE outcome IN ('WIN','LOSS','FLAT')
                  AND datetime(timestamp) >= datetime('now', ?)
                """,
                (f"-{hours} hours",),
            ).fetchone()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return dict(row) if row else {}


def fetch_pnl_by_pattern(hours: int = 24) -> Union[dict, DBOffline]:
    with registry_db() as conn:
        try:
            rows = conn.execute(
                """
                SELECT micro_signal, SUM(pnl) AS total_pnl
                FROM signal_records
                WHERE outcome IN ('WIN','LOSS','FLAT')
                  AND datetime(timestamp) >= datetime('now', ?)
                GROUP BY micro_signal
                """,
                (f"-{hours} hours",),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return {r["micro_signal"]: r["total_pnl"] for r in rows}


def fetch_system_events(limit: int = 50) -> Union[list[dict], DBOffline]:
    with registry_db() as conn:
        try:
            rows = conn.execute(
                "SELECT occurred_at, event_type, payload_json FROM system_events "
                "ORDER BY occurred_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] registry offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]


def fetch_candles(limit: int = 7200) -> Union[list[dict], DBOffline]:
    with main_db() as conn:
        try:
            rows = conn.execute(
                "SELECT open_time, open, high, low, close, volume "
                "FROM candles ORDER BY open_time DESC LIMIT ?",
                (limit,),
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] candles offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in reversed(rows)]


def fetch_backtest_results() -> Union[list[dict], DBOffline]:
    if not os.path.exists(BACKTEST_DB):
        return []
    with backtest_db() as conn:
        try:
            rows = conn.execute("SELECT * FROM results ORDER BY composite_score DESC").fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("[DB] backtest offline: %s", e)
            return DBOffline(str(e))
    return [dict(r) for r in rows]
