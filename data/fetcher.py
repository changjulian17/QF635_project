"""
data/fetcher.py
===============
Historical OHLCV data acquisition layer for CryptoSentinel backtesting.

Design Decisions
----------------
- Source    : Binance public REST API via CCXT (no API key required for OHLCV)
- Transport : CCXT handles rate limiting (1,200 req/min Binance limit)
- Cache     : SQLite on first run; subsequent runs load from disk and only
              fetch candles that are genuinely missing (incremental update)
- Resample  : 1-minute candles are fetched once and resampled to 5m/15m
              in-memory — avoids redundant network calls

Why not Binance Testnet for historical data?
--------------------------------------------
The Binance Spot Testnet uses synthetically generated prices that do not
reflect real market dynamics. A backtest on testnet data would test pattern
detection against fabricated candles, producing meaningless results.
The live Binance public API exposes genuine historical OHLCV going back
years, with no authentication required.

Typical fetch times (BTCUSDT 1m, 6 months ≈ 259,200 candles):
  First run  : ~35 seconds (260 paginated requests at 0.25s delay)
  Subsequent : <1 second (SQLite cache hit)
"""

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import ccxt
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = "data/ohlcv_cache.db"

# Milliseconds per timeframe — used for pagination math
TIMEFRAME_MS: dict[str, int] = {
    "1m":  60_000,
    "3m":  180_000,
    "5m":  300_000,
    "15m": 900_000,
    "1h":  3_600_000,
    "4h":  14_400_000,
    "1d":  86_400_000,
}

# pandas resample rules — maps our TF strings to pandas offset aliases
RESAMPLE_RULES: dict[str, str] = {
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1D",
}

# Binance public API returns max 1,000 candles per request
MAX_CANDLES_PER_REQUEST = 1_000

# Polite delay between paginated requests (seconds)
# 0.25s → 4 req/s → well under the 20 req/s weight limit for this endpoint
REQUEST_DELAY = 0.25


# ─────────────────────────────────────────────────────────────────────────────
# OHLCVFetcher
# ─────────────────────────────────────────────────────────────────────────────

class OHLCVFetcher:
    """
    Fetch and cache historical OHLCV data from Binance.

    Usage
    -----
    >>> fetcher = OHLCVFetcher(symbol="BTC/USDT")
    >>> df_1m   = fetcher.fetch(timeframe="1m", lookback_days=180)
    >>> df_5m   = fetcher.resample(df_1m, "5m")   # no additional network call

    The returned DataFrame has columns:
        timestamp (int, ms epoch), datetime (UTC), open, high, low, close, volume
    """

    def __init__(
        self,
        symbol: str = "BTC/USDT",
        db_path: str = DB_PATH,
    ) -> None:
        self.symbol  = symbol
        self.db_path = db_path

        # CCXT Binance instance — spot market, rate limiting enabled
        self.exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })

        self._init_db()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def fetch(
        self,
        timeframe: str = "1m",
        lookback_days: int = 180,
    ) -> pd.DataFrame:
        """
        Return a clean OHLCV DataFrame for ``symbol`` over the requested period.

        Strategy
        --------
        1. Load whatever is in the SQLite cache for the requested range.
        2. If the cache covers the full range → return immediately.
        3. If partial or empty → fetch only the missing tail from Binance,
           append to cache, then return the full range from cache.

        Parameters
        ----------
        timeframe     : Candle interval. One of "1m", "5m", "15m", "1h", "4h".
        lookback_days : How many days of history to return.

        Returns
        -------
        pd.DataFrame with columns: timestamp, datetime, open, high, low,
                                   close, volume.
        Sorted ascending by timestamp. All datetimes UTC.
        """
        since_ms = _days_ago_ms(lookback_days)
        until_ms = _now_ms()

        cached_df = self._load_from_cache(timeframe, since_ms, until_ms)

        if cached_df is not None and len(cached_df) > 0:
            last_ts = int(cached_df["timestamp"].max())
            gap_ms  = until_ms - last_ts
            tf_ms   = TIMEFRAME_MS.get(timeframe, 60_000)

            if gap_ms < 2 * tf_ms:
                # Cache is fresh enough — no fetch needed
                logger.info(
                    "[Fetcher] Cache hit: %d candles (%s, %dd).",
                    len(cached_df), timeframe, lookback_days,
                )
                return cached_df

            # Cache exists but is stale — fetch only the new tail
            fetch_since = last_ts + tf_ms
            logger.info(
                "[Fetcher] Partial cache (%d candles). "
                "Fetching tail from %s.",
                len(cached_df),
                datetime.fromtimestamp(fetch_since / 1000, tz=timezone.utc).isoformat(),
            )
        else:
            fetch_since = since_ms
            logger.info(
                "[Fetcher] No cache found. Fetching %dd of %s candles "
                "from Binance public API...",
                lookback_days, timeframe,
            )

        # Fetch missing candles from Binance
        new_candles = self._fetch_paginated(timeframe, fetch_since, until_ms)

        if new_candles:
            self._save_to_cache(timeframe, new_candles)
            logger.info("[Fetcher] Saved %d new candles to cache.", len(new_candles))

        # Return the complete range from cache (includes newly fetched data)
        result = self._load_from_cache(timeframe, since_ms, until_ms)
        logger.info(
            "[Fetcher] Returning %d candles (%s, %dd).",
            len(result) if result is not None else 0,
            timeframe, lookback_days,
        )
        return result

    def resample(self, df_1m: pd.DataFrame, target_tf: str) -> pd.DataFrame:
        """
        Resample a 1-minute DataFrame to a higher timeframe.

        This avoids making additional network requests. Fetch 1m once,
        derive all higher timeframes from it.

        Parameters
        ----------
        df_1m      : 1-minute OHLCV DataFrame (from ``fetch("1m", ...)``)
        target_tf  : Target timeframe string, e.g. "5m", "15m", "1h".

        Returns
        -------
        Resampled pd.DataFrame with the same column schema as ``fetch()``.
        """
        if target_tf not in RESAMPLE_RULES:
            raise ValueError(
                f"Unsupported resample target '{target_tf}'. "
                f"Choose from: {list(RESAMPLE_RULES)}"
            )

        rule = RESAMPLE_RULES[target_tf]

        df = (
            df_1m.set_index("datetime")
            .resample(rule)
            .agg({
                "open":   "first",
                "high":   "max",
                "low":    "min",
                "close":  "last",
                "volume": "sum",
            })
            .dropna(subset=["close"])
            .reset_index()
        )

        # Reconstruct ms timestamp column
        df["timestamp"] = (
            df["datetime"].astype("int64") // 10 ** 6
        )

        logger.info(
            "[Fetcher] Resampled 1m→%s: %d candles.", target_tf, len(df)
        )
        return df[["timestamp", "datetime", "open", "high", "low", "close", "volume"]]

    # ─────────────────────────────────────────────────────────────────────────
    # Private: Paginated Fetch
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch_paginated(
        self,
        timeframe: str,
        since_ms: int,
        until_ms: int,
    ) -> list[list]:
        """
        Fetch all candles in [since_ms, until_ms] using paginated requests.

        Binance returns at most 1,000 candles per call. We iterate forward
        in time until we reach ``until_ms`` or receive an empty batch.

        Returns
        -------
        List of raw OHLCV lists: [[timestamp, o, h, l, c, v], ...]
        """
        all_ohlcv:     list[list] = []
        current_since: int        = since_ms
        tf_ms          = TIMEFRAME_MS.get(timeframe, 60_000)
        request_count  = 0

        while current_since < until_ms:
            try:
                batch = self.exchange.fetch_ohlcv(
                    self.symbol,
                    timeframe,
                    since=current_since,
                    limit=MAX_CANDLES_PER_REQUEST,
                )
            except ccxt.NetworkError as exc:
                logger.warning("[Fetcher] Network error: %s. Retrying in 5s.", exc)
                time.sleep(5)
                continue
            except ccxt.ExchangeError as exc:
                logger.error("[Fetcher] Exchange error: %s. Aborting fetch.", exc)
                break

            if not batch:
                break  # No more data available

            all_ohlcv.extend(batch)
            last_ts        = batch[-1][0]
            current_since  = last_ts + tf_ms
            request_count += 1

            logger.debug(
                "[Fetcher] Req %d: %d candles, last=%s",
                request_count,
                len(batch),
                datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
                        .strftime("%Y-%m-%d %H:%M"),
            )

            # Polite rate limiting
            time.sleep(REQUEST_DELAY)

        logger.info(
            "[Fetcher] Paginated fetch complete: %d candles in %d requests.",
            len(all_ohlcv), request_count,
        )
        return all_ohlcv

    # ─────────────────────────────────────────────────────────────────────────
    # Private: SQLite Cache
    # ─────────────────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        """Create the SQLite database and ohlcv table if they don't exist."""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    symbol    TEXT    NOT NULL,
                    timeframe TEXT    NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open      REAL    NOT NULL,
                    high      REAL    NOT NULL,
                    low       REAL    NOT NULL,
                    close     REAL    NOT NULL,
                    volume    REAL    NOT NULL,
                    PRIMARY KEY (symbol, timeframe, timestamp)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup
                ON ohlcv (symbol, timeframe, timestamp)
            """)
        logger.debug("[Fetcher] SQLite cache initialised at '%s'.", self.db_path)

    def _save_to_cache(self, timeframe: str, ohlcv: list[list]) -> None:
        """
        Persist raw OHLCV list to SQLite.

        Uses INSERT OR IGNORE so re-running fetches never creates duplicates.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO ohlcv
                   (symbol, timeframe, timestamp, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (self.symbol, timeframe,
                     row[0], row[1], row[2], row[3], row[4], row[5])
                    for row in ohlcv
                ],
            )

    def _load_from_cache(
        self,
        timeframe: str,
        since_ms: int,
        until_ms: int,
    ) -> pd.DataFrame | None:
        """
        Load cached candles for the given range.

        Returns None if the cache table is empty for this query.
        Returns a DataFrame otherwise (may be partial coverage).
        """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                """SELECT timestamp, open, high, low, close, volume
                   FROM ohlcv
                   WHERE symbol    = ?
                     AND timeframe = ?
                     AND timestamp BETWEEN ? AND ?
                   ORDER BY timestamp ASC""",
                conn,
                params=(self.symbol, timeframe, since_ms, until_ms),
            )

        if df.empty:
            return None

        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df[["timestamp", "datetime", "open", "high", "low", "close", "volume"]]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    """Current UTC time in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _days_ago_ms(days: int) -> int:
    """UTC timestamp ``days`` ago, in milliseconds."""
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return int(dt.timestamp() * 1000)
