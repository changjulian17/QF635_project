"""
tests/test_fetcher.py
====================
Unit tests for data/fetcher.py — OHLCVFetcher and helpers.

All tests are offline: CCXT is mocked via unittest.mock.patch so
no real network calls are made during the test suite.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from data.fetcher import OHLCVFetcher, TIMEFRAME_MS, _days_ago_ms, _now_ms


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_candles(n: int, start_ms: int | None = None, tf_ms: int = 60_000) -> list[list]:
    """Generate n synthetic 1m OHLCV candles starting from start_ms."""
    if start_ms is None:
        start_ms = _days_ago_ms(2)
    rng   = np.random.default_rng(seed=42)
    price = 50_000.0
    rows  = []
    for i in range(n):
        ts = start_ms + i * tf_ms
        o  = price
        c  = price * (1.0 + rng.uniform(-0.001, 0.001))
        h  = max(o, c) * (1.0 + abs(rng.uniform(0, 0.0005)))
        l  = min(o, c) * (1.0 - abs(rng.uniform(0, 0.0005)))
        v  = rng.uniform(10.0, 100.0)
        rows.append([ts, o, h, l, c, v])
        price = c
    return rows


@pytest.fixture
def fetcher(tmp_path):
    """OHLCVFetcher backed by a temp SQLite DB, with a mocked CCXT exchange."""
    db = str(tmp_path / "test_ohlcv.db")
    with patch("data.fetcher.ccxt") as mock_ccxt:
        mock_exchange = MagicMock()
        mock_ccxt.binance.return_value = mock_exchange
        f = OHLCVFetcher(symbol="BTC/USDT", db_path=db)
    # f.exchange now points to mock_exchange (assigned in __init__)
    return f


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_fetch_returns_correct_schema(fetcher):
    """DataFrame must have datetime, open, high, low, close, volume columns."""
    # Start 2 days ago so all candles sit well inside the 3-day query window,
    # avoiding the few-ms drift between test setup and fetch() re-computing since_ms.
    candles = _make_candles(300, _days_ago_ms(2))
    # First call returns data, second (pagination) returns empty → loop stops
    fetcher.exchange.fetch_ohlcv.side_effect = [candles, []]

    df = fetcher.fetch(timeframe="1m", lookback_days=3)

    required = {"timestamp", "datetime", "open", "high", "low", "close", "volume"}
    assert required.issubset(set(df.columns))
    assert len(df) == 300
    assert pd.api.types.is_integer_dtype(df["timestamp"])
    assert hasattr(df["datetime"].dtype, "tz")   # timezone-aware


def test_cache_hit_under_one_second(fetcher):
    """Second fetch of the same range must load from SQLite cache in < 1 s."""
    # Generate 1442 candles: 2 extra ensure the last candle is within
    # 2 × tf_ms of now even after the few-ms drift in since_ms computation.
    now_ms   = _now_ms()
    since_ms = now_ms - 1442 * 60_000
    fetcher._save_to_cache("1m", _make_candles(1442, since_ms))

    # No exchange call should be needed
    fetcher.exchange.fetch_ohlcv.return_value = []

    t0 = time.perf_counter()
    df = fetcher.fetch(timeframe="1m", lookback_days=1)
    elapsed = time.perf_counter() - t0

    assert elapsed < 1.0, f"Cache hit took {elapsed:.2f}s (expected < 1s)"
    assert len(df) >= 1430          # at least a full day minus boundary slack
    fetcher.exchange.fetch_ohlcv.assert_not_called()


def test_incremental_update_appends_only_new_candles(fetcher):
    """Re-fetching a range must not duplicate rows already in cache."""
    # Anchor both phases to a fixed historical point to avoid boundary drift
    anchor_ms  = _days_ago_ms(3)   # well inside any 1-day query window
    first_half = _make_candles(720, anchor_ms)
    tail_start = anchor_ms + 720 * 60_000
    second_half = _make_candles(720, tail_start)

    # Phase 1: prime the cache with first 720 candles
    fetcher._save_to_cache("1m", first_half)

    # Phase 2: fetch returns the tail; INSERT OR IGNORE prevents duplicates
    fetcher.exchange.fetch_ohlcv.side_effect = [second_half, []]
    df = fetcher.fetch(timeframe="1m", lookback_days=3)

    # No duplicate timestamps regardless of exact row count
    assert df["timestamp"].duplicated().sum() == 0
    assert len(df) >= 720   # at least the first half we pre-populated


def test_resample_5m_from_1m_correct_ratio(fetcher):
    """5m candle count must be approximately 1m count / 5 (within 2% tolerance)."""
    n_1m     = 1000
    now_ms   = _now_ms()
    since_ms = now_ms - n_1m * 60_000
    candles  = _make_candles(n_1m, since_ms)
    fetcher._save_to_cache("1m", candles)
    fetcher.exchange.fetch_ohlcv.return_value = []

    df_1m = fetcher.fetch("1m", lookback_days=1)
    df_5m = fetcher.resample(df_1m, "5m")

    expected_5m = n_1m / 5
    ratio       = len(df_5m) / expected_5m
    assert 0.98 <= ratio <= 1.02, (
        f"5m candle count {len(df_5m)} too far from expected {expected_5m:.0f}"
    )


def test_resample_preserves_ohlcv_semantics(fetcher):
    """Resampled high must be max of constituent 1m highs; low/open/close verified."""
    base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    df_1m = pd.DataFrame({
        "timestamp": [int((base + timedelta(minutes=i)).timestamp() * 1000)
                      for i in range(5)],
        "datetime":  pd.to_datetime(
            [base + timedelta(minutes=i) for i in range(5)], utc=True
        ),
        "open":   [100.0, 101.0, 102.0, 103.0, 104.0],
        "high":   [105.0, 106.0, 107.0, 108.0, 109.0],
        "low":    [95.0,   96.0,  97.0,  98.0,  99.0],
        "close":  [101.0, 102.0, 103.0, 104.0, 105.0],
        "volume": [1.0,    2.0,   3.0,   4.0,   5.0],
    })

    df_5m = fetcher.resample(df_1m, "5m")

    assert len(df_5m) == 1
    row = df_5m.iloc[0]
    assert row["high"]   == 109.0  # max of all 5 highs
    assert row["low"]    == 95.0   # min of all 5 lows
    assert row["open"]   == 100.0  # first open
    assert row["close"]  == 105.0  # last close
    assert row["volume"] == 15.0   # sum of volumes


def test_fetch_handles_network_error_gracefully(fetcher, monkeypatch):
    """On NetworkError, fetcher retries; when exchange succeeds it returns data."""
    import ccxt as real_ccxt

    call_count = [0]
    # Anchor candles 2 days in the past; query 3 days so no boundary issue
    candles    = _make_candles(300, _days_ago_ms(2))

    def mock_fetch_ohlcv(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise real_ccxt.NetworkError("simulated timeout")
        if call_count[0] == 2:
            return candles
        return []  # stop pagination on 3rd call

    fetcher.exchange.fetch_ohlcv.side_effect = mock_fetch_ohlcv
    # Suppress the 5s retry sleep
    monkeypatch.setattr("data.fetcher.time.sleep", lambda _: None)

    df = fetcher.fetch(timeframe="1m", lookback_days=3)

    assert len(df) == 300
    assert call_count[0] >= 2   # retried at least once after the error
