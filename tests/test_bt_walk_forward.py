"""
tests/test_bt_walk_forward.py
=============================
Unit tests for backtesting/walk_forward.py — generate_windows,
_chain_equity_curves, _compute_buy_and_hold, _save_results, and the
run_walk_forward orchestrator (integration test with mocked engines).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from backtesting.event_engine import SimulatedTrade
from backtesting.walk_forward import (
    WalkForwardWindow,
    _chain_equity_curves,
    _compute_buy_and_hold,
    _save_results,
    generate_windows,
    run_walk_forward,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_ohlcv_df(n: int, seed: int = 0) -> pd.DataFrame:
    """Minimal OHLCV DataFrame with a DatetimeIndex-compatible datetime column."""
    rng   = np.random.default_rng(seed)
    ts0   = 1_700_000_000_000
    price = 50_000.0
    rows  = []
    for i in range(n):
        o = price
        c = price * (1.0 + rng.uniform(-0.002, 0.002))
        h = max(o, c) * 1.001
        l = min(o, c) * 0.999
        v = abs(float(rng.normal(50, 10)))
        rows.append({"timestamp": ts0 + i * 60_000,
                     "open": o, "high": h, "low": l, "close": c, "volume": v})
        price = c
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def _daily_equity(values: list[float], start: str = "2024-01-01") -> pd.Series:
    """Build a daily DatetimeIndex equity Series from a list of values."""
    idx = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
    return pd.Series(values, index=idx, dtype=float)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_generate_windows_correct_splits():
    """
    180-day 1m dataset → train=90d, test=30d, step=30d should produce 3 windows.

    Window 0: train[0 : 129600],   test[129600 : 172800]
    Window 1: train[43200 : 172800], test[172800 : 216000]
    Window 2: train[86400 : 216000], test[216000 : 259200]
    """
    candles_per_day = 1440   # 1m candles
    windows = generate_windows(
        n_candles  = 180 * candles_per_day,   # 259,200
        train_days = 90,
        test_days  = 30,
        step_days  = 30,
        timeframe  = "1m",
    )

    assert len(windows) == 3

    # First window anchors correctly
    assert windows[0].window_idx  == 0
    assert windows[0].train_start == 0
    assert windows[0].train_end   == 90 * candles_per_day
    assert windows[0].test_start  == 90 * candles_per_day
    assert windows[0].test_end    == 120 * candles_per_day

    # Windows step forward by step_days
    assert windows[1].train_start == 30 * candles_per_day
    assert windows[2].train_start == 60 * candles_per_day


def test_test_windows_non_overlapping():
    """Every adjacent pair of test windows must not share a single candle index."""
    windows = generate_windows(
        n_candles=180 * 1440, train_days=90, test_days=30,
        step_days=30, timeframe="1m",
    )
    for a, b in zip(windows[:-1], windows[1:]):
        assert a.test_end <= b.test_start, (
            f"Overlap between window {a.window_idx} and {b.window_idx}: "
            f"{a.test_end} > {b.test_start}"
        )


def test_train_end_equals_test_start():
    """For every window, train slice ends exactly where test slice begins (no gap)."""
    windows = generate_windows(
        n_candles=180 * 1440, train_days=90, test_days=30,
        step_days=30, timeframe="1m",
    )
    for w in windows:
        assert w.train_end == w.test_start, (
            f"Window {w.window_idx}: gap between train_end={w.train_end} "
            f"and test_start={w.test_start}"
        )


def test_generate_windows_returns_empty_when_too_short():
    """Dataset smaller than one train+test window must return an empty list."""
    windows = generate_windows(
        n_candles=100, train_days=90, test_days=30,
        step_days=30, timeframe="1m",
    )
    assert windows == []


def test_chain_equity_curves_no_discontinuity():
    """
    Two curves scaled end-to-end must be continuous at the junction.
    Curve A: [10000 → 11000], curve B: [10000 → 9000].
    After chaining, B is scaled by 11000/10000 = 1.1:
      chained[len_A - 1] == 11000  (last of A)
      chained[len_A]     == 11000  (first of scaled B == 11000*1.0)
    """
    a = _daily_equity([10_000.0, 11_000.0], "2024-01-01")
    b = _daily_equity([10_000.0,  9_000.0], "2024-01-03")

    chained = _chain_equity_curves([a, b], starting_equity=10_000.0)

    assert len(chained) == 4
    assert abs(chained.iloc[1] - 11_000.0) < 1e-6   # end of A
    # First value of scaled B == A's last value (junction is continuous)
    assert abs(chained.iloc[2] - 11_000.0) < 1e-6


def test_chain_equity_curves_scale_factor():
    """
    Curve B is scaled by (A_end / B_start) so the compound return is correct.
    A: 10000→12000 (+20%).  B: 10000→8000 (−20%).
    Scaled B: 12000→9600.   Net chained return: 9600/10000 − 1 = −4%.
    """
    a = _daily_equity([10_000.0, 12_000.0], "2024-01-01")
    b = _daily_equity([10_000.0,  8_000.0], "2024-01-03")

    chained = _chain_equity_curves([a, b], starting_equity=10_000.0)

    net_return = (chained.iloc[-1] - chained.iloc[0]) / chained.iloc[0]
    assert abs(net_return - (-0.04)) < 1e-6


def test_buy_and_hold_benchmark_row():
    """
    _compute_buy_and_hold must return a dict with is_benchmark=True,
    strategy='BUY_AND_HOLD', and positive total_return_pct on rising price.
    """
    df  = _make_ohlcv_df(200)
    # Override datetime so data spans 200 days — calculate_metrics requires
    # at least 2 daily returns (3+ days), so minute-level data (only ~3h) fails.
    df["datetime"] = pd.date_range("2024-01-01", periods=200, freq="D", tz="UTC")
    # Force a rising price so total_return_pct > 0
    df["close"] = np.linspace(50_000, 55_000, 200)
    df["open"]  = np.linspace(49_999, 54_999, 200)

    bah = _compute_buy_and_hold(df, starting_equity=10_000.0, timeframe="1m")

    assert bah.get("is_benchmark") is True
    assert bah["strategy"] == "BUY_AND_HOLD"
    assert bah["total_return_pct"] > 0.0


def test_save_results_to_db_and_csv(tmp_path):
    """_save_results must write a 'results' table to SQLite and a CSV file,
    with best_params JSON-serialised so it is queryable in both stores."""
    df = pd.DataFrame({
        "strategy":         ["Falling Wedge", "BUY_AND_HOLD"],
        "timeframe":        ["1m", "1m"],
        "sharpe_ratio":     [1.5, 0.8],
        "total_return_pct": [12.0, 7.0],
        "best_params":      [{"min_r2": 0.8}, {}],
    })

    db_path  = str(tmp_path / "results.db")
    csv_path = str(tmp_path / "results.csv")

    _save_results(df, db_path=db_path, csv_path=csv_path)

    # SQLite must have a 'results' table with the right row count
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        bp   = conn.execute(
            "SELECT best_params FROM results WHERE strategy='Falling Wedge'"
        ).fetchone()[0]
    assert rows == 2
    # best_params must be a parseable JSON string in SQLite
    assert json.loads(bp) == {"min_r2": 0.8}

    # CSV must be readable and best_params must survive as parseable JSON
    df_csv = pd.read_csv(csv_path)
    assert "strategy"    in df_csv.columns
    assert "best_params" in df_csv.columns
    assert len(df_csv) == 2
    assert json.loads(df_csv.loc[0, "best_params"]) == {"min_r2": 0.8}


# ─────────────────────────────────────────────────────────────────────────────
# Integration test — run_walk_forward with mocked engines
# ─────────────────────────────────────────────────────────────────────────────

def _make_trade(pnl: float = 50.0, pnl_pct: float = 0.005) -> SimulatedTrade:
    """Build a minimal closed SimulatedTrade for use in mock engines."""
    now = datetime(2023, 11, 14, tzinfo=timezone.utc)
    t = SimulatedTrade(
        strategy    = "Falling Wedge",
        side        = "LONG",
        entry_bar   = 1,
        entry_time  = now,
        entry_price = 50_000.0,
        quantity    = 0.01,
        stop_loss   = 49_000.0,
        take_profit = 53_000.0,
    )
    t.exit_bar        = 10
    t.exit_time       = now
    t.exit_price      = 53_000.0
    t.exit_reason     = "TP"
    t.pnl             = pnl
    t.pnl_pct         = pnl_pct
    t.duration_minutes= 30.0
    return t


def test_run_walk_forward_oos_aggregation(tmp_path):
    """
    run_walk_forward must aggregate metrics across OOS test windows only.

    Strategy: mock EventDrivenEngine to return a known equity curve and
    15 profitable trades per window, then verify:
      1. total_trades == n_windows × 15 (all test windows aggregated).
      2. total_return_pct > 0 (profitable mock trades reflected).
      3. The timestamps passed to each mock engine call are from the test
         slice, not the training slice (OOS separation check).
    """
    # 6 days of 1m OHLCV → train=2d, test=1d, step=1d → 4 windows
    # (train [0,2880), test [2880,4320); step → train [1440,4320), test [4320,5760) …)
    candles_per_day = 1440
    n = 6 * candles_per_day
    ts0 = 1_700_000_000_000
    dates = pd.date_range("2023-11-14", periods=n, freq="1min", tz="UTC")

    df = pd.DataFrame({
        "timestamp": [ts0 + i * 60_000 for i in range(n)],
        "open":   50_000.0,
        "high":   50_100.0,
        "low":    49_900.0,
        "close":  50_000.0,
        "volume": 1.0,
    })
    df["datetime"] = dates

    # Track test_df start-timestamps to verify OOS separation
    seen_test_ts0: list[int] = []
    trades_per_window = 15

    def _make_fake_engine(strategy, params, raw_mode=True,
                          cost_model=None, starting_equity=10_000.0):
        class _FakeEngine:
            def run(self, test_df):
                seen_test_ts0.append(int(test_df["timestamp"].iloc[0]))
                idx = pd.DatetimeIndex(pd.to_datetime(test_df["datetime"]))
                eq  = pd.Series(
                    np.linspace(starting_equity, starting_equity * 1.05, len(test_df)),
                    index=idx,
                )
                return eq, [_make_trade() for _ in range(trades_per_window)]
        return _FakeEngine()

    mock_opt = MagicMock()
    mock_opt.best_params = {
        "swing_window": 5, "pattern_lookback": 50, "min_r2": 0.8,
        "breakout_vol_mult": 1.5, "touch_tolerance_atr": 0.5,
        "atr_multiplier_sl": 1.5, "atr_multiplier_tp": 3.0, "atr_period": 14,
    }

    mock_fetcher = MagicMock()
    mock_fetcher.resample.return_value = df

    with patch("backtesting.walk_forward.run_phase1_optimisation",
               return_value=mock_opt), \
         patch("backtesting.walk_forward.EventDrivenEngine", _make_fake_engine):

        results = run_walk_forward(
            df_1m           = df,
            fetcher         = mock_fetcher,
            strategies      = ["Falling Wedge"],
            timeframes      = ["1m"],
            train_days      = 2,
            test_days       = 1,
            step_days       = 1,
            n_optuna_trials = 1,
            db_path         = str(tmp_path / "r.db"),
            csv_path        = str(tmp_path / "r.csv"),
        )

    # Strategy rows have NaN for is_benchmark; benchmark rows have True.
    is_bm = results.get("is_benchmark", pd.Series(False, index=results.index, dtype=bool))
    strat = results[~is_bm.map(lambda x: x is True)]
    assert not strat.empty, "No strategy rows in results"

    # Aggregated trade count: n_windows × trades_per_window
    windows = generate_windows(
        n_candles  = n,
        train_days = 2,
        test_days  = 1,
        step_days  = 1,
        timeframe  = "1m",
    )
    expected_trades = len(windows) * trades_per_window
    assert int(strat.iloc[0]["total_trades"]) == expected_trades, (
        f"Expected {expected_trades} trades, got {strat.iloc[0]['total_trades']}"
    )

    # Profitable mock → positive return
    assert strat.iloc[0]["total_return_pct"] > 0

    # OOS separation: every test call must start at a test-window boundary,
    # not inside a training window.
    expected_test_ts = {ts0 + w.test_start * 60_000 for w in windows}
    # Each window triggers raw + full → seen_test_ts0 has 2× entries per window
    seen_unique = set(seen_test_ts0)
    assert seen_unique == expected_test_ts, (
        f"Engine was called with wrong slices.\n"
        f"Expected starts: {sorted(expected_test_ts)}\n"
        f"Seen starts:     {sorted(seen_unique)}"
    )
