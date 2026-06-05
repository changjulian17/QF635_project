"""
tests/test_bt_vectorbt.py
=========================
Unit tests for backtesting/vectorbt_runner.py — run_phase1_optimisation
and the supporting Optuna/VectorBT infrastructure.

These tests verify structural correctness (params schema, study object, trial
count) rather than exact metric values, which are non-deterministic.
"""

from __future__ import annotations

import numpy as np
import optuna
import pandas as pd
import pytest

from backtesting.vectorbt_runner import (
    OptimisationResult,
    run_phase1_optimisation,
    _run_vectorbt,
    _sample_params,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def synthetic_df() -> pd.DataFrame:
    """
    1440-bar (1-day) 1m synthetic OHLCV with high enough volatility that
    SL/TP levels are reliably hit within a few dozen bars — guaranteeing
    closed trades for the Optuna objective to score.
    """
    rng   = np.random.default_rng(seed=42)
    n     = 1440
    idx   = pd.date_range("2024-01-01", periods=n, freq="1min", tz="UTC")
    price = 50_000.0
    rows  = []
    for i in range(n):
        ret = rng.normal(0.0, 0.004)          # 0.4%/bar → ATR ≈ 400 pts
        o   = price
        c   = price * (1.0 + ret)
        h   = max(o, c) * (1.0 + abs(rng.uniform(0.0, 0.002)))
        l   = min(o, c) * (1.0 - abs(rng.uniform(0.0, 0.002)))
        v   = abs(rng.normal(50.0, 20.0))
        rows.append({
            "timestamp": i * 60_000,
            "open": o, "high": h, "low": l, "close": c, "volume": v,
        })
        price = c
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_optimisation_returns_valid_params(synthetic_df):
    """20-trial run must return best_params containing all 8 required keys."""
    result = run_phase1_optimisation(
        strategy       = "Support / Resistance Breakout",
        train_df       = synthetic_df,
        timeframe      = "1m",
        n_trials       = 20,
        timeout_seconds= 120,
    )

    required = {
        "swing_window", "pattern_lookback", "min_r2",
        "breakout_vol_mult", "touch_tolerance_atr",
        "atr_multiplier_sl", "atr_multiplier_tp", "atr_period",
    }
    assert required.issubset(result.best_params.keys()), (
        f"Missing keys: {required - set(result.best_params.keys())}"
    )
    assert result.strategy == "Support / Resistance Breakout"
    assert result.timeframe == "1m"


def test_best_score_is_finite(synthetic_df):
    """best_score must be a finite float — NaN or ±inf would indicate a bug."""
    result = run_phase1_optimisation(
        strategy       = "Support / Resistance Breakout",
        train_df       = synthetic_df,
        timeframe      = "1m",
        n_trials       = 10,
        timeout_seconds= 60,
    )

    assert np.isfinite(result.best_score), (
        f"Non-finite best_score: {result.best_score!r}"
    )


def test_study_has_correct_n_trials(synthetic_df):
    """Completed trials must not exceed the requested n_trials budget."""
    n = 15
    result = run_phase1_optimisation(
        strategy       = "Falling Wedge",
        train_df       = synthetic_df,
        timeframe      = "1m",
        n_trials       = n,
        timeout_seconds= 60,
    )

    completed = sum(
        1 for t in result.study.trials
        if t.state == optuna.trial.TrialState.COMPLETE
    )
    assert completed <= n
    assert isinstance(result.study, optuna.Study)


def test_sample_params_contains_all_keys():
    """_sample_params must always return the full 8-key parameter dict."""
    study  = optuna.create_study(direction="maximize")
    trial  = study.ask()
    params = _sample_params(trial)

    required = {
        "swing_window", "pattern_lookback", "min_r2",
        "breakout_vol_mult", "touch_tolerance_atr",
        "atr_multiplier_sl", "atr_multiplier_tp", "atr_period",
    }
    assert required == set(params.keys())
    # All values must be numeric
    for k, v in params.items():
        assert isinstance(v, (int, float)), f"{k}={v!r} is not numeric"


def test_run_vectorbt_returns_metrics(synthetic_df):
    """_run_vectorbt with valid params must return a BacktestMetrics (no exception)."""
    params = {
        "swing_window": 5, "pattern_lookback": 50, "min_r2": 0.75,
        "breakout_vol_mult": 1.2, "touch_tolerance_atr": 0.5,
        "atr_multiplier_sl": 1.5, "atr_multiplier_tp": 3.0, "atr_period": 14,
    }

    metrics = _run_vectorbt("Support / Resistance Breakout", synthetic_df, params)

    assert metrics.strategy_name == "Support / Resistance Breakout"
    assert metrics.total_return_pct == metrics.total_return_pct   # not NaN


def test_valid_mask_filters_inverted_sl_tp(synthetic_df):
    """
    M1: entries where TP is on the wrong side of entry_approx (inverted stops)
    must be removed by valid_mask before reaching VectorBT, not crash the run.
    """
    from unittest.mock import patch
    from backtesting.signals import SignalArrays
    import numpy as np

    n = len(synthetic_df)
    # Craft signal arrays where every entry has LONG-style TP but SL *above* entry
    # (inverted — SL should be below for a LONG), so the valid_mask should neutralise them.
    entries  = np.zeros(n, dtype=bool)
    sl_stop  = np.full(n, np.nan)
    tp_stop  = np.full(n, np.nan)
    atr_arr  = np.full(n, 100.0)
    close    = synthetic_df["close"].values

    for i in range(50, n, 100):
        entries[i] = True
        tp_stop[i] = close[i] + 500.0   # TP above entry → LONG
        sl_stop[i] = close[i] + 800.0   # SL also above entry → INVERTED (invalid)

    bad_arrays = SignalArrays(entries=entries, sl_stop=sl_stop, tp_stop=tp_stop, atr=atr_arr)

    params = {
        "swing_window": 5, "pattern_lookback": 50, "min_r2": 0.75,
        "breakout_vol_mult": 1.2, "touch_tolerance_atr": 0.5,
        "atr_multiplier_sl": 1.5, "atr_multiplier_tp": 3.0, "atr_period": 14,
    }

    with patch("backtesting.vectorbt_runner.generate_signals", return_value=bad_arrays):
        # Must not raise — valid_mask should zero out all the inverted entries
        metrics = _run_vectorbt("Support / Resistance Breakout", synthetic_df, params)

    # All inverted entries suppressed → VBT sees no valid entries → empty metrics
    assert metrics.total_trades == 0, (
        f"Expected 0 trades (all entries filtered), got {metrics.total_trades}"
    )
