"""
tests/test_bt_signals.py
========================
Unit tests for backtesting/signals.py — signal generators, lookahead safety,
and false-positive rates across all 5 strategies.

The no-lookahead test is a HARD GATE (Phase 2D). If it fails the backtest
is architecturally invalid and must not proceed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtesting.signals import ALL_STRATEGIES, generate_signals


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_random_df(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """n-bar random walk OHLCV DataFrame with realistic volume variance."""
    price = 50_000.0
    rows  = []
    for i in range(n):
        ret = rng.normal(0.0, 0.003)
        o   = price
        c   = price * (1.0 + ret)
        h   = max(o, c) * (1.0 + abs(rng.uniform(0.0, 0.001)))
        l   = min(o, c) * (1.0 - abs(rng.uniform(0.0, 0.001)))
        v   = abs(rng.normal(50.0, 15.0))
        rows.append({
            "timestamp": i * 60_000,
            "open": o, "high": h, "low": l, "close": c, "volume": v,
        })
        price = c
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def _default_params() -> dict:
    return {
        "swing_window":        5,
        "pattern_lookback":   50,
        "min_r2":           0.80,
        "breakout_vol_mult":  1.5,
        "touch_tolerance_atr":0.5,
        "atr_multiplier_sl":  1.5,
        "atr_multiplier_tp":  3.0,
        "atr_period":        14,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_sr_breakout_detected_on_clear_signal():
    """S/R Breakout fires when close unambiguously exceeds rolling 95th percentile."""
    rng = np.random.default_rng(42)
    df  = _make_random_df(200, rng)

    # Inject an unambiguous breakout at bar 195:
    # close = 5× any previous close; volume = 10× average
    max_close = float(df["close"].iloc[:195].max())
    vol_mean  = float(df["volume"].mean())
    df.loc[195:, "close"]  = max_close * 5.0
    df.loc[195:, "high"]   = max_close * 5.1
    df.loc[195:, "low"]    = max_close * 4.9
    df.loc[195:, "volume"] = vol_mean  * 10.0

    arrays = generate_signals("Support / Resistance Breakout", df, _default_params())

    assert arrays.entries.sum() >= 1


def test_no_false_positives_on_random_walk():
    """Random walk (100 trials × 200 bars) false positive rate must be < 15%."""
    params  = _default_params()
    n_false = 0
    for seed in range(100):
        df     = _make_random_df(200, np.random.default_rng(seed))
        arrays = generate_signals("Falling Wedge", df, params)
        if arrays.entries.sum() > 0:
            n_false += 1

    fp_rate = n_false / 100
    assert fp_rate < 0.15, f"False positive rate {fp_rate:.0%} ≥ 15%"


def test_signal_no_lookahead():
    """
    CRITICAL GATE — Phase 2D hard gate.

    Appending 30 future candles must NOT change any signal at bars 0–49.

    Mechanism: pivot detection uses ±swing_window bars, but the per-bar
    filter (searchsorted hi < i - sw) ensures that when evaluating bar i,
    only confirmed pivots at indices < i - swing_window are used.
    Those pivots are determined by data within the original 50 bars, so
    adding future candles cannot alter them.

    If this test fails, ALL backtest results are invalid.
    """
    rng = np.random.default_rng(42)

    df_50 = _make_random_df(50, rng)

    # Build an 80-bar frame: original 50 bars + 30 fresh future bars
    extra = _make_random_df(30, np.random.default_rng(99))
    extra["timestamp"] += int(df_50["timestamp"].iloc[-1]) + 60_000
    extra["datetime"] = pd.to_datetime(extra["timestamp"], unit="ms", utc=True)
    df_80 = pd.concat([df_50, extra], ignore_index=True)

    params = _default_params()

    for strategy in ALL_STRATEGIES:
        a50 = generate_signals(strategy, df_50, params)
        a80 = generate_signals(strategy, df_80, params)

        assert np.array_equal(a50.entries, a80.entries[:50]), (
            f"LOOKAHEAD DETECTED in '{strategy}': "
            f"signals at bars 0–49 changed when 30 future bars were appended"
        )


def test_all_5_strategies_generate_without_exception():
    """All 5 strategies must run on 200 bars and return arrays of length 200."""
    rng    = np.random.default_rng(0)
    df     = _make_random_df(200, rng)
    params = _default_params()

    for strategy in ALL_STRATEGIES:
        arrays = generate_signals(strategy, df, params)
        assert len(arrays.entries) == 200
        assert len(arrays.sl_stop) == 200
        assert len(arrays.tp_stop) == 200
        assert len(arrays.atr)     == 200


def test_sl_tp_nan_where_no_entry():
    """SL/TP arrays must be NaN at every bar where entries is False."""
    rng    = np.random.default_rng(0)
    df     = _make_random_df(300, rng)
    params = _default_params()

    for strategy in ALL_STRATEGIES:
        arrays        = generate_signals(strategy, df, params)
        no_entry_mask = ~arrays.entries

        assert np.all(np.isnan(arrays.sl_stop[no_entry_mask])), (
            f"{strategy}: sl_stop is not NaN at non-entry bars"
        )
        assert np.all(np.isnan(arrays.tp_stop[no_entry_mask])), (
            f"{strategy}: tp_stop is not NaN at non-entry bars"
        )


def test_atr_strictly_positive_after_warmup():
    """ATR must be > 0 at every bar after the atr_period warm-up."""
    rng    = np.random.default_rng(0)
    df     = _make_random_df(200, rng)
    params = _default_params()

    arrays = generate_signals("Support / Resistance Breakout", df, params)

    warmup = int(params["atr_period"])
    assert np.all(arrays.atr[warmup:] > 0), (
        "ATR must be strictly positive after the warm-up period"
    )


def test_wedge_and_triangle_sl_tp_consistent_with_sr_breakout():
    """
    H3/H4: after the paired fixes, wedge and triangle SL distances must be
    computed from the same bar as the entry confirmation close.  We verify
    this indirectly: for a given ATR value, SL distance (close - sl_stop)
    must equal atr_sl × ATR to within floating-point tolerance — which holds
    only if current = closes[i] rather than closes[i-1].
    """
    rng    = np.random.default_rng(42)
    df     = _make_random_df(300, rng)
    params = _default_params()

    for strategy in ("Falling Wedge", "Rising Wedge", "Symmetrical Triangle"):
        arrays = generate_signals(strategy, df, params)
        entry_bars = np.where(arrays.entries)[0]
        if len(entry_bars) == 0:
            continue

        closes = df["close"].values
        atr_sl = float(params["atr_multiplier_sl"])

        for i in entry_bars:
            atr_i = arrays.atr[i]
            close_i = closes[i]
            sl_i    = arrays.sl_stop[i]
            tp_i    = arrays.tp_stop[i]
            # SL must be atr_sl × ATR away from closes[i], not closes[i-1]
            sl_dist = abs(close_i - sl_i)
            expected = atr_sl * atr_i
            assert abs(sl_dist - expected) < 1e-4 * atr_i, (
                f"{strategy} bar {i}: SL distance {sl_dist:.4f} ≠ "
                f"atr_sl×ATR = {expected:.4f} — current may still use closes[i-1]"
            )
