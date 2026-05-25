"""
tests/test_validator.py
=======================
Unit tests for data/validator.py — validate_ohlcv, repair_ohlcv,
ValidationReport, and all 9 individual check functions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data.validator import validate_ohlcv, repair_ohlcv, ValidationReport


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_valid_df(n: int = 300) -> pd.DataFrame:
    """Generate a clean, gap-free OHLCV DataFrame with n candles."""
    rng   = np.random.default_rng(seed=0)
    ts0   = 1_700_000_000_000   # fixed base timestamp (ms)
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


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_valid_dataframe_passes_all_9_checks():
    """Clean 300-candle df must return ValidationReport.is_valid = True with 9 checks."""
    df     = _make_valid_df(300)
    report = validate_ohlcv(df, symbol="BTCUSDT", timeframe="1m")

    assert report.is_valid
    assert len(report.checks) == 9
    assert report.n_candles == 300
    assert len(report.errors) == 0


def test_null_values_cause_row_removal_after_repair():
    """Row with NaN close must be removed by repair_ohlcv (non-positive price mask)."""
    df         = _make_valid_df(300)
    df.loc[50, "close"] = np.nan

    # Validation must flag the null
    report = validate_ohlcv(df)
    null_check = next(c for c in report.checks if "null" in c.name.lower())
    assert not null_check.passed

    # Repair removes the offending row
    repaired = repair_ohlcv(df)
    assert repaired["close"].isna().sum() == 0
    assert len(repaired) == 299


def test_duplicate_timestamps_removed_after_repair():
    """Two rows with the same timestamp → repair keeps first, drops second."""
    df      = _make_valid_df(300)
    dup_row = df.iloc[10:11].copy()
    df      = pd.concat([df, dup_row], ignore_index=True)
    assert df["timestamp"].duplicated().sum() == 1

    repaired = repair_ohlcv(df)
    assert repaired["timestamp"].duplicated().sum() == 0
    assert len(repaired) == 300   # duplicate removed, back to 300


def test_ohlc_consistency_enforced_by_repair():
    """Injected high < open violation must be clamped to valid range after repair."""
    df = _make_valid_df(300)
    # Force a violation: set high well below open
    df.loc[100, "high"] = df.loc[100, "open"] * 0.90

    # Validator must flag it
    report = validate_ohlcv(df)
    ohlc_check = next(c for c in report.checks if "ohlc" in c.name.lower())
    assert not ohlc_check.passed

    # After repair, high >= max(open, close) everywhere
    repaired = repair_ohlcv(df)
    violations = (repaired["high"] < repaired[["open", "close"]].max(axis=1)).sum()
    assert violations == 0


def test_minimum_candle_count_error():
    """Fewer than 200 candles must produce ERROR severity (abort-level)."""
    df     = _make_valid_df(50)
    report = validate_ohlcv(df, min_candles=200)

    assert not report.is_valid
    min_check = next(c for c in report.checks if "minimum" in c.name.lower())
    assert not min_check.passed
    assert min_check.severity == "ERROR"
    # Error list must contain this check
    assert any("minimum" in e.name.lower() for e in report.errors)


def test_extreme_move_warning_not_error():
    """>10% single-candle move is WARNING only — report.is_valid stays True."""
    df = _make_valid_df(300)
    # Inject a 20% close move; fix high/low so OHLC stays consistent
    prev_close = float(df.loc[49, "close"])
    big_close  = prev_close * 1.20
    df.loc[50, "close"] = big_close
    df.loc[50, "open"]  = prev_close
    df.loc[50, "high"]  = big_close * 1.001
    df.loc[50, "low"]   = prev_close * 0.999

    report = validate_ohlcv(df, max_single_move_pct=10.0)

    assert report.is_valid, "Extreme move should be WARNING, not ERROR"
    extreme_check = next(c for c in report.checks if "extreme" in c.name.lower())
    assert not extreme_check.passed
    assert extreme_check.severity == "WARNING"


def test_gap_detection_warning_logged():
    """A 5× expected interval gap must trigger a WARNING (not ERROR)."""
    df = _make_valid_df(300)
    # Inject a gap of 6 minutes between candles 99 and 100 (expected = 1 min)
    df.loc[100:, "timestamp"] += 5 * 60_000   # shift rest forward by 5 min

    report = validate_ohlcv(df, max_gap_multiplier=5.0)

    gap_check = next(c for c in report.checks if "gap" in c.name.lower())
    assert not gap_check.passed
    assert gap_check.severity == "WARNING"
    # Warning should not make the report invalid
    assert report.is_valid
