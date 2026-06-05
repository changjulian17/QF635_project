"""
data/validator.py
=================
Pre-backtest data quality checks for OHLCV DataFrames.

Why This Matters
----------------
Backtesting on corrupt data produces misleading results that can be
difficult to diagnose. Common issues include:

  - Timestamp gaps (exchange downtime, API pagination errors)
  - OHLC violations (high < open, low > close — physically impossible)
  - Duplicate timestamps (double-fetched rows)
  - Zero or negative prices (bad API response)
  - Extreme single-candle moves (likely erroneous data spikes)

All checks run in O(n) time and complete in milliseconds even on
260,000-candle datasets. Run before every backtest execution.

Usage
-----
>>> from data.validator import validate_ohlcv, repair_ohlcv
>>> ok, issues = validate_ohlcv(df)
>>> if not ok:
...     df = repair_ohlcv(df)           # attempt auto-repair
...     ok, issues = validate_ohlcv(df) # re-validate after repair
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Result Types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    """
    Outcome of a single validation check.

    Attributes
    ----------
    name      : Human-readable check name.
    passed    : True if the check found no issues.
    severity  : "ERROR" aborts the backtest; "WARNING" is logged only.
    detail    : Description of the issue (empty if passed).
    row_count : Number of affected rows (0 if passed).
    """
    name:      str
    passed:    bool
    severity:  str = "ERROR"   # "ERROR" | "WARNING"
    detail:    str = ""
    row_count: int = 0


@dataclass
class ValidationReport:
    """Aggregated results from all checks."""
    symbol:    str
    timeframe: str
    n_candles: int
    checks:    list[ValidationResult] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True only if no ERROR-level checks failed."""
        return all(
            r.passed or r.severity == "WARNING"
            for r in self.checks
        )

    @property
    def errors(self) -> list[ValidationResult]:
        return [r for r in self.checks if not r.passed and r.severity == "ERROR"]

    @property
    def warnings(self) -> list[ValidationResult]:
        return [r for r in self.checks if not r.passed and r.severity == "WARNING"]

    def summary(self) -> str:
        lines = [
            f"Validation Report — {self.symbol} {self.timeframe} "
            f"({self.n_candles:,} candles)",
            f"  Status : {'✓ VALID' if self.is_valid else '✗ INVALID'}",
        ]
        for check in self.checks:
            icon = "✓" if check.passed else ("✗" if check.severity == "ERROR" else "⚠")
            lines.append(f"  {icon} {check.name}: {check.detail or 'OK'}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def validate_ohlcv(
    df: pd.DataFrame,
    symbol:    str = "BTCUSDT",
    timeframe: str = "1m",
    max_gap_multiplier:     float = 5.0,
    max_single_move_pct:    float = 10.0,
    min_candles:            int   = 200,
) -> ValidationReport:
    """
    Run all quality checks on an OHLCV DataFrame.

    Parameters
    ----------
    df                    : OHLCV DataFrame from OHLCVFetcher.fetch().
    symbol                : Symbol name for the report.
    timeframe             : Timeframe string for the report.
    max_gap_multiplier    : A gap > N × expected_interval triggers a warning.
    max_single_move_pct   : A single candle move > N% triggers a warning.
    min_candles           : Minimum required candles for meaningful backtesting.

    Returns
    -------
    ValidationReport
    """
    report = ValidationReport(
        symbol=symbol, timeframe=timeframe, n_candles=len(df)
    )

    report.checks.append(_check_min_candles(df, min_candles))
    report.checks.append(_check_required_columns(df))
    report.checks.append(_check_no_nulls(df))
    report.checks.append(_check_positive_prices(df))
    report.checks.append(_check_ohlc_consistency(df))
    report.checks.append(_check_duplicate_timestamps(df))
    report.checks.append(_check_timestamp_gaps(df, max_gap_multiplier))
    report.checks.append(_check_extreme_moves(df, max_single_move_pct))
    report.checks.append(_check_zero_volume(df))

    # Log summary
    if report.is_valid:
        logger.info("[Validator] %s", report.summary())
    else:
        logger.error("[Validator] %s", report.summary())

    return report


def repair_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attempt to auto-repair common data issues.

    Repairs applied (in order):
      1. Remove duplicate timestamps (keep first occurrence).
      2. Remove rows with non-positive or null prices.
      3. Fix OHLC violations by clamping high/low to valid ranges.
      4. Forward-fill small timestamp gaps (≤ 3 consecutive missing candles).
         Only genuine exchange gaps are filled — timestamps removed by steps
         1–2 are never re-introduced.

    Returns a repaired copy. Does NOT modify the input DataFrame.
    """
    df = df.copy()
    initial_len = len(df)

    # 1. Remove duplicates
    df = df.drop_duplicates(subset=["timestamp"], keep="first")

    # 2. Remove non-positive / null prices
    price_cols   = ["open", "high", "low", "close"]
    mask         = (df[price_cols] > 0).all(axis=1)
    removed_ts   = set(df.loc[~mask, "timestamp"].tolist())   # intentionally removed
    df           = df[mask]

    # 3. Fix OHLC violations
    df["high"] = df[["high", "open", "close"]].max(axis=1)
    df["low"]  = df[["low",  "open", "close"]].min(axis=1)

    # 4. Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)

    # 5. Forward-fill genuine exchange gaps (≤ 3 consecutive missing candles).
    #    Timestamps that were deliberately dropped in step 2 are excluded so
    #    we do not silently re-introduce rows with bad data.
    if len(df) > 1:
        # Use min positive diff rather than mode: during outages, mode() can
        # return 2×interval (most common diff becomes a 2-bar gap), making the
        # gap-fill grid twice as coarse and silently under-filling missing bars.
        _diffs = pd.Series(df["timestamp"].values).diff().dropna()
        tf_ms  = int(_diffs[_diffs > 0].min())

        full_ts = np.arange(
            int(df["timestamp"].iloc[0]),
            int(df["timestamp"].iloc[-1]) + tf_ms,
            tf_ms,
            dtype=np.int64,
        )

        df_idx = df.set_index("timestamp").reindex(full_ts)
        is_na  = df_idx["close"].isna()

        # Exclude timestamps that were intentionally removed
        is_intentional = pd.Index(full_ts).isin(removed_ts)
        is_gap         = is_na & ~is_intentional

        if is_gap.any():
            group_id = (is_gap != is_gap.shift()).cumsum()
            run_len  = is_gap.groupby(group_id).transform("sum")

            df_filled = df_idx.ffill()
            # Restore: intentional removals + gaps longer than 3 bars
            bad_mask = is_na & (is_intentional | (run_len > 3))
            df_filled[bad_mask] = np.nan

            n_filled = int((is_gap & (run_len <= 3)).sum())
            if n_filled:
                logger.info("[Validator] Repair forward-filled %d gap candle(s).", n_filled)

            df = df_filled.dropna(subset=["close"]).reset_index()
            df = df.rename(columns={"index": "timestamp"})
            df["timestamp"] = df["timestamp"].astype(np.int64)
            df["datetime"]  = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    removed = initial_len - len(df)
    if removed > 0:
        logger.info("[Validator] Repair removed %d invalid row(s).", removed)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Individual Checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_min_candles(df: pd.DataFrame, minimum: int) -> ValidationResult:
    n      = len(df)
    passed = n >= minimum
    return ValidationResult(
        name      = "Minimum candle count",
        passed    = passed,
        severity  = "ERROR",
        detail    = "" if passed else f"{n} candles < minimum {minimum}",
        row_count = 0 if passed else n,
    )


def _check_required_columns(df: pd.DataFrame) -> ValidationResult:
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing  = required - set(df.columns)
    passed   = len(missing) == 0
    return ValidationResult(
        name     = "Required columns present",
        passed   = passed,
        severity = "ERROR",
        detail   = "" if passed else f"Missing columns: {sorted(missing)}",
    )


def _check_no_nulls(df: pd.DataFrame) -> ValidationResult:
    cols    = ["open", "high", "low", "close", "volume", "timestamp"]
    nulls   = df[cols].isnull().sum().sum()
    passed  = nulls == 0
    return ValidationResult(
        name      = "No null values",
        passed    = passed,
        severity  = "ERROR",
        detail    = "" if passed else f"{nulls} null values found",
        row_count = int(nulls),
    )


def _check_positive_prices(df: pd.DataFrame) -> ValidationResult:
    price_cols = ["open", "high", "low", "close"]
    bad        = (df[price_cols] <= 0).any(axis=1).sum()
    passed     = bad == 0
    return ValidationResult(
        name      = "Positive prices",
        passed    = passed,
        severity  = "ERROR",
        detail    = "" if passed else f"{bad} rows with non-positive price",
        row_count = int(bad),
    )


def _check_ohlc_consistency(df: pd.DataFrame) -> ValidationResult:
    bad_high = (df["high"] < df[["open", "close"]].max(axis=1)).sum()
    bad_low  = (df["low"]  > df[["open", "close"]].min(axis=1)).sum()
    total    = int(bad_high + bad_low)
    passed   = total == 0
    return ValidationResult(
        name      = "OHLC consistency",
        passed    = passed,
        severity  = "ERROR",
        detail    = (
            "" if passed
            else f"{bad_high} high violations, {bad_low} low violations"
        ),
        row_count = total,
    )


def _check_duplicate_timestamps(df: pd.DataFrame) -> ValidationResult:
    dupes  = df["timestamp"].duplicated().sum()
    passed = dupes == 0
    return ValidationResult(
        name      = "No duplicate timestamps",
        passed    = passed,
        severity  = "ERROR",
        detail    = "" if passed else f"{dupes} duplicate timestamps",
        row_count = int(dupes),
    )


def _check_timestamp_gaps(
    df: pd.DataFrame, max_gap_multiplier: float
) -> ValidationResult:
    if len(df) < 2:
        return ValidationResult(name="Timestamp gaps", passed=True)

    diffs    = df["timestamp"].diff().dropna()
    expected = diffs.mode()[0]
    gaps     = diffs[diffs > expected * max_gap_multiplier]
    passed   = len(gaps) == 0
    detail   = ""
    if not passed:
        max_gap_min = gaps.max() / 60_000
        detail = (
            f"{len(gaps)} gaps detected "
            f"(largest: {max_gap_min:.1f} min = "
            f"{max_gap_min / 60:.1f} hours)"
        )
    return ValidationResult(
        name      = "Timestamp gaps",
        passed    = passed,
        severity  = "WARNING",   # gaps don't abort — just log
        detail    = detail,
        row_count = len(gaps),
    )


def _check_extreme_moves(
    df: pd.DataFrame, max_pct: float
) -> ValidationResult:
    pct_moves = df["close"].pct_change(fill_method=None).abs() * 100
    extreme   = (pct_moves > max_pct).sum()
    passed    = extreme == 0
    detail    = ""
    if not passed:
        worst = pct_moves.max()
        detail = (
            f"{extreme} candles with >  {max_pct}% move "
            f"(worst: {worst:.1f}%)"
        )
    return ValidationResult(
        name      = "Extreme price moves",
        passed    = passed,
        severity  = "WARNING",
        detail    = detail,
        row_count = int(extreme),
    )


def _check_zero_volume(df: pd.DataFrame) -> ValidationResult:
    zeros  = (df["volume"] == 0).sum()
    passed = zeros == 0
    return ValidationResult(
        name      = "Non-zero volume",
        passed    = passed,
        severity  = "WARNING",
        detail    = "" if passed else f"{zeros} zero-volume candles",
        row_count = int(zeros),
    )
