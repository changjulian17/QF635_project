"""
tests/test_bt_metrics.py
========================
Unit tests for backtesting/metrics.py — calculate_metrics, BacktestMetrics,
composite_score, and passes_minimum_bar.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtesting.metrics import BacktestMetrics, calculate_metrics


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_equity(daily_returns: list[float], start: float = 10_000.0) -> pd.Series:
    """Build a daily DatetimeIndex equity curve from a list of daily returns."""
    n      = len(daily_returns) + 1
    dates  = pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC")
    vals   = [start]
    for r in daily_returns:
        vals.append(vals[-1] * (1.0 + r))
    return pd.Series(vals, index=dates)


def _trade(pnl: float, pnl_pct: float = None, duration_min: float = 60.0) -> dict:
    if pnl_pct is None:
        pnl_pct = pnl / 10_000.0
    return {"pnl": pnl, "pnl_pct": pnl_pct, "duration_minutes": duration_min}


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_sharpe_matches_manual_calculation():
    """Sharpe from calculate_metrics must equal the same formula applied manually."""
    rets   = np.tile([0.003, -0.001], 15)   # 30 alternating returns
    equity = _make_equity(rets.tolist())
    trades = [_trade(5.0)] * 20

    m = calculate_metrics(equity, trades, risk_free_rate=0.05)

    rfr_daily    = 0.05 / 365
    # Use pd.Series so .std() uses ddof=1, matching the code's pandas calculation
    excess        = pd.Series(rets) - rfr_daily
    sharpe_manual = float(excess.mean() / excess.std() * np.sqrt(365))

    assert abs(m.sharpe_ratio - sharpe_manual) < 1e-3


def test_max_drawdown_correct_on_known_curve():
    """Equity [100, 90, 80, 95] must produce MDD = 20%."""
    dates  = pd.date_range("2024-01-01", periods=4, freq="D", tz="UTC")
    equity = pd.Series([100.0, 90.0, 80.0, 95.0], index=dates)
    trades = [_trade(1.0)] * 10

    m = calculate_metrics(equity, trades)

    assert abs(m.max_drawdown_pct - 20.0) < 0.01


def test_profit_factor_ratio():
    """3 wins of $10, 2 losses of $5 → PF = 30 / 10 = 3.0."""
    equity = _make_equity([0.003] * 9)
    trades = [
        _trade( 10.0),
        _trade( 10.0),
        _trade( 10.0),
        _trade( -5.0),
        _trade( -5.0),
    ]

    m = calculate_metrics(equity, trades)

    assert abs(m.profit_factor - 3.0) < 1e-6


def test_composite_score_fewer_than_10_trades_returns_sentinel():
    """Any BacktestMetrics with total_trades < 10 must return composite_score = -999."""
    for n in [0, 1, 5, 9]:
        m = BacktestMetrics(
            total_trades=n,
            sharpe_ratio=2.0,
            profit_factor=2.0,
            calmar_ratio=1.0,
        )
        assert m.composite_score() == -999.0, (
            f"Expected -999 for {n} trades, got {m.composite_score()}"
        )


def test_passes_minimum_bar_all_thresholds():
    """Strategy meeting all 5 criteria must pass the minimum bar."""
    m = BacktestMetrics(
        total_trades   = 20,
        sharpe_ratio   = 1.0,
        max_drawdown_pct = 14.9,
        profit_factor  = 1.3,
        expected_value = 1.0,
    )
    assert m.passes_minimum_bar()


def test_fails_minimum_bar_on_low_sharpe():
    """Sharpe 0.8 → fails even if all other metrics pass."""
    m = BacktestMetrics(
        total_trades   = 20,
        sharpe_ratio   = 0.8,
        max_drawdown_pct = 14.9,
        profit_factor  = 1.3,
        expected_value = 1.0,
    )
    assert not m.passes_minimum_bar()


def test_fails_minimum_bar_on_high_drawdown():
    """Max drawdown > 15% → fails even if Sharpe and PF pass."""
    m = BacktestMetrics(
        total_trades   = 20,
        sharpe_ratio   = 1.5,
        max_drawdown_pct = 15.1,
        profit_factor  = 1.5,
        expected_value = 1.0,
    )
    assert not m.passes_minimum_bar()


def test_empty_trade_list_handled_gracefully():
    """Zero trades must return empty metrics — no exception raised."""
    equity = _make_equity([0.001] * 9)
    m = calculate_metrics(equity, [])

    assert m.total_trades == 0
    assert m.sharpe_ratio == 0.0
    assert m.profit_factor == 0.0


def test_buy_and_hold_benchmark_metrics():
    """Monotonically rising equity curve must produce positive total return."""
    equity = _make_equity([0.002] * 29)   # 1 month of +0.2%/day
    trades = [_trade(200.0)] * 20

    m = calculate_metrics(equity, trades, strategy_name="BUY_AND_HOLD")

    assert m.total_return_pct > 0.0
    assert m.annualised_return_pct > 0.0
    assert m.max_drawdown_pct == 0.0      # no drawdown on rising curve


def test_sortino_cap_proportional_to_sharpe():
    """
    M4: when there are no downside days, Sortino must be capped at
    min(3 × |Sharpe|, 10.0), not a flat 10.0.
    Uses a curve with variance (so Sharpe is computable) but no days
    below the risk-free rate (so down_std = 0 and the cap is applied).
    """
    # Vary returns so std > 0 (needed for a nonzero Sharpe), but keep every
    # return well above rfr_daily (~0.014%/day) so down_std stays zero.
    returns = [0.005 if i % 3 != 0 else 0.010 for i in range(59)]
    equity  = _make_equity(returns)
    trades  = [_trade(500.0)] * 25

    m = calculate_metrics(equity, trades)

    assert m.sharpe_ratio > 0, (
        f"Sharpe must be positive for a rising equity curve, got {m.sharpe_ratio}"
    )
    expected_cap = min(3.0 * abs(m.sharpe_ratio), 10.0)
    assert abs(m.sortino_ratio - expected_cap) < 1e-6, (
        f"Sortino={m.sortino_ratio:.4f} expected cap={expected_cap:.4f} "
        f"(Sharpe={m.sharpe_ratio:.4f})"
    )
