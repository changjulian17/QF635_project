"""
backtesting/metrics.py
======================
Full performance metrics suite for CryptoSentinel backtesting.

Metrics Computed
----------------
  Sharpe Ratio      — risk-adjusted return vs. zero (annualised)
  Sortino Ratio     — like Sharpe but only penalises downside volatility
  Calmar Ratio      — annualised return / max drawdown
  Max Drawdown      — largest peak-to-trough equity decline (%)
  Max DD Duration   — longest time spent below the previous equity peak
  Profit Factor     — gross profit / gross loss
  Win Rate          — % of trades that are profitable
  Expected Value    — average PnL per trade in dollar terms
  Recovery Factor   — total return / max drawdown
  Avg Win / Loss    — average winning and losing trade (%)
  Avg Duration      — average trade holding time (minutes)
  Total Return      — equity growth over the full period (%)
  Annualised Return — compound annual growth rate (%)

Minimum Bar
-----------
Strategies that fail passes_minimum_bar() should not be deployed,
regardless of how good they look on other metrics.

  total_trades   >= 20   (statistical significance)
  sharpe_ratio   >= 1.0
  max_drawdown   <= 15 %
  profit_factor  >= 1.3
  expected_value >  0

Usage
-----
>>> from backtesting.metrics import calculate_metrics
>>> metrics = calculate_metrics(equity_curve, trades, "Falling Wedge", "5m")
>>> print(metrics.summary())
>>> if metrics.passes_minimum_bar():
...     print("Strategy deployable")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Annual trading days — crypto trades 365 days a year
TRADING_DAYS_PER_YEAR = 365


# ─────────────────────────────────────────────────────────────────────────────
# Result Dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BacktestMetrics:
    """
    Complete set of performance metrics for one strategy / timeframe combo.

    All percentage values are stored as percentages (e.g. 5.0 = 5 %, not 0.05).
    """

    # Identity
    strategy_name:         str   = ""
    timeframe:             str   = ""
    period:                str   = ""   # e.g. "180d OOS"

    # Return metrics
    total_return_pct:      float = 0.0
    annualised_return_pct: float = 0.0

    # Risk-adjusted metrics
    sharpe_ratio:          float = 0.0
    sortino_ratio:         float = 0.0
    calmar_ratio:          float = 0.0

    # Drawdown
    max_drawdown_pct:      float = 0.0
    max_dd_duration_days:  float = 0.0

    # Trade statistics
    profit_factor:         float = 0.0
    win_rate_pct:          float = 0.0
    total_trades:          int   = 0
    avg_win_pct:           float = 0.0
    avg_loss_pct:          float = 0.0
    avg_trade_duration_min:float = 0.0
    expected_value:        float = 0.0   # avg PnL per trade in $
    recovery_factor:       float = 0.0   # total_return / max_dd

    # ── Derived helpers ───────────────────────────────────────────────────────

    def passes_minimum_bar(self) -> bool:
        """
        Return True if this strategy meets the minimum criteria for deployment.

        A strategy can pass on some metrics and fail on others. The minimum
        bar is intentionally conservative — it filters out strategies that
        look profitable only due to luck or overfitting.
        """
        return (
            self.total_trades          >= 20
            and self.sharpe_ratio      >= 1.0
            and self.max_drawdown_pct  <= 15.0
            and self.profit_factor     >= 1.3
            and self.expected_value    >  0.0
        )

    def composite_score(self) -> float:
        """
        Weighted composite score used by the strategy ranker and Optuna.

        Weights rationale:
          Sharpe (0.35)       — primary risk-adjusted return measure
          Profit Factor (0.25)— trade-level efficiency
          Calmar (0.20)       — return relative to worst-case drawdown
          Trade bonus (0.10)  — reward strategies with more trades (robustness)
          DD penalty (0.10)   — penalise drawdowns above 10%

        Returns -999 for strategies with < 10 trades (statistically meaningless).
        """
        if self.total_trades < 10:
            return -999.0

        trade_bonus   = min(1.0, self.total_trades / 50)
        dd_penalty    = max(0.0, (self.max_drawdown_pct - 10.0) / 10.0)
        # Cap Calmar at 3.0: short OOS windows (30d) annualise small returns
        # to very large Calmar values, corrupting the leaderboard ranking.
        calmar_capped = min(self.calmar_ratio, 3.0)

        return (
            self.sharpe_ratio    * 0.35
            + self.profit_factor * 0.25
            + calmar_capped      * 0.20
            + trade_bonus        * 0.10
            - dd_penalty         * 0.10
        )

    def summary(self) -> str:
        """One-line summary suitable for logging."""
        return (
            f"[{self.strategy_name} | {self.timeframe}] "
            f"Return={self.total_return_pct:+.1f}% "
            f"Sharpe={self.sharpe_ratio:.2f} "
            f"Sortino={self.sortino_ratio:.2f} "
            f"Calmar={self.calmar_ratio:.2f} "
            f"MaxDD={self.max_drawdown_pct:.1f}% "
            f"PF={self.profit_factor:.2f} "
            f"WR={self.win_rate_pct:.1f}% "
            f"Trades={self.total_trades} "
            f"EV=${self.expected_value:.2f} "
            f"Score={self.composite_score():.2f} "
            f"{'✓ PASS' if self.passes_minimum_bar() else '✗ FAIL'}"
        )

    def to_dict(self) -> dict:
        """Serialise to a flat dict for DataFrame / SQLite storage."""
        return {
            "strategy":             self.strategy_name,
            "timeframe":            self.timeframe,
            "period":               self.period,
            "total_return_pct":     self.total_return_pct,
            "annualised_return_pct":self.annualised_return_pct,
            "sharpe_ratio":         self.sharpe_ratio,
            "sortino_ratio":        self.sortino_ratio,
            "calmar_ratio":         self.calmar_ratio,
            "max_drawdown_pct":     self.max_drawdown_pct,
            "max_dd_duration_days": self.max_dd_duration_days,
            "profit_factor":        self.profit_factor,
            "win_rate_pct":         self.win_rate_pct,
            "total_trades":         self.total_trades,
            "avg_win_pct":          self.avg_win_pct,
            "avg_loss_pct":         self.avg_loss_pct,
            "avg_trade_duration_min":self.avg_trade_duration_min,
            "expected_value":       self.expected_value,
            "recovery_factor":      self.recovery_factor,
            "composite_score":      self.composite_score(),
            "passes_minimum_bar":   self.passes_minimum_bar(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main Calculator
# ─────────────────────────────────────────────────────────────────────────────

def calculate_metrics(
    equity_curve:    pd.Series,
    trades:          list[dict],
    strategy_name:   str   = "",
    timeframe:       str   = "1m",
    period:          str   = "",
    risk_free_rate:  float = 0.05,   # annual, e.g. 5% US T-bill
) -> BacktestMetrics:
    """
    Compute the full performance metrics suite.

    Parameters
    ----------
    equity_curve   : pd.Series of portfolio value, DatetimeIndex (UTC).
                     Must have at least 2 data points.
    trades         : list of dicts. Required keys per trade:
                       pnl           (float) — realised PnL in $
                       pnl_pct       (float) — PnL as fraction of equity at entry
                       duration_minutes (float)
    strategy_name  : Used in the report header only.
    timeframe      : Used in the report header only.
    period         : Description of the period, e.g. "90d OOS".
    risk_free_rate : Annual risk-free rate for Sharpe/Sortino calculation.

    Returns
    -------
    BacktestMetrics dataclass. All metrics are 0 / default if trades is empty.
    """
    if equity_curve is None or len(equity_curve) < 2 or not trades:
        logger.warning(
            "[Metrics] Insufficient data for %s %s — returning empty metrics.",
            strategy_name, timeframe,
        )
        return BacktestMetrics(
            strategy_name=strategy_name, timeframe=timeframe, period=period
        )

    # ── Daily returns ─────────────────────────────────────────────────────────
    daily_eq      = equity_curve.resample("1D").last().dropna()
    daily_returns = daily_eq.pct_change().dropna()

    if len(daily_returns) < 2:
        return BacktestMetrics(
            strategy_name=strategy_name, timeframe=timeframe, period=period
        )

    rfr_daily = risk_free_rate / TRADING_DAYS_PER_YEAR
    excess    = daily_returns - rfr_daily

    # ── Sharpe ────────────────────────────────────────────────────────────────
    ann_factor = np.sqrt(TRADING_DAYS_PER_YEAR)
    sharpe     = (
        (excess.mean() / excess.std() * ann_factor)
        if excess.std() > 1e-10 else 0.0
    )

    # ── Sortino (downside deviation only) ─────────────────────────────────────
    downside = daily_returns[daily_returns < 0]
    down_std = downside.std() if len(downside) > 1 else 0.0
    if down_std < 1e-9:
        # No negative returns — assign a capped value rather than ±inf
        sortino = 10.0 if excess.mean() > 0 else 0.0
    else:
        sortino = excess.mean() / down_std * ann_factor

    # ── Total & annualised return ─────────────────────────────────────────────
    initial_equity = equity_curve.iloc[0]
    final_equity   = equity_curve.iloc[-1]
    total_return   = (final_equity - initial_equity) / initial_equity

    total_days    = (equity_curve.index[-1] - equity_curve.index[0]).days
    ann_return    = (
        (1 + total_return) ** (TRADING_DAYS_PER_YEAR / max(total_days, 1)) - 1
    )

    # ── Max drawdown ─────────────────────────────────────────────────────────
    rolling_peak   = equity_curve.cummax()
    drawdown_curve = (equity_curve - rolling_peak) / rolling_peak
    max_dd         = float(abs(drawdown_curve.min()))

    # Max drawdown duration — vectorised to avoid O(n) Python iteration
    in_dd = drawdown_curve < -1e-8
    if in_dd.any():
        # Each time we leave a drawdown, the cumsum increments, labelling
        # each consecutive drawdown episode with a unique integer.
        episode_id = (~in_dd).cumsum()[in_dd]
        durations  = in_dd[in_dd].groupby(episode_id).apply(
            lambda g: g.index[-1] - g.index[0]
        )
        max_dd_dur = durations.max()
    else:
        max_dd_dur = timedelta(0)

    # ── Calmar ───────────────────────────────────────────────────────────────
    calmar = float(ann_return / max_dd) if max_dd > 1e-10 else 0.0

    # ── Recovery factor ───────────────────────────────────────────────────────
    recovery = float(total_return / max_dd) if max_dd > 1e-10 else 0.0

    # ── Trade-level stats ─────────────────────────────────────────────────────
    pnls     = [t["pnl"]     for t in trades]
    pnl_pcts = [t["pnl_pct"] for t in trades]
    wins     = [p for p in pnls     if p >  0]
    losses   = [p for p in pnls     if p <= 0]
    win_pcts = [p for p in pnl_pcts if p >  0]
    los_pcts = [p for p in pnl_pcts if p <= 0]

    n_trades      = len(trades)
    win_rate      = len(wins) / n_trades if n_trades > 0 else 0.0
    gross_profit  = sum(wins)
    gross_loss    = abs(sum(losses))
    profit_factor = (
        gross_profit / gross_loss
        if gross_loss > 1e-10 else (float("inf") if gross_profit > 0 else 0.0)
    )
    ev            = float(np.mean(pnls))   if pnls    else 0.0
    avg_win       = float(np.mean(win_pcts)) * 100 if win_pcts else 0.0
    avg_loss      = float(np.mean(los_pcts)) * 100 if los_pcts else 0.0
    avg_duration  = float(np.mean([t["duration_minutes"] for t in trades])) if trades else 0.0

    return BacktestMetrics(
        strategy_name          = strategy_name,
        timeframe              = timeframe,
        period                 = period,
        total_return_pct       = total_return * 100,
        annualised_return_pct  = ann_return   * 100,
        sharpe_ratio           = float(sharpe),
        sortino_ratio          = float(sortino),
        calmar_ratio           = calmar,
        max_drawdown_pct       = max_dd * 100,
        max_dd_duration_days   = max_dd_dur.days,
        profit_factor          = profit_factor,
        win_rate_pct           = win_rate * 100,
        total_trades           = n_trades,
        avg_win_pct            = avg_win,
        avg_loss_pct           = avg_loss,
        avg_trade_duration_min = avg_duration,
        expected_value         = ev,
        recovery_factor        = recovery,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Parameter Sensitivity Analysis
# ─────────────────────────────────────────────────────────────────────────────

def sensitivity_test(
    engine_factory,       # callable(params) → (equity_curve, trades)
    base_params:   dict,
    test_df:       pd.DataFrame,
    nudge_pcts:    list[float] = (-0.20, -0.10, 0.0, 0.10, 0.20),
) -> pd.DataFrame:
    """
    Test how sensitive Sharpe ratio is to ±20% changes in each parameter.

    A robust strategy should show < 30% Sharpe degradation across the
    nudge range. If a single ±10% parameter change collapses performance,
    the strategy has no genuine edge — it is curve-fitted.

    Parameters
    ----------
    engine_factory : Callable that accepts a params dict and returns
                     (equity_curve, trades). Use a lambda wrapping
                     EventDrivenEngine(strategy, params).run(test_df).
    base_params    : The best parameters from Optuna.
    test_df        : Out-of-sample test window DataFrame.
    nudge_pcts     : Parameter multipliers to test.

    Returns
    -------
    pd.DataFrame with rows = parameter nudges, columns = parameter names,
    values = Sharpe ratio at that nudge level.

    Example
    -------
    >>> factory = lambda p: EventDrivenEngine("Falling Wedge", p).run(test_df)
    >>> sens_df  = sensitivity_test(factory, best_params, test_df)
    >>> print(sens_df.to_string())
    """
    rows = []
    for param_name, base_val in base_params.items():
        if not isinstance(base_val, (int, float)):
            continue
        row = {"parameter": param_name, "base_value": base_val}
        for nudge in nudge_pcts:
            nudged         = {**base_params, param_name: base_val * (1 + nudge)}
            eq, tr         = engine_factory(nudged)
            m              = calculate_metrics(eq, tr)
            row[f"{nudge:+.0%}"] = round(m.sharpe_ratio, 3)
        rows.append(row)

    df = pd.DataFrame(rows).set_index("parameter")
    return df
