"""
strategy/builder.py
===================
StrategyBuilder — 3-stage pipeline that produces a deployable StrategySpec
from pre-computed walk-forward metrics.

Stage 1: Load  — accept OOS metrics dict
Stage 2: Validate — check minimum viability (trade count, Sharpe, drawdown, profit factor)
Stage 3: Register — construct StrategySpec and register with StrategyRegistry
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Optional

import pandas as pd

from strategy.spec import EntryRules, StatisticalValidity, StrategySpec

if TYPE_CHECKING:
    from backtesting.tick_replay import TickReplayEngine

logger = logging.getLogger(__name__)

# Import lazily to allow use without a registry (e.g. tests that only need build())
try:
    from strategy.registry import StrategyRegistry
except ImportError:
    StrategyRegistry = None  # type: ignore[assignment,misc]


class StrategyBuilder:
    """
    Produces a StrategySpec from walk-forward OOS metrics.

    Parameters
    ----------
    registry : Optional[StrategyRegistry]
        If supplied, build() registers the spec automatically.
        If None, the caller is responsible for registration.
    """

    MIN_OOS_TRADES = 10  # hard floor; promotion gate in StrategyRegistry enforces 50

    def __init__(self, registry: Optional["StrategyRegistry"] = None) -> None:
        self._registry = registry

    def build(
        self,
        strategy_name: str,
        timeframe: str,
        metrics: dict,
        entry_rules: Optional[EntryRules] = None,
        backtest_results_path: Optional[str] = None,
    ) -> StrategySpec:
        """
        Build and optionally register a BACKTEST-status StrategySpec.

        Parameters
        ----------
        strategy_name : str
            Human-readable name (e.g. "Sweep Protection").
        timeframe : str
            Candle timeframe used during backtesting (e.g. "1m", "5m").
            Appended to strategy_name as a label.
        metrics : dict
            Required keys:
              oos_trade_count  (int)
              sharpe_oos       (float)
              max_drawdown_pct (float)
              profit_factor    (float)
              win_rate_pct     (float)
              composite_score  (float)
        entry_rules : Optional[EntryRules]
            Optimised entry rule parameters. Defaults to EntryRules() if None.
        backtest_results_path : Optional[str]
            Path to the backtest results DB/CSV for audit trail.

        Returns
        -------
        StrategySpec with status="BACKTEST".

        Raises
        ------
        ValueError
            If metrics fail minimum viability checks.
        """
        self._validate(metrics)

        name = f"{strategy_name} [{timeframe}]"
        version = self._next_version(name)

        validity = StatisticalValidity(
            oos_trade_count  = int(metrics["oos_trade_count"]),
            sharpe_oos       = float(metrics["sharpe_oos"]),
            max_drawdown_pct = float(metrics["max_drawdown_pct"]),
            profit_factor    = float(metrics["profit_factor"]),
            win_rate_pct     = float(metrics["win_rate_pct"]),
            composite_score  = float(metrics["composite_score"]),
        )

        spec = StrategySpec(
            name                  = name,
            version               = version,
            status                = "BACKTEST",
            entry_rules           = entry_rules or EntryRules(),
            validity              = validity,
            backtest_results_path = backtest_results_path,
        )

        if self._registry is not None:
            self._registry.register(spec)

        return spec

    # ------------------------------------------------------------------ #
    # Walk-forward backtesting                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def walk_forward_metrics(
        engine: "TickReplayEngine",
        start_ms: int,
        end_ms: int,
        n_folds: int = 5,
    ) -> dict:
        """
        Rolling walk-forward: splits [start_ms, end_ms] into n_folds equal windows
        and runs replay_window on each independently. All OOS trades are pooled to
        produce stable aggregate metrics.

        Returns a metrics dict with all keys required by StrategyBuilder.build().

        Raises ValueError if the combined OOS result has no closed trades.

        Notes
        -----
        Each fold starts with a fresh (cold) feature state. For tick data with
        dense candle history this warms up within a few minutes of replay time,
        so the first 1-2 trades per fold may use slightly sparse ATR/RSI history.
        This is a conservative bias — it does not inflate backtest metrics.
        """
        if n_folds < 2:
            raise ValueError("n_folds must be >= 2 to produce meaningful OOS splits.")
        fold_ms = (end_ms - start_ms) // n_folds

        all_pnl_usd:  list[float]     = []
        eq_curves:    list[pd.Series] = []  # timestamped equity series for daily Sharpe

        for i in range(n_folds):
            fold_start = start_ms + i * fold_ms
            fold_end   = fold_start + fold_ms
            eq_curve, trades = engine.replay_window(fold_start, fold_end)

            for t in trades:
                all_pnl_usd.append(t.pnl_usd)

            if len(eq_curve) > 1:
                eq_curves.append(eq_curve)

            logger.info(
                "[WalkForward] Fold %d/%d: %d trades, fold equity Δ=%.2f",
                i + 1, n_folds, len(trades),
                (eq_curve.iloc[-1] - eq_curve.iloc[0]) if len(eq_curve) > 1 else 0.0,
            )

        if not all_pnl_usd:
            raise ValueError(
                "Walk-forward produced zero closed trades across all folds — "
                "extend the date range or review signal thresholds."
            )

        wins        = [p for p in all_pnl_usd if p > 0]
        losses      = [p for p in all_pnl_usd if p <= 0]
        n_total     = len(all_pnl_usd)
        win_rate    = len(wins) / n_total * 100.0
        gross_win   = sum(wins)
        gross_loss  = abs(sum(losses)) if losses else 0.0
        pf          = gross_win / gross_loss if gross_loss > 1e-9 else float("inf")

        # Max drawdown from the concatenated equity curve
        if eq_curves:
            full_equity = pd.concat(eq_curves).sort_index()
            rolling_peak  = full_equity.cummax()
            drawdown      = (full_equity - rolling_peak) / rolling_peak
            max_dd_pct    = float(abs(drawdown.min())) * 100.0
        else:
            max_dd_pct = 0.0

        # Sharpe from daily equity returns — consistent with metrics.py so that the
        # value stored in StrategySpec.validity.sharpe_oos matches the dashboard display.
        TRADING_DAYS_PER_YEAR = 365  # crypto markets run 24/7
        RISK_FREE_RATE_ANNUAL = 0.05
        rfr_daily = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_PER_YEAR
        if eq_curves:
            daily_eq = full_equity.resample("1D").last().dropna()
            daily_returns = daily_eq.pct_change().dropna()
            if len(daily_returns) > 1 and daily_returns.std() > 1e-10:
                excess    = daily_returns - rfr_daily
                ann_factor = math.sqrt(TRADING_DAYS_PER_YEAR)
                sharpe     = float(excess.mean() / excess.std() * ann_factor)
            else:
                sharpe = 0.0
        else:
            sharpe = 0.0

        # Composite score rewards Sharpe and profit factor, penalises drawdown.
        composite = sharpe * pf / (1.0 + max_dd_pct / 100.0) if math.isfinite(pf) else 0.0

        return {
            "oos_trade_count":  n_total,
            "sharpe_oos":       round(sharpe, 4),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "profit_factor":    round(pf, 4) if math.isfinite(pf) else 999.0,
            "win_rate_pct":     round(win_rate, 2),
            "composite_score":  round(composite, 4),
        }

    # ------------------------------------------------------------------ #
    # Internal helpers                                                      #
    # ------------------------------------------------------------------ #

    def _validate(self, metrics: dict) -> None:
        required = {
            "oos_trade_count", "sharpe_oos", "max_drawdown_pct",
            "profit_factor", "win_rate_pct", "composite_score",
        }
        missing = required - metrics.keys()
        if missing:
            raise ValueError(f"Missing required metric keys: {sorted(missing)}")

        trade_count = metrics["oos_trade_count"]
        if trade_count < self.MIN_OOS_TRADES:
            raise ValueError(
                f"oos_trade_count={trade_count} < {self.MIN_OOS_TRADES} minimum — "
                "no OOS trades to validate against."
            )

        sharpe = float(metrics["sharpe_oos"])
        if math.isnan(sharpe):
            raise ValueError("sharpe_oos is NaN — backtest produced no valid returns.")

        drawdown = float(metrics["max_drawdown_pct"])
        if drawdown < 0:
            raise ValueError(
                f"max_drawdown_pct={drawdown:.1f}% is negative — "
                "pass the absolute drawdown percentage."
            )

        pf = float(metrics["profit_factor"])
        if pf < 0:
            raise ValueError(
                f"profit_factor={pf:.3f} is negative — check gross loss calculation."
            )

    def _next_version(self, name: str) -> int:
        """Return 1 if no existing spec, or max(existing versions) + 1."""
        if self._registry is None:
            return 1
        max_ver = self._registry._max_version(name)
        return (max_ver + 1) if max_ver is not None else 1
