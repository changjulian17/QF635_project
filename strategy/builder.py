"""
strategy/builder.py
===================
StrategyBuilder — 3-stage pipeline that produces a deployable StrategySpec
from pre-computed walk-forward metrics.

Stage 1: Load  — accept OOS metrics dict
Stage 2: Validate — check minimum viability (trade count, no NaN Sharpe)
Stage 3: Register — construct StrategySpec and register with StrategyRegistry

Connection to backtest_results.db is TBD (Phase 2F/2G completion required).
"""

from __future__ import annotations

import math
from typing import Optional

from strategy.spec import EntryRules, StatisticalValidity, StrategySpec

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

        sharpe = metrics["sharpe_oos"]
        if math.isnan(float(sharpe)):
            raise ValueError("sharpe_oos is NaN — backtest produced no valid returns.")

    def _next_version(self, name: str) -> int:
        """Return 1 if no existing spec, or max(existing versions) + 1."""
        if self._registry is None:
            return 1
        max_ver = self._registry._max_version(name)
        return (max_ver + 1) if max_ver is not None else 1
