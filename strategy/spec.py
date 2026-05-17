"""
strategy/spec.py
================
StrategySpec dataclass and related types for the strategy lifecycle registry.

A StrategySpec captures the full configuration of a deployable strategy:
  - Entry rule parameters (thresholds that gate the 7-gate pipeline)
  - Statistical validity metrics from the walk-forward backtest
  - Lifecycle status (RESEARCH → BACKTEST → PAPER → LIVE)

The spec is serialised to YAML for human review and version-controlled
alongside the codebase. The registry enforces that a spec is frozen
once it reaches PAPER status — any change requires a version bump.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Entry Rules
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EntryRules:
    """
    Threshold parameters that govern the 7-gate strategy executor.

    These are the parameters that get optimised during the walk-forward
    backtest. They are frozen after PAPER promotion.
    """
    obi_threshold:    float = 0.25   # Gate 1: min OBI z-score alignment
    cvd_momentum_min: float = 0.8    # Gate 1: min CVD momentum z-score
    vol_ratio_min:    float = 1.4    # Gate 2: min volume ratio
    spread_max_bps:   float = 8.0    # Gate 4: max spread in basis points
    sweep_qty_mult:   float = 2.0    # microstructure: sweep qty multiplier
    atr_mult_sl:      float = 1.5    # risk: ATR multiplier for stop-loss
    atr_mult_tp:      float = 3.0    # risk: ATR multiplier for take-profit


# ─────────────────────────────────────────────────────────────────────────────
# Statistical Validity
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StatisticalValidity:
    """
    Out-of-sample performance metrics from walk-forward validation.

    Populated by StrategyBuilder after a successful backtest.
    Used by StrategyRegistry to gate promotion to PAPER.
    """
    oos_trade_count:  int   = 0      # total OOS trades across all WF windows
    sharpe_oos:       float = 0.0    # OOS Sharpe ratio (full-risk mode)
    max_drawdown_pct: float = 100.0  # OOS max drawdown %
    profit_factor:    float = 0.0    # OOS profit factor
    win_rate_pct:     float = 0.0    # OOS win rate %
    composite_score:  float = -999.0 # OOS composite score


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Spec
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StrategySpec:
    """
    Complete description of a versioned strategy.

    Lifecycle
    ---------
    RESEARCH  → BACKTEST → PAPER → LIVE

    A spec is uniquely identified by (name, version). The registry
    rejects duplicate (name, version) pairs. Once a spec reaches
    PAPER status, it is frozen — any parameter change requires
    incrementing the version field.

    Parameters
    ----------
    name        : Human-readable strategy name (e.g. "Falling Wedge v1").
    version     : Monotonically increasing integer. Bump for any change.
    status      : Current lifecycle stage.
    strategy_id : UUID assigned at registration (auto-generated).
    entry_rules : Threshold parameters for the 7-gate executor.
    validity    : Walk-forward OOS metrics (populated by StrategyBuilder).
    created_at  : ISO-8601 UTC timestamp of first registration.
    backtest_results_path : Path to the backtest results DB/CSV (if available).
    """
    name:                  str
    version:               int
    status:                Literal["RESEARCH", "BACKTEST", "PAPER", "LIVE"] = "RESEARCH"
    strategy_id:           str = field(
        default_factory=lambda: str(uuid.uuid4())
    )
    entry_rules:           EntryRules          = field(default_factory=EntryRules)
    validity:              StatisticalValidity = field(default_factory=StatisticalValidity)
    created_at:            str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    backtest_results_path: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialise to a flat dict suitable for YAML / SQLite storage."""
        return {
            "name":                  self.name,
            "version":               self.version,
            "status":                self.status,
            "strategy_id":           self.strategy_id,
            "created_at":            self.created_at,
            "backtest_results_path": self.backtest_results_path,
            # Entry rules
            "obi_threshold":         self.entry_rules.obi_threshold,
            "cvd_momentum_min":      self.entry_rules.cvd_momentum_min,
            "vol_ratio_min":         self.entry_rules.vol_ratio_min,
            "spread_max_bps":        self.entry_rules.spread_max_bps,
            "sweep_qty_mult":        self.entry_rules.sweep_qty_mult,
            "atr_mult_sl":           self.entry_rules.atr_mult_sl,
            "atr_mult_tp":           self.entry_rules.atr_mult_tp,
            # Statistical validity
            "oos_trade_count":       self.validity.oos_trade_count,
            "sharpe_oos":            self.validity.sharpe_oos,
            "max_drawdown_pct":      self.validity.max_drawdown_pct,
            "profit_factor":         self.validity.profit_factor,
            "win_rate_pct":          self.validity.win_rate_pct,
            "composite_score":       self.validity.composite_score,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StrategySpec":
        """Deserialise from a flat dict (e.g. loaded from YAML)."""
        return cls(
            name                  = d["name"],
            version               = d["version"],
            status                = d.get("status", "RESEARCH"),
            strategy_id           = d.get("strategy_id", str(uuid.uuid4())),
            created_at            = d.get("created_at", datetime.now(timezone.utc).isoformat()),
            backtest_results_path = d.get("backtest_results_path"),
            entry_rules = EntryRules(
                obi_threshold    = d.get("obi_threshold",    0.25),
                cvd_momentum_min = d.get("cvd_momentum_min", 0.8),
                vol_ratio_min    = d.get("vol_ratio_min",    1.4),
                spread_max_bps   = d.get("spread_max_bps",   8.0),
                sweep_qty_mult   = d.get("sweep_qty_mult",   2.0),
                atr_mult_sl      = d.get("atr_mult_sl",      1.5),
                atr_mult_tp      = d.get("atr_mult_tp",      3.0),
            ),
            validity = StatisticalValidity(
                oos_trade_count  = d.get("oos_trade_count",  0),
                sharpe_oos       = d.get("sharpe_oos",       0.0),
                max_drawdown_pct = d.get("max_drawdown_pct", 100.0),
                profit_factor    = d.get("profit_factor",    0.0),
                win_rate_pct     = d.get("win_rate_pct",     0.0),
                composite_score  = d.get("composite_score",  -999.0),
            ),
        )
