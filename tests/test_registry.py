"""
tests/test_registry.py
======================
Unit tests for strategy/registry.py and strategy/builder.py.
All tests use tmp_path — no shared filesystem state.
"""

from __future__ import annotations

import os

import pytest
import yaml

from strategy.builder import StrategyBuilder
from strategy.registry import StrategyRegistry
from strategy.spec import EntryRules, StatisticalValidity, StrategySpec


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_registry(tmp_path) -> StrategyRegistry:
    return StrategyRegistry(
        db_path=str(tmp_path / "registry.db"),
        yaml_dir=str(tmp_path),
    )


def _make_spec(name="TestStrat", version=1, status="RESEARCH", oos_trade_count=60) -> StrategySpec:
    spec = StrategySpec(name=name, version=version, status=status)
    spec.validity.oos_trade_count = oos_trade_count
    spec.validity.sharpe_oos = 1.2
    spec.validity.max_drawdown_pct = 10.0   # < 15% threshold
    spec.validity.profit_factor = 1.5       # > 1.3 threshold
    return spec


def _valid_metrics() -> dict:
    return {
        "oos_trade_count": 67,
        "sharpe_oos": 1.2,
        "max_drawdown_pct": 8.5,
        "profit_factor": 1.6,
        "win_rate_pct": 54.0,
        "composite_score": 0.73,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — Promotion gate blocks specs with < 50 OOS trades
# ─────────────────────────────────────────────────────────────────────────────

def test_promotion_gate_blocks_below_50_oos_trades(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec(oos_trade_count=38)
    registry.register(spec)

    can, reasons = registry.can_promote_to_paper(spec)

    assert not can
    assert len(reasons) == 1
    assert "38" in reasons[0] and "50" in reasons[0]


def test_promotion_gate_passes_at_50_oos_trades(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec(oos_trade_count=50)
    registry.register(spec)

    can, reasons = registry.can_promote_to_paper(spec)

    assert can
    assert reasons == []


def test_promotion_gate_blocks_low_sharpe(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    spec.validity.sharpe_oos = 0.7  # below PAPER_MIN_SHARPE = 1.0
    registry.register(spec)

    can, reasons = registry.can_promote_to_paper(spec)

    assert not can
    assert any("sharpe_oos" in r and "0.700" in r for r in reasons)


def test_promotion_gate_blocks_high_drawdown(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    spec.validity.max_drawdown_pct = 22.0  # above PAPER_MAX_DRAWDOWN_PCT = 15.0
    registry.register(spec)

    can, reasons = registry.can_promote_to_paper(spec)

    assert not can
    assert any("max_drawdown_pct" in r and "22.0" in r for r in reasons)


def test_promotion_gate_blocks_low_profit_factor(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    spec.validity.profit_factor = 1.1  # below PAPER_MIN_PROFIT_FACTOR = 1.3
    registry.register(spec)

    can, reasons = registry.can_promote_to_paper(spec)

    assert not can
    assert any("profit_factor" in r and "1.10" in r for r in reasons)


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — Registry rejects duplicate (name, version)
# ─────────────────────────────────────────────────────────────────────────────

def test_registry_rejects_duplicate_name_version(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    registry.register(spec)

    with pytest.raises(ValueError, match="already registered"):
        registry.register(spec)


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — YAML roundtrip preserves all fields
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_roundtrip(tmp_path):
    registry = _make_registry(tmp_path)
    original = _make_spec()
    original.entry_rules.spread_max_bps = 6.5
    original.validity.profit_factor = 1.8
    registry.register(original)

    yaml_path = str(tmp_path / "TestStrat_v1.yaml")
    assert os.path.exists(yaml_path)

    with open(yaml_path) as f:
        loaded = StrategySpec.from_dict(yaml.safe_load(f))

    assert loaded.name == original.name
    assert loaded.version == original.version
    assert loaded.status == original.status
    assert loaded.strategy_id == original.strategy_id
    assert loaded.entry_rules.spread_max_bps == original.entry_rules.spread_max_bps
    assert loaded.validity.profit_factor == original.validity.profit_factor


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — get_active_strategy returns highest-status spec
# ─────────────────────────────────────────────────────────────────────────────

def test_get_active_strategy_returns_highest_status(tmp_path):
    registry = _make_registry(tmp_path)

    research_spec = _make_spec(name="Strat", version=1, status="RESEARCH")
    backtest_spec = _make_spec(name="Strat", version=2, status="BACKTEST")

    registry.register(research_spec)
    registry.register(backtest_spec)

    active = registry.get_active_strategy()

    assert active is not None
    assert active.status == "BACKTEST"
    assert active.version == 2


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — Live promotion blocked when Sharpe < 70% of backtest
# ─────────────────────────────────────────────────────────────────────────────

def test_live_promotion_blocked_when_sharpe_below_70pct(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    spec.validity.sharpe_oos = 1.0  # backtest Sharpe
    registry.register(spec)

    paper_metrics = {
        "weeks_running": 4,
        "total_trades": 30,
        "sharpe_rolling": 0.6,  # 60% of 1.0 — below 70% threshold
    }
    can, reasons = registry.can_promote_to_live(spec, paper_metrics)

    assert not can
    assert any("70%" in r or "0.600" in r or "sharpe" in r.lower() for r in reasons)


def test_live_promotion_passes_when_all_gates_met(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    spec.validity.sharpe_oos = 1.0
    registry.register(spec)

    paper_metrics = {
        "weeks_running": 3,
        "total_trades": 25,
        "sharpe_rolling": 0.75,  # 75% of 1.0 — above 70%
    }
    can, reasons = registry.can_promote_to_live(spec, paper_metrics)

    assert can
    assert reasons == []


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — promote() updates status in both DB and YAML
# ─────────────────────────────────────────────────────────────────────────────

def test_promote_updates_status_in_db_and_yaml(tmp_path):
    import sqlite3

    registry = _make_registry(tmp_path)
    spec = _make_spec(oos_trade_count=60)
    registry.register(spec)

    registry.promote(spec.strategy_id, "PAPER")

    # Check DB
    conn = sqlite3.connect(str(tmp_path / "registry.db"))
    row = conn.execute(
        "SELECT status FROM strategies WHERE strategy_id=?", (spec.strategy_id,)
    ).fetchone()
    conn.close()
    assert row[0] == "PAPER"

    # Check YAML
    yaml_path = str(tmp_path / "TestStrat_v1.yaml")
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    assert data["status"] == "PAPER"


def test_promote_rejects_demotion(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec(oos_trade_count=60)
    registry.register(spec)
    registry.promote(spec.strategy_id, "PAPER")

    with pytest.raises(ValueError, match="Cannot demote"):
        registry.promote(spec.strategy_id, "BACKTEST")


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — StrategyBuilder produces a BACKTEST-status spec
# ─────────────────────────────────────────────────────────────────────────────

def test_builder_produces_backtest_status_spec(tmp_path):
    registry = _make_registry(tmp_path)
    builder = StrategyBuilder(registry=registry)

    spec = builder.build("Sweep Protection", "1m", metrics=_valid_metrics())

    assert spec.status == "BACKTEST"
    assert spec.name == "Sweep Protection [1m]"
    assert spec.validity.oos_trade_count == 67
    assert spec.validity.sharpe_oos == 1.2

    # Verify it landed in the registry
    active = registry.get_active_strategy()
    assert active is not None
    assert active.strategy_id == spec.strategy_id


def test_builder_without_registry_returns_spec(tmp_path):
    builder = StrategyBuilder()  # no registry
    spec = builder.build("No Registry Strat", "5m", metrics=_valid_metrics())

    assert spec.status == "BACKTEST"
    assert spec.version == 1


def test_builder_raises_on_missing_metrics(tmp_path):
    builder = StrategyBuilder()
    bad_metrics = {"oos_trade_count": 10}  # missing most keys

    with pytest.raises(ValueError, match="Missing required metric keys"):
        builder.build("Bad Strat", "1m", metrics=bad_metrics)


def test_builder_raises_on_zero_trade_count(tmp_path):
    builder = StrategyBuilder()
    metrics = _valid_metrics()
    metrics["oos_trade_count"] = 0

    with pytest.raises(ValueError, match="oos_trade_count=0"):
        builder.build("Zero Trades", "1m", metrics=metrics)
