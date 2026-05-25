"""
tests/test_registry.py
======================
Unit tests for strategy/registry.py and strategy/builder.py.
All tests use tmp_path — no shared filesystem state.
"""

from __future__ import annotations

import math
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


def test_builder_raises_on_negative_drawdown(tmp_path):
    builder = StrategyBuilder()
    metrics = _valid_metrics()
    metrics["max_drawdown_pct"] = -5.0

    with pytest.raises(ValueError, match="negative"):
        builder.build("Bad Drawdown", "1m", metrics=metrics)


def test_builder_raises_on_negative_profit_factor(tmp_path):
    builder = StrategyBuilder()
    metrics = _valid_metrics()
    metrics["profit_factor"] = -1.0

    with pytest.raises(ValueError, match="negative"):
        builder.build("Bad PF", "1m", metrics=metrics)


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — promote() enforces PAPER gate automatically
# ─────────────────────────────────────────────────────────────────────────────

def test_promote_to_paper_blocked_by_gate(tmp_path):
    """promote('PAPER') must raise when the spec fails can_promote_to_paper()."""
    registry = _make_registry(tmp_path)
    spec = _make_spec(oos_trade_count=10)   # below PAPER_MIN_OOS_TRADES=50
    registry.register(spec)

    with pytest.raises(ValueError, match="Promotion to PAPER blocked"):
        registry.promote(spec.strategy_id, "PAPER")


def test_promote_to_live_requires_paper_metrics(tmp_path):
    """promote('LIVE') without paper_metrics must raise immediately."""
    registry = _make_registry(tmp_path)
    spec = _make_spec(name="LiveTest", oos_trade_count=60)
    registry.register(spec)
    registry.promote(spec.strategy_id, "PAPER")

    with pytest.raises(ValueError, match="paper_metrics is required"):
        registry.promote(spec.strategy_id, "LIVE")


# ─────────────────────────────────────────────────────────────────────────────
# Test 9 — count_paper_trades and compute_rolling_sharpe
# ─────────────────────────────────────────────────────────────────────────────

def _seed_signal_records(db_path: str, strategy_id: str, n: int = 10) -> None:
    """Insert synthetic APPROVED WIN/LOSS records into signal_records."""
    import sqlite3, json
    from datetime import datetime, timezone, timedelta
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_records (
            signal_id TEXT PRIMARY KEY, strategy_id TEXT, timestamp TEXT,
            micro_signal TEXT, gate_passed TEXT, rejection_reason TEXT,
            lob_status TEXT, heartbeat_status TEXT,
            obi_zscore REAL, cvd_delta REAL, spread_bps REAL,
            confidence REAL, direction TEXT,
            outcome TEXT DEFAULT '', pnl REAL DEFAULT 0.0,
            pnl_pct REAL DEFAULT 0.0, duration_min REAL DEFAULT 0.0,
            features_json TEXT DEFAULT NULL
        )
    """)
    base = datetime.now(timezone.utc) - timedelta(days=5)
    for i in range(n):
        outcome = "WIN" if i % 2 == 0 else "LOSS"
        pnl_pct = 0.005 if outcome == "WIN" else -0.003
        ts = (base + timedelta(hours=i)).isoformat()
        conn.execute(
            "INSERT INTO signal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(i), strategy_id, ts, "SWEEP_WITH_PROTECTION", "APPROVED",
             "", "SYNCED", "HEALTHY", 1.5, 0.8, 3.0, 0.7, "LONG",
             outcome, pnl_pct * 1000, pnl_pct, 5.0, None),
        )
    conn.commit()
    conn.close()


def test_count_paper_trades_returns_correct_count(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    registry.register(spec)
    _seed_signal_records(str(tmp_path / "registry.db"), spec.strategy_id, n=12)

    count = registry.count_paper_trades(spec.strategy_id)
    assert count == 12


def test_count_paper_trades_returns_zero_for_unknown_id(tmp_path):
    registry = _make_registry(tmp_path)
    assert registry.count_paper_trades("nonexistent-id") == 0


def test_compute_rolling_sharpe_returns_float(tmp_path):
    registry = _make_registry(tmp_path)
    spec = _make_spec()
    registry.register(spec)
    _seed_signal_records(str(tmp_path / "registry.db"), spec.strategy_id, n=20)

    sharpe = registry.compute_rolling_sharpe(spec.strategy_id, days=30)
    assert isinstance(sharpe, float)
    assert math.isfinite(sharpe)


def test_compute_rolling_sharpe_returns_zero_with_no_trades(tmp_path):
    registry = _make_registry(tmp_path)
    sharpe = registry.compute_rolling_sharpe("no-trades-id", days=30)
    assert sharpe == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Test 10 — walk_forward_metrics
# ─────────────────────────────────────────────────────────────────────────────

class _MockEngine:
    """Minimal replay engine stub — returns fixed trades regardless of window."""

    def __init__(self, trades_per_fold: int = 8):
        self._trades_per_fold = trades_per_fold
        self._call_count = 0

    def replay_window(self, start_ms: int, end_ms: int):
        import pandas as pd
        from backtesting.tick_replay import ReplayTrade
        from models import MicroSignal
        self._call_count += 1
        sig = MicroSignal(signal_type="SWEEP_WITH_PROTECTION", direction="LONG", timestamp_ms=start_ms)
        trades = []
        for i in range(self._trades_per_fold):
            pnl = 10.0 if i % 3 != 0 else -5.0   # ~67% win rate
            trades.append(ReplayTrade(
                entry_ts_ms=start_ms + i * 1000,
                exit_ts_ms=start_ms + i * 1000 + 500,
                direction="LONG",
                entry_price=50_000.0,
                exit_price=50_010.0 if pnl > 0 else 49_990.0,
                qty=0.001,
                pnl_usd=pnl,
                exit_reason="TP" if pnl > 0 else "SL",
                signal=sig,
            ))
        eq = pd.Series(
            [10_000.0, 10_000.0 + sum(t.pnl_usd for t in trades)],
            index=pd.to_datetime([start_ms, end_ms], unit="ms", utc=True),
        )
        return eq, trades


def test_walk_forward_metrics_returns_valid_dict(tmp_path):
    engine = _MockEngine(trades_per_fold=10)
    start_ms = 1_700_000_000_000
    end_ms   = start_ms + 5 * 24 * 3_600_000   # 5 days

    metrics = StrategyBuilder.walk_forward_metrics(engine, start_ms, end_ms, n_folds=5)

    assert metrics["oos_trade_count"] == 50   # 5 folds × 10 trades
    assert 0.0 < metrics["win_rate_pct"] < 100.0
    assert metrics["profit_factor"] > 0
    assert metrics["max_drawdown_pct"] >= 0.0
    assert math.isfinite(metrics["sharpe_oos"])
    assert math.isfinite(metrics["composite_score"])
    assert engine._call_count == 5   # one call per fold


def test_walk_forward_metrics_raises_on_zero_trades(tmp_path):
    class _EmptyEngine:
        def replay_window(self, s, e):
            import pandas as pd
            return pd.Series(dtype=float), []

    with pytest.raises(ValueError, match="zero closed trades"):
        StrategyBuilder.walk_forward_metrics(_EmptyEngine(), 0, 86_400_000, n_folds=3)


def test_walk_forward_metrics_raises_on_single_fold(tmp_path):
    with pytest.raises(ValueError, match="n_folds must be >= 2"):
        StrategyBuilder.walk_forward_metrics(_MockEngine(), 0, 86_400_000, n_folds=1)
