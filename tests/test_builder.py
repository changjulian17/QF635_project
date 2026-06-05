"""StrategyBuilder unit tests (registry-coupled cases live in test_registry.py)."""
import pytest

from strategy.builder import StrategyBuilder
from strategy.spec import EntryRules


def _valid_metrics(**ov):
    m = dict(
        oos_trade_count=60, sharpe_oos=1.2, max_drawdown_pct=8.0,
        profit_factor=1.5, win_rate_pct=55.0, composite_score=1.0,
    )
    m.update(ov)
    return m


def test_build_returns_backtest_spec_with_validity():
    spec = StrategyBuilder().build("Strat", "5m", _valid_metrics(oos_trade_count=77, sharpe_oos=1.33))
    assert spec.status == "BACKTEST"
    assert spec.name == "Strat [5m]"
    assert spec.validity.oos_trade_count == 77
    assert spec.validity.sharpe_oos == 1.33


def test_build_passes_entry_rules_through():
    spec = StrategyBuilder().build("S", "1m", _valid_metrics(), entry_rules=EntryRules(obi_threshold=0.9))
    assert spec.entry_rules.obi_threshold == 0.9


def test_validate_rejects_nan_sharpe():
    with pytest.raises(ValueError, match="NaN"):
        StrategyBuilder().build("S", "1m", _valid_metrics(sharpe_oos=float("nan")))


def test_next_version_is_1_without_registry():
    assert StrategyBuilder().build("S", "1m", _valid_metrics()).version == 1
