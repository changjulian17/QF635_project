"""StrategySpec serialization + defaults (promotion gates live in test_registry.py)."""
from strategy.spec import EntryRules, StatisticalValidity, StrategySpec


def test_defaults():
    s = StrategySpec(name="X", version=1)
    assert s.status == "RESEARCH"
    assert s.strategy_id            # auto-generated uuid
    assert s.created_at
    assert isinstance(s.entry_rules, EntryRules)
    assert isinstance(s.validity, StatisticalValidity)


def test_to_dict_from_dict_roundtrip_preserves_all_fields():
    s = StrategySpec(
        name="Sweep [5m]", version=3, status="PAPER",
        entry_rules=EntryRules(obi_threshold=0.4, spread_max_bps=6.0, atr_mult_tp=2.5),
        validity=StatisticalValidity(
            oos_trade_count=120, sharpe_oos=1.4, max_drawdown_pct=8.0,
            profit_factor=1.6, win_rate_pct=58.0, composite_score=2.1,
        ),
    )
    r = StrategySpec.from_dict(s.to_dict())
    assert (r.name, r.version, r.status, r.strategy_id) == (s.name, s.version, s.status, s.strategy_id)
    assert r.entry_rules == s.entry_rules        # dataclass eq → all fields
    assert r.validity == s.validity


def test_from_dict_missing_optional_keys_uses_defaults():
    r = StrategySpec.from_dict({"name": "Min", "version": 1, "strategy_id": "abc"})
    assert r.status == "RESEARCH"
    assert r.entry_rules.obi_threshold == 0.25
    assert r.validity.oos_trade_count == 0
