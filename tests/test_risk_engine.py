from datetime import datetime, timezone, timedelta

import pytest

from models import CircuitBreakerStatus, PortfolioState
from risk.budget import DailyBudget
from risk.engine import RiskEngine
from risk.killswitch import GlobalKillswitch


def make_portfolio(**kwargs) -> PortfolioState:
    defaults = dict(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    return PortfolioState(**{**defaults, **kwargs})


def make_engine(
    portfolio: PortfolioState | None = None,
    budget: DailyBudget | None = None,
    killswitch: GlobalKillswitch | None = None,
    tier_change_cb=None,
) -> RiskEngine:
    pf = portfolio or make_portfolio()
    return RiskEngine(
        portfolio=pf,
        budget=budget or DailyBudget.from_equity(pf.starting_equity),
        killswitch=killswitch or GlobalKillswitch(pf.starting_equity),
        tier_change_cb=tier_change_cb,
    )


def test_sync_tier_full_by_default():
    engine = make_engine()

    assert engine.sync_tier() == "FULL"
    assert engine.portfolio.circuit_breaker == CircuitBreakerStatus.ACTIVE


def test_sync_tier_transitions_on_budget_loss():
    pf = make_portfolio()
    budget = DailyBudget.from_equity(pf.starting_equity)
    budget.realised_pnl = -80.0
    engine = make_engine(pf, budget)

    assert engine.sync_tier() == "MINIMAL"
    assert pf.circuit_breaker == CircuitBreakerStatus.ACTIVE


def test_sync_tier_halts_on_budget_breach():
    pf = make_portfolio()
    budget = DailyBudget.from_equity(pf.starting_equity)
    budget.realised_pnl = -120.0
    engine = make_engine(pf, budget)

    assert engine.sync_tier() == "HALTED"
    assert pf.circuit_breaker == CircuitBreakerStatus.HALTED


def test_sync_tier_invokes_transition_callback():
    seen: list[tuple[str, str]] = []
    pf = make_portfolio()
    budget = DailyBudget.from_equity(pf.starting_equity)
    engine = make_engine(pf, budget, tier_change_cb=lambda old, new: seen.append((old, new)))

    budget.realised_pnl = -60.0
    assert engine.sync_tier() == "REDUCED"
    assert seen == [("FULL", "REDUCED")]


def test_record_trade_result_updates_portfolio_and_budget():
    pf = make_portfolio()
    engine = make_engine(pf)

    engine.record_trade_result(-100.0)

    assert pf.equity == 9_900.0
    assert pf.daily_pnl == -100.0
    assert pf.consecutive_losses == 1
    assert pf.num_trades == 1
    assert abs(engine._budget.realised_pnl + 100.0) < 1e-9


def test_record_trade_result_resets_consecutive_losses_on_win():
    pf = make_portfolio(consecutive_losses=2)
    engine = make_engine(pf)

    engine.record_trade_result(50.0)

    assert pf.equity == 10_050.0
    assert pf.consecutive_losses == 0
    assert pf.num_wins == 1
    assert pf.num_trades == 1


def test_mark_unrealised_updates_budget_loss_pct():
    pf = make_portfolio()
    engine = make_engine(pf)

    engine.mark_unrealised(-80.0)

    assert abs(pf.budget_loss_pct - 0.008) < 1e-9


def test_reset_for_new_session_clears_daily_state():
    pf = make_portfolio(
        daily_pnl=-100.0,
        consecutive_losses=3,
        num_trades=5,
        num_wins=2,
        num_fill_samples=4,
        avg_slippage_bps=1.2,
        budget_loss_pct=0.01,
    )
    engine = make_engine(pf)
    engine._budget.realised_pnl = -100.0
    engine._cooldown_until = datetime.now(timezone.utc)

    engine.reset_for_new_session()

    assert pf.daily_pnl == 0.0
    assert pf.consecutive_losses == 0
    assert pf.num_trades == 0
    assert pf.num_wins == 0
    assert pf.num_fill_samples == 0
    assert pf.avg_slippage_bps == 0.0
    assert pf.budget_loss_pct == 0.0
    assert pf.circuit_breaker == CircuitBreakerStatus.ACTIVE
    assert engine.tier == "FULL"
    assert engine._budget.realised_pnl == 0.0



# ── GlobalKillswitch.update_dov ───────────────────────────────────────────────

def test_killswitch_update_dov_changes_hard_limit():
    ks = GlobalKillswitch(dov=10_000.0)        # _hard_limit = 100.0
    ks.update_dov(1_000_000.0)                 # _hard_limit = 10_000.0
    assert ks._hard_limit == pytest.approx(10_000.0)


def test_killswitch_update_dov_ignores_nonpositive():
    ks = GlobalKillswitch(dov=10_000.0)
    original = ks._hard_limit
    ks.update_dov(0.0)
    assert ks._hard_limit == pytest.approx(original)


# ── GlobalKillswitch.check_heartbeat (KS-2) ──────────────────────────────────

def test_killswitch_ks2_fires_on_consecutive_critical(monkeypatch):
    from config import settings as s
    monkeypatch.setattr(s, "HEARTBEAT_KS2_ENABLED", True)
    monkeypatch.setattr(s, "HEARTBEAT_CONSEC_LIMIT", 3)
    ks = GlobalKillswitch(dov=10_000.0)
    assert ks.check_heartbeat("CRITICAL", 600.0) is False   # 1 of 3
    assert ks.check_heartbeat("CRITICAL", 600.0) is False   # 2 of 3
    result = ks.check_heartbeat("CRITICAL", 600.0)          # 3 of 3
    assert result is True
    assert ks.is_active
    assert ks.state.trigger == "KS-2_HEARTBEAT"


def test_killswitch_ks2_disabled_flag_prevents_fire(monkeypatch):
    from config import settings as s
    monkeypatch.setattr(s, "HEARTBEAT_KS2_ENABLED", False)
    ks = GlobalKillswitch(dov=10_000.0)
    for _ in range(20):
        assert ks.check_heartbeat("CRITICAL", 2000.0) is False
    assert not ks.is_active


def test_killswitch_ks2_consec_resets_on_non_critical(monkeypatch):
    from config import settings as s
    monkeypatch.setattr(s, "HEARTBEAT_KS2_ENABLED", True)
    monkeypatch.setattr(s, "HEARTBEAT_CONSEC_LIMIT", 3)
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_heartbeat("CRITICAL", 600.0)   # 1 of 3
    ks.check_heartbeat("CRITICAL", 600.0)   # 2 of 3
    ks.check_heartbeat("HEALTHY",  10.0)    # reset
    assert ks.check_heartbeat("CRITICAL", 600.0) is False   # back to 1 of 3
    assert not ks.is_active


def test_killswitch_ks2_short_circuits_once_active(monkeypatch):
    from config import settings as s
    monkeypatch.setattr(s, "HEARTBEAT_KS2_ENABLED", True)
    monkeypatch.setattr(s, "HEARTBEAT_CONSEC_LIMIT", 1)
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_heartbeat("CRITICAL", 600.0)
    assert ks.check_heartbeat("HEALTHY", 10.0) is True   # permanently active


# ── DailyBudget.rebase ────────────────────────────────────────────────────────

def test_budget_rebase_updates_equity_preserves_pnl():
    b = DailyBudget.from_equity(10_000.0)
    b.realised_pnl = -50.0                     # simulate a loss already recorded
    b.rebase(1_000_000.0)
    assert b.dov          == pytest.approx(1_000_000.0)
    assert b.hard_limit   == pytest.approx(10_000.0)
    assert b.realised_pnl == pytest.approx(-50.0)  # PnL must survive rebase


# ── Circuit breakers: drawdown + consecutive-loss cooldown ──────────────────

def test_drawdown_circuit_breaker_halts():
    pf = make_portfolio(equity=9_400.0, peak_equity=10_000.0)   # 6% drawdown >= 5%
    eng = make_engine(pf)
    eng.sync_tier()
    assert pf.circuit_breaker is CircuitBreakerStatus.HALTED


def test_drawdown_below_threshold_does_not_halt():
    pf = make_portfolio(equity=9_600.0, peak_equity=10_000.0)   # 4% drawdown < 5%
    eng = make_engine(pf)
    eng.sync_tier()
    assert pf.circuit_breaker is not CircuitBreakerStatus.HALTED


def test_consecutive_loss_cooldown_pauses():
    pf = make_portfolio(consecutive_losses=3)                   # >= MAX_CONSECUTIVE_LOSSES
    eng = make_engine(pf)
    eng.sync_tier()
    assert pf.circuit_breaker is CircuitBreakerStatus.PAUSED


def test_cooldown_persists_after_losses_cleared():
    pf = make_portfolio(consecutive_losses=3)
    eng = make_engine(pf)
    eng.sync_tier()                                             # arms 300s cooldown
    pf.consecutive_losses = 0                                   # losses reset, but window still open
    eng.sync_tier()
    assert pf.circuit_breaker is CircuitBreakerStatus.PAUSED
