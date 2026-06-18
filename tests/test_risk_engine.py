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


# ── record_trade_result used as budget_update_cb ──────────────────────────────

def test_record_trade_result_updates_all_portfolio_counters():
    """record_trade_result must update every counter that the dashboard reads."""
    pf = make_portfolio()
    engine = make_engine(portfolio=pf)

    engine.record_trade_result(pnl=50.0)

    assert pf.equity      == pytest.approx(10_050.0)
    assert pf.daily_pnl   == pytest.approx(50.0)
    assert pf.peak_equity == pytest.approx(10_050.0)
    assert pf.num_trades  == 1
    assert pf.num_wins    == 1
    assert pf.consecutive_losses == 0


def test_record_trade_result_loss_increments_consecutive_losses():
    pf = make_portfolio()
    engine = make_engine(portfolio=pf)

    engine.record_trade_result(pnl=-100.0)

    assert pf.num_trades         == 1
    assert pf.num_wins           == 0
    assert pf.consecutive_losses == 1
    assert pf.daily_pnl          == pytest.approx(-100.0)
    assert pf.equity             == pytest.approx(9_900.0)


def test_record_trade_result_as_budget_update_cb_updates_budget():
    """Verify the callback wiring: record_trade_result must also update budget.realised_pnl."""
    pf     = make_portfolio()
    budget = DailyBudget.from_equity(pf.starting_equity)
    engine = make_engine(portfolio=pf, budget=budget)

    engine.record_trade_result(pnl=-200.0)

    assert budget.realised_pnl == pytest.approx(-200.0)
    assert pf.budget_loss_pct  == pytest.approx(budget.loss_pct)


def test_sync_tier_passive_on_consecutive_losses():
    from config import settings
    pf = make_portfolio(consecutive_losses=settings.MAX_CONSECUTIVE_LOSSES)
    engine = make_engine(pf)

    tier = engine.sync_tier()

    assert tier == "PASSIVE"
    assert pf.circuit_breaker == CircuitBreakerStatus.PAUSED


def test_sync_tier_restores_full_after_cooldown_expires_with_win():
    from config import settings
    pf = make_portfolio(consecutive_losses=settings.MAX_CONSECUTIVE_LOSSES)
    engine = make_engine(pf)

    engine.sync_tier()  # enters PAUSED → sets _cooldown_until, tier=PASSIVE
    assert engine.tier == "PASSIVE"

    engine.record_trade_result(+1.0)  # win resets consecutive_losses to 0
    engine._cooldown_until = datetime.now(timezone.utc) - timedelta(seconds=1)  # expire cooldown

    tier = engine.sync_tier()

    assert tier == "FULL"
    assert pf.circuit_breaker == CircuitBreakerStatus.ACTIVE


def test_record_trade_result_zeros_unrealised_on_close():
    """Closing a position must clear the floating mark so check_budget sees no double-count."""
    ks     = GlobalKillswitch(dov=10_000.0)   # hard_limit=100 — well above 1.9 total loss
    budget = DailyBudget.from_equity(10_000.0)
    engine = make_engine(budget=budget, killswitch=ks)

    budget.unrealised_pnl = -0.9   # stale MTM mark from last tick

    engine.record_trade_result(-1.0)

    assert budget.unrealised_pnl == 0.0, "unrealised_pnl must be zeroed when position closes"
    assert ks.is_active is False, "KS-1 must NOT fire spuriously due to stale floating mark"
