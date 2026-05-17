import asyncio
import pytest

from models import (
    CircuitBreakerStatus,
    Direction,
    PatternSignal,
    PatternType,
    PortfolioState,
)
from risk.budget import DailyBudget
from risk.engine import RiskEngine
from risk.killswitch import GlobalKillswitch
from risk.pyramid import PyramidController


def make_portfolio(**kwargs) -> PortfolioState:
    defaults = dict(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    return PortfolioState(**{**defaults, **kwargs})


def make_budget(dov: float = 10_000.0, realised_pnl: float = 0.0) -> DailyBudget:
    b = DailyBudget.from_equity(dov)
    b.realised_pnl = realised_pnl
    return b


def make_signal(confidence=0.8, entry=30_000.0, sl=29_700.0, tp=30_900.0) -> PatternSignal:
    return PatternSignal(
        pattern=PatternType.RESISTANCE_BREAKOUT,
        direction=Direction.LONG,
        confidence=confidence,
        entry_price=entry,
        stop_loss=sl,
        take_profit=tp,
    )


def make_engine(
    portfolio: PortfolioState,
    budget: DailyBudget | None = None,
    killswitch: GlobalKillswitch | None = None,
    pyramid=None,
) -> RiskEngine:
    from risk.pyramid import PyramidController
    return RiskEngine(asyncio.Queue(), asyncio.Queue(), portfolio, budget, killswitch, pyramid)


# ── Circuit breakers (existing, backward-compat) ──────────────────────────────

def test_circuit_breaker_active_by_default():
    engine = make_engine(make_portfolio())
    assert engine._check_circuit_breakers() == CircuitBreakerStatus.ACTIVE


def test_circuit_breaker_halts_on_max_drawdown():
    pf = make_portfolio(equity=9_000.0, peak_equity=10_000.0)  # 10% drawdown
    engine = make_engine(pf)
    assert engine._check_circuit_breakers() == CircuitBreakerStatus.HALTED


def test_circuit_breaker_halts_on_daily_loss():
    pf = make_portfolio(daily_pnl=-250.0)  # 2.5% daily loss > 2% limit
    engine = make_engine(pf)
    assert engine._check_circuit_breakers() == CircuitBreakerStatus.HALTED


def test_circuit_breaker_pauses_on_consecutive_losses():
    pf = make_portfolio(consecutive_losses=3)
    engine = make_engine(pf)
    assert engine._check_circuit_breakers() == CircuitBreakerStatus.PAUSED


# ── Signal evaluation (existing, backward-compat) ─────────────────────────────

def test_evaluate_approves_valid_signal():
    engine = make_engine(make_portfolio())
    req = engine._evaluate(make_signal())
    assert req.approved is True
    assert req.quantity > 0


def test_evaluate_rejects_low_confidence():
    engine = make_engine(make_portfolio())
    req = engine._evaluate(make_signal(confidence=0.3))
    assert req.approved is False
    assert "confidence" in req.rejection_reason.lower()


def test_evaluate_rejects_when_max_positions_reached():
    from models import Position
    from risk.pyramid import PyramidController
    pf = make_portfolio()
    pf.positions.append(
        Position(symbol="BTCUSDT", side=Direction.LONG,
                 entry_price=30_000.0, quantity=0.01,
                 stop_loss=29_700.0, take_profit=30_900.0)
    )
    # Sync pyramid: leg 1 opened at 30_000; signal also at 30_000 → PnL = 0 → not profitable
    pyr = PyramidController()
    pyr.open_leg(qty=0.01, entry_price=30_000.0, direction="LONG")
    engine = make_engine(pf, pyramid=pyr)
    req = engine._evaluate(make_signal())  # entry_price=30_000 → leg 1 break-even → rejected
    assert req.approved is False
    assert "pyramid" in req.rejection_reason.lower()


def test_evaluate_rejects_when_circuit_breaker_halted():
    pf = make_portfolio(equity=9_000.0, peak_equity=10_000.0)
    engine = make_engine(pf)
    req = engine._evaluate(make_signal())
    assert req.approved is False
    assert "circuit breaker" in req.rejection_reason.lower()


# ── Position sizing (existing, backward-compat) ───────────────────────────────

def test_position_sizing_normal():
    engine = make_engine(make_portfolio())
    sig = make_signal(entry=30_000.0, sl=29_700.0)  # $300 SL distance
    # With default budget: risk_amount = min(100, 60, 40) = 40
    # kelly = 0.25 * 0.8 = 0.2; leg_scalar leg1 = 1.0; qty = (40/300) * 0.2 = 0.0267
    qty = engine._size_position(sig)
    assert qty > 0


def test_position_sizing_zero_sl_distance():
    engine = make_engine(make_portfolio())
    sig = make_signal(entry=30_000.0, sl=30_000.0)
    assert engine._size_position(sig) == 0.0


# ── Trade result recording (existing, backward-compat) ────────────────────────

def test_record_win_resets_consecutive_losses():
    pf = make_portfolio(consecutive_losses=2)
    engine = make_engine(pf)
    engine.record_trade_result(200.0)
    assert pf.consecutive_losses == 0


def test_record_loss_increments_consecutive_losses():
    pf = make_portfolio()
    engine = make_engine(pf)
    engine.record_trade_result(-100.0)
    assert pf.consecutive_losses == 1


def test_record_win_updates_peak_equity():
    pf = make_portfolio(equity=10_000.0, peak_equity=10_000.0)
    engine = make_engine(pf)
    engine.record_trade_result(500.0)
    assert pf.peak_equity == 10_500.0


# ── 5-tier throttling ─────────────────────────────────────────────────────────

def test_5tier_full_by_default():
    engine = make_engine(make_portfolio())
    assert engine.tier == "FULL"


def test_5tier_reduces_at_05pct_dov():
    # 0.5% of 10_000 = 50 loss → exactly at REDUCED boundary
    budget = make_budget(realised_pnl=-50.0)
    engine = make_engine(make_portfolio(), budget)
    engine._check_circuit_breakers()
    assert engine.tier == "REDUCED"


def test_5tier_minimal_at_075pct_dov():
    budget = make_budget(realised_pnl=-75.0)  # 0.75% of DOV
    engine = make_engine(make_portfolio(), budget)
    engine._check_circuit_breakers()
    assert engine.tier == "MINIMAL"


def test_5tier_passive_at_09pct_dov():
    budget = make_budget(realised_pnl=-90.0)  # 0.9% of DOV
    engine = make_engine(make_portfolio(), budget)
    result = engine._check_circuit_breakers()
    assert engine.tier == "PASSIVE"
    assert result == CircuitBreakerStatus.PAUSED


def test_5tier_halted_at_10pct_dov():
    budget = make_budget(realised_pnl=-100.0)  # 1.0% of DOV
    engine = make_engine(make_portfolio(), budget)
    result = engine._check_circuit_breakers()
    assert engine.tier == "HALTED"
    assert result == CircuitBreakerStatus.HALTED


# ── Tier-specific confidence gates ────────────────────────────────────────────

def test_tier_confidence_gate_reduced():
    budget = make_budget(realised_pnl=-50.0)  # REDUCED tier
    engine = make_engine(make_portfolio(), budget)
    # confidence=0.60 < REDUCED minimum 0.65 → rejected
    req = engine._evaluate(make_signal(confidence=0.60))
    assert req.approved is False
    assert "REDUCED" in req.rejection_reason


def test_tier_confidence_gate_minimal():
    budget = make_budget(realised_pnl=-75.0)  # MINIMAL tier
    engine = make_engine(make_portfolio(), budget)
    # confidence=0.70 < MINIMAL minimum 0.80 → rejected
    req = engine._evaluate(make_signal(confidence=0.70))
    assert req.approved is False
    assert "MINIMAL" in req.rejection_reason


# ── Budget-capped sizing ──────────────────────────────────────────────────────

def test_sizing_capped_by_remaining_budget():
    # remaining = hard_limit + realised = 100 + (-94) = 6
    # remaining * 60% = 3.6 → binding cap (equity*1%=100, dov*0.4%=40)
    budget = make_budget(realised_pnl=-94.0)
    engine = make_engine(make_portfolio(), budget)
    sig = make_signal(entry=30_000.0, sl=29_700.0, confidence=0.8)
    qty = engine._size_position(sig)
    # risk_amount = 3.6; kelly = 0.2; tier=FULL scalar=1.0
    # qty = (3.6/300)*0.2 = 0.0024
    assert qty == pytest.approx(0.0024, rel=1e-3)


# ── Session reset ─────────────────────────────────────────────────────────────

def test_reset_for_new_session_clears_daily_counters():
    # consecutive_losses=0 so cooldown doesn't fire before the budget tier check
    pf = make_portfolio(daily_pnl=-150.0, consecutive_losses=0)
    budget = make_budget(realised_pnl=-75.0)  # 0.75% DOV → MINIMAL
    engine = make_engine(pf, budget)
    engine._check_circuit_breakers()
    assert engine.tier == "MINIMAL"

    engine.reset_for_new_session()

    assert pf.daily_pnl == 0.0
    assert pf.consecutive_losses == 0
    assert engine.tier == "FULL"
    assert engine._budget.realised_pnl == 0.0
    assert engine._budget.loss_pct == 0.0


# ── DailyBudget ──────────────────────────────────────────────────────────────

def test_budget_remaining_decreases_with_loss():
    b = DailyBudget.from_equity(10_000.0)
    b.realised_pnl = -50.0
    assert b.remaining == pytest.approx(50.0)


def test_budget_exhausted_when_remaining_zero():
    b = DailyBudget.from_equity(10_000.0)
    b.realised_pnl = -100.0
    assert b.remaining == pytest.approx(0.0)


def test_budget_loss_pct():
    b = DailyBudget.from_equity(10_000.0)
    b.realised_pnl = -200.0
    assert b.loss_pct == pytest.approx(0.02)


# ── PyramidController ────────────────────────────────────────────────────────

def test_pyramid_leg2_requires_leg1_profit():
    p = PyramidController()
    p.open_leg(qty=0.1, entry_price=30_000.0)
    ok, reason = p.can_add_leg(current_price=29_900.0)  # Leg 1 at loss
    assert ok is False
    assert "profitable" in reason


def test_pyramid_leg2_allowed_when_leg1_profitable():
    p = PyramidController()
    p.open_leg(qty=0.1, entry_price=30_000.0)
    ok, _ = p.can_add_leg(current_price=30_100.0)  # Leg 1 at profit
    assert ok is True


def test_pyramid_max_legs_enforced():
    p = PyramidController()
    p.open_leg(qty=0.100, entry_price=30_000.0)
    p.open_leg(qty=0.050, entry_price=30_100.0)
    p.open_leg(qty=0.025, entry_price=30_200.0)
    ok, reason = p.can_add_leg(current_price=30_300.0)
    assert ok is False
    assert "max" in reason.lower()


def test_pyramid_leg_scalar_halves():
    p = PyramidController()
    assert p.leg_scalar() == 1.0
    p.open_leg(0.1, 30_000.0)
    assert p.leg_scalar() == 0.5
    p.open_leg(0.05, 30_100.0)
    assert p.leg_scalar() == 0.25


# ── GlobalKillswitch ─────────────────────────────────────────────────────────

def test_killswitch_fires_on_budget_breach():
    ks = GlobalKillswitch(dov=10_000.0, hard_limit_pct=0.01)
    fired = ks.check_budget(realised_pnl=-200.0, unrealised_pnl=0.0)
    assert fired is True
    assert ks.is_active is True


def test_killswitch_no_fire_within_limit():
    ks = GlobalKillswitch(dov=10_000.0, hard_limit_pct=0.01)
    fired = ks.check_budget(realised_pnl=-50.0, unrealised_pnl=0.0)
    assert fired is False
    assert ks.is_active is False


def test_killswitch_fires_on_heartbeat_critical():
    # KS-2 requires settings.HEARTBEAT_CONSEC_LIMIT (3) consecutive CRITICALs
    ks = GlobalKillswitch(dov=10_000.0)
    assert ks.check_heartbeat("CRITICAL", delta_ms=600.0) is False  # 1st — no fire
    assert ks.check_heartbeat("CRITICAL", delta_ms=600.0) is False  # 2nd — no fire
    assert ks.check_heartbeat("CRITICAL", delta_ms=600.0) is True   # 3rd — fires
    assert ks.is_active is True


def test_killswitch_no_fire_on_degraded_heartbeat():
    ks = GlobalKillswitch(dov=10_000.0)
    fired = ks.check_heartbeat("DEGRADED", delta_ms=300.0)
    assert fired is False
    assert ks.is_active is False


def test_killswitch_fires_on_slippage_decay():
    ks = GlobalKillswitch(dov=10_000.0)
    # threshold = 3.0 * 1.5 = 4.5 bps; each fill gives ~5 bps
    for _ in range(20):
        ks.record_slippage(signal_price=30_000.0, fill_price=30_015.0, direction="LONG")
    assert ks.is_active is True


def test_killswitch_cannot_be_reset_without_restart():
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_budget(realised_pnl=-200.0, unrealised_pnl=0.0)
    assert ks.is_active is True
    # Subsequent call with healthy values must not clear the fired state (Rule 12)
    ks.check_budget(realised_pnl=0.0, unrealised_pnl=0.0)
    assert ks.is_active is True


# ── New tests: budget loss_pct fix ───────────────────────────────────────────

def test_loss_pct_includes_unrealised_pnl():
    b = DailyBudget.from_equity(10_000.0)
    b.realised_pnl   = -50.0
    b.unrealised_pnl = -30.0  # open position sitting at a loss
    # total loss = 80; 80/10000 = 0.008 → should push into MINIMAL tier
    assert b.loss_pct == pytest.approx(0.008)


# ── New tests: KS-2 consecutive counter ──────────────────────────────────────

def test_ks2_fires_only_after_consecutive_criticals():
    ks = GlobalKillswitch(dov=10_000.0)
    assert ks.check_heartbeat("CRITICAL", 600.0) is False
    assert ks.check_heartbeat("CRITICAL", 600.0) is False
    assert ks.check_heartbeat("CRITICAL", 600.0) is True
    assert ks.is_active is True


def test_ks2_resets_on_healthy_heartbeat():
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_heartbeat("CRITICAL", 600.0)
    ks.check_heartbeat("CRITICAL", 600.0)
    ks.check_heartbeat("HEALTHY", 50.0)   # resets counter
    assert ks.check_heartbeat("CRITICAL", 600.0) is False  # back to count=1
    assert ks.is_active is False


# ── New tests: pyramid direction fix ─────────────────────────────────────────

def test_pyramid_short_pnl_direction():
    p = PyramidController()
    p.open_leg(qty=0.1, entry_price=30_000.0, direction="SHORT")
    # Price fell: profitable for SHORT
    ok, _ = p.can_add_leg(current_price=29_900.0)
    assert ok is True


# ── New tests: killswitch wired into engine ───────────────────────────────────

def test_killswitch_blocks_evaluate():
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_budget(realised_pnl=-200.0, unrealised_pnl=0.0)  # fire KS
    engine = make_engine(make_portfolio(), killswitch=ks)
    req = engine._evaluate(make_signal())
    assert req.approved is False
    assert "HALTED" in req.rejection_reason


def test_killswitch_checked_on_record_trade():
    ks = GlobalKillswitch(dov=10_000.0, hard_limit_pct=0.01)
    pf = make_portfolio()
    engine = make_engine(pf, killswitch=ks)
    # A loss big enough to breach the hard limit (> $100 on $10k DOV)
    engine.record_trade_result(-150.0)
    assert ks.is_active is True


# ── New tests: pyramid wired into engine ──────────────────────────────────────

def test_pyramid_leg_via_engine():
    from models import Position
    pf = make_portfolio()
    # Simulate an open position (leg 1 entered at 29_900, now at 30_000 → profitable)
    pf.positions.append(
        Position(symbol="BTCUSDT", side=Direction.LONG,
                 entry_price=29_900.0, quantity=0.01,
                 stop_loss=29_700.0, take_profit=30_900.0)
    )
    from risk.pyramid import PyramidController
    pyr = PyramidController()
    pyr.open_leg(qty=0.01, entry_price=29_900.0, direction="LONG")  # sync leg 1
    engine = make_engine(pf, pyramid=pyr)
    # Signal at 30_000: leg 1 is profitable (+$1/unit) → pyramid should allow leg 2
    req = engine._evaluate(make_signal(entry=30_000.0, sl=29_700.0))
    assert req.approved is True
    assert req.quantity > 0


# ── New tests: mark_unrealised ────────────────────────────────────────────────

def test_mark_unrealised_updates_budget_loss_pct():
    pf = make_portfolio()
    engine = make_engine(pf)
    # Realised = 0, unrealised = -80 → loss_pct should be 0.8%
    engine.mark_unrealised(-80.0)
    assert engine._budget.loss_pct == pytest.approx(0.008)


def test_mark_unrealised_triggers_tier_downgrade():
    pf = make_portfolio()
    engine = make_engine(pf)
    engine.mark_unrealised(-75.0)  # 0.75% → MINIMAL tier on next evaluation
    engine._check_circuit_breakers()
    assert engine.tier == "MINIMAL"


# ── New tests: close_leg FIFO ─────────────────────────────────────────────────

def test_close_leg_removes_oldest_leg():
    p = PyramidController()
    p.open_leg(qty=0.10, entry_price=30_000.0, direction="LONG")
    p.open_leg(qty=0.05, entry_price=30_100.0, direction="LONG")
    p.close_leg()
    assert p.leg_count == 1
    # Remaining leg should be the second one (entry=30_100)
    assert p._legs[0]["entry"] == 30_100.0


def test_close_leg_on_empty_pyramid_is_safe():
    p = PyramidController()
    p.close_leg()  # must not raise
    assert p.leg_count == 0


def test_record_trade_closes_one_leg_not_all():
    from risk.pyramid import PyramidController
    pyr = PyramidController()
    pyr.open_leg(qty=0.10, entry_price=30_000.0, direction="LONG")
    pyr.open_leg(qty=0.05, entry_price=30_100.0, direction="LONG")
    engine = make_engine(make_portfolio(), pyramid=pyr)
    engine.record_trade_result(50.0)   # leg 1 closes
    assert engine._pyramid.leg_count == 1  # leg 2 still tracked


# ── New tests: session reset clears pyramid ───────────────────────────────────

def test_reset_for_new_session_clears_pyramid():
    from risk.pyramid import PyramidController
    pyr = PyramidController()
    pyr.open_leg(qty=0.10, entry_price=30_000.0, direction="LONG")
    pyr.open_leg(qty=0.05, entry_price=30_100.0, direction="LONG")
    engine = make_engine(make_portfolio(), pyramid=pyr)
    engine.reset_for_new_session()
    assert engine._pyramid.leg_count == 0
