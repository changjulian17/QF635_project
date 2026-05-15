import asyncio
import pytest

from models import (
    CircuitBreakerStatus,
    Direction,
    PatternSignal,
    PatternType,
    PortfolioState,
)
from risk.engine import RiskEngine


def make_portfolio(**kwargs) -> PortfolioState:
    defaults = dict(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    return PortfolioState(**{**defaults, **kwargs})


def make_signal(confidence=0.8, entry=30_000.0, sl=29_700.0, tp=30_900.0) -> PatternSignal:
    return PatternSignal(
        pattern=PatternType.RESISTANCE_BREAKOUT,
        direction=Direction.LONG,
        confidence=confidence,
        entry_price=entry,
        stop_loss=sl,
        take_profit=tp,
    )


def make_engine(portfolio: PortfolioState) -> RiskEngine:
    return RiskEngine(asyncio.Queue(), asyncio.Queue(), portfolio)


# --- Circuit breakers ---

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


# --- Signal evaluation ---

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
    pf = make_portfolio()
    pf.positions.append(
        Position(symbol="BTCUSDT", side=Direction.LONG,
                 entry_price=30_000.0, quantity=0.01,
                 stop_loss=29_700.0, take_profit=30_900.0)
    )
    engine = make_engine(pf)
    req = engine._evaluate(make_signal())
    assert req.approved is False


def test_evaluate_rejects_when_circuit_breaker_halted():
    pf = make_portfolio(equity=9_000.0, peak_equity=10_000.0)
    engine = make_engine(pf)
    req = engine._evaluate(make_signal())
    assert req.approved is False
    assert "circuit breaker" in req.rejection_reason.lower()


# --- Position sizing ---

def test_position_sizing_normal():
    engine = make_engine(make_portfolio())
    sig = make_signal(entry=30_000.0, sl=29_700.0)  # $300 risk distance
    qty = engine._size_position_vol_target(sig)
    # risk_amount = 10000 * 0.01 = 100; raw_qty = 100/300 ≈ 0.333; kelly = 0.25 * 0.8 = 0.2; qty ≈ 0.0667
    assert qty > 0


def test_position_sizing_zero_sl_distance():
    engine = make_engine(make_portfolio())
    sig = make_signal(entry=30_000.0, sl=30_000.0)
    assert engine._size_position_vol_target(sig) == 0.0


# --- Trade result recording ---

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


# --- 5-tier throttling (Phase 1K) ---

def test_5tier_full_by_default():
    engine = make_engine(make_portfolio())
    assert engine.tier == "FULL"


def test_5tier_reduces_on_daily_loss_50pct():
    pf = make_portfolio(daily_pnl=-100.0)   # 1% of 10k = 50% of 2% limit
    engine = make_engine(pf)
    engine._check_circuit_breakers()
    assert engine.tier == "REDUCED"


def test_5tier_minimal_on_daily_loss_75pct():
    pf = make_portfolio(daily_pnl=-150.0)   # 1.5% of 10k = 75% of 2% limit
    engine = make_engine(pf)
    engine._check_circuit_breakers()
    assert engine.tier == "MINIMAL"


def test_5tier_halted_on_max_drawdown():
    pf = make_portfolio(equity=9_000.0, peak_equity=10_000.0)
    engine = make_engine(pf)
    engine._check_circuit_breakers()
    assert engine.tier == "HALTED"


def test_reset_for_new_session_clears_daily_counters():
    pf = make_portfolio(daily_pnl=-150.0, consecutive_losses=3)
    engine = make_engine(pf)
    engine._check_circuit_breakers()
    engine.reset_for_new_session()
    assert pf.daily_pnl == 0.0
    assert pf.consecutive_losses == 0
    assert engine.tier == "FULL"


# --- DailyBudget (Phase 1K) ---

from risk.budget import DailyBudget


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


# --- PyramidController (Phase 1K) ---

from risk.pyramid import PyramidController


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
    prices = [30_000.0, 30_100.0, 30_200.0]
    for i, price in enumerate(prices):
        p.open_leg(qty=0.1 * _LEG_SCALARS[i + 1], entry_price=price)
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


_LEG_SCALARS = {1: 1.0, 2: 0.5, 3: 0.25}


# --- GlobalKillswitch (Phase 1K) ---

from risk.killswitch import GlobalKillswitch


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
    ks = GlobalKillswitch(dov=10_000.0)
    fired = ks.check_heartbeat("CRITICAL", delta_ms=600.0)
    assert fired is True
    assert ks.is_active is True


def test_killswitch_no_fire_on_degraded_heartbeat():
    ks = GlobalKillswitch(dov=10_000.0)
    fired = ks.check_heartbeat("DEGRADED", delta_ms=300.0)
    assert fired is False


def test_killswitch_fires_on_slippage_decay():
    ks = GlobalKillswitch(dov=10_000.0)
    # Flood window with slippage well above threshold (3.0 × 1.5 = 4.5 bps)
    for _ in range(20):
        ks.record_slippage(signal_price=30_000.0, fill_price=30_015.0, direction="LONG")
    assert ks.is_active is True


def test_killswitch_cannot_be_reset_without_restart():
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_budget(realised_pnl=-200.0, unrealised_pnl=0.0)
    assert ks.is_active is True
    # Simulate "trying to reset" by calling check again with healthy values
    ks.check_budget(realised_pnl=0.0, unrealised_pnl=0.0)
    assert ks.is_active is True   # still fired — Rule 12
