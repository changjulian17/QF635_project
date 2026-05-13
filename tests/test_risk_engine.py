import asyncio
import pytest

from models import (
    CircuitBreakerStatus,
    Direction,
    PatternSignal,
    PatternType,
    PortfolioState,
)
from risk_engine import RiskEngine


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
