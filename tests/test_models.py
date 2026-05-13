from models import CircuitBreakerStatus, PortfolioState


def make_portfolio(**kwargs) -> PortfolioState:
    defaults = dict(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    return PortfolioState(**{**defaults, **kwargs})


def test_drawdown_pct_at_peak():
    pf = make_portfolio()
    assert pf.drawdown_pct == 0.0


def test_drawdown_pct_below_peak():
    pf = make_portfolio(equity=9_000.0, peak_equity=10_000.0)
    assert abs(pf.drawdown_pct - 0.10) < 1e-9


def test_drawdown_pct_zero_peak_guard():
    pf = make_portfolio(equity=0.0, starting_equity=0.0, peak_equity=0.0)
    assert pf.drawdown_pct == 0.0


def test_daily_loss_pct_with_loss():
    pf = make_portfolio(daily_pnl=-200.0)
    assert abs(pf.daily_loss_pct - 0.02) < 1e-9


def test_daily_loss_pct_with_profit():
    pf = make_portfolio(daily_pnl=500.0)
    assert pf.daily_loss_pct == 0.0


def test_circuit_breaker_default():
    pf = make_portfolio()
    assert pf.circuit_breaker == CircuitBreakerStatus.ACTIVE
