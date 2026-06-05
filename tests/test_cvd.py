"""Tests for CVDCalculator in core/cvd.py."""
from datetime import datetime, timezone

import pytest

from core.cvd import CVDCalculator, WelfordOnline
from models import AggTrade


def _trade(qty: float, is_buyer_maker: bool) -> AggTrade:
    return AggTrade(
        timestamp=datetime.now(timezone.utc),
        price=30000.0,
        qty=qty,
        is_buyer_maker=is_buyer_maker,
    )


# ── CVDCalculator ──────────────────────────────────────────────────────────────

def test_cvd_buy_increments():
    """Aggressive buy (is_buyer_maker=False) should increase CVD."""
    cvd = CVDCalculator()
    cvd.update(_trade(1.0, is_buyer_maker=False))
    assert cvd.get_cvd() == pytest.approx(1.0)


def test_cvd_sell_decrements():
    """Aggressive sell (is_buyer_maker=True) should decrease CVD."""
    cvd = CVDCalculator()
    cvd.update(_trade(1.0, is_buyer_maker=True))
    assert cvd.get_cvd() == pytest.approx(-1.0)


def test_cvd_accumulates():
    cvd = CVDCalculator()
    cvd.update(_trade(2.0, is_buyer_maker=False))  # +2
    cvd.update(_trade(0.5, is_buyer_maker=True))   # -0.5
    cvd.update(_trade(1.0, is_buyer_maker=False))  # +1
    assert cvd.get_cvd() == pytest.approx(2.5)


def test_cvd_delta_5bar():
    """get_cvd_delta(5) should return the CVD change over the last 5 ticks."""
    cvd = CVDCalculator()
    # 10 buy ticks of 1.0 each
    for _ in range(10):
        cvd.update(_trade(1.0, is_buyer_maker=False))
    # history[-1]=10, history[-6]=5 → delta = 5
    assert cvd.get_cvd_delta(5) == pytest.approx(5.0)


def test_cvd_delta_insufficient_history():
    cvd = CVDCalculator()
    cvd.update(_trade(1.0, is_buyer_maker=False))
    assert cvd.get_cvd_delta(5) == 0.0


def test_cvd_delta_default_ticks():
    """Default ticks=5 should be consistent with explicit ticks=5."""
    cvd = CVDCalculator()
    for _ in range(10):
        cvd.update(_trade(1.0, is_buyer_maker=False))
    assert cvd.get_cvd_delta() == cvd.get_cvd_delta(5)


def test_cvd_tick_std_positive_after_trades():
    cvd = CVDCalculator()
    # Mix of buys and sells to create variance
    for i in range(20):
        cvd.update(_trade(float(i % 3 + 1), is_buyer_maker=(i % 2 == 0)))
    assert cvd.get_cvd_tick_std() > 0.0


def test_get_cvd_excludes_trades_older_than_24h():
    """Trades with timestamps > 24h ago must not contribute to get_cvd()."""
    from datetime import timedelta
    cvd = CVDCalculator()
    old_trade = AggTrade(
        timestamp=datetime.now(timezone.utc) - timedelta(hours=25),
        price=30000.0, qty=5.0, is_buyer_maker=False,  # would be +5 if counted
    )
    recent_trade = AggTrade(
        timestamp=datetime.now(timezone.utc),
        price=30000.0, qty=1.0, is_buyer_maker=False,  # +1
    )
    cvd.update(old_trade)
    cvd.update(recent_trade)
    assert cvd.get_cvd() == pytest.approx(1.0)  # old_trade evicted; only recent_trade counts


def test_reset_daily_clears_state():
    cvd = CVDCalculator()
    for _ in range(10):
        cvd.update(_trade(1.0, is_buyer_maker=False))
    cvd.reset_daily()
    assert cvd.get_cvd() == 0.0
    assert cvd.get_cvd_delta(5) == 0.0
    assert cvd.get_cvd_tick_std() == 0.0


# ── WelfordOnline ──────────────────────────────────────────────────────────────

def test_welford_mean_variance():
    w = WelfordOnline()
    for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]:
        w.update(v)
    assert w._mean == pytest.approx(5.0)
    assert w.variance == pytest.approx(32 / 7)   # sample variance (n-1 divisor)
    assert w.std == pytest.approx((32 / 7) ** 0.5)


def test_welford_single_value_zero_std():
    w = WelfordOnline()
    w.update(42.0)
    assert w.std == 0.0


def test_welford_zero_before_any_update():
    w = WelfordOnline()
    assert w.std == 0.0
    assert w.variance == 0.0


def test_welford_zscore_strictly_causal():
    """z-score at observation N must not depend on observations N+1..M."""
    w50 = WelfordOnline()
    w200 = WelfordOnline()
    values = [float(i % 7) for i in range(200)]
    z50 = z200 = None
    for i, v in enumerate(values):
        z = w200.zscore(v)
        if i < 50:
            w50.zscore(v)
        if i == 49:
            z50 = w50.zscore(values[50]) if False else None  # capture state after 50
        if i == 50:
            z200 = z
    # Independently replay first 51 values in w50
    w50b = WelfordOnline()
    for v in values[:50]:
        w50b.update(v)
    z50_direct = w50b.zscore(values[50])
    assert abs(z50_direct - z200) < 1e-9


def test_welford_percentile_rank_midpoint():
    """Value equal to the mean should have rank near 0.5."""
    w = WelfordOnline()
    for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
        w.update(v)
    rank = w.percentile_rank(3.0)   # mean is 3.0
    assert 0.4 < rank < 0.6


def test_welford_zscore_returns_zero_before_variance():
    w = WelfordOnline()
    w.update(5.0)
    assert w.zscore(5.0) == 0.0   # std still 0 → z=0
