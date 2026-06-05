"""Tests for stop-distance clamping used in position sizing (defense-in-depth)."""
import math

from risk.sizing import clamp_stop_bps, cap_risk_fraction


def test_clamp_floors_below_min():
    # A near-zero stop (e.g. testnet protection wall ~at mid) is floored to the minimum,
    # bounding position size instead of letting qty = risk / ~0 explode.
    assert clamp_stop_bps(0.0008, min_bps=1.0, max_bps=25.0) == 1.0


def test_clamp_caps_above_max():
    assert clamp_stop_bps(40.0, min_bps=1.0, max_bps=25.0) == 25.0


def test_clamp_passes_value_in_range():
    assert clamp_stop_bps(8.5, min_bps=1.0, max_bps=25.0) == 8.5


def test_cap_risk_fraction_unchanged_when_budget_ample():
    # risk$ = 10000 * 0.0002 = $2, well under $500 remaining → unchanged
    assert cap_risk_fraction(0.0002, equity=10_000.0, budget_remaining=500.0) == 0.0002


def test_cap_risk_fraction_caps_when_budget_low():
    # risk$ would be $2 but only $1 of daily loss budget remains → cap to $1/equity
    assert cap_risk_fraction(0.0002, equity=10_000.0, budget_remaining=1.0) == 1.0 / 10_000.0


def test_cap_risk_fraction_zero_when_budget_exhausted():
    assert cap_risk_fraction(0.0002, equity=10_000.0, budget_remaining=0.0) == 0.0


def test_cap_risk_fraction_unbounded_budget_is_noop():
    assert cap_risk_fraction(0.0002, equity=10_000.0, budget_remaining=math.inf) == 0.0002
