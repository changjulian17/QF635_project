"""Tests for stop-distance clamping used in position sizing (defense-in-depth)."""
from risk.sizing import clamp_stop_bps


def test_clamp_floors_below_min():
    # A near-zero stop (e.g. testnet protection wall ~at mid) is floored to the minimum,
    # bounding position size instead of letting qty = risk / ~0 explode.
    assert clamp_stop_bps(0.0008, min_bps=1.0, max_bps=25.0) == 1.0


def test_clamp_caps_above_max():
    assert clamp_stop_bps(40.0, min_bps=1.0, max_bps=25.0) == 25.0


def test_clamp_passes_value_in_range():
    assert clamp_stop_bps(8.5, min_bps=1.0, max_bps=25.0) == 8.5
