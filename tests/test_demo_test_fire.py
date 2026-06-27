"""Unit tests for the demo test-fire close-qty guard.

Imports only the pure helper — the module has no top-level config/env side
effects, so this collects cleanly without DEMO API keys.
"""
from scripts.demo_test_fire import close_sell_qty


def test_long_at_least_bought_sells_bought():
    # Live long covers what we bought — sell exactly the bought qty.
    assert close_sell_qty(0.001, 0.003) == 0.001


def test_long_less_than_bought_sells_the_smaller_net():
    # Watchdog/other actor reduced the long below what we bought — sell what's there.
    assert close_sell_qty(0.003, 0.001) == 0.001


def test_flat_returns_zero():
    # No long position (watchdog raced the close) — skip the SELL, no short opened.
    assert close_sell_qty(0.001, 0.0) == 0.0


def test_short_returns_zero():
    # Net short — never SELL into it (would deepen a short).
    assert close_sell_qty(0.001, -0.002) == 0.0


def test_sub_step_long_rounds_down_to_zero():
    # A long smaller than one step rounds down — nothing sellable.
    assert close_sell_qty(0.001, 0.0004, step=0.001) == 0.0


def test_rounds_down_to_step_multiple():
    # Sellable qty is floored to a whole number of steps.
    assert close_sell_qty(0.0025, 0.0025, step=0.001) == 0.002
