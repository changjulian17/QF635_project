"""
tests/test_bt_costs.py
======================
Unit tests for backtesting/costs.py — TransactionCostModel.
"""

from __future__ import annotations

import pytest

from backtesting.costs import TransactionCostModel


def test_round_trip_cost_is_30bps():
    """Entry taker (10bps) + exit maker (10bps) + 2×slippage (5bps) = 30bps."""
    model    = TransactionCostModel()
    price    = 67_000.0
    qty      = 0.1
    notional = price * qty

    entry = model.entry_cost(price, qty, is_market=True)
    exit_ = model.exit_cost(price, qty, is_market=False)

    round_trip_pct = (entry + exit_) / notional
    assert abs(round_trip_pct - 0.0030) < 1e-4, (
        f"Round-trip cost {round_trip_pct:.6f} ≠ 0.0030"
    )


def test_round_trip_pct_property_matches_entry_exit_sum():
    """round_trip_pct property must equal actual entry+exit / notional."""
    model = TransactionCostModel()
    price = 50_000.0
    qty   = 0.05

    entry = model.entry_cost(price, qty)
    exit_ = model.exit_cost(price, qty)
    computed_pct = (entry + exit_) / (price * qty)

    assert abs(model.round_trip_pct - computed_pct) < 1e-9


def test_market_exit_uses_taker_fee():
    """Emergency exit (is_market=True) must be more expensive than limit exit."""
    # Give taker > maker so the effect is visible
    model = TransactionCostModel(taker_fee_rate=0.002, maker_fee_rate=0.001)
    price = 67_000.0
    qty   = 0.1

    market_exit = model.exit_cost(price, qty, is_market=True)
    limit_exit  = model.exit_cost(price, qty, is_market=False)

    assert market_exit > limit_exit, (
        "Market exit should be more expensive than limit exit"
    )


def test_zero_quantity_returns_zero_cost():
    """Degenerate input: qty=0 must return 0, not raise."""
    model = TransactionCostModel()
    assert model.entry_cost(67_000.0, 0.0) == 0.0
    assert model.exit_cost(67_000.0,  0.0) == 0.0


def test_costs_are_always_positive():
    """Cost must be positive for any valid (price, quantity) pair."""
    model = TransactionCostModel()
    for price in [10_000.0, 50_000.0, 100_000.0]:
        for qty in [0.001, 0.01, 0.1, 1.0]:
            assert model.entry_cost(price, qty) > 0.0
            assert model.exit_cost(price, qty)  > 0.0
