import asyncio

import numpy as np
import pytest

from engine.pattern_detector import PatternDetector


def make_detector() -> PatternDetector:
    return PatternDetector(asyncio.Queue(), asyncio.Queue())


# --- ATR ---

def test_compute_atr_basic():
    # Simple case: every bar has range 10, so ATR should be 10
    highs = np.array([110.0] * 15)
    lows = np.array([100.0] * 15)
    closes = np.array([105.0] * 15)
    atr = PatternDetector._compute_atr(highs, lows, closes)
    assert abs(atr - 10.0) < 1e-6


def test_compute_atr_empty():
    atr = PatternDetector._compute_atr(np.array([100.0]), np.array([90.0]), np.array([95.0]))
    assert atr == 0.0


# --- Swing detection ---

def test_find_swing_highs():
    d = make_detector()
    # Put a clear high at index 5 in a window-5 context
    prices = np.array([100, 101, 102, 103, 104, 110, 104, 103, 102, 101, 100, 99, 98, 97, 96], dtype=float)
    highs = d._find_swings(prices, is_high=True)
    assert 5 in highs


def test_find_swing_lows():
    d = make_detector()
    prices = np.array([110, 109, 108, 107, 106, 90, 106, 107, 108, 109, 110, 111, 112, 113, 114], dtype=float)
    lows = d._find_swings(prices, is_high=False)
    assert 5 in lows


def test_find_swings_empty_when_no_pivots():
    d = make_detector()
    prices = np.linspace(100, 200, 20)  # monotone — no swing highs
    result = d._find_swings(prices, is_high=True)
    assert result == []


# --- S/R breakout ---

def test_sr_resistance_breakout():
    d = make_detector()
    base = np.full(30, 100.0)
    # Push last price well above 95th percentile
    closes = np.concatenate([base, [150.0]])
    highs = closes.copy()
    lows = closes.copy() - 1
    atr = 5.0
    vol_ratio = 2.0
    sig = d._check_sr_breakout(closes, highs, lows, atr, vol_ratio)
    assert sig is not None
    from models import Direction
    assert sig.direction == Direction.LONG


def test_sr_support_breakout():
    d = make_detector()
    base = np.full(30, 100.0)
    closes = np.concatenate([base, [50.0]])
    highs = closes.copy() + 1
    lows = closes.copy()
    atr = 5.0
    vol_ratio = 2.0
    sig = d._check_sr_breakout(closes, highs, lows, atr, vol_ratio)
    assert sig is not None
    from models import Direction
    assert sig.direction == Direction.SHORT


def test_sr_no_breakout_in_range():
    d = make_detector()
    closes = np.full(31, 100.0)
    highs = closes + 0.5
    lows = closes - 0.5
    atr = 5.0
    vol_ratio = 2.0
    sig = d._check_sr_breakout(closes, highs, lows, atr, vol_ratio)
    assert sig is None
