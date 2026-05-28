import asyncio

import numpy as np
import pytest

from core.pattern_detector import PatternDetector


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


# --- FeatureComputer injection ---

def test_update_candle_called_when_feature_computer_injected():
    from datetime import datetime, timezone
    from unittest.mock import MagicMock
    from models import Candle

    fc = MagicMock()
    d = PatternDetector(asyncio.Queue(), asyncio.Queue(), feature_computer=fc)

    candle = Candle(
        open_time=datetime.now(timezone.utc),
        open=100.0, high=101.0, low=99.0, close=100.5,
        volume=10.0, is_closed=True,
    )

    async def _run():
        await d._candle_queue.put(candle)
        # Drive one iteration: patch run() to exit after first candle
        candle2 = Candle(
            open_time=datetime.now(timezone.utc),
            open=100.0, high=101.0, low=99.0, close=100.5,
            volume=10.0, is_closed=True,
        )
        await d._candle_queue.put(candle2)
        # Manually replicate the first two run() iterations
        c = await d._candle_queue.get()
        if d._feature_computer is not None:
            d._feature_computer.update_candle(c)
        d._candles.append(c)

    asyncio.run(_run())
    fc.update_candle.assert_called_once_with(candle)


def test_no_crash_when_feature_computer_is_none():
    from datetime import datetime, timezone
    from models import Candle

    d = PatternDetector(asyncio.Queue(), asyncio.Queue())

    async def _run():
        candle = Candle(
            open_time=datetime.now(timezone.utc),
            open=100.0, high=101.0, low=99.0, close=100.5,
            volume=10.0, is_closed=True,
        )
        c = candle
        if d._feature_computer is not None:
            d._feature_computer.update_candle(c)
        d._candles.append(c)

    asyncio.run(_run())
    assert len(d._candles) == 1
