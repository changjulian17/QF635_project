"""Tests for FeatureComputer in strategy/features.py."""
from datetime import datetime, timezone, timedelta

import pytest

from strategy.features import FeatureComputer, FeatureParams, WelfordOnline
from models import Candle, FeatureVector, LOBLevel, LOBSnapshot, SharedState
from core.cvd import CVDCalculator


# ── Helpers ───────────────────────────────────────────────────────────────────

def _candle(
    close: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    volume: float = 10.0,
    dt: datetime | None = None,
) -> Candle:
    if dt is None:
        dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return Candle(
        open_time=dt,
        open=close - 0.5,
        high=high,
        low=low,
        close=close,
        volume=volume,
        is_closed=True,
    )


def _snap(bid: float = 99.5, ask: float = 100.5) -> LOBSnapshot:
    return LOBSnapshot(
        timestamp=datetime.now(timezone.utc),
        bids=[LOBLevel(price=bid - i * 0.1, qty=1.0 + i * 0.1) for i in range(20)],
        asks=[LOBLevel(price=ask + i * 0.1, qty=1.0 + i * 0.1) for i in range(20)],
        last_update_id=1000,
    )


def _warmed_up_fc(n_bars: int = 15, params: FeatureParams | None = None) -> FeatureComputer:
    """Return a FeatureComputer that has seen enough candles to produce a FeatureVector."""
    fc = FeatureComputer(params)
    for i in range(n_bars):
        fc.update_candle(_candle(close=100.0 + i * 0.1, volume=10.0 + i * 0.5))
    fc.update_orderbook(_snap(), walls=[])
    return fc


# ── Returns None before warm-up ───────────────────────────────────────────────

def test_compute_returns_none_before_rsi_ready():
    fc = FeatureComputer()
    cvd = CVDCalculator()
    state = SharedState()
    fc.update_candle(_candle())
    result = fc.compute(cvd, state)
    assert result is None


# ── FeatureVector has all required fields ─────────────────────────────────────

def test_feature_vector_has_all_fields():
    fc = _warmed_up_fc()
    cvd = CVDCalculator()
    state = SharedState()
    fv = fc.compute(cvd, state)
    assert fv is not None
    assert isinstance(fv, FeatureVector)
    # Check every field exists and is a sensible type
    assert isinstance(fv.lob_status, str)
    assert isinstance(fv.price_vs_vwap, float)
    assert isinstance(fv.obi_zscore, float)
    assert isinstance(fv.cvd_delta, float)
    assert isinstance(fv.vol_ratio, float)
    assert isinstance(fv.atr_percentile, float)
    assert isinstance(fv.rsi_value, float)
    assert isinstance(fv.spread_bps, float)
    assert isinstance(fv.pattern_r2, float)
    assert fv.vwap_reclaim in (0, 1)
    assert fv.vol_climax in (0, 1)
    assert fv.cvd_positive in (0, 1)
    assert fv.wall_detected in (0, 1)
    assert isinstance(fv.wall_distance_bps, float)
    assert isinstance(fv.absorption_ratio, float)
    assert fv.protection_wall_present in (0, 1)


def test_to_ml_array_length():
    fc = _warmed_up_fc()
    cvd = CVDCalculator()
    fv = fc.compute(cvd, SharedState())
    assert fv is not None
    arr = fv.to_ml_array()
    assert len(arr) == 15


# ── VWAP resets at midnight ───────────────────────────────────────────────────

def test_vwap_resets_at_midnight():
    fc = FeatureComputer()
    day1 = datetime(2025, 1, 1, 23, 55, 0, tzinfo=timezone.utc)
    day2 = datetime(2025, 1, 2,  0,  5, 0, tzinfo=timezone.utc)

    # Feed candles on day 1 at price 100
    for i in range(5):
        fc.update_candle(_candle(close=100.0, dt=day1 + timedelta(minutes=i)))

    vwap_day1 = fc._vwap

    # Feed candles on day 2 at price 200 — after reset, VWAP should reflect day 2 only
    for i in range(5):
        fc.update_candle(_candle(close=200.0, high=201.0, low=199.0, dt=day2 + timedelta(minutes=i)))

    vwap_day2 = fc._vwap
    assert vwap_day2 > vwap_day1 + 50   # day2 VWAP should be near 200, not 150


def test_vwap_accumulates_within_day():
    fc = FeatureComputer()
    base = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    fc.update_candle(_candle(close=100.0, high=102.0, low=98.0, volume=10.0, dt=base))
    fc.update_candle(_candle(close=110.0, high=112.0, low=108.0, volume=10.0, dt=base + timedelta(minutes=5)))
    # VWAP should be between 100 and 110
    assert 100.0 < fc._vwap < 110.0


# ── RSI ───────────────────────────────────────────────────────────────────────

def test_rsi_all_up_bars_near_100():
    fc = FeatureComputer(FeatureParams(rsi_period=5))
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(10):
        fc.update_candle(_candle(close=100.0 + i, dt=base + timedelta(minutes=i)))
    assert fc._rsi_value > 90.0


def test_rsi_all_down_bars_near_0():
    fc = FeatureComputer(FeatureParams(rsi_period=5))
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(10):
        fc.update_candle(_candle(close=100.0 - i, dt=base + timedelta(minutes=i)))
    assert fc._rsi_value < 10.0


# ── vol_ratio ─────────────────────────────────────────────────────────────────

def test_vol_ratio_high_volume_above_1():
    fc = FeatureComputer()
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # Seed with normal volume
    for i in range(15):
        fc.update_candle(_candle(volume=10.0, dt=base + timedelta(minutes=i)))
    # Big volume bar — vol_ratio should be >> 1
    fc.update_candle(_candle(volume=100.0, dt=base + timedelta(minutes=15)))
    assert fc._vol_ratio > 5.0


def test_vol_climax_fires_above_3x():
    fc = _warmed_up_fc()
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # Override vol_ratio to simulate a climax
    fc._vol_ratio = 4.0
    cvd = CVDCalculator()
    fv = fc.compute(cvd, SharedState())
    assert fv is not None
    assert fv.vol_climax == 1


# ── OBI z-score causality ─────────────────────────────────────────────────────

def test_obi_zscore_strictly_causal():
    """obi_zscore at step 50 must be identical whether history is 51 or 200 steps."""
    def _run(n_steps: int) -> float:
        fc = FeatureComputer(FeatureParams(rsi_period=5))
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        for i in range(15):
            fc.update_candle(_candle(dt=base + timedelta(minutes=i)))
        result_at_50 = None
        for i in range(n_steps):
            snap = _snap(bid=99.0 + i * 0.001, ask=101.0 + i * 0.001)
            fc.update_orderbook(snap, walls=[])
            if i == 49:
                result_at_50 = fc._obi_zscore
        return result_at_50

    z_51  = _run(51)
    z_200 = _run(200)
    assert z_51 is not None
    assert abs(z_51 - z_200) < 1e-9


# ── Wall features ─────────────────────────────────────────────────────────────

def test_wall_detected_when_walls_present():
    fc = _warmed_up_fc()
    snap = _snap(bid=99.5, ask=100.5)
    walls = [{"price": 99.0, "qty": 50.0, "sigma": 3.5, "side": "bid"}]
    fc.update_orderbook(snap, walls=walls)
    assert fc._wall_detected == 1
    assert fc._wall_distance_bps > 0


def test_wall_not_detected_when_no_walls():
    fc = _warmed_up_fc()
    fc.update_orderbook(_snap(), walls=[])
    assert fc._wall_detected == 0
    assert fc._wall_distance_bps == 0.0


# ── spread_bps ────────────────────────────────────────────────────────────────

def test_spread_bps_correct():
    fc = _warmed_up_fc()
    # bid=99.0, ask=101.0 → spread=2.0, mid=100.0 → 200 bps
    fc.update_orderbook(_snap(bid=99.0, ask=101.0), walls=[])
    assert abs(fc._spread_bps - 200.0) < 1.0
