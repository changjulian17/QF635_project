"""Unit tests for MicrostructureEngine signal detectors — no network, no async."""
import asyncio
from collections import deque
from datetime import datetime, timezone

import pytest

from engine.lob_engine import LocalOrderBook
from engine.microstructure_engine import MicrostructureEngine
from models import AggTrade, LOBLevel, LOBSnapshot


def make_engine() -> MicrostructureEngine:
    return MicrostructureEngine(
        lob=LocalOrderBook(),
        trade_queue=asyncio.Queue(),
        depth_queue=asyncio.Queue(),
        metrics_store=deque(maxlen=100),
        db_path=":memory:",
    )


def make_snap(bids: list[tuple], asks: list[tuple], ts=None) -> LOBSnapshot:
    return LOBSnapshot(
        timestamp=ts or datetime.now(timezone.utc),
        bids=[LOBLevel(price=p, qty=q) for p, q in bids],
        asks=[LOBLevel(price=p, qty=q) for p, q in asks],
        last_update_id=1,
    )


def make_trade(price: float, qty: float, is_buyer_maker: bool) -> AggTrade:
    return AggTrade(
        timestamp=datetime.now(timezone.utc),
        price=price,
        qty=qty,
        is_buyer_maker=is_buyer_maker,
    )


BIDS = [(30000.0, 2.0), (29999.0, 1.5), (29998.0, 1.0)]
ASKS = [(30001.0, 2.0), (30002.0, 1.5), (30003.0, 1.0)]


# ── OBI ───────────────────────────────────────────────────────────────────────

def test_obi_neutral_equal_book():
    engine = make_engine()
    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    bar = engine._compute_bar(snap, [])
    assert bar.obi == 0.0


def test_obi_positive_when_bid_heavy():
    engine = make_engine()
    snap = make_snap(bids=[(30000.0, 3.0)], asks=[(30001.0, 1.0)])
    bar = engine._compute_bar(snap, [])
    assert bar.obi > 0


def test_obi_negative_when_ask_heavy():
    engine = make_engine()
    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 3.0)])
    bar = engine._compute_bar(snap, [])
    assert bar.obi < 0


def test_obi_range():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    bar = engine._compute_bar(snap, [])
    assert -1.0 <= bar.obi <= 1.0


# ── CVD / volume delta ────────────────────────────────────────────────────────

def test_cvd_increases_on_buy_aggression():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    trades = [make_trade(30001.0, 0.5, is_buyer_maker=False)]  # buyer aggressed
    bar = engine._compute_bar(snap, trades)
    assert bar.delta > 0
    assert bar.buy_volume == pytest.approx(0.5)
    assert bar.sell_volume == 0.0


def test_cvd_decreases_on_sell_aggression():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    trades = [make_trade(30000.0, 0.5, is_buyer_maker=True)]  # seller aggressed
    bar = engine._compute_bar(snap, trades)
    assert bar.delta < 0
    assert bar.sell_volume == pytest.approx(0.5)


def test_cvd_accumulates_across_bars():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    engine._compute_bar(snap, [make_trade(30001.0, 1.0, False)])  # +1
    bar2 = engine._compute_bar(snap, [make_trade(30001.0, 0.5, False)])  # +0.5
    assert bar2.cvd == pytest.approx(1.5)


# ── Sweep detection ───────────────────────────────────────────────────────────

def test_detect_sweep_up_large_buy():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    # Buy volume > 80% of top-5 ask levels
    trades = [make_trade(30001.0, 10.0, is_buyer_maker=False)]
    su, sd = engine._detect_sweep(snap, trades)
    assert su is True
    assert sd is False


def test_detect_sweep_down_large_sell():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    trades = [make_trade(30000.0, 10.0, is_buyer_maker=True)]
    su, sd = engine._detect_sweep(snap, trades)
    assert su is False
    assert sd is True


def test_detect_sweep_no_trades():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    su, sd = engine._detect_sweep(snap, [])
    assert su is False
    assert sd is False


def test_detect_sweep_small_trade_no_trigger():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    trades = [make_trade(30001.0, 0.001, is_buyer_maker=False)]
    su, sd = engine._detect_sweep(snap, trades)
    assert su is False


# ── Book flip (spoofing) detection ────────────────────────────────────────────

def test_detect_book_flip_returns_false_without_prev_snap():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    fb, fa = engine._detect_book_flip(snap, [])
    assert fb is False
    assert fa is False


def test_detect_book_flip_no_flip_stable_book():
    engine = make_engine()
    snap1 = make_snap(bids=BIDS, asks=ASKS)
    engine._prev_snap = snap1
    # Same book — no large level disappeared
    fb, fa = engine._detect_book_flip(snap1, [])
    assert fb is False
    assert fa is False


# ── Break+protect detection ───────────────────────────────────────────────────

def test_break_protect_returns_false_without_prev_snap():
    engine = make_engine()
    snap = make_snap(bids=BIDS, asks=ASKS)
    bpl, bps = engine._detect_break_protect(snap, [], 30000.5, 0.0)
    assert bpl is False
    assert bps is False


def test_break_protect_long_requires_obi_above_threshold():
    engine = make_engine()
    snap1 = make_snap(bids=BIDS, asks=ASKS)
    engine._prev_snap = snap1

    # Trigger an "up" breakout
    big_buy = [make_trade(30001.0, 5.0, is_buyer_maker=False)]
    engine._detect_break_protect(snap1, big_buy, 30005.0, 0.0)  # records breakout

    snap2 = make_snap(bids=BIDS, asks=ASKS)
    # OBI below threshold → no confirmation
    bpl, _ = engine._detect_break_protect(snap2, [], 30005.0, obi=0.1)
    assert bpl is False

    # OBI above threshold → confirmed
    bpl, _ = engine._detect_break_protect(snap2, [], 30005.0, obi=0.5)
    assert bpl is True


# ── Mid price / spread ────────────────────────────────────────────────────────

def test_mid_price_and_spread():
    engine = make_engine()
    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30002.0, 1.0)])
    bar = engine._compute_bar(snap, [])
    assert bar.mid_price == pytest.approx(30001.0)
    assert bar.spread == pytest.approx(2.0)
