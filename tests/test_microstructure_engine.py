"""Unit tests for MicrostructureEngine signal detectors — no network, no async."""
import asyncio
from collections import deque
from datetime import datetime, timezone

import pytest

from core.lob_engine import LocalOrderBook
from engine.microstructure_engine import MicrostructureEngine
from models import AggTrade, LOBLevel, LOBSnapshot


def make_engine() -> MicrostructureEngine:
    return MicrostructureEngine(
        lob=LocalOrderBook(),
        trade_queue=asyncio.Queue(),
        depth_queue=asyncio.Queue(),
        metrics_store=deque(maxlen=100),
        ms_bar_queue=asyncio.Queue(),
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


# ── Sweep: pre-trade book reference ──────────────────────────────────────────

def test_detect_sweep_uses_pre_trade_book():
    """Sweep denominator is the pre-trade book, not the depleted post-trade book."""
    engine = make_engine()
    # Pre-trade: 10 BTC across top-5 ask levels
    pre_snap = make_snap(
        bids=[(29999.0, 2.0)] * 3,
        asks=[(30001.0, 2.0), (30002.0, 2.0), (30003.0, 2.0), (30004.0, 2.0), (30005.0, 2.0)],
    )
    engine._prev_snap = pre_snap
    # Post-trade: sweep consumed 8.5 BTC — only 1.5 BTC residual remains
    post_snap = make_snap(
        bids=[(29999.0, 2.0)] * 3,
        asks=[(30005.0, 1.5)],
    )
    # buy_vol=8.5 > pre-trade top_ask_vol(10) * 0.8 = 8.0 → True
    trades = [make_trade(30001.0, 8.5, is_buyer_maker=False)]
    su, sd = engine._detect_sweep(post_snap, trades)
    assert su is True


def test_detect_sweep_no_trigger_against_pre_trade_book():
    """Trade exceeding depleted post-trade book but not the pre-trade book is not a sweep."""
    engine = make_engine()
    pre_snap = make_snap(
        bids=[(29999.0, 2.0)] * 3,
        asks=[(30001.0, 2.0), (30002.0, 2.0), (30003.0, 2.0), (30004.0, 2.0), (30005.0, 2.0)],
    )
    engine._prev_snap = pre_snap
    # Residual only 0.5 BTC; a 3.0 BTC buy exceeds 0.5*0.8 but NOT 10*0.8=8.0
    post_snap = make_snap(
        bids=[(29999.0, 2.0)] * 3,
        asks=[(30005.0, 0.5)],
    )
    trades = [make_trade(30001.0, 3.0, is_buyer_maker=False)]
    su, _ = engine._detect_sweep(post_snap, trades)
    assert su is False


# ── Iceberg detection ─────────────────────────────────────────────────────────

def test_iceberg_bid_normal_partial_fill_no_trigger():
    """Level hit by tiny volume retaining ~92% is NOT an iceberg — no hidden reserve."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(30000.0, 100.0)], asks=[(30001.0, 1.0)])
    # hit_vol=8; natural remainder = 92; current=92 → recovered=0, not > 0
    snap = make_snap(bids=[(30000.0, 92.0)], asks=[(30001.0, 1.0)])
    trades = [make_trade(30000.0, 8.0, is_buyer_maker=True)]
    ib, ia = engine._detect_iceberg(snap, trades)
    assert ib is False


def test_iceberg_bid_triggers_on_genuine_replenishment():
    """Level has MORE qty than the natural post-fill remainder — hidden reserve detected."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(30000.0, 100.0)], asks=[(30001.0, 1.0)])
    # hit_vol=20; natural remainder=80; current=95 → recovered=15 > 0 AND 95 >= 80
    snap = make_snap(bids=[(30000.0, 95.0)], asks=[(30001.0, 1.0)])
    trades = [make_trade(30000.0, 20.0, is_buyer_maker=True)]
    ib, ia = engine._detect_iceberg(snap, trades)
    assert ib is True


def test_iceberg_ask_normal_partial_fill_no_trigger():
    """Ask level hit by small volume that barely changed — not an iceberg."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(29999.0, 1.0)], asks=[(30001.0, 100.0)])
    # hit_vol=5; natural remainder=95; current=92 → recovered=92-95=-3 < 0
    snap = make_snap(bids=[(29999.0, 1.0)], asks=[(30001.0, 92.0)])
    trades = [make_trade(30001.0, 5.0, is_buyer_maker=False)]
    ib, ia = engine._detect_iceberg(snap, trades)
    assert ia is False


def test_iceberg_ask_triggers_on_genuine_replenishment():
    """Ask side: level shows more qty than natural post-fill remainder."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(29999.0, 1.0)], asks=[(30001.0, 100.0)])
    # hit_vol=30; natural remainder=70; current=88 → recovered=18 > 0 AND 88 >= 80
    snap = make_snap(bids=[(29999.0, 1.0)], asks=[(30001.0, 88.0)])
    trades = [make_trade(30001.0, 30.0, is_buyer_maker=False)]
    ib, ia = engine._detect_iceberg(snap, trades)
    assert ia is True


# ── Book-flip z-score baseline ────────────────────────────────────────────────

def test_book_flip_baseline_from_prev_snap():
    """Z-score is computed against the prev_snap distribution, not the thinned current book."""
    engine = make_engine()
    # prev_snap: one massive bid wall plus small normal levels
    prev_snap = make_snap(
        bids=[(30000.0, 500.0), (29999.0, 2.0), (29998.0, 2.0), (29997.0, 2.0)],
        asks=[(30001.0, 2.0), (30002.0, 2.0), (30003.0, 2.0), (30004.0, 2.0)],
    )
    engine._prev_snap = prev_snap
    # current snap: the 500-unit bid vanished (cancelled), book is now thin
    snap = make_snap(
        bids=[(29999.0, 2.0), (29998.0, 2.0), (29997.0, 2.0)],
        asks=[(30001.0, 2.0), (30002.0, 2.0), (30003.0, 2.0), (30004.0, 2.0)],
    )
    trades = [make_trade(29999.5, 5.0, is_buyer_maker=True)]
    # Should not raise; fix ensures baseline is from prev_snap (has the 500-unit level)
    fb, fa = engine._detect_book_flip(snap, trades)
    assert isinstance(fb, bool)
    assert isinstance(fa, bool)


def test_book_flip_no_false_positive_from_deflated_baseline():
    """A non-outlier level disappearing must not trigger a flip regardless of current book state."""
    engine = make_engine()
    # All levels uniform — no outlier in prev_snap
    prev_snap = make_snap(
        bids=[(30000.0 - i, 2.0) for i in range(5)],
        asks=[(30001.0 + i, 2.0) for i in range(5)],
    )
    engine._prev_snap = prev_snap
    # One bid level gone, current book is thin (as if cancelled)
    snap = make_snap(
        bids=[(30000.0 - i, 2.0) for i in range(1, 5)],
        asks=[(30001.0 + i, 2.0) for i in range(5)],
    )
    trades = [make_trade(29999.5, 3.0, is_buyer_maker=True)]
    fb, fa = engine._detect_book_flip(snap, trades)
    # The disappeared level (qty=2.0) is not statistically large in prev_snap → no flip
    assert fb is False


# ── Price dict pruning ────────────────────────────────────────────────────────

def test_prune_removes_stale_prices():
    """Keys far from current mid are evicted from all price dicts."""
    engine = make_engine()
    mid_price = 30000.0
    stale_price = 20000.0  # ~33% away — well outside ±2% band
    in_band_price = 30010.0  # ~0.03% away — inside ±2% band

    engine._level_hist[stale_price].append(5.0)
    engine._peak_bid_qty[stale_price] = 10.0
    engine._peak_ask_qty[stale_price] = 10.0
    engine._level_hist[in_band_price].append(3.0)
    engine._peak_bid_qty[in_band_price] = 3.0

    engine._prune_price_dicts(mid_price)

    assert stale_price not in engine._level_hist
    assert stale_price not in engine._peak_bid_qty
    assert stale_price not in engine._peak_ask_qty
    assert in_band_price in engine._level_hist
    assert in_band_price in engine._peak_bid_qty


def test_prune_triggered_every_n_bars():
    """Pruning fires when _bar_count reaches PRICE_PRUNE_INTERVAL."""
    from config import settings as cfg
    engine = make_engine()
    engine._bar_count = cfg.PRICE_PRUNE_INTERVAL - 1  # one bar away from trigger

    stale_price = 1.0
    engine._level_hist[stale_price].append(1.0)
    engine._peak_bid_qty[stale_price] = 1.0

    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    engine._compute_bar(snap, [])  # increments to PRICE_PRUNE_INTERVAL → prune fires

    assert stale_price not in engine._level_hist
    assert stale_price not in engine._peak_bid_qty


def test_prune_not_triggered_before_interval():
    """Dict entries survive when the prune interval has not yet been reached."""
    engine = make_engine()
    engine._bar_count = 0

    stale_price = 1.0
    engine._level_hist[stale_price].append(1.0)

    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    engine._compute_bar(snap, [])  # bar_count → 1, far from interval

    assert stale_price in engine._level_hist


# ── Liquidity flip detection ──────────────────────────────────────────────────

def test_detect_liq_flip_support_to_resistance():
    """A price that was a peak bid is now present on the ask side at ≥50% of peak qty."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    engine._peak_bid_qty[30000.0] = 100.0  # historically dominant bid
    snap = make_snap(bids=[(29999.0, 1.0)], asks=[(30000.0, 60.0), (30001.0, 1.0)])
    ltr, lts = engine._detect_liq_flip(snap)
    assert ltr is True
    assert lts is False


def test_detect_liq_flip_no_signal_without_history():
    """No peak history → liq_flip always returns (False, False)."""
    engine = make_engine()
    engine._prev_snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    snap = make_snap(bids=[(30000.0, 1.0)], asks=[(30001.0, 1.0)])
    ltr, lts = engine._detect_liq_flip(snap)
    assert ltr is False
    assert lts is False


# ── Break+protect: minimum volume floor ──────────────────────────────────────

def test_break_protect_below_min_vol_no_breakout():
    """Buy volume below BREAK_MIN_VOL does not register a breakout even with 2:1 ratio."""
    from config import settings as cfg
    engine = make_engine()
    prev = make_snap(bids=[(30000.0, 2.0)], asks=[(30001.0, 2.0)])
    engine._prev_snap = prev
    snap = make_snap(bids=[(30001.0, 2.0)], asks=[(30002.0, 2.0)])  # mid moved up
    tiny_buy = [make_trade(30001.0, cfg.BREAK_MIN_VOL * 0.1, is_buyer_maker=False)]
    bpl, bps = engine._detect_break_protect(snap, tiny_buy, 30001.5, obi=0.5)
    assert bpl is False
