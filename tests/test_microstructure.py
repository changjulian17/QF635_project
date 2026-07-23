"""Tests for strategy/microstructure.py — pure detection functions."""
import time

import pytest

from strategy.microstructure import (
    detect_absorption,
    detect_sweep_with_protection,
    identify_walls,
    price_move_floor_pct,
    rolling_abs_move_threshold,
)
from models import WallState


# ── Helpers ───────────────────────────────────────────────────────────────────

def _levels(base_qty: float, n: int = 20, spike_idx: int | None = None, spike_qty: float = 50.0):
    """Return (price, qty) list with optional outlier."""
    qtys = [base_qty + (i % 3) * 0.1 for i in range(n)]  # slight variance
    if spike_idx is not None:
        qtys[spike_idx] = spike_qty
    return [(float(30000 - i * 10), qtys[i]) for i in range(n)]


def _wall(
    side: str = "bid",
    qty_initial: float = 50.0,
    qty_current: float = 50.0,
    age_ms: int = 600,
) -> WallState:
    now = int(time.time() * 1000)
    return WallState(
        price         = 30000.0,
        qty_initial   = qty_initial,
        qty_current   = qty_current,
        first_seen_ts = now - age_ms,
        last_seen_ts  = now,
        side          = side,
        sigma         = 3.0,
    )


# ── identify_walls ────────────────────────────────────────────────────────────

def test_wall_identified_above_sigma():
    levels = _levels(1.0, n=20, spike_idx=5, spike_qty=50.0)
    walls = identify_walls(levels, "bid", sigma_threshold=2.5, window=5)
    prices = {w["price"] for w in walls}
    assert levels[5][0] in prices


def test_wall_below_sigma_not_identified():
    # No outlier — uniform quantities should produce no walls
    levels = _levels(1.0, n=20)
    walls = identify_walls(levels, "bid", sigma_threshold=2.5, window=5)
    assert walls == []


def test_wall_too_few_levels_returns_empty():
    levels = [(float(i), 1.0) for i in range(5)]   # fewer than 2*window+1
    assert identify_walls(levels, "bid") == []


def test_wall_sigma_field_populated():
    levels = _levels(1.0, n=20, spike_idx=10, spike_qty=50.0)
    walls = identify_walls(levels, "ask", sigma_threshold=2.5, window=5)
    assert all(w["sigma"] >= 2.5 for w in walls)


def test_wall_side_field_correct():
    levels = _levels(1.0, n=20, spike_idx=10, spike_qty=50.0)
    for side in ("bid", "ask"):
        walls = identify_walls(levels, side)
        assert all(w["side"] == side for w in walls)


# ── detect_absorption ─────────────────────────────────────────────────────────
# Directional convention: bid wall absorbs sellers (cvd_delta_1t < 0);
#                         ask wall absorbs buyers  (cvd_delta_1t > 0).

def test_absorption_bid_wall_with_sell_aggression():
    # Bid wall, sell aggression (CVD negative) → should arm
    ws = _wall(side="bid", qty_initial=50.0, qty_current=40.0, age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=-0.5, price_move_pct=0.0001, reload_ratio=0.80) is True


def test_absorption_ask_wall_with_buy_aggression():
    # Ask wall, buy aggression (CVD positive) → should arm
    ws = _wall(side="ask", qty_initial=50.0, qty_current=40.0, age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=0.5, price_move_pct=0.0001, reload_ratio=0.80) is True


def test_absorption_false_wrong_direction_bid():
    # Bid wall but buy aggression (positive CVD) → wrong direction → False
    ws = _wall(side="bid", qty_initial=50.0, qty_current=40.0, age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=0.5, price_move_pct=0.0001, reload_ratio=0.80) is False


def test_absorption_false_wrong_direction_ask():
    # Ask wall but sell aggression (negative CVD) → wrong direction → False
    ws = _wall(side="ask", qty_initial=50.0, qty_current=40.0, age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=-0.5, price_move_pct=0.0001, reload_ratio=0.80) is False


def test_absorption_false_when_not_persistent():
    ws = _wall(side="bid", qty_initial=50.0, qty_current=40.0, age_ms=100)  # < 500 ms
    assert detect_absorption(ws, cvd_delta_1t=-0.5, price_move_pct=0.0001, reload_ratio=0.80) is False


def test_absorption_false_when_price_breaks():
    ws = _wall(side="bid", qty_initial=50.0, qty_current=40.0, age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=-0.5, price_move_pct=0.001, reload_ratio=0.80) is False


def test_absorption_false_when_no_aggression():
    ws = _wall(side="bid", age_ms=600)
    assert detect_absorption(ws, cvd_delta_1t=0.0, price_move_pct=0.0001, reload_ratio=0.80) is False


def test_absorption_false_when_reload_low():
    ws = _wall(side="bid", qty_initial=50.0, qty_current=25.0, age_ms=600)  # reload=0.50 < 0.70
    assert detect_absorption(ws, cvd_delta_1t=-0.5, price_move_pct=0.0001, reload_ratio=0.50) is False


# ── detect_sweep_with_protection ─────────────────────────────────────────────

def _fresh_ask_wall() -> WallState:
    now = int(time.time() * 1000)
    return WallState(
        price=30100.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="ask", sigma=3.0,
    )


def test_sweep_with_protection_fires():
    consumed = _wall(side="ask", qty_initial=50.0, qty_current=5.0, age_ms=600)
    # Replace with bid protection wall for long sweep
    now = int(time.time() * 1000)
    bid_wall = WallState(
        price=29995.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    fired, info = detect_sweep_with_protection(
        wall_consumed      = consumed,
        price_move_pct     = 0.0005,   # > 0.03%
        cvd_spike_std      = 0.1,      # telemetry only; no entry gate
        fresh_walls_behind = [bid_wall],
        mid_price          = 30005.0,
    )
    assert fired is True
    assert info["direction"] == "LONG"
    assert info["consumed_wall"] is consumed


def test_sweep_without_protection_does_not_fire():
    consumed = _wall(side="ask", qty_initial=50.0, qty_current=5.0, age_ms=600)
    fired, info = detect_sweep_with_protection(
        wall_consumed      = consumed,
        price_move_pct     = 0.0005,
        cvd_spike_std      = 2.0,
        fresh_walls_behind = [],       # no protection wall
    )
    assert fired is False
    assert info == {}


def test_sweep_requires_all_four_conditions():
    now = int(time.time() * 1000)
    protection = WallState(
        price=29995.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="bid", sigma=3.0,
    )

    # Not consumed (reload_ratio = 0.90)
    not_consumed = _wall(side="ask", qty_initial=50.0, qty_current=45.0, age_ms=600)
    fired, _ = detect_sweep_with_protection(not_consumed, 0.0005, 2.0, [protection], mid_price=30005.0)
    assert fired is False

    # Price did not move enough
    consumed = _wall(side="ask", qty_initial=50.0, qty_current=5.0, age_ms=600)
    fired, _ = detect_sweep_with_protection(consumed, 0.00001, 2.0, [protection], mid_price=30005.0)
    assert fired is False

    # CVD spike is telemetry only and must not block entry.
    fired, _ = detect_sweep_with_protection(consumed, 0.0005, 0.0, [protection], mid_price=30005.0)
    assert fired is True

    # All conditions met
    fired, _ = detect_sweep_with_protection(consumed, 0.0005, 2.0, [protection], mid_price=30005.0)
    assert fired is True


def test_sweep_direction_short_from_bid_wall():
    now = int(time.time() * 1000)
    consumed_bid = _wall(side="bid", qty_initial=50.0, qty_current=5.0, age_ms=600)
    ask_protection = WallState(
        price=30005.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="ask", sigma=3.0,
    )
    fired, info = detect_sweep_with_protection(
        wall_consumed=consumed_bid,
        price_move_pct=-0.0005,
        cvd_spike_std=2.0,
        fresh_walls_behind=[ask_protection],
        mid_price=29995.0,
    )
    assert fired is True
    assert info["direction"] == "SHORT"


def test_sweep_rejects_wrong_signed_price_move():
    now = int(time.time() * 1000)
    consumed_ask = _wall(side="ask", qty_initial=50.0, qty_current=5.0, age_ms=600)
    bid_protection = WallState(
        price=29995.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    fired, _ = detect_sweep_with_protection(
        consumed_ask, -0.0005, 5.0, [bid_protection], mid_price=30005.0
    )
    assert fired is False


def test_sweep_rejects_far_protection_wall():
    now = int(time.time() * 1000)
    consumed_ask = _wall(side="ask", qty_initial=50.0, qty_current=5.0, age_ms=600)
    far_bid = WallState(
        price=29900.0, qty_initial=30.0, qty_current=30.0,
        first_seen_ts=now - 500, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    fired, _ = detect_sweep_with_protection(
        consumed_ask, 0.0005, 5.0, [far_bid], mid_price=30005.0
    )
    assert fired is False


def test_dynamic_threshold_floor_and_warmed_percentile():
    floor = price_move_floor_pct()
    assert rolling_abs_move_threshold([0.01], floor_pct=floor, min_samples=3) == pytest.approx(floor)
    threshold = rolling_abs_move_threshold(
        [0.0001, 0.0002, 0.0004, 0.0010],
        floor_pct=floor,
        percentile=0.75,
        min_samples=3,
    )
    assert threshold == pytest.approx(0.0004)


# --- FeatureComputer injection ---

def test_update_orderbook_called_when_feature_computer_injected():
    import asyncio
    from unittest.mock import MagicMock, AsyncMock

    fc = MagicMock()
    detector = _make_detector(feature_computer=fc)

    snapshot = {
        "bids": [["30010.0", "1.0"], ["30005.0", "0.5"]],
        "asks": [["30015.0", "0.8"], ["30020.0", "0.3"]],
    }

    asyncio.run(detector._process_snapshot(snapshot))
    fc.update_orderbook.assert_called_once()
    args = fc.update_orderbook.call_args
    snap, wall_dicts = args[0]
    assert snap.bids[0].price == pytest.approx(30010.0)
    assert snap.asks[0].price == pytest.approx(30015.0)


def test_absorption_batch_deduplicates_same_wall():
    """Same wall price armed on two consecutive ticks produces a single batch entry."""
    import asyncio
    detector = _make_detector()

    snapshot = {
        "bids": [["30000.0", "1.0"], ["29990.0", "0.5"]],
        "asks": [["30010.0", "0.8"]],
    }

    now = int(time.time() * 1000)
    wall = WallState(
        price=30000.0, qty_initial=50.0, qty_current=50.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    detector._wall_states[30000.0] = wall
    detector._absorption_armed[30000.0] = True

    # Arm the same wall twice by direct batch mutation (simulates two consecutive ticks)
    detector._absorption_batch[wall.price] = wall
    detector._absorption_batch[wall.price] = wall  # second tick — same key

    assert len(detector._absorption_batch) == 1


# ── Hub event emission ────────────────────────────────────────────────────


def test_absorption_emits_hub_event_on_first_arm():
    """Absorption transition (not-armed → armed) broadcasts one typed event payload."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    hub = MagicMock()
    hub.broadcast = AsyncMock()
    detector = _make_detector(hub=hub)
    detector._cvd.get_cvd_delta = MagicMock(return_value=-1.0)  # sellers aggressing → bid wall absorbs

    now = int(time.time() * 1000)
    wall = WallState(
        price=30000.0, qty_initial=50.0, qty_current=50.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    detector._wall_states[30000.0] = wall
    detector._prev_mid = 0.0  # → price_move_pct = 0.0, within floor

    snapshot = {
        "bids": [["30000.0", "50.0"], ["29990.0", "0.1"]],
        "asks": [["30010.0", "0.1"]],
    }
    asyncio.run(detector._process_snapshot(snapshot))

    assert hub.broadcast.call_count == 1
    payload = hub.broadcast.call_args[0][0]
    assert payload["type"]  == "event"
    assert payload["event"] == "absorption"
    assert payload["price"] == pytest.approx(30000.0)
    assert payload["side"]  == "bid"
    assert "ts" in payload
    assert payload["reload_ratio"] == pytest.approx(1.0)


def test_absorption_does_not_re_emit_on_subsequent_tick():
    """Once a wall is armed, repeated detections must not broadcast again."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    hub = MagicMock()
    hub.broadcast = AsyncMock()
    detector = _make_detector(hub=hub)
    detector._cvd.get_cvd_delta = MagicMock(return_value=-1.0)

    now = int(time.time() * 1000)
    wall = WallState(
        price=30000.0, qty_initial=50.0, qty_current=50.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    detector._wall_states[30000.0] = wall
    detector._absorption_armed[30000.0] = True  # already armed from a prior tick

    snapshot = {
        "bids": [["30000.0", "50.0"], ["29990.0", "0.1"]],
        "asks": [["30010.0", "0.1"]],
    }
    asyncio.run(detector._process_snapshot(snapshot))
    assert hub.broadcast.call_count == 0


def test_sweep_emits_hub_event_alongside_signal_queue():
    """Sweep+protection firing broadcasts a typed sweep payload to the hub."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    hub = MagicMock()
    hub.broadcast = AsyncMock()
    detector = _make_detector(hub=hub)
    detector._cvd.get_cvd_delta = MagicMock(return_value=2.0)
    detector._cvd.get_cvd_tick_std = MagicMock(return_value=0.5)
    detector._cvd.is_warmed_up = True

    now = int(time.time() * 1000)
    # Consumed ask wall (reload_ratio = 0.1 < 0.15) → LONG sweep
    detector._wall_states[30050.0] = WallState(
        price=30050.0, qty_initial=20.0, qty_current=2.0,
        first_seen_ts=now - 5_000, last_seen_ts=now,
        side="ask", sigma=3.0,
    )
    # Fresh bid wall behind (first_seen within LOB_FRESH_WALL_MS)
    detector._wall_states[29950.0] = WallState(
        price=29950.0, qty_initial=10.0, qty_current=10.0,
        first_seen_ts=now - 100, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    detector._prev_mid = 29950.0  # mid jumps to ~30000 → +16.7 bps move (above 3 bps floor)

    snapshot = {
        "bids": [["29950.0", "10.0"], ["29940.0", "0.1"]],
        "asks": [["30050.0", "2.0"], ["30060.0", "0.1"]],
    }
    asyncio.run(detector._process_snapshot(snapshot))

    # Sweep should have fired: signal on queue + hub event broadcast
    assert not detector._signal_queue.empty()
    sweep_calls = [c for c in hub.broadcast.call_args_list
                   if c.args[0].get("event") == "sweep"]
    assert len(sweep_calls) == 1
    payload = sweep_calls[0].args[0]
    assert payload["type"]      == "event"
    assert payload["side"]      == "ask"
    assert payload["direction"] == "LONG"
    assert payload["price"]     == pytest.approx(30050.0)
    assert payload["price_move_pct"] > 0


def test_no_hub_no_emission_no_crash():
    """Detector must run normally when hub is None (the default)."""
    import asyncio
    from unittest.mock import MagicMock

    detector = _make_detector(hub=None)
    detector._cvd.get_cvd_delta = MagicMock(return_value=-1.0)

    now = int(time.time() * 1000)
    detector._wall_states[30000.0] = WallState(
        price=30000.0, qty_initial=50.0, qty_current=50.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side="bid", sigma=3.0,
    )
    snapshot = {
        "bids": [["30000.0", "50.0"], ["29990.0", "0.1"]],
        "asks": [["30010.0", "0.1"]],
    }
    # Just confirm it doesn't raise — no hub means no broadcast attempt
    asyncio.run(detector._process_snapshot(snapshot))


def _make_detector(feature_computer=None, hub=None):
    import asyncio
    from strategy.microstructure import MicrostructureDetector
    from unittest.mock import MagicMock

    cvd = MagicMock()
    cvd.get_cvd_delta.return_value = 0.0
    cvd.get_cvd_tick_std.return_value = 0.0
    cvd.is_warmed_up = False

    return MicrostructureDetector(
        depth_queue=asyncio.Queue(),
        trade_queue=asyncio.Queue(),
        signal_queue=asyncio.Queue(),
        cvd_calculator=cvd,
        feature_computer=feature_computer,
        hub=hub,
    )
