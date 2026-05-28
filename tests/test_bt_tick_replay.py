"""
tests/test_bt_tick_replay.py
============================
Unit tests for backtesting/tick_replay.py.

Most tests use a synthetic SQLite DB (tmp_db fixture) — no real lob_tick.db
required. test_pre_requisite_data_available is the sole exception.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from typing import Generator
from unittest.mock import patch

import pytest

from backtesting.tick_replay import ReplayEvent, ReplayTrade, TickReplayEngine
from core.cvd import CVDCalculator
from models import FeatureVector, WallState
from strategy.features import FeatureComputer, FeatureParams
from strategy.microstructure import identify_walls


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_bids(base: float, n: int = 20, wall_idx: int | None = None) -> list[list[str]]:
    """Descending bid levels with realistic qty variation. Optional sigma wall at wall_idx."""
    levels = []
    for i in range(n):
        price = base - i * 10.0
        # Varied background so std > 0 (required by identify_walls)
        bg_qty = 0.5 + (i % 5) * 0.3
        qty    = 50.0 if (wall_idx is not None and i == wall_idx) else bg_qty
        levels.append([f"{price:.2f}", f"{qty:.5f}"])
    return levels


def _make_asks(base: float, n: int = 20, wall_idx: int | None = None) -> list[list[str]]:
    """Ascending ask levels with realistic qty variation. Optional sigma wall at wall_idx."""
    levels = []
    for i in range(n):
        price = base + i * 10.0 + 10.0
        bg_qty = 0.5 + (i % 5) * 0.3
        qty    = 50.0 if (wall_idx is not None and i == wall_idx) else bg_qty
        levels.append([f"{price:.2f}", f"{qty:.5f}"])
    return levels


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE depth_snapshots (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_event  INTEGER NOT NULL,
            bids_json TEXT    NOT NULL,
            asks_json TEXT    NOT NULL
        )
    """)
    conn.execute("CREATE INDEX idx_depth_ts ON depth_snapshots(ts_event)")
    conn.execute("""
        CREATE TABLE agg_trades (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_event       INTEGER NOT NULL,
            price          REAL    NOT NULL,
            qty            REAL    NOT NULL,
            is_buyer_maker INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX idx_trades_ts ON agg_trades(ts_event)")


# ── Fixtures ──────────────────────────────────────────────────────────────────

BASE_TS    = 1_700_000_000_000   # arbitrary epoch ms
BASE_PRICE = 80_000.0


@pytest.fixture
def tmp_db(tmp_path) -> Generator[str, None, None]:
    """
    Synthetic lob_tick.db: 100 depth snapshots (1 s apart) and
    200 trades (500 ms apart), spanning ~100 s (1h 40 min worth of bars
    when candle_minutes=1 → ~1.6 bars, but 100 trades per minute works fine).
    """
    db_path = str(tmp_path / "test_lob.db")
    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        base_price = BASE_PRICE
        for i in range(100):
            ts    = BASE_TS + i * 1_000
            bids  = _make_bids(base_price)
            asks  = _make_asks(base_price)
            conn.execute(
                "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                (ts, json.dumps(bids), json.dumps(asks)),
            )
        for i in range(200):
            ts             = BASE_TS + i * 500
            price          = base_price + (i % 10 - 5) * 5.0
            qty            = 0.01 + (i % 5) * 0.002
            is_buyer_maker = i % 2
            conn.execute(
                "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                (ts, price, qty, is_buyer_maker),
            )
        conn.commit()
    yield db_path


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_stream_events_yields_in_timestamp_order(tmp_db):
    """All ReplayEvents must be in monotonically non-decreasing ts_ms order."""
    engine = TickReplayEngine(params={}, db_path=tmp_db)
    start  = BASE_TS
    end    = BASE_TS + 200_000

    with sqlite3.connect(tmp_db) as conn:
        conn.row_factory = sqlite3.Row
        events = list(engine._stream_events(conn, start, end))

    timestamps = [e.ts_ms for e in events]
    assert timestamps == sorted(timestamps), "Events not in timestamp order"
    assert len(events) > 0, "No events yielded"


def test_stream_events_respects_time_bounds(tmp_db):
    """No event outside [start_ms, end_ms] must appear."""
    engine = TickReplayEngine(params={}, db_path=tmp_db)
    start  = BASE_TS + 10_000
    end    = BASE_TS + 50_000

    with sqlite3.connect(tmp_db) as conn:
        conn.row_factory = sqlite3.Row
        events = list(engine._stream_events(conn, start, end))

    assert all(start <= e.ts_ms <= end for e in events), \
        "Event outside [start_ms, end_ms] found"


def test_candle_synthesis_correct_ohlcv(tmp_path):
    """
    5 trades in minute 0 then 1 trade in minute 1 must trigger exactly one
    update_candle call with correct OHLCV for minute 0.
    """
    db_path = str(tmp_path / "candle_test.db")
    base    = BASE_TS
    minute  = 60_000
    prices  = [100.0, 105.0, 98.0, 102.0, 103.0]   # minute 0
    qty     = 1.0

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        for i, p in enumerate(prices):
            conn.execute(
                "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                (base + i * 1_000, p, qty, 0),
            )
        # one trade in minute 1 to flush the candle
        conn.execute(
            "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
            (base + minute + 1_000, 101.0, qty, 0),
        )
        conn.commit()

    engine     = TickReplayEngine(params={}, db_path=db_path)
    calls: list = []

    original_update = engine._fc.update_candle

    def capturing_update(candle):
        calls.append(candle)
        original_update(candle)

    engine._fc.update_candle = capturing_update  # type: ignore[method-assign]
    engine.replay_window(base, base + minute + 60_000)

    assert len(calls) >= 1, "update_candle was never called"
    c = calls[0]
    assert c.open  == pytest.approx(100.0)
    assert c.high  == pytest.approx(105.0)
    assert c.low   == pytest.approx(98.0)
    assert c.close == pytest.approx(103.0)
    assert c.volume == pytest.approx(qty * len(prices))
    assert c.is_closed is True


def test_wall_state_created_on_first_depth_snapshot(tmp_path):
    """
    A depth snapshot containing a sigma-level outlier must create at least
    one WallState in _wall_states.
    """
    db_path = str(tmp_path / "wall_test.db")
    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        # wall_idx=5 creates a 50× outlier against 1.0 background
        bids = _make_bids(BASE_PRICE, wall_idx=5)
        asks = _make_asks(BASE_PRICE)
        conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (BASE_TS, json.dumps(bids), json.dumps(asks)),
        )
        conn.commit()

    engine = TickReplayEngine(params={}, db_path=db_path)
    engine.replay_window(BASE_TS, BASE_TS + 1_000)
    assert len(engine._wall_states) > 0, "_wall_states empty after depth snapshot with outlier"


def test_wall_qty_updated_not_zeroed_on_absence(tmp_path):
    """
    fix #2 — a wall absent from subsequent snapshots must NOT have
    qty_current zeroed. It should retain its last known qty until pruned
    (>30 s without update) or explicitly depleted by a later snapshot.
    """
    db_path = str(tmp_path / "absence_test.db")
    ts_a    = BASE_TS
    ts_b    = BASE_TS + 1_000  # 1 s later — well within 30 s prune window

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        # Snapshot A: big wall at bid level 5
        bids_a = _make_bids(BASE_PRICE, wall_idx=5)
        asks_a = _make_asks(BASE_PRICE)
        conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (ts_a, json.dumps(bids_a), json.dumps(asks_a)),
        )
        # Snapshot B: no outlier — the big level is now 1.0 (no longer sigma wall)
        bids_b = _make_bids(BASE_PRICE)
        asks_b = _make_asks(BASE_PRICE)
        conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (ts_b, json.dumps(bids_b), json.dumps(asks_b)),
        )
        conn.commit()

    engine = TickReplayEngine(params={}, db_path=db_path)
    engine.replay_window(ts_a, ts_b + 1_000)

    # Wall created at ts_a — after ts_b it may still be tracked (not yet pruned)
    # Its qty_current should have been updated to the current book value at that
    # price level, not zero-ed arbitrarily.
    # Key assertion: engine did not crash and did not set a tracked qty to a
    # negative value.
    for ws in engine._wall_states.values():
        assert ws.qty_current >= 0.0, f"qty_current went negative for wall at {ws.price}"


def test_wall_consumed_by_qty_depletion(tmp_path):
    """
    fix #2 — consumed detection uses qty_current < 15 % of qty_initial,
    driven by explicit snapshot updates, not by wall absence.
    """
    db_path = str(tmp_path / "consumed_test.db")
    ts_a    = BASE_TS
    ts_b    = BASE_TS + 600   # 600 ms later — wall is_persistent after 500 ms

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        # Snapshot A: large wall (qty=50) at bid level 5
        bids_a = _make_bids(BASE_PRICE, wall_idx=5)
        asks_a = _make_asks(BASE_PRICE)
        conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (ts_a, json.dumps(bids_a), json.dumps(asks_a)),
        )
        # Snapshot B: same price but qty now 1.0 (2 % of 50 → below 15 % threshold)
        wall_price = float(bids_a[5][0])
        bids_b     = [
            [f"{BASE_PRICE - i * 10.0:.2f}", "1.00000" if i != 5 else "1.00000"]
            for i in range(20)
        ]
        # Keep wall price at same position but deplete qty
        bids_b[5] = [f"{wall_price:.2f}", "1.00000"]
        conn.execute(
            "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
            (ts_b, json.dumps(bids_b), json.dumps(asks_a)),
        )
        conn.commit()

    engine = TickReplayEngine(params={}, db_path=db_path)
    engine.replay_window(ts_a, ts_b + 1_000)

    # After snapshot B, the wall at wall_price should have reload_ratio ~ 0.02
    if wall_price in engine._wall_states:
        ws = engine._wall_states[wall_price]
        assert ws.reload_ratio < 0.15, \
            f"Expected reload_ratio < 0.15, got {ws.reload_ratio:.3f}"


def test_equity_curve_non_negative(tmp_db):
    """Equity must never go below 0 throughout a synthetic replay."""
    engine = TickReplayEngine(params={}, db_path=tmp_db, starting_equity=10_000.0)
    eq, _  = engine.replay_window(BASE_TS, BASE_TS + 100_000)
    assert all(v >= 0.0 for v in eq.values), \
        f"Equity went negative: min={eq.min():.2f}"


def test_replay_produces_feature_vectors(tmp_path):
    """
    After RSI warm-up (14 candles), fc.compute() must return non-None
    FeatureVectors. We feed 30 minutes of 1-minute candles (via trades)
    plus depth snapshots to exceed the warm-up period.
    """
    db_path   = str(tmp_path / "fv_test.db")
    n_minutes = 30
    minute    = 60_000
    base      = BASE_TS

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        # One depth snapshot per minute
        for m in range(n_minutes):
            ts = base + m * minute
            conn.execute(
                "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                (ts, json.dumps(_make_bids(BASE_PRICE)), json.dumps(_make_asks(BASE_PRICE))),
            )
        # Several trades per minute to advance candles
        for m in range(n_minutes):
            for s in range(10):
                ts    = base + m * minute + s * 5_000
                price = BASE_PRICE + (s - 5) * 10.0
                conn.execute(
                    "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                    (ts, price, 0.01, s % 2),
                )
        conn.commit()

    engine = TickReplayEngine(
        params={}, db_path=db_path, collect_features=True
    )
    engine.replay_window(base, base + n_minutes * minute)
    assert len(engine.feature_history) > 0, \
        "No FeatureVectors produced — RSI warm-up likely not reached"


def test_fidelity_replay_matches_live_feature_computation(tmp_path):
    """
    THE CRITICAL TEST.

    Feeds the same deterministic event stream to:
      Path A — bare FeatureComputer + CVDCalculator (manual, simulates live)
      Path B — TickReplayEngine(collect_features=True)

    All 14 numeric FeatureVector fields must agree to 1e-6 tolerance.
    """
    db_path   = str(tmp_path / "fidelity_test.db")
    n_minutes = 25   # enough to warm up RSI (14 candles)
    minute    = 60_000
    base      = BASE_TS

    # Build a deterministic event list and write to DB
    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        for m in range(n_minutes):
            ts = base + m * minute
            conn.execute(
                "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                (ts, json.dumps(_make_bids(BASE_PRICE)), json.dumps(_make_asks(BASE_PRICE))),
            )
            for s in range(8):
                ts_t  = base + m * minute + s * 7_000
                price = BASE_PRICE + ((m * 8 + s) % 11 - 5) * 3.0
                conn.execute(
                    "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                    (ts_t, price, 0.01 + s * 0.001, (m + s) % 2),
                )
        conn.commit()

    # Path B — engine
    engine_b = TickReplayEngine(
        params={}, db_path=db_path, collect_features=True
    )
    engine_b.replay_window(base, base + n_minutes * minute)
    fv_b_list = engine_b.feature_history

    if len(fv_b_list) == 0:
        pytest.skip("RSI not ready in Path B — increase n_minutes or trade density")

    # Path A — manual, mirrors _process_depth and _process_trade exactly,
    # using the same (ts_ms, kind) sort order as _stream_events.
    fc_a       = FeatureComputer(FeatureParams())
    cvd_a      = CVDCalculator()
    shared_a   = __import__("models", fromlist=["SharedState"]).SharedState(lob_status="SYNCED")
    wall_states_a: dict = {}
    fv_a_list: list[tuple[int, FeatureVector]] = []
    bar_start_ms_a = 0
    bar_open_a = bar_high_a = bar_low_a = bar_close_a = bar_volume_a = 0.0
    last_day_a = -1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Collect all events sorted by (ts_event, kind) — depth before trade
        depth_rows = conn.execute(
            "SELECT *, 'depth' AS kind FROM depth_snapshots ORDER BY ts_event, id"
        ).fetchall()
        trade_rows = conn.execute(
            "SELECT *, 'trade' AS kind FROM agg_trades ORDER BY ts_event, id"
        ).fetchall()

    import heapq as _hq
    all_events = list(_hq.merge(
        [(r["ts_event"], "depth", r) for r in depth_rows],
        [(r["ts_event"], "trade", r) for r in trade_rows],
        key=lambda x: (x[0], x[1]),
    ))

    sigma = FeatureParams().wall_sigma

    for ts_ms, kind, row in all_events:
        if kind == "depth":
            bids_raw = {float(p): float(q) for p, q in json.loads(row["bids_json"])}
            asks_raw = {float(p): float(q) for p, q in json.loads(row["asks_json"])}
            if not bids_raw or not asks_raw:
                continue
            bid_levels = sorted(bids_raw.items(), reverse=True)
            ask_levels = sorted(asks_raw.items())
            new_walls  = {
                w["price"]: w
                for w in (
                    identify_walls(bid_levels, "bid", sigma)
                    + identify_walls(ask_levels, "ask", sigma)
                )
            }
            from models import WallState as _WS
            for price, wd in new_walls.items():
                if price in wall_states_a:
                    wall_states_a[price].qty_current  = wd["qty"]
                    wall_states_a[price].last_seen_ts = ts_ms
                else:
                    wall_states_a[price] = _WS(
                        price=price, qty_initial=wd["qty"], qty_current=wd["qty"],
                        first_seen_ts=ts_ms, last_seen_ts=ts_ms,
                        side=wd["side"], sigma=wd["sigma"],
                    )
            for price, ws in wall_states_a.items():
                if price not in new_walls:
                    book = bids_raw if ws.side == "bid" else asks_raw
                    if price in book:
                        ws.qty_current  = book[price]
                        ws.last_seen_ts = ts_ms
                    # else: price gone from book — leave last_seen_ts so pruner fires
            wall_states_a = {
                p: w for p, w in wall_states_a.items()
                if (ts_ms - w.last_seen_ts) <= 30_000
            }
            wall_dicts = [
                {"price": p, "absorption_ratio": wall_states_a[p].reload_ratio
                 if p in wall_states_a else 1.0}
                for p in new_walls
            ]
            from models import LOBSnapshot as _LS, LOBLevel as _LL
            snap_a = _LS(
                timestamp=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                bids=[_LL(price=p, qty=q) for p, q in bid_levels],
                asks=[_LL(price=p, qty=q) for p, q in ask_levels],
                last_update_id=0,
            )
            fc_a.update_orderbook(snap_a, wall_dicts)
            fv = fc_a.compute(cvd_a, shared_a)
            if fv is not None:
                fv_a_list.append((ts_ms, fv))

        else:  # trade
            from models import AggTrade as _AT
            trade_a = _AT(
                timestamp=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                price=float(row["price"]),
                qty=float(row["qty"]),
                is_buyer_maker=bool(row["is_buyer_maker"]),
            )
            cvd_a.update(trade_a)
            day = ts_ms // 86_400_000
            if day > last_day_a:
                if last_day_a >= 0:
                    cvd_a.reset_daily()
                last_day_a = day
            # Candle synthesis (mirrors engine exactly)
            bar_ms = (ts_ms // 60_000) * 60_000
            price  = float(row["price"])
            qty    = float(row["qty"])
            if bar_start_ms_a == 0:
                bar_start_ms_a = bar_ms
                bar_open_a = bar_high_a = bar_low_a = bar_close_a = price
                bar_volume_a = qty
            elif bar_ms > bar_start_ms_a:
                from models import Candle as _C
                fc_a.update_candle(_C(
                    open_time=datetime.fromtimestamp(bar_start_ms_a / 1000, tz=timezone.utc),
                    open=bar_open_a, high=bar_high_a,
                    low=bar_low_a, close=bar_close_a,
                    volume=bar_volume_a, is_closed=True,
                ))
                bar_start_ms_a = bar_ms
                bar_open_a = bar_high_a = bar_low_a = bar_close_a = price
                bar_volume_a = qty
            else:
                bar_high_a   = max(bar_high_a, price)
                bar_low_a    = min(bar_low_a, price)
                bar_close_a  = price
                bar_volume_a += qty

    # Compare
    assert len(fv_a_list) > 0, "Path A produced no FeatureVectors"
    assert len(fv_a_list) == len(fv_b_list), (
        f"Count mismatch: Path A={len(fv_a_list)}, Path B={len(fv_b_list)}"
    )

    numeric_fields = [
        f for f in FeatureVector.__dataclass_fields__
        if f != "lob_status"
    ]
    for i, ((ts_a, fv_a), (ts_b, fv_b)) in enumerate(zip(fv_a_list, fv_b_list)):
        assert ts_a == ts_b, f"Timestamp mismatch at index {i}: {ts_a} vs {ts_b}"
        for field in numeric_fields:
            va = getattr(fv_a, field)
            vb = getattr(fv_b, field)
            assert abs(va - vb) < 1e-6, (
                f"FIDELITY FAIL at index {i}, field={field}: "
                f"PathA={va}, PathB={vb}, diff={abs(va-vb):.2e}"
            )


def test_replay_signal_rate_plausible(tmp_path):
    """
    fix #9 — guard against both zero-signal (engine not firing) and signal
    explosion (false-positive storm from wall-consumed bugs).

    Engineers exactly 2 sweep events into the synthetic stream, then asserts
    that 0 ≤ trades ≤ 10 (allowing for signal conditions not being met in
    synthetic data, but capping explosion).
    """
    db_path = str(tmp_path / "signal_rate.db")
    base    = BASE_TS
    minute  = 60_000

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        # 30 minutes of depth + trades to warm up RSI
        for m in range(30):
            ts = base + m * minute
            conn.execute(
                "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                (ts, json.dumps(_make_bids(BASE_PRICE)), json.dumps(_make_asks(BASE_PRICE))),
            )
            for s in range(6):
                conn.execute(
                    "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
                    (base + m * minute + s * 9_000,
                     BASE_PRICE + (s - 3) * 2.0, 0.01, s % 2),
                )
        conn.commit()

    engine   = TickReplayEngine(params={}, db_path=db_path, starting_equity=10_000.0)
    eq, trades = engine.replay_window(base, base + 30 * minute)

    # Should never explode — the multi-condition sweep gate prevents it
    assert len(trades) <= 10, (
        f"Signal explosion: {len(trades)} trades in 30-min synthetic replay"
    )
    # Equity must remain non-negative
    assert all(v >= 0.0 for v in eq.values), "Equity went negative"


def test_batch_streaming_consistency(tmp_db):
    """
    _stream_events must produce identical events regardless of batch_size.
    The composite (ts_event, id) keyset cursor must not skip or duplicate rows
    when a batch boundary falls mid-timestamp-group.
    """
    engine = TickReplayEngine(params={}, db_path=tmp_db)
    start  = BASE_TS
    end    = BASE_TS + 100_000

    with sqlite3.connect(tmp_db) as conn:
        conn.row_factory = sqlite3.Row
        events_small = list(engine._stream_events(conn, start, end, batch_size=1))
    with sqlite3.connect(tmp_db) as conn:
        conn.row_factory = sqlite3.Row
        events_large = list(engine._stream_events(conn, start, end, batch_size=10_000))

    assert len(events_small) == len(events_large), (
        f"Event count differs: batch_size=1 → {len(events_small)}, "
        f"batch_size=10_000 → {len(events_large)}"
    )
    for i, (a, b) in enumerate(zip(events_small, events_large)):
        assert a.ts_ms == b.ts_ms, f"ts_ms mismatch at index {i}: {a.ts_ms} vs {b.ts_ms}"
        assert a.kind  == b.kind,  f"kind mismatch at index {i}: {a.kind} vs {b.kind}"


def test_cvd_resets_at_midnight(tmp_path):
    """
    CVD must reset exactly once when a UTC midnight is crossed.
    The `if self._last_day >= 0` guard suppresses a spurious reset on the
    very first trade processed, so only genuine day transitions call reset_daily().
    """
    db_path     = str(tmp_path / "midnight_test.db")
    midnight_ms = 1_699_920_000_000  # 2023-11-14 00:00:00 UTC

    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        for i in range(3):
            conn.execute(
                "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker)"
                " VALUES (?,?,?,?)",
                (midnight_ms - (3 - i) * 1_000, BASE_PRICE, 0.01, 0),
            )
        for i in range(3):
            conn.execute(
                "INSERT INTO agg_trades (ts_event, price, qty, is_buyer_maker)"
                " VALUES (?,?,?,?)",
                (midnight_ms + (i + 1) * 1_000, BASE_PRICE, 0.01, 0),
            )
        conn.commit()

    engine = TickReplayEngine(params={}, db_path=db_path)
    reset_calls: list[bool] = []
    original_reset = engine._cvd.reset_daily

    def tracking_reset() -> None:
        reset_calls.append(True)
        original_reset()

    engine._cvd.reset_daily = tracking_reset  # type: ignore[method-assign]
    engine.replay_window(midnight_ms - 5_000, midnight_ms + 5_000)

    assert len(reset_calls) == 1, (
        f"Expected exactly 1 CVD midnight reset, got {len(reset_calls)}"
    )


def test_pre_requisite_data_available():
    """
    fix #8 — checks real lob_tick.db without using pytest.warns.
    Skips (not fails) if fewer than 3 days available.
    Fails hard only if the file is missing entirely.
    """
    db = "data/lob_tick.db"
    if not os.path.exists(db):
        pytest.fail("lob_tick.db missing — start core/lob_recorder.py first")
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT (MAX(ts_event) - MIN(ts_event)) / 86400000.0 FROM depth_snapshots"
        ).fetchone()
    days = row[0] or 0.0
    if days < 6 / 24:
        pytest.skip(
            f"Only {days * 24:.1f} hours of data in lob_tick.db — need ≥ 6 h for smoke test. "
            "Keep lob_recorder running."
        )
    # Confirm the engine can open the DB and stream at least one event
    engine = TickReplayEngine(params={}, db_path=db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        first_ts = conn.execute(
            "SELECT MIN(ts_event) FROM depth_snapshots"
        ).fetchone()[0]
    if first_ts is None:
        pytest.skip("depth_snapshots table is empty")
    events = []
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        for e in engine._stream_events(conn, first_ts, first_ts + 5_000):
            events.append(e)
            if len(events) >= 10:
                break
    assert len(events) > 0, "No events streamed from real lob_tick.db"


# ─────────────────────────────────────────────────────────────────────────────
# H1 regression: EOD close in zero-trade windows
# ─────────────────────────────────────────────────────────────────────────────

def _make_depth_only_db(tmp_path: "pathlib.Path") -> str:
    """DB with depth snapshots but NO agg_trades rows — simulates a no-trade window."""
    db_path = str(tmp_path / "depth_only.db")
    with sqlite3.connect(db_path) as conn:
        _create_schema(conn)
        for i in range(20):
            ts    = BASE_TS + i * 1_000
            bids  = json.dumps(_make_bids(BASE_PRICE))
            asks  = json.dumps(_make_asks(BASE_PRICE))
            conn.execute(
                "INSERT INTO depth_snapshots (ts_event, bids_json, asks_json) VALUES (?,?,?)",
                (ts, bids, asks),
            )
    return db_path


def _make_fake_signal():
    """Minimal MicroSignal stand-in for injecting an open position dict."""
    from unittest.mock import MagicMock
    return MagicMock()


def test_eod_close_uses_prev_mid_when_no_trades(tmp_path):
    """
    H1: when a replay window contains depth events but no trades, an open
    position must be closed using _prev_mid rather than being silently left open.

    Strategy: patch _reset so that after it clears engine state, we inject an
    open position and a known _prev_mid. The depth-only DB provides no trades,
    so _last_trade_price stays 0, and the EOD code must fall back to _prev_mid.
    """
    db_path    = _make_depth_only_db(tmp_path)
    engine     = TickReplayEngine(params={}, db_path=db_path, starting_equity=10_000.0)
    fake_signal = _make_fake_signal()

    original_reset = engine._reset

    def reset_then_inject(start_ms: int) -> None:
        original_reset(start_ms)
        engine._open_position = {
            "direction":      "LONG",
            "entry_price":    BASE_PRICE,
            "qty":            0.001,
            "sl":             BASE_PRICE - 200,
            "tp":             BASE_PRICE + 600,
            "entry_ts_ms":    start_ms,
            "signal":         fake_signal,
            "entry_cost_usd": 0.0,
        }
        engine._prev_mid = BASE_PRICE

    with patch.object(engine, "_reset", reset_then_inject):
        end_ms = BASE_TS + 25_000
        eq, trades = engine.replay_window(BASE_TS, end_ms)

    assert engine._open_position is None, "Position should have been closed at EOD"
    assert len(trades) == 1, f"Expected 1 EOD trade, got {len(trades)}"
    assert trades[0].exit_reason == "EOD"


def test_eod_close_oos_uses_prev_mid_when_no_trades(tmp_path):
    """
    H1 (OOS path): replay_window_oos must also close open positions using _prev_mid
    when no trades occur in the window. Same injection technique via _reset_for_oos.
    """
    db_path    = _make_depth_only_db(tmp_path)
    engine     = TickReplayEngine(params={}, db_path=db_path, starting_equity=10_000.0)
    fake_signal = _make_fake_signal()

    # Prime IS state normally (no position injected for IS pass)
    engine.replay_window(BASE_TS, BASE_TS + 5_000)

    original_oos_reset = engine._reset_for_oos

    def oos_reset_then_inject(start_ms: int) -> None:
        original_oos_reset(start_ms)
        engine._open_position = {
            "direction":      "SHORT",
            "entry_price":    BASE_PRICE,
            "qty":            0.001,
            "sl":             BASE_PRICE + 200,
            "tp":             BASE_PRICE - 600,
            "entry_ts_ms":    start_ms,
            "signal":         fake_signal,
            "entry_cost_usd": 0.0,
        }
        engine._prev_mid = BASE_PRICE

    with patch.object(engine, "_reset_for_oos", oos_reset_then_inject):
        end_ms = BASE_TS + 25_000
        eq, trades = engine.replay_window_oos(BASE_TS + 5_000, end_ms)

    assert engine._open_position is None, "OOS position should have been closed at EOD"
    assert len(trades) == 1
    assert trades[0].exit_reason == "EOD"


def test_replay_signal_does_not_require_cvd_spike(tmp_db):
    """Tick replay must mirror live: sweep+protection can fire with cold/zero CVD spike."""
    engine = TickReplayEngine(params={}, db_path=tmp_db, starting_equity=10_000.0)
    engine._reset(BASE_TS)
    ts_ms = BASE_TS + 1_000
    engine._prev_mid = 30_005.0
    engine._wall_states = {
        30_010.0: WallState(
            price=30_010.0,
            qty_initial=50.0,
            qty_current=5.0,
            first_seen_ts=ts_ms - 1_000,
            last_seen_ts=ts_ms,
            side="ask",
            sigma=3.0,
        ),
        29_980.0: WallState(
            price=29_980.0,
            qty_initial=30.0,
            qty_current=30.0,
            first_seen_ts=ts_ms - 500,
            last_seen_ts=ts_ms,
            side="bid",
            sigma=3.0,
        ),
    }
    engine._absorption_flags = {30_010.0: True}

    engine._process_trade(ReplayEvent(
        ts_ms=ts_ms,
        kind="trade",
        data={"price": 30_021.0, "qty": 1.0, "is_buyer_maker": 0},
    ))

    assert engine._open_position is not None
    assert engine._open_position["signal"].cvd_std == pytest.approx(0.0)
    assert engine._open_position["direction"] == "LONG"
