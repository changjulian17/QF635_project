"""Tests for dashboard._logic.update_lob_buffer (LOB streaming buffer logic)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard._logic import add_event_markers, update_event_buffer, update_lob_buffer


def _msg(ts, cvd_delta=0.0):
    return {"ts": ts, "cvd_delta": cvd_delta, "obi": 0.1,
            "bid_levels": [], "ask_levels": []}


def test_append_grows_buffer():
    buf = update_lob_buffer([], _msg("t1"), max_points=10)
    buf = update_lob_buffer(buf, _msg("t2"), max_points=10)
    assert [m["ts"] for m in buf] == ["t1", "t2"]


def test_trims_to_max_points_keeping_newest():
    buf = []
    for i in range(5):
        buf = update_lob_buffer(buf, _msg(f"t{i}"), max_points=3)
    assert [m["ts"] for m in buf] == ["t2", "t3", "t4"]


def test_duplicate_ts_is_ignored():
    buf = update_lob_buffer([], _msg("t1"), max_points=10)
    buf = update_lob_buffer(buf, _msg("t1"), max_points=10)  # same ts again
    assert len(buf) == 1


def test_does_not_mutate_input_buffer():
    original = [_msg("t1")]
    result = update_lob_buffer(original, _msg("t2"), max_points=10)
    assert len(original) == 1   # input untouched
    assert len(result) == 2


def test_non_dict_or_missing_ts_ignored():
    buf = [_msg("t1")]
    assert update_lob_buffer(buf, "not a dict", max_points=10) is buf
    assert update_lob_buffer(buf, {"no_ts": 1}, max_points=10) is buf


def test_build_lob_figure_accepts_mixed_timestamp_formats():
    import dash

    dash.Dash(__name__, use_pages=True, pages_folder="")
    from dashboard.pages.lob import _build_lob_figure

    snaps = [
        {
            "ts": "2026-06-01T10:00:00+00:00",
            "mid_price": 30000.0,
            "spread": 10.0,
            "obi": 0.1,
            "cvd_delta": 1.0,
            "bid_levels": [],
            "ask_levels": [],
        },
        {
            "ts": "not-a-timestamp",
            "mid_price": 30010.0,
            "spread": 12.0,
            "obi": 0.2,
            "cvd_delta": -0.5,
            "bid_levels": [],
            "ask_levels": [],
        },
        {
            "ts": "2026-06-01T10:00:02+00:00",
            "mid_price": 30005.0,
            "spread": 11.0,
            "obi": 0.15,
            "cvd_delta": 0.25,
            "bid_levels": [],
            "ask_levels": [],
        },
    ]

    fig = _build_lob_figure(snaps, hm_minutes=1, half_range=100, contrast_pctile=95, trade_pctile=95)
    assert len(fig.data) > 0


def test_running_cvd_is_cumulative_sum_over_buffer():
    """Running CVD (computed at render time) is the cumulative sum of cvd_delta."""
    buf = []
    for ts, delta in [("t1", 1.0), ("t2", -0.5), ("t3", 2.0)]:
        buf = update_lob_buffer(buf, _msg(ts, cvd_delta=delta), max_points=10)
    running = []
    total = 0.0
    for m in buf:
        total += m["cvd_delta"]
        running.append(total)
    assert running == [1.0, 0.5, 2.5]


def test_lob_buffer_ignores_event_messages():
    """Snapshot buffer must reject messages tagged as events so it stays homogeneous."""
    buf = update_lob_buffer([], _msg("t1"), max_points=10)
    event = {"type": "event", "event": "sweep", "ts": "t2", "price": 30000.0, "side": "bid"}
    result = update_lob_buffer(buf, event, max_points=10)
    assert result is buf  # unchanged


# ── update_event_buffer ───────────────────────────────────────────────────


def _event(ts, kind="absorption", side="bid", price=30000.0):
    return {"type": "event", "event": kind, "ts": ts, "price": price, "side": side}


def test_event_buffer_appends_events():
    buf = update_event_buffer([], _event("t1"), max_events=10)
    buf = update_event_buffer(buf, _event("t2", kind="sweep"), max_events=10)
    assert [(e["ts"], e["event"]) for e in buf] == [("t1", "absorption"), ("t2", "sweep")]


def test_event_buffer_trims_to_max_keeping_newest():
    buf = []
    for i in range(5):
        buf = update_event_buffer(buf, _event(f"t{i}"), max_events=3)
    assert [e["ts"] for e in buf] == ["t2", "t3", "t4"]


def test_event_buffer_rejects_snapshots():
    buf = [_event("t1")]
    snap = _msg("t2")  # no type field — treated as snapshot by default
    assert update_event_buffer(buf, snap, max_events=10) is buf


def test_event_buffer_rejects_malformed_payloads():
    buf = [_event("t1")]
    assert update_event_buffer(buf, "not a dict", max_events=10) is buf
    assert update_event_buffer(buf, {"type": "event", "ts": "t2"}, max_events=10) is buf  # no event
    assert update_event_buffer(buf, {"type": "event", "event": "sweep"}, max_events=10) is buf  # no ts


# ── add_event_markers ─────────────────────────────────────────────────────


def _figure_with_event_traces(events, ts_labels, hm_ts_snap):
    fig = make_subplots(rows=4, cols=1)
    n_abs, n_swp = add_event_markers(fig, events, ts_labels, hm_ts_snap)
    abs_trace = next(t for t in fig.data if t.name == "Absorption")
    swp_trace = next(t for t in fig.data if t.name == "Sweep")
    return abs_trace, swp_trace, n_abs, n_swp


def test_add_markers_drops_events_outside_window():
    """Events whose ts falls outside [hm_ts_snap[0], hm_ts_snap[-1]] are not rendered."""
    ts_labels  = ["10:00:00", "10:00:01"]
    hm_ts_snap = pd.Series([pd.Timestamp("2026-06-01T10:00:00+00:00"),
                            pd.Timestamp("2026-06-01T10:00:01+00:00")])
    before  = {"type": "event", "event": "absorption", "ts": "2026-06-01T09:59:00+00:00",
               "price": 100.0, "side": "bid", "reload_ratio": 0.9}
    inside  = {"type": "event", "event": "sweep", "ts": "2026-06-01T10:00:00+00:00",
               "price": 101.0, "side": "ask", "direction": "SHORT", "price_move_pct": -0.001}
    after   = {"type": "event", "event": "sweep", "ts": "2026-06-01T11:00:00+00:00",
               "price": 102.0, "side": "ask"}
    _, _, n_abs, n_swp = _figure_with_event_traces([before, inside, after], ts_labels, hm_ts_snap)
    assert (n_abs, n_swp) == (0, 1)


def test_add_markers_routes_absorption_and_sweep_to_separate_traces():
    """Each event type lands in its own trace with distinct symbols."""
    ts_labels  = ["10:00:00"]
    hm_ts_snap = pd.Series([pd.Timestamp("2026-06-01T10:00:00+00:00")])
    events = [
        {"type": "event", "event": "absorption", "ts": "2026-06-01T10:00:00+00:00",
         "price": 30000.0, "side": "bid", "reload_ratio": 0.85},
        {"type": "event", "event": "absorption", "ts": "2026-06-01T10:00:00+00:00",
         "price": 30100.0, "side": "ask", "reload_ratio": 0.75},
        {"type": "event", "event": "sweep", "ts": "2026-06-01T10:00:00+00:00",
         "price": 30050.0, "side": "bid", "direction": "SHORT", "price_move_pct": -0.002},
    ]
    abs_trace, swp_trace, n_abs, n_swp = _figure_with_event_traces(events, ts_labels, hm_ts_snap)
    assert (n_abs, n_swp) == (2, 1)
    # Absorption symbols depend on side: bid → triangle-up, ask → triangle-down
    assert list(abs_trace.marker.symbol) == ["triangle-up", "triangle-down"]
    # Sweep always uses star
    assert list(swp_trace.marker.symbol) == ["star"]


def test_add_markers_empty_events_still_adds_traces():
    """Even with no events, both traces must exist so the legend doesn't shift."""
    ts_labels  = ["10:00:00"]
    hm_ts_snap = pd.Series([pd.Timestamp("2026-06-01T10:00:00+00:00")])
    abs_trace, swp_trace, n_abs, n_swp = _figure_with_event_traces([], ts_labels, hm_ts_snap)
    assert (n_abs, n_swp) == (0, 0)
    assert abs_trace.name == "Absorption"
    assert swp_trace.name == "Sweep"


def test_add_markers_color_encodes_wall_side():
    """Bid-side events are green, ask-side events are red — color encodes the wall side."""
    ts_labels  = ["10:00:00"]
    hm_ts_snap = pd.Series([pd.Timestamp("2026-06-01T10:00:00+00:00")])
    events = [
        {"type": "event", "event": "sweep", "ts": "2026-06-01T10:00:00+00:00",
         "price": 30000.0, "side": "bid", "direction": "SHORT", "price_move_pct": -0.001},
        {"type": "event", "event": "sweep", "ts": "2026-06-01T10:00:00+00:00",
         "price": 30100.0, "side": "ask", "direction": "LONG", "price_move_pct": 0.001},
    ]
    _, swp_trace, *_ = _figure_with_event_traces(events, ts_labels, hm_ts_snap)
    colors = list(swp_trace.marker.color)
    assert "0,220,100" in colors[0]   # green for bid
    assert "220,60,60" in colors[1]   # red for ask
