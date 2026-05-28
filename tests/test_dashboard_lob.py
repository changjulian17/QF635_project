"""Tests for dashboard._logic.update_lob_buffer (LOB streaming buffer logic)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard._logic import update_lob_buffer


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
