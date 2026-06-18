"""Binance diff-depth seed bridge + contiguity predicates."""
from core.lob_sync import (
    seed_bridge_ok,
    is_contiguous,
    futures_seed_bridge_ok,
    futures_is_contiguous,
)


def test_bridge_ok_when_first_diff_brackets_snapshot():
    # U <= lastUpdateId+1 <= u
    assert seed_bridge_ok(first_U=100, first_u=110, last_update_id=104) is True
    assert seed_bridge_ok(first_U=105, first_u=105, last_update_id=104) is True   # exact bridge


def test_bridge_fails_when_first_diff_starts_too_late():
    # first buffered diff begins after lastUpdateId+1 → missed events → gap
    assert seed_bridge_ok(first_U=107, first_u=110, last_update_id=104) is False


def test_bridge_fails_when_first_diff_ends_before_bridge():
    # diff entirely before the snapshot's next update → stale, not a bridge
    assert seed_bridge_ok(first_U=100, first_u=103, last_update_id=104) is False


def test_contiguous_true_when_no_gap():
    assert is_contiguous(prev_u=110, next_U=111) is True


def test_contiguous_false_on_gap():
    assert is_contiguous(prev_u=110, next_U=113) is False   # 111,112 missing
    assert is_contiguous(prev_u=110, next_U=110) is False   # duplicate/overlap


# ── USD-M Futures variants ────────────────────────────────────────────────────
# Futures diff-depth management differs from Spot:
#   bridge:      U <= lastUpdateId <= u        (no +1, unlike spot)
#   contiguity:  event.pu == previous event.u  (uses the 'pu' field, not U)

def test_futures_bridge_ok_when_first_diff_brackets_snapshot():
    # U <= lastUpdateId <= u
    assert futures_seed_bridge_ok(first_U=100, first_u=110, last_update_id=104) is True
    assert futures_seed_bridge_ok(first_U=104, first_u=104, last_update_id=104) is True   # exact
    assert futures_seed_bridge_ok(first_U=100, first_u=104, last_update_id=104) is True   # ends on snapshot


def test_futures_bridge_fails_when_first_diff_starts_after_snapshot():
    # spot would accept lastUpdateId+1; futures must straddle lastUpdateId itself
    assert futures_seed_bridge_ok(first_U=105, first_u=110, last_update_id=104) is False


def test_futures_bridge_fails_when_first_diff_ends_before_snapshot():
    assert futures_seed_bridge_ok(first_U=100, first_u=103, last_update_id=104) is False


def test_futures_contiguous_true_when_pu_matches_prev_u():
    assert futures_is_contiguous(prev_u=110, next_pu=110) is True


def test_futures_contiguous_false_when_pu_skips():
    assert futures_is_contiguous(prev_u=110, next_pu=111) is False   # gap: event began past prev_u
    assert futures_is_contiguous(prev_u=110, next_pu=108) is False   # overlap/rewind
