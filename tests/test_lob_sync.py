"""Binance diff-depth seed bridge + contiguity predicates."""
from core.lob_sync import seed_bridge_ok, is_contiguous


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
