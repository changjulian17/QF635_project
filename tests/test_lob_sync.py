"""Binance diff-depth seed bridge + contiguity predicates."""
import asyncio

import pytest

from core.lob_sync import (
    SeedDiscontinuity,
    seed_bridge_ok,
    is_contiguous,
    futures_seed_bridge_ok,
    futures_is_contiguous,
    seed_futures_book,
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


# ── seed_futures_book: wait + bridge + pu-chain orchestration ──────────────────
# Shared by ws_consumer + lob_recorder. The wait gate is strict `u > lastUpdateId`
# (matching the apply-loop skip `u <= lastUpdateId`), so a snapshot landing exactly on
# an event boundary no longer passes the gate then gets skipped → spurious failure.

def _run(coro):
    return asyncio.run(coro)


def test_seed_returns_bridge_chain_and_applies_only_forward_events():
    """Returns the last applied u; stale (u <= lastUpdateId) events are skipped, not applied."""
    applied = []
    pending = [
        {"U": 900,  "u": 950,  "pu": 880,  "b": [], "a": []},   # behind snapshot → skipped
        {"U": 951,  "u": 1050, "pu": 950,  "b": [], "a": []},   # bridge: 951 <= 1000 <= 1050
        {"U": 1051, "u": 1100, "pu": 1050, "b": [], "a": []},   # chained: pu == prev u
    ]
    result = _run(seed_futures_book(
        last_update_id=1000, pending=pending,
        apply_diff=applied.append, bridge_wait_s=1.0, poll_s=0.0,
    ))
    assert result == 1100
    assert [e["u"] for e in applied] == [1050, 1100]   # the stale 950 event never applied


def test_seed_boundary_event_does_not_fail_spuriously():
    """Regression: an event with u == lastUpdateId must NOT trigger 'no bridge event'.

    With the old `>=` gate that event released the wait then got skipped (`u <= last`),
    leaving no bridge → spurious failure. The strict `>` gate waits for the next event,
    which still straddles lastUpdateId and bridges cleanly.
    """
    # Only a boundary event present → wait can't release (no u > 1000) → clean timeout,
    # NOT a 'no bridge event' error.
    boundary_only = [{"U": 951, "u": 1000, "pu": 950, "b": [], "a": []}]
    with pytest.raises(SeedDiscontinuity, match="no diff reached"):
        _run(seed_futures_book(
            last_update_id=1000, pending=boundary_only,
            apply_diff=lambda d: None, bridge_wait_s=0.0, poll_s=0.0,
        ))

    # Boundary event followed by a real forward event → bridges on the forward event,
    # which must itself straddle lastUpdateId (U <= 1000 <= u).
    applied = []
    pending = [
        {"U": 951, "u": 1000, "pu": 950, "b": [], "a": []},   # boundary, skipped (u <= 1000)
        {"U": 990, "u": 1080, "pu": 950, "b": [], "a": []},   # bridge: 990 <= 1000 <= 1080
    ]
    result = _run(seed_futures_book(
        last_update_id=1000, pending=pending,
        apply_diff=applied.append, bridge_wait_s=1.0, poll_s=0.0,
    ))
    assert result == 1080
    assert [e["u"] for e in applied] == [1080]


def test_seed_raises_on_pu_gap():
    pending = [
        {"U": 951,  "u": 1050, "pu": 950,  "b": [], "a": []},   # bridge ok
        {"U": 1060, "u": 1100, "pu": 1055, "b": [], "a": []},   # pu 1055 != prev u 1050 → gap
    ]
    with pytest.raises(SeedDiscontinuity, match="gap"):
        _run(seed_futures_book(
            last_update_id=1000, pending=pending,
            apply_diff=lambda d: None, bridge_wait_s=1.0, poll_s=0.0,
        ))


def test_seed_raises_on_bridge_fail():
    """First forward event starts after lastUpdateId (U > last) → missed events → bridge fail."""
    pending = [{"U": 1005, "u": 1050, "pu": 1004, "b": [], "a": []}]   # U=1005 > 1000
    with pytest.raises(SeedDiscontinuity, match="bridge fail"):
        _run(seed_futures_book(
            last_update_id=1000, pending=pending,
            apply_diff=lambda d: None, bridge_wait_s=1.0, poll_s=0.0,
        ))


def test_seed_times_out_when_no_diff_reaches_snapshot():
    pending = [
        {"U": 900, "u": 950, "pu": 880, "b": [], "a": []},
        {"U": 951, "u": 990, "pu": 950, "b": [], "a": []},
    ]
    with pytest.raises(SeedDiscontinuity, match="no diff reached"):
        _run(seed_futures_book(
            last_update_id=1000, pending=pending,
            apply_diff=lambda d: None, bridge_wait_s=0.0, poll_s=0.0,
        ))
