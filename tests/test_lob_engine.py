"""Unit tests for LocalOrderBook — pure synchronous LOB logic."""
from engine.lob_engine import LocalOrderBook


def snapshot_data(last_id: int = 100, bids=None, asks=None) -> dict:
    return {
        "lastUpdateId": last_id,
        "bids": bids or [["30000.00", "1.5"], ["29999.00", "0.8"]],
        "asks": asks or [["30001.00", "1.2"], ["30002.00", "0.5"]],
    }


def diff_event(U: int, u: int, bids=None, asks=None) -> dict:
    return {"U": U, "u": u, "b": bids or [], "a": asks or []}


# ── Initialisation ────────────────────────────────────────────────────────────

def test_not_ready_before_snapshot():
    lob = LocalOrderBook()
    assert not lob.is_ready
    assert lob.get_snapshot() is None


def test_ready_after_snapshot():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    assert lob.is_ready


def test_snapshot_populates_bids_and_asks():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = lob.get_snapshot()
    assert snap is not None
    assert len(snap.bids) == 2
    assert len(snap.asks) == 2


def test_bids_sorted_descending():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = lob.get_snapshot()
    prices = [l.price for l in snap.bids]
    assert prices == sorted(prices, reverse=True)


def test_asks_sorted_ascending():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = lob.get_snapshot()
    prices = [l.price for l in snap.asks]
    assert prices == sorted(prices)


def test_get_snapshot_respects_depth():
    bids = [[str(30000 - i), "1.0"] for i in range(10)]
    asks = [[str(30001 + i), "1.0"] for i in range(10)]
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(bids=bids, asks=asks))
    snap = lob.get_snapshot(depth=3)
    assert len(snap.bids) == 3
    assert len(snap.asks) == 3


# ── apply_diff ────────────────────────────────────────────────────────────────

def test_apply_diff_adds_new_bid_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["29998.00", "2.0"]]))
    snap = lob.get_snapshot()
    prices = [l.price for l in snap.bids]
    assert 29998.0 in prices


def test_apply_diff_updates_existing_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["30000.00", "9.9"]]))
    snap = lob.get_snapshot()
    top_bid = next(l for l in snap.bids if l.price == 30000.0)
    assert abs(top_bid.qty - 9.9) < 1e-9


def test_apply_diff_removes_zero_qty_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["30000.00", "0.0"]]))
    snap = lob.get_snapshot()
    prices = [l.price for l in snap.bids]
    assert 30000.0 not in prices


def test_apply_diff_skips_stale_events():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    result = lob.apply_diff(diff_event(U=50, u=99))  # u <= lastUpdateId → skip
    assert result is True
    assert lob.is_ready


def test_apply_diff_detects_sequence_gap():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101))   # apply first event (sets last_update_id=101)
    result = lob.apply_diff(diff_event(U=105, u=106))  # gap: expected U<=102, got 105
    assert result is False
    assert not lob.is_ready


def test_apply_diff_contiguous_sequence_succeeds():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    assert lob.apply_diff(diff_event(U=101, u=102)) is True
    assert lob.apply_diff(diff_event(U=103, u=104)) is True
    assert lob.is_ready


# ── reset ─────────────────────────────────────────────────────────────────────

def test_reset_clears_state():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    lob.reset()
    assert not lob.is_ready
    assert lob.get_snapshot() is None
