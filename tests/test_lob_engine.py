"""Unit tests for LocalOrderBook — pure synchronous LOB logic."""
from core.lob_engine import LocalOrderBook
from models import LOBStateMachineState, SharedState


def snapshot_data(last_id: int = 100, bids=None, asks=None) -> dict:
    return {
        "lastUpdateId": last_id,
        "bids": bids or [["30000.00", "1.5"], ["29999.00", "0.8"]],
        "asks": asks or [["30001.00", "1.2"], ["30002.00", "0.5"]],
    }


def diff_event(U: int, u: int, bids=None, asks=None) -> dict:
    return {"U": U, "u": u, "b": bids or [], "a": asks or []}


# ── Initialisation ────────────────────────────────────────────────────────────

async def test_not_ready_before_snapshot():
    lob = LocalOrderBook()
    assert not lob.is_ready
    assert await lob.get_snapshot() is None


def test_ready_after_snapshot():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    assert lob.is_ready


def test_best_bid_ask_not_ready():
    lob = LocalOrderBook()
    assert lob.best_bid_ask() is None


def test_best_bid_ask_returns_top_of_book():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())   # bids=[30000, 29999], asks=[30001, 30002]
    assert lob.best_bid_ask() == (30000.0, 30001.0)


async def test_snapshot_populates_bids_and_asks():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = await lob.get_snapshot()
    assert snap is not None
    assert len(snap.bids) == 2
    assert len(snap.asks) == 2


async def test_bids_sorted_descending():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = await lob.get_snapshot()
    prices = [l.price for l in snap.bids]
    assert prices == sorted(prices, reverse=True)


async def test_asks_sorted_ascending():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    snap = await lob.get_snapshot()
    prices = [l.price for l in snap.asks]
    assert prices == sorted(prices)


async def test_get_snapshot_respects_depth():
    bids = [[str(30000 - i), "1.0"] for i in range(10)]
    asks = [[str(30001 + i), "1.0"] for i in range(10)]
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(bids=bids, asks=asks))
    snap = await lob.get_snapshot(depth=3)
    assert len(snap.bids) == 3
    assert len(snap.asks) == 3


# ── apply_diff ────────────────────────────────────────────────────────────────

async def test_apply_diff_adds_new_bid_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["29998.00", "2.0"]]))
    snap = await lob.get_snapshot()
    prices = [l.price for l in snap.bids]
    assert 29998.0 in prices


async def test_apply_diff_updates_existing_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["30000.00", "9.9"]]))
    snap = await lob.get_snapshot()
    top_bid = next(l for l in snap.bids if l.price == 30000.0)
    assert abs(top_bid.qty - 9.9) < 1e-9


async def test_apply_diff_removes_zero_qty_level():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data(last_id=100))
    lob.apply_diff(diff_event(U=101, u=101, bids=[["30000.00", "0.0"]]))
    snap = await lob.get_snapshot()
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

async def test_reset_clears_state():
    lob = LocalOrderBook()
    lob.set_snapshot(snapshot_data())
    await lob.reset()
    assert not lob.is_ready
    assert await lob.get_snapshot() is None


# ── State machine (Phase 1D) ──────────────────────────────────────────────────

def snap_msg(last_id: int, bids=None, asks=None) -> dict:
    return {
        "lastUpdateId": last_id,
        "bids": bids or [["30000.00", "1.5"], ["29999.00", "0.8"]],
        "asks": asks or [["30001.00", "1.2"], ["30002.00", "0.5"]],
    }


async def test_state_machine_transition_to_synced():
    lob = LocalOrderBook()
    assert lob.state == LOBStateMachineState.UNINITIALISED
    ok = await lob.apply_snapshot(snap_msg(last_id=1000))
    assert ok is True
    assert lob.state == LOBStateMachineState.SYNCED
    assert lob.lob_status == "SYNCED"
    assert lob.is_ready


async def test_gap_detected_sets_stale():
    lob = LocalOrderBook()
    await lob.apply_snapshot(snap_msg(last_id=1000))
    assert lob.state == LOBStateMachineState.SYNCED
    # Feed a stale snapshot (lastUpdateId regressed)
    ok = await lob.apply_snapshot(snap_msg(last_id=900))
    assert ok is False
    assert lob.state == LOBStateMachineState.GAP_DETECTED
    assert lob.lob_status == "GAP_DETECTED"
    # Book should still hold the last valid snapshot
    assert lob.is_ready


async def test_reinitialise_on_gap():
    lob = LocalOrderBook()
    await lob.apply_snapshot(snap_msg(last_id=1000))
    await lob.apply_snapshot(snap_msg(last_id=900))   # triggers GAP_DETECTED
    assert lob.state == LOBStateMachineState.GAP_DETECTED
    # Next valid snapshot should recover
    ok = await lob.apply_snapshot(snap_msg(last_id=1100))
    assert ok is True
    assert lob.state == LOBStateMachineState.SYNCED


def test_gap_severity_tiering():
    lob = LocalOrderBook()
    assert lob._classify_gap(100)   == "CONTINUE"
    assert lob._classify_gap(499)   == "CONTINUE"
    assert lob._classify_gap(500)   == "HALT_ENTRIES"
    assert lob._classify_gap(4999)  == "HALT_ENTRIES"
    assert lob._classify_gap(5000)  == "CLOSE_REVIEW"
    assert lob._classify_gap(50000) == "CLOSE_REVIEW"


async def test_shared_state_updated_on_transition():
    state = SharedState()
    lob = LocalOrderBook(shared_state=state)
    assert state.lob_status == "UNINITIALISED"
    await lob.apply_snapshot(snap_msg(last_id=500))
    assert state.lob_status == "SYNCED"
    await lob.apply_snapshot(snap_msg(last_id=100))   # stale → GAP_DETECTED
    assert state.lob_status == "GAP_DETECTED"


async def test_get_current_walls_identifies_outlier():
    # Build a book with realistic qty variance so std > 0, then plant a 50x outlier
    base = [0.8 + (i % 5) * 0.15 for i in range(20)]   # 0.8, 0.95, 1.1, 1.25, 1.4, ...
    bids = [[str(30000 - i * 10), str(round(base[i], 2))] for i in range(20)]
    bids[5] = [str(30000 - 5 * 10), "50.0"]   # clear outlier at index 5
    asks = [["30100.00", str(round(base[i], 2))] for i in range(20)]
    lob = LocalOrderBook()
    await lob.apply_snapshot(snap_msg(last_id=1, bids=bids, asks=asks))
    walls = await lob.get_current_walls(sigma=2.5, window=5)
    wall_prices = {w["price"] for w in walls if w["side"] == "bid"}
    assert float(bids[5][0]) in wall_prices


async def test_reset_returns_to_uninitialised():
    lob = LocalOrderBook()
    await lob.apply_snapshot(snap_msg(last_id=1000))
    assert lob.state == LOBStateMachineState.SYNCED
    await lob.reset()
    assert lob.state == LOBStateMachineState.UNINITIALISED
    assert not lob.is_ready
