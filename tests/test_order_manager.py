"""
Unit tests for execution/order_manager.py — Phase 1L (post-review fixes).

All tests use mocked AsyncClient; no real network calls are made.
DRY_RUN is patched per-test to control code paths.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from config import settings
from execution.order_manager import OrderManager, _weighted_avg_fill
from models import FillDetail, MicroOrderRequest, MicroSignal, WallState
from risk.killswitch import GlobalKillswitch


# ── Shared builders ───────────────────────────────────────────────────────────

def _make_wall(price: float, side: str = "bid", qty: float = 100.0) -> WallState:
    return WallState(
        price         = price,
        qty_initial   = qty,
        qty_current   = qty,
        first_seen_ts = 0,
        last_seen_ts  = 500,
        side          = side,
        sigma         = 3.0,
    )


def _make_req(
    direction: str = "LONG",
    notional_hint: float = 0.001,
    age_ms: int = 0,           # offset from now; 0 = freshly minted signal
) -> MicroOrderRequest:
    sig = MicroSignal(
        signal_type     = "SWEEP_WITH_PROTECTION",
        direction       = direction,
        timestamp_ms    = int(time.time() * 1000) - age_ms,
        consumed_wall   = _make_wall(95_000.0, side="ask"),
        protection_wall = _make_wall(
            94_000.0 if direction == "LONG" else 96_000.0,
            side="bid" if direction == "LONG" else "ask",
        ),
    )
    return MicroOrderRequest(
        micro_signal   = sig,
        signal_id      = "test-signal-id-abcd1234",
        order_type     = "IOC_LIMIT",
        side           = "BUY" if direction == "LONG" else "SELL",
        limit_price    = None,
        ioc_timeout_ms = 200,
        confidence     = 0.70,
        notional_hint  = notional_hint,
    )


def _make_manager(
    book_fn=None,
) -> tuple[OrderManager, asyncio.Queue, asyncio.Queue, GlobalKillswitch]:
    sig_q  = asyncio.Queue()
    fill_q = asyncio.Queue()
    ks     = GlobalKillswitch(dov=10_000.0)
    om     = OrderManager(sig_q, fill_q, ks, equity_fn=lambda: 10_000.0, book_fn=book_fn)
    return om, sig_q, fill_q, ks


def _filled_resp(price: float = 95_015.0, qty: float = 0.001) -> dict:
    return {
        "orderId":     "101",
        "status":      "FILLED",
        "executedQty": str(qty),
        "fills":       [{"price": str(price), "qty": str(qty)}],
    }


# ── 1. LONG limit price formula ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_long_limit_price_formula():
    """LONG: limit_price = best_ask + 0.5 × spread, stored in FillDetail.limit_price."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")
    best_bid, best_ask = 95_000.0, 95_010.0
    expected_limit = round(best_ask + (best_ask - best_bid) * 0.5, 2)  # 95_015.0

    with patch.object(settings, "DRY_RUN", True):
        await om._submit_aggressive_limit(req, "BUY", best_bid, best_ask)

    fill: FillDetail = fill_q.get_nowait()
    assert fill.limit_price == pytest.approx(expected_limit)


# ── 2. SHORT limit price formula ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_short_limit_price_formula():
    """SHORT: limit_price = best_bid - 0.5 × spread."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("SHORT")
    best_bid, best_ask = 95_000.0, 95_010.0
    expected_limit = round(best_bid - (best_ask - best_bid) * 0.5, 2)  # 94_995.0

    with patch.object(settings, "DRY_RUN", True):
        await om._submit_aggressive_limit(req, "SELL", best_bid, best_ask)

    fill: FillDetail = fill_q.get_nowait()
    assert fill.limit_price == pytest.approx(expected_limit)


# ── 3. IOC expired → None, no side-effects ───────────────────────────────────

@pytest.mark.asyncio
async def test_ioc_expired_returns_none():
    """EXPIRED response with executedQty=0 → returns None; fill_queue empty; fill_event not set."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(
        return_value={"orderId": "999", "status": "EXPIRED", "executedQty": "0"}
    )

    with patch.object(settings, "DRY_RUN", False):
        result = await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert result is None
    assert fill_q.empty()
    assert not req.fill_event.is_set()


# ── 4. Fill emits FillDetail to fill_queue ────────────────────────────────────

@pytest.mark.asyncio
async def test_fill_emits_to_fill_queue():
    """Successful IOC fill puts exactly one FillDetail on fill_queue."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")
    fill_px = 95_015.0

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(return_value=_filled_resp(fill_px))

    with patch.object(settings, "DRY_RUN", False):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert fill_q.qsize() == 1
    detail: FillDetail = fill_q.get_nowait()
    assert detail.signal_id == req.signal_id
    assert detail.side       == "BUY"
    assert detail.fill_price == pytest.approx(fill_px)
    assert detail.order_id   == "101"


# ── 5. Fill sets fill_event ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fill_sets_fill_event():
    """req.fill_event is set after a confirmed fill."""
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(return_value=_filled_resp())

    assert not req.fill_event.is_set()
    with patch.object(settings, "DRY_RUN", False):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert req.fill_event.is_set()


# ── 6. Slippage recorded to killswitch ────────────────────────────────────────

@pytest.mark.asyncio
async def test_slippage_recorded_to_killswitch():
    """Fill above best_ask (LONG) → positive slippage bps in killswitch buffer."""
    om, _, _, ks = _make_manager()
    req = _make_req("LONG")
    best_ask = 95_010.0
    fill_px  = 95_020.0  # 10 pts above signal price → ≈ 1.05 bps

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(return_value=_filled_resp(fill_px))

    with patch.object(settings, "DRY_RUN", False):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, best_ask)

    expected_bps = (fill_px - best_ask) / best_ask * 10_000
    assert len(ks._slippage_buf) == 1
    assert ks._slippage_buf[0] == pytest.approx(expected_bps, abs=0.01)


# ── 7. accepting_new_signals=False drains without API calls ───────────────────

@pytest.mark.asyncio
async def test_accepting_false_discards_signals():
    """Signals are silently dropped when accepting_new_signals is False."""
    om, sig_q, fill_q, _ = _make_manager()
    om.accepting_new_signals = False
    om._client = AsyncMock()

    await sig_q.put(_make_req("LONG"))

    task = asyncio.create_task(om._order_loop())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert fill_q.empty()
    om._client.create_order.assert_not_called()


# ── 8. DRY_RUN skips API, still emits fill ────────────────────────────────────

@pytest.mark.asyncio
async def test_dry_run_no_api_call():
    """DRY_RUN=True: no create_order call; fill_event set; fill_queue populated."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")
    om._client = AsyncMock()

    with patch.object(settings, "DRY_RUN", True):
        resp = await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert resp is not None
    om._client.create_order.assert_not_called()
    assert req.fill_event.is_set()
    assert not fill_q.empty()


# ── 9. handle_protection_wall_removed (DRY_RUN) ───────────────────────────────

@pytest.mark.asyncio
async def test_handle_wall_removed_dry_run():
    """DRY_RUN: handle_protection_wall_removed sets position_closed_event; state reset."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_position_side         = "BUY"
    om._open_position_qty          = 0.001
    om._open_position_closed_event = closed_event
    om._open_oco_list_id           = 555

    with patch.object(settings, "DRY_RUN", True):
        await om.handle_protection_wall_removed("LONG")

    assert closed_event.is_set()
    assert om._open_position_side         is None
    assert om._open_position_qty          == 0.0
    assert om._open_oco_list_id           is None
    assert om._open_position_closed_event is None


# ── 10. S1: stale signal rejected before order placement ─────────────────────

@pytest.mark.asyncio
async def test_stale_signal_rejected_at_submission():
    """Signal older than ioc_timeout_ms is rejected without calling create_order."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG", age_ms=500)   # 500ms old, timeout=200ms

    om._client = AsyncMock()

    with patch.object(settings, "DRY_RUN", False):
        result = await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert result is None
    assert fill_q.empty()
    om._client.create_order.assert_not_called()


# ── 11. C1: OCO failure triggers emergency close ──────────────────────────────

@pytest.mark.asyncio
async def test_oco_failure_triggers_emergency_close():
    """If create_oco_order raises, _emergency_close is called for the filled qty."""
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(return_value=_filled_resp(95_015.0, qty=0.001))
    om._client.create_oco_order = AsyncMock(side_effect=Exception("OCO_ERROR"))
    om._client.get_order_book = AsyncMock(return_value={
        "bids": [["95000.0", "1.0"]],
        "asks": [["95010.0", "1.0"]],
    })

    emergency_close_calls: list = []
    original_emergency = om._emergency_close
    async def spy_emergency(qty, entry_side, reason):
        emergency_close_calls.append((qty, entry_side, reason))
        return False, 0.0  # simulate unfilled to avoid further side-effects
    om._emergency_close = spy_emergency

    with patch.object(settings, "DRY_RUN", False):
        await om._place_oco(req, fill_price=95_015.0, fill_qty=0.001, entry_side="BUY")

    assert len(emergency_close_calls) == 1
    qty, side, reason = emergency_close_calls[0]
    assert qty    == pytest.approx(0.001)
    assert side   == "BUY"
    assert reason == "OCO_FAILED"


# ── 12. C2: position lock prevents state corruption ──────────────────────────

@pytest.mark.asyncio
async def test_position_lock_used_on_fill():
    """Open-position state is set inside _position_lock (lock must exist and be released)."""
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")

    # Verify lock is available (not already acquired) before and after the call.
    assert not om._position_lock.locked()

    with patch.object(settings, "DRY_RUN", True):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert not om._position_lock.locked()
    assert om._open_position_side == "BUY"


# ── 13. M1: book_fn used when injected ───────────────────────────────────────

@pytest.mark.asyncio
async def test_book_fn_used_in_dry_run():
    """Injected book_fn is called instead of REST; its prices flow into limit calculation."""
    book_fn = MagicMock(return_value=(94_990.0, 95_002.0))
    om, _, fill_q, _ = _make_manager(book_fn=book_fn)
    req = _make_req("LONG")

    with patch.object(settings, "DRY_RUN", True):
        book = await om._resolve_book()

    book_fn.assert_called_once()
    assert book == (94_990.0, 95_002.0)


# ── 14. M2: entry qty floor-quantized to LOT_SIZE stepSize ──────────────────

@pytest.mark.asyncio
async def test_entry_qty_floor_quantized_to_step():
    """Computed entry qty is truncated down to the nearest QTY_STEP_SIZE multiple.

    _make_req uses notional_hint=0.001, equity=10_000, protection wall at 94_000
    for LONG. With best_ask=95_010: raw_bps=106.3 → capped to 25 bps →
    sl_distance = 95_010 × 0.0025 = 237.525, raw_qty = 10/237.525 = 0.042099...
    round(..., 5) would give 0.04210 (not a whole number of steps).
    Floor-quantize must give 0.04209 (4209 steps exactly).
    """
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")

    with patch.object(settings, "DRY_RUN", True):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    fill: FillDetail = fill_q.get_nowait()
    assert fill.qty == pytest.approx(0.04209)
    assert fill.qty % settings.QTY_STEP_SIZE == pytest.approx(0.0)


# ── 15. M3: full fill_queue drops record, does not block ─────────────────────

@pytest.mark.asyncio
async def test_fill_queue_full_drops_without_blocking():
    """put_nowait: a maxsize=1 queue that is already full drops the FillDetail silently."""
    sig_q  = asyncio.Queue()
    fill_q = asyncio.Queue(maxsize=1)
    ks     = GlobalKillswitch(dov=10_000.0)
    om     = OrderManager(sig_q, fill_q, ks, equity_fn=lambda: 10_000.0)

    # Pre-fill the queue to capacity
    fill_q.put_nowait("sentinel")

    req = _make_req("LONG")
    with patch.object(settings, "DRY_RUN", True):
        resp = await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    # Order still "succeeded" — fill_event set — but FillDetail was dropped
    assert resp is not None
    assert req.fill_event.is_set()
    assert fill_q.get_nowait() == "sentinel"   # original item still there
    assert fill_q.empty()


# ── 16. m2: OCO list_id stored as int, cancel uses int ───────────────────────

@pytest.mark.asyncio
async def test_oco_list_id_stored_as_int():
    """orderListId from exchange response is stored as int (not str) to prevent int('') crash."""
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")

    om._client = AsyncMock()
    om._client.create_order = AsyncMock(return_value=_filled_resp())
    om._client.create_oco_order = AsyncMock(return_value={"orderListId": 9876})

    with patch.object(settings, "DRY_RUN", False):
        await om._place_oco(req, fill_price=95_015.0, fill_qty=0.001, entry_side="BUY")

    assert om._open_oco_list_id == 9876
    assert isinstance(om._open_oco_list_id, int)


# ── 17. S2: position_closed_event not set when emergency close unfilled ───────

@pytest.mark.asyncio
async def test_s2_closed_event_not_set_on_unfilled_exit():
    """handle_protection_wall_removed: position_closed_event stays clear if exit IOC unfilled."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_position_side         = "BUY"
    om._open_position_qty          = 0.001
    om._open_position_closed_event = closed_event
    om._open_oco_list_id           = None

    # Simulate: OCO cancel skipped (oco_id is None); emergency close returns unfilled
    om._client = AsyncMock()
    async def _unfilled_close(qty, entry_side, reason):
        return False, 0.0
    om._emergency_close = _unfilled_close

    with patch.object(settings, "DRY_RUN", False):
        await om.handle_protection_wall_removed("LONG")

    assert not closed_event.is_set()
    assert om._open_position_side is None   # state still reset


# ── Helper: _weighted_avg_fill ────────────────────────────────────────────────

def test_weighted_avg_fill_single():
    resp = {"fills": [{"price": "100.0", "qty": "1.0"}]}
    assert _weighted_avg_fill(resp) == pytest.approx(100.0)


def test_weighted_avg_fill_multiple():
    # (100×1 + 102×3) / 4 = 101.5
    resp = {"fills": [{"price": "100.0", "qty": "1.0"}, {"price": "102.0", "qty": "3.0"}]}
    assert _weighted_avg_fill(resp) == pytest.approx(101.5)


def test_weighted_avg_fill_no_fills_uses_price():
    resp = {"price": "99.5", "fills": []}
    assert _weighted_avg_fill(resp) == pytest.approx(99.5)


# ── 18. S1 race fix: _placing_oco defers Gate 6 action ───────────────────────

@pytest.mark.asyncio
async def test_s1_placing_oco_defers_wall_removed_handler():
    """
    While _placing_oco is True, handle_protection_wall_removed must NOT call
    _emergency_close — it sets _cancel_oco_on_placement and returns.
    """
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_position_side         = "BUY"
    om._open_position_qty          = 0.001
    om._open_position_closed_event = closed_event
    om._placing_oco                = True  # simulate: OCO REST call in-flight

    emergency_called = False
    async def spy_emergency(qty, entry_side, reason):
        nonlocal emergency_called
        emergency_called = True
        return True, 95_000.0
    om._emergency_close = spy_emergency

    with patch.object(settings, "DRY_RUN", False):
        await om.handle_protection_wall_removed("LONG")

    # Handler must have set the deferred flag and returned without closing
    assert om._cancel_oco_on_placement is True
    assert not emergency_called
    assert not closed_event.is_set()


# ── 19. S1 race fix: _place_oco performs deferred cancel-and-close ────────────

@pytest.mark.asyncio
async def test_s1_deferred_cancel_executed_after_oco_placed():
    """
    When _cancel_oco_on_placement is True after OCO placement, _place_oco
    cancels the OCO and calls _emergency_close with reason WALL_REMOVED_DURING_OCO.
    """
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")

    om._client = AsyncMock()
    om._client.create_oco_order = AsyncMock(return_value={"orderListId": 42})
    om._client.delete_oco_order = AsyncMock()

    emergency_calls: list = []
    async def spy_emergency(qty, entry_side, reason):
        emergency_calls.append(reason)
        return True, 94_990.0  # simulate confirmed close
    om._emergency_close = spy_emergency

    # Simulate Gate 6 having set the deferred cancel flag before _place_oco runs
    om._cancel_oco_on_placement = True

    with patch.object(settings, "DRY_RUN", False):
        await om._place_oco(req, fill_price=95_015.0, fill_qty=0.001, entry_side="BUY")

    assert emergency_calls == ["WALL_REMOVED_DURING_OCO"]
    om._client.delete_oco_order.assert_called_once()
    # After deferred close, position state must be fully reset
    assert om._open_position_side         is None
    assert om._open_position_qty          == 0.0
    assert om._placing_oco                is False
    assert om._cancel_oco_on_placement    is False


# ── 20. S2 fix: state reset after successful emergency close from _place_oco ──

@pytest.mark.asyncio
async def test_s2_state_reset_after_oco_failed_and_closed():
    """
    OCO fails → _emergency_close returns True → _reset_open_position called
    and position_closed_event set. Gate 6 cannot trigger a second close.
    """
    om, _, _, _ = _make_manager()
    req = _make_req("LONG")
    closed_event = asyncio.Event()
    req.position_closed_event = closed_event

    # Pre-populate state as if a fill has occurred
    om._open_position_side         = "BUY"
    om._open_position_qty          = 0.001
    om._open_position_closed_event = closed_event

    om._client = AsyncMock()
    om._client.create_oco_order = AsyncMock(side_effect=Exception("OCO_REJECT"))

    async def confirmed_close(qty, entry_side, reason):
        return True, 94_990.0  # position successfully closed
    om._emergency_close = confirmed_close

    with patch.object(settings, "DRY_RUN", False):
        await om._place_oco(req, fill_price=95_015.0, fill_qty=0.001, entry_side="BUY")

    # State must be zeroed so a subsequent Gate 6 call cannot double-close
    assert om._open_position_side         is None
    assert om._open_position_qty          == 0.0
    assert om._open_oco_list_id           is None
    assert om._open_position_closed_event is None
    assert om._placing_oco                is False
    # position_closed_event must be set for Gate 6's monitor to exit cleanly
    assert closed_event.is_set()


# ── 21. OCO watcher records WIN on natural TP fill ───────────────────────────

@pytest.mark.asyncio
async def test_watch_oco_win_records_outcome_and_sets_event():
    """ALL_DONE OCO with exit_price > entry_price (LONG) → WIN, position_closed_event set."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    entry_price = 95_000.0
    exit_price  = 97_000.0

    om._open_signal_id    = "sig-win-test"
    om._open_entry_price  = entry_price
    om._open_entry_time   = time.monotonic() - 30
    om._open_position_side = "BUY"
    om._open_position_qty  = 0.001
    om._open_position_closed_event = closed_event

    outcomes: list = []
    async def capture_outcome(signal_id, outcome, pnl, pnl_pct, duration_min, r_multiple=None):
        outcomes.append((signal_id, outcome, pnl))
    om._update_outcome_cb = capture_outcome

    om._client = AsyncMock()
    om._client.get_oco_order = AsyncMock(return_value={
        "listStatusType": "ALL_DONE",
        "orders": [{"orderId": 42}],
    })
    om._client.get_order = AsyncMock(return_value={
        "executedQty": "0.001",
        "avgPrice": str(exit_price),
    })

    await om._watch_oco_outcome(
        oco_list_id=123, signal_id="sig-win-test",
        entry_side="BUY", entry_price=entry_price, fill_qty=0.001,
        entry_time=time.monotonic() - 30,
        position_closed_event=closed_event, poll_interval_s=0.01,
    )
    await asyncio.sleep(0)  # let the create_task coroutine execute

    assert closed_event.is_set()
    assert len(outcomes) == 1
    assert outcomes[0][0] == "sig-win-test"
    assert outcomes[0][1] == "WIN"
    assert outcomes[0][2] == pytest.approx((exit_price - entry_price) * 0.001)


# ── 22. OCO watcher exits cleanly on external close ──────────────────────────

@pytest.mark.asyncio
async def test_watch_oco_exits_cleanly_on_external_close():
    """If position_closed_event is already set, watcher exits without polling or recording."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    closed_event.set()  # position already closed before watcher polls

    outcomes: list = []
    async def capture(*args):
        outcomes.append(args)
    om._update_outcome_cb = capture
    om._client = AsyncMock()

    await om._watch_oco_outcome(
        oco_list_id=99, signal_id="sig-ext", entry_side="BUY",
        entry_price=95_000.0, fill_qty=0.001,
        entry_time=time.monotonic(),
        position_closed_event=closed_event, poll_interval_s=0.01,
    )

    assert not outcomes                          # no outcome recorded
    om._client.get_oco_order.assert_not_called() # no REST call made


# ── 23. OCO watcher continues polling while EXECUTING ────────────────────────

@pytest.mark.asyncio
async def test_watch_oco_continues_polling_while_executing():
    """EXECUTING status loops; ALL_DONE on second call → outcome recorded."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()

    om._open_signal_id   = "sig-poll"
    om._open_entry_time  = time.monotonic()
    om._open_entry_price = 95_000.0

    call_count = 0
    async def oco_side(**_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            return {"listStatusType": "EXECUTING"}
        return {"listStatusType": "ALL_DONE", "orders": [{"orderId": 1}]}

    om._client = AsyncMock()
    om._client.get_oco_order = AsyncMock(side_effect=oco_side)
    om._client.get_order = AsyncMock(return_value={
        "executedQty": "0.001", "avgPrice": "96000.0"
    })

    await om._watch_oco_outcome(
        oco_list_id=5, signal_id="sig-poll", entry_side="BUY",
        entry_price=95_000.0, fill_qty=0.001,
        entry_time=time.monotonic(),
        position_closed_event=closed_event, poll_interval_s=0.01,
    )

    assert call_count == 2
    assert closed_event.is_set()


# ── 24. Active exposure guard ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_active_exposure_blocks_second_submit():
    """A second signal cannot overwrite an already tracked open position."""
    om, _, fill_q, _ = _make_manager(book_fn=lambda: (95_000.0, 95_010.0))
    om._open_position_side = "BUY"
    om._open_position_qty = 0.001

    with patch.object(settings, "DRY_RUN", True):
        await om._submit(_make_req("LONG"))

    assert fill_q.empty()
    assert om._open_position_side == "BUY"
    assert om._open_position_qty == pytest.approx(0.001)


@pytest.mark.asyncio
async def test_entry_in_flight_clears_after_unfilled_entry():
    """Rejected/unfilled entries must release the in-flight exposure guard."""
    om, _, _, _ = _make_manager(book_fn=lambda: (95_000.0, 95_010.0))
    om._client = AsyncMock()
    om._client.create_order = AsyncMock(
        return_value={"orderId": "999", "status": "EXPIRED", "executedQty": "0"}
    )

    with patch.object(settings, "DRY_RUN", False):
        await om._submit(_make_req("LONG"))

    assert not om.has_active_exposure()


@pytest.mark.asyncio
async def test_natural_oco_close_clears_active_exposure():
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_signal_id = "sig-clear"
    om._open_entry_price = 95_000.0
    om._open_entry_time = time.monotonic()
    om._open_position_side = "BUY"
    om._open_position_qty = 0.001
    om._open_position_closed_event = closed_event

    om._client = AsyncMock()
    om._client.get_oco_order = AsyncMock(return_value={
        "listStatusType": "ALL_DONE",
        "orders": [{"orderId": 42}],
    })
    om._client.get_order = AsyncMock(return_value={
        "executedQty": "0.001",
        "avgPrice": "96_000.0".replace("_", ""),
    })

    await om._watch_oco_outcome(
        oco_list_id=123, signal_id="sig-clear",
        entry_side="BUY", entry_price=95_000.0, fill_qty=0.001,
        entry_time=time.monotonic(),
        position_closed_event=closed_event, poll_interval_s=0.01,
    )

    assert closed_event.is_set()
    assert not om.has_active_exposure()


@pytest.mark.asyncio
async def test_safety_exit_cancels_oco_and_emergency_closes():
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_position_side = "BUY"
    om._open_position_qty = 0.001
    om._open_position_closed_event = closed_event
    om._open_oco_list_id = 77
    om._open_signal_id = "sig-safety"
    om._open_entry_price = 95_000.0
    om._open_entry_time = time.monotonic()
    om._client = AsyncMock()
    om._client.delete_oco_order = AsyncMock()

    emergency_reasons = []
    async def confirmed_close(qty, entry_side, reason):
        emergency_reasons.append(reason)
        return True, 94_900.0
    om._emergency_close = confirmed_close

    with patch.object(settings, "DRY_RUN", False):
        await om.handle_safety_exit("MAX_HOLD")

    om._client.delete_oco_order.assert_called_once()
    assert emergency_reasons == ["SAFETY_MAX_HOLD"]
    assert closed_event.is_set()
    assert not om.has_active_exposure()
