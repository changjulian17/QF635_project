"""
Unit tests for execution/order_manager.py — Phase 1L.

All tests use mocked AsyncClient; no real network calls are made.
DRY_RUN is patched per-test to control code paths.
"""

import asyncio
from unittest.mock import AsyncMock, patch

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


def _make_req(direction: str = "LONG", notional_hint: float = 0.001) -> MicroOrderRequest:
    sig = MicroSignal(
        signal_type    = "SWEEP_WITH_PROTECTION",
        direction      = direction,
        timestamp_ms   = 0,
        consumed_wall  = _make_wall(95_000.0, side="ask"),
        protection_wall= _make_wall(
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


def _make_manager() -> tuple[OrderManager, asyncio.Queue, asyncio.Queue, GlobalKillswitch]:
    sig_q  = asyncio.Queue()
    fill_q = asyncio.Queue()
    ks     = GlobalKillswitch(dov=10_000.0)
    om     = OrderManager(sig_q, fill_q, ks, equity_fn=lambda: 10_000.0)
    return om, sig_q, fill_q, ks


# ── 1. LONG limit price formula ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_long_limit_price_formula():
    """LONG: limit_price = best_ask + 0.5 × spread, stored in FillDetail.limit_price."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")
    best_bid, best_ask = 95_000.0, 95_010.0
    expected_limit = round(best_ask + (best_ask - best_bid) * 0.5, 2)  # 95_015.0

    with patch.object(settings, "DRY_RUN", True):
        resp = await om._submit_aggressive_limit(req, "BUY", best_bid, best_ask)

    assert resp is not None
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
        resp = await om._submit_aggressive_limit(req, "SELL", best_bid, best_ask)

    assert resp is not None
    fill: FillDetail = fill_q.get_nowait()
    assert fill.limit_price == pytest.approx(expected_limit)


# ── 3. IOC expired → None, no side-effects ───────────────────────────────────

@pytest.mark.asyncio
async def test_ioc_expired_returns_none():
    """EXPIRED response with executedQty=0 → returns None; fill_queue empty; fill_event not set."""
    om, _, fill_q, _ = _make_manager()
    req = _make_req("LONG")

    mock_client = AsyncMock()
    mock_client.create_order = AsyncMock(
        return_value={"orderId": "999", "status": "EXPIRED", "executedQty": "0"}
    )
    om._client = mock_client

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

    mock_client = AsyncMock()
    mock_client.create_order = AsyncMock(return_value={
        "orderId":     "101",
        "status":      "FILLED",
        "executedQty": "0.001",
        "fills":       [{"price": str(fill_px), "qty": "0.001"}],
    })
    om._client = mock_client

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

    mock_client = AsyncMock()
    mock_client.create_order = AsyncMock(return_value={
        "orderId":     "202",
        "status":      "FILLED",
        "executedQty": "0.001",
        "fills":       [{"price": "95015.0", "qty": "0.001"}],
    })
    om._client = mock_client

    assert not req.fill_event.is_set()
    with patch.object(settings, "DRY_RUN", False):
        await om._submit_aggressive_limit(req, "BUY", 95_000.0, 95_010.0)

    assert req.fill_event.is_set()


# ── 6. Slippage recorded to killswitch ────────────────────────────────────────

@pytest.mark.asyncio
async def test_slippage_recorded_to_killswitch():
    """Fill above best_ask (LONG) → positive slippage bps recorded in killswitch buffer."""
    om, _, _, ks = _make_manager()
    req = _make_req("LONG")
    best_ask = 95_010.0
    # fill 10 points above best_ask → slippage = 10/95010 × 10000 ≈ 1.05 bps
    fill_px = 95_020.0

    mock_client = AsyncMock()
    mock_client.create_order = AsyncMock(return_value={
        "orderId":     "303",
        "status":      "FILLED",
        "executedQty": "0.001",
        "fills":       [{"price": str(fill_px), "qty": "0.001"}],
    })
    om._client = mock_client

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

    req = _make_req("LONG")
    await sig_q.put(req)

    # Run _order_loop for one iteration, then cancel
    task = asyncio.create_task(om._order_loop())
    await asyncio.sleep(0)   # let the loop consume the item
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert fill_q.empty()
    assert not req.fill_event.is_set()
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
    """DRY_RUN: handle_protection_wall_removed sets position_closed_event and resets state."""
    om, _, _, _ = _make_manager()
    closed_event = asyncio.Event()
    om._open_position_side          = "BUY"
    om._open_position_qty           = 0.001
    om._open_position_closed_event  = closed_event
    om._open_oco_list_id            = "555"

    with patch.object(settings, "DRY_RUN", True):
        await om.handle_protection_wall_removed("LONG")

    assert closed_event.is_set()
    # state reset
    assert om._open_position_side    is None
    assert om._open_position_qty     == 0.0
    assert om._open_oco_list_id      is None
    assert om._open_position_closed_event is None


# ── Helper: _weighted_avg_fill ────────────────────────────────────────────────

def test_weighted_avg_fill_single():
    resp = {"fills": [{"price": "100.0", "qty": "1.0"}]}
    assert _weighted_avg_fill(resp) == pytest.approx(100.0)


def test_weighted_avg_fill_multiple():
    resp = {
        "fills": [
            {"price": "100.0", "qty": "1.0"},
            {"price": "102.0", "qty": "3.0"},
        ]
    }
    # (100×1 + 102×3) / 4 = 406/4 = 101.5
    assert _weighted_avg_fill(resp) == pytest.approx(101.5)


def test_weighted_avg_fill_no_fills_uses_price():
    resp = {"price": "99.5", "fills": []}
    assert _weighted_avg_fill(resp) == pytest.approx(99.5)
