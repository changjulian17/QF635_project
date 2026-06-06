"""Tests for UserDataStreamConsumer + OrderManager.on_execution_report integration."""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dashboard._logic import handle_position_event


# ── handle_position_event (pure function) ────────────────────────────────────

def test_handle_position_event_opened():
    state = {}
    msg = {
        "type": "position_opened",
        "ts": 1000.0,
        "side": "BUY",
        "entry_price": 50000.0,
        "quantity": 0.001,
        "stop_loss": 49000.0,
        "take_profit": 52000.0,
    }
    result = handle_position_event(state, msg)
    assert result["side"] == "BUY"
    assert result["entry_price"] == 50000.0
    assert "type" not in result   # type key stripped


def test_handle_position_event_close():
    state = {"side": "BUY", "entry_price": 50000.0}
    msg = {"type": "close_event", "ts": 2000.0, "outcome": "WIN", "pnl": 5.0}
    result = handle_position_event(state, msg)
    assert result == {}


def test_handle_position_event_unknown_type():
    state = {"side": "BUY"}
    msg = {"type": "portfolio", "equity": 1000.0}
    result = handle_position_event(state, msg)
    assert result == {"side": "BUY"}


def test_handle_position_event_does_not_mutate():
    original = {"side": "BUY"}
    msg = {"type": "close_event", "ts": 1.0}
    result = handle_position_event(original, msg)
    assert result == {}
    assert original == {"side": "BUY"}   # unchanged


# ── on_execution_report ───────────────────────────────────────────────────────

def _make_order_manager(portfolio_hub=None):
    """Return a minimal OrderManager wired for testing without real Binance connectivity."""
    from execution.order_manager import OrderManager
    om = OrderManager.__new__(OrderManager)
    # Initialise all internal state manually (mirrors __init__ but without signal queues).
    om._position_lock               = asyncio.Lock()
    om._open_position_side          = None
    om._open_position_qty           = 0.0
    om._open_oco_list_id            = None
    om._open_position_closed_event  = None
    om._open_signal_id              = None
    om._open_entry_price            = 0.0
    om._open_entry_time             = 0.0
    om._open_sl_price               = 0.0
    om._open_tp_price               = 0.0
    om._oco_watcher_task            = None
    om._placing_oco                 = False
    om._cancel_oco_on_placement     = False
    om._entry_in_flight             = False
    om._emergency_close_in_progress = False
    om._budget_update_cb            = None
    om._update_outcome_cb           = None
    om._portfolio_hub               = portfolio_hub
    return om


def _open_position(om, *, side="BUY", entry_price=50000.0, qty=0.001,
                   oco_list_id=99, sl_price=49000.0, tp_price=52000.0,
                   signal_id="sig-abc"):
    om._open_position_side   = side
    om._open_entry_price     = entry_price
    om._open_position_qty    = qty
    om._open_oco_list_id     = oco_list_id
    om._open_sl_price        = sl_price
    om._open_tp_price        = tp_price
    om._open_signal_id       = signal_id
    om._open_entry_time      = time.monotonic() - 30
    pce = asyncio.Event()
    om._open_position_closed_event = pce
    return pce


@pytest.mark.asyncio
async def test_on_execution_report_ignores_non_filled():
    om = _make_order_manager()
    _open_position(om, oco_list_id=99)
    msg = {"e": "executionReport", "X": "NEW", "g": 99, "L": "51000.0"}
    await om.on_execution_report(msg)
    assert om._open_position_side == "BUY"   # state unchanged


@pytest.mark.asyncio
async def test_on_execution_report_ignores_wrong_oco_id():
    om = _make_order_manager()
    _open_position(om, oco_list_id=99)
    msg = {"e": "executionReport", "X": "FILLED", "g": 42, "L": "51000.0"}
    await om.on_execution_report(msg)
    assert om._open_position_side == "BUY"   # wrong OCO id — ignored


@pytest.mark.asyncio
async def test_on_execution_report_resets_state_on_fill():
    om = _make_order_manager()
    pce = _open_position(om, oco_list_id=99, entry_price=50000.0, side="BUY")
    msg = {"e": "executionReport", "X": "FILLED", "g": 99, "L": "51000.0"}
    await om.on_execution_report(msg)
    assert om._open_position_side is None
    assert om._open_oco_list_id is None
    assert pce.is_set()


@pytest.mark.asyncio
async def test_on_execution_report_cancels_watcher_task():
    om = _make_order_manager()
    _open_position(om, oco_list_id=99)

    async def _never_ends():
        await asyncio.sleep(999)

    om._oco_watcher_task = asyncio.create_task(_never_ends())
    msg = {"e": "executionReport", "X": "FILLED", "g": 99, "L": "51000.0"}
    await om.on_execution_report(msg)
    await asyncio.sleep(0)   # let cancellation propagate
    assert om._oco_watcher_task is None


@pytest.mark.asyncio
async def test_on_execution_report_broadcasts_close_event():
    hub = MagicMock()
    hub.broadcast = AsyncMock()
    om = _make_order_manager(portfolio_hub=hub)
    _open_position(om, oco_list_id=99, side="BUY", entry_price=50000.0)
    msg = {"e": "executionReport", "X": "FILLED", "g": 99, "L": "51500.0"}
    await om.on_execution_report(msg)
    await asyncio.sleep(0)
    hub.broadcast.assert_called_once()
    call_arg = hub.broadcast.call_args[0][0]
    assert call_arg["type"] == "close_event"
    assert call_arg["outcome"] == "WIN"
    assert call_arg["pnl"] > 0


@pytest.mark.asyncio
async def test_on_execution_report_no_hub_no_error():
    om = _make_order_manager(portfolio_hub=None)
    _open_position(om, oco_list_id=99)
    msg = {"e": "executionReport", "X": "FILLED", "g": 99, "L": "51000.0"}
    await om.on_execution_report(msg)   # must not raise
    assert om._open_position_side is None


# ── get_open_position ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_open_position_none_when_no_position():
    om = _make_order_manager()
    assert om.get_open_position() is None


@pytest.mark.asyncio
async def test_get_open_position_returns_state():
    om = _make_order_manager()
    _open_position(om, side="SELL", entry_price=60000.0, qty=0.002,
                   sl_price=61000.0, tp_price=57000.0)
    pos = om.get_open_position()
    assert pos is not None
    assert pos["side"] == "SELL"
    assert pos["entry_price"] == 60000.0
    assert pos["stop_loss"] == 61000.0
    assert pos["take_profit"] == 57000.0
    assert pos["quantity"] == 0.002


@pytest.mark.asyncio
async def test_get_open_position_none_after_fill():
    om = _make_order_manager()
    _open_position(om, oco_list_id=99)
    msg = {"e": "executionReport", "X": "FILLED", "g": 99, "L": "50500.0"}
    await om.on_execution_report(msg)
    assert om.get_open_position() is None
