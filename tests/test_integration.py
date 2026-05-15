"""
Integration tests — exercises multiple components together.

Tests verify the 7-gate funnel produces telemetry at every rejection,
and that the GlobalKillswitch correctly blocks orders when fired.
"""

import asyncio
import os
import sqlite3
import tempfile

import pytest
import pytest_asyncio

from core.cvd import CVDCalculator
from core.signal_telemetry import SignalRecord, SignalTelemetry
from execution.order_manager import OrderManager
from models import (
    MicroSignal,
    OrderRequest,
    PatternSignal,
    PatternType,
    Direction,
    PortfolioState,
    SharedState,
)
from risk.budget import DailyBudget
from risk.killswitch import GlobalKillswitch
from strategy.executor import RuleBasedScorer, StrategyExecutor
from strategy.features import FeatureComputer


# ── Helpers ───────────────────────────────────────────────────────────────────

def _micro_signal(direction: str = "LONG") -> MicroSignal:
    return MicroSignal(
        signal_type     = "SWEEP_WITH_PROTECTION",
        direction       = direction,
        timestamp_ms    = int(asyncio.get_event_loop().time() * 1000),
        prior_absorption = True,
        cvd_std         = 2.0,
        price_move_pct  = 0.0005,
    )


# ── Gate funnel tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_gate0_rejects_when_lob_not_synced():
    """Gate 0 must reject signals when lob_status != SYNCED."""
    micro_q   = asyncio.Queue()
    signal_q  = asyncio.Queue()
    telem_q   = asyncio.Queue()

    shared    = SharedState()
    shared.lob_status = "UNINITIALISED"   # not SYNCED
    shared.heartbeat_status = "HEALTHY"

    budget    = DailyBudget.from_equity(10_000.0)
    scorer    = RuleBasedScorer()
    executor  = StrategyExecutor(micro_q, signal_q, telem_q, FeatureComputer(), shared, budget, scorer)

    await micro_q.put(_micro_signal())

    async def _run():
        await asyncio.wait_for(executor.run(), timeout=0.5)

    with pytest.raises((asyncio.TimeoutError, Exception)):
        await _run()

    rec: SignalRecord = telem_q.get_nowait()
    assert rec.gate_passed == "GATE_0_FAIL"
    assert signal_q.empty()


@pytest.mark.asyncio
async def test_gate0_rejects_on_critical_heartbeat():
    """Gate 0 must reject when heartbeat is CRITICAL."""
    micro_q  = asyncio.Queue()
    signal_q = asyncio.Queue()
    telem_q  = asyncio.Queue()

    shared   = SharedState()
    shared.lob_status       = "SYNCED"
    shared.heartbeat_status = "CRITICAL"

    budget   = DailyBudget.from_equity(10_000.0)
    executor = StrategyExecutor(micro_q, signal_q, telem_q, FeatureComputer(), shared, budget)

    await micro_q.put(_micro_signal())

    try:
        await asyncio.wait_for(executor.run(), timeout=0.3)
    except (asyncio.TimeoutError, Exception):
        pass

    rec: SignalRecord = telem_q.get_nowait()
    assert rec.gate_passed == "GATE_0_FAIL"


@pytest.mark.asyncio
async def test_gate1_rejects_without_sweep_signal():
    """Gate 1 rejects signals that are not SWEEP_WITH_PROTECTION."""
    micro_q  = asyncio.Queue()
    signal_q = asyncio.Queue()
    telem_q  = asyncio.Queue()

    shared   = SharedState()
    shared.lob_status       = "SYNCED"
    shared.heartbeat_status = "HEALTHY"

    budget   = DailyBudget.from_equity(10_000.0)
    executor = StrategyExecutor(micro_q, signal_q, telem_q, FeatureComputer(), shared, budget)

    sig = MicroSignal(
        signal_type  = "ABSORPTION_ONLY",  # not a sweep signal
        direction    = "LONG",
        timestamp_ms = 0,
    )
    await micro_q.put(sig)

    try:
        await asyncio.wait_for(executor.run(), timeout=0.3)
    except (asyncio.TimeoutError, Exception):
        pass

    rec: SignalRecord = telem_q.get_nowait()
    assert rec.gate_passed == "GATE_1_FAIL"


@pytest.mark.asyncio
async def test_telemetry_emitted_on_every_rejection():
    """Every gate rejection must produce exactly one SignalRecord in telemetry_queue."""
    micro_q  = asyncio.Queue()
    signal_q = asyncio.Queue()
    telem_q  = asyncio.Queue()

    shared = SharedState()
    shared.lob_status       = "UNINITIALISED"
    shared.heartbeat_status = "HEALTHY"

    budget   = DailyBudget.from_equity(10_000.0)
    executor = StrategyExecutor(micro_q, signal_q, telem_q, FeatureComputer(), shared, budget)

    n_signals = 3
    for _ in range(n_signals):
        await micro_q.put(_micro_signal())

    try:
        await asyncio.wait_for(executor.run(), timeout=0.5)
    except (asyncio.TimeoutError, Exception):
        pass

    assert telem_q.qsize() == n_signals
    while not telem_q.empty():
        rec = telem_q.get_nowait()
        assert rec.gate_passed.startswith("GATE_")


# ── Killswitch integration tests ──────────────────────────────────────────────

def test_killswitch_fires_on_budget_breach():
    """KS-1: killswitch fires when total loss exceeds hard limit."""
    ks = GlobalKillswitch(dov=10_000.0, hard_limit_pct=0.01)
    assert not ks.is_active

    fired = ks.check_budget(realised_pnl=-200.0, unrealised_pnl=0.0)
    assert fired
    assert ks.is_active
    assert ks.state.trigger == "KS-1_BUDGET"


def test_killswitch_fires_on_critical_heartbeat():
    """KS-2: killswitch fires on CRITICAL heartbeat status."""
    ks = GlobalKillswitch(dov=10_000.0)
    fired = ks.check_heartbeat("CRITICAL", delta_ms=600.0)
    assert fired
    assert ks.is_active
    assert "KS-2" in ks.state.trigger


def test_killswitch_fires_on_slippage_decay():
    """KS-3: fires when rolling average slippage exceeds threshold."""
    ks = GlobalKillswitch(dov=10_000.0)
    # Fill 20 trades with 5 bps slippage (threshold is SLIPPAGE_RESEARCH_BPS × MULTIPLIER)
    for _ in range(20):
        fired = ks.record_slippage(30_000.0, 30_015.0, "LONG")  # ~5 bps
    assert fired
    assert ks.is_active


def test_killswitch_permanent_after_fire():
    """Once fired, killswitch cannot be reset (Rule 12)."""
    ks = GlobalKillswitch(dov=10_000.0)
    ks.check_budget(realised_pnl=-500.0, unrealised_pnl=0.0)
    assert ks.is_active

    # Subsequent checks always return True — no way to un-fire without restart
    assert ks.check_budget(realised_pnl=0.0, unrealised_pnl=0.0)
    assert ks.check_heartbeat("HEALTHY", delta_ms=10.0)
    assert ks.is_active


@pytest.mark.asyncio
async def test_killswitch_blocks_new_orders_via_monitor():
    """killswitch_monitor sets accepting_new_signals=False when KS-1 fires."""
    from main import _killswitch_monitor

    shared       = SharedState()
    ks           = GlobalKillswitch(dov=10_000.0, hard_limit_pct=0.01)
    order_q      = asyncio.Queue()
    portfolio    = PortfolioState(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    portfolio.daily_pnl = -200.0   # exceeds 1% hard limit
    order_manager = OrderManager(order_q, portfolio)
    order_manager.accepting_new_signals = True

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Run monitor with fast interval; it should detect budget breach and stop signals
        try:
            await asyncio.wait_for(
                _killswitch_monitor(ks, portfolio, shared, order_manager, db_path, interval=0.05),
                timeout=0.3,
            )
        except asyncio.TimeoutError:
            pass

        assert not order_manager.accepting_new_signals
        assert ks.is_active
    finally:
        os.unlink(db_path)


@pytest.mark.asyncio
async def test_fill_handler_triggers_ks3_on_high_slippage():
    """_fill_handler fires KS-3 slippage killswitch after 20 high-slippage fills."""
    from main import _fill_handler

    fill_q       = asyncio.Queue()
    budget       = DailyBudget.from_equity(10_000.0)
    ks           = GlobalKillswitch(dov=10_000.0)
    order_q      = asyncio.Queue()
    portfolio    = PortfolioState(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)
    order_manager = OrderManager(order_q, portfolio)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Feed 20 fills with high slippage (~5 bps LONG)
        for _ in range(20):
            await fill_q.put({
                "side":        "BUY",
                "qty":         0.001,
                "fill_price":  30_015.0,
                "entry_price": 30_000.0,
            })

        try:
            await asyncio.wait_for(
                _fill_handler(fill_q, budget, ks, order_manager, portfolio, db_path),
                timeout=0.5,
            )
        except asyncio.TimeoutError:
            pass

        assert ks.is_active
        assert not order_manager.accepting_new_signals
    finally:
        os.unlink(db_path)


@pytest.mark.asyncio
async def test_signal_telemetry_writes_to_db():
    """SignalTelemetry flushes records to registry.db on demand."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    telem_q    = asyncio.Queue()
    telemetry  = SignalTelemetry(telem_q, db_path=db_path)

    rec = SignalRecord(gate_passed="GATE_0_FAIL", rejection_reason="test")
    await telem_q.put(rec)

    # Run telemetry long enough to trigger a flush
    try:
        await asyncio.wait_for(telemetry.run(), timeout=0.5)
    except asyncio.TimeoutError:
        pass

    # Force flush whatever is left
    await telemetry._flush()

    try:
        conn   = sqlite3.connect(db_path)
        rows   = conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()[0]
        conn.close()
        assert rows >= 1
    finally:
        os.unlink(db_path)
