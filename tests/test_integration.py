"""
import pytest
Integration tests — Phase 1N: GlobalKillswitch + 7-gate funnel.

Run with:  source .venv/bin/activate && python -m pytest tests/test_integration.py -v
"""
import asyncio
import sqlite3
import time
from unittest.mock import MagicMock, patch

import pytest

from config import settings
from core.signal_telemetry import SignalTelemetry
from execution.order_manager import OrderManager
from models import (
    FeatureVector, MicroOrderRequest, MicroSignal, SharedState, WallState,
)
from risk.budget import DailyBudget
from risk.killswitch import GlobalKillswitch
from strategy.executor import StrategyExecutor


# ── Shared helpers ────────────────────────────────────────────────────────────

def _wall(price: float = 94_000.0, side: str = "bid") -> WallState:
    now = int(time.time() * 1000)
    return WallState(
        price=price, qty_initial=100.0, qty_current=100.0,
        first_seen_ts=now - 600, last_seen_ts=now, side=side, sigma=3.0,
    )


def _make_req(direction: str = "LONG") -> MicroOrderRequest:
    sig = MicroSignal(
        signal_type="SWEEP_WITH_PROTECTION",
        direction=direction,
        timestamp_ms=int(time.time() * 1000),
        consumed_wall=_wall(95_000.0, "ask"),
        protection_wall=_wall(94_000.0 if direction == "LONG" else 96_000.0),
        prior_absorption=True,
    )
    return MicroOrderRequest(
        micro_signal=sig,
        signal_id="test-ks-signal-id-1234",
        order_type="IOC_LIMIT",
        side="BUY" if direction == "LONG" else "SELL",
        limit_price=None,
        ioc_timeout_ms=200,
        confidence=0.70,
        notional_hint=0.001,
    )


# ── Test 1: Gate funnel produces signal_records ───────────────────────────────

def test_gate_funnel_produces_signal_records(tmp_path):
    """
    Executor + SignalTelemetry running concurrently write GATE_0_FAIL,
    GATE_1_FAIL, and APPROVED rows into the signal_records DB table.
    """

    async def _run():
        micro_q = asyncio.Queue()
        om_q    = asyncio.Queue()
        tel_q   = asyncio.Queue(maxsize=500)

        state = SharedState(
            heartbeat_status="HEALTHY",
            lob_status="SYNCED",
            last_delta_ms=0.0,
        )
        budget    = DailyBudget.from_equity(10_000.0)
        telemetry = SignalTelemetry(tel_q, db_path=str(tmp_path / "test.db"))

        fv_normal = FeatureVector(
            lob_status="SYNCED",
            obi_zscore=1.0,
            cvd_delta=1.0,
            vol_ratio=3.0,
            spread_bps=3.0,
            cvd_positive=1,
            rsi_value=55.0,
        )
        mock_fc = MagicMock()
        # Signals 1+2 never reach compute (gate 0/1 reject first).
        # Signal 3: None → GATE_2_FAIL ("feature vector not ready").
        # Signal 4: fv_normal → passes all gates → APPROVED.
        mock_fc.compute.side_effect = [None, fv_normal]

        executor = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=om_q,
            telemetry_queue=tel_q,
            feature_computer=mock_fc,
            shared_state=state,
            budget=budget,
        )

        exec_task  = asyncio.create_task(executor.run())
        telem_task = asyncio.create_task(telemetry.run())
        # Yield so both tasks enter their receive loops before we send signals.
        await asyncio.sleep(0)

        # Signal 1 — fails Gate 0: LOB not SYNCED
        # State is set BEFORE put; executor is already waiting on queue.get().
        state.lob_status = "GAP_DETECTED"
        await micro_q.put(MicroSignal(
            signal_type="SWEEP_WITH_PROTECTION",
            direction="LONG",
            timestamp_ms=int(time.time() * 1000),
            consumed_wall=_wall(95_000.0, "ask"),
            protection_wall=_wall(94_000.0),
            prior_absorption=True,
        ))
        await asyncio.sleep(0.05)  # let executor process signal 1 before state changes

        # Signal 2 — fails Gate 1: wrong signal_type (restore LOB first)
        state.lob_status = "SYNCED"
        await micro_q.put(MicroSignal(
            signal_type="PLAIN_SWEEP",
            direction="LONG",
            timestamp_ms=int(time.time() * 1000),
            consumed_wall=_wall(95_000.0, "ask"),
            protection_wall=_wall(94_000.0),
            prior_absorption=True,
        ))
        await asyncio.sleep(0.05)  # let executor process signal 2

        # Signal 3 — fails Gate 2: feature vector not ready (compute returns None)
        await micro_q.put(MicroSignal(
            signal_type="SWEEP_WITH_PROTECTION",
            direction="LONG",
            timestamp_ms=int(time.time() * 1000),
            consumed_wall=_wall(95_000.0, "ask"),
            protection_wall=_wall(94_000.0),
            prior_absorption=True,
        ))
        await asyncio.sleep(0.05)  # let executor process signal 3

        # Signal 4 — passes all gates (APPROVED)
        await micro_q.put(MicroSignal(
            signal_type="SWEEP_WITH_PROTECTION",
            direction="LONG",
            timestamp_ms=int(time.time() * 1000),
            consumed_wall=_wall(95_000.0, "ask"),
            protection_wall=_wall(94_000.0),
            prior_absorption=True,
        ))
        await asyncio.sleep(0.1)  # let executor process signal 4 + telemetry drain

        exec_task.cancel()
        telem_task.cancel()
        # CancelledError handler in telemetry calls _flush_remaining() → commits to DB.
        for t in (exec_task, telem_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    asyncio.run(_run())

    conn = sqlite3.connect(str(tmp_path / "test.db"))
    gate_values = {
        r[0] for r in conn.execute("SELECT gate_passed FROM signal_records").fetchall()
    }
    conn.close()

    assert "GATE_0_FAIL" in gate_values, f"Missing GATE_0_FAIL — got {gate_values}"
    assert "GATE_1_FAIL" in gate_values, f"Missing GATE_1_FAIL — got {gate_values}"
    assert "GATE_2_FAIL" in gate_values, f"Missing GATE_2_FAIL — got {gate_values}"
    assert "APPROVED"    in gate_values, f"Missing APPROVED    — got {gate_values}"


# ── Test 2: Killswitch blocks orders when active ──────────────────────────────

def test_killswitch_blocks_orders_when_active():
    """
    Pre-fired killswitch causes _order_loop to discard signals;
    fill_queue stays empty (no orders placed).
    """

    async def _run():
        sig_q  = asyncio.Queue()
        fill_q = asyncio.Queue()

        ks = GlobalKillswitch(dov=10_000.0)
        ks._fire(
            trigger="KS-1_BUDGET",
            detail="test forced fire",
            latency=0.0,
            slippage=0.0,
            total_loss=-200.0,
        )
        assert ks.is_active

        om = OrderManager(
            signal_queue=sig_q,
            fill_queue=fill_q,
            killswitch=ks,
            equity_fn=lambda: 10_000.0,
        )

        await sig_q.put(_make_req("LONG"))

        with patch.object(settings, "DRY_RUN", True):
            om_task = asyncio.create_task(om._order_loop())
            await asyncio.sleep(0.05)
            om_task.cancel()
            try:
                await om_task
            except asyncio.CancelledError:
                pass

        assert fill_q.empty(), "fill_queue must be empty when killswitch is active"

    asyncio.run(_run())


# ── Test 3: tier scaling flows Executor → OrderManager → fill_processor ────────

def _run_tier(tier: str):
    """Push one APPROVED signal through the full chain at `tier`; return the
    resulting open-position qty and the fill_processor's fill-sample count."""
    from unittest.mock import AsyncMock
    from main import _process_fills
    from models import PortfolioState

    async def _run():
        micro_q, om_q, fill_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        tel_q = asyncio.Queue(maxsize=500)
        state = SharedState(heartbeat_status="HEALTHY", lob_status="SYNCED", last_delta_ms=0.0)
        budget = DailyBudget.from_equity(10_000.0)
        portfolio = PortfolioState(equity=10_000.0, starting_equity=10_000.0, peak_equity=10_000.0)

        fv = FeatureVector(
            lob_status="SYNCED", obi_zscore=1.0, cvd_delta=1.0, vol_ratio=3.0,
            spread_bps=3.0, cvd_positive=1, rsi_value=55.0,
        )
        mock_fc = MagicMock()
        mock_fc.compute.return_value = fv

        class _StubScorer:
            def score(self, _fv, _sig):
                return 0.9   # clears Gate 2 at every tier (MINIMAL needs >= 0.8)

        executor = StrategyExecutor(
            micro_signal_queue=micro_q, signal_queue=om_q, telemetry_queue=tel_q,
            feature_computer=mock_fc, shared_state=state, budget=budget,
            rule_scorer=_StubScorer(),
        )
        executor.set_risk_tier(tier)

        om = OrderManager(
            signal_queue=om_q, fill_queue=fill_q,
            killswitch=GlobalKillswitch(dov=10_000.0),
            equity_fn=lambda: 10_000.0,
            book_fn=lambda: (94_999.99, 95_000.01),
        )
        telemetry = MagicMock()
        telemetry.update_fill = AsyncMock()

        with patch.object(settings, "DRY_RUN", True):
            tasks = [
                asyncio.create_task(executor.run()),
                asyncio.create_task(om._order_loop()),
                asyncio.create_task(_process_fills(fill_q, portfolio, telemetry)),
            ]
            await asyncio.sleep(0)
            await micro_q.put(MicroSignal(
                signal_type="SWEEP_WITH_PROTECTION", direction="LONG",
                timestamp_ms=int(time.time() * 1000),
                consumed_wall=_wall(95_000.0, "ask"),
                protection_wall=_wall(94_000.0, "bid"),
                prior_absorption=True,
            ))
            await asyncio.sleep(0.15)
            qty = om._open_position_qty
            fills = portfolio.num_fill_samples
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        return qty, fills

    return asyncio.run(_run())


def test_tier_scaling_full_vs_minimal_end_to_end():
    full_qty, full_fills = _run_tier("FULL")
    min_qty,  min_fills  = _run_tier("MINIMAL")

    assert full_fills == 1, "fill_processor did not consume the FULL-tier fill"
    assert min_fills  == 1, "fill_processor did not consume the MINIMAL-tier fill"
    assert full_qty > 0 and min_qty > 0
    # MINIMAL tier scalar (0.25) vs FULL (1.0) — same signal/book → qty ratio ≈ 0.25
    # Absolute tolerance of one lot-size step accounts for floor-quantization rounding.
    from config import settings as _s
    assert abs(min_qty - 0.25 * full_qty) <= _s.QTY_STEP_SIZE
