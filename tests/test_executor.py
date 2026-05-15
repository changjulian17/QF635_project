"""Tests for the 7-gate StrategyExecutor in strategy/executor.py."""
import asyncio
import time

import pytest

from strategy.executor import (
    RuleBasedScorer,
    StrategyExecutor,
    gate_0_data_fidelity,
    gate_1_microstructure,
    gate_2_confidence,
    gate_3_capital,
    gate_4_order_selection,
    gate_5_execution_sync,
)
from models import FeatureVector, MicroSignal, SharedState, WallState


# ── Helpers ───────────────────────────────────────────────────────────────────

def _wall(side: str = "ask") -> WallState:
    now = int(time.time() * 1000)
    return WallState(
        price=30000.0, qty_initial=50.0, qty_current=5.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side=side, sigma=3.0,
    )


def _signal(direction: str = "LONG", age_ms: int = 10) -> MicroSignal:
    return MicroSignal(
        signal_type    = "SWEEP_WITH_PROTECTION",
        direction      = direction,
        timestamp_ms   = int(time.time() * 1000) - age_ms,
        consumed_wall  = _wall("ask"),
        protection_wall = _wall("bid"),
    )


def _fv(**kwargs) -> FeatureVector:
    defaults = dict(
        lob_status="SYNCED", obi_zscore=0.5, cvd_delta=1.0,
        vol_ratio=2.0, spread_bps=3.0, cvd_positive=1,
        rsi_value=55.0,
    )
    defaults.update(kwargs)
    return FeatureVector(**defaults)


# ── Gate 0 ────────────────────────────────────────────────────────────────────

def test_gate0_fails_on_stale_lob():
    ok, reason = gate_0_data_fidelity("GAP_DETECTED", "HEALTHY")
    assert ok is False
    assert "GAP_DETECTED" in reason


def test_gate0_fails_on_heartbeat_critical():
    ok, reason = gate_0_data_fidelity("SYNCED", "CRITICAL")
    assert ok is False
    assert "CRITICAL" in reason


def test_gate0_passes_when_healthy():
    ok, _ = gate_0_data_fidelity("SYNCED", "HEALTHY")
    assert ok is True


def test_gate0_passes_degraded_heartbeat():
    ok, _ = gate_0_data_fidelity("SYNCED", "DEGRADED")
    assert ok is True


# ── Gate 1 ────────────────────────────────────────────────────────────────────

def test_gate1_fails_without_sweep():
    sig = MicroSignal(signal_type="UNKNOWN", direction="LONG", timestamp_ms=0)
    ok, reason = gate_1_microstructure(sig, False)
    assert ok is False


def test_gate1_fails_missing_consumed_wall():
    sig = MicroSignal(
        signal_type="SWEEP_WITH_PROTECTION", direction="LONG",
        timestamp_ms=0, consumed_wall=None, protection_wall=_wall("bid"),
    )
    ok, reason = gate_1_microstructure(sig, False)
    assert ok is False


def test_gate1_passes_valid_signal():
    ok, _ = gate_1_microstructure(_signal(), False)
    assert ok is True


# ── Gate 2 ────────────────────────────────────────────────────────────────────

def test_gate2_fails_below_confidence():
    scorer = RuleBasedScorer()
    fv = _fv(obi_zscore=-5.0, cvd_positive=0, vol_ratio=0.1, spread_bps=20.0)
    ok, reason, score = gate_2_confidence(fv, _signal("LONG"), scorer)
    assert ok is False
    assert score < 0.58


def test_gate2_passes_above_confidence():
    scorer = RuleBasedScorer()
    fv = _fv(obi_zscore=1.0, cvd_positive=1, vol_ratio=3.0, spread_bps=3.0)
    ok, reason, score = gate_2_confidence(fv, _signal("LONG"), scorer)
    assert ok is True
    assert score >= 0.58


# ── Gate 3 ────────────────────────────────────────────────────────────────────

def test_gate3_fails_when_halted():
    ok, reason = gate_3_capital(None, "HALTED")
    assert ok is False


def test_gate3_passes_full_tier():
    ok, _ = gate_3_capital(None, "FULL")
    assert ok is True


# ── Gate 4 ────────────────────────────────────────────────────────────────────

def test_gate4_fails_on_wide_spread():
    ok, reason, _ = gate_4_order_selection(_signal(), spread_bps=50.0, spread_p95=5.0)
    assert ok is False
    assert "spread" in reason.lower()


def test_gate4_passes_normal_spread():
    ok, _, order_type = gate_4_order_selection(_signal(), spread_bps=3.0, spread_p95=5.0)
    assert ok is True
    assert order_type == "IOC_LIMIT"


# ── Gate 5 ────────────────────────────────────────────────────────────────────

def test_gate5_fails_on_stale_signal():
    ok, reason = gate_5_execution_sync(
        signal_timestamp_ms=int(time.time() * 1000) - 5000,
        last_delta_ms=10.0,
    )
    assert ok is False
    assert "stale" in reason.lower()


def test_gate5_fails_on_high_latency():
    ok, reason = gate_5_execution_sync(
        signal_timestamp_ms=int(time.time() * 1000),
        last_delta_ms=600.0,
    )
    assert ok is False


def test_gate5_passes_fresh_signal():
    ok, _ = gate_5_execution_sync(
        signal_timestamp_ms=int(time.time() * 1000) - 10,
        last_delta_ms=50.0,
    )
    assert ok is True


# ── RuleBasedScorer ───────────────────────────────────────────────────────────

def test_scorer_max_score_long():
    scorer = RuleBasedScorer()
    fv  = _fv(obi_zscore=1.0, vol_ratio=4.0, spread_bps=3.0, cvd_positive=1)
    sig = _signal("LONG")
    score = scorer.score(fv, sig)
    assert score == pytest.approx(1.0)


def test_scorer_zero_score_misaligned():
    scorer = RuleBasedScorer()
    fv  = _fv(obi_zscore=-5.0, vol_ratio=0.0, spread_bps=20.0, cvd_positive=0)
    sig = _signal("LONG")
    score = scorer.score(fv, sig)
    assert score == pytest.approx(0.0)


# ── Full executor integration ─────────────────────────────────────────────────

class _MockFC:
    """Feature computer stub that always returns a passing FeatureVector."""
    def compute(self, cvd_calculator, shared_state, **kwargs):
        return _fv(obi_zscore=1.0, cvd_positive=1, vol_ratio=3.0, spread_bps=3.0)


def test_all_gates_pass_approved_telemetry():
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
        )
        await micro_q.put(_signal())
        await ex._evaluate(await micro_q.get())

        assert not signal_q.empty()
        assert not telem_q.empty()
        rec = await telem_q.get()
        assert rec.gate_passed == "APPROVED"

    asyncio.run(_run())


def test_telemetry_emitted_at_every_rejection():
    """Gate 0 failure must still emit a telemetry record."""
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="GAP_DETECTED", heartbeat_status="HEALTHY")

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
        )
        await ex._evaluate(_signal())

        assert signal_q.empty()           # not approved
        assert not telem_q.empty()        # but telemetry emitted
        rec = await telem_q.get()
        assert rec.gate_passed == "GATE_0_FAIL"

    asyncio.run(_run())
