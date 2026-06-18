"""Tests for the 7-gate StrategyExecutor in strategy/executor.py."""
import asyncio
import time
from unittest.mock import patch

import pytest

from config import settings

from strategy.executor import (
    PersistenceMonitor,
    RuleBasedScorer,
    StrategyExecutor,
    _wall_present,
    gate_0_data_fidelity,
    gate_1_microstructure,
    gate_2_confidence,
    gate_3_capital,
    gate_3_position_size,
    gate_4_order_selection,
    gate_5_execution_sync,
)
from models import FeatureVector, MicroOrderRequest, MicroSignal, SharedState, WallState


# ── Helpers ───────────────────────────────────────────────────────────────────

def _wall(side: str = "ask") -> WallState:
    now = int(time.time() * 1000)
    return WallState(
        price=30000.0, qty_initial=50.0, qty_current=5.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side=side, sigma=3.0,
    )


def _signal(direction: str = "LONG", age_ms: int = 10, prior_absorption: bool = True) -> MicroSignal:
    return MicroSignal(
        signal_type      = "SWEEP_WITH_PROTECTION",
        direction        = direction,
        timestamp_ms     = int(time.time() * 1000) - age_ms,
        consumed_wall    = _wall("ask"),
        protection_wall  = _wall("bid"),
        prior_absorption = prior_absorption,
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


def test_gate0_fails_on_lob_heartbeat_critical():
    ok, reason = gate_0_data_fidelity("SYNCED", "HEALTHY", "CRITICAL")
    assert ok is False
    assert "LOB heartbeat" in reason


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
    ok, _ = gate_1_microstructure(_signal(), True)
    assert ok is True


def test_gate1_fails_no_absorption():
    ok, reason = gate_1_microstructure(_signal(), False)
    assert ok is False
    assert "absorption" in reason.lower()


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
    ok, reason, _ = gate_4_order_selection(spread_bps=50.0, spread_p95=5.0)
    assert ok is False
    assert "spread" in reason.lower()


def test_gate4_passes_normal_spread():
    ok, _, order_type = gate_4_order_selection(spread_bps=3.0, spread_p95=5.0)
    assert ok is True
    assert order_type == "IOC_LIMIT"


# ── Wall presence helper ──────────────────────────────────────────────────────

def test_wall_present_exact_match():
    assert _wall_present(30000.0, [{"price": 30000.0}])


def test_wall_present_float_drift():
    assert _wall_present(30000.0, [{"price": 30000.005}])   # within 1 cent tol


def test_wall_present_outside_tolerance():
    assert not _wall_present(30000.0, [{"price": 30001.0}])


def test_wall_present_empty_book():
    assert not _wall_present(30000.0, [])


# ── Gate 5 ────────────────────────────────────────────────────────────────────

def test_gate5_fails_on_stale_signal():
    ok, reason = gate_5_execution_sync(
        signal_timestamp_ms=int(time.time() * 1000) - 5000,
        last_delta_ms=10.0,
    )
    assert ok is False
    assert "stale" in reason.lower()


def test_gate5_fails_on_high_latency(monkeypatch):
    from config import settings as s
    monkeypatch.setattr(s, "HEARTBEAT_CRITICAL_MS", 500)
    ok, reason = gate_5_execution_sync(
        signal_timestamp_ms=int(time.time() * 1000),
        last_delta_ms=600.0,
    )
    assert ok is False
    assert "latency" in reason.lower()


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


def test_scorer_ignores_cvd_alignment():
    scorer = RuleBasedScorer()
    sig = _signal("LONG")
    aligned = _fv(obi_zscore=1.0, vol_ratio=2.0, spread_bps=3.0, cvd_positive=1)
    misaligned = _fv(obi_zscore=1.0, vol_ratio=2.0, spread_bps=3.0, cvd_positive=0)
    assert scorer.score(aligned, sig) == scorer.score(misaligned, sig)


def test_scorer_zero_score_misaligned():
    scorer = RuleBasedScorer()
    fv  = _fv(obi_zscore=-5.0, vol_ratio=0.0, spread_bps=20.0, cvd_positive=0)
    sig = _signal("LONG")
    score = scorer.score(fv, sig)
    assert score == pytest.approx(0.0)


# ── Full executor integration ─────────────────────────────────────────────────

class _MockFC:
    """Feature computer stub that always returns a passing FeatureVector."""
    def __init__(self, spread_bps: float = 3.0) -> None:
        self._spread_bps = spread_bps

    def compute(self, cvd_calculator, shared_state, **kwargs):
        return _fv(obi_zscore=1.0, cvd_positive=1, vol_ratio=3.0, spread_bps=self._spread_bps)


def test_approved_order_request_contract():
    """MicroOrderRequest must carry the full executable contract after approval."""
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
        await ex._evaluate(_signal())
        await asyncio.sleep(0)  # let gate6_watch task run and self-terminate

        req: MicroOrderRequest = await signal_q.get()
        assert req.order_type == "IOC_LIMIT"
        assert req.side == "BUY"
        assert req.limit_price is None              # execution layer resolves via LOB
        assert req.notional_hint > 0.0              # Kelly × risk_pct × confidence
        assert isinstance(req.fill_event, asyncio.Event)
        assert not req.fill_event.is_set()          # unsignalled until execution layer fills
        assert req.signal_id != ""

    asyncio.run(_run())


def test_gate6_watch_starts_monitor_after_fill():
    """Gate 6 persistence monitor must not start until fill_event is set."""
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        class _MockLOB:
            async def get_current_walls(self, sigma=2.5):
                return []   # wall absent immediately → monitor exits on first check

        class _MockOM:
            received = False
            async def handle_protection_wall_removed(self, side):
                _MockOM.received = True

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
            lob_engine=_MockLOB(),
            order_manager=_MockOM(),
            gate6_check_interval_ms=5,   # fast polling for test
        )
        await ex._evaluate(_signal())
        req: MicroOrderRequest = await signal_q.get()

        assert ex._gate6_tasks, "watch task should be tracked before fill"
        req.fill_event.set()                       # simulate execution layer confirming fill
        await asyncio.sleep(0.05)                  # 50ms >> 5ms check interval

        # gate6_tasks transitions: watch exits → monitor starts → wall absent → monitor exits
        # after sleep both should have cleaned up (wall returns empty list immediately)
        assert _MockOM.received, "order_manager should have been notified of wall removal"

    asyncio.run(_run())


def test_evaluate_cancels_prior_gate6_task_before_starting_new():
    """A stale Gate6 task from a prior signal must be cancelled before a new one starts —
    there must never be a window with two live monitors for different signals."""
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        class _MockLOB:
            async def get_current_walls(self, sigma=2.5):
                return [{"price": 30000.0}]   # wall present — stale task would run forever

        class _MockOM:
            async def handle_protection_wall_removed(self, side):
                pass

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
            lob_engine=_MockLOB(),
            order_manager=_MockOM(),
            gate6_check_interval_ms=5,
        )

        stale = asyncio.create_task(asyncio.sleep(10))
        ex._gate6_tasks.add(stale)

        await ex._evaluate(_signal())

        assert stale.cancelled(), "stale Gate6 task must be cancelled before a new one starts"
        assert stale not in ex._gate6_tasks, "stale task must be removed from the tracking set"

    asyncio.run(_run())


def test_gate6_monitor_stops_on_position_closed():
    """Monitor must exit cleanly without firing the wall alert when position closes normally."""
    async def _run():
        class _MockLOB:
            async def get_current_walls(self, sigma=2.5):
                return [{"price": 30000.0}]   # wall always present

        class _MockOM:
            alerted = False
            async def handle_protection_wall_removed(self, side):
                _MockOM.alerted = True

        closed_event = asyncio.Event()
        task = asyncio.create_task(
            PersistenceMonitor().monitor(
                protection_wall_price=30000.0,
                position_side="LONG",
                lob_engine=_MockLOB(),
                order_manager=_MockOM(),
                position_closed_event=closed_event,
                check_interval_ms=5,
            )
        )
        closed_event.set()                  # position exits via TP/SL
        await asyncio.sleep(0.05)

        assert task.done(), "monitor should have stopped after position closed"
        assert not _MockOM.alerted, "should not alert order_manager on normal position exit"

    asyncio.run(_run())


def test_gate6_requires_consecutive_absence():
    """A single wall-absent check must not fire — only GATE6_WALL_ABSENT_CONSEC consecutive misses."""
    async def _run():
        calls = {"n": 0}

        class _MockLOB:
            async def get_current_walls(self, sigma=2.5):
                calls["n"] += 1
                # absent on the first check, present on every subsequent check —
                # a flicker that must not accumulate toward the threshold.
                return [] if calls["n"] == 1 else [{"price": 30000.0}]

        class _MockOM:
            alerted = False
            async def handle_protection_wall_removed(self, side):
                _MockOM.alerted = True

        closed_event = asyncio.Event()
        task = asyncio.create_task(
            PersistenceMonitor().monitor(
                protection_wall_price=30000.0,
                position_side="LONG",
                lob_engine=_MockLOB(),
                order_manager=_MockOM(),
                position_closed_event=closed_event,
                check_interval_ms=5,
            )
        )
        await asyncio.sleep(0.05)   # several polling cycles
        closed_event.set()
        await asyncio.sleep(0.02)

        assert not _MockOM.alerted, "single absent check must not trigger wall-removed alert"
        task.cancel()

    asyncio.run(_run())


def test_gate6_fires_after_consecutive_absence():
    """GATE6_WALL_ABSENT_CONSEC consecutive misses must fire the alert exactly once."""
    async def _run():
        class _MockLOB:
            async def get_current_walls(self, sigma=2.5):
                return []   # always absent

        class _MockOM:
            alert_count = 0
            async def handle_protection_wall_removed(self, side):
                _MockOM.alert_count += 1

        closed_event = asyncio.Event()
        await PersistenceMonitor().monitor(
            protection_wall_price=30000.0,
            position_side="LONG",
            lob_engine=_MockLOB(),
            order_manager=_MockOM(),
            position_closed_event=closed_event,
            check_interval_ms=5,
        )

        assert _MockOM.alert_count == 1, (
            f"expected exactly one alert after {settings.GATE6_WALL_ABSENT_CONSEC} "
            f"consecutive misses, got {_MockOM.alert_count}"
        )

    asyncio.run(_run())


def test_spread_p95_log_space():
    """Log-space p95 must exceed the linear-space approximation for right-skewed data."""
    import math as _math

    state = SharedState()
    ex = StrategyExecutor(
        micro_signal_queue=asyncio.Queue(),
        signal_queue=asyncio.Queue(),
        telemetry_queue=asyncio.Queue(),
        feature_computer=_MockFC(),
        shared_state=state,
    )
    # Feed 15 samples with a strong right tail (spike to 80 bps)
    samples = [2.0, 2.1, 2.3, 2.0, 2.5, 2.2, 2.4, 1.9, 2.1, 2.3, 3.0, 5.0, 80.0, 60.0, 40.0]
    for s in samples:
        ex._log_spread_stats.update(_math.log(max(s, 1e-4)))

    p95 = ex._spread_p95
    linear_mean = sum(samples) / len(samples)
    # Log-space p95 should capture the right tail; must exceed the linear mean
    assert p95 > linear_mean, (
        f"log-space p95={p95:.2f} should exceed linear mean={linear_mean:.2f} "
        "for right-skewed data"
    )
    # And must be less than the max outlier (not exploding)
    assert p95 < max(samples) * 2


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


# ── New gap-fill tests ────────────────────────────────────────────────────────

def test_gate3_fails_when_passive():
    ok, reason = gate_3_capital(None, "PASSIVE")
    assert ok is False
    assert "PASSIVE" in reason


def test_gate3_fails_on_active_exposure():
    ok, reason = gate_3_capital(None, "FULL", active_exposure=True)
    assert ok is False
    assert "active microstructure exposure" in reason


def test_gate2_tier_min_confidence_minimal():
    """MINIMAL tier must require confidence >= 0.80; 0.65 passes Gate 2 base but should be rejected."""
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=10.0)

        class _LowScoreFC:
            def compute(self, cvd_calculator, shared_state, **kwargs):
                # Score lands above FULL min but below MINIMAL after CVD removal.
                return _fv(obi_zscore=0.3, vol_ratio=1.4, spread_bps=3.0, cvd_positive=0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_LowScoreFC(),
            shared_state=state,
        )
        ex.set_risk_tier("MINIMAL")
        await ex._evaluate(_signal())

        assert signal_q.empty(), "signal should not be approved below MINIMAL min confidence"
        rec = telem_q.get_nowait()
        assert rec.gate_passed == "GATE_2_FAIL"
        assert "MINIMAL" in rec.rejection_reason

    asyncio.run(_run())


def test_notional_hint_halved_in_reduced_tier():
    """notional_hint must be 50% of FULL-tier for identical signal in REDUCED tier."""
    async def _run():
        state = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=10.0)

        async def _approve(tier: str) -> float:
            micro_q  = asyncio.Queue()
            signal_q = asyncio.Queue()
            telem_q  = asyncio.Queue()
            ex = StrategyExecutor(
                micro_signal_queue=micro_q,
                signal_queue=signal_q,
                telemetry_queue=telem_q,
                feature_computer=_MockFC(),
                shared_state=state,
            )
            ex.set_risk_tier(tier)
            await ex._evaluate(_signal())
            if signal_q.empty():
                return 0.0
            req = signal_q.get_nowait()
            return req.notional_hint

        hint_full    = await _approve("FULL")
        hint_reduced = await _approve("REDUCED")

        assert hint_full > 0
        assert abs(hint_reduced - hint_full * 0.5) < 1e-9, (
            f"REDUCED hint {hint_reduced} should be 50% of FULL hint {hint_full}"
        )

    asyncio.run(_run())


def test_rate_limit_rejects_rapid_second_signal(monkeypatch):
    """A second signal within MIN_SIGNAL_INTERVAL_MS must be rejected as RATE_LIMIT."""
    from config import settings as _settings
    monkeypatch.setattr(_settings, "MIN_SIGNAL_INTERVAL_MS", 500)

    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=10.0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
        )
        # First signal — should be approved and set _last_approved_ms
        await ex._evaluate(_signal())
        assert not signal_q.empty(), "first signal should be approved"
        signal_q.get_nowait()

        # Second signal immediately — must be rate-limited
        await ex._evaluate(_signal())
        assert signal_q.empty(), "second rapid signal should be rate-limited"

        recs = []
        while not telem_q.empty():
            recs.append(telem_q.get_nowait())
        rejections = [r for r in recs if r.gate_passed == "RATE_LIMIT" and "rate limit" in (r.rejection_reason or "")]
        assert rejections, "rate-limit rejection telemetry must be emitted"

    asyncio.run(_run())


# ── Fix 1: strategy_id propagation ───────────────────────────────────────────

def test_strategy_id_propagated_to_telemetry():
    """Executor must stamp every SignalRecord with the injected strategy_id."""
    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="UNINITIALISED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
            strategy_id="test-uuid-abc123",
        )
        await ex._evaluate(_signal())
        rec = telem_q.get_nowait()
        assert rec.strategy_id == "test-uuid-abc123"

    asyncio.run(_run())


def test_strategy_id_default_fallback():
    """Default strategy_id must be 'v3.0' when none is provided."""
    async def _run():
        telem_q = asyncio.Queue()
        state   = SharedState(lob_status="UNINITIALISED", heartbeat_status="HEALTHY", last_delta_ms=20.0)
        ex = StrategyExecutor(
            micro_signal_queue=asyncio.Queue(),
            signal_queue=asyncio.Queue(),
            telemetry_queue=telem_q,
            feature_computer=_MockFC(),
            shared_state=state,
        )
        await ex._evaluate(_signal())
        rec = telem_q.get_nowait()
        assert rec.strategy_id == "v3.0"

    asyncio.run(_run())


# ── Fix 2: EntryRules applied to Gate 4 ──────────────────────────────────────

def test_gate4_hard_cap_blocks_wide_spread():
    """spread_max_bps from EntryRules must block spreads exceeding the hard cap."""
    from strategy.spec import EntryRules

    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(spread_bps=4.0),
            shared_state=state,
        )
        ex.set_entry_rules(EntryRules(spread_max_bps=3.0))
        await ex._evaluate(_signal())
        assert signal_q.empty(), "signal must be rejected when spread exceeds hard cap"
        recs = []
        while not telem_q.empty():
            recs.append(telem_q.get_nowait())
        gate4_fails = [r for r in recs if r.gate_passed == "GATE_4_FAIL"]
        assert gate4_fails, "GATE_4_FAIL telemetry must be emitted"
        assert "hard cap" in gate4_fails[-1].rejection_reason

    asyncio.run(_run())


def test_gate4_adaptive_p95_still_applies_with_permissive_hard_cap():
    """Adaptive p95 filter must reject signals independently of the hard cap."""
    import math
    from strategy.spec import EntryRules

    async def _run():
        micro_q  = asyncio.Queue()
        signal_q = asyncio.Queue()
        telem_q  = asyncio.Queue()
        state    = SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0)

        ex = StrategyExecutor(
            micro_signal_queue=micro_q,
            signal_queue=signal_q,
            telemetry_queue=telem_q,
            feature_computer=_MockFC(spread_bps=12.0),
            shared_state=state,
        )
        ex.set_entry_rules(EntryRules(spread_max_bps=100.0))  # hard cap disabled
        # Seed spread stats so p95 ≈ 5 bps; a 12 bps spread should then fail
        for _ in range(15):
            ex._log_spread_stats.update(math.log(5.0))
        await ex._evaluate(_signal())
        assert signal_q.empty(), "signal must be rejected when spread exceeds 2×p95"
        recs = []
        while not telem_q.empty():
            recs.append(telem_q.get_nowait())
        gate4_fails = [r for r in recs if r.gate_passed == "GATE_4_FAIL"]
        assert gate4_fails
        assert "p95" in gate4_fails[-1].rejection_reason

    asyncio.run(_run())


def test_set_entry_rules_updates_rule_based_scorer():
    """set_entry_rules must propagate obi_threshold into RuleBasedScorer."""
    from strategy.spec import EntryRules
    from strategy.executor import RuleBasedScorer

    ex = StrategyExecutor(
        micro_signal_queue=asyncio.Queue(),
        signal_queue=asyncio.Queue(),
        telemetry_queue=asyncio.Queue(),
        feature_computer=_MockFC(),
        shared_state=SharedState(lob_status="SYNCED", heartbeat_status="HEALTHY", last_delta_ms=20.0),
        rule_scorer=RuleBasedScorer(),
    )
    ex.set_entry_rules(EntryRules(obi_threshold=0.99))
    assert isinstance(ex._scorer, RuleBasedScorer)
    assert ex._scorer._rules.obi_threshold == pytest.approx(0.99)
    # A signal with obi_zscore=0.5 must now score 0 on OBI component
    fv  = _fv(obi_zscore=0.5)
    sig = _signal("LONG")
    score = ex._scorer.score(fv, sig)
    assert score < 0.45, "OBI component must be 0 when zscore < new threshold"


# ── Fix 3: gate_3_position_size helpers and tests ─────────────────────────────

def _wall_at(price: float, side: str) -> WallState:
    """WallState at an explicit price (unlike _wall() which hardcodes $30k)."""
    now = int(time.time() * 1000)
    return WallState(
        price=price, qty_initial=50.0, qty_current=5.0,
        first_seen_ts=now - 600, last_seen_ts=now,
        side=side, sigma=3.0,
    )


def _signal_with_mid(mid: float, wall_distance_bps: float, direction: str = "LONG") -> MicroSignal:
    """Signal with explicit mid_price so gate_3_position_size check is not skipped."""
    pw_price = (
        mid * (1 - wall_distance_bps / 10_000) if direction == "LONG"
        else mid * (1 + wall_distance_bps / 10_000)
    )
    return MicroSignal(
        signal_type="SWEEP_WITH_PROTECTION",
        direction=direction,
        timestamp_ms=int(time.time() * 1000) - 10,
        consumed_wall=_wall_at(mid * 1.01, "ask"),
        protection_wall=_wall_at(pw_price, "bid" if direction == "LONG" else "ask"),
        prior_absorption=True,
        mid_price=mid,
    )


def test_gate_3_position_size_unit_passes():
    ok, _ = gate_3_position_size(8_000.0, 10_000.0, 0.90)
    assert ok is True


def test_gate_3_position_size_unit_rejects():
    ok, reason = gate_3_position_size(10_000.0, 10_000.0, 0.90)
    assert ok is False
    assert "est. notional" in reason


def test_gate_3_position_size_rejects_in_executor():
    """1 bps wall → est_notional ~$22,800 >> $9k (90% of $10k) → GATE_3_FAIL."""
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
            equity_fn=lambda: 10_000.0,
        )
        await ex._evaluate(_signal_with_mid(mid=73_628.0, wall_distance_bps=1))

        assert signal_q.empty(), "oversized signal must not reach order queue"
        rec = await telem_q.get()
        assert rec.gate_passed == "GATE_3_FAIL"
        assert "est. notional" in (rec.rejection_reason or "")

    asyncio.run(_run())


def test_gate_3_position_size_passes_in_executor():
    """25 bps wall → est_notional ~$912 << $9k (90% of $10k) → APPROVED.
    notional_hint=0.000228 (confidence=0.912 × KELLY=0.25 × RISK_PCT=0.001),
    so est_notional = equity × 0.000228 × 10_000/25 = $912 < $9,000."""
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
            equity_fn=lambda: 10_000.0,
        )
        await ex._evaluate(_signal_with_mid(mid=73_628.0, wall_distance_bps=25))
        await asyncio.sleep(0)  # let gate6_watch task self-terminate

        assert not signal_q.empty(), "signal within size limit must reach order queue"
        rec = await telem_q.get()
        assert rec.gate_passed == "APPROVED"

    asyncio.run(_run())


def test_gate_3_position_size_skipped_when_no_equity_fn():
    """Without equity_fn the position-size check is skipped and signal is approved."""
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
            # equity_fn intentionally omitted
        )
        # Even a very tight wall passes when no equity_fn is wired
        await ex._evaluate(_signal_with_mid(mid=73_628.0, wall_distance_bps=1))
        await asyncio.sleep(0)

        assert not signal_q.empty(), "gate must be skipped when equity_fn is None"

    asyncio.run(_run())


@pytest.mark.asyncio
async def test_evaluate_exception_does_not_crash_run_loop():
    """Unhandled exception in _evaluate is caught — run() continues processing next signal."""
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

    call_count = 0

    async def _patched_evaluate(signal):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated gate error")

    ex._evaluate = _patched_evaluate  # type: ignore[method-assign]

    sig = _signal()
    await micro_q.put(sig)
    await micro_q.put(sig)

    task = asyncio.create_task(ex.run())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert call_count == 2, "run() must process second signal after first raises"
