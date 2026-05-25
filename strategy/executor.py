"""
7-Gate Strategy Executor — evaluates every MicroSignal through a sequential
gate pipeline and emits approved MicroOrderRequests with full telemetry at each exit.

Gates (master arch §5.5):
  Gate 0 — Data Fidelity     (LOB SYNCED + heartbeat not CRITICAL)
  Gate 1 — Microstructure    (confirmed Sweep+Protection+prior Absorption signal)
  Gate 2 — Confidence        (rule-based scorer >= MIN_CONFIDENCE)
  Gate 3 — Capital           (daily budget remaining)
  Gate 4 — Order Selection   (spread within session-aware p95 range)
  Gate 5 — Execution Sync    (signal not stale)
  Gate 6 — Persistence       (post-entry; protection wall still present)
"""

import asyncio
import json
import logging
import math
import time

from config import settings
from core.cvd import WelfordOnline
from core.signal_telemetry import SignalRecord
from models import FeatureVector, MicroOrderRequest, MicroSignal, SharedState
from risk.engine import TIER_MIN_CONFIDENCE, TIER_SCALARS

logger = logging.getLogger(__name__)

_IOC_STALE_MS = settings.IOC_TIMEOUT_MS   # signal older than this is rejected


def _wall_present(price: float, walls: list[dict], tol: float = 0.01) -> bool:
    """Tolerance-aware wall lookup. Avoids false absences from float repr drift."""
    return any(abs(w["price"] - price) <= tol for w in walls)


# ── Gate functions ────────────────────────────────────────────────────────────

def gate_0_data_fidelity(lob_status: str, heartbeat_status: str) -> tuple[bool, str]:
    if lob_status != "SYNCED":
        return False, f"LOB not SYNCED ({lob_status})"
    if heartbeat_status == "CRITICAL":
        return False, "heartbeat CRITICAL"
    return True, ""


def gate_1_microstructure(signal: MicroSignal, absorption_armed: bool) -> tuple[bool, str]:
    if signal.signal_type != "SWEEP_WITH_PROTECTION":
        return False, f"unexpected signal type: {signal.signal_type}"
    if signal.consumed_wall is None:
        return False, "no consumed wall"
    if signal.protection_wall is None:
        return False, "no protection wall"
    if not absorption_armed:
        return False, "protection wall has no prior absorption"
    return True, ""


def gate_2_confidence(
    fv: FeatureVector,
    signal: MicroSignal,
    scorer,
) -> tuple[bool, str, float]:
    score = scorer.score(fv, signal)
    if score < settings.MIN_CONFIDENCE:
        return False, f"confidence {score:.3f} < {settings.MIN_CONFIDENCE}", score
    return True, "", score


def gate_3_capital(budget, tier: str) -> tuple[bool, str]:
    if tier in ("HALTED", "PASSIVE"):
        return False, f"risk tier {tier} — no new entries"
    if budget is not None and budget.remaining <= 0:
        return False, f"daily budget exhausted (remaining={budget.remaining:.2f})"
    return True, ""


def gate_4_order_selection(
    spread_bps: float,
    spread_p95: float,
) -> tuple[bool, str, str]:
    if spread_bps > spread_p95 * 2:
        return False, f"spread {spread_bps:.1f}bps > 2×p95 {spread_p95:.1f}bps", ""
    order_type = "IOC_LIMIT"
    return True, "", order_type


def gate_5_execution_sync(
    signal_timestamp_ms: int,
    last_delta_ms: float,
) -> tuple[bool, str]:
    age_ms = int(time.time() * 1000) - signal_timestamp_ms
    if age_ms > _IOC_STALE_MS:
        return False, f"signal stale ({age_ms}ms > {_IOC_STALE_MS}ms)"
    if last_delta_ms > settings.HEARTBEAT_CRITICAL_MS:
        return False, f"latency too high ({last_delta_ms:.0f}ms)"
    return True, ""


# ── Rule-based confidence scorer ──────────────────────────────────────────────

class RuleBasedScorer:
    """
    Placeholder scorer until XGBoost is trained on tick data.
    Weights: OBI alignment 0.30, vol surge 0.25, spread 0.20, CVD 0.25.
    """

    def score(self, fv: FeatureVector, signal: MicroSignal) -> float:
        score = 0.0
        # OBI directional alignment
        if signal.direction == "LONG" and fv.obi_zscore > 0.20:
            score += 0.30
        elif signal.direction == "SHORT" and fv.obi_zscore < -0.20:
            score += 0.30
        # Volume surge
        score += min(fv.vol_ratio / 4.0, 0.25)
        # Spread within normal range
        if fv.spread_bps < 8.0:
            score += 0.20
        # CVD alignment
        if signal.direction == "LONG" and fv.cvd_positive:
            score += 0.25
        elif signal.direction == "SHORT" and not fv.cvd_positive:
            score += 0.25
        return round(min(score, 1.0), 3)


# ── Post-entry Gate 6 ─────────────────────────────────────────────────────────

class PersistenceMonitor:
    """
    Polls the LOB every check_interval_ms to confirm the protection wall
    is still present. Alerts order_manager if the wall is removed.
    """

    async def monitor(
        self,
        protection_wall_price: float,
        position_side: str,
        lob_engine,
        order_manager,
        position_closed_event: asyncio.Event,
        check_interval_ms: int = 200,
    ) -> None:
        interval = check_interval_ms / 1000.0
        while True:
            # Race the poll interval against position-closed. If the position exits
            # normally (TP/SL), stop quietly without firing the wall-removed alert.
            try:
                await asyncio.wait_for(position_closed_event.wait(), timeout=interval)
                logger.debug(
                    "[Gate6] Position closed — stopping monitor for wall %.2f",
                    protection_wall_price,
                )
                return
            except asyncio.TimeoutError:
                pass

            walls = await lob_engine.get_current_walls()
            if not _wall_present(protection_wall_price, walls):
                logger.warning(
                    "[Gate6] Protection wall at %.2f removed — alerting order manager",
                    protection_wall_price,
                )
                if hasattr(order_manager, "handle_protection_wall_removed"):
                    await order_manager.handle_protection_wall_removed(position_side)
                return


# ── Null CVD fallback ─────────────────────────────────────────────────────────

class _NullCVD:
    """Stands in when no CVD calculator is injected. Warns once per instance."""

    def __init__(self) -> None:
        self._warned = False

    def get_cvd_delta(self, ticks: int = 5) -> float:
        if not self._warned:
            logger.warning(
                "[Executor] No CVD calculator injected — cvd_delta forced to 0; "
                "inject explicitly for accurate confidence scoring"
            )
            self._warned = True
        return 0.0


# ── Strategy Executor ─────────────────────────────────────────────────────────

class StrategyExecutor:
    """
    Runs MicroSignals through all 7 gates. Emits telemetry at every gate exit
    (pass and fail). Approved signals become MicroOrderRequests forwarded to
    signal_queue. Gate 6 (PersistenceMonitor) starts as a background task
    after approval when lob_engine and order_manager are injected.

    Telemetry is non-blocking: put_nowait drops records if the queue is full
    rather than stalling the approval-to-order path.
    """

    _SPREAD_P95_COLD_START = 5.0   # fallback until ≥10 spread samples observed
    _SPREAD_P95_FACTOR     = 1.64  # normal approximation for 95th percentile

    def __init__(
        self,
        micro_signal_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        telemetry_queue: asyncio.Queue,
        feature_computer,
        shared_state: SharedState,
        budget=None,
        rule_scorer=None,
        cvd_calculator=None,
        lob_engine=None,
        order_manager=None,
        gate6_check_interval_ms: int = 200,
    ) -> None:
        self._micro_q       = micro_signal_queue
        self._signal_q      = signal_queue
        self._telem_q       = telemetry_queue
        self._fc            = feature_computer
        self._state         = shared_state
        self._budget        = budget
        if rule_scorer is not None:
            self._scorer = rule_scorer
        else:
            from strategy.scorer import ScorerFactory   # lazy import — avoids circular dep at load time
            self._scorer = ScorerFactory.load_or_fallback()
        self._risk_tier: str = "FULL"
        self._lob_engine    = lob_engine
        self._order_manager = order_manager

        if cvd_calculator is None:
            logger.warning(
                "[Executor] No CVD calculator injected — using null fallback; "
                "inject cvd_calculator for accurate confidence scoring"
            )
        self._cvd = cvd_calculator or _NullCVD()

        # Log-space spread tracker — correct for right-skewed spread distributions.
        # Stores log(spread_bps) so p95 = exp(log_mean + 1.64 × log_std), which
        # handles the fat right tail that mean+1.64σ in linear space underestimates.
        self._log_spread_stats = WelfordOnline()

        # Tracked Gate 6 tasks (persistence monitors + their fill-wait watchers).
        self._gate6_tasks: set[asyncio.Task] = set()
        self._gate6_check_ms = gate6_check_interval_ms

        self._last_approved_ms: int = 0

    def set_risk_tier(self, tier: str) -> None:
        """
        Sync the active risk tier from RiskEngine into Gate 3.
        Call this after every fill via RiskEngine.record_trade_result so
        REDUCED/MINIMAL/PASSIVE throttling applies to the microstructure path.

        Valid tiers: FULL | REDUCED | MINIMAL | PASSIVE | HALTED
        """
        valid = {"FULL", "REDUCED", "MINIMAL", "PASSIVE", "HALTED"}
        if tier not in valid:
            logger.warning("[Executor] Ignoring unknown risk tier %r", tier)
            return
        if tier != self._risk_tier:
            logger.info("[Executor] Risk tier updated: %s → %s", self._risk_tier, tier)
            self._risk_tier = tier

    @property
    def _spread_p95(self) -> float:
        """Session-aware 95th-percentile spread via log-normal approximation."""
        if self._log_spread_stats.n < 10:
            return self._SPREAD_P95_COLD_START
        log_p95 = self._log_spread_stats.mean + self._SPREAD_P95_FACTOR * self._log_spread_stats.std
        return math.exp(log_p95)

    async def run(self) -> None:
        while True:
            signal: MicroSignal = await self._micro_q.get()
            await self._evaluate(signal)

    async def _evaluate(self, signal: MicroSignal) -> None:
        rec = SignalRecord(
            micro_signal = signal.signal_type,
            direction    = signal.direction,
        )

        # Gate 0 — data fidelity
        ok, reason = gate_0_data_fidelity(
            self._state.lob_status, self._state.heartbeat_status
        )
        if not ok:
            self._reject(rec, "GATE_0_FAIL", reason)
            return

        # Gate 1 — microstructure (prior absorption on protection wall required)
        ok, reason = gate_1_microstructure(signal, signal.prior_absorption)
        if not ok:
            self._reject(rec, "GATE_1_FAIL", reason)
            return

        # Gate 2 — confidence
        fv = self._fc.compute(cvd_calculator=self._cvd, shared_state=self._state)

        if fv is None:
            self._reject(rec, "GATE_2_FAIL", "feature vector not ready")
            return

        # Snapshot current p95 BEFORE including this bar (causal), then update in log-space
        spread_p95 = self._spread_p95
        self._log_spread_stats.update(math.log(max(fv.spread_bps, 1e-4)))

        ok, reason, confidence = gate_2_confidence(fv, signal, self._scorer)
        rec.confidence       = confidence
        rec.obi_zscore       = fv.obi_zscore
        rec.cvd_delta        = fv.cvd_delta
        rec.spread_bps       = fv.spread_bps
        rec.features_json    = json.dumps(fv.to_ml_array())
        rec.lob_status       = fv.lob_status
        rec.heartbeat_status = self._state.heartbeat_status
        if not ok:
            self._reject(rec, "GATE_2_FAIL", reason)
            return
        # Tier-elevated minimum confidence (REDUCED=0.65, MINIMAL=0.80)
        min_conf = TIER_MIN_CONFIDENCE.get(self._risk_tier, settings.MIN_CONFIDENCE)
        if confidence < min_conf:
            self._reject(rec, "GATE_2_FAIL",
                         f"confidence {confidence:.3f} < tier-{self._risk_tier} min {min_conf:.2f}")
            return

        # Gate 3 — capital
        ok, reason = gate_3_capital(self._budget, self._risk_tier)
        if not ok:
            self._reject(rec, "GATE_3_FAIL", reason)
            return

        # Gate 4 — order selection (session-aware spread p95)
        ok, reason, order_type = gate_4_order_selection(fv.spread_bps, spread_p95)
        if not ok:
            self._reject(rec, "GATE_4_FAIL", reason)
            return

        # Gate 5 — execution sync
        ok, reason = gate_5_execution_sync(signal.timestamp_ms, self._state.last_delta_ms)
        if not ok:
            self._reject(rec, "GATE_5_FAIL", reason)
            return

        # Rate-limit: enforce minimum interval between approvals
        if settings.MIN_SIGNAL_INTERVAL_MS > 0:
            now_ms = int(time.time() * 1000)
            elapsed_ms = now_ms - self._last_approved_ms
            if elapsed_ms < settings.MIN_SIGNAL_INTERVAL_MS:
                self._reject(rec, "GATE_5_FAIL",
                             f"rate limit: {elapsed_ms}ms < {settings.MIN_SIGNAL_INTERVAL_MS}ms")
                return

        # All gates passed — assemble executable order request
        rec.gate_passed = "APPROVED"
        self._emit_telemetry(rec)

        # notional_hint = risk fraction of equity; execution layer applies:
        #   qty = (equity × notional_hint) / abs(entry_price − protection_wall_price)
        tier_scalar   = TIER_SCALARS.get(self._risk_tier, 1.0)
        notional_hint = round(
            confidence * settings.KELLY_FRACTION * settings.RISK_PER_TRADE_PCT * tier_scalar, 6
        )
        order_req = MicroOrderRequest(
            micro_signal   = signal,
            signal_id      = rec.signal_id,
            order_type     = order_type,
            side           = "BUY" if signal.direction == "LONG" else "SELL",
            limit_price    = None,   # execution layer resolves via live LOB
            ioc_timeout_ms = _IOC_STALE_MS,
            confidence     = confidence,
            notional_hint  = notional_hint,
        )
        await self._signal_q.put(order_req)
        self._last_approved_ms = int(time.time() * 1000)
        logger.info(
            "[Executor] APPROVED %s %s confidence=%.3f order_type=%s notional_hint=%.4f%%",
            signal.direction, signal.signal_type, confidence, order_type,
            notional_hint * 100,
        )

        # Gate 6 — watch for fill confirmation, then start persistence monitor.
        # Guard at call site: no task is created when Gate 6 deps are absent.
        if self._lob_engine and self._order_manager and signal.protection_wall:
            watch = asyncio.create_task(
                self._gate6_watch(
                    fill_event=order_req.fill_event,
                    position_closed_event=order_req.position_closed_event,
                    signal=signal,
                    signal_id=rec.signal_id,
                ),
                name=f"gate6_watch_{rec.signal_id[:8]}",
            )
            watch.add_done_callback(self._on_gate6_task_done)
            self._gate6_tasks.add(watch)
        else:
            logger.debug("[Gate6] Skipped — lob_engine/order_manager not injected")

    async def _gate6_watch(
        self,
        fill_event: asyncio.Event,
        position_closed_event: asyncio.Event,
        signal: MicroSignal,
        signal_id: str,
    ) -> None:
        """
        Waits for fill confirmation then starts PersistenceMonitor.
        Only created when lob_engine, order_manager, and protection_wall are all present
        (guarded at call site). Exits silently if the fill never arrives within the timeout.
        """
        fill_timeout = _IOC_STALE_MS * 10 / 1000.0
        try:
            await asyncio.wait_for(fill_event.wait(), timeout=fill_timeout)
        except asyncio.TimeoutError:
            logger.debug(
                "[Gate6] No fill confirmation for %s within %.0fms — monitor not started",
                signal_id[:8], fill_timeout * 1000,
            )
            return

        try:
            logger.info(
                "[Gate6] Fill confirmed for %s — starting persistence monitor on wall %.2f",
                signal_id[:8], signal.protection_wall.price,
            )
            mon = asyncio.create_task(
                PersistenceMonitor().monitor(
                    protection_wall_price=signal.protection_wall.price,
                    position_side=signal.direction,
                    lob_engine=self._lob_engine,
                    order_manager=self._order_manager,
                    position_closed_event=position_closed_event,
                    check_interval_ms=self._gate6_check_ms,
                ),
                name=f"gate6_persistence_{signal_id[:8]}",
            )
            mon.add_done_callback(self._on_gate6_task_done)
            self._gate6_tasks.add(mon)
        except Exception:
            logger.exception(
                "[Gate6] Failed to start persistence monitor for %s — "
                "triggering wall-removed alert as safety fallback",
                signal_id[:8],
            )
            if self._order_manager is not None:
                await self._order_manager.handle_protection_wall_removed(signal.direction)

    def _on_gate6_task_done(self, task: asyncio.Task) -> None:
        self._gate6_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                "[Gate6] Task %s crashed: %s", task.get_name(), task.exception()
            )

    def _reject(self, rec: SignalRecord, gate: str, reason: str) -> None:
        rec.gate_passed      = gate
        rec.rejection_reason = reason
        self._emit_telemetry(rec)
        logger.debug("[Executor] %s: %s", gate, reason)

    def _emit_telemetry(self, rec: SignalRecord) -> None:
        try:
            self._telem_q.put_nowait(rec)
        except asyncio.QueueFull:
            logger.warning(
                "[Executor] Telemetry queue full — record dropped (gate=%s)", rec.gate_passed
            )
