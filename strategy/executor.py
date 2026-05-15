"""
7-Gate Strategy Executor — evaluates every MicroSignal through a sequential
gate pipeline and emits approved OrderRequests with full telemetry at each exit.

Gates (master arch §5.5):
  Gate 0 — Data Fidelity     (LOB SYNCED + heartbeat not CRITICAL)
  Gate 1 — Microstructure    (confirmed Sweep+Protection signal)
  Gate 2 — Confidence        (rule-based scorer >= MIN_CONFIDENCE)
  Gate 3 — Capital           (daily budget remaining)
  Gate 4 — Order Selection   (spread within acceptable range)
  Gate 5 — Execution Sync    (signal not stale)
  Gate 6 — Persistence       (post-entry; protection wall still present)
"""

import asyncio
import logging
import time
from typing import Optional

from config import settings
from core.signal_telemetry import SignalRecord, SignalTelemetry
from models import FeatureVector, MicroSignal, SharedState

logger = logging.getLogger(__name__)

_IOC_STALE_MS = settings.IOC_TIMEOUT_MS   # signal older than this is rejected


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
    if tier == "HALTED":
        return False, "risk tier HALTED"
    if budget is not None and budget.remaining <= 0:
        return False, f"daily budget exhausted (remaining={budget.remaining:.2f})"
    return True, ""


def gate_4_order_selection(
    signal: MicroSignal,
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
        if signal.direction == "LONG" and fv.obi_zscore > -0.20:
            score += 0.30
        elif signal.direction == "SHORT" and fv.obi_zscore < 0.20:
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
        check_interval_ms: int = 200,
    ) -> None:
        interval = check_interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            walls = lob_engine.get_current_walls()
            wall_prices = {w["price"] for w in walls}
            if protection_wall_price not in wall_prices:
                logger.warning(
                    "[Gate6] Protection wall at %.2f removed — alerting order manager",
                    protection_wall_price,
                )
                if hasattr(order_manager, "handle_protection_wall_removed"):
                    await order_manager.handle_protection_wall_removed(position_side)
                break


# ── Strategy Executor ─────────────────────────────────────────────────────────

class StrategyExecutor:
    """
    Runs MicroSignals through all 7 gates. Emits telemetry at every gate exit
    (pass and fail). Approved signals are forwarded to signal_queue.
    """

    def __init__(
        self,
        micro_signal_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        telemetry_queue: asyncio.Queue,
        feature_computer,
        shared_state: SharedState,
        budget=None,
        rule_scorer=None,
        spread_p95: float = 5.0,
    ) -> None:
        self._micro_q   = micro_signal_queue
        self._signal_q  = signal_queue
        self._telem_q   = telemetry_queue
        self._fc        = feature_computer
        self._state     = shared_state
        self._budget    = budget
        self._scorer    = rule_scorer or RuleBasedScorer()
        self._spread_p95 = spread_p95
        self._risk_tier: str = "FULL"

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
            await self._reject(rec, "GATE_0_FAIL", reason)
            return

        # Gate 1 — microstructure
        ok, reason = gate_1_microstructure(signal, signal.prior_absorption)
        if not ok:
            await self._reject(rec, "GATE_1_FAIL", reason)
            return

        # Gate 2 — confidence
        fv = self._fc.compute(
            cvd_calculator=self._fc._cvd if hasattr(self._fc, "_cvd") else _NullCVD(),
            shared_state=self._state,
        ) if hasattr(self._fc, "compute") else None

        if fv is None:
            await self._reject(rec, "GATE_2_FAIL", "feature vector not ready")
            return

        ok, reason, confidence = gate_2_confidence(fv, signal, self._scorer)
        rec.confidence   = confidence
        rec.obi_zscore   = fv.obi_zscore
        rec.cvd_delta    = fv.cvd_delta
        rec.spread_bps   = fv.spread_bps
        rec.lob_status   = fv.lob_status
        rec.heartbeat_status = self._state.heartbeat_status
        if not ok:
            await self._reject(rec, "GATE_2_FAIL", reason)
            return

        # Gate 3 — capital
        ok, reason = gate_3_capital(self._budget, self._risk_tier)
        if not ok:
            await self._reject(rec, "GATE_3_FAIL", reason)
            return

        # Gate 4 — order selection
        ok, reason, order_type = gate_4_order_selection(
            signal, fv.spread_bps, self._spread_p95
        )
        if not ok:
            await self._reject(rec, "GATE_4_FAIL", reason)
            return

        # Gate 5 — execution sync
        ok, reason = gate_5_execution_sync(signal.timestamp_ms, self._state.last_delta_ms)
        if not ok:
            await self._reject(rec, "GATE_5_FAIL", reason)
            return

        # All gates passed
        signal.confidence = confidence
        rec.gate_passed = "APPROVED"
        await self._telem_q.put(rec)
        await self._signal_q.put(signal)
        logger.info(
            "[Executor] APPROVED %s %s confidence=%.3f",
            signal.direction, signal.signal_type, confidence,
        )

    async def _reject(self, rec: SignalRecord, gate: str, reason: str) -> None:
        rec.gate_passed      = gate
        rec.rejection_reason = reason
        await self._telem_q.put(rec)
        logger.debug("[Executor] %s: %s", gate, reason)


class _NullCVD:
    def get_cvd_delta(self, bars: int = 5) -> float:
        return 0.0
