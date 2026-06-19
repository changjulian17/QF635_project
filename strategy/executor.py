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
from collections.abc import Callable

from config import settings
from core.cvd import WelfordOnline
from core.signal_telemetry import SignalRecord
from models import FeatureVector, MicroOrderRequest, MicroSignal, SharedState
from risk.engine import TIER_MIN_CONFIDENCE, TIER_SCALARS
from risk.sizing import clamp_stop_bps, cap_risk_fraction
from strategy.spec import EntryRules

logger = logging.getLogger(__name__)

_IOC_STALE_MS = settings.IOC_TIMEOUT_MS   # signal older than this is rejected


def _wall_present(price: float, walls: list[dict], tol: float = 0.01) -> bool:
    """Tolerance-aware wall lookup. Avoids false absences from float repr drift."""
    return any(abs(w["price"] - price) <= tol for w in walls)


# ── Gate functions ────────────────────────────────────────────────────────────

def gate_0_data_fidelity(
    lob_status: str,
    heartbeat_status: str,
    lob_heartbeat_status: str = "HEALTHY",
) -> tuple[bool, str]:
    if lob_status != "SYNCED":
        return False, f"LOB not SYNCED ({lob_status})"
    if heartbeat_status in ("CRITICAL", "SUSTAINED_DEGRADED"):
        return False, f"price heartbeat {heartbeat_status}"
    if lob_heartbeat_status in ("CRITICAL", "SUSTAINED_DEGRADED"):
        return False, f"LOB heartbeat {lob_heartbeat_status}"
    return True, ""


def gate_1_microstructure(signal: MicroSignal, absorption_armed: bool) -> tuple[bool, str]:
    if signal.signal_type != "SWEEP_WITH_PROTECTION":
        return False, f"unexpected signal type: {signal.signal_type}"
    if signal.consumed_wall is None:
        return False, "no consumed wall"
    if signal.protection_wall is None:
        return False, "no protection wall"
    if not absorption_armed:
        return False, "consumed wall has no prior absorption"
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


def gate_3_capital(budget, tier: str, active_exposure: bool = False) -> tuple[bool, str]:
    if active_exposure:
        return False, "active microstructure exposure"
    if tier in ("HALTED", "PASSIVE"):
        return False, f"risk tier {tier} — no new entries"
    if budget is not None and budget.remaining <= 0:
        return False, f"daily budget exhausted (remaining={budget.remaining:.2f})"
    return True, ""


def gate_3_position_size(
    estimated_notional: float,
    equity: float,
    max_notional_pct: float,
) -> tuple[bool, str]:
    """Reject signals whose estimated position size would exceed max_notional_pct of equity."""
    max_notional = equity * max_notional_pct
    if estimated_notional > max_notional:
        return False, (
            f"est. notional ${estimated_notional:,.0f} exceeds "
            f"{max_notional_pct * 100:.0f}% equity (${max_notional:,.0f})"
        )
    return True, ""


def gate_4_order_selection(
    spread_bps: float,
    spread_p95: float,
    spread_hard_cap_bps: float = 8.0,
) -> tuple[bool, str, str]:
    if spread_bps > spread_hard_cap_bps:
        return False, f"spread {spread_bps:.1f}bps > hard cap {spread_hard_cap_bps:.1f}bps", ""
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
    Weights: OBI alignment 0.45, vol surge 0.35, spread 0.20.
    CVD is retained for telemetry/ML features, but is not a rule-based gate.
    """

    def __init__(self, rules: EntryRules | None = None) -> None:
        self._rules = rules or EntryRules()

    def score(self, fv: FeatureVector, signal: MicroSignal) -> float:
        score = 0.0
        thresh = self._rules.obi_threshold
        # OBI directional alignment
        if signal.direction == "LONG" and fv.obi_zscore > thresh:
            score += 0.45
        elif signal.direction == "SHORT" and fv.obi_zscore < -thresh:
            score += 0.45
        # Volume surge
        score += min((fv.vol_ratio / 4.0) * 0.35, 0.35)
        # Spread within normal range
        if fv.spread_bps < self._rules.spread_max_bps:
            score += 0.20
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
        shared_state: SharedState | None = None,
        check_interval_ms: int = 200,
    ) -> None:
        interval = check_interval_ms / 1000.0
        started_ms = int(time.time() * 1000)
        wall_absent_count = 0
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

            now_ms = int(time.time() * 1000)
            if now_ms - started_ms >= settings.MICRO_MAX_HOLD_MS:
                logger.warning("[Gate6] Max hold exceeded — triggering safety exit")
                if hasattr(order_manager, "handle_safety_exit"):
                    await order_manager.handle_safety_exit("MAX_HOLD")
                return

            if shared_state is not None and (
                shared_state.heartbeat_status in ("CRITICAL", "SUSTAINED_DEGRADED")
                or shared_state.last_delta_ms > settings.HEARTBEAT_CRITICAL_MS
            ):
                logger.warning("[Gate6] Heartbeat/latency critical — triggering safety exit")
                if hasattr(order_manager, "handle_safety_exit"):
                    await order_manager.handle_safety_exit("LATENCY_CRITICAL")
                return

            if hasattr(lob_engine, "get_snapshot"):
                snap = await lob_engine.get_snapshot(depth=1)
                if snap and snap.bids and snap.asks:
                    best_bid = snap.bids[0].price
                    best_ask = snap.asks[0].price
                    mid = (best_bid + best_ask) / 2.0
                    spread_bps = ((best_ask - best_bid) / mid * 10_000) if mid > 0 else 0.0
                    if spread_bps > settings.MICRO_EXIT_SPREAD_HARD_CAP_BPS:
                        logger.warning(
                            "[Gate6] Spread %.1fbps > %.1fbps — triggering safety exit",
                            spread_bps,
                            settings.MICRO_EXIT_SPREAD_HARD_CAP_BPS,
                        )
                        if hasattr(order_manager, "handle_safety_exit"):
                            await order_manager.handle_safety_exit("SPREAD_HARD_CAP")
                        return

            walls = await lob_engine.get_current_walls(sigma=settings.LOB_WALL_SIGMA)
            if not _wall_present(protection_wall_price, walls):
                wall_absent_count += 1
                if wall_absent_count < settings.GATE6_WALL_ABSENT_CONSEC:
                    logger.debug(
                        "[Gate6] Wall absent %d/%d checks — waiting for confirmation",
                        wall_absent_count, settings.GATE6_WALL_ABSENT_CONSEC,
                    )
                else:
                    logger.warning(
                        "[Gate6] Protection wall at %.2f removed (confirmed over %d checks)",
                        protection_wall_price, wall_absent_count,
                    )
                    if hasattr(order_manager, "handle_protection_wall_removed"):
                        await order_manager.handle_protection_wall_removed(position_side)
                    return
            else:
                wall_absent_count = 0   # explicit reset — flicker must not accumulate


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
        strategy_id: str = "v3.0",
        equity_fn: Callable[[], float] | None = None,
    ) -> None:
        self._micro_q       = micro_signal_queue
        self._signal_q      = signal_queue
        self._telem_q       = telemetry_queue
        self._fc            = feature_computer
        self._state         = shared_state
        self._budget        = budget
        self._strategy_id   = strategy_id
        self._equity_fn     = equity_fn
        self._entry_rules   = EntryRules()
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

    def set_entry_rules(self, rules: EntryRules) -> None:
        """Apply optimised entry thresholds from the active registered spec.

        Call once at startup after resolving the active strategy from the registry.
        Also updates RuleBasedScorer thresholds when that fallback scorer is active.
        """
        self._entry_rules = rules
        if isinstance(self._scorer, RuleBasedScorer):
            self._scorer = RuleBasedScorer(rules)
        logger.info(
            "[Executor] Entry rules applied: spread_max_bps=%.1f obi_threshold=%.2f",
            rules.spread_max_bps, rules.obi_threshold,
        )

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
            try:
                await self._evaluate(signal)
            except Exception as exc:
                logger.critical(
                    "[Executor] Unhandled exception in _evaluate — signal dropped: %s",
                    exc, exc_info=True,
                )

    async def _evaluate(self, signal: MicroSignal) -> None:
        rec = SignalRecord(
            micro_signal = signal.signal_type,
            direction    = signal.direction,
            strategy_id  = self._strategy_id,
        )

        cw = signal.consumed_wall
        pw = signal.protection_wall
        logger.info(
            "[Executor] --> %s %s mid=%.2f consumed=%s@%.2f(%.3f) protection=%s@%.2f(%.3f) absorption=%s cvd=%.1fstd move=%+.3f%%",
            signal.direction, signal.signal_type, signal.mid_price,
            cw.side if cw else "?", cw.price if cw else 0.0, cw.qty_current if cw else 0.0,
            pw.side if pw else "?", pw.price if pw else 0.0, pw.qty_current if pw else 0.0,
            signal.prior_absorption, signal.cvd_std, signal.price_move_pct * 100,
        )

        # Gate 0 — data fidelity
        ok, reason = gate_0_data_fidelity(
            self._state.lob_status,
            self._state.heartbeat_status,
            self._state.lob_heartbeat_status,
        )
        logger.info(
            "[Gate0] %s lob=%s hb=%s%s",
            "PASS" if ok else "FAIL",
            self._state.lob_status, self._state.heartbeat_status,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_0_FAIL", reason)
            return

        # Gate 1 — microstructure (prior absorption on consumed wall required)
        ok, reason = gate_1_microstructure(signal, signal.prior_absorption)
        logger.info(
            "[Gate1] %s type=%s consumed=%s protection=%s absorption=%s%s",
            "PASS" if ok else "FAIL",
            signal.signal_type,
            cw.side if cw else "?",
            pw.side if pw else "?",
            signal.prior_absorption,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_1_FAIL", reason)
            return

        # Gate 2 — confidence
        fv = self._fc.compute(cvd_calculator=self._cvd, shared_state=self._state)

        if fv is None:
            logger.info("[Gate2] FAIL fv=None (warmup incomplete)")
            self._reject(rec, "GATE_2_FAIL", "feature vector not ready")
            return

        # Snapshot current p95 BEFORE including this bar (causal), then update in log-space
        spread_p95 = self._spread_p95
        self._log_spread_stats.update(math.log(max(fv.spread_bps, 1e-4)))

        # Effective minimum is the stricter of base threshold and tier-elevated threshold
        effective_min_conf = max(
            settings.MIN_CONFIDENCE,
            TIER_MIN_CONFIDENCE.get(self._risk_tier, settings.MIN_CONFIDENCE),
        )
        ok, reason, confidence = gate_2_confidence(fv, signal, self._scorer)
        rec.confidence       = confidence
        rec.obi_zscore       = fv.obi_zscore
        rec.cvd_delta        = fv.cvd_delta
        rec.spread_bps       = fv.spread_bps
        rec.features_json    = json.dumps(fv.to_ml_array())
        rec.lob_status       = fv.lob_status
        rec.heartbeat_status = self._state.heartbeat_status

        # Tier-elevated minimum confidence (REDUCED=0.65, MINIMAL=0.80)
        if ok and confidence < effective_min_conf:
            ok = False
            reason = f"confidence {confidence:.3f} < tier-{self._risk_tier} min {effective_min_conf:.2f}"

        logger.info(
            "[Gate2] %s confidence=%.3f (min=%.3f) obi=%+.2f vol=%.1fx spread=%.1fbps cvd=%+.4f%s",
            "PASS" if ok else "FAIL",
            confidence, effective_min_conf,
            fv.obi_zscore, fv.vol_ratio, fv.spread_bps, fv.cvd_delta,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_2_FAIL", reason)
            return

        # notional_hint needs confidence (Gate 2) and risk_tier — compute here so
        # Gate 3 position-size check can use it before the order request is built.
        tier_scalar   = TIER_SCALARS.get(self._risk_tier, 1.0)
        notional_hint = round(
            confidence * settings.KELLY_FRACTION * settings.RISK_PER_TRADE_PCT * tier_scalar, 6
        )

        # Gate 3 — capital
        active_exposure = (
            bool(self._order_manager.has_active_exposure())
            if self._order_manager and hasattr(self._order_manager, "has_active_exposure")
            else False
        )
        budget_remaining = self._budget.remaining if self._budget is not None else float("inf")
        ok, reason = gate_3_capital(self._budget, self._risk_tier, active_exposure)
        logger.info(
            "[Gate3] %s tier=%s budget=%.2f exposure=%s%s",
            "PASS" if ok else "FAIL",
            self._risk_tier, budget_remaining, active_exposure,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_3_FAIL", reason)
            return

        # Cap per-trade risk by the remaining daily-loss budget. gate_3_capital only
        # checks remaining > 0, so near exhaustion a full-size trade could risk more
        # than the allowance left — clamp the risk fraction to remaining / equity.
        if self._equity_fn is not None and self._budget is not None:
            _eq = self._equity_fn()
            if _eq > 0:
                notional_hint = round(cap_risk_fraction(notional_hint, _eq, budget_remaining), 6)

        # Gate 3 position-size check — reject if estimated notional exceeds MAX_ORDER_NOTIONAL_PCT
        # of equity. Uses bps-capped sl_distance from mid_price for a consistent estimate with
        # the execution layer. Skipped when equity_fn is not injected (e.g. tests, backtests).
        if self._equity_fn is not None and signal.protection_wall and signal.mid_price > 0:
            equity     = self._equity_fn()
            pw_price   = signal.protection_wall.price
            raw_bps    = abs(signal.mid_price - pw_price) / signal.mid_price * 10_000
            capped_bps = clamp_stop_bps(
                raw_bps, settings.PROTECTION_MIN_DISTANCE_BPS, settings.PROTECTION_MAX_DISTANCE_BPS
            )
            sl_est     = signal.mid_price * capped_bps / 10_000
            if sl_est > 0:
                _book    = self._lob_engine.best_bid_ask() if self._lob_engine else None
                live_mid = (_book[0] + _book[1]) / 2.0 if _book else signal.mid_price
                est_notional = (equity * notional_hint / sl_est) * live_mid
                ok, reason   = gate_3_position_size(
                    est_notional, equity, settings.MAX_ORDER_NOTIONAL_PCT
                )
                if not ok:
                    logger.info("[Gate3] FAIL position-size %s", reason)
                    self._reject(rec, "GATE_3_FAIL", reason)
                    return

        # Gate 4 — order selection (session-aware spread p95 + strategy hard cap)
        ok, reason, order_type = gate_4_order_selection(
            fv.spread_bps, spread_p95, self._entry_rules.spread_max_bps
        )
        logger.info(
            "[Gate4] %s spread=%.1fbps p95=%.1fbps hard_cap=%.1fbps%s",
            "PASS" if ok else "FAIL",
            fv.spread_bps, spread_p95, self._entry_rules.spread_max_bps,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_4_FAIL", reason)
            return

        # Gate 5 — execution sync
        ok, reason = gate_5_execution_sync(signal.timestamp_ms, self._state.last_delta_ms)
        signal_age_ms = int(time.time() * 1000) - signal.timestamp_ms
        logger.info(
            "[Gate5] %s age=%dms latency=%.0fms%s",
            "PASS" if ok else "FAIL",
            signal_age_ms, self._state.last_delta_ms,
            "" if ok else f" → {reason}",
        )
        if not ok:
            self._reject(rec, "GATE_5_FAIL", reason)
            return

        # Rate-limit: enforce minimum interval between approvals
        if settings.MIN_SIGNAL_INTERVAL_MS > 0:
            now_ms = int(time.time() * 1000)
            elapsed_ms = now_ms - self._last_approved_ms
            if elapsed_ms < settings.MIN_SIGNAL_INTERVAL_MS:
                logger.info(
                    "[RateLimit] FAIL elapsed=%dms < min=%dms",
                    elapsed_ms, settings.MIN_SIGNAL_INTERVAL_MS,
                )
                self._reject(rec, "RATE_LIMIT",
                             f"rate limit: {elapsed_ms}ms < {settings.MIN_SIGNAL_INTERVAL_MS}ms")
                return

        # All gates passed — assemble executable order request
        rec.gate_passed = "APPROVED"
        self._emit_telemetry(rec)

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
            "[Executor] APPROVED %s %s confidence=%.3f notional=%.4f%% tier=%s | consumed=%s@%.2f protection=%s@%.2f mid=%.2f",
            signal.direction, order_type, confidence, notional_hint * 100,
            self._risk_tier,
            cw.side if cw else "?", cw.price if cw else 0.0,
            pw.side if pw else "?", pw.price if pw else 0.0,
            signal.mid_price,
        )

        # Gate 6 — watch for fill confirmation, then start persistence monitor.
        # Guard at call site: no task is created when Gate 6 deps are absent.
        if self._lob_engine and self._order_manager and signal.protection_wall:
            if self._gate6_tasks:
                stale = list(self._gate6_tasks)
                for t in stale:
                    t.cancel()
                await asyncio.gather(*stale, return_exceptions=True)
                self._gate6_tasks.clear()
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
                    shared_state=self._state,
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
