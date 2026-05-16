import asyncio
import logging
from datetime import datetime, timezone, timedelta

from config import settings
from models import (
    CircuitBreakerStatus,
    OrderRequest,
    PatternSignal,
    PortfolioState,
)
from risk.budget import DailyBudget

logger = logging.getLogger(__name__)

# 5-tier throttle table: tier → position size scalar
_TIER_SCALARS: dict[str, float] = {
    "FULL":    1.00,
    "REDUCED": 0.50,
    "MINIMAL": 0.25,
    "PASSIVE": 0.00,   # spec: "no new entries, manage only"
    "HALTED":  0.00,
}

# 5-tier minimum confidence gates
_TIER_MIN_CONFIDENCE: dict[str, float] = {
    "FULL":    0.50,
    "REDUCED": 0.65,
    "MINIMAL": 0.80,
    "PASSIVE": 1.01,   # effectively unreachable → always rejected
    "HALTED":  1.01,
}


class RiskEngine:
    MAX_POSITIONS    = 1
    COOLDOWN_SECONDS = 300

    def __init__(
        self,
        signal_queue: asyncio.Queue,
        order_queue: asyncio.Queue,
        portfolio: PortfolioState,
        budget: DailyBudget | None = None,
    ) -> None:
        self._signal_queue   = signal_queue
        self._order_queue    = order_queue
        self.portfolio       = portfolio
        self._budget         = budget or DailyBudget.from_equity(portfolio.starting_equity)
        self._tier: str      = "FULL"
        self._cooldown_until: datetime | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("[Risk] Risk engine started.")
        while True:
            signal: PatternSignal = await self._signal_queue.get()
            order = self._evaluate(signal)
            await self._order_queue.put(order)

    # ── Evaluation ────────────────────────────────────────────────────────────

    def _evaluate(self, signal: PatternSignal) -> OrderRequest:
        cb_status = self._check_circuit_breakers()
        if cb_status != CircuitBreakerStatus.ACTIVE:
            reason = f"Circuit breaker {cb_status.name}"
            logger.warning("[Risk] REJECTED — %s", reason)
            return OrderRequest(signal=signal, quantity=0.0, approved=False, rejection_reason=reason)

        min_conf = _TIER_MIN_CONFIDENCE[self._tier]
        if signal.confidence < min_conf:
            reason = f"Tier {self._tier} requires confidence >= {min_conf:.2f}, got {signal.confidence:.2f}"
            return OrderRequest(signal=signal, quantity=0.0, approved=False, rejection_reason=reason)

        if len(self.portfolio.positions) >= self.MAX_POSITIONS:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason="Max positions reached")

        quantity = self._size_position(signal)
        if quantity <= 0:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason="Zero quantity after sizing")

        logger.info("[Risk] APPROVED — %s qty=%.6f tier=%s", signal.pattern.name, quantity, self._tier)
        return OrderRequest(signal=signal, quantity=quantity, approved=True)

    # ── Circuit breakers + 5-tier throttle ───────────────────────────────────

    def _check_circuit_breakers(self) -> CircuitBreakerStatus:
        pf  = self.portfolio
        now = datetime.now(timezone.utc)

        # Hard stop: equity drawdown
        if pf.drawdown_pct >= settings.MAX_DRAWDOWN_PCT:
            self._tier = "HALTED"
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical("[Risk] CIRCUIT BREAKER: drawdown %.2f%% → HALT", pf.drawdown_pct * 100)
            return CircuitBreakerStatus.HALTED

        # Hard stop: budget exhausted (spec: ≥ 1% DOV)
        if self._budget.loss_pct >= settings.TIER_HALTED_PCT:
            self._tier = "HALTED"
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical("[Risk] CIRCUIT BREAKER: budget loss %.2f%% → HALT", self._budget.loss_pct * 100)
            return CircuitBreakerStatus.HALTED

        # Belt-and-suspenders: legacy portfolio daily-loss hard stop
        if pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT:
            self._tier = "HALTED"
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical("[Risk] CIRCUIT BREAKER: daily loss %.2f%% → HALT", pf.daily_loss_pct * 100)
            return CircuitBreakerStatus.HALTED

        # Consecutive-loss cooldown
        if self._cooldown_until and now < self._cooldown_until:
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            return CircuitBreakerStatus.PAUSED

        if pf.consecutive_losses >= settings.MAX_CONSECUTIVE_LOSSES:
            self._cooldown_until = now + timedelta(seconds=self.COOLDOWN_SECONDS)
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            logger.warning("[Risk] Consecutive losses=%d → PAUSE %ds", pf.consecutive_losses, self.COOLDOWN_SECONDS)
            return CircuitBreakerStatus.PAUSED

        # DOV-fraction tier transitions (spec-correct absolute thresholds)
        if self._budget.loss_pct >= settings.TIER_PASSIVE_PCT:
            self._tier = "PASSIVE"
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            logger.warning("[Risk] Budget loss %.2f%% → tier=PASSIVE (no new entries)", self._budget.loss_pct * 100)
            return CircuitBreakerStatus.PAUSED
        elif self._budget.loss_pct >= settings.TIER_MINIMAL_PCT:
            self._tier = "MINIMAL"
        elif self._budget.loss_pct >= settings.TIER_REDUCED_PCT:
            self._tier = "REDUCED"
        else:
            self._tier = "FULL"

        pf.circuit_breaker = CircuitBreakerStatus.ACTIVE
        return CircuitBreakerStatus.ACTIVE

    # ── Position sizing ───────────────────────────────────────────────────────

    def _size_position(self, signal: PatternSignal) -> float:
        """
        risk_amount = min(equity × 1%, remaining_budget × 60%, DOV × 0.4%)
        final_qty   = (risk_amount / sl_distance) × kelly_scale × tier_scalar
        """
        equity      = self.portfolio.equity
        sl_distance = abs(signal.entry_price - signal.stop_loss)

        if sl_distance < 1e-8:
            return 0.0

        risk_amount = min(
            equity * settings.RISK_PER_TRADE_PCT,
            max(self._budget.remaining, 0.0) * 0.60,
            self._budget.dov * 0.004,
        )
        kelly_scale = settings.KELLY_FRACTION * signal.confidence
        tier_scalar = _TIER_SCALARS[self._tier]
        return round((risk_amount / sl_distance) * kelly_scale * tier_scalar, 6)

    def _size_position_vol_target(self, signal: PatternSignal) -> float:
        """Alias kept for backward compatibility with existing tests."""
        return self._size_position(signal)

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade_result(self, pnl: float) -> None:
        self.portfolio.equity      += pnl
        self.portfolio.daily_pnl   += pnl
        self.portfolio.peak_equity  = max(self.portfolio.peak_equity, self.portfolio.equity)
        self._budget.realised_pnl  += pnl   # keeps budget.loss_pct current for tiering

        if pnl < 0:
            self.portfolio.consecutive_losses += 1
        else:
            self.portfolio.consecutive_losses = 0

        logger.info(
            "[Risk] Trade PnL=%.2f | Equity=%.2f | Drawdown=%.2f%% | "
            "BudgetLoss=%.2f%% | CB=%s | Tier=%s",
            pnl, self.portfolio.equity, self.portfolio.drawdown_pct * 100,
            self._budget.loss_pct * 100, self.portfolio.circuit_breaker.name, self._tier,
        )

    # ── Session reset ─────────────────────────────────────────────────────────

    def reset_for_new_session(self) -> None:
        """Called by midnight reset loop to clear daily counters."""
        self.portfolio.daily_pnl          = 0.0
        self.portfolio.consecutive_losses = 0
        self._cooldown_until              = None
        self._tier                        = "FULL"
        self.portfolio.circuit_breaker    = CircuitBreakerStatus.ACTIVE
        self._budget.reset(self.portfolio.equity)
        logger.info("[Risk] Session reset — daily counters and budget cleared.")

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def tier(self) -> str:
        return self._tier
