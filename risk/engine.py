import asyncio
import logging
from datetime import datetime, timezone, timedelta

from config import settings
from models import (
    CircuitBreakerStatus,
    Direction,
    OrderRequest,
    PatternSignal,
    PortfolioState,
)

logger = logging.getLogger(__name__)

# 5-tier throttle table: tier → size scalar
_TIER_SCALARS: dict[str, float] = {
    "FULL":    1.00,
    "REDUCED": 0.50,
    "MINIMAL": 0.25,
    "PASSIVE": 0.10,
    "HALTED":  0.00,
}


class RiskEngine:
    MIN_CONFIDENCE   = 0.50
    MAX_POSITIONS    = 1
    COOLDOWN_SECONDS = 300

    def __init__(self, signal_queue: asyncio.Queue, order_queue: asyncio.Queue, portfolio: PortfolioState) -> None:
        self._signal_queue = signal_queue
        self._order_queue  = order_queue
        self.portfolio     = portfolio
        self._cooldown_until: datetime | None = None
        self._tier: str = "FULL"

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
            logger.warning(f"[Risk] REJECTED — {reason}")
            return OrderRequest(signal=signal, quantity=0.0, approved=False, rejection_reason=reason)

        if signal.confidence < self.MIN_CONFIDENCE:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason=f"Low confidence {signal.confidence:.2f}")

        if len(self.portfolio.positions) >= self.MAX_POSITIONS:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason="Max positions reached")

        quantity = self._size_position(signal)
        if quantity <= 0:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason="Zero quantity after sizing")

        logger.info(f"[Risk] APPROVED — {signal.pattern.name} qty={quantity:.6f} tier={self._tier}")
        return OrderRequest(signal=signal, quantity=quantity, approved=True)

    # ── Circuit breakers + 5-tier throttle ───────────────────────────────────

    def _check_circuit_breakers(self) -> CircuitBreakerStatus:
        pf = self.portfolio

        if pf.drawdown_pct >= settings.MAX_DRAWDOWN_PCT:
            self._tier = "HALTED"
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical(f"[Risk] CIRCUIT BREAKER: Max drawdown {pf.drawdown_pct:.2%} → HALT")
            return CircuitBreakerStatus.HALTED

        if pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT:
            self._tier = "HALTED"
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical(f"[Risk] CIRCUIT BREAKER: Daily loss {pf.daily_loss_pct:.2%} → HALT")
            return CircuitBreakerStatus.HALTED

        if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            return CircuitBreakerStatus.PAUSED

        if pf.consecutive_losses >= settings.MAX_CONSECUTIVE_LOSSES:
            self._cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=self.COOLDOWN_SECONDS)
            self._tier = "REDUCED"
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            logger.warning(f"[Risk] Consecutive losses={pf.consecutive_losses} → tier=REDUCED, PAUSE {self.COOLDOWN_SECONDS}s")
            return CircuitBreakerStatus.PAUSED

        # Tiering based on daily loss progress
        if pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT * 0.75:
            self._tier = "MINIMAL"
        elif pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT * 0.50:
            self._tier = "REDUCED"
        else:
            self._tier = "FULL"

        pf.circuit_breaker = CircuitBreakerStatus.ACTIVE
        return CircuitBreakerStatus.ACTIVE

    # ── Position sizing ───────────────────────────────────────────────────────

    def _size_position(self, signal: PatternSignal) -> float:
        """
        Risk amount = min(equity × 1%, dov × 0.4%)
        base_qty    = risk_amount / sl_distance
        final_qty   = base_qty × kelly_scale × tier_scalar
        """
        equity      = self.portfolio.equity
        dov         = getattr(self.portfolio, "starting_equity", equity)
        sl_distance = abs(signal.entry_price - signal.stop_loss)

        if sl_distance < 1e-8:
            return 0.0

        risk_amount  = min(equity * settings.RISK_PER_TRADE_PCT, dov * 0.004)
        base_qty     = risk_amount / sl_distance
        kelly_scale  = settings.KELLY_FRACTION * signal.confidence
        tier_scalar  = _TIER_SCALARS.get(self._tier, 1.0)
        return round(base_qty * kelly_scale * tier_scalar, 6)

    # Legacy name kept for backward compat with existing tests
    def _size_position_vol_target(self, signal: PatternSignal) -> float:
        return self._size_position(signal)

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade_result(self, pnl: float) -> None:
        self.portfolio.equity    += pnl
        self.portfolio.daily_pnl += pnl
        self.portfolio.peak_equity = max(self.portfolio.peak_equity, self.portfolio.equity)

        if pnl < 0:
            self.portfolio.consecutive_losses += 1
        else:
            self.portfolio.consecutive_losses = 0

        logger.info(
            f"[Risk] Trade PnL={pnl:.2f} | Equity={self.portfolio.equity:.2f} | "
            f"Drawdown={self.portfolio.drawdown_pct:.2%} | CB={self.portfolio.circuit_breaker.name} | "
            f"Tier={self._tier}"
        )

    # ── Session reset ─────────────────────────────────────────────────────────

    def reset_for_new_session(self) -> None:
        """Called by midnight reset loop to clear daily counters."""
        self.portfolio.daily_pnl        = 0.0
        self.portfolio.consecutive_losses = 0
        self._cooldown_until            = None
        self._tier                      = "FULL"
        self.portfolio.circuit_breaker  = CircuitBreakerStatus.ACTIVE
        logger.info("[Risk] Session reset — daily counters cleared.")

    @property
    def tier(self) -> str:
        return self._tier

