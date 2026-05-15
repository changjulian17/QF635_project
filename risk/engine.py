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


class RiskEngine:
    MIN_CONFIDENCE = 0.50
    MAX_POSITIONS = 1
    COOLDOWN_SECONDS = 300

    def __init__(self, signal_queue: asyncio.Queue, order_queue: asyncio.Queue, portfolio: PortfolioState) -> None:
        self._signal_queue = signal_queue
        self._order_queue = order_queue
        self.portfolio = portfolio
        self._cooldown_until: datetime | None = None

    async def run(self) -> None:
        logger.info("[Risk] Risk engine started.")
        while True:
            signal: PatternSignal = await self._signal_queue.get()
            order = self._evaluate(signal)
            await self._order_queue.put(order)

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

        quantity = self._size_position_vol_target(signal)
        if quantity <= 0:
            return OrderRequest(signal=signal, quantity=0.0, approved=False,
                                rejection_reason="Zero quantity after sizing")

        logger.info(f"[Risk] APPROVED — {signal.pattern.name} qty={quantity:.6f}")
        return OrderRequest(signal=signal, quantity=quantity, approved=True)

    def _check_circuit_breakers(self) -> CircuitBreakerStatus:
        pf = self.portfolio

        if pf.drawdown_pct >= settings.MAX_DRAWDOWN_PCT:
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical(f"[Risk] CIRCUIT BREAKER: Max drawdown {pf.drawdown_pct:.2%} breached → HALT")
            return CircuitBreakerStatus.HALTED

        if pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT:
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical(f"[Risk] CIRCUIT BREAKER: Daily loss {pf.daily_loss_pct:.2%} → HALT")
            return CircuitBreakerStatus.HALTED

        if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            return CircuitBreakerStatus.PAUSED

        if pf.consecutive_losses >= settings.MAX_CONSECUTIVE_LOSSES:
            self._cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=self.COOLDOWN_SECONDS)
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            logger.warning(f"[Risk] Consecutive losses={pf.consecutive_losses} → PAUSE {self.COOLDOWN_SECONDS}s")
            return CircuitBreakerStatus.PAUSED

        pf.circuit_breaker = CircuitBreakerStatus.ACTIVE
        return CircuitBreakerStatus.ACTIVE

    def _size_position_vol_target(self, signal: PatternSignal) -> float:
        equity = self.portfolio.equity
        risk_amount = equity * settings.RISK_PER_TRADE_PCT
        sl_distance = abs(signal.entry_price - signal.stop_loss)

        if sl_distance < 1e-8:
            return 0.0

        raw_qty = risk_amount / sl_distance
        kelly_scale = settings.KELLY_FRACTION * signal.confidence
        return round(raw_qty * kelly_scale, 6)

    def record_trade_result(self, pnl: float) -> None:
        self.portfolio.equity += pnl
        self.portfolio.daily_pnl += pnl
        self.portfolio.peak_equity = max(self.portfolio.peak_equity, self.portfolio.equity)

        if pnl < 0:
            self.portfolio.consecutive_losses += 1
        else:
            self.portfolio.consecutive_losses = 0

        logger.info(f"[Risk] Trade PnL={pnl:.2f} | Equity={self.portfolio.equity:.2f} | "
                    f"Drawdown={self.portfolio.drawdown_pct:.2%} | CB={self.portfolio.circuit_breaker.name}")
