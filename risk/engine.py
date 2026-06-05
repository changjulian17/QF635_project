import logging
from collections.abc import Callable
from datetime import datetime, timezone, timedelta
from enum import StrEnum

from config import settings
from models import CircuitBreakerStatus, PortfolioState
from risk.budget import DailyBudget
from risk.killswitch import GlobalKillswitch

logger = logging.getLogger(__name__)


class _Tier(StrEnum):
    FULL    = "FULL"
    REDUCED = "REDUCED"
    MINIMAL = "MINIMAL"
    PASSIVE = "PASSIVE"
    HALTED  = "HALTED"


# 5-tier throttle table: tier → position size scalar
TIER_SCALARS: dict[str, float] = {
    "FULL":    1.00,
    "REDUCED": 0.50,
    "MINIMAL": 0.25,
    "PASSIVE": 0.00,   # spec: "no new entries, manage only"
    "HALTED":  0.00,
}

# 5-tier minimum confidence gates
TIER_MIN_CONFIDENCE: dict[str, float] = {
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
        portfolio: PortfolioState,
        budget: DailyBudget | None = None,
        killswitch: GlobalKillswitch | None = None,
        tier_change_cb: Callable[[str, str], None] | None = None,
    ) -> None:
        self.portfolio       = portfolio
        self._budget         = budget      or DailyBudget.from_equity(portfolio.starting_equity)
        self._killswitch     = killswitch  or GlobalKillswitch(portfolio.starting_equity)
        self._tier: _Tier    = _Tier.FULL
        self._cooldown_until: datetime | None = None
        self._tier_change_cb = tier_change_cb

    # ── Circuit breakers + 5-tier throttle ───────────────────────────────────

    def _check_circuit_breakers(self) -> CircuitBreakerStatus:
        pf = self.portfolio

        # Permanent killswitch (Rule 12 — process restart required to clear)
        if self._killswitch.is_active:
            self._tier = _Tier.HALTED
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.debug("[Risk] killswitch active → HALT (suppressed after first fire)")
            return CircuitBreakerStatus.HALTED

        now      = datetime.now(timezone.utc)
        loss_pct = self._budget.loss_pct

        # Hard stop: equity drawdown
        if pf.drawdown_pct >= settings.MAX_DRAWDOWN_PCT:
            self._tier = _Tier.HALTED
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical("[Risk] CIRCUIT BREAKER: drawdown %.2f%% → HALT", pf.drawdown_pct * 100)
            return CircuitBreakerStatus.HALTED

        # Hard stop: budget exhausted (spec: ≥ 1% DOV)
        if loss_pct >= settings.TIER_HALTED_PCT:
            self._tier = _Tier.HALTED
            pf.circuit_breaker = CircuitBreakerStatus.HALTED
            logger.critical("[Risk] CIRCUIT BREAKER: budget loss %.2f%% → HALT", loss_pct * 100)
            return CircuitBreakerStatus.HALTED

        # Belt-and-suspenders: legacy portfolio daily-loss hard stop
        if pf.daily_loss_pct >= settings.DAILY_LOSS_LIMIT_PCT:
            self._tier = _Tier.HALTED
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
        if loss_pct >= settings.TIER_PASSIVE_PCT:
            self._tier = _Tier.PASSIVE
            pf.circuit_breaker = CircuitBreakerStatus.PAUSED
            logger.warning("[Risk] Budget loss %.2f%% → tier=PASSIVE (no new entries)", loss_pct * 100)
            return CircuitBreakerStatus.PAUSED
        elif loss_pct >= settings.TIER_MINIMAL_PCT:
            self._tier = _Tier.MINIMAL
        elif loss_pct >= settings.TIER_REDUCED_PCT:
            self._tier = _Tier.REDUCED
        else:
            self._tier = _Tier.FULL

        pf.circuit_breaker = CircuitBreakerStatus.ACTIVE
        return CircuitBreakerStatus.ACTIVE

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade_result(self, pnl: float) -> None:
        self.portfolio.equity      += pnl
        self.portfolio.daily_pnl   += pnl
        self.portfolio.peak_equity  = max(self.portfolio.peak_equity, self.portfolio.equity)
        self._budget.realised_pnl  += pnl

        if pnl < 0:
            self.portfolio.consecutive_losses += 1
        else:
            self.portfolio.consecutive_losses = 0
            self.portfolio.num_wins += 1

        self.portfolio.num_trades += 1
        self.portfolio.budget_loss_pct = self._budget.loss_pct
        self._killswitch.check_budget(self._budget.realised_pnl, self._budget.unrealised_pnl)

        logger.info(
            "[Risk] Trade PnL=%.2f | Equity=%.2f | Drawdown=%.2f%% | "
            "BudgetLoss=%.2f%% | CB=%s | Tier=%s",
            pnl, self.portfolio.equity, self.portfolio.drawdown_pct * 100,
            self._budget.loss_pct * 100, self.portfolio.circuit_breaker.name, self._tier,
        )

    # ── Unrealised mark ───────────────────────────────────────────────────────

    def mark_unrealised(self, pnl: float) -> None:
        """Update the budget's unrealised exposure so tier transitions fire proactively."""
        self._budget.unrealised_pnl = pnl
        self.portfolio.budget_loss_pct = self._budget.loss_pct

    # ── Tier sync (called by MTM loop, not on inbound signals) ───────────────────

    def sync_tier(self) -> str:
        """Evaluate circuit breakers without an inbound signal; fire tier_change_cb on transition."""
        old_tier = str(self._tier)
        self._check_circuit_breakers()
        new_tier = str(self._tier)
        if new_tier != old_tier and self._tier_change_cb:
            self._tier_change_cb(old_tier, new_tier)
        return new_tier

    # ── Session reset ─────────────────────────────────────────────────────────

    def reset_for_new_session(self) -> None:
        """Called by midnight reset loop to clear daily counters."""
        self.portfolio.daily_pnl          = 0.0
        self.portfolio.consecutive_losses = 0
        self.portfolio.num_trades         = 0
        self.portfolio.num_wins           = 0
        self.portfolio.num_fill_samples   = 0
        self.portfolio.avg_slippage_bps   = 0.0
        self.portfolio.budget_loss_pct    = 0.0
        self._cooldown_until              = None
        self._tier                        = _Tier.FULL
        self.portfolio.circuit_breaker    = CircuitBreakerStatus.ACTIVE
        self._budget.reset(self.portfolio.equity)
        logger.info("[Risk] Session reset — daily counters and budget cleared.")

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def tier(self) -> str:
        return self._tier
