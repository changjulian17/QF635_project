"""
GlobalKillswitch — hard stop with three triggers (master arch §8).

KS-1  Budget breach      — daily loss exceeds hard limit
KS-2  Heartbeat critical — consecutive critical latency packets
KS-3  Slippage decay     — rolling avg slippage > research baseline × multiplier

Once fired, is_active is permanently True until process restart (Rule 12).
"""

import datetime
import logging
from collections import deque

from config import settings
from models import KillswitchState

logger = logging.getLogger(__name__)

_SLIPPAGE_WINDOW = 20


class GlobalKillswitch:

    def __init__(self, dov: float, hard_limit_pct: float = 0.01) -> None:
        self._hard_limit      = dov * hard_limit_pct
        self._state           = KillswitchState()
        self._slippage_buf: deque[float] = deque(maxlen=_SLIPPAGE_WINDOW)
        self._consec_critical: int = 0

    def update_dov(self, new_equity: float, hard_limit_pct: float = 0.01) -> None:
        """Rebase KS-1 hard loss limit to actual opening equity (call post-reconcile)."""
        if new_equity <= 0:
            return
        self._hard_limit = new_equity * hard_limit_pct

    # ── Public checks ─────────────────────────────────────────────────────────

    def check_budget(self, realised_pnl: float, unrealised_pnl: float) -> bool:
        """KS-1: fire if total loss exceeds daily hard limit."""
        if self._state.fired:
            return True
        total_loss = realised_pnl + unrealised_pnl
        if total_loss < -self._hard_limit:
            return self._fire(
                "KS-1_BUDGET",
                f"total_loss={total_loss:.2f} < -hard_limit={-self._hard_limit:.2f}",
                latency=0.0,
                slippage=0.0,
                total_loss=total_loss,
            )
        return False

    def check_heartbeat(self, heartbeat_status: str, delta_ms: float) -> bool:
        """KS-2: fire after settings.HEARTBEAT_CONSEC_LIMIT consecutive CRITICAL packets."""
        if self._state.fired:
            return True
        if heartbeat_status == "CRITICAL":
            self._consec_critical += 1
            if self._consec_critical >= settings.HEARTBEAT_CONSEC_LIMIT:
                return self._fire(
                    "KS-2_HEARTBEAT",
                    f"heartbeat=CRITICAL x{self._consec_critical} delta_ms={delta_ms:.0f}",
                    latency=delta_ms,
                    slippage=0.0,
                    total_loss=0.0,
                )
        else:
            self._consec_critical = 0
        return False

    def record_slippage(self, signal_price: float, fill_price: float, direction: str) -> bool:
        """
        KS-3: track fill slippage; fire when rolling average exceeds
        research baseline × SLIPPAGE_MULTIPLIER.
        """
        if self._state.fired:
            return True
        if signal_price <= 0:
            return False

        if direction == "LONG":
            slippage_bps = (fill_price - signal_price) / signal_price * 10_000
        else:
            slippage_bps = (signal_price - fill_price) / signal_price * 10_000

        self._slippage_buf.append(slippage_bps)
        if len(self._slippage_buf) < _SLIPPAGE_WINDOW:
            return False

        avg       = sum(self._slippage_buf) / len(self._slippage_buf)
        threshold = settings.SLIPPAGE_RESEARCH_BPS * settings.SLIPPAGE_MULTIPLIER
        if avg > threshold:
            return self._fire(
                "KS-3_SLIPPAGE",
                f"avg_slippage={avg:.2f}bps > threshold={threshold:.2f}bps",
                latency=0.0,
                slippage=avg,
                total_loss=0.0,
            )
        return False

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        return self._state.fired

    @property
    def state(self) -> KillswitchState:
        return self._state

    # ── Internal ──────────────────────────────────────────────────────────────

    def _fire(
        self,
        trigger: str,
        detail: str,
        latency: float,
        slippage: float,
        total_loss: float,
    ) -> bool:
        self._state.fired              = True
        self._state.trigger            = trigger
        self._state.fired_at           = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._state.latency_at_fire    = latency
        self._state.slippage_at_fire   = slippage
        self._state.total_loss_at_fire = total_loss
        logger.critical("[KS] KILLSWITCH FIRED — %s: %s", trigger, detail)
        return True
