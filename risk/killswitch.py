"""
GlobalKillswitch — hard stop with three triggers (master arch §8).

KS-1  Budget breach      — daily loss exceeds hard limit
KS-2  Heartbeat critical — consecutive critical latency packets
KS-3  Slippage decay     — rolling avg slippage > research baseline × multiplier

Once fired, is_active is permanently True until process restart (Rule 12).
"""

import logging
from collections import deque

from config import settings
from models import KillswitchState

logger = logging.getLogger(__name__)

_SLIPPAGE_WINDOW = 20   # rolling window for slippage average


class GlobalKillswitch:

    def __init__(
        self,
        dov: float,
        hard_limit_pct: float = 0.01,
    ) -> None:
        self._dov            = dov
        self._hard_limit     = dov * hard_limit_pct
        self._state          = KillswitchState()
        self._slippage_buf: deque[float] = deque(maxlen=_SLIPPAGE_WINDOW)

    # ── Public checks ─────────────────────────────────────────────────────────

    def check_budget(self, realised_pnl: float, unrealised_pnl: float) -> bool:
        """KS-1: fire if total loss exceeds daily hard limit. Returns True if fired."""
        if self._state.fired:
            return True
        total_loss = realised_pnl + unrealised_pnl
        if total_loss < -self._hard_limit:
            return self._fire(
                "KS-1_BUDGET",
                f"total_loss={total_loss:.2f} < -hard_limit={-self._hard_limit:.2f}",
                latency=0.0,
                slippage=0.0,
            )
        return False

    def check_heartbeat(self, heartbeat_status: str, delta_ms: float) -> bool:
        """KS-2: fire if heartbeat is CRITICAL. Returns True if fired."""
        if self._state.fired:
            return True
        if heartbeat_status == "CRITICAL":
            return self._fire(
                "KS-2_HEARTBEAT",
                f"heartbeat=CRITICAL delta_ms={delta_ms:.0f}",
                latency=delta_ms,
                slippage=0.0,
            )
        return False

    def record_slippage(self, signal_price: float, fill_price: float, direction: str) -> bool:
        """
        KS-3: track fill slippage; fire when rolling average exceeds
        research baseline × SLIPPAGE_MULTIPLIER.
        Returns True if fired.
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

        avg = sum(self._slippage_buf) / len(self._slippage_buf)
        threshold = settings.SLIPPAGE_RESEARCH_BPS * settings.SLIPPAGE_MULTIPLIER
        if avg > threshold:
            return self._fire(
                "KS-3_SLIPPAGE",
                f"avg_slippage={avg:.2f}bps > threshold={threshold:.2f}bps",
                latency=0.0,
                slippage=avg,
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

    def _fire(self, trigger: str, detail: str, latency: float, slippage: float) -> bool:
        import datetime
        self._state.fired              = True
        self._state.trigger            = trigger
        self._state.fired_at           = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._state.latency_at_fire    = latency
        self._state.slippage_at_fire   = slippage
        logger.critical(
            "[KS] KILLSWITCH FIRED — %s: %s", trigger, detail
        )
        return True
