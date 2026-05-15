"""
CVD Calculator — standalone cumulative volume delta tracker.

Sign convention (matches Binance aggTrade semantics):
  is_buyer_maker=False  → buyer aggressed the ask  → +qty (buy pressure)
  is_buyer_maker=True   → seller aggressed the bid → -qty (sell pressure)
"""

import math
from collections import deque

from models import AggTrade


class WelfordOnline:
    """Numerically stable online mean/variance (Knuth/Welford algorithm)."""

    def __init__(self) -> None:
        self.n: int = 0
        self._mean: float = 0.0
        self._M2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self._mean
        self._mean += delta / self.n
        self._M2 += delta * (value - self._mean)

    @property
    def variance(self) -> float:
        return self._M2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


class CVDCalculator:
    """
    Tracks cumulative volume delta (CVD) tick-by-tick.

    Intended to consume AggTrade objects from the raw_tick_queue.
    Call update() on each trade; query get_cvd(), get_cvd_delta(),
    and get_cvd_std() from the strategy layer.
    """

    def __init__(self, history_len: int = 200) -> None:
        self._cvd: float = 0.0
        self._history: deque[float] = deque(maxlen=history_len)
        self._delta_stats = WelfordOnline()

    def update(self, trade: AggTrade) -> None:
        """Process one aggTrade and update CVD."""
        signed_qty = -trade.qty if trade.is_buyer_maker else trade.qty
        prev_cvd = self._cvd
        self._cvd += signed_qty
        self._history.append(self._cvd)
        delta = self._cvd - prev_cvd
        self._delta_stats.update(delta)

    def get_cvd(self) -> float:
        """Current cumulative volume delta."""
        return self._cvd

    def get_cvd_delta(self, bars: int = 5) -> float:
        """CVD change over the last `bars` ticks. Returns 0.0 if insufficient history."""
        if len(self._history) <= bars:
            return 0.0
        return self._history[-1] - self._history[-1 - bars]

    def get_cvd_std(self) -> float:
        """Welford online std of per-tick CVD deltas."""
        return self._delta_stats.std

    def reset_daily(self) -> None:
        """Reset CVD at UTC midnight. History and stats are also cleared."""
        self._cvd = 0.0
        self._history.clear()
        self._delta_stats = WelfordOnline()
