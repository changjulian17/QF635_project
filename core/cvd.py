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
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        return self._M2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def zscore(self, value: float) -> float:
        """Return z-score of value against the CURRENT distribution, then update.
        Strictly causal: the incoming value is scored before it influences the stats."""
        s = self.std
        z = (value - self._mean) / s if s > 1e-9 else 0.0
        self.update(value)
        return z

    def percentile_rank(self, value: float) -> float:
        """Approximate percentile rank via normal CDF, then update."""
        s = self.std
        if s < 1e-9:
            self.update(value)
            return 0.5
        z = (value - self._mean) / s
        # Abramowitz & Stegun approximation of standard normal CDF
        t = 1.0 / (1.0 + 0.2316419 * abs(z))
        poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
        cdf = 1.0 - (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * z * z) * poly
        rank = cdf if z >= 0 else 1.0 - cdf
        self.update(value)
        return rank


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
        self._cvd += signed_qty
        self._history.append(self._cvd)
        self._delta_stats.update(signed_qty)

    def get_cvd(self) -> float:
        """Current cumulative volume delta."""
        return self._cvd

    def get_cvd_delta(self, ticks: int = 5) -> float:
        """CVD change over the last `ticks` ticks. Returns 0.0 if insufficient history."""
        if len(self._history) <= ticks:
            return 0.0
        return self._history[-1] - self._history[-1 - ticks]

    def get_cvd_tick_std(self) -> float:
        """Welford online std of per-trade signed quantity (each trade's contribution to CVD, i.e. per-tick CVD change)."""
        return self._delta_stats.std

    def reset_daily(self) -> None:
        """Reset CVD at UTC midnight. History and stats are also cleared."""
        self._cvd = 0.0
        self._history.clear()
        self._delta_stats = WelfordOnline()
