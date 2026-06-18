"""
FeatureComputer — assembles all 15 FeatureVector fields from live data streams.

All statistics are strictly causal: WelfordOnline z-scores and percentile ranks
are computed against the distribution seen so far, then updated (Rule 9).
No rolling-window NaN-fill, no look-ahead.
"""

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional

from core.cvd import WelfordOnline
from models import Candle, FeatureVector, LOBSnapshot, SharedState

__all__ = ["WelfordOnline", "FeatureParams", "FeatureComputer"]


@dataclass
class FeatureParams:
    rsi_period:              int   = 14
    atr_period:              int   = 14
    vwap_reset_utc_midnight: bool  = True
    wall_sigma:              float = 2.5
    cvd_delta_ticks:         int   = 5
    vol_climax_ratio:        float = 3.0
    vwap_atr_scale:          float = 2.0


class FeatureComputer:
    """
    Consumes closed candles and LOB snapshots; maintains all running statistics
    to produce a FeatureVector on demand.

    Typical call pattern (once per depth20 tick):
        fc.update_candle(candle)          # on each closed kline
        fc.update_orderbook(snap, walls)  # on each depth20 snapshot
        fv = fc.compute(cvd_calc, shared_state)
    """

    def __init__(self, params: FeatureParams | None = None) -> None:
        self._p = params or FeatureParams()

        # Online stats — updated during update_candle / update_orderbook
        self._obi_stats = WelfordOnline()
        self._vol_stats = WelfordOnline()
        self._atr_stats = WelfordOnline()

        # Candle buffers — ATR and RSI use separate close deques (different periods)
        self._highs:      deque[float] = deque(maxlen=self._p.atr_period + 2)
        self._lows:       deque[float] = deque(maxlen=self._p.atr_period + 2)
        self._atr_closes: deque[float] = deque(maxlen=self._p.atr_period + 2)
        self._closes:     deque[float] = deque(maxlen=self._p.rsi_period + 2)

        # VWAP accumulators (reset at UTC midnight)
        self._tpv_sum:       float = 0.0
        self._vol_sum:       float = 0.0
        self._vwap:          float = 0.0
        self._last_vwap_day: int   = -1

        # Wilder RSI state
        self._avg_gain:  float = 0.0
        self._avg_loss:  float = 0.0
        self._rsi_ready: bool  = False
        self._rsi_bars:  int   = 0

        # Pre-computed feature values (updated per candle / per LOB snapshot)
        self._rsi_value:         float = 50.0
        self._atr:               float = 0.0
        self._vol_ratio:         float = 1.0
        self._obi_zscore:        float = 0.0
        self._atr_percentile:    float = 0.5
        self._spread_bps:        float = 0.0
        self._mid_price:         float = 0.0
        self._wall_detected:     int   = 0
        self._wall_distance_bps: float = 0.0
        self._absorption_ratio:  float = 0.0
        self._prev_close:        float = 0.0
        self._cur_close:         float = 0.0

        # Wilder ATR state (Wilder EMA, alpha = 1/period)
        self._wilder_atr:       float = 0.0
        self._wilder_atr_sum:   float = 0.0   # accumulator during init phase
        self._wilder_atr_bars:  int   = 0
        self._wilder_atr_ready: bool  = False

    @property
    def wall_sigma(self) -> float:
        return self._p.wall_sigma

    @property
    def current_atr(self) -> float:
        """Latest Wilder ATR in price units. 0.0 until atr_period candles are seen."""
        return self._atr

    # ── Candle update ─────────────────────────────────────────────────────────

    def update_candle(self, candle: Candle) -> None:
        """Process a closed candle. Updates VWAP, ATR, RSI, and volume stats."""
        if not candle.is_closed:
            return

        self._prev_close = self._cur_close
        self._cur_close  = candle.close

        # VWAP — reset at UTC midnight
        utc_day = candle.open_time.toordinal()
        if self._p.vwap_reset_utc_midnight and utc_day != self._last_vwap_day:
            self._tpv_sum       = 0.0
            self._vol_sum       = 0.0
            self._last_vwap_day = utc_day

        tp = (candle.high + candle.low + candle.close) / 3.0
        self._tpv_sum += tp * candle.volume
        self._vol_sum += candle.volume
        self._vwap = self._tpv_sum / self._vol_sum if self._vol_sum > 0 else candle.close

        # ATR — Wilder EMA (alpha = 1/period), causal: prev_close captured before appending
        prev_close = self._atr_closes[-1] if self._atr_closes else 0.0
        self._highs.append(candle.high)
        self._lows.append(candle.low)
        self._atr_closes.append(candle.close)
        self._closes.append(candle.close)
        self._atr = self._step_wilder_atr(candle.high, candle.low, prev_close)

        # vol_ratio: score current volume against prior distribution (causal), then update
        if self._vol_stats.n > 0:
            mean = self._vol_stats.mean
            self._vol_ratio = candle.volume / mean if mean > 1e-9 else 1.0
        else:
            self._vol_ratio = 1.0
        self._vol_stats.update(candle.volume)

        # atr_percentile: rank current ATR against prior distribution, then update
        if self._atr > 0:
            self._atr_percentile = self._atr_stats.percentile_rank(self._atr)
        # percentile_rank() already called update() internally

        # Wilder RSI
        self._update_rsi(candle.close)

    def _step_wilder_atr(self, high: float, low: float, prev_close: float) -> float:
        """Update and return Wilder smoothed ATR (alpha = 1/period).
        Initialises with a simple average over the first `atr_period` bars."""
        tr = (
            max(high - low, abs(high - prev_close), abs(low - prev_close))
            if prev_close > 0.0
            else high - low
        )
        period = self._p.atr_period
        self._wilder_atr_bars += 1
        if self._wilder_atr_bars < period:
            self._wilder_atr_sum += tr
            return 0.0
        if self._wilder_atr_bars == period:
            self._wilder_atr_sum += tr
            self._wilder_atr = self._wilder_atr_sum / period
            self._wilder_atr_ready = True
            return self._wilder_atr
        k = 1.0 / period
        self._wilder_atr = self._wilder_atr * (1.0 - k) + tr * k
        return self._wilder_atr

    def reset(self) -> None:
        """Reset all accumulated state. Called by TickReplayEngine between replay windows."""
        self._obi_stats      = WelfordOnline()
        self._vol_stats      = WelfordOnline()
        self._atr_stats      = WelfordOnline()
        self._highs.clear()
        self._lows.clear()
        self._atr_closes.clear()
        self._closes.clear()
        self._tpv_sum        = 0.0
        self._vol_sum        = 0.0
        self._vwap           = 0.0
        self._last_vwap_day  = -1
        self._avg_gain       = 0.0
        self._avg_loss       = 0.0
        self._rsi_ready      = False
        self._rsi_bars       = 0
        self._rsi_value      = 50.0
        self._atr            = 0.0
        self._vol_ratio      = 1.0
        self._obi_zscore     = 0.0
        self._atr_percentile = 0.5
        self._spread_bps     = 0.0
        self._mid_price      = 0.0
        self._wall_detected      = 0
        self._wall_distance_bps  = 0.0
        self._absorption_ratio   = 0.0
        self._prev_close     = 0.0
        self._cur_close      = 0.0
        self._wilder_atr       = 0.0
        self._wilder_atr_sum   = 0.0
        self._wilder_atr_bars  = 0
        self._wilder_atr_ready = False

    def _update_rsi(self, close: float) -> None:
        closes = list(self._closes)
        if len(closes) < 2:
            return
        change = closes[-1] - closes[-2]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        self._rsi_bars += 1

        if self._rsi_bars <= self._p.rsi_period:
            self._avg_gain += gain
            self._avg_loss += loss
            if self._rsi_bars >= self._p.rsi_period:
                self._avg_gain /= self._p.rsi_period
                self._avg_loss /= self._p.rsi_period
                self._rsi_ready = True
        else:
            k = 1.0 / self._p.rsi_period
            self._avg_gain = self._avg_gain * (1 - k) + gain * k
            self._avg_loss = self._avg_loss * (1 - k) + loss * k

        if self._rsi_ready:
            if self._avg_loss > 1e-9:
                rs = self._avg_gain / self._avg_loss
                self._rsi_value = 100.0 - (100.0 / (1.0 + rs))
            else:
                self._rsi_value = 100.0 if self._avg_gain > 0 else 50.0

    # ── Order book update ─────────────────────────────────────────────────────

    def update_orderbook(self, snap: LOBSnapshot, walls: list[dict]) -> None:
        """Process a depth20 snapshot. Updates OBI, spread, and wall features."""
        if not snap.bids or not snap.asks:
            return

        from config import settings
        best_bid = snap.bids[0].price
        best_ask = snap.asks[0].price
        mid      = (best_bid + best_ask) / 2.0
        self._mid_price = mid

        depth   = settings.LOB_OBI_DEPTH
        bid_vol = sum(l.qty for l in snap.bids[:depth])
        ask_vol = sum(l.qty for l in snap.asks[:depth])
        denom   = bid_vol + ask_vol
        obi     = (bid_vol - ask_vol) / denom if denom > 0 else 0.0

        # obi_zscore: score then update (causal)
        self._obi_zscore = self._obi_stats.zscore(obi)

        self._spread_bps = ((best_ask - best_bid) / mid * 10_000) if mid > 0 else 0.0

        # Wall features
        if walls and mid > 0:
            nearest = min(walls, key=lambda w: abs(w["price"] - mid))
            self._wall_detected     = 1
            self._wall_distance_bps = abs(nearest["price"] - mid) / mid * 10_000
            self._absorption_ratio  = nearest.get("absorption_ratio", 0.0)
        else:
            self._wall_detected     = 0
            self._wall_distance_bps = 0.0
            self._absorption_ratio  = 0.0

    # ── Assemble feature vector ───────────────────────────────────────────────

    def compute(
        self,
        cvd_calculator,
        shared_state: SharedState,
        pattern_r2: float = 0.0,
        protection_wall_present: int = 0,
    ) -> Optional[FeatureVector]:
        """
        Return a FeatureVector assembled from the latest stored values.
        Returns None until at least rsi_period candles have been seen.
        """
        if not self._rsi_ready:
            return None

        atr       = self._atr if self._atr > 0 else 1.0
        cvd_delta = cvd_calculator.get_cvd_delta(self._p.cvd_delta_ticks)

        price_vs_vwap = math.tanh((self._cur_close - self._vwap) / (self._p.vwap_atr_scale * atr))
        vwap_reclaim  = int(self._prev_close < self._vwap <= self._cur_close)
        vol_climax    = int(self._vol_ratio > self._p.vol_climax_ratio)
        cvd_positive  = int(cvd_delta > 0)

        return FeatureVector(
            lob_status              = shared_state.lob_status,
            price_vs_vwap           = price_vs_vwap,
            obi_zscore              = self._obi_zscore,
            cvd_delta               = cvd_delta,
            vol_ratio               = self._vol_ratio,
            atr_percentile          = self._atr_percentile,
            rsi_value               = self._rsi_value,
            spread_bps              = self._spread_bps,
            pattern_r2              = pattern_r2,
            vwap_reclaim            = vwap_reclaim,
            vol_climax              = vol_climax,
            cvd_positive            = cvd_positive,
            wall_detected           = self._wall_detected,
            wall_distance_bps       = self._wall_distance_bps,
            absorption_ratio        = self._absorption_ratio,
            protection_wall_present = protection_wall_present,
        )
