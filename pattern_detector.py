import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

import numpy as np
from scipy.stats import linregress

from config import settings
from models import Candle, Direction, PatternSignal, PatternType

logger = logging.getLogger(__name__)


class PatternDetector:
    def __init__(self, candle_queue: asyncio.Queue, signal_queue: asyncio.Queue) -> None:
        self._candle_queue = candle_queue
        self._signal_queue = signal_queue
        self._candles: deque[Candle] = deque(maxlen=settings.PATTERN_LOOKBACK)

    async def run(self) -> None:
        logger.info("[Detector] Pattern detector started.")
        while True:
            candle: Candle = await self._candle_queue.get()
            self._candles.append(candle)

            if len(self._candles) < 20:
                continue

            signals = self._detect_all()
            for sig in signals:
                logger.info(f"[Detector] Signal: {sig.pattern.name} {sig.direction.name} conf={sig.confidence:.2f}")
                await self._signal_queue.put(sig)

    def _detect_all(self) -> list[PatternSignal]:
        candles = list(self._candles)
        closes = np.array([c.close for c in candles])
        highs = np.array([c.high for c in candles])
        lows = np.array([c.low for c in candles])
        volumes = np.array([c.volume for c in candles])

        atr = self._compute_atr(highs, lows, closes)
        avg_volume = np.mean(volumes[-20:]) if len(volumes) >= 1 else 1.0
        current_volume = volumes[-1]
        vol_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        signals: list[PatternSignal] = []

        swing_highs_idx = self._find_swings(highs, is_high=True)
        swing_lows_idx = self._find_swings(lows, is_high=False)

        if len(swing_highs_idx) >= 3 and len(swing_lows_idx) >= 3:
            wedge_sig = self._check_wedge(
                highs, lows, closes, swing_highs_idx, swing_lows_idx, atr, vol_ratio
            )
            if wedge_sig:
                signals.append(wedge_sig)

        sr_sig = self._check_sr_breakout(closes, highs, lows, atr, vol_ratio)
        if sr_sig:
            signals.append(sr_sig)

        return signals

    def _find_swings(self, series: np.ndarray, is_high: bool) -> list[int]:
        w = settings.SWING_WINDOW
        pivots = []
        for i in range(w, len(series) - w):
            window = series[i - w: i + w + 1]
            if is_high and series[i] == np.max(window):
                pivots.append(i)
            elif not is_high and series[i] == np.min(window):
                pivots.append(i)
        return pivots

    def _check_wedge(self, highs, lows, closes, sh_idx, sl_idx, atr: float, vol_ratio: float) -> PatternSignal | None:
        x_h = np.array(sh_idx[-3:])
        y_h = highs[x_h]
        x_l = np.array(sl_idx[-3:])
        y_l = lows[x_l]

        slope_h, intercept_h, r_h, _, _ = linregress(x_h, y_h)
        slope_l, intercept_l, r_l, _, _ = linregress(x_l, y_l)

        r2_h = float(r_h ** 2)
        r2_l = float(r_l ** 2)

        if r2_h < settings.MIN_R2 or r2_l < settings.MIN_R2:
            return None

        current_close = closes[-1]
        n = len(closes) - 1
        lower_trendline_now = slope_l * n + intercept_l

        pattern = None
        direction = None

        if slope_h > 0 and slope_l > 0 and slope_l > slope_h:
            if current_close < lower_trendline_now and vol_ratio >= settings.BREAKOUT_VOL_MULT:
                pattern = PatternType.RISING_WEDGE
                direction = Direction.SHORT

        elif slope_h < 0 and slope_l < 0 and abs(slope_l) > abs(slope_h):
            if current_close > lower_trendline_now and vol_ratio >= settings.BREAKOUT_VOL_MULT:
                pattern = PatternType.FALLING_WEDGE
                direction = Direction.LONG

        elif slope_h < 0 and slope_l > 0:
            if vol_ratio >= settings.BREAKOUT_VOL_MULT:
                prior_trend = closes[-1] - closes[max(0, len(closes) - settings.PATTERN_LOOKBACK)]
                direction = Direction.LONG if prior_trend > 0 else Direction.SHORT
                pattern = PatternType.SYMMETRICAL_TRIANGLE

        if pattern is None:
            return None

        entry = current_close
        if direction == Direction.LONG:
            sl = entry - settings.ATR_MULTIPLIER_SL * atr
            tp = entry + settings.ATR_MULTIPLIER_TP * atr
        else:
            sl = entry + settings.ATR_MULTIPLIER_SL * atr
            tp = entry - settings.ATR_MULTIPLIER_TP * atr

        confidence = min(1.0, (r2_h + r2_l) / 2 * (vol_ratio / settings.BREAKOUT_VOL_MULT))

        return PatternSignal(
            pattern=pattern,
            direction=direction,
            confidence=confidence,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            detected_at=datetime.now(timezone.utc),
            r2=(r2_h + r2_l) / 2,
            volume_ratio=vol_ratio,
        )

    def _check_sr_breakout(self, closes, highs, lows, atr: float, vol_ratio: float) -> PatternSignal | None:
        lookback = closes[-30:]
        resistance = np.percentile(lookback, 95)
        support = np.percentile(lookback, 5)
        current = closes[-1]
        tolerance = atr * 0.3

        if current > resistance + tolerance and vol_ratio >= settings.BREAKOUT_VOL_MULT:
            sl = current - settings.ATR_MULTIPLIER_SL * atr
            tp = current + settings.ATR_MULTIPLIER_TP * atr
            return PatternSignal(
                pattern=PatternType.RESISTANCE_BREAKOUT,
                direction=Direction.LONG,
                confidence=min(1.0, vol_ratio / 3),
                entry_price=current, stop_loss=sl, take_profit=tp,
                volume_ratio=vol_ratio,
            )

        if current < support - tolerance and vol_ratio >= settings.BREAKOUT_VOL_MULT:
            sl = current + settings.ATR_MULTIPLIER_SL * atr
            tp = current - settings.ATR_MULTIPLIER_TP * atr
            return PatternSignal(
                pattern=PatternType.SUPPORT_BREAKOUT,
                direction=Direction.SHORT,
                confidence=min(1.0, vol_ratio / 3),
                entry_price=current, stop_loss=sl, take_profit=tp,
                volume_ratio=vol_ratio,
            )
        return None

    @staticmethod
    def _compute_atr(highs, lows, closes, period: int = 14) -> float:
        trs = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)
        if not trs:
            return 0.0
        return float(np.mean(trs[-period:]))
