"""
backtesting/signals.py
======================
VectorBT-compatible signal generators for all CryptoSentinel strategies.

Architecture Role
-----------------
This module sits between the raw OHLCV data and VectorBT's portfolio
simulation layer. It converts the *same pattern detection logic* used
in the live system (pattern_detector.py) into boolean numpy arrays
that VectorBT can consume.

  OHLCV DataFrame
        │
        ▼
  generate_*_signals()   ← this module
        │ entries (bool array)
        │ sl_stop (float array)
        │ tp_stop (float array)
        ▼
  vbt.Portfolio.from_signals()
        │
        ▼
  BacktestMetrics

Signal Array Contract
---------------------
All generator functions return a namedtuple SignalArrays with:

  entries  : np.ndarray[bool]  — True at the candle index where an
                                  entry signal fires.
  sl_stop  : np.ndarray[float] — Stop-loss price for each entry
                                  (NaN where entries is False).
  tp_stop  : np.ndarray[float] — Take-profit price for each entry
                                  (NaN where entries is False).

Lookahead Bias in VectorBT
--------------------------
VectorBT is vectorised — it processes the entire array at once.
The sliding-window loops in each generator are O(n) but process only
window[i-lookback:i] at step i, so no future data is accessed.

The main lookahead risk in a vectorised system is normalisation
(e.g. using df['close'].mean() which includes future bars).
Each generator explicitly uses only rolling/expanding statistics.

Usage
-----
>>> from backtesting.signals import generate_signals
>>> arrays = generate_signals("Falling Wedge", df, params)
>>> pf = vbt.Portfolio.from_signals(
...     close   = df.set_index("datetime")["close"],
...     entries = pd.Series(arrays.entries, index=df.index),
...     sl_stop = pd.Series(arrays.sl_stop, index=df.index),
...     tp_stop = pd.Series(arrays.tp_stop, index=df.index),
...     ...
... )
"""

from __future__ import annotations

import logging
from typing import NamedTuple

import numpy as np
import pandas as pd
from scipy.stats import linregress

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Return type
# ─────────────────────────────────────────────────────────────────────────────

class SignalArrays(NamedTuple):
    """
    Output of every signal generator.

    entries  : bool array — True where a signal fires.
    sl_stop  : float array — stop-loss price (NaN where no signal).
    tp_stop  : float array — take-profit price (NaN where no signal).
    atr      : float array — ATR value at each bar (used externally).
    """
    entries: np.ndarray
    sl_stop: np.ndarray
    tp_stop: np.ndarray
    atr:     np.ndarray


# ─────────────────────────────────────────────────────────────────────────────
# Public router
# ─────────────────────────────────────────────────────────────────────────────

# Map strategy names to their generator functions
STRATEGY_REGISTRY: dict[str, str] = {
    "Rising Wedge":                    "wedge_rising",
    "Falling Wedge":                   "wedge_falling",
    "Symmetrical Triangle":            "triangle_symmetrical",
    "Support / Resistance Breakout":   "sr_breakout",
    "Trendline Bounce":                "trendline_bounce",
}

ALL_STRATEGIES = list(STRATEGY_REGISTRY.keys())


def generate_signals(
    strategy: str,
    df:       pd.DataFrame,
    params:   dict,
) -> SignalArrays:
    """
    Route to the correct signal generator for ``strategy``.

    Parameters
    ----------
    strategy : Strategy name — must be a key in STRATEGY_REGISTRY.
    df       : OHLCV DataFrame from OHLCVFetcher.
    params   : Parameter dict from Optuna or config.

    Returns
    -------
    SignalArrays namedtuple.

    Raises
    ------
    ValueError if strategy name is not recognised.
    """
    if strategy not in STRATEGY_REGISTRY:
        raise ValueError(
            f"Unknown strategy '{strategy}'. "
            f"Available: {list(STRATEGY_REGISTRY)}"
        )

    func_name = STRATEGY_REGISTRY[strategy]
    func      = globals()[f"_gen_{func_name}"]
    arrays    = func(df, params)

    n_signals = int(arrays.entries.sum())
    logger.debug(
        "[Signals] %s | %d signal(s) in %d candles.",
        strategy, n_signals, len(df),
    )
    return arrays


# ─────────────────────────────────────────────────────────────────────────────
# Shared Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _compute_atr(
    highs:  np.ndarray,
    lows:   np.ndarray,
    closes: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """
    Rolling Average True Range — fully vectorised via pandas rolling mean.
    Returns NaN for the first ``period`` bars.
    """
    prev_c = pd.Series(closes).shift(1)
    tr = pd.concat([
        pd.Series(highs) - pd.Series(lows),
        (pd.Series(highs) - prev_c).abs(),
        (pd.Series(lows)  - prev_c).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean().values


def _precompute_swing_highs(series: np.ndarray, window: int) -> np.ndarray:
    """
    Return sorted array of indices where series[i] is the global max within
    ±window bars. Only indices in [window, len-window) are checked, so every
    returned pivot is confirmed (no lookahead relative to the pivot bar itself).

    Vectorised via rolling max — O(n) vs the previous O(n×sw) Python loop.
    """
    s           = pd.Series(series)
    rolling_max = s.rolling(2 * window + 1, center=True).max()
    # Valid only where the full window fits (avoids NaN edges)
    mask = (s == rolling_max) & s.notna() & rolling_max.notna()
    idx  = np.where(mask.values)[0]
    # Restrict to [window, n-window) — same bounds as before
    return idx[(idx >= window) & (idx < len(series) - window)].astype(np.intp)


def _precompute_swing_lows(series: np.ndarray, window: int) -> np.ndarray:
    """
    Return sorted array of indices where series[i] is the global min within
    ±window bars. Vectorised via rolling min — O(n).
    """
    s           = pd.Series(series)
    rolling_min = s.rolling(2 * window + 1, center=True).min()
    mask = (s == rolling_min) & s.notna() & rolling_min.notna()
    idx  = np.where(mask.values)[0]
    return idx[(idx >= window) & (idx < len(series) - window)].astype(np.intp)


def _empty_arrays(n: int) -> SignalArrays:
    """Return zero-signal arrays of length n."""
    return SignalArrays(
        entries = np.zeros(n, dtype=bool),
        sl_stop = np.full(n, np.nan),
        tp_stop = np.full(n, np.nan),
        atr     = np.full(n, np.nan),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 1 & 2 — Falling Wedge (LONG) / Rising Wedge (SHORT)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_wedge_falling(df: pd.DataFrame, params: dict) -> SignalArrays:
    """
    Falling Wedge — Bullish breakout signal.

    Detection conditions (all must be satisfied):
      1. Upper trendline (swing highs) has negative slope.
      2. Lower trendline (swing lows) has negative slope.
      3. Lower trendline is steeper (converging lines).
      4. R² of both trendline fits ≥ min_r2.
      5. Current close is above the lower trendline (breakout).
      6. Current volume is ≥ breakout_vol_mult × 20-bar avg volume.
    """
    return _gen_wedge(df, params, bullish=True)


def _gen_wedge_rising(df: pd.DataFrame, params: dict) -> SignalArrays:
    """
    Rising Wedge — Bearish breakout signal.

    Detection conditions (all must be satisfied):
      1. Upper trendline has positive slope.
      2. Lower trendline has positive slope.
      3. Lower trendline is steeper (converging lines).
      4. R² of both trendline fits ≥ min_r2.
      5. Current close is below the lower trendline (breakdown).
      6. Volume surge confirmation.
    """
    return _gen_wedge(df, params, bullish=False)


def _gen_wedge(df: pd.DataFrame, params: dict, bullish: bool) -> SignalArrays:
    """Shared wedge detection logic for falling (bullish) and rising (bearish)."""
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    vols   = df["volume"].values
    n      = len(df)

    sw       = int(params.get("swing_window",       5))
    lookback = int(params.get("pattern_lookback",  50))
    min_r2   = float(params.get("min_r2",        0.80))
    vol_mult = float(params.get("breakout_vol_mult", 1.5))
    atr_sl   = float(params.get("atr_multiplier_sl", 1.5))
    atr_tp   = float(params.get("atr_multiplier_tp", 3.0))
    atr_per  = int(params.get("atr_period", 14))

    atr_arr = _compute_atr(highs, lows, closes, period=atr_per)
    vol_ma  = pd.Series(vols).rolling(20).mean().values

    # Pre-compute all confirmed pivot indices once — O(n×sw) total, not per bar.
    # A pivot at index k requires k±sw to be within bounds, so it is confirmed
    # (no lookahead) once bar k+sw has closed.  The per-bar filter below
    # enforces this by requiring pivot_index < i - sw.
    all_sh = _precompute_swing_highs(highs, sw)
    all_sl = _precompute_swing_lows (lows,  sw)

    entries = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan)
    tp_stop = np.full(n, np.nan)

    for i in range(lookback, n):
        # Skip if ATR or vol MA not yet available
        if np.isnan(atr_arr[i]) or np.isnan(vol_ma[i]) or vol_ma[i] == 0:
            continue

        # Volume confirmation — must check BEFORE expensive regression
        vol_ratio = vols[i] / vol_ma[i]
        if vol_ratio < vol_mult:
            continue

        # Pivots within window [i-lookback, i-sw) — binary-search O(log p).
        # Upper bound i-sw ensures each pivot is confirmed (k+sw < i).
        lo_h = np.searchsorted(all_sh, i - lookback)
        hi_h = np.searchsorted(all_sh, i - sw)
        sh   = all_sh[lo_h:hi_h]

        lo_l = np.searchsorted(all_sl, i - lookback)
        hi_l = np.searchsorted(all_sl, i - sw)
        sl   = all_sl[lo_l:hi_l]

        if len(sh) < 3 or len(sl) < 3:
            continue

        # Convert global indices to window-local (0…lookback-1) for regression
        offset = i - lookback
        x_h = sh[-3:] - offset;  y_h = highs[sh[-3:]]
        x_l = sl[-3:] - offset;  y_l = lows [sl[-3:]]

        slope_h, _,      r_h, _, _ = linregress(x_h, y_h)
        slope_l, icpt_l, r_l, _, _ = linregress(x_l, y_l)

        if r_h ** 2 < min_r2 or r_l ** 2 < min_r2:
            continue

        # Extrapolate lower trendline to the last bar in the window (index lookback-1)
        lower_now = slope_l * (lookback - 1) + icpt_l
        current   = closes[i - 1]   # last completed bar in window
        atr       = atr_arr[i]

        if bullish:
            # Falling wedge: both slopes negative, lower steeper, close above
            if (slope_h < 0 and slope_l < 0
                    and abs(slope_l) > abs(slope_h)
                    and current > lower_now):
                entries[i] = True
                sl_stop[i] = current - atr_sl * atr
                tp_stop[i] = current + atr_tp * atr
        else:
            # Rising wedge: both slopes positive, lower steeper, close below
            if (slope_h > 0 and slope_l > 0
                    and slope_l > slope_h
                    and current < lower_now):
                entries[i] = True
                sl_stop[i] = current + atr_sl * atr   # short stop above
                tp_stop[i] = current - atr_tp * atr   # short TP below

    return SignalArrays(entries, sl_stop, tp_stop, atr_arr)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 3 — Symmetrical Triangle
# ─────────────────────────────────────────────────────────────────────────────

def _gen_triangle_symmetrical(df: pd.DataFrame, params: dict) -> SignalArrays:
    """
    Symmetrical Triangle — Continuation breakout.

    Detection:
      Descending highs + ascending lows converging to an apex.
      Breakout direction follows the prior trend (20-bar slope).

    Entry fires when:
      1. Upper slope < 0, lower slope > 0, both R² ≥ min_r2.
      2. Volume surge ≥ vol_mult.
      3. Close breaks above (bullish) or below (bearish) the triangle.
    """
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    vols   = df["volume"].values
    n      = len(df)

    sw       = int(params.get("swing_window",       5))
    lookback = int(params.get("pattern_lookback",  50))
    min_r2   = float(params.get("min_r2",        0.80))
    vol_mult = float(params.get("breakout_vol_mult", 1.5))
    atr_sl   = float(params.get("atr_multiplier_sl", 1.5))
    atr_tp   = float(params.get("atr_multiplier_tp", 3.0))
    atr_per  = int(params.get("atr_period", 14))
    trend_period = 20   # bars for prior-trend direction

    atr_arr = _compute_atr(highs, lows, closes, period=atr_per)
    vol_ma  = pd.Series(vols).rolling(20).mean().values

    all_sh = _precompute_swing_highs(highs, sw)
    all_sl = _precompute_swing_lows (lows,  sw)

    entries = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan)
    tp_stop = np.full(n, np.nan)

    for i in range(lookback, n):
        if np.isnan(atr_arr[i]) or np.isnan(vol_ma[i]) or vol_ma[i] == 0:
            continue

        vol_ratio = vols[i] / vol_ma[i]
        if vol_ratio < vol_mult:
            continue

        lo_h = np.searchsorted(all_sh, i - lookback)
        hi_h = np.searchsorted(all_sh, i - sw)
        sh   = all_sh[lo_h:hi_h]

        lo_l = np.searchsorted(all_sl, i - lookback)
        hi_l = np.searchsorted(all_sl, i - sw)
        sl   = all_sl[lo_l:hi_l]

        if len(sh) < 3 or len(sl) < 3:
            continue

        offset = i - lookback
        x_h = sh[-3:] - offset;  y_h = highs[sh[-3:]]
        x_l = sl[-3:] - offset;  y_l = lows [sl[-3:]]

        slope_h, icpt_h, r_h, _, _ = linregress(x_h, y_h)
        slope_l, icpt_l, r_l, _, _ = linregress(x_l, y_l)

        if r_h ** 2 < min_r2 or r_l ** 2 < min_r2:
            continue

        # Triangle condition: descending highs + ascending lows
        if not (slope_h < 0 and slope_l > 0):
            continue

        upper_now = slope_h * (lookback - 1) + icpt_h
        lower_now = slope_l * (lookback - 1) + icpt_l
        current   = closes[i - 1]
        atr       = atr_arr[i]

        # Prior trend direction from 20-bar simple slope
        prior_trend = closes[i] - closes[max(0, i - trend_period)]
        bullish     = prior_trend > 0

        if bullish and current > upper_now:
            entries[i] = True
            sl_stop[i] = current - atr_sl * atr
            tp_stop[i] = current + atr_tp * atr
        elif not bullish and current < lower_now:
            entries[i] = True
            sl_stop[i] = current + atr_sl * atr
            tp_stop[i] = current - atr_tp * atr

    return SignalArrays(entries, sl_stop, tp_stop, atr_arr)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 4 — Support / Resistance Breakout
# ─────────────────────────────────────────────────────────────────────────────

def _gen_sr_breakout(df: pd.DataFrame, params: dict) -> SignalArrays:
    """
    Support / Resistance Breakout.

    Level Detection:
      Resistance = rolling 95th percentile of closes over ``lookback`` bars.
      Support    = rolling  5th percentile of closes over ``lookback`` bars.

    Entry fires when:
      1. Close is beyond the level by ≥ tolerance (ATR × 0.3).
      2. Volume surge ≥ vol_mult × 20-bar avg volume.

    Note: Using rolling percentiles rather than fixed horizontal levels
    avoids lookahead bias — the percentile at bar i is computed only
    from bars [i-lookback : i].
    """
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    vols   = df["volume"].values
    n      = len(df)

    lookback = int(params.get("pattern_lookback",  50))
    vol_mult = float(params.get("breakout_vol_mult", 1.5))
    atr_sl   = float(params.get("atr_multiplier_sl", 1.5))
    atr_tp   = float(params.get("atr_multiplier_tp", 3.0))
    atr_per  = int(params.get("atr_period", 14))

    atr_arr = _compute_atr(highs, lows, closes, period=atr_per)
    vol_ma  = pd.Series(vols).rolling(20).mean().values

    # Rolling percentile levels (fully vectorised via pandas)
    close_s    = pd.Series(closes)
    resistance = close_s.rolling(lookback).quantile(0.95).values
    support    = close_s.rolling(lookback).quantile(0.05).values

    entries = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan)
    tp_stop = np.full(n, np.nan)

    for i in range(lookback, n):
        if (np.isnan(atr_arr[i]) or np.isnan(vol_ma[i]) or vol_ma[i] == 0
                or np.isnan(resistance[i]) or np.isnan(support[i])):
            continue

        vol_ratio = vols[i] / vol_ma[i]
        if vol_ratio < vol_mult:
            continue

        current   = closes[i]
        atr       = atr_arr[i]
        tolerance = atr * 0.3

        if current > resistance[i] + tolerance:
            entries[i] = True
            sl_stop[i] = current - atr_sl * atr
            tp_stop[i] = current + atr_tp * atr

        elif current < support[i] - tolerance:
            entries[i] = True
            sl_stop[i] = current + atr_sl * atr
            tp_stop[i] = current - atr_tp * atr

    return SignalArrays(entries, sl_stop, tp_stop, atr_arr)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 5 — Trendline Bounce
# ─────────────────────────────────────────────────────────────────────────────

def _gen_trendline_bounce(df: pd.DataFrame, params: dict) -> SignalArrays:
    """
    Trendline Bounce — Trend-following entry on trendline touch.

    Detection:
      Fit a linear regression through the last ``lookback`` closes.
      If R² ≥ min_r2, the trend is considered reliable.
      A signal fires when price touches the regression line
      within a tolerance band of ± (ATR × 0.5).

    Long signal:  upward trend + price touches line from above.
    Short signal: downward trend + price touches line from below.
    """
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    n      = len(df)

    lookback = int(params.get("pattern_lookback",  50))
    min_r2   = float(params.get("min_r2",        0.80))
    atr_sl   = float(params.get("atr_multiplier_sl", 1.5))
    atr_tp   = float(params.get("atr_multiplier_tp", 3.0))
    atr_per  = int(params.get("atr_period", 14))
    touch_mult = float(params.get("touch_tolerance_atr", 0.5))

    atr_arr = _compute_atr(highs, lows, closes, period=atr_per)

    entries = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan)
    tp_stop = np.full(n, np.nan)

    x_base = np.arange(lookback)   # re-used across iterations

    for i in range(lookback, n):
        if np.isnan(atr_arr[i]):
            continue

        w_c     = closes[i - lookback: i]
        slope, intercept, r, _, _ = linregress(x_base, w_c)

        if r ** 2 < min_r2:
            continue

        trendline_now = slope * (lookback - 1) + intercept
        current       = closes[i]
        atr           = atr_arr[i]
        tolerance     = atr * touch_mult

        if abs(current - trendline_now) > tolerance:
            continue   # price not near the trendline

        if slope > 0 and current >= trendline_now:
            # Uptrend + price touching line from above → LONG
            entries[i] = True
            sl_stop[i] = current - atr_sl * atr
            tp_stop[i] = current + atr_tp * atr

        elif slope < 0 and current <= trendline_now:
            # Downtrend + price touching line from below → SHORT
            entries[i] = True
            sl_stop[i] = current + atr_sl * atr
            tp_stop[i] = current - atr_tp * atr

    return SignalArrays(entries, sl_stop, tp_stop, atr_arr)
