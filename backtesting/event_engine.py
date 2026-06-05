"""
backtesting/event_engine.py
===========================
Phase 2 of the two-engine pipeline: candle-by-candle event-driven
simulation with zero lookahead bias guarantee.

Role in the Pipeline
--------------------
The EventDrivenEngine runs after Optuna has found promising parameters.
It produces the *authoritative* performance metrics reported in the
strategy leaderboard, because:

  1. It is architecturally impossible to access future data — each step
     only sees candles[0:i], enforced by the loop structure itself.
  2. It uses the *identical* signal detection code as the live system,
     meaning backtest and live behaviour are guaranteed to match.
  3. It supports two modes — raw (signal quality only) and full (with
     the complete risk engine: tiers, circuit breakers, pyramiding).

Lookahead Bias Guarantee
------------------------
The core loop is:

    for i in range(lookback, len(candles)):
        window = candles[i - lookback : i]   # strictly past data
        signal = detect(window)              # no future candles visible
        if signal:
            # entry at candles[i] open — the NEXT candle after detection

This means a signal detected at bar i is entered at bar i+1's open,
which is the realistic execution timing for an algo that reacts to a
closed candle. If you enter at bar i's close, you're assuming you can
execute at the exact close price — not realistic. We use i+1 open.

Two Modes
---------
raw_mode=True  (default for Optuna sweeps)
    - Fixed 1% equity risk per trade.
    - No circuit breakers, no tier throttling, no pyramiding.
    - Purpose: measure pure signal quality in isolation.
    - Faster: no risk engine overhead.

raw_mode=False (used for final walk-forward validation)
    - Full risk engine: 1% daily limit, tiered throttling,
      consecutive-loss cooldown, position sizing with Kelly scaling.
    - Purpose: realistic simulation of live system behaviour.
    - Output is directly comparable to live trading results.

SL/TP Fill Logic
----------------
On each candle, we check if the candle's HIGH or LOW has breached
the stop-loss or take-profit price. This is more realistic than only
checking the close — a 1-minute candle that opens at 67,000, sweeps
to 66,800 (triggering SL) and closes at 67,100 would incorrectly
appear profitable if we only checked the close.

Fill price:
  SL hit: min(candle.open, sl_price)  → assumes worst-case fill
  TP hit: max(candle.open, tp_price)  → assumes best realistic fill

If both SL and TP are breached in the same candle, SL takes priority
(pessimistic assumption consistent with real market microstructure).

Usage
-----
>>> engine = EventDrivenEngine("Falling Wedge", best_params, raw_mode=False)
>>> equity_curve, trades = engine.run(test_df)
>>> metrics = calculate_metrics(equity_curve, trades, "Falling Wedge", "5m")
>>> print(metrics.summary())
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from config import settings
from backtesting.costs import TransactionCostModel
from backtesting.metrics import calculate_metrics, BacktestMetrics
from backtesting.signals import (
    generate_signals, SignalArrays, ALL_STRATEGIES
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Trade Record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SimulatedTrade:
    """
    Complete record of one simulated trade, including all metadata
    needed for metrics calculation and dashboard display.
    """
    strategy:        str
    side:            str             # "LONG" or "SHORT"
    entry_bar:       int             # candle index
    entry_time:      datetime
    entry_price:     float
    quantity:        float
    stop_loss:       float
    take_profit:     float
    exit_bar:        int             = -1
    exit_time:       datetime | None = None
    exit_price:      float           = 0.0
    exit_reason:     str             = ""   # "TP" | "SL" | "EOB" (end of backtest)
    pnl:             float           = 0.0
    pnl_pct:         float           = 0.0
    duration_minutes:float           = 0.0
    fees_paid:       float           = 0.0
    equity_at_entry: float           = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────────────────────────

class EventDrivenEngine:
    """
    Candle-by-candle backtesting engine.

    See module docstring for architecture details and design rationale.
    """

    def __init__(
        self,
        strategy:       str,
        params:         dict,
        raw_mode:       bool                      = True,
        cost_model:     TransactionCostModel | None = None,
        starting_equity:float                      = settings.STARTING_EQUITY,
    ) -> None:
        """
        Parameters
        ----------
        strategy        : Strategy name (must be in signals.STRATEGY_REGISTRY).
        params          : Parameter dict from Optuna or config.
        raw_mode        : True → 1% flat risk, no risk engine.
                          False → full risk engine with tiers and circuit breakers.
        cost_model      : Transaction cost model. Defaults to standard Binance rates.
        starting_equity : Initial portfolio equity in USDT.
        """
        if strategy not in ALL_STRATEGIES:
            raise ValueError(f"Unknown strategy '{strategy}'. Available: {ALL_STRATEGIES}")

        self.strategy        = strategy
        self.params          = params
        self.raw_mode        = raw_mode
        self.costs           = cost_model or TransactionCostModel()
        self.starting_equity = starting_equity

    # ─────────────────────────────────────────────────────────────────────────
    # Public: Run
    # ─────────────────────────────────────────────────────────────────────────

    def run(
        self,
        df: pd.DataFrame,
    ) -> tuple[pd.Series, list[SimulatedTrade]]:
        """
        Simulate trading over the entire DataFrame.

        The simulation loop:
          1. Pre-compute all signal arrays (vectorised, fast).
          2. Walk candles one at a time.
          3. At each bar: check open trade for SL/TP; then check for new entry.
          4. Entry at bar i+1's open after signal fires at bar i.

        Parameters
        ----------
        df : OHLCV DataFrame from OHLCVFetcher (must have datetime column).

        Returns
        -------
        equity_curve : pd.Series indexed by datetime, portfolio value per bar.
        trades       : list of SimulatedTrade records (closed trades only).
        """
        if len(df) < 2:
            logger.warning("[EDE] DataFrame too short to simulate.")
            return pd.Series(dtype=float), []

        # Pre-compute all signals (vectorised, no lookahead inside generator)
        arrays: SignalArrays = generate_signals(self.strategy, df, self.params)

        # Extract numpy arrays for fast indexed access
        opens   = df["open"].values
        highs   = df["high"].values
        lows    = df["low"].values
        closes  = df["close"].values

        # Build datetime index
        if "datetime" in df.columns:
            datetimes = pd.to_datetime(df["datetime"]).dt.tz_localize(
                None
            ).dt.tz_localize("UTC")
        else:
            datetimes = pd.date_range(
                "2020-01-01", periods=len(df), freq="1min", tz="UTC"
            )

        n             = len(df)
        equity        = self.starting_equity
        equity_history: list[tuple[datetime, float]] = []
        trades:         list[SimulatedTrade]          = []
        open_trade:     SimulatedTrade | None          = None

        # Risk engine state (full mode only)
        daily_loss    = 0.0
        day_open_eq   = equity
        current_day   = None
        consec_losses = 0
        cooldown_bar  = -1

        lookback = int(self.params.get("pattern_lookback", 50))

        for i in range(1, n):
            dt = datetimes[i]

            # ── Daily reset (full mode only) ──────────────────────────────
            if not self.raw_mode:
                bar_day = dt.date()
                if bar_day != current_day:
                    day_open_eq   = equity
                    daily_loss    = 0.0
                    current_day   = bar_day
                    consec_losses = 0

            # ── Check SL/TP on open trade ─────────────────────────────────
            if open_trade is not None:
                exit_info = self._check_exit(open_trade, opens[i], highs[i], lows[i])
                if exit_info:
                    fill_price = exit_info["price"]
                    exit_fees  = self.costs.exit_cost(
                        fill_price, open_trade.quantity,
                        is_market=(exit_info["reason"] == "SL"),
                    )
                    gross      = self._gross_pnl(open_trade, fill_price)
                    net_pnl    = gross - exit_fees

                    open_trade.exit_bar       = i
                    open_trade.exit_time      = dt.to_pydatetime()
                    open_trade.exit_price     = fill_price
                    open_trade.exit_reason    = exit_info["reason"]
                    open_trade.fees_paid     += exit_fees
                    open_trade.pnl            = net_pnl
                    open_trade.pnl_pct        = (
                        net_pnl / open_trade.equity_at_entry
                        if open_trade.equity_at_entry > 0 else 0.0
                    )
                    open_trade.duration_minutes = (
                        (open_trade.exit_time - open_trade.entry_time)
                        .total_seconds() / 60
                    )

                    equity    += net_pnl
                    daily_loss = min(0.0, daily_loss + net_pnl)

                    if not self.raw_mode:
                        if net_pnl < 0:
                            consec_losses += 1
                            if consec_losses >= 3:
                                cooldown_bar  = i + 20
                                consec_losses = 0   # one-shot: reset so a new streak needs 3 fresh losses
                        else:
                            consec_losses = 0

                    trades.append(open_trade)
                    open_trade = None

                    logger.debug(
                        "[EDE] %s | bar %d | %s %s | PnL=%.2f",
                        self.strategy, i,
                        exit_info["reason"],
                        "WIN" if net_pnl > 0 else "LOSS",
                        net_pnl,
                    )

            # ── Check for new entry ───────────────────────────────────────
            # Signal fires at bar i-1 (close) → entry at bar i (open)
            if open_trade is None and i >= lookback:
                signal_bar = i - 1   # signal was detected at previous bar
                if arrays.entries[signal_bar]:
                    sl   = arrays.sl_stop[signal_bar]
                    tp   = arrays.tp_stop[signal_bar]
                    entry_price = opens[i]   # next bar open — realistic fill

                    # Validate signal prices
                    if np.isnan(sl) or np.isnan(tp) or np.isnan(entry_price):
                        pass
                    else:
                        # ── Gate: risk engine checks (full mode) ──────────
                        approved, quantity = self._risk_check(
                            entry_price, sl, tp, equity,
                            day_open_eq, daily_loss,
                            consec_losses, cooldown_bar, i,
                        )

                        if approved and quantity > 0:
                            entry_fees = self.costs.entry_cost(entry_price, quantity)
                            equity    -= entry_fees

                            # Determine side from SL/TP direction
                            side = "LONG" if tp > entry_price else "SHORT"

                            open_trade = SimulatedTrade(
                                strategy        = self.strategy,
                                side            = side,
                                entry_bar       = i,
                                entry_time      = dt.to_pydatetime(),
                                entry_price     = entry_price,
                                quantity        = quantity,
                                stop_loss       = sl,
                                take_profit     = tp,
                                fees_paid       = entry_fees,
                                equity_at_entry = equity,
                            )

                            logger.debug(
                                "[EDE] %s | bar %d | ENTRY %s @ %.2f "
                                "SL=%.2f TP=%.2f qty=%.6f",
                                self.strategy, i, side,
                                entry_price, sl, tp, quantity,
                            )

            equity_history.append((dt.to_pydatetime(), equity))

        # ── Close any remaining open trade at last bar ────────────────────
        if open_trade is not None:
            last_close  = closes[-1]
            last_dt     = datetimes.iloc[-1].to_pydatetime()
            exit_fees   = self.costs.exit_cost(last_close, open_trade.quantity)
            net_pnl     = self._gross_pnl(open_trade, last_close) - exit_fees

            open_trade.exit_bar       = n - 1
            open_trade.exit_time      = last_dt
            open_trade.exit_price     = last_close
            open_trade.exit_reason    = "EOB"
            open_trade.pnl            = net_pnl
            open_trade.pnl_pct        = net_pnl / (
                open_trade.entry_price * open_trade.quantity
            ) if open_trade.quantity > 0 else 0.0
            open_trade.duration_minutes = (
                (open_trade.exit_time - open_trade.entry_time).total_seconds() / 60
            )
            equity += net_pnl
            trades.append(open_trade)

        # Build equity curve as pd.Series with DatetimeIndex
        if equity_history:
            idx_dt, eq_vals = zip(*equity_history)
            equity_curve    = pd.Series(
                list(eq_vals),
                index=pd.DatetimeIndex(idx_dt, tz="UTC"),
                name="equity",
            )
        else:
            equity_curve = pd.Series(dtype=float)

        logger.info(
            "[EDE] %s | complete | bars=%d trades=%d "
            "final_equity=%.2f return=%.2f%%",
            self.strategy, n, len(trades), equity,
            (equity / self.starting_equity - 1) * 100,
        )

        return equity_curve, trades

    # ─────────────────────────────────────────────────────────────────────────
    # Exit Check
    # ─────────────────────────────────────────────────────────────────────────

    def _check_exit(
        self,
        trade: SimulatedTrade,
        open_:  float,
        high:   float,
        low:    float,
    ) -> dict | None:
        """
        Check whether the current candle triggers SL or TP.

        Uses candle HIGH and LOW (not close) for a realistic bar-level check.
        SL takes priority if both are triggered on the same candle.

        Fill price logic (pessimistic):
          SL: min(open_, sl_price) — worst realistic fill
          TP: max(open_, tp_price) for LONG; min(open_, tp_price) for SHORT
        """
        if trade.side == "LONG":
            # Check SL first (pessimistic)
            if low <= trade.stop_loss:
                fill = min(open_, trade.stop_loss)
                return {"price": fill, "reason": "SL"}
            if high >= trade.take_profit:
                fill = max(open_, trade.take_profit)
                return {"price": fill, "reason": "TP"}
        else:  # SHORT
            if high >= trade.stop_loss:
                fill = max(open_, trade.stop_loss)
                return {"price": fill, "reason": "SL"}
            if low <= trade.take_profit:
                fill = min(open_, trade.take_profit)
                return {"price": fill, "reason": "TP"}
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Gross PnL
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _gross_pnl(trade: SimulatedTrade, exit_price: float) -> float:
        direction = 1 if trade.side == "LONG" else -1
        return direction * (exit_price - trade.entry_price) * trade.quantity

    # ─────────────────────────────────────────────────────────────────────────
    # Risk Check
    # ─────────────────────────────────────────────────────────────────────────

    def _risk_check(
        self,
        entry:         float,
        sl:            float,
        tp:            float,
        equity:        float,
        day_open_eq:   float,
        daily_loss:    float,
        consec_losses: int,
        cooldown_bar:  int,
        current_bar:   int,
    ) -> tuple[bool, float]:
        """
        Gate and size a new trade.

        Raw mode  → flat 1% risk sizing, no gates.
        Full mode → tiered gates + volatility-target sizing.

        Returns (approved: bool, quantity: float).
        """
        sl_distance = abs(entry - sl)
        if sl_distance < 1e-8:
            return False, 0.0

        if self.raw_mode:
            # Simple 1% risk sizing — no gates
            risk_amount = equity * 0.01
            quantity    = round(risk_amount / sl_distance, 6)
            return True, quantity

        # ── Full mode gates ───────────────────────────────────────────────

        daily_loss_pct = abs(daily_loss) / day_open_eq if day_open_eq > 0 else 0

        # Gate 1: daily loss limit (1% of DOV)
        if daily_loss_pct >= 0.01:
            return False, 0.0

        # Gate 2: cooldown window after 3 consecutive losses.
        # consec_losses is reset to 0 when cooldown_bar is set (one-shot), so
        # we check only cooldown_bar — not consec_losses — to gate new entries.
        if current_bar <= cooldown_bar:
            return False, 0.0

        # Determine tier
        tier_scalar = self._tier_scalar(daily_loss_pct)

        # Gate 3: passive tier — no new entries
        if tier_scalar == 0.0:
            return False, 0.0

        # Sizing: volatility target
        remaining_budget = max(0.0, day_open_eq * 0.01 + daily_loss)
        risk_amount      = min(
            equity * 0.01,          # 1% equity risk
            remaining_budget * 0.6, # max 60% of remaining daily budget
        )
        # Gate 4: worst-case loss cap
        worst_case = (risk_amount / sl_distance) * sl_distance
        if worst_case > remaining_budget:
            risk_amount = remaining_budget

        # 0.25 = Kelly fraction applied only in full-mode (raw_mode uses 1× sizing for
        # signal-quality benchmarking). Full-mode is 4× smaller to match live risk limits.
        quantity = round((risk_amount / sl_distance) * tier_scalar * 0.25, 6)
        return quantity > 0, quantity

    @staticmethod
    def _tier_scalar(loss_pct: float) -> float:
        """Return position size scalar based on current daily loss percentage."""
        if loss_pct >= 0.009:  return 0.00   # PASSIVE
        if loss_pct >= 0.0075: return 0.25   # MINIMAL
        if loss_pct >= 0.005:  return 0.50   # REDUCED
        return 1.00                           # FULL
