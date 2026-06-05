"""
tests/test_bt_event_engine.py
=============================
Unit tests for backtesting/event_engine.py.

Coverage:
  - _check_exit: SL/TP precedence, LONG/SHORT fill logic
  - _tier_scalar: all four tier boundaries
  - _risk_check: raw mode sizing, full-mode daily limit gate,
                 cooldown gate, tier-scaled sizing
  - Full engine run (mocked signals): daily consec_losses reset,
    cooldown suspension, end-of-backtest close
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from backtesting.event_engine import EventDrivenEngine, SimulatedTrade
from backtesting.signals import SignalArrays


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _engine(raw_mode: bool = True, equity: float = 10_000.0) -> EventDrivenEngine:
    """Minimal engine instance with default params."""
    return EventDrivenEngine(
        strategy        = "Falling Wedge",
        params          = {"pattern_lookback": 5},
        raw_mode        = raw_mode,
        starting_equity = equity,
    )


def _trade(
    entry: float = 50_000.0,
    sl:    float = 49_000.0,
    tp:    float = 52_000.0,
    side:  str   = "LONG",
) -> SimulatedTrade:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return SimulatedTrade(
        strategy    = "Falling Wedge",
        side        = side,
        entry_bar   = 0,
        entry_time  = now,
        entry_price = entry,
        quantity    = 0.1,
        stop_loss   = sl,
        take_profit = tp,
    )


def _make_df(n: int, start: str = "2024-01-01 00:00") -> pd.DataFrame:
    """Flat OHLCV DataFrame — all candles near 50 000, no signals fire."""
    ts0   = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
    dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    return pd.DataFrame({
        "timestamp": [ts0 + i * 60_000 for i in range(n)],
        "open":      50_000.0,
        "high":      50_010.0,
        "low":       49_990.0,
        "close":     50_000.0,
        "volume":    1.0,
        "datetime":  dates,
    })


def _signal_arrays(
    n:           int,
    entry_bars:  list[int],
    sl:          float = 49_000.0,
    tp:          float = 52_000.0,
) -> SignalArrays:
    """SignalArrays with entry signals at specified bars."""
    entries = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan)
    tp_stop = np.full(n, np.nan)
    atr     = np.full(n, 100.0)
    for b in entry_bars:
        entries[b] = True
        sl_stop[b] = sl
        tp_stop[b] = tp
    return SignalArrays(entries=entries, sl_stop=sl_stop, tp_stop=tp_stop, atr=atr)


# ─────────────────────────────────────────────────────────────────────────────
# _check_exit — fill logic and SL/TP precedence
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckExit:
    def test_long_sl_hit_below_open(self):
        """LONG SL fills at min(open, sl_price) — gap-down open below SL."""
        eng  = _engine()
        t    = _trade(entry=50_000, sl=49_000, tp=52_000, side="LONG")
        info = eng._check_exit(t, open_=48_500, high=49_200, low=48_400)
        assert info["reason"] == "SL"
        assert info["price"] == min(48_500, 49_000)   # 48 500

    def test_long_tp_fill_at_tp_price(self):
        """LONG TP fills at max(open, tp_price)."""
        eng  = _engine()
        t    = _trade(entry=50_000, sl=49_000, tp=52_000, side="LONG")
        info = eng._check_exit(t, open_=50_100, high=52_500, low=50_000)
        assert info["reason"] == "TP"
        assert info["price"] == max(50_100, 52_000)   # 52 000

    def test_long_sl_wins_when_both_hit_same_bar(self):
        """SL takes priority if both SL and TP are breached on the same bar."""
        eng  = _engine()
        t    = _trade(entry=50_000, sl=49_000, tp=52_000, side="LONG")
        info = eng._check_exit(t, open_=50_000, high=53_000, low=48_000)
        assert info["reason"] == "SL", "SL must win over TP on same-bar conflict"

    def test_short_sl_wins_when_both_hit_same_bar(self):
        """SHORT: SL (high >= sl_price) takes priority over TP (low <= tp_price)."""
        eng  = _engine()
        t    = _trade(entry=50_000, sl=51_000, tp=48_000, side="SHORT")
        info = eng._check_exit(t, open_=50_000, high=52_000, low=47_000)
        assert info["reason"] == "SL"

    def test_no_exit_when_price_inside_range(self):
        """No exit if neither SL nor TP is touched."""
        eng  = _engine()
        t    = _trade(entry=50_000, sl=49_000, tp=52_000, side="LONG")
        info = eng._check_exit(t, open_=50_100, high=50_500, low=49_500)
        assert info is None


# ─────────────────────────────────────────────────────────────────────────────
# _tier_scalar — all four tier boundaries (>= makes boundaries inclusive
# to the MORE restrictive tier, not the less restrictive one)
# ─────────────────────────────────────────────────────────────────────────────

class TestTierScalar:
    @pytest.mark.parametrize("loss_pct,expected", [
        (0.000,  1.00),   # FULL    — no loss yet
        (0.0049, 1.00),   # FULL    — just below REDUCED threshold
        (0.005,  0.50),   # REDUCED — exactly at boundary (>= 0.005)
        (0.0074, 0.50),   # REDUCED — just below MINIMAL threshold
        (0.0075, 0.25),   # MINIMAL — exactly at boundary (>= 0.0075)
        (0.0089, 0.25),   # MINIMAL — just below PASSIVE threshold
        (0.009,  0.00),   # PASSIVE — exactly at boundary (>= 0.009)
        (0.020,  0.00),   # PASSIVE — well past limit
    ])
    def test_tier_boundaries(self, loss_pct, expected):
        assert EventDrivenEngine._tier_scalar(loss_pct) == expected


# ─────────────────────────────────────────────────────────────────────────────
# _risk_check — gates and sizing
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskCheck:
    def test_raw_mode_always_approves(self):
        """raw_mode=True: all gates bypassed, 1% equity risk sizing."""
        eng = _engine(raw_mode=True, equity=10_000)
        approved, qty = eng._risk_check(
            entry=50_000, sl=49_000, tp=52_000,
            equity=10_000, day_open_eq=10_000, daily_loss=0.0,
            consec_losses=10, cooldown_bar=9999, current_bar=5,
        )
        assert approved
        # 1% of 10 000 = 100 USDT risk; SL distance = 1 000 → qty = 0.1
        assert abs(qty - 0.1) < 1e-6

    def test_full_mode_daily_limit_gate(self):
        """Full mode blocks entry when daily loss ≥ 1% of day-open equity."""
        eng = _engine(raw_mode=False, equity=9_900)
        approved, qty = eng._risk_check(
            entry=50_000, sl=49_000, tp=52_000,
            equity=9_900, day_open_eq=10_000, daily_loss=-100.0,   # exactly 1%
            consec_losses=0, cooldown_bar=-1, current_bar=5,
        )
        assert not approved
        assert qty == 0.0

    def test_full_mode_cooldown_gate(self):
        """Full mode blocks entry when current_bar is within the cooldown window."""
        eng = _engine(raw_mode=False)
        # After 3 losses consec_losses is reset to 0 — gate checks cooldown_bar only
        approved, qty = eng._risk_check(
            entry=50_000, sl=49_000, tp=52_000,
            equity=10_000, day_open_eq=10_000, daily_loss=-20.0,
            consec_losses=0, cooldown_bar=30, current_bar=25,   # inside cooldown
        )
        assert not approved

    def test_full_mode_cooldown_lifts_after_bar(self):
        """Entry is approved again once current_bar > cooldown_bar."""
        eng = _engine(raw_mode=False)
        approved, qty = eng._risk_check(
            entry=50_000, sl=49_000, tp=52_000,
            equity=10_000, day_open_eq=10_000, daily_loss=-20.0,
            consec_losses=0, cooldown_bar=30, current_bar=31,   # past cooldown
        )
        assert approved
        assert qty > 0

    def test_full_mode_zero_sl_distance_rejected(self):
        """Entry rejected when SL == entry (division by zero guard)."""
        eng = _engine(raw_mode=False)
        approved, qty = eng._risk_check(
            entry=50_000, sl=50_000, tp=52_000,
            equity=10_000, day_open_eq=10_000, daily_loss=0.0,
            consec_losses=0, cooldown_bar=-1, current_bar=1,
        )
        assert not approved


# ─────────────────────────────────────────────────────────────────────────────
# Full engine run — via mocked generate_signals
#
# Entry timing note: signal fires at bar b → engine enters at bar b+1 open
# (signal_bar = i-1 is checked at step i).  SL is then checked starting at
# bar b+2 (step i=b+2).  To force an SL hit, set low[b+2] < sl_price.
# ─────────────────────────────────────────────────────────────────────────────

class TestFullEngineRun:

    def test_consecutive_loss_cooldown_suspends_entries(self):
        """
        3 losing trades → cooldown_bar = i + 20.
        A 4th signal within the cooldown window must be blocked.
        Total closed trades must equal 3, not 4.
        """
        n = 200
        df = _make_df(n)

        # 3 signals that produce SL hits + 1 signal inside the cooldown window
        # Signals at bars 2, 12, 22 → entries at 3, 13, 23 → SL at 4, 14, 24
        # After bar 24: cooldown_bar = 44.  Signal at bar 30 → entry at bar 31
        # → current_bar=31 <= cooldown_bar=44 → blocked.
        signal_bars = [2, 12, 22, 30]
        sl_price    = 49_500.0   # below default low=49_990

        arrays = _signal_arrays(n, signal_bars, sl=sl_price, tp=55_000.0)

        df = df.copy()
        # Force SL at bar b+2 (= entry bar + 1 check iteration)
        for b in [2, 12, 22]:
            df.loc[b + 2, "low"]  = 49_400.0
            df.loc[b + 2, "high"] = 50_050.0

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=False, starting_equity=10_000.0,
            )
            _, trades = eng.run(df)

        sl_trades = [t for t in trades if t.exit_reason == "SL"]
        assert len(sl_trades) == 3, (
            f"Expected 3 SL trades (4th blocked by cooldown), got {len(sl_trades)}"
        )

    def test_daily_reset_clears_consecutive_losses(self):
        """
        2 losses on day 1, midnight crossing, 2 losses on day 2.
        Without daily reset, bar-3 streak triggers cooldown and blocks signal 32.
        With reset, each day starts fresh — all 4 trades must execute.
        """
        n = 400
        start = "2024-01-01 23:50"
        ts0   = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
        dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "timestamp": [ts0 + i * 60_000 for i in range(n)],
            "open":  50_000.0, "high": 50_010.0,
            "low":   49_990.0, "close": 50_000.0, "volume": 1.0,
            "datetime": dates,
        })

        # Bar 10 = "2024-01-02 00:00" (midnight).
        # Signals 2 and 12 land on day 1; signals 22 and 32 on day 2.
        signal_bars = [2, 12, 22, 32]
        sl_price    = 49_500.0
        arrays = _signal_arrays(n, signal_bars, sl=sl_price, tp=55_000.0)

        df = df.copy()
        for b in signal_bars:
            df.loc[b + 2, "low"]  = 49_400.0
            df.loc[b + 2, "high"] = 50_050.0

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=False, starting_equity=10_000.0,
            )
            _, trades = eng.run(df)

        assert len(trades) == 4, (
            f"Expected 4 trades (daily reset prevents cross-day streak), "
            f"got {len(trades)}."
        )

    def test_end_of_backtest_closes_open_trade(self):
        """A trade still open at the last bar must be closed at EOB."""
        n  = 20
        df = _make_df(n)
        # Signal at bar 5 → entry at bar 6 → TP far away → stays open
        arrays = _signal_arrays(n, entry_bars=[5], sl=49_000.0, tp=55_000.0)

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=True, starting_equity=10_000.0,
            )
            _, trades = eng.run(df)

        assert len(trades) == 1
        assert trades[0].exit_reason == "EOB"

    def test_raw_mode_no_daily_gate(self):
        """
        In raw mode, entries are never blocked by daily loss limits.
        5 SL signals — all 5 must produce closed trades.
        """
        n = 200
        df = _make_df(n)
        signal_bars = [2, 22, 42, 62, 82]
        sl_price    = 49_500.0
        arrays = _signal_arrays(n, signal_bars, sl=sl_price, tp=55_000.0)

        df = df.copy()
        for b in signal_bars:
            df.loc[b + 2, "low"]  = 49_400.0
            df.loc[b + 2, "high"] = 50_050.0

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=True, starting_equity=10_000.0,
            )
            _, trades = eng.run(df)

        assert len(trades) == 5
        assert all(t.exit_reason == "SL" for t in trades)

    def test_equity_curve_length_matches_dataframe(self):
        """Equity curve must have one value per bar (loop runs range(1, n))."""
        n  = 50
        df = _make_df(n)
        arrays = _signal_arrays(n, entry_bars=[], sl=49_000.0, tp=52_000.0)

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=True,
            )
            eq_curve, _ = eng.run(df)

        assert len(eq_curve) == n - 1

    def test_pnl_pct_uses_equity_at_entry_not_notional(self):
        """
        H2: pnl_pct must equal net_pnl / equity_at_entry, not net_pnl / notional.
        A single TP trade lets us verify the denominator precisely.
        """
        n = 30
        # Bar 5 signal → entry at bar 6 open (50 000), TP at 52 000.
        # equity at entry ≈ 10 000 - entry_fee ≈ 9 985.
        df     = _make_df(n)
        # Signal fires at bar 5 → entry at bar 6 open. TP check starts at bar 7.
        df.loc[7, "high"]  = 53_000.0   # bar 7 high exceeds TP=52000
        df.loc[6, "open"]  = 50_000.0
        arrays = _signal_arrays(n, entry_bars=[5], sl=49_000.0, tp=52_000.0)

        with patch("backtesting.event_engine.generate_signals", return_value=arrays):
            eng = EventDrivenEngine(
                "Falling Wedge", {"pattern_lookback": 1},
                raw_mode=True, starting_equity=10_000.0,
            )
            _, trades = eng.run(df)

        assert len(trades) == 1
        t = trades[0]
        assert t.equity_at_entry > 0, "equity_at_entry must be stored on SimulatedTrade"
        expected_pnl_pct = t.pnl / t.equity_at_entry
        assert abs(t.pnl_pct - expected_pnl_pct) < 1e-9, (
            f"pnl_pct={t.pnl_pct:.6f} expected {expected_pnl_pct:.6f} "
            f"(pnl={t.pnl:.4f}, equity_at_entry={t.equity_at_entry:.4f})"
        )
