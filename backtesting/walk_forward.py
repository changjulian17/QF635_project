"""
backtesting/walk_forward.py
===========================
Walk-forward validation controller for CryptoSentinel.

What is Walk-Forward Analysis?
-------------------------------
Walk-forward analysis is the industry-standard method for testing
whether a strategy's performance is genuine (i.e. from real edge)
or illusory (i.e. from curve-fitting to historical data).

The key insight: if you optimise parameters *and* measure performance
on the *same* data, the results will almost always look good — even
for a random strategy. Walk-forward forces you to optimise on one
window and measure on a *different* window the optimiser never saw.

Rolling Walk-Forward (used here):
    Window 1:  Train days  0–89  │ Test days  90–119
    Window 2:  Train days 30–119 │ Test days 120–149
    Window 3:  Train days 60–149 │ Test days 150–179
    ...
    Final OOS performance = aggregate of ALL test windows

Because the train window rolls forward, each test window evaluates
parameters optimised on the immediately preceding 90 days — closely
mimicking how you would deploy the strategy in production (re-optimise
every month, trade the following month with those parameters).

Two-Phase Execution per Window
-------------------------------
Phase 1 (VectorBT + Optuna):
    - Fast parameter search on the training window.
    - 300 Bayesian trials in ~3–5 minutes.
    - Output: best_params for this window.

Phase 2 (EventDrivenEngine):
    a) Raw mode   — signal quality, no risk engine
    b) Full mode  — with 1% daily limit, tiers, sizing

The reported leaderboard uses Phase 2 full-mode metrics.
Phase 2 raw mode is reported separately for diagnostic purposes
(raw vs risk-engine metrics shows how much the risk system helps).

Strategy Leaderboard
--------------------
After all windows complete, strategies are ranked by composite_score()
on out-of-sample metrics. The top strategy that passes passes_minimum_bar()
is recommended for live deployment.

Results are saved to:
  data/backtest_results.db  (SQLite — for dashboard queries)
  data/backtest_results.csv (CSV — for manual inspection)

Usage
-----
>>> from data.fetcher import OHLCVFetcher
>>> from backtesting.walk_forward import run_walk_forward
>>> fetcher = OHLCVFetcher()
>>> df_1m   = fetcher.fetch(timeframe="1m", lookback_days=180)
>>> results = run_walk_forward(df_1m, fetcher, n_optuna_trials=300)
>>> print(results.to_string())
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field

import pandas as pd

from backtesting.event_engine import EventDrivenEngine, SimulatedTrade
from backtesting.metrics      import BacktestMetrics, calculate_metrics
from backtesting.signals      import ALL_STRATEGIES
from backtesting.vectorbt_runner import run_phase1_optimisation
from data.fetcher             import OHLCVFetcher, TIMEFRAME_MS

logger = logging.getLogger(__name__)

RESULTS_DB  = "data/backtest_results.db"
RESULTS_CSV = "data/backtest_results.csv"

DEFAULT_TIMEFRAMES  = ["1m", "5m", "15m"]
DEFAULT_TRAIN_DAYS  = 90
DEFAULT_TEST_DAYS   = 30
DEFAULT_STEP_DAYS   = 30
MIN_OOS_TRADES      = 10   # windows below this are excluded from aggregate metrics


# ─────────────────────────────────────────────────────────────────────────────
# Window Type
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WalkForwardWindow:
    """
    One train/test split expressed as row-index ranges.

    Attributes
    ----------
    window_idx  : Sequential window number (0-based).
    train_start : Inclusive start row of training slice.
    train_end   : Exclusive end row of training slice.
    test_start  : Inclusive start row of test slice.
    test_end    : Exclusive end row of test slice.
    """
    window_idx:  int
    train_start: int
    train_end:   int
    test_start:  int
    test_end:    int

    @property
    def train_len(self) -> int:
        return self.train_end - self.train_start

    @property
    def test_len(self) -> int:
        return self.test_end - self.test_start


# ─────────────────────────────────────────────────────────────────────────────
# Walk-Forward Window Generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_windows(
    n_candles:   int,
    train_days:  int,
    test_days:   int,
    step_days:   int,
    timeframe:   str,
) -> list[WalkForwardWindow]:
    """
    Generate rolling walk-forward window index pairs.

    Parameters
    ----------
    n_candles   : Total number of candles in the full dataset.
    train_days  : Training window length in calendar days.
    test_days   : Test window length in calendar days.
    step_days   : How many days to advance the window each iteration.
    timeframe   : Candle timeframe string (e.g. "1m", "5m").

    Returns
    -------
    List of WalkForwardWindow objects. Returns empty list if the dataset
    is too short to fit even one window.
    """
    tf_ms           = TIMEFRAME_MS.get(timeframe, 60_000)
    candles_per_day = int(86_400_000 / tf_ms)

    train_len = train_days * candles_per_day
    test_len  = test_days  * candles_per_day
    step_len  = step_days  * candles_per_day

    windows = []
    start   = 0
    idx     = 0

    while start + train_len + test_len <= n_candles:
        windows.append(WalkForwardWindow(
            window_idx  = idx,
            train_start = start,
            train_end   = start + train_len,
            test_start  = start + train_len,
            test_end    = start + train_len + test_len,
        ))
        start += step_len
        idx   += 1

    if not windows:
        logger.warning(
            "[WF] Insufficient data: need %d candles for one window, "
            "have %d (%s, train=%dd test=%dd).",
            train_len + test_len, n_candles, timeframe, train_days, test_days,
        )

    return windows


# ─────────────────────────────────────────────────────────────────────────────
# Main Controller
# ─────────────────────────────────────────────────────────────────────────────

def run_walk_forward(
    df_1m:           pd.DataFrame,
    fetcher:         OHLCVFetcher,
    strategies:      list[str]   = None,
    timeframes:      list[str]   = None,
    train_days:      int         = DEFAULT_TRAIN_DAYS,
    test_days:       int         = DEFAULT_TEST_DAYS,
    step_days:       int         = DEFAULT_STEP_DAYS,
    n_optuna_trials: int         = 300,
    starting_equity: float       = 10_000.0,
    db_path:         str         = RESULTS_DB,
    csv_path:        str         = RESULTS_CSV,
) -> pd.DataFrame:
    """
    Run the full walk-forward pipeline across all strategies and timeframes.

    For each (strategy, timeframe, walk-forward window):
      1. Phase 1: Optuna finds best parameters on training window.
      2. Phase 2a: EventDrivenEngine validates on test window (raw mode).
      3. Phase 2b: EventDrivenEngine validates on test window (full mode).

    OOS metrics from step 2b are aggregated across all windows and
    compiled into the final leaderboard.

    Parameters
    ----------
    df_1m            : Full 1-minute OHLCV DataFrame.
    fetcher          : OHLCVFetcher instance (used for resampling).
    strategies       : Strategies to test. Defaults to ALL_STRATEGIES.
    timeframes       : Timeframes to test. Defaults to ["1m", "5m", "15m"].
    train_days       : Training window length (days).
    test_days        : Test window length (days).
    step_days        : Window step size (days).
    n_optuna_trials  : Optuna trials per (strategy, timeframe, window).
    starting_equity  : Initial equity per simulation.

    Returns
    -------
    pd.DataFrame sorted by composite_score descending. Each row is one
    (strategy, timeframe) combination with aggregated OOS metrics.
    Columns match BacktestMetrics.to_dict() plus best_params.
    """
    strategies = strategies or ALL_STRATEGIES
    timeframes = timeframes or DEFAULT_TIMEFRAMES

    # Prepare resampled DataFrames up front
    dfs: dict[str, pd.DataFrame] = {"1m": df_1m}
    for tf in timeframes:
        if tf != "1m":
            dfs[tf] = fetcher.resample(df_1m, tf)

    all_results: list[dict] = []
    windows_per_tf: dict[str, list[WalkForwardWindow]] = {}

    total_combinations = len(strategies) * len(timeframes)
    combination_idx    = 0

    for tf in timeframes:
        df = dfs[tf]
        windows = generate_windows(
            n_candles  = len(df),
            train_days = train_days,
            test_days  = test_days,
            step_days  = step_days,
            timeframe  = tf,
        )
        windows_per_tf[tf] = windows

        if not windows:
            logger.warning("[WF] Skipping %s — no valid windows.", tf)
            continue

        logger.info(
            "\n%s\n[WF] Timeframe: %s | %d windows | "
            "%d strategies\n%s",
            "=" * 65, tf, len(windows), len(strategies), "=" * 65,
        )

        for strategy in strategies:
            combination_idx += 1
            logger.info(
                "\n[WF] (%d/%d) %s | %s",
                combination_idx, total_combinations, strategy, tf,
            )

            oos_trades_raw:  list[SimulatedTrade] = []
            oos_trades_full: list[SimulatedTrade] = []
            oos_equity_raw:  list[pd.Series]      = []
            oos_equity_full: list[pd.Series]      = []
            best_params_per_window: list[dict]    = []

            for w in windows:
                train_df = df.iloc[w.train_start : w.train_end].reset_index(drop=True)
                test_df  = df.iloc[w.test_start  : w.test_end ].reset_index(drop=True)

                # ── Phase 1: Optimise on training window ──────────────────
                logger.info(
                    "  Window %d/%d | train=%d bars | test=%d bars",
                    w.window_idx + 1, len(windows),
                    w.train_len, w.test_len,
                )

                opt = run_phase1_optimisation(
                    strategy        = strategy,
                    train_df        = train_df,
                    timeframe       = tf,
                    n_trials        = n_optuna_trials,
                    starting_equity = starting_equity,
                )
                best_params_per_window.append(opt.best_params)

                # ── Phase 2a: Raw validation ──────────────────────────────
                engine_raw = EventDrivenEngine(
                    strategy        = strategy,
                    params          = opt.best_params,
                    raw_mode        = True,
                    starting_equity = starting_equity,
                )
                eq_raw, tr_raw = engine_raw.run(test_df)
                oos_trades_raw.extend(tr_raw)
                oos_equity_raw.append(eq_raw)

                # ── Phase 2b: Full risk engine validation ─────────────────
                engine_full = EventDrivenEngine(
                    strategy        = strategy,
                    params          = opt.best_params,
                    raw_mode        = False,
                    starting_equity = starting_equity,
                )
                eq_full, tr_full = engine_full.run(test_df)

                if len(tr_full) < MIN_OOS_TRADES:
                    logger.warning(
                        "  Window %d: only %d full-mode trades (< %d min) "
                        "— excluded from aggregate.",
                        w.window_idx + 1, len(tr_full), MIN_OOS_TRADES,
                    )
                else:
                    oos_trades_full.extend(tr_full)
                    oos_equity_full.append(eq_full)

                logger.info(
                    "  Window %d done | raw_trades=%d full_trades=%d",
                    w.window_idx + 1, len(tr_raw), len(tr_full),
                )

            # ── Aggregate OOS results across all windows ──────────────────
            if not oos_equity_full:
                continue

            chained_eq_raw  = _chain_equity_curves(oos_equity_raw,  starting_equity)
            chained_eq_full = _chain_equity_curves(oos_equity_full, starting_equity)

            oos_period = f"{test_days * len(windows)}d OOS ({len(windows)} windows)"

            # Convert SimulatedTrade objects to dicts for metrics calculator
            metrics_raw = calculate_metrics(
                equity_curve  = chained_eq_raw,
                trades        = _trades_to_dicts(oos_trades_raw),
                strategy_name = strategy,
                timeframe     = tf,
                period        = oos_period + " [raw]",
            )
            metrics_full = calculate_metrics(
                equity_curve  = chained_eq_full,
                trades        = _trades_to_dicts(oos_trades_full),
                strategy_name = strategy,
                timeframe     = tf,
                period        = oos_period + " [full risk]",
            )

            logger.info("\n  RAW:  %s", metrics_raw.summary())
            logger.info("  FULL: %s\n", metrics_full.summary())

            # Store both in results; rank on full-mode metrics
            row = metrics_full.to_dict()
            row.update({
                "raw_sharpe":      metrics_raw.sharpe_ratio,
                "raw_return_pct":  metrics_raw.total_return_pct,
                "raw_trades":      metrics_raw.total_trades,
                "best_params":     best_params_per_window[-1],   # most recent window
                "n_wf_windows":    len(windows),
            })
            all_results.append(row)

    # ── Buy-and-hold benchmark rows (one per timeframe, OOS period only) ─────
    for tf in timeframes:
        df  = dfs.get(tf)
        wns = windows_per_tf.get(tf, [])   # reuse already-computed windows
        if df is not None and wns:
            oos_df  = df.iloc[wns[0].test_start : wns[-1].test_end].reset_index(drop=True)
            bah_row = _compute_buy_and_hold(oos_df, starting_equity, tf)
            if bah_row:
                all_results.append(bah_row)

    if not all_results:
        logger.error("[WF] No results produced. Check data length and parameters.")
        return pd.DataFrame()

    # Separate strategy rows from benchmark rows so they sort independently.
    # Benchmarks always have is_benchmark=True; strategy rows never do.
    strat_rows = [r for r in all_results if not r.get("is_benchmark")]
    bm_rows    = [r for r in all_results if r.get("is_benchmark")]

    strat_df = (
        pd.DataFrame(strat_rows)
        .sort_values("composite_score", ascending=False)
        .reset_index(drop=True)
    ) if strat_rows else pd.DataFrame()

    bm_df = pd.DataFrame(bm_rows).reset_index(drop=True) if bm_rows else pd.DataFrame()

    results_df = pd.concat([strat_df, bm_df], ignore_index=True)

    _save_results(results_df, db_path=db_path, csv_path=csv_path)
    _print_leaderboard(results_df)

    return results_df


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _chain_equity_curves(
    curves:          list[pd.Series],
    starting_equity: float,
) -> pd.Series:
    """
    Chain multiple equity curves end-to-end so they form a continuous series.

    Each window's equity curve starts at ``starting_equity``. We scale
    each subsequent curve so it begins where the previous one ended,
    simulating a single portfolio running across all test windows.
    """
    if not curves:
        return pd.Series(dtype=float)

    valid  = [c for c in curves if c is not None and len(c) > 0]
    if not valid:
        return pd.Series(dtype=float)

    chained = [valid[0]]
    for prev, curr in zip(valid[:-1], valid[1:]):
        scale = prev.iloc[-1] / curr.iloc[0] if curr.iloc[0] != 0 else 1.0
        chained.append(curr * scale)

    # Windows are non-overlapping by construction (generate_windows guarantees
    # test_end[i] == test_start[i+1]), so concat preserves chronological order.
    # sort_index() is intentionally omitted — it would interleave curves if
    # any window returned a RangeIndex equity series.
    return pd.concat(chained)


def _compute_buy_and_hold(
    df:              pd.DataFrame,
    starting_equity: float,
    timeframe:       str,
) -> dict:
    """
    Passive benchmark: buy at first bar open, hold to last bar close.
    Computed over the same OOS slice as the strategies so the comparison is fair.
    """
    if len(df) < 2:
        return {}

    if "datetime" in df.columns:
        idx = pd.DatetimeIndex(df["datetime"])
    else:
        idx = pd.RangeIndex(len(df))

    entry_price = float(df["open"].iloc[0])
    eq_values   = starting_equity * df["close"].values / entry_price

    # Prepend starting_equity at the bar-open timestamp so the equity curve
    # starts at exactly starting_equity and total_return = close[-1]/open[0]-1
    # (without this, iloc[0] = starting_equity * close[0]/open[0], which uses
    # close[0] as the effective entry price — a small but systematic error).
    if isinstance(idx, pd.DatetimeIndex):
        tf_ms   = TIMEFRAME_MS.get(timeframe, 60_000)
        t_entry = idx[0] - pd.Timedelta(milliseconds=tf_ms)
        eq_curve = pd.concat([
            pd.Series([starting_equity],
                      index=pd.DatetimeIndex([t_entry], tz=idx.tz),
                      name="equity"),
            pd.Series(eq_values, index=idx, name="equity"),
        ])
    else:
        eq_curve = pd.Series(eq_values, index=idx, name="equity")

    total_minutes = len(df) * TIMEFRAME_MS.get(timeframe, 60_000) / 60_000
    pnl_pct       = df["close"].iloc[-1] / entry_price - 1
    trade = {
        "pnl":              starting_equity * pnl_pct,
        "pnl_pct":          pnl_pct,
        "duration_minutes": total_minutes,
    }

    m   = calculate_metrics(eq_curve, [trade], "BUY_AND_HOLD", timeframe)
    row = m.to_dict()
    row.update({
        "raw_sharpe":     m.sharpe_ratio,
        "raw_return_pct": m.total_return_pct,
        "raw_trades":     1,
        "best_params":    {},
        "n_wf_windows":   1,
        "is_benchmark":   True,   # excluded from strategy sort and minimum-bar gate
    })
    return row


def _trades_to_dicts(trades: list[SimulatedTrade]) -> list[dict]:
    """Convert SimulatedTrade objects to the dict format expected by calculate_metrics."""
    return [
        {
            "pnl":              t.pnl,
            "pnl_pct":          t.pnl_pct,
            "duration_minutes": t.duration_minutes,
        }
        for t in trades
    ]


def _save_results(
    df:       pd.DataFrame,
    db_path:  str = RESULTS_DB,
    csv_path: str = RESULTS_CSV,
) -> None:
    """Persist results to SQLite and CSV."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    # JSON-serialize best_params so it is queryable in both SQLite and CSV.
    # Without this, dicts are dropped from DB and stored as unparse-able repr
    # strings in CSV.
    out = df.copy()
    if "best_params" in out.columns:
        out["best_params"] = out["best_params"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else str(x)
        )

    with sqlite3.connect(db_path) as conn:
        out.to_sql("results", conn, if_exists="replace", index=True)

    out.to_csv(csv_path, index=False)
    logger.info("[WF] Results saved → %s | %s", db_path, csv_path)


def _print_leaderboard(df: pd.DataFrame) -> None:
    """Print a formatted leaderboard table to stdout."""
    is_bm    = df.get("is_benchmark", pd.Series(False, index=df.index, dtype=bool))
    is_bm    = is_bm.map(lambda x: x is True)   # True→True, NaN/False→False; no downcasting
    strat_df = df[~is_bm]
    bm_df    = df[is_bm]

    strat_cols = [
        "strategy", "timeframe",
        "total_return_pct", "sharpe_ratio", "sortino_ratio",
        "max_drawdown_pct", "profit_factor", "win_rate_pct",
        "total_trades", "composite_score", "passes_minimum_bar",
    ]
    bm_cols = [
        "strategy", "timeframe",
        "total_return_pct", "sharpe_ratio", "sortino_ratio",
        "max_drawdown_pct",
    ]

    sep = "=" * 100
    print(f"\n{sep}")
    print("STRATEGY LEADERBOARD — Out-of-Sample Walk-Forward Results")
    print(sep)

    if not strat_df.empty:
        cols = [c for c in strat_cols if c in strat_df.columns]
        print(strat_df[cols].to_string(index=True, float_format="%.2f"))
    else:
        print("  (no strategy results)")

    print(sep)

    # ── Suspicious Sharpe flag ────────────────────────────────────────────────
    if "sharpe_ratio" in strat_df.columns:
        suspicious = strat_df[strat_df["sharpe_ratio"] > 5.0]
        if not suspicious.empty:
            print(
                "\n⚠️  SUSPICIOUS: Sharpe > 5.0 detected — possible overfitting "
                "or lookahead bias. Investigate before deploying."
            )
            for _, r in suspicious.iterrows():
                print(f"    {r['strategy']} | {r['timeframe']} | "
                      f"Sharpe={r['sharpe_ratio']:.2f}")

    # ── Recommended strategy ──────────────────────────────────────────────────
    pmb     = strat_df.get("passes_minimum_bar")                        # None if column absent
    passing = strat_df[pmb == True] if pmb is not None else strat_df.iloc[:0]  # noqa: E712
    if not passing.empty:
        best = passing.iloc[0]
        print(
            f"\n🏆  RECOMMENDED STRATEGY: {best['strategy']} "
            f"on {best['timeframe']}\n"
            f"    Sharpe={best['sharpe_ratio']:.2f}  "
            f"Return={best['total_return_pct']:.1f}%  "
            f"MaxDD={best['max_drawdown_pct']:.1f}%  "
            f"Trades={int(best['total_trades'])}"
        )
    else:
        print(
            "\n⚠️   No strategy passed the minimum bar. "
            "Review signal quality or adjust thresholds."
        )

    # ── Benchmark reference section ───────────────────────────────────────────
    if not bm_df.empty:
        print(f"\n{'─' * 100}")
        print("BENCHMARK REFERENCE (passive buy-and-hold, same OOS period)")
        print(f"{'─' * 100}")
        cols = [c for c in bm_cols if c in bm_df.columns]
        print(bm_df[cols].to_string(index=False, float_format="%.2f"))

    print()
