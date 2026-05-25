"""
backtest.py
===========
Command-line entry point for the CryptoSentinel backtesting engine.

This script orchestrates the full two-phase pipeline:
  Phase 1 — VectorBT + Optuna parameter optimisation (training windows)
  Phase 2 — Event-driven validation (test windows, raw + full risk engine)
  Output  — Strategy leaderboard + SQLite/CSV results

All results are automatically saved to:
  data/backtest_results.db  (queried by the Streamlit dashboard)
  data/backtest_results.csv (for manual inspection)

Usage Examples
--------------
# Quick validation (50 Optuna trials, 90-day history, 5m candles)
  python backtest.py --fast

# Standard research run (300 trials, 6 months, all timeframes)
  python backtest.py --days 180

# Single strategy deep-dive on 5m candles
  python backtest.py --strategy "Falling Wedge" --timeframes 5m --days 180 --trials 500

# All strategies, specific timeframe, longer history
  python backtest.py --days 365 --timeframes 1m,5m --trials 300

# Dry run: fetch and validate data only (no backtesting)
  python backtest.py --data-check-only --days 180

Environment
-----------
No API key is required. Historical OHLCV data is fetched from the
Binance *public* REST API (read-only, unauthenticated).

CCXT is used as the HTTP client. It respects Binance's rate limits
automatically when ``enableRateLimit=True``.

The first run fetches and caches ~35s worth of data for 6 months
of 1m candles. Subsequent runs load from SQLite in < 1s.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from data.fetcher      import OHLCVFetcher
from data.validator    import validate_ohlcv, repair_ohlcv
from backtesting.signals     import ALL_STRATEGIES
from backtesting.walk_forward import (
    run_walk_forward,
    DEFAULT_TIMEFRAMES,
    DEFAULT_TRAIN_DAYS,
    DEFAULT_TEST_DAYS,
    DEFAULT_STEP_DAYS,
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt = "%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/backtest.log", mode="a"),
    ],
)
logger = logging.getLogger("backtest")


# ─────────────────────────────────────────────────────────────────────────────
# Argument Parser
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog        = "backtest.py",
        description = "CryptoSentinel — Backtesting Engine",
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog = """
Examples:
  python backtest.py --fast
  python backtest.py --days 180 --timeframes 5m,15m
  python backtest.py --strategy "Falling Wedge" --timeframes 5m --trials 500
  python backtest.py --data-check-only --days 90
        """,
    )

    p.add_argument(
        "--symbol",
        type    = str,
        default = "BTC/USDT",
        help    = "Trading pair symbol (default: BTC/USDT)",
    )
    p.add_argument(
        "--days",
        type    = int,
        default = 180,
        help    = "Historical lookback in days (default: 180)",
    )
    p.add_argument(
        "--strategy",
        type    = str,
        default = "all",
        help    = (
            "Strategy to test. Use 'all' for all strategies, or one of:\n"
            + "\n".join(f"  '{s}'" for s in ALL_STRATEGIES)
        ),
    )
    p.add_argument(
        "--timeframes",
        type    = str,
        default = ",".join(DEFAULT_TIMEFRAMES),
        help    = "Comma-separated timeframes (default: 1m,5m,15m)",
    )
    p.add_argument(
        "--trials",
        type    = int,
        default = 300,
        help    = "Optuna trials per strategy/window (default: 300)",
    )
    p.add_argument(
        "--train-days",
        type    = int,
        default = DEFAULT_TRAIN_DAYS,
        help    = f"Walk-forward training window days (default: {DEFAULT_TRAIN_DAYS})",
    )
    p.add_argument(
        "--test-days",
        type    = int,
        default = DEFAULT_TEST_DAYS,
        help    = f"Walk-forward test window days (default: {DEFAULT_TEST_DAYS})",
    )
    p.add_argument(
        "--step-days",
        type    = int,
        default = DEFAULT_STEP_DAYS,
        help    = f"Walk-forward step size in days (default: {DEFAULT_STEP_DAYS})",
    )
    p.add_argument(
        "--equity",
        type    = float,
        default = 10_000.0,
        help    = "Starting equity in USDT (default: 10000)",
    )
    p.add_argument(
        "--fast",
        action  = "store_true",
        help    = "Quick mode: 50 Optuna trials (useful for CI / smoke testing)",
    )
    p.add_argument(
        "--data-check-only",
        action  = "store_true",
        help    = "Fetch and validate data only; do not run backtesting",
    )
    p.add_argument(
        "--no-repair",
        action  = "store_true",
        help    = "Abort on data quality errors instead of auto-repairing",
    )
    p.add_argument(
        "--verbose",
        action  = "store_true",
        help    = "Enable DEBUG-level logging",
    )

    return p


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    """
    Entry point.

    Returns 0 on success, 1 on unrecoverable error.
    """
    parser = _build_parser()
    args   = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    n_trials   = 50 if args.fast else args.trials
    timeframes = [t.strip() for t in args.timeframes.split(",")]
    strategies = (
        ALL_STRATEGIES
        if args.strategy.lower() == "all"
        else [args.strategy]
    )

    # Validate strategy names
    for s in strategies:
        if s not in ALL_STRATEGIES:
            logger.error(
                "Unknown strategy '%s'. Available: %s", s, ALL_STRATEGIES
            )
            return 1

    # ── Header ───────────────────────────────────────────────────────────────
    started_at = datetime.now(timezone.utc)
    print("\n" + "=" * 65)
    print("  CryptoSentinel Backtesting Engine")
    print("=" * 65)
    print(f"  Symbol     : {args.symbol}")
    print(f"  History    : {args.days} days")
    print(f"  Strategies : {strategies}")
    print(f"  Timeframes : {timeframes}")
    print(f"  WF windows : train={args.train_days}d / test={args.test_days}d / step={args.step_days}d")
    print(f"  Optuna     : {n_trials} trials per window")
    print(f"  Equity     : ${args.equity:,.0f} USDT")
    print(f"  Started    : {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 65 + "\n")

    # ── Step 1: Fetch Data ────────────────────────────────────────────────────
    logger.info("[Main] Fetching historical data...")
    fetcher = OHLCVFetcher(symbol=args.symbol)

    try:
        df_1m = fetcher.fetch(timeframe="1m", lookback_days=args.days)
    except Exception as exc:
        logger.error("[Main] Data fetch failed: %s", exc)
        return 1

    if df_1m is None or df_1m.empty:
        logger.error("[Main] No data returned. Aborting.")
        return 1

    logger.info(
        "[Main] Fetched %d candles | %s → %s",
        len(df_1m),
        df_1m["datetime"].iloc[0].strftime("%Y-%m-%d"),
        df_1m["datetime"].iloc[-1].strftime("%Y-%m-%d"),
    )

    # ── Step 2: Validate Data ─────────────────────────────────────────────────
    logger.info("[Main] Validating data quality...")
    report = validate_ohlcv(df_1m, symbol=args.symbol, timeframe="1m")
    print(report.summary())

    if not report.is_valid:
        if args.no_repair:
            logger.error(
                "[Main] Data validation failed and --no-repair is set. Aborting."
            )
            return 1
        logger.warning("[Main] Attempting auto-repair...")
        df_1m   = repair_ohlcv(df_1m)
        report2 = validate_ohlcv(df_1m, symbol=args.symbol, timeframe="1m")
        if not report2.is_valid:
            logger.error("[Main] Repair failed. Aborting.")
            return 1
        logger.info("[Main] Repair successful.")

    if args.data_check_only:
        logger.info("[Main] --data-check-only flag set. Exiting after validation.")
        return 0

    # ── Step 3: Run Walk-Forward ──────────────────────────────────────────────
    logger.info("[Main] Starting walk-forward analysis...")

    try:
        results = run_walk_forward(
            df_1m           = df_1m,
            fetcher         = fetcher,
            strategies      = strategies,
            timeframes      = timeframes,
            train_days      = args.train_days,
            test_days       = args.test_days,
            step_days       = args.step_days,
            n_optuna_trials = n_trials,
            starting_equity = args.equity,
        )
    except KeyboardInterrupt:
        logger.warning("[Main] Interrupted by user.")
        return 1
    except Exception as exc:
        logger.error("[Main] Walk-forward failed: %s", exc, exc_info=True)
        return 1

    # ── Step 4: Summary ───────────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n✓ Completed in {elapsed:.0f}s")
    print(f"  Results saved → data/backtest_results.db")
    print(f"  Results saved → data/backtest_results.csv")
    print(f"  Log saved     → data/backtest.log\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
