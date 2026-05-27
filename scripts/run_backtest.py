#!/usr/bin/env python3
"""
CLI entry point for the tick-level walk-forward backtest.

Runs TickReplayEngine in a rolling IS/OOS walk-forward over lob_tick.db,
then writes results to data/backtest_results.db (the file read by the
/backtest dashboard page).

Usage
-----
    python scripts/run_backtest.py                            # defaults
    python scripts/run_backtest.py --train-days 20 --test-days 7
    python scripts/run_backtest.py --start 2024-01-01 --end 2024-03-01

Requirements
------------
    - data/lob_tick.db must exist with at least (train_days + test_days) of data.
    - Run `python -m core.lob_recorder` first if the DB is empty or stale.
"""
import argparse
import sys
import pathlib

# Ensure repo root is on sys.path when run as a script
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_backtest")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tick walk-forward backtest runner")
    p.add_argument("--db",          default="data/lob_tick.db",          help="Path to lob_tick.db")
    p.add_argument("--results-db",  default="data/backtest_results.db",  help="Output results DB path")
    p.add_argument("--start",       default=None, help="UTC start datetime (YYYY-MM-DD or ISO-8601)")
    p.add_argument("--end",         default=None, help="UTC end datetime (YYYY-MM-DD or ISO-8601)")
    p.add_argument("--train-days",  type=int, default=30, help="IS window length in days (default 30)")
    p.add_argument("--test-days",   type=int, default=10, help="OOS window length in days (default 10)")
    p.add_argument("--step-days",   type=int, default=10, help="Rolling step between windows (default 10)")
    p.add_argument("--equity",      type=float, default=10_000.0, help="Starting equity per window (default 10000)")
    return p.parse_args()


def _parse_dt(s: str | None) -> datetime | None:
    if s is None:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised datetime format: {s!r}. Use YYYY-MM-DD or ISO-8601.")


def main() -> None:
    args = _parse_args()

    db_path = pathlib.Path(args.db)
    if not db_path.exists():
        logger.error("lob_tick.db not found at %s — run `python -m core.lob_recorder` first", db_path)
        sys.exit(1)

    # Ensure output directory exists
    results_path = pathlib.Path(args.results_db)
    results_path.parent.mkdir(parents=True, exist_ok=True)

    start_ts = _parse_dt(args.start)
    end_ts   = _parse_dt(args.end)

    logger.info("Starting tick walk-forward backtest")
    logger.info("  DB:         %s", db_path)
    logger.info("  Results:    %s", results_path)
    logger.info("  Train days: %d  |  Test days: %d  |  Step days: %d",
                args.train_days, args.test_days, args.step_days)
    if start_ts:
        logger.info("  Start:      %s", start_ts.isoformat())
    if end_ts:
        logger.info("  End:        %s", end_ts.isoformat())

    from backtesting.walk_forward import run_tick_walk_forward

    result_df = run_tick_walk_forward(
        db_path         = str(db_path),
        start_ts        = start_ts,
        end_ts          = end_ts,
        train_days      = args.train_days,
        test_days       = args.test_days,
        step_days       = args.step_days,
        starting_equity = args.equity,
        db_results_path = str(results_path),
    )

    if result_df.empty:
        logger.warning("Walk-forward returned no results — check that lob_tick.db has enough data")
        sys.exit(1)

    logger.info("Walk-forward complete. Results written to %s", results_path)
    logger.info("\n%s", result_df.to_string())


if __name__ == "__main__":
    main()
