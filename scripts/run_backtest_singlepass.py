#!/usr/bin/env python3
"""
Single full-period backtest over lob_tick.db.

Walk-forward needs many trades per window; this low-frequency strategy fires
only ~15 trades over the ~20 days of available data, so instead we do ONE pass:
warm up FeatureComputer Welford stats on the first --warmup-days, then evaluate
across ALL remaining days in a single OOS pass. No per-window trade minimum, so
every trade is captured.

Usage
-----
    python scripts/run_backtest_singlepass.py                 # 5d warm-up, rest = OOS
    python scripts/run_backtest_singlepass.py --warmup-days 7
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import logging
import sqlite3
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("singlepass")

_MS_PER_DAY = 86_400_000


def main() -> None:
    p = argparse.ArgumentParser(description="Single full-period tick backtest")
    p.add_argument("--db", default="data/lob_tick.db")
    p.add_argument("--warmup-days", type=float, default=5.0)
    p.add_argument("--equity", type=float, default=10_000.0)
    args = p.parse_args()

    db_path = pathlib.Path(args.db)
    if not db_path.exists():
        logger.error("DB not found at %s", db_path)
        sys.exit(1)

    with sqlite3.connect(str(db_path)) as conn:
        lo, hi = conn.execute(
            "SELECT MIN(ts_event), MAX(ts_event) FROM depth_snapshots"
        ).fetchone()
    lo, hi = int(lo), int(hi)

    warmup_end = lo + int(args.warmup_days * _MS_PER_DAY)
    if warmup_end >= hi:
        logger.error("warmup-days exceeds available data span")
        sys.exit(1)

    def _d(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")

    logger.info("Single-pass backtest over %s", db_path)
    logger.info("  Warm-up (IS):  %s → %s", _d(lo), _d(warmup_end))
    logger.info("  Evaluate (OOS): %s → %s", _d(warmup_end), _d(hi))

    from backtesting.tick_replay import TickReplayEngine

    engine = TickReplayEngine(params={}, db_path=str(db_path), starting_equity=args.equity)

    logger.info("Warming up feature stats…")
    engine.replay_window(lo, warmup_end)

    logger.info("Evaluating OOS…")
    eq, trades = engine.replay_window_oos(warmup_end, hi)

    # ── Report ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(f"TRADES: {len(trades)}")
    print("=" * 72)
    if trades:
        print(f"{'entry':<17}{'dir':<6}{'entry$':>10}{'exit$':>10}{'pnl$':>10}  reason")
        for t in trades:
            print(
                f"{_d(t.entry_ts_ms):<17}{t.direction:<6}"
                f"{t.entry_price:>10.2f}{t.exit_price:>10.2f}{t.pnl_usd:>10.2f}  {t.exit_reason}"
            )

        pnls = [t.pnl_usd for t in trades]
        wins = [x for x in pnls if x > 0]
        total = sum(pnls)
        gross_win = sum(wins)
        gross_loss = -sum(x for x in pnls if x < 0)
        final_eq = eq.iloc[-1]
        peak = eq.cummax()
        mdd = ((eq - peak) / peak).min() * 100

        print("\n" + "-" * 72)
        print(f"  Net PnL:        ${total:,.2f}  ({total / args.equity * 100:+.2f}% of equity)")
        print(f"  Final equity:   ${final_eq:,.2f}")
        print(f"  Win rate:       {len(wins)}/{len(trades)} = {len(wins) / len(trades) * 100:.1f}%")
        print(f"  Avg trade:      ${total / len(trades):,.2f}")
        print(f"  Profit factor:  {gross_win / gross_loss:.2f}" if gross_loss else "  Profit factor:  inf (no losers)")
        print(f"  Max drawdown:   {mdd:.2f}%")
        print("-" * 72)
    else:
        print("No trades fired in the evaluation window.")


if __name__ == "__main__":
    main()
