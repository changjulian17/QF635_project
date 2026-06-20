#!/usr/bin/env python3
"""
Signal forward-excursion observer over lob_tick.db.

Measurement-only: runs the microstructure detector over the OOS window and, for EVERY
sweep signal, records its forward MAE/MFE over the next --window-min minutes — with no
stop and no single-position blocking (overlapping/concurrent windows). This answers two
questions the trading sweep can't:

  1. Frequency — how many signals fire, per day, and how many overlap at once.
  2. Stop vs direction — among would-be LOSERS (signals that end the window down), how far
     did they run IN OUR FAVOUR first?
       large loser-MFE ⇒ recoverable ⇒ STOP problem (widen the stop / decoupled target)
       tiny  loser-MFE ⇒ never went our way ⇒ DIRECTION problem (bias_flip)

Usage
-----
    python scripts/run_signal_observer.py --warmup-days 5 --window-min 10
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import logging
import sqlite3
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("observer")
logger.setLevel(logging.INFO)

_MS_PER_DAY = 86_400_000


def _stats(values: list[float]) -> tuple[int, float]:
    return (len(values), (sum(values) / len(values)) if values else 0.0)


def main() -> None:
    p = argparse.ArgumentParser(description="Signal forward-excursion observer")
    p.add_argument("--db", default="data/lob_tick.db")
    p.add_argument("--warmup-days", type=float, default=5.0)
    p.add_argument("--window-min", type=float, default=10.0, help="forward window per signal")
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

    from backtesting.tick_replay import TickReplayEngine

    window_ms = int(args.window_min * 60_000)
    engine = TickReplayEngine(
        params={}, db_path=str(db_path), observe_mode=True, observe_window_ms=window_ms,
    )
    logger.info("Observer over %s | warm-up %s→%s | observe %s→%s | window=%.0fmin",
                db_path, _d(lo), _d(warmup_end), _d(warmup_end), _d(hi), args.window_min)

    engine.replay_window(lo, warmup_end)          # warm FeatureComputer / CVD stats
    obs = engine.observe(warmup_end, hi)          # measurement pass

    oos_days = (hi - warmup_end) / _MS_PER_DAY
    winners  = [o for o in obs if o.end_return_bps > 0]
    losers   = [o for o in obs if o.end_return_bps < 0]
    n_w, win_mfe = _stats([o.mfe_bps for o in winners])
    n_l, los_mfe = _stats([o.mfe_bps for o in losers])
    _,   win_mae = _stats([o.mae_bps for o in winners])
    _,   los_mae = _stats([o.mae_bps for o in losers])

    print("\n" + "=" * 72)
    print(f"SIGNAL OBSERVER — {args.window_min:.0f}-min forward windows")
    print("=" * 72)
    print(f"  Signals:        {len(obs)}  ({len(obs) / oos_days:.1f} / day over {oos_days:.1f}d)")
    print(f"  Max concurrent: {engine._obs_max_concurrent}")
    print(f"  Outcome split:  {n_w} up / {n_l} down at window close")
    print("-" * 72)
    print(f"  {'group':<16}{'n':>6}{'avg MFE bps':>14}{'avg MAE bps':>14}")
    print(f"  {'would-be WIN':<16}{n_w:>6}{win_mfe:>14.2f}{win_mae:>14.2f}")
    print(f"  {'would-be LOSS':<16}{n_l:>6}{los_mfe:>14.2f}{los_mae:>14.2f}")
    print("-" * 72)
    print("  READ: large would-be-LOSS MFE → losers recover → STOP problem.")
    print("        tiny  would-be-LOSS MFE → never go our way → DIRECTION problem.")
    print("=" * 72)


if __name__ == "__main__":
    main()
