#!/usr/bin/env python3
"""
Threshold-sweep backtest.

Runs the tick-replay backtest across a grid of strategy thresholds and reports
trades + P&L for each setting, so "should we loosen the numbers to trade more?"
becomes a data-backed decision (frequency *vs* profitability) instead of a guess.

Pick the setting that maximises a real metric (total P&L / win rate), not the one
with the most trades — more trades usually means weaker setups.

Usage
-----
    python scripts/sweep_thresholds.py                 # default grid, full DB window
    python scripts/sweep_thresholds.py --db data/lob_tick.db --equity 10000

Requires data/lob_tick.db recorded with the FIXED recorder ($1 buckets). Thin or
over-coarse data yields 0 trades across the whole grid — record multi-day data first.
"""
import argparse
import itertools
import logging
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
logging.basicConfig(level=logging.WARNING)

import config  # noqa: E402

# ── Grid (edit to taste) ─────────────────────────────────────────────────────
SIGMA_GRID = [2.0, 2.5, 3.0]            # LOB_WALL_SIGMA — lower → more walls
CONF_GRID  = [0.45, 0.58, 0.70]         # MIN_CONFIDENCE (Gate 2) — lower → more signals
MAXD_GRID  = [25.0, 40.0]               # PROTECTION_MAX_DISTANCE_BPS — higher → accept farther walls


def main() -> None:
    ap = argparse.ArgumentParser(description="Threshold-sweep tick-replay backtest")
    ap.add_argument("--db", default="data/lob_tick.db")
    ap.add_argument("--equity", type=float, default=10_000.0)
    args = ap.parse_args()

    if not pathlib.Path(args.db).exists():
        print(f"DB not found: {args.db} — run `python -m core.lob_recorder` first.")
        sys.exit(1)

    c = sqlite3.connect(args.db)
    lo, hi = c.execute("SELECT min(ts_event), max(ts_event) FROM depth_snapshots").fetchone()
    c.close()
    if lo is None:
        print(f"No depth data in {args.db}.")
        sys.exit(1)
    print(f"Backtest window: {(hi - lo) / 60000:.1f} min  |  grid = "
          f"{len(SIGMA_GRID)}×{len(CONF_GRID)}×{len(MAXD_GRID)} = "
          f"{len(SIGMA_GRID) * len(CONF_GRID) * len(MAXD_GRID)} runs\n")

    from backtesting.tick_replay import TickReplayEngine

    base_conf = config.settings.MIN_CONFIDENCE
    base_maxd = config.settings.PROTECTION_MAX_DISTANCE_BPS
    results = []
    try:
        for sigma, conf, maxd in itertools.product(SIGMA_GRID, CONF_GRID, MAXD_GRID):
            config.settings.MIN_CONFIDENCE = conf
            config.settings.PROTECTION_MAX_DISTANCE_BPS = maxd
            eng = TickReplayEngine(
                params={"feature": {"wall_sigma": sigma}},
                db_path=args.db, starting_equity=args.equity,
            )
            _eq, trades = eng.replay_window(lo, hi)
            pnl  = sum(t.pnl_usd for t in trades)
            wins = sum(1 for t in trades if t.pnl_usd > 0)
            wr   = wins / len(trades) * 100 if trades else 0.0
            results.append((sigma, conf, maxd, len(trades), wr, pnl))
    finally:
        config.settings.MIN_CONFIDENCE = base_conf
        config.settings.PROTECTION_MAX_DISTANCE_BPS = base_maxd

    results.sort(key=lambda r: r[5], reverse=True)  # best P&L first
    print(f"{'sigma':>5} {'conf':>5} {'maxbps':>6} {'trades':>6} {'win%':>6} {'pnl$':>10}")
    print("-" * 46)
    for sigma, conf, maxd, n, wr, pnl in results:
        print(f"{sigma:>5.1f} {conf:>5.2f} {maxd:>6.0f} {n:>6d} {wr:>6.1f} {pnl:>+10.2f}")

    if sum(r[3] for r in results) == 0:
        print("\n⚠ 0 trades across the whole grid — data is too thin/coarse or too short.")
        print("  Record multi-day data with the fixed recorder ($1 buckets), then re-run.")


if __name__ == "__main__":
    main()
