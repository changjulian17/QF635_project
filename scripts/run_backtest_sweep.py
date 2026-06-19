#!/usr/bin/env python3
"""
Multi-variant backtest sweep over lob_tick.db.

Runs several strategy variants through the same warm-up → OOS split, scores each
with the shared metrics suite, ranks them by expected_value (avg PnL per trade),
prints a leaderboard, and persists to a SEPARATE data/sweep_results.db so the
existing chart-pattern walk-forward results in data/backtest_results.db are left
untouched.

Each variant isolates one hypothesis from the live-telemetry diagnosis (the live
strategy stops out 79% of losers in <3 s on a too-tight protection-wall stop):
  baseline       — raw wall stop (reproduces run_backtest_singlepass.py)
  vol_floor_1.5  — floor the stop at 1.5×ATR so it clears the noise band
  vol_floor_3.0  — floor at 3.0×ATR
  edge_gate      — vol_floor + skip trades whose edge < round-trip cost
  tp_2x / tp_4x  — vary the take-profit R:R
  gated          — apply the RuleBasedScorer confidence gate (Gate 2)

Thin-sample warning: this low-frequency strategy fires only ~15 trades over the
~20 days available, so ranking is on expected_value (Sharpe / composite_score are
−999/noisy below 10 trades). Treat results as directional, not conclusive.

Usage
-----
    python scripts/run_backtest_sweep.py --warmup-days 5
    python scripts/run_backtest_sweep.py --warmup-days 5 --register-winner
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(
    level=logging.WARNING,  # quiet the per-fold engine logs; sweep prints its own report
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sweep")
logger.setLevel(logging.INFO)

_MS_PER_DAY = 86_400_000
_SWEEP_DB  = "data/sweep_results.db"
_SWEEP_CSV = "data/sweep_results.csv"


def _variants():
    """Candidate variants — each isolates one diagnosis hypothesis."""
    from backtesting.tick_replay import VariantConfig
    from strategy.spec import EntryRules
    return [
        VariantConfig(name="baseline"),
        VariantConfig(name="vol_floor_1.5", stop_mode="vol_floor", stop_floor_atr_mult=1.5),
        VariantConfig(name="vol_floor_3.0", stop_mode="vol_floor", stop_floor_atr_mult=3.0),
        # min_edge_bps must exceed the round-trip cost floor (~30 bps) to bind —
        # below that, max(round_trip_pct, min_edge_bps/1e4) == round_trip_pct (a no-op).
        VariantConfig(name="edge_gate",     stop_mode="vol_floor", stop_floor_atr_mult=1.5,
                      min_edge_bps=50.0),
        VariantConfig(name="tp_2x", atr_mult_tp=2.0),
        VariantConfig(name="tp_4x", atr_mult_tp=4.0),
        VariantConfig(name="gated", apply_entry_gate=True, entry_rules=EntryRules()),
        # Directional-bias gate — the sweep showed all stop/TP variants lose, the
        # signature of wrong entry DIRECTION. Veto (or flip) sweeps that fight a
        # CVD+trend bias. Run the single-input variants first to see which input
        # carries the signal before the stricter bias_both (which thins the sample).
        VariantConfig(name="bias_cvd",   bias_mode="skip", bias_use_trend=False),
        VariantConfig(name="bias_trend", bias_mode="skip", bias_use_cvd=False),
        VariantConfig(name="bias_both",  bias_mode="skip"),
        VariantConfig(name="bias_volfloor", bias_mode="skip",
                      stop_mode="vol_floor", stop_floor_atr_mult=1.5),
        # Decisive test: if entries are systematically wrong-direction, FLIP (trade the
        # bias) should beat skip. If flip also fails, the signal is noise, not mistimed.
        VariantConfig(name="bias_flip",  bias_mode="flip"),
    ]


def _adapt_trades(trades) -> list[dict]:
    """ReplayTrade → the dict contract calculate_metrics expects (pnl/pnl_pct/duration_minutes).

    pnl_pct is return on notional (entry_price × qty); it feeds avg_win/avg_loss_pct
    display only — ranking is on expected_value (avg $ PnL), which uses pnl.
    """
    out = []
    for t in trades:
        notional = t.entry_price * t.qty
        out.append({
            "pnl":              t.pnl_usd,
            "pnl_pct":          t.pnl_usd / notional if notional > 0 else 0.0,
            "duration_minutes": (t.exit_ts_ms - t.entry_ts_ms) / 60_000.0,
        })
    return out


def _run_variant(variant, db_path, equity, lo, warmup_end, hi):
    """Backtest one variant; return its metrics dict row, or None if it errored/no-trades."""
    from backtesting.metrics import calculate_metrics
    from backtesting.tick_replay import TickReplayEngine
    try:
        engine = TickReplayEngine(
            params={}, db_path=db_path, starting_equity=equity, variant=variant,
        )
        engine.replay_window(lo, warmup_end)               # IS warm-up
        eq, trades = engine.replay_window_oos(warmup_end, hi)  # OOS evaluation
        metrics = calculate_metrics(
            eq, _adapt_trades(trades),
            strategy_name=f"micro::{variant.name}",
            timeframe="tick",
            period="single-pass OOS",
        )
        row = metrics.to_dict()
        row["is_benchmark"] = False
        return row
    except Exception as exc:  # one bad variant must not abort the whole sweep
        logger.warning("[sweep] variant %s failed: %s", variant.name, exc)
        return None


def _print_leaderboard(df: pd.DataFrame) -> None:
    cols = ["strategy", "total_trades", "win_rate_pct", "expected_value",
            "total_return_pct", "profit_factor", "composite_score"]
    view = df[[c for c in cols if c in df.columns]].copy()
    sep = "=" * 92
    print("\n" + sep)
    print("MULTI-VARIANT LEADERBOARD — ranked by expected_value (avg $ PnL / trade)")
    print(sep)
    print(view.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print(sep)


def _register_winner(df: pd.DataFrame, variants_by_name: dict) -> None:
    from strategy.builder import StrategyBuilder
    from strategy.registry import StrategyRegistry
    from strategy.spec import EntryRules

    top = df.iloc[0]
    name = str(top["strategy"]).split("::", 1)[-1]
    v = variants_by_name.get(name)
    if v is None:
        er = EntryRules()
    elif v.entry_rules is not None:
        er = v.entry_rules
    else:
        er = EntryRules(atr_mult_tp=v.atr_mult_tp, stop_floor_atr_mult=v.stop_floor_atr_mult)
    metrics = {
        "oos_trade_count":  int(top["total_trades"]),
        "sharpe_oos":       float(top["sharpe_ratio"]),
        "max_drawdown_pct": float(top["max_drawdown_pct"]),
        "profit_factor":    float(top["profit_factor"]),
        "win_rate_pct":     float(top["win_rate_pct"]),
        "composite_score":  float(top["composite_score"]),
    }
    try:
        builder = StrategyBuilder(registry=StrategyRegistry())
        spec = builder.build(
            strategy_name=str(top["strategy"]), timeframe="tick",
            metrics=metrics, entry_rules=er, backtest_results_path=_SWEEP_DB,
        )
        print(f"\n✓ Registered winner '{spec.name}' v{spec.version} (status={spec.status})")
    except ValueError as exc:
        print(f"\n⚠️  Winner did not meet the registration bar — not registered: {exc}")


def main() -> None:
    p = argparse.ArgumentParser(description="Multi-variant tick backtest sweep")
    p.add_argument("--db", default="data/lob_tick.db")
    p.add_argument("--warmup-days", type=float, default=5.0)
    p.add_argument("--equity", type=float, default=10_000.0)
    p.add_argument("--register-winner", action="store_true",
                   help="register the top variant into the strategy registry")
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

    variants = _variants()
    logger.info("Sweep over %s | warm-up %s→%s | OOS %s→%s | %d variants",
                db_path, _d(lo), _d(warmup_end), _d(warmup_end), _d(hi), len(variants))

    rows = []
    for v in variants:
        logger.info("running variant: %s", v.name)
        row = _run_variant(v, str(db_path), args.equity, lo, warmup_end, hi)
        if row is not None:
            rows.append(row)

    if not rows:
        print("No variant produced results.")
        return

    df = (
        pd.DataFrame(rows)
        .sort_values(["expected_value", "total_return_pct"], ascending=False)
        .reset_index(drop=True)
    )
    _print_leaderboard(df)

    # Persist to the dedicated sweep DB (separate from data/backtest_results.db).
    from backtesting.walk_forward import _save_results
    _save_results(df, db_path=_SWEEP_DB, csv_path=_SWEEP_CSV)
    print(f"\nSaved {len(df)} rows → {_SWEEP_DB}")

    if args.register_winner:
        _register_winner(df, {v.name: v for v in variants})


if __name__ == "__main__":
    main()
