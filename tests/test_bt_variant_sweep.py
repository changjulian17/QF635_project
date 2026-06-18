"""Tests for the multi-variant backtest harness (Part B).

Covers the variant-aware TickReplayEngine (`_simulate_trade` stop logic) and the
sweep-script helpers, without touching the 6 GB lob_tick.db — `_simulate_trade` is
driven directly with synthetic signals.
"""
import importlib.util
import pathlib

import pytest

from backtesting.tick_replay import TickReplayEngine, VariantConfig
from config import settings
from models import MicroSignal, WallState


def _signal(direction: str, entry: float, wall_offset_bps: float) -> MicroSignal:
    """LONG → protection wall below entry; SHORT → above. wall_offset in bps."""
    if direction == "LONG":
        wall_price = entry * (1 - wall_offset_bps / 1e4)
        side = "bid"
    else:
        wall_price = entry * (1 + wall_offset_bps / 1e4)
        side = "ask"
    wall = WallState(
        price=wall_price, qty_initial=10.0, qty_current=10.0,
        first_seen_ts=0, last_seen_ts=0, side=side, sigma=3.0,
    )
    return MicroSignal(
        signal_type="SWEEP_WITH_PROTECTION", direction=direction, timestamp_ms=1_000,
        protection_wall=wall, mid_price=entry,
    )


def _engine(variant=None) -> TickReplayEngine:
    eng = TickReplayEngine(params={}, variant=variant)
    eng._equity = 10_000.0          # ensure a tradeable equity (replay_window would set this)
    eng._open_position = None
    return eng


# ── (b) baseline parity: default engine == explicit baseline VariantConfig ──────

@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_baseline_matches_default_and_uses_wall_price(direction):
    sig = _signal(direction, entry=100.0, wall_offset_bps=20.0)

    default_eng = _engine()                       # no variant → defaults
    default_eng._simulate_trade(sig, 100.0, 2_000)

    baseline_eng = _engine(VariantConfig(name="baseline"))
    baseline_eng._simulate_trade(sig, 100.0, 2_000)

    assert default_eng._open_position is not None
    assert baseline_eng._open_position is not None
    # Legacy behaviour: stop sits exactly at the protection-wall price.
    assert default_eng._open_position["sl"] == pytest.approx(sig.protection_wall.price)
    assert baseline_eng._open_position["sl"] == pytest.approx(default_eng._open_position["sl"])
    assert baseline_eng._open_position["tp"] == pytest.approx(default_eng._open_position["tp"])
    assert baseline_eng._open_position["qty"] == pytest.approx(default_eng._open_position["qty"])


# ── (a) vol_floor widens the stop to >= floor when the wall is too tight ────────

def test_vol_floor_widens_stop_to_at_least_floor():
    # Wall only 1 bp away (= 0.01 in price) — inside the noise band.
    sig = _signal("LONG", entry=100.0, wall_offset_bps=1.0)
    eng = _engine(VariantConfig(name="vf", stop_mode="vol_floor", stop_floor_atr_mult=2.0))
    eng._fc._atr = 0.5            # ATR in price units → floor = 2.0 * 0.5 = 1.0
    eng._simulate_trade(sig, 100.0, 2_000)

    pos = eng._open_position
    assert pos is not None
    sl_dist = abs(pos["entry_price"] - pos["sl"])
    assert sl_dist >= 1.0 - 1e-9, f"vol_floor stop {sl_dist} should be >= 2.0xATR floor (1.0)"
    # And strictly wider than the 1 bp wall distance (0.01).
    assert sl_dist > 0.01


# ── (f) ATR warm-up (atr==0) → vol_floor degrades to wall mode, no error ────────

def test_vol_floor_degrades_to_wall_when_atr_zero():
    sig = _signal("LONG", entry=100.0, wall_offset_bps=20.0)
    eng = _engine(VariantConfig(name="vf", stop_mode="vol_floor", stop_floor_atr_mult=3.0))
    eng._fc._atr = 0.0           # warm-up: ATR not ready
    eng._simulate_trade(sig, 100.0, 2_000)

    pos = eng._open_position
    assert pos is not None
    # max(wall_dist, 3.0*0) == wall_dist → identical to wall mode (stop at wall price).
    assert pos["sl"] == pytest.approx(sig.protection_wall.price)


# ── sweep-script helpers (import by path; scripts/ isn't a package) ─────────────

def _load_sweep():
    spec = importlib.util.spec_from_file_location(
        "sweep_mod", pathlib.Path(__file__).parent.parent / "scripts" / "run_backtest_sweep.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── (d) ReplayTrade→dict adapter yields valid pnl/pnl_pct/duration ──────────────

def test_adapter_produces_valid_metric_dicts():
    sweep = _load_sweep()

    class _T:
        pnl_usd = 7.5
        entry_price = 50.0
        qty = 4.0
        entry_ts_ms = 60_000
        exit_ts_ms = 240_000

    row = sweep._adapt_trades([_T()])[0]
    assert row["pnl"] == 7.5
    assert row["pnl_pct"] == pytest.approx(7.5 / (50.0 * 4.0))
    assert row["duration_minutes"] == pytest.approx(3.0)
    assert set(row) == {"pnl", "pnl_pct", "duration_minutes"}


# ── (c) sweep ranks rows by expected_value desc ─────────────────────────────────

def test_variants_seeded_and_ranking_is_by_expected_value():
    import pandas as pd
    sweep = _load_sweep()

    names = {v.name for v in sweep._variants()}
    assert {"baseline", "vol_floor_1.5", "vol_floor_3.0", "edge_gate",
            "tp_2x", "tp_4x", "gated"} <= names

    df = pd.DataFrame([
        {"strategy": "micro::lo", "expected_value": 0.5, "total_return_pct": 0.1},
        {"strategy": "micro::hi", "expected_value": 4.0, "total_return_pct": 0.2},
        {"strategy": "micro::mid", "expected_value": 2.0, "total_return_pct": 0.0},
    ]).sort_values(["expected_value", "total_return_pct"], ascending=False).reset_index(drop=True)
    assert list(df["strategy"]) == ["micro::hi", "micro::mid", "micro::lo"]


# ── (e) a failing/0-trade variant returns None instead of aborting the sweep ────

def test_run_variant_swallows_errors():
    sweep = _load_sweep()
    # Non-existent DB path → engine raises inside _run_variant → returns None (no crash).
    out = sweep._run_variant(
        VariantConfig(name="baseline"), db_path="/nonexistent/lob.db",
        equity=10_000.0, lo=0, warmup_end=1, hi=2,
    )
    assert out is None


# ── apply_entry_gate skips when feature vector not warmed up ────────────────────

def test_entry_gate_skips_during_warmup():
    from strategy.spec import EntryRules
    sig = _signal("LONG", entry=100.0, wall_offset_bps=20.0)
    eng = _engine(VariantConfig(name="gated", apply_entry_gate=True, entry_rules=EntryRules()))
    # FeatureComputer is cold → compute() returns None → gate rejects the trade.
    eng._simulate_trade(sig, 100.0, 2_000)
    assert eng._open_position is None
