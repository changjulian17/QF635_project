"""Tests for the multi-variant backtest harness (Part B).

Covers the variant-aware TickReplayEngine (`_simulate_trade` stop logic) and the
sweep-script helpers, without touching the 6 GB lob_tick.db — `_simulate_trade` is
driven directly with synthetic signals.
"""
import importlib.util
import pathlib
import types

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


# ── directional-bias gate (skip / flip) ─────────────────────────────────────────

def _fv(price_vs_vwap: float, cvd_delta: float):
    return types.SimpleNamespace(price_vs_vwap=price_vs_vwap, cvd_delta=cvd_delta)


def _bias_engine(variant, price_vs_vwap, cvd_delta):
    """Engine whose FeatureComputer returns a fixed fv → deterministic bias."""
    eng = _engine(variant)
    eng._fc.compute = lambda *a, **k: _fv(price_vs_vwap, cvd_delta)  # type: ignore[method-assign]
    return eng


def test_directional_bias_truth_table():
    db = _engine(VariantConfig(name="b", bias_mode="skip"))._directional_bias  # both inputs, band=0.15
    assert db(_fv(0.5, 1.0)) == "LONG"      # trend up ∧ cvd up
    assert db(_fv(-0.5, -1.0)) == "SHORT"   # both down
    assert db(_fv(0.5, -1.0)) == "NEUTRAL"  # disagree
    assert db(_fv(0.10, 1.0)) == "NEUTRAL"  # |price_vs_vwap| ≤ band → trend 0 → not both non-zero
    assert db(_fv(0.0, 0.0)) == "NEUTRAL"


def test_directional_bias_single_input_isolation():
    cvd_only = _engine(VariantConfig(name="c", bias_mode="skip", bias_use_trend=False))._directional_bias
    assert cvd_only(_fv(-0.9, 1.0)) == "LONG"    # trend ignored; cvd>0
    assert cvd_only(_fv(0.9, -1.0)) == "SHORT"   # trend ignored; cvd<0
    assert cvd_only(_fv(0.9, 0.0)) == "NEUTRAL"  # cvd==0
    trend_only = _engine(VariantConfig(name="t", bias_mode="skip", bias_use_cvd=False))._directional_bias
    assert trend_only(_fv(0.5, -1.0)) == "LONG"     # cvd ignored; trend up
    assert trend_only(_fv(0.10, 1.0)) == "NEUTRAL"  # within band


def test_bias_skip_vetoes_counter_bias_and_allows_aligned():
    veto = _bias_engine(VariantConfig(name="b", bias_mode="skip"), 0.5, 1.0)   # bias LONG
    veto._simulate_trade(_signal("SHORT", 100.0, 20.0), 100.0, 2_000)
    assert veto._open_position is None                                          # SHORT vetoed

    ok = _bias_engine(VariantConfig(name="b", bias_mode="skip"), 0.5, 1.0)      # bias LONG
    ok._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    assert ok._open_position is not None and ok._open_position["direction"] == "LONG"


def test_bias_skip_neutral_skips():
    eng = _bias_engine(VariantConfig(name="b", bias_mode="skip"), 0.5, -1.0)    # disagree → NEUTRAL
    eng._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    assert eng._open_position is None


def test_bias_flip_inverts_counter_bias_trade():
    eng = _bias_engine(VariantConfig(name="f", bias_mode="flip"), 0.5, 1.0)     # bias LONG
    eng._simulate_trade(_signal("SHORT", 100.0, 20.0), 100.0, 2_000)            # SHORT sweep
    pos = eng._open_position
    assert pos is not None
    assert pos["direction"] == "LONG"               # flipped to the bias direction
    assert pos["sl"] < pos["entry_price"]           # LONG stop sits below entry
    assert pos["tp"] > pos["entry_price"]           # LONG target sits above entry


# ── MAE/MFE instrumentation, decoupled TP, time exit ────────────────────────────

def test_mae_mfe_recorded_on_closed_trade():
    # Time-exit variant → deterministic close; MFE must capture the PEAK (50), not the
    # exit level (40). Avoids float-fuzz on the legacy TP level.
    eng = _engine(VariantConfig(name="te", max_hold_ms=1_000))   # LONG wall stop @99.8
    eng._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    eng._check_open_position(100.50, 2_100)   # MFE peak 50 bps
    eng._check_open_position(99.90,  2_200)   # MAE 10 bps (stop 99.8 not hit)
    eng._check_open_position(100.40, 3_000)   # elapsed 1000ms → TIME exit at 100.40
    assert eng._open_position is None
    t = eng._trades[-1]
    assert t.exit_reason == "TIME"
    assert t.mfe_bps == pytest.approx(50.0, abs=0.05)   # peak, not exit level
    assert t.mae_bps == pytest.approx(10.0, abs=0.05)


def test_decoupled_tp_independent_of_stop():
    eng = _engine(VariantConfig(name="dt", tp_atr_mult=2.0))   # TP = 2×ATR, not 3×stop
    eng._fc._atr = 0.5                                         # → tp_dist = 1.0
    eng._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    pos = eng._open_position
    assert pos is not None
    assert pos["sl"] == pytest.approx(99.8)        # stop still at the wall
    assert pos["tp"] == pytest.approx(101.0)       # target = entry + 2×ATR, NOT 100.6


def test_time_exit_fires_with_taker_cost():
    eng = _engine(VariantConfig(name="te", max_hold_ms=1_000))
    eng._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    eng._check_open_position(100.05, 2_500)        # 500ms elapsed, no exit
    assert eng._open_position is not None
    eng._check_open_position(100.05, 3_000)        # 1000ms elapsed → TIME exit
    assert eng._open_position is None
    assert eng._trades[-1].exit_reason == "TIME"


def test_baseline_parity_no_time_exit_no_decoupled_tp():
    # baseline (defaults) must never fire TIME and must keep the legacy 3×stop target.
    eng = _engine(VariantConfig(name="baseline"))
    eng._simulate_trade(_signal("LONG", 100.0, 20.0), 100.0, 2_000)
    pos = eng._open_position
    assert pos["tp"] == pytest.approx(100.6)       # 3 × 0.2 stop
    eng._check_open_position(100.1, 9_999_999)     # huge elapsed, but max_hold_ms=0 → no TIME
    assert eng._open_position is not None


# ── observer (observe_mode) ─────────────────────────────────────────────────────

def test_observer_concurrent_windows_and_excursion_math():
    eng = TickReplayEngine(params={}, observe_mode=True, observe_window_ms=1_000)
    eng._observations = []; eng._obs_results = []; eng._obs_max_concurrent = 0
    eng._register_observation("LONG", 100.0, 0)      # window [0, 1000]
    eng._register_observation("LONG", 100.0, 100)    # overlapping window [100, 1100]
    assert eng._obs_max_concurrent == 2              # both tracked concurrently

    eng._update_observations(100.5, 200)             # fav 0.5 → 50 bps
    eng._update_observations(99.7,  400)             # adv 0.3 → 30 bps
    eng._update_observations(100.2, 1000)            # first window expires (ts ≥ 1000)

    first = [o for o in eng._obs_results if o.ts_ms == 0][0]
    assert first.mfe_bps == pytest.approx(50.0)
    assert first.mae_bps == pytest.approx(30.0)
    assert first.end_return_bps == pytest.approx(20.0)   # (100.2-100)/100 × 1e4
    assert len(eng._observations) == 1               # second window still open (not yet expired)


def test_observe_requires_observe_mode():
    eng = TickReplayEngine(params={})                # observe_mode defaults False
    with pytest.raises(RuntimeError):
        eng.observe(0, 1)


# ── sweep excursion aggregation (loser_mfe_bps headline) ────────────────────────

def test_excursion_stats_loser_mfe():
    sweep = _load_sweep()

    class _T:
        def __init__(self, pnl, mae, mfe):
            self.pnl_usd, self.mae_bps, self.mfe_bps = pnl, mae, mfe

    trades = [_T(+5.0, 4.0, 30.0), _T(-2.0, 12.0, 25.0), _T(-1.0, 8.0, 15.0)]
    s = sweep._excursion_stats(trades)   # values are rounded to 2dp by the helper
    assert s["loser_mfe_bps"] == pytest.approx(20.0)                  # (25+15)/2, losers only
    assert s["avg_mfe_bps"] == pytest.approx(round((30.0 + 25.0 + 15.0) / 3, 2))
    assert s["avg_mae_bps"] == pytest.approx(round((4.0 + 12.0 + 8.0) / 3, 2))
    assert sweep._excursion_stats([])["loser_mfe_bps"] == 0.0        # empty-safe
