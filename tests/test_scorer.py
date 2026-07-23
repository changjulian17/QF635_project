"""Tests for XGBoostScorer and ScorerFactory in strategy/scorer.py."""

import json
import sqlite3

import numpy as np

from models import FeatureVector, MicroSignal
from strategy.scorer import FEATURE_ORDER, XGBoostScorer, ScorerFactory


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_db(tmp_path, n: int = 60, separable: bool = False) -> str:
    """
    Build a SQLite db with n synthetic APPROVED + labeled signal_records.
    separable=True: obi_zscore (index 1 of FEATURE_ORDER) sign → outcome,
    giving XGBoost a learnable signal so AUC is well above chance.
    """
    db = str(tmp_path / "reg.db")
    rng = np.random.default_rng(42)
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE signal_records (
                signal_id        TEXT PRIMARY KEY,
                strategy_id      TEXT,
                timestamp        TEXT,
                micro_signal     TEXT,
                gate_passed      TEXT,
                rejection_reason TEXT,
                lob_status       TEXT,
                heartbeat_status TEXT,
                obi_zscore       REAL,
                cvd_delta        REAL,
                spread_bps       REAL,
                confidence       REAL,
                direction        TEXT,
                outcome          TEXT DEFAULT '',
                pnl              REAL DEFAULT 0.0,
                pnl_pct          REAL DEFAULT 0.0,
                duration_min     REAL DEFAULT 0.0,
                features_json    TEXT DEFAULT NULL
            )
        """)
        for i in range(n):
            fv_arr = rng.standard_normal(15).tolist()
            if separable:
                fv_arr[1] = float(rng.choice([-2.0, 2.0]))   # obi_zscore at index 1
                outcome = "WIN" if fv_arr[1] > 0 else "LOSS"
            else:
                outcome = rng.choice(["WIN", "LOSS"])
            conn.execute(
                "INSERT INTO signal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(i), "v3.0", f"2026-01-{(i % 28) + 1:02d}T{i % 24:02d}:00:00",
                    "SWEEP_WITH_PROTECTION", "APPROVED", "",
                    "SYNCED", "HEALTHY",
                    fv_arr[1], fv_arr[2], fv_arr[6],   # obi_zscore, cvd_delta, spread_bps
                    0.7, "LONG",
                    outcome, 0.0, 0.0, 3.0 + (i % 8),  # duration_min: 3–10 min, all > 2.0
                    json.dumps(fv_arr),
                ),
            )
        conn.commit()
    return db


def _signal() -> MicroSignal:
    return MicroSignal(
        signal_type="SWEEP_WITH_PROTECTION",
        direction="LONG",
        timestamp_ms=0,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_scorer_trains_on_synthetic_labeled_data(tmp_path):
    """train_from_registry() completes without exception on 60 synthetic rows with learnable signal."""
    db = _make_db(tmp_path, n=60, separable=True)
    scorer = XGBoostScorer.train_from_registry(db_path=db, min_trades=50)
    assert scorer._model is not None


def test_scorer_auc_above_chance_on_synthetic_data(tmp_path):
    """With separable features (obi_zscore sign → outcome), AUC > 0.60."""
    db = _make_db(tmp_path, n=100, separable=True)
    scorer = XGBoostScorer.train_from_registry(db_path=db, min_trades=50)
    assert scorer.auc_on_test_set() > 0.60


def test_feature_importances_cover_all_features(tmp_path):
    """feature_importances() must return all 15 features with positive total importance."""
    db = _make_db(tmp_path, n=100, separable=True)
    scorer = XGBoostScorer.train_from_registry(db_path=db, min_trades=50)
    imps = scorer.feature_importances()
    assert set(imps.index) == set(FEATURE_ORDER), \
        f"Missing features: {set(FEATURE_ORDER) - set(imps.index)}"
    assert imps.sum() > 0, "All importances are zero — model did not learn"
    assert len(imps) == len(FEATURE_ORDER)


def test_scorer_falls_back_to_rule_based_when_no_model_file(tmp_path):
    """ScorerFactory.load_or_fallback(missing_path) returns a RuleBasedScorer."""
    from strategy.executor import RuleBasedScorer
    scorer = ScorerFactory.load_or_fallback(path=str(tmp_path / "nonexistent.pkl"))
    assert isinstance(scorer, RuleBasedScorer)


def test_scorer_saves_and_loads_roundtrip(tmp_path):
    """save() then load() yields identical predictions on 5 random FeatureVectors."""
    db = _make_db(tmp_path, n=100, separable=True)
    scorer = XGBoostScorer.train_from_registry(db_path=db, min_trades=50)
    pkl = str(tmp_path / "scorer.pkl")
    scorer.save(pkl)
    loaded = XGBoostScorer.load(pkl)

    rng = np.random.default_rng(0)
    sig = _signal()
    for _ in range(5):
        # Build a FeatureVector with random numeric values via to_ml_array() round-trip
        raw = rng.standard_normal(15).tolist()
        fv = FeatureVector(
            price_vs_vwap=raw[0], obi_zscore=raw[1], cvd_delta=raw[2],
            vol_ratio=abs(raw[3]), atr_percentile=abs(raw[4]) % 1,
            rsi_value=50.0 + raw[5] * 5, spread_bps=abs(raw[6]) + 1,
            pattern_r2=abs(raw[7]) % 1, vwap_reclaim=int(raw[8] > 0),
            vol_climax=int(raw[9] > 0), cvd_positive=int(raw[10] > 0),
            wall_detected=int(raw[11] > 0), wall_distance_bps=abs(raw[12]),
            absorption_ratio=abs(raw[13]) % 1,
            protection_wall_present=int(raw[14] > 0),
        )
        s1 = scorer.score(fv, sig)
        s2 = loaded.score(fv, sig)
        assert abs(s1 - s2) < 1e-9, f"Roundtrip mismatch: {s1} vs {s2}"


def test_score_output_bounded(tmp_path):
    """score() always returns a value in [0.0, 1.0] for any FeatureVector."""
    db = _make_db(tmp_path, n=100, separable=True)
    scorer = XGBoostScorer.train_from_registry(db_path=db, min_trades=50)
    sig = _signal()
    for _ in range(20):
        score = scorer.score(FeatureVector(), sig)
        assert 0.0 <= score <= 1.0, f"score {score} out of [0, 1]"


def test_score_with_no_model_returns_neutral():
    """XGBoostScorer with no trained model must return exactly 0.5, not raise."""
    scorer = XGBoostScorer()
    result = scorer.score(FeatureVector(), _signal())
    assert result == 0.5
