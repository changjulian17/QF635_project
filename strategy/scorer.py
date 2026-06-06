"""XGBoost confidence scorer + factory for Gate 2 of StrategyExecutor."""

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import roc_auc_score
from xgboost import XGBClassifier

from models import FeatureVector, MicroSignal

logger = logging.getLogger(__name__)

# pattern_r2 (no live source) and protection_wall_present (constant at scoring time —
# only Sweep+Protection signals reach Gate 2) were dropped: they were always 0/constant
# in live + replay, wasting model capacity. Must stay aligned with FeatureVector.to_ml_array().
FEATURE_ORDER: list[str] = [
    "price_vs_vwap", "obi_zscore", "cvd_delta", "vol_ratio",
    "atr_percentile", "rsi_value", "spread_bps",
    "vwap_reclaim", "vol_climax", "cvd_positive", "wall_detected",
    "wall_distance_bps", "absorption_ratio",
]

_MODEL_STALE_DAYS = 30


@runtime_checkable
class BaseScorer(Protocol):
    def score(self, fv: FeatureVector, signal: MicroSignal) -> float: ...


class XGBoostScorer:
    """
    Trained on APPROVED signal_records from strategies/registry.db.
    Time-ordered 80/20 train/test split — no random shuffle.
    """

    def __init__(self) -> None:
        self._model: Optional[XGBClassifier] = None
        self._auc: float = 0.0
        self._calibration_error: float = 0.0
        self._trained_at: float = 0.0

    @classmethod
    def train_from_registry(
        cls,
        db_path: str = "strategies/registry.db",
        min_trades: int = 50,
    ) -> "XGBoostScorer":
        """
        Loads labeled APPROVED signals, trains model, validates AUC ≥ 0.62.
        Raises ValueError if min_trades not met, test split is single-class,
        or AUC < 0.62.
        """
        rows = _load_labeled_rows(db_path, min_trades)
        X = np.array([json.loads(r["features_json"]) for r in rows], dtype=float)
        y = np.array([1 if r["outcome"] == "WIN" else 0 for r in rows])

        split = int(len(rows) * 0.8)
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]

        if len(np.unique(y_test)) < 2:
            raise ValueError(
                f"Test split has only one class — collect more balanced WIN/LOSS outcomes "
                f"before training. (test set size={len(y_test)}, "
                f"classes={np.unique(y_test).tolist()})"
            )

        model = XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            eval_metric="logloss", random_state=42,
            early_stopping_rounds=20,
        )
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        if auc < 0.62:
            raise ValueError(
                f"AUC {auc:.3f} < 0.62 — model not reliable enough to replace RuleBasedScorer"
            )

        # Expected Calibration Error — measures whether score() = 0.7 means ~70% win rate.
        # n_bins=5 with strategy="quantile" is robust on small test sets.
        fraction_pos, mean_pred = calibration_curve(
            y_test, model.predict_proba(X_test)[:, 1], n_bins=5, strategy="quantile"
        )
        cal_error = float(np.mean(np.abs(fraction_pos - mean_pred)))
        if cal_error > 0.10:
            logger.warning(
                "[Scorer] ECE=%.3f > 0.10 — score() values are miscalibrated; "
                "Kelly-sizing is unreliable until more data is collected.",
                cal_error,
            )

        scorer = cls()
        scorer._model = model
        scorer._auc = auc
        scorer._calibration_error = cal_error
        scorer._trained_at = time.time()
        logger.info(
            "[Scorer] XGBoostScorer trained AUC=%.3f ECE=%.3f on %d trades",
            auc, cal_error, len(rows),
        )
        return scorer

    def score(self, fv: FeatureVector, signal: MicroSignal) -> float:
        """Win probability in [0.0, 1.0]. Returns neutral 0.5 if model not loaded."""
        if self._model is None:
            return 0.5
        x = np.array(fv.to_ml_array(), dtype=float).reshape(1, -1)
        return float(self._model.predict_proba(x)[0, 1])

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({
            "model":             self._model,
            "auc":               self._auc,
            "calibration_error": self._calibration_error,
            "trained_at":        self._trained_at,
        }, path)

    @classmethod
    def load(cls, path: str) -> "XGBoostScorer":
        data = joblib.load(path)
        scorer = cls()
        scorer._model             = data["model"]
        scorer._auc               = data["auc"]
        scorer._calibration_error = data.get("calibration_error", 0.0)
        scorer._trained_at        = data.get("trained_at", 0.0)
        return scorer

    def feature_importances(self) -> pd.Series:
        if self._model is None:
            return pd.Series(dtype=float)
        return pd.Series(
            self._model.feature_importances_, index=FEATURE_ORDER
        ).sort_values(ascending=False)

    def auc_on_test_set(self) -> float:
        return self._auc

    def calibration_error(self) -> float:
        """Expected Calibration Error on the held-out test set. Lower is better."""
        return self._calibration_error


class ScorerFactory:
    @staticmethod
    def load_or_fallback(
        path: str = "strategies/models/scorer.pkl",
    ) -> BaseScorer:
        """
        Loads XGBoostScorer from pkl if present; otherwise falls back to RuleBasedScorer.
        Logs which scorer is active at startup. Warns if model is older than 30 days.
        """
        from strategy.executor import RuleBasedScorer   # lazy import — avoids circular dep

        if Path(path).exists():
            try:
                scorer = XGBoostScorer.load(path)
                auc = scorer.auc_on_test_set()
                if auc < 0.62:
                    logger.warning(
                        "[ScorerFactory] Loaded model AUC=%.3f < 0.62 — falling back to RuleBasedScorer",
                        auc,
                    )
                    return RuleBasedScorer()
                age_days = (time.time() - scorer._trained_at) / 86_400
                if age_days > _MODEL_STALE_DAYS:
                    logger.warning(
                        "[ScorerFactory] XGBoostScorer is %.0f days old — consider retraining.",
                        age_days,
                    )
                logger.info(
                    "[ScorerFactory] XGBoostScorer loaded from %s (AUC=%.3f, ECE=%.3f, age=%.0fd)",
                    path, auc, scorer.calibration_error(), age_days,
                )
                return scorer
            except (OSError, EOFError, KeyError, ValueError, RuntimeError) as exc:
                logger.warning(
                    "[ScorerFactory] Failed to load %s: %s — falling back to RuleBasedScorer",
                    path, exc,
                )
        else:
            logger.info("[ScorerFactory] No model at %s — using RuleBasedScorer", path)
        return RuleBasedScorer()


def _load_labeled_rows(db_path: str, min_trades: int) -> list[dict]:
    """
    Returns time-ordered APPROVED rows that have non-empty features_json and a WIN/LOSS outcome.
    Excludes trades with duration_min <= 2.0 to filter out noise-stopped trades — short stops
    may reflect liquidity voids rather than genuine signal failure, which would confound training.
    """
    n_features = len(FEATURE_ORDER)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT features_json, outcome FROM signal_records
               WHERE gate_passed = 'APPROVED'
                 AND outcome IN ('WIN', 'LOSS')
                 AND features_json IS NOT NULL
                 AND features_json != ''
                 AND duration_min > 2.0
               ORDER BY timestamp ASC"""
        ).fetchall()
    valid: list[dict] = []
    for row in rows:
        arr = json.loads(row["features_json"])
        if len(arr) != n_features:
            logger.warning(
                "[Scorer] Skipping row with %d features (expected %d) — schema mismatch",
                len(arr), n_features,
            )
            continue
        valid.append(dict(row))
    if len(valid) < min_trades:
        raise ValueError(
            f"Need {min_trades}+ labeled trades with features_json, have {len(valid)} valid. "
            "Keep Phase 1N running."
        )
    return valid
