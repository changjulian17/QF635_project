"""
strategy/registry.py
====================
StrategyRegistry — dual-store persistence (YAML + SQLite) with promotion gates.

Lifecycle: RESEARCH -> BACKTEST -> PAPER -> LIVE

Each spec is written to:
  - {yaml_dir}/{name}_v{version}.yaml  (human-readable, git-trackable)
  - {db_path} strategies table          (queryable metadata + audit trail)

Specs are frozen once they reach PAPER status: re-registering the same
(name, version) with different parameters raises ValueError.
"""

from __future__ import annotations

import dataclasses
import math
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import yaml

from strategy.spec import EntryRules, StatisticalValidity, StrategySpec

_STATUS_RANK = {"RESEARCH": 0, "BACKTEST": 1, "PAPER": 2, "LIVE": 3}


class StrategyRegistry:
    PAPER_MIN_OOS_TRADES = 50
    PAPER_MIN_SHARPE = 1.0
    PAPER_MAX_DRAWDOWN_PCT = 15.0
    PAPER_MIN_PROFIT_FACTOR = 1.3
    LIVE_MIN_WEEKS_RUNNING = 2
    LIVE_MIN_PAPER_TRADES = 20
    LIVE_MIN_PAPER_SHARPE_RATIO = 0.70  # paper Sharpe >= 70% of backtest Sharpe

    def __init__(
        self,
        db_path: str = "strategies/registry.db",
        yaml_dir: str = "strategies",
    ) -> None:
        self._db_path = db_path
        self._yaml_dir = yaml_dir
        os.makedirs(yaml_dir, exist_ok=True)
        os.makedirs(os.path.dirname(db_path), exist_ok=True) if os.path.dirname(db_path) else None
        self._init_db()

    # ------------------------------------------------------------------ #
    # Internal helpers                                                      #
    # ------------------------------------------------------------------ #

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS strategies (
                    strategy_id TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    version     INTEGER NOT NULL,
                    status      TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    promoted_at TEXT,
                    UNIQUE (name, version)
                )
            """)

    def _yaml_path(self, name: str, version: int) -> str:
        safe_name = name.replace(" ", "_").replace("/", "_")
        return os.path.join(self._yaml_dir, f"{safe_name}_v{version}.yaml")

    def _write_yaml(self, spec: StrategySpec) -> None:
        path = self._yaml_path(spec.name, spec.version)
        with open(path, "w") as f:
            yaml.dump(spec.to_dict(), f, default_flow_style=False, sort_keys=True)

    def _load_yaml(self, name: str, version: int) -> StrategySpec:
        path = self._yaml_path(name, version)
        with open(path) as f:
            return StrategySpec.from_dict(yaml.safe_load(f))

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def register(self, spec: StrategySpec) -> None:
        """Validate, write YAML, and INSERT into DB. Raises ValueError on duplicate."""
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT strategy_id, status FROM strategies WHERE name=? AND version=?",
                (spec.name, spec.version),
            ).fetchone()
            if existing:
                if _STATUS_RANK.get(existing["status"], 0) >= _STATUS_RANK["PAPER"]:
                    raise ValueError(
                        f"Spec '{spec.name}' v{spec.version} is frozen after "
                        f"{existing['status']} promotion — bump the version to make changes."
                    )
                raise ValueError(
                    f"Spec '{spec.name}' v{spec.version} already registered "
                    f"(id={existing['strategy_id']})."
                )
            self._write_yaml(spec)
            conn.execute(
                "INSERT INTO strategies (strategy_id, name, version, status, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (spec.strategy_id, spec.name, spec.version, spec.status, spec.created_at),
            )

    def can_promote_to_paper(
        self, spec: StrategySpec
    ) -> tuple[bool, list[str]]:
        """Gate: trade count, Sharpe, drawdown, profit factor. Returns (True, []) or (False, [reasons])."""
        reasons: list[str] = []
        if spec.validity.oos_trade_count < self.PAPER_MIN_OOS_TRADES:
            reasons.append(
                f"oos_trade_count={spec.validity.oos_trade_count} < {self.PAPER_MIN_OOS_TRADES} required"
            )
        if spec.validity.sharpe_oos < self.PAPER_MIN_SHARPE:
            reasons.append(
                f"sharpe_oos={spec.validity.sharpe_oos:.3f} < {self.PAPER_MIN_SHARPE} required"
            )
        if spec.validity.max_drawdown_pct > self.PAPER_MAX_DRAWDOWN_PCT:
            reasons.append(
                f"max_drawdown_pct={spec.validity.max_drawdown_pct:.1f}% > {self.PAPER_MAX_DRAWDOWN_PCT}% limit"
            )
        if spec.validity.profit_factor < self.PAPER_MIN_PROFIT_FACTOR:
            reasons.append(
                f"profit_factor={spec.validity.profit_factor:.2f} < {self.PAPER_MIN_PROFIT_FACTOR} required"
            )
        return (not reasons, reasons)

    def can_promote_to_live(
        self, spec: StrategySpec, paper_metrics: dict
    ) -> tuple[bool, list[str]]:
        """
        Gate for LIVE promotion.

        paper_metrics keys required:
          weeks_running  (int)   — weeks since PAPER promotion
          total_trades   (int)   — total PAPER trades executed
          sharpe_rolling (float) — rolling Sharpe over paper period
        """
        reasons: list[str] = []
        weeks = paper_metrics.get("weeks_running", 0)
        trades = paper_metrics.get("total_trades", 0)
        sharpe = paper_metrics.get("sharpe_rolling", 0.0)
        min_sharpe = self.LIVE_MIN_PAPER_SHARPE_RATIO * spec.validity.sharpe_oos

        if weeks < self.LIVE_MIN_WEEKS_RUNNING:
            reasons.append(
                f"weeks_running={weeks} < {self.LIVE_MIN_WEEKS_RUNNING} required"
            )
        if trades < self.LIVE_MIN_PAPER_TRADES:
            reasons.append(
                f"total_trades={trades} < {self.LIVE_MIN_PAPER_TRADES} required"
            )
        if sharpe < min_sharpe:
            reasons.append(
                f"sharpe_rolling={sharpe:.3f} < {min_sharpe:.3f} "
                f"(70% of backtest Sharpe {spec.validity.sharpe_oos:.3f})"
            )
        return (not reasons, reasons)

    def promote(
        self,
        strategy_id: str,
        new_status: str,
        paper_metrics: Optional[dict] = None,
    ) -> None:
        """
        Update status in DB (commits first) then rewrites YAML.

        Promotion gates are enforced automatically:
          PAPER: must pass can_promote_to_paper().
          LIVE:  must pass can_promote_to_live(); caller must supply paper_metrics.

        DB is written before YAML so the DB is always the authoritative forward record.
        A crash after DB commit but before YAML rewrite leaves YAML at old status;
        re-running promote() will recover.
        """
        if new_status not in _STATUS_RANK:
            raise ValueError(f"Unknown status '{new_status}'")
        name = version = None
        with self._conn() as conn:
            row = conn.execute(
                "SELECT name, version, status FROM strategies WHERE strategy_id=?",
                (strategy_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"strategy_id '{strategy_id}' not found in registry.")
            current_rank = _STATUS_RANK.get(row["status"], 0)
            new_rank = _STATUS_RANK[new_status]
            if new_rank <= current_rank:
                raise ValueError(
                    f"Cannot demote '{row['name']}' v{row['version']} "
                    f"from {row['status']} to {new_status}."
                )
            name, version = row["name"], row["version"]

        # Load the spec and enforce gates before touching the DB.
        spec = self._load_yaml(name, version)
        if new_status == "PAPER":
            ok, reasons = self.can_promote_to_paper(spec)
            if not ok:
                raise ValueError(
                    f"Promotion to PAPER blocked for '{name}' v{version}: {reasons}"
                )
        elif new_status == "LIVE":
            if paper_metrics is None:
                raise ValueError(
                    "paper_metrics is required to promote to LIVE. "
                    "Supply weeks_running, total_trades, sharpe_rolling."
                )
            ok, reasons = self.can_promote_to_live(spec, paper_metrics)
            if not ok:
                raise ValueError(
                    f"Promotion to LIVE blocked for '{name}' v{version}: {reasons}"
                )

        with self._conn() as conn:
            promoted_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE strategies SET status=?, promoted_at=? WHERE strategy_id=?",
                (new_status, promoted_at, strategy_id),
            )
        self._write_yaml(dataclasses.replace(spec, status=new_status))

    def get_active_strategy(self) -> Optional[StrategySpec]:
        """Return highest-status spec. Ties broken by most-recent created_at."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT name, version, status, created_at FROM strategies"
            ).fetchall()
        if not rows:
            return None
        best = max(rows, key=lambda r: (_STATUS_RANK.get(r["status"], 0), r["created_at"]))
        return self._load_yaml(best["name"], best["version"])

    def list_all(self) -> list[StrategySpec]:
        """Return all specs ordered by status rank (desc) then created_at (desc)."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT name, version, status, created_at FROM strategies"
            ).fetchall()
        specs = [self._load_yaml(r["name"], r["version"]) for r in rows]
        return sorted(
            specs,
            key=lambda s: (_STATUS_RANK.get(s.status, 0), s.created_at),
            reverse=True,
        )

    def _max_version(self, name: str) -> Optional[int]:
        """Return the highest registered version for a given name, or None."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MAX(version) FROM strategies WHERE name=?", (name,)
            ).fetchone()
        return row[0]

    def count_paper_trades(self, strategy_id: str) -> int:
        """Count APPROVED trades with a WIN/LOSS outcome for this strategy.

        Returns 0 if signal_records has not been created by SignalTelemetry yet.
        """
        try:
            with self._conn() as conn:
                row = conn.execute(
                    """SELECT COUNT(*) FROM signal_records
                       WHERE strategy_id = ?
                         AND gate_passed = 'APPROVED'
                         AND outcome IN ('WIN', 'LOSS')""",
                    (strategy_id,),
                ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.OperationalError:
            return 0

    def compute_rolling_sharpe(self, strategy_id: str, days: int) -> float:
        """
        Trade-level annualised Sharpe over the last `days` calendar days.

        Uses pnl_pct (net return per trade) from signal_records.
        Returns 0.0 if fewer than 2 closed trades are found.
        Annualisation factor = sqrt(trades_per_day × 365).
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    """SELECT pnl_pct FROM signal_records
                       WHERE strategy_id = ?
                         AND gate_passed = 'APPROVED'
                         AND outcome IN ('WIN', 'LOSS')
                         AND timestamp >= ?
                       ORDER BY timestamp ASC""",
                    (strategy_id, cutoff),
                ).fetchall()
        except sqlite3.OperationalError:
            return 0.0
        if len(rows) < 2:
            return 0.0
        returns = [r[0] for r in rows]
        n = len(returns)
        mean_r = sum(returns) / n
        variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
        std_r = math.sqrt(variance)
        if std_r < 1e-9:
            return 0.0
        ann_factor = math.sqrt((n / days) * 365)
        return (mean_r / std_r) * ann_factor
