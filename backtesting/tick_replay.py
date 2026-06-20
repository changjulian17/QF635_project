"""
backtesting/tick_replay.py
==========================
Event-driven replay of data/lob_tick.db through the live feature/signal stack.

Reuses FeatureComputer, CVDCalculator, identify_walls, detect_absorption, and
detect_sweep_with_protection from the live codebase without asyncio.
detect_sweep_with_protection accepts an optional now_ms parameter so the
replay engine passes ts_ms instead of wall-clock time for freshness checks.
"""

from __future__ import annotations

import heapq
import json
import sqlite3
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Literal, Optional

import pandas as pd

from backtesting.costs import TransactionCostModel
from config import settings
from core.cvd import CVDCalculator
from models import (
    AggTrade,
    Candle,
    FeatureVector,
    LOBLevel,
    LOBSnapshot,
    MicroSignal,
    SharedState,
    WallState,
)
from strategy.features import FeatureComputer, FeatureParams
from strategy.microstructure import (
    detect_absorption,
    detect_sweep_with_protection,
    identify_walls,
    rolling_abs_move_threshold,
)
from strategy.spec import EntryRules

_CONSUMED_RATIO = 0.15      # pre-filter: skip walls still intact (mirrors live code)
_STALE_WALL_MS  = 30_000    # prune wall states absent > 30 s


@dataclass
class VariantConfig:
    """Per-variant strategy parameters for the multi-variant backtest sweep.

    Defaults reproduce the legacy single-pass behaviour exactly — wall-based stop,
    TP = settings.ATR_MULTIPLIER_TP, no extra edge floor, no entry gate — so a
    `baseline` variant matches scripts/run_backtest_singlepass.py byte-for-byte.
    """
    name:                str                       = "baseline"
    stop_mode:           Literal["wall", "vol_floor"] = "wall"
    stop_floor_atr_mult: float                     = 0.0
    atr_mult_tp:         float                     = settings.ATR_MULTIPLIER_TP
    min_edge_bps:        float                     = 0.0
    apply_entry_gate:    bool                      = False
    entry_rules:         Optional[EntryRules]      = None
    # Directional-bias gate from CVD + trend. "off": no bias. "skip": veto sweeps that
    # fight the bias. "flip": take the bias direction instead when they conflict.
    bias_mode:           Literal["off", "skip", "flip"] = "off"
    bias_use_cvd:        bool                      = True
    bias_use_trend:      bool                      = True
    bias_vwap_band:      float                     = 0.15
    # Decoupled target / time exit (for the "does a wider stop recover?" test).
    # tp_atr_mult > 0 → TP = tp_atr_mult × ATR, independent of the stop distance
    # (0 ⇒ legacy TP = atr_mult_tp × stop). max_hold_ms > 0 → force exit after that
    # long (0 ⇒ no time exit).
    tp_atr_mult:         float                     = 0.0
    max_hold_ms:         float                     = 0.0


@dataclass
class ReplayEvent:
    ts_ms: int
    kind:  Literal["depth", "trade"]
    data:  dict


@dataclass
class ReplayTrade:
    entry_ts_ms:  int
    exit_ts_ms:   int
    direction:    str    # "LONG" | "SHORT"
    entry_price:  float
    exit_price:   float
    qty:          float
    pnl_usd:      float  # net of both entry and exit costs
    exit_reason:  str    # "SL" | "TP" | "EOD" | "TIME"
    signal:       MicroSignal
    mae_bps:      float = 0.0  # max adverse excursion: furthest AGAINST us (bps from entry)
    mfe_bps:      float = 0.0  # max favorable excursion: furthest FOR us (bps from entry)


@dataclass
class SignalObservation:
    """Forward-excursion record for ONE sweep signal, independent of any position.

    Produced by the observer pass (observe_mode); measures the unconstrained N-min path
    after a signal — used to decide whether losers recover (stop problem) or not (direction).
    """
    ts_ms:          int
    direction:      str    # "LONG" | "SHORT"
    entry_price:    float
    mae_bps:        float  # furthest against the signal direction over the window (bps)
    mfe_bps:        float  # furthest in favour over the window (bps)
    end_return_bps: float  # signed return entry→window-close (bps); <0 = would-be loser


class TickReplayEngine:
    """
    Replays lob_tick.db through the live feature stack in synchronous mode.

    Call replay_window(start_ms, end_ms) to run a backtest over a time window.
    The engine can be called multiple times; state is reset at the start of
    each replay_window call.

    Parameters
    ----------
    params          : dict with optional "feature" sub-dict for FeatureParams.
    db_path         : Path to lob_tick.db.
    starting_equity : Starting portfolio equity in USDT.
    candle_minutes  : Bar length for OHLCV candle synthesis from trades.
    collect_features: If True, FeatureVector computed at each depth event is
                      appended to self.feature_history for the fidelity test.
    """

    def __init__(
        self,
        params: dict,
        db_path: str = "data/lob_tick.db",
        starting_equity: float = 10_000.0,
        candle_minutes: int = 1,
        collect_features: bool = False,
        variant: Optional[VariantConfig] = None,
        observe_mode: bool = False,
        observe_window_ms: int = 600_000,
    ) -> None:
        self._db_path         = db_path
        self._fc              = FeatureComputer(FeatureParams(**params.get("feature", {})))
        self._cvd             = CVDCalculator()
        self._shared_state    = SharedState(lob_status="SYNCED")
        self._starting_equity = starting_equity
        self._candle_minutes  = candle_minutes
        self._collect_features = collect_features

        # Observer (measurement-only) state. When observe_mode, the engine runs detection
        # on every event WITHOUT trading and records each signal's forward MAE/MFE over the
        # next observe_window_ms (overlapping/concurrent windows). Trading variants leave
        # observe_mode False, so the trading path is unchanged.
        self._observe_mode      = observe_mode
        self._observe_window_ms = observe_window_ms
        self._observations: list[dict] = []                 # active windows
        self._obs_results:  list[SignalObservation] = []    # retired windows
        self._obs_max_concurrent = 0

        # Variant config — defaults reproduce legacy single-pass behaviour exactly.
        self._variant = variant or VariantConfig()
        # Entry-gate scorer (only when the variant opts in). Lazy import avoids a
        # circular dependency at module load (executor → spec, but not backtesting).
        self._gate_scorer = None
        if self._variant.apply_entry_gate:
            from strategy.executor import RuleBasedScorer
            self._gate_scorer = RuleBasedScorer(self._variant.entry_rules)

        # Public — populated when collect_features=True
        self.feature_history: list[tuple[int, FeatureVector]] = []

        # Runtime state (reset in replay_window)
        self._equity:           float                  = starting_equity
        self._wall_states:      dict[float, WallState] = {}
        self._absorption_flags: dict[float, bool]      = {}
        self._open_position:    Optional[dict]          = None
        self._equity_curve:     list[tuple[int, float]] = []
        self._trades:           list[ReplayTrade]       = []
        self._cost_model        = TransactionCostModel()

        # Candle accumulator
        self._bar_start_ms: int   = 0
        self._bar_open:     float = 0.0
        self._bar_high:     float = 0.0
        self._bar_low:      float = 0.0
        self._bar_close:    float = 0.0
        self._bar_volume:   float = 0.0

        # Misc tracking
        self._last_day:          int   = -1
        self._prev_mid:          float = 0.0
        self._last_trade_price:  float = 0.0
        self._abs_mid_history: deque[float] = deque(maxlen=settings.MICRO_PRICE_MOVE_WINDOW)

    # ── Public API ────────────────────────────────────────────────────────────

    def replay_window(
        self, start_ms: int, end_ms: int
    ) -> tuple[pd.Series, list[ReplayTrade]]:
        """
        Replay events from [start_ms, end_ms].

        Returns
        -------
        equity_curve : pd.Series indexed by ts_ms. One point per fill event
                       (entry cost deduction and position close), bookended by
                       (start_ms, starting_equity) and (end_ms, final_equity).
                       Sparse — resample + ffill downstream for dense series.
        trades       : list[ReplayTrade], one per closed position (includes EOD).
        """
        self._reset(start_ms)

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            for event in self._stream_events(conn, start_ms, end_ms):
                if event.kind == "depth":
                    self._process_depth(event)
                else:
                    self._process_trade(event)

        # EOD close — use last trade price, falling back to mid if no trades occurred in window
        if self._open_position is not None:
            close_price = self._last_trade_price if self._last_trade_price > 0 else self._prev_mid
            if close_price > 0:
                self._close_position(close_price, end_ms, "EOD")
            else:
                logger.warning("[TickReplay] EOD: no price available to close position — skipping close")

        self._equity_curve.append((end_ms, self._equity))

        ts_vals  = [t for t, _ in self._equity_curve]
        eq_vals  = [e for _, e in self._equity_curve]
        dt_index = pd.to_datetime(ts_vals, unit="ms", utc=True)
        return pd.Series(eq_vals, index=dt_index), list(self._trades)

    def replay_window_oos(
        self, start_ms: int, end_ms: int
    ) -> tuple[pd.Series, list[ReplayTrade]]:
        """
        OOS evaluation pass that preserves FeatureComputer warm-up state from a
        preceding IS replay_window() call.

        Use this as the second step of a walk-forward pair:
            engine.replay_window(is_start, is_end)      # IS: warms up Welford stats
            eq, trades = engine.replay_window_oos(oos_start, oos_end)  # OOS: evaluate

        Resets equity, trades, wall states, and positions (so OOS PnL starts
        fresh) but does NOT call fc.reset() — IS Welford distributions carry
        forward so OBI z-scores, ATR percentiles, and vol_ratio are meaningful
        from the very first OOS bar rather than starting cold.
        """
        self._reset_for_oos(start_ms)

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            for event in self._stream_events(conn, start_ms, end_ms):
                if event.kind == "depth":
                    self._process_depth(event)
                else:
                    self._process_trade(event)

        if self._open_position is not None:
            close_price = self._last_trade_price if self._last_trade_price > 0 else self._prev_mid
            if close_price > 0:
                self._close_position(close_price, end_ms, "EOD")
            else:
                logger.warning("[TickReplay] EOD: no price available to close position — skipping close")

        self._equity_curve.append((end_ms, self._equity))

        ts_vals  = [t for t, _ in self._equity_curve]
        eq_vals  = [e for _, e in self._equity_curve]
        dt_index = pd.to_datetime(ts_vals, unit="ms", utc=True)
        return pd.Series(eq_vals, index=dt_index), list(self._trades)

    # ── Internal reset ────────────────────────────────────────────────────────

    def _reset(self, start_ms: int) -> None:
        self._equity           = self._starting_equity
        self._equity_curve     = [(start_ms, self._equity)]
        self._trades           = []
        self._wall_states      = {}
        self._absorption_flags = {}
        self._open_position    = None
        self._bar_start_ms     = 0
        self._last_day         = -1
        self._prev_mid         = 0.0
        self._last_trade_price = 0.0
        self._abs_mid_history.clear()
        self.feature_history   = []
        # Reset FeatureComputer so OOS windows don't inherit IS distribution state.
        # CVD is not reset here — the midnight handler in _process_trade resets it at
        # day boundaries naturally; resetting here would interfere with window-spanning sessions.
        self._fc.reset()

    def _reset_for_oos(self, start_ms: int) -> None:
        """Reset evaluation state only — preserves FeatureComputer Welford distributions
        accumulated during the preceding IS pass. Called by replay_window_oos()."""
        self._equity           = self._starting_equity
        self._equity_curve     = [(start_ms, self._equity)]
        self._trades           = []
        self._wall_states      = {}
        self._absorption_flags = {}
        self._open_position    = None
        self._bar_start_ms     = 0
        self._last_day         = -1
        self._prev_mid         = 0.0
        self._last_trade_price = 0.0
        self.feature_history   = []
        # _fc and _abs_mid_history intentionally NOT reset — IS warm-up state is preserved

    # ── Event streaming ───────────────────────────────────────────────────────

    def _stream_events(
        self,
        conn: sqlite3.Connection,
        start_ms: int,
        end_ms: int,
        batch_size: int = 10_000,
    ) -> Iterator[ReplayEvent]:
        """
        Yields ReplayEvents from both tables merged in (ts_ms, kind) order.
        Uses composite (ts_event, id) keyset pagination — O(n) per table.
        Tie-break: "depth" < "trade" lexicographically, so depth events
        always precede trade events at the same millisecond.
        """

        def _batch_gen(table: str, kind: str) -> Iterator[tuple]:
            last_ts, last_id = start_ms - 1, -1
            while True:
                rows = conn.execute(
                    f"SELECT * FROM {table} "
                    f"WHERE (ts_event > ? OR (ts_event = ? AND id > ?)) "
                    f"  AND ts_event <= ? "
                    f"ORDER BY ts_event, id LIMIT ?",
                    (last_ts, last_ts, last_id, end_ms, batch_size),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    yield (row["ts_event"], kind, row)
                last_ts = rows[-1]["ts_event"]
                last_id = rows[-1]["id"]
                if len(rows) < batch_size:
                    break

        for ts, kind, row in heapq.merge(
            _batch_gen("depth_snapshots", "depth"),
            _batch_gen("agg_trades",      "trade"),
            key=lambda x: (x[0], x[1]),
        ):
            yield ReplayEvent(ts_ms=ts, kind=kind, data=dict(row))

    # ── Depth processing ──────────────────────────────────────────────────────

    def _process_depth(self, event: ReplayEvent) -> None:
        ts_ms = event.ts_ms
        try:
            bids_raw = {
                float(p): float(q)
                for p, q in json.loads(event.data["bids_json"])
            }
            asks_raw = {
                float(p): float(q)
                for p, q in json.loads(event.data["asks_json"])
            }
        except (ValueError, KeyError, json.JSONDecodeError):
            return
        if not bids_raw or not asks_raw:
            return

        best_bid = max(bids_raw)
        best_ask = min(asks_raw)
        mid      = (best_bid + best_ask) / 2.0

        bid_levels = sorted(bids_raw.items(), reverse=True)
        ask_levels = sorted(asks_raw.items())

        sigma = self._fc.wall_sigma
        new_walls: dict[float, dict] = {
            w["price"]: w
            for w in (
                identify_walls(bid_levels, "bid", sigma)
                + identify_walls(ask_levels, "ask", sigma)
            )
        }

        # Update existing walls; create new ones (mirrors live _process_snapshot)
        for price, wall_data in new_walls.items():
            if price in self._wall_states:
                ws              = self._wall_states[price]
                ws.qty_current  = wall_data["qty"]
                ws.last_seen_ts = ts_ms
            else:
                self._wall_states[price] = WallState(
                    price         = price,
                    qty_initial   = wall_data["qty"],
                    qty_current   = wall_data["qty"],
                    first_seen_ts = ts_ms,
                    last_seen_ts  = ts_ms,
                    side          = wall_data["side"],
                    sigma         = wall_data["sigma"],
                )
                self._absorption_flags[price] = False

        # Update qty_current for tracked walls absent from identified set
        # by reading their current qty from the raw book (mirrors live code).
        # Only update last_seen_ts when the price level still exists in the book —
        # if the level is gone entirely, leave last_seen_ts unchanged so the
        # stale-wall pruner can remove it after _STALE_WALL_MS.
        for price, ws in self._wall_states.items():
            if price not in new_walls:
                book = bids_raw if ws.side == "bid" else asks_raw
                if price in book:
                    ws.qty_current  = book[price]
                    ws.last_seen_ts = ts_ms
                # else: price gone from book — leave last_seen_ts so pruner fires

        # Prune stale walls
        stale = [
            p for p, ws in self._wall_states.items()
            if (ts_ms - ws.last_seen_ts) > _STALE_WALL_MS
        ]
        for p in stale:
            del self._wall_states[p]
            self._absorption_flags.pop(p, None)

        # Build wall_dicts for update_orderbook (needs price + absorption_ratio)
        wall_dicts = [
            {
                "price": price,
                "absorption_ratio": self._wall_states[price].reload_ratio
                if price in self._wall_states else 1.0,
            }
            for price in new_walls
        ]

        snap = LOBSnapshot(
            timestamp      = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            bids           = [LOBLevel(price=p, qty=q) for p, q in bid_levels],
            asks           = [LOBLevel(price=p, qty=q) for p, q in ask_levels],
            last_update_id = 0,
        )
        if self._prev_mid > 0:
            self._abs_mid_history.append(abs((mid - self._prev_mid) / self._prev_mid))
        self._fc.update_orderbook(snap, wall_dicts)
        self._prev_mid = mid

        fv = self._fc.compute(self._cvd, self._shared_state)
        if fv is not None and self._collect_features:
            self.feature_history.append((ts_ms, fv))

    # ── Trade processing ──────────────────────────────────────────────────────

    def _process_trade(self, event: ReplayEvent) -> None:
        ts_ms          = event.ts_ms
        price          = float(event.data["price"])
        qty            = float(event.data["qty"])
        is_buyer_maker = bool(event.data["is_buyer_maker"])

        self._last_trade_price = price

        trade = AggTrade(
            timestamp      = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            price          = price,
            qty            = qty,
            is_buyer_maker = is_buyer_maker,
        )
        self._cvd.update(trade)

        # Midnight CVD reset
        day = ts_ms // 86_400_000
        if day > self._last_day:
            if self._last_day >= 0:
                self._cvd.reset_daily()
            self._last_day = day

        # Candle synthesis
        self._accumulate_candle(ts_ms, price, qty)

        if self._observe_mode:
            # Measurement-only: update/retire forward-excursion windows; never trade, and
            # always run detection below (no single-position gate) so EVERY signal is seen.
            self._update_observations(price, ts_ms)
        else:
            # SL/TP check always runs first
            self._check_open_position(price, ts_ms)
            # Skip signal scan while a position is open
            if self._open_position is not None:
                return

        price_move_pct = (
            (price - self._prev_mid) / self._prev_mid
            if self._prev_mid > 0 else 0.0
        )
        price_move_threshold = rolling_abs_move_threshold(tuple(self._abs_mid_history))
        cvd_delta_1t = self._cvd.get_cvd_delta(1)
        cvd_std      = self._cvd.get_cvd_tick_std()
        cvd_spike_std = (
            abs(cvd_delta_1t) / cvd_std if cvd_std > 1e-9 else 0.0
        ) if self._cvd.is_warmed_up else 0.0

        # Absorption detection
        for ws in self._wall_states.values():
            if detect_absorption(ws, cvd_delta_1t, price_move_pct, ws.reload_ratio):
                self._absorption_flags[ws.price] = True

        # Sweep + Protection detection — now_ms=ts_ms routes freshness checks
        # through replay time instead of wall-clock time.
        for ws in list(self._wall_states.values()):
            if ws.reload_ratio >= _CONSUMED_RATIO:
                continue
            opposite = "ask" if ws.side == "bid" else "bid"
            fresh_behind = [
                w for w in self._wall_states.values()
                if w.side == opposite
            ]
            fired, info = detect_sweep_with_protection(
                ws,
                price_move_pct,
                cvd_spike_std,
                fresh_behind,
                now_ms=ts_ms,
                mid_price=self._prev_mid,
                price_move_threshold=price_move_threshold,
            )
            if fired:
                signal = MicroSignal(
                    signal_type      = "SWEEP_WITH_PROTECTION",
                    direction        = info["direction"],
                    timestamp_ms     = ts_ms,
                    consumed_wall    = info["consumed_wall"],
                    protection_wall  = info.get("protection_wall"),
                    prior_absorption = self._absorption_flags.get(ws.price, False),
                    cvd_std          = cvd_spike_std,
                    price_move_pct   = price_move_pct,
                    mid_price        = self._prev_mid,
                )
                del self._wall_states[ws.price]
                self._absorption_flags.pop(ws.price, None)
                if self._observe_mode:
                    self._register_observation(info["direction"], price, ts_ms)
                else:
                    self._simulate_trade(signal, price, ts_ms)
                break  # one signal per trade event

    # ── Candle synthesis ──────────────────────────────────────────────────────

    def _accumulate_candle(self, ts_ms: int, price: float, qty: float) -> None:
        bar_ms = (
            (ts_ms // (self._candle_minutes * 60_000))
            * (self._candle_minutes * 60_000)
        )
        if self._bar_start_ms == 0:
            self._bar_start_ms = bar_ms
            self._bar_open = self._bar_high = self._bar_low = self._bar_close = price
            self._bar_volume = qty
        elif bar_ms > self._bar_start_ms:
            self._fc.update_candle(Candle(
                open_time = datetime.fromtimestamp(
                    self._bar_start_ms / 1000, tz=timezone.utc
                ),
                open      = self._bar_open,
                high      = self._bar_high,
                low       = self._bar_low,
                close     = self._bar_close,
                volume    = self._bar_volume,
                is_closed = True,
            ))
            self._bar_start_ms = bar_ms
            self._bar_open = self._bar_high = self._bar_low = self._bar_close = price
            self._bar_volume = qty
        else:
            self._bar_high   = max(self._bar_high, price)
            self._bar_low    = min(self._bar_low, price)
            self._bar_close  = price
            self._bar_volume += qty

    # ── Position management ───────────────────────────────────────────────────

    def _directional_bias(self, fv) -> str:
        """LONG / SHORT / NEUTRAL from trend (price_vs_vwap) and flow (cvd_delta).

        With both inputs active, the bias is directional only if they agree AND are
        both non-zero; otherwise NEUTRAL. The bias_use_* toggles isolate a single input.
        """
        band   = self._variant.bias_vwap_band
        active: list[int] = []
        if self._variant.bias_use_trend:
            active.append(1 if fv.price_vs_vwap > band else -1 if fv.price_vs_vwap < -band else 0)
        if self._variant.bias_use_cvd:
            active.append(1 if fv.cvd_delta > 0 else -1 if fv.cvd_delta < 0 else 0)
        if not active or any(a == 0 for a in active) or len(set(active)) != 1:
            return "NEUTRAL"
        return "LONG" if active[0] > 0 else "SHORT"

    def _simulate_trade(
        self, signal: MicroSignal, entry_price: float, ts_ms: int
    ) -> None:
        if self._open_position is not None:
            return
        if signal.protection_wall is None:
            return
        if self._equity <= 0:
            return

        # Compute the FeatureVector ONCE, only if a feature-based gate needs it
        # (confidence or directional bias). Baseline / non-gated variants never compute
        # it, so their path — and warm-up trade behaviour — is unchanged (parity).
        need_fv = self._gate_scorer is not None or self._variant.bias_mode != "off"
        fv = self._fc.compute(self._cvd, self._shared_state) if need_fv else None

        # Optional confidence gate — mirror the live Gate 2 check. fv is None during
        # warm-up (rsi not ready), which is a skip just like the live path.
        if self._gate_scorer is not None:
            if fv is None or self._gate_scorer.score(fv, signal) < settings.MIN_CONFIDENCE:
                return

        # The protection wall must sit on the correct side of entry (sweep with
        # protection behind it). Validates the original sweep regardless of bias.
        wall_price = signal.protection_wall.price
        if signal.direction == "LONG" and wall_price >= entry_price:
            return
        if signal.direction == "SHORT" and wall_price <= entry_price:
            return

        # Directional-bias gate. "skip" vetoes counter-bias sweeps; "flip" trades the bias
        # direction instead. NEUTRAL (rangebound/conflicted tape) is always a skip.
        effective_direction = signal.direction
        if self._variant.bias_mode != "off":
            if fv is None:                       # warm-up: no bias yet (mirrors gate skip)
                return
            bias = self._directional_bias(fv)
            if bias == "NEUTRAL":
                return
            if bias != signal.direction:
                if self._variant.bias_mode == "skip":
                    return
                effective_direction = bias       # "flip"

        # Stop distance (magnitude). "wall": distance to the protection wall (legacy).
        # "vol_floor": floored at stop_floor_atr_mult × ATR so it clears the
        # microstructure noise band (the live <3 s instant-stop failure mode).
        wall_dist = abs(entry_price - wall_price)
        if self._variant.stop_mode == "vol_floor":
            sl_dist = max(wall_dist, self._variant.stop_floor_atr_mult * self._fc.current_atr)
        else:
            sl_dist = wall_dist
        if sl_dist < 1e-9:
            return
        # Place the stop on the correct side of the EFFECTIVE direction (flipped trades
        # reflect the magnitude to the opposite side).
        sl_price = entry_price - sl_dist if effective_direction == "LONG" else entry_price + sl_dist

        # Target distance. Legacy (tp_atr_mult=0): tp = atr_mult_tp × stop, so R:R is locked.
        # Decoupled (tp_atr_mult>0): tp = tp_atr_mult × ATR, independent of the stop — lets a
        # wide-stop trade that dips and recovers bank a reachable target.
        rr = self._variant.atr_mult_tp
        tp_dist = (
            self._variant.tp_atr_mult * self._fc.current_atr
            if self._variant.tp_atr_mult > 0.0 else rr * sl_dist
        )
        # Breakeven/edge filter: target must clear the larger of round-trip cost and the
        # variant's min-edge floor (tp_atr_mult=0 & min_edge_bps=0 ⇒ legacy check exactly).
        edge_floor = max(self._cost_model.round_trip_pct, self._variant.min_edge_bps / 1e4)
        if tp_dist < entry_price * edge_floor:
            return

        # Position sizing aligned with live: 1% equity risk × Kelly fraction.
        # (Confidence is not scored in backtest; Kelly fraction alone is applied.)
        risk_usd = self._equity * settings.RISK_PER_TRADE_PCT * settings.KELLY_FRACTION
        qty      = risk_usd / sl_dist
        tp_price = entry_price + tp_dist if effective_direction == "LONG" else entry_price - tp_dist

        entry_cost    = self._cost_model.entry_cost(entry_price, qty)
        self._equity -= entry_cost

        self._open_position = {
            "direction":     effective_direction,
            "entry_price":   entry_price,
            "qty":           qty,
            "sl":            sl_price,
            "tp":            tp_price,
            "entry_ts_ms":   ts_ms,
            "signal":        signal,
            "entry_cost_usd": entry_cost,
            "mae":           0.0,   # running max adverse excursion (price units, positive)
            "mfe":           0.0,   # running max favorable excursion (price units, positive)
        }
        self._equity_curve.append((ts_ms, self._equity))

    def _check_open_position(self, trade_price: float, ts_ms: int) -> None:
        if self._open_position is None:
            return
        pos       = self._open_position
        direction = pos["direction"]
        entry     = pos["entry_price"]
        sl        = pos["sl"]
        tp        = pos["tp"]

        # Track max favorable / adverse excursion (price units, positive magnitudes).
        # Observation-only — does not affect exits. MFE is print-granular (sampled at trade
        # prints, not inter-print mids), so it is mildly conservative / understated.
        if direction == "LONG":
            fav, adv = trade_price - entry, entry - trade_price
        else:
            fav, adv = entry - trade_price, trade_price - entry
        pos["mfe"] = max(pos.get("mfe", 0.0), fav)
        pos["mae"] = max(pos.get("mae", 0.0), adv)

        if direction == "LONG":
            hit_sl = trade_price <= sl
            hit_tp = trade_price >= tp
        else:
            hit_sl = trade_price >= sl
            hit_tp = trade_price <= tp

        if hit_sl:
            # Fill at the trade print that triggered the stop, not the exact SL level.
            # The SL level is the floor/ceiling; the actual fill is the tick that crossed it.
            self._close_position(trade_price, ts_ms, "SL")
        elif hit_tp:
            self._close_position(tp, ts_ms, "TP")
        elif self._variant.max_hold_ms > 0.0 and (ts_ms - pos["entry_ts_ms"]) >= self._variant.max_hold_ms:
            # Forced time exit at market on the current print.
            self._close_position(trade_price, ts_ms, "TIME")

    def _close_position(
        self, exit_price: float, ts_ms: int, reason: str
    ) -> None:
        if self._open_position is None:
            return
        pos            = self._open_position
        direction      = pos["direction"]
        qty            = pos["qty"]
        entry_price    = pos["entry_price"]
        entry_cost_usd = pos["entry_cost_usd"]

        if direction == "LONG":
            gross_pnl = (exit_price - entry_price) * qty
        else:
            gross_pnl = (entry_price - exit_price) * qty

        # SL and forced TIME exits are taker (market-out); TP/EOD rest as maker.
        exit_cost      = self._cost_model.exit_cost(
            exit_price, qty, is_market=(reason in ("SL", "TIME"))
        )
        net_pnl        = gross_pnl - exit_cost
        self._equity  += net_pnl

        mae_bps = (pos.get("mae", 0.0) / entry_price * 1e4) if entry_price > 0 else 0.0
        mfe_bps = (pos.get("mfe", 0.0) / entry_price * 1e4) if entry_price > 0 else 0.0
        self._trades.append(ReplayTrade(
            entry_ts_ms  = pos["entry_ts_ms"],
            exit_ts_ms   = ts_ms,
            direction    = direction,
            entry_price  = entry_price,
            exit_price   = exit_price,
            qty          = qty,
            pnl_usd      = gross_pnl - entry_cost_usd - exit_cost,
            exit_reason  = reason,
            signal       = pos["signal"],
            mae_bps      = mae_bps,
            mfe_bps      = mfe_bps,
        ))
        self._open_position = None
        self._equity_curve.append((ts_ms, self._equity))

    # ── Forward-excursion observer (measurement-only; observe_mode) ──────────────

    def _register_observation(self, direction: str, entry_price: float, ts_ms: int) -> None:
        self._observations.append({
            "ts":          ts_ms,
            "direction":   direction,
            "entry_price": entry_price,
            "expiry_ts":   ts_ms + self._observe_window_ms,
            "mae":         0.0,
            "mfe":         0.0,
        })
        if len(self._observations) > self._obs_max_concurrent:
            self._obs_max_concurrent = len(self._observations)

    def _update_observations(self, price: float, ts_ms: int) -> None:
        """Update every active window's MAE/MFE from this print; retire those past expiry."""
        still_open: list[dict] = []
        for o in self._observations:
            entry = o["entry_price"]
            if o["direction"] == "LONG":
                fav, adv = price - entry, entry - price
            else:
                fav, adv = entry - price, price - entry
            if fav > o["mfe"]:
                o["mfe"] = fav
            if adv > o["mae"]:
                o["mae"] = adv
            if ts_ms >= o["expiry_ts"]:
                self._retire_observation(o, price, ts_ms)
            else:
                still_open.append(o)
        self._observations = still_open

    def _retire_observation(self, o: dict, exit_price: float, ts_ms: int) -> None:
        entry = o["entry_price"]
        sign  = 1.0 if o["direction"] == "LONG" else -1.0
        self._obs_results.append(SignalObservation(
            ts_ms          = o["ts"],
            direction      = o["direction"],
            entry_price    = entry,
            mae_bps        = (o["mae"] / entry * 1e4) if entry > 0 else 0.0,
            mfe_bps        = (o["mfe"] / entry * 1e4) if entry > 0 else 0.0,
            end_return_bps = (sign * (exit_price - entry) / entry * 1e4) if entry > 0 else 0.0,
        ))

    def observe(self, start_ms: int, end_ms: int) -> list["SignalObservation"]:
        """Measurement-only pass: record every signal's forward MAE/MFE over observe_window_ms.

        Requires observe_mode=True. Call after replay_window(lo, warmup_end) so the
        FeatureComputer/CVD stats are warm. Returns the retired observations; still-open
        windows at end_ms are flushed at the last seen price. `_obs_max_concurrent` holds the
        peak number of simultaneously-open windows after the call.
        """
        if not self._observe_mode:
            raise RuntimeError("observe() requires observe_mode=True")
        self._reset_for_oos(start_ms)   # preserve warm _fc/_cvd; reset wall/eval state
        self._observations = []
        self._obs_results = []
        self._obs_max_concurrent = 0

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            for event in self._stream_events(conn, start_ms, end_ms):
                if event.kind == "depth":
                    self._process_depth(event)
                else:
                    self._process_trade(event)

        flush_price = self._last_trade_price if self._last_trade_price > 0 else self._prev_mid
        for o in self._observations:
            self._retire_observation(o, flush_price, end_ms)
        self._observations = []
        return list(self._obs_results)
