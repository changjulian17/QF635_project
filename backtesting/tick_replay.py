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

_CONSUMED_RATIO = 0.15      # pre-filter: skip walls still intact (mirrors live code)
_STALE_WALL_MS  = 30_000    # prune wall states absent > 30 s


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
    exit_reason:  str    # "SL" | "TP" | "EOD"
    signal:       MicroSignal


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
    ) -> None:
        self._db_path         = db_path
        self._fc              = FeatureComputer(FeatureParams(**params.get("feature", {})))
        self._cvd             = CVDCalculator()
        self._shared_state    = SharedState(lob_status="SYNCED")
        self._starting_equity = starting_equity
        self._candle_minutes  = candle_minutes
        self._collect_features = collect_features

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
                )
                del self._wall_states[ws.price]
                self._absorption_flags.pop(ws.price, None)
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

    def _simulate_trade(
        self, signal: MicroSignal, entry_price: float, ts_ms: int
    ) -> None:
        if self._open_position is not None:
            return
        if signal.protection_wall is None:
            return
        if self._equity <= 0:
            return
        # SL at protection wall price — mirrors live order_manager which uses
        # protection_wall.price (not the consumed wall that was swept).
        sl_price = signal.protection_wall.price
        # Guard: SL must be on the correct side of entry price
        if signal.direction == "LONG" and sl_price >= entry_price:
            return
        if signal.direction == "SHORT" and sl_price <= entry_price:
            return
        sl_dist = abs(entry_price - sl_price)
        if sl_dist < 1e-9:
            return
        # Breakeven filter: TP gross = ATR_MULTIPLIER_TP×sl_dist×qty; need that > round-trip cost.
        rr = settings.ATR_MULTIPLIER_TP
        if rr * sl_dist < entry_price * self._cost_model.round_trip_pct:
            return

        # Position sizing aligned with live: 1% equity risk × Kelly fraction.
        # (Confidence is not scored in backtest; Kelly fraction alone is applied.)
        risk_usd = self._equity * settings.RISK_PER_TRADE_PCT * settings.KELLY_FRACTION
        qty      = risk_usd / sl_dist
        # TP at ATR_MULTIPLIER_TP × sl_dist — mirrors live OCO bracket formula.
        if signal.direction == "LONG":
            tp_price = entry_price + rr * sl_dist
        else:
            tp_price = entry_price - rr * sl_dist

        entry_cost    = self._cost_model.entry_cost(entry_price, qty)
        self._equity -= entry_cost

        self._open_position = {
            "direction":     signal.direction,
            "entry_price":   entry_price,
            "qty":           qty,
            "sl":            sl_price,
            "tp":            tp_price,
            "entry_ts_ms":   ts_ms,
            "signal":        signal,
            "entry_cost_usd": entry_cost,
        }
        self._equity_curve.append((ts_ms, self._equity))

    def _check_open_position(self, trade_price: float, ts_ms: int) -> None:
        if self._open_position is None:
            return
        pos       = self._open_position
        direction = pos["direction"]
        sl        = pos["sl"]
        tp        = pos["tp"]

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

        exit_cost      = self._cost_model.exit_cost(
            exit_price, qty, is_market=(reason == "SL")
        )
        net_pnl        = gross_pnl - exit_cost
        self._equity  += net_pnl

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
        ))
        self._open_position = None
        self._equity_curve.append((ts_ms, self._equity))
