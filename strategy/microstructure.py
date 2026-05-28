"""
Microstructure Detector — Wall / Absorption / Sweep + Protection signals.

Three-signal hierarchy (master arch §5):
  1. identify_walls()               — detection only, no trade
  2. detect_absorption()            — context, arms the system
  3. detect_sweep_with_protection() — primary trade trigger
"""

import asyncio
import logging
import statistics
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from config import settings
from models import AggTrade, LOBLevel, LOBSnapshot, MicroSignal, WallState

logger = logging.getLogger(__name__)

_CONSUMED_RATIO   = 0.15     # wall qty below this fraction of initial → consumed


def price_move_floor_pct(floor_bps: float | None = None) -> float:
    """Convert a bps threshold to decimal return units."""
    bps = settings.MICRO_PRICE_MOVE_FLOOR_BPS if floor_bps is None else floor_bps
    return bps / 10_000.0


def rolling_abs_move_threshold(
    abs_returns: list[float] | tuple[float, ...],
    floor_pct: float | None = None,
    percentile: float | None = None,
    min_samples: int | None = None,
) -> float:
    """Causal hybrid threshold: rolling absolute-return percentile with a hard floor."""
    floor = price_move_floor_pct() if floor_pct is None else floor_pct
    pct = settings.MICRO_PRICE_MOVE_PERCENTILE if percentile is None else percentile
    minimum = settings.MICRO_PRICE_MOVE_MIN_SAMPLES if min_samples is None else min_samples
    if len(abs_returns) < minimum:
        return floor
    ordered = sorted(abs_returns)
    idx = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * pct)))
    return max(floor, ordered[idx])


def valid_protection_walls(
    wall_consumed: WallState,
    price_move_pct: float,
    fresh_walls_behind: list[WallState],
    mid_price: float,
    now_ms: int,
    max_distance_bps: float | None = None,
) -> tuple[str | None, list[WallState]]:
    """
    Return the implied direction and fresh protection walls that are truly behind
    the breakout. CVD is intentionally absent from this decision.
    """
    if wall_consumed.side == "ask":
        direction = "LONG"
        if price_move_pct <= 0.0:
            return None, []
        required_side = "bid"
        def is_behind(w: WallState) -> bool:
            return w.price < mid_price and w.price < wall_consumed.price
    elif wall_consumed.side == "bid":
        direction = "SHORT"
        if price_move_pct >= 0.0:
            return None, []
        required_side = "ask"
        def is_behind(w: WallState) -> bool:
            return w.price > mid_price and w.price > wall_consumed.price
    else:
        return None, []

    max_bps = settings.PROTECTION_MAX_DISTANCE_BPS if max_distance_bps is None else max_distance_bps
    valid = [
        w for w in fresh_walls_behind
        if w.side == required_side
        and (now_ms - w.first_seen_ts) <= settings.LOB_FRESH_WALL_MS
        and mid_price > 0.0
        and abs(w.price - mid_price) / mid_price * 10_000 <= max_bps
        and is_behind(w)
    ]
    valid.sort(key=lambda w: abs(w.price - mid_price))
    return direction, valid


# ── Pure detection functions ──────────────────────────────────────────────────

def identify_walls(
    levels: list[tuple[float, float]],
    side: str,
    sigma_threshold: float = 2.5,
    window: int = 5,
) -> list[dict]:
    """
    Identify resting liquidity walls in a sorted list of (price, qty) levels.

    A wall is a level where:
        (qty - median(surrounding ± window)) / std(surrounding ± window) >= sigma_threshold

    Returns list of {"price", "qty", "sigma", "side"}.
    Levels must be sorted: bids descending, asks ascending.
    """
    if len(levels) < window * 2 + 1:
        return []

    qtys = [q for _, q in levels]
    walls: list[dict] = []

    for i, (price, qty) in enumerate(levels):
        lo = max(0, i - window)
        hi = min(len(levels), i + window + 1)
        surrounding = [qtys[j] for j in range(lo, hi) if j != i]
        if len(surrounding) < 3:
            continue
        med = statistics.median(surrounding)
        try:
            std = statistics.stdev(surrounding)
        except statistics.StatisticsError:
            continue
        if std < 1e-9:
            continue
        z = (qty - med) / std
        if z >= sigma_threshold:
            walls.append({"price": price, "qty": qty, "sigma": round(z, 3), "side": side})

    return walls


def detect_absorption(
    wall: WallState,
    cvd_delta_1t: float,
    price_move_pct: float,
    reload_ratio: float,   # explicit param (not wall.reload_ratio) so tests can inject arbitrary values
) -> bool:
    """
    Return True when all four absorption conditions are met:
      1. Wall has been visible >= 500 ms (is_persistent)
      2. Directional aggression against the wall:
           bid wall → sellers aggressing (cvd_delta_1t < 0)
           ask wall → buyers aggressing  (cvd_delta_1t > 0)
      3. Price barely moved (|price_move_pct| < configured floor)
      4. Wall has reloaded to >= 70% of initial qty
    """
    if wall.side == "bid":
        directional_aggression = cvd_delta_1t < -1e-9
    else:
        directional_aggression = cvd_delta_1t > 1e-9

    return (
        wall.is_persistent
        and directional_aggression
        and abs(price_move_pct) < price_move_floor_pct()
        and reload_ratio >= 0.70
    )


def detect_sweep_with_protection(
    wall_consumed: WallState,
    price_move_pct: float,
    cvd_spike_std: float,
    fresh_walls_behind: list[WallState],
    now_ms: Optional[int] = None,
    mid_price: Optional[float] = None,
    price_move_threshold: Optional[float] = None,
    max_protection_distance_bps: Optional[float] = None,
) -> tuple[bool, dict]:
    """
    Return (True, signal_info) when all sweep+protection conditions are met:
      1. Wall consumed — qty_current < 15% of qty_initial
      2. Price moved beyond the threshold in the sweep direction
      3. Fresh Wall appeared on the far side within 3 s

    now_ms : override the current timestamp (ms epoch). When None, uses
             time.time(). Pass the replay event timestamp for backtesting so
             freshness checks use replay time, not wall-clock time.

    Returns (False, {}) if any condition fails.
    """
    _now = now_ms if now_ms is not None else int(time.time() * 1000)
    consumed = wall_consumed.reload_ratio < _CONSUMED_RATIO
    threshold = price_move_floor_pct() if price_move_threshold is None else price_move_threshold
    effective_mid = mid_price if mid_price is not None else wall_consumed.price
    direction, fresh_list = valid_protection_walls(
        wall_consumed,
        price_move_pct,
        fresh_walls_behind,
        effective_mid,
        _now,
        max_distance_bps=max_protection_distance_bps,
    )
    price_moved = (
        price_move_pct >= threshold
        if direction == "LONG"
        else price_move_pct <= -threshold
        if direction == "SHORT"
        else False
    )

    if consumed and price_moved and fresh_list and direction:
        protection = fresh_list[0]
        return True, {
            "direction": direction,
            "consumed_wall": wall_consumed,
            "protection_wall": protection,
            "price_move_pct": price_move_pct,
            "cvd_spike_std": cvd_spike_std,
            "price_move_threshold": threshold,
        }

    return False, {}


# ── Stateful detector ─────────────────────────────────────────────────────────

class MicrostructureDetector:
    """
    Consumes depth100 snapshots (reconstructed from incremental diff stream)
    and aggTrades; emits MicroSignal on confirmed Sweep + Protection events.

    Wall lifecycle:
      identify_walls() → WallState created/updated each tick
      detect_absorption() → _absorption_armed = True
      detect_sweep_with_protection() → MicroSignal emitted
    """

    def __init__(
        self,
        depth_queue: asyncio.Queue,
        trade_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        cvd_calculator,
        sigma_threshold: float = 2.5,
        window: int = 5,
        feature_computer=None,
    ) -> None:
        self._depth_queue  = depth_queue
        self._trade_queue  = trade_queue
        self._signal_queue = signal_queue
        self._cvd          = cvd_calculator
        self._sigma        = sigma_threshold
        self._window       = window
        self._feature_computer = feature_computer

        self._wall_states: dict[float, WallState] = {}
        self._absorption_armed: dict[float, bool] = {}  # keyed by wall price
        self._prev_mid: float = 0.0
        self._abs_mid_history: deque[float] = deque(maxlen=settings.MICRO_PRICE_MOVE_WINDOW)
        self._absorption_batch: dict[float, WallState] = {}
        self._absorption_last_log_ms: int = 0

    async def run(self) -> None:
        asyncio.create_task(self._collect_trades())
        await self._depth_loop()

    # ── Internal loops ────────────────────────────────────────────────────────

    async def _depth_loop(self) -> None:
        while True:
            msg = await self._depth_queue.get()
            await self._process_snapshot(msg)

    async def _collect_trades(self) -> None:
        while True:
            item = await self._trade_queue.get()
            if not isinstance(item, AggTrade):
                continue
            self._cvd.update(item)

    # ── Snapshot processing ───────────────────────────────────────────────────

    async def _process_snapshot(self, msg: dict) -> None:
        try:
            bids_raw = {float(p): float(q) for p, q in msg.get("bids", [])}
            asks_raw = {float(p): float(q) for p, q in msg.get("asks", [])}
        except (ValueError, TypeError) as exc:
            logger.error("[MS] Malformed depth snapshot — skipping: %s", exc)
            return
        if not bids_raw or not asks_raw:
            return

        best_bid = max(bids_raw)
        best_ask = min(asks_raw)
        mid      = (best_bid + best_ask) / 2.0
        now_ms   = int(time.time() * 1000)

        bid_levels = sorted(bids_raw.items(), reverse=True)
        ask_levels = sorted(asks_raw.items())
        new_walls = {
            w["price"]: w
            for w in identify_walls(bid_levels, "bid", self._sigma, self._window)
            + identify_walls(ask_levels, "ask", self._sigma, self._window)
        }

        # Update existing walls; create new ones
        for price, wall_data in new_walls.items():
            if price in self._wall_states:
                ws = self._wall_states[price]
                ws.qty_current  = wall_data["qty"]
                ws.last_seen_ts = now_ms
            else:
                self._wall_states[price] = WallState(
                    price         = price,
                    qty_initial   = wall_data["qty"],
                    qty_current   = wall_data["qty"],
                    first_seen_ts = now_ms,
                    last_seen_ts  = now_ms,
                    side          = wall_data["side"],
                    sigma         = wall_data["sigma"],
                )

        # Update qty_current for walls that disappeared from the identified set
        for price, ws in self._wall_states.items():
            if price not in new_walls:
                book = bids_raw if ws.side == "bid" else asks_raw
                ws.qty_current  = book.get(price, 0.0)
                ws.last_seen_ts = now_ms

        if self._feature_computer is not None:
            snap = LOBSnapshot(
                timestamp      = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc),
                bids           = [LOBLevel(price=p, qty=q) for p, q in bid_levels],
                asks           = [LOBLevel(price=p, qty=q) for p, q in ask_levels],
                last_update_id = 0,
            )
            wall_dicts = [
                {"price": ws.price, "absorption_ratio": ws.reload_ratio}
                for ws in self._wall_states.values()
            ]
            self._feature_computer.update_orderbook(snap, wall_dicts)

        # Absorption check — 3-tick delta reduces single-trade noise while staying
        # responsive enough to detect multi-trade aggression against a wall.
        price_move_pct   = (mid - self._prev_mid) / self._prev_mid if self._prev_mid > 0 else 0.0
        price_move_threshold = rolling_abs_move_threshold(tuple(self._abs_mid_history))
        cvd_delta_3t     = self._cvd.get_cvd_delta(3)
        for ws in self._wall_states.values():
            if detect_absorption(ws, cvd_delta_3t, price_move_pct, ws.reload_ratio):
                self._absorption_armed[ws.price] = True
                self._absorption_batch[ws.price] = ws

        if self._absorption_batch and now_ms - self._absorption_last_log_ms >= 1_000:
            batch      = list(self._absorption_batch.values())
            bid_walls  = [w for w in batch if w.side == "bid"]
            ask_walls  = [w for w in batch if w.side == "ask"]
            max_reload = max(batch, key=lambda w: w.reload_ratio)
            prices     = [w.price for w in batch]
            logger.info(
                "[MS] Absorption: %d walls armed (%d bid / %d ask) | "
                "max reload=%.0f%% @%.2f | range %.2f–%.2f",
                len(batch), len(bid_walls), len(ask_walls),
                max_reload.reload_ratio * 100, max_reload.price,
                min(prices), max(prices),
            )
            self._absorption_batch = {}
            self._absorption_last_log_ms = now_ms

        # Sweep + Protection check — 1-tick delta captures the momentary spike
        cvd_delta_1t  = self._cvd.get_cvd_delta(1)
        cvd_std       = self._cvd.get_cvd_tick_std()
        cvd_spike_std = (abs(cvd_delta_1t) / cvd_std if cvd_std > 1e-9 else 0.0) if self._cvd.is_warmed_up else 0.0

        for price, ws in list(self._wall_states.items()):
            if ws.reload_ratio >= _CONSUMED_RATIO:
                continue
            opposite = "ask" if ws.side == "bid" else "bid"
            fresh_behind = [
                w for w in self._wall_states.values()
                if w.side == opposite and (now_ms - w.first_seen_ts) <= settings.LOB_FRESH_WALL_MS
            ]
            fired, info = detect_sweep_with_protection(
                ws,
                price_move_pct,
                cvd_spike_std,
                fresh_behind,
                now_ms=now_ms,
                mid_price=mid,
                price_move_threshold=price_move_threshold,
            )
            if fired:
                pw = info.get("protection_wall")
                signal = MicroSignal(
                    signal_type      = "SWEEP_WITH_PROTECTION",
                    direction        = info["direction"],
                    timestamp_ms     = now_ms,
                    consumed_wall    = ws,
                    protection_wall  = pw,
                    prior_absorption = self._absorption_armed.get(price, False),
                    cvd_std          = cvd_spike_std,
                    price_move_pct   = price_move_pct,
                    mid_price        = mid,
                )
                await self._signal_queue.put(signal)
                logger.info(
                    "[MS] Sweep+Protection %s mid=%.2f | consumed=%s@%.2f(%.3f) protection=%s@%.2f(%.3f) cvd=%.1fstd move=%+.3f%%",
                    info["direction"], mid,
                    ws.side, ws.price, ws.qty_current,
                    pw.side if pw else "?", pw.price if pw else 0.0, pw.qty_current if pw else 0.0,
                    cvd_spike_std, price_move_pct * 100,
                )
                self._absorption_armed.pop(price, None)
                del self._wall_states[price]
                break  # one signal per depth tick — prevents cascade signals from simultaneous sweeps

        # Prune stale walls and their absorption flags
        stale = [p for p, w in self._wall_states.items() if (now_ms - w.last_seen_ts) >= settings.LOB_STALE_WALL_MS]
        for p in stale:
            del self._wall_states[p]
            self._absorption_armed.pop(p, None)
        if self._prev_mid > 0:
            self._abs_mid_history.append(abs(price_move_pct))
        self._prev_mid = mid
