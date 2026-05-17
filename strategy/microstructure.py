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
from typing import Optional

from models import AggTrade, MicroSignal, WallState

logger = logging.getLogger(__name__)

_CONSUMED_RATIO   = 0.15     # wall qty below this fraction of initial → consumed
_PRICE_MOVE_THRESH = 0.0003  # 0.03% price move required for sweep
_CVD_SPIKE_STD    = 1.5      # CVD must spike > 1.5σ
_FRESH_WALL_MS    = 3_000    # protection wall must appear within 3 s
_WALL_PRICE_TOL   = 1.0      # USD tolerance for matching trade price to wall
_STALE_WALL_MS    = 30_000   # prune wall states not seen for 30 s


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
    reload_ratio: float,
) -> bool:
    """
    Return True when all four absorption conditions are met:
      1. Wall has been visible >= 500 ms (is_persistent)
      2. Some net aggression against the wall (|cvd_delta_1t| > 0)
      3. Price barely moved (|price_move_pct| < 0.03%)
      4. Wall has reloaded to >= 70% of initial qty
    """
    return (
        wall.is_persistent
        and abs(cvd_delta_1t) > 0
        and abs(price_move_pct) < _PRICE_MOVE_THRESH
        and reload_ratio >= 0.70
    )


def detect_sweep_with_protection(
    wall_consumed: WallState,
    price_move_pct: float,
    cvd_spike_std: float,
    fresh_walls_behind: list[WallState],
) -> tuple[bool, dict]:
    """
    Return (True, signal_info) when all four sweep+protection conditions are met:
      1. Wall consumed — qty_current < 15% of qty_initial
      2. Price moved > 0.03% in the sweep direction
      3. CVD spike > 1.5σ
      4. Fresh Wall appeared on the far side within 3 s

    Returns (False, {}) if any condition fails.
    """
    consumed    = wall_consumed.reload_ratio < _CONSUMED_RATIO
    price_moved = abs(price_move_pct) > _PRICE_MOVE_THRESH
    cvd_spike   = cvd_spike_std > _CVD_SPIKE_STD

    now_ms = int(time.time() * 1000)
    fresh_list = [
        w for w in fresh_walls_behind
        if (now_ms - w.first_seen_ts) <= _FRESH_WALL_MS
    ]
    has_protection = len(fresh_list) > 0

    if consumed and price_moved and cvd_spike and has_protection:
        newest = min(fresh_list, key=lambda w: now_ms - w.first_seen_ts)
        direction = "LONG" if wall_consumed.side == "ask" else "SHORT"
        return True, {
            "direction": direction,
            "consumed_wall": wall_consumed,
            "protection_wall": newest,
            "price_move_pct": price_move_pct,
            "cvd_spike_std": cvd_spike_std,
        }

    return False, {}


# ── Stateful detector ─────────────────────────────────────────────────────────

class MicrostructureDetector:
    """
    Consumes depth20 snapshots and aggTrades; emits MicroSignal on
    confirmed Sweep + Protection events.

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
    ) -> None:
        self._depth_queue  = depth_queue
        self._trade_queue  = trade_queue
        self._signal_queue = signal_queue
        self._cvd          = cvd_calculator
        self._sigma        = sigma_threshold
        self._window       = window

        self._wall_states: dict[float, WallState] = {}
        self._absorption_armed: bool = False
        self._prev_mid: float = 0.0

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
            self._check_wall_aggression(item)

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

        # Absorption check
        price_move_pct  = (mid - self._prev_mid) / self._prev_mid if self._prev_mid > 0 else 0.0
        cvd_delta_1t    = self._cvd.get_cvd_delta(1)
        for ws in self._wall_states.values():
            if detect_absorption(ws, cvd_delta_1t, price_move_pct, ws.reload_ratio):
                self._absorption_armed = True
                logger.debug("[MS] Absorption armed at price=%.2f", ws.price)
                break

        # Sweep + Protection check
        cvd_std = self._cvd.get_cvd_tick_std()
        _cvd_n_ready = self._cvd._delta_stats.n >= 10
        cvd_spike_std = (abs(cvd_delta_1t) / cvd_std if cvd_std > 1e-9 else 0.0) if _cvd_n_ready else 0.0

        for price, ws in list(self._wall_states.items()):
            if ws.reload_ratio >= _CONSUMED_RATIO:
                continue
            opposite = "ask" if ws.side == "bid" else "bid"
            fresh_behind = [
                w for w in self._wall_states.values()
                if w.side == opposite and (now_ms - w.first_seen_ts) <= _FRESH_WALL_MS
            ]
            fired, info = detect_sweep_with_protection(
                ws, price_move_pct, cvd_spike_std, fresh_behind
            )
            if fired:
                signal = MicroSignal(
                    signal_type      = "SWEEP_WITH_PROTECTION",
                    direction        = info["direction"],
                    timestamp_ms     = now_ms,
                    consumed_wall    = ws,
                    protection_wall  = info.get("protection_wall"),
                    prior_absorption = self._absorption_armed,
                    cvd_std          = cvd_spike_std,
                    price_move_pct   = price_move_pct,
                )
                await self._signal_queue.put(signal)
                logger.info(
                    "[MS] Sweep+Protection %s @ %.2f → signal emitted",
                    info["direction"], price,
                )
                self._absorption_armed = False
                del self._wall_states[price]
                break

        # Prune stale walls
        self._wall_states = {
            p: w for p, w in self._wall_states.items()
            if (now_ms - w.last_seen_ts) < _STALE_WALL_MS
        }
        self._prev_mid = mid

    def _check_wall_aggression(self, trade: AggTrade) -> None:
        for ws in self._wall_states.values():
            if abs(trade.price - ws.price) <= _WALL_PRICE_TOL:
                ws.aggression_hits  += 1
                ws.total_aggressed  += trade.qty
