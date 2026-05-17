import asyncio
import logging
import statistics
from datetime import datetime, timezone

from models import LOBLevel, LOBSnapshot, LOBStateMachineState, SharedState

logger = logging.getLogger(__name__)


class LocalOrderBook:
    """
    Maintains a local copy of the Binance order book from the depth20@100ms
    full-snapshot stream (not diff-depth).

    State machine:
      UNINITIALISED → SYNCED       : first valid snapshot applied
      SYNCED        → SYNCED       : each subsequent snapshot with lastUpdateId > prev
      SYNCED        → GAP_DETECTED : lastUpdateId regressed (stale/out-of-order snapshot)
      GAP_DETECTED  → SYNCED       : next valid snapshot with lastUpdateId > last_valid
      any           → DISCONNECTED : set externally by ws_consumer on reconnect

    Gap severity tiering (applies to updateId delta when regression detected):
      < 500       → CONTINUE       (minor drift, keep running)
      500–4999    → HALT_ENTRIES   (pause new entries)
      ≥ 5000      → CLOSE_REVIEW   (evaluate open positions)

    Legacy diff-depth interface (set_snapshot / apply_diff) is preserved for
    backward compatibility with existing tests.
    """

    GAP_RESPONSE: dict[int, str] = {
        500:   "CONTINUE",
        5_000: "HALT_ENTRIES",
        30_000: "CLOSE_REVIEW",
    }

    def __init__(self, shared_state: SharedState | None = None) -> None:
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_update_id: int = 0
        self._ready: bool = False
        self._state = LOBStateMachineState.UNINITIALISED
        self._shared_state = shared_state
        self._gap_severity: str = "CONTINUE"
        self._snapshot_count: int = 0
        self._lock = asyncio.Lock()
        self._last_event_time: datetime | None = None

    # ── Snapshot stream (depth20@100ms) ───────────────────────────────────────

    async def apply_snapshot(self, msg: dict) -> bool:
        """
        Apply a depth20@100ms full-snapshot message.

        Returns True if the snapshot was applied, False if it was rejected
        (stale lastUpdateId). On rejection the book is NOT cleared — it
        retains the last valid state.
        """
        async with self._lock:
            last_update_id = int(msg.get("lastUpdateId", 0))
            bids = msg.get("bids", [])
            asks = msg.get("asks", [])

            if self._snapshot_count > 0 and last_update_id <= self._last_update_id:
                gap = self._last_update_id - last_update_id
                self._gap_severity = self._classify_gap(gap)
                if self._state != LOBStateMachineState.GAP_DETECTED:
                    self._transition(
                        LOBStateMachineState.GAP_DETECTED,
                        f"lastUpdateId regressed by {gap} (severity={self._gap_severity})",
                    )
                return False

            # Prefer exchange transaction time (T), fall back to event time (E),
            # then wall clock — so LOBSnapshot.timestamp reflects market time not receive time.
            raw_ts = msg.get("T") or msg.get("E")
            self._last_event_time = (
                datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
                if raw_ts is not None
                else datetime.now(timezone.utc)
            )

            self._bids = {float(p): float(q) for p, q in bids}
            self._asks = {float(p): float(q) for p, q in asks}
            self._last_update_id = last_update_id
            self._ready = True
            self._snapshot_count += 1

            if self._state != LOBStateMachineState.SYNCED:
                self._transition(
                    LOBStateMachineState.SYNCED,
                    f"snapshot applied lastUpdateId={last_update_id}",
                )
            return True

    # ── State machine ─────────────────────────────────────────────────────────

    def _transition(self, new_state: LOBStateMachineState, reason: str = "") -> None:
        old = self._state
        self._state = new_state
        suffix = f": {reason}" if reason else ""
        logger.info("[LOB] %s → %s%s", old.value, new_state.value, suffix)
        if self._shared_state is not None:
            self._shared_state.lob_status = new_state.value

    def _classify_gap(self, gap: int) -> str:
        for threshold in sorted(self.GAP_RESPONSE):
            if gap < threshold:
                return self.GAP_RESPONSE[threshold]
        return "CLOSE_REVIEW"

    @property
    def state(self) -> LOBStateMachineState:
        return self._state

    @property
    def gap_severity(self) -> str:
        return self._gap_severity

    # ── Wall identification ───────────────────────────────────────────────────

    async def get_current_walls(self, sigma: float = 2.5, window: int = 5) -> list[dict]:
        """
        Scan visible book levels and return those that qualify as resting
        liquidity walls: levels where qty >= median(surrounding±window) + sigma × std.

        Returns list of {"price": float, "qty": float, "sigma": float, "side": str}.
        """
        async with self._lock:
            walls: list[dict] = []
            walls.extend(
                self._walls_in_side(sorted(self._bids.items(), reverse=True), "bid", sigma, window)
            )
            walls.extend(
                self._walls_in_side(sorted(self._asks.items()), "ask", sigma, window)
            )
            return walls

    def _walls_in_side(
        self,
        levels: list[tuple[float, float]],
        side: str,
        sigma: float,
        window: int,
    ) -> list[dict]:
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
            if z >= sigma:
                walls.append({"price": price, "qty": qty, "sigma": round(z, 3), "side": side})
        return walls

    # ── Legacy diff-depth interface (backward compat) ─────────────────────────

    def set_snapshot(self, data: dict) -> None:
        """Apply a REST depth snapshot. Kept for legacy test compatibility."""
        self._bids = {float(p): float(q) for p, q in data["bids"]}
        self._asks = {float(p): float(q) for p, q in data["asks"]}
        self._last_update_id = int(data["lastUpdateId"])
        self._ready = True
        self._state = LOBStateMachineState.SYNCED
        logger.info("[LOB] Snapshot applied. lastUpdateId=%d", self._last_update_id)

    def apply_diff(self, event: dict) -> bool:
        """Apply one diff-depth event. Kept for legacy test compatibility."""
        U: int = int(event["U"])
        u: int = int(event["u"])

        if u <= self._last_update_id:
            return True

        if self._last_update_id > 0 and U > self._last_update_id + 1:
            logger.warning(
                "[LOB] Sequence gap — expected U<=%d, got U=%d. Book is stale; reinitialising.",
                self._last_update_id + 1,
                U,
            )
            self._ready = False
            self._transition(
                LOBStateMachineState.GAP_DETECTED,
                f"expected U<={self._last_update_id + 1}, got U={U}",
            )
            return False

        self._apply_diff_event(event)
        return True

    def _apply_diff_event(self, event: dict) -> None:
        for price_str, qty_str in event.get("b", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0.0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty

        for price_str, qty_str in event.get("a", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0.0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty

        self._last_update_id = int(event["u"])

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_snapshot(self, depth: int = 20) -> LOBSnapshot | None:
        async with self._lock:
            if not self._ready:
                return None

            sorted_bids = sorted(self._bids.items(), reverse=True)[:depth]
            sorted_asks = sorted(self._asks.items())[:depth]

            return LOBSnapshot(
                timestamp=self._last_event_time or datetime.now(timezone.utc),
                bids=[LOBLevel(price=p, qty=q) for p, q in sorted_bids],
                asks=[LOBLevel(price=p, qty=q) for p, q in sorted_asks],
                last_update_id=self._last_update_id,
            )

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def lob_status(self) -> str:
        return self._state.value

    async def reset(self) -> None:
        async with self._lock:
            self._bids.clear()
            self._asks.clear()
            self._last_update_id = 0
            self._ready = False
            self._snapshot_count = 0
            self._gap_severity = "CONTINUE"
            self._last_event_time = None
            self._transition(LOBStateMachineState.UNINITIALISED, "reset")
