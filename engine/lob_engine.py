import logging
from datetime import datetime, timezone

from models import LOBLevel, LOBSnapshot

logger = logging.getLogger(__name__)


class LocalOrderBook:
    """
    Maintains a local copy of the Binance order book via the diff-depth stream.

    Synchronisation flow (per Binance docs):
      1. Caller connects WS and starts buffering depth events.
      2. Caller fetches REST snapshot and calls set_snapshot().
      3. Buffered events with u <= lastUpdateId are discarded; the rest are
         applied in order via apply_diff().
      4. From this point apply_diff() validates U == prev_u + 1 on every
         subsequent event and returns False on any gap so the caller can
         reinitialise.
    """

    def __init__(self) -> None:
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_update_id: int = 0
        self._ready: bool = False

    # ── Initialisation ────────────────────────────────────────────────────

    def set_snapshot(self, data: dict) -> None:
        """Apply a REST depth snapshot. data is the raw JSON response dict."""
        self._bids = {float(p): float(q) for p, q in data["bids"]}
        self._asks = {float(p): float(q) for p, q in data["asks"]}
        self._last_update_id = int(data["lastUpdateId"])
        self._ready = True
        logger.info(f"[LOB] Snapshot applied. lastUpdateId={self._last_update_id}")

    # ── Live updates ──────────────────────────────────────────────────────

    def apply_diff(self, event: dict) -> bool:
        """
        Apply one diff-depth WebSocket event.

        Returns True on success, False if a sequence gap was detected
        (caller should reinitialise the book).

        Events where u <= lastUpdateId are silently skipped (they arrived
        before or during snapshot fetch and are already stale).
        """
        U: int = int(event["U"])
        u: int = int(event["u"])

        # Skip events that predate our snapshot
        if u <= self._last_update_id:
            return True

        # Validate monotonic sequence after the first applied event
        if self._last_update_id > 0 and U > self._last_update_id + 1:
            logger.warning(
                f"[LOB] Sequence gap — expected U<={self._last_update_id + 1}, got U={U}. "
                "Book is stale; reinitialising."
            )
            self._ready = False
            return False

        self._apply_event(event)
        return True

    def _apply_event(self, event: dict) -> None:
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

    # ── Read ──────────────────────────────────────────────────────────────

    def get_snapshot(self, depth: int = 20) -> LOBSnapshot | None:
        if not self._ready:
            return None

        sorted_bids = sorted(self._bids.items(), reverse=True)[:depth]
        sorted_asks = sorted(self._asks.items())[:depth]

        return LOBSnapshot(
            timestamp=datetime.now(timezone.utc),
            bids=[LOBLevel(price=p, qty=q) for p, q in sorted_bids],
            asks=[LOBLevel(price=p, qty=q) for p, q in sorted_asks],
            last_update_id=self._last_update_id,
        )

    @property
    def is_ready(self) -> bool:
        return self._ready

    def reset(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = 0
        self._ready = False
