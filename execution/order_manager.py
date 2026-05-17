"""
Execution layer — IOC aggressive limit orders (Rule 10, master arch §9).

Entry orders are IOC limit orders priced to cross the spread:
  LONG  → limit = best_ask + (best_ask - best_bid) × 0.5
  SHORT → limit = best_bid - (best_ask - best_bid) × 0.5

If the order expires unfilled it is NOT retried — the signal is stale.
On fill: slippage is recorded to GlobalKillswitch (KS-3) and a FillDetail
is put on fill_queue for portfolio updates and telemetry outcome recording.

OCO lifecycle and concurrency guarantees
────────────────────────────────────────
All mutable open-position fields are guarded by _position_lock.

_placing_oco flag (S1 race fix):
  Set True (under lock) before the OCO REST call; False after.
  If Gate 6 fires handle_protection_wall_removed() while the flag is True,
  it sets _cancel_oco_on_placement=True and returns.
  _place_oco reads that flag atomically on completion, then cancels the
  freshly-placed OCO and calls _emergency_close — no double-close possible.

S2 state-reset guarantee:
  After any path where _emergency_close() succeeds (OCO_FAILED in _place_oco,
  or WALL_REMOVED_DURING_OCO), _reset_open_position() is called immediately
  and position_closed_event is set. Gate 6 cannot fire a second close attempt
  on an already-closed position.

Order construction uses typed order classes (execution/orders.py).
Adding a new order type requires only a new subclass — not changes here.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from binance import AsyncClient

from config import settings
from execution.orders import IOCLimitOrder, OCOOrder
from models import FillDetail, MicroOrderRequest
from risk.killswitch import GlobalKillswitch

logger = logging.getLogger(__name__)

# Used only when DRY_RUN=True and no book_fn is injected.
# Inject book_fn (e.g. from LOB engine) for realistic DRY_RUN sizing.
_DRY_RUN_BEST_BID = 95_000.0
_DRY_RUN_BEST_ASK = 95_010.0

_MAX_CONNECT_ATTEMPTS = 3
_RECONNECT_DELAY_S    = 5.0


def _weighted_avg_fill(resp: dict) -> float:
    """Weighted-average fill price from a Binance order response."""
    fills = resp.get("fills", [])
    if not fills:
        return float(resp.get("price", 0.0))
    total_qty = sum(float(f["qty"]) for f in fills)
    if total_qty == 0.0:
        logger.warning("[Exec] _weighted_avg_fill: all fill quantities are zero — returning 0.0")
        return 0.0
    return sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty


class OrderManager:
    """
    Consumes MicroOrderRequests from signal_queue, executes them as IOC
    aggressive limit orders, and emits FillDetail records to fill_queue.
    """

    def __init__(
        self,
        signal_queue: asyncio.Queue,
        fill_queue: asyncio.Queue,
        killswitch: GlobalKillswitch,
        equity_fn: Callable[[], float],
        book_fn: Callable[[], tuple[float, float]] | None = None,
        ks_fire_cb: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """
        book_fn: optional callable returning (best_bid, best_ask) without a REST
        round-trip — wire to the LOB engine's current top-of-book for lowest latency
        and accurate DRY_RUN position sizing. Falls back to REST when None.
        ks_fire_cb: called with a reason string when KS-3 slippage fires.
        """
        self._signal_q   = signal_queue
        self.fill_queue  = fill_queue
        self._killswitch = killswitch
        self._equity_fn  = equity_fn
        self._book_fn    = book_fn
        self._ks_fire_cb = ks_fire_cb
        self._client: AsyncClient | None = None

        # Graceful-shutdown gate: set False before draining the queue.
        self.accepting_new_signals: bool = True

        # Single lock guards all _open_position_* and placement-flag fields.
        self._position_lock = asyncio.Lock()

        # ── Open-position state (written atomically on fill; cleared after exit) ──
        self._open_position_side:         str | None          = None
        self._open_position_qty:          float               = 0.0
        self._open_oco_list_id:           int | None          = None
        self._open_position_closed_event: asyncio.Event | None = None

        # ── OCO placement race flags (S1 fix) ────────────────────────────────────
        # _placing_oco:            True while create_oco_order REST call is in-flight.
        # _cancel_oco_on_placement: Gate 6 sets this while _placing_oco is True;
        #                           _place_oco reads it on completion and cancels.
        self._placing_oco:            bool = False
        self._cancel_oco_on_placement: bool = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN mode — skipping Binance connection.")
            await self._order_loop()
            return
        if not await self._connect():
            logger.error("[Exec] All connect attempts failed — entering drain mode.")
            await self._drain_loop()
            return
        await self._order_loop()

    async def _connect(self, delay_s: float = _RECONNECT_DELAY_S) -> bool:
        """
        Establish (or re-establish) the Binance AsyncClient.
        delay_s controls the sleep between retries — pass a smaller value
        when called from the hot path (_resolve_book) to avoid long stalls.
        """
        for attempt in range(1, _MAX_CONNECT_ATTEMPTS + 1):
            try:
                if self._client:
                    try:
                        await self._client.close_connection()
                    except Exception:
                        pass
                self._client = await AsyncClient.create(
                    api_key    = settings.BINANCE_API_KEY,
                    api_secret = settings.BINANCE_API_SECRET,
                    testnet    = settings.BINANCE_TESTNET,
                )
                logger.info("[Exec] Binance AsyncClient connected (attempt %d).", attempt)
                return True
            except Exception as exc:
                logger.error(
                    "[Exec] Connect attempt %d/%d failed: %s",
                    attempt, _MAX_CONNECT_ATTEMPTS, exc,
                )
                if attempt < _MAX_CONNECT_ATTEMPTS:
                    await asyncio.sleep(delay_s)
        return False

    async def _drain_loop(self) -> None:
        """Silently discard queued orders when Binance is unreachable."""
        while True:
            await self._signal_q.get()

    async def _order_loop(self) -> None:
        while True:
            req: MicroOrderRequest = await self._signal_q.get()
            if not self.accepting_new_signals:
                logger.info(
                    "[Exec] Shutdown — discarding signal %s", req.signal_id[:8]
                )
                continue
            if self._killswitch.is_active:
                logger.warning(
                    "[Exec] Killswitch active — discarding signal %s", req.signal_id[:8]
                )
                continue
            await self._submit(req)

    # ── Core submission ───────────────────────────────────────────────────────

    async def _submit(self, req: MicroOrderRequest) -> None:
        book = await self._resolve_book()
        if book is None:
            logger.warning(
                "[Exec] Could not resolve order book — skipping %s", req.signal_id[:8]
            )
            return
        best_bid, best_ask = book

        resp = await self._submit_aggressive_limit(req, req.side, best_bid, best_ask)
        if resp is None:
            return

        fill_price = _weighted_avg_fill(resp)
        fill_qty   = float(resp.get("executedQty", 0))
        await self._place_oco(req, fill_price, fill_qty, req.side)

    async def _resolve_book(self) -> tuple[float, float] | None:
        """
        Return (best_bid, best_ask) from the fastest available source.
        Priority: injected book_fn → REST (with one reconnect retry on failure).
        In DRY_RUN without book_fn, falls back to module-level synthetic prices.
        """
        if self._book_fn is not None:
            return self._book_fn()

        if settings.DRY_RUN:
            logger.debug("[Exec] DRY RUN — no book_fn, using synthetic prices")
            return _DRY_RUN_BEST_BID, _DRY_RUN_BEST_ASK

        for attempt in range(2):
            try:
                book = await self._client.get_order_book(
                    symbol=settings.SYMBOL, limit=5
                )
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                if not bids or not asks:
                    logger.warning("[Exec] Empty order book received")
                    return None
                return float(bids[0][0]), float(asks[0][0])
            except Exception as exc:
                logger.error(
                    "[Exec] Order book fetch failed (attempt %d): %s", attempt + 1, exc
                )
                if attempt == 0:
                    logger.info("[Exec] Attempting reconnect before retry ...")
                    # Short delay in hot path to avoid stalling _order_loop for 5s
                    if not await self._connect(delay_s=0.5):
                        return None
        return None

    async def _submit_aggressive_limit(
        self,
        req: MicroOrderRequest,
        side: str,
        best_bid: float,
        best_ask: float,
    ) -> dict | None:
        """
        Place an IOC aggressive limit order via IOCLimitOrder.
        Returns the exchange response (or synthetic DRY_RUN dict) on fill;
        returns None if stale or expired unfilled.

        Side-effects on confirmed fill (all under _position_lock):
          fill_event set, FillDetail put to fill_queue (non-blocking),
          slippage recorded to killswitch, open-position state cached.
        """
        # Re-check staleness; signal may have aged during book fetch RTT.
        age_ms = int(time.time() * 1000) - req.micro_signal.timestamp_ms
        if age_ms > req.ioc_timeout_ms:
            logger.warning(
                "[Exec] Signal %s stale at submission (%dms > %dms) — skipped",
                req.signal_id[:8], age_ms, req.ioc_timeout_ms,
            )
            return None

        spread = best_ask - best_bid
        if side == "BUY":
            limit_price  = round(best_ask + spread * 0.5, 2)
            signal_price = best_ask
        else:
            limit_price  = round(best_bid - spread * 0.5, 2)
            signal_price = best_bid

        protection_wall_price = req.micro_signal.protection_wall.price
        sl_distance = abs(signal_price - protection_wall_price)
        if sl_distance < 1e-8:
            logger.warning(
                "[Exec] Zero SL distance for signal %s — skipped", req.signal_id[:8]
            )
            return None

        qty = round((self._equity_fn() * req.notional_hint) / sl_distance, 6)
        if qty <= 0:
            logger.warning(
                "[Exec] Zero qty for signal %s — skipped", req.signal_id[:8]
            )
            return None

        entry_order = IOCLimitOrder(
            symbol   = settings.SYMBOL,
            side     = side,
            quantity = qty,
            price    = limit_price,
        )

        if settings.DRY_RUN:
            logger.info(
                "[Exec] DRY RUN — %s | signal_id=%s",
                entry_order, req.signal_id[:8],
            )
            resp: dict = {
                "orderId":     "DRY_RUN",
                "status":      "FILLED",
                "executedQty": str(qty),
                "fills":       [{"price": str(limit_price), "qty": str(qty)}],
            }
        else:
            try:
                resp = await self._client.create_order(**entry_order.to_entry_params())
            except Exception as exc:
                logger.error("[Exec] IOC order failed: %s", exc, exc_info=True)
                return None

            exec_qty = float(resp.get("executedQty", "0"))
            if exec_qty == 0.0:
                logger.warning(
                    "[Exec] IOC expired unfilled (status=%s) — signal stale, "
                    "no retry | signal_id=%s",
                    resp.get("status"), req.signal_id[:8],
                )
                return None

        fill_price = _weighted_avg_fill(resp)
        direction  = req.micro_signal.direction
        ks_fired   = self._killswitch.record_slippage(signal_price, fill_price, direction)
        if ks_fired and self._ks_fire_cb is not None:
            asyncio.create_task(self._ks_fire_cb("KILLSWITCH_SLIPPAGE"))

        slippage_bps = (
            (fill_price - signal_price) / signal_price * 10_000
            if direction == "LONG"
            else (signal_price - fill_price) / signal_price * 10_000
        )
        fill_qty = float(resp["executedQty"])

        # Acquire lock; all open-position writes are atomic with fill_queue put.
        async with self._position_lock:
            # put_nowait: never block the execution loop on a slow consumer.
            try:
                self.fill_queue.put_nowait(FillDetail(
                    signal_id    = req.signal_id,
                    side         = side,
                    fill_price   = fill_price,
                    qty          = fill_qty,
                    limit_price  = limit_price,
                    slippage_bps = slippage_bps,
                    order_id     = str(resp.get("orderId", "DRY_RUN")),
                ))
            except asyncio.QueueFull:
                logger.error(
                    "[Exec] fill_queue full — FillDetail dropped for %s", req.signal_id[:8]
                )
            req.fill_event.set()
            self._open_position_side         = side
            self._open_position_qty          = fill_qty
            self._open_position_closed_event = req.position_closed_event

        logger.info(
            "[Exec] FILLED — %s %.6f @ %.2f slippage=%.2fbps | signal_id=%s",
            side, fill_qty, fill_price, slippage_bps, req.signal_id[:8],
        )
        return resp

    # ── OCO bracket ───────────────────────────────────────────────────────────

    async def _place_oco(
        self,
        req: MicroOrderRequest,
        fill_price: float,
        fill_qty: float,
        entry_side: str,
    ) -> None:
        """
        Place TP limit + SL stop-limit OCO bracket via OCOOrder after entry fill.

        Concurrency (S1 fix):
          _placing_oco is set True before the REST call and False after.
          If Gate 6 fires during placement it sets _cancel_oco_on_placement=True
          and returns. _place_oco reads the flag atomically on completion and
          cancels the freshly-placed OCO before calling _emergency_close.

        OCO failure (C1 / S2 fix):
          On exception: _emergency_close is called immediately. On confirmed
          close, _reset_open_position() is called and position_closed_event is
          set so Gate 6 cannot fire a second close attempt.
        """
        sl_price    = req.micro_signal.protection_wall.price
        sl_distance = abs(fill_price - sl_price)
        exit_side   = "SELL" if entry_side == "BUY" else "BUY"

        if entry_side == "BUY":
            tp_price = fill_price + settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit = round(sl_price * 0.999, 2)
        else:
            tp_price = fill_price - settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit = round(sl_price * 1.001, 2)

        oco = OCOOrder(
            symbol   = settings.SYMBOL,
            side     = exit_side,
            quantity = fill_qty,
            tp_price = tp_price,
            sl_price = sl_price,
            sl_limit = sl_limit,
        )

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — %s", oco)
            return

        # S1: flag placement in-flight before the REST call.
        async with self._position_lock:
            self._placing_oco = True

        try:
            oco_resp = await self._client.create_oco_order(**oco.to_entry_params())

            # Atomically: record OCO id, clear flag, snapshot deferred-cancel flag.
            async with self._position_lock:
                raw_id                        = oco_resp.get("orderListId")
                self._open_oco_list_id        = int(raw_id) if raw_id is not None else None
                self._placing_oco             = False
                should_cancel                 = self._cancel_oco_on_placement
                self._cancel_oco_on_placement = False

            if should_cancel:
                # Gate 6 fired while OCO was in-flight — cancel the just-placed OCO.
                logger.warning(
                    "[Exec] Wall removed during OCO placement — cancelling OCO and closing"
                )
                if self._open_oco_list_id is not None:
                    try:
                        await self._client.delete_oco_order(
                            symbol      = settings.SYMBOL,
                            orderListId = self._open_oco_list_id,
                        )
                    except Exception as cancel_exc:
                        logger.error(
                            "[Exec] Deferred OCO cancel failed: %s", cancel_exc
                        )
                closed = await self._emergency_close(
                    fill_qty, entry_side, reason="WALL_REMOVED_DURING_OCO"
                )
                if closed and req.position_closed_event:
                    req.position_closed_event.set()
                async with self._position_lock:
                    self._reset_open_position()
            else:
                logger.info(
                    "[Exec] OCO placed — TP=%.2f SL=%.2f listId=%s",
                    tp_price, sl_price, self._open_oco_list_id,
                )

        except Exception as exc:
            # C1: naked position — emergency-close immediately.
            async with self._position_lock:
                self._placing_oco             = False
                self._cancel_oco_on_placement = False
            logger.error("[Exec] OCO failed: %s", exc, exc_info=True)
            closed = await self._emergency_close(fill_qty, entry_side, reason="OCO_FAILED")
            # S2: reset state after confirmed close so Gate 6 cannot double-close.
            if closed:
                if req.position_closed_event:
                    req.position_closed_event.set()
                async with self._position_lock:
                    self._reset_open_position()

    # ── Emergency close ───────────────────────────────────────────────────────

    async def _emergency_close(
        self, qty: float, entry_side: str, reason: str
    ) -> bool:
        """
        Aggressive IOC close for unprotected positions via IOCLimitOrder.
        Returns True on confirmed fill, False if unfilled or an exception occurred.
        Callers must act on the return value — this method does not reset state.
        """
        close_side = "SELL" if entry_side == "BUY" else "BUY"
        logger.critical(
            "[Exec] Emergency close — %s %.6f reason=%s", close_side, qty, reason
        )
        if not self._client:
            return False
        try:
            book = await self._client.get_order_book(symbol=settings.SYMBOL, limit=5)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            spread   = best_ask - best_bid

            close_price = (
                round(best_ask + spread * 0.5, 2)
                if close_side == "BUY"
                else round(best_bid - spread * 0.5, 2)
            )
            close_order = IOCLimitOrder(
                symbol   = settings.SYMBOL,
                side     = close_side,
                quantity = qty,
                price    = close_price,
            )
            resp = await self._client.create_order(**close_order.to_entry_params())

            if float(resp.get("executedQty", "0")) == 0.0:
                logger.critical(
                    "[Exec] Emergency close IOC expired unfilled — position still open!"
                )
                return False
            logger.info(
                "[Exec] Emergency close confirmed — %s %.6f @ %.2f",
                close_side, qty, close_price,
            )
            return True
        except Exception as exc:
            logger.critical("[Exec] Emergency close failed: %s", exc, exc_info=True)
            return False

    # ── Gate 6 callback ───────────────────────────────────────────────────────

    async def handle_protection_wall_removed(self, position_side: str) -> None:
        """
        Called by PersistenceMonitor (Gate 6) when the protection wall is no
        longer in the book.

        S1 race fix:
          If _placing_oco is True, the OCO REST call is in-flight. Setting
          _cancel_oco_on_placement=True delegates cancel+close to _place_oco,
          which reads the flag atomically on completion. This prevents the
          double-close race where both Gate 6 and _place_oco try to close.

        S2 guarantee:
          position_closed_event is set ONLY on a confirmed exit fill.
        """
        logger.warning(
            "[Exec] Gate6 alert — protection wall removed (side=%s)", position_side
        )

        async with self._position_lock:
            qty         = self._open_position_qty
            side        = self._open_position_side
            oco_id      = self._open_oco_list_id
            event       = self._open_position_closed_event
            placing_oco = self._placing_oco
            if placing_oco:
                # Delegate cancel+close to _place_oco (S1 fix).
                self._cancel_oco_on_placement = True

        if placing_oco:
            logger.warning(
                "[Exec] OCO placement in progress — deferred cancel-and-close scheduled"
            )
            return

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — would cancel OCO and close position")
            if event:
                event.set()
            async with self._position_lock:
                self._reset_open_position()
            return

        if oco_id is not None and self._client:
            try:
                await self._client.delete_oco_order(
                    symbol      = settings.SYMBOL,
                    orderListId = oco_id,
                )
                logger.info("[Exec] OCO %d cancelled for early exit", oco_id)
            except Exception as exc:
                logger.error("[Exec] OCO cancel failed: %s", exc, exc_info=True)

        # S2 — fire position_closed_event only on confirmed fill.
        closed = False
        if side and qty > 0:
            closed = await self._emergency_close(qty, side, reason="WALL_REMOVED")

        if closed:
            if event:
                event.set()
        else:
            logger.critical(
                "[Exec] Early exit IOC unfilled — position still open; "
                "position_closed_event NOT set; manual intervention required"
            )

        async with self._position_lock:
            self._reset_open_position()

    def _reset_open_position(self) -> None:
        """Must be called under _position_lock."""
        self._open_position_side          = None
        self._open_position_qty           = 0.0
        self._open_oco_list_id            = None
        self._open_position_closed_event  = None
        self._placing_oco                 = False
        self._cancel_oco_on_placement     = False

    # ── Killswitch hard stop ──────────────────────────────────────────────────

    async def force_close_all(self, reason: str) -> None:
        """
        Killswitch-triggered hard stop: reject all new signals and immediately
        close any open position.  Called by emergency_close_all in main.py.

        Mirrors the Gate-6 cancel+close logic with the S1 race-fix intact.
        """
        self.accepting_new_signals = False
        logger.critical("[Exec] force_close_all — reason=%s", reason)

        async with self._position_lock:
            qty         = self._open_position_qty
            side        = self._open_position_side
            oco_id      = self._open_oco_list_id
            event       = self._open_position_closed_event
            placing_oco = self._placing_oco

        if qty == 0.0 or side is None:
            logger.info("[Exec] force_close_all: no open position to close")
            return

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — force_close_all: resetting position state")
            if event:
                event.set()
            async with self._position_lock:
                self._reset_open_position()
            return

        # Live: if OCO is still being placed, delegate to _place_oco (S1 fix).
        if placing_oco:
            async with self._position_lock:
                self._cancel_oco_on_placement = True
            logger.warning("[Exec] force_close_all: OCO in-flight — deferred cancel scheduled")
            return

        if oco_id is not None and self._client:
            try:
                await self._client.delete_oco_order(
                    symbol=settings.SYMBOL,
                    orderListId=oco_id,
                )
                logger.info("[Exec] force_close_all: OCO %d cancelled", oco_id)
            except Exception as exc:
                logger.error("[Exec] force_close_all: OCO cancel failed: %s", exc)

        closed = await self._emergency_close(qty, side, reason)
        if closed and event:
            event.set()
        elif not closed:
            logger.critical(
                "[Exec] force_close_all: emergency close unfilled — manual intervention required"
            )

        async with self._position_lock:
            self._reset_open_position()
