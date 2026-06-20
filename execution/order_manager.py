"""
Execution layer — IOC aggressive limit orders (Rule 10, master arch §9).

Entry orders are IOC limit orders priced to cross the spread:
  LONG  → limit = best_ask + (best_ask - best_bid) × 0.25
  SHORT → limit = best_bid - (best_ask - best_bid) × 0.25

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
import math
import time
from collections.abc import Awaitable, Callable

from binance import AsyncClient

from config import settings
from core.alerting import AlertDispatcher
from execution.orders import FuturesMarketOrder, FuturesSLOrder, FuturesTPOrder, IOCLimitOrder
from models import FillDetail, MicroOrderRequest
from risk.killswitch import GlobalKillswitch
from risk.sizing import clamp_stop_bps

logger = logging.getLogger(__name__)

# Used only when DRY_RUN=True and no book_fn is injected.
# Inject book_fn (e.g. from LOB engine) for realistic DRY_RUN sizing.
_DRY_RUN_BEST_BID = 95_000.0
_DRY_RUN_BEST_ASK = 95_010.0

_MAX_CONNECT_ATTEMPTS  = 3
_RECONNECT_DELAY_S     = 5.0
_IOC_OVERSHOOT_FRACTION = 0.25   # fraction of spread crossed beyond best bid/ask
_MAX_CLOSE_RETRIES     = 5       # max emergency-close attempts before blocking new signals


def _weighted_avg_fill(resp: dict) -> float:
    """Weighted-average fill price from a Binance order response.

    Futures create_order responses carry avgPrice instead of a fills array.
    """
    avg = resp.get("avgPrice")
    if avg:
        return float(avg)
    fills = resp.get("fills", [])
    if not fills:
        return float(resp.get("price", 0.0))
    total_qty = sum(float(f["qty"]) for f in fills)
    if total_qty == 0.0:
        logger.warning("[Exec] _weighted_avg_fill: all fill quantities are zero — returning 0.0")
        return 0.0
    return sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty


def _net_fill_qty(resp: dict, side: str, base_asset: str) -> float:
    """Net base-asset qty available after fees.

    On Binance Spot, BUY fees may be deducted in the base asset (e.g. BTC on
    BTCUSDT). Placing an OCO or IOC close for the gross executedQty triggers
    -2010 when the fee was taken in BTC and the full gross amount is not held.
    SHORT fees are deducted in the quote asset, so no adjustment is needed.
    """
    gross = float(resp.get("executedQty", 0))
    if side != "BUY":
        return gross
    fills = resp.get("fills", [])
    if not fills:
        return gross
    net = 0.0
    for f in fills:
        qty = float(f["qty"])
        if f.get("commissionAsset", "").upper() == base_asset.upper():
            net += qty - float(f.get("commission", 0))
        else:
            net += qty
    return max(net, 0.0)


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
        book_fn: Callable[[], tuple[float, float] | None] | None = None,
        ks_fire_cb: Callable[[str], Awaitable[None]] | None = None,
        update_outcome_cb: Callable[[str, str, float, float, float, float | None], Awaitable[None]] | None = None,
        budget_update_cb: Callable[[float], None] | None = None,
        portfolio_hub=None,
        alert_dispatcher: AlertDispatcher | None = None,
    ) -> None:
        """
        book_fn: optional callable returning (best_bid, best_ask) without a REST
        round-trip — wire to the LOB engine's current top-of-book for lowest latency
        and accurate DRY_RUN position sizing. Falls back to REST when None.
        ks_fire_cb: called with a reason string when KS-3 slippage fires.
        update_outcome_cb: called with (signal_id, outcome, pnl, pnl_pct, duration_min, r_multiple)
        after each position closes — wire to SignalTelemetry.update_outcome.
        budget_update_cb: called with realised pnl (float) on every close — wire to
        increment DailyBudget.realised_pnl so KS-1 budget checks reflect actual trades.
        portfolio_hub: optional RealtimeHub — broadcasts position_opened and close_event
        to the live dashboard immediately on fill, without waiting for the 1 Hz MTM loop.
        """
        self._signal_q          = signal_queue
        self.fill_queue         = fill_queue
        self._killswitch        = killswitch
        self._equity_fn         = equity_fn
        self._book_fn           = book_fn
        self._ks_fire_cb        = ks_fire_cb
        self._update_outcome_cb = update_outcome_cb
        self._budget_update_cb  = budget_update_cb
        self._portfolio_hub     = portfolio_hub
        self._alert_dispatcher  = alert_dispatcher
        self._client: AsyncClient | None = None

        # Graceful-shutdown gate: set False before draining the queue.
        self.accepting_new_signals: bool = True

        # Counts consecutive emergency-close failures; reset on position open.
        self._close_retry_count: int = 0
        # True when max retries blocked new signals; allows restoration on position close.
        self._close_block_active: bool = False

        # Single lock guards all _open_position_* and placement-flag fields.
        self._position_lock = asyncio.Lock()

        # ── Open-position state (written atomically on fill; cleared after exit) ──
        self._open_position_side:         str | None          = None
        self._open_position_qty:          float               = 0.0
        self._open_tp_order_id:           int | None          = None
        self._open_sl_order_id:           int | None          = None
        self._open_position_closed_event: asyncio.Event | None = None
        self._open_signal_id:             str | None          = None
        self._open_entry_price:           float               = 0.0
        self._open_entry_time:            float               = 0.0   # time.monotonic()
        self._open_sl_price:              float               = 0.0   # set by _place_oco; used to compute R-multiple
        self._open_tp_price:              float               = 0.0   # set by _place_oco alongside SL
        self._oco_watcher_task:           asyncio.Task | None = None  # polls OCO until natural fill
        self._tp_sl_monitor_task:         asyncio.Task | None = None  # BINANCE_DEMO software TP/SL watcher

        # ── OCO placement race flags (S1 fix) ────────────────────────────────────
        # _placing_oco:            True while create_oco_order REST call is in-flight.
        # _cancel_oco_on_placement: Gate 6 sets this while _placing_oco is True;
        #                           _place_oco reads it on completion and cancels.
        self._placing_oco:            bool = False
        self._cancel_oco_on_placement: bool = False
        self._entry_in_flight:          bool = False
        self._emergency_close_in_progress: bool = False

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
                _api_key    = settings.DEMO_BINANCE_API_KEY if settings.BINANCE_DEMO else settings.BINANCE_API_KEY
                _api_secret = settings.DEMO_BINANCE_API_SECRET if settings.BINANCE_DEMO else settings.BINANCE_API_SECRET
                self._client = await AsyncClient.create(
                    api_key    = _api_key,
                    api_secret = _api_secret,
                    testnet    = settings.BINANCE_TESTNET,
                    demo       = settings.BINANCE_DEMO,
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
            if self.has_active_exposure():
                logger.warning(
                    "[Exec] Active exposure — discarding signal %s", req.signal_id[:8]
                )
                self._write_entry_failure(req.signal_id, "NO_FILL")
                continue
            await self._submit(req)

    # ── Core submission ───────────────────────────────────────────────────────

    def _has_active_exposure_locked(self) -> bool:
        return (
            self._entry_in_flight
            or self._emergency_close_in_progress
            or self._placing_oco
            or self._open_position_side is not None
            or self._open_position_qty > 0.0
        )

    def has_active_exposure(self) -> bool:
        """Return True when a microstructure entry/position/close is active."""
        return self._has_active_exposure_locked()

    def get_open_position(self) -> dict | None:
        """Return in-memory open position for display. None if no position open."""
        if self._open_position_side is None:
            return None
        return {
            "symbol":         settings.SYMBOL,
            "side":           self._open_position_side,
            "entry_price":    self._open_entry_price,
            "quantity":       self._open_position_qty,
            "stop_loss":      self._open_sl_price  or None,
            "take_profit":    self._open_tp_price  or None,
            "unrealised_pnl": None,
        }

    async def get_exchange_position(self) -> tuple[float, str | None]:
        """Query the exchange for the current BTCUSDT position.

        Returns (qty, side_str) where side_str is 'BUY' or 'SELL', or (0.0, None) if flat.
        Returns (0.0, None) when client is None, DRY_RUN, or on any exception.
        """
        if settings.DRY_RUN or self._client is None:
            return 0.0, None
        try:
            account = await self._client.futures_account()
            for pos in account.get("positions", []):
                if pos.get("symbol") == settings.SYMBOL:
                    amt = float(pos.get("positionAmt", 0.0))
                    if abs(amt) > 0.0:
                        return abs(amt), "BUY" if amt > 0 else "SELL"
        except Exception as exc:
            logger.warning("[Exec] get_exchange_position failed: %s", exc)
        return 0.0, None

    def get_unrealised_pnl(self) -> float:
        """MTM PnL of open position at current mid-price. 0.0 if no position or no book."""
        if self._open_position_side is None or self._open_position_qty <= 0.0:
            return 0.0
        book = self._book_fn() if self._book_fn else None
        if book is None:
            return 0.0
        mid = (book[0] + book[1]) / 2.0
        if self._open_position_side == "BUY":
            return (mid - self._open_entry_price) * self._open_position_qty
        return (self._open_entry_price - mid) * self._open_position_qty

    async def _submit(self, req: MicroOrderRequest) -> None:
        async with self._position_lock:
            if self._has_active_exposure_locked():
                logger.warning(
                    "[Exec] Active exposure at submit — skipping %s", req.signal_id[:8]
                )
                self._write_entry_failure(req.signal_id, "NO_FILL")
                return
            self._entry_in_flight = True
        try:
            book = await self._resolve_book()
            if book is None:
                logger.warning(
                    "[Exec] Could not resolve order book — skipping %s", req.signal_id[:8]
                )
                self._write_entry_failure(req.signal_id, "NO_FILL")
                return
            best_bid, best_ask = book

            resp = await self._submit_aggressive_limit(req, req.side, best_bid, best_ask)
            if resp is None:
                self._write_entry_failure(req.signal_id, "NO_FILL")
                return

            fill_price = _weighted_avg_fill(resp)
            step       = settings.QTY_STEP_SIZE
            oco_qty    = round(
                math.floor(
                    _net_fill_qty(resp, req.side, settings.SYMBOL.removesuffix("USDT")) / step
                ) * step,
                3,
            )
            await self._place_oco(req, fill_price, oco_qty, req.side)
            if settings.BINANCE_DEMO and self._open_position_qty > 0:
                self._tp_sl_monitor_task = asyncio.create_task(
                    self._watch_tp_sl(
                        position_side=req.side,
                        fill_qty=oco_qty,
                        signal_id=req.signal_id,
                        entry_price=fill_price,
                        entry_time=self._open_entry_time,
                        position_closed_event=req.position_closed_event,
                    ),
                    name=f"tp_sl_monitor_{req.signal_id[:8]}",
                )
        finally:
            async with self._position_lock:
                self._entry_in_flight = False

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
                book = await self._client.futures_order_book(
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
        tick   = settings.PRICE_TICK_SIZE
        if side == "BUY":
            limit_price  = round(round((best_ask + spread * _IOC_OVERSHOOT_FRACTION) / tick) * tick, 1)
            signal_price = best_ask
        else:
            limit_price  = round(round((best_bid - spread * _IOC_OVERSHOOT_FRACTION) / tick) * tick, 1)
            signal_price = best_bid

        protection_wall_price = req.micro_signal.protection_wall.price
        raw_bps    = abs(signal_price - protection_wall_price) / signal_price * 10_000
        capped_bps = clamp_stop_bps(
            raw_bps, settings.PROTECTION_MIN_DISTANCE_BPS, settings.PROTECTION_MAX_DISTANCE_BPS
        )
        sl_distance = signal_price * capped_bps / 10_000
        if sl_distance < 1e-8:
            logger.warning(
                "[Exec] Zero SL distance for signal %s — skipped", req.signal_id[:8]
            )
            return None

        raw_qty = (self._equity_fn() * req.notional_hint) / sl_distance
        step    = settings.QTY_STEP_SIZE

        qty = round(math.floor(raw_qty / step) * step, 3)
        if qty <= 0:
            logger.warning(
                "[Exec] Zero qty for signal %s — skipped", req.signal_id[:8]
            )
            return None

        notional = qty * limit_price
        if notional < settings.MIN_NOTIONAL:
            logger.warning(
                "[Exec] Notional %.2f < MIN_NOTIONAL %.2f for signal %s "
                "(sl_distance=%.2f likely caused by real-LOB vs testnet price mismatch) — skipped",
                notional, settings.MIN_NOTIONAL, req.signal_id[:8], sl_distance,
            )
            return None

        if settings.BINANCE_DEMO:
            # Demo book has thin synthetic liquidity — MARKET order guarantees immediate fill.
            entry_order = FuturesMarketOrder(
                symbol   = settings.SYMBOL,
                side     = side,
                quantity = qty,
            )
        else:
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
                resp = await self._client.futures_create_order(**entry_order.to_entry_params())
            except Exception as exc:
                logger.error("[Exec] IOC order failed: %s", exc, exc_info=True)
                return None

            exec_qty = float(resp.get("executedQty", "0"))
            if exec_qty == 0.0 and settings.BINANCE_DEMO:
                # Demo fills MARKET orders asynchronously (REST returns NEW immediately).
                order_id = resp.get("orderId")
                for _ in range(15):          # 15 × 200 ms = 3 s max
                    await asyncio.sleep(0.2)
                    try:
                        polled = await self._client.futures_get_order(
                            symbol=settings.SYMBOL, orderId=order_id
                        )
                    except Exception as exc:
                        logger.warning("[Exec] Demo fill-poll error: %s", exc)
                        break
                    exec_qty = float(polled.get("executedQty", "0"))
                    if exec_qty > 0.0:
                        resp = polled
                        logger.info(
                            "[Exec] Demo async fill: %s qty=%s @ %s | signal_id=%s",
                            side, exec_qty, polled.get("avgPrice"), req.signal_id[:8],
                        )
                        break

            if exec_qty == 0.0:
                logger.warning(
                    "[Exec] IOC expired unfilled (status=%s) — signal stale, "
                    "no retry | signal_id=%s",
                    resp.get("status"), req.signal_id[:8],
                )
                self._write_entry_failure(req.signal_id, "NO_FILL")
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
        step     = settings.QTY_STEP_SIZE
        net_qty  = round(
            math.floor(
                _net_fill_qty(resp, side, settings.SYMBOL.removesuffix("USDT")) / step
            ) * step,
            3,
        )

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
            self._entry_in_flight            = False
            self._open_position_side         = side
            self._open_position_qty          = net_qty
            self._open_position_closed_event = req.position_closed_event
            self._open_signal_id             = req.signal_id
            self._open_entry_price           = fill_price
            self._open_entry_time            = time.monotonic()

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
        Place TP + SL bracket via FuturesTPOrder / FuturesSLOrder after entry fill.

        Concurrency (S1 fix):
          _placing_oco is set True before the first REST call and False after both.
          If Gate 6 fires during placement it sets _cancel_oco_on_placement=True
          and returns. _place_oco reads the flag atomically on completion and
          cancels any placed orders before calling _emergency_close.

        Partial-placement (C1 / S2 fix):
          On any exception: cancel whatever orders were placed, then emergency-close.
          On confirmed close, _reset_open_position() is called and
          position_closed_event is set so Gate 6 cannot fire a second close attempt.
        """
        raw_wall   = req.micro_signal.protection_wall.price
        raw_bps    = abs(fill_price - raw_wall) / fill_price * 10_000
        capped_bps = clamp_stop_bps(
            raw_bps, settings.PROTECTION_MIN_DISTANCE_BPS, settings.PROTECTION_MAX_DISTANCE_BPS
        )
        sl_distance = fill_price * capped_bps / 10_000
        tick        = settings.PRICE_TICK_SIZE
        raw_sl      = fill_price - sl_distance if entry_side == "BUY" else fill_price + sl_distance
        sl_price    = round(round(raw_sl / tick) * tick, 1)
        exit_side   = "SELL" if entry_side == "BUY" else "BUY"
        self._open_sl_price = sl_price

        if entry_side == "BUY":
            tp_price = fill_price + settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit = round(round(sl_price * 0.999 / tick) * tick, 1)
        else:
            tp_price = fill_price - settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit = round(round(sl_price * 1.001 / tick) * tick, 1)
        self._open_tp_price = tp_price

        tp_order = FuturesTPOrder(
            symbol      = settings.SYMBOL,
            side        = exit_side,
            quantity    = fill_qty,
            stop_price  = tp_price,
            limit_price = tp_price,
        )
        sl_order = FuturesSLOrder(
            symbol      = settings.SYMBOL,
            side        = exit_side,
            quantity    = fill_qty,
            stop_price  = sl_price,
            limit_price = sl_limit,
        )

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — TP=%s SL=%s", tp_order, sl_order)
            return

        # Demo accounts route TP/SL through the Algo Conditional API, returning
        # algoId instead of orderId. Gate6 handles exit within ~300ms via
        # handle_protection_wall_removed, so bracket placement is not needed.
        if settings.BINANCE_DEMO:
            logger.info("[Exec] BINANCE_DEMO — skipping bracket placement (TP=%s SL=%s)",
                        tp_order, sl_order)
            return

        # S1: flag placement in-flight before the first REST call.
        async with self._position_lock:
            self._placing_oco = True

        try:
            tp_params = tp_order.to_entry_params()
            sl_params = sl_order.to_entry_params()
            if settings.BINANCE_DEMO:
                # Demo returns HTTP 200 {"code":-2022} (soft error) for reduceOnly=true.
                tp_params.pop("reduceOnly", None)
                sl_params.pop("reduceOnly", None)

            tp_resp = await self._client.futures_create_order(**tp_params)
            sl_resp = await self._client.futures_create_order(**sl_params)

            if "orderId" not in tp_resp or "orderId" not in sl_resp:
                logger.error(
                    "[Exec] Bracket order missing orderId — tp=%s sl=%s",
                    tp_resp, sl_resp,
                )
                raise KeyError(f"orderId absent: tp={tp_resp} sl={sl_resp}")

            # Atomically: record order IDs, clear flag, snapshot deferred-cancel flag.
            async with self._position_lock:
                self._open_tp_order_id        = int(tp_resp["orderId"])
                self._open_sl_order_id        = int(sl_resp["orderId"])
                self._placing_oco             = False
                should_cancel                 = self._cancel_oco_on_placement
                self._cancel_oco_on_placement = False

            if should_cancel:
                # Gate 6 fired while orders were in-flight — cancel both and close.
                logger.warning(
                    "[Exec] Wall removed during bracket placement — cancelling TP/SL and closing"
                )
                await self._cancel_bracket_orders()
                closed, close_p = await self._emergency_close(
                    fill_qty, entry_side, reason="WALL_REMOVED_DURING_OCO"
                )
                async with self._position_lock:
                    entry_t  = self._open_entry_time
                    entry_sl = self._open_sl_price
                    if closed:
                        self._reset_open_position()
                    else:
                        self._emergency_close_in_progress = False  # release for retry
                if closed:
                    self._record_outcome(req.signal_id, entry_side, fill_price, close_p, fill_qty, entry_t, entry_sl)
                    if req.position_closed_event:
                        req.position_closed_event.set()
                else:
                    logger.critical(
                        "[Exec] WALL_REMOVED_DURING_OCO close FAILED — position still open; "
                        "position_closed_event NOT set"
                    )
            else:
                logger.info(
                    "[Exec] Bracket placed — TP=%.2f (id=%s) SL=%.2f (id=%s)",
                    tp_price, self._open_tp_order_id,
                    sl_price, self._open_sl_order_id,
                )
                self._oco_watcher_task = asyncio.create_task(
                    self._watch_oco_outcome(
                        oco_tp_id             = self._open_tp_order_id,
                        oco_sl_id             = self._open_sl_order_id,
                        signal_id             = req.signal_id,
                        entry_side            = entry_side,
                        entry_price           = fill_price,
                        fill_qty              = fill_qty,
                        entry_time            = self._open_entry_time,
                        position_closed_event = req.position_closed_event,
                    ),
                    name=f"oco_watcher_{req.signal_id[:8]}",
                )
                if self._portfolio_hub is not None:
                    _t = asyncio.create_task(
                        self._portfolio_hub.broadcast({
                            "type":        "position_opened",
                            "ts":          time.time(),
                            "signal_id":   req.signal_id,
                            "side":        entry_side,
                            "entry_price": fill_price,
                            "quantity":    fill_qty,
                            "stop_loss":   sl_price,
                            "take_profit": tp_price,
                        })
                    )
                    _t.add_done_callback(
                        lambda t: logger.error(
                            "[Exec] position_opened broadcast failed: %s", t.exception()
                        ) if not t.cancelled() and t.exception() is not None else None
                    )

        except Exception as exc:
            # C1: naked position — cancel any partially-placed orders then emergency-close.
            # Raise _emergency_close_in_progress before the first await so Gate6's
            # handle_protection_wall_removed cannot fire a concurrent close.
            async with self._position_lock:
                self._placing_oco                 = False
                self._cancel_oco_on_placement     = False
                self._emergency_close_in_progress = True
            logger.error("[Exec] Bracket placement failed: %s", exc, exc_info=True)
            await self._cancel_bracket_orders()
            closed, close_p = await self._emergency_close(fill_qty, entry_side, reason="OCO_FAILED")
            # S2: reset state after confirmed close so Gate 6 cannot double-close.
            if closed:
                async with self._position_lock:
                    entry_t = self._open_entry_time
                    entry_sl = self._open_sl_price
                    self._reset_open_position()
                self._record_outcome(req.signal_id, entry_side, fill_price, close_p, fill_qty, entry_t, entry_sl)
                if req.position_closed_event:
                    req.position_closed_event.set()

    # ── Bracket cancel helper ─────────────────────────────────────────────────

    async def _cancel_bracket_orders(self) -> None:
        """Cancel whichever TP/SL bracket orders are currently recorded."""
        if not self._client:
            return
        for oid in (self._open_tp_order_id, self._open_sl_order_id):
            if oid is not None:
                try:
                    await self._client.futures_cancel_order(
                        symbol=settings.SYMBOL, orderId=oid
                    )
                except Exception as cancel_exc:
                    logger.error("[Exec] Cancel order %d failed: %s", oid, cancel_exc)

    # ── Bracket natural-fill watcher ──────────────────────────────────────────

    async def _watch_oco_outcome(
        self,
        oco_tp_id: int | None,
        oco_sl_id: int | None,
        signal_id: str,
        entry_side: str,
        entry_price: float,
        fill_qty: float,
        entry_time: float,
        position_closed_event: asyncio.Event,
        poll_interval_s: float = 2.0,
    ) -> None:
        """
        Background task: polls TP and SL order status every poll_interval_s until:
          a) position_closed_event fires (Gate 6 / killswitch closed it) → exit quietly, or
          b) either order reaches FILLED → cancel the other leg, record outcome, set event.

        Runs only in live mode (not spawned when DRY_RUN=True).
        Cancelled automatically by _reset_open_position() on any other close path.
        """
        while True:
            try:
                await asyncio.wait_for(position_closed_event.wait(), timeout=poll_interval_s)
                return  # position closed via another path — exit cleanly
            except asyncio.TimeoutError:
                pass

            if not self._client:
                return

            exit_price = 0.0
            filled_id: int | None = None
            for oid in (oco_tp_id, oco_sl_id):
                if oid is None:
                    continue
                try:
                    detail = await self._client.futures_get_order(
                        symbol=settings.SYMBOL, orderId=oid
                    )
                    if detail.get("status") == "FILLED":
                        exit_price = float(detail.get("avgPrice") or detail.get("price", 0))
                        filled_id  = oid
                        break
                except Exception as exc:
                    logger.warning("[Exec] futures_get_order(%d) failed: %s", oid, exc)

            if filled_id is None:
                continue  # neither filled yet

            if exit_price <= 0:
                logger.warning(
                    "[Exec] Bracket fill detected but exit price unknown — signal=%s",
                    signal_id[:8],
                )
                if not position_closed_event.is_set():
                    async with self._position_lock:
                        if self._open_signal_id == signal_id:
                            self._reset_open_position()
                    position_closed_event.set()
                return

            # Cancel the surviving leg.
            other_id = oco_sl_id if filled_id == oco_tp_id else oco_tp_id
            if other_id is not None and self._client:
                try:
                    await self._client.futures_cancel_order(
                        symbol=settings.SYMBOL, orderId=other_id
                    )
                except Exception as exc:
                    logger.warning("[Exec] Cancel surviving bracket leg %d failed: %s", other_id, exc)

            if not position_closed_event.is_set():
                async with self._position_lock:
                    et = self._open_entry_time if self._open_signal_id == signal_id else entry_time
                    sl = self._open_sl_price if self._open_signal_id == signal_id else 0.0
                    if self._open_signal_id == signal_id:
                        self._reset_open_position()
                self._record_outcome(signal_id, entry_side, entry_price, exit_price, fill_qty, et, sl)
                position_closed_event.set()
                logger.info(
                    "[Exec] Bracket natural fill — %s exit=%.2f signal=%s",
                    "WIN" if (exit_price > entry_price) == (entry_side == "BUY") else "LOSS",
                    exit_price, signal_id[:8],
                )
            return

    # ── Software TP/SL monitor (BINANCE_DEMO only — no real bracket) ──────────

    async def _watch_tp_sl(
        self,
        position_side: str,
        fill_qty: float,
        signal_id: str,
        entry_price: float,
        entry_time: float,
        position_closed_event: asyncio.Event,
        poll_interval_s: float = 0.1,
    ) -> None:
        """
        Polls book_fn for TP/SL touch against the live _open_tp_price/_open_sl_price
        (read fresh each iteration, not captured at task-creation time). Mirrors
        _watch_oco_outcome's lifecycle: exits cleanly the instant any other path
        sets position_closed_event. Only started when settings.BINANCE_DEMO is True —
        see _place_oco's BINANCE_DEMO early-return: on demo, no bracket is ever
        placed, so this is the only price-target closer besides Gate6 / MAX_HOLD.
        """
        while True:
            try:
                await asyncio.wait_for(position_closed_event.wait(), timeout=poll_interval_s)
                return  # closed via bracket / Gate6 / killswitch — exit quietly
            except asyncio.TimeoutError:
                pass

            book = self._book_fn() if self._book_fn else None
            if book is None:
                continue
            best_bid, best_ask = book
            mid = (best_bid + best_ask) / 2.0
            tp_price, sl_price = self._open_tp_price, self._open_sl_price

            touched = (
                (mid >= tp_price or mid <= sl_price) if position_side == "BUY"
                else (mid <= tp_price or mid >= sl_price)
            )
            if not touched:
                continue

            async with self._position_lock:
                if self._placing_oco or self._emergency_close_in_progress or position_closed_event.is_set():
                    continue  # bracket placement or another close path already active/done
                self._emergency_close_in_progress = True

            await self._cancel_bracket_orders()  # no-op if no bracket was placed (demo)
            closed, close_p = await self._emergency_close(fill_qty, position_side, reason="TP_SL_TOUCH")

            async with self._position_lock:
                if self._open_signal_id != signal_id:
                    continue  # position already reset by a different path while we awaited
                if not closed:
                    self._emergency_close_in_progress = False  # release so loop can retry
                else:
                    et, sl = self._open_entry_time, self._open_sl_price
                    self._reset_open_position()

            if closed:
                self._record_outcome(signal_id, position_side, entry_price, close_p, fill_qty, et, sl)
                if position_closed_event and not position_closed_event.is_set():
                    position_closed_event.set()
                return

            self._close_retry_count += 1
            logger.critical(
                "[Exec] TP/SL emergency close FAILED — position still open, retrying | signal_id=%s | attempt=%d/%d",
                signal_id[:8], self._close_retry_count, _MAX_CLOSE_RETRIES,
            )
            if self._close_retry_count >= _MAX_CLOSE_RETRIES:
                self.accepting_new_signals = False
                self._close_block_active = True
            if self._close_retry_count == _MAX_CLOSE_RETRIES:
                logger.critical(
                    "[Exec] Max close retries (%d) reached — blocking new signals until position resolves | signal_id=%s",
                    _MAX_CLOSE_RETRIES, signal_id[:8],
                )
                if self._alert_dispatcher is not None:
                    asyncio.create_task(
                        self._alert_dispatcher.notify_killswitch(
                            f"MAX_CLOSE_RETRIES_{_MAX_CLOSE_RETRIES}", 0.0
                        )
                    )
            await asyncio.sleep(poll_interval_s * 5)  # brief backoff before retry

    # ── User data stream callback ─────────────────────────────────────────────

    async def on_execution_report(self, msg: dict) -> None:
        """Called by UserDataStreamConsumer for every ORDER_TRADE_UPDATE event.

        Fires immediately on bracket TP/SL fill — cancels the REST poller and
        records the outcome without waiting for the 2-second polling cycle.
        Futures events wrap order fields inside the 'o' key.
        """
        order    = msg.get("o", {})   # futures: all order fields are inside 'o'
        if order.get("X") != "FILLED":
            return

        order_id = int(order.get("i", -1))
        other_id: int | None = None

        async with self._position_lock:
            if order_id not in (self._open_tp_order_id, self._open_sl_order_id):
                return

            other_id = (
                self._open_sl_order_id if order_id == self._open_tp_order_id
                else self._open_tp_order_id
            )

            # Capture state before reset clears it.
            signal_id   = self._open_signal_id
            entry_side  = self._open_position_side
            entry_price = self._open_entry_price
            fill_qty    = self._open_position_qty
            entry_time  = self._open_entry_time
            sl_price    = self._open_sl_price
            pce         = self._open_position_closed_event
            self._reset_open_position()

        exit_price = float(order.get("L", 0.0))
        logger.info(
            "[Exec] ORDER_TRADE_UPDATE fill — side=%s exit=%.2f signal=%s",
            entry_side, exit_price, (signal_id or "")[:8],
        )

        # Cancel the surviving bracket leg.
        if other_id is not None and self._client:
            try:
                await self._client.futures_cancel_order(
                    symbol=settings.SYMBOL, orderId=other_id
                )
            except Exception as exc:
                logger.warning("[Exec] Cancel surviving bracket leg %d failed: %s", other_id, exc)

        result = self._record_outcome(
            signal_id, entry_side, entry_price, exit_price, fill_qty, entry_time, sl_price
        )
        if pce:
            pce.set()

        if result is not None and self._portfolio_hub is not None:
            outcome, pnl, pnl_pct, duration_min, r_multiple = result
            _t = asyncio.create_task(
                self._portfolio_hub.broadcast({
                    "type":         "close_event",
                    "ts":           time.time(),
                    "signal_id":    signal_id,
                    "outcome":      outcome,
                    "pnl":          pnl,
                    "pnl_pct":      pnl_pct,
                    "duration_min": duration_min,
                    "r_multiple":   r_multiple,
                })
            )
            _t.add_done_callback(
                lambda t: logger.error(
                    "[Exec] close_event broadcast failed: %s", t.exception()
                ) if not t.cancelled() and t.exception() is not None else None
            )

    def _write_entry_failure(self, signal_id: str, outcome: str) -> None:
        """Write a terminal non-trade outcome to telemetry. No-op when not wired."""
        if self._update_outcome_cb is None:
            return
        try:
            asyncio.create_task(
                self._update_outcome_cb(signal_id, outcome, 0.0, 0.0, 0.0, None)
            )
        except RuntimeError:
            logger.warning("[Exec] No event loop — %s outcome not recorded for %s",
                           outcome, signal_id[:8])

    # ── Outcome recording ─────────────────────────────────────────────────────

    def _record_outcome(
        self,
        signal_id: str | None,
        entry_side: str,
        entry_price: float,
        close_price: float,
        qty: float,
        entry_time: float,
        sl_price: float = 0.0,
    ) -> "tuple[str, float, float, float, float | None] | None":
        """Record trade outcome: update budget and schedule telemetry write. No-op when not wired.
        Returns (outcome, pnl, pnl_pct, duration_min, r_multiple) or None if inputs invalid.
        """
        if not signal_id or entry_price <= 0 or qty <= 0:
            return None
        pnl = (close_price - entry_price) * qty if entry_side == "BUY" else (entry_price - close_price) * qty
        pnl_pct = pnl / (entry_price * qty)
        outcome = "WIN" if pnl > 0.0 else "LOSS" if pnl < 0.0 else "FLAT"
        duration_min = (time.monotonic() - entry_time) / 60.0
        r_multiple: float | None = None
        if sl_price > 0:
            initial_risk = abs(entry_price - sl_price) * qty
            if initial_risk > 1e-8:
                r_multiple = pnl / initial_risk
        if self._budget_update_cb is not None:
            self._budget_update_cb(pnl)
        if self._update_outcome_cb is None:
            return (outcome, pnl, pnl_pct, duration_min, r_multiple)
        try:
            asyncio.create_task(
                self._update_outcome_cb(signal_id, outcome, pnl, pnl_pct, duration_min, r_multiple)
            )
        except RuntimeError:
            logger.error("[Exec] No running event loop — outcome not scheduled for %s", signal_id)
        return (outcome, pnl, pnl_pct, duration_min, r_multiple)

    # ── Emergency close ───────────────────────────────────────────────────────

    async def _emergency_close(
        self, qty: float, entry_side: str, reason: str
    ) -> tuple[bool, float]:
        """
        Aggressive IOC close for unprotected positions via IOCLimitOrder.
        Returns (True, close_price) on confirmed fill, (False, 0.0) otherwise.
        Callers must act on the return value — this method does not reset state.
        """
        close_side = "SELL" if entry_side == "BUY" else "BUY"
        logger.critical(
            "[Exec] Emergency close — %s %.6f reason=%s", close_side, qty, reason
        )
        if not self._client:
            return False, 0.0
        try:
            book = await self._client.futures_order_book(symbol=settings.SYMBOL, limit=5)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            spread   = best_ask - best_bid

            tick  = settings.PRICE_TICK_SIZE
            close_price = (
                round(round((best_ask + spread * _IOC_OVERSHOOT_FRACTION) / tick) * tick, 1)
                if close_side == "BUY"
                else round(round((best_bid - spread * _IOC_OVERSHOOT_FRACTION) / tick) * tick, 1)
            )
            if settings.BINANCE_DEMO:
                close_order = FuturesMarketOrder(
                    symbol   = settings.SYMBOL,
                    side     = close_side,
                    quantity = qty,
                )
            else:
                close_order = IOCLimitOrder(
                    symbol   = settings.SYMBOL,
                    side     = close_side,
                    quantity = qty,
                    price    = close_price,
                )
            resp = await self._client.futures_create_order(**close_order.to_entry_params())

            close_exec_qty = float(resp.get("executedQty", "0"))
            if close_exec_qty == 0.0 and settings.BINANCE_DEMO:
                order_id = resp.get("orderId")
                for _ in range(15):
                    await asyncio.sleep(0.2)
                    try:
                        polled = await self._client.futures_get_order(
                            symbol=settings.SYMBOL, orderId=order_id
                        )
                    except Exception as exc:
                        logger.warning("[Exec] Demo close-poll error: %s", exc)
                        break
                    close_exec_qty = float(polled.get("executedQty", "0"))
                    if close_exec_qty > 0.0:
                        resp = polled
                        break

            if close_exec_qty == 0.0:
                logger.critical(
                    "[Exec] Emergency close IOC expired unfilled — position still open!"
                )
                return False, 0.0
            logger.info(
                "[Exec] Emergency close confirmed — %s %.6f @ %.2f",
                close_side, qty, close_price,
            )
            return True, close_price
        except Exception as exc:
            logger.critical("[Exec] Emergency close failed: %s", exc, exc_info=True)
            return False, 0.0

    # ── Gate 6 callback ───────────────────────────────────────────────────────

    async def handle_protection_wall_removed(
        self, position_side: str, reason: str = "WALL_REMOVED"
    ) -> None:
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
            "[Exec] Gate6 alert — %s (side=%s)", reason, position_side
        )

        async with self._position_lock:
            qty               = self._open_position_qty
            side              = self._open_position_side
            event             = self._open_position_closed_event
            placing_oco       = self._placing_oco
            close_in_progress = self._emergency_close_in_progress
            if placing_oco:
                # Delegate cancel+close to _place_oco (S1 fix).
                self._cancel_oco_on_placement = True

        if placing_oco:
            logger.warning(
                "[Exec] Bracket placement in progress — deferred cancel-and-close scheduled"
            )
            return

        if close_in_progress:
            logger.info("[Exec] Emergency close already in progress — Gate6 close suppressed")
            return

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — would cancel bracket and close position")
            async with self._position_lock:
                sig_id  = self._open_signal_id
                ep      = self._open_entry_price
                et      = self._open_entry_time
                sl      = self._open_sl_price
                self._reset_open_position()
            self._record_outcome(sig_id, side or "", ep, ep, qty, et, sl)
            if event:
                event.set()
            return

        await self._cancel_bracket_orders()

        # S2 — fire position_closed_event only on confirmed fill.
        closed, close_p = False, 0.0
        if side and qty > 0:
            async with self._position_lock:
                self._emergency_close_in_progress = True
            closed, close_p = await self._emergency_close(qty, side, reason=reason)

        async with self._position_lock:
            sig_id = self._open_signal_id
            ep     = self._open_entry_price
            et     = self._open_entry_time
            sl     = self._open_sl_price
            if closed:
                self._reset_open_position()
            else:
                self._emergency_close_in_progress = False  # release for _watch_tp_sl retry

        if closed:
            self._record_outcome(sig_id, side or "", ep, close_p, qty, et, sl)
            if event:
                event.set()
        else:
            logger.critical(
                "[Exec] Early exit IOC unfilled — position still open; "
                "position_closed_event NOT set; _watch_tp_sl will retry"
            )

    async def handle_safety_exit(self, reason: str) -> None:
        """Cancel the active OCO and aggressively close for a safety condition."""
        async with self._position_lock:
            side = self._open_position_side
        position_side = "LONG" if side == "BUY" else "SHORT" if side == "SELL" else "UNKNOWN"
        await self.handle_protection_wall_removed(position_side, reason=f"SAFETY_{reason}")

    def _reset_open_position(self) -> None:
        """Must be called under _position_lock."""
        if self._oco_watcher_task and not self._oco_watcher_task.done():
            self._oco_watcher_task.cancel()
        self._oco_watcher_task            = None
        if self._tp_sl_monitor_task and not self._tp_sl_monitor_task.done():
            self._tp_sl_monitor_task.cancel()
        self._tp_sl_monitor_task          = None
        self._open_position_side          = None
        self._open_position_qty           = 0.0
        self._open_tp_order_id            = None
        self._open_sl_order_id            = None
        self._open_position_closed_event  = None
        self._open_signal_id              = None
        self._open_entry_price            = 0.0
        self._open_entry_time             = 0.0
        self._open_sl_price               = 0.0
        self._open_tp_price               = 0.0
        self._placing_oco                 = False
        self._cancel_oco_on_placement     = False
        self._entry_in_flight             = False
        self._emergency_close_in_progress = False
        if self._close_block_active:
            self.accepting_new_signals = True
            self._close_block_active = False
        self._close_retry_count           = 0

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
            event       = self._open_position_closed_event
            placing_oco = self._placing_oco

        if qty == 0.0 or side is None:
            logger.info("[Exec] force_close_all: no open position to close")
            return

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — force_close_all: resetting position state")
            async with self._position_lock:
                sig_id = self._open_signal_id
                ep     = self._open_entry_price
                et     = self._open_entry_time
                sl     = self._open_sl_price
                self._reset_open_position()
            self._record_outcome(sig_id, side or "", ep, ep, qty, et, sl)
            if event:
                event.set()
            return

        # Live: if bracket is still being placed, delegate to _place_oco (S1 fix).
        if placing_oco:
            async with self._position_lock:
                self._cancel_oco_on_placement = True
            logger.warning("[Exec] force_close_all: bracket in-flight — deferred cancel scheduled")
            return

        await self._cancel_bracket_orders()

        async with self._position_lock:
            self._emergency_close_in_progress = True
        closed, close_p = await self._emergency_close(qty, side, reason)

        async with self._position_lock:
            sig_id = self._open_signal_id
            ep     = self._open_entry_price
            et     = self._open_entry_time
            sl     = self._open_sl_price
            self._reset_open_position()

        if closed:
            self._record_outcome(sig_id, side or "", ep, close_p, qty, et, sl)
            if event:
                event.set()
        elif not closed:
            logger.critical(
                "[Exec] force_close_all: emergency close unfilled — manual intervention required"
            )

    async def close_orphan_position(self, qty: float, side: str) -> None:
        """Close a position detected on the exchange that local state has no record of.

        Unlike force_close_all, does NOT set accepting_new_signals=False — the engine
        should resume normal operation after cleaning up the orphan."""
        logger.critical("[Exec] Closing orphan position — %s %.6f (no local state)", side, qty)
        closed, close_p = await self._emergency_close(qty, side, reason="ORPHAN_POSITION")
        if closed:
            logger.critical("[Exec] Orphan position closed @ %.2f — trading can resume", close_p)
        else:
            logger.critical(
                "[Exec] Orphan position close FAILED — position still open; "
                "accepting_new_signals blocked until resolved"
            )
            self.accepting_new_signals = False
