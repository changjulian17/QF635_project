"""
Execution layer — IOC aggressive limit orders (Rule 10, master arch §9).

Entry orders are IOC limit orders priced to cross the spread:
  LONG  → limit = best_ask + (best_ask - best_bid) × 0.5
  SHORT → limit = best_bid - (best_ask - best_bid) × 0.5

If the order expires unfilled it is NOT retried — the signal is stale.
On fill: slippage is recorded to GlobalKillswitch (KS-3) and a FillDetail
is put on fill_queue for portfolio updates and telemetry outcome recording.
An OCO bracket (TP limit + SL stop-limit) is placed immediately after fill.

Gate 6 calls handle_protection_wall_removed() when the protection wall
disappears post-entry; the handler initiates an early IOC exit.
"""

import asyncio
import logging
from collections.abc import Callable

from binance import AsyncClient

from config import settings
from models import FillDetail, MicroOrderRequest, SharedState
from risk.killswitch import GlobalKillswitch

logger = logging.getLogger(__name__)

# Synthetic best-bid/ask used in DRY_RUN when no real book is fetched.
_DRY_RUN_BEST_BID = 95_000.0
_DRY_RUN_BEST_ASK = 95_010.0


def _weighted_avg_fill(resp: dict) -> float:
    """Weighted-average fill price from a Binance order response."""
    fills = resp.get("fills", [])
    if not fills:
        return float(resp.get("price", 0.0))
    total_qty = sum(float(f["qty"]) for f in fills)
    if total_qty == 0.0:
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
    ) -> None:
        self._signal_q    = signal_queue
        self.fill_queue   = fill_queue
        self._killswitch  = killswitch
        self._equity_fn   = equity_fn
        self._client: AsyncClient | None = None

        # Graceful-shutdown gate: set False before draining the queue.
        self.accepting_new_signals: bool = True

        # Open-position state — updated on fill; cleared after exit.
        self._open_position_side:         str | None          = None
        self._open_position_qty:          float               = 0.0
        self._open_oco_list_id:           str | None          = None
        self._open_position_closed_event: asyncio.Event | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN mode — skipping Binance connection.")
            await self._order_loop()
            return
        try:
            self._client = await AsyncClient.create(
                api_key    = settings.BINANCE_API_KEY,
                api_secret = settings.BINANCE_API_SECRET,
                testnet    = settings.BINANCE_TESTNET,
            )
            logger.info("[Exec] Binance AsyncClient connected (testnet).")
        except Exception as exc:
            logger.error(
                "[Exec] Cannot connect to Binance testnet: %s — drain mode.", exc
            )
            await self._drain_loop()
            return
        await self._order_loop()

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
        side = req.side  # "BUY" | "SELL"

        if settings.DRY_RUN:
            best_bid, best_ask = _DRY_RUN_BEST_BID, _DRY_RUN_BEST_ASK
        else:
            try:
                book = await self._client.get_order_book(
                    symbol=settings.SYMBOL, limit=5
                )
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                if not bids or not asks:
                    logger.warning(
                        "[Exec] Empty order book — skipping %s", req.signal_id[:8]
                    )
                    return
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
            except Exception as exc:
                logger.error(
                    "[Exec] Order book fetch failed: %s", exc, exc_info=True
                )
                return

        resp = await self._submit_aggressive_limit(req, side, best_bid, best_ask)
        if resp is None:
            return

        fill_price = _weighted_avg_fill(resp)
        fill_qty   = float(resp.get("executedQty", 0))
        await self._place_oco(req, fill_price, fill_qty, side)

    async def _submit_aggressive_limit(
        self,
        req: MicroOrderRequest,
        side: str,
        best_bid: float,
        best_ask: float,
    ) -> dict | None:
        """
        Place an IOC aggressive limit order. Returns the exchange response (or a
        synthetic DRY_RUN dict) on fill; returns None if the order expired unfilled.
        Side-effects on fill: fill_event set, FillDetail put to fill_queue,
        slippage recorded to killswitch, open-position state cached.
        """
        spread = best_ask - best_bid
        if side == "BUY":
            limit_price  = round(best_ask + spread * 0.5, 2)
            signal_price = best_ask
        else:
            limit_price  = round(best_bid - spread * 0.5, 2)
            signal_price = best_bid

        # Position sizing: qty = (equity × notional_hint) / sl_distance
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

        if settings.DRY_RUN:
            logger.info(
                "[Exec] DRY RUN — IOC %s LIMIT %.6f @ %.2f | signal_id=%s",
                side, qty, limit_price, req.signal_id[:8],
            )
            resp: dict = {
                "orderId":     "DRY_RUN",
                "status":      "FILLED",
                "executedQty": str(qty),
                "fills":       [{"price": str(limit_price), "qty": str(qty)}],
            }
        else:
            try:
                resp = await self._client.create_order(
                    symbol        = settings.SYMBOL,
                    side          = side,
                    type          = "LIMIT",
                    timeInForce   = "IOC",
                    quantity      = qty,
                    price         = str(limit_price),
                )
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
        direction  = req.micro_signal.direction  # "LONG" | "SHORT"
        self._killswitch.record_slippage(signal_price, fill_price, direction)

        slippage_bps = (
            (fill_price - signal_price) / signal_price * 10_000
            if direction == "LONG"
            else (signal_price - fill_price) / signal_price * 10_000
        )
        fill_qty = float(resp["executedQty"])

        await self.fill_queue.put(FillDetail(
            signal_id    = req.signal_id,
            side         = side,
            fill_price   = fill_price,
            qty          = fill_qty,
            limit_price  = limit_price,
            slippage_bps = slippage_bps,
            order_id     = str(resp.get("orderId", "DRY_RUN")),
        ))
        req.fill_event.set()

        self._open_position_side          = side
        self._open_position_qty           = fill_qty
        self._open_position_closed_event  = req.position_closed_event

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
        """Place TP limit + SL stop-limit OCO bracket after entry fill."""
        sl_price    = req.micro_signal.protection_wall.price
        sl_distance = abs(fill_price - sl_price)
        exit_side   = "SELL" if entry_side == "BUY" else "BUY"

        if entry_side == "BUY":
            tp_price  = fill_price + settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit  = round(sl_price * 0.999, 2)
        else:
            tp_price  = fill_price - settings.ATR_MULTIPLIER_TP * sl_distance
            sl_limit  = round(sl_price * 1.001, 2)

        if settings.DRY_RUN:
            logger.info(
                "[Exec] DRY RUN — OCO %s qty=%.6f TP=%.2f SL=%.2f",
                exit_side, fill_qty, tp_price, sl_price,
            )
            return

        try:
            oco_resp = await self._client.create_oco_order(
                symbol               = settings.SYMBOL,
                side                 = exit_side,
                quantity             = fill_qty,
                price                = str(round(tp_price, 2)),
                stopPrice            = str(round(sl_price, 2)),
                stopLimitPrice       = str(sl_limit),
                stopLimitTimeInForce = "GTC",
            )
            self._open_oco_list_id = str(oco_resp.get("orderListId", ""))
            logger.info(
                "[Exec] OCO placed — TP=%.2f SL=%.2f listId=%s",
                tp_price, sl_price, self._open_oco_list_id,
            )
        except Exception as exc:
            logger.error("[Exec] OCO failed: %s", exc, exc_info=True)

    # ── Gate 6 callback ───────────────────────────────────────────────────────

    async def handle_protection_wall_removed(self, position_side: str) -> None:
        """
        Called by PersistenceMonitor (Gate 6) when the protection wall is no
        longer in the book. Cancels the open OCO and closes the position with
        an aggressive IOC limit.
        """
        logger.warning(
            "[Exec] Gate6 alert — protection wall removed, initiating early exit "
            "(side=%s)", position_side,
        )

        if settings.DRY_RUN:
            logger.info("[Exec] DRY RUN — would cancel OCO and close position")
            if self._open_position_closed_event:
                self._open_position_closed_event.set()
            self._reset_open_position()
            return

        # Cancel the open OCO bracket
        if self._open_oco_list_id and self._client:
            try:
                await self._client.delete_oco_order(
                    symbol      = settings.SYMBOL,
                    orderListId = int(self._open_oco_list_id),
                )
                logger.info("[Exec] OCO %s cancelled", self._open_oco_list_id)
            except Exception as exc:
                logger.error("[Exec] OCO cancel failed: %s", exc, exc_info=True)

        # Close with an aggressive IOC limit
        if self._client and self._open_position_side and self._open_position_qty > 0:
            close_side = "SELL" if self._open_position_side == "BUY" else "BUY"
            try:
                book = await self._client.get_order_book(
                    symbol=settings.SYMBOL, limit=5
                )
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

                resp = await self._client.create_order(
                    symbol      = settings.SYMBOL,
                    side        = close_side,
                    type        = "LIMIT",
                    timeInForce = "IOC",
                    quantity    = self._open_position_qty,
                    price       = str(close_price),
                )
                logger.info(
                    "[Exec] Early exit order placed — orderId=%s", resp.get("orderId")
                )
            except Exception as exc:
                logger.error(
                    "[Exec] Early exit order failed: %s", exc, exc_info=True
                )

        if self._open_position_closed_event:
            self._open_position_closed_event.set()
        self._reset_open_position()

    def _reset_open_position(self) -> None:
        self._open_position_side         = None
        self._open_position_qty          = 0.0
        self._open_oco_list_id           = None
        self._open_position_closed_event = None
