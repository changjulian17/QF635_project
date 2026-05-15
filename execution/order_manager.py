"""
Order Manager — IOC aggressive limit execution with slippage tracking.

Rule 10: Never use market orders. Use IOC limit at best_ask + spread×0.5
(LONG) or best_bid - spread×0.5 (SHORT). If the IOC expires unfilled,
the signal is stale — log and discard; do NOT retry.
"""

import asyncio
import logging

from binance import AsyncClient

from config import settings
from models import Direction, OrderRequest, PortfolioState

logger = logging.getLogger(__name__)


class OrderManager:

    def __init__(
        self,
        order_queue: asyncio.Queue,
        portfolio: PortfolioState,
        killswitch=None,
        fill_queue: asyncio.Queue | None = None,
        telemetry=None,
    ) -> None:
        self._order_queue      = order_queue
        self.portfolio         = portfolio
        self._killswitch       = killswitch
        self._fill_queue       = fill_queue
        self._telemetry        = telemetry
        self._client: AsyncClient | None = None
        self.accepting_new_signals: bool = True

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            self._client = await AsyncClient.create(
                api_key    = settings.BINANCE_API_KEY,
                api_secret = settings.BINANCE_API_SECRET,
                testnet    = settings.BINANCE_TESTNET,
            )
            logger.info("[Exec] Binance AsyncClient connected (testnet).")
        except Exception as exc:
            logger.error("[Exec] Cannot connect to Binance testnet: %s — running in drain mode.", exc)
            await self._drain_loop()
            return
        await self._order_loop()

    async def _drain_loop(self) -> None:
        """Discard orders when Binance is unreachable. Prevents queue backpressure."""
        while True:
            await self._order_queue.get()

    async def _order_loop(self) -> None:
        while True:
            req: OrderRequest = await self._order_queue.get()
            if not self.accepting_new_signals:
                logger.info("[Exec] Shutdown in progress — discarding order.")
                continue
            if not req.approved:
                logger.debug("[Exec] Skipping rejected order: %s", req.rejection_reason)
                continue
            await self._submit(req)

    # ── Submission ────────────────────────────────────────────────────────────

    async def _submit(self, req: OrderRequest) -> None:
        sig  = req.signal
        side = "BUY" if sig.direction == Direction.LONG else "SELL"

        if settings.DRY_RUN:
            logger.info(
                "[Exec] DRY RUN — would place %s IOC_LIMIT %s %s "
                "| entry=%.2f SL=%.2f TP=%.2f",
                side, req.quantity, settings.SYMBOL,
                sig.entry_price, sig.stop_loss, sig.take_profit,
            )
            return

        try:
            best_bid, best_ask = await self._get_best_prices()
            resp = await self._submit_aggressive_limit(req, side, best_bid, best_ask)
            if resp is None:
                return   # IOC expired unfilled — signal stale, do not retry

            fill_price = float(resp.get("price") or resp.get("fills", [{}])[0].get("price", sig.entry_price))
            logger.info("[Exec] Order filled: %s %s qty=%s fill=%.2f", resp.get("orderId"), side, req.quantity, fill_price)

            # KS-3 slippage tracking
            if self._killswitch is not None:
                self._killswitch.record_slippage(sig.entry_price, fill_price, sig.direction.name)

            # Notify fill queue for portfolio + telemetry update
            if self._fill_queue is not None:
                await self._fill_queue.put({
                    "signal_id":   getattr(req, "signal_id", None),
                    "side":        side,
                    "qty":         req.quantity,
                    "fill_price":  fill_price,
                    "entry_price": sig.entry_price,
                })

            await self._place_oco(sig, req.quantity, side)

        except Exception as exc:
            logger.error("[Exec] Order failed: %s", exc, exc_info=True)

    async def _submit_aggressive_limit(
        self,
        req: OrderRequest,
        side: str,
        best_bid: float,
        best_ask: float,
    ) -> dict | None:
        """
        Place an IOC aggressive limit order.
          LONG:  limit_price = best_ask + spread × 0.5
          SHORT: limit_price = best_bid - spread × 0.5
        Returns the exchange response dict, or None if the IOC expired unfilled.
        """
        spread = best_ask - best_bid
        if side == "BUY":
            limit_price = round(best_ask + spread * 0.5, 2)
        else:
            limit_price = round(best_bid - spread * 0.5, 2)

        resp = await self._client.create_order(
            symbol        = settings.SYMBOL,
            side          = side,
            type          = "LIMIT",
            timeInForce   = "IOC",
            quantity      = req.quantity,
            price         = str(limit_price),
        )

        status = resp.get("status", "")
        if status in ("EXPIRED", "CANCELED") or float(resp.get("executedQty", 0)) == 0:
            logger.warning(
                "[Exec] IOC expired unfilled — signal is stale. orderId=%s",
                resp.get("orderId"),
            )
            return None

        return resp

    async def _get_best_prices(self) -> tuple[float, float]:
        book = await self._client.get_order_book(symbol=settings.SYMBOL, limit=5)
        best_bid = float(book["bids"][0][0])
        best_ask = float(book["asks"][0][0])
        return best_bid, best_ask

    async def _place_oco(self, sig, qty: float, entry_side: str) -> None:
        exit_side = "SELL" if entry_side == "BUY" else "BUY"
        try:
            await self._client.create_oco_order(
                symbol              = settings.SYMBOL,
                side                = exit_side,
                quantity            = qty,
                price               = str(round(sig.take_profit, 2)),
                stopPrice           = str(round(sig.stop_loss, 2)),
                stopLimitPrice      = str(round(
                    sig.stop_loss * (0.999 if exit_side == "SELL" else 1.001), 2
                )),
                stopLimitTimeInForce = "GTC",
            )
            logger.info("[Exec] OCO placed SL=%.2f TP=%.2f", sig.stop_loss, sig.take_profit)
        except Exception as exc:
            logger.error("[Exec] OCO failed: %s", exc, exc_info=True)

    # ── Gate 6 hook ───────────────────────────────────────────────────────────

    async def handle_protection_wall_removed(self, position_side: str) -> None:
        """Called by PersistenceMonitor when the protection wall disappears."""
        logger.warning(
            "[Exec] Gate 6 alert: protection wall removed for %s position — "
            "monitoring for early exit opportunity.", position_side,
        )
