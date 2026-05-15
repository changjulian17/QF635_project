import asyncio
import logging

from binance import AsyncClient

from config import settings
from models import Direction, OrderRequest, PortfolioState

logger = logging.getLogger(__name__)


class OrderManager:
    def __init__(self, order_queue: asyncio.Queue, portfolio: PortfolioState) -> None:
        self._order_queue = order_queue
        self.portfolio = portfolio
        self._client: AsyncClient | None = None

    async def start(self) -> None:
        try:
            self._client = await AsyncClient.create(
                api_key=settings.BINANCE_API_KEY,
                api_secret=settings.BINANCE_API_SECRET,
                testnet=settings.BINANCE_TESTNET,
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
            if not req.approved:
                logger.debug(f"[Exec] Skipping rejected order: {req.rejection_reason}")
                continue
            await self._submit(req)

    async def _submit(self, req: OrderRequest) -> None:
        sig = req.signal
        side = "BUY" if sig.direction == Direction.LONG else "SELL"

        if settings.DRY_RUN:
            logger.info(
                f"[Exec] DRY RUN — would place {side} MARKET {req.quantity} {settings.SYMBOL} "
                f"| entry={sig.entry_price:.2f} SL={sig.stop_loss:.2f} TP={sig.take_profit:.2f}"
            )
            return

        try:
            resp = await self._client.create_order(
                symbol=settings.SYMBOL,
                side=side,
                type="MARKET",
                quantity=req.quantity,
            )
            logger.info(f"[Exec] Order submitted: {resp.get('orderId')} {side} {req.quantity}")

            await self._place_oco(sig, req.quantity, side)

        except Exception as exc:
            logger.error(f"[Exec] Order failed: {exc}", exc_info=True)

    async def _place_oco(self, sig, qty: float, entry_side: str) -> None:
        exit_side = "SELL" if entry_side == "BUY" else "BUY"
        try:
            await self._client.create_oco_order(
                symbol=settings.SYMBOL,
                side=exit_side,
                quantity=qty,
                price=str(round(sig.take_profit, 2)),
                stopPrice=str(round(sig.stop_loss, 2)),
                stopLimitPrice=str(round(sig.stop_loss * (0.999 if exit_side == "SELL" else 1.001), 2)),
                stopLimitTimeInForce="GTC",
            )
            logger.info(f"[Exec] OCO placed SL={sig.stop_loss:.2f} TP={sig.take_profit:.2f}")
        except Exception as exc:
            logger.error(f"[Exec] OCO failed: {exc}", exc_info=True)
