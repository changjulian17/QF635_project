import asyncio
import logging

from config import settings
from models import PortfolioState
from websocket_consumer import BinanceWebSocketConsumer
from pattern_detector import PatternDetector
from risk_engine import RiskEngine
from order_manager import OrderManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STARTING_EQUITY = 10_000.0


async def main() -> None:
    logger.info("=== CryptoSentinel Starting ===")

    portfolio = PortfolioState(
        equity=STARTING_EQUITY,
        starting_equity=STARTING_EQUITY,
        peak_equity=STARTING_EQUITY,
    )

    raw_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    candle_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    signal_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    order_queue: asyncio.Queue = asyncio.Queue(maxsize=50)

    ws_consumer = BinanceWebSocketConsumer(raw_queue, candle_queue)
    detector = PatternDetector(candle_queue, signal_queue)
    risk = RiskEngine(signal_queue, order_queue, portfolio)
    executor = OrderManager(order_queue, portfolio)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(ws_consumer.start(), name="ws_consumer")
        tg.create_task(detector.run(), name="pattern_detector")
        tg.create_task(risk.run(), name="risk_engine")
        tg.create_task(executor.start(), name="order_manager")


if __name__ == "__main__":
    asyncio.run(main())
