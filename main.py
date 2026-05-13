import asyncio
import logging
from collections import deque

from config import settings
from lob_engine import LocalOrderBook
from microstructure_engine import MicrostructureEngine
from models import PortfolioState
from order_manager import OrderManager
from pattern_detector import PatternDetector
from risk_engine import RiskEngine
from websocket_consumer import BinanceWebSocketConsumer

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

    # Queues
    candle_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    trade_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    depth_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    signal_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    order_queue: asyncio.Queue = asyncio.Queue(maxsize=50)

    # Shared in-memory metrics store (newest bar first); also consumed by future scalper
    metrics_store: deque = deque(maxlen=settings.LOB_HISTORY)

    # Components
    ws_consumer = BinanceWebSocketConsumer(candle_queue, trade_queue, depth_queue)
    lob = LocalOrderBook()
    ms_engine = MicrostructureEngine(lob, trade_queue, depth_queue, metrics_store)
    detector = PatternDetector(candle_queue, signal_queue)
    risk = RiskEngine(signal_queue, order_queue, portfolio)
    executor = OrderManager(order_queue, portfolio)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(ws_consumer.start(), name="ws_consumer")
        tg.create_task(ms_engine.run(), name="microstructure_engine")
        tg.create_task(detector.run(), name="pattern_detector")
        tg.create_task(risk.run(), name="risk_engine")
        tg.create_task(executor.start(), name="order_manager")


if __name__ == "__main__":
    asyncio.run(main())
