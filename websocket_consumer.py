import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings
from models import AggTrade, Candle

logger = logging.getLogger(__name__)

# 24-hour hard reconnect (Binance enforces a 24 h connection lifetime)
_MAX_CONNECTION_SECONDS = 86_000  # reconnect 400 s before the 24 h limit


class BinanceWebSocketConsumer:
    STREAMS = [
        f"{settings.SYMBOL.lower()}@aggTrade",
        f"{settings.SYMBOL.lower()}@kline_{settings.CANDLE_INTERVAL}",
        f"{settings.SYMBOL.lower()}@depth@100ms",
    ]

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        trade_queue: asyncio.Queue,
        depth_queue: asyncio.Queue,
    ) -> None:
        self._candle_queue = candle_queue
        self._trade_queue = trade_queue
        self._depth_queue = depth_queue
        self._running = False
        self._reconnect_delay = 1.0
        self._max_delay = 60.0

    async def start(self) -> None:
        self._running = True
        await self._connect_loop()

    async def stop(self) -> None:
        self._running = False

    async def _connect_loop(self) -> None:
        stream_path = "/".join(self.STREAMS)
        uri = f"{settings.WS_BASE}/{stream_path}"
        attempt = 0

        while self._running:
            try:
                logger.info(f"[WS] Connecting (attempt {attempt + 1}): {uri}")
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info("[WS] Connected.")
                    self._reconnect_delay = 1.0
                    attempt = 0
                    await self._receive_loop(ws)

            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning(f"[WS] Connection closed: {exc}")
            except Exception as exc:
                logger.error(f"[WS] Unexpected error: {exc}", exc_info=True)

            if not self._running:
                break

            attempt += 1
            logger.info(f"[WS] Reconnecting in {self._reconnect_delay:.1f}s…")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_delay)

    async def _receive_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        conn_start = asyncio.get_event_loop().time()

        async for raw_msg in ws:
            if not self._running:
                break

            # Proactive 24-hour reconnect: close cleanly before Binance kills us
            if asyncio.get_event_loop().time() - conn_start > _MAX_CONNECTION_SECONDS:
                logger.info("[WS] Approaching 24 h limit — reconnecting proactively.")
                await ws.close()
                break

            try:
                msg = json.loads(raw_msg)
                event_type = msg.get("e")

                if event_type == "aggTrade":
                    trade = AggTrade(
                        timestamp=datetime.fromtimestamp(msg["T"] / 1000, tz=timezone.utc),
                        price=float(msg["p"]),
                        qty=float(msg["q"]),
                        is_buyer_maker=bool(msg["m"]),
                    )
                    await self._trade_queue.put(trade)

                elif event_type == "kline":
                    kline = msg["k"]
                    if kline.get("x"):
                        candle = Candle(
                            open_time=datetime.fromtimestamp(kline["t"] / 1000, tz=timezone.utc),
                            open=float(kline["o"]),
                            high=float(kline["h"]),
                            low=float(kline["l"]),
                            close=float(kline["c"]),
                            volume=float(kline["v"]),
                            is_closed=True,
                        )
                        await self._candle_queue.put(candle)
                        logger.debug(f"[WS] Closed candle: {candle}")

                elif event_type == "depthUpdate":
                    await self._depth_queue.put(msg)

            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning(f"[WS] Malformed message: {exc}")
