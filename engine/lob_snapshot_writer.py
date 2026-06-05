"""
LOB snapshot writer coroutine — extracted from main.py for testability.

Polls LocalOrderBook at ~1 Hz and writes snapshots to the lob_snapshots
table so the dashboard /lob page has live data. OBI is computed using
settings.LOB_OBI_DEPTH levels to match the engine's own OBI computation.

When a RealtimeHub is supplied, each snapshot is also broadcast to subscribed
WebSocket clients so the dashboard can stream updates instead of polling the DB.
"""
import asyncio
import logging

from config import settings
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from engine.db_writer import DBWriter
from engine.realtime_hub import RealtimeHub
from models import LOBSnapshot

logger = logging.getLogger(__name__)

# Bound the WebSocket payload: only push levels within this band of mid, capped
# at this many per side. LOB_DEPTH is 1000, far more than the dashboard renders.
_PUSH_PRICE_BAND = 2000.0
_PUSH_MAX_LEVELS = 400


def _trim_levels(levels: list, mid: float) -> list[list[float]]:
    """Return [[price, qty], ...] for levels within the push band, capped in count.

    ``levels`` is ordered best-first (closest to mid), so a simple prefix slice
    keeps the most relevant liquidity.
    """
    out: list[list[float]] = []
    for level in levels[:_PUSH_MAX_LEVELS]:
        if abs(level.price - mid) > _PUSH_PRICE_BAND:
            break
        out.append([level.price, level.qty])
    return out


async def lob_snapshot_writer(
    lob_engine: LocalOrderBook,
    cvd_calculator: CVDCalculator,
    db_writer: DBWriter,
    interval: float = 1.0,
    hub: RealtimeHub | None = None,
) -> None:
    """Write LOB snapshots to DB at ~1 Hz. Tolerates transient errors without crashing.

    If ``hub`` is provided, also broadcast each snapshot to subscribed clients.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            if lob_engine.lob_status != "SYNCED":
                continue
            snapshot: LOBSnapshot | None = await lob_engine.get_snapshot(depth=settings.LOB_DEPTH)
            if snapshot is None:
                continue
            # Use LOB_OBI_DEPTH levels — consistent with engine OBI computation
            obi_bids = snapshot.bids[:settings.LOB_OBI_DEPTH]
            obi_asks = snapshot.asks[:settings.LOB_OBI_DEPTH]
            total_bid = sum(l.qty for l in obi_bids)
            total_ask = sum(l.qty for l in obi_asks)
            obi = (total_bid - total_ask) / (total_bid + total_ask) if (total_bid + total_ask) > 0 else 0.0
            mid = (snapshot.bids[0].price + snapshot.asks[0].price) / 2 if snapshot.bids and snapshot.asks else 0.0
            spread = (snapshot.asks[0].price - snapshot.bids[0].price) if snapshot.bids and snapshot.asks else 0.0
            cvd_delta = cvd_calculator.get_cvd_delta()
            await db_writer.write_lob_snapshot(snapshot, obi, spread, mid, cvd_delta)

            if hub is not None:
                payload = {
                    "type": "snapshot",
                    "ts": snapshot.timestamp.isoformat(),
                    "mid_price": mid,
                    "spread": spread,
                    "obi": obi,
                    "cvd_delta": cvd_delta,
                    "bid_levels": _trim_levels(snapshot.bids, mid),
                    "ask_levels": _trim_levels(snapshot.asks, mid),
                }
                await hub.broadcast(payload)
        except Exception:
            logger.exception("[LOBSnapshot] Transient error — continuing")
