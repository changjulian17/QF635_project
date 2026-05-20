"""
LOB snapshot writer coroutine — extracted from main.py for testability.

Polls LocalOrderBook at ~1 Hz and writes snapshots to the lob_snapshots
table so the dashboard /lob page has live data. OBI is computed using
settings.LOB_OBI_DEPTH levels to match the engine's own OBI computation.
"""
import asyncio
import logging

from config import settings
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from engine.db_writer import DBWriter
from models import LOBSnapshot

logger = logging.getLogger(__name__)


async def lob_snapshot_writer(
    lob_engine: LocalOrderBook,
    cvd_calculator: CVDCalculator,
    db_writer: DBWriter,
    interval: float = 1.0,
) -> None:
    """Write LOB snapshots to DB at ~1 Hz. Tolerates transient errors without crashing."""
    while True:
        await asyncio.sleep(interval)
        try:
            if lob_engine.lob_status != "SYNCED":
                continue
            snapshot: LOBSnapshot | None = await lob_engine.get_snapshot()
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
            await db_writer.write_lob_snapshot(snapshot, obi, spread, mid, cvd_calculator.get_cvd_delta())
        except Exception:
            logger.exception("[LOBSnapshot] Transient error — continuing")
