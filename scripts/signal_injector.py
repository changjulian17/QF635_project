"""
Development-only signal injector.

Puts synthetic SWEEP_WITH_PROTECTION signals into micro_signal_queue after
LOB sync + feature warmup, so the full gate pipeline can be exercised without
waiting for a natural microstructure event.

Start via:  ./start_test.sh  (sets TEST_SIGNAL_INJECT=true)
Never used in paper trading, backtesting, or production.
"""
import asyncio
import logging
from datetime import datetime, timezone

from config import settings
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from models import MicroSignal, SharedState, WallState
from strategy.features import FeatureComputer


async def run(
    queue: asyncio.Queue,
    lob_engine: LocalOrderBook,
    feature_computer: FeatureComputer,
    shared_state: SharedState,
    cvd_calculator: CVDCalculator,
) -> None:
    _log = logging.getLogger("signal_injector")
    count = 0

    # Poll until LOB is synced and FeatureComputer has enough candles for RSI
    while True:
        fv = feature_computer.compute(cvd_calculator, shared_state)
        if shared_state.lob_status == "SYNCED" and fv is not None:
            break
        _log.info(
            "[Injector] Waiting for warmup (lob=%s fv_ready=%s)...",
            shared_state.lob_status, fv is not None,
        )
        await asyncio.sleep(5.0)

    _log.info(
        "[Injector] Warmup complete — injecting signals every %dms  (Ctrl-C to stop)",
        settings.TEST_INJECT_INTERVAL_MS,
    )

    while True:
        await asyncio.sleep(settings.TEST_INJECT_INTERVAL_MS / 1000.0)

        snap = await lob_engine.get_snapshot(depth=5)
        if snap is None or not snap.bids or not snap.asks:
            _log.warning("[Injector] LOB snapshot empty — skipping")
            continue

        mid       = (snap.bids[0].price + snap.asks[0].price) / 2.0
        now_ms    = int(datetime.now(timezone.utc).timestamp() * 1000)
        direction = "LONG" if count % 2 == 0 else "SHORT"

        if direction == "LONG":
            # Ask wall swept through (price broke above) + fresh bid support below
            consumed   = WallState(price=round(mid * 1.0010, 2), qty_initial=2.0, qty_current=0.1,
                                   first_seen_ts=now_ms - 2000, last_seen_ts=now_ms, side="ask", sigma=3.5)
            protection = WallState(price=round(mid * 0.9990, 2), qty_initial=1.5, qty_current=1.5,
                                   first_seen_ts=now_ms - 400, last_seen_ts=now_ms, side="bid", sigma=3.0)
            price_move = 0.0011
        else:
            # Bid wall swept through (price broke below) + fresh ask resistance above
            consumed   = WallState(price=round(mid * 0.9990, 2), qty_initial=2.0, qty_current=0.1,
                                   first_seen_ts=now_ms - 2000, last_seen_ts=now_ms, side="bid", sigma=3.5)
            protection = WallState(price=round(mid * 1.0010, 2), qty_initial=1.5, qty_current=1.5,
                                   first_seen_ts=now_ms - 400, last_seen_ts=now_ms, side="ask", sigma=3.0)
            price_move = -0.0011

        sig = MicroSignal(
            signal_type      = "SWEEP_WITH_PROTECTION",
            direction        = direction,
            timestamp_ms     = now_ms,
            consumed_wall    = consumed,
            protection_wall  = protection,
            prior_absorption = True,
            cvd_std          = 2.5,
            price_move_pct   = price_move,
            mid_price        = mid,
        )
        await queue.put(sig)
        count += 1
        _log.info(
            "[Injector] Signal #%d %s mid=%.2f | consumed=%s@%.2f protection=%s@%.2f",
            count, direction, mid,
            consumed.side, consumed.price, protection.side, protection.price,
        )
