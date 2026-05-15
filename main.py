"""
CryptoSentinel — asyncio orchestrator (v3.0 architecture).

Startup sequence (master arch §9):
  1. Initialise all components
  2. Connect to Binance testnet AsyncClient
  3. Reconcile on startup — BEFORE starting any coroutines
  4. Register SIGTERM/SIGINT handlers
  5. LOB warm-up guard (0.5s)
  6. Start TaskGroup with all coroutines
"""

import asyncio
import logging
import signal
from collections import deque
from datetime import datetime, timedelta, timezone

from binance import AsyncClient

from config import settings
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from core.lob_recorder import LOBRecorder
from core.pattern_detector import PatternDetector
from core.signal_telemetry import SignalTelemetry
from core.startup_reconciler import _write_event, reconcile_on_startup
from core.ws_consumer import BinanceWebSocketConsumer
from engine.db_writer import DBWriter, init_db
from execution.order_manager import OrderManager
from models import PortfolioState, SharedState
from risk.budget import DailyBudget
from risk.engine import RiskEngine
from risk.killswitch import GlobalKillswitch
from strategy.executor import RuleBasedScorer, StrategyExecutor
from strategy.features import FeatureComputer
from strategy.microstructure import MicrostructureDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STARTING_EQUITY = 10_000.0


# ── Utility coroutines ────────────────────────────────────────────────────────

async def _drain(queue: asyncio.Queue) -> None:
    """Consume and discard messages so the queue never backs up."""
    while True:
        await queue.get()


async def _depth_fanout(
    source: asyncio.Queue,
    lob_queue: asyncio.Queue,
    micro_queue: asyncio.Queue,
) -> None:
    """Fan out depth snapshots to both the LOB state machine and the wall detector."""
    while True:
        msg = await source.get()
        await lob_queue.put(msg)
        await micro_queue.put(msg)


async def _lob_sync(lob: LocalOrderBook, queue: asyncio.Queue) -> None:
    """Drive the LOB state machine so SharedState.lob_status stays current."""
    while True:
        msg = await queue.get()
        lob.apply_snapshot(msg)


async def _midnight_reset_loop(
    risk_engine: RiskEngine,
    cvd_calculator: CVDCalculator,
) -> None:
    """Sleep until next UTC midnight + 5 s, then reset daily counters."""
    while True:
        now = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=5, microsecond=0
        )
        sleep_secs = (next_midnight - now).total_seconds()
        logger.info("[Main] Midnight reset in %.0f s.", sleep_secs)
        await asyncio.sleep(sleep_secs)
        risk_engine.reset_for_new_session()
        cvd_calculator.reset_daily()
        logger.info("[Main] Midnight reset complete — daily counters cleared.")


async def _shutdown_watchdog(
    shutdown_event: asyncio.Event,
    order_manager: OrderManager,
    signal_telemetry: SignalTelemetry,
    portfolio: PortfolioState,
    db_path: str,
) -> None:
    """Wait for the shutdown signal, then flush and log before exiting."""
    await shutdown_event.wait()
    logger.info("[Main] Shutdown signal received — beginning graceful shutdown.")

    order_manager.accepting_new_signals = False

    # Flush pending telemetry records immediately
    await signal_telemetry._flush()

    # Log any open positions (OCO bracket protects them; do NOT close manually)
    if portfolio.positions:
        for pid, pos in portfolio.positions.items():
            logger.warning("[Shutdown] Open position retained: id=%s %s", pid, pos)
    else:
        logger.info("[Shutdown] No open positions.")

    # Persist shutdown event
    _write_event(db_path, "GRACEFUL_SHUTDOWN", {
        "equity": portfolio.equity,
        "daily_pnl": portfolio.daily_pnl,
        "open_positions": len(portfolio.positions),
    })

    logger.info("[Main] Graceful shutdown complete.")
    raise SystemExit(0)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("=== CryptoSentinel v3.0 Starting ===")
    init_db()

    # ── 1. Initialise shared state and portfolio ──────────────────────────────

    shared_state = SharedState()
    portfolio = PortfolioState(
        equity=STARTING_EQUITY,
        starting_equity=STARTING_EQUITY,
        peak_equity=STARTING_EQUITY,
    )

    # ── 2. Queues ─────────────────────────────────────────────────────────────

    # ws_consumer outputs
    candle_queue:       asyncio.Queue = asyncio.Queue(maxsize=200)
    candle_db_queue:    asyncio.Queue = asyncio.Queue(maxsize=200)
    trade_queue:        asyncio.Queue = asyncio.Queue(maxsize=5000)
    _raw_depth_queue:   asyncio.Queue = asyncio.Queue(maxsize=5000)

    # Depth fan-out targets
    lob_depth_queue:    asyncio.Queue = asyncio.Queue(maxsize=5000)
    micro_depth_queue:  asyncio.Queue = asyncio.Queue(maxsize=5000)

    # Pattern / risk path (existing wedge-based signals)
    pattern_signal_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    signal_db_queue:      asyncio.Queue = asyncio.Queue(maxsize=50)
    order_queue:          asyncio.Queue = asyncio.Queue(maxsize=50)

    # New wall-based signal path
    micro_signal_queue:   asyncio.Queue = asyncio.Queue(maxsize=100)
    micro_approved_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    telemetry_queue:      asyncio.Queue = asyncio.Queue(maxsize=500)
    fill_queue:           asyncio.Queue = asyncio.Queue(maxsize=200)

    # Legacy (DBWriter expects ms_bar_queue; nothing writes to it in Phase 1M)
    ms_bar_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)

    # ── 3. Instantiate components ─────────────────────────────────────────────

    lob            = LocalOrderBook(shared_state=shared_state)
    cvd_calculator = CVDCalculator()
    feature_computer = FeatureComputer()
    budget         = DailyBudget.from_equity(STARTING_EQUITY)
    killswitch     = GlobalKillswitch(dov=STARTING_EQUITY)
    rule_scorer    = RuleBasedScorer()

    ws_consumer = BinanceWebSocketConsumer(
        candle_queue,
        candle_db_queue,
        trade_queue,
        _raw_depth_queue,
        shared_state=shared_state,
    )
    lob_recorder     = LOBRecorder()
    micro_detector   = MicrostructureDetector(
        micro_depth_queue, trade_queue, micro_signal_queue, cvd_calculator
    )
    strategy_executor = StrategyExecutor(
        micro_signal_queue,
        micro_approved_queue,
        telemetry_queue,
        feature_computer,
        shared_state,
        budget,
        rule_scorer,
    )
    signal_telemetry = SignalTelemetry(telemetry_queue)
    pattern_detector = PatternDetector(candle_queue, pattern_signal_queue, signal_db_queue)
    risk             = RiskEngine(pattern_signal_queue, order_queue, portfolio)
    order_manager    = OrderManager(
        order_queue,
        portfolio,
        killswitch=killswitch,
        fill_queue=fill_queue,
    )
    db_writer = DBWriter(candle_db_queue, signal_db_queue, portfolio, ms_bar_queue)

    # ── 4. Connect Binance testnet client for reconciliation ──────────────────

    client = None
    try:
        client = await AsyncClient.create(
            api_key    = settings.BINANCE_API_KEY,
            api_secret = settings.BINANCE_API_SECRET,
            testnet    = settings.BINANCE_TESTNET,
        )
        logger.info("[Main] Binance AsyncClient connected.")
    except Exception as exc:
        logger.warning("[Main] Cannot connect to Binance (%s) — running without reconciliation.", exc)

    # ── 5. Startup reconciliation (before any coroutines start) ───────────────

    await reconcile_on_startup(
        client    = client,
        portfolio = portfolio,
        risk_engine = risk,
        db_path   = settings.REGISTRY_DB,
        symbol    = settings.SYMBOL,
    )

    # ── 6. Signal handlers ────────────────────────────────────────────────────

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    # ── 7. LOB warm-up guard ──────────────────────────────────────────────────

    await asyncio.sleep(0.5)

    logger.info("[Main] Starting all coroutines.")

    # ── 8. Run all coroutines ─────────────────────────────────────────────────

    try:
        async with asyncio.TaskGroup() as tg:
            # Data ingestion
            tg.create_task(lob_recorder.start(),    name="lob_recorder")
            tg.create_task(ws_consumer.start(),     name="ws_consumer")

            # LOB state machine
            tg.create_task(
                _depth_fanout(_raw_depth_queue, lob_depth_queue, micro_depth_queue),
                name="depth_fanout",
            )
            tg.create_task(_lob_sync(lob, lob_depth_queue), name="lob_sync")

            # Wall-based signal pipeline
            tg.create_task(micro_detector.run(),      name="micro_detector")
            tg.create_task(strategy_executor.run(),   name="strategy_executor")
            tg.create_task(signal_telemetry.run(),    name="signal_telemetry")

            # Pattern-based signal pipeline (legacy wedge path)
            tg.create_task(pattern_detector.run(),    name="pattern_detector")
            tg.create_task(risk.run(),                name="risk_engine")
            tg.create_task(order_manager.start(),     name="order_manager")

            # Persistence
            tg.create_task(db_writer.run(),           name="db_writer")

            # Drain queues that have no active consumer in Phase 1M
            tg.create_task(_drain(micro_approved_queue), name="drain_micro_approved")
            tg.create_task(_drain(fill_queue),           name="drain_fill")

            # Session management
            tg.create_task(
                _midnight_reset_loop(risk, cvd_calculator),
                name="midnight_reset",
            )
            tg.create_task(
                _shutdown_watchdog(
                    shutdown_event,
                    order_manager,
                    signal_telemetry,
                    portfolio,
                    settings.REGISTRY_DB,
                ),
                name="shutdown_watchdog",
            )

    except* SystemExit:
        logger.info("[Main] Exited via graceful shutdown.")
    except* (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("[Main] Interrupted.")
    finally:
        if client is not None:
            try:
                await client.close_connection()
            except Exception:
                pass
        logger.info("=== CryptoSentinel stopped ===")


if __name__ == "__main__":
    asyncio.run(main())
