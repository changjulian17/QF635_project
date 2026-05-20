"""
CryptoSentinel — asyncio orchestrator.

Startup sequence (master arch §9):
  1. Initialise all components
  2. Connect to Binance testnet (AsyncClient)
  3. Reconcile on startup — BEFORE starting any coroutines
  4. Register SIGTERM/SIGINT handlers
  5. LOB warm-up guard (0.5 s)
  6. Start TaskGroup with all coroutines:
       lob_recorder, ws_consumer, depth_fanout, lob_engine, micro_detector,
       pattern_detector, strategy_executor, risk_engine, order_manager,
       signal_telemetry, midnight_reset_loop, db_writer, fill_processor,
       portfolio_mtm_loop
"""
import asyncio
import logging
import signal
from datetime import datetime, timedelta

from aiohttp import web
from binance import AsyncClient

from config import settings
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from core.lob_recorder import LOBRecorder
from core.pattern_detector import PatternDetector
from core.signal_telemetry import SignalTelemetry
from core.startup_reconciler import reconcile_on_startup
from core.ws_consumer import BinanceWebSocketConsumer
from engine.db_writer import DBWriter, init_db
from execution.order_manager import OrderManager
from models import FillDetail, LOBSnapshot, PortfolioState, SharedState
from risk.budget import DailyBudget
from risk.engine import RiskEngine
from risk.killswitch import GlobalKillswitch
from risk.pyramid import PyramidController
from strategy.executor import StrategyExecutor
from strategy.features import FeatureComputer
from strategy.microstructure import MicrostructureDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STARTING_EQUITY = 10_000.0
SYMBOL = "BTCUSDT"


# ── Shutdown handler ──────────────────────────────────────────────────────────

async def shutdown_handler(
    order_manager: OrderManager,
    portfolio: PortfolioState,
    telemetry: SignalTelemetry,
) -> None:
    """Stop accepting new signals, flush telemetry, log open positions."""
    order_manager.accepting_new_signals = False
    await telemetry._flush_remaining()
    for pos in portfolio.positions:
        logger.info("[Shutdown] Open position (OCO active): %s", pos)
    telemetry.write_system_event(
        "GRACEFUL_SHUTDOWN",
        {"open_positions": len(portfolio.positions), "ts": datetime.utcnow().isoformat()},
    )
    logger.info("[Shutdown] Graceful shutdown complete")


# ── Killswitch emergency close ────────────────────────────────────────────────

async def emergency_close_all(
    order_manager: OrderManager,
    portfolio: PortfolioState,
    telemetry: SignalTelemetry,
    reason: str,
) -> None:
    """Hard stop: close all open positions, write system event, reject new signals."""
    logger.critical("[KS] emergency_close_all — reason=%s", reason)
    await order_manager.force_close_all(reason)
    telemetry.write_system_event(
        "KILLSWITCH_FIRED",
        {"reason": reason, "equity": portfolio.equity, "ts": datetime.utcnow().isoformat()},
    )


# ── Portfolio mark-to-market loop (KS-1) ─────────────────────────────────────

async def _portfolio_mtm_loop(
    killswitch: GlobalKillswitch,
    budget: DailyBudget,
    order_manager: OrderManager,
    portfolio: PortfolioState,
    telemetry: SignalTelemetry,
) -> None:
    """Check KS-1 budget breach every second. Exits after triggering emergency_close_all."""
    while True:
        await asyncio.sleep(1.0)
        if killswitch.is_active:
            return
        if killswitch.check_budget(budget.realised_pnl, budget.unrealised_pnl):
            await emergency_close_all(
                order_manager, portfolio, telemetry, "KILLSWITCH_BUDGET"
            )
            return


# ── Midnight reset ────────────────────────────────────────────────────────────

async def midnight_reset_loop(
    risk_engine: RiskEngine,
    cvd_calculator: CVDCalculator,
) -> None:
    """Sleep until next UTC midnight + 5 s, then reset all daily counters."""
    while True:
        now = datetime.utcnow()
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=5, microsecond=0
        )
        await asyncio.sleep((next_midnight - now).total_seconds())
        risk_engine.reset_for_new_session()
        cvd_calculator.reset_daily()
        logger.info("[Midnight] Session reset complete")


# ── TaskGroup helper coroutines ───────────────────────────────────────────────

async def _watch_shutdown(event: asyncio.Event) -> None:
    """Cancel the TaskGroup when SIGTERM/SIGINT fires."""
    await event.wait()
    raise asyncio.CancelledError("shutdown signal received")


_fanout_drop_count = 0


async def _depth_fanout(
    src: asyncio.Queue,
    lob_q: asyncio.Queue,
    ms_q: asyncio.Queue,
) -> None:
    """Fan out depth snapshots to both lob_engine and micro_detector."""
    global _fanout_drop_count
    while True:
        msg = await src.get()
        for q in (lob_q, ms_q):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                _fanout_drop_count += 1
                logger.warning("[Fanout] Depth snapshot dropped (queue full) — total drops=%d", _fanout_drop_count)


async def _run_lob_engine(lob: LocalOrderBook, depth_q: asyncio.Queue) -> None:
    """Apply each depth snapshot to the shared LocalOrderBook."""
    while True:
        msg = await depth_q.get()
        await lob.apply_snapshot(msg)


async def _drain_queue(queue: asyncio.Queue) -> None:
    """Consume and discard all messages so the queue never backs up."""
    while True:
        await queue.get()


async def _lob_snapshot_writer(
    lob_engine: LocalOrderBook,
    cvd_calculator: CVDCalculator,
    db_writer: DBWriter,
    interval: float = 1.0,
) -> None:
    """Write LOB snapshots to DB at ~1 Hz for dashboard /lob page."""
    while True:
        await asyncio.sleep(interval)
        if lob_engine.lob_status != "SYNCED":
            continue
        snapshot: LOBSnapshot | None = await lob_engine.get_snapshot()
        if snapshot is None:
            continue
        total_bid = sum(l.qty for l in snapshot.bids)
        total_ask = sum(l.qty for l in snapshot.asks)
        obi = (total_bid - total_ask) / (total_bid + total_ask) if (total_bid + total_ask) > 0 else 0.0
        mid = (snapshot.bids[0].price + snapshot.asks[0].price) / 2 if snapshot.bids and snapshot.asks else 0.0
        spread = (snapshot.asks[0].price - snapshot.bids[0].price) if snapshot.bids and snapshot.asks else 0.0
        await db_writer.write_lob_snapshot(snapshot, obi, spread, mid, cvd_calculator.get_cvd_delta())


async def _api_server(
    killswitch: GlobalKillswitch,
    portfolio: PortfolioState,
    shared_state: SharedState,
    order_manager: OrderManager,
    telemetry: SignalTelemetry,
    port: int,
) -> None:
    """aiohttp REST API co-resident with the engine TaskGroup (localhost only)."""

    async def _handle_health(request: web.Request) -> web.Response:
        return web.json_response({
            "lob_status": shared_state.lob_status,
            "heartbeat_status": shared_state.heartbeat_status,
            "risk_tier": portfolio.circuit_breaker.name,
            "killswitch_active": killswitch.is_active,
            "dry_run": settings.DRY_RUN,
        })

    async def _handle_portfolio(request: web.Request) -> web.Response:
        positions = [
            {
                "symbol": p.symbol,
                "side": p.side.name,
                "entry_price": p.entry_price,
                "quantity": p.quantity,
                "stop_loss": p.stop_loss,
                "take_profit": p.take_profit,
                "unrealised_pnl": p.unrealised_pnl,
            }
            for p in portfolio.positions
        ]
        return web.json_response({
            "equity": portfolio.equity,
            "daily_pnl": portfolio.daily_pnl,
            "drawdown_pct": portfolio.drawdown_pct,
            "consecutive_losses": portfolio.consecutive_losses,
            "positions": positions,
        })

    async def _handle_killswitch(request: web.Request) -> web.Response:
        if killswitch.is_active:
            return web.json_response({"fired": True, "already_active": True})
        asyncio.create_task(
            emergency_close_all(order_manager, portfolio, telemetry, "KILLSWITCH_UI")
        )
        return web.json_response({"fired": True})

    app = web.Application()
    app.router.add_get("/api/health", _handle_health)
    app.router.add_get("/api/portfolio", _handle_portfolio)
    app.router.add_post("/api/killswitch", _handle_killswitch)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    logger.info("[API] REST API listening on http://127.0.0.1:%d", port)
    await asyncio.Future()  # run until TaskGroup cancels this task


async def _process_fills(
    fill_q: asyncio.Queue,
    portfolio: PortfolioState,
) -> None:
    """Log IOC entry fills. Realised P&L is tracked in DailyBudget via budget.realised_pnl."""
    while True:
        fill: FillDetail = await fill_q.get()
        logger.info(
            "[Fill] signal=%s side=%s qty=%.6f price=%.2f slippage=%.1f bps",
            fill.signal_id, fill.side, fill.qty, fill.fill_price, fill.slippage_bps,
        )


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("=== CryptoSentinel Starting ===")

    # 1. Initialise all components ────────────────────────────────────────────
    init_db()

    portfolio = PortfolioState(
        equity=STARTING_EQUITY,
        starting_equity=STARTING_EQUITY,
        peak_equity=STARTING_EQUITY,
    )
    shared_state = SharedState(
        heartbeat_status="HEALTHY",
        last_delta_ms=0,
        lob_status="UNINITIALISED",
    )

    killswitch      = GlobalKillswitch(STARTING_EQUITY)
    budget          = DailyBudget.from_equity(STARTING_EQUITY)
    pyramid         = PyramidController()
    cvd_calculator  = CVDCalculator()
    lob_engine      = LocalOrderBook(shared_state=shared_state)
    feature_computer = FeatureComputer()

    # Queues ──────────────────────────────────────────────────────────────────
    # Legacy pattern pipeline: PatternDetector → RiskEngine (kept running; output drained)
    candle_queue:       asyncio.Queue = asyncio.Queue(maxsize=200)
    candle_db_queue:    asyncio.Queue = asyncio.Queue(maxsize=200)
    signal_queue:       asyncio.Queue = asyncio.Queue(maxsize=50)
    signal_db_queue:    asyncio.Queue = asyncio.Queue(maxsize=50)
    re_order_queue:     asyncio.Queue = asyncio.Queue(maxsize=50)
    ms_bar_queue:       asyncio.Queue = asyncio.Queue(maxsize=5000)

    # Micro pipeline: WSConsumer → depth fanout → LOB + MicroDetector → Executor → OrderManager
    trade_queue:        asyncio.Queue = asyncio.Queue(maxsize=5000)
    raw_depth_queue:    asyncio.Queue = asyncio.Queue(maxsize=5000)
    lob_depth_queue:    asyncio.Queue = asyncio.Queue(maxsize=5000)
    ms_depth_queue:     asyncio.Queue = asyncio.Queue(maxsize=5000)
    micro_signal_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    telemetry_queue:    asyncio.Queue = asyncio.Queue(maxsize=500)
    om_queue:           asyncio.Queue = asyncio.Queue(maxsize=50)
    fill_queue:         asyncio.Queue = asyncio.Queue(maxsize=200)

    # Killswitch callbacks — closures capture by reference; resolved at call-time
    # after all components are constructed and before the TaskGroup starts.
    async def _heartbeat_cb(status: str, delta_ms: float) -> None:
        if killswitch.is_active:
            return
        if killswitch.check_heartbeat(status, delta_ms):
            await emergency_close_all(order_manager, portfolio, telemetry, "KILLSWITCH_HEARTBEAT")

    async def _ks_fire_cb(reason: str) -> None:
        await emergency_close_all(order_manager, portfolio, telemetry, reason)

    # Components ──────────────────────────────────────────────────────────────
    ws_consumer = BinanceWebSocketConsumer(
        candle_queue=candle_queue,
        candle_db_queue=candle_db_queue,
        trade_queue=trade_queue,
        depth_queue=raw_depth_queue,
        shared_state=shared_state,
        heartbeat_cb=_heartbeat_cb,
    )
    lob_recorder = LOBRecorder()
    micro_detector = MicrostructureDetector(
        depth_queue=ms_depth_queue,
        trade_queue=trade_queue,
        signal_queue=micro_signal_queue,
        cvd_calculator=cvd_calculator,
    )
    pattern_detector = PatternDetector(
        candle_queue=candle_queue,
        signal_queue=signal_queue,
        signal_db_queue=signal_db_queue,
    )
    telemetry = SignalTelemetry(telemetry_queue=telemetry_queue)

    # OrderManager constructed first so its reference can be injected into StrategyExecutor
    order_manager = OrderManager(
        signal_queue=om_queue,
        fill_queue=fill_queue,
        killswitch=killswitch,
        equity_fn=lambda: portfolio.equity,
        ks_fire_cb=_ks_fire_cb,
    )
    strategy_executor = StrategyExecutor(
        micro_signal_queue=micro_signal_queue,
        signal_queue=om_queue,
        telemetry_queue=telemetry_queue,
        feature_computer=feature_computer,
        shared_state=shared_state,
        budget=budget,
        cvd_calculator=cvd_calculator,
        lob_engine=lob_engine,
        order_manager=order_manager,
    )
    risk_engine = RiskEngine(
        signal_queue=signal_queue,
        order_queue=re_order_queue,
        portfolio=portfolio,
        budget=budget,
        killswitch=killswitch,
        pyramid=pyramid,
    )
    db_writer = DBWriter(
        candle_queue=candle_db_queue,
        signal_queue=signal_db_queue,
        portfolio=portfolio,
        ms_bar_queue=ms_bar_queue,
    )

    # 2. Connect to Binance testnet ───────────────────────────────────────────
    client = None
    try:
        client = await AsyncClient.create(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            testnet=True,
        )
        logger.info("[Main] Connected to Binance testnet")
    except Exception as exc:
        logger.warning("[Main] Could not connect to Binance testnet: %s — proceeding without client", exc)

    # 3. Reconcile on startup — BEFORE starting any coroutines ────────────────
    if client is not None:
        await reconcile_on_startup(client, portfolio, risk_engine, symbol=SYMBOL)
    else:
        logger.info("[Main] Skipping reconciliation — no Binance client")

    # 4. Register SIGTERM/SIGINT handlers ─────────────────────────────────────
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, shutdown_event.set)
    loop.add_signal_handler(signal.SIGINT, shutdown_event.set)

    # 5. LOB warm-up guard ────────────────────────────────────────────────────
    await asyncio.sleep(0.5)

    # 6. Start TaskGroup with all coroutines ──────────────────────────────────
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_watch_shutdown(shutdown_event),                                   name="shutdown_watcher")
            tg.create_task(lob_recorder.start(),                                              name="lob_recorder")
            tg.create_task(ws_consumer.start(),                                               name="ws_consumer")
            tg.create_task(_depth_fanout(raw_depth_queue, lob_depth_queue, ms_depth_queue),  name="depth_fanout")
            tg.create_task(_run_lob_engine(lob_engine, lob_depth_queue),                     name="lob_engine")
            tg.create_task(micro_detector.run(),                                              name="micro_detector")
            tg.create_task(pattern_detector.run(),                                            name="pattern_detector")
            tg.create_task(strategy_executor.run(),                                           name="strategy_executor")
            tg.create_task(risk_engine.run(),                                                 name="risk_engine")
            tg.create_task(order_manager.start(),                                             name="order_manager")
            tg.create_task(telemetry.run(),                                                   name="signal_telemetry")
            tg.create_task(midnight_reset_loop(risk_engine, cvd_calculator),                  name="midnight_reset")
            tg.create_task(db_writer.run(),                                                   name="db_writer")
            tg.create_task(_drain_queue(re_order_queue),                                      name="re_order_drain")
            tg.create_task(_process_fills(fill_queue, portfolio),                             name="fill_processor")
            tg.create_task(
                _portfolio_mtm_loop(killswitch, budget, order_manager, portfolio, telemetry),
                name="portfolio_mtm_loop",
            )
            tg.create_task(
                _lob_snapshot_writer(lob_engine, cvd_calculator, db_writer),
                name="lob_snapshot_writer",
            )
            tg.create_task(
                _api_server(
                    killswitch, portfolio, shared_state,
                    order_manager, telemetry, settings.DASHBOARD_API_PORT,
                ),
                name="api_server",
            )
    except* asyncio.CancelledError:
        pass
    except* Exception as eg:
        logger.error("[Main] TaskGroup error(s): %s", eg.exceptions)
    finally:
        await shutdown_handler(order_manager, portfolio, telemetry)
        if client is not None:
            await client.close_connection()
        logger.info("=== CryptoSentinel Stopped ===")


if __name__ == "__main__":
    asyncio.run(main())
