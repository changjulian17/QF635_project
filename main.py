"""
CryptoSentinel — asyncio orchestrator.

Startup sequence (master arch §9):
  1. Initialise all components
  2. Connect to Binance testnet (AsyncClient)
  3. Reconcile on startup — BEFORE starting any coroutines
  4. Register SIGTERM/SIGINT handlers
  5. LOB warm-up guard (0.5 s)
  6. Start TaskGroup with all coroutines:
       lob_recorder, ws_price_consumer, ws_lob_consumer, depth_fanout, lob_engine, micro_detector,
       feature_candle_loop, strategy_executor, order_manager,
       signal_telemetry, midnight_reset_loop, db_writer, fill_processor,
       portfolio_mtm_loop
"""
import asyncio
import logging
import os
import signal
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

from aiohttp import web
from binance import AsyncClient

from config import settings
from core.alerting import AlertDispatcher
from core.cvd import CVDCalculator
from core.lob_engine import LocalOrderBook
from core.lob_recorder import LOBRecorder
from core.signal_telemetry import SignalTelemetry
from core.startup_reconciler import reconcile_on_startup
from core.user_data_stream import UserDataStreamConsumer
from core.user_data_stream import UserDataStreamConsumer
from core.ws_consumer import BinanceWebSocketConsumer
from engine.db_writer import DBWriter, init_db
from engine.lob_snapshot_writer import lob_snapshot_writer as _lob_snapshot_writer
from engine.realtime_hub import RealtimeHub
from execution.order_manager import OrderManager
from models import Candle, FillDetail, PortfolioState, SharedState
from risk.budget import DailyBudget
from risk.engine import RiskEngine
from risk.killswitch import GlobalKillswitch
from strategy.executor import StrategyExecutor
from strategy.features import FeatureComputer
from strategy.microstructure import MicrostructureDetector
from strategy.registry import StrategyRegistry
from dashboard._db import DBOffline, fetch_pnl_by_pattern, fetch_session_stats

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_LOG_LEVEL  = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

logging.basicConfig(level=_LOG_LEVEL, format=_LOG_FORMAT)

os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
_file_handler = RotatingFileHandler(
    settings.LOG_FILE,
    maxBytes=settings.LOG_MAX_BYTES,
    backupCount=settings.LOG_BACKUP_COUNT,
)
_file_handler.setLevel(_LOG_LEVEL)
_file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
logging.getLogger().addHandler(_file_handler)
logging.getLogger("websockets").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

SYMBOL          = "BTCUSDT"
STARTING_EQUITY = settings.STARTING_EQUITY


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
        {"open_positions": len(portfolio.positions), "ts": datetime.now(timezone.utc).isoformat()},
    )
    logger.info("[Shutdown] Graceful shutdown complete")


# ── Killswitch emergency close ────────────────────────────────────────────────

async def emergency_close_all(
    order_manager: OrderManager,
    portfolio: PortfolioState,
    telemetry: SignalTelemetry,
    reason: str,
    alert_dispatcher: AlertDispatcher | None = None,
) -> None:
    """Hard stop: close all open positions, write system event, reject new signals."""
    logger.critical("[KS] emergency_close_all — reason=%s", reason)
    await order_manager.force_close_all(reason)
    telemetry.write_system_event(
        "KILLSWITCH_FIRED",
        {"reason": reason, "equity": portfolio.equity, "ts": datetime.now(timezone.utc).isoformat()},
    )
    if alert_dispatcher is not None:
        asyncio.create_task(alert_dispatcher.notify_killswitch(reason, portfolio.equity))


# ── Portfolio mark-to-market loop (KS-1) ─────────────────────────────────────

async def _portfolio_mtm_loop(
    killswitch: GlobalKillswitch,
    budget: DailyBudget,
    order_manager: OrderManager,
    portfolio: PortfolioState,
    telemetry: SignalTelemetry,
    risk_engine: RiskEngine,
    strategy_executor: StrategyExecutor,
    alert_dispatcher: AlertDispatcher,
    hub: RealtimeHub | None = None,
) -> None:
    """Check KS-1 budget breach, sync risk tier, and broadcast portfolio state each tick.

    The broadcast (when ``hub`` is provided) carries the same fields as
    ``/api/portfolio`` plus ``risk_tier`` and ``killswitch_active`` so /live can
    render entirely from this stream without an extra REST poll.
    """
    while True:
        await asyncio.sleep(0.25)
        if killswitch.is_active:
            return
        risk_engine.mark_unrealised(order_manager.get_unrealised_pnl())
        if killswitch.check_budget(budget.realised_pnl, budget.unrealised_pnl):
            await emergency_close_all(
                order_manager, portfolio, telemetry, "KILLSWITCH_BUDGET", alert_dispatcher
            )
            return
        new_tier = risk_engine.sync_tier()
        strategy_executor.set_risk_tier(new_tier)
        if hub is not None:
            payload = _build_portfolio_payload(
                portfolio, risk_tier=new_tier,
                killswitch_active=killswitch.is_active, order_manager=order_manager,
            )
            payload["type"] = "portfolio"
            payload["ts"]   = datetime.now(timezone.utc).isoformat()
            await hub.broadcast(payload)


# ── Midnight reset ────────────────────────────────────────────────────────────

async def midnight_reset_loop(risk_engine: RiskEngine, killswitch: GlobalKillswitch) -> None:
    """Sleep until next UTC midnight + 5 s, then reset daily risk counters."""
    while True:
        now = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=5, microsecond=0
        )
        await asyncio.sleep((next_midnight - now).total_seconds())
        risk_engine.reset_for_new_session()
        killswitch.reset_slippage_buffer()
        logger.info("[Midnight] Session reset complete")


# ── Orphan position watchdog ──────────────────────────────────────────────────

async def _orphan_watchdog(
    order_manager: OrderManager,
    killswitch: GlobalKillswitch,
    alert_dispatcher,
    interval_s: float = 30.0,
) -> None:
    """Poll the exchange every interval_s for positions not tracked locally."""
    while True:
        await asyncio.sleep(interval_s)
        if killswitch.is_active:
            return
        exch_qty, exch_side = await order_manager.get_exchange_position()
        local_exposed = order_manager.has_active_exposure()
        if exch_qty > 0 and not local_exposed:
            logger.critical(
                "[Orphan] Exchange has %.4f %s, local state clear — closing",
                exch_qty, exch_side,
            )
            await order_manager.close_orphan_position(qty=exch_qty, side=exch_side or "BUY")
            if alert_dispatcher is not None:
                asyncio.create_task(
                    alert_dispatcher.notify_killswitch("ORPHAN_POSITION_DETECTED", 0.0)
                )


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


async def _feature_candle_loop(
    candle_q: asyncio.Queue,
    feature_computer: FeatureComputer,
) -> None:
    """Feed closed candles into FeatureComputer without running the legacy pattern pipeline."""
    while True:
        candle: Candle = await candle_q.get()
        feature_computer.update_candle(candle)


async def _backfill_feature_candles(
    client: AsyncClient,
    feature_computer: FeatureComputer,
    limit: int = 30,
) -> None:
    """Pre-feed recent CLOSED klines so the FeatureComputer is warm at startup.

    Without this, the feature vector needs ~rsi_period closed candles of the
    configured TIMEFRAME to accumulate live (≈70 min at 5m), during which every
    signal fails Gate 2 with "feature vector not ready". Backfilling makes the
    strategy tradeable within seconds of launch instead.
    """
    try:
        klines = await client.get_klines(
            symbol=SYMBOL, interval=settings.TIMEFRAME, limit=limit
        )
    except Exception as exc:
        logger.warning("[Main] Feature candle backfill failed (%s) — cold warmup", exc)
        return
    closed = klines[:-1] if klines else []  # last kline is the in-progress candle
    for k in closed:
        feature_computer.update_candle(Candle(
            open_time=datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            open=float(k[1]), high=float(k[2]), low=float(k[3]),
            close=float(k[4]), volume=float(k[5]), is_closed=True,
        ))
    logger.info(
        "[Main] Backfilled %d closed %s candles — feature vector warm at startup",
        len(closed), settings.TIMEFRAME,
    )


def _build_portfolio_payload(
    portfolio: PortfolioState,
    risk_tier: str | None = None,
    killswitch_active: bool | None = None,
    order_manager=None,
) -> dict:
    """Serialise portfolio + risk state for /api/portfolio and the /ws/portfolio stream.

    Shared by the REST handler and the realtime broadcast so the two never drift.
    When ``order_manager`` is supplied and it reports an open dry-run position, that
    position replaces the live position list (DRY_RUN paper-trading view).
    """
    positions = [
        {
            "symbol":         p.symbol,
            "side":           p.side.name,
            "entry_price":    p.entry_price,
            "quantity":       p.quantity,
            "stop_loss":      p.stop_loss,
            "take_profit":    p.take_profit,
            "unrealised_pnl": p.unrealised_pnl,
        }
        for p in portfolio.positions
    ]
    if order_manager is not None:
        live_pos = order_manager.get_open_position()
        if live_pos is not None:
            positions = [live_pos]
    payload: dict = {
        "equity":             portfolio.equity,
        "usdt_balance":       portfolio.usdt_balance,
        "btc_balance":        portfolio.btc_balance,
        "btc_mtm":            round(portfolio.btc_balance * portfolio.btc_price, 2),
        "btc_price":          portfolio.btc_price,
        "daily_pnl":          portfolio.daily_pnl,
        "drawdown_pct":       portfolio.drawdown_pct,
        "consecutive_losses": portfolio.consecutive_losses,
        "num_trades":         portfolio.num_trades,
        "num_wins":           portfolio.num_wins,
        "num_fill_samples":   portfolio.num_fill_samples,
        "avg_slippage_bps":   portfolio.avg_slippage_bps,
        "budget_loss_pct":    portfolio.budget_loss_pct,
        "positions":          positions,
    }
    if risk_tier is not None:
        payload["risk_tier"] = risk_tier
    if killswitch_active is not None:
        payload["killswitch_active"] = killswitch_active
    return payload


async def _api_server(
    killswitch: GlobalKillswitch,
    portfolio: PortfolioState,
    shared_state: SharedState,
    order_manager: OrderManager,
    telemetry: SignalTelemetry,
    port: int,
    alert_dispatcher: AlertDispatcher | None = None,
    lob_hub: RealtimeHub | None = None,
    portfolio_hub: RealtimeHub | None = None,
    signal_hub: RealtimeHub | None = None,
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
        return web.json_response(
            _build_portfolio_payload(portfolio, order_manager=order_manager)
        )

    async def _handle_session(request: web.Request) -> web.Response:
        stats = await asyncio.to_thread(fetch_session_stats, 24)
        if isinstance(stats, DBOffline):
            return web.json_response({"error": "registry offline"}, status=503)
        pnl_by_pattern = await asyncio.to_thread(fetch_pnl_by_pattern, 24)
        total = stats.get("total_trades") or 0
        wins  = stats.get("wins") or 0
        return web.json_response({
            **{k: (v if v is not None else 0) for k, v in stats.items()},
            "win_rate":        wins / total if total > 0 else 0.0,
            "budget_loss_pct": portfolio.budget_loss_pct,
            "pnl_by_pattern":  pnl_by_pattern if not isinstance(pnl_by_pattern, DBOffline) else {},
        })

    async def _handle_killswitch(request: web.Request) -> web.Response:
        if killswitch.is_active:
            return web.json_response({"fired": True, "already_active": True})
        task = asyncio.create_task(
            emergency_close_all(order_manager, portfolio, telemetry, "KILLSWITCH_UI", alert_dispatcher)
        )
        task.add_done_callback(
            lambda t: logger.error("[KS] emergency_close_all failed: %s", t.exception())
            if not t.cancelled() and t.exception() else None
        )
        return web.json_response({"fired": True})

    async def _handle_ws(request: web.Request, target_hub: RealtimeHub | None) -> web.WebSocketResponse:
        """Generic WebSocket handler: register on a hub, keep open until client closes."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        if target_hub is not None:
            target_hub.register(ws)
        try:
            async for _msg in ws:  # keep connection open; client never sends
                pass
        finally:
            if target_hub is not None:
                target_hub.unregister(ws)
        return ws

    async def _handle_ws_lob(request: web.Request) -> web.WebSocketResponse:
        return await _handle_ws(request, lob_hub)

    async def _handle_ws_portfolio(request: web.Request) -> web.WebSocketResponse:
        return await _handle_ws(request, portfolio_hub)

    async def _handle_ws_signals(request: web.Request) -> web.WebSocketResponse:
        return await _handle_ws(request, signal_hub)

    app = web.Application()
    app.router.add_get("/api/health", _handle_health)
    app.router.add_get("/api/portfolio", _handle_portfolio)
    app.router.add_get("/api/session", _handle_session)
    app.router.add_post("/api/killswitch", _handle_killswitch)
    app.router.add_get("/ws/lob", _handle_ws_lob)
    app.router.add_get("/ws/portfolio", _handle_ws_portfolio)
    app.router.add_get("/ws/signals", _handle_ws_signals)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    logger.info("[API] REST API listening on http://127.0.0.1:%d", port)
    try:
        await asyncio.Future()  # run until TaskGroup cancels this task
    finally:
        await runner.cleanup()  # guarantee socket release on any exit path


async def _process_fills(
    fill_q:        asyncio.Queue,
    portfolio:     PortfolioState,
    telemetry:     SignalTelemetry,
    order_manager=None,
    portfolio_hub: RealtimeHub | None = None,
) -> None:
    """Consume and log IOC entry fills. Outcome/PnL recording happens via update_outcome_cb."""
    while True:
        fill: FillDetail = await fill_q.get()
        logger.info(
            "[Fill] signal=%s side=%s qty=%.6f price=%.2f slippage=%.1f bps",
            fill.signal_id, fill.side, fill.qty, fill.fill_price, fill.slippage_bps,
        )
        if portfolio.num_fill_samples == 0:
            portfolio.avg_slippage_bps = fill.slippage_bps
        else:
            portfolio.avg_slippage_bps = 0.1 * fill.slippage_bps + 0.9 * portfolio.avg_slippage_bps
        portfolio.num_fill_samples += 1
        await telemetry.update_fill(fill.signal_id, fill.slippage_bps)
        if portfolio_hub is not None:
            payload = _build_portfolio_payload(portfolio, order_manager=order_manager)
            payload["type"] = "portfolio"
            payload["ts"]   = datetime.now(timezone.utc).isoformat()
            await portfolio_hub.broadcast(payload)


# ── Engine health writer ──────────────────────────────────────────────────────

async def _health_writer(
    shared_state: SharedState,
    killswitch: GlobalKillswitch,
    risk_engine: RiskEngine,
    db_writer: DBWriter,
    interval: float = 5.0,
) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await db_writer.write_engine_health(
                lob_status=shared_state.lob_status,
                hb_status=shared_state.heartbeat_status,
                risk_tier=risk_engine.tier.name,
                ks_active=killswitch.is_active,
                consecutive_losses=risk_engine.portfolio.consecutive_losses,
                cooldown_until_ms=(
                    int(risk_engine.cooldown_until.timestamp() * 1000)
                    if risk_engine.cooldown_until else None
                ),
            )
        except Exception:
            pass  # non-critical; dashboard falls back to stale badge gracefully


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("=== CryptoSentinel Starting ===")

    # 1. Initialise all components ────────────────────────────────────────────
    init_db()
    alert_dispatcher = AlertDispatcher(settings.ALERT_WEBHOOK_URL)

    portfolio = PortfolioState(
        equity=settings.STARTING_EQUITY,
        starting_equity=settings.STARTING_EQUITY,
        peak_equity=settings.STARTING_EQUITY,
    )
    shared_state = SharedState(
        heartbeat_status="HEALTHY",
        last_delta_ms=0,
        lob_status="UNINITIALISED",
    )

    killswitch      = GlobalKillswitch(settings.STARTING_EQUITY)
    budget          = DailyBudget.from_equity(settings.STARTING_EQUITY)
    cvd_calculator  = CVDCalculator()
    lob_engine      = LocalOrderBook(shared_state=shared_state)
    feature_computer = FeatureComputer()

    # Queues ──────────────────────────────────────────────────────────────────
    # Candle queue feeds FeatureComputer directly. Legacy pattern execution is disabled.
    candle_queue:       asyncio.Queue = asyncio.Queue(maxsize=200)
    candle_db_queue:    asyncio.Queue = asyncio.Queue(maxsize=200)
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
            await emergency_close_all(
                order_manager, portfolio, telemetry, "KILLSWITCH_HEARTBEAT", alert_dispatcher
            )

    async def _ks_fire_cb(reason: str) -> None:
        await emergency_close_all(order_manager, portfolio, telemetry, reason, alert_dispatcher)

    def _on_tier_change(old_tier: str, new_tier: str) -> None:
        telemetry.write_system_event(
            "TIER_TRANSITION",
            {"from": old_tier, "to": new_tier, "loss_pct": budget.loss_pct},
        )
        asyncio.create_task(
            alert_dispatcher.notify_tier_change(old_tier, new_tier, budget.loss_pct)
        )

    # Real-time broadcast hubs — one per logical stream. Keeping them separate so
    # clients only receive the messages they care about (no client-side filtering,
    # no bandwidth waste on multi-page dashboards). Created early so producers
    # (microstructure detector, snapshot writer, alerts) can be constructed with a
    # hub reference.
    lob_hub       = RealtimeHub()  # /ws/lob: snapshots + microstructure events
    portfolio_hub = RealtimeHub()  # /ws/portfolio: equity / PnL / positions / risk tier
    signal_hub    = RealtimeHub()  # /ws/signals: live gate-decision tape

    # Components ──────────────────────────────────────────────────────────────
    _sym = settings.SYMBOL.lower()
    price_consumer = BinanceWebSocketConsumer(
        trade_queue=trade_queue,
        shared_state=shared_state,
        heartbeat_cb=_heartbeat_cb,
        streams=[f"{_sym}@bookTicker", f"{_sym}@aggTrade"],
        heartbeat_key="heartbeat_status",
    )
    lob_consumer = BinanceWebSocketConsumer(
        candle_queue=candle_queue,
        candle_db_queue=candle_db_queue,
        depth_queue=raw_depth_queue,
        shared_state=shared_state,
        streams=[f"{_sym}@depth@500ms", f"{_sym}@kline_{settings.TIMEFRAME}"],
        heartbeat_key="lob_heartbeat_status",
        critical_ms=settings.HEARTBEAT_LOB_CRITICAL_MS,
    )
    lob_recorder = LOBRecorder()
    micro_detector = MicrostructureDetector(
        depth_queue=ms_depth_queue,
        trade_queue=trade_queue,
        signal_queue=micro_signal_queue,
        cvd_calculator=cvd_calculator,
        feature_computer=feature_computer,
        hub=lob_hub,
    )
    telemetry = SignalTelemetry(telemetry_queue=telemetry_queue, hub=signal_hub)

    # Resolve active registered strategy so strategy_id and entry_rules can be
    # passed into StrategyExecutor. Falls back to defaults when no spec is registered.
    _registry     = StrategyRegistry(db_path=settings.REGISTRY_DB)
    _active_spec  = _registry.get_active_strategy()
    _strategy_id  = _active_spec.strategy_id if _active_spec else "v3.0"
    if _active_spec:
        logger.info(
            "[Main] Active strategy: %s v%d (%s)", _active_spec.name,
            _active_spec.version, _active_spec.status,
        )
    else:
        logger.warning("[Main] No registered strategy found — using default strategy_id and EntryRules")

    # RiskEngine constructed before OrderManager so record_trade_result can be
    # wired as budget_update_cb — it updates equity, daily_pnl, num_trades, and
    # all other portfolio counters on every position close.
    risk_engine = RiskEngine(
        portfolio=portfolio,
        budget=budget,
        killswitch=killswitch,
        tier_change_cb=_on_tier_change,
    )
    order_manager = OrderManager(
        signal_queue=om_queue,
        fill_queue=fill_queue,
        killswitch=killswitch,
        equity_fn=lambda: portfolio.equity,
        book_fn=lob_engine.best_bid_ask,
        ks_fire_cb=_ks_fire_cb,
        update_outcome_cb=telemetry.update_outcome,
        budget_update_cb=risk_engine.record_trade_result,
        portfolio_hub=portfolio_hub,
        alert_dispatcher=alert_dispatcher,
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
        strategy_id=_strategy_id,
        equity_fn=lambda: portfolio.equity,
    )
    if _active_spec:
        strategy_executor.set_entry_rules(_active_spec.entry_rules)
    db_writer = DBWriter(
        candle_queue=candle_db_queue,
        portfolio=portfolio,
        ms_bar_queue=ms_bar_queue,
    )

    # 2. Connect to Binance testnet ───────────────────────────────────────────
    client = None
    try:
        _api_key    = settings.DEMO_BINANCE_API_KEY if settings.BINANCE_DEMO else settings.BINANCE_API_KEY
        _api_secret = settings.DEMO_BINANCE_API_SECRET if settings.BINANCE_DEMO else settings.BINANCE_API_SECRET
        _mode_label = "demo" if settings.BINANCE_DEMO else "testnet" if settings.BINANCE_TESTNET else "live"
        client = await AsyncClient.create(
            api_key    = _api_key,
            api_secret = _api_secret,
            testnet    = settings.BINANCE_TESTNET,
            demo       = settings.BINANCE_DEMO,
        )
        logger.info("[Main] Connected to Binance %s", _mode_label)
    except Exception as exc:
        logger.warning("[Main] Could not connect to Binance %s: %s — proceeding without client", _mode_label, exc)

    # 3. Reconcile on startup — BEFORE starting any coroutines ────────────────
    if client is not None:
        reconcile_result = await reconcile_on_startup(client, portfolio, risk_engine, symbol=SYMBOL)
        await _backfill_feature_candles(client, feature_computer)
        if reconcile_result.get("has_orphan_position"):
            pos_amt = reconcile_result["orphan_position_qty"]
            await order_manager.close_orphan_position(
                qty=abs(pos_amt),
                side="BUY" if pos_amt > 0 else "SELL",
            )
    else:
        logger.info("[Main] Skipping reconciliation — no Binance client")

    user_data_stream = UserDataStreamConsumer(client=client) if client is not None else None

    # Rebase risk limits to actual Binance account equity
    actual_equity = portfolio.equity
    if actual_equity > 0:
        killswitch.update_dov(actual_equity)
        if killswitch.check_budget(portfolio.daily_pnl, 0.0):
            logger.critical(
                "[Main] KS-1 re-fired at startup — daily budget already blown (pnl=%.2f)",
                portfolio.daily_pnl,
            )
        budget.rebase(actual_equity)
        logger.info(
            "[Main] Risk limits rebased to actual equity=%.2f "
            "(KS-1 hard_limit=%.2f budget hard_limit=%.2f)",
            actual_equity,
            actual_equity * 0.01,
            actual_equity * 0.01,
        )

    # 4. Register SIGTERM/SIGINT handlers ─────────────────────────────────────
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, shutdown_event.set)
    loop.add_signal_handler(signal.SIGINT, shutdown_event.set)

    # 5. LOB warm-up guard ────────────────────────────────────────────────────
    await asyncio.sleep(0.5)

    async def _on_execution_report(msg: dict) -> None:
        await order_manager.on_execution_report(msg)

    # 6. Start TaskGroup with all coroutines ──────────────────────────────────
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_watch_shutdown(shutdown_event),                                   name="shutdown_watcher")
            tg.create_task(lob_recorder.start(),                                              name="lob_recorder")
            tg.create_task(price_consumer.start(),                                             name="ws_price_consumer")
            tg.create_task(lob_consumer.start(),                                               name="ws_lob_consumer")
            tg.create_task(_depth_fanout(raw_depth_queue, lob_depth_queue, ms_depth_queue),  name="depth_fanout")
            tg.create_task(_run_lob_engine(lob_engine, lob_depth_queue),                     name="lob_engine")
            tg.create_task(micro_detector.run(),                                              name="micro_detector")
            tg.create_task(_feature_candle_loop(candle_queue, feature_computer),              name="feature_candle_loop")
            tg.create_task(strategy_executor.run(),                                           name="strategy_executor")
            tg.create_task(order_manager.start(),                                             name="order_manager")
            tg.create_task(telemetry.run(),                                                   name="signal_telemetry")
            tg.create_task(midnight_reset_loop(risk_engine, killswitch),                       name="midnight_reset")
            tg.create_task(_orphan_watchdog(order_manager, killswitch, alert_dispatcher),      name="orphan_watchdog")
            tg.create_task(db_writer.run(),                                                   name="db_writer")
            tg.create_task(
                _process_fills(fill_queue, portfolio, telemetry,
                               order_manager=order_manager,
                               portfolio_hub=portfolio_hub),
                name="fill_processor",
            )
            tg.create_task(
                _portfolio_mtm_loop(
                    killswitch, budget, order_manager, portfolio, telemetry,
                    risk_engine, strategy_executor, alert_dispatcher,
                    hub=portfolio_hub,
                ),
                name="portfolio_mtm_loop",
            )
            tg.create_task(
                _lob_snapshot_writer(lob_engine, cvd_calculator, db_writer, hub=lob_hub),
                name="lob_snapshot_writer",
            )
            tg.create_task(
                _health_writer(shared_state, killswitch, risk_engine, db_writer),
                name="health_writer",
            )
            tg.create_task(
                _api_server(
                    killswitch, portfolio, shared_state,
                    order_manager, telemetry, settings.DASHBOARD_API_PORT,
                    alert_dispatcher, lob_hub=lob_hub, portfolio_hub=portfolio_hub,
                    signal_hub=signal_hub,
                ),
                name="api_server",
            )
            if not settings.DRY_RUN and user_data_stream is not None:
                tg.create_task(
                    user_data_stream.start(execution_report_cb=_on_execution_report),
                    name="user_data_stream",
                )
            if settings.TEST_SIGNAL_INJECT:
                logger.info("[Main] TEST_SIGNAL_INJECT=True — signal injector starting")
                from scripts.signal_injector import run as _injector_run
                tg.create_task(
                    _injector_run(micro_signal_queue, lob_engine, feature_computer, shared_state, cvd_calculator),
                    name="signal_injector",
                )
    except* asyncio.CancelledError:
        pass
    except* Exception as eg:
        logger.error("[Main] TaskGroup error(s): %s", eg.exceptions)
    finally:
        await shutdown_handler(order_manager, portfolio, telemetry)
        if user_data_stream is not None:
            await user_data_stream.stop()
        if client is not None:
            await client.close_connection()
        logger.info("=== CryptoSentinel Stopped ===")


if __name__ == "__main__":
    asyncio.run(main())
