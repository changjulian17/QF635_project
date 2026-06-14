# CLAUDE.md — CryptoSentinel project guidance

Real-time granular LOB microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

**Current phase:** Phase 3 complete. All deliverables shipped.

## Project layout

```
main.py          — asyncio orchestrator (entry point)
config.py        — pydantic settings loaded from .env
models.py        — shared dataclasses and enums

core/
  ws_consumer.py       — WebSocket consumer + HeartbeatMonitor
  lob_engine.py        — Local Order Book + full state machine (UNINITIALISED → SYNCED)
  lob_recorder.py      — Always-on LOB data collector (real Binance public stream)
  cvd.py               — Standalone CVD calculator (WelfordOnline std)
  signal_telemetry.py  — Async signal record writer (all gates, pass + fail)
  startup_reconciler.py — Exchange state reconciliation on startup + midnight reset
  user_data_stream.py  — Binance user data stream (listenKey lifecycle, executionReport dispatch)
  alerting.py          — AlertDispatcher — optional webhook notifications for killswitch/tier events

strategy/
  features.py          — FeatureComputer + WelfordOnline (15 features, no look-ahead)
  microstructure.py    — Wall Identification / Absorption / Sweep + Fresh Wall detector
  executor.py          — StrategyExecutor — 7-Gate pipeline + RuleBasedScorer
  scorer.py            — XGBoostScorer + ScorerFactory (ML confidence gate, falls back to RuleBasedScorer)
  spec.py              — StrategySpec, EntryRules, StatisticalValidity dataclasses
  registry.py          — StrategyRegistry — dual-store (YAML + SQLite) lifecycle + promotion gates
  builder.py           — StrategyBuilder — 3-stage pipeline: load metrics → validate → register

risk/
  engine.py            — RiskEngine — 5-tier throttling, DOV, circuit breakers
  budget.py            — DailyBudget — shared pool, remaining, loss_pct, reset
  killswitch.py        — GlobalKillswitch — KS-1 budget / KS-2 heartbeat / KS-3 slippage
  sizing.py            — Position-sizing helpers (clamp_stop_bps)

execution/
  order_manager.py     — IOC aggressive limit orders + OCO brackets
  orders.py            — Order domain classes (IOCLimitOrder, FuturesTPOrder, FuturesSLOrder)

engine/              — legacy shim directory + realtime infrastructure
  db_writer.py       — SQLite persistence + rolling cleanup + lob_snapshots writer
  lob_snapshot_writer.py — LOB snapshot writer coroutine (~1 Hz, lob_snapshots table)
  realtime_hub.py    — RealtimeHub — fan-out JSON pushes to /ws/lob WebSocket clients
  microstructure_engine.py — legacy (superseded by strategy/microstructure.py; not started in live path)
  risk_engine.py     — re-exports risk.engine.RiskEngine

dashboard/           — Dash multi-page dashboard (Phase 3)
  app.py             — entry point; dark theme, nav, engine status badge
  _db.py             — WAL-mode SQLite helpers shared by all pages
  _logic.py          — shared business logic for dashboard pages
  _utils.py          — shared Plotly utilities (empty_fig)
  pages/
    live.py          — /live: portfolio metrics, signal funnel, kill switch
    lob.py           — /lob: LOB heatmap, OBI/CVD/Spread subplots
    walls.py         — /walls: 1s candlestick + rolling VWAP + liquidity wall heatmap
    backtest.py      — /backtest: strategy leaderboard from backtest results
    registry.py      — /registry: strategy lifecycle, decay monitoring, LIVE promotion
    config.py        — /config: settings reference, emergency stop, event log

scripts/
  test_connection.py     — verify Binance testnet connectivity and auth
  test_spot_connection.py — connectivity check for demo futures
  test_futures_demo.py   — full round-trip demo futures connectivity test
  test_orders.py         — BUY + SELL round-trip execution test
  backfill_agg_trades.py — one-time backfill of aggTrade history from Binance USDM Futures REST
  signal_injector.py     — synthetic signal injection (start_test.sh only)

tests/               — pytest unit tests (549 tests across 40 files)
```

## Quick actions

Activate the virtual environment:

	source .venv/bin/activate

Install dependencies:

	pip install -r requirements.txt

Verify connectivity (after adding testnet keys to `.env`):

	python scripts/test_connection.py

Start LOB Recorder (terminal 1 — collects real Binance tick data, no API key needed):

	python -m core.lob_recorder

Start the trading engine (terminal 2):

	python main.py

Start the Dash dashboard (terminal 3 — Phase 3):

	python dashboard/app.py

Run unit tests:

	python -m pytest tests/ -v

## Configuration notes

- Put Binance testnet credentials in `.env` (gitignored). Use `.env.example` as a template.
- `DRY_RUN=True` in config.py — set `DRY_RUN=False` in `.env` to enable live order execution.
- `LOB_RECORDER_WS` points to real Binance public stream (`wss://stream.binance.com:9443`), not testnet — this is intentional (Rule 4).
- Never add API keys to source files.
- Always use the `.venv` virtual environment in the project root.

## Phase status

| Phase | Status | Key deliverable |
|-------|--------|----------------|
| 1A — Directory restructure | ✅ | core/, strategy/, risk/, execution/ |
| 1B — Config + Models | ✅ | WallState, FeatureVector, MicroSignal, KillswitchState |
| 1C — HeartbeatMonitor | ✅ | ws_consumer.py |
| 1D — LOB state machine | ✅ | lob_engine.py |
| 1E — LOB Recorder | ✅ | core/lob_recorder.py |
| 1F — CVD Calculator | ✅ | core/cvd.py |
| 1G — FeatureComputer | ✅ | strategy/features.py |
| 1H — Microstructure Detector | ✅ | strategy/microstructure.py |
| 1I — Signal Telemetry | ✅ | core/signal_telemetry.py |
| 1J — 7-Gate Executor | ✅ | strategy/executor.py |
| 1K — Risk Engine rewrite | ✅ | risk/ (engine, budget, killswitch, sizing) |
| 1L — IOC execution layer | ✅ | execution/order_manager.py + orders.py |
| 1M — Startup reconciler | ✅ | core/startup_reconciler.py + main.py rewrite |
| 1N — Integration test | ✅ | tests/test_integration.py |
| 1O — User data stream | ✅ | core/user_data_stream.py — listenKey lifecycle + executionReport dispatch |
| 1P — Alerting | ✅ | core/alerting.py — AlertDispatcher webhook notifications |
| 2G — Tick Replay Engine | ✅ | backtesting/tick_replay.py |
| 2H — XGBoost Confidence Scorer | ✅ | strategy/scorer.py + ScorerFactory wired into Gate 2 |
| 2I — Strategy Registry | ✅ | strategy/spec.py, registry.py, builder.py + tests/test_registry.py |
| 3A — REST API | ✅ | _api_server coroutine in main.py (aiohttp, /api/health, /api/portfolio, /api/session, /api/killswitch) |
| 3B — LOB snapshot writer | ✅ | engine/lob_snapshot_writer.py + lob_snapshots table in db_writer.py |
| 3C — RealtimeHub | ✅ | engine/realtime_hub.py — /ws/lob, /ws/portfolio, /ws/signals WebSocket fan-out |
| 3D–3H — Dash dashboard | ✅ | dashboard/ app with 6 pages: live, lob, walls, backtest, registry, config |
| 3I — Phase 3 tests | ✅ | test_rest_api.py (11), test_lob_snapshot_writer.py (9), test_realtime_hub.py (6), test_dashboard_live.py (10), test_dashboard_lob.py (16), test_dashboard_registry.py (4), test_dashboard_walls.py (6), test_portfolio_broadcast.py (7), test_signal_broadcast.py (9) |
