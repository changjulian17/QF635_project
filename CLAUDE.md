# CLAUDE.md — CryptoSentinel project guidance

Real-time granular LOB microstructure analysis and paper-trading system for the BTCUSDT USD-M perpetual on Binance Futures.

**Current phase:** Phase 3 complete (v3.1). All deliverables shipped. v3.1 migrated the venue from Binance Spot Testnet to **Binance USD-M Futures** (native SHORT + reduce-only TP/SL brackets), added a `TRADING_MODE` preset system (`testnet`/`demo`/`live`), hardened WS reconnect / LOB gap re-init, tightened the LOB bucket $25 → $1, and merged the dashboard's Backtest + Registry pages into `/strategies`.

## Project layout

```
main.py          — asyncio orchestrator (entry point)
config.py        — pydantic settings loaded from .env
models.py        — shared dataclasses and enums

core/
  ws_consumer.py       — WebSocket consumer + HeartbeatMonitor (live engine runs two: price [bookTicker+aggTrade] and LOB [depth@500ms+kline])
  lob_engine.py        — Local Order Book + full state machine (UNINITIALISED → SYNCED)
  lob_recorder.py      — Always-on LOB data collector (Binance Futures public stream, depth@100ms, $1 buckets)
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
  order_manager.py     — IOC aggressive limit entries + reduce-only TP/SL bracket (Binance USD-M Futures)
  orders.py            — Futures order classes (IOCLimitOrder, FuturesMarketOrder, FuturesTPOrder, FuturesSLOrder)

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
    strategies.py    — /strategies: tabbed backtest leaderboard + registry lifecycle/decay/LIVE promotion (merged backtest + registry)
    config.py        — /config: settings reference, emergency stop, event log

scripts/
  test_connection.py     — verify Binance Futures testnet/demo connectivity and auth
  test_spot_connection.py — connectivity check for demo futures
  test_futures_demo.py   — full round-trip demo futures connectivity test (start_demo.sh pre-flight)
  test_orders.py         — BUY + SELL round-trip execution test
  test_oco_demo.py       — demo futures reduce-only TP/SL bracket smoke test
  test_market_order_demo.py — demo futures market-order smoke test
  test_ws_stability.py   — WebSocket reconnect / heartbeat soak test
  test_heartbeat_cascade.py — heartbeat state-machine cascade verification
  sweep_thresholds.py    — sweep/wall threshold-sweep harness over recorded LOB data
  run_backtest.py        — CLI for tick-level walk-forward backtest
  backfill_agg_trades.py — one-time backfill of aggTrade history from Binance USDM Futures REST
  signal_injector.py     — synthetic signal injection (start_test.sh / start_demo.sh only)

tests/               — pytest unit tests (591 tests across 46 files)
```

## Quick actions

Activate the virtual environment:

	source .venv/bin/activate

Install dependencies:

	pip install -r requirements.txt

Verify connectivity (after adding API keys to `.env`):

	python scripts/test_connection.py

Launch the full stack via the mode wrapper scripts (each opens 3 macOS Terminal windows: LOB Recorder, Trading Engine, Dashboard):

	./start.sh         # live    (TRADING_MODE=live, real money)
	./start_demo.sh    # demo    (TRADING_MODE=demo, demo.binance.com; add --dry-run for synthetic fills)
	./start_test.sh    # testnet (TRADING_MODE=testnet + signal injection)

Or start components manually (set TRADING_MODE first, e.g. `export TRADING_MODE=demo`):

	python -m core.lob_recorder    # terminal 1 — Futures public stream, no API key needed
	python main.py                 # terminal 2 — trading engine
	python dashboard/app.py        # terminal 3 — dashboard at http://127.0.0.1:8050

Run unit tests:

	python -m pytest tests/ -v

## Configuration notes

- `TRADING_MODE` (`testnet`/`demo`/`live`) selects the venue and applies a preset of WS/REST endpoints, heartbeat thresholds, and safety defaults in `config.py::_apply_mode_presets`. **Any value explicitly set in `.env` always wins** — presets only fill unset fields.
- Put Binance Futures credentials in `.env` (gitignored): `DEMO_BINANCE_API_KEY`/`DEMO_BINANCE_API_SECRET` for testnet/demo, `BINANCE_API_KEY`/`BINANCE_API_SECRET` for live. Use `.env.example` as a template.
- `DRY_RUN=True` in config.py — the start scripts set `DRY_RUN=False`; `start_demo.sh --dry-run` forces synthetic fills.
- `LOB_RECORDER_WS` points to the Binance Futures public stream (`fstream.binance.com` in demo/live; `stream.binancefuture.com` testnet), separate from the trading-account connection — this is intentional (Rule 4).
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
| 3D–3H — Dash dashboard | ✅ | dashboard/ app with 5 pages: live, lob, walls, strategies (merged backtest + registry), config |
| 3I — Phase 3 tests | ✅ | test_rest_api.py (11), test_lob_snapshot_writer.py (9), test_realtime_hub.py (6), test_dashboard_live.py (10), test_dashboard_lob.py (16), test_dashboard_strategies.py (5), test_dashboard_registry.py (4), test_dashboard_walls.py (6), test_portfolio_broadcast.py (7), test_signal_broadcast.py (9) |
| 3J — Futures migration (v3.1) | ✅ | Spot→USD-M Futures, TRADING_MODE presets (testnet/demo/live), reduce-only TP/SL brackets, two WS consumers (depth@500ms), WS reconnect + LOB gap re-init hardening, $1 LOB buckets; new suites: test_config (5), test_budget (6), test_killswitch (12), test_builder (4), test_spec (3) |
