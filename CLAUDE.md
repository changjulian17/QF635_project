# CLAUDE.md — CryptoSentinel project guidance

Real-time granular LOB microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

**Current phase:** Phase 1L complete (1A–1L done; 1M–1N pending). 257 tests passing.

## Project layout

```
main.py          — asyncio orchestrator (entry point)
dashboard.py     — Streamlit UI (active during Phase 1)
config.py        — pydantic settings loaded from .env
models.py        — shared dataclasses and enums

core/
  ws_consumer.py       — WebSocket consumer + HeartbeatMonitor
  lob_engine.py        — Local Order Book + full state machine (UNINITIALISED → SYNCED)
  lob_recorder.py      — Always-on LOB data collector (real Binance public stream)
  cvd.py               — Standalone CVD calculator (WelfordOnline std)
  signal_telemetry.py  — Async signal record writer (all gates, pass + fail)
  pattern_detector.py  — OHLCV chart pattern detection (context/boost)

strategy/
  features.py          — FeatureComputer + WelfordOnline (15 features, no look-ahead)
  microstructure.py    — Wall Identification / Absorption / Sweep + Fresh Wall detector
  executor.py          — StrategyExecutor — 7-Gate pipeline + RuleBasedScorer

risk/
  engine.py            — RiskEngine — 5-tier throttling, DOV, circuit breakers
  budget.py            — DailyBudget — shared pool, remaining, loss_pct, reset
  pyramid.py           — PyramidController — 3-leg scaling (100%/50%/25%)
  killswitch.py        — GlobalKillswitch — KS-1 budget / KS-2 heartbeat / KS-3 slippage

execution/
  order_manager.py     — IOC aggressive limit orders + OCO brackets

engine/              — legacy shim directory (re-exports to new locations)
  db_writer.py       — SQLite persistence + rolling cleanup (kept here)
  microstructure_engine.py — legacy (superseded by strategy/microstructure.py)
  risk_engine.py     — re-exports risk.engine.RiskEngine

pages/               — Streamlit multi-page app
  health.py          — system health checker (auto-refreshes every 10 s)

scripts/
  test_connection.py — verify Binance testnet connectivity and auth
  test_orders.py     — BUY + SELL round-trip execution test

tests/               — pytest unit tests (212 tests across 13 files)
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

Start the Streamlit dashboard (terminal 3):

	streamlit run dashboard.py

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
| 1K — Risk Engine rewrite | ✅ | risk/ (engine, budget, pyramid, killswitch) |
| 1L — IOC execution layer | ✅ | execution/order_manager.py rewrite |
| 1M — Startup reconciler | ⏳ | core/startup_reconciler.py |
| 1N — Integration test | ⏳ | tests/test_integration.py |
