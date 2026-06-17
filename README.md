# CryptoSentinel v3.1

Real-time granular LOB microstructure analysis and paper-trading system for the **BTCUSDT USD-M perpetual** on **Binance Futures**.

Detects institutional **Liquidity Walls**, observes **Absorption** and **Book Sweep** events at specific price levels, and fires trades only when a Wall is consumed and Fresh Protective Liquidity appears behind the breakout — confirmed through a 7-gate execution pipeline with full signal telemetry.

> **v3.1 — Futures migration + operational hardening.** The execution venue moved from Binance Spot Testnet to **Binance USD-M Futures** (perpetual, supporting native SHORT entries and reduce-only TP/SL brackets). A single `TRADING_MODE` setting (`testnet` / `demo` / `live`) now drives all environment presets and the three launch scripts. WebSocket reconnect/heartbeat handling and LOB gap re-initialisation were hardened, the LOB price-bucket aggregation was tightened from $25 → $1, and the dashboard's Backtest and Registry pages were merged into a single `/strategies` view.

---

## System Philosophy

> *"Price movement is a function of aggressive market orders consuming specific, persistent liquidity levels — not the aggregate balance of the book."*

CryptoSentinel is built on a granular, level-specific insight: **price moves when aggressive orders hit resting liquidity walls and successfully consume them**. Aggregate OBI is retained as a secondary sentiment filter only.

### Signal Hierarchy

```
PRIMARY — Granular LOB Wall Interaction
  ┌─────────────────────────────────────────────────────┐
  │  Wall Identified    Level depth > 2.5σ above         │
  │                     surrounding 10-tick median        │
  │                                                      │
  │  Absorption         Aggression hits Wall, price holds │
  │                     Wall reloads faster than consumed │
  │                     → CONTEXT ONLY, arms the system  │
  │                                                      │
  │  Sweep + Fresh Wall Wall consumed (price > 0.03%),   │
  │                     new protective Wall appears       │
  │                     immediately behind breakout       │
  │                     → FIRES THE TRADE                │
  └─────────────────────────────────────────────────────┘
           │
           ▼
SECONDARY FILTER — OBI Sentiment Alignment
  OBI must directionally agree with the Sweep direction.
  Strong OBI disagreement → reject (deceptive sweep)
           │
           ▼
OPTIONAL BOOST — Chart Pattern Context (OHLCV) [not active in live path]
  pattern_r2 defaults to 0.0 — core/pattern_detector.py is not in the live codebase
  Pattern absent → signal still valid without boost
           │
           ▼
FILTER — Confidence Scorer
  Rule-based placeholder (→ XGBoost after data collection)
  confidence ≥ 0.58 required to proceed
           │
           ▼
  7-Gate Trade Life Cycle → Binance Futures Execution
```

---

## Project Structure

```
CryptoSentinel/
│
├── main.py                    # Async orchestrator — all coroutines
├── backtest.py                # CLI backtest runner — VectorBT + Optuna two-phase pipeline
├── config.py                  # Pydantic V2 settings from .env
├── models.py                  # Shared dataclasses and enums
├── requirements.txt
│
├── core/                      # Real-time data processing
│   ├── ws_consumer.py         # WebSocket consumer + HeartbeatMonitor
│   ├── lob_recorder.py        # Always-on LOB data collector (real Binance public stream)
│   ├── lob_engine.py          # Local Order Book + full state machine
│   ├── cvd.py                 # Standalone CVD calculator
│   ├── signal_telemetry.py    # Signal record writer (all gates, pass + fail)
│   ├── startup_reconciler.py  # Exchange state reconciliation on startup + midnight reset
│   └── alerting.py            # AlertDispatcher — optional webhook notifications
│
├── strategy/                  # Alpha generation and execution
│   ├── features.py            # FeatureComputer + WelfordOnline (single source of truth)
│   ├── microstructure.py      # Wall Identification / Absorption / Sweep + Fresh Wall
│   ├── executor.py            # StrategyExecutor — 7-Gate pipeline + confidence scorer
│   ├── spec.py                # StrategySpec — immutable YAML-backed strategy descriptor
│   ├── scorer.py              # XGBoostScorer + ScorerFactory (ML confidence gate, Gate 2)
│   ├── registry.py            # StrategyRegistry — dual-store lifecycle (YAML + SQLite)
│   └── builder.py             # StrategyBuilder — 3-stage pipeline: validate → register
│
├── risk/                      # Risk management
│   ├── engine.py              # RiskEngine — 5-tier throttling, DOV, circuit breakers
│   ├── budget.py              # DailyBudget — shared pool for all open positions
│   ├── killswitch.py          # GlobalKillswitch — Budget / Heartbeat / Slippage triggers
│   └── sizing.py              # Position-sizing helpers (clamp_stop_bps)
│
├── execution/                 # Order management (Binance USD-M Futures)
│   ├── order_manager.py       # IOC aggressive limit entries + reduce-only TP/SL bracket
│   └── orders.py              # Futures order classes: IOCLimitOrder, FuturesMarketOrder, FuturesTPOrder, FuturesSLOrder
│
├── backtesting/               # Offline strategy research
│   ├── tick_replay.py         # Path A: event-driven replay of lob_tick.db through live stack
│   ├── event_engine.py        # Path A: tick-level event replay engine
│   ├── signals.py             # Signal generation for backtesting
│   ├── walk_forward.py        # Path B: OHLCV walk-forward (VectorBT + Optuna)
│   ├── vectorbt_runner.py     # VectorBT execution wrapper
│   ├── metrics.py             # Sharpe, Sortino, Calmar, MDD, PF, WR
│   └── costs.py               # Transaction cost model
│
├── data/                      # Data acquisition and storage
│   ├── fetcher.py             # OHLCVFetcher (CCXT + SQLite cache)
│   ├── validator.py           # 9-check data quality validator
│   ├── lob_tick.db            # LOB Recorder output (Binance Futures public stream — live-writing)
│   ├── ohlcv_cache.db         # OHLCV SQLite cache (generated — pending Paper Trading step 2)
│   └── backtest_results.db    # Walk-forward results storage (generated — pending Paper Trading step 3)
│
├── dashboard/                 # Dash multi-page application (Phase 3)
│   ├── app.py                 # Entry point — dark theme, nav, engine status badge (http://127.0.0.1:8050)
│   ├── _db.py                 # WAL-mode SQLite helpers shared by all pages
│   ├── _logic.py              # Shared business logic for dashboard pages
│   ├── _utils.py              # Shared Plotly utilities (empty_fig)
│   └── pages/
│       ├── live.py            # /live       — Portfolio metrics, signal funnel, kill switch
│       ├── lob.py             # /lob        — LOB heatmap + CVD + OBI + spread subplots
│       ├── walls.py           # /walls      — 1s candlestick + rolling VWAP + liquidity wall heatmap
│       ├── strategies.py      # /strategies — Tabbed view: backtest leaderboard + registry lifecycle/decay/promotion
│       └── config.py          # /config     — Settings reference, emergency stop, event log
│
├── strategies/                # Strategy artifacts
│   ├── registry.db            # SQLite: signal_records + system events
│   └── {name}_v{version}.yaml # Frozen YAML strategy specs (written directly to strategies/ by StrategyRegistry)
│
├── engine/                    # Legacy shim directory + real-time hub
│   ├── db_writer.py           # SQLite persistence + rolling cleanup + lob_snapshots table
│   ├── lob_snapshot_writer.py # LOB snapshot writer coroutine (~1 Hz, lob_snapshots table)
│   ├── realtime_hub.py        # RealtimeHub — fan-out JSON pushes to /ws/lob WebSocket clients
│   ├── microstructure_engine.py # Legacy MicrostructureEngine (NOT started in live path; used by backtesting tests only)
│   ├── websocket_consumer.py  # Shim re-exporting core.ws_consumer
│   ├── lob_engine.py          # Shim re-exporting core.lob_engine
│   ├── risk_engine.py         # Shim re-exporting risk.engine
│   └── order_manager.py       # Shim re-exporting execution.order_manager
│
├── scripts/
│   ├── test_connection.py     # Connectivity + auth check (futures testnet / demo)
│   ├── test_spot_connection.py# Connectivity check for demo futures (DEMO_BINANCE_API_KEY)
│   ├── test_futures_demo.py   # Full round-trip demo futures connectivity test (start_demo.sh pre-flight)
│   ├── test_orders.py         # BUY + SELL round-trip test
│   ├── test_market_order_demo.py # Demo futures market-order smoke test
│   ├── test_oco_demo.py       # Demo futures reduce-only TP/SL bracket smoke test
│   ├── test_ws_stability.py   # WebSocket reconnect / heartbeat soak test
│   ├── test_heartbeat_cascade.py # Heartbeat state-machine cascade verification
│   ├── sweep_thresholds.py    # Sweep/wall threshold-sweep harness over recorded LOB data
│   ├── validate_bugs.py       # Ad-hoc regression validation harness
│   ├── run_backtest.py        # CLI for tick-level walk-forward backtest (writes to backtest_results.db)
│   ├── backfill_agg_trades.py # One-time backfill of aggTrade history from Binance USDM Futures REST
│   └── signal_injector.py     # Synthetic signal injection — dev/testnet/demo only (start_test.sh / start_demo.sh)
│
├── tests/                     # pytest unit + integration tests (591 total, 46 files)
│   │                          # Phase 1 — live trading engine
│   ├── test_models.py
│   ├── test_config.py             # TRADING_MODE preset application + tier-ordering validation
│   ├── test_lob_engine.py
│   ├── test_lob_recorder.py
│   ├── test_ws_consumer.py
│   ├── test_microstructure.py
│   ├── test_microstructure_engine.py
│   ├── test_features.py
│   ├── test_executor.py
│   ├── test_cvd.py
│   ├── test_signal_telemetry.py
│   ├── test_db_writer.py
│   ├── test_order_manager.py
│   ├── test_orders.py
│   ├── test_risk_engine.py
│   ├── test_budget.py             # DailyBudget remaining / loss_pct / midnight reset
│   ├── test_killswitch.py         # GlobalKillswitch KS-1 / KS-2 / KS-3 triggers
│   ├── test_sizing.py
│   ├── test_startup_reconciler.py
│   ├── test_user_data_stream.py
│   ├── test_user_data_stream_bootstrap.py
│   ├── test_integration.py
│   │                          # Phase 2 — backtesting + strategy lifecycle
│   ├── test_bt_costs.py
│   ├── test_bt_event_engine.py
│   ├── test_bt_metrics.py
│   ├── test_bt_signals.py
│   ├── test_bt_tick_replay.py
│   ├── test_bt_vectorbt.py
│   ├── test_bt_walk_forward.py
│   ├── test_fetcher.py
│   ├── test_registry.py
│   ├── test_builder.py            # StrategyBuilder walk-forward metrics + build pipeline
│   ├── test_spec.py               # StrategySpec to_dict / from_dict roundtrip
│   ├── test_scorer.py
│   ├── test_validator.py
│   │                          # Phase 3 — dashboard + REST API + LOB snapshot writer
│   ├── test_alerting.py
│   ├── test_dashboard_live.py
│   ├── test_dashboard_lob.py
│   ├── test_dashboard_strategies.py  # /strategies merged backtest + registry page
│   ├── test_dashboard_registry.py
│   ├── test_dashboard_walls.py
│   ├── test_lob_snapshot_writer.py
│   ├── test_realtime_hub.py
│   ├── test_rest_api.py
│   ├── test_portfolio_broadcast.py
│   └── test_signal_broadcast.py
│
```

---

## Data Pipeline

### Stream Selection

The live trading engine runs **two** WebSocket consumers (both against `settings.WS_BASE`, the Binance Futures endpoint selected by `TRADING_MODE`):

- **Price consumer** — `btcusdt@bookTicker` + `btcusdt@aggTrade`
- **LOB consumer** — `btcusdt@depth@500ms` + `btcusdt@kline_{TIMEFRAME}`

The LOB Recorder is a separate process that subscribes to `btcusdt@depth@100ms` + `btcusdt@aggTrade` — maintaining a full local order book seeded from a REST `depth?limit=1000` snapshot on each connect. Buffered diffs received during the REST fetch are merged in sequence-ID order before the book is declared SYNCED. The top 100 levels per side are retained (`_DEPTH_LEVELS = 100`) and aggregated into **$1 USD price buckets** (`_BUCKET_WIDTH = 1.0`) before writing to `lob_tick.db`. 100 levels span enough range to detect deep-book institutional walls above the transaction-cost floor.

| Stream | Consumer | Purpose | Update Rate |
|--------|----------|---------|-------------|
| `btcusdt@aggTrade` | price consumer + LOB Recorder | CVD · aggressive volume | Per taker sweep |
| `btcusdt@depth@500ms` | LOB consumer (live engine) | Wall detection · OBI · spread — incremental diff, depth reconstructed by `LocalOrderBook` / `MicrostructureDetector` | 500ms |
| `btcusdt@depth@100ms` | LOB Recorder | Tick capture for backtesting; aggregated to $1 buckets before writing to `lob_tick.db` | 100ms |
| `btcusdt@bookTicker` | price consumer | Heartbeat tracking only; raw dict routed to `trade_queue` but discarded by `MicrostructureDetector._collect_trades()` — spread is computed from the reconstructed depth snapshot | Real-time |
| `btcusdt@kline_{TIMEFRAME}` | LOB consumer | OHLCV candles for FeatureComputer (VWAP, ATR, RSI, volume) | On close |

The two-consumer split isolates the high-rate depth/kline stream from the price/trade stream so a stall on one connection does not block the other, and lets the heartbeat monitor apply a separate `HEARTBEAT_LOB_CRITICAL_MS` budget to the depth stream.

### Async Queue Architecture

```
Queue               Producer                   Consumer                   maxsize
──────────────────────────────────────────────────────────────────────────────────
trade_queue         price consumer             MicrostructureDetector     5,000
raw_depth_queue     LOB consumer               _depth_fanout              5,000
lob_depth_queue     _depth_fanout              LocalOrderBook             5,000
ms_depth_queue      _depth_fanout              MicrostructureDetector     5,000
candle_queue        LOB consumer               FeatureComputer            200
candle_db_queue     LOB consumer               DBWriter                   200
micro_signal_queue  MicrostructureDetector     StrategyExecutor           100
om_queue            StrategyExecutor           OrderManager               50
fill_queue          OrderManager               fill_processor             200
telemetry_queue     StrategyExecutor           SignalTelemetry            500
ms_bar_queue        [no active producer]       DBWriter (loop blocks; no data)  5,000   ← no producer in live path; DBWriter consumer registered but never receives
```

---

## 7-Gate Trade Life Cycle

Every potential trade passes through seven sequential gates. Failure at any gate emits a `SignalRecord` to `signal_records` in `registry.db`. No gate may be skipped.

| Gate | Name | Pass Condition | Rejection Code |
|------|------|----------------|----------------|
| **Gate 0** | Data Fidelity | `lob_status == SYNCED` AND `heartbeat` not in `(CRITICAL, SUSTAINED_DEGRADED)` | `GATE_0_FAIL: LOB_STALE` or `GATE_0_FAIL: HEARTBEAT_CRITICAL` or `GATE_0_FAIL: HEARTBEAT_SUSTAINED_DEGRADED` |
| **Gate 1** | Microstructure Trigger | Sweep + Fresh Wall confirmed | `GATE_1_FAIL: NO_SWEEP_SIGNAL` |
| **Gate 2** | Confidence Score | `confidence >= 0.58` | `GATE_2_FAIL: LOW_CONFIDENCE 0.47 < 0.58` |
| **Gate 3** | Capital Gate | `remaining_budget > 0` AND `tier not in (HALTED, PASSIVE)` AND `no active exposure` | `GATE_3_FAIL: BUDGET_EXHAUSTED` or `GATE_3_FAIL: RISK_TIER_{tier}` or `GATE_3_FAIL: ACTIVE_EXPOSURE` |
| **Gate 4** | Order Selection | `spread_bps <= EntryRules.spread_max_bps` (8.0 bps hard cap) AND `spread_bps <= spread_p95 × 2` | `GATE_4_FAIL: SPREAD_TOO_WIDE` |
| **Gate 5** | Execution Sync | `signal_age < 200ms` AND `last_delta < HEARTBEAT_CRITICAL_MS` (500ms) | `GATE_5_FAIL: SIGNAL_STALE` |
| **Gate 6** | Persistence Monitor | Protection Wall still present (post-entry) | `GATE_6_ALERT: PROTECTION_WALL_REMOVED` |

Gate 6 is the only post-entry gate. It runs as an async task after fill confirmation and triggers early exit if the protection wall is cancelled.

---

## Key Components

### LOB Engine — State Machine (`core/lob_engine.py`)

```
UNINITIALISED → SYNCED
                  │
            GAP_DETECTED → SYNCED (after re-seed)
                  │
            DISCONNECTED (set externally by HeartbeatMonitor on KS-2)
```

Gap severity tiering (keyed on update ID regression count, not time):
| Update ID Gap | Action |
|---|---|
| < 500 | CONTINUE — minor drift, keep running |
| 500 – 4,999 | HALT_ENTRIES — pause new entries, keep open positions |
| ≥ 5,000 | CLOSE_REVIEW — evaluate open positions, consider closing |

### LOB Recorder (`core/lob_recorder.py`)

Connects to the Binance Futures public stream selected by `TRADING_MODE` (`fstream.binance.com` in `demo`/`live`; `stream.binancefuture.com` for `testnet` smoke tests) — no API key required. Records raw `depth@100ms` + `aggTrade` tick data, aggregated to $1 buckets, to `data/lob_tick.db` for backtesting Path A. Runs as a separate process from the trading-engine connection per Rule 4.

### HeartbeatMonitor (`core/ws_consumer.py`)

Tracks `(local_time − Binance event time E)` on every WebSocket message. Status is computed over a rolling 10-message window (rate-based), not per-packet:

| Status | Condition | Action |
|--------|-----------|--------|
| HEALTHY | < 30% of 10-msg window exceeds WARN_MS | Normal trading |
| DEGRADED | ≥ 50% of 10-msg window exceeds WARN_MS | Log warning (one log on entry) |
| SUSTAINED_DEGRADED | DEGRADED for ≥ HEARTBEAT_SUSTAINED_MS (10 s) at ≥ 50% rate | Log sustained warning |
| CRITICAL | CONSEC_LIMIT (3) consecutive packets > CRITICAL_MS (500ms) | Killswitch condition — CLOSE_ALL |

### FeatureComputer (`strategy/features.py`)

Single class used identically in live trading and backtesting. Uses Welford online algorithm for all z-scores and percentile ranks — strictly causal, no look-ahead, no NaN warm-up.

| Feature | Source |
|---------|--------|
| `price_vs_vwap` | `tanh((close − vwap) / (2 × ATR))` |
| `obi_zscore` | Welford z-score of OBI (secondary sentiment) |
| `cvd_delta` | `CVD[i] − CVD[i−5]` (5-bar change) |
| `vol_ratio` | `volume / Welford mean` |
| `atr_percentile` | Welford percentile rank of ATR |
| `rsi_value` | Wilder RSI, period=14 |
| `spread_bps` | `(ask − bid) / mid × 10,000` |
| `pattern_r2` | Trendline regression R² (0 if no pattern) |
| `vwap_reclaim` | 1 if close crossed above VWAP |
| `vol_climax` | 1 if `vol_ratio > 3×` |
| `cvd_positive` | 1 if `cvd_delta > 0` |
| `wall_detected` ★ | 1 if Wall (>2.5σ) within 5 ticks of mid |
| `wall_distance_bps` ★ | Distance from mid to nearest Wall |
| `absorption_ratio` ★ | `wall.qty_current / wall.qty_initial` |
| `protection_wall_present` ★ | 1 if fresh Wall detected post-sweep ≤3s |

★ = v3.0 additions

### Microstructure Detector (`strategy/microstructure.py`)

**Wall Identification:** `depth[L] > median(surrounding 10 ticks) + 2.5 × std(surrounding 10 ticks)`

**Absorption:** Wall persistent ≥500ms, aggressive flow hitting it, price held (<0.03% move), Wall reloaded to ≥70% of original qty → arms the system, no trade.

**Sweep + Fresh Wall:** Wall consumed (<15% original qty), price moved beyond adaptive threshold (floor: 0.03%), fresh Wall appeared on far side within 3s → fires the trade. CVD spike is measured and recorded in signal telemetry (`cvd_std` field) but is not a hard gate condition in `detect_sweep_with_protection()`.

### Risk Engine (`risk/engine.py`)

**Daily Open Value (DOV):** Fixed at UTC midnight. Never recalculated intraday.

**Five-Tier Throttling:**

| Tier | Daily Loss | Size Scalar | Min Confidence |
|------|-----------|-------------|----------------|
| FULL | 0–0.5% | 100% | 0.50 |
| REDUCED | 0.5–0.75% | 50% | 0.65 |
| MINIMAL | 0.75–0.9% | 25% | 0.80 |
| PASSIVE | 0.9–1.0% | 0% | — |
| HALTED | ≥ 1.0% | 0% | — |

**Position Sizing:**
```
notional_hint = confidence × KELLY_FRACTION × RISK_PER_TRADE_PCT × tier_scalar
sl_distance   = signal_price × clamp(protection_wall_bps, PROTECTION_MIN_DISTANCE_BPS, PROTECTION_MAX_DISTANCE_BPS) / 10,000
qty           = floor((equity × notional_hint / sl_distance) / QTY_STEP_SIZE) × QTY_STEP_SIZE
```
`notional_hint` is computed in `StrategyExecutor` (Gate 2). Final qty is computed in `OrderManager._place_entry`.

**Circuit Breakers:**
```
Trigger 1: daily_loss ≥ 1% of DOV          → HALTED
Trigger 2: drawdown ≥ 5% from peak          → HALTED
Trigger 3: 3 consecutive losses              → PAUSED (5 min cooldown)
Trigger 4: daily_loss ≥ 0.5%                → Tier REDUCED
Trigger 5: daily_loss ≥ 0.75%               → Tier MINIMAL
```

### Global Killswitch (`risk/killswitch.py`)

Hard override that bypasses all other logic. Once fired, requires system restart to clear. Executes CLOSE_ALL immediately: cancel all orders, market-close all positions, write `KILLSWITCH_FIRED` to `registry.db`.

| Trigger | Condition |
|---------|-----------|
| **KS-1 Budget Breach** | `(realised_pnl + unrealised_pnl) < −(DOV × 1%)` |
| **KS-2 Heartbeat Loss** | `heartbeat_status == CRITICAL` (>500ms × 3 packets) |
| **KS-3 Slippage Decay** | Rolling 20-trade avg slippage > `research_bps × 1.5` |

### Order Manager (`execution/order_manager.py`)

All entries use **IOC aggressive limit orders** on Binance USD-M Futures (`futures_create_order`) — never market orders (Rule 10):
```
LONG:  limit_price = best_ask + (spread × 0.5)   # cross half the spread
SHORT: limit_price = best_bid − (spread × 0.5)

Time-in-force: IOC — cancel if not filled immediately
If expired: do NOT retry — signal is stale
```

After fill: a **reduce-only TP/SL bracket** is placed (`FuturesTPOrder` TAKE_PROFIT + `FuturesSLOrder` STOP_MARKET, both `reduceOnly=true`). The `_placing_oco` / `_cancel_oco_on_placement` flags serialise bracket placement against Gate 6 so a wall-removal exit fired mid-placement cancels the just-placed bracket rather than double-closing. On demo futures, a soft `-2022` reduceOnly rejection falls back to placing the bracket without the reduce-only flag.

### Signal Telemetry (`core/signal_telemetry.py`)

Every signal evaluation — whether it passes all gates or is rejected at Gate 0 — is written to `signal_records` in `registry.db`. Flush every 50 records or 10 seconds. Enables gate funnel analysis, per-signal-type performance, and strategy improvement.

---

## Quick Start

Requires **Python 3.13+**.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add Binance Futures API keys to `.env` (demo keys come from demo.binance.com → API Management):
```
DEMO_BINANCE_API_KEY=your_demo_key
DEMO_BINANCE_API_SECRET=your_demo_secret
# Live-money keys (only needed for TRADING_MODE=live):
# BINANCE_API_KEY=your_live_key
# BINANCE_API_SECRET=your_live_secret
```

### Trading modes

A single `TRADING_MODE` setting selects the venue and applies a preset of WebSocket endpoints, heartbeat thresholds, and safety defaults (`config.py::_apply_mode_presets`). **Any value explicitly set in `.env` always wins** — presets only fill unset fields.

| Mode | Launch script | Venue / WS | Behaviour |
|------|---------------|------------|-----------|
| `testnet` | `./start_test.sh` | Futures testnet (`stream.binancefuture.com`) | Real orders / fake money, `MIN_CONFIDENCE=0.1`, **signal injection on**, relaxed heartbeat |
| `demo` | `./start_demo.sh` | demo.binance.com (`fstream.binance.com`) | Paper orders on demo futures, `MIN_CONFIDENCE=0.1`, **signal injection on**, KS-2 disabled; `--dry-run` for synthetic fills |
| `live` | `./start.sh` | Binance Futures (`fstream.binance.com`) | **Real money**, `MIN_CONFIDENCE=0.58`, full risk management, no injection |

### Starting the system

```bash
./start.sh          # live  (TRADING_MODE=live)
./start_demo.sh     # demo  (TRADING_MODE=demo) — add --dry-run for synthetic fills
./start_test.sh     # testnet + signal injection (TRADING_MODE=testnet)
```

Each script runs pre-flight checks before launching:

1. `.env` file present
2. `.venv` activated
3. Not already running (PID guard at `/tmp/cs_engine.pid`)
4. Connectivity test passes (`scripts/test_connection.py`, or `scripts/test_futures_demo.py` for demo)

It then archives the previous log and opens three separate macOS Terminal windows — one per component (on Linux, run the printed commands manually):

| Window | Command | Notes |
|--------|---------|-------|
| LOB Recorder | `python -m core.lob_recorder` | Binance Futures public stream, no API key needed |
| Trading Engine | `python main.py` | Requires the mode's API credentials in `.env` |
| Dash Dashboard | `python dashboard/app.py` | UI at http://127.0.0.1:8050 |

### Stopping the system

```bash
./stop.sh
```

Sends SIGTERM to each process, waits up to 5 seconds, then SIGKILL if still running. Closes the three Terminal windows afterwards.

### Signal-injection execution test

`./start_test.sh` (and `./start_demo.sh`) set `TEST_SIGNAL_INJECT=true`, `MIN_CONFIDENCE=0.1`, `DRY_RUN=false`. After LOB warmup, synthetic LONG/SHORT `SWEEP_WITH_PROTECTION` signals are injected every 30 s to exercise the full execution pipeline. Monitor:

```bash
tail -f logs/cryptosentinel.log | grep -E '\[Injector\]|\[Gate[0-6]\]|\[Executor\]'
```

> `testnet` / `demo` injection is for execution-path validation only — not for paper trading, backtesting, or production.

### Manual startup (alternative)

```bash
# Pick a mode (testnet | demo | live)
export TRADING_MODE=demo

# Verify connectivity and auth
python scripts/test_connection.py

# Terminal 1 — LOB Recorder
python -m core.lob_recorder

# Terminal 2 — Trading engine
python main.py

# Terminal 3 — Dash dashboard
python dashboard/app.py           # → http://127.0.0.1:8050
```

### Run tests

```bash
python -m pytest tests/ -v
```

---

## Configuration Reference

All settings live in `config.py` and can be overridden via `.env`.

### Binance Connection
| Setting | Default | Description |
|---|---|---|
| `TRADING_MODE` | `testnet` | `testnet` / `demo` / `live` — selects the preset applied by `_apply_mode_presets` (WS/REST endpoints, heartbeat, injection). Set via the launch scripts. |
| `BINANCE_API_KEY` | — | Live-money Futures API key (used only by `live` mode / `start.sh`) |
| `BINANCE_API_SECRET` | — | Live-money Futures API secret |
| `DEMO_BINANCE_API_KEY` | — | Demo futures API key (used by `start_demo.sh` / `start_test.sh`) |
| `DEMO_BINANCE_API_SECRET` | — | Demo futures API secret |
| `BINANCE_TESTNET` | `True` | Base default; presets set it `False` for futures testnet/demo/live. Cannot be `True` together with `BINANCE_DEMO`. |
| `BINANCE_DEMO` | `False` | Use demo.binance.com futures credentials (`demo`/`testnet` presets set `True`) |
| `SYMBOL` | `BTCUSDT` | Trading pair (USD-M perpetual) |
| `WS_BASE` | `wss://stream.binancefuture.com` | Trading-engine WebSocket base; presets switch to `wss://fstream.binance.com` for demo/live |
| `REST_BASE` | `https://testnet.binancefuture.com` | Trading-engine REST base; presets switch to `https://fapi.binance.com` for demo/live |
| `LOB_RECORDER_WS` | `wss://stream.binancefuture.com` | Binance Futures public stream for the LOB Recorder (futures testnet by default; presets switch to `fstream.binance.com`) |
| `LOB_RECORDER_REST` | `https://testnet.binancefuture.com` | REST base matching `LOB_RECORDER_WS` (must stay in sync) |

### Strategy
| Setting | Default | Description |
|---|---|---|
| `TIMEFRAME` | `5m` | Kline interval for candle stream |
| `PATTERN_LOOKBACK` | `50` | Candle history for pattern detection — **not used by active code** (`core/pattern_detector.py` absent) |
| `SWING_WINDOW` | `5` | Bars each side for swing pivots — **not used by active code** |
| `MIN_R2` | `0.80` | Minimum trendline R² — **not used by active code** |
| `BREAKOUT_VOL_MULT` | `1.5` | Volume × average for breakout — **not used by active code** |
| `MIN_CONFIDENCE` | `0.58` | Gate 2 confidence threshold |

### LOB / Microstructure
| Setting | Default | Description |
|---|---|---|
| `LOB_WALL_SIGMA` | `2.5` | σ threshold for Wall identification |
| `LOB_WALL_WINDOW` | `5` | Ticks each side for Wall median/std |
| `LOB_DEPTH` | `1000` | Levels used in `get_snapshot()` depth reads (lob_snapshot_writer, legacy engine). `BinanceWebSocketConsumer` hard-codes `_DEPTH_LEVELS = 100`; `LOBRecorder` hard-codes `_DEPTH_LEVELS = 100`. Only relevant when calling `lob_engine.get_snapshot(depth=settings.LOB_DEPTH)`. |
| `LOB_OBI_DEPTH` | `20` | Levels used for OBI calculation |
| `LOB_HISTORY` | `18000` | In-memory bars retained (~5h) |
| `LOB_HEATMAP_BUCKET` | `1.0` | USD bucket width for dashboard heatmap (tightened from 5.0 in v3.1; LOB Recorder writes $1 buckets) |
| `LOB_GAP_RECONNECT_MIN_CONSECUTIVE` | `3` | Reconnect/re-seed the LOB only after this many consecutive update-ID gaps (avoids reconnect storms on transient drift) |
| `RELOAD_SIGMA` | `3.0` | σ threshold for iceberg reload detection — **legacy-engine-only; see TODO** |
| `ICEBERG_WINDOW_MS` | `500` | Lookback window for iceberg replenishment — **legacy-engine-only; see TODO** |
| `ICEBERG_MIN_REPLENISH` | `0.80` | Min reload fraction to confirm iceberg — **legacy-engine-only; see TODO** |
| `ICEBERG_MIN_QTY` | `0.5` | Min absolute qty to qualify as iceberg — **legacy-engine-only; see TODO** |
| `SWEEP_LEVELS` | `5` | Top-N levels checked for sweep volume — **legacy-engine-only; see TODO** |
| `SWEEP_THRESHOLD` | `0.80` | Buy/sell vol fraction threshold — **legacy-engine-only; see TODO** |
| `BREAK_PROTECT_WINDOW_MS` | `2000` | Break+protect window (ms) — **legacy-engine-only; see TODO** |
| `OBI_BREAK_THRESH` | `0.40` | OBI reference level — used by dashboard `/lob` page as a visual reference line only (not a trading gate) |
| `MICRO_MAX_HOLD_MS` | `60_000` | Max hold before forced exit (Gate 6) |
| `MICRO_EXIT_SPREAD_HARD_CAP_BPS` | `12.0` | Spread hard cap for Gate 6 post-entry exit trigger (not Gate 4; Gate 4 uses `EntryRules.spread_max_bps` = 8.0) |
| `LOB_FRESH_WALL_MS` | `3_000` | Protection wall must appear within this window |
| `LOB_STALE_WALL_MS` | `30_000` | Prune wall states not seen for this long |
| `PROTECTION_MAX_DISTANCE_BPS` | `25.0` | Max protection wall distance from mid |
| `PROTECTION_MIN_DISTANCE_BPS` | `1.0` | Min protection wall distance from mid — floors the stop so size stays bounded; rejects degenerate near-mid walls |
| `PRICE_PRUNE_INTERVAL` | `100` | Prune stale price keys every N bars — **legacy-engine-only; see TODO** |
| `PRICE_PRUNE_BAND` | `0.02` | Keep prices within ±2% of current mid — **legacy-engine-only; see TODO** |
| `MICRO_PRICE_MOVE_FLOOR_BPS` | `3.0` | Minimum price move to confirm sweep |
| `MICRO_PRICE_MOVE_WINDOW` | `300` | Rolling window for dynamic price-move threshold |
| `MICRO_PRICE_MOVE_PERCENTILE` | `0.90` | Percentile rank for dynamic threshold |
| `MICRO_PRICE_MOVE_MIN_SAMPLES` | `50` | Min samples before dynamic threshold activates |

### Heartbeat
| Setting | Default | Description |
|---|---|---|
| `HEARTBEAT_WARN_MS` | `200` | Per-packet delta threshold for "degraded" classification (presets relax this to 5000 in testnet/demo) |
| `HEARTBEAT_CRITICAL_MS` | `500` | Per-packet delta threshold for "critical" classification (presets relax to 30000 in testnet/demo) |
| `HEARTBEAT_LOB_CRITICAL_MS` | `30000` | Separate critical threshold for the `depth@500ms` LOB stream (higher due to the aggregation window) |
| `HEARTBEAT_CONSEC_LIMIT` | `3` | Consecutive critical packets required to fire KS-2 (presets raise to 20 in testnet/demo) |
| `HEARTBEAT_KS2_ENABLED` | `True` | Master switch for KS-2 heartbeat killswitch; demo preset sets `False` to tolerate poor WS links |
| `HEARTBEAT_DEGRADED_RATE_THRESH` | `0.5` | ≥50% of 10-msg window → DEGRADED |
| `HEARTBEAT_DEGRADED_RECOVERY_THRESH` | `0.3` | <30% of 10-msg window → recover to HEALTHY (hysteresis) |
| `HEARTBEAT_SUSTAINED_MS` | `10_000` | Duration at DEGRADED rate before SUSTAINED_DEGRADED |

### Risk Engine
| Setting | Default | Description |
|---|---|---|
| `STARTING_EQUITY` | `1_000_000.0` | Design account size (1M USDT futures testnet); **overwritten at startup** by the reconciler's real `futures_account_balance` |
| `MAX_DRAWDOWN_PCT` | `0.05` | 5% drawdown from peak → HALTED |
| `DAILY_LOSS_LIMIT_PCT` | `0.02` | Legacy portfolio daily-loss hard stop (belt-and-suspenders) |
| `TIER_REDUCED_PCT` | `0.005` | ≥ 0.5% DOV loss → REDUCED (50% size, min conf 0.65) |
| `TIER_MINIMAL_PCT` | `0.0075` | ≥ 0.75% DOV loss → MINIMAL (25% size, min conf 0.80) |
| `TIER_PASSIVE_PCT` | `0.009` | ≥ 0.9% DOV loss → PASSIVE (no new entries) |
| `TIER_HALTED_PCT` | `0.01` | ≥ 1.0% DOV loss → HALTED |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 5-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.001` | Equity risked per trade (0.1%) — sized for ~10 bps microstructure stops |
| `KELLY_FRACTION` | `0.25` | Fractional Kelly applied to sizing |
| `ATR_MULTIPLIER_SL` | `1.5` | Stop-loss distance in ATR units |
| `ATR_MULTIPLIER_TP` | `3.0` | Take-profit distance in ATR units |
| `SLIPPAGE_RESEARCH_BPS` | `3.0` | Expected slippage (KS-3 baseline) |
| `SLIPPAGE_MULTIPLIER` | `1.5` | KS-3 fires above `research × multiplier` |

### Execution
| Setting | Default | Description |
|---|---|---|
| `DRY_RUN` | `True` | Skip live order submission (presets set `False`; `start_demo.sh --dry-run` forces `True`) |
| `LIQUIDATE_BTC_ON_STARTUP` | `False` | Spot-only legacy guard. Kept `False` on futures so SHORT entries are not blocked; futures accounts hold no spot BTC. |
| `IOC_TIMEOUT_MS` | `200` | IOC order expiry — do not retry |
| `MAX_ORDER_NOTIONAL_PCT` | `0.90` | Gate 3 rejects if estimated notional exceeds 90% of equity |
| `QTY_STEP_SIZE` | `0.001` | BTCUSDT perpetual-futures LOT_SIZE `stepSize`; all quantities are floored to this grid |
| `PRICE_TICK_SIZE` | `0.10` | BTCUSDT perpetual-futures price tick; limit prices are rounded to this grid |
| `MIN_NOTIONAL` | `100.0` | BTCUSDT NOTIONAL filter minimum (USD); orders below this are rejected pre-submission |
| `WS_RECV_TIMEOUT_S` | `20.0` | Max seconds between WS messages before forcing a reconnect (presets relax to 30–60) |
| `WS_PING_TIMEOUT_S` | `20.0` | Max seconds awaiting a WS pong before reconnect (presets relax to 30–60) |
| `WS_PING_INTERVAL_S` | `20.0` | Seconds between WS pings |

### Logging
| Setting | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Root log level — set `DEBUG` in `.env` for full gate-input trace |
| `LOG_FILE` | `logs/cryptosentinel.log` | Rotating log file path |
| `LOG_MAX_BYTES` | `5_000_000` | Max bytes per log file before rotation (5 MB) |
| `LOG_BACKUP_COUNT` | `5` | Rotated files retained (~25 MB total) |

### Speed Bumps
| Setting | Default | Description |
|---|---|---|
| `MIN_SIGNAL_INTERVAL_MS` | `0` | Min ms between signal approvals (0 = disabled) |

### Test Harness
| Setting | Default | Description |
|---|---|---|
| `TEST_SIGNAL_INJECT` | `False` | Enable synthetic signal injection — only active via `start_test.sh` |
| `TEST_INJECT_INTERVAL_MS` | `30_000` | ms between injected synthetic signals |

### Persistence
| Setting | Default | Description |
|---|---|---|
| `REGISTRY_DB` | `strategies/registry.db` | Strategy registry + signal telemetry |
| `LOB_TICK_DB` | `data/lob_tick.db` | LOB Recorder tick data |
| `BACKTEST_RESULTS_DB` | `data/backtest_results.db` | Walk-forward backtest results storage |

### Dashboard
| Setting | Default | Description |
|---|---|---|
| `DASHBOARD_API_PORT` | `8080` | Port for aiohttp REST API (`/api/health`, `/api/portfolio`, `/api/killswitch`) |

### Alerting
| Setting | Default | Description |
|---|---|---|
| `ALERT_WEBHOOK_URL` | `""` | Optional webhook URL for kill-switch alerts (empty = disabled) |

---

## Tests

```
Phase 1 — Live trading engine
tests/test_config.py                      5 tests  — TRADING_MODE preset application, explicit-env precedence, tier-ordering validation
tests/test_risk_engine.py                20 tests  — sync_tier transitions, tier callback, record_trade_result, mark_unrealised, circuit breakers
tests/test_budget.py                      6 tests  — DailyBudget remaining, loss_pct, midnight reset
tests/test_killswitch.py                 12 tests  — KS-1 budget, KS-2 heartbeat, KS-3 slippage triggers
tests/test_microstructure_engine.py      30 tests  — legacy microstructure engine
tests/test_executor.py                   46 tests  — all 7 gates, telemetry emission, rate-limit pacing
tests/test_order_manager.py              37 tests  — IOC entry, futures TP/SL bracket, fill handling, Gate 6 race
tests/test_lob_engine.py                 22 tests  — state machine, gap detection, wall scan
tests/test_startup_reconciler.py         16 tests  — reconciliation, midnight reset
tests/test_microstructure.py             27 tests  — wall identification, absorption, sweep, qty_peak
tests/test_lob_recorder.py               30 tests  — recorder flush, reconnect, $1 bucketing, stats
tests/test_features.py                   14 tests  — Welford, no-lookahead, VWAP reset
tests/test_cvd.py                        15 tests  — buy/sell CVD, 5-bar delta, std
tests/test_db_writer.py                   9 tests  — SQLite write, upsert, purge
tests/test_signal_telemetry.py           23 tests  — flush, batch, timeout, outcome update
tests/test_orders.py                     14 tests  — futures order classes and enums
tests/test_ws_consumer.py                15 tests  — heartbeat states, rate thresholds, hysteresis, sustained-degraded
tests/test_models.py                      6 tests  — PortfolioState, WallState, FeatureVector
tests/test_sizing.py                      7 tests  — clamp_stop_bps bounds
tests/test_user_data_stream.py           14 tests  — listenKey lifecycle, executionReport dispatch, reconnect
tests/test_user_data_stream_bootstrap.py  6 tests  — bootstrap open positions from executionReport stream
tests/test_integration.py                 3 tests  — end-to-end signal → execution pipeline

Phase 2 — Backtesting + strategy lifecycle
tests/test_bt_event_engine.py            24 tests  — event-driven engine: tiers, exits, full run
tests/test_registry.py                   29 tests  — lifecycle gates, YAML roundtrip, promotion
tests/test_builder.py                     4 tests  — StrategyBuilder walk-forward metrics + build pipeline
tests/test_spec.py                        3 tests  — StrategySpec to_dict / from_dict roundtrip
tests/test_bt_tick_replay.py             16 tests  — tick replay fidelity, streaming, CVD reset
tests/test_bt_walk_forward.py             9 tests  — window splits, OOS isolation, leaderboard
tests/test_bt_metrics.py                 10 tests  — Sharpe, MDD, PF, composite score
tests/test_validator.py                   7 tests  — null repair, duplicate removal, gap detection
tests/test_scorer.py                      7 tests  — XGBoost train, AUC, save/load, fallback
tests/test_fetcher.py                     6 tests  — OHLCV fetch, cache, resample
tests/test_bt_signals.py                  7 tests  — signal arrays, no-lookahead, SL/TP NaN
tests/test_bt_vectorbt.py                 6 tests  — Optuna optimisation, sensitivity
tests/test_bt_costs.py                    5 tests  — round-trip cost, maker/taker, zero qty

Phase 3 — REST API + LOB snapshot writer + Dash dashboard
tests/test_rest_api.py                   11 tests  — /api/health, /api/portfolio, /api/killswitch, /api/session
tests/test_lob_snapshot_writer.py         9 tests  — snapshot writer, rolling cap, WAL mode
tests/test_realtime_hub.py                6 tests  — RealtimeHub fan-out, connection drop, /ws/lob endpoint
tests/test_dashboard_live.py             10 tests  — /live page callback, engine badge, kill switch, signal funnel
tests/test_dashboard_lob.py              16 tests  — /lob buffer helpers, CVD accumulation, dedup, heatmap
tests/test_dashboard_strategies.py        5 tests  — /strategies merged backtest leaderboard + registry lifecycle
tests/test_dashboard_registry.py          4 tests  — registry lifecycle display helpers
tests/test_dashboard_walls.py             6 tests  — /walls page callback, trace structure, invalid-JSON guard
tests/test_alerting.py                    3 tests  — AlertDispatcher webhook, empty-URL guard
tests/test_portfolio_broadcast.py         7 tests  — /ws/portfolio WebSocket fan-out
tests/test_signal_broadcast.py            9 tests  — /ws/signals WebSocket fan-out
──────────────────────────────────────────────────────────────────────────────
Total                                   591 tests across 46 files
```

---

## Path to Paper Trading

The trading engine is fully operational on Binance USD-M Futures (testnet / demo). The remaining work before enabling live paper trading (`DRY_RUN=False` in `live` mode) is data accumulation and strategy validation:

| Step | Action | Status |
|------|--------|--------|
| **1. Accumulate LOB data** | LOB Recorder is collecting at `btcusdt@depth@100ms` (incremental diff, 100 levels, $1-bucket aggregation). Keep `lob_recorder` running continuously. Target ≥30 days for statistically robust walk-forward splits. | ✅ Done (recorder operational) |
| **2. Fetch OHLCV history** | Run `data/fetcher.py` to populate `ohlcv_cache.db` for Path B backtesting | 🔜 Pending |
| **3. Run backtests** | Path A: `backtesting/event_engine.py` replay of `lob_tick.db`. Path B: `backtesting/walk_forward.py` OHLCV walk-forward via VectorBT + Optuna | 🔜 Pending |
| **4. Tune strategy config** | Optimise wall sigma (`LOB_WALL_SIGMA`), confidence threshold (`MIN_CONFIDENCE`), ATR multipliers via Optuna. Evaluate Sharpe, Sortino, MDD, PF across out-of-sample windows. | 🔜 Pending |
| **5. Train XGBoost scorer** | `strategy/scorer.py` — `XGBoostScorer.train_from_registry()` trains on APPROVED `signal_records`; `ScorerFactory` auto-loads at startup, falls back to `RuleBasedScorer` if no pkl exists. AUC gate ≥ 0.62, ECE logged, staleness warning after 30 days. | ✅ Done |
| **6. Freeze StrategySpec** | `strategy/spec.py` + `strategy/registry.py` + `strategy/builder.py` — `StrategyBuilder` produces BACKTEST-status specs; `StrategyRegistry` dual-stores to YAML + SQLite with 4-gate PAPER promotion (≥50 OOS trades, Sharpe ≥ 1.0, MDD ≤ 15%, PF ≥ 1.3) and 3-gate LIVE promotion. | ✅ Done |
| **7. Promote to paper trading** | Set `DRY_RUN=False` in `.env`. Monitor Gate funnel and PnL via the Dash dashboard (`dashboard/app.py`). | 🔜 Pending |

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Runtime | Python | 3.13+ | Async, type hints |
| Async | asyncio | stdlib | Event loop |
| LOB Recorder WS | websockets | 10.4 | Binance Futures public stream |
| Exchange WS/REST | python-binance | 1.0.36 | USD-M Futures execution (testnet / demo / live) |
| Historical data | ccxt | 4.3+ | OHLCV fetch (no key) |
| Data | pandas + numpy | 3.0.3 + 2.4.4 | DataFrames + arrays |
| Stats | scipy | 1.14+ | linregress, statistics |
| ML scorer | xgboost | 2.1+ | Confidence scoring (Phase 2) |
| Backtest fast | vectorbt | 0.25+ | OHLCV Path B (Phase 2) |
| Optimiser | optuna | 3.6+ | Bayesian param search (Phase 2) |
| Config | pydantic-settings | 2.3.x | .env management |
| Dashboard | Dash + dash-bootstrap | 2.17 + 1.6 | All UI pages (Phase 3) |
| Charts | plotly | 5.22.x | All visualisations |
| Persistence | SQLite | stdlib | All databases |
| Logging | Python stdlib logging | 3.13+ | RotatingFileHandler, INFO/DEBUG configurable via `LOG_LEVEL` |
| Serialisation | PyYAML | 6.0.1 | StrategySpec configs |

---

## System Invariants (Coherence Rules)

These rules are invariants. Any code that violates them is incorrect.

| Rule | Invariant |
|------|-----------|
| **Rule 1** | `FeatureComputer` is one class. Called identically in live trading, tick backtest, and OHLCV backtest. No separate calculation paths. |
| **Rule 2** | Microstructure fires, pattern boosts. A chart pattern alone never triggers a trade. |
| **Rule 3** | Risk budget is a shared pool. All open pyramid legs draw from the same daily budget. Unrealised PnL counts against the budget. |
| **Rule 4** | LOB Recorder uses the Binance Futures public market-data stream (`fstream.binance.com` in `demo`/`live`), not the trading-account connection. Recording from futures *testnet* (`stream.binancefuture.com`) is valid only for infrastructure smoke tests — backtesting on testnet LOB data is meaningless. |
| **Rule 5** | DOV (Daily Open Value) is fixed at UTC midnight. Never recalculated intraday. |
| **Rule 6** | StrategySpec is immutable after PAPER promotion. Changes require a new version number. Old YAML is never deleted. |
| **Rule 7** | Tick backtest must simulate level-specific Wall interactions using `lob_tick.db`. No OHLCV proxies for microstructure parameter tuning. |
| **Rule 8** | Every evaluated signal must be written to `signal_records`. Rejected signals are as important as approved ones. No gate exit without a `SignalRecord`. |
| **Rule 9** | No rolling z-scores with NaN-fill in FeatureComputer. All z-scores use `WelfordOnline`. The pattern `rolling(N).std().fillna(series.std())` is forbidden. |
| **Rule 10** | No market orders for microstructure entries. All entries use aggressive IOC limit orders (`best_ask + spread × 0.5`). |
| **Rule 11** | HeartbeatMonitor runs on every WebSocket message — not sampled, not batched. `heartbeat_status` must be available to Gate 0 and GlobalKillswitch without additional async calls. |
| **Rule 12** | GlobalKillswitch cannot be bypassed. Once `is_active == True`, no new orders may be placed. Manual override requires system restart — not a config flag. |
| **Rule 13** | LOB data used for strategy validation must be collected at depth@100 or deeper, with price-bucket aggregation for storage efficiency. depth@20 data spans only $2–4 from mid at BTC prices; all walls are within the transaction cost floor and no signal can be net profitable. depth@20 data is valid only for infrastructure smoke tests. |

---

## Phased Delivery

| Phase | Weeks | Goal | Status |
|-------|-------|------|--------|
| **Phase 1A–1K** | 1–3 | Foundation: LOB Recorder, HeartbeatMonitor, LOB state machine, FeatureComputer, Wall/Absorption/Sweep signals, 7-Gate executor, 5-tier Risk Engine, DailyBudget, PyramidController, GlobalKillswitch, signal telemetry | ✅ Done |
| **Phase 1L–1N** | 3 | Foundation: IOC limit orders (1L), startup reconciler + midnight reset (1M), integration test + killswitch wire-up (1N) | ✅ Done |
| **Phase 2A–2F** | 4–6 | Backtesting infrastructure: data layer, costs, metrics, signal generator, VectorBT+Optuna, walk-forward orchestrator (OHLCV Path B complete) | ✅ Done |
| **Phase 2G** | 6 | Tick Replay Engine: `backtesting/tick_replay.py` — event-driven replay of `lob_tick.db` through live feature/signal stack; fidelity test passes | ✅ Done |
| **Phase 2H** | 6 | XGBoost Confidence Scorer: `strategy/scorer.py` — trains on APPROVED `signal_records`, AUC ≥ 0.62 gate, ECE calibration, staleness detection, wired into Gate 2 via `ScorerFactory` | ✅ Done |
| **Phase 2I** | 7 | Strategy Registry: `strategy/spec.py`, `registry.py`, `builder.py` — full lifecycle RESEARCH→BACKTEST→PAPER→LIVE with dual-store (YAML+SQLite), 4-gate PAPER promotion, 3-gate LIVE promotion | ✅ Done |
| **Phase 2J** | 7–8 | Strategy config tuning: accumulate ≥30 days of `depth@100` LOB data, run walk-forward backtests, tune params, run tick replay validation, promote first spec to PAPER | 🔜 Next |
| **Phase 3** | 9–12 | Dashboard: Dash multi-page app (5 pages: /live, /lob, /walls, /strategies, /config — backtest + registry merged into /strategies), REST API, LOB snapshot writer, decay monitoring, LIVE promotion pipeline | ✅ Done |

---

## TODO

- [x] Write up strategy documentation — entry logic, gate rationale, Wall/Absorption/Sweep signal design (`documentation/strategy.md`)
- [x] Write up model documentation — FeatureComputer inputs, XGBoost scorer architecture, training pipeline including component interaction diagram (`documentation/model.md`)
- [x] Write up trading algorithm documentation — end-to-end flow from LOB tick to order submission (`documentation/algorithm.md`)
- [x] Fix gate 3 issue; risk management is not using live position data. need to ensure BTC, USDT data is being passed through
- [x] Fix one test case. make sure it uses the correct notional
- [ ] check market bubbles in dashboard. make sure it is top 1 percentile volume for the whole heatmap window. otherwise might be better to have even higher threshhold up to 99.5 percentile contrast
- [ ] need to test the live dashboard works. 
    1. ensure kill switch and stop trading button available
    2. ensure pnl works
    3. ensure drawdown working and available
- [ ] Consider placing a minimum-quantity resting order behind/after a significant liquidity wall — a fill on that order signals the wall has been consumed, providing a cleaner consumption trigger than depth-diff heuristics. Quantity must be as small as possible (min `QTY_STEP_SIZE` on Binance USD-M Futures).
- [ ] run through start_test.sh and make sure the trade execution with injector is working. trades work now. make sure its tracked in live dashboard
- [ ] troubleshoot and refine strategy
- [ ] troubleshoot and refine model
- [ ] troubleshoot and refine algorithm
#### check there is controls in all critical processes 
- [ ] check there is controls in Market Data Gateway. no 0.0 or infty values or extreme or non-numeric values
- [ ] check there is controls in all aggregator. for example, make sure bid < ask and other exchange assumptions need to be validated
- [ ] check there is controls in all order management. authorisation, is the correct person sending the order? make sure the right module sends the order.
- [ ] check there is controls in all order gateway. ensure rate limit if there is something is wrong with model executions


### Lead-quant backlog

- [ ] **Dead code — MicrostructureEngine**: `engine/microstructure_engine.py` (~410 lines) is not started in `main.py`'s TaskGroup and has no live producer. `ms_bar_queue` is orphaned. Either wire it into the live path or delete it — currently it creates confusion about the actual signal source.
- [ ] **Dead code — engine shims**: `engine/lob_engine.py`, `engine/risk_engine.py`, `engine/websocket_consumer.py`, `engine/order_manager.py` are one-line re-export shims. No external code should depend on them; remove the shims and update any remaining importers.
- [ ] **Redundant config — pattern settings**: `PATTERN_LOOKBACK`, `SWING_WINDOW`, `BREAKOUT_VOL_MULT`, `MIN_R2` in `config.py` have no consumer in the live codebase. Remove or annotate clearly as reserved for a future pattern module.
- [ ] **Unused FeatureVector fields**: `pattern_r2` and `protection_wall_present` are always 0 in the live path — `executor.py` never passes them into `FeatureComputer.compute()`. XGBoost trains on these zero-variance columns. Either wire the inputs or drop the features from `FEATURE_ORDER` and retrain.
- [ ] **Gate 4 spread check is duplicated**: `gate_4_order_selection()` checks both `EntryRules.spread_max_bps` (8.0) and `spread_p95 × 2`. The hard cap and the adaptive cap serve the same purpose; consolidate into a single threshold derived from session p95 data with a sensible floor.
- [ ] **Position sizing ignores remaining_budget**: `notional_hint` is computed from `confidence × KELLY × RISK_PCT × tier_scalar` without consulting `DailyBudget.remaining`. A run of near-threshold trades could drain the budget silently — add a budget-fraction cap to `notional_hint` in `gate_3_position_size`.
- [ ] **Test coverage gap — StrategyExecutor + RiskEngine integration**: Gate 3 capital gate and tier-elevated confidence are tested in isolation but no test exercises the full `StrategyExecutor → OrderManager → fill_processor` path under a non-FULL risk tier. Add at least one integration scenario for REDUCED/MINIMAL tier flow.
- [x] **Test coverage gap — HeartbeatMonitor SUSTAINED_DEGRADED**: `test_ws_consumer.py` now includes `test_heartbeat_sustained_degraded` and `test_heartbeat_recovery_from_sustained` covering the 10 s transition and hysteresis band recovery (12 tests total).
- [x] **Test coverage gap — RiskEngine throttle and circuit breakers**: `test_risk_engine.py` expanded from 8 → 20 tests covering the 5-tier ladder, drawdown circuit breaker, consecutive-loss cooldown, and budget-loss tier thresholds.
- [ ] **Dead config — CANDLE_INTERVAL**: `config.py` defines `CANDLE_INTERVAL: str = "1s"` with the comment "legacy — ws_consumer uses this", but `ws_consumer.py` never imports or reads this setting (it uses `TIMEFRAME` for klines). Remove the setting or delete it before more code takes a dependency on it.
- [ ] **Dead config — legacy-engine-only settings**: `SWEEP_THRESHOLD`, `SWEEP_LEVELS`, `BREAK_PROTECT_WINDOW_MS`, `ICEBERG_PRICE_TOL`, `BOOK_FLIP_SIGMA`, `BOOK_FLIP_MIN_CONSUMED`, `BOOK_FLIP_AGG_RATIO`, `BREAK_MIN_VOL`, `RELOAD_SIGMA`, `ICEBERG_WINDOW_MS`, `ICEBERG_MIN_REPLENISH`, `ICEBERG_MIN_QTY`, `PRICE_PRUNE_INTERVAL`, `PRICE_PRUNE_BAND` are defined in `config.py` but consumed exclusively by `engine/microstructure_engine.py` (the legacy engine that is not started in the live path). Note: `OBI_BREAK_THRESH` remains in the list above but IS used — it drives a reference line in `dashboard/pages/lob.py` (visual only, not a trading gate). Removing the legacy engine removes all consumers for the other settings listed; delete them at that point.
- [ ] **STARTING_EQUITY duplication**: `main.py` defines `STARTING_EQUITY = 10_000.0` as a module-level constant instead of reading `settings.STARTING_EQUITY`. If someone sets `STARTING_EQUITY` in `.env`, the main orchestrator ignores it — only `backtesting/event_engine.py` picks it up. Consolidate: replace the `main.py` constant with `settings.STARTING_EQUITY` so the value is controlled from a single source.
- [ ] **Absorption prerequisite adds false negatives**: `gate_1_microstructure()` hard-gates on `prior_absorption == True`. A wall that appears and is immediately consumed (e.g. within the first 500ms of its first_seen_ts) will always be rejected — absorption can never arm a wall that is gone before it persists. At the 100ms tick rate this blocks genuine fast institutional sweeps of newly-posted deep liquidity. Evaluate demoting Absorption from a Gate 1 hard prerequisite to a Gate 2 scoring bonus (e.g. `absorption_ratio > 0 → +0.10` in `RuleBasedScorer`) to recover these signals without opening a false-positive flood.
- [ ] **Dual-store registry is over-engineered for a single-strategy system**: `strategy/registry.py` writes every spec to both a YAML file and a SQLite `strategies` table. For the current single-strategy deployment, a single SQLite store would suffice; the YAML mirror adds sync risk (YAML written first, then DB — a crash between the two leaves them inconsistent). Consolidate into SQLite-only in `strategy/registry.py` and `strategy/builder.py`, retaining YAML export as an explicit `export()` method for human review.
- [ ] **Low-signal features in FEATURE_ORDER**: `strategy/scorer.py` trains XGBoost on all 15 features including `pattern_r2` (always 0.0 in live path — `strategy/executor.py` never passes it to `FeatureComputer.compute()`), `vwap_reclaim` (binary flag, rarely 1 on a per-tick basis), and `rsi_value` (14-bar candle momentum on a 5m timeframe — coarse relative to 100ms microstructure signals). After first XGBoost training run, call `scorer.feature_importances()` and prune any feature with importance < 0.01 from `FEATURE_ORDER`; retrain and compare AUC.
- [ ] **Parallel depth consumers double memory pressure**: `main.py` line 492 fans out each reconstructed depth snapshot to both `lob_depth_queue` (consumed by `_run_lob_engine`) and `ms_depth_queue` (consumed by `MicrostructureDetector`). Both consumers parse the same `{"bids": [...], "asks": [...]}` dict independently. Evaluate whether `MicrostructureDetector` could subscribe to `LocalOrderBook`'s processed output instead of the raw diff queue, halving snapshot copies in memory at the cost of adding a processing dependency.
- [x] **Test coverage gap — strategy/builder.py and strategy/spec.py**: `tests/test_builder.py` (4 tests — walk-forward metrics + build pipeline) and `tests/test_spec.py` (3 tests — `to_dict()`/`from_dict()` roundtrip) now exist.
- [ ] **Gate 3 capital gate has a belt-and-suspenders budget check**: `gate_3_capital()` (`strategy/executor.py` lines 75–79) checks both `tier in ("HALTED", "PASSIVE")` and `budget.remaining <= 0`. These are not independent — when DOV loss reaches `TIER_HALTED_PCT`, `RiskEngine._check_circuit_breakers()` (`risk/engine.py` lines 82–86) sets `tier = HALTED`, so the tier check would already reject. The `budget.remaining <= 0` path only fires during the ~1-second gap before the next MTM loop tier sync. Evaluate whether increasing the MTM loop frequency or making the tier sync synchronous on budget update would allow the `budget.remaining` check to be removed.
- [x] **Test coverage gap — risk/budget.py and risk/killswitch.py**: `tests/test_budget.py` (6 tests — `remaining`, `loss_pct`, midnight reset) and `tests/test_killswitch.py` (12 tests — KS-1 budget, KS-2 heartbeat, KS-3 slippage) now exist as dedicated unit suites.
