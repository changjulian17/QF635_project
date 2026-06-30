# CryptoSentinel v3.0

Real-time granular LOB microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

Detects institutional **Liquidity Walls**, observes **Absorption** and **Book Sweep** events at specific price levels, and fires trades only when a Wall is consumed and Fresh Protective Liquidity appears behind the breakout — confirmed through a 7-gate execution pipeline with full signal telemetry.

---

## System Philosophy

> *"Price movement is a function of aggressive market orders consuming specific, persistent liquidity levels — not the aggregate balance of the book."*

CryptoSentinel v3.0 is built on a granular, level-specific insight: **price moves when aggressive orders hit resting liquidity walls and successfully consume them**. Aggregate OBI is retained as a secondary sentiment filter only.

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
  XGBoostScorer (falls back to RuleBasedScorer when no trained artifact)
  confidence ≥ 0.58 required to proceed
           │
           ▼
  7-Gate Trade Life Cycle → Testnet Execution
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
│   └── builder.py             # StrategyBuilder — 3-stage pipeline: load metrics → validate → register
│
├── risk/                      # Risk management
│   ├── engine.py              # RiskEngine — 5-tier throttling, DOV, circuit breakers
│   ├── budget.py              # DailyBudget — shared pool for all open positions
│   └── killswitch.py          # GlobalKillswitch — Budget / Heartbeat / Slippage triggers
│
├── execution/                 # Order management
│   ├── order_manager.py       # IOC aggressive limit orders + OCO brackets
│   └── orders.py              # Order domain classes and enums
│
├── backtesting/               # Offline strategy research
│   ├── tick_replay.py         # Path A: event-driven replay of lob_tick.db through live stack
│   ├── event_engine.py        # Path B: candle-by-candle OHLCV backtesting engine
│   ├── signals.py             # Signal generation for backtesting
│   ├── walk_forward.py        # Path B: OHLCV walk-forward (VectorBT + Optuna)
│   ├── vectorbt_runner.py     # VectorBT execution wrapper
│   ├── metrics.py             # Sharpe, Sortino, Calmar, MDD, PF, WR
│   └── costs.py               # Transaction cost model
│
├── data/                      # Data acquisition and storage
│   ├── fetcher.py             # OHLCVFetcher (CCXT + SQLite cache)
│   ├── validator.py           # 9-check data quality validator
│   ├── lob_tick.db            # LOB Recorder output (real Binance data — live-writing)
│   ├── ohlcv_cache.db         # OHLCV SQLite cache (generated — pending Paper Trading step 2)
│   └── backtest_results.db    # Walk-forward results storage (generated — pending Paper Trading step 3)
│
├── dashboard/                 # Dash multi-page application (Phase 3)
│   ├── app.py                 # Entry point — dark theme, nav, engine status badge
│   ├── _db.py                 # WAL-mode SQLite helpers shared by all pages
│   ├── _logic.py              # Shared business logic for dashboard pages
│   ├── _utils.py              # Shared Plotly utilities (empty_fig)
│   └── pages/
│       ├── live.py            # /live     — Portfolio metrics, signal funnel, kill switch
│       ├── lob.py             # /lob      — LOB heatmap + CVD + OBI + spread subplots
│       ├── walls.py           # /walls    — 1s candlestick + rolling VWAP + liquidity wall heatmap
│       ├── backtest.py        # /backtest — Strategy leaderboard from backtest results
│       ├── registry.py        # /registry — Strategy lifecycle, decay monitoring, LIVE promotion
│       └── config.py          # /config   — Settings reference, emergency stop, event log
│
├── strategies/                # Strategy artifacts
│   ├── registry.db            # SQLite: signal_records + system events
│   └── {name}_v{version}.yaml # Frozen YAML strategy specs (written directly to strategies/ by StrategyRegistry)
│
├── engine/                    # Shared persistence, snapshot, and hub components
│   ├── db_writer.py           # SQLite persistence + rolling cleanup + lob_snapshots table
│   ├── lob_snapshot_writer.py # LOB snapshot writer coroutine (~1 Hz, lob_snapshots table)
│   ├── realtime_hub.py        # RealtimeHub — fan-out JSON pushes to WebSocket clients
│   └── microstructure_engine.py # Legacy MicrostructureEngine (NOT started in live path; unit tests only)
│
├── scripts/
│   ├── test_connection.py     # Connectivity + auth check
│   ├── test_orders.py         # BUY + SELL round-trip test
│   ├── run_backtest.py        # CLI for tick-level walk-forward backtest (writes to backtest_results.db)
│   └── signal_injector.py     # Synthetic signal injection — dev/testnet only (start_test.sh)
│
├── tests/                     # pytest unit + integration tests (510 total)
│   │                          # Phase 1 — live trading engine
│   ├── test_models.py
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
│   ├── test_startup_reconciler.py
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
│   ├── test_scorer.py
│   ├── test_validator.py
│   │                          # Phase 3 — dashboard + REST API + LOB snapshot writer
│   ├── test_alerting.py
│   ├── test_dashboard_live.py
│   ├── test_dashboard_lob.py
│   ├── test_dashboard_registry.py
│   ├── test_dashboard_walls.py
│   ├── test_lob_snapshot_writer.py
│   ├── test_portfolio_broadcast.py
│   ├── test_realtime_hub.py
│   ├── test_rest_api.py
│   └── test_signal_broadcast.py
│
```

---

## Data Pipeline

### Stream Selection

The LOB Recorder subscribes to `btcusdt@depth@100ms` — the **incremental diff-depth stream** — and maintains a full local order book seeded from a REST `depth?limit=1000` snapshot on each connect. Buffered diffs received during the REST fetch are merged in sequence-ID order before the book is declared SYNCED. The top 100 levels per side are retained (`_DEPTH_LEVELS = 100`) and aggregated into $25 USD price buckets (`_BUCKET_WIDTH = 25.0`) before writing to `lob_tick.db`. At typical BTC prices, 100 levels span ~$50–200 from mid — sufficient range to detect deep-book institutional walls above the transaction cost floor (~$115 at 30bps round-trip).

| Stream | Purpose | Update Rate |
|--------|---------|-------------|
| `btcusdt@aggTrade` | CVD · aggressive volume | Per taker sweep |
| `btcusdt@depth@100ms` | Wall detection · OBI · spread — incremental diff, 100 levels; LOB Recorder aggregates to $25 buckets before writing to `lob_tick.db` (live engine processes raw levels) | 100ms |
| `btcusdt@bookTicker` | Heartbeat tracking only; raw dict routed to `trade_queue` but discarded by `MicrostructureDetector._collect_trades()` — spread is computed from the reconstructed depth snapshot | Real-time |
| `btcusdt@kline_5m` | OHLCV candles for FeatureComputer (VWAP, ATR, RSI, volume) | On close (5m) |

### Async Queue Architecture

```
Queue               Producer                   Consumer                   maxsize
──────────────────────────────────────────────────────────────────────────────────
trade_queue         WS Consumer (testnet)      MicrostructureDetector     5,000
raw_depth_queue     WS Consumer (testnet)      _depth_fanout              5,000
lob_depth_queue     _depth_fanout              LocalOrderBook             5,000
ms_depth_queue      _depth_fanout              MicrostructureDetector     5,000
candle_queue        WS Consumer (testnet)      FeatureComputer            200
candle_db_queue     WS Consumer (testnet)      DBWriter                   200
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
| **Gate 3** | Capital Gate | `remaining_budget > 0` AND `tier not in (HALTED, PASSIVE)` AND `no active exposure` AND `estimated_notional <= MAX_ORDER_NOTIONAL_PCT × equity` | `GATE_3_FAIL: BUDGET_EXHAUSTED` or `GATE_3_FAIL: RISK_TIER_{tier}` or `GATE_3_FAIL: ACTIVE_EXPOSURE` or `GATE_3_FAIL: NOTIONAL_EXCEEDED` |
| **Gate 4** | Order Selection | `spread_bps <= EntryRules.spread_max_bps` (8.0 bps hard cap) AND `spread_bps <= spread_p95 × 2` | `GATE_4_FAIL: SPREAD_TOO_WIDE` |
| **Gate 5** | Execution Sync | `signal_age < 200ms` AND `last_delta < HEARTBEAT_CRITICAL_MS` (500ms) | `GATE_5_FAIL: SIGNAL_STALE` |
| **Gate 6** | Persistence Monitor | Protection wall present AND hold time < `MICRO_MAX_HOLD_MS` AND heartbeat not `(CRITICAL, SUSTAINED_DEGRADED)` AND spread < `MICRO_EXIT_SPREAD_HARD_CAP_BPS` (post-entry) | `GATE_6_ALERT: PROTECTION_WALL_REMOVED` or `GATE_6_ALERT: MAX_HOLD` or `GATE_6_ALERT: LATENCY_CRITICAL` or `GATE_6_ALERT: SPREAD_HARD_CAP` |

Gate 6 is the only post-entry gate. It runs as an async task after fill confirmation and triggers early exit when the protection wall is removed, the position hold time exceeds `MICRO_MAX_HOLD_MS`, heartbeat becomes `CRITICAL` or `SUSTAINED_DEGRADED`, or exit spread exceeds `MICRO_EXIT_SPREAD_HARD_CAP_BPS`.

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

Connects to `wss://stream.binance.com:9443` (real Binance public stream — no API key). Records raw tick data to `data/lob_tick.db` for backtesting Path A. Separate from the testnet trading connection per Rule 4.

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
| `wall_detected` ★ | 1 if any tracked Wall (>2.5σ) is currently present (identified using ±`LOB_WALL_WINDOW` surrounding levels) |
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
sl_distance   = signal_price × min(protection_wall_bps, PROTECTION_MAX_DISTANCE_BPS) / 10,000
qty           = floor((equity × notional_hint / sl_distance) / QTY_STEP_SIZE) × QTY_STEP_SIZE
```
`notional_hint` is computed in `StrategyExecutor` (Gate 2). Final qty is computed in `OrderManager._submit_aggressive_limit`.

**Circuit Breakers:**
```
Trigger 1: daily_loss ≥ 1% of DOV          → HALTED
Trigger 2: drawdown ≥ 5% from peak          → HALTED
Trigger 3: legacy daily_loss_pct ≥ 2%       → HALTED (belt-and-suspenders, DAILY_LOSS_LIMIT_PCT)
Trigger 4: 3 consecutive losses              → PAUSED (5 min cooldown)
Trigger 5: daily_loss ≥ 0.5%                → Tier REDUCED
Trigger 6: daily_loss ≥ 0.75%               → Tier MINIMAL
```

### Global Killswitch (`risk/killswitch.py`)

Hard override that bypasses all other logic. Once fired, requires system restart to clear. Executes CLOSE_ALL immediately: cancel all orders, market-close all positions, write `KILLSWITCH_FIRED` to `registry.db`.

| Trigger | Condition |
|---------|-----------|
| **KS-1 Budget Breach** | `(realised_pnl + unrealised_pnl) < −(DOV × 1%)` |
| **KS-2 Heartbeat Loss** | `heartbeat_status == CRITICAL` (>500ms × 3 packets) |
| **KS-3 Slippage Decay** | Rolling 20-trade avg slippage > `research_bps × 1.5` |

### Order Manager (`execution/order_manager.py`)

All entries use **IOC aggressive limit orders** — never market orders (Rule 10):
```
LONG:  limit_price = best_ask + (spread × 0.5)   # cross half the spread
SHORT: limit_price = best_bid − (spread × 0.5)

Time-in-force: IOC — cancel if not filled immediately
If expired: do NOT retry — signal is stale
```

After fill: OCO bracket placed (take-profit limit + stop-limit).

### Signal Telemetry (`core/signal_telemetry.py`)

Every signal evaluation — whether it passes all gates or is rejected at Gate 0 — is written to `signal_records` in `registry.db`. Flush every 50 records or 10 seconds. Enables gate funnel analysis, per-signal-type performance, and strategy improvement.

---

## Quick Start

Requires **Python 3.13+**.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add testnet API keys to `.env`:
```
BINANCE_API_KEY=your_testnet_key
BINANCE_API_SECRET=your_testnet_secret
```

### Starting the system

```bash
./start.sh
```

`start.sh` runs three pre-flight checks before launching:

1. `.env` file present
2. `.venv` activated
3. Connectivity test (`scripts/test_connection.py`) passes

If all checks pass, it opens three separate Terminal windows — one for each component:

| Window | Command | Notes |
|--------|---------|-------|
| LOB Recorder | `python -m core.lob_recorder` | Real Binance public stream, no API key needed |
| Trading Engine | `python main.py` | Requires testnet credentials in `.env` |
| Dash Dashboard | `python dashboard/app.py` | UI at http://127.0.0.1:8050 |

### Stopping the system

```bash
./stop.sh
```

Sends SIGTERM to each process, waits up to 5 seconds, then SIGKILL if still running. Closes the three Terminal windows afterwards.

### Testnet execution test (signal injection)

To validate the full execution pipeline with synthetic signals against the Binance testnet:

```bash
./start_test.sh
```

Sets `DRY_RUN=false`, `MIN_CONFIDENCE=0.1`, `TEST_SIGNAL_INJECT=true`. After ~45 s LOB warmup, synthetic LONG/SHORT `SWEEP_WITH_PROTECTION` signals are injected every 30 s. Monitor:

```bash
tail -f logs/cryptosentinel.log | grep -E '\[Injector\]|\[Gate[0-6]\]|\[Executor\]'
```

> Not for paper trading, backtesting, or production — testnet orders only.

### Manual startup (alternative)

```bash
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
| `BINANCE_API_KEY` | — | Testnet API key (from `.env`) |
| `BINANCE_API_SECRET` | — | Testnet API secret (from `.env`) |
| `BINANCE_TESTNET` | `True` | Always use testnet for execution |
| `SYMBOL` | `BTCUSDT` | Trading pair |
| `WS_BASE` | `wss://stream.testnet.binance.vision` | Testnet WebSocket endpoint (trading engine) |
| `REST_BASE` | `https://testnet.binance.vision` | Testnet REST endpoint (trading engine) |
| `LOB_RECORDER_WS` | `wss://stream.binance.com:9443` | Real Binance public stream (LOB Recorder only) |

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
| `LOB_WALL_SIGMA` | `2.5` | σ threshold for Wall identification — only wired into Gate 6's `get_current_walls(sigma=...)` call (`strategy/executor.py`); `MicrostructureDetector` (the live wall source) is constructed in `main.py` without this kwarg and always uses its own hardcoded `2.5` default |
| `LOB_WALL_WINDOW` | `5` | Ticks each side for Wall median/std — **not wired to any live consumer**; both `MicrostructureDetector` (`strategy/microstructure.py`) and `get_current_walls()` (`core/lob_engine.py`) always use their own hardcoded `window=5` default regardless of this setting |
| `LOB_DEPTH` | `1000` | Levels used in `get_snapshot()` depth reads (lob_snapshot_writer, legacy engine). `BinanceWebSocketConsumer` hard-codes `_DEPTH_LEVELS = 100`; `LOBRecorder` hard-codes `_DEPTH_LEVELS = 100`. Only relevant when calling `lob_engine.get_snapshot(depth=settings.LOB_DEPTH)`. |
| `LOB_OBI_DEPTH` | `20` | Levels used for OBI calculation |
| `LOB_HISTORY` | `18000` | In-memory bars retained (~5h) |
| `LOB_HEATMAP_BUCKET` | `5.0` | USD bucket width for dashboard heatmap |
| `RELOAD_SIGMA` | `3.0` | σ threshold for iceberg reload detection — **legacy-engine-only; see TODO** |
| `ICEBERG_WINDOW_MS` | `500` | Lookback window for iceberg replenishment — **legacy-engine-only; see TODO** |
| `ICEBERG_MIN_REPLENISH` | `0.80` | Min reload fraction to confirm iceberg — **legacy-engine-only; see TODO** |
| `ICEBERG_MIN_QTY` | `0.5` | Min absolute qty to qualify as iceberg — **legacy-engine-only; see TODO** |
| `SWEEP_LEVELS` | `5` | Top-N levels checked for sweep volume — **legacy-engine-only; see TODO** |
| `SWEEP_THRESHOLD` | `0.80` | Buy/sell vol fraction threshold — **legacy-engine-only; see TODO** |
| `BREAK_PROTECT_WINDOW_MS` | `2000` | Break+protect window (ms) — **legacy-engine-only; see TODO** |
| `ICEBERG_PRICE_TOL` | `0.10` | Price tolerance for iceberg detection — **legacy-engine-only; see TODO** |
| `BOOK_FLIP_SIGMA` | `3.0` | Min z-score for book-flip level detection — **legacy-engine-only; see TODO** |
| `BOOK_FLIP_MIN_CONSUMED` | `0.30` | Max fraction consumed to infer book-flip cancellation — **legacy-engine-only; see TODO** |
| `BOOK_FLIP_AGG_RATIO` | `0.50` | Min aggression ratio (fraction of mean qty) to confirm book-flip — **legacy-engine-only; see TODO** |
| `BREAK_MIN_VOL` | `1.0` | Min absolute buy/sell volume to register a breakout — **legacy-engine-only; see TODO** |
| `OBI_BREAK_THRESH` | `0.40` | OBI reference level — used by dashboard `/lob` page as a visual reference line only (not a trading gate) |
| `MICRO_MAX_HOLD_MS` | `60_000` | Max hold before forced exit (Gate 6) |
| `MICRO_EXIT_SPREAD_HARD_CAP_BPS` | `12.0` | Spread hard cap for Gate 6 post-entry exit trigger (not Gate 4; Gate 4 uses `EntryRules.spread_max_bps` = 8.0) |
| `LOB_FRESH_WALL_MS` | `3_000` | Protection wall must appear within this window |
| `LOB_STALE_WALL_MS` | `30_000` | Prune wall states not seen for this long |
| `PROTECTION_MAX_DISTANCE_BPS` | `25.0` | Max protection wall distance from mid |
| `PRICE_PRUNE_INTERVAL` | `100` | Prune stale price keys every N bars — **legacy-engine-only; see TODO** |
| `PRICE_PRUNE_BAND` | `0.02` | Keep prices within ±2% of current mid — **legacy-engine-only; see TODO** |
| `MICRO_PRICE_MOVE_FLOOR_BPS` | `3.0` | Minimum price move to confirm sweep |
| `MICRO_PRICE_MOVE_WINDOW` | `300` | Rolling window for dynamic price-move threshold |
| `MICRO_PRICE_MOVE_PERCENTILE` | `0.90` | Percentile rank for dynamic threshold |
| `MICRO_PRICE_MOVE_MIN_SAMPLES` | `50` | Min samples before dynamic threshold activates |

### Heartbeat
| Setting | Default | Description |
|---|---|---|
| `HEARTBEAT_WARN_MS` | `200` | Per-packet delta threshold for "degraded" classification |
| `HEARTBEAT_CRITICAL_MS` | `500` | Per-packet delta threshold for "critical" classification |
| `HEARTBEAT_CONSEC_LIMIT` | `3` | Consecutive critical packets required to fire KS-2 |
| `HEARTBEAT_DEGRADED_RATE_THRESH` | `0.5` | ≥50% of 10-msg window → DEGRADED |
| `HEARTBEAT_DEGRADED_RECOVERY_THRESH` | `0.3` | <30% of 10-msg window → recover to HEALTHY (hysteresis) |
| `HEARTBEAT_SUSTAINED_MS` | `10_000` | Duration at DEGRADED rate before SUSTAINED_DEGRADED |

### Risk Engine
| Setting | Default | Description |
|---|---|---|
| `STARTING_EQUITY` | `1_000_000.0` | Design account size (1M USDT testnet); overwritten by startup reconciler with actual Binance balance |
| `MAX_DRAWDOWN_PCT` | `0.05` | 5% drawdown from peak → HALTED |
| `DAILY_LOSS_LIMIT_PCT` | `0.02` | Legacy portfolio daily-loss hard stop (belt-and-suspenders) |
| `TIER_REDUCED_PCT` | `0.005` | ≥ 0.5% DOV loss → REDUCED (50% size, min conf 0.65) |
| `TIER_MINIMAL_PCT` | `0.0075` | ≥ 0.75% DOV loss → MINIMAL (25% size, min conf 0.80) |
| `TIER_PASSIVE_PCT` | `0.009` | ≥ 0.9% DOV loss → PASSIVE (no new entries) |
| `TIER_HALTED_PCT` | `0.01` | ≥ 1.0% DOV loss → HALTED |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 5-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.001` | Equity risked per trade (0.1% — sized for ~10 bps microstructure stops) |
| `KELLY_FRACTION` | `0.25` | Fractional Kelly applied to sizing |
| `ATR_MULTIPLIER_SL` | `1.5` | Stop-loss ATR multiplier — **not used in any live calculation** (live SL is protection-wall-based via `PROTECTION_MAX_DISTANCE_BPS`); shown in `/config` dashboard page and used in `config.py` validation only |
| `ATR_MULTIPLIER_TP` | `3.0` | Take-profit multiple — `OrderManager` sets TP = entry ± `ATR_MULTIPLIER_TP × sl_distance` (where sl_distance is wall-based, not ATR); also used by `backtesting/tick_replay.py` as the R:R multiplier |
| `SLIPPAGE_RESEARCH_BPS` | `3.0` | Expected slippage (KS-3 baseline) |
| `SLIPPAGE_MULTIPLIER` | `1.5` | KS-3 fires above `research × multiplier` |

### Execution
| Setting | Default | Description |
|---|---|---|
| `DRY_RUN` | `True` | Skip live order submission |
| `IOC_TIMEOUT_MS` | `200` | IOC order expiry — do not retry |
| `MAX_ORDER_NOTIONAL_PCT` | `0.90` | Gate 3 rejects if estimated notional exceeds 90% of equity |
| `QTY_STEP_SIZE` | `0.00001` | BTCUSDT LOT_SIZE `stepSize`; all quantities are floored to this grid |
| `MIN_NOTIONAL` | `100.0` | BTCUSDT minimum order notional (USD); orders below this are rejected pre-submission |

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
| `DASHBOARD_API_PORT` | `8080` | Port for aiohttp REST API (`/api/health`, `/api/portfolio`, `/api/session`, `/api/killswitch`) and WebSocket streams (`/ws/lob`, `/ws/portfolio`, `/ws/signals`) |

### Alerting
| Setting | Default | Description |
|---|---|---|
| `ALERT_WEBHOOK_URL` | `""` | Optional webhook URL for kill-switch alerts (empty = disabled) |

---

## Tests

```
Phase 1 — Live trading engine
tests/test_risk_engine.py            11 tests  — sync_tier transitions, tier callback, record_trade_result, mark_unrealised, session reset
tests/test_microstructure_engine.py  30 tests  — legacy microstructure engine
tests/test_executor.py               45 tests  — all 7 gates, telemetry emission
tests/test_order_manager.py          34 tests  — IOC entry, OCO bracket, fill handling
tests/test_lob_engine.py             22 tests  — state machine, gap detection, wall scan
tests/test_startup_reconciler.py     17 tests  — reconciliation, midnight reset
tests/test_microstructure.py         26 tests  — wall identification, absorption, sweep
tests/test_lob_recorder.py           27 tests  — recorder flush, reconnect, stats
tests/test_features.py               14 tests  — Welford, no-lookahead, VWAP reset
tests/test_cvd.py                    15 tests  — buy/sell CVD, 5-bar delta, std
tests/test_db_writer.py               9 tests  — SQLite write, upsert, purge
tests/test_signal_telemetry.py       23 tests  — flush, batch, timeout, outcome update, hub broadcast, tape management
tests/test_orders.py                 11 tests  — order domain classes and enums
tests/test_ws_consumer.py            12 tests  — heartbeat states, rate thresholds, hysteresis
tests/test_models.py                  6 tests  — PortfolioState, WallState, FeatureVector
tests/test_integration.py             2 tests  — end-to-end signal → execution pipeline

Phase 3 — REST API + LOB snapshot writer + Dash dashboard
tests/test_rest_api.py               11 tests  — /api/health, /api/portfolio, /api/session, /api/killswitch
tests/test_lob_snapshot_writer.py     9 tests  — snapshot writer, rolling cap, WAL mode
tests/test_realtime_hub.py            6 tests  — RealtimeHub fan-out, connection drop, /ws/lob endpoint
tests/test_dashboard_live.py         10 tests  — /live page callback, engine badge, kill switch
tests/test_dashboard_lob.py          15 tests  — /lob buffer helpers, CVD accumulation, dedup
tests/test_dashboard_registry.py      4 tests  — /registry page, strategy lifecycle display
tests/test_dashboard_walls.py         6 tests  — /walls page callback, trace structure, invalid-JSON guard
tests/test_alerting.py                3 tests  — AlertDispatcher webhook, empty-URL guard
tests/test_signal_broadcast.py        9 tests  — /ws/signals payload shape, SignalTelemetry hub broadcast, tape management
tests/test_portfolio_broadcast.py     7 tests  — /ws/portfolio payload shape, _portfolio_mtm_loop broadcast, killswitch guard

Phase 2 — Backtesting + strategy lifecycle
tests/test_bt_event_engine.py        24 tests  — event-driven engine: tiers, exits, full run
tests/test_registry.py               29 tests  — lifecycle gates, YAML roundtrip, promotion
tests/test_bt_tick_replay.py         16 tests  — tick replay fidelity, streaming, CVD reset
tests/test_bt_walk_forward.py         9 tests  — window splits, OOS isolation, leaderboard
tests/test_bt_metrics.py             10 tests  — Sharpe, MDD, PF, composite score
tests/test_validator.py               7 tests  — null repair, duplicate removal, gap detection
tests/test_scorer.py                  7 tests  — XGBoost train, AUC, save/load, fallback
tests/test_fetcher.py                 6 tests  — OHLCV fetch, cache, resample
tests/test_bt_signals.py              7 tests  — signal arrays, no-lookahead, SL/TP NaN
tests/test_bt_vectorbt.py             6 tests  — Optuna optimisation, sensitivity
tests/test_bt_costs.py                5 tests  — round-trip cost, maker/taker, zero qty
──────────────────────────────────────────────────────────────────────────────
Total                               510 tests
```

---

## Path to Paper Trading

The trading engine is fully operational on the Binance Spot Testnet. The remaining work before enabling live paper trading (`DRY_RUN=False`) is data accumulation and strategy validation:

| Step | Action | Status |
|------|--------|--------|
| **1. Accumulate LOB data** | LOB Recorder is collecting at `btcusdt@depth@100ms` (incremental diff, 100 levels, $25-bucket aggregation). Keep `lob_recorder` running continuously. Target ≥30 days for statistically robust walk-forward splits. | ✅ Done (recorder operational) |
| **2. Fetch OHLCV history** | Run `data/fetcher.py` to populate `ohlcv_cache.db` for Path B backtesting | 🔜 Pending |
| **3. Run backtests** | Path A: `backtesting/tick_replay.py` replay of `lob_tick.db`. Path B: `backtesting/walk_forward.py` OHLCV walk-forward via VectorBT + Optuna | 🔜 Pending |
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
| LOB Recorder WS | websockets | 10.4 | Real Binance public stream |
| Exchange WS/REST | python-binance | 1.0.36 | Testnet execution |
| Historical data | ccxt | 4.3+ | OHLCV fetch (no key) |
| Data | pandas + numpy | 3.0.3 + 2.4.4 | DataFrames + arrays |
| Stats | scipy | 1.14+ | linregress, statistics |
| ML scorer | xgboost | 2.1+ | Confidence scoring (Phase 2) |
| Backtest fast | vectorbt | 0.25+ | OHLCV Path B (Phase 2) |
| Optimiser | optuna | 3.6+ | Bayesian param search (Phase 2) |
| Config | pydantic-settings | 2.3.x | .env management |
| Dashboard | Dash + dash-bootstrap-components | 4.1+ / 2.0+ | All UI pages (Phase 3) |
| Dashboard WS | dash-extensions | 1.0+ | Real-time WebSocket components (`/live`, `/lob`) |
| Charts | plotly | 5.22.x | All visualisations |
| Persistence | SQLite | stdlib | All databases |
| Logging | Python stdlib logging | 3.13+ | RotatingFileHandler, INFO/DEBUG configurable via `LOG_LEVEL` |
| Serialisation | PyYAML | 6.0+ | StrategySpec configs |

---

## System Invariants (Coherence Rules)

These rules are invariants. Any code that violates them is incorrect.

| Rule | Invariant |
|------|-----------|
| **Rule 1** | `FeatureComputer` is one class. Called identically in live trading, tick backtest, and OHLCV backtest. No separate calculation paths. |
| **Rule 2** | Microstructure fires, pattern boosts. A chart pattern alone never triggers a trade. |
| **Rule 3** | Risk budget is a shared pool. All open pyramid legs draw from the same daily budget. Unrealised PnL counts against the budget. |
| **Rule 4** | LOB Recorder uses real Binance public API (`wss://stream.binance.com`), not testnet. Backtesting on testnet LOB data is meaningless. |
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
| **Phase 1A–1K** | 1–3 | Foundation: LOB Recorder, HeartbeatMonitor, LOB state machine, FeatureComputer, Wall/Absorption/Sweep signals, 7-Gate executor, 5-tier Risk Engine, DailyBudget, GlobalKillswitch, signal telemetry | ✅ Done |
| **Phase 1L–1P** | 3 | Foundation: IOC limit orders (1L), startup reconciler + midnight reset (1M), integration test + killswitch wire-up (1N), AlertDispatcher webhook notifications (1P) | ✅ Done |
| **Phase 2A–2F** | 4–6 | Backtesting infrastructure: data layer, costs, metrics, signal generator, VectorBT+Optuna, walk-forward orchestrator (OHLCV Path B complete) | ✅ Done |
| **Phase 2G** | 6 | Tick Replay Engine: `backtesting/tick_replay.py` — event-driven replay of `lob_tick.db` through live feature/signal stack; fidelity test passes | ✅ Done |
| **Phase 2H** | 6 | XGBoost Confidence Scorer: `strategy/scorer.py` — trains on APPROVED `signal_records`, AUC ≥ 0.62 gate, ECE calibration, staleness detection, wired into Gate 2 via `ScorerFactory` | ✅ Done |
| **Phase 2I** | 7 | Strategy Registry: `strategy/spec.py`, `registry.py`, `builder.py` — full lifecycle RESEARCH→BACKTEST→PAPER→LIVE with dual-store (YAML+SQLite), 4-gate PAPER promotion, 3-gate LIVE promotion | ✅ Done |
| **Phase 2J** | 7–8 | Strategy config tuning: accumulate ≥30 days of `depth@100` LOB data, run walk-forward backtests, tune params, run tick replay validation, promote first spec to PAPER | 🔜 Next |
| **Phase 3** | 9–12 | Dashboard: Dash multi-page app (6 pages: /live, /lob, /walls, /backtest, /registry, /config), REST API, LOB snapshot writer, decay monitoring, LIVE promotion pipeline | ✅ Done |

---

## TODO

- [x] Write up strategy documentation — entry logic, gate rationale, Wall/Absorption/Sweep signal design (`documentation/strategy.md`)
- [x] Write up model documentation — FeatureComputer inputs, XGBoost scorer architecture, training pipeline including component interaction diagram (`documentation/model.md`)
- [x] Write up trading algorithm documentation — end-to-end flow from LOB tick to order submission (`documentation/algorithm.md`)
- [x] Fix gate 3 issue; risk management is not using live position data. need to ensure BTC, USDT data is being passed through
- [ ] Fix one test case. make sure it uses the correct notional
- [ ] check market bubbles in dashboard. make sure it is top 1 percentile volume for the whole heatmap window. otherwise might be better to have even higher threshhold up to 99.5 percentile contrast
- [ ] need to test the live dashboard works
- [ ] Consider placing a minimum-quantity resting order behind/after a significant liquidity wall — a fill on that order signals the wall has been consumed, providing a cleaner consumption trigger than depth-diff heuristics. Quantity must be as small as possible (min tick size on Binance Spot Testnet).
- [ ] run through start_test.sh and make sure the trade execution with injector is working. trades work now. make sure its tracked in live dashboard
- [ ] review all code and test scripts to ensure no unused classes or functions
- [ ] troubleshoot and refine strategy
- [ ] troubleshoot and refine model
- [ ] troubleshoot and refine algorithm

### Lead-quant backlog

- [ ] **Dead code — MicrostructureEngine**: `engine/microstructure_engine.py` (~410 lines) is not started in `main.py`'s TaskGroup and has no live producer. `ms_bar_queue` is orphaned. Either wire it into the live path or delete it — currently it creates confusion about the actual signal source.
- [x] **Dead code — engine shims**: `engine/lob_engine.py`, `engine/risk_engine.py`, `engine/websocket_consumer.py`, `engine/order_manager.py` have been removed. `engine/` now contains only real implementations: `db_writer.py`, `lob_snapshot_writer.py`, `realtime_hub.py`, and `microstructure_engine.py` (legacy, backtesting only).
- [ ] **Redundant config — pattern settings**: `PATTERN_LOOKBACK`, `SWING_WINDOW`, `BREAKOUT_VOL_MULT`, `MIN_R2` in `config.py` have no consumer in the live codebase. Remove or annotate clearly as reserved for a future pattern module.
- [ ] **Unused FeatureVector fields**: `pattern_r2` and `protection_wall_present` are always 0 in the live path — `executor.py` never passes them into `FeatureComputer.compute()`. XGBoost trains on these zero-variance columns. Either wire the inputs or drop the features from `FEATURE_ORDER` and retrain.
- [ ] **Gate 4 spread check is duplicated**: `gate_4_order_selection()` checks both `EntryRules.spread_max_bps` (8.0) and `spread_p95 × 2`. The hard cap and the adaptive cap serve the same purpose; consolidate into a single threshold derived from session p95 data with a sensible floor.
- [ ] **Position sizing ignores remaining_budget**: `notional_hint` is computed from `confidence × KELLY × RISK_PCT × tier_scalar` without consulting `DailyBudget.remaining`. A run of near-threshold trades could drain the budget silently — add a budget-fraction cap to `notional_hint` in `gate_3_position_size`.
- [ ] **Test coverage gap — StrategyExecutor + RiskEngine integration**: Gate 3 capital gate and tier-elevated confidence are tested in isolation but no test exercises the full `StrategyExecutor → OrderManager → fill_processor` path under a non-FULL risk tier. Add at least one integration scenario for REDUCED/MINIMAL tier flow.
- [x] **Test coverage gap — HeartbeatMonitor SUSTAINED_DEGRADED**: `test_ws_consumer.py` now includes `test_heartbeat_sustained_degraded` and `test_heartbeat_recovery_from_sustained` covering the 10 s transition and hysteresis band recovery (12 tests total).
- [ ] **Test coverage gap — RiskEngine throttle and circuit breakers**: `test_risk_engine.py` has only 11 tests (sync_tier transitions and basic record_trade_result). The 5-tier drawdown circuit breaker, consecutive-loss cooldown timer, budget-loss tier thresholds, and all `_check_circuit_breakers()` branches remain under-covered. This is a liability for a risk-critical module — expand to at least 25 tests covering the full tier ladder and each circuit breaker trigger.
- [ ] **Dead config — CANDLE_INTERVAL**: `config.py` defines `CANDLE_INTERVAL: str = "1s"` with the comment "legacy — ws_consumer uses this", but `ws_consumer.py` never imports or reads this setting (it uses `TIMEFRAME` for klines). Remove the setting or delete it before more code takes a dependency on it.
- [ ] **Dead config — UI_REFRESH_INTERVAL**: `config.py` defines `UI_REFRESH_INTERVAL: float = 1.0` but no module outside `config.py` reads or imports this setting (confirmed: grep finds zero consumers). Either wire it into the Dash dashboard refresh callbacks or remove it.
- [ ] **Dead config — ATR_MULTIPLIER_SL**: `config.py` defines `ATR_MULTIPLIER_SL: float = 1.5` but no production code uses it for calculation — the live path derives stop-loss distance from the protection wall price (`PROTECTION_MAX_DISTANCE_BPS`). It is only displayed on the `/config` dashboard page and used in a `config.py` self-validation assertion (`ATR_MULTIPLIER_TP > ATR_MULTIPLIER_SL`). Consider removing and replacing the assertion with `ATR_MULTIPLIER_TP > 0`.
- [ ] **Dead config — legacy-engine-only settings**: `SWEEP_THRESHOLD`, `SWEEP_LEVELS`, `BREAK_PROTECT_WINDOW_MS`, `ICEBERG_PRICE_TOL`, `BOOK_FLIP_SIGMA`, `BOOK_FLIP_MIN_CONSUMED`, `BOOK_FLIP_AGG_RATIO`, `BREAK_MIN_VOL`, `RELOAD_SIGMA`, `ICEBERG_WINDOW_MS`, `ICEBERG_MIN_REPLENISH`, `ICEBERG_MIN_QTY`, `PRICE_PRUNE_INTERVAL`, `PRICE_PRUNE_BAND` are defined in `config.py` but consumed exclusively by `engine/microstructure_engine.py` (the legacy engine that is not started in the live path). Note: `OBI_BREAK_THRESH` remains in the list above but IS used — it drives a reference line in `dashboard/pages/lob.py` (visual only, not a trading gate). Removing the legacy engine removes all consumers for the other settings listed; delete them at that point.
- [x] **STARTING_EQUITY duplication**: Resolved. `main.py` line 67 defines `STARTING_EQUITY = settings.STARTING_EQUITY`, reading from the single `config.py` source. All component initialisation also reads from `settings.STARTING_EQUITY` directly.
- [ ] **Absorption prerequisite adds false negatives**: `gate_1_microstructure()` hard-gates on `prior_absorption == True`. A wall that appears and is immediately consumed (e.g. within the first 500ms of its first_seen_ts) will always be rejected — absorption can never arm a wall that is gone before it persists. At the 100ms tick rate this blocks genuine fast institutional sweeps of newly-posted deep liquidity. Evaluate demoting Absorption from a Gate 1 hard prerequisite to a Gate 2 scoring bonus (e.g. `absorption_ratio > 0 → +0.10` in `RuleBasedScorer`) to recover these signals without opening a false-positive flood.
- [ ] **Dual-store registry is over-engineered for a single-strategy system**: `strategy/registry.py` writes every spec to both a YAML file and a SQLite `strategies` table. For the current single-strategy deployment, a single SQLite store would suffice; the YAML mirror adds sync risk (YAML written first, then DB — a crash between the two leaves them inconsistent). Consolidate into SQLite-only in `strategy/registry.py` and `strategy/builder.py`, retaining YAML export as an explicit `export()` method for human review.
- [ ] **Unused EntryRules fields**: `strategy/spec.py` defines `cvd_momentum_min` (0.8), `vol_ratio_min` (1.4), and `sweep_qty_mult` (2.0) in `EntryRules`, but none of these fields are consumed by any gate function or scorer. `RuleBasedScorer` reads only `obi_threshold` and `spread_max_bps` from `EntryRules`. `gate_4_order_selection()` receives `spread_max_bps` via the executor. The three unused fields silently persist in YAML strategy specs without affecting behavior. Either wire them into the gate pipeline or remove them to reduce spec-to-code surface area.
- [ ] **Low-signal features in FEATURE_ORDER**: `strategy/scorer.py` trains XGBoost on all 15 features including `pattern_r2` (always 0.0 in live path — `strategy/executor.py` never passes it to `FeatureComputer.compute()`), `vwap_reclaim` (binary flag, rarely 1 on a per-tick basis), and `rsi_value` (14-bar candle momentum on a 5m timeframe — coarse relative to 100ms microstructure signals). After first XGBoost training run, call `scorer.feature_importances()` and prune any feature with importance < 0.01 from `FEATURE_ORDER`; retrain and compare AUC.
- [ ] **Parallel depth consumers double memory pressure**: `_depth_fanout` (`main.py` lines 174–188) fans out each reconstructed depth snapshot to both `lob_depth_queue` (consumed by `_run_lob_engine`) and `ms_depth_queue` (consumed by `MicrostructureDetector`). Both consumers parse the same `{"bids": [...], "asks": [...]}` dict independently. Evaluate whether `MicrostructureDetector` could subscribe to `LocalOrderBook`'s processed output instead of the raw diff queue, halving snapshot copies in memory at the cost of adding a processing dependency.
- [ ] **Test coverage gap — strategy/builder.py and strategy/spec.py**: Neither `tests/test_builder.py` nor `tests/test_spec.py` exists. `StrategyBuilder.walk_forward_metrics()` and `StrategyBuilder.build()` (which drive the BACKTEST→PAPER promotion path) are untested. `StrategySpec.to_dict()` / `StrategySpec.from_dict()` roundtrip is exercised only indirectly via `test_registry.py`. Add dedicated test files for both modules.
- [ ] **Gate 3 capital gate has a belt-and-suspenders budget check**: `gate_3_capital()` (`strategy/executor.py` lines 72–79) checks both `tier in ("HALTED", "PASSIVE")` and `budget.remaining <= 0`. These are not independent — when DOV loss reaches `TIER_HALTED_PCT`, `RiskEngine._check_circuit_breakers()` (`risk/engine.py` lines 82–86) sets `tier = HALTED`, so the tier check would already reject. The `budget.remaining <= 0` path only fires during the ~1-second gap before the next MTM loop tier sync. Evaluate whether increasing the MTM loop frequency or making the tier sync synchronous on budget update would allow the `budget.remaining` check to be removed.
- [ ] **Test coverage gap — risk/budget.py and risk/killswitch.py**: Neither `tests/test_budget.py` nor `tests/test_killswitch.py` exists. `DailyBudget.remaining`, `DailyBudget.loss_pct`, reset on midnight boundary, and `GlobalKillswitch` trigger conditions (KS-1 budget breach, KS-2 heartbeat, KS-3 slippage) are only tested indirectly through `test_risk_engine.py` fixtures. These are risk-critical code paths — add dedicated unit tests for each module (`risk/budget.py` and `risk/killswitch.py`).
- [ ] **Test coverage gap — dashboard/pages/backtest.py and dashboard/pages/config.py**: Neither `tests/test_dashboard_backtest.py` nor `tests/test_dashboard_config.py` exists. The `/backtest` leaderboard page (`dashboard/pages/backtest.py`) and the `/config` settings-reference + emergency-stop page (`dashboard/pages/config.py`) have zero dedicated tests. The four other dashboard pages (`/live`, `/lob`, `/walls`, `/registry`) all have test files; add equivalent coverage for these two to close the gap.
- [ ] **Test coverage gap — dashboard/_db.py, dashboard/_utils.py, dashboard/app.py**: 11 of `dashboard/_db.py`'s 17 functions (`main_db`, `backtest_db`, `lob_tick_db`, `fetch_agg_trades`, `fetch_cvd_series_24h`, `fetch_portfolio_history`, `fetch_strategies`, `fetch_session_stats`, `fetch_pnl_by_pattern`, `fetch_system_events`, `fetch_backtest_results`) have zero references anywhere in `tests/`; only `_connect`, `registry_db`, `fetch_lob_snapshots`, `fetch_signal_funnel`, `fetch_gate_funnel_drift`, and `fetch_candles` are exercised. `dashboard/_utils.py`'s sole function, `empty_fig`, has zero test references. `dashboard/app.py` has no dedicated test file and is not imported by any existing test. Add targeted unit tests for the untested `_db.py` fetchers, `empty_fig`, and basic `app.py` page-registration/layout smoke coverage.
