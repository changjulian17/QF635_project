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
OPTIONAL BOOST — Chart Pattern Context (OHLCV)
  Pattern present at Wall level → +0.10–0.15 to XGBoost score
  Pattern absent → signal still valid
           │
           ▼
FILTER — Confidence Scorer
  Rule-based placeholder (→ XGBoost after data collection)
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
│   ├── pattern_detector.py    # OHLCV chart pattern detection (context/boost)
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
│   ├── budget.py              # DailyBudget — shared pool for all pyramid legs
│   ├── pyramid.py             # PyramidController — 3-leg scaling
│   └── killswitch.py          # GlobalKillswitch — Budget / Heartbeat / Slippage triggers
│
├── execution/                 # Order management
│   ├── order_manager.py       # IOC aggressive limit orders + OCO brackets
│   └── orders.py              # Order domain classes and enums
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
├── data/                      # Data acquisition
│   ├── fetcher.py             # OHLCVFetcher (CCXT + SQLite cache)
│   └── validator.py           # 9-check data quality validator
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
│   └── configs/               # Frozen YAML strategy specs
│
├── data/
│   ├── lob_tick.db            # LOB Recorder output (real Binance data — live-writing)
│   ├── ohlcv_cache.db         # OHLCV SQLite cache (CCXT)
│   └── backtest_results.db    # Backtest results storage
│
├── engine/                    # Legacy shim directory + real-time hub
│   ├── db_writer.py           # SQLite persistence + rolling cleanup + lob_snapshots table
│   ├── lob_snapshot_writer.py # LOB snapshot writer coroutine (~1 Hz, lob_snapshots table)
│   ├── realtime_hub.py        # RealtimeHub — fan-out JSON pushes to /ws/lob WebSocket clients
│   ├── websocket_consumer.py  # Shim re-exporting core.ws_consumer
│   ├── pattern_detector.py    # Shim re-exporting core.pattern_detector
│   └── order_manager.py       # Shim re-exporting execution.order_manager
│
├── scripts/
│   ├── test_connection.py     # Connectivity + auth check
│   ├── test_orders.py         # BUY + SELL round-trip test
│   ├── run_backtest.py        # CLI for tick-level walk-forward backtest (writes to backtest_results.db)
│   └── signal_injector.py     # Synthetic signal injection — dev/testnet only (start_test.sh)
│
├── tests/                     # pytest unit + integration tests
│   ├── test_models.py
│   ├── test_lob_engine.py
│   ├── test_microstructure.py
│   ├── test_features.py
│   ├── test_executor.py
│   ├── test_cvd.py
│   ├── test_signal_telemetry.py
│   ├── test_db_writer.py
│   ├── test_pattern_detector.py
│   ├── test_risk_engine.py
│   └── test_integration.py
│
```

---

## Data Pipeline

### Stream Selection

The LOB Recorder subscribes to `btcusdt@depth@100ms` — the **incremental diff-depth stream** — and maintains a full local order book seeded from a REST `depth?limit=1000` snapshot on each connect. Buffered diffs received during the REST fetch are merged in sequence-ID order before the book is declared SYNCED. The top 100 levels per side are retained (`_DEPTH_LEVELS = 100`) and aggregated into $25 USD price buckets (`_BUCKET_WIDTH = 25.0`) before writing to `lob_tick.db`. At BTC prices (~$77k), 100 levels span ~$50–200 from mid — sufficient range to detect deep-book institutional walls above the transaction cost floor (~$115 at 30bps round-trip).

| Stream | Purpose | Update Rate |
|--------|---------|-------------|
| `btcusdt@aggTrade` | CVD · aggressive volume | Per taker sweep |
| `btcusdt@depth@100ms` | Wall detection · OBI · spread — incremental diff, 100 levels, $25 buckets | 100ms |
| `btcusdt@bookTicker` | Best bid/ask for spread calc | Real-time |
| `btcusdt@kline_5m` | OHLCV candles for patterns | On close (5m) |

### Async Queue Architecture

```
Queue               Producer                   Consumer              maxsize
──────────────────────────────────────────────────────────────────────────
raw_tick_queue      WS Consumer (testnet)      CVD Calculator        2,000
depth_queue         WS Consumer (testnet)      LOB Engine            500
candle_queue        WS Consumer (testnet)      PatternDetector       200
micro_signal_queue  MicrostructureDetector     StrategyExecutor      100
signal_queue        StrategyExecutor           Risk Engine           100
order_queue         Risk Engine                Order Manager         50
fill_queue          Order Manager              Portfolio State       200
telemetry_queue     StrategyExecutor           SignalTelemetry       500
```

---

## 7-Gate Trade Life Cycle

Every potential trade passes through seven sequential gates. Failure at any gate emits a `SignalRecord` to `signal_records` in `registry.db`. No gate may be skipped.

| Gate | Name | Pass Condition | Rejection Code |
|------|------|----------------|----------------|
| **Gate 0** | Data Fidelity | `lob_status == SYNCED` AND `heartbeat != CRITICAL` | `GATE_0_FAIL: LOB_STALE` or `GATE_0_FAIL: HEARTBEAT_CRITICAL` |
| **Gate 1** | Microstructure Trigger | Sweep + Fresh Wall confirmed | `GATE_1_FAIL: NO_SWEEP_SIGNAL` |
| **Gate 2** | Confidence Score | `confidence >= 0.58` | `GATE_2_FAIL: LOW_CONFIDENCE 0.47 < 0.58` |
| **Gate 3** | Capital Gate | `remaining_budget > 0` AND `tier != HALTED` | `GATE_3_FAIL: BUDGET_EXHAUSTED` |
| **Gate 4** | Order Selection | `spread_bps <= spread_p95 × 2` | `GATE_4_FAIL: SPREAD_TOO_WIDE` |
| **Gate 5** | Execution Sync | `signal_age < 200ms` AND `last_delta < 200ms` | `GATE_5_FAIL: SIGNAL_STALE` |
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

Gap severity tiering:
| Gap Duration | Action |
|---|---|
| < 500ms | CONTINUE — attempt re-sync |
| 500ms – 5s | HALT_ENTRIES — no new entries, keep open positions |
| > 5s | CLOSE_REVIEW — log warning, consider closing |

### LOB Recorder (`core/lob_recorder.py`)

Connects to `wss://stream.binance.com` (real Binance public stream — no API key). Records raw tick data to `data/lob_tick.db` for backtesting Path A. Separate from the testnet trading connection per Rule 4.

### HeartbeatMonitor (`core/ws_consumer.py`)

Tracks `(local_time − Binance event time E)` on every WebSocket message:

| Status | Delta | Action |
|--------|-------|--------|
| HEALTHY | < 200ms | Normal trading |
| DEGRADED | 200–500ms | Log warning (normal on testnet) |
| CRITICAL | > 500ms × 3 consecutive | Killswitch condition — CLOSE_ALL |

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

**Sweep + Fresh Wall:** Wall consumed (<15% original qty), price moved >0.03%, CVD spike >1.5σ, fresh Wall appeared on far side within 3s → fires the trade.

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
risk_amount  = min(equity × 1%, remaining_budget × 60%, DOV × 0.4%)
base_qty     = risk_amount / sl_distance
final_qty    = base_qty × (KELLY_FRACTION × confidence) × tier_scalar
```

**Circuit Breakers:**
```
Trigger 1: daily_loss ≥ 1% of DOV          → HALTED
Trigger 2: drawdown ≥ 5% from peak          → HALTED
Trigger 3: 3 consecutive losses              → PAUSED (15 min cooldown)
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
| `LOB_RECORDER_WS` | `wss://stream.binance.com:9443` | Real Binance public stream (LOB Recorder only) |

### Strategy
| Setting | Default | Description |
|---|---|---|
| `TIMEFRAME` | `5m` | Kline interval for patterns |
| `PATTERN_LOOKBACK` | `50` | Candle history for pattern detection |
| `SWING_WINDOW` | `5` | Bars each side for swing pivots |
| `MIN_R2` | `0.80` | Minimum trendline R² |
| `BREAKOUT_VOL_MULT` | `1.5` | Volume × average for breakout |
| `MIN_CONFIDENCE` | `0.58` | Gate 2 confidence threshold |

### LOB / Microstructure
| Setting | Default | Description |
|---|---|---|
| `LOB_WALL_SIGMA` | `2.5` | σ threshold for Wall identification |
| `LOB_WALL_WINDOW` | `5` | Ticks each side for Wall median/std |
| `LOB_DEPTH` | `1000` | Levels in the live LOB Engine's local book (LOB Recorder uses `_DEPTH_LEVELS = 100` hardcoded) |
| `LOB_OBI_DEPTH` | `20` | Levels used for OBI calculation |
| `LOB_HISTORY` | `18000` | In-memory bars retained (~5h) |
| `LOB_HEATMAP_BUCKET` | `5.0` | USD bucket width for dashboard heatmap |
| `RELOAD_SIGMA` | `3.0` | σ threshold for iceberg reload detection |
| `ICEBERG_WINDOW_MS` | `500` | Lookback window for iceberg replenishment |
| `ICEBERG_MIN_REPLENISH` | `0.80` | Min reload fraction to confirm iceberg |
| `ICEBERG_MIN_QTY` | `0.5` | Min absolute qty to qualify as iceberg |
| `SWEEP_LEVELS` | `5` | Top-N levels checked for sweep volume |
| `SWEEP_THRESHOLD` | `0.80` | Buy/sell vol must exceed this fraction of top-N depth |
| `BREAK_PROTECT_WINDOW_MS` | `2000` | Fresh-wall recency window post-sweep (ms) |
| `OBI_BREAK_THRESH` | `0.40` | OBI threshold for breakout confirmation |
| `MICRO_MAX_HOLD_MS` | `60_000` | Max hold before forced exit (Gate 6) |
| `MICRO_EXIT_SPREAD_HARD_CAP_BPS` | `12.0` | Spread hard cap for Gate 4 order selection |
| `LOB_FRESH_WALL_MS` | `3_000` | Protection wall must appear within this window |
| `LOB_STALE_WALL_MS` | `30_000` | Prune wall states not seen for this long |
| `PROTECTION_MAX_DISTANCE_BPS` | `25.0` | Max protection wall distance from mid |
| `PRICE_PRUNE_INTERVAL` | `100` | Prune stale price keys every N bars |
| `PRICE_PRUNE_BAND` | `0.02` | Keep prices within ±2% of current mid |
| `MICRO_PRICE_MOVE_FLOOR_BPS` | `3.0` | Minimum price move to confirm sweep |
| `MICRO_PRICE_MOVE_WINDOW` | `300` | Rolling window for dynamic price-move threshold |
| `MICRO_PRICE_MOVE_PERCENTILE` | `0.90` | Percentile rank for dynamic threshold |
| `MICRO_PRICE_MOVE_MIN_SAMPLES` | `50` | Min samples before dynamic threshold activates |

### Heartbeat
| Setting | Default | Description |
|---|---|---|
| `HEARTBEAT_WARN_MS` | `200` | Log warning above this delta |
| `HEARTBEAT_CRITICAL_MS` | `500` | Killswitch threshold |
| `HEARTBEAT_CONSEC_LIMIT` | `3` | Consecutive critical packets to fire KS |

### Risk Engine
| Setting | Default | Description |
|---|---|---|
| `MAX_DRAWDOWN_PCT` | `0.05` | 5% drawdown from peak → HALTED |
| `DAILY_LOSS_LIMIT_PCT` | `0.02` | Legacy portfolio daily-loss hard stop (belt-and-suspenders) |
| `TIER_REDUCED_PCT` | `0.005` | ≥ 0.5% DOV loss → REDUCED (50% size, min conf 0.65) |
| `TIER_MINIMAL_PCT` | `0.0075` | ≥ 0.75% DOV loss → MINIMAL (25% size, min conf 0.80) |
| `TIER_PASSIVE_PCT` | `0.009` | ≥ 0.9% DOV loss → PASSIVE (no new entries) |
| `TIER_HALTED_PCT` | `0.01` | ≥ 1.0% DOV loss → HALTED |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 5-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.01` | Equity risked per trade (1%) |
| `KELLY_FRACTION` | `0.25` | Fractional Kelly applied to sizing |
| `ATR_MULTIPLIER_SL` | `1.5` | Stop-loss distance in ATR units |
| `ATR_MULTIPLIER_TP` | `3.0` | Take-profit distance in ATR units |
| `SLIPPAGE_RESEARCH_BPS` | `3.0` | Expected slippage (KS-3 baseline) |
| `SLIPPAGE_MULTIPLIER` | `1.5` | KS-3 fires above `research × multiplier` |

### Execution
| Setting | Default | Description |
|---|---|---|
| `DRY_RUN` | `True` | Skip live order submission |
| `IOC_TIMEOUT_MS` | `200` | IOC order expiry — do not retry |

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
tests/test_risk_engine.py           51 tests  — 5-tier, killswitch, pyramid, circuit breakers
tests/test_microstructure_engine.py 30 tests  — legacy microstructure engine
tests/test_executor.py              40 tests  — all 7 gates, telemetry emission
tests/test_order_manager.py         30 tests  — IOC entry, OCO bracket, fill handling
tests/test_lob_engine.py            22 tests  — state machine, gap detection, wall scan
tests/test_startup_reconciler.py    14 tests  — reconciliation, midnight reset
tests/test_microstructure.py        22 tests  — wall identification, absorption, sweep
tests/test_lob_recorder.py          27 tests  — recorder flush, reconnect, stats
tests/test_features.py              14 tests  — Welford, no-lookahead, VWAP reset
tests/test_cvd.py                   14 tests  — buy/sell CVD, 5-bar delta, std
tests/test_db_writer.py             12 tests  — SQLite write, upsert, purge
tests/test_signal_telemetry.py      10 tests  — flush, batch, timeout, outcome update
tests/test_orders.py                10 tests  — order domain classes and enums
tests/test_pattern_detector.py      10 tests  — ATR, swing detection, S/R breakout
tests/test_ws_consumer.py           12 tests  — heartbeat states, rate thresholds, hysteresis
tests/test_models.py                 6 tests  — PortfolioState, WallState, FeatureVector
tests/test_integration.py            2 tests  — end-to-end signal → execution pipeline

Phase 3 — REST API + LOB snapshot writer + Dash dashboard
tests/test_rest_api.py              11 tests  — /api/health, /api/portfolio, /api/killswitch
tests/test_lob_snapshot_writer.py    9 tests  — snapshot writer, rolling cap, WAL mode
tests/test_realtime_hub.py           6 tests  — RealtimeHub fan-out, connection drop, /ws/lob endpoint
tests/test_dashboard_live.py         6 tests  — /live page callback, engine badge, kill switch
tests/test_dashboard_lob.py          6 tests  — /lob buffer helpers, CVD accumulation, dedup
tests/test_dashboard_registry.py     4 tests  — /registry page, strategy lifecycle display
tests/test_dashboard_walls.py        6 tests  — /walls page callback, trace structure, invalid-JSON guard
tests/test_alerting.py               3 tests  — AlertDispatcher webhook, empty-URL guard

Phase 2 — Backtesting + strategy lifecycle
tests/test_bt_event_engine.py       17 tests  — event-driven engine: tiers, exits, full run
tests/test_registry.py              29 tests  — lifecycle gates, YAML roundtrip, promotion
tests/test_bt_tick_replay.py        16 tests  — tick replay fidelity, streaming, CVD reset
tests/test_bt_walk_forward.py        9 tests  — window splits, OOS isolation, leaderboard
tests/test_bt_metrics.py            10 tests  — Sharpe, MDD, PF, composite score
tests/test_validator.py              7 tests  — null repair, duplicate removal, gap detection
tests/test_scorer.py                 7 tests  — XGBoost train, AUC, save/load, fallback
tests/test_fetcher.py                6 tests  — OHLCV fetch, cache, resample
tests/test_bt_signals.py             7 tests  — signal arrays, no-lookahead, SL/TP NaN
tests/test_bt_vectorbt.py            6 tests  — Optuna optimisation, sensitivity
tests/test_bt_costs.py               5 tests  — round-trip cost, maker/taker, zero qty
──────────────────────────────────────────────────────────────────────────────
Total                              496 tests
```

---

## Path to Paper Trading

The trading engine is fully operational on the Binance Spot Testnet. The remaining work before enabling live paper trading (`DRY_RUN=False`) is data accumulation and strategy validation:

| Step | Action | Status |
|------|--------|--------|
| **1. Accumulate LOB data** | LOB Recorder is collecting at `btcusdt@depth@100ms` (incremental diff, 100 levels, $25-bucket aggregation). Keep `lob_recorder` running continuously. Target ≥30 days for statistically robust walk-forward splits. | ✅ Done (recorder operational) |
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
| LOB Recorder WS | websockets | 10.4 | Real Binance public stream |
| Exchange WS/REST | python-binance | 1.0.36 | Testnet execution |
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
| **Phase 1A–1K** | 1–3 | Foundation: LOB Recorder, HeartbeatMonitor, LOB state machine, FeatureComputer, Wall/Absorption/Sweep signals, 7-Gate executor, 5-tier Risk Engine, DailyBudget, PyramidController, GlobalKillswitch, signal telemetry | ✅ Done |
| **Phase 1L–1N** | 3 | Foundation: IOC limit orders (1L), startup reconciler + midnight reset (1M), integration test + killswitch wire-up (1N) | ✅ Done |
| **Phase 2A–2F** | 4–6 | Backtesting infrastructure: data layer, costs, metrics, signal generator, VectorBT+Optuna, walk-forward orchestrator (OHLCV Path B complete) | ✅ Done |
| **Phase 2G** | 6 | Tick Replay Engine: `backtesting/tick_replay.py` — event-driven replay of `lob_tick.db` through live feature/signal stack; fidelity test passes | ✅ Done |
| **Phase 2H** | 6 | XGBoost Confidence Scorer: `strategy/scorer.py` — trains on APPROVED `signal_records`, AUC ≥ 0.62 gate, ECE calibration, staleness detection, wired into Gate 2 via `ScorerFactory` | ✅ Done |
| **Phase 2I** | 7 | Strategy Registry: `strategy/spec.py`, `registry.py`, `builder.py` — full lifecycle RESEARCH→BACKTEST→PAPER→LIVE with dual-store (YAML+SQLite), 4-gate PAPER promotion, 3-gate LIVE promotion | ✅ Done |
| **Phase 2J** | 7–8 | Strategy config tuning: accumulate ≥30 days of `depth@100` LOB data, run walk-forward backtests, tune params, run tick replay validation, promote first spec to PAPER | 🔜 Next |
| **Phase 3** | 9–12 | Dashboard: Dash multi-page app (6 pages: /live, /lob, /walls, /backtest, /registry, /config), REST API, LOB snapshot writer, decay monitoring, LIVE promotion pipeline | ✅ Done |

---

## TODO

- [ ] Write up strategy documentation — entry logic, gate rationale, Wall/Absorption/Sweep signal design
- [ ] Write up model documentation — FeatureComputer inputs, XGBoost scorer architecture, training pipeline. should include the diagram for how our components interact, including exchange, LOB, trade management.
- [ ] Write up trading algorithm documentation — end-to-end flow from LOB tick to order submission
- [ ] Consider placing a minimum-quantity resting order behind/after a significant liquidity wall — a fill on that order signals the wall has been consumed, providing a cleaner consumption trigger than depth-diff heuristics. Quantity must be as small as possible (min tick size on Binance Spot Testnet).
- [ ] run through start_test.sh and make sure the trade execution with injector is working
- [ ] review all code and test scripts to ensure no unused classes or functions
