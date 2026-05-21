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
├── .env.example
│
├── core/                      # Real-time data processing
│   ├── ws_consumer.py         # WebSocket consumer + HeartbeatMonitor
│   ├── lob_recorder.py        # Always-on LOB data collector (real Binance public stream)
│   ├── lob_engine.py          # Local Order Book + full state machine
│   ├── cvd.py                 # Standalone CVD calculator
│   ├── pattern_detector.py    # OHLCV chart pattern detection (context/boost)
│   ├── signal_telemetry.py    # Signal record writer (all gates, pass + fail)
│   └── startup_reconciler.py  # Exchange state reconciliation on startup + midnight reset
│
├── strategy/                  # Alpha generation and execution
│   ├── features.py            # FeatureComputer + WelfordOnline (single source of truth)
│   ├── microstructure.py      # Wall Identification / Absorption / Sweep + Fresh Wall
│   ├── executor.py            # StrategyExecutor — 7-Gate pipeline + confidence scorer
│   └── spec.py                # StrategySpec — immutable YAML-backed strategy descriptor
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
├── scripts/
│   ├── test_connection.py     # Connectivity + auth check
│   └── test_orders.py         # BUY + SELL round-trip test
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
└── dashboard.py               # Streamlit dashboard (legacy — active during Phase 1)
```

---

## Data Pipeline

### Stream Selection

> ## ⚠️ CRITICAL DATA REQUIREMENT — depth@20 IS INSUFFICIENT FOR THIS STRATEGY
>
> The current LOB Recorder subscribes to `btcusdt@depth@20@100ms` — only 20 price levels.
> At BTC prices (~$77,000), 20 levels span just **$2–4 from mid**. All detected walls are
> within $3 of the current price.
>
> The Sweep + Fresh Wall strategy is designed to trade **deep-book institutional walls**
> that sit far enough from mid to absorb aggressive flow without being immediately consumed.
> The minimum viable SL distance at 30bps round-trip costs is **~$115 at $77k BTC**
> (breakeven = entry_price × round_trip_pct / 2). No wall detected from depth@20 data
> will ever exceed this threshold — **every signal generated from depth@20 data is
> guaranteed to be a net loser after transaction costs.**
>
> **Required change before backtesting is meaningful:**
> Switch `core/lob_recorder.py` to subscribe to `btcusdt@depth@100@100ms`
> (100 levels, spanning ~$50–200 from mid at current prices).
>
> **Storage strategy — price bucketing:**
> 100 levels recorded at full tick precision (~$0.01 granularity) is 5× the storage of
> depth@20. To keep `lob_tick.db` manageable, aggregate levels into fixed-width price
> buckets before writing (e.g., $25 buckets). Each bucket records total qty for all
> levels within that $25 range. This reduces rows-per-snapshot by 60–80% while
> preserving LOB shape at the distances relevant for wall detection. Sub-bucket
> precision (exact price within a $25 range) is irrelevant for identifying walls
> $50–500 from mid.
>
> Existing `data/lob_tick.db` collected with depth@20 **cannot be used for strategy
> validation**. It remains valid only for engine smoke-testing and infrastructure checks.

| Stream | Purpose | Update Rate |
|--------|---------|-------------|
| `btcusdt@aggTrade` | CVD · aggressive volume | Per taker sweep |
| `btcusdt@depth@100@100ms` | Wall detection · OBI · spread — **100 levels required** | 100ms |
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
UNINITIALISED → SNAPSHOT_PENDING → BUFFERING → SYNCED
                                                  │
                                            GAP_DETECTED → REINITIALISING → SNAPSHOT_PENDING
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
| **KS-3 Slippage Decay** | Rolling 5-trade avg slippage > `research_bps × 1.5` |

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

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add testnet API keys to `.env`:
```
BINANCE_API_KEY=your_testnet_key
BINANCE_API_SECRET=your_testnet_secret
```

```bash
# Verify connectivity and auth
python scripts/test_connection.py

# Start LOB Recorder (terminal 1 — collects real Binance tick data)
python -m core.lob_recorder

# Start trading engine (terminal 2)
python main.py

# Start Dash dashboard (terminal 3 — Phase 3)
python dashboard/app.py           # → http://127.0.0.1:8050

# Start legacy Streamlit dashboard (Phase 1 only — deprecated)
streamlit run dashboard.py        # → http://localhost:8501

# Run tests
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
| `LOB_DEPTH` | `20` | Order book levels fetched — **must be changed to `100`** (see ⚠️ above) |
| `LOB_HISTORY` | `18000` | In-memory bars retained (~5h) |

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

### Persistence
| Setting | Default | Description |
|---|---|---|
| `REGISTRY_DB` | `strategies/registry.db` | Strategy registry + signal telemetry |
| `LOB_TICK_DB` | `data/lob_tick.db` | LOB Recorder tick data |

---

## Tests

```
Phase 1 — Live trading engine
tests/test_risk_engine.py           49 tests  — 5-tier, killswitch, pyramid, circuit breakers
tests/test_microstructure_engine.py 30 tests  — legacy microstructure engine
tests/test_executor.py              29 tests  — all 7 gates, telemetry emission
tests/test_order_manager.py         22 tests  — IOC entry, OCO bracket, fill handling
tests/test_lob_engine.py            20 tests  — state machine, gap detection, wall scan
tests/test_startup_reconciler.py    14 tests  — reconciliation, midnight reset
tests/test_microstructure.py        14 tests  — wall identification, absorption, sweep
tests/test_lob_recorder.py          14 tests  — recorder flush, reconnect, stats
tests/test_features.py              14 tests  — Welford, no-lookahead, VWAP reset
tests/test_cvd.py                   14 tests  — buy/sell CVD, 5-bar delta, std
tests/test_db_writer.py             12 tests  — SQLite write, upsert, purge
tests/test_signal_telemetry.py      10 tests  — flush, batch, timeout, outcome update
tests/test_orders.py                10 tests  — order domain classes and enums
tests/test_pattern_detector.py       8 tests  — ATR, swing detection, S/R breakout
tests/test_ws_consumer.py            6 tests  — heartbeat states, shared state update
tests/test_models.py                 6 tests  — PortfolioState, WallState, FeatureVector
tests/test_integration.py            2 tests  — end-to-end signal → execution pipeline

Phase 3 — REST API + LOB snapshot writer + Dash dashboard
tests/test_rest_api.py              10 tests  — /api/health, /api/portfolio, /api/killswitch
tests/test_lob_snapshot_writer.py    6 tests  — snapshot writer, rolling cap, WAL mode
tests/test_dashboard_live.py         5 tests  — /live page callback, engine badge, kill switch
tests/test_dashboard_registry.py     4 tests  — /registry page, strategy lifecycle display

Phase 2 — Backtesting + strategy lifecycle
tests/test_bt_event_engine.py       23 tests  — event-driven engine: tiers, exits, full run
tests/test_registry.py              16 tests  — lifecycle gates, YAML roundtrip, promotion
tests/test_bt_tick_replay.py        13 tests  — tick replay fidelity, streaming, CVD reset
tests/test_bt_walk_forward.py        9 tests  — window splits, OOS isolation, leaderboard
tests/test_bt_metrics.py             9 tests  — Sharpe, MDD, PF, composite score
tests/test_validator.py              7 tests  — null repair, duplicate removal, gap detection
tests/test_scorer.py                 7 tests  — XGBoost train, AUC, save/load, fallback
tests/test_fetcher.py                6 tests  — OHLCV fetch, cache, resample
tests/test_bt_signals.py             6 tests  — signal arrays, no-lookahead, SL/TP NaN
tests/test_bt_vectorbt.py            5 tests  — Optuna optimisation, sensitivity
tests/test_bt_costs.py               5 tests  — round-trip cost, maker/taker, zero qty
──────────────────────────────────────────────────────────────────────────────
Total                              405 tests
```

---

## Path to Paper Trading

The trading engine is fully operational on the Binance Spot Testnet. The remaining work before enabling live paper trading (`DRY_RUN=False`) is data accumulation and strategy validation:

| Step | Action | Status |
|------|--------|--------|
| **1. Accumulate LOB data** | ⚠️ **Switch recorder to `depth@100` with $25-bucket aggregation before collecting data for strategy use** (see Data Pipeline warning above). Then keep `lob_recorder` running continuously. Target ≥30 days for statistically robust walk-forward splits. Data collected at depth@20 is valid for infrastructure smoke tests only — not strategy validation. | 🔜 Blocked on recorder upgrade |
| **2. Fetch OHLCV history** | Run `data/fetcher.py` to populate `ohlcv_cache.db` for Path B backtesting | 🔜 Pending |
| **3. Run backtests** | Path A: `backtesting/event_engine.py` replay of `lob_tick.db`. Path B: `backtesting/walk_forward.py` OHLCV walk-forward via VectorBT + Optuna | 🔜 Pending |
| **4. Tune strategy config** | Optimise wall sigma (`LOB_WALL_SIGMA`), confidence threshold (`MIN_CONFIDENCE`), ATR multipliers via Optuna. Evaluate Sharpe, Sortino, MDD, PF across out-of-sample windows. | 🔜 Pending |
| **5. Train XGBoost scorer** | `strategy/scorer.py` — `XGBoostScorer.train_from_registry()` trains on APPROVED `signal_records`; `ScorerFactory` auto-loads at startup, falls back to `RuleBasedScorer` if no pkl exists. AUC gate ≥ 0.62, ECE logged, staleness warning after 30 days. | ✅ Done |
| **6. Freeze StrategySpec** | `strategy/spec.py` + `strategy/registry.py` + `strategy/builder.py` — `StrategyBuilder` produces BACKTEST-status specs; `StrategyRegistry` dual-stores to YAML + SQLite with 4-gate PAPER promotion (≥50 OOS trades, Sharpe ≥ 1.0, MDD ≤ 15%, PF ≥ 1.3) and 3-gate LIVE promotion. | ✅ Done |
| **7. Promote to paper trading** | Set `DRY_RUN=False` in `.env`. Monitor Gate funnel and PnL via `dashboard.py`. | 🔜 Pending |

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Runtime | Python | 3.11+ | Async, type hints |
| Async | asyncio | stdlib | Event loop |
| LOB Recorder WS | websockets | 12.0 | Real Binance public stream |
| Exchange WS/REST | python-binance | 1.0.19 | Testnet execution |
| Historical data | ccxt | 4.3.x | OHLCV fetch (no key) |
| Data | pandas + numpy | 2.2 + 1.26 | DataFrames + arrays |
| Indicators | pandas-ta | 0.3.14b | ATR, RSI |
| Stats | scipy | 1.13.0 | linregress, statistics |
| ML scorer | xgboost | 2.0.x | Confidence scoring (Phase 2) |
| Backtest fast | vectorbt | 0.26.x | OHLCV Path B (Phase 2) |
| Optimiser | optuna | 3.6.x | Bayesian param search (Phase 2) |
| Config | pydantic-settings | 2.3.x | .env management |
| Dashboard | Dash + dash-bootstrap | 2.17 + 1.6 | All UI pages (Phase 3 — primary) |
| Dashboard (legacy) | Streamlit | 1.35.0 | Phase 1 only — deprecated |
| Charts | plotly | 5.22.x | All visualisations |
| Persistence | SQLite | stdlib | All databases |
| Logging | loguru | 0.7.x | Structured logs |
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
