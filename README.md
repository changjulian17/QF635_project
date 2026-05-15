# CryptoSentinel

Real-time microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

**Version 3.0** — Granular Level-Specific LOB Architecture. Alpha is derived from Wall–Aggression interaction at specific price levels, not aggregate OBI.

---

## System Philosophy — The Granular LOB Thesis

> *"Price movement is a function of aggressive market orders consuming specific, persistent liquidity walls — not the aggregate balance of the book."*

CryptoSentinel v3.0 is built on a precise institutional insight: price moves when and because aggressive orders hit specific resting liquidity **Walls** and successfully consume them.

```
Resting Liquidity Wall
  A cluster of passive limit orders at a specific price level so large
  that it exceeds 2.5σ above the median depth of the surrounding 10 ticks.

Market Aggression
  Aggressive market or IOC limit orders that deliberately target a Wall.
  Visible in the aggTrade stream as concentrated volume hitting a level.

Price Movement
  Absorption: aggression hits Wall → Wall reloads → price HOLDS → no trade
  Sweep:      aggression consumes Wall → price MOVES → Fresh Wall appears
              behind the breakout → price CONTINUES

The system detects which scenario is unfolding in real time and
acts only when a Sweep + Fresh Protective Wall is confirmed.
```

### Signal Hierarchy

```
PRIMARY — Granular LOB Wall Interaction
  Wall Identified     Level depth > 2.5σ above surrounding 10-tick median
  Absorption          Aggression hits Wall, price holds — Wall reloads
  Sweep + Fresh Wall  Wall consumed (price > 0.03%), new protective Wall
                      appears immediately behind breakout
  → FIRES THE TRADE (Sweep + Fresh Wall only)
  → Absorption is CONTEXT — no trade, arms system

SECONDARY FILTER — OBI Sentiment Alignment
  OBI must directionally agree with the Sweep
  LONG Sweep: OBI > -0.20  (not strongly bearish)
  SHORT Sweep: OBI < +0.20 (not strongly bullish)

OPTIONAL BOOST — Chart Pattern Context
  Pattern present at Wall level: +0.10–0.15 to XGBoost confidence score
  Pattern absent: signal still valid

CONFIDENCE GATE — Rule-Based Scorer (XGBoost placeholder)
  Weighted combination of OBI alignment, vol ratio, spread, CVD
  Minimum score 0.58 required to proceed
```

---

## Project Structure

```
cryptosentinel/
│
├── main.py                    ← Async orchestrator — all coroutines
├── config.py                  ← Pydantic settings from .env
├── models.py                  ← Shared dataclasses and enums
├── requirements.txt
├── .env.example
│
├── core/                      ← Real-time data processing
│   ├── ws_consumer.py         WebSocket consumer + HeartbeatMonitor
│   ├── lob_recorder.py        Always-on LOB data collector (real Binance API)
│   ├── lob_engine.py          Local Order Book + state machine
│   ├── cvd.py                 Standalone CVD calculator
│   ├── pattern_detector.py    OHLCV chart pattern detection (context/boost)
│   ├── signal_telemetry.py    Signal record writer (all gates, pass + fail)
│   └── startup_reconciler.py  Exchange state reconciliation on startup
│
├── strategy/                  ← Alpha generation and execution
│   ├── features.py            FeatureComputer (single source of truth)
│   ├── microstructure.py      Wall identification, Absorption, Sweep detection
│   └── executor.py            7-Gate strategy executor + rule-based scorer
│
├── risk/                      ← Risk management
│   ├── engine.py              RiskEngine (5-tier throttling, DOV, circuit breakers)
│   ├── budget.py              DailyBudget (shared pool for all legs)
│   ├── pyramid.py             PyramidController (3 legs max)
│   └── killswitch.py          GlobalKillswitch (hard override — 3 triggers)
│
├── execution/                 ← Order management
│   └── order_manager.py       IOC aggressive limit orders + OCO brackets
│
├── engine/                    ← Legacy (being migrated to above dirs)
│   └── db_writer.py           Async SQLite persistence
│
├── scripts/
│   ├── test_connection.py     Connectivity + auth check
│   └── test_orders.py         BUY + SELL round-trip test
│
├── tests/                     ← pytest unit tests
│   ├── test_models.py
│   ├── test_lob_engine.py
│   ├── test_microstructure_engine.py
│   ├── test_db_writer.py
│   ├── test_pattern_detector.py
│   ├── test_risk_engine.py
│   ├── test_ws_consumer.py
│   ├── test_cvd.py
│   ├── test_features.py
│   ├── test_microstructure.py
│   ├── test_executor.py
│   └── test_signal_telemetry.py
│
├── strategies/
│   ├── registry.db            SQLite: signal_records + system events
│   └── configs/               Frozen YAML strategy specs
│
├── data/
│   ├── lob_tick.db            LOB recorder database (real Binance data)
│   └── ohlcv_cache.db         OHLCV SQLite cache
│
├── dashboard.py               ← Streamlit UI (Phase 1; Dash migration in Phase 3)
└── pages/
    └── health.py              System health checker
```

---

## Data Pipeline

```
Real Binance (no key needed)          Binance Testnet
  wss://stream.binance.com              wss://stream.testnet.binance.vision
          │                                       │
  LOBRecorder                           WS Consumer (+ HeartbeatMonitor)
  lob_tick.db                                     │
  (tick data for backtesting)    ┌────────────────┼────────────────┐
                                 │                │                │
                         raw_tick_queue    depth_queue      candle_queue
                         (aggTrade)        (depth20@100ms)  (kline_5m)
                                 │                │                │
                         CVD Calculator    LOB Engine      PatternDetector
                                 │                │                │
                                 └────────────────┘                │
                                          │                        │
                                  FeatureComputer                  │
                                  (15 features, Welford)           │
                                          │              pattern_signal
                                          └──────┬────────────────┘
                                                 │
                                    MicrostructureDetector
                                    (Wall ID → Absorption → Sweep)
                                                 │
                                          micro_signal_queue
                                                 │
                                       StrategyExecutor (7 Gates)
                                       + telemetry_queue ──► SignalTelemetry
                                                 │               │
                                          signal_queue     registry.db
                                                 │
                                          RiskEngine
                                          (5-tier, DOV, Pyramid,
                                           GlobalKillswitch checks)
                                                 │
                                          order_queue
                                                 │
                                         OrderManager
                                         (IOC limit → OCO bracket)
                                                 │
                                         fill_queue
                                                 │
                                       Portfolio State ──► RiskEngine
```

### Async Queue Architecture

| Queue | Producer | Consumer | maxsize |
|-------|----------|----------|---------|
| `raw_tick_queue` | WS Consumer | CVD Calculator | 2,000 |
| `depth_queue` | WS Consumer | LOB Engine | 500 |
| `candle_queue` | WS Consumer | PatternDetector, FeatureComputer | 200 |
| `micro_signal_queue` | MicrostructureDetector | StrategyExecutor | 100 |
| `signal_queue` | StrategyExecutor | Risk Engine | 100 |
| `order_queue` | Risk Engine | Order Manager | 50 |
| `fill_queue` | Order Manager | Portfolio State | 200 |
| `telemetry_queue` | StrategyExecutor | SignalTelemetry | 500 |

---

## Core Components

### WebSocket Consumer + HeartbeatMonitor (`core/ws_consumer.py`)

Subscribes to four Binance streams simultaneously:

| Stream | Purpose |
|--------|---------|
| `btcusdt@aggTrade` | CVD · aggressive volume per taker sweep |
| `btcusdt@depth20@100ms` | Wall detection · OBI · spread (top 20 levels) |
| `btcusdt@bookTicker` | Best bid/ask for spread calculation |
| `btcusdt@kline_5m` | OHLCV candles for pattern detection |

**HeartbeatMonitor** — tracks delta between Binance event time (`E` field) and local time on every message:

| State | Delta | Action |
|-------|-------|--------|
| HEALTHY | < 200ms | Normal trading |
| DEGRADED | 200–500ms | Log warning; continue |
| CRITICAL | > 500ms × 3 consecutive | Killswitch condition (KS-2) |

### LOB Recorder (`core/lob_recorder.py`)

Always-on data collector connecting to the **real** Binance public stream (no API key). Records `depth_snapshots` and `agg_trades` to `data/lob_tick.db` for tick-level backtesting.

- WAL mode + hybrid flush (500 records or 5 seconds)
- Runs independently of the testnet trading loop
- Can be started standalone: `python -m core.lob_recorder`

> **Rule 4:** LOB Recorder uses the real Binance API. Backtesting on testnet LOB data is meaningless (synthetic prices).

### LOB Engine — State Machine (`core/lob_engine.py`)

Full synchronisation state machine:

```
UNINITIALISED → SNAPSHOT_PENDING → BUFFERING → SYNCED
                                                  │
                                            GAP_DETECTED → REINITIALISING
                                                               │
                                                        SNAPSHOT_PENDING (retry)
```

Gap severity tiering:

| Gap Duration | Action |
|---|---|
| < 500ms | CONTINUE — re-sync, keep trading |
| 500ms – 5s | HALT_ENTRIES — no new entries, keep open positions |
| > 5s | CLOSE_REVIEW — log warning, consider closing |

`lob_status` ∈ `{SYNCED, STALE, DISCONNECTED}` — gated at every trade entry.

### Microstructure Detector (`strategy/microstructure.py`)

Implements the three v3.0 level-specific signals:

**Signal 1 — Wall Identification:**
```
Wall at level L when:
  depth[L] > median(depth[L-5 : L+5]) + 2.5 × std(depth[L-5 : L+5])
```
Walls are tracked over time via `WallState`: `qty_initial`, `qty_current`, `aggression_hits`, `reload_ratio`.

**Signal 2 — Absorption (Context — no trade):**
```
All must hold:
  1. Wall persistent ≥ 500ms
  2. Aggressive flow hitting the Wall
  3. Price has NOT moved > 0.03%  (absorption, not sweep)
  4. Wall reloaded ≥ 70% of original qty
```
Absorption **arms** the system. A subsequent Sweep of an absorbed Wall has higher expected move.

**Signal 3 — Sweep + Fresh Protective Liquidity (Trade trigger):**
```
All must hold:
  1. Wall qty consumed to < 15% of original
  2. Price moved > 0.03% through Wall level
  3. CVD spike > 1.5σ  (confirms directional aggression)
  4. Fresh Wall appeared BEHIND breakout within 3 seconds
     (institutions defending the new level — not a false breakout)
```

**Signal 4 — Wedge Compression (XGBoost boost only):**
Chart pattern context from OHLCV. `pattern_r2` feature boosts XGBoost confidence +0.10–0.15. Pattern alone never triggers a trade.

### Feature Computer (`strategy/features.py`)

Single class — identical code in live trading and backtesting. Computes 15 features using **Welford online algorithm** (strictly causal, no look-ahead, no NaN warm-up).

| Feature | Formula |
|---------|---------|
| `price_vs_vwap` | `tanh((close - vwap) / (2 × ATR))` |
| `obi_zscore` | Welford z-score of OBI (secondary sentiment filter) |
| `cvd_delta` | `CVD[now] - CVD[now-5]` |
| `vol_ratio` | `volume / Welford mean` |
| `atr_percentile` | Welford percentile rank of ATR |
| `rsi_value` | Wilder RSI period=14 |
| `spread_bps` | `(ask - bid) / mid × 10,000` |
| `pattern_r2` | Trendline regression R² |
| `vwap_reclaim` | 1 if close crossed above VWAP |
| `vol_climax` | 1 if vol_ratio > 3× |
| `cvd_positive` | 1 if cvd_delta > 0 |
| `wall_detected` ★ | 1 if Wall (>2.5σ) within 5 ticks of mid |
| `wall_distance_bps` ★ | Distance from mid to nearest Wall |
| `absorption_ratio` ★ | `wall.qty_current / wall.qty_initial` |
| `protection_wall_present` ★ | 1 if fresh Wall detected post-sweep ≤3s |

★ = new v3.0 features. VWAP resets at UTC midnight every session.

### 7-Gate Trade Life Cycle (`strategy/executor.py`)

Every potential trade passes through seven sequential gates. Failure at any gate writes a `SignalRecord` to `registry.db` with the gate and rejection reason.

| Gate | Name | Pass Condition |
|------|------|----------------|
| **Gate 0** | Data Fidelity | `lob_status == SYNCED` AND `heartbeat != CRITICAL` |
| **Gate 1** | Microstructure Trigger | `sweep_detected == True` AND `protection_wall` present |
| **Gate 2** | Confidence Score | `score >= 0.58` (rule-based placeholder; XGBoost after training) |
| **Gate 3** | Capital Gate | `remaining_budget > 0` AND `tier != HALTED` |
| **Gate 4** | Order Selection | `spread_bps < 2 × spread_p95` |
| **Gate 5** | Execution Sync | `signal_age < 200ms` AND `last_delta_ms < 200ms` |
| **Gate 6** | Persistence Monitor | Protection Wall still present (post-entry, ongoing) |

Every evaluated signal — pass or fail — is recorded. This is the primary tool for strategy improvement.

```sql
-- Gate funnel: where are signals being lost?
SELECT gate_passed, COUNT(*) as n
FROM signal_records
GROUP BY gate_passed ORDER BY n DESC;
```

### Risk Engine (`risk/engine.py`)

**Daily Open Value (DOV)** — fixed at UTC midnight. All positions draw from a shared pool.
```
Hard limit = DOV × 1%
Budget = hard_limit − (realised_pnl + unrealised_pnl)
```

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

**Pyramid Rules (up to 3 legs):**
- Leg 1: up to 60% of remaining budget
- Leg 2: 50% of Leg 1 risk; only if Leg 1 in profit; trail Leg 1 stop to breakeven
- Leg 3: 25% of Leg 1 risk; only if Legs 1+2 in profit; trail stop to Leg 2 entry

**Circuit Breakers:**

| Trigger | Condition | Action |
|---------|-----------|--------|
| Budget breach | `daily_loss ≥ 1% DOV` | HALTED |
| Drawdown | `drawdown ≥ 5% from peak` | HALTED |
| Consecutive losses | `≥ 3 losses` | PAUSED 15 min |
| Tier REDUCED | `daily_loss ≥ 0.5%` | 50% size |
| Tier MINIMAL | `daily_loss ≥ 0.75%` | 25% size |

### Global Killswitch (`risk/killswitch.py`)

Hard override — bypasses all other logic. Once fired, cannot be reset without system restart.

| Trigger | Condition |
|---------|-----------|
| **KS-1 Budget Breach** | `realised_pnl + unrealised_pnl < -(DOV × 1%)` |
| **KS-2 Heartbeat Loss** | `heartbeat_status == "CRITICAL"` (>500ms × 3 packets) |
| **KS-3 Slippage Decay** | Rolling 5-trade avg slippage > `research_bps × 1.5` |

On fire: `CLOSE_ALL` → cancel all orders → market-close all positions → `HALTED` → write `KILLSWITCH_FIRED` to `registry.db`.

### Execution Layer (`execution/order_manager.py`)

All entries use **aggressive IOC limit orders** — never market orders.

```
LONG:  limit_price = best_ask + (spread × 0.5)
SHORT: limit_price = best_bid − (spread × 0.5)
Time-in-force: IOC — cancel if not filled within 200ms (signal is stale)
```

After fill confirmation: place OCO bracket (take-profit limit + stop-limit with 0.1% buffer).

---

## Quick Start

```bash
# Create and activate virtual environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Add testnet API keys to .env (get from https://testnet.binance.vision)
cp .env.example .env
# Edit .env with your keys

# Verify connectivity and auth
python scripts/test_connection.py

# Start LOB Recorder — collect real tick data (terminal 1, always-on)
python -m core.lob_recorder

# Start trading engine (terminal 2)
python main.py

# Start Streamlit dashboard (terminal 3)
streamlit run dashboard.py        # → http://localhost:8501

# Run test suite
python -m pytest tests/ -v
```

---

## Configuration Reference

All settings live in `config.py`, overridable via `.env`.

| Setting | Default | Description |
|---------|---------|-------------|
| `BINANCE_API_KEY` | — | Testnet API key |
| `BINANCE_API_SECRET` | — | Testnet API secret |
| `BINANCE_TESTNET` | `True` | Always use testnet for execution |
| `SYMBOL` | `BTCUSDT` | Trading pair |
| `TIMEFRAME` | `5m` | Kline stream interval |
| `DRY_RUN` | `True` | Skip live order submission |
| `STARTING_EQUITY` | `10000.0` | Initial paper portfolio value |
| `MIN_CONFIDENCE` | `0.58` | Minimum confidence score (Gate 2) |
| `MAX_DRAWDOWN_PCT` | `0.05` | Drawdown circuit breaker (5%) |
| `DAILY_LOSS_LIMIT_PCT` | `0.01` | Daily loss halt (1%) |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 15-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.01` | Equity risked per trade (1%) |
| `KELLY_FRACTION` | `0.25` | Kelly fraction applied to raw size |
| `ATR_MULTIPLIER_SL` | `1.5` | Stop-loss distance in ATR units |
| `ATR_MULTIPLIER_TP` | `3.0` | Take-profit distance in ATR units |
| `LOB_WALL_SIGMA` | `2.5` | Wall identification σ threshold |
| `LOB_WALL_WINDOW` | `5` | Ticks each side for Wall detection |
| `HEARTBEAT_WARN_MS` | `200` | Latency warning threshold |
| `HEARTBEAT_CRITICAL_MS` | `500` | Latency critical threshold |
| `HEARTBEAT_CONSEC_LIMIT` | `3` | Consecutive critical packets → Killswitch |
| `IOC_TIMEOUT_MS` | `200` | IOC order expiry; stale signal if missed |
| `SLIPPAGE_RESEARCH_BPS` | `3.0` | Expected slippage per side |
| `SLIPPAGE_MULTIPLIER` | `1.5` | Killswitch fires at research × multiplier |
| `PATTERN_LOOKBACK` | `50` | Candle history for pattern detection |
| `MIN_R2` | `0.80` | Minimum R² for trendline fit |
| `LOB_RECORDER_WS` | `wss://stream.binance.com:9443` | Real Binance stream (LOB Recorder) |
| `REGISTRY_DB` | `strategies/registry.db` | Signal telemetry + system events |
| `LOB_TICK_DB` | `data/lob_tick.db` | LOB Recorder output |

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Runtime | Python | 3.11+ | Async, type hints |
| Async | asyncio | stdlib | Event loop |
| Exchange WS/REST | python-binance | 1.0.19 | Testnet execution |
| LOB Recorder WS | websockets | 12.0 | Real Binance public stream |
| Historical data | ccxt | 4.3.x | OHLCV fetch (no key needed) |
| Data | pandas + numpy | 2.2 + 1.26 | DataFrames + arrays |
| Indicators | pandas-ta | 0.3.14b | ATR, RSI |
| Stats | scipy | 1.13.0 | linregress, statistics |
| ML scorer | xgboost | 2.0.x | Confidence scoring (Phase 2) |
| Optimiser | optuna | 3.6.x | Bayesian param search (Phase 2) |
| Config | pydantic-settings | 2.3.x | .env management |
| Dashboard | Streamlit | 1.35.0 | Phase 1 UI (Dash in Phase 3) |
| Charts | plotly | 5.22.x | All visualisations |
| Persistence | SQLite | stdlib | All databases |
| Logging | loguru | 0.7.x | Structured logs |
| Serialisation | PyYAML | 6.0.1 | StrategySpec configs |

---

## Tests

```
tests/test_models.py                  6 tests  — PortfolioState, WallState, FeatureVector
tests/test_lob_engine.py             16 tests  — LOB state machine + sequence validation
tests/test_microstructure_engine.py  16 tests  — legacy OBI, CVD, sweep, book-flip
tests/test_db_writer.py               9 tests  — SQLite write, upsert, purge, WAL
tests/test_pattern_detector.py        8 tests  — ATR, swing detection, S/R breakout
tests/test_risk_engine.py            20 tests  — 5-tier throttling, killswitch, pyramid
tests/test_ws_consumer.py             4 tests  — HeartbeatMonitor states
tests/test_cvd.py                     3 tests  — CVD calculation and delta
tests/test_features.py                4 tests  — Welford causal, no look-ahead
tests/test_microstructure.py          7 tests  — Wall ID, Absorption, Sweep
tests/test_executor.py                7 tests  — 7-Gate funnel + telemetry
tests/test_signal_telemetry.py        4 tests  — batch flush, timeout flush, outcome update
────────────────────────────────────────────────────────────
Total                               ~104 tests  — all passing
```

---

## System Invariants (12 Coherence Rules)

These invariants hold across all code. Any change that violates them is incorrect regardless of other arguments.

| # | Rule |
|---|------|
| 1 | **FeatureComputer is one class.** Called identically in live trading, tick backtest, and OHLCV backtest. No separate calculation paths. No inline feature code elsewhere. |
| 2 | **Microstructure fires, Pattern boosts.** A trade requires a Sweep + Fresh Wall signal. A chart pattern adds XGBoost confidence. A pattern alone never triggers a trade. |
| 3 | **Risk budget is a shared pool.** All open legs (pyramid) draw from the same daily budget. Unrealised PnL counts against the budget — not just realised. |
| 4 | **LOB Recorder uses real API, not testnet.** LOB Recorder connects to `wss://stream.binance.com` (real prices, no key). Execution goes to testnet. Backtesting on testnet LOB data is meaningless. |
| 5 | **DOV is fixed at UTC midnight.** Daily Open Value is recorded once at session start. Never recalculated intraday. |
| 6 | **StrategySpec is immutable after PAPER promotion.** Any change creates a new version number. Old YAML is preserved, never deleted. |
| 7 | **Tick backtest must simulate level-specific Wall interactions.** Must use `lob_tick.db` to replay actual depth-level data. Wall detection, Absorption, and Sweep + Fresh Wall must run on replayed depth20 snapshots in timestamp order. |
| 8 | **Every evaluated signal must be written to signal_records.** The StrategyExecutor writes a SignalRecord at every gate — pass or fail. A signal that disappears without a record is a debugging black hole. |
| 9 | **No rolling z-scores with NaN-fill in FeatureComputer.** All z-scores and percentile ranks must use `WelfordOnline`. The pattern `rolling(N).std().fillna(series.std())` is forbidden — it uses future data for early observations. |
| 10 | **No market orders for microstructure entries.** All entries must use aggressive IOC limit orders (`best_ask + spread × 0.5`). The slippage sensitivity table must show `viable=true` at 3bps before PAPER promotion. |
| 11 | **HeartbeatMonitor must run on every WebSocket message.** `HeartbeatMonitor.record(event_time_ms)` is called on every message in `_receive_loop()` — not sampled. `heartbeat_status` must be available to Gate 0 and the Killswitch via `SharedState`. |
| 12 | **GlobalKillswitch cannot be bypassed.** Once `is_active == True`, no new orders may be placed. Checks run at every portfolio update, every WebSocket message, and every fill confirmation. Manual reset requires system restart — there is no dismiss function. |

---

## Phased Delivery Plan

### Phase 1 — Foundation + LOB Recorder (Weeks 1–3)
*Goal: LOB Recorder collecting real tick data, testnet trading loop fully operational, all v3.0 additions built.*

**Week 1 — Infrastructure (data-first)**
- [ ] LOB Recorder running: `python -m core.lob_recorder`
- [ ] Verify `data/lob_tick.db` growing (`depth_snapshots` + `agg_trades`)
- [ ] LOB Engine full state machine: `UNINITIALISED → SNAPSHOT_PENDING → BUFFERING → SYNCED → GAP_DETECTED`
- [ ] `lob_status` field exposed via `SharedState`; gap severity tiering implemented
- [ ] Directory restructure: `core/`, `strategy/`, `risk/`, `execution/`
- [ ] HeartbeatMonitor wired to every WebSocket message
- [ ] Stream update: `kline_1s → kline_5m`, `depth@100ms diff → depth20@100ms snapshot`
- [ ] `config.py` + `.env` extended with all new settings

**Week 2 — Signal layer + telemetry**
- [ ] `core/cvd.py` — standalone CVD Calculator with Welford std
- [ ] `strategy/features.py` — FeatureComputer: all 15 features, Welford online (no rolling z-scores)
- [ ] `strategy/microstructure.py` — Wall ID, Absorption, Sweep + Fresh Wall detectors
- [ ] `core/pattern_detector.py` — moved; pattern_r2 feeds FeatureVector as boost only
- [ ] `signal_records` table in `strategies/registry.db` (WAL mode)
- [ ] `core/signal_telemetry.py` — async batch writer, flush every 10s or 50 records
- [ ] `strategy/executor.py` — 7-Gate executor; `SignalRecord` emitted at every gate exit (Rule 8)
- [ ] Risk Engine Gate 0: `lob_status != "SYNCED"` → immediate reject
- [ ] Verify: after 1 hour of running, `signal_records` has rows with `GATE_0_FAIL`, `GATE_1_FAIL`, `GATE_2_FAIL`, `APPROVED`

**Week 3 — Risk, execution, reconciliation**
- [ ] `risk/engine.py` — 5-tier throttling, DOV, shared budget pool, position sizing formula
- [ ] `risk/budget.py` — DailyBudget (realised + unrealised combined)
- [ ] `risk/pyramid.py` — PyramidController (3 legs, trail stop on add)
- [ ] `risk/killswitch.py` — GlobalKillswitch (KS-1 budget, KS-2 heartbeat, KS-3 slippage)
- [ ] `execution/order_manager.py` — IOC aggressive limit orders; no market orders (Rule 10)
- [ ] `core/startup_reconciler.py` — query exchange on every `main.py` start; restore daily PnL
- [ ] `main.py` — full rewrite with correct startup sequence and midnight reset loop
- [ ] Graceful shutdown: SIGTERM handler; telemetry flushed; open positions logged (not auto-closed)
- [ ] First testnet paper trade placed, logged to `registry.db` and `signal_records`

---

### Phase 2 — Backtest + Strategy Builder (Weeks 4–8)
*Goal: First StrategySpec from real tick data, XGBoost confidence scorer trained.*

> Requires ≥ 2 weeks of `data/lob_tick.db` accumulated from Phase 1.

**Weeks 4–5 — OHLCV Path B** (can start immediately, no LOB data needed)
- [ ] `data/fetcher.py` — OHLCVFetcher (CCXT + SQLite cache)
- [ ] `data/validator.py` — 9-check data quality validator
- [ ] `backtesting/signals.py` — all 5 OHLCV pattern generators (VectorBT-compatible)
- [ ] `backtesting/vectorbt_runner.py` — Phase 1: VectorBT + Optuna (300 TPE trials)
- [ ] `backtesting/event_engine.py` — Phase 2: event-driven OOS validation (zero look-ahead)
- [ ] `backtesting/walk_forward.py` — 90d train / 30d test rolling windows
- [ ] `backtesting/metrics.py` — Sharpe, Sortino, Calmar, MDD, PF, WR, EV
- [ ] `backtesting/costs.py` — TransactionCostModel (slippage sensitivity table per StrategySpec)

**Week 6 — Tick Path A** (requires ≥ 2 weeks of `lob_tick.db`)
- [ ] `backtesting/tick_backtester.py` — replay `lob_tick.db` in timestamp order
- [ ] FeatureComputer used identically in tick replay and live (Rule 1)
- [ ] Wall ID + Absorption + Sweep detection run on replayed depth20 snapshots (Rule 7)
- [ ] Optuna search over microstructure parameters: `obi_threshold`, `cvd_momentum_min`, `vol_ratio_min`, `spread_max_bps`, `atr_multiplier_sl/tp`, `min_confidence`
- [ ] Single train/test split: days 1–20 train, days 21–28 test

**Week 7 — XGBoost Scorer**
- [ ] Training dataset from tick backtest signals (label = profitable trade)
- [ ] XGBoost trained on all 15 features; saved to `strategies/models/`
- [ ] Validate: Wall/Absorption features dominate importance; `pattern_r2` positive but smaller
- [ ] Fidelity test: FeatureComputer identical output in live, tick backtest, OHLCV backtest
- [ ] Slippage sensitivity table generated; must show `viable=true` at 3bps (Rule 10)
- [ ] Replace `RuleBasedScorer` placeholder with trained XGBoost model in `strategy/executor.py`

**Week 8 — Strategy Builder + Registry**
- [ ] `strategy/spec.py` — StrategySpec dataclass + YAML serialisation
- [ ] `strategy/builder.py` — 3-stage automated builder:
  - Stage 1: individual indicator benchmark
  - Stage 2: combination search (iterative addition)
  - Stage 3: full Optuna refinement + sensitivity test (±20% nudge)
- [ ] `strategy/registry.py` — StrategyRegistry (CRUD + lifecycle promotion gates)
- [ ] `strategy/decay.py` — rolling 30d Sharpe vs backtest benchmark
- [ ] Statistical validity block computed per StrategySpec (Jobson-Korkie CI)
- [ ] Promotion gates enforced: OOS trades ≥ 50, Sharpe CI lower ≥ 0.80, trades/week ≥ 5
- [ ] First StrategySpec registered at `RESEARCH`, reviewed, promoted to `PAPER`

---

### Phase 3 — Dash Dashboard + Paper Trading Review (Weeks 9–12)
*Goal: Full Dash observability, paper performance review, LIVE promotion decision.*

**Weeks 9–10 — Dash Dashboard (live + LOB pages)**
- [ ] `dashboard/app.py` — Dash multi-page app (`use_pages=True`, Cyborg theme)
- [ ] `dashboard/pages/live.py` — `/live`:
  - Candlestick (5m) + VWAP line + signal markers ▲▼ + entry/SL/TP lines
  - Microstructure panel: OBI gauge, CVD δ, spread, Wall/Absorption/Sweep indicators
  - Risk panel: daily loss bar, budget remaining, tier status, consecutive losses
  - Open position panel: legs, avg entry, trail stop, TP, unrealised PnL
  - Signal log + trade blotter (today)
  - Kill switch button + Pause/Resume controls
  - `dcc.Interval` 1s refresh
- [ ] `dashboard/pages/lob.py` — `/lob`:
  - LOB heatmap: colour-coded bid/ask depth, Walls highlighted
  - CVD running chart (60-bar window)
  - OBI gauge (circular, −1 red → 0 grey → +1 green)
  - Spread chart + LOB Recorder health (days recorded, DB size, last event time)
  - `lob_status` indicator: 🟢 SYNCED / 🟡 STALE / 🔴 DISCONNECTED
- [ ] `dashboard/components/` — reusable panels: `risk_panel.py`, `lob_heatmap.py`, `signal_log.py`

**Week 11 — Research + Registry + Config pages**
- [ ] `dashboard/pages/backtest.py` — `/backtest`:
  - Path selector: Tick Microstructure (Path A) / OHLCV Patterns (Path B)
  - Run Backtest button (background callback)
  - Strategy leaderboard (Sharpe colour gradient)
  - Equity curve overlay (top 3 strategies)
  - XGBoost feature importance bar chart
  - Gate funnel chart (signal_records breakdown by gate_passed)
- [ ] `dashboard/pages/registry.py` — `/registry`:
  - Strategy lifecycle table (LIVE=green, PAPER=blue, RESEARCH=yellow, RETIRED=grey)
  - Performance history chart per selected strategy
  - Rolling 30d Sharpe vs backtest benchmark
  - Promote / Retire action buttons with confirmation modal
- [ ] `dashboard/pages/config.py` — `/config`:
  - Risk parameters display (live override supported)
  - LOB Recorder status: days recorded, storage MB, last event time
  - System health: queue depths, WS connection status, heartbeat delta
  - Emergency kill switch (large red button + confirmation dialog)
- [ ] Remove Streamlit `dashboard.py` and `pages/` once Dash covers all functionality

**Week 12 — Monitoring + Paper Review + Documentation**
- [ ] `monitoring/decay.py` — Decay monitor running daily: alert on Sharpe < backtest × 0.6
- [ ] Compare paper Sharpe vs backtest Sharpe (target: within 30% — `paper ≥ backtest × 0.70`)
- [ ] Paper → LIVE promotion decision: ≥ 2 weeks paper, ≥ 20 paper trades, Sharpe gate
- [ ] `README.md` complete with all coherence rules documented
- [ ] `backtest.py` — CLI runner for both backtest paths: `python backtest.py --path A --start 2026-04-01`

---

## Security Notes

- Put Binance **testnet** credentials in `.env` (gitignored). Use `.env.example` as a template.
- `DRY_RUN=True` by default — set `DRY_RUN=False` in `.env` to enable live order execution on testnet.
- Never add API keys to source files.
- Always use the `.venv` virtual environment in the project root.
- Keys should be restricted to Read + Spot Trading (no Withdrawals) in Binance API settings.
