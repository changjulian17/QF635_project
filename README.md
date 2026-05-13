# CryptoSentinel

Real-time market microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

Streams live order book depth, trade flow, and 1-second klines from Binance WebSocket feeds, computes a suite of microstructure indicators at 10 Hz, detects chart patterns on closed candles, and executes paper trades through a risk-gated order manager.

---

## Project structure

```
CryptoSentinel/
├── main.py              # asyncio TaskGroup orchestrator (entry point)
├── dashboard.py         # Streamlit UI (entry point)
├── config.py            # Pydantic V2 settings — all params loaded from .env
├── models.py            # Shared dataclasses and enums
│
├── engine/              # All real-time processing components
│   ├── websocket_consumer.py   # Combined WS stream consumer
│   ├── lob_engine.py           # Local order book (diff-depth sync)
│   ├── microstructure_engine.py# 10 Hz bar computation + signal detectors
│   ├── pattern_detector.py     # Chart pattern detection
│   ├── risk_engine.py          # Circuit breakers + position sizing
│   ├── order_manager.py        # Order execution (DRY_RUN by default)
│   └── db_writer.py            # Async SQLite persistence
│
├── scripts/
│   ├── test_connection.py      # Connectivity + auth check
│   └── test_orders.py          # BUY + SELL round-trip test
│
├── tests/                      # pytest unit tests (65 tests)
│   ├── test_models.py
│   ├── test_lob_engine.py
│   ├── test_microstructure_engine.py
│   ├── test_db_writer.py
│   ├── test_pattern_detector.py
│   └── test_risk_engine.py
│
├── requirements.txt
├── .env                 # gitignored — add your testnet keys here
└── .env.example
```

---

## Data pipeline

```
Binance Spot Testnet WebSocket (combined stream)
  │
  ├── btcusdt@aggTrade   ──► trade_queue  ──► MicrostructureEngine
  │                                                │
  ├── btcusdt@kline_1s   ──► candle_queue ──► PatternDetector ──► RiskEngine ──► OrderManager
  │                      └─► candle_db_queue ──► DBWriter
  │                                                │
  └── btcusdt@depth@100ms ─► depth_queue  ──► LocalOrderBook ──► MicrostructureEngine
                                                                        │
                                                              signal_db_queue ──► DBWriter
                                                              metrics_store   ──► Dashboard
                                                              SQLite          ──► Dashboard
```

---

## Features

### WebSocket consumer (`engine/websocket_consumer.py`)
- Subscribes to three Binance streams simultaneously via the combined stream endpoint (`/stream?streams=`)
- **aggTrade** — individual trade events with side (buyer/seller aggressor)
- **kline_1s** — 1-second OHLCV candles; emits only on candle close (`x=true`)
- **depth@100ms** — order book diff events at 100 ms cadence
- Exponential backoff reconnect (1 s → 60 s) on any disconnect
- Proactive 24-hour reconnect (Binance enforces a 24 h connection lifetime)

### Local order book (`engine/lob_engine.py`)
Follows the exact Binance synchronisation procedure:
1. Buffer incoming depth events while fetching a REST snapshot
2. Apply snapshot; discard buffered events with `u ≤ lastUpdateId`
3. Validate strict monotonic sequence on every subsequent event
4. On any sequence gap: mark book as stale, trigger reinitialisation

### Microstructure engine (`engine/microstructure_engine.py`)
Produces one `MicrostructureBar` per depth event (~10 Hz). Each bar contains:

| Indicator | Description |
|---|---|
| **Mid price** | `(best_bid + best_ask) / 2` |
| **Spread** | `best_ask − best_bid` |
| **OBI** | Order Book Imbalance `(bid_vol − ask_vol) / (bid_vol + ask_vol)` ∈ [−1, 1] |
| **Delta** | Net aggression this bar: buy volume − sell volume |
| **CVD** | Cumulative Volume Delta — running sum of delta across bars |
| **Reload bid/ask** | Statistical spike (> `RELOAD_SIGMA` σ) at a level that was recently consumed |
| **Iceberg bid/ask** | Level consumed by a trade yet replenishes ≥ 50% within `ICEBERG_WINDOW_MS` |
| **Sweep up/down** | Aggressive volume exceeds 80% of the top-N book levels |
| **Book flip bid/ask** | Large limit order disappears without proportional trade consumption (spoofing), immediately followed by opposite aggression |
| **Liq flip to res/sup** | A historically dominant bid level is now a dominant ask level (support → resistance) and vice versa |
| **Break + protect long/short** | Large directional aggression moves price; OBI then flips decisively in the same direction within `BREAK_PROTECT_WINDOW_MS` |

All bars and the raw bid/ask levels are persisted to SQLite for dashboard replay.

### Pattern detector (`engine/pattern_detector.py`)
Runs on every closed 1-second candle. Requires 20+ candles before activating.

| Pattern | Trigger |
|---|---|
| **Rising wedge** | Both trendlines slope up, lower line steeper, close breaks below lower line + volume spike |
| **Falling wedge** | Both trendlines slope down, lower line steeper, close breaks above lower line + volume spike |
| **Symmetrical triangle** | Converging trendlines (R² ≥ `MIN_R2`), close at apex + volume spike |
| **Resistance breakout** | Close > 95th-percentile of last 30 candles + ATR buffer + volume spike |
| **Support breakout** | Close < 5th-percentile of last 30 candles + ATR buffer + volume spike |

Trendlines are fitted with `scipy.stats.linregress`. Confidence is a function of R² and volume ratio. SL/TP levels are set at `ATR_MULTIPLIER_SL` × ATR and `ATR_MULTIPLIER_TP` × ATR from entry.

### Risk engine (`engine/risk_engine.py`)
Three independent circuit breakers halt or pause trading:

| Breaker | Condition | Action |
|---|---|---|
| Max drawdown | `(peak − equity) / peak ≥ MAX_DRAWDOWN_PCT` | HALT (permanent) |
| Daily loss | `abs(daily_pnl) / starting_equity ≥ DAILY_LOSS_LIMIT_PCT` | HALT (permanent) |
| Consecutive losses | `consecutive_losses ≥ MAX_CONSECUTIVE_LOSSES` | PAUSE 5 min cooldown |

Position sizing uses a Kelly-fractioned, ATR-normalised formula:
```
risk_amount = equity × RISK_PER_TRADE_PCT
raw_qty     = risk_amount / |entry − stop_loss|
quantity    = raw_qty × (KELLY_FRACTION × confidence)
```

### Order manager (`engine/order_manager.py`)
- Connects via `python-binance` `AsyncClient` to testnet
- On `DRY_RUN=True` (default): logs the would-be order, skips execution
- On `DRY_RUN=False`: places a market order then an OCO (stop-limit + take-profit)

### DB writer (`engine/db_writer.py`)
- Writes closed candles, pattern signals, and portfolio state to `cryptosentinel.db` (SQLite)
- Portfolio state is flushed every 5 seconds
- Daily cleanup task purges records older than 7 days

### Dashboard (`dashboard.py`)
Built with Streamlit 1.57+, uses `@st.fragment(run_every=30)` for flicker-free 30-second auto-refresh with a manual **Refresh now** button.

| Panel | Data source |
|---|---|
| Portfolio metrics | SQLite `portfolio` table |
| Microstructure bar | SQLite `microstructure_bars` (latest row) |
| Liquidity heatmap | Bid/ask depth grid with volume bubbles and signal overlays |
| OBI / CVD / Spread | Rolling time-series charts |
| Price + pattern signals | Candlestick chart with LONG ▲ / SHORT ▼ markers |

---

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add testnet API keys to `.env` (get them from `https://testnet.binance.vision/`):
```
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

```bash
# Verify connectivity and auth
python scripts/test_connection.py

# Test order round-trip (market BUY + SELL, ~$81 notional)
python scripts/test_orders.py

# Run engine (terminal 1)
python main.py

# Run dashboard (terminal 2)
streamlit run dashboard.py        # → http://localhost:8501

# Run tests
python -m pytest tests/ -v
```

---

## Configuration reference

All settings live in `config.py` and can be overridden via `.env`.

| Setting | Default | Description |
|---|---|---|
| `BINANCE_API_KEY` | — | Testnet API key |
| `BINANCE_API_SECRET` | — | Testnet API secret |
| `BINANCE_TESTNET` | `True` | Always use testnet |
| `SYMBOL` | `BTCUSDT` | Trading pair |
| `CANDLE_INTERVAL` | `1s` | Kline stream interval |
| `PATTERN_LOOKBACK` | `50` | Candle history for pattern detection |
| `SWING_WINDOW` | `5` | Bars each side for swing high/low |
| `BREAKOUT_VOL_MULT` | `1.5` | Volume × avg required for breakout confirmation |
| `MIN_R2` | `0.80` | Minimum R² for trendline fit |
| `DRY_RUN` | `True` | Skip live order submission |
| `MAX_DRAWDOWN_PCT` | `0.05` | Drawdown circuit breaker (5%) |
| `DAILY_LOSS_LIMIT_PCT` | `0.02` | Daily loss halt (2%) |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 5-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.01` | Equity risked per trade (1%) |
| `KELLY_FRACTION` | `0.25` | Kelly fraction applied to raw size |
| `ATR_MULTIPLIER_SL` | `1.5` | Stop-loss distance in ATR units |
| `ATR_MULTIPLIER_TP` | `3.0` | Take-profit distance in ATR units |
| `LOB_DEPTH` | `20` | Order book levels for OBI + heatmap |
| `RELOAD_SIGMA` | `3.0` | σ threshold for reload detection |
| `ICEBERG_WINDOW_MS` | `500` | Look-back window for iceberg detection |
| `SWEEP_LEVELS` | `5` | Book levels used for sweep comparison |
| `BREAK_PROTECT_WINDOW_MS` | `2000` | Window to confirm break+protect signal |
| `OBI_BREAK_THRESH` | `0.40` | OBI magnitude required for break+protect |

---

## Tests

```
tests/test_models.py                 6 tests  — PortfolioState properties
tests/test_lob_engine.py            12 tests  — LOB snapshot, diff, sequence validation
tests/test_microstructure_engine.py 16 tests  — OBI, CVD, sweep, book-flip, break+protect
tests/test_db_writer.py              9 tests  — SQLite write, upsert, purge
tests/test_pattern_detector.py       8 tests  — ATR, swing detection, S/R breakout
tests/test_risk_engine.py           14 tests  — circuit breakers, sizing, trade recording
─────────────────────────────────────────────────────
Total                               65 tests  — all passing
```
