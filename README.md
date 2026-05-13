# CryptoSentinel

Real-time microstructure analysis and pattern-recognition paper-trading system on Binance Spot Testnet (BTCUSDT).

## Architecture

```
Binance WS (combined stream)
  ├── aggTrade        → trade_queue  → MicrostructureEngine
  ├── kline_1s        → candle_queue → PatternDetector → RiskEngine → OrderManager (DRY_RUN)
  │                   → candle_db_queue → DBWriter → SQLite
  └── depth@100ms     → depth_queue  → LocalOrderBook → MicrostructureEngine → SQLite
                                                       → signal_db_queue → DBWriter → SQLite
```

## Components

| File | Role |
|---|---|
| `config.py` | Pydantic V2 settings loaded from `.env` |
| `models.py` | Dataclasses: Candle, AggTrade, LOBLevel, LOBSnapshot, MicrostructureBar, PatternSignal, PortfolioState |
| `websocket_consumer.py` | Resilient asyncio WS consumer — aggTrade, kline_1s, depth@100ms streams with 24 h proactive reconnect |
| `lob_engine.py` | Local order book — applies Binance diff-depth events with sequence validation |
| `microstructure_engine.py` | 10 Hz microstructure bar computation: OBI, CVD, reload, iceberg, sweep, book-flip, liquidity-flip, break+protect |
| `pattern_detector.py` | Wedge / triangle / S&R breakout detection (scipy linregress) |
| `risk_engine.py` | Circuit breakers (drawdown, daily loss, consecutive losses) + Kelly position sizing |
| `order_manager.py` | Async market order + OCO placement — skips execution when `DRY_RUN=True` |
| `db_writer.py` | SQLite persistence — candles, signals, portfolio with 7-day rolling cleanup |
| `main.py` | asyncio TaskGroup orchestrator |
| `dashboard.py` | Streamlit UI — liquidity heatmap, OBI/CVD, price chart, microstructure signals |
| `test_binance_connection.py` | Connectivity + auth check (reads keys from `.env`) |
| `test_orders.py` | End-to-end market BUY + SELL round-trip test |

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and add testnet keys from `https://testnet.binance.vision/`

```bash
# Verify connectivity
python test_binance_connection.py

# Test order round-trip (small BUY + SELL)
python test_orders.py

# Run engine (terminal 1)
python main.py

# Run dashboard (terminal 2)
streamlit run dashboard.py
```

Dashboard: `http://localhost:8501`

## Configuration (`.env`)

```
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

All other parameters can be overridden via `.env`:

| Setting | Default | Description |
|---|---|---|
| `DRY_RUN` | `True` | Skip live order submission |
| `CANDLE_INTERVAL` | `1s` | Kline stream interval |
| `PATTERN_LOOKBACK` | `50` | Candles used by pattern detector |
| `MAX_DRAWDOWN_PCT` | `0.05` | Circuit breaker threshold |
| `DAILY_LOSS_LIMIT_PCT` | `0.02` | Daily loss halt |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Triggers 5-min cooldown |
| `RISK_PER_TRADE_PCT` | `0.01` | Equity risked per trade |
| `LOB_DEPTH` | `20` | Book levels used for OBI / heatmap |
| `OBI_BREAK_THRESH` | `0.40` | OBI magnitude for break+protect confirmation |

## Dashboard panels

- **Portfolio** — paper equity, daily PnL, drawdown, circuit breaker status
- **Microstructure bar** — live mid price, spread, OBI, CVD, active signals
- **Liquidity heatmap** — bid/ask depth over time with volume bubbles and signal overlays
- **OBI / CVD / Spread** — rolling time-series charts
- **Price + Pattern Signals** — candlestick chart with detected pattern markers

## Tests

```bash
python -m pytest tests/ -v
```

27 unit tests covering models, risk engine, and pattern detector.
