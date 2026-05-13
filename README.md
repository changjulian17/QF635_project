# CryptoSentinel

Real-time wedge & breakout pattern trading system on Binance Spot Testnet (BTCUSDT).

## Architecture

```
Binance WS ──► BinanceWebSocketConsumer
                  │ aggTrade  → raw_queue  → drained (future tick features)
                  │ kline     → candle_queue → PatternDetector
                                                │ signals → RiskEngine
                                                           │ orders  → OrderManager
                                                                        │ OCO placed on testnet
                  └─ candle_db_queue ─┐
                                      ├──► DBWriter ──► cryptosentinel.db
                              signal_db_queue ─┘
```

## Quick start

1. Create virtual env and activate:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and add testnet keys from https://testnet.binance.vision/

4. Verify connectivity and keys:

```bash
python test_binance_connection.py
```

5. Test order execution (places a buy + sell round-trip):

```bash
python test_orders.py
```

6. Run engine (terminal 1):

```bash
python main.py
```

7. Run dashboard (terminal 2):

```bash
streamlit run dashboard.py
```

## Files

| File | Role |
|---|---|
| `config.py` | Pydantic V2 settings loaded from `.env` |
| `models.py` | Dataclasses and enums (Candle, PatternSignal, PortfolioState …) |
| `websocket_consumer.py` | Resilient asyncio WS consumer for aggTrade + kline streams |
| `pattern_detector.py` | Wedge / triangle / S&R breakout detection (scipy linregress) |
| `risk_engine.py` | Circuit breakers (drawdown, daily loss, consecutive losses) + Kelly position sizing |
| `order_manager.py` | Async market order + OCO placement via `python-binance` AsyncClient |
| `db_writer.py` | SQLite persistence — candles, signals, portfolio written async |
| `main.py` | asyncio TaskGroup orchestrator wiring all components |
| `dashboard.py` | Streamlit UI — live account balance, candlestick chart, signals, portfolio |
| `test_binance_connection.py` | Connectivity + auth check |
| `test_orders.py` | End-to-end order round-trip test |

## Tests

```bash
python -m pytest tests/ -v
```

27 unit tests covering models, risk engine, and pattern detector.

## Configuration (`.env`)

```
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

All other parameters (`RISK_PER_TRADE_PCT`, `MAX_DRAWDOWN_PCT`, etc.) are set in `config.py` and can be overridden via `.env`.

## Dashboard panels

- **Live account** — BTC / USDT / BNB balances fetched directly from testnet
- **Open orders** — active orders on the exchange
- **Portfolio** — paper-trading equity, daily PnL, drawdown, circuit breaker status
- **BTCUSDT chart** — candlestick chart with signal markers (LONG ▲ / SHORT ▼)
- **Recent signals** — table of last 20 detected patterns
