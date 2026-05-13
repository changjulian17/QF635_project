# CLAUDE.md — CryptoSentinel project guidance

Real-time microstructure analysis and paper-trading system for BTCUSDT on the Binance Spot Testnet.

## Project layout

```
main.py          — asyncio orchestrator (entry point)
dashboard.py     — Streamlit UI (entry point)
config.py        — pydantic settings loaded from .env
models.py        — shared dataclasses and enums

engine/          — all real-time processing components
  websocket_consumer.py  — aggTrade + kline_1s + depth@100ms WS streams
  lob_engine.py          — local order book (diff-depth sync)
  microstructure_engine.py — OBI, CVD, reload, iceberg, sweep, book-flip, break+protect
  pattern_detector.py    — wedge / triangle / S/R breakout detection
  risk_engine.py         — circuit breakers + Kelly position sizing
  order_manager.py       — async order execution (DRY_RUN by default)
  db_writer.py           — SQLite persistence + 7-day rolling cleanup

scripts/
  test_connection.py  — verify Binance testnet connectivity and auth
  test_orders.py      — BUY + SELL round-trip execution test

tests/           — pytest unit tests (27 tests)
```

## Quick actions

Activate the virtual environment:

	source .venv/bin/activate

Install dependencies:

	pip install -r requirements.txt

Verify connectivity (after adding testnet keys to `.env`):

	python scripts/test_connection.py

Run order round-trip test:

	python scripts/test_orders.py

Start the trading engine:

	python main.py

Start the Streamlit dashboard (separate terminal):

	streamlit run dashboard.py

Run unit tests:

	python -m pytest tests/ -v

## Configuration notes

- Put Binance testnet credentials in `.env` (gitignored). Use `.env.example` as a template.
- `DRY_RUN=True` in config.py — set `DRY_RUN=False` in `.env` to enable live order execution.
- Never add API keys to source files.
- Always use the `.venv` virtual environment in the project root.
