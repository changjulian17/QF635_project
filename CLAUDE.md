# CLAUDE.md — CryptoSentinel project guidance

This repository contains the CryptoSentinel university project: a real-time pattern-recognition and paper-trading system for BTCUSDT on the Binance Spot Testnet.

Quick actions for Claude Code or any assistant working on this repo:

- Activate the project virtual environment:

	source .venv/bin/activate

- Install dependencies:

	pip install -r requirements.txt

- Run a connectivity check (after adding testnet keys to `.env`):

	python test_binance_connection.py

- Start the trading engine (async orchestrator):

	python main.py

- Start the Streamlit dashboard (in a separate terminal):

	streamlit run dashboard.py

Configuration notes:

- Put Binance testnet credentials in `.env` (this file is gitignored). Use `.env.example` as a template.
- The system uses `python-binance` AsyncClient for REST execution and a custom asyncio `websockets` consumer for streaming aggTrade and kline data.

Files of interest for assistants:

- `config.py` — pydantic settings loaded from `.env`
- `websocket_consumer.py` — resilient WS ingestion
- `pattern_detector.py` — wedge / triangle / S/R detection logic
- `risk_engine.py` — circuit breakers + position sizing
- `order_manager.py` — Async execution (testnet)
- `main.py` — asyncio orchestrator that wires components
- `dashboard.py` — Streamlit UI for live monitoring

When updating or running code:

- Never add real API keys to source files — use `.env` only.
- Use the `.venv` virtual environment that lives in the project root.
- Run unit tests with `pytest` and integration checks against the testnet only.

If you need the full project spec, README, or architecture diagram, open `README.md`.
