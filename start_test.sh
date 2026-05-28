#!/usr/bin/env bash
# Testnet execution test: real orders on Binance testnet + synthetic signal injection.
# NOT for paper trading, backtesting, or production.
set -eo pipefail
source .venv/bin/activate
exec env \
  DRY_RUN=false \
  MIN_CONFIDENCE=0.1 \
  TEST_SIGNAL_INJECT=true \
  python main.py
