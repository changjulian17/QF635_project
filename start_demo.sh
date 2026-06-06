#!/usr/bin/env bash
# Demo mode startup: connects to demo.binance.com (full visual trading UI).
# Trades appear on demo.binance.com — visit the site to confirm fills.
# Requires DEMO_BINANCE_API_KEY and DEMO_BINANCE_API_SECRET in .env.
# NOTE: macOS only — uses osascript / Terminal.app.
# On Linux, start each component manually using the commands printed at the end.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_ok()   { echo -e "${GREEN}[OK]${NC}  $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERR]${NC} $*"; }

echo "=== CryptoSentinel DEMO startup ==="

# --- Pre-flight: .env ---
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    log_err ".env file not found. Copy .env.example and add your credentials."
    exit 1
fi
log_ok ".env found"

# Source .env to validate DEMO_BINANCE_API_KEY is present
set -a; source "$SCRIPT_DIR/.env"; set +a
if [[ -z "${DEMO_BINANCE_API_KEY:-}" ]]; then
    log_err "DEMO_BINANCE_API_KEY not set in .env"
    log_err "Get demo API keys at demo.binance.com → Account → API Management"
    exit 1
fi
log_warn "DEMO MODE — paper money orders will be placed on demo.binance.com"

# --- Pre-flight: .venv ---
if [[ ! -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
    log_err ".venv not found. Run: python -m venv .venv && pip install -r requirements.txt"
    exit 1
fi
source "$SCRIPT_DIR/.venv/bin/activate"
PYTHON="$SCRIPT_DIR/.venv/bin/python"
log_ok ".venv activated"

# --- Pre-flight: already running? ---
if [[ -f /tmp/cs_engine.pid ]]; then
    _pid=$(cat /tmp/cs_engine.pid)
    if kill -0 "$_pid" 2>/dev/null; then
        log_err "Trading Engine is already running (PID $_pid). Run ./stop.sh first."
        exit 1
    fi
fi

# --- Pre-flight: connectivity test ---
echo "Running connectivity test (scripts/test_connection.py)..."
if ! BINANCE_DEMO=true BINANCE_TESTNET=false "$PYTHON" "$SCRIPT_DIR/scripts/test_connection.py"; then
    log_err "Connectivity test failed. Check DEMO_BINANCE_API_KEY / DEMO_BINANCE_API_SECRET in .env."
    exit 1
fi
log_ok "Connectivity OK"

# --- Export demo env overrides ---
# These override .env defaults; pydantic-settings resolves env vars before the .env file.
export BINANCE_TESTNET=false
export BINANCE_DEMO=true
export DRY_RUN=false
export WS_BASE="wss://demo-stream.binance.com"
export REST_BASE="https://demo-api.binance.com"

# --- Launch each component in a new Terminal window ---
open_window() {
    local label="$1"
    local cmd="$2"
    osascript -e "
        tell application \"Terminal\"
            activate
            do script \"echo '=== $label ==='; cd '$SCRIPT_DIR' && source .venv/bin/activate && $cmd\"
            set custom title of front window to \"$label\"
        end tell
    "
}

echo "Launching DEMO components (BINANCE_DEMO=true, DRY_RUN=false, MIN_CONFIDENCE=0.1, TEST_SIGNAL_INJECT=true)..."
open_window "LOB Recorder"             "'$PYTHON' -m core.lob_recorder"
open_window "Trading Engine (DEMO)"   "BINANCE_TESTNET=false BINANCE_DEMO=true DRY_RUN=false WS_BASE=wss://demo-stream.binance.com REST_BASE=https://demo-api.binance.com MIN_CONFIDENCE=0.1 TEST_SIGNAL_INJECT=true TIMEFRAME=1s '$PYTHON' main.py & echo \$! > /tmp/cs_engine.pid && wait"
open_window "Dash Dashboard"           "'$PYTHON' dashboard/app.py"

log_ok "All three components launched in separate Terminal windows."
echo ""
echo "  LOB Recorder          → python -m core.lob_recorder"
echo "  Trading Engine (DEMO) → BINANCE_DEMO=true DRY_RUN=false python main.py"
echo "  Dash Dashboard        → http://127.0.0.1:8050"
echo ""
echo "  View trades at        → https://demo.binance.com/en/trade/BTC_USDT?type=spot"
