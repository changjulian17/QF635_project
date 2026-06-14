#!/usr/bin/env bash
# Demo mode startup: connects to demo.binance.com (full visual trading UI).
# Trades appear on demo.binance.com — visit the site to confirm fills.
# Requires DEMO_BINANCE_API_KEY and DEMO_BINANCE_API_SECRET in .env.
#
# Usage:
#   ./start_demo.sh             — live demo orders on demo.binance.com
#   ./start_demo.sh --dry-run   — synthetic fills only (no API key needed)
#   DRY_RUN=true ./start_demo.sh
#
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

# --- Parse --dry-run flag ---
DRY_RUN_FLAG=false
for arg in "$@"; do
    if [[ "$arg" == "--dry-run" ]]; then
        DRY_RUN_FLAG=true
    fi
done

echo "=== CryptoSentinel DEMO startup ==="

# --- Pre-flight: .env ---
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    log_err ".env file not found. Copy .env.example and add your credentials."
    exit 1
fi
log_ok ".env found"

# Source .env to pick up any overrides (e.g. DRY_RUN=true)
set -a; source "$SCRIPT_DIR/.env"; set +a

# Resolve effective DRY_RUN: flag wins, then .env value, then default false
if [[ "$DRY_RUN_FLAG" == "true" ]]; then
    DRY_RUN=true
fi
DRY_RUN="${DRY_RUN:-false}"

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

if [[ "$DRY_RUN" == "true" ]]; then
    log_warn "DRY RUN MODE — synthetic fills only, no real orders placed"
    log_warn "Skipping API key check and connectivity test"
else
    # --- Pre-flight: demo API key ---
    if [[ -z "${DEMO_BINANCE_API_KEY:-}" ]]; then
        log_err "DEMO_BINANCE_API_KEY not set in .env"
        log_err "Get demo API keys at demo.binance.com → Account → API Management"
        log_err "Or run with --dry-run for synthetic fills without an API key"
        exit 1
    fi
    log_warn "DEMO MODE — paper money orders will be placed on demo.binance.com"

    # --- Pre-flight: connectivity test ---
    echo "Running connectivity test (scripts/test_futures_demo.py)..."
    if ! "$PYTHON" "$SCRIPT_DIR/scripts/test_futures_demo.py"; then
        log_err "Connectivity test failed. Check DEMO_BINANCE_API_KEY / DEMO_BINANCE_API_SECRET in .env."
        exit 1
    fi
    log_ok "Connectivity OK"
fi

# --- Build env prefix for launched processes ---
# TRADING_MODE=demo gives: MIN_CONFIDENCE=0.1, TEST_SIGNAL_INJECT=true, BINANCE_DEMO=true
# DRY_RUN=true overrides the preset's DRY_RUN=false when explicitly set
ENV_PREFIX="TRADING_MODE=demo"
if [[ "$DRY_RUN" == "true" ]]; then
    ENV_PREFIX="$ENV_PREFIX DRY_RUN=true"
fi

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

if [[ "$DRY_RUN" == "true" ]]; then
    echo "Launching DEMO (DRY RUN) components (MIN_CONFIDENCE=0.1, TEST_SIGNAL_INJECT=true, synthetic fills)..."
else
    echo "Launching DEMO components (BINANCE_DEMO=true, DRY_RUN=false, MIN_CONFIDENCE=0.1, TEST_SIGNAL_INJECT=true)..."
fi

open_window "LOB Recorder"           "$ENV_PREFIX '$PYTHON' -m core.lob_recorder"
open_window "Trading Engine (DEMO)"  "$ENV_PREFIX '$PYTHON' main.py & echo \$! > /tmp/cs_engine.pid && wait"
open_window "Dash Dashboard"         "$ENV_PREFIX '$PYTHON' dashboard/app.py"

log_ok "All three components launched in separate Terminal windows."
echo ""
echo "  LOB Recorder          → $ENV_PREFIX python -m core.lob_recorder"
echo "  Trading Engine (DEMO) → $ENV_PREFIX python main.py"
echo "  Dash Dashboard        → http://127.0.0.1:8050"
if [[ "$DRY_RUN" == "true" ]]; then
    echo ""
    echo "  DRY RUN: fills are synthetic — check dashboard at http://127.0.0.1:8050/live"
else
    echo ""
    echo "  View trades at        → https://demo.binance.com/en/futures/BTCUSDT"
fi
