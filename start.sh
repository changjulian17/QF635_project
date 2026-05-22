#!/usr/bin/env bash
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

echo "=== CryptoSentinel startup ==="

# --- Pre-flight: .env ---
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    log_err ".env file not found. Copy .env.example and add your Binance testnet credentials."
    exit 1
fi
log_ok ".env found"

# --- Pre-flight: .venv ---
if [[ ! -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
    log_err ".venv not found. Run: python -m venv .venv && pip install -r requirements.txt"
    exit 1
fi
source "$SCRIPT_DIR/.venv/bin/activate"
log_ok ".venv activated"

# --- Pre-flight: connectivity test ---
echo "Running connectivity test (scripts/test_connection.py)..."
if ! python "$SCRIPT_DIR/scripts/test_connection.py"; then
    log_err "Connectivity test failed. Check your .env credentials and network."
    exit 1
fi
log_ok "Connectivity OK"

# --- Launch each component in a new Terminal window ---
open_window() {
    local label="$1"
    local cmd="$2"
    osascript -e "
        tell application \"Terminal\"
            activate
            set w to do script \"echo '=== $label ==='; cd '$SCRIPT_DIR' && source .venv/bin/activate && $cmd\"
            set custom title of front window to \"$label\"
        end tell
    "
}

echo "Launching components..."
open_window "LOB Recorder"    "python -m core.lob_recorder"
open_window "Trading Engine"  "python main.py"
open_window "Dash Dashboard"  "python dashboard/app.py"

log_ok "All three components launched in separate Terminal windows."
echo ""
echo "  LOB Recorder   → python -m core.lob_recorder"
echo "  Trading Engine → python main.py"
echo "  Dash Dashboard → http://127.0.0.1:8050"
