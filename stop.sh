#!/usr/bin/env bash
set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_ok()   { echo -e "${GREEN}[OK]${NC}  $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERR]${NC} $*"; }

GRACEFUL_TIMEOUT=5

# Gracefully stop a process matched by pattern.
# Sends SIGTERM, waits up to GRACEFUL_TIMEOUT seconds, then SIGKILL.
stop_process() {
    local label="$1"
    local pattern="$2"

    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [[ -z "$pids" ]]; then
        log_warn "$label — not running"
        return
    fi

    echo "  Stopping $label (PIDs: $pids) ..."
    kill -TERM $pids 2>/dev/null || true

    local elapsed=0
    while kill -0 $pids 2>/dev/null; do
        if (( elapsed >= GRACEFUL_TIMEOUT )); then
            log_warn "$label — still alive after ${GRACEFUL_TIMEOUT}s, sending SIGKILL"
            kill -KILL $pids 2>/dev/null || true
            break
        fi
        sleep 1
        (( elapsed++ )) || true
    done

    log_ok "$label stopped"
}

# Close a Terminal window by its custom title.
close_terminal_window() {
    local title="$1"
    osascript -e "
        tell application \"Terminal\"
            set windowList to every window whose custom title is \"$title\"
            repeat with w in windowList
                close w
            end repeat
        end tell
    " 2>/dev/null || true
}

echo "=== CryptoSentinel shutdown ==="

stop_process "LOB Recorder"   "core.lob_recorder"
ENGINE_PID_FILE="/tmp/cs_engine.pid"
if [[ -f "$ENGINE_PID_FILE" ]]; then
    engine_pid=$(cat "$ENGINE_PID_FILE")
    if kill -0 "$engine_pid" 2>/dev/null; then
        echo "  Stopping Trading Engine (PID: $engine_pid) ..."
        kill -TERM "$engine_pid" 2>/dev/null || true
        elapsed=0
        while kill -0 "$engine_pid" 2>/dev/null; do
            if (( elapsed >= GRACEFUL_TIMEOUT )); then
                log_warn "Trading Engine — still alive after ${GRACEFUL_TIMEOUT}s, sending SIGKILL"
                kill -KILL "$engine_pid" 2>/dev/null || true
                break
            fi
            sleep 1
            (( elapsed++ )) || true
        done
        log_ok "Trading Engine stopped"
    else
        log_warn "Trading Engine — not running (stale PID file)"
    fi
    rm -f "$ENGINE_PID_FILE"
else
    stop_process "Trading Engine" "python.*main\.py"
fi
stop_process "Dash Dashboard" "dashboard/app\.py"

echo "Closing Terminal windows..."
close_terminal_window "LOB Recorder"
close_terminal_window "Trading Engine"
close_terminal_window "Trading Engine (TESTNET)"
close_terminal_window "Dash Dashboard"

log_ok "Shutdown complete."
