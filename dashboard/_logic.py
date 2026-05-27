"""Pure logic functions shared by dashboard pages — importable without Dash app context."""
import requests


def validate_ks_confirm(value: str) -> bool:
    """Return True (button disabled) unless value is exactly 'CONFIRM'."""
    return value != "CONFIRM"


def fire_killswitch(api_base: str) -> bool:
    """POST to /api/killswitch. Returns True if request succeeded."""
    try:
        resp = requests.post(f"{api_base}/api/killswitch", timeout=3)
        return resp.ok
    except Exception:
        return False


def decay_badge_color(rolling_sharpe: float, backtest_sharpe: float) -> str:
    """Return Bootstrap color string for a decay monitoring badge."""
    if backtest_sharpe <= 0:
        return "secondary"
    ratio = rolling_sharpe / backtest_sharpe
    if ratio >= 0.85:
        return "success"
    if ratio >= 0.70:
        return "warning"
    return "danger"


def decay_badge_label(rolling_sharpe: float, backtest_sharpe: float) -> str:
    """Return human-readable label for a decay monitoring badge."""
    if backtest_sharpe <= 0:
        return "N/A"
    ratio = rolling_sharpe / backtest_sharpe
    if ratio >= 0.85:
        return f"{ratio:.0%} — Healthy"
    if ratio >= 0.70:
        return f"{ratio:.0%} — Warning"
    return f"{ratio:.0%} — Decay Alert"


def update_lob_buffer(buffer: list[dict], msg: dict, max_points: int) -> list[dict]:
    """Append a streamed LOB snapshot to the rolling buffer, trimmed to max_points.

    Pure function (no Dash/browser deps) so the streaming logic is unit-testable.

    buffer: prior snapshots, oldest first. msg: the newly received WS payload.
    Returns a NEW list (does not mutate the input). A message whose ``ts`` matches
    the last buffered snapshot is ignored, so a duplicate push can't add a column.
    Running CVD is the cumulative sum of ``cvd_delta`` and is computed at render time.
    """
    if not isinstance(msg, dict) or "ts" not in msg:
        return buffer
    if buffer and buffer[-1].get("ts") == msg.get("ts"):
        return buffer
    new_buffer = buffer + [msg]
    if max_points > 0 and len(new_buffer) > max_points:
        new_buffer = new_buffer[-max_points:]
    return new_buffer
