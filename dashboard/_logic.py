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
