"""Pure logic functions shared by dashboard pages — importable without Dash app context."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests


def validate_ks_confirm(value: str) -> bool:
    """Return True (button disabled) unless value is exactly 'CONFIRM'."""
    return value != "CONFIRM"


def fire_killswitch(api_base: str) -> bool:
    """POST to /api/killswitch. Returns True if request succeeded."""
    try:
        resp = requests.post(f"{api_base}/api/killswitch", timeout=3)
        return resp.ok
    except requests.RequestException:
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


def update_signal_tape(buffer: list[dict], msg: dict, max_events: int) -> list[dict]:
    """Append a streamed signal_event to the rolling tape buffer, trimmed to max_events.

    Pure function (no Dash/browser deps) so the streaming logic is unit-testable.

    Only messages with ``type == "signal_event"`` are accepted; snapshots and
    malformed payloads pass through unchanged. The buffer is newest-first so the
    UI can slice the head without reversing.
    """
    if not isinstance(msg, dict) or msg.get("type") != "signal_event":
        return buffer
    if "ts" not in msg or "gate_passed" not in msg:
        return buffer
    new_buffer = [msg] + buffer  # newest first
    if max_events > 0 and len(new_buffer) > max_events:
        new_buffer = new_buffer[:max_events]
    return new_buffer


def update_portfolio_state(state: dict, msg: dict) -> dict:
    """Replace the cached portfolio state with the latest WS payload.

    Pure function (no Dash/browser deps) so the streaming logic is unit-testable.

    Portfolio is a *snapshot of current state*, not a stream of events, so each
    valid push replaces the previous state entirely. Malformed messages or those
    tagged with a non-portfolio type return the existing state unchanged.
    """
    if not isinstance(msg, dict):
        return state
    if msg.get("type") != "portfolio":
        return state
    if "equity" not in msg:  # minimal shape check — the one field we always render
        return state
    return msg


def update_lob_buffer(buffer: list[dict], msg: dict, max_points: int) -> list[dict]:
    """Append a streamed LOB snapshot to the rolling buffer, trimmed to max_points.

    Pure function (no Dash/browser deps) so the streaming logic is unit-testable.

    buffer: prior snapshots, oldest first. msg: the newly received WS payload.
    Returns a NEW list (does not mutate the input). A message whose ``ts`` matches
    the last buffered snapshot is ignored, so a duplicate push can't add a column.
    Messages with ``type != "snapshot"`` (events, etc.) are ignored so the buffer
    stays homogeneous. Running CVD is the cumulative sum of ``cvd_delta`` and is
    computed at render time.
    """
    if not isinstance(msg, dict) or "ts" not in msg:
        return buffer
    if msg.get("type", "snapshot") != "snapshot":
        return buffer
    if buffer and buffer[-1].get("ts") == msg.get("ts"):
        return buffer
    new_buffer = buffer + [msg]
    if max_points > 0 and len(new_buffer) > max_points:
        new_buffer = new_buffer[-max_points:]
    return new_buffer


def add_event_markers(
    fig: go.Figure,
    events: list[dict],
    ts_labels: list[str],
    hm_ts_snap,
) -> tuple[int, int]:
    """Render absorption/sweep markers on the heatmap (row 1), snapped to snapshot ts.

    Markers outside the heatmap window are dropped. Two traces are always added
    (even when empty) so the legend layout stays stable across redraws.

    Symbols: triangle-up = bid-side absorption, triangle-down = ask-side absorption,
    star = sweep (both sides). Color: green = bid wall, red = ask wall.

    Returns ``(n_absorption, n_sweep)`` so callers can verify drop behavior.
    """
    abs_x, abs_y, abs_sym, abs_clr, abs_txt = [], [], [], [], []
    swp_x, swp_y, swp_sym, swp_clr, swp_txt = [], [], [], [], []

    if events and ts_labels and hm_ts_snap is not None and len(hm_ts_snap) > 0:
        snap_ms   = np.array([int(t.timestamp() * 1000) for t in hm_ts_snap], dtype=np.int64)
        win_start = int(snap_ms[0])
        win_end   = int(snap_ms[-1])
        for ev in events:
            try:
                ev_ms = int(pd.Timestamp(ev["ts"]).timestamp() * 1000)
            except Exception:
                continue
            if ev_ms < win_start or ev_ms > win_end:
                continue
            x_label = ts_labels[int(np.argmin(np.abs(snap_ms - ev_ms)))]
            price   = ev.get("price")
            side    = ev.get("side")
            color   = "rgba(0,220,100,0.9)" if side == "bid" else "rgba(220,60,60,0.9)"
            if ev.get("event") == "absorption":
                abs_x.append(x_label); abs_y.append(price); abs_clr.append(color)
                abs_sym.append("triangle-up" if side == "bid" else "triangle-down")
                abs_txt.append(f"Absorption {side}@{price:.2f} reload={ev.get('reload_ratio', 0):.0%}")
            elif ev.get("event") == "sweep":
                swp_x.append(x_label); swp_y.append(price); swp_sym.append("star"); swp_clr.append(color)
                move = (ev.get("price_move_pct") or 0.0) * 100
                swp_txt.append(f"Sweep {ev.get('direction', '?')} {side}@{price:.2f} move={move:+.3f}%")

    fig.add_trace(go.Scatter(
        x=abs_x, y=abs_y, mode="markers",
        marker=dict(symbol=abs_sym or "triangle-up", size=14,
                    color=abs_clr or "yellow", line=dict(width=1, color="white")),
        name="Absorption", text=abs_txt,
        hovertemplate="%{text}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=swp_x, y=swp_y, mode="markers",
        marker=dict(symbol=swp_sym or "star", size=18,
                    color=swp_clr or "magenta", line=dict(width=1.5, color="white")),
        name="Sweep", text=swp_txt,
        hovertemplate="%{text}<extra></extra>",
    ), row=1, col=1)
    return len(abs_x), len(swp_x)


def update_event_buffer(buffer: list[dict], msg: dict, max_events: int) -> list[dict]:
    """Append a streamed microstructure event to the event buffer, trimmed to max_events.

    Pure function. Only messages with ``type == "event"`` are accepted; snapshots
    and malformed payloads pass through unchanged. The buffer is oldest-first.
    """
    if not isinstance(msg, dict) or msg.get("type") != "event":
        return buffer
    if "ts" not in msg or "event" not in msg:
        return buffer
    new_buffer = buffer + [msg]
    if max_events > 0 and len(new_buffer) > max_events:
        new_buffer = new_buffer[-max_events:]
    return new_buffer
