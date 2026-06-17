"""Pure logic functions shared by dashboard pages — importable without Dash app context."""
import json

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
from plotly.subplots import make_subplots

from config import settings
from dashboard._utils import empty_fig
from strategy.microstructure import identify_walls


def validate_ks_confirm(value: str) -> bool:
    """Return True (button disabled) unless value is exactly 'CONFIRM'."""
    return value != "CONFIRM"


# Selectable display window → hours. The equity curve is retention-capped to 7d by the
# caller (the portfolio table is purged at 7d); signal_records aggregates honour all.
WINDOW_HOURS = {"1h": 1, "24h": 24, "7d": 168, "30d": 720}
DEFAULT_WINDOW = "24h"


def window_to_hours(window: str | None) -> int:
    """Map a window label ('1h'/'24h'/'7d'/'30d') to hours, defaulting to 24."""
    return WINDOW_HOURS.get(window or DEFAULT_WINDOW, 24)


def dov_budget_color(loss_pct: float, reduced_pct: float, passive_pct: float) -> str:
    """Risk-tier Bootstrap colour for the DOV daily-loss budget (all args fractions of DOV)."""
    if loss_pct >= passive_pct:
        return "danger"
    if loss_pct >= reduced_pct:
        return "warning"
    return "success"


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
    """Merge the latest WS portfolio payload into the cached state.

    Pure function (no Dash/browser deps) so the streaming logic is unit-testable.

    Uses merge-then-update semantics: fields present in ``msg`` always win, but
    fields absent from ``msg`` (e.g. ``risk_tier`` in a fill-push that was built
    without it) are preserved from the prior ``state``. This prevents the Risk Tier
    card from briefly blanking when a partial fill-push arrives between 1 Hz MTM ticks.
    Malformed messages or those tagged with a non-portfolio type return unchanged state.
    """
    if not isinstance(msg, dict):
        return state
    if msg.get("type") != "portfolio":
        return state
    if "equity" not in msg:  # minimal shape check — the one field we always render
        return state
    merged = dict(state)
    merged.update(msg)
    return merged


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


# Mapping of microstructure_bars flag column -> (marker event category, side, label).
# "absorption"/"sweep" categories are renderable by add_event_markers; "other" rows
# appear only in the event tape. Order controls tape display order within one bar.
_MS_EVENT_FLAGS = [
    ("sweep_up",            "sweep",      "ask", "Sweep up"),
    ("sweep_down",          "sweep",      "bid", "Sweep down"),
    ("reload_bid",          "absorption", "bid", "Reload bid"),
    ("reload_ask",          "absorption", "ask", "Reload ask"),
    ("iceberg_bid",         "other",      "bid", "Iceberg bid"),
    ("iceberg_ask",         "other",      "ask", "Iceberg ask"),
    ("book_flip_bid",       "other",      "bid", "Book-flip bid"),
    ("book_flip_ask",       "other",      "ask", "Book-flip ask"),
    ("liq_flip_to_res",     "other",      "ask", "Liq-flip → resistance"),
    ("liq_flip_to_sup",     "other",      "bid", "Liq-flip → support"),
    ("break_protect_long",  "other",      "bid", "Break-protect long"),
    ("break_protect_short", "other",      "ask", "Break-protect short"),
]


def microstructure_bars_to_events(rows: list[dict]) -> list[dict]:
    """Melt non-zero ``microstructure_bars`` flag columns into event dicts.

    The flag columns are per-bar booleans, not the absorption/sweep event-dict shape
    that ``add_event_markers`` consumes — so this adapter bridges the two. Each output
    event is ``{ts, event, side, price, kind, label}`` (sweep rows also carry
    ``direction``); ``price`` is the bar's mid_price since the flags are per-bar, not
    per-level. ``event`` is "absorption" | "sweep" (renderable as heatmap markers) or
    "other" (tape only). Output preserves input (ascending) order.
    """
    events: list[dict] = []
    for r in rows:
        ts = r.get("ts")
        price = r.get("mid_price")
        for col, event, side, label in _MS_EVENT_FLAGS:
            if r.get(col):
                ev = {"ts": ts, "event": event, "side": side,
                      "price": price, "kind": col, "label": label}
                if event == "sweep":
                    ev["direction"] = "up" if col == "sweep_up" else "down"
                events.append(ev)
    return events


def add_event_markers(fig, events, ts_labels, hm_ts_snap):
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


def handle_position_event(state: dict, msg: dict) -> dict:
    """Update cached position state from a position_opened or close_event WS message.

    Pure function — returns a new dict; does not mutate inputs.
    - position_opened: returns the position payload from msg (replaces any prior state)
    - close_event: returns {} (position cleared)
    Any other type returns state unchanged.
    """
    event_type = msg.get("type")
    if event_type == "position_opened":
        return {k: v for k, v in msg.items() if k != "type"}
    if event_type == "close_event":
        return {}
    return state


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


def build_walls_figure(candles, snapshots, window_min, half_range, contrast_pctile,
                       stale_threshold_s: int = 30):
    """Build the Walls figure (candles + VWAP + wall heatmap) from already-fetched data.

    Pure (Dash-free, no DB access) so it is unit-testable: callers fetch ``candles``
    and ``snapshots`` and pass them in. Empty/insufficient inputs return an annotated
    empty figure rather than raising.
    """
    if not candles or not snapshots:
        return empty_fig("Waiting for data…")

    cdf = pd.DataFrame(candles)
    cdf["open_time"] = pd.to_datetime(cdf["open_time"], format="ISO8601", utc=True)

    sdf = pd.DataFrame(snapshots)
    sdf["ts"] = pd.to_datetime(sdf["ts"], format="ISO8601", utc=True)

    # VWAP must be computed before clipping so the rolling window has full history
    cdf["vwap"] = (
        (cdf["close"] * cdf["volume"]).rolling(3600, min_periods=1).sum()
        / cdf["volume"].rolling(3600, min_periods=1).sum()
    )

    t_end = min(cdf["open_time"].iloc[-1], sdf["ts"].iloc[-1])
    t_avail = max(cdf["open_time"].iloc[0], sdf["ts"].iloc[0])
    t_start = max(t_avail, t_end - pd.Timedelta(minutes=window_min))

    cdf = cdf[(cdf["open_time"] >= t_start) & (cdf["open_time"] <= t_end)].copy()
    sdf = sdf[(sdf["ts"] >= t_start) & (sdf["ts"] <= t_end)].copy()

    if cdf.empty or sdf.empty:
        return empty_fig("Insufficient data in selected window")

    # Use the last snapshot in the display window, not the raw fetch tail
    latest = sdf.iloc[-1]
    bid_walls, ask_walls = [], []
    try:
        raw_bids = json.loads(latest["bid_levels_json"] or "[]")
        bid_walls = identify_walls(sorted(raw_bids, key=lambda x: -x[0]), side="bid")
    except Exception:
        pass
    try:
        raw_asks = json.loads(latest["ask_levels_json"] or "[]")
        ask_walls = identify_walls(sorted(raw_asks, key=lambda x: x[0]), side="ask")
    except Exception:
        pass

    all_walls = bid_walls + ask_walls

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.45],
        vertical_spacing=0.03,
        subplot_titles=("Price / VWAP / Walls", "Liquidity Heatmap"),
    )

    fig.add_trace(go.Candlestick(
        x=cdf["open_time"],
        open=cdf["open"], high=cdf["high"], low=cdf["low"], close=cdf["close"],
        increasing_line_color="lime", decreasing_line_color="tomato",
        name="Price",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=cdf["open_time"], y=cdf["vwap"],
        mode="lines", line=dict(color="orange", width=1.5, dash="dot"),
        name="VWAP (1h)",
    ), row=1, col=1)

    last_ts = cdf["open_time"].iloc[-1].isoformat()
    for w in all_walls:
        color = "lime" if w["side"] == "bid" else "tomato"
        fig.add_hline(y=w["price"], line=dict(color=color, dash="dash", width=1), row=1, col=1)
        fig.add_annotation(
            x=last_ts, y=w["price"],
            text=f"{w['price']:,.0f} | {w['sigma']:.1f}σ",
            showarrow=False, xanchor="left",
            font=dict(color=color, size=10),
            xref="x1", yref="y1",
        )

    bucket_size = settings.LOB_HEATMAP_BUCKET
    current_mid = float(sdf["mid_price"].iloc[-1])
    price_lo = current_mid - half_range
    price_buckets = np.arange(price_lo, current_mid + half_range + bucket_size, bucket_size)
    n_prices = len(price_buckets)

    bid_matrix = np.zeros((n_prices, len(sdf)))
    ask_matrix = np.zeros((n_prices, len(sdf)))
    valid_cols = []

    for col_idx, row in enumerate(sdf.itertuples(index=False)):
        col_ok = False
        for attr, matrix in [("bid_levels_json", bid_matrix), ("ask_levels_json", ask_matrix)]:
            try:
                raw = getattr(row, attr)
                if raw:
                    for p, q in json.loads(raw):
                        ri = int((p - price_lo) / bucket_size)
                        if 0 <= ri < n_prices:
                            matrix[ri, col_idx] += q
                    col_ok = True
            except Exception:
                pass
        valid_cols.append(col_ok)

    # Prevents x-axis misalignment when both bid and ask JSON fail for a column
    if not all(valid_cols):
        vm = np.array(valid_cols, dtype=bool)
        bid_matrix = bid_matrix[:, vm]
        ask_matrix = ask_matrix[:, vm]
        sdf = sdf[vm].reset_index(drop=True)

    if sdf.empty:
        return empty_fig("Insufficient data in selected window")

    hm_ts = sdf["ts"].tolist()
    mid_prices = sdf["mid_price"].tolist()

    all_nonzero = np.concatenate([bid_matrix[bid_matrix > 0], ask_matrix[ask_matrix > 0]])
    max_vol = np.percentile(all_nonzero, contrast_pctile) if len(all_nonzero) else 1.0

    mid_ri = int((current_mid - price_lo) / bucket_size)
    for ri in range(max(0, mid_ri - 1), min(n_prices, mid_ri + 2)):
        bid_matrix[ri, :] = 0.0
        ask_matrix[ri, :] = 0.0

    # Wall highlight matrix sized to post-mask column count
    wall_matrix = np.zeros_like(bid_matrix)
    for w in all_walls:
        ri = int((w["price"] - price_lo) / bucket_size)
        if 0 <= ri < n_prices:
            wall_matrix[ri, :] = max_vol

    fig.add_trace(go.Heatmap(
        z=bid_matrix, x=hm_ts, y=price_buckets,
        colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,200,80,0.85)"]],
        zmin=0, zmax=max_vol, showscale=False,
        hovertemplate="Bid qty: %{z:.4f}<extra></extra>", name="Bids",
    ), row=2, col=1)
    fig.add_trace(go.Heatmap(
        z=ask_matrix, x=hm_ts, y=price_buckets,
        colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(220,40,40,0.85)"]],
        zmin=0, zmax=max_vol, showscale=False,
        hovertemplate="Ask qty: %{z:.4f}<extra></extra>", name="Asks",
    ), row=2, col=1)
    fig.add_trace(go.Heatmap(
        z=wall_matrix, x=hm_ts, y=price_buckets,
        colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(255,255,100,0.45)"]],
        zmin=0, zmax=max_vol, showscale=False,
        hoverinfo="skip", name="Walls",
    ), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=hm_ts, y=mid_prices, mode="lines",
        line=dict(color="white", width=1), name="Mid", hoverinfo="skip",
    ), row=2, col=1)

    fig.update_layout(
        height=860, template="plotly_dark",
        legend=dict(orientation="h", y=-0.06, font=dict(size=10)),
        margin=dict(t=40, b=20),
    )
    fig.update_xaxes(rangeslider_visible=False)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(title_text="Time (UTC)", row=2, col=1)
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)

    age_s = (pd.Timestamp.now(tz="UTC") - sdf["ts"].iloc[-1]).total_seconds()
    if age_s > stale_threshold_s:
        fig.add_annotation(
            text=f"⚠ Last snapshot {age_s:.0f}s ago — data may be stale",
            xref="paper", yref="paper", x=0.01, y=0.99,
            showarrow=False, font=dict(color="orange", size=12),
            bgcolor="rgba(0,0,0,0.5)",
        )

    return fig
