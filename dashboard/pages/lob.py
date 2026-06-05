"""
/lob page — real-time LOB microstructure view.

Data arrives by **WebSocket subscription** (not polling): the engine pushes one
snapshot per second to /ws/lob (see engine/realtime_hub.py + main.py). Each push
is appended to a server-side rolling buffer; the chart redraws on every new tick.

On page load the buffer is seeded once from the lob_snapshots table so the chart
is populated immediately, then live pushes take over. If the engine is offline the
WebSocket simply stays closed and the badge reflects that — the page still shows
whatever history the DB holds.
"""
import json
from datetime import datetime, timezone, timedelta

import dash
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, callback, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from plotly.subplots import make_subplots

from config import settings
from dashboard._db import fetch_lob_snapshots, fetch_agg_trades, fetch_cvd_series_24h, DBOffline
from dashboard._logic import add_event_markers, update_event_buffer, update_lob_buffer
from dashboard._utils import empty_fig as _empty_fig

dash.register_page(__name__, path="/lob", name="LOB")

_DEPTH_WARNING = settings.LOB_DEPTH < 100
_STALE_THRESHOLD_S = 30  # flag data as stale after 30s without a new snapshot
_SGT = timezone(timedelta(hours=8))
_WS_URL = f"ws://127.0.0.1:{settings.DASHBOARD_API_PORT}/ws/lob"

# Server-side rolling buffer of streamed snapshots (shared across clients, which is
# correct here — the engine pushes identical data to everyone). Sized to the largest
# selectable window (60 min × 60 s) so the slider can widen without losing history.
_MAX_BUFFER = 3600
_BUFFER: list[dict] = []

# Parallel buffer of microstructure events (absorption / sweep) — rendered as markers
# on the heatmap row. Cap is independent of snapshot count: in heavy market activity
# a single 60-min window could see hundreds of events.
_MAX_EVENTS = 2000
_EVENTS: list[dict] = []


layout = html.Div([
    WebSocket(id="lob-ws", url=_WS_URL),
    dcc.Store(id="lob-tick"),                                # bumps on each new snapshot
    dcc.Interval(id="lob-backfill", interval=500, max_intervals=1),  # one-shot DB seed
    dcc.Interval(id="lob-clock-interval", interval=1_000),   # 1s — SGT clock

    # ── Header: SGT clock + connection badge ───────────────────────────────
    html.Div([
        html.Span(id="lob-conn-status"),
        html.Span(id="lob-clock", style={
            "fontFamily": "monospace", "fontSize": "1.1rem", "color": "#adb5bd",
        }),
    ], className="d-flex justify-content-between align-items-center mb-2"),

    # ── Depth warning banner ───────────────────────────────────────────────
    dbc.Alert(
        [
            html.Strong("LOB depth warning: "),
            f"LOB depth is {settings.LOB_DEPTH} levels. "
            "Walls detected at this depth may be within the transaction cost floor. "
            "Upgrade to depth@100 before using this page for strategy decisions.",
        ],
        color="warning",
        is_open=_DEPTH_WARNING,
        className="mb-3",
    ),

    # ── Controls ───────────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dbc.Label("Heatmap window (min)"),
            dcc.Slider(id="lob-hm-window", min=1, max=60, step=1, value=15,
                       marks={1: "1m", 15: "15m", 30: "30m", 60: "60m"}),
        ], width=3),
        dbc.Col([
            dbc.Label("Price range (±$)"),
            dcc.Slider(id="lob-price-range", min=100, max=2000, step=100, value=500,
                       marks={100: "100", 500: "500", 1000: "1000", 2000: "2000"}),
        ], width=3),
        dbc.Col([
            dbc.Label("Contrast (pctile)"),
            dcc.Slider(id="lob-contrast", min=80, max=99, step=1, value=95,
                       marks={80: "80", 90: "90", 95: "95", 99: "99"}),
        ], width=3),
        dbc.Col([
            dbc.Label("Trade size (pctile)"),
            dcc.Slider(id="lob-trade-pctile", min=70, max=99, step=1, value=80,
                       marks={70: "70", 80: "80", 90: "90", 99: "99"}),
        ], width=3),
    ], className="mb-3"),

    # ── Refresh button ─────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col(
            dbc.Button("Refresh Now", id="lob-refresh-btn", color="secondary", size="sm"),
            width="auto",
        ),
    ], className="mb-3"),

    # ── Chart ──────────────────────────────────────────────────────────────
    dcc.Graph(id="lob-chart", style={"height": "860px"}),
])


def _build_lob_figure(snaps, hm_minutes, half_range, contrast_pctile, trade_pctile, events=None):
    """Build the 4-row LOB figure from a list of snapshot dicts (ascending by ts).

    Each snapshot: {ts, mid_price, spread, obi, cvd_delta, bid_levels, ask_levels}
    where *_levels are [[price, qty], ...] lists.

    ``events``: optional list of {ts, event: "absorption"|"sweep", price, side, ...}.
    Events whose ts falls inside the heatmap window are rendered as markers on row 1.
    """
    rows_needed = hm_minutes * 60
    df = pd.DataFrame(snaps)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    age_s = (datetime.now(timezone.utc) - df["ts"].iloc[-1].to_pydatetime()).total_seconds()
    is_stale = age_s > _STALE_THRESHOLD_S

    hm_df = df.tail(rows_needed).copy()
    obi_df = df.tail(rows_needed).copy()

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.50, 0.17, 0.17, 0.16],
        vertical_spacing=0.03,
        subplot_titles=("Liquidity Heatmap", "OBI", "CVD (24h)", "Spread"),
    )

    # ── Row 1: heatmap ─────────────────────────────────────────────────────
    ts_labels: list[str] = []
    if not hm_df.empty:
        window_center = float((hm_df["mid_price"].min() + hm_df["mid_price"].max()) / 2)
        price_lo = window_center - half_range
        price_hi = window_center + half_range
        bucket_size = settings.LOB_HEATMAP_BUCKET
        price_buckets = np.arange(price_lo, price_hi + bucket_size, bucket_size)
        n_times, n_prices = len(hm_df), len(price_buckets)

        bid_matrix = np.zeros((n_prices, n_times))
        ask_matrix = np.zeros((n_prices, n_times))

        valid_cols = []
        for col_idx, row in enumerate(hm_df.itertuples(index=False)):
            col_ok = False
            for levels, matrix in (("bid_levels", bid_matrix), ("ask_levels", ask_matrix)):
                try:
                    for p, q in getattr(row, levels) or []:
                        ri = int((p - price_lo) / bucket_size)
                        if 0 <= ri < n_prices:
                            matrix[ri, col_idx] += q
                    col_ok = True
                except Exception:
                    pass
            valid_cols.append(col_ok)

        ts_labels = hm_df["ts"].dt.tz_convert(_SGT).dt.strftime("%H:%M:%S").tolist()
        mid_prices = hm_df["mid_price"].tolist()

        # Drop columns where both bid and ask levels failed — prevents x-axis misalignment
        if not all(valid_cols):
            vm = np.array(valid_cols, dtype=bool)
            bid_matrix = bid_matrix[:, vm]
            ask_matrix = ask_matrix[:, vm]
            ts_labels  = [t for t, ok in zip(ts_labels, valid_cols) if ok]
            mid_prices = [p for p, ok in zip(mid_prices, valid_cols) if ok]
        hm_ts_snap = hm_df["ts"][np.array(valid_cols, dtype=bool)] if not all(valid_cols) else hm_df["ts"]

        all_nonzero = np.concatenate([bid_matrix[bid_matrix > 0], ask_matrix[ask_matrix > 0]])
        max_vol = np.percentile(all_nonzero, contrast_pctile) if len(all_nonzero) else 1.0

        for col_idx, mp in enumerate(mid_prices):
            col_mid_ri = int((mp - price_lo) / bucket_size)
            for ri in range(max(0, col_mid_ri - 5), min(n_prices, col_mid_ri + 6)):
                bid_matrix[ri, col_idx] = 0.0
                ask_matrix[ri, col_idx] = 0.0

        fig.add_trace(go.Heatmap(
            z=bid_matrix, x=ts_labels, y=price_buckets,
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,200,80,0.85)"]],
            zmin=0, zmax=max_vol, showscale=False,
            hovertemplate="Bid qty: %{z:.4f}<extra></extra>", name="Bids",
        ), row=1, col=1)
        fig.add_trace(go.Heatmap(
            z=ask_matrix, x=ts_labels, y=price_buckets,
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(220,40,40,0.85)"]],
            zmin=0, zmax=max_vol, showscale=False,
            hovertemplate="Ask qty: %{z:.4f}<extra></extra>", name="Asks",
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=ts_labels, y=mid_prices, mode="lines",
            line=dict(color="white", width=1.5), name="Mid price", hoverinfo="skip",
        ), row=1, col=1)

    # ── Rows 2-4: OBI / CVD / Spread ───────────────────────────────────────
    if not obi_df.empty:
        obi_ts = obi_df["ts"].dt.tz_convert(_SGT).dt.strftime("%H:%M:%S").tolist()
        fig.add_trace(go.Scatter(
            x=obi_ts, y=obi_df["obi"].tolist(), mode="lines",
            line=dict(color="cyan", width=1.2), name="OBI",
            fill="tozeroy", fillcolor="rgba(0,200,200,0.15)",
        ), row=2, col=1)
        fig.add_hline(y=settings.OBI_BREAK_THRESH,  line=dict(dash="dot", color="lime", width=1), row=2, col=1)
        fig.add_hline(y=-settings.OBI_BREAK_THRESH, line=dict(dash="dot", color="red",  width=1), row=2, col=1)
        fig.add_hline(y=0, line=dict(color="white", width=0.5), row=2, col=1)

        since_24h_ms = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
        cvd_rows = fetch_cvd_series_24h(since_24h_ms)
        if cvd_rows and not isinstance(cvd_rows, DBOffline):
            cvd_ms  = np.array([r["ts_sec_ms"] for r in cvd_rows], dtype=np.int64)
            cvd_cum = np.cumsum([r["delta"]     for r in cvd_rows])
            # Use .timestamp()*1000: obi_df["ts"] is datetime64[us, UTC] in pandas 3.x,
            # so astype(int64) gives microseconds — // 10**6 would yield seconds, not ms.
            obi_ms  = np.array([int(t.timestamp() * 1000) for t in obi_df["ts"]], dtype=np.int64)
            cvd_y   = [float(cvd_cum[np.argmin(np.abs(cvd_ms - t))]) for t in obi_ms]
        else:
            cvd_y = [0.0] * len(obi_ts)

        fig.add_trace(go.Scatter(
            x=obi_ts, y=cvd_y, mode="lines",
            line=dict(color="orange", width=1.2), name="CVD (24h)",
            fill="tozeroy", fillcolor="rgba(255,165,0,0.12)",
        ), row=3, col=1)
        fig.add_hline(y=0, line=dict(color="white", width=0.5), row=3, col=1)

        fig.add_trace(go.Scatter(
            x=obi_ts, y=obi_df["spread"].tolist(), mode="lines",
            line=dict(color="violet", width=1), name="Spread",
        ), row=4, col=1)
    else:
        for row in (2, 3, 4):
            fig.add_trace(go.Scatter(x=[], y=[], showlegend=False), row=row, col=1)

    # ── Row 1 overlay: market order bubbles (from DB agg_trades) ───────────
    if not hm_df.empty and ts_labels:
        snap_ms   = np.array([int(t.timestamp() * 1000) for t in hm_ts_snap], dtype=np.int64)
        win_start = int(snap_ms[0])
        win_end   = int(snap_ms[-1])

        trades = fetch_agg_trades(win_start, until_ts_ms=win_end, limit=None)
        buy_x,  buy_y,  buy_sz,  buy_txt  = [], [], [], []
        sell_x, sell_y, sell_sz, sell_txt = [], [], [], []

        if trades and not isinstance(trades, DBOffline):
            tdf = pd.DataFrame(trades)
            tdf = tdf[(tdf["ts_event"] >= win_start) & (tdf["ts_event"] <= win_end)].copy()
            if not tdf.empty:
                threshold = tdf["qty"].quantile((trade_pctile or 80) / 100)
                tdf = tdf[tdf["qty"] >= threshold].copy()
            if not tdf.empty:
                snapped_x = [
                    ts_labels[int(np.argmin(np.abs(snap_ms - t)))]
                    for t in tdf["ts_event"].values
                ]
                sizes = np.clip(np.log1p(tdf["qty"].values) * 6, 5, 24)
                buy_mask  = tdf["is_buyer_maker"].values == 0
                sell_mask = ~buy_mask
                buy_x   = [snapped_x[i] for i, ok in enumerate(buy_mask)  if ok]
                buy_y   = tdf["price"].values[buy_mask].tolist()
                buy_sz  = sizes[buy_mask].tolist()
                buy_txt = tdf["qty"].values[buy_mask].round(4).astype(str).tolist()
                sell_x   = [snapped_x[i] for i, ok in enumerate(sell_mask) if ok]
                sell_y   = tdf["price"].values[sell_mask].tolist()
                sell_sz  = sizes[sell_mask].tolist()
                sell_txt = tdf["qty"].values[sell_mask].round(4).astype(str).tolist()

        fig.add_trace(go.Scatter(
            x=buy_x, y=buy_y, mode="markers",
            marker=dict(size=buy_sz or 8, color="rgba(0,220,100,0.8)",
                        line=dict(width=0.5, color="white")),
            name="Buy MO", text=buy_txt,
            hovertemplate="qty: %{text}<extra></extra>",
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=sell_x, y=sell_y, mode="markers",
            marker=dict(size=sell_sz or 8, color="rgba(220,60,60,0.8)",
                        line=dict(width=0.5, color="white")),
            name="Sell MO", text=sell_txt,
            hovertemplate="qty: %{text}<extra></extra>",
        ), row=1, col=1)
    else:
        for label in ("Buy MO", "Sell MO"):
            fig.add_trace(go.Scatter(x=[], y=[], mode="markers",
                                     name=label, showlegend=False), row=1, col=1)

    # ── Row 1 overlay: microstructure event markers ────────────────────────
    add_event_markers(fig, events, ts_labels, hm_ts_snap if not hm_df.empty else None)

    fig.update_layout(
        height=860, template="plotly_dark",
        uirevision="lob-chart",
        yaxis_title="Price (USDT)",
        legend=dict(orientation="h", y=-0.06, font=dict(size=10)),
        margin=dict(t=40, b=20),
    )
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=3, col=1)
    fig.update_xaxes(title_text="Time (SGT)", row=4, col=1)

    if is_stale:
        fig.add_annotation(
            text=f"⚠ Last snapshot {age_s:.0f}s ago — data may be stale",
            xref="paper", yref="paper", x=0.01, y=0.99,
            showarrow=False, font=dict(color="orange", size=12),
            bgcolor="rgba(0,0,0,0.5)",
        )
    return fig


def _seed_buffer_from_db() -> str | None:
    """Fetch LOB snapshots from DB into _BUFFER. Returns latest ts or None."""
    global _BUFFER
    rows = fetch_lob_snapshots(limit=_MAX_BUFFER)
    if isinstance(rows, DBOffline) or not rows:
        return None
    seeded = []
    for r in rows:  # ascending (oldest first)
        try:
            bid = json.loads(r.get("bid_levels_json") or "[]")
            ask = json.loads(r.get("ask_levels_json") or "[]")
        except Exception:
            bid, ask = [], []
        seeded.append({
            "ts": r["ts"], "mid_price": r["mid_price"], "spread": r["spread"],
            "obi": r["obi"], "cvd_delta": r["cvd_delta"],
            "bid_levels": bid, "ask_levels": ask,
        })
    _BUFFER = seeded
    return seeded[-1]["ts"]


@callback(
    Output("lob-tick", "data", allow_duplicate=True),
    Input("lob-backfill", "n_intervals"),
    prevent_initial_call=True,
)
def backfill_buffer(_n):
    """Seed the server-side buffer once from the DB so the chart isn't empty on load."""
    if _BUFFER:
        return no_update
    ts = _seed_buffer_from_db()
    return ts if ts else no_update


@callback(
    Output("lob-tick", "data", allow_duplicate=True),
    Input("lob-refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_buffer(_n_clicks):
    """Re-seed the buffer from DB on manual refresh and trigger a redraw."""
    ts = _seed_buffer_from_db()
    return ts if ts else no_update


@callback(
    Output("lob-tick", "data"),
    Input("lob-ws", "message"),
    prevent_initial_call=True,
)
def on_ws_message(message):
    """Route one pushed WS message to either snapshot or event buffer; return its ts."""
    global _BUFFER, _EVENTS
    if not message or "data" not in message:
        return no_update
    try:
        msg = json.loads(message["data"])
    except (TypeError, ValueError):
        return no_update
    msg_type = msg.get("type", "snapshot")
    if msg_type == "event":
        _EVENTS = update_event_buffer(_EVENTS, msg, _MAX_EVENTS)
    else:
        _BUFFER = update_lob_buffer(_BUFFER, msg, _MAX_BUFFER)
    return msg.get("ts", no_update)


@callback(
    Output("lob-chart", "figure"),
    Input("lob-tick", "data"),
    Input("lob-hm-window", "value"),
    Input("lob-price-range", "value"),
    Input("lob-contrast", "value"),
    Input("lob-trade-pctile", "value"),
)
def render_lob_chart(_tick, hm_minutes, half_range, contrast_pctile, trade_pctile):
    if not _BUFFER:
        return _empty_fig("Waiting for LOB data… (start the engine for the live stream)")
    return _build_lob_figure(
        list(_BUFFER), hm_minutes, half_range, contrast_pctile, trade_pctile,
        events=list(_EVENTS),
    )


@callback(
    Output("lob-conn-status", "children"),
    Input("lob-ws", "state"),
)
def update_conn_status(state):
    ready = (state or {}).get("readyState")
    if ready == 1:
        return dbc.Badge("● LIVE", color="success", className="fs-6")
    if ready == 0:
        return dbc.Badge("● Connecting…", color="warning", className="fs-6")
    return dbc.Badge("● Engine offline", color="secondary", className="fs-6")


@callback(
    Output("lob-clock", "children"),
    Input("lob-clock-interval", "n_intervals"),
)
def update_clock(_):
    now_sgt = datetime.now(_SGT)
    return f"SGT  {now_sgt.strftime('%Y-%m-%d  %H:%M:%S')}"
