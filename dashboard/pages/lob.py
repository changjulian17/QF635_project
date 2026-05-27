import json
from datetime import datetime, timezone, timedelta

import dash
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, callback, no_update, Patch
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots

from config import settings
from dashboard._db import fetch_lob_snapshots, fetch_agg_trades, DBOffline
from dashboard._utils import empty_fig as _empty_fig

dash.register_page(__name__, path="/lob", name="LOB")

_DEPTH_WARNING = settings.LOB_DEPTH < 100
_STALE_THRESHOLD_S = 30  # flag data as stale after 30s without a new snapshot
_SGT = timezone(timedelta(hours=8))

layout = html.Div([
    dcc.Interval(id="lob-interval", interval=30_000),       # 30s — full rebuild
    dcc.Interval(id="lob-lines-interval", interval=5_000),  # 5s — incremental line update
    dcc.Interval(id="lob-clock-interval", interval=1_000),  # 1s — SGT clock
    dcc.Store(id="lob-last-ts"),                            # tracks last snapshot ts

    # ── SGT clock ─────────────────────────────────────────────────────────
    html.Div(id="lob-clock", style={
        "textAlign": "right", "fontFamily": "monospace",
        "fontSize": "1.1rem", "color": "#adb5bd", "marginBottom": "6px",
    }),

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
    dbc.Row([
        dbc.Col([
            dbc.Button("Refresh Now", id="lob-refresh-btn", color="secondary",
                       size="sm"),
        ], width=3),
    ], className="mb-3"),

    # ── Chart ──────────────────────────────────────────────────────────────
    dcc.Graph(id="lob-chart", style={"height": "860px"}),
])



@callback(
    Output("lob-chart", "figure"),
    Output("lob-last-ts", "data"),
    Input("lob-interval", "n_intervals"),
    Input("lob-refresh-btn", "n_clicks"),
    Input("lob-hm-window", "value"),
    Input("lob-price-range", "value"),
    Input("lob-contrast", "value"),
    Input("lob-trade-pctile", "value"),
)
def update_lob_chart(n, n_clicks, hm_minutes, half_range, contrast_pctile, trade_pctile):
    rows_needed = hm_minutes * 60
    snapshots = fetch_lob_snapshots(limit=max(rows_needed, 3600))

    if isinstance(snapshots, DBOffline):
        return _empty_fig("LOB DB offline — start engine first"), None

    if not snapshots:
        return _empty_fig("Waiting for LOB snapshot data…"), None

    df = pd.DataFrame(snapshots)
    df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", utc=True)

    # H1: staleness check
    age_s = (datetime.now(timezone.utc) - df["ts"].iloc[-1].to_pydatetime()).total_seconds()
    is_stale = age_s > _STALE_THRESHOLD_S

    hm_df = df.tail(rows_needed).copy()
    obi_df = df.tail(rows_needed).copy()

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.50, 0.17, 0.17, 0.16],
        vertical_spacing=0.03,
        subplot_titles=("Liquidity Heatmap", "OBI", "CVD", "Spread"),
    )

    # ── Row 1: heatmap ─────────────────────────────────────────────────────
    if not hm_df.empty:
        current_mid   = float(hm_df["mid_price"].iloc[-1])
        window_center = float((hm_df["mid_price"].min() + hm_df["mid_price"].max()) / 2)
        price_lo = window_center - half_range
        price_hi = window_center + half_range
        bucket_size = settings.LOB_HEATMAP_BUCKET
        price_buckets = np.arange(price_lo, price_hi + bucket_size, bucket_size)
        n_times, n_prices = len(hm_df), len(price_buckets)

        bid_matrix = np.zeros((n_prices, n_times))
        ask_matrix = np.zeros((n_prices, n_times))

        # H3: track which columns parsed successfully for accurate x-axis labelling
        valid_cols = []
        for col_idx, row in enumerate(hm_df.itertuples(index=False)):
            col_ok = False
            try:
                bid_json = row.bid_levels_json
                if bid_json:
                    for p, q in json.loads(bid_json):
                        ri = int((p - price_lo) / bucket_size)
                        if 0 <= ri < n_prices:
                            bid_matrix[ri, col_idx] += q
                    col_ok = True
            except Exception:
                pass
            try:
                ask_json = row.ask_levels_json
                if ask_json:
                    for p, q in json.loads(ask_json):
                        ri = int((p - price_lo) / bucket_size)
                        if 0 <= ri < n_prices:
                            ask_matrix[ri, col_idx] += q
                    col_ok = True
            except Exception:
                pass
            valid_cols.append(col_ok)

        ts_labels = hm_df["ts"].dt.tz_convert(_SGT).dt.strftime("%H:%M:%S").tolist()
        mid_prices = hm_df["mid_price"].tolist()

        # H3: drop columns where both bid and ask JSON failed — prevents x-axis misalignment
        if not all(valid_cols):
            vm = np.array(valid_cols, dtype=bool)
            bid_matrix = bid_matrix[:, vm]
            ask_matrix = ask_matrix[:, vm]
            ts_labels  = [t for t, ok in zip(ts_labels, valid_cols) if ok]
            mid_prices = [p for p, ok in zip(mid_prices, valid_cols) if ok]
        hm_ts_snap = hm_df["ts"][np.array(valid_cols, dtype=bool)] if not all(valid_cols) else hm_df["ts"]

        all_nonzero = np.concatenate([bid_matrix[bid_matrix > 0], ask_matrix[ask_matrix > 0]])
        max_vol = np.percentile(all_nonzero, contrast_pctile) if len(all_nonzero) else 1.0

        mid_ri = int((current_mid - price_lo) / bucket_size)
        for ri in range(max(0, mid_ri - 5), min(n_prices, mid_ri + 6)):
            bid_matrix[ri, :] = 0.0
            ask_matrix[ri, :] = 0.0

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

    # ── Rows 2-4: OBI / CVD / Spread — guarded against empty data (H2) ────
    if not obi_df.empty:
        obi_ts = obi_df["ts"].dt.tz_convert(_SGT).dt.strftime("%H:%M:%S").tolist()
        # M4: fill NaN before cumsum so one bad row doesn't corrupt the rest
        cvd_running = obi_df["cvd_delta"].fillna(0.0).cumsum().tolist()

        fig.add_trace(go.Scatter(
            x=obi_ts, y=obi_df["obi"].tolist(), mode="lines",
            line=dict(color="cyan", width=1.2), name="OBI",
            fill="tozeroy", fillcolor="rgba(0,200,200,0.15)",
        ), row=2, col=1)
        fig.add_hline(y=settings.OBI_BREAK_THRESH,  line=dict(dash="dot", color="lime", width=1), row=2, col=1)
        fig.add_hline(y=-settings.OBI_BREAK_THRESH, line=dict(dash="dot", color="red",  width=1), row=2, col=1)
        fig.add_hline(y=0, line=dict(color="white", width=0.5), row=2, col=1)

        fig.add_trace(go.Scatter(
            x=obi_ts, y=cvd_running, mode="lines",
            line=dict(color="orange", width=1.2), name="CVD",
            fill="tozeroy", fillcolor="rgba(255,165,0,0.12)",
        ), row=3, col=1)
        fig.add_hline(y=0, line=dict(color="white", width=0.5), row=3, col=1)

        fig.add_trace(go.Scatter(
            x=obi_ts, y=obi_df["spread"].tolist(), mode="lines",
            line=dict(color="violet", width=1), name="Spread",
        ), row=4, col=1)
    else:
        # preserve subplot structure with empty traces
        for row in (2, 3, 4):
            fig.add_trace(go.Scatter(x=[], y=[], showlegend=False), row=row, col=1)

    # ── Row 1 overlay: market order bubbles (indices 6 & 7) ────────────────
    if not hm_df.empty and ts_labels:
        snap_ms   = np.array([int(t.timestamp() * 1000) for t in hm_ts_snap], dtype=np.int64)
        win_start = int(snap_ms[0])
        win_end   = int(snap_ms[-1])

        trades = fetch_agg_trades(win_start)
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

    # H1: staleness annotation overlaid on chart
    if is_stale:
        fig.add_annotation(
            text=f"⚠ Last snapshot {age_s:.0f}s ago — data may be stale",
            xref="paper", yref="paper", x=0.01, y=0.99,
            showarrow=False, font=dict(color="orange", size=12),
            bgcolor="rgba(0,0,0,0.5)",
        )

    last_ts = df["ts"].iloc[-1].isoformat() if not df.empty else None
    return fig, last_ts


@callback(
    Output("lob-chart", "figure", allow_duplicate=True),
    Input("lob-lines-interval", "n_intervals"),
    State("lob-last-ts", "data"),
    State("lob-hm-window", "value"),
    prevent_initial_call=True,
)
def update_lob_lines(n, last_ts, hm_minutes):
    """Incremental update: replaces only OBI/CVD/Spread trace data every 5s."""
    if last_ts is None:
        return no_update

    rows_needed = (hm_minutes or 15) * 60
    snapshots = fetch_lob_snapshots(limit=rows_needed)

    if isinstance(snapshots, DBOffline) or not snapshots:
        return no_update

    df = pd.DataFrame(snapshots)
    df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", utc=True)
    obi_df = df.tail(rows_needed)

    if obi_df.empty:
        return no_update

    obi_ts = obi_df["ts"].dt.tz_convert(_SGT).dt.strftime("%H:%M:%S").tolist()
    cvd_running = obi_df["cvd_delta"].fillna(0.0).cumsum().tolist()

    patched = Patch()
    patched["data"][3]["x"] = obi_ts
    patched["data"][3]["y"] = obi_df["obi"].tolist()
    patched["data"][4]["x"] = obi_ts
    patched["data"][4]["y"] = cvd_running
    patched["data"][5]["x"] = obi_ts
    patched["data"][5]["y"] = obi_df["spread"].tolist()
    return patched


@callback(
    Output("lob-clock", "children"),
    Input("lob-clock-interval", "n_intervals"),
)
def update_clock(_):
    now_sgt = datetime.now(_SGT)
    return f"SGT  {now_sgt.strftime('%Y-%m-%d  %H:%M:%S')}"
