import json
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, callback
from plotly.subplots import make_subplots

from config import settings
from dashboard._db import fetch_candles, fetch_lob_snapshots, DBOffline
from dashboard._utils import empty_fig as _empty_fig
from strategy.microstructure import identify_walls

dash.register_page(__name__, path="/walls", name="Walls")

_STALE_THRESHOLD_S = 30

layout = html.Div([
    dcc.Interval(id="walls-interval", interval=15000),
    dbc.Row([
        dbc.Col([
            dbc.Label("Window (min)"),
            dcc.Slider(id="walls-window", min=5, max=60, step=5, value=15,
                       marks={5: "5m", 15: "15m", 30: "30m", 60: "60m"}),
        ], width=4),
        dbc.Col([
            dbc.Label("Price range (±$)"),
            dcc.Slider(id="walls-range", min=100, max=2000, step=100, value=500,
                       marks={100: "100", 500: "500", 1000: "1k", 2000: "2k"}),
        ], width=4),
        dbc.Col([
            dbc.Label("Contrast (pctile)"),
            dcc.Slider(id="walls-contrast", min=80, max=99, step=1, value=95,
                       marks={80: "80", 90: "90", 95: "95", 99: "99"}),
        ], width=4),
    ], className="mb-3"),
    dcc.Loading(
        dcc.Graph(id="walls-chart", style={"height": "860px"}),
        type="circle",
    ),
])


@callback(
    Output("walls-chart", "figure"),
    Input("walls-interval", "n_intervals"),
    Input("walls-window", "value"),
    Input("walls-range", "value"),
    Input("walls-contrast", "value"),
)
def update_walls_chart(n, window_min, half_range, contrast_pctile):
    candles = fetch_candles(limit=7200)
    snapshots = fetch_lob_snapshots(limit=3600)

    if isinstance(candles, DBOffline) or isinstance(snapshots, DBOffline):
        return _empty_fig("DB offline — start engine first")
    if not candles or not snapshots:
        return _empty_fig("Waiting for data…")

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
        return _empty_fig("Insufficient data in selected window")

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
        return _empty_fig("Insufficient data in selected window")

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

    age_s = (datetime.now(timezone.utc) - sdf["ts"].iloc[-1].to_pydatetime()).total_seconds()
    if age_s > _STALE_THRESHOLD_S:
        fig.add_annotation(
            text=f"⚠ Last snapshot {age_s:.0f}s ago — data may be stale",
            xref="paper", yref="paper", x=0.01, y=0.99,
            showarrow=False, font=dict(color="orange", size=12),
            bgcolor="rgba(0,0,0,0.5)",
        )

    return fig
