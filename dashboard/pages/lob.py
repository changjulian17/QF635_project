import json

import dash
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots

from config import settings
from dashboard._db import fetch_lob_snapshots

dash.register_page(__name__, path="/lob", name="LOB")

_DEPTH_WARNING = settings.LOB_DEPTH < 100

layout = html.Div([
    dcc.Interval(id="lob-interval", interval=5000),

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
        ], width=4),
        dbc.Col([
            dbc.Label("Price range (±$)"),
            dcc.Slider(id="lob-price-range", min=100, max=2000, step=100, value=500,
                       marks={100: "100", 500: "500", 1000: "1000", 2000: "2000"}),
        ], width=4),
        dbc.Col([
            dbc.Label("Contrast (pctile)"),
            dcc.Slider(id="lob-contrast", min=80, max=99, step=1, value=95,
                       marks={80: "80", 90: "90", 95: "95", 99: "99"}),
        ], width=4),
    ], className="mb-3"),

    # ── Chart ──────────────────────────────────────────────────────────────
    dcc.Loading(
        dcc.Graph(id="lob-chart", style={"height": "860px"}),
        type="circle",
    ),
])


@callback(
    Output("lob-chart", "figure"),
    Input("lob-interval", "n_intervals"),
    Input("lob-hm-window", "value"),
    Input("lob-price-range", "value"),
    Input("lob-contrast", "value"),
)
def update_lob_chart(n, hm_minutes, half_range, contrast_pctile):
    rows_needed = hm_minutes * 60
    snapshots = fetch_lob_snapshots(limit=max(rows_needed, 3600))

    if not snapshots:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            annotations=[{
                "text": "Waiting for LOB snapshot data…",
                "showarrow": False,
                "font": {"size": 16},
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
            }],
        )
        return fig

    df = pd.DataFrame(snapshots)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    hm_df = df.tail(rows_needed).copy()
    obi_df = df.tail(3600).copy()

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.50, 0.17, 0.17, 0.16],
        vertical_spacing=0.03,
        subplot_titles=("Liquidity Heatmap", "OBI", "CVD", "Spread"),
    )

    # ── Row 1: heatmap ─────────────────────────────────────────────────────
    if not hm_df.empty:
        current_mid = float(hm_df["mid_price"].iloc[-1])
        price_lo = current_mid - half_range
        price_hi = current_mid + half_range
        bucket_size = settings.LOB_HEATMAP_BUCKET
        price_buckets = np.arange(price_lo, price_hi + bucket_size, bucket_size)
        n_times, n_prices = len(hm_df), len(price_buckets)

        bid_matrix = np.zeros((n_prices, n_times))
        ask_matrix = np.zeros((n_prices, n_times))

        for col_idx, row in enumerate(hm_df.itertuples(index=False)):
            try:
                for p, q in json.loads(row.bid_levels_json):
                    ri = int((p - price_lo) / bucket_size)
                    if 0 <= ri < n_prices:
                        bid_matrix[ri, col_idx] += q
            except Exception:
                pass
            try:
                for p, q in json.loads(row.ask_levels_json):
                    ri = int((p - price_lo) / bucket_size)
                    if 0 <= ri < n_prices:
                        ask_matrix[ri, col_idx] += q
            except Exception:
                pass

        mid_ri = int((current_mid - price_lo) / bucket_size)
        for ri in range(max(0, mid_ri - 1), min(n_prices, mid_ri + 2)):
            bid_matrix[ri, :] = 0.0
            ask_matrix[ri, :] = 0.0

        ts_labels = hm_df["ts"].dt.strftime("%H:%M:%S").tolist()
        all_nonzero = np.concatenate([bid_matrix[bid_matrix > 0], ask_matrix[ask_matrix > 0]])
        max_vol = np.percentile(all_nonzero, contrast_pctile) if len(all_nonzero) else 1.0

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
            x=ts_labels, y=hm_df["mid_price"].tolist(), mode="lines",
            line=dict(color="white", width=1.5), name="Mid price", hoverinfo="skip",
        ), row=1, col=1)

    # ── Rows 2-4: OBI / CVD / Spread ──────────────────────────────────────
    obi_ts = obi_df["ts"].dt.strftime("%H:%M:%S").tolist()

    # Cumulate cvd_delta to get running CVD
    cvd_running = obi_df["cvd_delta"].cumsum().tolist()

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

    fig.update_layout(
        height=860, template="plotly_dark",
        yaxis_title="Price (USDT)",
        legend=dict(orientation="h", y=-0.06, font=dict(size=10)),
        margin=dict(t=40, b=20),
    )
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=3, col=1)
    fig.update_xaxes(title_text="Time (UTC)", row=4, col=1)

    return fig
