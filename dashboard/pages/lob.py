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
import time
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
from dashboard._db import (
    fetch_lob_snapshots,
    fetch_agg_trades,
    fetch_agg_trade_bin_qtys,
    fetch_microstructure_bars,
    fetch_cvd_series_24h,
    fetch_candles,
    DBOffline,
)
from dashboard._logic import (
    add_event_markers,
    build_walls_figure,
    microstructure_bars_to_events,
    update_event_buffer,
    update_lob_buffer,
)
from dashboard._utils import empty_fig as _empty_fig

dash.register_page(__name__, path="/lob", name="LOB")

_DEPTH_WARNING = settings.LOB_DEPTH < 500
_STALE_THRESHOLD_S = 30  # flag data as stale after 30s without a new snapshot
_SGT = timezone(timedelta(hours=8))
_WS_URL = f"ws://127.0.0.1:{settings.DASHBOARD_API_PORT}/ws/lob"

# Server-side rolling buffer of streamed snapshots (shared across clients, which is
# correct here — the engine pushes identical data to everyone). Sized to the largest
# selectable window (20 min × 60 s) so the slider can widen without losing history.
_MAX_BUFFER = 1200
_BUFFER: list[dict] = []

# Parallel buffer of microstructure events (absorption / sweep) — rendered as markers
# on the heatmap row. Cap is independent of snapshot count: in heavy market activity
# a single 20-min window could see hundreds of events.
_MAX_EVENTS = 2000
_EVENTS: list[dict] = []

# Persisted microstructure events (from the microstructure_bars table), refreshed by
# the events-panel poll and overlaid as heatmap markers by the WS render. Unlike
# _EVENTS (absorption/sweep broadcast over the WS), these survive restarts.
_MS_WINDOW_MIN = 20
_MS_EVENTS: list[dict] = []

# ── Bubble baseline cache ──────────────────────────────────────────────────
# Percentile threshold is derived from 24h of bin-aggregated volumes, not the
# current window, so the slider value is stable across window sizes.
_BUCKET_SIZE: float = settings.LOB_HEATMAP_BUCKET
_BASELINE_BIN_QTYS: np.ndarray = np.array([])
_BASELINE_TS: float = 0.0
_BASELINE_TTL: float = 300.0


def _refresh_baseline_if_stale() -> None:
    """Recompute the 24h bin-aggregated volume distribution (at most every TTL seconds)."""
    global _BASELINE_BIN_QTYS, _BASELINE_TS
    if time.time() - _BASELINE_TS < _BASELINE_TTL:
        return
    since_24h = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
    bin_qtys = fetch_agg_trade_bin_qtys(since_24h, _BUCKET_SIZE, limit=800_000)
    if bin_qtys and not isinstance(bin_qtys, DBOffline):
        _BASELINE_BIN_QTYS = np.asarray(bin_qtys, dtype=np.float64)
        _BASELINE_TS = time.time()


layout = html.Div([
    WebSocket(id="lob-ws", url=_WS_URL),
    dcc.Store(id="lob-tick"),                                # bumps on each new snapshot
    dcc.Interval(id="lob-backfill", interval=500, max_intervals=1),  # one-shot DB seed
    dcc.Interval(id="lob-reseed", interval=60_000),                 # 60s fallback re-seed
    dcc.Interval(id="lob-clock-interval", interval=1_000),          # 1s — SGT clock

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

    # ── View tabs: Microstructure (live LOB) · Walls (candles + wall heatmap) ─
    dbc.Tabs([
        dbc.Tab(label="Microstructure", tab_id="lob-tab-micro", children=html.Div([
            # ── Controls ───────────────────────────────────────────────────
            dbc.Row([
                dbc.Col([
                    dbc.Label("Heatmap window (min)"),
                    dcc.Slider(id="lob-hm-window", min=1, max=20, step=1, value=15,
                               marks={1: "1m", 5: "5m", 10: "10m", 20: "20m"}),
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
                    dbc.Label("Bin volume (pctile)"),
                    dcc.Slider(id="lob-trade-pctile", min=70, max=99, step=1, value=80,
                               marks={70: "70", 80: "80", 90: "90", 99: "99"}),
                ], width=3),
            ], className="mb-3 mt-3"),

            # ── Refresh button ─────────────────────────────────────────────
            dbc.Row([
                dbc.Col(
                    dbc.Button("Refresh Now", id="lob-refresh-btn", color="secondary", size="sm"),
                    width="auto",
                ),
            ], className="mb-3"),

            # ── Chart ──────────────────────────────────────────────────────
            dcc.Graph(id="lob-chart", style={"height": "860px"}),

            # ── Microstructure events panel (from microstructure_bars) ──────
            html.Hr(),
            html.H5(f"Microstructure Events (last {_MS_WINDOW_MIN}m)", className="mb-2"),
            dcc.Interval(id="lob-ms-interval", interval=5000),
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="lob-cvd24-chart", style={"height": "200px"}),
                    dcc.Graph(id="lob-volbars-chart", style={"height": "200px"}),
                ], width=7),
                dbc.Col([
                    html.Div("Event tape (newest first)", className="text-muted small mb-1"),
                    html.Div(id="lob-ms-tape", style={
                        "maxHeight": "400px", "overflowY": "auto",
                        "fontFamily": "monospace", "fontSize": "0.8rem",
                        "backgroundColor": "#1a1d20", "padding": "0.5rem",
                        "borderRadius": "0.25rem", "border": "1px solid #495057",
                    }),
                ], width=5),
            ]),
        ])),

        dbc.Tab(label="Walls", tab_id="lob-tab-walls", children=html.Div([
            dcc.Interval(id="lobw-interval", interval=15000),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Window (min)"),
                    dcc.Slider(id="lobw-window", min=5, max=60, step=5, value=15,
                               marks={5: "5m", 15: "15m", 30: "30m", 60: "60m"}),
                ], width=4),
                dbc.Col([
                    dbc.Label("Price range (±$)"),
                    dcc.Slider(id="lobw-range", min=100, max=2000, step=100, value=500,
                               marks={100: "100", 500: "500", 1000: "1k", 2000: "2k"}),
                ], width=4),
                dbc.Col([
                    dbc.Label("Contrast (pctile)"),
                    dcc.Slider(id="lobw-contrast", min=80, max=99, step=1, value=95,
                               marks={80: "80", 90: "90", 95: "95", 99: "99"}),
                ], width=4),
            ], className="mb-3 mt-3"),
            dcc.Loading(
                dcc.Graph(id="lobw-chart", style={"height": "860px"}),
                type="circle",
            ),
        ])),
    ], id="lob-tabs", active_tab="lob-tab-micro"),
])


def _parse_levels(entry: dict, key: str) -> list:
    """Return bid/ask levels from entry, parsing JSON lazily and caching in-place.

    WS entries already have the key as a list — returned immediately.
    DB-seeded entries have *_json strings instead; parsed on first access and cached
    under the bare key so subsequent renders skip the JSON decode.
    isinstance guard is NaN-safe (pd.DataFrame fills absent keys with float NaN).
    """
    val = entry.get(key)
    if isinstance(val, list):
        return val
    raw = entry.get(key + "_json")
    val = json.loads(raw) if raw else []
    entry[key] = val
    return val


def _levels_to_arrays(entry: dict, key: str) -> tuple[np.ndarray, np.ndarray]:
    levels = _parse_levels(entry, key)
    if not levels:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)
    arr = np.asarray(levels, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[1] < 2:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)
    return arr[:, 0], arr[:, 1]


def _parse_lob_timestamps(values: pd.Series) -> pd.Series:
    """Parse LOB timestamps defensively across DB and live-stream formats.

    Older rows, live WS payloads, and test fixtures can all surface slightly
    different timestamp shapes. Mixed-format parsing avoids a hard failure when
    a single row does not match the first inferred datetime pattern.
    """
    try:
        parsed = pd.to_datetime(values, utc=True, format="mixed", errors="coerce")
    except TypeError:
        parsed = pd.to_datetime(values, utc=True, errors="coerce")
    return parsed


def _build_ts_labels(ts_series: pd.Series) -> tuple[list[str], list[int]]:
    """Return (labels, boundary_indices) for a tz-aware timestamp Series.

    Labels use %H:%M:%S except at the first entry of each new SGT calendar day,
    where the format is "%-d %b\n%H:%M:%S" (e.g. "12 Jun\n00:00:05").
    boundary_indices contains the position of every such day-change label.
    """
    sgt = ts_series.dt.tz_convert(_SGT)
    dates = sgt.dt.date.tolist()
    labels: list[str] = []
    boundaries: list[int] = []
    for i, (d, t) in enumerate(zip(dates, sgt)):
        if i == 0 or d != dates[i - 1]:
            labels.append(t.strftime("%-d %b\n%H:%M:%S"))
            boundaries.append(i)
        else:
            labels.append(t.strftime("%H:%M:%S"))
    return labels, boundaries


def _build_lob_figure(snaps, hm_minutes, half_range, contrast_pctile, trade_pctile, events=None):
    """Build the 4-row LOB figure from a list of snapshot dicts (ascending by ts).

    Each snapshot: {ts, mid_price, spread, obi, cvd_delta, bid_levels, ask_levels}
    where *_levels are [[price, qty], ...] lists.

    ``events``: optional list of {ts, event: "absorption"|"sweep", price, side, ...}.
    Events whose ts falls inside the heatmap window are rendered as markers on row 1.
    """
    snaps = list(snaps)
    rows_needed = hm_minutes * 60
    df = pd.DataFrame(snaps)
    parsed_ts = _parse_lob_timestamps(df["ts"])
    valid_mask = parsed_ts.notna()
    snaps = [snap for snap, ok in zip(snaps, valid_mask) if ok]
    df = df.loc[valid_mask].copy()
    df["ts"] = parsed_ts[valid_mask].to_numpy()

    if df.empty:
        return _empty_fig("Waiting for valid LOB timestamps…")

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

        # hm_snaps_raw is aligned with hm_df by construction (both are the last
        # rows_needed entries of the filtered snapshots. Direct dict iteration avoids the
        # pd.DataFrame NaN-for-missing-keys footgun on DB-seeded entries.
        hm_snaps_raw = snaps[max(0, len(snaps) - rows_needed):]
        # hm_snaps_raw is aligned with hm_df by construction (both are the last
        # rows_needed entries of snaps/df). Direct dict iteration avoids the
        # pd.DataFrame NaN-for-missing-keys footgun on DB-seeded entries.
        hm_snaps_raw = list(snaps)[max(0, len(snaps) - rows_needed):]
        valid_cols = []
        for col_idx, entry_dict in enumerate(hm_snaps_raw):
            col_ok = False
            for levels_key, matrix in (("bid_levels", bid_matrix), ("ask_levels", ask_matrix)):
                try:
                    prices, qtys = _levels_to_arrays(entry_dict, levels_key)
                except Exception:
                    continue
                if len(prices) == 0:
                    col_ok = True
                    continue
                ri = ((prices - price_lo) / bucket_size).astype(np.int64)
                mask = (ri >= 0) & (ri < n_prices)
                if np.any(mask):
                    np.add.at(matrix[:, col_idx], ri[mask], qtys[mask])
                col_ok = True
            valid_cols.append(col_ok)

        sgt_series = hm_df["ts"].dt.tz_convert(_SGT)
        ts_labels, _day_boundaries = _build_ts_labels(hm_df["ts"])
        _boundary_labels = [ts_labels[i] for i in _day_boundaries[1:]]
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
        obi_ts, _ = _build_ts_labels(obi_df["ts"])
        fig.add_trace(go.Scatter(
            x=obi_ts, y=obi_df["obi"].tolist(), mode="lines",
            line=dict(color="cyan", width=1.2), name="OBI",
            fill="tozeroy", fillcolor="rgba(0,200,200,0.15)",
        ), row=2, col=1)
        fig.add_hline(y=settings.OBI_BREAK_THRESH,  line=dict(dash="dot", color="lime", width=1), row=2, col=1)
        fig.add_hline(y=-settings.OBI_BREAK_THRESH, line=dict(dash="dot", color="red",  width=1), row=2, col=1)
        fig.add_hline(y=0, line=dict(color="white", width=0.5), row=2, col=1)

        # CVD from buffer — no DB query; cvd_delta already stored per snapshot
        buf_ts_ms = np.array([int(t.timestamp() * 1000) for t in df["ts"]], dtype=np.int64)
        cvd_cum   = np.cumsum([float(s.get("cvd_delta") or 0.0) for s in snaps])
        obi_ms    = np.array([int(t.timestamp() * 1000) for t in obi_df["ts"]], dtype=np.int64)
        cvd_idx   = np.searchsorted(buf_ts_ms, obi_ms, side="left").clip(0, len(cvd_cum) - 1)
        cvd_y     = cvd_cum[cvd_idx].tolist()

        fig.add_trace(go.Scatter(
            x=obi_ts, y=cvd_y, mode="lines",
            line=dict(color="orange", width=1.2), name="CVD",
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

    # ── Row 1 overlay: market order bubbles ───────────────────────────────
    if not hm_df.empty and ts_labels:
        _refresh_baseline_if_stale()
        threshold = (
            float(np.percentile(_BASELINE_BIN_QTYS, trade_pctile or 80))
            if len(_BASELINE_BIN_QTYS) else 0.0
        )

        snap_ms   = np.array([int(t.timestamp() * 1000) for t in hm_ts_snap], dtype=np.int64)
        win_start = int(snap_ms[0])
        win_end   = int(snap_ms[-1])

        trades = fetch_agg_trades(win_start, until_ts_ms=win_end, limit=10_000)
        buy_x,  buy_y,  buy_sz,  buy_txt  = [], [], [], []
        sell_x, sell_y, sell_sz, sell_txt = [], [], [], []

        if trades and not isinstance(trades, DBOffline):
            tdf = pd.DataFrame(trades)
            if not tdf.empty:
                trade_ms = tdf["ts_event"].to_numpy(dtype=np.int64)
                right = np.searchsorted(snap_ms, trade_ms, side="left")
                right = np.clip(right, 0, len(snap_ms) - 1)
                left = np.clip(right - 1, 0, len(snap_ms) - 1)
                use_left = np.abs(trade_ms - snap_ms[left]) <= np.abs(snap_ms[right] - trade_ms)
                nearest_idx = np.where(use_left, left, right)
                tdf["snapped_x"] = np.asarray(ts_labels, dtype=object)[nearest_idx]
                tdf["price_bkt"] = (tdf["price"] / bucket_size).round() * bucket_size
                tdf["ts_sec"]    = tdf["ts_event"] // 1000

                agg = (
                    tdf.groupby(["snapped_x", "price_bkt", "is_buyer_maker"], as_index=False)
                       .agg(qty=("qty", "sum"), n=("qty", "count"))
                )
                agg = agg[agg["qty"] >= threshold].copy()

                if not agg.empty:
                    sizes     = np.clip(np.log1p(agg["qty"].values) * 6, 5, 24)
                    buy_mask  = agg["is_buyer_maker"].values == 0
                    sell_mask = ~buy_mask
                    buy_x    = agg["snapped_x"].values[buy_mask].tolist()
                    buy_y    = agg["price_bkt"].values[buy_mask].tolist()
                    buy_sz   = sizes[buy_mask].tolist()
                    buy_txt  = [f"qty={q:.4f} n={n}" for q, n in zip(
                                    agg["qty"].values[buy_mask], agg["n"].values[buy_mask])]
                    sell_x   = agg["snapped_x"].values[sell_mask].tolist()
                    sell_y   = agg["price_bkt"].values[sell_mask].tolist()
                    sell_sz  = sizes[sell_mask].tolist()
                    sell_txt = [f"qty={q:.4f} n={n}" for q, n in zip(
                                    agg["qty"].values[sell_mask], agg["n"].values[sell_mask])]

        fig.add_trace(go.Scatter(
            x=buy_x, y=buy_y, mode="markers",
            marker=dict(size=buy_sz or 8, color="rgba(0,220,100,0.8)",
                        line=dict(width=0.5, color="white")),
            name="Buy MO", text=buy_txt,
            hovertemplate="%{text}<extra></extra>",
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=sell_x, y=sell_y, mode="markers",
            marker=dict(size=sell_sz or 8, color="rgba(220,60,60,0.8)",
                        line=dict(width=0.5, color="white")),
            name="Sell MO", text=sell_txt,
            hovertemplate="%{text}<extra></extra>",
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
    if not hm_df.empty:
        fig.update_yaxes(range=[price_lo, price_hi], row=1, col=1)
        unique_dates = sorted({t.date() for t in sgt_series})
        if len(unique_dates) == 1:
            date_str = unique_dates[0].strftime("%-d %b %Y")
        else:
            d0, d1 = unique_dates[0], unique_dates[-1]
            date_str = f"{d0.day}–{d1.strftime('%-d %b %Y')}"
        fig.update_xaxes(title_text=f"Time (SGT)  ·  {date_str}", row=4, col=1)
        for label in _boundary_labels:
            fig.add_vline(x=label, line=dict(color="rgba(255,255,255,0.35)", width=1, dash="dot"))
    else:
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
        seeded.append({
            "ts": r["ts"], "mid_price": r["mid_price"], "spread": r["spread"],
            "obi": r["obi"], "cvd_delta": r["cvd_delta"],
            "bid_levels_json": r.get("bid_levels_json") or "[]",
            "ask_levels_json": r.get("ask_levels_json") or "[]",
        })
    _BUFFER = seeded
    return seeded[-1]["ts"]


@callback(
    Output("lob-tick", "data", allow_duplicate=True),
    Input("lob-backfill", "n_intervals"),
    prevent_initial_call=True,
)
def backfill_buffer(_n):
    """Re-seed snapshots from DB and clear stale events on every page mount.

    Always re-seeding ensures that snapshots accumulated while the page was not
    open (engine writes continuously regardless of dashboard clients) are shown
    immediately rather than waiting for the WS to trickle them in live.

    _EVENTS is cleared because absorption/sweep events are broadcast-only and
    not persisted to any DB table. Stale events from a prior session can sit at
    old price levels after price moves, appearing far from mid. Clearing here
    forces fresh accumulation once the WS reconnects.
    """
    global _EVENTS
    _EVENTS = []
    ts = _seed_buffer_from_db()
    return ts if ts else no_update


@callback(
    Output("lob-tick", "data", allow_duplicate=True),
    Input("lob-reseed", "n_intervals"),
    prevent_initial_call=True,
)
def periodic_reseed(_n):
    """Re-seed buffer from DB every 60s so chart stays fresh during WS outages."""
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
    # Combine WS-broadcast events (_EVENTS) with persisted microstructure_bars events
    # (_MS_EVENTS, refreshed by the events-panel poll). add_event_markers filters both
    # to the heatmap window and ignores "other" kinds, so the overlay stays clean.
    return _build_lob_figure(
        list(_BUFFER), hm_minutes, half_range, contrast_pctile, trade_pctile,
        events=list(_EVENTS) + list(_MS_EVENTS),
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


# ── Microstructure events panel (surfaces the microstructure_bars table) ───────

_SIDE_COLOR = {"bid": "success", "ask": "danger"}


def _fmt_sgt(ts) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(_SGT).strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return str(ts)[11:19]


def _build_ms_tape(events: list[dict]):
    if not events:
        return html.Div("No microstructure events in window.", className="text-muted")
    rows = []
    for ev in reversed(events[-80:]):  # newest first, capped
        price = ev.get("price")
        price_str = f"{price:,.1f}" if isinstance(price, (int, float)) else "—"
        rows.append(html.Div([
            html.Span(_fmt_sgt(ev.get("ts")), className="text-muted me-2"),
            dbc.Badge(ev.get("label", "?"),
                      color=_SIDE_COLOR.get(ev.get("side"), "secondary"), className="me-2"),
            html.Span(price_str, className="text-muted small"),
        ], className="mb-1"))
    return rows


def _build_volbars_fig(rows: list[dict]) -> go.Figure:
    if not rows:
        return _empty_fig("No microstructure bars yet")
    df = pd.DataFrame(rows)
    ts = _parse_lob_timestamps(df["ts"]).dt.tz_convert(_SGT)
    buy = df["buy_volume"].fillna(0).astype(float)
    sell = -df["sell_volume"].fillna(0).astype(float)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=ts, y=buy, marker_color="rgba(0,200,80,0.8)", name="Buy vol"))
    fig.add_trace(go.Bar(x=ts, y=sell, marker_color="rgba(220,60,60,0.8)", name="Sell vol"))
    fig.add_hline(y=0, line=dict(color="white", width=0.5))
    fig.update_layout(
        template="plotly_dark", barmode="relative",
        margin=dict(l=50, r=10, t=28, b=24),
        title=dict(text=f"Buy / Sell volume (last {_MS_WINDOW_MIN}m)", font=dict(size=12)),
        showlegend=False,
    )
    return fig


def _build_cvd24_fig() -> go.Figure:
    since_ms = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
    series = fetch_cvd_series_24h(since_ms)
    if isinstance(series, DBOffline) or not series:
        return _empty_fig("No 24h CVD (agg_trades) yet")
    ts = [datetime.fromtimestamp(r["ts_sec_ms"] / 1000, tz=_SGT) for r in series]
    cvd = np.cumsum([float(r["delta"] or 0.0) for r in series])
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts, y=cvd, mode="lines",
        line=dict(color="orange", width=1.2),
        fill="tozeroy", fillcolor="rgba(255,165,0,0.12)", name="CVD 24h",
    ))
    fig.add_hline(y=0, line=dict(color="white", width=0.5))
    fig.update_layout(
        template="plotly_dark", margin=dict(l=50, r=10, t=28, b=24),
        title=dict(text="CVD — 24h (agg trades)", font=dict(size=12)),
        showlegend=False,
    )
    return fig


@callback(
    Output("lob-ms-tape", "children"),
    Output("lob-volbars-chart", "figure"),
    Output("lob-cvd24-chart", "figure"),
    Input("lob-ms-interval", "n_intervals"),
)
def update_microstructure_panel(_n):
    """Poll microstructure_bars → refresh the tape, volume bars, 24h CVD, and the
    persisted-events buffer (_MS_EVENTS) overlaid as markers by render_lob_chart."""
    global _MS_EVENTS
    rows = fetch_microstructure_bars(minutes=_MS_WINDOW_MIN, limit=2000)
    if isinstance(rows, DBOffline):
        _MS_EVENTS = []
        return (html.Div("DB offline — start the engine.", className="text-muted"),
                _empty_fig("DB offline"), _build_cvd24_fig())
    rows = rows or []
    _MS_EVENTS = microstructure_bars_to_events(rows)
    return _build_ms_tape(_MS_EVENTS), _build_volbars_fig(rows), _build_cvd24_fig()


# ── Walls view (folded in from the former /walls page) ─────────────────────────

@callback(
    Output("lobw-chart", "figure"),
    Input("lobw-interval", "n_intervals"),
    Input("lobw-window", "value"),
    Input("lobw-range", "value"),
    Input("lobw-contrast", "value"),
)
def update_walls_chart(n, window_min, half_range, contrast_pctile):
    candles = fetch_candles(limit=7200)
    snapshots = fetch_lob_snapshots(limit=3600)
    if isinstance(candles, DBOffline) or isinstance(snapshots, DBOffline):
        return _empty_fig("DB offline — start engine first")
    return build_walls_figure(
        candles, snapshots, window_min, half_range, contrast_pctile,
        stale_threshold_s=_STALE_THRESHOLD_S,
    )
