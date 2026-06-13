import json
import time
import time

import dash
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket

from config import settings
from dashboard._db import fetch_portfolio_history, fetch_session_stats, fetch_signal_funnel, fetch_recent_signals, DBOffline
from dashboard._logic import (
    fire_killswitch as _fire_ks,
    handle_position_event,
    update_portfolio_state,
    update_signal_tape,
    validate_ks_confirm as _validate_ks,
)
from dashboard._utils import empty_fig as _empty_fig

dash.register_page(__name__, path="/", name="Live", redirect_from=["/live"])

_API_BASE       = f"http://127.0.0.1:{settings.DASHBOARD_API_PORT}"
_WS_URL         = f"ws://127.0.0.1:{settings.DASHBOARD_API_PORT}/ws/portfolio"
_SIGNALS_WS_URL = f"ws://127.0.0.1:{settings.DASHBOARD_API_PORT}/ws/signals"

# Server-side cache of the latest portfolio snapshot (shared across clients — the
# engine pushes identical data to everyone, so a single cache is correct).
_PORTFOLIO: dict = {}

# Server-side rolling tape of recent signal events (newest first). Shared across
# clients — the engine pushes the same data to everyone.
_TAPE_MAX = 50
_TAPE: list[dict] = []

# Latest position event from the user data stream (position_opened / close_event).
# Cleared to {} on close_event; takes precedence over the 1 Hz MTM snapshot.
_LAST_POSITION_EVENT: dict = {}

# 30-second TTL cache for the two 24-hour aggregate queries that run at 5-second poll cadence.
_STATS_CACHE: dict | list | None = None
_STATS_CACHE_TS: float = 0.0
_FUNNEL_CACHE: list | None = None
_FUNNEL_CACHE_TS: float = 0.0
_LIVE_STATS_TTL: float = 30.0

_TIER_COLORS = {
    "ACTIVE": "success",
    "REDUCED": "warning",
    "MINIMAL": "warning",
    "PASSIVE": "danger",
    "HALTED": "danger",
}


def _gate_color(gate_passed: str) -> str:
    """Bootstrap color for the live-tape badge — green when approved, red when
    rejected early, yellow when killed in the middle of the pipeline."""
    if gate_passed == "APPROVED":
        return "success"
    if gate_passed.startswith("GATE_0") or gate_passed.startswith("GATE_1"):
        return "danger"   # rejected on the cheap gates → noisy / common
    return "warning"      # rejected mid-pipeline → notable


def _metric_card(label: str, value: str, color: str = "light", width: int = 3) -> dbc.Col:
    return dbc.Col(
        dbc.Card([
            dbc.CardBody([
                html.P(label, className="text-muted mb-1 small"),
                html.H4(value, className=f"text-{color} mb-0"),
            ])
        ], color="dark", outline=True),
        width=width,
    )


layout = html.Div([
    WebSocket(id="live-ws", url=_WS_URL),
    WebSocket(id="live-signals-ws", url=_SIGNALS_WS_URL),
    dcc.Store(id="live-portfolio-tick"),                # bumps on each portfolio WS push
    dcc.Store(id="live-signals-tick"),                  # bumps on each new signal event
    dcc.Store(id="live-position-event-tick"),           # bumps on position_opened/close_event
    dcc.Interval(id="live-interval", interval=5000),    # 5s — drives session stats / funnel fallback
    dcc.Interval(id="live-tape-init", interval=500, max_intervals=1),  # fires once to seed tape from DB

    # ── Header: connection badge ────────────────────────────────────────────
    html.Div([
        html.Span(id="live-conn-status"),
    ], className="d-flex justify-content-end align-items-center mb-2"),

    # ── Portfolio metrics ──────────────────────────────────────────────────
    dbc.Row(id="live-metrics-row", className="mb-2 g-3"),
    html.Small("Account Balances (session open)", className="text-muted ms-1"),
    dbc.Row(id="live-balance-row", className="mb-3 g-3"),

    # ── Session stats ──────────────────────────────────────────────────────
    html.H5("Session Stats (last 24h)", className="mb-2 mt-1"),
    dbc.Row(id="live-session-row", className="mb-3 g-3"),

    # ── Equity curve ──────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col(dcc.Graph(id="live-equity-chart", style={"height": "220px"}), width=12),
    ], className="mb-4"),

    # ── Live signal tape ───────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H5("Live Signal Tape", className="mb-0 d-inline-block me-2"),
                html.Span(id="live-signals-conn-status", className="d-inline-block"),
            ], className="mb-2"),
            html.Div(id="live-signal-tape", style={
                "maxHeight": "240px", "overflowY": "auto",
                "fontFamily": "monospace", "fontSize": "0.85rem",
                "backgroundColor": "#1a1d20", "padding": "0.5rem",
                "borderRadius": "0.25rem", "border": "1px solid #495057",
            }),
        ], width=12),
    ], className="mb-3"),

    # ── Signal funnel ──────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            html.H5("Signal Funnel (last 24h)", className="mb-2"),
            html.Div(id="live-funnel-table"),
        ], width=6),

        # ── Active positions ─────────────────────────────────────────────
        dbc.Col([
            html.H5("Active Positions", className="mb-2"),
            html.Div(id="live-positions-table"),
        ], width=6),
    ], className="mb-4"),

    # ── Kill switch ────────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            html.H5("Emergency Stop", className="text-danger"),
            html.Div(id="live-ks-status"),
            dbc.Button(
                "FIRE KILLSWITCH",
                id="live-ks-open-modal",
                color="danger",
                size="lg",
                className="mt-2",
            ),
            dbc.Modal([
                dbc.ModalHeader(dbc.ModalTitle("Confirm Emergency Stop")),
                dbc.ModalBody([
                    html.P(
                        "This will immediately close all positions and halt the engine. "
                        "A process restart is required to resume trading.",
                        className="text-warning",
                    ),
                    dbc.Label("Type CONFIRM to proceed:"),
                    dbc.Input(id="live-ks-confirm-input", placeholder="CONFIRM", type="text"),
                    html.Div(id="live-ks-fire-error", className="mt-2"),
                ]),
                dbc.ModalFooter([
                    dbc.Button("Cancel", id="live-ks-cancel", color="secondary", className="me-2"),
                    dbc.Button("Fire", id="live-ks-confirm-btn", color="danger", disabled=True),
                ]),
            ], id="live-ks-modal", is_open=False),
        ], width=4),
    ]),
])


# ── Render helpers ─────────────────────────────────────────────────────────

def _placeholder_metrics() -> list:
    return [_metric_card(label, "—") for label in ("Equity", "Daily PnL", "Drawdown", "Risk Tier")]


def _build_metrics_row(portfolio: dict) -> list:
    equity    = portfolio.get("equity")
    daily_pnl = portfolio.get("daily_pnl")
    drawdown  = portfolio.get("drawdown_pct")
    risk_tier = portfolio.get("risk_tier") or "—"
    tier_color = _TIER_COLORS.get(risk_tier, "light")

    equity_str = f"${equity:,.2f}"        if isinstance(equity,    (int, float)) else "—"
    pnl_str    = f"${daily_pnl:+,.2f}"    if isinstance(daily_pnl, (int, float)) else "—"
    dd_str     = f"{drawdown * 100:.2f}%" if isinstance(drawdown,  (int, float)) else "—"
    pnl_color  = "success" if isinstance(daily_pnl, (int, float)) and daily_pnl >= 0 else "danger"
    dd_color   = "success" if isinstance(drawdown,  (int, float)) and drawdown < 0.02 else "warning"

    return [
        _metric_card("Equity", equity_str),
        _metric_card("Daily PnL", pnl_str, pnl_color),
        _metric_card("Drawdown", dd_str, dd_color),
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.P("Risk Tier", className="text-muted mb-1 small"),
                    dbc.Badge(risk_tier, color=tier_color, className="fs-6"),
                ])
            ], color="dark", outline=True),
            width=3,
        ),
    ]


def _build_balance_row(portfolio: dict) -> list:
    usdt_bal  = portfolio.get("usdt_balance")
    btc_bal   = portfolio.get("btc_balance")
    btc_mtm   = portfolio.get("btc_mtm")
    btc_price = portfolio.get("btc_price", 0)

    usdt_str  = f"${usdt_bal:,.2f}"   if isinstance(usdt_bal, (int, float)) else "—"
    btc_str   = f"{btc_bal:.6f} BTC"  if isinstance(btc_bal,  (int, float)) else "—"
    mtm_str   = f"${btc_mtm:,.2f}"    if isinstance(btc_mtm,  (int, float)) and btc_price != 0 else "—"

    return [
        _metric_card("USDT Balance (at open)", usdt_str, width=4),
        _metric_card("BTC Holdings (at open)", btc_str,  width=4),
        _metric_card("BTC Value (at open)",    mtm_str,  width=4),
    ]


def _build_positions_table(portfolio: dict):
    positions = portfolio.get("positions", []) or []
    if not positions:
        return html.P("No open positions.", className="text-muted")
    return dbc.Table([
        html.Thead(html.Tr([
            html.Th("Side"), html.Th("Entry"), html.Th("Qty"),
            html.Th("SL"), html.Th("TP"), html.Th("Unrealised PnL"),
        ])),
        html.Tbody([
            html.Tr([
                html.Td(p.get("side", "—")),
                html.Td(f"{p['entry_price']:.2f}" if p.get("entry_price") is not None else "—"),
                html.Td(f"{p['quantity']:.6f}"    if p.get("quantity")    is not None else "—"),
                html.Td(f"{p['stop_loss']:.2f}"   if p.get("stop_loss")   is not None else "—"),
                html.Td(f"{p['take_profit']:.2f}" if p.get("take_profit") is not None else "—"),
                html.Td(f"{p['unrealised_pnl']:+.4f}" if p.get("unrealised_pnl") is not None else "—"),
            ])
            for p in positions
        ]),
    ], striped=True, bordered=True, hover=True, size="sm")


def _build_funnel_table(funnel_result):
    if isinstance(funnel_result, DBOffline):
        return dbc.Alert("Registry DB offline — start main.py first.", color="secondary")
    if not funnel_result:
        return html.P("No signal data in last 24h.", className="text-muted")
    total = sum(r["cnt"] for r in funnel_result) or 1
    return dbc.Table([
        html.Thead(html.Tr([html.Th("Gate"), html.Th("Count"), html.Th("% of Total")])),
        html.Tbody([
            html.Tr([
                html.Td(r["gate_passed"]),
                html.Td(r["cnt"]),
                html.Td(f"{r['cnt'] / total * 100:.1f}%"),
            ])
            for r in sorted(funnel_result, key=lambda x: x["gate_passed"])
        ]),
    ], striped=True, bordered=True, hover=True, size="sm")


# ── WS routing: append portfolio snapshot to the cache ─────────────────────

@callback(
    Output("live-portfolio-tick", "data"),
    Input("live-ws", "message"),
    prevent_initial_call=True,
)
def on_ws_message(message):
    """Route one pushed portfolio payload to the server-side cache; return ts to trigger redraw."""
    global _PORTFOLIO
    if not message or "data" not in message:
        return no_update
    try:
        msg = json.loads(message["data"])
    except (TypeError, ValueError):
        return no_update
    _PORTFOLIO = update_portfolio_state(_PORTFOLIO, msg)
    return msg.get("ts", no_update)


# ── WS routing: position_opened / close_event ──────────────────────────────

@callback(
    Output("live-position-event-tick", "data"),
    Input("live-ws", "message"),
    prevent_initial_call=True,
)
def on_ws_position_event(message):
    global _LAST_POSITION_EVENT
    if not message or "data" not in message:
        return no_update
    try:
        msg = json.loads(message["data"])
    except (TypeError, ValueError):
        return no_update
    if msg.get("type") not in ("position_opened", "close_event"):
        return no_update
    _LAST_POSITION_EVENT = handle_position_event(_LAST_POSITION_EVENT, msg)
    return msg.get("ts", no_update)


# ── Portfolio section (WS-driven, 1 Hz) ────────────────────────────────────

@callback(
    Output("live-metrics-row", "children"),
    Output("live-balance-row", "children"),
    Input("live-portfolio-tick", "data"),
    Input("live-interval", "n_intervals"),
)
def render_portfolio_section(_tick, _n):
    if not _PORTFOLIO:
        history = fetch_portfolio_history(limit=1)
        if history and not isinstance(history, DBOffline):
            last = history[-1]
            fallback = {
                "equity":       last.get("equity"),
                "daily_pnl":    last.get("daily_pnl"),
                "drawdown_pct": last.get("drawdown_pct"),
            }
            return (_build_metrics_row(fallback), _build_balance_row({}))
        return (_placeholder_metrics(), _build_balance_row({}))
    return (
        _build_metrics_row(_PORTFOLIO),
        _build_balance_row(_PORTFOLIO),
    )


# ── Active positions (sole owner of live-positions-table) ──────────────────

@callback(
    Output("live-positions-table", "children"),
    Input("live-portfolio-tick", "data"),
    Input("live-position-event-tick", "data"),
)
def render_positions(_portfolio_tick, _event_tick):
    if _LAST_POSITION_EVENT:
        return _build_positions_table({"positions": [_LAST_POSITION_EVENT]})
    return _build_positions_table(_PORTFOLIO)


# ── Session stats + equity curve (5 s interval; 24 h aggregates) ───────────

@callback(
    Output("live-session-row", "children"),
    Output("live-equity-chart", "figure"),
    Input("live-interval", "n_intervals"),
)
def update_session_section(_n):
    global _STATS_CACHE, _STATS_CACHE_TS
    now = time.time()
    if _STATS_CACHE is None or (now - _STATS_CACHE_TS) >= _LIVE_STATS_TTL:
        _STATS_CACHE = fetch_session_stats(hours=24)
        _STATS_CACHE_TS = now
    session_result = _STATS_CACHE
    if isinstance(session_result, DBOffline) or not session_result:
        session_row = [dbc.Col(dbc.Alert("No session data yet.", color="secondary"), width=12)]
    else:
        s = session_result
        total_t = s.get("total_trades") or 0
        wins    = s.get("wins") or 0
        win_rate  = wins / total_t if total_t > 0 else 0.0
        pf        = s.get("profit_factor")
        avg_r     = s.get("avg_r_multiple")
        avg_dur   = s.get("avg_duration_min")
        avg_slip  = s.get("avg_slippage_bps")
        losses    = total_t - wins

        session_row = [
            _metric_card("Trades", f"{total_t}  ({wins}W / {losses}L)"),
            _metric_card("Win Rate", f"{win_rate * 100:.1f}%",
                         "success" if win_rate >= 0.5 else "warning"),
            _metric_card("Profit Factor", f"{pf:.2f}" if pf is not None else "—",
                         "success" if (pf or 0) >= 1.3 else "warning"),
            _metric_card("Avg R-Multiple", f"{avg_r:.2f}R" if avg_r is not None else "—"),
            _metric_card("Avg Duration", f"{avg_dur:.1f} min" if avg_dur is not None else "—"),
            _metric_card("Avg Slippage", f"{avg_slip:.1f} bps" if avg_slip is not None else "—"),
        ]

    # ── Equity curve ──────────────────────────────────────────────────────
    history = fetch_portfolio_history(limit=720)
    if isinstance(history, DBOffline) or not history:
        equity_fig = _empty_fig("No portfolio history — start main.py to begin recording.")
    else:
        ts_vals  = [r["ts"] for r in history]
        eq_vals  = [r["equity"] if r["equity"] is not None else 0.0 for r in history]
        baseline = eq_vals[0] or 0.0  # guard: equity column is REAL (nullable)
        equity_fig = go.Figure()
        equity_fig.add_trace(go.Scatter(
            x=ts_vals, y=eq_vals, mode="lines",
            line=dict(color="cyan", width=1.5),
            fill="tozeroy", fillcolor="rgba(0,200,200,0.15)",
            hovertemplate="%{x|%H:%M:%S}<br>$%{y:,.2f}<extra></extra>",
        ))
        equity_fig.add_hline(
            y=baseline,
            line=dict(color="rgba(255,255,255,0.4)", width=0.8, dash="dot"),
            annotation_text=f"Open  ${baseline:,.0f}",
            annotation_position="top left",
            annotation_font=dict(size=10, color="rgba(255,255,255,0.5)"),
        )
        equity_fig.update_layout(
            title=dict(text="Equity Curve", font=dict(size=13)),
            template="plotly_dark",
            margin=dict(l=70, r=20, t=35, b=30),
            yaxis=dict(title="Equity (USDT)", tickformat="$,.0f"),
            xaxis=dict(title=None),
            showlegend=False,
        )

    return session_row, equity_fig


# ── Signal tape + funnel (push-driven from /ws/signals) ────────────────────

@callback(
    Output("live-signals-tick", "data"),
    Input("live-signals-ws", "message"),
    prevent_initial_call=True,
)
def on_signal_ws_message(message):
    """Append a streamed signal event to the tape; return ts to trigger downstream redraws."""
    global _TAPE
    if not message or "data" not in message:
        return no_update
    try:
        msg = json.loads(message["data"])
    except (TypeError, ValueError):
        return no_update
    _TAPE = update_signal_tape(_TAPE, msg, _TAPE_MAX)
    return msg.get("ts", no_update)


@callback(
    Output("live-signals-tick", "data", allow_duplicate=True),
    Input("live-tape-init", "n_intervals"),
    prevent_initial_call=True,
)
def _hydrate_tape_from_db(_n):
    """Seed the tape from DB on page load so history is visible before the next WS push."""
    global _TAPE
    if _TAPE:
        return no_update
    rows = fetch_recent_signals(limit=_TAPE_MAX)
    if isinstance(rows, DBOffline) or not rows:
        return no_update
    _TAPE = rows  # already newest-first from ORDER BY DESC
    return rows[0].get("ts", no_update)


@callback(
    Output("live-signal-tape", "children"),
    Input("live-signals-tick", "data"),
)
def render_signal_tape(_tick):
    if not _TAPE:
        return html.Div("Waiting for signal events… (start the engine to populate)",
                        className="text-muted")
    rows = []
    for ev in _TAPE:
        gate      = ev.get("gate_passed", "?")
        direction = ev.get("direction") or "—"
        # ISO timestamp like 2026-06-01T17:10:30.123+00:00 → take HH:MM:SS
        ts        = ev.get("ts", "")
        ts_short  = ts[11:19] if len(ts) >= 19 else ts
        reason    = ev.get("rejection_reason") or ""
        confidence = ev.get("confidence")
        conf_str  = f"conf={confidence:.2f} " if isinstance(confidence, (int, float)) and confidence else ""
        rows.append(html.Div([
            html.Span(ts_short, className="text-muted me-2"),
            dbc.Badge(direction, color="info", className="me-2", style={"minWidth": "60px"}),
            dbc.Badge(gate, color=_gate_color(gate), className="me-2"),
            html.Span(conf_str + reason, className="text-muted small"),
        ], className="mb-1"))
    return rows


@callback(
    Output("live-funnel-table", "children"),
    Input("live-signals-tick", "data"),       # push: refresh on each new event
    Input("live-interval", "n_intervals"),     # fallback: 5 s poll if WS is offline
)
def refresh_funnel_table(_tick, _n):
    global _FUNNEL_CACHE, _FUNNEL_CACHE_TS
    now = time.time()
    if _FUNNEL_CACHE is None or (now - _FUNNEL_CACHE_TS) >= _LIVE_STATS_TTL:
        _FUNNEL_CACHE = fetch_signal_funnel(hours=24)
        _FUNNEL_CACHE_TS = now
    return _build_funnel_table(_FUNNEL_CACHE)


@callback(
    Output("live-signals-conn-status", "children"),
    Input("live-signals-ws", "state"),
)
def update_signals_conn(state):
    ready = (state or {}).get("readyState")
    if ready == 1:
        return dbc.Badge("● LIVE", color="success")
    if ready == 0:
        return dbc.Badge("● Connecting…", color="warning")
    return dbc.Badge("● Engine offline", color="secondary")


# ── Killswitch status (engine-state-driven) ────────────────────────────────

@callback(
    Output("live-ks-status", "children"),
    Output("live-ks-open-modal", "disabled"),
    Input("engine-state-store", "data"),
)
def update_ks_status(engine_state):
    ks_active     = engine_state.get("killswitch_active", False) if engine_state else False
    engine_online = engine_state is not None
    if ks_active:
        return dbc.Alert(
            "KILLSWITCH ACTIVE — engine restart required to resume trading.", color="danger",
        ), True
    if not engine_online:
        return dbc.Alert("Engine offline — kill switch unavailable.", color="secondary"), True
    return html.Span(), False


# ── WS connection badge ────────────────────────────────────────────────────

@callback(
    Output("live-conn-status", "children"),
    Input("live-ws", "state"),
)
def update_conn_status(state):
    ready = (state or {}).get("readyState")
    if ready == 1:
        return dbc.Badge("● LIVE", color="success", className="fs-6")
    if ready == 0:
        return dbc.Badge("● Connecting…", color="warning", className="fs-6")
    return dbc.Badge("● Engine offline", color="secondary", className="fs-6")


@callback(
    Output("live-ks-modal", "is_open"),
    Output("live-ks-confirm-input", "value"),   # C3: always clear input on close
    Output("live-ks-fire-error", "children"),   # C4: surface POST failures
    Input("live-ks-open-modal", "n_clicks"),
    Input("live-ks-cancel", "n_clicks"),
    Input("live-ks-confirm-btn", "n_clicks"),
    State("live-ks-confirm-input", "value"),
    State("live-ks-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_ks_modal(open_clicks, cancel_clicks, confirm_clicks, confirm_text, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return is_open, no_update, no_update
    trigger = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger == "live-ks-open-modal":
        return True, "", no_update
    if trigger == "live-ks-cancel":
        return False, "", no_update
    if trigger == "live-ks-confirm-btn" and confirm_text == "CONFIRM":
        success = _fire_ks(_API_BASE)
        if not success:
            return True, "", dbc.Alert(
                "Failed to reach engine — check that main.py is running.", color="danger"
            )
        return False, "", no_update
    return is_open, no_update, no_update


@callback(
    Output("live-ks-confirm-btn", "disabled"),
    Input("live-ks-confirm-input", "value"),
)
def validate_ks_confirm(value):
    return _validate_ks(value)
