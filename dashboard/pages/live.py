import json

import dash
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket

from config import settings
from dashboard._db import fetch_pnl_by_pattern, fetch_session_stats, fetch_signal_funnel, DBOffline
from dashboard._logic import (
    fire_killswitch as _fire_ks,
    update_portfolio_state,
    validate_ks_confirm as _validate_ks,
)

dash.register_page(__name__, path="/", name="Live", redirect_from=["/live"])

_API_BASE = f"http://127.0.0.1:{settings.DASHBOARD_API_PORT}"
_WS_URL   = f"ws://127.0.0.1:{settings.DASHBOARD_API_PORT}/ws/portfolio"

# Server-side cache of the latest portfolio snapshot (shared across clients — the
# engine pushes identical data to everyone, so a single cache is correct).
_PORTFOLIO: dict = {}

_TIER_COLORS = {
    "ACTIVE": "success",
    "REDUCED": "warning",
    "MINIMAL": "warning",
    "PASSIVE": "danger",
    "HALTED": "danger",
}


def _metric_card(label: str, value: str, color: str = "light") -> dbc.Col:
    return dbc.Col(
        dbc.Card([
            dbc.CardBody([
                html.P(label, className="text-muted mb-1 small"),
                html.H4(value, className=f"text-{color} mb-0"),
            ])
        ], color="dark", outline=True),
        width=3,
    )


layout = html.Div([
    WebSocket(id="live-ws", url=_WS_URL),
    dcc.Store(id="live-portfolio-tick"),                # bumps on each WS push
    dcc.Interval(id="live-interval", interval=5000),    # 5s — drives session stats / funnel

    # ── Header: connection badge ────────────────────────────────────────────
    html.Div([
        html.Span(id="live-conn-status"),
    ], className="d-flex justify-content-end align-items-center mb-2"),

    # ── Portfolio metrics ──────────────────────────────────────────────────
    dbc.Row(id="live-metrics-row", className="mb-3 g-3"),

    # ── Session stats ──────────────────────────────────────────────────────
    html.H5("Session Stats (last 24h)", className="mb-2 mt-1"),
    dbc.Row(id="live-session-row", className="mb-2 g-3"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="live-pnl-pattern-chart", style={"height": "220px"}), width=12),
    ], className="mb-4"),

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


# ── Portfolio section (WS-driven, 1 Hz) ────────────────────────────────────

@callback(
    Output("live-metrics-row", "children"),
    Output("live-positions-table", "children"),
    Input("live-portfolio-tick", "data"),
)
def render_portfolio_section(_tick):
    if not _PORTFOLIO:
        return _placeholder_metrics(), html.P("Waiting for engine…", className="text-muted")
    return _build_metrics_row(_PORTFOLIO), _build_positions_table(_PORTFOLIO)


# ── Session stats + funnel section (5 s interval; 24 h aggregates) ─────────

@callback(
    Output("live-session-row", "children"),
    Output("live-pnl-pattern-chart", "figure"),
    Output("live-funnel-table", "children"),
    Input("live-interval", "n_intervals"),
)
def update_session_section(_n):
    session_result = fetch_session_stats(hours=24)
    if isinstance(session_result, DBOffline) or not session_result:
        session_row = [dbc.Col(dbc.Alert("No session data yet.", color="secondary"), width=12)]
        pnl_fig = go.Figure()
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

        pnl_by_pat = fetch_pnl_by_pattern(hours=24)
        if isinstance(pnl_by_pat, DBOffline) or not pnl_by_pat:
            pnl_fig = go.Figure()
        else:
            labels = list(pnl_by_pat.keys())
            values = list(pnl_by_pat.values())
            colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in values]
            pnl_fig = go.Figure(go.Bar(x=labels, y=values, marker_color=colors))
            pnl_fig.update_layout(
                title="P&L by Signal Type (last 24h)",
                template="plotly_dark",
                margin=dict(l=40, r=20, t=40, b=30),
                yaxis_title="P&L (USDT)",
                showlegend=False,
            )

    funnel_result = fetch_signal_funnel(hours=24)
    if isinstance(funnel_result, DBOffline):
        funnel_table = dbc.Alert("Registry DB offline — start main.py first.", color="secondary")
    elif funnel_result:
        total = sum(r["cnt"] for r in funnel_result) or 1
        funnel_table = dbc.Table([
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
    else:
        funnel_table = html.P("No signal data in last 24h.", className="text-muted")

    return session_row, pnl_fig, funnel_table


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
