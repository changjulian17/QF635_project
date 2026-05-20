import requests
import dash
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_portfolio_history, fetch_signal_funnel
from dashboard._logic import validate_ks_confirm as _validate_ks, fire_killswitch as _fire_ks, decay_badge_color

dash.register_page(__name__, path="/live", name="Live")

_API_BASE = f"http://127.0.0.1:{settings.DASHBOARD_API_PORT}"

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
    dcc.Interval(id="live-interval", interval=5000),

    # ── Portfolio metrics ──────────────────────────────────────────────────
    dbc.Row(id="live-metrics-row", className="mb-3 g-3"),

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
                ]),
                dbc.ModalFooter([
                    dbc.Button("Cancel", id="live-ks-cancel", color="secondary", className="me-2"),
                    dbc.Button("Fire", id="live-ks-confirm-btn", color="danger", disabled=True),
                ]),
            ], id="live-ks-modal", is_open=False),
        ], width=4),
    ]),
])


@callback(
    Output("live-metrics-row", "children"),
    Output("live-funnel-table", "children"),
    Output("live-positions-table", "children"),
    Output("live-ks-status", "children"),
    Output("live-ks-open-modal", "disabled"),
    Input("live-interval", "n_intervals"),
    Input("engine-state-store", "data"),
)
def update_live_page(n, engine_state):
    # ── Engine state ───────────────────────────────────────────────────────
    ks_active = engine_state.get("killswitch_active", False) if engine_state else False
    engine_online = engine_state is not None

    # ── Portfolio via REST ─────────────────────────────────────────────────
    portfolio = None
    try:
        resp = requests.get(f"{_API_BASE}/api/portfolio", timeout=2)
        if resp.ok:
            portfolio = resp.json()
    except Exception:
        pass

    equity = portfolio["equity"] if portfolio else "—"
    daily_pnl = portfolio["daily_pnl"] if portfolio else "—"
    drawdown = portfolio["drawdown_pct"] if portfolio else "—"
    consec = portfolio["consecutive_losses"] if portfolio else "—"
    risk_tier = engine_state.get("risk_tier", "—") if engine_state else "—"
    tier_color = _TIER_COLORS.get(risk_tier, "light")

    equity_str = f"${equity:,.2f}" if isinstance(equity, (int, float)) else equity
    pnl_str = f"${daily_pnl:+,.2f}" if isinstance(daily_pnl, (int, float)) else daily_pnl
    dd_str = f"{drawdown * 100:.2f}%" if isinstance(drawdown, (int, float)) else drawdown

    metrics_row = [
        _metric_card("Equity", equity_str),
        _metric_card("Daily PnL", pnl_str, "success" if isinstance(daily_pnl, float) and daily_pnl >= 0 else "danger"),
        _metric_card("Drawdown", dd_str, "success" if isinstance(drawdown, float) and drawdown < 0.02 else "warning"),
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

    # ── Signal funnel ──────────────────────────────────────────────────────
    funnel_rows = fetch_signal_funnel(hours=24)
    if funnel_rows:
        total = sum(r["cnt"] for r in funnel_rows)
        funnel_table = dbc.Table([
            html.Thead(html.Tr([html.Th("Gate"), html.Th("Count"), html.Th("% of Total")])),
            html.Tbody([
                html.Tr([
                    html.Td(r["gate_passed"]),
                    html.Td(r["cnt"]),
                    html.Td(f"{r['cnt'] / total * 100:.1f}%" if total else "—"),
                ])
                for r in sorted(funnel_rows, key=lambda x: x["gate_passed"])
            ]),
        ], striped=True, bordered=True, hover=True, dark=True, size="sm")
    else:
        funnel_table = html.P("No signal data in last 24h.", className="text-muted")

    # ── Active positions ───────────────────────────────────────────────────
    positions = portfolio.get("positions", []) if portfolio else []
    if positions:
        pos_table = dbc.Table([
            html.Thead(html.Tr([
                html.Th("Side"), html.Th("Entry"), html.Th("Qty"),
                html.Th("SL"), html.Th("TP"), html.Th("Unrealised PnL"),
            ])),
            html.Tbody([
                html.Tr([
                    html.Td(p["side"]),
                    html.Td(f"{p['entry_price']:.2f}"),
                    html.Td(f"{p['quantity']:.6f}"),
                    html.Td(f"{p['stop_loss']:.2f}"),
                    html.Td(f"{p['take_profit']:.2f}"),
                    html.Td(f"{p['unrealised_pnl']:+.4f}"),
                ])
                for p in positions
            ]),
        ], striped=True, bordered=True, hover=True, dark=True, size="sm")
    else:
        pos_table = html.P("No open positions.", className="text-muted")

    # ── Kill switch status ─────────────────────────────────────────────────
    if ks_active:
        ks_status = dbc.Alert(
            "KILLSWITCH ACTIVE — engine restart required to resume trading.",
            color="danger",
        )
        ks_disabled = True
    elif not engine_online:
        ks_status = dbc.Alert("Engine offline — kill switch unavailable.", color="secondary")
        ks_disabled = True
    else:
        ks_status = html.Span()
        ks_disabled = False

    return metrics_row, funnel_table, pos_table, ks_status, ks_disabled


@callback(
    Output("live-ks-modal", "is_open"),
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
        return is_open
    trigger = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger == "live-ks-open-modal":
        return True
    if trigger == "live-ks-cancel":
        return False
    if trigger == "live-ks-confirm-btn" and confirm_text == "CONFIRM":
        _fire_ks(_API_BASE)
        return False
    return is_open


@callback(
    Output("live-ks-confirm-btn", "disabled"),
    Input("live-ks-confirm-input", "value"),
)
def validate_ks_confirm(value):
    return _validate_ks(value)
