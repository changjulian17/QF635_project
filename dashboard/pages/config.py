import requests

import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_system_events

dash.register_page(__name__, path="/config", name="Config")

_API_BASE = f"http://127.0.0.1:{settings.DASHBOARD_API_PORT}"

_SETTINGS_DESCRIPTIONS = {
    "SYMBOL": "Trading symbol",
    "TIMEFRAME": "Candle timeframe for pattern detection",
    "DRY_RUN": "Paper trading mode (no real orders)",
    "STARTING_EQUITY": "Initial portfolio equity (USDT)",
    "MAX_DRAWDOWN_PCT": "Maximum portfolio drawdown threshold",
    "DAILY_LOSS_LIMIT_PCT": "Daily loss limit as % of equity",
    "RISK_PER_TRADE_PCT": "Risk per trade as % of equity",
    "KELLY_FRACTION": "Kelly criterion fraction",
    "ATR_MULTIPLIER_SL": "ATR multiplier for stop-loss",
    "ATR_MULTIPLIER_TP": "ATR multiplier for take-profit",
    "LOB_DEPTH": "LOB depth levels (low < 100 triggers warning)",
    "LOB_HISTORY": "Rolling LOB snapshot retention (rows)",
    "LOB_OBI_DEPTH": "Depth levels used for OBI calculation",
    "OBI_BREAK_THRESH": "OBI threshold for breakout signal",
    "SWEEP_LEVELS": "Number of LOB levels for sweep detection",
    "HEARTBEAT_WARN_MS": "Heartbeat warning threshold (ms)",
    "HEARTBEAT_CRITICAL_MS": "Heartbeat critical threshold (ms)",
    "DASHBOARD_API_PORT": "REST API port",
    "BACKTEST_RESULTS_DB": "Backtest results database path",
    "REGISTRY_DB": "Strategy registry database path",
    "LOB_TICK_DB": "LOB tick data database path",
}

_HIGHLIGHT_FIELDS = {"LOB_DEPTH", "DRY_RUN"}


def _build_settings_table():
    rows = []
    for key, desc in _SETTINGS_DESCRIPTIONS.items():
        value = getattr(settings, key, "—")
        is_warning = (
            (key == "LOB_DEPTH" and isinstance(value, int) and value < 100) or
            (key == "DRY_RUN" and value is True)
        )
        rows.append(html.Tr(
            [html.Td(key), html.Td(str(value)), html.Td(desc)],
            className="table-warning" if is_warning else "",
        ))
    return dbc.Table(
        [
            html.Thead(html.Tr([html.Th("Setting"), html.Th("Value"), html.Th("Description")])),
            html.Tbody(rows),
        ],
        striped=True, bordered=True, hover=True, dark=True, size="sm",
    )


layout = html.Div([
    dcc.Interval(id="cfg-interval", interval=30000),

    # ── Config reference ───────────────────────────────────────────────────
    html.H4("System Configuration", className="mb-3"),
    html.Div([
        dbc.Badge("DRY RUN MODE", color="warning", className="me-2 fs-6") if settings.DRY_RUN
        else dbc.Badge("LIVE TRADING", color="danger", className="me-2 fs-6"),
        dbc.Badge(
            "LOB DEPTH WARNING" if settings.LOB_DEPTH < 100 else f"LOB Depth: {settings.LOB_DEPTH}",
            color="warning" if settings.LOB_DEPTH < 100 else "secondary",
            className="me-2",
        ),
        html.Small(f"API Port: {settings.DASHBOARD_API_PORT}", className="text-muted"),
    ], className="mb-3"),
    _build_settings_table(),

    html.Hr(),

    # ── Emergency stop (same as /live) ─────────────────────────────────────
    html.H4("Emergency Stop", className="text-danger mt-3"),
    html.Div(id="cfg-ks-status"),
    dbc.Button(
        "FIRE KILLSWITCH",
        id="cfg-ks-open-modal",
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
            dbc.Input(id="cfg-ks-confirm-input", placeholder="CONFIRM", type="text"),
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cfg-ks-cancel", color="secondary", className="me-2"),
            dbc.Button("Fire", id="cfg-ks-confirm-btn", color="danger", disabled=True),
        ]),
    ], id="cfg-ks-modal", is_open=False),

    html.Hr(),

    # ── System event log ───────────────────────────────────────────────────
    html.H4("System Event Log", className="mt-3 mb-2"),
    html.Div(id="cfg-event-log"),
])


@callback(
    Output("cfg-ks-status", "children"),
    Output("cfg-ks-open-modal", "disabled"),
    Output("cfg-event-log", "children"),
    Input("cfg-interval", "n_intervals"),
    Input("engine-state-store", "data"),
)
def update_config_page(n, engine_state):
    ks_active = engine_state.get("killswitch_active", False) if engine_state else False
    engine_online = engine_state is not None

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

    events = fetch_system_events(limit=50)
    if events:
        import json
        event_table = dbc.Table([
            html.Thead(html.Tr([html.Th("Timestamp"), html.Th("Event Type"), html.Th("Payload")])),
            html.Tbody([
                html.Tr([
                    html.Td((e.get("occurred_at") or "")[:19]),
                    html.Td(e.get("event_type", "—")),
                    html.Td(
                        str(e.get("payload", ""))[:120],
                        style={"fontFamily": "monospace", "fontSize": "0.8em"},
                    ),
                ])
                for e in events
            ]),
        ], striped=True, bordered=True, hover=True, dark=True, size="sm")
    else:
        event_table = html.P("No system events recorded yet.", className="text-muted")

    return ks_status, ks_disabled, event_table


@callback(
    Output("cfg-ks-modal", "is_open"),
    Input("cfg-ks-open-modal", "n_clicks"),
    Input("cfg-ks-cancel", "n_clicks"),
    Input("cfg-ks-confirm-btn", "n_clicks"),
    State("cfg-ks-confirm-input", "value"),
    State("cfg-ks-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_cfg_ks_modal(open_clicks, cancel_clicks, confirm_clicks, confirm_text, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return is_open
    trigger = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger == "cfg-ks-open-modal":
        return True
    if trigger == "cfg-ks-cancel":
        return False
    if trigger == "cfg-ks-confirm-btn" and confirm_text == "CONFIRM":
        try:
            requests.post(f"{_API_BASE}/api/killswitch", timeout=3)
        except Exception:
            pass
        return False
    return is_open


@callback(
    Output("cfg-ks-confirm-btn", "disabled"),
    Input("cfg-ks-confirm-input", "value"),
)
def validate_cfg_ks_confirm(value):
    return value != "CONFIRM"
