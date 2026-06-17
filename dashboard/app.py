"""
CryptoSentinel Phase 3 Dashboard — entry point.

Usage:
    python dashboard/app.py
"""
import sys
import os

# Add project root to path so config and dashboard modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State

from config import settings
from dashboard._db import fetch_engine_health as _fetch_engine_health

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="CryptoSentinel",
)

_NAV_ITEMS = [
    dbc.NavItem(dbc.NavLink("Live", href="/", active="exact")),
    dbc.NavItem(dbc.NavLink("LOB", href="/lob", active="exact")),
    dbc.NavItem(dbc.NavLink("Strategies", href="/strategies", active="exact")),
    dbc.NavItem(dbc.NavLink("Config", href="/config", active="exact")),
]

_engine_badge = dbc.Badge(
    id="engine-status-badge",
    children="OFFLINE",
    color="secondary",
    className="ms-2 fs-6",
)

navbar = dbc.Navbar(
    dbc.Container([
        dbc.NavbarBrand("CryptoSentinel", href="/"),
        dbc.Nav(_NAV_ITEMS, navbar=True, className="me-auto"),
        html.Div([
            html.Small("Engine:", className="text-muted me-1"),
            _engine_badge,
        ], className="d-flex align-items-center"),
    ]),
    color="dark",
    dark=True,
    sticky="top",
)

app.layout = html.Div([
    dcc.Location(id="url"),
    dcc.Store(id="engine-state-store"),
    dcc.Interval(id="engine-poll-interval", interval=5000),
    navbar,
    dbc.Container(dash.page_container, fluid=True, className="mt-3"),
])


@app.callback(
    Output("engine-state-store", "data"),
    Input("engine-poll-interval", "n_intervals"),
)
def poll_engine_state(n_intervals):
    return _fetch_engine_health()


@app.callback(
    Output("engine-status-badge", "children"),
    Output("engine-status-badge", "color"),
    Input("engine-state-store", "data"),
)
def update_engine_badge(state):
    if state is None:
        return "OFFLINE", "secondary"
    if state.get("killswitch_active"):
        return "KILLSWITCH ACTIVE", "danger"
    return "ONLINE", "success"


if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=8050)
