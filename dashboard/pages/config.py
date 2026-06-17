"""Config page — read-only settings reference.

Engine status chips, the system event log, and the emergency kill switch now live
on the Live page; this page is the static configuration reference only.
"""
import dash
from dash import html
import dash_bootstrap_components as dbc

from config import settings

dash.register_page(__name__, path="/config", name="Config")

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
        striped=True, bordered=True, hover=True, size="sm",
    )


layout = html.Div([
    html.H4("System Configuration", className="mb-3"),
    html.P(
        "Live engine status, the system event log, and the emergency stop are on the Live page.",
        className="text-muted small",
    ),
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
])
