import requests
import dash
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_portfolio_history, fetch_session_stats, fetch_signal_funnel, DBOffline
from dashboard._logic import validate_ks_confirm as _validate_ks, fire_killswitch as _fire_ks
from dashboard._utils import empty_fig as _empty_fig

dash.register_page(__name__, path="/", name="Live", redirect_from=["/live"])

_API_BASE = f"http://127.0.0.1:{settings.DASHBOARD_API_PORT}"

_TIER_COLORS = {
    "ACTIVE": "success",
    "REDUCED": "warning",
    "MINIMAL": "warning",
    "PASSIVE": "danger",
    "HALTED": "danger",
}


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
    dcc.Interval(id="live-interval", interval=5000),

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


@callback(
    Output("live-metrics-row", "children"),
    Output("live-session-row", "children"),
    Output("live-equity-chart", "figure"),
    Output("live-funnel-table", "children"),
    Output("live-positions-table", "children"),
    Output("live-ks-status", "children"),
    Output("live-ks-open-modal", "disabled"),
    Output("live-balance-row", "children"),
    Input("live-interval", "n_intervals"),
    Input("engine-state-store", "data"),
)
def update_live_page(n, engine_state):
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

    equity    = portfolio.get("equity")       if portfolio else None
    daily_pnl = portfolio.get("daily_pnl")   if portfolio else None
    drawdown  = portfolio.get("drawdown_pct") if portfolio else None
    usdt_bal  = portfolio.get("usdt_balance") if portfolio else None
    btc_bal   = portfolio.get("btc_balance")  if portfolio else None
    btc_mtm   = portfolio.get("btc_mtm")      if portfolio else None
    btc_price = portfolio.get("btc_price", 0) if portfolio else 0
    risk_tier = engine_state.get("risk_tier", "—") if engine_state else "—"
    tier_color = _TIER_COLORS.get(risk_tier, "light")

    usdt_str  = f"${usdt_bal:,.2f}"          if isinstance(usdt_bal, (int, float)) else "—"
    btc_str   = f"{btc_bal:.6f} BTC"        if isinstance(btc_bal,  (int, float)) else "—"
    mtm_str   = f"${btc_mtm:,.2f}"          if isinstance(btc_mtm,  (int, float)) and btc_price != 0 else "—"

    balance_row = [
        _metric_card("USDT Balance (at open)", usdt_str, width=4),
        _metric_card("BTC Holdings (at open)", btc_str,  width=4),
        _metric_card("BTC Value (at open)",    mtm_str,  width=4),
    ]

    equity_str = f"${equity:,.2f}"          if isinstance(equity,    (int, float)) else "—"
    pnl_str    = f"${daily_pnl:+,.2f}"      if isinstance(daily_pnl, (int, float)) else "—"
    dd_str     = f"{drawdown * 100:.2f}%"   if isinstance(drawdown,  (int, float)) else "—"
    pnl_color  = "success" if isinstance(daily_pnl, (int, float)) and daily_pnl >= 0 else "danger"
    dd_color   = "success" if isinstance(drawdown,  (int, float)) and drawdown < 0.02 else "warning"

    metrics_row = [
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

    # ── Session stats ──────────────────────────────────────────────────────
    session_result = fetch_session_stats(hours=24)
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

    # ── Signal funnel ──────────────────────────────────────────────────────
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

    return metrics_row, session_row, equity_fig, funnel_table, pos_table, ks_status, ks_disabled, balance_row


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
