import json
import os

import dash
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_backtest_results, DBOffline

dash.register_page(__name__, path="/backtest", name="Backtest")

layout = html.Div([
    dcc.Interval(id="bt-interval", interval=30000),
    dcc.Store(id="bt-results-store"),
    html.H4("Strategy Leaderboard", className="mb-3"),
    html.Div(id="bt-content"),
    html.Hr(),
    html.H5("Walk-Forward Details", className="mt-3 mb-2"),
    html.P("Click a strategy row to see full walk-forward metrics.", className="text-muted small"),
    html.Div(id="bt-detail-panel"),
])


def _sharpe_style(sharpe: float) -> str:
    if sharpe >= 1.0:
        return "success"
    if sharpe >= 0.5:
        return "warning"
    return "danger"


@callback(
    Output("bt-content", "children"),
    Output("bt-results-store", "data"),
    Input("bt-interval", "n_intervals"),
)
def update_backtest(n):
    if not os.path.exists(settings.BACKTEST_RESULTS_DB):
        return dbc.Alert(
            "No backtest results yet — run python backtest.py to generate results.",
            color="info",
        ), []

    results = fetch_backtest_results()
    if isinstance(results, DBOffline):
        return dbc.Alert(
            "Backtest DB offline — run python backtest.py first.",
            color="secondary",
        ), []
    if not results:
        return dbc.Alert(
            "No results found in backtest database. Run python backtest.py first.",
            color="info",
        ), []

    strategies = [r for r in results if not r.get("is_benchmark")]
    benchmarks = [r for r in results if r.get("is_benchmark")]

    def make_table(rows, title, offset=0):
        if not rows:
            return html.Div()
        table_rows = []
        for i, r in enumerate(rows):
            sharpe = r.get("sharpe_ratio", 0) or 0
            color = _sharpe_style(float(sharpe))
            table_rows.append(html.Tr([
                html.Td(dbc.Button(
                    r.get("strategy", "—"),
                    id={"type": "bt-select", "index": offset + i},
                    color="link", size="sm", className="p-0 text-start",
                )),
                html.Td(r.get("timeframe", "—")),
                html.Td(dbc.Badge(f"{sharpe:.2f}", color=color)),
                html.Td(f"{(r.get('max_drawdown_pct') or 0) * 100:.1f}%"),
                html.Td(f"{r.get('profit_factor') or 0:.2f}"),
                html.Td(f"{r.get('win_rate_pct') or 0:.1f}%"),
                html.Td(r.get("total_trades", 0)),
                html.Td(f"{r.get('composite_score') or 0:.3f}"),
                html.Td("✅" if r.get("passes_minimum_bar") else "❌"),
            ]))

        return html.Div([
            html.H6(title, className="mb-2"),
            dbc.Table(
                [
                    html.Thead(html.Tr([
                        html.Th("Strategy"), html.Th("Timeframe"), html.Th("Sharpe"),
                        html.Th("Max DD"), html.Th("PF"), html.Th("Win%"),
                        html.Th("Trades"), html.Th("Score"), html.Th("Passes"),
                    ])),
                    html.Tbody(table_rows),
                ],
                striped=True, bordered=True, hover=True, size="sm",
            ),
        ])

    return html.Div([
        make_table(strategies, "Strategies", offset=0),
        html.Hr(),
        make_table(benchmarks, "Benchmarks", offset=len(strategies)),
    ]), results


@callback(
    Output("bt-detail-panel", "children"),
    Input({"type": "bt-select", "index": dash.ALL}, "n_clicks"),
    State("bt-results-store", "data"),
    prevent_initial_call=True,
)
def show_detail(n_clicks, results):
    if not results or not any(n for n in (n_clicks or []) if n):
        return no_update
    ctx = dash.callback_context
    if not ctx.triggered:
        return no_update
    idx = json.loads(ctx.triggered[0]["prop_id"].split(".")[0])["index"]
    if idx >= len(results):
        return no_update
    r = results[idx]

    def _metric(label, value):
        return dbc.Col(html.Div([
            html.Small(label, className="text-muted d-block"),
            html.Strong(value),
        ]), width=3, className="mb-2")

    full_col = dbc.Col([
        html.H6("Full (with risk engine)", className="text-info mb-2"),
        dbc.Row([
            _metric("Sharpe", f"{r.get('sharpe_ratio') or 0:.3f}"),
            _metric("Sortino", f"{r.get('sortino_ratio') or 0:.3f}"),
            _metric("Max DD", f"{(r.get('max_drawdown_pct') or 0) * 100:.1f}%"),
            _metric("Profit Factor", f"{r.get('profit_factor') or 0:.2f}"),
            _metric("Win Rate", f"{r.get('win_rate_pct') or 0:.1f}%"),
            _metric("Total Return", f"{r.get('total_return_pct') or 0:.1f}%"),
            _metric("Trades", str(r.get("total_trades") or 0)),
            _metric("WF Windows", str(r.get("n_wf_windows") or 0)),
        ]),
    ], md=6)

    raw_col = dbc.Col([
        html.H6("Raw (signal-only, no risk engine)", className="text-warning mb-2"),
        dbc.Row([
            _metric("Sharpe", f"{r.get('raw_sharpe') or 0:.3f}"),
            _metric("Return", f"{r.get('raw_return_pct') or 0:.1f}%"),
            _metric("Trades", str(r.get("raw_trades") or 0)),
        ]),
    ], md=6)

    params_section = html.Div()
    if r.get("best_params"):
        try:
            params = json.loads(r["best_params"])
            params_section = html.Div([
                html.Hr(),
                html.H6("Best Optuna Parameters", className="mb-2"),
                html.Pre(
                    json.dumps(params, indent=2),
                    style={"fontSize": "0.8em", "background": "#1a1a2e", "padding": "10px"},
                ),
            ])
        except Exception:
            pass

    return dbc.Card([
        dbc.CardHeader(
            f"{r.get('strategy', '—')} ({r.get('timeframe', '—')}) — "
            f"{'✅ Passes minimum bar' if r.get('passes_minimum_bar') else '❌ Below minimum bar'}"
        ),
        dbc.CardBody([
            dbc.Row([full_col, raw_col]),
            params_section,
        ]),
    ], color="dark", outline=True, className="mt-2")
