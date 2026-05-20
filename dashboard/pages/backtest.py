import json
import os

import dash
from dash import dash_table, dcc, html, Input, Output, callback, no_update
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_backtest_results

dash.register_page(__name__, path="/backtest", name="Backtest")

_LEADERBOARD_COLS = [
    "strategy", "timeframe", "sharpe_ratio", "max_drawdown_pct",
    "profit_factor", "win_rate_pct", "total_trades", "composite_score",
    "passes_minimum_bar",
]

layout = html.Div([
    dcc.Interval(id="bt-interval", interval=30000),
    html.H4("Strategy Leaderboard", className="mb-3"),
    html.Div(id="bt-content"),
    html.Hr(),
    html.H5("Walk-Forward Details", className="mt-3 mb-2"),
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
    Input("bt-interval", "n_intervals"),
)
def update_backtest(n):
    if not os.path.exists(settings.BACKTEST_RESULTS_DB):
        return dbc.Alert(
            "No backtest results yet — run python backtest.py to generate results.",
            color="info",
        )

    results = fetch_backtest_results()
    if not results:
        return dbc.Alert(
            "No results found in backtest database. Run python backtest.py first.",
            color="info",
        )

    strategies = [r for r in results if not r.get("is_benchmark")]
    benchmarks = [r for r in results if r.get("is_benchmark")]

    def make_table(rows, title):
        if not rows:
            return html.Div()
        table_rows = []
        for r in rows:
            sharpe = r.get("sharpe_ratio", 0) or 0
            color = _sharpe_style(float(sharpe))
            table_rows.append(html.Tr([
                html.Td(r.get("strategy", "—")),
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
                striped=True, bordered=True, hover=True, dark=True, size="sm",
            ),
        ])

    return html.Div([
        make_table(strategies, "Strategies"),
        html.Hr(),
        make_table(benchmarks, "Benchmarks"),
    ])
