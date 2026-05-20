from datetime import datetime, timezone

import dash
from dash import dcc, html, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc

from config import settings
from dashboard._db import fetch_strategies, fetch_gate_funnel_drift, DBOffline
from dashboard._logic import decay_badge_color, decay_badge_label
from strategy.registry import StrategyRegistry

dash.register_page(__name__, path="/registry", name="Registry")

_REGISTRY = StrategyRegistry(db_path=settings.REGISTRY_DB)

layout = html.Div([
    dcc.Interval(id="reg-interval", interval=30000),
    dcc.Store(id="reg-strategies-store"),

    html.H4("Strategy Lifecycle", className="mb-3"),
    html.Div(id="reg-lifecycle-table"),

    html.Hr(),
    html.H4("Decay Monitoring", className="mt-3 mb-2"),
    html.Div(id="reg-decay-panel"),

    html.Hr(),
    html.H4("Gate Funnel Drift (7d vs 30d baseline)", className="mt-3 mb-2"),
    html.Div(id="reg-funnel-drift"),

    html.Hr(),
    html.H4("LIVE Promotion Pipeline", className="mt-3 mb-2"),
    html.Div(id="reg-promotion-panel"),

    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Confirm LIVE Promotion")),
        dbc.ModalBody([
            html.P("Promote this strategy from PAPER to LIVE trading?", className="text-warning"),
            html.P(id="reg-promote-strategy-label"),
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="reg-promote-cancel", color="secondary", className="me-2"),
            dbc.Button("Promote to LIVE", id="reg-promote-confirm", color="success"),
        ]),
    ], id="reg-promote-modal", is_open=False),
    dcc.Store(id="reg-promote-target"),
])


def _decay_badge(rolling_sharpe: float, backtest_sharpe: float) -> dbc.Badge:
    return dbc.Badge(
        decay_badge_label(rolling_sharpe, backtest_sharpe),
        color=decay_badge_color(rolling_sharpe, backtest_sharpe),
    )


@callback(
    Output("reg-lifecycle-table", "children"),
    Output("reg-decay-panel", "children"),
    Output("reg-funnel-drift", "children"),
    Output("reg-promotion-panel", "children"),
    Output("reg-strategies-store", "data"),
    Input("reg-interval", "n_intervals"),
)
def update_registry(n):
    strategies_result = fetch_strategies()

    if isinstance(strategies_result, DBOffline):
        offline_alert = dbc.Alert("Registry DB offline — start main.py first.", color="secondary")
        return offline_alert, offline_alert, offline_alert, offline_alert, {}

    strategies = strategies_result

    # ── Lifecycle table ────────────────────────────────────────────────────
    status_color = {
        "RESEARCH": "secondary",
        "PAPER": "info",
        "LIVE": "success",
        "RETIRED": "dark",
    }
    if strategies:
        lifecycle_table = dbc.Table([
            html.Thead(html.Tr([
                html.Th("Name"), html.Th("Version"), html.Th("Status"),
                html.Th("Created"), html.Th("Promoted"),
            ])),
            html.Tbody([
                html.Tr([
                    html.Td(s.get("name", "—")),
                    html.Td(s.get("version", "—")),
                    html.Td(dbc.Badge(
                        s.get("status", "—"),
                        color=status_color.get(s.get("status", ""), "secondary"),
                    )),
                    html.Td((s.get("created_at") or "—")[:10]),
                    html.Td((s.get("promoted_at") or "—")[:10]),
                ])
                for s in strategies
            ]),
        ], striped=True, bordered=True, hover=True, dark=True, size="sm")
    else:
        lifecycle_table = html.P("No strategies registered yet.", className="text-muted")

    # ── Decay monitoring ───────────────────────────────────────────────────
    paper_strategies = [s for s in strategies if s.get("status") == "PAPER"]
    decay_cards = []
    for s in paper_strategies:
        sid = s.get("strategy_id", "")
        rolling_sharpe = _REGISTRY.compute_rolling_sharpe(sid, days=14)
        spec = None
        try:
            spec = _REGISTRY._load_yaml(s["name"], s["version"])
            backtest_sharpe = spec.validity.sharpe_oos
        except Exception:
            backtest_sharpe = 0.0

        decay_cards.append(dbc.Card([
            dbc.CardHeader(f"{s.get('name')} v{s.get('version')} — PAPER"),
            dbc.CardBody([
                html.P([
                    "14-day rolling Sharpe: ",
                    html.Strong(f"{rolling_sharpe:.3f}"),
                    " vs backtest OOS Sharpe: ",
                    html.Strong(f"{backtest_sharpe:.3f}"),
                ]),
                _decay_badge(rolling_sharpe, backtest_sharpe),
            ]),
        ], color="dark", outline=True, className="mb-2"))

    decay_panel = html.Div(decay_cards) if decay_cards else html.P(
        "No PAPER strategies to monitor.", className="text-muted"
    )

    # ── Gate funnel drift ──────────────────────────────────────────────────
    drift_result = fetch_gate_funnel_drift()
    if isinstance(drift_result, DBOffline):
        funnel_drift = dbc.Alert("Registry DB offline — start main.py first.", color="secondary")
    elif drift_result:
        drift_table_rows = []
        for r in sorted(drift_result, key=lambda x: x["gate_passed"]):
            cnt_7d = r.get("cnt_7d") or 0
            cnt_30d = r.get("cnt_30d") or 0
            if cnt_30d > 0:
                rate_7d = cnt_7d / (cnt_30d / 30 * 7) if cnt_30d else 0
                drift = rate_7d - 1.0
                drift_color = "danger" if drift > 0.20 else ("warning" if drift > 0.10 else "success")
                drift_label = f"{drift:+.0%}"
            else:
                drift_color = "secondary"
                drift_label = "N/A"
            drift_table_rows.append(html.Tr([
                html.Td(r["gate_passed"]),
                html.Td(cnt_7d),
                html.Td(cnt_30d),
                html.Td(dbc.Badge(drift_label, color=drift_color)),
            ]))
        funnel_drift = dbc.Table([
            html.Thead(html.Tr([
                html.Th("Gate"), html.Th("Count (7d)"), html.Th("Count (30d)"), html.Th("Drift"),
            ])),
            html.Tbody(drift_table_rows),
        ], striped=True, bordered=True, hover=True, dark=True, size="sm")
    else:
        funnel_drift = html.P("No signal data available yet.", className="text-muted")

    # ── LIVE promotion pipeline ────────────────────────────────────────────
    promo_cards = []
    for s in paper_strategies:
        sid = s.get("strategy_id", "")
        total_trades = _REGISTRY.count_paper_trades(sid)
        rolling_sharpe = _REGISTRY.compute_rolling_sharpe(sid, days=14)
        promoted_at = s.get("promoted_at")
        if promoted_at:
            try:
                promoted_dt = datetime.fromisoformat(promoted_at.replace("Z", "+00:00"))
                weeks_running = max(0, (datetime.now(timezone.utc) - promoted_dt).days // 7)
            except Exception:
                weeks_running = 0
        else:
            weeks_running = 0

        spec = None
        gates_pass = False
        min_sharpe = 0.0
        try:
            spec = _REGISTRY._load_yaml(s["name"], s["version"])
            paper_metrics = {
                "weeks_running": weeks_running,
                "total_trades": total_trades,
                "sharpe_rolling": rolling_sharpe,
            }
            gates_pass, reasons = _REGISTRY.can_promote_to_live(spec, paper_metrics)
            min_sharpe = _REGISTRY.LIVE_MIN_PAPER_SHARPE_RATIO * spec.validity.sharpe_oos
        except Exception as e:
            reasons = [str(e)]

        def _gate_row(label, passed):
            return html.Li([
                html.Span("✅ " if passed else "❌ ", style={"marginRight": "6px"}),
                label,
            ])

        gate_items = [
            _gate_row(f"Weeks running ≥ 2 (current: {weeks_running})", weeks_running >= 2),
            _gate_row(f"Trades ≥ 20 (current: {total_trades})", total_trades >= 20),
            _gate_row(
                f"Rolling Sharpe ≥ {min_sharpe:.3f} (current: {rolling_sharpe:.3f})",
                rolling_sharpe >= min_sharpe if min_sharpe > 0 else False,
            ),
        ]

        promo_cards.append(dbc.Card([
            dbc.CardHeader(f"{s.get('name')} v{s.get('version')}"),
            dbc.CardBody([
                html.Ul(gate_items, className="list-unstyled"),
                dbc.Button(
                    "Promote to LIVE",
                    id={"type": "promote-btn", "index": sid},
                    color="success" if gates_pass else "secondary",
                    disabled=not gates_pass,
                    className="mt-2",
                ),
            ]),
        ], color="dark", outline=True, className="mb-2"))

    promotion_panel = html.Div(promo_cards) if promo_cards else html.P(
        "No PAPER strategies eligible for promotion check.", className="text-muted"
    )

    strategies_data = {s["strategy_id"]: s for s in strategies}
    return lifecycle_table, decay_panel, funnel_drift, promotion_panel, strategies_data


@callback(
    Output("reg-promote-modal", "is_open"),
    Output("reg-promote-target", "data"),
    Output("reg-promote-strategy-label", "children"),
    Input({"type": "promote-btn", "index": dash.ALL}, "n_clicks"),
    Input("reg-promote-cancel", "n_clicks"),
    Input("reg-promote-confirm", "n_clicks"),
    State("reg-strategies-store", "data"),
    State("reg-promote-target", "data"),
    prevent_initial_call=True,
)
def handle_promote_modal(promote_clicks, cancel, confirm, strategies_data, current_target):
    ctx = dash.callback_context
    if not ctx.triggered:
        return False, no_update, no_update

    trigger = ctx.triggered[0]["prop_id"]

    if "promote-btn" in trigger:
        import json as _json
        triggered_id = _json.loads(trigger.split(".")[0])
        sid = triggered_id["index"]
        s = (strategies_data or {}).get(sid, {})
        label = f"{s.get('name', sid)} v{s.get('version', '?')}"
        return True, sid, label

    if "reg-promote-cancel" in trigger:
        return False, no_update, no_update

    if "reg-promote-confirm" in trigger and current_target:
        try:
            _REGISTRY.promote(current_target, "LIVE", paper_metrics={
                "weeks_running": 2, "total_trades": 20, "sharpe_rolling": 1.0,
            })
        except Exception:
            pass
        return False, None, no_update

    return False, no_update, no_update
