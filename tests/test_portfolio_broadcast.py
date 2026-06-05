"""Tests for the portfolio realtime stream: _build_portfolio_payload + _portfolio_mtm_loop."""
import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fake_portfolio(**overrides):
    """Build a PortfolioState-shaped mock that _build_portfolio_payload can serialise."""
    p = MagicMock()
    p.equity             = overrides.get("equity",             10_000.0)
    p.daily_pnl          = overrides.get("daily_pnl",          -12.34)
    p.drawdown_pct       = overrides.get("drawdown_pct",       0.0012)
    p.consecutive_losses = overrides.get("consecutive_losses", 1)
    p.num_trades         = overrides.get("num_trades",         4)
    p.num_wins           = overrides.get("num_wins",           2)
    p.num_fill_samples   = overrides.get("num_fill_samples",   4)
    p.avg_slippage_bps   = overrides.get("avg_slippage_bps",   1.2)
    p.budget_loss_pct    = overrides.get("budget_loss_pct",    0.05)
    p.positions          = overrides.get("positions",          [])
    return p


# ── _build_portfolio_payload ──────────────────────────────────────────────


def test_build_payload_has_all_rest_fields():
    """The payload must contain every field /api/portfolio's REST consumer expects."""
    from main import _build_portfolio_payload
    payload = _build_portfolio_payload(_fake_portfolio())
    expected_keys = {
        "equity", "daily_pnl", "drawdown_pct", "consecutive_losses",
        "num_trades", "num_wins", "num_fill_samples", "avg_slippage_bps",
        "budget_loss_pct", "positions",
    }
    assert expected_keys.issubset(payload.keys())


def test_build_payload_serialises_positions():
    """Position objects are flattened into dicts (so json.dumps works)."""
    from main import _build_portfolio_payload
    pos = MagicMock()
    pos.symbol         = "BTCUSDT"
    pos.side.name      = "LONG"
    pos.entry_price    = 73_000.0
    pos.quantity       = 0.01
    pos.stop_loss      = 72_500.0
    pos.take_profit    = 74_000.0
    pos.unrealised_pnl = 5.0
    portfolio = _fake_portfolio(positions=[pos])
    payload   = _build_portfolio_payload(portfolio)
    assert payload["positions"] == [{
        "symbol":         "BTCUSDT",
        "side":           "LONG",
        "entry_price":    73_000.0,
        "quantity":       0.01,
        "stop_loss":      72_500.0,
        "take_profit":    74_000.0,
        "unrealised_pnl": 5.0,
    }]


def test_build_payload_omits_risk_tier_when_not_provided():
    """REST callers don't pass risk_tier; it should be absent rather than null."""
    from main import _build_portfolio_payload
    payload = _build_portfolio_payload(_fake_portfolio())
    assert "risk_tier"         not in payload
    assert "killswitch_active" not in payload


def test_build_payload_includes_risk_tier_when_provided():
    """WS broadcast passes risk_tier + killswitch_active so /live cards are self-contained."""
    from main import _build_portfolio_payload
    payload = _build_portfolio_payload(_fake_portfolio(), risk_tier="REDUCED", killswitch_active=False)
    assert payload["risk_tier"]         == "REDUCED"
    assert payload["killswitch_active"] is False


# ── _portfolio_mtm_loop broadcast ─────────────────────────────────────────


def _common_loop_mocks():
    """Build the minimal set of mocks _portfolio_mtm_loop needs for the happy path."""
    killswitch = MagicMock()
    killswitch.is_active   = False
    killswitch.check_budget = MagicMock(return_value=False)
    budget = MagicMock()
    budget.realised_pnl   = 0.0
    budget.unrealised_pnl = 0.0
    order_manager     = MagicMock()
    telemetry         = MagicMock()
    risk_engine       = MagicMock()
    risk_engine.sync_tier = MagicMock(return_value="ACTIVE")
    strategy_executor = MagicMock()
    alert_dispatcher  = MagicMock()
    return killswitch, budget, order_manager, telemetry, risk_engine, strategy_executor, alert_dispatcher


@pytest.mark.asyncio
async def test_portfolio_mtm_loop_broadcasts_typed_payload():
    """Each loop iteration broadcasts {"type": "portfolio", ...} via the supplied hub."""
    from main import _portfolio_mtm_loop

    killswitch, budget, om, telemetry, risk, exe, alerts = _common_loop_mocks()
    portfolio = _fake_portfolio()
    hub = MagicMock()
    hub.broadcast = AsyncMock()

    call_count = 0
    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise asyncio.CancelledError()

    with patch("main.asyncio.sleep", fake_sleep):
        try:
            await _portfolio_mtm_loop(
                killswitch, budget, om, portfolio, telemetry,
                risk, exe, alerts, hub=hub,
            )
        except asyncio.CancelledError:
            pass

    hub.broadcast.assert_awaited_once()
    payload = hub.broadcast.await_args[0][0]
    assert payload["type"]              == "portfolio"
    assert "ts"                          in payload
    assert payload["equity"]            == 10_000.0
    assert payload["risk_tier"]         == "ACTIVE"
    assert payload["killswitch_active"] is False


@pytest.mark.asyncio
async def test_portfolio_mtm_loop_no_hub_runs_normally():
    """Without a hub, the loop runs without crashing — no broadcast attempted."""
    from main import _portfolio_mtm_loop

    killswitch, budget, om, telemetry, risk, exe, alerts = _common_loop_mocks()
    portfolio = _fake_portfolio()

    call_count = 0
    async def fake_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise asyncio.CancelledError()

    with patch("main.asyncio.sleep", fake_sleep):
        try:
            await _portfolio_mtm_loop(
                killswitch, budget, om, portfolio, telemetry,
                risk, exe, alerts, hub=None,
            )
        except asyncio.CancelledError:
            pass
    # Just confirms no crash. sync_tier should still have been called.
    risk.sync_tier.assert_called()


@pytest.mark.asyncio
async def test_portfolio_mtm_loop_skips_broadcast_when_killswitch_active():
    """If killswitch is active at the top of the tick, the loop returns before broadcasting."""
    from main import _portfolio_mtm_loop

    killswitch, budget, om, telemetry, risk, exe, alerts = _common_loop_mocks()
    killswitch.is_active = True   # ← preempts the broadcast
    portfolio = _fake_portfolio()
    hub = MagicMock()
    hub.broadcast = AsyncMock()

    async def fake_sleep(t):
        return  # one sleep, then loop hits the is_active check and returns

    with patch("main.asyncio.sleep", fake_sleep):
        await _portfolio_mtm_loop(
            killswitch, budget, om, portfolio, telemetry,
            risk, exe, alerts, hub=hub,
        )
    hub.broadcast.assert_not_awaited()
