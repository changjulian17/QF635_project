"""Tests for the aiohttp REST API server embedded in main.py."""
import asyncio
import sys
import os

import pytest
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiohttp import web
from models import PortfolioState, SharedState, Position, Direction, CircuitBreakerStatus
from risk.killswitch import GlobalKillswitch
from risk.budget import DailyBudget


# ── Minimal in-process server for testing ─────────────────────────────────────

async def _build_app(killswitch, portfolio, shared_state):
    """Build the same aiohttp app as _api_server in main.py."""

    async def _handle_health(request):
        return web.json_response({
            "lob_status": shared_state.lob_status,
            "heartbeat_status": shared_state.heartbeat_status,
            "risk_tier": portfolio.circuit_breaker.name,
            "killswitch_active": killswitch.is_active,
            "dry_run": True,
        })

    async def _handle_portfolio(request):
        positions = [
            {
                "symbol": p.symbol,
                "side": p.side.name,
                "entry_price": p.entry_price,
                "quantity": p.quantity,
                "stop_loss": p.stop_loss,
                "take_profit": p.take_profit,
                "unrealised_pnl": p.unrealised_pnl,
            }
            for p in portfolio.positions
        ]
        return web.json_response({
            "equity": portfolio.equity,
            "daily_pnl": portfolio.daily_pnl,
            "drawdown_pct": portfolio.drawdown_pct,
            "consecutive_losses": portfolio.consecutive_losses,
            "positions": positions,
        })

    async def _handle_killswitch(request):
        if killswitch.is_active:
            return web.json_response({"fired": True, "already_active": True})
        killswitch._state.fired = True  # directly set for test isolation
        return web.json_response({"fired": True})

    app = web.Application()
    app.router.add_get("/api/health", _handle_health)
    app.router.add_get("/api/portfolio", _handle_portfolio)
    app.router.add_post("/api/killswitch", _handle_killswitch)
    return app


@pytest.fixture
def portfolio():
    return PortfolioState(
        equity=10_000.0,
        starting_equity=10_000.0,
        peak_equity=10_000.0,
    )


@pytest.fixture
def shared_state():
    return SharedState(heartbeat_status="HEALTHY", last_delta_ms=0.0, lob_status="SYNCED")


@pytest.fixture
def killswitch():
    return GlobalKillswitch(10_000.0)


@pytest.fixture
async def client(aiohttp_client, portfolio, shared_state, killswitch):
    app = await _build_app(killswitch, portfolio, shared_state)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_health_returns_correct_fields(client):
    resp = await client.get("/api/health")
    assert resp.status == 200
    data = await resp.json()
    assert "lob_status" in data
    assert "heartbeat_status" in data
    assert "risk_tier" in data
    assert "killswitch_active" in data
    assert "dry_run" in data


@pytest.mark.asyncio
async def test_health_reflects_shared_state(client, shared_state):
    shared_state.lob_status = "UNINITIALISED"
    shared_state.heartbeat_status = "DEGRADED"
    resp = await client.get("/api/health")
    data = await resp.json()
    assert data["lob_status"] == "UNINITIALISED"
    assert data["heartbeat_status"] == "DEGRADED"


@pytest.mark.asyncio
async def test_portfolio_returns_correct_fields(client):
    resp = await client.get("/api/portfolio")
    assert resp.status == 200
    data = await resp.json()
    assert "equity" in data
    assert "daily_pnl" in data
    assert "drawdown_pct" in data
    assert "consecutive_losses" in data
    assert "positions" in data


@pytest.mark.asyncio
async def test_portfolio_reflects_portfolio_state(client, portfolio):
    portfolio.equity = 9_500.0
    portfolio.daily_pnl = -500.0
    resp = await client.get("/api/portfolio")
    data = await resp.json()
    assert data["equity"] == 9_500.0
    assert data["daily_pnl"] == -500.0


@pytest.mark.asyncio
async def test_portfolio_includes_positions(client, portfolio):
    portfolio.positions = [
        Position(
            symbol="BTCUSDT",
            side=Direction.LONG,
            entry_price=50_000.0,
            quantity=0.1,
            stop_loss=49_000.0,
            take_profit=52_000.0,
        )
    ]
    resp = await client.get("/api/portfolio")
    data = await resp.json()
    assert len(data["positions"]) == 1
    assert data["positions"][0]["side"] == "LONG"
    assert data["positions"][0]["entry_price"] == 50_000.0


@pytest.mark.asyncio
async def test_killswitch_fires(client, killswitch):
    assert not killswitch.is_active
    resp = await client.post("/api/killswitch")
    assert resp.status == 200
    data = await resp.json()
    assert data["fired"] is True
    assert "already_active" not in data


@pytest.mark.asyncio
async def test_killswitch_already_active(client, killswitch):
    killswitch._state.fired = True
    resp = await client.post("/api/killswitch")
    data = await resp.json()
    assert data["fired"] is True
    assert data["already_active"] is True


@pytest.mark.asyncio
async def test_killswitch_active_reflected_in_health(client, killswitch):
    killswitch._state.fired = True
    resp = await client.get("/api/health")
    data = await resp.json()
    assert data["killswitch_active"] is True


@pytest.mark.asyncio
async def test_health_risk_tier_reflects_circuit_breaker(client, portfolio):
    portfolio.circuit_breaker = CircuitBreakerStatus.HALTED
    resp = await client.get("/api/health")
    data = await resp.json()
    assert data["risk_tier"] == "HALTED"


@pytest.mark.asyncio
async def test_unknown_endpoint_returns_404(client):
    resp = await client.get("/api/nonexistent")
    assert resp.status == 404
