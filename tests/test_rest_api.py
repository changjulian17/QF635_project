"""Tests for the aiohttp REST API server embedded in main.py."""
import asyncio
import sys
import os

import pytest
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, patch

from models import PortfolioState, SharedState, Position, Direction, CircuitBreakerStatus
from risk.killswitch import GlobalKillswitch
from risk.budget import DailyBudget


# Tests exercise the production app from main.create_api_app (not a copied mini app),
# so REST wiring regressions in main.py are caught here.


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
    from main import create_api_app
    app = create_api_app(killswitch, portfolio, shared_state, order_manager=None, telemetry=None)
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
    # The production handler schedules emergency_close_all as a background task;
    # stub it so the unit test stays on the REST-handler boundary.
    with patch("main.emergency_close_all", new=AsyncMock()):
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


def test_server_binds_localhost_only():
    """REST API source must hardcode 127.0.0.1 — never 0.0.0.0 (no-auth endpoint)."""
    main_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py")
    with open(main_path) as f:
        src = f.read()
    assert "127.0.0.1" in src, "_api_server must bind to 127.0.0.1"
    assert "0.0.0.0" not in src, "_api_server must not bind to 0.0.0.0 (all interfaces)"
