"""Tests for /walls page callback: offline, empty, trace structure, invalid-JSON guard.

Dash is not installed in the test environment. We stub it at the module level so
walls.py can be imported. The @callback decorator is replaced with an identity
wrapper so update_walls_chart remains callable as a plain function.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Dash stub ─────────────────────────────────────────────────────────────────
# Must be installed before walls.py is first imported.

def _identity_decorator(*args, **kwargs):
    """Replacement for dash.callback — returns a no-op decorator that keeps the fn."""
    def decorator(fn):
        return fn
    return decorator


_dash_stub = MagicMock()
_dash_stub.callback = _identity_decorator
_dash_stub.register_page = MagicMock()

for _mod_name, _stub in [
    ("dash", _dash_stub),
    ("dash.dcc", MagicMock()),
    ("dash.html", MagicMock()),
    ("dash_bootstrap_components", MagicMock()),
]:
    sys.modules.setdefault(_mod_name, _stub)


# ── Test data helpers ─────────────────────────────────────────────────────────

def _make_candles(n: int, base_price: float = 95_000.0) -> list[dict]:
    now = datetime.now(timezone.utc)
    return [
        {
            "open_time": (now - timedelta(seconds=n - i)).isoformat(),
            "open": base_price, "high": base_price + 10,
            "low": base_price - 10, "close": base_price,
            "volume": 1.0,
        }
        for i in range(n)
    ]


def _make_snapshots(
    n: int,
    base_price: float = 95_000.0,
    valid_json: bool = True,
) -> list[dict]:
    now = datetime.now(timezone.utc)
    # Vary quantities so identify_walls has non-zero std to work with
    bids = json.dumps([[base_price - i * 5, 0.5 + i * 0.1] for i in range(20)])
    asks = json.dumps([[base_price + i * 5, 0.5 + i * 0.1] for i in range(20)])
    return [
        {
            "ts": (now - timedelta(seconds=n - i)).isoformat(),
            "mid_price": base_price,
            "bid_levels_json": bids if valid_json else "corrupt{",
            "ask_levels_json": asks if valid_json else "corrupt{",
        }
        for i in range(n)
    ]


# ── 1. DB offline ─────────────────────────────────────────────────────────────

def test_walls_candles_db_offline():
    """DBOffline from fetch_candles → 'DB offline' annotation, no crash."""
    from dashboard._db import DBOffline
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=DBOffline("no db")), \
         patch("dashboard.pages.walls.fetch_lob_snapshots", return_value=[]):
        fig = update_walls_chart(0, 15, 500, 95)

    texts = [a["text"] for a in fig.layout.annotations]
    assert any("DB offline" in t for t in texts)


def test_walls_snapshots_db_offline():
    """DBOffline from fetch_lob_snapshots → same 'DB offline' message."""
    from dashboard._db import DBOffline
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=[]), \
         patch("dashboard.pages.walls.fetch_lob_snapshots", return_value=DBOffline("no db")):
        fig = update_walls_chart(0, 15, 500, 95)

    texts = [a["text"] for a in fig.layout.annotations]
    assert any("DB offline" in t for t in texts)


# ── 2. Empty data ─────────────────────────────────────────────────────────────

def test_walls_empty_data():
    """Empty fetch results → 'Waiting for data' annotation."""
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=[]), \
         patch("dashboard.pages.walls.fetch_lob_snapshots", return_value=[]):
        fig = update_walls_chart(0, 15, 500, 95)

    texts = [a["text"] for a in fig.layout.annotations]
    assert any("Waiting" in t for t in texts)


# ── 3. Happy path ─────────────────────────────────────────────────────────────

def test_walls_happy_path_trace_count():
    """Valid data → exactly 6 traces in the correct order."""
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=_make_candles(120)), \
         patch("dashboard.pages.walls.fetch_lob_snapshots", return_value=_make_snapshots(120)):
        fig = update_walls_chart(0, 15, 500, 95)

    # Row 1: Candlestick + VWAP  |  Row 2: Bid heatmap + Ask heatmap + Wall heatmap + Mid scatter
    assert len(fig.data) == 6
    assert fig.data[0].type == "candlestick"
    assert fig.data[1].type == "scatter"    # VWAP
    assert fig.data[2].type == "heatmap"    # Bids
    assert fig.data[3].type == "heatmap"    # Asks
    assert fig.data[4].type == "heatmap"    # Wall highlights
    assert fig.data[5].type == "scatter"    # Mid price


def test_walls_happy_path_layout():
    """Valid data → dark template, correct height, rangeslider off."""
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=_make_candles(120)), \
         patch("dashboard.pages.walls.fetch_lob_snapshots", return_value=_make_snapshots(120)):
        fig = update_walls_chart(0, 15, 500, 95)

    assert fig.layout.height == 860
    assert fig.layout.xaxis.rangeslider.visible is False


# ── 4. valid_cols mask — all-corrupt JSON ─────────────────────────────────────

def test_walls_all_invalid_json_returns_insufficient_data():
    """All-corrupt JSON → valid_cols all-False → sdf empty → 'Insufficient data'."""
    from dashboard.pages.walls import update_walls_chart

    with patch("dashboard.pages.walls.fetch_candles", return_value=_make_candles(120)), \
         patch("dashboard.pages.walls.fetch_lob_snapshots",
               return_value=_make_snapshots(120, valid_json=False)):
        fig = update_walls_chart(0, 15, 500, 95)

    texts = [a["text"] for a in fig.layout.annotations]
    assert any("Insufficient" in t for t in texts)
