"""Tests for the walls figure builder (now folded into the /lob page's Walls tab).

The figure logic was extracted into the Dash-free ``dashboard._logic.build_walls_figure``
helper, so these tests import it directly — no ``dash`` stub needed (and therefore no
``sys.modules`` pollution of other test modules).
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard._logic import build_walls_figure  # noqa: E402


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


# ── 1. Empty data ─────────────────────────────────────────────────────────────

def test_walls_empty_data():
    """Empty inputs → 'Waiting for data' annotation, no crash."""
    fig = build_walls_figure([], [], 15, 500, 95)
    texts = [a["text"] for a in fig.layout.annotations]
    assert any("Waiting" in t for t in texts)


# ── 2. Happy path ─────────────────────────────────────────────────────────────

def test_walls_happy_path_trace_count():
    """Valid data → exactly 6 traces in the correct order."""
    fig = build_walls_figure(_make_candles(120), _make_snapshots(120), 15, 500, 95)
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
    fig = build_walls_figure(_make_candles(120), _make_snapshots(120), 15, 500, 95)
    assert fig.layout.height == 860
    assert fig.layout.xaxis.rangeslider.visible is False


# ── 3. valid_cols mask — all-corrupt JSON ─────────────────────────────────────

def test_walls_all_invalid_json_returns_insufficient_data():
    """All-corrupt JSON → valid_cols all-False → sdf empty → 'Insufficient data'."""
    fig = build_walls_figure(
        _make_candles(120), _make_snapshots(120, valid_json=False), 15, 500, 95
    )
    texts = [a["text"] for a in fig.layout.annotations]
    assert any("Insufficient" in t for t in texts)
