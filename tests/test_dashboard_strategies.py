"""Phase 1 consolidation tests.

The Backtest + Registry pages were merged into a single /strategies page, and the
Config engine-status chips + system event log were folded into /live. These tests
assert the consolidation registered correctly and the lifted callbacks still exist.

Imports happen at module load (collection time) so they run with the real ``dash``
before any sibling test (e.g. test_dashboard_walls) stubs ``sys.modules["dash"]``.
"""
import sys
import os
import importlib.util

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dashboard.app as _app  # noqa: E402 — triggers page discovery + @callback registration
from dash._callback import GLOBAL_CALLBACK_MAP  # noqa: E402
from dashboard.pages import strategies as _strategies  # noqa: E402
from dashboard.pages import config as _config  # noqa: E402

# Snapshot at import time, before any sys.modules["dash"] stubbing elsewhere.
_PAGE_PATHS = {p["path"] for p in _app.dash.page_registry.values()}
_CALLBACK_KEYS = " ".join(GLOBAL_CALLBACK_MAP.keys())
_STRATEGIES_LAYOUT = str(_strategies.layout)
_CONFIG_LAYOUT = str(_config.layout)


def test_strategies_page_registered_and_old_pages_gone():
    assert "/strategies" in _PAGE_PATHS
    assert "/backtest" not in _PAGE_PATHS
    assert "/registry" not in _PAGE_PATHS
    # The two pages the user lives on, plus the slim Config, remain.
    assert {"/", "/lob", "/config"} <= _PAGE_PATHS


def test_old_page_modules_no_longer_exist():
    for mod in ("dashboard.pages.backtest", "dashboard.pages.registry"):
        assert importlib.util.find_spec(mod) is None, f"{mod} should be deleted"


def test_merged_and_folded_callbacks_registered():
    # Leaderboard (was backtest) + Live & Decay (was registry) callbacks
    for cid in ("bt-content", "bt-detail-panel", "reg-lifecycle-table", "reg-promote-modal"):
        assert cid in _CALLBACK_KEYS, f"missing strategies callback for {cid}"
    # Config status + event log folded into Live
    for cid in ("live-system-meta", "live-system-event-log"):
        assert cid in _CALLBACK_KEYS, f"missing folded system-panel callback for {cid}"


def test_strategies_layout_has_both_tabs():
    assert "Leaderboard" in _STRATEGIES_LAYOUT
    assert "Live & Decay" in _STRATEGIES_LAYOUT


def test_config_page_is_slim_no_killswitch():
    """Slim Config keeps the settings table but drops the kill switch / event log."""
    assert "System Configuration" in _CONFIG_LAYOUT
    assert "cfg-ks-modal" not in _CONFIG_LAYOUT
    assert "cfg-event-log" not in _CONFIG_LAYOUT
