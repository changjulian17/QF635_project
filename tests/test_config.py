"""Tests for Settings preset values (config.py _apply_mode_presets)."""
import pytest


def _make_settings(monkeypatch, mode: str):
    """Instantiate a fresh Settings() with a clean env slate for preset-sensitive fields.

    pydantic-settings adds a field to model_fields_set when it is explicitly
    present in the environment or .env file, which causes _apply_mode_presets
    to skip that field.  Clearing TIMEFRAME from the environment ensures the
    preset value is what gets applied, not a stale .env override.

    We call Settings() directly (not importlib.reload) to avoid clobbering the
    module-level singleton that other tests monkeypatch via `config.settings`.
    """
    monkeypatch.delenv("TIMEFRAME",    raising=False)
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.setenv("TRADING_MODE", mode)
    from config import Settings
    return Settings()


def test_demo_preset_timeframe_is_1m(monkeypatch):
    s = _make_settings(monkeypatch, "demo")
    assert s.TIMEFRAME == "1m"


def test_testnet_preset_timeframe_is_1m(monkeypatch):
    s = _make_settings(monkeypatch, "testnet")
    assert s.TIMEFRAME == "1m"


@pytest.mark.parametrize("mode", ["demo", "testnet", "live"])
def test_timeframe_not_1s_in_any_preset(monkeypatch, mode):
    s = _make_settings(monkeypatch, mode)
    assert s.TIMEFRAME != "1s", f"mode={mode!r} still sets TIMEFRAME=1s"
