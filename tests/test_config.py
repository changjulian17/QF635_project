"""Tests for Settings preset values (config.py _apply_mode_presets)."""
import subprocess
import sys

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


# ── credential-aware validation ───────────────────────────────────────────────
# DRY_RUN (paper trading) places no orders, so it must not require exchange keys —
# tests / fresh clones import config without any private credentials.

# Explicit BINANCE_DEMO/BINANCE_TESTNET + _env_file=None so init-kwargs pin the
# scenario regardless of ambient env / .env.

def test_dry_run_imports_without_demo_keys():
    """BINANCE_DEMO=True + DRY_RUN=True must construct with no demo keys (paper trading)."""
    from config import Settings
    s = Settings(BINANCE_TESTNET=False, BINANCE_DEMO=True, DRY_RUN=True,
                 DEMO_BINANCE_API_KEY="", DEMO_BINANCE_API_SECRET="", _env_file=None)
    assert s.DRY_RUN is True and s.BINANCE_DEMO is True


def test_demo_mode_without_dry_run_still_requires_keys():
    """Real demo trading (DRY_RUN=False) must still demand demo keys."""
    from config import Settings
    with pytest.raises(ValueError, match="DEMO_BINANCE_API"):
        Settings(BINANCE_TESTNET=False, BINANCE_DEMO=True, DRY_RUN=False,
                 DEMO_BINANCE_API_KEY="", DEMO_BINANCE_API_SECRET="", _env_file=None)


def test_tier_ordering_violation_raises_valueerror():
    """Bad tier ordering must raise ValueError (not a stripped-under-O AssertionError)."""
    from config import Settings
    with pytest.raises(ValueError):
        Settings(BINANCE_TESTNET=False, BINANCE_DEMO=False, DRY_RUN=True,
                 TIER_REDUCED_PCT=0.5, TIER_MINIMAL_PCT=0.1, _env_file=None)


@pytest.mark.parametrize(
    "overrides",
    [
        {"MICRO_PRICE_MOVE_WINDOW": 0},
        {"MICRO_PRICE_MOVE_PERCENTILE": 1.0},
        {"MICRO_PRICE_MOVE_MIN_SAMPLES": 0},
        {"PROTECTION_MIN_DISTANCE_BPS": 30.0},
        {"MICRO_MAX_HOLD_MS": 0},
        {"MICRO_EXIT_SPREAD_HARD_CAP_BPS": 0.0},
        {"MIN_SIGNAL_INTERVAL_MS": 300},
    ],
)
def test_runtime_invariant_violations_raise_valueerror(overrides):
    """Runtime config invariants must raise ValueError, never AssertionError."""
    from config import Settings

    kwargs = {
        "BINANCE_TESTNET": False,
        "BINANCE_DEMO": False,
        "DRY_RUN": True,
        "_env_file": None,
        **overrides,
    }
    with pytest.raises(ValueError):
        Settings(**kwargs)


def test_runtime_invariants_hold_under_optimized_python():
    """Config validation must still reject bad values when Python strips asserts."""
    code = (
        "from config import Settings\n"
        "Settings(BINANCE_TESTNET=False, BINANCE_DEMO=False, DRY_RUN=True, "
        "MICRO_MAX_HOLD_MS=0, _env_file=None)\n"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", code],
        cwd=".",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "MICRO_MAX_HOLD_MS" in result.stderr
