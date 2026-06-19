"""KS-1 (budget), KS-2 (heartbeat), KS-3 (slippage) trigger tests."""
from config import settings
from risk.killswitch import GlobalKillswitch


def _ks(dov: float = 10_000.0) -> GlobalKillswitch:
    return GlobalKillswitch(dov)  # hard_limit = dov × 1% = 100


# ── KS-1: daily budget ──────────────────────────────────────────────────────

def test_ks1_no_fire_within_budget():
    ks = _ks()
    assert ks.check_budget(-50.0, -40.0) is False   # -90 > -100
    assert ks.is_active is False


def test_ks1_fires_when_loss_exceeds_hard_limit():
    ks = _ks()
    assert ks.check_budget(-100.0, -1.0) is True     # -101 < -100
    assert ks.is_active is True


def test_ks1_at_exact_limit_does_not_fire():
    ks = _ks()
    assert ks.check_budget(-100.0, 0.0) is False      # strict: -100 is not < -100


def test_ks1_sticky_after_fire():
    ks = _ks()
    ks.check_budget(-200.0, 0.0)
    assert ks.check_budget(0.0, 0.0) is True           # stays fired


# ── KS-2: heartbeat ─────────────────────────────────────────────────────────

def test_ks2_fires_after_consecutive_critical():
    ks = _ks()
    for _ in range(settings.HEARTBEAT_CONSEC_LIMIT - 1):
        assert ks.check_heartbeat("CRITICAL", 600.0) is False
    assert ks.check_heartbeat("CRITICAL", 600.0) is True


def test_ks2_healthy_packet_resets_counter():
    ks = _ks()
    ks.check_heartbeat("CRITICAL", 600.0)
    ks.check_heartbeat("HEALTHY", 50.0)                # resets the streak
    for _ in range(settings.HEARTBEAT_CONSEC_LIMIT - 1):
        assert ks.check_heartbeat("CRITICAL", 600.0) is False
    assert ks.check_heartbeat("CRITICAL", 600.0) is True


# ── KS-3: slippage (window = 20, threshold = 3.0 × 1.5 = 4.5 bps) ────────────

def test_ks3_no_fire_before_window_fills():
    ks = _ks()
    for _ in range(19):                                # 10 bps each, but < 20 samples
        assert ks.record_slippage(100.0, 100.1, "LONG") is False


def test_ks3_fires_when_avg_exceeds_threshold():
    ks = _ks()
    fired = False
    for _ in range(20):
        fired = ks.record_slippage(100.0, 100.1, "LONG")   # +10 bps > 4.5
    assert fired is True


def test_ks3_no_fire_when_avg_below_threshold():
    ks = _ks()
    for _ in range(25):
        ks.record_slippage(100.0, 100.02, "LONG")          # +2 bps < 4.5
    assert ks.is_active is False


def test_ks3_short_side_slippage_sign():
    ks = _ks()
    for _ in range(20):
        ks.record_slippage(100.0, 99.9, "SHORT")           # fill below signal → +10 bps
    assert ks.is_active is True


def test_ks3_ignores_nonpositive_signal_price():
    ks = _ks()
    assert ks.record_slippage(0.0, 1.0, "LONG") is False


# ── update_dov ──────────────────────────────────────────────────────────────

def test_update_dov_rescales_hard_limit():
    ks = _ks()
    ks.update_dov(100_000.0)                            # hard_limit now 1000
    assert ks.check_budget(-150.0, 0.0) is False        # would have fired at dov=10k


# ── T1-B: reset_slippage_buffer ─────────────────────────────────────────────

def test_midnight_reset_clears_slippage_buffer():
    """reset_slippage_buffer() must empty the KS-3 rolling window so yesterday's
    slippage cannot pre-arm the trigger for the next session."""
    ks = _ks()
    for _ in range(19):                                 # fill buffer to 19 of 20
        ks.record_slippage(100.0, 100.1, "LONG")
    assert len(ks._slippage_buf) == 19

    ks.reset_slippage_buffer()

    assert len(ks._slippage_buf) == 0
    # After reset, KS-3 must not fire until a full new window accumulates
    assert ks.record_slippage(100.0, 100.1, "LONG") is False


# ── T2-A: outlier cap ────────────────────────────────────────────────────────

def test_ks3_outlier_capped_before_mean():
    """A single 200 bps outlier fill must be capped at _SLIPPAGE_OUTLIER_CAP_BPS (50)
    before being appended, so it cannot spike the rolling mean past the KS-3 threshold."""
    from risk.killswitch import _SLIPPAGE_OUTLIER_CAP_BPS
    ks = _ks()

    # Fill 19 clean samples at 0 bps (all in the mid).
    for _ in range(19):
        ks.record_slippage(100.0, 100.0, "LONG")   # 0 bps slippage

    # Single 200 bps outlier — without the cap this would push avg to 200/20 = 10 bps
    # which exceeds the 4.5 bps threshold and would fire KS-3.
    # With the cap it is stored as _SLIPPAGE_OUTLIER_CAP_BPS (50), so avg = 50/20 = 2.5 bps < 4.5.
    fired = ks.record_slippage(100.0, 102.00, "LONG")  # 200 bps raw

    # Verify the stored sample was capped
    assert ks._slippage_buf[-1] == _SLIPPAGE_OUTLIER_CAP_BPS, (
        f"expected capped value {_SLIPPAGE_OUTLIER_CAP_BPS}, got {ks._slippage_buf[-1]}"
    )
    # Verify KS-3 did NOT fire (capped avg = 50/20 = 2.5 bps < threshold)
    assert fired is False, "single outlier capped at 50 bps must not trigger KS-3"
    assert ks.is_active is False
