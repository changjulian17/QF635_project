"""DailyBudget — remaining, loss_pct, rebase, and midnight reset boundary."""
from risk.budget import DailyBudget


def _b() -> DailyBudget:
    return DailyBudget.from_equity(10_000.0)   # hard_limit = 100


def test_from_equity_sets_dov_and_hard_limit():
    b = _b()
    assert b.dov == 10_000.0
    assert b.hard_limit == 100.0


def test_remaining_decreases_with_realised_loss():
    b = _b()
    b.realised_pnl = -40.0
    assert b.remaining == 60.0


def test_remaining_includes_unrealised():
    b = _b()
    b.realised_pnl = -30.0
    b.unrealised_pnl = -20.0
    assert b.remaining == 50.0


def test_loss_pct_counts_losses_only():
    b = _b()
    b.realised_pnl = -50.0
    assert b.loss_pct == 0.005
    b.realised_pnl = 50.0          # a gain is not a loss
    assert b.loss_pct == 0.0


def test_rebase_preserves_pnl():
    b = _b()
    b.realised_pnl = -40.0
    b.rebase(20_000.0)
    assert b.dov == 20_000.0
    assert b.hard_limit == 200.0
    assert b.realised_pnl == -40.0      # pnl preserved across intraday rebase


def test_reset_clears_pnl_and_rebases():
    """Midnight reset boundary: a new session zeroes PnL and re-anchors DOV."""
    b = _b()
    b.realised_pnl = -40.0
    b.unrealised_pnl = -10.0
    b.reset(12_000.0)
    assert b.dov == 12_000.0
    assert b.hard_limit == 120.0
    assert b.realised_pnl == 0.0
    assert b.unrealised_pnl == 0.0
    assert b.remaining == 120.0
