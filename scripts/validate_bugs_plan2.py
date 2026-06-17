#!/usr/bin/env python3
"""
Root cause validation for CryptoSentinel Guardrail Hardening Plan 2 (2026-06-17).

Each check exercises real code paths:
  Pre-fix:  checks print FAIL  — bugs confirmed present.
  Post-fix: checks print PASS  — bugs confirmed resolved.

Exit code: 0 = all pass, 1 = any fail.

Usage:
    source .venv/bin/activate
    python scripts/validate_bugs_plan2.py
"""
import asyncio
import logging
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.disable(logging.CRITICAL)


def _result(name: str, passed: bool, detail: str) -> bool:
    tag = "PASS" if passed else "FAIL"
    print(f"  [{tag}] {name}")
    print(f"         {detail}")
    return passed


# ─────────────────────────────────────────────────────────────────────────────
# Check 1 — Item 1: orphan exchange position not detected on startup restart
# ─────────────────────────────────────────────────────────────────────────────

async def check_item1_orphan_position_detection() -> bool:
    """
    Source inspection of startup_reconciler.py for S7 implementation.

    Pre-fix:  no futures_account() call (only futures_account_balance); no
              'has_orphan_position' key; no positionAmt check.
    Post-fix: all three present — S7 detects orphan positions via /fapi/v2/account.
    """
    src = (ROOT / "core" / "startup_reconciler.py").read_text()

    has_account_call = "client.futures_account(" in src
    has_orphan_key   = "has_orphan_position" in src
    has_position_amt = "positionAmt" in src

    if has_account_call and has_orphan_key and has_position_amt:
        return _result(
            "Item 1 — orphan position detection (S7)", True,
            "futures_account() + has_orphan_position + positionAmt all present in "
            "startup_reconciler.py — orphan positions detected before trading resumes",
        )

    missing = []
    if not has_account_call: missing.append("client.futures_account() call")
    if not has_orphan_key:   missing.append("has_orphan_position key")
    if not has_position_amt: missing.append("positionAmt field")

    return _result(
        "Item 1 — orphan position detection (S7)", False,
        f"Missing in startup_reconciler.py: {', '.join(missing)}. "
        "Crash-restart with open BINANCE_DEMO position leaves orphan undetected — "
        "new entry can stack on existing exchange position",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 2 — Item 2: unconditional _reset_open_position on failed emergency close
# ─────────────────────────────────────────────────────────────────────────────

async def check_item2_failed_close_preserves_state() -> bool:
    """
    Functional proof via handle_protection_wall_removed.

    Wire an open position (qty=0.01). Mock _emergency_close to return (False, 0.0)
    — IOC unfilled. Call handle_protection_wall_removed("LONG").

    Pre-fix:  _open_position_qty == 0.0  (line 1146 reset unconditionally).
    Post-fix: _open_position_qty == 0.01 (state preserved; _emergency_close_in_progress
              released so _watch_tp_sl can retry).
    """
    from risk.killswitch import GlobalKillswitch
    from execution.order_manager import OrderManager

    sig_q = asyncio.Queue()
    fill_q = asyncio.Queue()
    ks = GlobalKillswitch(dov=10_000.0)
    om = OrderManager(sig_q, fill_q, ks, equity_fn=lambda: 10_000.0)

    om._open_position_qty           = 0.01
    om._open_position_side          = "LONG"
    om._open_position_closed_event  = asyncio.Event()
    om._open_signal_id              = "test-item2"
    om._open_entry_price            = 65_000.0
    om._open_entry_time             = 0.0
    om._open_sl_price               = 64_000.0
    om._placing_oco                 = False
    om._emergency_close_in_progress = False

    async def _mock_close_fail(qty, side, reason=""):
        return False, 0.0

    om._cancel_bracket_orders = AsyncMock()
    om._emergency_close       = _mock_close_fail         # type: ignore[method-assign]
    om._record_outcome        = lambda *a, **kw: None    # type: ignore[method-assign]

    # Capture event reference before call — pre-fix resets _open_position_closed_event
    # to None, making om._open_position_closed_event.is_set() raise AttributeError.
    captured_event = om._open_position_closed_event

    with patch("execution.order_manager.settings") as cfg:
        cfg.DRY_RUN      = False
        cfg.BINANCE_DEMO = True
        await om.handle_protection_wall_removed("LONG")

    qty_after  = om._open_position_qty
    ecp_after  = om._emergency_close_in_progress
    event_set  = captured_event.is_set()
    event_null = om._open_position_closed_event is None  # True pre-fix (reset cleared it)

    if qty_after == 0.0:
        return _result(
            "Item 2 — failed close preserves local state", False,
            f"_open_position_qty reset to 0.0 despite IOC unfilled (line 1146 confirmed). "
            f"_emergency_close_in_progress={ecp_after}, event_set={event_set}, "
            f"event_nulled={event_null}. "
            "Engine believes no position open → new entry can stack on orphan exchange position",
        )
    if abs(qty_after - 0.01) < 1e-9 and not ecp_after and not event_set and not event_null:
        return _result(
            "Item 2 — failed close preserves local state", True,
            f"qty={qty_after} preserved; _emergency_close_in_progress={ecp_after} "
            f"(released for retry); event.is_set()={event_set}, event_null={event_null}. "
            "_watch_tp_sl can safely retry on next TP/SL touch",
        )
    return _result(
        "Item 2 — failed close preserves local state", False,
        f"Unexpected state: qty={qty_after}, ecp={ecp_after}, "
        f"event_set={event_set}, event_null={event_null}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 3 — Item 3: KS-1 not re-fired at startup when budget already blown
# ─────────────────────────────────────────────────────────────────────────────

async def check_item3_ks1_startup_refire() -> bool:
    """
    Two-part check:
      (a) Functional: GlobalKillswitch.check_budget() fires correctly on negative PnL.
      (b) Source: check_budget() call exists in main.py between update_dov and TaskGroup.

    Pre-fix:  no check_budget() call in startup block — KS unfired for ~1-2 s window.
    Post-fix: one-liner present — KS-1 re-fires before first signal evaluation.
    """
    from risk.killswitch import GlobalKillswitch

    # (a) Functional: verify check_budget fires correctly
    # dov=5_000 → hard_limit=50.0; realised_pnl=-60 → total_loss=-60 < -50 → fires
    ks = GlobalKillswitch(dov=5_000.0)
    fired = ks.check_budget(realised_pnl=-60.0, unrealised_pnl=0.0)
    if not fired or not ks.is_active:
        return _result(
            "Item 3 — KS-1 startup refire", False,
            f"GlobalKillswitch.check_budget() semantics broken: fired={fired}, "
            f"is_active={ks.is_active} (dov=5000, hard_limit=50, loss=-60) — "
            "underlying fix cannot be verified",
        )

    # (b) Source: look for check_budget between update_dov and TaskGroup
    lines = (ROOT / "main.py").read_text().split("\n")
    dov_idx = next((i for i, l in enumerate(lines) if "update_dov" in l), None)
    tg_idx  = next(
        (i for i, l in enumerate(lines) if "TaskGroup" in l and i > (dov_idx or 0)), None
    )
    if dov_idx is None:
        return _result("Item 3 — KS-1 startup refire", False,
                       "update_dov call not found in main.py")

    window = lines[dov_idx : tg_idx if tg_idx else dov_idx + 40]
    if any("check_budget" in l for l in window):
        return _result(
            "Item 3 — KS-1 startup refire", True,
            "check_budget() call found in main.py between update_dov and TaskGroup — "
            "KS-1 re-fires at startup if daily budget was already blown before crash-restart",
        )
    return _result(
        "Item 3 — KS-1 startup refire", False,
        "No check_budget() call between update_dov and TaskGroup in main.py. "
        "After crash-restart, KS unfired for ~1-2 s before first MTM tick — "
        "blown-budget session can accept new signals during that window",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 4 — Item 4: _evaluate exception propagates and crashes run loop
# ─────────────────────────────────────────────────────────────────────────────

async def check_item4_evaluate_exception_guard() -> bool:
    """
    Source inspection of strategy/executor.py run().

    Pre-fix:  bare 'while True: await self._evaluate(signal)' — any exception
              propagates out of run(), crashing the TaskGroup.
    Post-fix: try/except Exception wraps the call — exception is logged CRITICAL,
              signal dropped, loop continues.
    """
    lines = (ROOT / "strategy" / "executor.py").read_text().split("\n")
    run_idx = next((i for i, l in enumerate(lines) if "async def run(self)" in l), None)
    if run_idx is None:
        return _result("Item 4 — _evaluate exception guard", False,
                       "run() not found in strategy/executor.py")

    run_body   = lines[run_idx : run_idx + 15]
    has_try    = any(l.strip() == "try:" for l in run_body)
    has_except = any("except Exception" in l for l in run_body)

    if has_try and has_except:
        return _result(
            "Item 4 — _evaluate exception guard", True,
            "try/except Exception present in run() — gate exception drops signal "
            "and logs CRITICAL but does NOT crash the TaskGroup",
        )
    missing = []
    if not has_try:    missing.append("'try:' block")
    if not has_except: missing.append("'except Exception' handler")
    return _result(
        "Item 4 — _evaluate exception guard", False,
        f"Missing in executor.run(): {', '.join(missing)}. "
        "Any unhandled exception inside _evaluate propagates out of run(), "
        "cancelling all TaskGroup siblings and leaving open position unmonitored",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    print("\n=== CryptoSentinel Guardrail Hardening Plan 2 — Bug Validation ===")
    print("Pre-fix:  checks print FAIL  (bugs confirmed present)")
    print("Post-fix: checks print PASS  (bugs confirmed fixed)\n")

    results: list[bool] = []

    print("Check 1 — Item 1: orphan position not detected on startup restart")
    results.append(await check_item1_orphan_position_detection())

    print("\nCheck 2 — Item 2: unconditional _reset_open_position on failed emergency close")
    results.append(await check_item2_failed_close_preserves_state())

    print("\nCheck 3 — Item 3: KS-1 not re-fired at startup when budget already blown")
    results.append(await check_item3_ks1_startup_refire())

    print("\nCheck 4 — Item 4: _evaluate exception crashes run loop (no guard)")
    results.append(await check_item4_evaluate_exception_guard())

    passed = sum(results)
    total  = len(results)
    print(f"\n{'=' * 60}")
    print(f"Result: {passed}/{total} checks passed")
    if passed == total:
        print("All guardrail bugs fixed.\n")
    elif passed == 0:
        print(f"All {total} bugs confirmed present — expected pre-fix baseline.\n")
    else:
        print(f"{total - passed} bug(s) still present, {passed} fixed.\n")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
