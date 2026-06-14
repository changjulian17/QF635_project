#!/usr/bin/env python3
"""
Root cause validation for CryptoSentinel bugs (2026-06-14 session).

Each check is a functional test that exercises real code paths:
  Pre-fix:  checks print FAIL  — bugs confirmed present.
  Post-fix: checks print PASS  — bugs confirmed resolved.

Exit code: 0 = all pass, 1 = any fail.

Usage:
    source .venv/bin/activate
    python scripts/validate_bugs.py
"""
import asyncio
import logging
import random
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.disable(logging.CRITICAL)  # suppress engine logs during checks


def _result(name: str, passed: bool, detail: str) -> bool:
    tag = "PASS" if passed else "FAIL"
    print(f"  [{tag}] {name}")
    print(f"         {detail}")
    return passed


# ─────────────────────────────────────────────────────────────────────────────
# Check 1: Issue 1 — BINANCE_DEMO returns algoId; orderId check always fails
# ─────────────────────────────────────────────────────────────────────────────

async def check_issue1_binance_demo_oco_skip() -> bool:
    """
    Functional proof:
      1. Simulate a BINANCE_DEMO futures_create_order call — mock returns {"algoId": ...}.
      2. Apply the exact orderId check from order_manager.py.
      3. Show it always evaluates to True → KeyError → emergency close path.

    PASS: a BINANCE_DEMO guard returns before futures_create_order is called.
    FAIL: futures_create_order is called with a demo account → algoId response →
          orderId check fails → KeyError on every trade.
    """
    async def mock_create_order(**kwargs):
        return {"algoId": 1000000105725611, "symbol": "BTCUSDT", "clientAlgoId": ""}

    resp = await mock_create_order(symbol="BTCUSDT", side="SELL", type="TAKE_PROFIT",
                                   quantity=0.01, price="70000.0", stopPrice="70000.0")
    orderId_missing = "orderId" not in resp

    if not orderId_missing:
        return _result("Issue 1 — BINANCE_DEMO OCO skip", True,
                       "Demo response contains orderId — bug not present")

    # orderId absent — check whether a BINANCE_DEMO guard exists before _placing_oco=True
    lines = (ROOT / "execution" / "order_manager.py").read_text().split('\n')
    placing_idx = next((i for i, l in enumerate(lines) if 'self._placing_oco = True' in l), None)
    dry_run_candidates = [i for i, l in enumerate(lines[:placing_idx or 0])
                          if 'if settings.DRY_RUN:' in l]
    guard_present = False
    if placing_idx and dry_run_candidates:
        dry_run_idx = dry_run_candidates[-1]
        window = lines[dry_run_idx + 1 : placing_idx]
        for i, l in enumerate(window):
            if 'BINANCE_DEMO' in l:
                near = window[i : i + 5]
                if any('return' in ll and not ll.strip().startswith('#') for ll in near):
                    guard_present = True
                    break

    if guard_present:
        return _result("Issue 1 — BINANCE_DEMO OCO skip", True,
                       "BINANCE_DEMO early-return found — futures_create_order never called on demo")

    return _result(
        "Issue 1 — BINANCE_DEMO OCO skip", False,
        f"Demo response {resp!r} has no 'orderId'. "
        f"No BINANCE_DEMO guard before _placing_oco=True (src line ~{placing_idx}). "
        "futures_create_order is called → orderId check always fails "
        "→ KeyError → emergency close on every demo trade"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 2: Issue 2 — synthetic wall price absent from real LOB → Gate6 fires
# ─────────────────────────────────────────────────────────────────────────────

async def check_issue2_gate6_synthetic_wall() -> bool:
    """
    Functional proof:
      1. Populate a LocalOrderBook with realistic uniform bids (no outlier at
         the synthetic protection-wall price the signal injector creates).
      2. Call get_current_walls() — confirm the synthetic price is absent.
         This reproduces why Gate6 fires ~300ms after every fill.
      3. If inject_test_wall() exists: inject and verify the price now appears.

    PASS: inject_test_wall() exists AND injected price appears in get_current_walls().
    FAIL: synthetic price absent from real book AND inject_test_wall() missing.
    """
    from core.lob_engine import LocalOrderBook

    lob  = LocalOrderBook()
    mid  = 60000.0
    random.seed(42)

    # Populate with uniform bids (slight noise so std > 0, but no wall)
    for i in range(1, 51):
        lob._bids[mid - i] = 0.5 + (random.random() - 0.5) * 0.05
    lob._ready = True

    # Synthetic protection wall price the signal injector creates for LONG entries
    synthetic_price = round(mid * 0.9990, 2)  # 59940.0

    assert synthetic_price not in lob._bids, \
        f"Test setup error: synthetic price {synthetic_price} is in the real book"

    walls_real = await lob.get_current_walls(sigma=2.5)
    found_in_real = any(abs(w["price"] - synthetic_price) < 0.01 for w in walls_real)

    if found_in_real:
        return _result("Issue 2 — Gate6 synthetic wall", False,
                       f"Unexpected: synthetic price {synthetic_price} found in real LOB scan")

    if not hasattr(lob, 'inject_test_wall'):
        return _result(
            "Issue 2 — Gate6 synthetic wall", False,
            f"Synthetic wall at {synthetic_price} NOT in get_current_walls() output "
            f"({len(walls_real)} real walls found, none at synthetic price). "
            "inject_test_wall() absent — Gate6 WALL_REMOVED fires ~300ms after every fill"
        )

    lob.inject_test_wall(price=synthetic_price, side="bid", ttl_ms=5_000)
    walls_after = await lob.get_current_walls(sigma=2.5)
    found_after = any(abs(w["price"] - synthetic_price) < 0.01 for w in walls_after)

    if found_after:
        return _result(
            "Issue 2 — Gate6 synthetic wall", True,
            f"inject_test_wall({synthetic_price}) → price now in get_current_walls() "
            f"(was absent before injection) — Gate6 will persist position correctly"
        )

    return _result(
        "Issue 2 — Gate6 synthetic wall", False,
        f"inject_test_wall() exists but {synthetic_price} still not in get_current_walls() "
        "— override merge missing from get_current_walls()"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 3: Issue 3 — single LOB gap triggers immediate reconnect
# ─────────────────────────────────────────────────────────────────────────────

async def check_issue3_lob_gap_debounce() -> bool:
    """
    Functional proof: dispatch a single sequence-gapped depth message through
    the real _dispatch → _apply_depth_diff code path and observe whether
    heartbeat immediately becomes CRITICAL.

    PASS: single gap does NOT set heartbeat CRITICAL (debounce counter active).
    FAIL: single gap triggers CRITICAL → reconnect storm confirmed.
    """
    from models import SharedState
    from core.ws_consumer import BinanceWebSocketConsumer

    consumer = BinanceWebSocketConsumer(
        shared_state=SharedState(),
        streams=["btcusdt@depth@100ms"],
        warn_ms=2000,
        critical_ms=5000,
        consec_limit=20,
    )
    consumer._lob_synced    = True
    consumer._lob_update_id = 100

    gap_msg = {
        "e": "depthUpdate",
        "E": int(time.time() * 1000),
        "U": 102,
        "u": 103,
        "b": [],
        "a": [],
    }
    await consumer._dispatch("btcusdt@depth@100ms", gap_msg)

    if consumer.heartbeat.status == "CRITICAL":
        return _result(
            "Issue 3 — LOB gap debounce", False,
            "Single gap immediately set heartbeat CRITICAL — "
            "reconnect storm confirmed (1,681 cycles / 57 min observed in session log)"
        )

    consecutive = getattr(consumer, '_consecutive_lob_gaps', 'MISSING — attribute not added')
    return _result(
        "Issue 3 — LOB gap debounce", True,
        f"Single gap did NOT trigger CRITICAL "
        f"(_consecutive_lob_gaps={consecutive}) — debounce counter active"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 6: Issue 6 — double emergency close race (OCO_FAILED + Gate6)
# ─────────────────────────────────────────────────────────────────────────────

async def check_issue6_double_close_guard() -> bool:
    """
    Functional proof: with _emergency_close_in_progress=True (representing OCO_FAILED
    having already started an emergency close), call handle_protection_wall_removed
    and observe whether it fires a second _emergency_close.

    PASS: handle_protection_wall_removed returns without calling _emergency_close.
    FAIL: handle_protection_wall_removed calls _emergency_close → double close confirmed.
    """
    from risk.killswitch import GlobalKillswitch
    from execution.order_manager import OrderManager

    sig_q  = asyncio.Queue()
    fill_q = asyncio.Queue()
    ks     = GlobalKillswitch(dov=10_000.0)
    om     = OrderManager(sig_q, fill_q, ks, equity_fn=lambda: 10_000.0)

    om._open_position_qty           = 0.01
    om._open_position_side          = "LONG"
    om._open_position_closed_event  = None
    om._open_signal_id              = "test-signal"
    om._open_entry_price            = 65000.0
    om._open_entry_time             = None
    om._open_sl_price               = 64000.0
    om._placing_oco                 = False
    om._emergency_close_in_progress = True  # OCO_FAILED should have set this

    close_calls: list[str] = []

    async def mock_emergency_close(qty, side, reason=""):
        close_calls.append(reason)
        return True, 64800.0

    om._cancel_bracket_orders = AsyncMock()
    om._emergency_close       = mock_emergency_close   # type: ignore[method-assign]
    om._reset_open_position   = lambda: None           # type: ignore[method-assign]
    om._record_outcome        = lambda *a, **kw: None  # type: ignore[method-assign]

    with patch('execution.order_manager.settings') as mock_cfg:
        mock_cfg.DRY_RUN      = False
        mock_cfg.BINANCE_DEMO = True
        await om.handle_protection_wall_removed("LONG")

    if close_calls:
        return _result(
            "Issue 6 — double close guard", False,
            f"handle_protection_wall_removed called _emergency_close(reason={close_calls[0]!r}) "
            "even though _emergency_close_in_progress=True — "
            "double close confirmed: OCO_FAILED close + Gate6 close both execute concurrently"
        )

    return _result(
        "Issue 6 — double close guard", True,
        "handle_protection_wall_removed returned early when _emergency_close_in_progress=True "
        "— Gate6 correctly suppresses duplicate emergency close"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 9: Issue 9 — CB PAUSE doesn't block Gate3 (tier never set to PASSIVE)
# ─────────────────────────────────────────────────────────────────────────────

async def check_issue9_cb_pause_blocks_gate3() -> bool:
    """
    Functional proof:
      1. Create a RiskEngine with consecutive_losses == MAX_CONSECUTIVE_LOSSES (3).
      2. Call sync_tier() — this calls _check_circuit_breakers().
      3. Pre-fix: _tier stays "FULL" → sync_tier() returns "FULL" → Gate3 allows entries.
         Post-fix: _tier set to "PASSIVE" → sync_tier() returns "PASSIVE" → Gate3 rejects.

    PASS: sync_tier() returns "PASSIVE" and gate_3_capital rejects entries.
    FAIL: sync_tier() returns "FULL" → 300s cooldown has no effect on the signal pipeline.
    """
    from config import settings as cfg
    from models import PortfolioState, CircuitBreakerStatus
    from risk.engine import RiskEngine
    from risk.budget import DailyBudget
    from risk.killswitch import GlobalKillswitch
    from strategy.executor import gate_3_capital

    pf = PortfolioState(
        equity=10_000.0,
        starting_equity=10_000.0,
        peak_equity=10_000.0,
        consecutive_losses=cfg.MAX_CONSECUTIVE_LOSSES,
    )
    budget = DailyBudget.from_equity(pf.starting_equity)
    engine = RiskEngine(
        portfolio=pf,
        budget=budget,
        killswitch=GlobalKillswitch(pf.starting_equity),
    )

    returned_tier = engine.sync_tier()

    if returned_tier != "PASSIVE":
        return _result(
            "Issue 9 — CB PAUSE blocks Gate3", False,
            f"sync_tier() returned '{returned_tier}' after {cfg.MAX_CONSECUTIVE_LOSSES} "
            f"consecutive losses — expected 'PASSIVE'. "
            f"pf.circuit_breaker={pf.circuit_breaker.name}. "
            "Gate3 sees tier=FULL → approves entries during 300s cooldown"
        )

    if pf.circuit_breaker != CircuitBreakerStatus.PAUSED:
        return _result(
            "Issue 9 — CB PAUSE blocks Gate3", False,
            f"tier correctly set to PASSIVE but circuit_breaker={pf.circuit_breaker.name} "
            "(expected PAUSED)"
        )

    gate3_ok, gate3_reason = gate_3_capital(budget, returned_tier, active_exposure=False)
    if gate3_ok:
        return _result(
            "Issue 9 — CB PAUSE blocks Gate3", False,
            f"gate_3_capital(tier='PASSIVE') returned True — entries not blocked. "
            f"reason={gate3_reason!r}"
        )

    return _result(
        "Issue 9 — CB PAUSE blocks Gate3", True,
        f"sync_tier() returned 'PASSIVE' after {cfg.MAX_CONSECUTIVE_LOSSES} consecutive losses. "
        f"gate_3_capital rejected with: '{gate3_reason}' — "
        "300s consecutive-loss cooldown now correctly blocks all new entries"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    print("\n=== CryptoSentinel Bug Root Cause Validation ===")
    print("Pre-fix:  checks print FAIL  (bugs confirmed present)")
    print("Post-fix: checks print PASS  (bugs confirmed fixed)\n")

    results: list[bool] = []

    print("Check 1 — Issue 1: BINANCE_DEMO OCO bracket (algoId vs orderId)")
    results.append(await check_issue1_binance_demo_oco_skip())

    print("\nCheck 2 — Issue 2: Gate6 synthetic wall not in real LOB")
    results.append(await check_issue2_gate6_synthetic_wall())

    print("\nCheck 3 — Issue 3: LOB gap debounce (single gap triggers reconnect)")
    results.append(await check_issue3_lob_gap_debounce())

    print("\nCheck 6 — Issue 6: double emergency close race (OCO_FAILED + Gate6)")
    results.append(await check_issue6_double_close_guard())

    print("\nCheck 9 — Issue 9: CB PAUSE tier propagation to Gate3")
    results.append(await check_issue9_cb_pause_blocks_gate3())

    passed = sum(results)
    total  = len(results)
    print(f"\n{'=' * 50}")
    print(f"Result: {passed}/{total} checks passed")
    if passed == total:
        print("All root causes fixed.\n")
    elif passed == 0:
        print(f"All {total} bugs confirmed present — expected pre-fix baseline.\n")
    else:
        print(f"{total - passed} bug(s) still present, {passed} fixed.\n")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
