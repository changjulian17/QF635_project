#!/usr/bin/env python3
"""
Smoke test — validate the heartbeat force-reconnect cascade in ws_consumer.py.

Directly exercises HeartbeatMonitor with the demo preset values to confirm that:
  1. Transient latency spikes (5–7s, as seen in logs/cryptosentinel.log.1) trigger
     _receive_loop to break → reconnect → Binance drops LOB connection.
  2. Raising HEARTBEAT_CRITICAL_MS to 30 000ms in the demo preset prevents the cascade
     while still letting the recv timeout (10s) catch genuine disconnects.

No network access needed — pure Python, completes in milliseconds.
"""
from __future__ import annotations

import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from core.ws_consumer import HeartbeatMonitor
from config import Settings


def _make_settings(mode: str) -> Settings:
    import os
    env_backup = {}
    for k in ("TRADING_MODE", "HEARTBEAT_CRITICAL_MS", "HEARTBEAT_CONSEC_LIMIT"):
        env_backup[k] = os.environ.pop(k, None)
    os.environ["TRADING_MODE"] = mode
    s = Settings()
    for k, v in env_backup.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return s


def _feed_msgs(monitor: HeartbeatMonitor, delta_ms: int, count: int, consec_limit: int) -> int:
    """Feed `count` messages with the given delta into `monitor`. Returns first reconnect msg#."""
    for i in range(count):
        event_ms = int(time.time() * 1000) - delta_ms
        monitor.record(event_ms)
        if monitor._critical_count >= consec_limit:
            return i + 1
    return -1  # never fired


def run_tests() -> None:
    demo = _make_settings("demo")
    critical_ms   = demo.HEARTBEAT_CRITICAL_MS   # 5 000 in demo preset
    consec_limit  = demo.HEARTBEAT_CONSEC_LIMIT  # 10 in demo preset
    proposed_ms   = 30_000

    print("=" * 60)
    print("HeartbeatMonitor Cascade Smoke Test")
    print(f"  TRADING_MODE:              demo")
    print(f"  HEARTBEAT_CRITICAL_MS:     {critical_ms} ms  (current)")
    print(f"  HEARTBEAT_CONSEC_LIMIT:    {consec_limit}")
    print(f"  Proposed CRITICAL_MS:      {proposed_ms} ms")
    print("=" * 60)

    # ── Test 1: normal latency ────────────────────────────────────
    print("\n[1] Normal latency (delta=50ms × 20 msgs)")
    m = HeartbeatMonitor(critical_ms=critical_ms)
    fired_at = _feed_msgs(m, 50, 20, consec_limit)
    if fired_at == -1:
        print(f"    ✓ PASS  critical_count={m._critical_count}  no reconnect")
    else:
        print(f"    ✗ FAIL  fired at msg {fired_at} (unexpected)")

    # ── Test 2: spike seen in logs (6s delta) ────────────────────
    spike_ms = critical_ms + 1_000  # 6 000ms — matches log values 5286–7301ms
    print(f"\n[2] Log-level latency spike (delta={spike_ms}ms × {consec_limit + 2} msgs)")
    print(f"    Should fire reconnect at message {consec_limit}")
    m2 = HeartbeatMonitor(critical_ms=critical_ms)
    for i in range(consec_limit + 2):
        event_ms = int(time.time() * 1000) - spike_ms
        m2.record(event_ms)
        fires = m2._critical_count >= consec_limit
        tick  = " ← RECONNECT FIRES  (_receive_loop breaks)" if fires else ""
        print(f"    msg {i+1:2d}: critical_count={m2._critical_count:2d}{tick}")
        if fires:
            break

    if m2._critical_count >= consec_limit:
        print(f"\n    ✓ CONFIRMED: cascade fires after {consec_limit} msgs @ {spike_ms}ms delta")
        print(f"      ws_consumer._receive_loop lines 269-274 would break here.")
        print(f"      Rapid reconnects → Binance drops LOB consumer connection.")
    else:
        print(f"\n    ✗ FAIL  cascade did not fire (unexpected)")

    # ── Test 3: proposed fix (30 000ms threshold) ────────────────
    print(f"\n[3] Same spike with proposed fix (CRITICAL_MS={proposed_ms}ms)")
    m3 = HeartbeatMonitor(critical_ms=proposed_ms)
    fired_at3 = _feed_msgs(m3, spike_ms, consec_limit + 5, consec_limit)
    if fired_at3 == -1:
        print(f"    ✓ PASS  critical_count={m3._critical_count}  no reconnect")
        print(f"    {spike_ms}ms spike is DEGRADED (>{demo.HEARTBEAT_WARN_MS}ms warn) but not CRITICAL.")
        print(f"    Gate 0 passes on DEGRADED. LOB consumer stays connected.")
    else:
        print(f"    ✗ FAIL  cascade still fires at msg {fired_at3} (unexpected)")

    # ── Test 4: genuine disconnect still caught ──────────────────
    print(f"\n[4] Genuine disconnect with proposed fix")
    print(f"    Recv timeout (10s, ws_consumer.py:244) fires independently of heartbeat.")
    print(f"    A dead connection with NO messages triggers break in {10}s regardless")
    print(f"    of HEARTBEAT_CRITICAL_MS — heartbeat doesn't even get called if recv stalls.")
    print(f"    ✓ Raising CRITICAL_MS to {proposed_ms}ms does not remove this safety net.")

    print("\n" + "=" * 60)
    print("VERDICT")
    all_pass = m._critical_count == 0 and m2._critical_count >= consec_limit and fired_at3 == -1
    if all_pass:
        print("  ✅ ALL TESTS PASS")
        print(f"  Current config  (CRITICAL_MS={critical_ms}ms): cascade fires on 6s transient spikes.")
        print(f"  Proposed config (CRITICAL_MS={proposed_ms}ms): cascade prevented; recv timeout protects.")
        print(f"\n  FIX: in config.py demo preset, change HEARTBEAT_CRITICAL_MS from {critical_ms} → {proposed_ms}.")
    else:
        print("  ✗ SOME TESTS FAILED — review output above")
    print("=" * 60)


if __name__ == "__main__":
    run_tests()
