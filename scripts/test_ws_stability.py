#!/usr/bin/env python3
"""
WebSocket stability diagnostic for Binance Futures streams.

Opens TWO concurrent WebSocket connections matching the engine's layout:
  Conn A (LOB):        btcusdt@depth@500ms
  Conn B (Tick+Candle): btcusdt@bookTicker + btcusdt@aggTrade + btcusdt@kline_1m

Runs N independent sessions, measuring:
  - Inter-message gaps and simulated recv-timeout fires at [5,10,15,20,30,60]s
  - Message latency: wall_ms − E (exchange timestamp vs local clock)
  - Per-minute message rate for throttle detection (>50% drop vs baseline)

HEARTBEAT LATENCY MODE (--heartbeat-latency):
  Subscribes to btcusdt@bookTicker on fstream.binance.com for 60s and measures
  delta between Binance's "E" field and local clock — the exact value HeartbeatMonitor
  records on every message.  Reports whether HEARTBEAT_CRITICAL_MS=5000ms would fire
  under current network conditions.

Endpoints:
  Production (demo mode): wss://fstream.binance.com      (BINANCE_DEMO=True)
  Testnet   (default):    wss://stream.binancefuture.com  (engine default WS_BASE)

Usage:
    python scripts/test_ws_stability.py                       # 3×10min, production
    python scripts/test_ws_stability.py --runs 1              # single session
    python scripts/test_ws_stability.py --duration 120        # 2-min sessions
    python scripts/test_ws_stability.py --endpoint wss://stream.binancefuture.com
    python scripts/test_ws_stability.py --heartbeat-latency   # confirm heartbeat root cause
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import websockets

# Engine stream layout — two separate WS connections per consumer
CONN_A_STREAMS = ["btcusdt@depth@500ms"]                                      # LOB consumer (depth-only)
CONN_B_STREAMS = ["btcusdt@bookTicker", "btcusdt@aggTrade", "btcusdt@kline_1m"]  # Tick+Candle consumer
ALL_CONNS      = [("LOB",         CONN_A_STREAMS),
                  ("Tick+Candle", CONN_B_STREAMS)]

# Endpoints
PROD_BASE    = "wss://fstream.binance.com"          # used when BINANCE_DEMO=True
TESTNET_BASE = "wss://stream.binancefuture.com"      # engine default WS_BASE

RUN_SECONDS = 600    # 10 minutes per session (override with --duration)
WINDOW_S    = 60     # per-minute bucket

# Candidate recv-timeout values to simulate (seconds)
THRESHOLDS  = [5, 10, 15, 20, 30, 60]
OLD_THRESHOLD = 5.0    # threshold before our fix
NEW_THRESHOLD = 10.0   # threshold after our fix


# ──────────────────────────────────────────────────────────────────────────────
# Per-stream statistics accumulator
# ──────────────────────────────────────────────────────────────────────────────

class StreamStats:
    def __init__(self, label: str):
        self.label       = label
        self.msg_count   = 0
        self.gaps: list[float]      = []
        self.latencies: list[float] = []
        self._last_t: float | None  = None

        self.windows: list[dict]    = []
        self._win_start   = time.monotonic()
        self._win_count   = 0
        self._win_max_gap = 0.0
        self._win_fires: dict[int, int] = {t: 0 for t in THRESHOLDS}

        self.total_fires: dict[int, int] = {t: 0 for t in THRESHOLDS}

    def record(self, event_ms: int | None) -> None:
        now     = time.monotonic()
        wall_ms = time.time() * 1000

        if self._last_t is not None:
            gap = now - self._last_t
            self.gaps.append(gap)
            self._win_max_gap = max(self._win_max_gap, gap)
            for t in THRESHOLDS:
                if gap > t:
                    self._win_fires[t] += 1
                    self.total_fires[t] += 1

        self._last_t = now
        self.msg_count += 1
        self._win_count += 1

        if event_ms is not None:
            self.latencies.append(wall_ms - event_ms)

        if (now - self._win_start) >= WINDOW_S:
            self._flush_window()

    def _flush_window(self) -> None:
        elapsed = time.monotonic() - self._win_start
        self.windows.append({
            "n":       len(self.windows) + 1,
            "elapsed": elapsed,
            "count":   self._win_count,
            "rate":    self._win_count / elapsed if elapsed > 0 else 0.0,
            "max_gap": self._win_max_gap,
            "fires":   dict(self._win_fires),
        })
        self._win_start   = time.monotonic()
        self._win_count   = 0
        self._win_max_gap = 0.0
        self._win_fires   = {t: 0 for t in THRESHOLDS}

    def flush_final(self) -> None:
        if self._win_count > 0:
            self._flush_window()

    @property
    def max_gap(self) -> float:
        return max(self.gaps) if self.gaps else 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Single WebSocket consumer task
# ──────────────────────────────────────────────────────────────────────────────

async def consume_stream(
    conn_label: str,
    stream_names: list[str],
    stats: dict[str, StreamStats],
    deadline: float,
    ws_base: str,
) -> None:
    uri = f"{ws_base}/stream?streams={'/'.join(stream_names)}"
    try:
        async with websockets.connect(
            uri,
            ping_interval=10,
            ping_timeout=15,
            close_timeout=0.1,
        ) as ws:
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining + 1.0, 65.0))
                except (asyncio.TimeoutError, TimeoutError):
                    break

                try:
                    outer  = json.loads(raw)
                    stream = outer.get("stream", "")
                    msg    = outer.get("data", outer)
                    label  = stream.split("@", 1)[1] if "@" in stream else stream
                    if label in stats:
                        stats[label].record(msg.get("E"))
                except (json.JSONDecodeError, KeyError, AttributeError):
                    pass

    except (OSError, websockets.exceptions.WebSocketException) as exc:
        print(f"  [{conn_label}] Connection error: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Session — two concurrent connections
# ──────────────────────────────────────────────────────────────────────────────

async def run_session(
    session_num: int,
    total: int,
    duration_s: float,
    ws_base: str,
) -> dict[str, StreamStats]:
    print(f"\n{'='*64}")
    print(f"Run {session_num}/{total}  —  {duration_s/60:.0f}-min session  [{ws_base}]")
    for conn_label, streams in ALL_CONNS:
        uri = f"{ws_base}/stream?streams={'/'.join(streams)}"
        print(f"  [{conn_label}] {uri}")
    print(f"{'='*64}")

    stats: dict[str, StreamStats] = {}
    for _, streams in ALL_CONNS:
        for s in streams:
            label = s.split("@", 1)[1] if "@" in s else s
            stats[label] = StreamStats(label)

    deadline = time.monotonic() + duration_s
    print(f"  Both connections starting …")

    async def progress_ticker() -> None:
        last_min = 0
        while time.monotonic() < deadline:
            await asyncio.sleep(1.0)
            elapsed_min = int((duration_s - (deadline - time.monotonic())) / 60)
            if elapsed_min > last_min:
                last_min = elapsed_min
                total_msgs = sum(s.msg_count for s in stats.values())
                print(f"  [{elapsed_min}m elapsed]  {total_msgs} msgs total")

    tasks = [
        asyncio.create_task(consume_stream(label, streams, stats, deadline, ws_base))
        for label, streams in ALL_CONNS
    ]
    tasks.append(asyncio.create_task(progress_ticker()))

    done, _ = await asyncio.wait(tasks[:-1], return_when=asyncio.ALL_COMPLETED)
    tasks[-1].cancel()
    await asyncio.gather(tasks[-1], return_exceptions=True)

    for s in stats.values():
        s.flush_final()

    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Report helpers
# ──────────────────────────────────────────────────────────────────────────────

def _fmt(v: float, fmt: str = ".2f") -> str:
    return "n/a" if math.isnan(v) else format(v, fmt)


def _pct(data: list[float], p: int) -> float:
    if not data:
        return float("nan")
    s = sorted(data)
    idx = max(0, min(int(len(s) * p / 100), len(s) - 1))
    return s[idx]


def print_session_report(session_num: int, total: int, stats: dict[str, StreamStats]) -> None:
    print(f"\n{'─'*64}")
    print(f"Run {session_num}/{total}  —  RESULTS")
    print(f"{'─'*64}")

    all_latencies: list[float] = []

    for label, s in sorted(stats.items()):
        if s.msg_count == 0:
            print(f"\n  {label:<24}  NO MESSAGES RECEIVED")
            continue

        elapsed  = sum(w["elapsed"] for w in s.windows)
        avg_rate = s.msg_count / elapsed if elapsed > 0 else 0.0
        p95_gap  = _pct(s.gaps, 95)

        print(f"\n  {label}:")
        print(f"    msgs={s.msg_count}  rate={avg_rate:.2f}/s  "
              f"max_gap={s.max_gap:.2f}s  p95_gap={p95_gap:.2f}s")

        if s.latencies:
            clean = [l for l in s.latencies if not math.isnan(l)]
            if clean:
                all_latencies.extend(clean)
                print(f"    latency  p50={_pct(clean, 50):.0f}ms  "
                      f"p95={_pct(clean, 95):.0f}ms  max={max(clean):.0f}ms")

        print(f"    Recv-timeout fires (simulated):")
        for t in THRESHOLDS:
            fires  = s.total_fires[t]
            marker = " ← PROBLEMATIC" if fires > 0 and t <= 10 else (
                     " ← watch"      if fires > 0 else "")
            print(f"      @ {t:2d}s : {fires:4d} fires{marker}")

        if len(s.windows) >= 2:
            baseline  = s.windows[0]["rate"]
            throttled = [w["n"] for w in s.windows[1:]
                         if baseline > 0 and w["rate"] < baseline * 0.5]
            if throttled:
                print(f"    ⚠️  THROTTLE DETECTED: rate dropped >50% in "
                      f"minute(s) {throttled} (baseline {baseline:.2f}/s)")
            else:
                print(f"    ✓ No throttling ({len(s.windows)} windows, "
                      f"baseline {baseline:.2f}/s)")

        print(f"    Per-minute windows:")
        for w in s.windows:
            warn = " ⚠️" if w["max_gap"] > 5 else ""
            f5   = w["fires"].get(5, 0)
            f30  = w["fires"].get(30, 0)
            print(f"      min {w['n']:2d}: rate={w['rate']:.2f}/s  "
                  f"max_gap={w['max_gap']:.2f}s{warn}  "
                  f"5s_fires={f5}  30s_fires={f30}")

    if all_latencies:
        print(f"\n  Overall latency (all streams):")
        print(f"    p50={_pct(all_latencies, 50):.0f}ms  "
              f"p95={_pct(all_latencies, 95):.0f}ms  "
              f"max={max(all_latencies):.0f}ms")


def print_final_summary(all_runs: list[dict[str, StreamStats]], ws_base: str) -> None:
    print(f"\n{'='*64}")
    print(f"FINAL SUMMARY  —  {len(all_runs)} run(s)  [{ws_base}]")
    print(f"{'='*64}")

    labels = sorted(all_runs[0].keys()) if all_runs else []

    print(f"\nSimulated recv-timeout fire counts (all runs combined):")
    print(f"  {'stream':<24}" + "".join(f"  {t:>4}s" for t in THRESHOLDS))
    print("  " + "─" * (24 + 7 * len(THRESHOLDS)))

    for label in labels:
        fires_total = {
            t: sum(r[label].total_fires.get(t, 0) for r in all_runs if label in r)
            for t in THRESHOLDS
        }
        print(f"  {label:<24}" + "".join(f"  {fires_total[t]:>5}" for t in THRESHOLDS))

    # ── Root cause validation ─────────────────────────────────────────────────
    print(f"\n{'─'*64}")
    print(f"ROOT CAUSE VALIDATION")
    print(f"  Hypothesis: recv timeout at {OLD_THRESHOLD:.0f}s (old) caused LOB resets "
          f"due to stream gaps")
    print(f"  Fix:        recv timeout raised to {NEW_THRESHOLD:.0f}s")
    print(f"{'─'*64}")

    any_old_fires = False
    any_new_fires = False

    for label in labels:
        if all(r.get(label) and r[label].msg_count == 0 for r in all_runs):
            print(f"  {label:<24} SKIP (no messages received)")
            continue

        old_fires = sum(r[label].total_fires.get(int(OLD_THRESHOLD), 0)
                        for r in all_runs if label in r)
        new_fires = sum(r[label].total_fires.get(int(NEW_THRESHOLD), 0)
                        for r in all_runs if label in r)
        max_g     = max((r[label].max_gap for r in all_runs if label in r), default=0.0)

        if old_fires > 0:
            any_old_fires = True
            verdict = f"✅ CONFIRMED  ({old_fires} reconnects would have happened)"
        else:
            verdict = f"❌ REFUTED   (max_gap={max_g:.2f}s < {OLD_THRESHOLD:.0f}s threshold)"

        new_verdict = "✅ 0 fires" if new_fires == 0 else f"⚠️ {new_fires} fires remain"

        print(f"  {label}:")
        print(f"    old {OLD_THRESHOLD:.0f}s threshold: {verdict}")
        print(f"    new {NEW_THRESHOLD:.0f}s threshold: {new_verdict}  (max_gap={max_g:.2f}s)")

        if old_fires == 0 and new_fires == 0:
            any_new_fires = False

    print()
    if any_old_fires:
        print(f"  CONCLUSION: Root cause CONFIRMED on {ws_base}")
        print(f"    Stream gaps exceeded {OLD_THRESHOLD:.0f}s — bumping to {NEW_THRESHOLD:.0f}s fixes the reconnect loop.")
    else:
        print(f"  CONCLUSION: Root cause NOT confirmed on {ws_base}")
        print(f"    No stream gap exceeded {OLD_THRESHOLD:.0f}s — the {OLD_THRESHOLD:.0f}s threshold "
              f"would not have caused reconnects on this endpoint.")
        if ws_base == PROD_BASE:
            print(f"    ⚠️  Note: engine default WS_BASE is {TESTNET_BASE} (testnet), not "
                  f"this production endpoint.")
            print(f"    Run with --endpoint {TESTNET_BASE} or --compare to test the actual engine endpoint.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


async def run_heartbeat_latency(duration_s: float = 60.0) -> None:
    """
    Measure the actual delta between Binance's "E" (event_time_ms) field and local
    clock for bookTicker messages on fstream.binance.com — exactly what the engine's
    HeartbeatMonitor computes on every message.

    Reports delta distribution and tells you exactly how quickly the engine's
    force-reconnect threshold (HEARTBEAT_CONSEC_LIMIT consecutive > HEARTBEAT_CRITICAL_MS)
    would fire under current network conditions.

    Current demo preset values:
      HEARTBEAT_CRITICAL_MS  = 5 000 ms  (ws_consumer.py line 269 break condition)
      HEARTBEAT_CONSEC_LIMIT = 10 msgs   (how many consecutive before forced reconnect)
    """
    DEMO_CRITICAL_MS   = 5_000   # current demo preset
    DEMO_CONSEC_LIMIT  = 10      # current demo preset
    PROPOSED_CRITICAL  = 30_000  # proposed new threshold for demo preset

    uri = f"{PROD_BASE}/stream?streams=btcusdt@bookTicker"
    print(f"\n  Connecting to {uri} for {duration_s:.0f}s …")

    deltas: list[float] = []
    consec_above_5s     = 0
    consec_above_30s    = 0
    first_reconnect_at  = None   # first time consec_above_5s hits DEMO_CONSEC_LIMIT

    deadline = time.monotonic() + duration_s
    try:
        async with websockets.connect(
            uri, ping_interval=10, ping_timeout=15, close_timeout=0.1,
        ) as ws:
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining + 1.0, 15.0))
                except (asyncio.TimeoutError, TimeoutError):
                    print(f"  ⏱  No bookTicker message for 15s — connection may be stalled")
                    break

                try:
                    outer = json.loads(raw)
                    msg   = outer.get("data", outer)
                    e_ms  = msg.get("E")
                    if e_ms is None:
                        continue
                    delta_ms = time.time() * 1000 - int(e_ms)
                    deltas.append(delta_ms)

                    if delta_ms > DEMO_CRITICAL_MS:
                        consec_above_5s += 1
                        if consec_above_5s == DEMO_CONSEC_LIMIT and first_reconnect_at is None:
                            first_reconnect_at = len(deltas)
                    else:
                        consec_above_5s = 0

                    if delta_ms > PROPOSED_CRITICAL:
                        consec_above_30s += 1
                    else:
                        consec_above_30s = 0

                except (json.JSONDecodeError, KeyError, TypeError):
                    pass
    except (OSError, websockets.exceptions.WebSocketException) as exc:
        print(f"  Connection error: {exc}")

    if not deltas:
        print("  No messages received — cannot measure delta.")
        return

    deltas_s = sorted(deltas)
    n = len(deltas_s)
    p50  = deltas_s[n // 2]
    p95  = deltas_s[int(n * 0.95)]
    p99  = deltas_s[int(n * 0.99)]
    above_5s  = sum(1 for d in deltas if d > DEMO_CRITICAL_MS)
    above_30s = sum(1 for d in deltas if d > PROPOSED_CRITICAL)

    print(f"\n  bookTicker delta distribution ({n} msgs in {duration_s:.0f}s):")
    print(f"    min={deltas_s[0]:.0f}ms  p50={p50:.0f}ms  p95={p95:.0f}ms  "
          f"p99={p99:.0f}ms  max={deltas_s[-1]:.0f}ms")
    print(f"    msgs > {DEMO_CRITICAL_MS//1000}s  (current CRITICAL_MS):  {above_5s} / {n}"
          f"  ({100*above_5s/n:.0f}%)")
    print(f"    msgs > {PROPOSED_CRITICAL//1000}s (proposed CRITICAL_MS): {above_30s} / {n}"
          f"  ({100*above_30s/n:.0f}%)")

    print(f"\n{'─'*64}")
    if first_reconnect_at is not None:
        print(f"  ⚠️  Force-reconnect fires at message #{first_reconnect_at} "
              f"(CONSEC_LIMIT={DEMO_CONSEC_LIMIT} consecutive >{DEMO_CRITICAL_MS//1000}s)")
        print(f"  Current thresholds will trigger repeated reconnects under these conditions.")
        print(f"  ROOT CAUSE CONFIRMED: raise HEARTBEAT_CRITICAL_MS to {PROPOSED_CRITICAL//1000}s in demo preset.")
    else:
        run_rate = above_5s / n if n > 0 else 0
        if run_rate > 0.3:
            print(f"  ⚠️  {above_5s}/{n} msgs exceed {DEMO_CRITICAL_MS//1000}s — force-reconnect will fire eventually.")
            print(f"  ROOT CAUSE LIKELY: conditions in this window were not sustained enough.")
            print(f"  Raise HEARTBEAT_CRITICAL_MS to {PROPOSED_CRITICAL//1000}s as a precaution.")
        else:
            print(f"  ✓  delta is consistently < {DEMO_CRITICAL_MS//1000}s in this window.")
            print(f"  Root cause may be transient. Check logs around the reconnect timestamps.")
    print(f"{'─'*64}")


async def main(runs: int, duration_s: float, ws_base: str, heartbeat_latency: bool) -> None:
    if heartbeat_latency:
        print("WebSocket Diagnostic — HEARTBEAT LATENCY MEASUREMENT")
        print(f"  Measuring bookTicker delta on {PROD_BASE} for 60s")
        print("  Validates whether HEARTBEAT_CRITICAL_MS=5000ms is too tight\n")
        await run_heartbeat_latency(duration_s=60.0)
        return

    print("WebSocket Stability Diagnostic — Binance Futures")
    print(f"  {runs} run(s) × {duration_s/60:.0f} min = ~{runs * duration_s / 60:.0f} min total")
    print(f"  Endpoint: {ws_base}")
    print(f"  Conn A (LOB):        {', '.join(CONN_A_STREAMS)}")
    print(f"  Conn B (Tick+Candle): {', '.join(CONN_B_STREAMS)}")
    print(f"  Thresholds: {THRESHOLDS}s\n")

    all_runs: list[dict[str, StreamStats]] = []
    for i in range(1, runs + 1):
        result = await run_session(i, runs, duration_s, ws_base)
        all_runs.append(result)
        print_session_report(i, runs, result)
        if i < runs:
            print(f"\n  Sleeping 5s before next run …")
            await asyncio.sleep(5)

    print_final_summary(all_runs, ws_base)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance Futures WS stability diagnostic")
    parser.add_argument("--runs",     type=int,   default=3,
                        help="Sessions per endpoint (default: 3)")
    parser.add_argument("--duration", type=float, default=600,
                        help="Seconds/session (default: 600)")
    parser.add_argument("--endpoint", default=PROD_BASE,
                        help=f"WS base URL (default: {PROD_BASE})")
    parser.add_argument("--heartbeat-latency", action="store_true",
                        help="Measure bookTicker 'E' field delta vs local clock on "
                             "fstream.binance.com for 60s.  Validates whether the engine's "
                             "HEARTBEAT_CRITICAL_MS=5000ms threshold fires under current "
                             "network conditions and confirms the force-reconnect root cause.")
    args = parser.parse_args()
    asyncio.run(main(
        runs=args.runs,
        duration_s=args.duration,
        ws_base=args.endpoint,
        heartbeat_latency=args.heartbeat_latency,
    ))
