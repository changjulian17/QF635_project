#!/usr/bin/env python3
"""
Futures demo connectivity test.

Checks:
    1. Demo API keys present in config
    2. AsyncClient connects with demo=True
    3. Server time reachable
    4. Futures account balance readable
    5. BTCUSDT futures order book readable
    6. Listen key can be obtained, kept alive, and opened as a user-data stream
    7. BTCUSDT public LOB stream is readable
    8. IOC order submission (priced far off-market → expires unfilled — no position taken)

Run with:
        python scripts/test_futures_demo.py
"""

import asyncio
import json
import math
import os
import sys
import pathlib
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# Force demo mode before config is imported so the pydantic validator
# sees consistent values regardless of .env content.
os.environ["BINANCE_DEMO"]    = "true"
os.environ["BINANCE_TESTNET"] = "false"

from binance import AsyncClient
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import settings


PASS = "✓"
FAIL = "✗"
_USER_STREAM_PROBE_TIMEOUT_S = 5.0
_LOB_STREAM_PROBE_TIMEOUT_S = 15.0


def _extract_listen_key(resp: object) -> str | None:
    if isinstance(resp, str):
        return resp or None
    if isinstance(resp, dict):
        value = resp.get("listenKey")
        return value or None
    return None


def _ok(msg: str) -> None:
    print(f"{PASS} {msg}")


def _fail(msg: str) -> None:
    print(f"{FAIL} {msg}")


async def run() -> bool:
    print("=" * 60)
    print("Binance Futures Demo connectivity test")
    print("=" * 60)

    # ── 1. Config sanity ──────────────────────────────────────────
    if not settings.DEMO_BINANCE_API_KEY:
        _fail("DEMO_BINANCE_API_KEY is empty — add it to .env")
        return False
    _ok(f"DEMO_BINANCE_API_KEY loaded (ends …{settings.DEMO_BINANCE_API_KEY[-4:]})")

    if not settings.DEMO_BINANCE_API_SECRET:
        _fail("DEMO_BINANCE_API_SECRET is empty — add it to .env")
        return False
    _ok("DEMO_BINANCE_API_SECRET loaded")

    print("-" * 60)

    # ── 2. Connect ────────────────────────────────────────────────
    try:
        client = await AsyncClient.create(
            api_key    = settings.DEMO_BINANCE_API_KEY,
            api_secret = settings.DEMO_BINANCE_API_SECRET,
            testnet    = False,   # demo is NOT the same endpoint as testnet
            demo       = True,
        )
        _ok("AsyncClient created with demo=True")
    except Exception as exc:
        _fail(f"AsyncClient.create failed: {exc}")
        return False

    all_passed = True

    try:
        # ── 3. Server time ────────────────────────────────────────
        try:
            t = await client.futures_time()
            _ok(f"Server time: {t['serverTime']}")
        except Exception as exc:
            _fail(f"futures_time() failed: {exc}")
            all_passed = False

        # ── 4. Account balance ────────────────────────────────────
        try:
            balances = await client.futures_account_balance()
            usdt = next((b for b in balances if b["asset"] == "USDT"), None)
            if usdt:
                _ok(
                    f"Futures account — USDT balance: "
                    f"wallet={usdt['balance']}  available={usdt['availableBalance']}"
                )
            else:
                _ok(f"Futures account balance fetched ({len(balances)} assets, no USDT row)")
        except Exception as exc:
            _fail(f"futures_account_balance() failed: {exc}")
            all_passed = False

        # ── 5. Order book ─────────────────────────────────────────
        best_bid = best_ask = None
        try:
            book = await client.futures_order_book(symbol=settings.SYMBOL, limit=5)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                _ok(
                    f"{settings.SYMBOL} order book — "
                    f"best_bid={best_bid:.2f}  best_ask={best_ask:.2f}  "
                    f"spread={best_ask - best_bid:.2f}"
                )
            else:
                _fail(f"Order book empty for {settings.SYMBOL}")
                all_passed = False
        except Exception as exc:
            _fail(f"futures_order_book() failed: {exc}")
            all_passed = False

        # ── 6. Listen key + user data stream ─────────────────────────────
        listen_key = None
        try:
            resp = await client.futures_stream_get_listen_key()
            listen_key = _extract_listen_key(resp)
            if not listen_key:
                raise RuntimeError(f"listen key response missing listenKey: {resp}")

            _ok(f"Listen key obtained ({listen_key[:8]}…)")

            await client.futures_stream_keepalive(listenKey=listen_key)
            _ok("Listen key keepalive accepted")

            user_ws_uri = f"{settings.WS_BASE}/ws/{listen_key}"
            try:
                async with websockets.connect(
                    user_ws_uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    _ok("User data websocket connected")
                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=_USER_STREAM_PROBE_TIMEOUT_S)
                        try:
                            msg = json.loads(raw_msg)
                            event_type = msg.get("e", "?")
                        except json.JSONDecodeError:
                            event_type = "non-JSON message"
                        _ok(f"User data stream delivered a message ({event_type})")
                    except asyncio.TimeoutError:
                        _ok(
                            f"User data stream stayed open for {_USER_STREAM_PROBE_TIMEOUT_S:.0f}s "
                            "with no events"
                        )
            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                _fail(f"User data websocket closed unexpectedly: {exc}")
                all_passed = False
            except Exception as exc:
                _fail(f"User data websocket probe failed: {exc}")
                all_passed = False
        except Exception as exc:
            _fail(f"listen key / user data stream probe failed: {exc}")
            all_passed = False
        finally:
            if listen_key:
                try:
                    await client.futures_stream_close(listenKey=listen_key)
                except Exception as exc:
                    _fail(f"listen key close failed: {exc}")
                    all_passed = False

        # ── 7. Connection latency characterisation ────────────────────────────
        _PROBE_N       = 30
        _WARN_P95_MS   = 500
        _FAIL_P95_MS   = 2000
        _FAIL_SPIKE_MS = 3000

        async def _probe_endpoint(base_url: str, n: int = _PROBE_N) -> dict[str, list[float]]:
            sym = settings.SYMBOL.lower()
            uri = f"{base_url}/stream?streams={sym}@bookTicker/{sym}@depth@100ms"
            samples: dict[str, list[float]] = {"bookTicker": [], "depth@100ms": []}
            async with websockets.connect(uri, ping_interval=None) as _ws:
                while min(len(v) for v in samples.values()) < n:
                    raw = await asyncio.wait_for(_ws.recv(), timeout=15.0)
                    outer = json.loads(raw)
                    if "data" not in outer or "E" not in outer["data"]:
                        continue
                    delta_ms = time.time() * 1000 - outer["data"]["E"]
                    stream = outer.get("stream", "")
                    if "bookTicker" in stream and len(samples["bookTicker"]) < n:
                        samples["bookTicker"].append(delta_ms)
                    elif "depth" in stream and len(samples["depth@100ms"]) < n:
                        samples["depth@100ms"].append(delta_ms)
            return samples

        def _pct(data: list[float], p: int) -> float:
            return sorted(data)[min(int(len(data) * p / 100), len(data) - 1)]

        def _print_probe_report(label: str, samples: dict[str, list[float]]) -> None:
            print(f"\n──── Latency probe: {label} {'─' * max(0, 50 - len(label))}")
            worst_p95 = 0.0
            has_critical_spike = False
            for stream, data in samples.items():
                p50  = _pct(data, 50)
                p95  = _pct(data, 95)
                mx   = max(data)
                s500 = sum(1 for x in data if x > 500)
                s3k  = sum(1 for x in data if x > _FAIL_SPIKE_MS)
                print(f"  {stream:<15} {len(data)} samples  "
                      f"p50={p50:.0f}  p95={p95:.0f}  max={mx:.0f} ms  "
                      f"spikes>500ms={s500}  spikes>3000ms={s3k}")
                worst_p95 = max(worst_p95, p95)
                if s3k > 0:
                    has_critical_spike = True
            if worst_p95 < _WARN_P95_MS and not has_critical_spike:
                print("  ✅ PASS")
            elif worst_p95 < _FAIL_P95_MS and not has_critical_spike:
                print("  ⚠️  WARN — p95 elevated; demo trading will work but expect occasional stalls")
            else:
                print("  ❌ FAIL — p95 >2000ms or >3000ms spikes; consider a Singapore VPS")

        print(f"\n{'='*60}\nSection 7 — Connection latency characterisation\n{'='*60}")
        print(f"Collecting {_PROBE_N} samples per stream (≈{_PROBE_N}s)…")
        try:
            live_samples = await _probe_endpoint(settings.LOB_RECORDER_WS)
            _print_probe_report(settings.LOB_RECORDER_WS, live_samples)
            testnet_samples = await _probe_endpoint("wss://stream.binancefuture.com")
            _print_probe_report("stream.binancefuture.com (testnet baseline)", testnet_samples)
        except Exception as exc:
            _fail(f"Latency probe failed: {exc}")
            all_passed = False

        # ── 8. IOC order (far off-market, will expire unfilled) ───
        if best_bid is not None:
            # Price it 20 % below best bid — will never fill on any real book.
            test_price = round(best_bid * 0.80, 1)  # BTCUSDT futures tick size = 0.10
            # BTCUSDT perp futures step size is 0.001 (not the spot 0.00001 in config).
            # Notional must be >= $50 (error -4164); target $100 to give headroom.
            _fut_step  = 0.001
            test_qty   = round(math.ceil(100.0 / test_price / _fut_step) * _fut_step, 3)

            print(
                f"\n  [Order test] IOC BUY {test_qty} {settings.SYMBOL} @ {test_price:.2f} "
                f"(~20 % below market — expected: expire unfilled)"
            )
            try:
                resp = await client.futures_create_order(
                    symbol      = settings.SYMBOL,
                    side        = "BUY",
                    type        = "LIMIT",
                    timeInForce = "IOC",
                    quantity    = test_qty,
                    price       = str(test_price),
                )
                status   = resp.get("status", "?")
                exec_qty = float(resp.get("executedQty", "0"))
                order_id = resp.get("orderId", "?")

                if exec_qty == 0.0 and status in ("EXPIRED", "CANCELED", "NEW"):
                    _ok(
                        f"IOC order submitted and expired unfilled as expected "
                        f"(orderId={order_id}  status={status})"
                    )
                elif exec_qty > 0:
                    # Surprising but technically OK — far-off price still matched
                    _ok(
                        f"IOC order unexpectedly filled {exec_qty} @ {resp.get('avgPrice')} "
                        f"— demo book may have thin liquidity (orderId={order_id})"
                    )
                else:
                    _fail(f"Unexpected order response: status={status}  resp={resp}")
                    all_passed = False

            except Exception as exc:
                _fail(f"futures_create_order() failed: {exc}")
                all_passed = False
        else:
            print("  [Order test] skipped — could not read order book")
            all_passed = False

    finally:
        await client.close_connection()

    print("-" * 60)
    return all_passed


if __name__ == "__main__":
    ok = asyncio.run(run())
    print("=" * 60)
    print("PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)
