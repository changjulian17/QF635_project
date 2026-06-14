#!/usr/bin/env python3
"""
Diagnostic MARKET order test for Binance Futures demo account.

Places a real MARKET BUY (~$100 notional), polls for fill, then closes
with a MARKET SELL. Prints full raw API responses at every step so we
can identify why our engine's MARKET orders return executedQty=0.

Run with:
    python scripts/test_market_order_demo.py
"""
import asyncio
import json
import math
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
os.environ["BINANCE_DEMO"]    = "true"
os.environ["BINANCE_TESTNET"] = "false"

from binance import AsyncClient

from config import settings

SYMBOL          = "BTCUSDT"
FUT_STEP        = 0.001
TARGET_NOTIONAL = 100.0   # USD — at or above internal MIN_NOTIONAL ($100)


def _j(obj) -> str:
    return json.dumps(obj, indent=2)


async def run() -> bool:
    print("=" * 60)
    print("Demo Account — MARKET order diagnostic")
    print("=" * 60)

    if not settings.DEMO_BINANCE_API_KEY:
        print("✗ DEMO_BINANCE_API_KEY not set in .env — aborting")
        return False

    client = await AsyncClient.create(
        api_key    = settings.DEMO_BINANCE_API_KEY,
        api_secret = settings.DEMO_BINANCE_API_SECRET,
        testnet    = False,
        demo       = True,
    )
    try:
        # ── 1. Position mode ─────────────────────────────────────────
        pos_mode   = await client.futures_get_position_mode()
        hedge_mode = pos_mode.get("dualSidePosition", False)
        print(f"\n[1] Position mode: {'HEDGE (dualSidePosition=true)' if hedge_mode else 'ONE-WAY'}")
        print(f"    Raw: {pos_mode}")

        # ── 2. Order book ─────────────────────────────────────────────
        book     = await client.futures_order_book(symbol=SYMBOL, limit=5)
        best_ask = float(book["asks"][0][0])
        best_bid = float(book["bids"][0][0])
        print(f"\n[2] {SYMBOL} bid={best_bid:.2f}  ask={best_ask:.2f}  spread={best_ask - best_bid:.4f}")

        # ── 3. MARKET BUY ─────────────────────────────────────────────
        qty    = round(math.ceil(TARGET_NOTIONAL / best_ask / FUT_STEP) * FUT_STEP, 3)
        params: dict = {
            "symbol":   SYMBOL,
            "side":     "BUY",
            "type":     "MARKET",
            "quantity": qty,
        }
        if hedge_mode:
            params["positionSide"] = "LONG"
        print(f"\n[3] Placing MARKET BUY — params: {params}")

        t0   = time.monotonic()
        resp = await client.futures_create_order(**params)
        ms   = (time.monotonic() - t0) * 1000
        print(f"    REST response ({ms:.0f}ms):\n{_j(resp)}")

        exec_qty_immediate = float(resp.get("executedQty", "0"))
        order_id = resp.get("orderId")
        status   = resp.get("status", "?")

        # ── 4. Poll 2 s later if not immediately filled ───────────────
        if exec_qty_immediate == 0.0:
            print(f"\n[4] executedQty=0 immediately (status={status}) — polling in 2 s…")
            await asyncio.sleep(2.0)
            polled   = await client.futures_get_order(symbol=SYMBOL, orderId=order_id)
            print(f"    Polled:\n{_j(polled)}")
            exec_qty = float(polled.get("executedQty", "0"))
            if exec_qty == 0.0:
                print("\n✗ MARKET BUY NOT FILLED after 2 s poll")
                if hedge_mode:
                    print("  → Account is in HEDGE mode. Check positionSide handling.")
                else:
                    print("  → ONE-WAY mode. Demo may need different params or has no liquidity.")
                return False
            print(f"\n✓ MARKET BUY FILLED (async): {exec_qty} @ {polled.get('avgPrice')}")
        else:
            exec_qty = exec_qty_immediate
            print(f"\n✓ MARKET BUY FILLED immediately: {exec_qty} @ {resp.get('avgPrice')}")

        # ── 5. Close with MARKET SELL ─────────────────────────────────
        # Wait for position to settle on demo before sending the close.
        print("\n[5] Waiting 2 s for position to settle before close…")
        await asyncio.sleep(2.0)

        close_params: dict = {
            "symbol":   SYMBOL,
            "side":     "SELL",
            "type":     "MARKET",
            "quantity": exec_qty,
        }
        if hedge_mode:
            close_params["positionSide"] = "LONG"
        # No reduceOnly — demo rejects it, and production FuturesMarketOrder doesn't send it either.
        print(f"    Closing with MARKET SELL — params: {close_params}")
        close_resp = await client.futures_create_order(**close_params)
        print(f"    Close response:\n{_j(close_resp)}")

        close_qty = float(close_resp.get("executedQty", "0"))
        if close_qty == 0.0:
            close_order_id = close_resp.get("orderId")
            print(f"\n[6] Close executedQty=0 (status={close_resp.get('status')}) — polling 2 s…")
            await asyncio.sleep(2.0)
            try:
                close_polled = await client.futures_get_order(symbol=SYMBOL, orderId=close_order_id)
                print(f"    Polled close:\n{_j(close_polled)}")
                close_qty = float(close_polled.get("executedQty", "0"))
            except Exception as exc:
                print(f"    Poll failed: {exc}")

        if close_qty > 0:
            print(f"\n✓ Position closed: {close_qty} @ {close_resp.get('avgPrice') or close_polled.get('avgPrice')}")
            return True
        else:
            print(f"\n✗ Close NOT filled (executedQty={close_qty}) — open position remains!")
            return False

    finally:
        await client.close_connection()


if __name__ == "__main__":
    ok = asyncio.run(run())
    print("\n" + "=" * 60)
    print("PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)
