#!/usr/bin/env python3
"""
Integration test: OCO bracket orders against the real Binance Demo (futures) API.

Purpose
-------
Verify that Binance Demo accounts return `algoId` (not `orderId`) for TP/SL
conditional orders, proving the root cause of Issue 1 and confirming that
Fix 1 (BINANCE_DEMO early-return in _place_oco) is the correct remedy.

Sequence
--------
1. Connect to Binance Demo using DEMO_BINANCE_API_KEY / DEMO_BINANCE_API_SECRET
2. Open a minimum-size MARKET LONG (0.001 BTC)
3. Attempt bracket placement:
   a) With reduceOnly=true (original broken path)
   b) Without reduceOnly (also broken — algoId still returned)
4. Print raw API responses, highlighting algoId vs orderId
5. Close position with a MARKET SELL
6. Print verdict: CONFIRMED (bug exists) or UNEXPECTED (orderId returned — bug absent)

Run from project root:
    source .venv/bin/activate
    python scripts/test_oco_demo.py

Requires DEMO_BINANCE_API_KEY and DEMO_BINANCE_API_SECRET in .env.
"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import asyncio
import json
import logging

from binance import AsyncClient
from config import settings

logging.basicConfig(level=logging.WARNING)  # suppress binance library noise

TRADE_QTY = 0.001   # smallest BTCUSDT futures lot
TICK      = 0.10    # price tick size


async def run() -> None:
    if not settings.DEMO_BINANCE_API_KEY or not settings.DEMO_BINANCE_API_SECRET:
        print("✗  DEMO_BINANCE_API_KEY / DEMO_BINANCE_API_SECRET not set in .env")
        print("   Add them and re-run.")
        sys.exit(1)

    print("=" * 60)
    print("  CryptoSentinel — OCO Demo API Integration Test")
    print("=" * 60)
    print(f"  Account type : Binance Demo (demo={settings.BINANCE_DEMO})")
    print(f"  Symbol       : {settings.SYMBOL}")
    print(f"  Trade qty    : {TRADE_QTY} BTC")
    print()

    client = await AsyncClient.create(
        api_key    = settings.DEMO_BINANCE_API_KEY,
        api_secret = settings.DEMO_BINANCE_API_SECRET,
        testnet    = False,
        demo       = True,
    )

    entry_order_id = None

    try:
        # ── 1. Fetch current mid price ────────────────────────────────────────
        book = await client.futures_order_book(symbol=settings.SYMBOL, limit=5)
        best_bid = float(book["bids"][0][0])
        best_ask = float(book["asks"][0][0])
        mid      = (best_bid + best_ask) / 2.0
        print(f"  Order book   : bid={best_bid:,.2f}  ask={best_ask:,.2f}  mid={mid:,.2f}")
        print()

        # ── 2. Open a MARKET LONG ─────────────────────────────────────────────
        print(">>> Step 1: Open MARKET LONG ...")
        entry_resp = await client.futures_create_order(
            symbol   = settings.SYMBOL,
            side     = "BUY",
            type     = "MARKET",
            quantity = TRADE_QTY,
        )
        print(f"    Raw response keys : {list(entry_resp.keys())}")
        status   = entry_resp.get("status", "?")
        exec_qty = float(entry_resp.get("executedQty", 0))
        avg_px   = float(entry_resp.get("avgPrice", 0) or entry_resp.get("price", 0))
        entry_order_id = entry_resp.get("orderId")

        if exec_qty == 0:
            # Demo sometimes returns executedQty=0 — poll once
            await asyncio.sleep(1)
            eid = entry_resp.get("orderId")
            if eid:
                entry_resp = await client.futures_get_order(
                    symbol=settings.SYMBOL, orderId=eid
                )
                exec_qty = float(entry_resp.get("executedQty", 0))
                avg_px   = float(entry_resp.get("avgPrice", 0))
                status   = entry_resp.get("status", "?")

        fill_price = avg_px if avg_px > 0 else mid
        print(f"    Status           : {status}")
        print(f"    Executed qty     : {exec_qty} BTC")
        print(f"    Fill price       : {fill_price:,.2f}")
        print()

        if exec_qty == 0:
            print("✗  Entry unfilled — cannot test brackets. Exiting.")
            return

        # ── 3. Compute bracket prices ─────────────────────────────────────────
        stop_bps   = 25.0                        # 25bps stop
        sl_dist    = fill_price * stop_bps / 10_000
        tp_price   = round(round((fill_price + sl_dist * 3) / TICK) * TICK, 1)
        sl_price   = round(round((fill_price - sl_dist)     / TICK) * TICK, 1)
        sl_limit   = round(round(sl_price * 0.999           / TICK) * TICK, 1)

        print(f"  Bracket prices   : TP={tp_price:,.2f}  SL-stop={sl_price:,.2f}  SL-limit={sl_limit:,.2f}")
        print()

        # ── 4a. TP bracket WITH reduceOnly (original path) ────────────────────
        print(">>> Step 2a: Place TP order (TAKE_PROFIT, reduceOnly=true) ...")
        try:
            tp_resp = await client.futures_create_order(
                symbol      = settings.SYMBOL,
                side        = "SELL",
                type        = "TAKE_PROFIT",
                timeInForce = "GTC",
                quantity    = exec_qty,
                price       = str(tp_price),
                stopPrice   = str(tp_price),
                reduceOnly  = "true",
            )
            _report_bracket_response("TP (reduceOnly=true)", tp_resp)
            # Cancel immediately to avoid dangling order
            oid = tp_resp.get("orderId") or tp_resp.get("algoId")
            if oid and "orderId" in tp_resp:
                await client.futures_cancel_order(symbol=settings.SYMBOL, orderId=oid)
                print(f"    ✓  Cancelled (orderId={oid})")
            elif oid:
                print(f"    ⚠  Cannot cancel via futures_cancel_order — algoId={oid}")
        except Exception as exc:
            print(f"    ✗  Exception: {exc}")
        print()

        # ── 4b. SL bracket WITH reduceOnly ────────────────────────────────────
        print(">>> Step 2b: Place SL order (STOP, reduceOnly=true) ...")
        try:
            sl_resp = await client.futures_create_order(
                symbol      = settings.SYMBOL,
                side        = "SELL",
                type        = "STOP",
                timeInForce = "GTC",
                quantity    = exec_qty,
                price       = str(sl_limit),
                stopPrice   = str(sl_price),
                reduceOnly  = "true",
            )
            _report_bracket_response("SL (reduceOnly=true)", sl_resp)
            oid = sl_resp.get("orderId") or sl_resp.get("algoId")
            if oid and "orderId" in sl_resp:
                await client.futures_cancel_order(symbol=settings.SYMBOL, orderId=oid)
                print(f"    ✓  Cancelled (orderId={oid})")
            elif oid:
                print(f"    ⚠  Cannot cancel via futures_cancel_order — algoId={oid}")
        except Exception as exc:
            print(f"    ✗  Exception: {exc}")
        print()

        # ── 4c. TP bracket WITHOUT reduceOnly ─────────────────────────────────
        print(">>> Step 2c: Place TP order (TAKE_PROFIT, no reduceOnly) ...")
        try:
            tp_resp2 = await client.futures_create_order(
                symbol      = settings.SYMBOL,
                side        = "SELL",
                type        = "TAKE_PROFIT",
                timeInForce = "GTC",
                quantity    = exec_qty,
                price       = str(tp_price),
                stopPrice   = str(tp_price),
            )
            _report_bracket_response("TP (no reduceOnly)", tp_resp2)
            oid = tp_resp2.get("orderId") or tp_resp2.get("algoId")
            if oid and "orderId" in tp_resp2:
                await client.futures_cancel_order(symbol=settings.SYMBOL, orderId=oid)
                print(f"    ✓  Cancelled (orderId={oid})")
            elif oid:
                print(f"    ⚠  Cannot cancel via futures_cancel_order — algoId={oid}")
        except Exception as exc:
            print(f"    ✗  Exception: {exc}")
        print()

    finally:
        # ── 5. Always close position ──────────────────────────────────────────
        print(">>> Step 3: Close position with MARKET SELL ...")
        try:
            close_resp = await client.futures_create_order(
                symbol   = settings.SYMBOL,
                side     = "SELL",
                type     = "MARKET",
                quantity = TRADE_QTY,
            )
            close_qty = float(close_resp.get("executedQty", 0))
            close_px  = float(close_resp.get("avgPrice", 0))
            if close_qty == 0:
                await asyncio.sleep(1)
                oid = close_resp.get("orderId")
                if oid:
                    close_resp = await client.futures_get_order(
                        symbol=settings.SYMBOL, orderId=oid
                    )
                    close_qty = float(close_resp.get("executedQty", 0))
                    close_px  = float(close_resp.get("avgPrice", 0))
            print(f"    ✓  Closed {close_qty} BTC @ {close_px:,.2f}")
        except Exception as exc:
            print(f"    ✗  Close failed: {exc}")
        print()

        await client.close_connection()

    # ── 6. Verdict ────────────────────────────────────────────────────────────
    print("=" * 60)
    print("  VERDICT")
    print("=" * 60)
    print()
    print("  See 'algoId' key in bracket responses above?")
    print()
    print("  YES (expected) →  CONFIRMED: Binance Demo routes TP/SL through")
    print("                    the Algo Conditional API, returning algoId.")
    print("                    Fix 1 (BINANCE_DEMO early-return) is CORRECT.")
    print("                    The old code raised KeyError: orderId absent")
    print("                    on every trade → emergency close every time.")
    print()
    print("  NO (unexpected) → algoId absent — verify DEMO credentials are")
    print("                    for a real Demo account (not testnet).")
    print()


def _report_bracket_response(label: str, resp: dict) -> None:
    has_order_id = "orderId" in resp
    has_algo_id  = "algoId"  in resp
    print(f"    Label            : {label}")
    print(f"    Response keys    : {list(resp.keys())}")
    if has_order_id:
        print(f"    orderId          : {resp['orderId']}  ← STANDARD (cancel works)")
    if has_algo_id:
        print(f"    algoId           : {resp['algoId']}  ← ALGO API (cancel fails with KeyError)")
    if not has_order_id and not has_algo_id:
        print(f"    Full response    : {json.dumps(resp, indent=6)}")
    if has_algo_id and not has_order_id:
        print(f"    *** BUG CONFIRMED: orderId absent — production code raises KeyError ***")
    elif has_order_id:
        print(f"    ✓  orderId present — standard path works")


if __name__ == "__main__":
    asyncio.run(run())
